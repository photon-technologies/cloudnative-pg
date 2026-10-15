/*
Copyright © contributors to CloudNativePG, established as
CloudNativePG a Series of LF Projects, LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0
*/

package lease

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cloudnative-pg/cloudnative-pg/pkg/management/postgres"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

func lifecycleFixture(t *testing.T, holder string) (*Runnable, *fake.Clientset, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	client := fake.NewClientset(&coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "default"}, Spec: coordinationv1.LeaseSpec{HolderIdentity: ptr.To(holder), LeaseDurationSeconds: ptr.To(int32(15))}})
	instance := postgres.NewInstance().WithNamespace("default").WithClusterName("cluster").WithPodName("cluster-2")
	instance.PgData = t.TempDir()
	if err := os.WriteFile(filepath.Join(instance.PgData, "standby.signal"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	r := New(client, instance)
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("runnable failed: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("runnable did not stop")
		}
	})
	return r, client, ctx
}

func lifecycleConfig() Config {
	return Config{LeaseDuration: 3 * time.Second, RenewDeadline: 2 * time.Second, RetryPeriod: 100 * time.Millisecond, ReleasedLeaseDuration: time.Second}
}
func leaseHolder(t *testing.T, ctx context.Context, c *fake.Clientset) string {
	t.Helper()
	l, err := c.CoordinationV1().Leases("default").Get(ctx, "cluster", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return ptr.Deref(l.Spec.HolderIdentity, "")
}
func setHolder(t *testing.T, ctx context.Context, c *fake.Clientset, holder string) {
	t.Helper()
	l, err := c.CoordinationV1().Leases("default").Get(ctx, "cluster", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	l.Spec.HolderIdentity = ptr.To(holder)
	_, err = c.CoordinationV1().Leases("default").Update(ctx, l, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
}
func remainsHolder(t *testing.T, ctx context.Context, c *fake.Clientset, want string) {
	t.Helper()
	deadline := time.Now().Add(350 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := leaseHolder(t, ctx, c); got != want {
			t.Fatalf("holder %q, want %q", got, want)
		}
		time.Sleep(15 * time.Millisecond)
	}
}

func TestCancelledCandidateCannotAcquireReleasedLeaseAndCanBeSelectedAgain(t *testing.T) {
	r, c, ctx := lifecycleFixture(t, "cluster-1")
	attempt, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
	defer cancel()
	if err := r.Acquire(attempt, lifecycleConfig()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected waiting candidate, got %v", err)
	}
	if err := r.CancelCandidate(ctx); err != nil {
		t.Fatal(err)
	}
	setHolder(t, ctx, c, "")
	remainsHolder(t, ctx, c, "")
	// The old held signal and observation timer must not satisfy a new election.
	setHolder(t, ctx, c, "cluster-1")
	again, stop := context.WithTimeout(ctx, 80*time.Millisecond)
	if err := r.Acquire(again, lifecycleConfig()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("obsolete generation satisfied new acquisition: %v", err)
	}
	stop()
	setHolder(t, ctx, c, "")
	if err := r.Acquire(ctx, lifecycleConfig()); err != nil {
		t.Fatal(err)
	}
	if got := leaseHolder(t, ctx, c); got != "cluster-2" {
		t.Fatal(got)
	}
}

func TestHeldCandidateReleasesAfterRenewalStopsAndReacquires(t *testing.T) {
	r, c, ctx := lifecycleFixture(t, "")
	for i := 0; i < 3; i++ {
		if err := r.Acquire(ctx, lifecycleConfig()); err != nil {
			t.Fatal(err)
		}
		if got := leaseHolder(t, ctx, c); got != "cluster-2" {
			t.Fatal(got)
		}
		if err := r.CancelCandidate(ctx); err != nil {
			t.Fatal(err)
		}
		remainsHolder(t, ctx, c, "")
	}
}

func TestPrimaryRetainsLeaseAndRenewalWhenNoLongerTarget(t *testing.T) {
	r, c, ctx := lifecycleFixture(t, "")
	if err := r.Acquire(ctx, lifecycleConfig()); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(r.instance.PgData, "standby.signal")); err != nil {
		t.Fatal(err)
	}
	if err := r.CancelCandidate(ctx); err != nil {
		t.Fatal(err)
	}
	remainsHolder(t, ctx, c, "cluster-2")
	if err := r.Acquire(ctx, lifecycleConfig()); err != nil {
		t.Fatal(err)
	}
}

func TestAmbiguousRoleDoesNotReleaseLease(t *testing.T) {
	r, c, ctx := lifecycleFixture(t, "")
	if err := r.Acquire(ctx, lifecycleConfig()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(r.instance.PgData, "standby.signal")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("standby.signal", path); err != nil {
		t.Fatal(err)
	}
	if err := r.CancelCandidate(ctx); err == nil {
		t.Fatal("ambiguous role accepted")
	}
	remainsHolder(t, ctx, c, "cluster-2")
}

func TestFailedCandidateReleaseCanRetryWithoutResumingElection(t *testing.T) {
	r, c, ctx := lifecycleFixture(t, "")
	if err := r.Acquire(ctx, lifecycleConfig()); err != nil {
		t.Fatal(err)
	}
	// Retire the renewal loop before injecting an error into the release write.
	r.mu.Lock()
	g := r.generation
	g.cancel()
	r.mu.Unlock()
	<-g.done
	failures := 2
	c.PrependReactor("update", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
		if failures > 0 {
			failures--
			return true, nil, errors.New("injected release failure")
		}
		return false, nil, nil
	})
	if err := r.CancelCandidate(ctx); err == nil {
		t.Fatal("release failure ignored")
	}
	if err := r.Acquire(ctx, lifecycleConfig()); err == nil {
		t.Fatal("failed retirement reported ownership")
	}
	if err := r.Acquire(ctx, lifecycleConfig()); err != nil {
		t.Fatal(err)
	}
	if got := leaseHolder(t, ctx, c); got != "cluster-2" {
		t.Fatal(got)
	}
	if err := r.CancelCandidate(ctx); err != nil {
		t.Fatal(err)
	}
	remainsHolder(t, ctx, c, "")
}

type waitObservedContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *waitObservedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func TestConcurrentWaitersObserveCandidateCancellation(t *testing.T) {
	r, _, ctx := lifecycleFixture(t, "cluster-1")
	var wg sync.WaitGroup
	errorsSeen := make(chan error, 8)
	entered := make([]chan struct{}, 8)
	for i := 0; i < 8; i++ {
		entered[i] = make(chan struct{})
		waitCtx := &waitObservedContext{Context: ctx, entered: entered[i]}
		wg.Add(1)
		go func() { defer wg.Done(); errorsSeen <- r.Acquire(waitCtx, lifecycleConfig()) }()
	}
	for _, ready := range entered {
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatal("waiter did not enter election")
		}
	}
	if err := r.CancelCandidate(ctx); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter outcome: %v", err)
		}
	}
}
