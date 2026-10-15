//go:build integration

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
	"testing"
	"time"

	"github.com/cloudnative-pg/cloudnative-pg/pkg/management/postgres"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// Exercise conditional Lease updates against the API server, not only a fake
// client that accepts stale ResourceVersions. No real PostgreSQL is started.
func TestCancelledCandidateAgainstKubernetesAPI(t *testing.T) {
	environment := &envtest.Environment{}
	config, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Error(err)
		}
	})
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "lease-regression"}}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	_, err = client.CoordinationV1().Leases("lease-regression").Create(ctx, &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "cluster"}, Spec: coordinationv1.LeaseSpec{HolderIdentity: ptr.To("cluster-1"), LeaseDurationSeconds: ptr.To(int32(3))}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	create := func(name string, standby bool) (*Runnable, context.CancelFunc, <-chan error) {
		instance := postgres.NewInstance().WithNamespace("lease-regression").WithClusterName("cluster").WithPodName(name)
		instance.PgData = t.TempDir()
		if standby {
			if err := os.WriteFile(filepath.Join(instance.PgData, "standby.signal"), nil, 0600); err != nil {
				t.Fatal(err)
			}
		}
		r := New(client, instance)
		runCtx, stop := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- r.Start(runCtx); close(done) }()
		t.Cleanup(stop)
		return r, stop, done
	}
	r1, stop1, done1 := create("cluster-1", false)
	r2, stop2, done2 := create("cluster-2", true)
	r3, stop3, done3 := create("cluster-3", true)
	t.Cleanup(func() {
		for _, p := range []struct {
			stop context.CancelFunc
			done <-chan error
		}{{stop1, done1}, {stop2, done2}, {stop3, done3}} {
			p.stop()
			select {
			case err, ok := <-p.done:
				if ok && err != nil {
					t.Error(err)
				}
			case <-time.After(3 * time.Second):
				t.Error("runnable did not stop")
			}
		}
	})
	// A completed runnable closes its result channel so cleanup remains idempotent.
	waitStop := func(stop context.CancelFunc, done <-chan error) {
		stop()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if err := r1.Acquire(ctx, lifecycleConfig()); err != nil {
		t.Fatal(err)
	}
	short, end := context.WithTimeout(ctx, 150*time.Millisecond)
	if err := r2.Acquire(short, lifecycleConfig()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("candidate unexpectedly acquired: %v", err)
	}
	end()
	if err := r2.CancelCandidate(ctx); err != nil {
		t.Fatal(err)
	}
	waitStop(stop1, done1)
	if err := r1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r3.Acquire(ctx, lifecycleConfig()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		l, err := client.CoordinationV1().Leases("lease-regression").Get(ctx, "cluster", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if ptr.Deref(l.Spec.HolderIdentity, "") != "cluster-3" {
			t.Fatal("cancelled candidate stole primary lease")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := r3.CancelCandidate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r2.Acquire(ctx, lifecycleConfig()); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(r2.instance.PgData, "standby.signal")); err != nil {
		t.Fatal(err)
	}
	if err := r2.CancelCandidate(ctx); err != nil {
		t.Fatal(err)
	}
	l, err := client.CoordinationV1().Leases("lease-regression").Get(ctx, "cluster", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if ptr.Deref(l.Spec.HolderIdentity, "") != "cluster-2" {
		t.Fatal("primary lost its lease")
	}
}
