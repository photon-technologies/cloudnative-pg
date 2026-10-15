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

package controller

import (
	"context"
	"errors"
	"github.com/cloudnative-pg/cloudnative-pg/internal/cmd/manager/instance/run/lease"
	"github.com/cloudnative-pg/cloudnative-pg/internal/cnpi/plugin/repository"
	"k8s.io/utils/ptr"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	apiv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	schemeBuilder "github.com/cloudnative-pg/cloudnative-pg/internal/scheme"
	"github.com/cloudnative-pg/cloudnative-pg/internal/webhook/guard"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/certs"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/management/postgres"
	instancecertificate "github.com/cloudnative-pg/cloudnative-pg/pkg/reconciler/instance/certificate"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// invalidClusterValidator drives the admission guard's validation-failure path,
// so EnsureResourceIsAdmitted short-circuits the reconcile loop with a requeue,
// reproducing an invalid cached Cluster.
type invalidClusterValidator struct{}

func (invalidClusterValidator) ValidateCreate(context.Context, *apiv1.Cluster) (admission.Warnings, error) {
	return nil, errors.New("cluster is invalid")
}

func (invalidClusterValidator) ValidateUpdate(
	context.Context, *apiv1.Cluster, *apiv1.Cluster,
) (admission.Warnings, error) {
	return nil, nil
}

func (invalidClusterValidator) ValidateDelete(context.Context, *apiv1.Cluster) (admission.Warnings, error) {
	return nil, nil
}

var _ = Describe("instance reconciler health probe certificate", func() {
	It("loads the server certificate even when the cached Cluster fails admission validation",
		func(ctx SpecContext) {
			const (
				namespace        = "default"
				clusterName      = "cluster-example"
				serverSecretName = clusterName + "-server"
			)

			root, err := certs.CreateRootCA("common-name", "organization-unit")
			Expect(err).ToNot(HaveOccurred())
			pair, err := root.CreateAndSignPair("host", certs.CertTypeServer, nil)
			Expect(err).ToNot(HaveOccurred())
			serverSecret := pair.GenerateCertificateSecret(namespace, serverSecretName)

			cluster := &apiv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: clusterName},
				Status: apiv1.ClusterStatus{
					Certificates: apiv1.CertificatesStatus{
						CertificatesConfiguration: apiv1.CertificatesConfiguration{
							ServerTLSSecret: serverSecretName,
						},
					},
				},
			}

			fakeClient := fake.NewClientBuilder().
				WithScheme(schemeBuilder.BuildWithAllKnownScheme()).
				WithObjects(cluster, serverSecret).
				Build()

			pgInstance := postgres.NewInstance().
				WithNamespace(namespace).
				WithPodName(clusterName + "-1").
				WithClusterName(clusterName)

			reconciler := &InstanceReconciler{
				client:                fakeClient,
				instance:              pgInstance,
				certificateReconciler: instancecertificate.NewReconciler(fakeClient, pgInstance),
				admission:             &guard.Admission[*apiv1.Cluster]{Validator: invalidClusterValidator{}},
			}

			old := &apiv1.Cluster{Spec: apiv1.ClusterSpec{SmartShutdownTimeout: ptr.To(int32(37))}}
			pgInstance.SetCluster(old)
			result, err := reconciler.Reconcile(ctx, reconcile.Request{})

			// The guard requeued without error: the loop short-circuited, so
			// nothing below the guard (RefreshSecrets included) ran.
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RequeueAfter).To(BeNumerically(">", 0))
			Expect(pgInstance.GetClusterOrDefault()).To(BeIdenticalTo(old))

			// The probe certificate was still loaded, because the load runs
			// before the guard. This fails if the call is moved back below it.
			Expect(pgInstance.GetServerCertificate()).ToNot(BeNil())
		})
})

type recordingCandidateCancellation struct{ called bool }

func (r *recordingCandidateCancellation) Acquire(context.Context, lease.Config) error { return nil }
func (r *recordingCandidateCancellation) CancelCandidate(context.Context) error {
	r.called = true
	return nil
}

var _ = Describe("instance configuration during plugin failure", func() {
	DescribeTable("refreshes admitted shutdown settings before a missing plugin stops reconciliation", func(ctx SpecContext, target string, wantCancellation bool) {
		const name = "cluster-missing-plugin"
		root, err := certs.CreateRootCA("common-name", "organization-unit")
		Expect(err).NotTo(HaveOccurred())
		pair, err := root.CreateAndSignPair("host", certs.CertTypeServer, nil)
		Expect(err).NotTo(HaveOccurred())
		secret := pair.GenerateCertificateSecret("default", name+"-server")
		cluster := &apiv1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name},
			Spec:       apiv1.ClusterSpec{SmartShutdownTimeout: ptr.To(int32(0)), Plugins: []apiv1.PluginConfiguration{{Name: "missing-plugin", Enabled: ptr.To(true), IsWALArchiver: ptr.To(true)}}},
			Status:     apiv1.ClusterStatus{TargetPrimary: target, PluginStatus: []apiv1.PluginStatus{{Name: "missing-plugin", Capabilities: []string{"TYPE_INSTANCE_SIDECAR_INJECTION"}}}, Certificates: apiv1.CertificatesStatus{CertificatesConfiguration: apiv1.CertificatesConfiguration{ServerTLSSecret: secret.Name}}},
		}
		c := fake.NewClientBuilder().WithScheme(schemeBuilder.BuildWithAllKnownScheme()).WithObjects(cluster, secret).Build()
		instance := postgres.NewInstance().WithNamespace("default").WithPodName(name + "-1").WithClusterName(name)
		old := cluster.DeepCopy()
		old.Spec.SmartShutdownTimeout = ptr.To(int32(180))
		instance.SetCluster(old)
		cancelled := &recordingCandidateCancellation{}
		r := &InstanceReconciler{client: c, instance: instance, primaryLeaseAcquirer: cancelled, certificateReconciler: instancecertificate.NewReconciler(c, instance), pluginRepository: repository.New()}
		_, err = r.Reconcile(ctx, reconcile.Request{})
		Expect(err).To(HaveOccurred())
		Expect(instance.GetClusterOrDefault().GetSmartShutdownTimeout()).To(BeZero())
		Expect(cancelled.called).To(Equal(wantCancellation))
	},
		Entry("obsolete candidate", "another-primary", true),
		Entry("selected primary", "cluster-missing-plugin-1", false),
	)
})
