package status_test

import (
	"context"
	"sync"
	"time"

	"github.com/awslabs/operatorpkg/status"
	"github.com/awslabs/operatorpkg/test"
	. "github.com/awslabs/operatorpkg/test/expectations"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	prometheus "github.com/prometheus/client_model/go"
	"github.com/samber/lo"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var _ = Describe("Controller", func() {
	var ctx context.Context
	var recorder *record.FakeRecorder
	var controller *status.Controller[*TestObject]
	var kubeClient client.Client
	BeforeEach(func() {
		recorder = record.NewFakeRecorder(10)
		kubeClient = fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()
		controller = status.NewController[*TestObject](kubeClient, recorder)
		ctx = log.IntoContext(context.Background(), ginkgo.GinkgoLogr)
	})
	It("should emit termination metrics when deletion timestamp is set", func() {
		testObject := test.Object(&TestObject{})
		ExpectApplied(ctx, kubeClient, testObject)
		ExpectDeletionTimestampSet(ctx, kubeClient, testObject)
		ExpectReconciled(ctx, controller, testObject)
		metric := GetMetric("operator_termination_current_time_seconds", map[string]string{status.MetricLabelName: testObject.Name})
		Expect(metric).ToNot(BeNil())
		Expect(metric.GetGauge().GetValue()).To(BeNumerically(">", 0))

		// Patch the finalizer
		mergeFrom := client.MergeFrom(testObject.DeepCopyObject().(client.Object))
		testObject.SetFinalizers([]string{})
		Expect(client.IgnoreNotFound(kubeClient.Patch(ctx, testObject, mergeFrom))).To(Succeed())
		ExpectReconciled(ctx, controller, testObject)
		Expect(GetMetric("operator_termination_current_time_seconds", map[string]string{status.MetricLabelName: testObject.Name})).To(BeNil())
		metric = GetMetric("operator_termination_duration_seconds", map[string]string{status.MetricLabelName: testObject.Name})
		Expect(metric).ToNot(BeNil())
		Expect(metric.GetHistogram().GetSampleCount()).To(BeNumerically(">", 0))
	})
	It("should emit metrics and events on a transition", func() {
		testObject := test.Object(&TestObject{})
		testObject.StatusConditions() // initialize conditions

		// conditions not set
		ExpectApplied(ctx, kubeClient, testObject)
		ExpectReconciled(ctx, controller, testObject)

		// Ready Condition
		Expect(GetMetric("operator_status_condition_count", conditionLabels(status.ConditionReady, metav1.ConditionTrue))).To(BeNil())
		Expect(GetMetric("operator_status_condition_count", conditionLabels(status.ConditionReady, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_count", conditionLabels(status.ConditionReady, metav1.ConditionUnknown)).GetGauge().GetValue()).To(BeEquivalentTo(1))
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(status.ConditionReady, metav1.ConditionTrue))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(status.ConditionReady, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(status.ConditionReady, metav1.ConditionUnknown)).GetGauge().GetValue()).ToNot(BeZero())

		// Foo Condition
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeFoo, metav1.ConditionTrue))).To(BeNil())
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeFoo, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeFoo, metav1.ConditionUnknown)).GetGauge().GetValue()).To(BeEquivalentTo(1))
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeFoo, metav1.ConditionTrue))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeFoo, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeFoo, metav1.ConditionUnknown)).GetGauge().GetValue()).ToNot(BeZero())

		// Bar Condition
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeBar, metav1.ConditionTrue))).To(BeNil())
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeBar, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeBar, metav1.ConditionUnknown)).GetGauge().GetValue()).To(BeEquivalentTo(1))
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeBar, metav1.ConditionTrue))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeBar, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeBar, metav1.ConditionUnknown)).GetGauge().GetValue()).ToNot(BeZero())

		Expect(GetMetric("operator_status_condition_transition_seconds")).To(BeNil())
		Expect(GetMetric("operator_status_condition_transitions_total")).To(BeNil())

		Eventually(recorder.Events).Should(BeEmpty())

		// Transition Foo
		time.Sleep(time.Second * 1)
		testObject.StatusConditions().SetTrue(ConditionTypeFoo)
		ExpectApplied(ctx, kubeClient, testObject)
		ExpectReconciled(ctx, controller, testObject)
		ExpectStatusConditions(ctx, kubeClient, FastTimeout, testObject, status.Condition{Type: ConditionTypeFoo, Status: metav1.ConditionTrue})

		// Ready Condition
		Expect(GetMetric("operator_status_condition_count", conditionLabels(status.ConditionReady, metav1.ConditionTrue))).To(BeNil())
		Expect(GetMetric("operator_status_condition_count", conditionLabels(status.ConditionReady, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_count", conditionLabels(status.ConditionReady, metav1.ConditionUnknown)).GetGauge().GetValue()).To(BeEquivalentTo(1))
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(status.ConditionReady, metav1.ConditionTrue))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(status.ConditionReady, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(status.ConditionReady, metav1.ConditionUnknown)).GetGauge().GetValue()).ToNot(BeZero())

		// Foo Condition
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeFoo, metav1.ConditionTrue)).GetGauge().GetValue()).To(BeEquivalentTo(1))
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeFoo, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeFoo, metav1.ConditionUnknown))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeFoo, metav1.ConditionTrue))).ToNot(BeZero())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeFoo, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeFoo, metav1.ConditionUnknown))).To(BeNil())

		// Bar Condition
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeBar, metav1.ConditionTrue))).To(BeNil())
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeBar, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeBar, metav1.ConditionUnknown)).GetGauge().GetValue()).To(BeEquivalentTo(1))
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeBar, metav1.ConditionTrue))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeBar, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeBar, metav1.ConditionUnknown)).GetGauge().GetValue()).ToNot(BeZero())

		Expect(GetMetric("operator_status_condition_transition_seconds", conditionLabels(status.ConditionReady, metav1.ConditionTrue))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transition_seconds", conditionLabels(status.ConditionReady, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transition_seconds", conditionLabels(status.ConditionReady, metav1.ConditionUnknown))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transition_seconds", conditionLabels(ConditionTypeFoo, metav1.ConditionTrue))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transition_seconds", conditionLabels(ConditionTypeFoo, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transition_seconds", conditionLabels(ConditionTypeFoo, metav1.ConditionUnknown)).GetHistogram().GetSampleCount()).To(BeNumerically(">", 0))
		Expect(GetMetric("operator_status_condition_transition_seconds", conditionLabels(ConditionTypeBar, metav1.ConditionTrue))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transition_seconds", conditionLabels(ConditionTypeBar, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transition_seconds", conditionLabels(ConditionTypeBar, metav1.ConditionUnknown))).To(BeNil())

		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(status.ConditionReady, metav1.ConditionTrue))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(status.ConditionReady, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(status.ConditionReady, metav1.ConditionUnknown))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(ConditionTypeFoo, metav1.ConditionTrue)).GetCounter().GetValue()).To(BeEquivalentTo(1))
		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(ConditionTypeFoo, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(ConditionTypeFoo, metav1.ConditionUnknown))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(ConditionTypeBar, metav1.ConditionTrue))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(ConditionTypeBar, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(ConditionTypeBar, metav1.ConditionUnknown))).To(BeNil())

		Expect(recorder.Events).To(Receive(Equal("Normal Foo Status condition transitioned, Type: Foo, Status: Unknown -> True, Reason: Foo")))

		// Transition Bar, root condition should also flip
		testObject.StatusConditions().SetTrueWithReason(ConditionTypeBar, "reason", "message")
		ExpectApplied(ctx, kubeClient, testObject)
		ExpectReconciled(ctx, controller, testObject)
		ExpectStatusConditions(ctx, kubeClient, FastTimeout, testObject, status.Condition{Type: ConditionTypeBar, Status: metav1.ConditionTrue, Reason: "reason", Message: "message"})

		// Ready Condition
		Expect(GetMetric("operator_status_condition_count", conditionLabels(status.ConditionReady, metav1.ConditionTrue)).GetGauge().GetValue()).To(BeEquivalentTo(1))
		Expect(GetMetric("operator_status_condition_count", conditionLabels(status.ConditionReady, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_count", conditionLabels(status.ConditionReady, metav1.ConditionUnknown))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(status.ConditionReady, metav1.ConditionTrue)).GetGauge().GetValue()).ToNot(BeZero())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(status.ConditionReady, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(status.ConditionReady, metav1.ConditionUnknown))).To(BeNil())

		// Foo Condition
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeFoo, metav1.ConditionTrue)).GetGauge().GetValue()).To(BeEquivalentTo(1))
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeFoo, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeFoo, metav1.ConditionUnknown))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeFoo, metav1.ConditionTrue)).GetGauge().GetValue()).ToNot(BeZero())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeFoo, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeFoo, metav1.ConditionUnknown))).To(BeNil())

		// Bar Condition
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeBar, metav1.ConditionTrue)).GetGauge().GetValue()).To(BeEquivalentTo(1))
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeBar, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeBar, metav1.ConditionUnknown))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeBar, metav1.ConditionTrue)).GetGauge().GetValue()).ToNot(BeZero())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeBar, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeBar, metav1.ConditionUnknown))).To(BeNil())

		// Ready Condition
		Expect(GetMetric("operator_status_condition_count", conditionLabels(status.ConditionReady, metav1.ConditionTrue)).GetGauge().GetValue()).To(BeEquivalentTo(1))
		Expect(GetMetric("operator_status_condition_count", conditionLabels(status.ConditionReady, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_count", conditionLabels(status.ConditionReady, metav1.ConditionUnknown))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(status.ConditionReady, metav1.ConditionTrue)).GetGauge().GetValue()).ToNot(BeZero())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(status.ConditionReady, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(status.ConditionReady, metav1.ConditionUnknown))).To(BeNil())

		// Foo Condition
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeFoo, metav1.ConditionTrue)).GetGauge().GetValue()).To(BeEquivalentTo(1))
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeFoo, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeFoo, metav1.ConditionUnknown))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeFoo, metav1.ConditionTrue)).GetGauge().GetValue()).ToNot(BeZero())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeFoo, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeFoo, metav1.ConditionUnknown))).To(BeNil())

		// Bar Condition
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeBar, metav1.ConditionTrue)).GetGauge().GetValue()).To(BeEquivalentTo(1))
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeBar, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeBar, metav1.ConditionUnknown))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeBar, metav1.ConditionTrue)).GetGauge().GetValue()).ToNot(BeZero())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeBar, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeBar, metav1.ConditionUnknown))).To(BeNil())

		Expect(GetMetric("operator_status_condition_transition_seconds", conditionLabels(status.ConditionReady, metav1.ConditionTrue))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transition_seconds", conditionLabels(status.ConditionReady, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transition_seconds", conditionLabels(status.ConditionReady, metav1.ConditionUnknown)).GetHistogram().GetSampleCount()).To(BeNumerically(">", 0))
		Expect(GetMetric("operator_status_condition_transition_seconds", conditionLabels(ConditionTypeFoo, metav1.ConditionTrue))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transition_seconds", conditionLabels(ConditionTypeFoo, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transition_seconds", conditionLabels(ConditionTypeFoo, metav1.ConditionUnknown)).GetHistogram().GetSampleCount()).To(BeNumerically(">", 0))
		Expect(GetMetric("operator_status_condition_transition_seconds", conditionLabels(ConditionTypeBar, metav1.ConditionTrue))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transition_seconds", conditionLabels(ConditionTypeBar, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transition_seconds", conditionLabels(ConditionTypeBar, metav1.ConditionUnknown)).GetHistogram().GetSampleCount()).To(BeNumerically(">", 0))

		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(status.ConditionReady, metav1.ConditionTrue)).GetCounter().GetValue()).To(BeEquivalentTo(1))
		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(status.ConditionReady, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(status.ConditionReady, metav1.ConditionUnknown))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(ConditionTypeFoo, metav1.ConditionTrue)).GetCounter().GetValue()).To(BeEquivalentTo(1))
		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(ConditionTypeFoo, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(ConditionTypeFoo, metav1.ConditionUnknown))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(ConditionTypeBar, metav1.ConditionTrue)).GetCounter().GetValue()).To(BeEquivalentTo(1))
		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(ConditionTypeBar, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(ConditionTypeBar, metav1.ConditionUnknown))).To(BeNil())

		Expect(recorder.Events).To(Receive(Equal("Normal Bar Status condition transitioned, Type: Bar, Status: Unknown -> True, Reason: reason, Message: message")))
		Expect(recorder.Events).To(Receive(Equal("Normal Ready Status condition transitioned, Type: Ready, Status: Unknown -> True, Reason: Ready")))

		// Delete the object, state should clear
		ExpectDeleted(ctx, kubeClient, testObject)
		ExpectReconciled(ctx, controller, testObject)

		// Ready Condition
		Expect(GetMetric("operator_status_condition_count", conditionLabels(status.ConditionReady, metav1.ConditionTrue))).To(BeNil())
		Expect(GetMetric("operator_status_condition_count", conditionLabels(status.ConditionReady, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_count", conditionLabels(status.ConditionReady, metav1.ConditionUnknown))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(status.ConditionReady, metav1.ConditionTrue))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(status.ConditionReady, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(status.ConditionReady, metav1.ConditionUnknown))).To(BeNil())

		// Foo Condition
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeFoo, metav1.ConditionTrue))).To(BeNil())
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeFoo, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeFoo, metav1.ConditionUnknown))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeFoo, metav1.ConditionTrue))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeFoo, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeFoo, metav1.ConditionUnknown))).To(BeNil())

		// Bar Condition
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeBar, metav1.ConditionTrue))).To(BeNil())
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeBar, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_count", conditionLabels(ConditionTypeBar, metav1.ConditionUnknown))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeBar, metav1.ConditionTrue))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeBar, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_current_status_seconds", conditionLabels(ConditionTypeBar, metav1.ConditionUnknown))).To(BeNil())
	})
	It("should emit transition total metrics for abnormal conditions", func() {
		testObject := test.Object(&TestObject{})
		testObject.StatusConditions() // initialize conditions

		// conditions not set
		ExpectApplied(ctx, kubeClient, testObject)
		ExpectReconciled(ctx, controller, testObject)

		// set the baz condition and transition it to true
		testObject.StatusConditions().SetTrue(ConditionTypeBaz)

		ExpectApplied(ctx, kubeClient, testObject)
		ExpectReconciled(ctx, controller, testObject)
		ExpectStatusConditions(ctx, kubeClient, FastTimeout, testObject, status.Condition{Type: ConditionTypeBaz, Status: metav1.ConditionTrue, Reason: ConditionTypeBaz, Message: ""})

		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(ConditionTypeBaz, metav1.ConditionTrue)).GetCounter().GetValue()).To(BeEquivalentTo(1))
		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(ConditionTypeBaz, metav1.ConditionFalse))).To(BeNil())
		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(ConditionTypeBaz, metav1.ConditionUnknown))).To(BeNil())

		// set the bar condition and transition it to false
		testObject.StatusConditions().SetFalse(ConditionTypeBaz, "reason", "message")

		ExpectApplied(ctx, kubeClient, testObject)
		ExpectReconciled(ctx, controller, testObject)
		ExpectStatusConditions(ctx, kubeClient, FastTimeout, testObject, status.Condition{Type: ConditionTypeBaz, Status: metav1.ConditionFalse, Reason: "reason", Message: "message"})

		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(ConditionTypeBaz, metav1.ConditionTrue)).GetCounter().GetValue()).To(BeEquivalentTo(1))
		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(ConditionTypeBaz, metav1.ConditionFalse)).GetCounter().GetValue()).To(BeEquivalentTo(1))
		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(ConditionTypeBaz, metav1.ConditionUnknown))).To(BeNil())

		// clear the condition and don't expect the metrics to change
		_ = testObject.StatusConditions().Clear(ConditionTypeBaz)

		ExpectApplied(ctx, kubeClient, testObject)
		ExpectReconciled(ctx, controller, testObject)

		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(ConditionTypeBaz, metav1.ConditionTrue)).GetCounter().GetValue()).To(BeEquivalentTo(1))
		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(ConditionTypeBaz, metav1.ConditionFalse)).GetCounter().GetValue()).To(BeEquivalentTo(1))
		Expect(GetMetric("operator_status_condition_transitions_total", conditionLabels(ConditionTypeBaz, metav1.ConditionUnknown))).To(BeNil())
	})
	It("should not race when reconciling status conditions simultaneously", func() {
		var objs []*TestObject
		for range 100 {
			testObject := test.Object(&TestObject{})
			testObject.StatusConditions() // initialize conditions
			// conditions not set
			ExpectApplied(ctx, kubeClient, testObject)
			objs = append(objs, testObject)
		}

		// Run 100 object reconciles at once to attempt to trigger a data raceg
		var wg sync.WaitGroup
		for _, obj := range objs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer GinkgoRecover()

				ExpectReconciled(ctx, controller, obj)
			}()
		}

		for _, obj := range objs {
			// set the baz condition and transition it to true
			obj.StatusConditions().SetTrue(ConditionTypeBaz)
			ExpectApplied(ctx, kubeClient, obj)
		}

		// Run 100 object reconciles at once to attempt to trigger a data race
		for _, obj := range objs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer GinkgoRecover()

				ExpectReconciled(ctx, controller, obj)
			}()
		}
	})
})

// GetMetric attempts to find a metric given name and labels
// If no metric is found, the *prometheus.Metric will be nil
func GetMetric(name string, labels ...map[string]string) *prometheus.Metric {
	family, found := lo.Find(lo.Must(metrics.Registry.Gather()), func(family *prometheus.MetricFamily) bool { return family.GetName() == name })
	if !found {
		return nil
	}
	for _, m := range family.Metric {
		temp := lo.Assign(labels...)
		for _, labelPair := range m.Label {
			if v, ok := temp[labelPair.GetName()]; ok && v == labelPair.GetValue() {
				delete(temp, labelPair.GetName())
			}
		}
		if len(temp) == 0 {
			return m
		}
	}
	return nil
}

func conditionLabels(t status.ConditionType, s metav1.ConditionStatus) map[string]string {
	return map[string]string{
		status.MetricLabelConditionType:   string(t),
		status.MetricLabelConditionStatus: string(s),
	}
}
