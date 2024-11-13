package status

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	pmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/awslabs/operatorpkg/object"
	"github.com/samber/lo"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/record"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type Controller[T Object] struct {
	objectGVK                     schema.GroupVersionKind
	kubeClient                    client.Client
	eventRecorder                 record.EventRecorder
	observedConditions            sync.Map // map[reconcile.Request]ConditionSet
	terminatingObjects            sync.Map // map[reconcile.Request]DeletionTimestamp
	ConditionDuration             pmetrics.ObservationMetric
	ConditionCount                pmetrics.GaugeMetric
	ConditionCurrentStatusSeconds pmetrics.GaugeMetric
	ConditionTransitionsTotal     pmetrics.CounterMetric
	TerminationCurrentTimeSeconds pmetrics.GaugeMetric
	TerminationDuration           pmetrics.ObservationMetric
}

type ControllerOpts struct {
	GVK schema.GroupVersionKind
}

func NewController[T Object](client client.Client, eventRecorder record.EventRecorder, opts ...ControllerOpts) *Controller[T] {
	var gvk schema.GroupVersionKind
	switch len(opts) {
	case 0:
		gvk = object.GVK(object.New[T]())
	case 1:
		gvk = opts[0].GVK
	default:
		panic("expected no or one argument for controller options")
	}

	return &Controller[T]{
		objectGVK:                     gvk,
		kubeClient:                    client,
		eventRecorder:                 eventRecorder,
		ConditionDuration:             conditionDurationMetric(strings.ToLower(gvk.Kind)),
		ConditionCount:                conditionCountMetric(strings.ToLower(gvk.Kind)),
		ConditionCurrentStatusSeconds: conditionCurrentStatusSecondsMetric(strings.ToLower(gvk.Kind)),
		ConditionTransitionsTotal:     conditionTransitionsTotalMetric(strings.ToLower(gvk.Kind)),
		TerminationCurrentTimeSeconds: terminationCurrentTimeSecondsMetric(strings.ToLower(gvk.Kind)),
		TerminationDuration:           terminationDurationMetric(strings.ToLower(gvk.Kind)),
	}
}

func (c *Controller[T]) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		For(object.New[T]()).
		WithOptions(controller.Options{MaxConcurrentReconciles: 10}).
		Named(fmt.Sprintf("operatorpkg.%s.status", strings.ToLower(c.objectGVK.Kind))).
		Complete(c)
}

func (c *Controller[T]) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	return c.reconcile(ctx, req, object.New[T]())
}

type GenericObjectController[T client.Object] struct {
	*Controller[*unstructuredAdapter]
}

func NewGenericObjectController[T client.Object](client client.Client, eventRecorder record.EventRecorder) *GenericObjectController[T] {
	return &GenericObjectController[T]{
		Controller: NewController[*unstructuredAdapter](client, eventRecorder, ControllerOpts{
			GVK: object.GVK(object.New[T]()),
		}),
	}
}

func (c *GenericObjectController[T]) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		For(object.New[T]()).
		WithOptions(controller.Options{MaxConcurrentReconciles: 10}).
		Named(fmt.Sprintf("operatorpkg.%s.status", strings.ToLower(reflect.TypeOf(object.New[T]()).Elem().Name()))).
		Complete(c)
}

func (c *GenericObjectController[T]) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	return c.reconcile(ctx, req, NewUnstructuredAdapter(object.New[T]()))
}

func (c *Controller[T]) reconcile(ctx context.Context, req reconcile.Request, o Object) (reconcile.Result, error) {
	gvk := object.GVK(o)

	if err := c.kubeClient.Get(ctx, req.NamespacedName, o); err != nil {
		if errors.IsNotFound(err) {
			c.deletePartialMatchGaugeMetric(c.ConditionCount, ConditionCount, map[string]string{
				MetricLabelGroup:     gvk.Group,
				MetricLabelNamespace: req.Namespace,
				MetricLabelName:      req.Name,
			})
			c.deletePartialMatchGaugeMetric(c.ConditionCurrentStatusSeconds, ConditionCurrentStatusSeconds, map[string]string{
				MetricLabelGroup:     gvk.Group,
				MetricLabelNamespace: req.Namespace,
				MetricLabelName:      req.Name,
			})
			c.deletePartialMatchGaugeMetric(c.TerminationCurrentTimeSeconds, TerminationCurrentTimeSeconds, map[string]string{
				MetricLabelNamespace: req.Namespace,
				MetricLabelName:      req.Name,
				MetricLabelGroup:     gvk.Group,
			})
			if deletionTS, ok := c.terminatingObjects.Load(req); ok {
				c.observeHistogram(c.TerminationDuration, TerminationDuration, time.Since(deletionTS.(*metav1.Time).Time).Seconds(), map[string]string{
					MetricLabelGroup: gvk.Group,
				})
			}
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("getting object, %w", err)
	}

	currentConditions := o.StatusConditions()
	observedConditions := ConditionSet{}
	if v, ok := c.observedConditions.Load(req); ok {
		observedConditions = v.(ConditionSet)
	}
	c.observedConditions.Store(req, currentConditions)

	// Detect and record condition counts
	for _, condition := range o.GetConditions() {
		c.setGaugeMetric(c.ConditionCount, ConditionCount, 1, map[string]string{
			MetricLabelGroup:           gvk.Group,
			MetricLabelNamespace:       req.Namespace,
			MetricLabelName:            req.Name,
			MetricLabelConditionType:   condition.Type,
			MetricLabelConditionStatus: string(condition.Status),
			MetricLabelConditionReason: condition.Reason,
		})
		c.setGaugeMetric(c.ConditionCurrentStatusSeconds, ConditionCurrentStatusSeconds, time.Since(condition.LastTransitionTime.Time).Seconds(), map[string]string{
			MetricLabelGroup:           gvk.Group,
			MetricLabelNamespace:       req.Namespace,
			MetricLabelName:            req.Name,
			MetricLabelConditionType:   condition.Type,
			MetricLabelConditionStatus: string(condition.Status),
			MetricLabelConditionReason: condition.Reason,
		})
	}
	if o.GetDeletionTimestamp() != nil {
		c.setGaugeMetric(c.TerminationCurrentTimeSeconds, TerminationCurrentTimeSeconds, time.Since(o.GetDeletionTimestamp().Time).Seconds(), map[string]string{
			MetricLabelNamespace: req.Namespace,
			MetricLabelName:      req.Name,
			MetricLabelGroup:     gvk.Group,
		})
		c.terminatingObjects.Store(req, o.GetDeletionTimestamp())
	}
	for _, observedCondition := range observedConditions.List() {
		if currentCondition := currentConditions.Get(observedCondition.Type); currentCondition == nil || currentCondition.Status != observedCondition.Status {
			c.deleteGaugeMetric(c.ConditionCount, ConditionCount, map[string]string{
				MetricLabelGroup:           gvk.Group,
				MetricLabelNamespace:       req.Namespace,
				MetricLabelName:            req.Name,
				MetricLabelConditionType:   observedCondition.Type,
				MetricLabelConditionStatus: string(observedCondition.Status),
				MetricLabelConditionReason: observedCondition.Reason,
			})
			c.deleteGaugeMetric(c.ConditionCurrentStatusSeconds, ConditionCurrentStatusSeconds, map[string]string{
				MetricLabelGroup:           gvk.Group,
				MetricLabelNamespace:       req.Namespace,
				MetricLabelName:            req.Name,
				MetricLabelConditionType:   observedCondition.Type,
				MetricLabelConditionStatus: string(observedCondition.Status),
				MetricLabelConditionReason: observedCondition.Reason,
			})
		}
	}

	// Detect and record status transitions. This approach is best effort,
	// since we may batch multiple writes within a single reconcile loop.
	// It's exceedingly difficult to atomically track all changes to an
	// object, since the Kubernetes is evenutally consistent by design.
	// Despite this, we can catch the majority of transition by remembering
	// what we saw last, and reporting observed changes.
	//
	// We rejected the alternative of tracking these changes within the
	// condition library itself, since you cannot guarantee that a
	// transition made in memory was successfully persisted.
	//
	// Automatic monitoring systems must assume that these observations are
	// lossy, specifically for when a condition transition rapidly. However,
	// for the common case, we want to alert when a transition took a long
	// time, and our likelyhood of observing this is much higher.
	for _, condition := range currentConditions.List() {
		observedCondition := observedConditions.Get(condition.Type)
		if observedCondition.GetStatus() == condition.GetStatus() {
			continue
		}
		// A condition transitions if it either didn't exist before or it has changed
		c.incCounterMetric(c.ConditionTransitionsTotal, ConditionTransitionsTotal, map[string]string{
			MetricLabelGroup:           gvk.Group,
			MetricLabelConditionType:   condition.Type,
			MetricLabelConditionStatus: string(condition.Status),
			MetricLabelConditionReason: condition.Reason,
		})
		if observedCondition == nil {
			continue
		}
		duration := condition.LastTransitionTime.Time.Sub(observedCondition.LastTransitionTime.Time).Seconds()
		c.observeHistogram(c.ConditionDuration, ConditionDuration, duration, map[string]string{
			MetricLabelGroup:           gvk.Group,
			MetricLabelConditionType:   observedCondition.Type,
			MetricLabelConditionStatus: string(observedCondition.Status),
		})
		c.eventRecorder.Event(o, v1.EventTypeNormal, condition.Type, fmt.Sprintf("Status condition transitioned, Type: %s, Status: %s -> %s, Reason: %s%s",
			condition.Type,
			observedCondition.Status,
			condition.Status,
			condition.Reason,
			lo.Ternary(condition.Message != "", fmt.Sprintf(", Message: %s", condition.Message), ""),
		))
	}
	return reconcile.Result{RequeueAfter: time.Second * 10}, nil
}

func (c *Controller[T]) incCounterMetric(current pmetrics.CounterMetric, deprecated pmetrics.CounterMetric, labels map[string]string) {
	current.Inc(labels)
	labels[MetricLabelKind] = c.objectGVK.Kind
	deprecated.Inc(labels)
}

func (c *Controller[T]) setGaugeMetric(current pmetrics.GaugeMetric, deprecated pmetrics.GaugeMetric, value float64, labels map[string]string) {
	current.Set(value, labels)
	labels[MetricLabelKind] = c.objectGVK.Kind
	deprecated.Set(value, labels)
}

func (c *Controller[T]) deleteGaugeMetric(current pmetrics.GaugeMetric, deprecated pmetrics.GaugeMetric, labels map[string]string) {
	current.Delete(labels)
	labels[MetricLabelKind] = c.objectGVK.Kind
	deprecated.Delete(labels)
}

func (c *Controller[T]) deletePartialMatchGaugeMetric(current pmetrics.GaugeMetric, deprecated pmetrics.GaugeMetric, labels map[string]string) {
	current.DeletePartialMatch(labels)
	labels[MetricLabelKind] = c.objectGVK.Kind
	deprecated.DeletePartialMatch(labels)
}

func (c *Controller[T]) observeHistogram(current pmetrics.ObservationMetric, deprecated pmetrics.ObservationMetric, value float64, labels map[string]string) {
	current.Observe(value, labels)
	labels[MetricLabelKind] = c.objectGVK.Kind
	deprecated.Observe(value, labels)
}
