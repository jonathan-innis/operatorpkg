package publisher

import (
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/middleware/private/metrics"
	"github.com/aws/aws-sdk-go-v2/aws/middleware/private/metrics/middleware"
	smithy "github.com/aws/smithy-go/middleware"
	"github.com/awslabs/operatorpkg/metrics/awssdk/common"
	"github.com/prometheus/client_golang/prometheus"
)

func WithPrometheusMetrics(cfg aws.Config, r *prometheus.Registry) aws.Config {
	p := NewPrometheusPublisher(r)
	cfg.APIOptions = append(cfg.APIOptions, func(s *smithy.Stack) error {
		return middleware.WithMetricMiddlewares(p, http.DefaultClient)(s)
	})
	return cfg
}

// PrometheusPublisher is a MetricPublisher implementation that publishes metrics to the Prometheus registry.
type PrometheusPublisher struct {
	registry prometheus.Registerer
}

// NewPrometheusPublisher creates a new PrometheusPublisher with the specified namespace and serializer.
func NewPrometheusPublisher(r prometheus.Registerer) *PrometheusPublisher {
	common.MustRegisterMetrics(r)
	return &PrometheusPublisher{
		registry: r,
	}
}

// PostRequestMetrics publishes request metrics to the prometheus registry.
func (p *PrometheusPublisher) PostRequestMetrics(data *metrics.MetricData) error {
	common.TotalRequests.With(common.RequestLabels(data.ServiceID, data.OperationName, data.StatusCode)).Inc()
	common.RequestLatency.With(common.RequestLabels(data.ServiceID, data.OperationName, data.StatusCode)).Observe(float64(data.APICallDuration.Milliseconds()))
	common.RetryCount.With(common.RequestLabels(data.ServiceID, data.OperationName, data.StatusCode)).Observe(float64(data.RetryCount))

	for _, attempt := range data.Attempts {
		common.TotalRequestAttempts.With(common.RequestLabels(data.ServiceID, data.OperationName, attempt.StatusCode)).Inc()
		common.RequestAttemptLatency.With(common.RequestLabels(data.ServiceID, data.OperationName, data.StatusCode)).Observe(float64(attempt.ServiceCallDuration))
	}
	return nil
}

// PostStreamMetrics publishes the stream metrics to the prometheus registry.
func (p *PrometheusPublisher) PostStreamMetrics(data *metrics.MetricData) error {
	common.TotalRequests.With(common.RequestLabels(data.ServiceID, data.OperationName, data.StatusCode)).Inc()
	return nil
}
