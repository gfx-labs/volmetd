package collector

import "github.com/prometheus/client_golang/prometheus"

// Metric defines a single metric to collect from a data source
type Metric[T any] struct {
	Desc  *prometheus.Desc
	Type  prometheus.ValueType
	Value func(T) float64
}

// Counter creates a counter metric
func Counter[T any](name, help string, labels []string, value func(T) float64) Metric[T] {
	return Metric[T]{
		Desc:  prometheus.NewDesc("volmetd_"+name, help, labels, nil),
		Type:  prometheus.CounterValue,
		Value: value,
	}
}

// Gauge creates a gauge metric
func Gauge[T any](name, help string, labels []string, value func(T) float64) Metric[T] {
	return Metric[T]{
		Desc:  prometheus.NewDesc("volmetd_"+name, help, labels, nil),
		Type:  prometheus.GaugeValue,
		Value: value,
	}
}

// MetricSet is a collection of metrics for a data source
type MetricSet[T any] []Metric[T]

// Collect emits all metrics for the given data and labels
func (ms MetricSet[T]) Collect(data T, labels []string, ch chan<- prometheus.Metric) {
	for _, m := range ms {
		ch <- prometheus.MustNewConstMetric(m.Desc, m.Type, m.Value(data), labels...)
	}
}
