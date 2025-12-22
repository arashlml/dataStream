package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	TotalDocuments        prometheus.Gauge
	TotalReadDocuments    *prometheus.CounterVec
	TotalWrittenDocuments prometheus.Counter
	TotalFailedDocuments  prometheus.Counter
	TotalAttempts         prometheus.Counter
	ReadDuration          prometheus.Histogram
	WriteDuration         prometheus.Histogram
}

func New(namespace, subsystem string) *Metrics {
	m := Metrics{
		TotalDocuments: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "total_documents",
			Help:      "Total number of documents to be processed.",
		}),
		TotalReadDocuments: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "read_documents_total",
			Help:      "Total number of documents read from the source.",
		}, []string{"uri", "path"}),
		TotalWrittenDocuments: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "written_documents_total",
			Help:      "Total number of documents successfully written to the destination.",
		}),
		TotalFailedDocuments: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "failed_documents_total",
			Help:      "Total number of documents that failed to be written.",
		}),
		TotalAttempts: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "attempts_total",
			Help:      "Total number of processing attempts.",
		}),
		ReadDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "read_duration_seconds",
			Help:      "The duration of how long it took to complete a read.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15),
		}),
		WriteDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "write_duration_seconds",
			Help:      "The duration of how long it took to complete a write.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15),
		}),
	}
	prometheus.MustRegister(m.WriteDuration)
	return &m
}
