package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	TotalReadDocuments    prometheus.Counter
	TotalWrittenDocuments prometheus.Counter
	TotalFailedDocuments  prometheus.Counter
	ReadDuration          prometheus.Histogram
	WriteDuration         prometheus.Histogram
	ErrorCounter          *prometheus.CounterVec
}

func New(namespace, subsystem string) *Metrics {
	m := Metrics{
		TotalReadDocuments: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "read_documents_total",
			Help:      "Total number of documents read from the source.",
		}),
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
		ErrorCounter: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "errors_total",
			Help:      "Total number of errors, labeled by service, last_id, and error_message.",
		}, []string{"service_name", "last_id", "error_message"}),
	}
	prometheus.MustRegister(m.WriteDuration)
	return &m
}