package metrics

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	TotalReadDocuments     prometheus.Counter
	TotalWrittenDocuments  prometheus.Counter
	TotalFailedDocuments   prometheus.Counter
	ReadDuration           prometheus.Histogram
	WriteDuration          prometheus.Histogram
	TotalReadOperations    prometheus.Counter
	TotalWrittenOperations prometheus.Counter
	ErrorCounter           *prometheus.CounterVec
}

func StartMetricsServer(port string) {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Printf("Metrics server starting on port %s", port)
		if err := http.ListenAndServe(":"+port, nil); err != nil {
			log.Fatalf("Failed to start metrics server: %v", err)
		}
	}()
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
			Help:      "Total number of errors, labeled by service, and error_message.",
		}, []string{"service_name", "error_message"}),

		TotalReadOperations: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "read_operations_total",
			Help:      "Total number of read operations.",
		}),
		TotalWrittenOperations: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "written_operations_total",
			Help:      "Total number of write operations.",
		}),
	}
	prometheus.MustRegister(m.WriteDuration)
	return &m
}
