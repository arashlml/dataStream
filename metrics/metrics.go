package metrics

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func StartMetricsServer(port string) {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Printf("Metrics server starting on port %s", port)
		if err := http.ListenAndServe(":"+port, nil); err != nil {
			log.Fatalf("Failed to start metrics server: %v", err)
		}
	}()
}

var namespace = "myapp"
var subsystem = "pipeline"
var TotalReadDocuments = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "read_documents_total",
	Help:      "Total number of documents read from the source.",
})
var TotalWrittenDocuments = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "written_documents_total",
	Help:      "Total number of documents successfully written to the destination.",
})
var TotalFailedDocuments = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "failed_documents_total",
	Help:      "Total number of documents that failed to be written.",
})
var ReadDuration = promauto.NewHistogram(prometheus.HistogramOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "read_duration_seconds",
	Help:      "The duration of how long it took to complete a read.",
	Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15),
})
var WriteDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "write_duration_seconds",
	Help:      "The duration of how long it took to complete a write.",
	Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15),
})
var ErrorCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "errors_total",
	Help:      "Total number of errors, labeled by service, and error_message.",
}, []string{"service_name", "error_message"})

var TotalReadOperations = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "read_operations_total",
	Help:      "Total number of read operations.",
})
var TotalWrittenOperations = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "written_operations_total",
	Help:      "Total number of write operations.",
})
