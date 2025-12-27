Here are the proposed changes to add a new error metric that can be categorized by type.

### 1. Modify `metrics/metrics.go`

First, we need to add the new `ErrorsTotal` counter to your `Metrics` struct.

**File**: `metrics/metrics.go`

```go
// REPLACE THIS STRUCT DEFINITION
type Metrics struct {
	TotalReadDocuments    prometheus.Counter
	TotalWrittenDocuments prometheus.Counter
	TotalFailedDocuments  prometheus.Counter
	ReadDuration          prometheus.Histogram
	WriteDuration         prometheus.Histogram
}
```

**With this new version:**
```go
// NEW STRUCT DEFINITION
type Metrics struct {
	TotalReadDocuments    prometheus.Counter
	TotalWrittenDocuments prometheus.Counter
	TotalFailedDocuments  prometheus.Counter
	ReadDuration          prometheus.Histogram
	WriteDuration         prometheus.Histogram
	ErrorsTotal           *prometheus.CounterVec
}
```

---

Next, we need to initialize this new counter in the `New` function.

**File**: `metrics/metrics.go`

In the `New` function, find this block:
```go
// FIND THIS BLOCK
		WriteDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "write_duration_seconds",
			Help:      "The duration of how long it took to complete a write.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15),
		}),
	}
```

**And append the `ErrorsTotal` initialization to it, like so:**
```go
// APPEND THE FOLLOWING
		WriteDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "write_duration_seconds",
			Help:      "The duration of how long it took to complete a write.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15),
		}),
		ErrorsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "errors_total",
			Help:      "Total number of errors, labeled by type.",
		}, []string{"type"}),
	}
```
---
### 2. Use the New Metric in Your Service

Now that the metric is defined, you can use it in your services to count errors. Here is an example of how to use it in `service/reader_service/reader_service.go`.

**File**: `service/reader_service/reader_service.go`

In the `New` function, you can count errors that happen when loading the last ID.

```go
// FIND THIS BLOCK
	lastID, err := r.store.LoadLastID()
	if err != nil {
		r.logger.Error("service.reader.service.new.loadLastID.error",
			"error", err.Error())
	}
```

**And add the metric increment like this:**
```go
// ADD THE METRIC
	lastID, err := r.store.LoadLastID()
	if err != nil {
		r.logger.Error("service.reader.service.new.loadLastID.error",
			"error", err.Error())
		r.metric.ErrorsTotal.WithLabel("load_last_id").Inc() // Add this line
	}
```

---

Similarly, in the `Read` function, you can count errors from the iterator.

**File**: `service/reader_service/reader_service.go`

```go
// FIND THIS BLOCK
	batch, err := r.iterator.Next(ctx, r.lastID)
	if err != nil {
		r.logger.Error("read.service.next.error",
			"error", err,
			"lastID", r.lastID)
		return nil, err
	}
```

**And add the metric increment like this:**
```go
// ADD THE METRIC
	batch, err := r.iterator.Next(ctx, r.lastID)
	if err != nil {
		r.logger.Error("read.service.next.error",
			"error", err,
			"lastID", r.lastID)
		r.metric.ErrorsTotal.WithLabel("iterator_next").Inc() // Add this line
		return nil, err
	}
```

You can now apply these changes. Let me know if you would like me to proceed with applying them to the files directly.