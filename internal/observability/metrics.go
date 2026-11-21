// Package observability provides structured logging and a small, dependency-free
// Prometheus-compatible metrics registry for the API and worker processes.
package observability

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Metric names exported by the API and worker. Names ending in _total are
// counters; _seconds are histograms in seconds; gauges are instantaneous.
const (
	MetricHTTPRequestsTotal    = "api_http_requests_total"
	MetricHTTPRequestDuration  = "api_http_request_duration_seconds"
	MetricRateLimitedTotal     = "api_rate_limited_total"
	MetricJobsSubmittedTotal   = "jobs_submitted_total"
	MetricJobsCompletedTotal   = "jobs_completed_total"
	MetricJobsFailedTotal      = "jobs_failed_total"
	MetricJobsRetriedTotal     = "jobs_retried_total"
	MetricJobsDeadTotal        = "jobs_dead_lettered_total"
	MetricJobsReplayedTotal    = "jobs_replayed_total"
	MetricJobsCancelledTotal   = "jobs_cancelled_total"
	MetricJobAttemptsTotal     = "job_attempts_total"
	MetricJobWaitDuration      = "job_wait_duration_seconds"
	MetricJobExecutionDuration = "job_execution_duration_seconds"
	MetricWorkerActive         = "worker_active_count"
	MetricHeartbeatFailures    = "worker_heartbeat_failures_total"
	MetricExpiredLeases        = "worker_expired_leases_total"
	MetricQueueDepth           = "jobs_queue_depth"
	MetricQueueOldestDue       = "jobs_oldest_due_seconds"
	MetricDBPoolInUse          = "db_pool_in_use"
	MetricDBPoolIdle           = "db_pool_idle"
	MetricDBPoolWaitCount      = "db_pool_wait_count"
	MetricDBPoolMaxConns       = "db_pool_max_conns"
)

// Label is a single Prometheus label. Use L to build labels at call sites.
type Label struct {
	Name  string
	Value string
}

// L constructs a metric label.
func L(name, value string) Label {
	return Label{Name: name, Value: value}
}

type metricKind uint8

const (
	kindCounter metricKind = iota
	kindGauge
	kindHistogram
)

type metricDef struct {
	kind   metricKind
	help   string
	bounds []float64
}

var (
	durationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}
	waitBuckets     = []float64{0.1, 0.5, 1, 5, 15, 60, 300, 900, 3600}

	definitions = map[string]metricDef{
		MetricHTTPRequestsTotal:    {kind: kindCounter, help: "HTTP requests handled by the API."},
		MetricHTTPRequestDuration:  {kind: kindHistogram, help: "HTTP request duration in seconds.", bounds: durationBuckets},
		MetricRateLimitedTotal:     {kind: kindCounter, help: "Requests rejected by a tenant rate limit."},
		MetricJobsSubmittedTotal:   {kind: kindCounter, help: "Job submissions, by result (created, replayed, rejected, error)."},
		MetricJobsCompletedTotal:   {kind: kindCounter, help: "Jobs completed successfully."},
		MetricJobsFailedTotal:      {kind: kindCounter, help: "Handler failures durably persisted."},
		MetricJobsRetriedTotal:     {kind: kindCounter, help: "Failures that scheduled another attempt."},
		MetricJobsDeadTotal:        {kind: kindCounter, help: "Jobs moved to the dead state."},
		MetricJobsReplayedTotal:    {kind: kindCounter, help: "Dead jobs replayed by an operator."},
		MetricJobsCancelledTotal:   {kind: kindCounter, help: "Jobs cancelled by a client or operator."},
		MetricJobAttemptsTotal:     {kind: kindCounter, help: "Job execution attempts started by a worker."},
		MetricJobWaitDuration:      {kind: kindHistogram, help: "Time between job creation and execution in seconds.", bounds: waitBuckets},
		MetricJobExecutionDuration: {kind: kindHistogram, help: "Handler execution duration in seconds.", bounds: durationBuckets},
		MetricWorkerActive:         {kind: kindGauge, help: "Handlers currently executing in this process."},
		MetricHeartbeatFailures:    {kind: kindCounter, help: "Lease heartbeat errors."},
		MetricExpiredLeases:        {kind: kindCounter, help: "Expired leases resolved by recovery."},
		MetricQueueDepth:           {kind: kindGauge, help: "Jobs ready to claim across the whole queue."},
		MetricQueueOldestDue:       {kind: kindGauge, help: "Age in seconds of the oldest due job."},
		MetricDBPoolInUse:          {kind: kindGauge, help: "Database connections currently acquired."},
		MetricDBPoolIdle:           {kind: kindGauge, help: "Idle database connections."},
		MetricDBPoolWaitCount:      {kind: kindGauge, help: "Cumulative waiters for a database connection."},
		MetricDBPoolMaxConns:       {kind: kindGauge, help: "Configured maximum database connections."},
	}
)

type histogram struct {
	count   uint64
	sum     float64
	buckets []uint64
}

func (h *histogram) observe(value float64, bounds []float64) {
	h.count++
	h.sum += value
	for i, bound := range bounds {
		if value <= bound {
			h.buckets[i]++
		}
	}
}

type sample struct {
	key    string
	labels []Label
	number float64
	hist   *histogram
}

type metric struct {
	name string
	def  metricDef

	mu      sync.Mutex
	values  map[string]*sample
	ordered []string
}

func (mt *metric) sample(labels []Label, withHistogram bool) *sample {
	key, sorted := labelKey(labels)
	mt.mu.Lock()
	defer mt.mu.Unlock()
	if value, ok := mt.values[key]; ok {
		return value
	}
	value := &sample{key: key, labels: sorted}
	if withHistogram {
		value.hist = &histogram{buckets: make([]uint64, len(mt.def.bounds))}
	}
	mt.values[key] = value
	mt.ordered = append(mt.ordered, key)
	return value
}

// Metrics is a concurrency-safe registry of counters, gauges and histograms.
// Its zero value is not usable; construct one with NewMetrics. All recording
// methods are safe to call on a nil *Metrics, which makes instrumentation
// optional at call sites.
type Metrics struct {
	mu      sync.RWMutex
	metrics map[string]*metric
	names   []string
}

// NewMetrics returns a registry preloaded with the project's metric names.
func NewMetrics() *Metrics {
	registry := &Metrics{metrics: make(map[string]*metric)}
	for name, def := range definitions {
		registry.metrics[name] = &metric{
			name:   name,
			def:    def,
			values: make(map[string]*sample),
		}
		registry.names = append(registry.names, name)
	}
	sort.Strings(registry.names)
	return registry
}

func (m *Metrics) getOrCreate(name string, kind metricKind) *metric {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	mt := m.metrics[name]
	m.mu.RUnlock()
	if mt != nil {
		return mt
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if mt = m.metrics[name]; mt == nil {
		def := definitions[name]
		if def.help == "" {
			def = metricDef{kind: kind, help: name, bounds: durationBuckets}
		}
		mt = &metric{name: name, def: def, values: make(map[string]*sample)}
		m.metrics[name] = mt
		m.names = append(m.names, name)
		sort.Strings(m.names)
	}
	return mt
}

// Inc adds one to a counter or gauge.
func (m *Metrics) Inc(name string, labels ...Label) {
	m.Add(name, 1, labels...)
}

// Add adds value to a counter or gauge.
func (m *Metrics) Add(name string, value float64, labels ...Label) {
	mt := m.getOrCreate(name, kindCounter)
	if mt == nil {
		return
	}
	s := mt.sample(labels, false)
	mt.mu.Lock()
	s.number += value
	mt.mu.Unlock()
}

// Set replaces the value of a gauge.
func (m *Metrics) Set(name string, value float64, labels ...Label) {
	mt := m.getOrCreate(name, kindGauge)
	if mt == nil {
		return
	}
	s := mt.sample(labels, false)
	mt.mu.Lock()
	s.number = value
	mt.mu.Unlock()
}

// Observe records one value in a histogram.
func (m *Metrics) Observe(name string, value float64, labels ...Label) {
	mt := m.getOrCreate(name, kindHistogram)
	if mt == nil {
		return
	}
	s := mt.sample(labels, true)
	mt.mu.Lock()
	s.hist.observe(value, mt.def.bounds)
	mt.mu.Unlock()
}

func labelKey(labels []Label) (string, []Label) {
	if len(labels) == 0 {
		return "", nil
	}
	sorted := append([]Label(nil), labels...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	var key strings.Builder
	count := 0
	for _, label := range sorted {
		if label.Name == "" {
			continue
		}
		if count > 0 {
			key.WriteByte(',')
		}
		key.WriteString(label.Name)
		key.WriteString("=\"")
		key.WriteString(escapeLabel(label.Value))
		key.WriteByte('"')
		count++
	}
	return key.String(), sorted
}

func escapeLabel(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return strings.ReplaceAll(value, "\n", `\n`)
}

func kindName(kind metricKind) string {
	switch kind {
	case kindGauge:
		return "gauge"
	case kindHistogram:
		return "histogram"
	default:
		return "counter"
	}
}

func joinLabels(base, extra string) string {
	switch {
	case base == "":
		return extra
	case extra == "":
		return base
	default:
		return base + "," + extra
	}
}

func seriesLine(name, labels, value string) string {
	if labels == "" {
		return name + " " + value + "\n"
	}
	return name + "{" + labels + "} " + value + "\n"
}

// render builds the Prometheus text exposition format under the registry locks,
// so network writes never block metric updates.
func (m *Metrics) render() string {
	if m == nil {
		return ""
	}
	var out strings.Builder
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, name := range m.names {
		mt := m.metrics[name]
		fmt.Fprintf(&out, "# HELP %s %s\n# TYPE %s %s\n", name, mt.def.help, name, kindName(mt.def.kind))

		mt.mu.Lock()
		keys := append([]string(nil), mt.ordered...)
		sort.Strings(keys)
		for _, key := range keys {
			s := mt.values[key]
			switch mt.def.kind {
			case kindHistogram:
				for i, bound := range mt.def.bounds {
					le := strconv.FormatFloat(bound, 'g', -1, 64)
					fmt.Fprintf(&out, "%s_bucket{%s} %d\n", name, joinLabels(s.key, `le="`+le+`"`), s.hist.buckets[i])
				}
				fmt.Fprintf(&out, "%s_bucket{%s} %d\n", name, joinLabels(s.key, `le="+Inf"`), s.hist.count)
				out.WriteString(seriesLine(name+"_sum", s.key, strconv.FormatFloat(s.hist.sum, 'g', -1, 64)))
				out.WriteString(seriesLine(name+"_count", s.key, strconv.FormatUint(s.hist.count, 10)))
			default:
				out.WriteString(seriesLine(name, s.key, strconv.FormatFloat(s.number, 'g', -1, 64)))
			}
		}
		mt.mu.Unlock()
	}
	return out.String()
}

// Handler exposes the registry in Prometheus text format. When token is
// non-empty, requests must present Authorization: Bearer <token>.
func (m *Metrics) Handler(token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if token != "" {
			parts := strings.Fields(r.Header.Get("Authorization"))
			if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") ||
				subtle.ConstantTimeCompare([]byte(parts[1]), []byte(token)) != 1 {
				w.Header().Set("WWW-Authenticate", "Bearer")
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodGet {
			_, _ = io.WriteString(w, m.render())
		}
	})
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

// ServeMetrics runs a dedicated metrics listener until ctx is cancelled, then
// shuts it down gracefully. Exposing metrics beyond loopback requires a bearer
// token so the endpoint is never accidentally public.
func ServeMetrics(ctx context.Context, addr, token string, m *Metrics) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid metrics address %q: %w", addr, err)
	}
	if !isLoopbackHost(host) && strings.TrimSpace(token) == "" {
		return errors.New("METRICS_TOKEN is required when metrics are exposed beyond loopback")
	}

	server := &http.Server{
		Addr:              addr,
		Handler:           m.Handler(token),
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.ListenAndServe() }()

	select {
	case err := <-serverErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve metrics: %w", err)
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown metrics: %w", err)
		}
		if err := <-serverErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve metrics: %w", err)
		}
		return nil
	}
}

// PoolStats is a snapshot of database pool health.
type PoolStats struct {
	InUse     int64
	Idle      int64
	WaitCount int64
	MaxConns  int64
}

// PollPoolStats publishes pool gauges immediately and then on each tick until
// ctx is cancelled. stats must be safe to call from this goroutine.
func PollPoolStats(ctx context.Context, interval time.Duration, stats func() PoolStats, m *Metrics) {
	if stats == nil || m == nil {
		return
	}
	if interval <= 0 {
		interval = 15 * time.Second
	}
	publish := func() {
		s := stats()
		m.Set(MetricDBPoolInUse, float64(s.InUse))
		m.Set(MetricDBPoolIdle, float64(s.Idle))
		m.Set(MetricDBPoolWaitCount, float64(s.WaitCount))
		m.Set(MetricDBPoolMaxConns, float64(s.MaxConns))
	}
	publish()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			publish()
		}
	}
}
