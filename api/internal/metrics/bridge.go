package metrics

import "github.com/prometheus/client_golang/prometheus"

// IngestStats is a snapshot of the writer's counters.
type IngestStats struct {
	Batched     int64
	Written     int64
	Positions   int64
	Duplicates  int64
	FlushErrors int64
}

// JobStats is a snapshot of one job kind's queue counters.
type JobStats struct {
	Completed int64
	Failed    int64
}

// WSStats is a snapshot of the WebSocket hub.
type WSStats struct {
	Subscribers int
	Dispatched  int64
	Dropped     int64
}

// Sources are the snapshot closures the bridge reads on each scrape. Any field
// may be nil; a nil source contributes no series. Keeping these as closures lets
// the ingest, notify, queue, ws, and cdc packages stay Prometheus-free.
type Sources struct {
	Ingest        func() IngestStats
	Notifications func() map[string]int64    // channel -> received count
	Jobs          func() map[string]JobStats // job kind -> counters
	WS            func() WSStats
	CDCLagBytes   func() (int64, bool) // bytes, ok (false when CDC is disabled)
}

// bridge is a Prometheus collector that reads the component snapshots at scrape
// time and emits them as const metrics. Because the underlying values are
// already monotonic counters (or point-in-time gauges), no state is held here.
type bridge struct {
	src Sources

	ingestBatched  *prometheus.Desc
	ingestWritten  *prometheus.Desc
	positions      *prometheus.Desc
	duplicates     *prometheus.Desc
	flushErrors    *prometheus.Desc
	notifyReceived *prometheus.Desc
	jobsCompleted  *prometheus.Desc
	jobsFailed     *prometheus.Desc
	wsSubscribers  *prometheus.Desc
	wsDispatched   *prometheus.Desc
	wsDropped      *prometheus.Desc
	cdcLag         *prometheus.Desc
}

func newBridge(s Sources) *bridge {
	return &bridge{
		src:            s,
		ingestBatched:  prometheus.NewDesc("ais_messages_batched_total", "AIS messages accepted into a write batch.", nil, nil),
		ingestWritten:  prometheus.NewDesc("ais_messages_written_total", "Raw AIS message rows written.", nil, nil),
		positions:      prometheus.NewDesc("ais_positions_written_total", "Position rows appended to the hypertable.", nil, nil),
		duplicates:     prometheus.NewDesc("ais_messages_duplicate_total", "Messages flagged as cross-source duplicates.", nil, nil),
		flushErrors:    prometheus.NewDesc("ais_writer_flush_errors_total", "Failed writer flushes.", nil, nil),
		notifyReceived: prometheus.NewDesc("ais_notifications_received_total", "LISTEN/NOTIFY notifications received.", []string{"channel"}, nil),
		jobsCompleted:  prometheus.NewDesc("ais_jobs_completed_total", "Queue jobs completed.", []string{"kind"}, nil),
		jobsFailed:     prometheus.NewDesc("ais_jobs_failed_total", "Queue jobs failed.", []string{"kind"}, nil),
		wsSubscribers:  prometheus.NewDesc("ais_ws_subscribers", "Connected WebSocket subscribers.", nil, nil),
		wsDispatched:   prometheus.NewDesc("ais_ws_frames_dispatched_total", "Position frames delivered to subscribers.", nil, nil),
		wsDropped:      prometheus.NewDesc("ais_ws_frames_dropped_total", "Position frames dropped on subscriber backpressure.", nil, nil),
		cdcLag:         prometheus.NewDesc("ais_cdc_lag_bytes", "Replication-slot lag in WAL bytes.", nil, nil),
	}
}

func (b *bridge) Describe(ch chan<- *prometheus.Desc) {
	ch <- b.ingestBatched
	ch <- b.ingestWritten
	ch <- b.positions
	ch <- b.duplicates
	ch <- b.flushErrors
	ch <- b.notifyReceived
	ch <- b.jobsCompleted
	ch <- b.jobsFailed
	ch <- b.wsSubscribers
	ch <- b.wsDispatched
	ch <- b.wsDropped
	ch <- b.cdcLag
}

func (b *bridge) Collect(ch chan<- prometheus.Metric) {
	if b.src.Ingest != nil {
		s := b.src.Ingest()
		counter(ch, b.ingestBatched, float64(s.Batched))
		counter(ch, b.ingestWritten, float64(s.Written))
		counter(ch, b.positions, float64(s.Positions))
		counter(ch, b.duplicates, float64(s.Duplicates))
		counter(ch, b.flushErrors, float64(s.FlushErrors))
	}
	if b.src.Notifications != nil {
		for channel, n := range b.src.Notifications() {
			ch <- prometheus.MustNewConstMetric(b.notifyReceived, prometheus.CounterValue, float64(n), channel)
		}
	}
	if b.src.Jobs != nil {
		for kind, s := range b.src.Jobs() {
			ch <- prometheus.MustNewConstMetric(b.jobsCompleted, prometheus.CounterValue, float64(s.Completed), kind)
			ch <- prometheus.MustNewConstMetric(b.jobsFailed, prometheus.CounterValue, float64(s.Failed), kind)
		}
	}
	if b.src.WS != nil {
		s := b.src.WS()
		gauge(ch, b.wsSubscribers, float64(s.Subscribers))
		counter(ch, b.wsDispatched, float64(s.Dispatched))
		counter(ch, b.wsDropped, float64(s.Dropped))
	}
	if b.src.CDCLagBytes != nil {
		if n, ok := b.src.CDCLagBytes(); ok {
			gauge(ch, b.cdcLag, float64(n))
		}
	}
}

func counter(ch chan<- prometheus.Metric, d *prometheus.Desc, v float64) {
	ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v)
}

func gauge(ch chan<- prometheus.Metric, d *prometheus.Desc, v float64) {
	ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v)
}
