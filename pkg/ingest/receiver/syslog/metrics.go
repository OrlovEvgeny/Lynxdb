package syslog

import "github.com/prometheus/client_golang/prometheus"

// Metrics records syslog receiver counters in the server Prometheus registry.
type Metrics struct {
	received          *prometheus.CounterVec
	dropped           *prometheus.CounterVec
	activeConnections prometheus.Gauge
	parseErrors       *prometheus.CounterVec
}

func NewMetrics(reg *prometheus.Registry) *Metrics {
	m := &Metrics{
		received: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lynxdb_syslog_messages_received_total",
			Help: "Total syslog messages received.",
		}, []string{"transport", "dialect"}),
		dropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lynxdb_syslog_messages_dropped_total",
			Help: "Total syslog messages dropped.",
		}, []string{"transport", "reason"}),
		activeConnections: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "lynxdb_syslog_active_connections",
			Help: "Current active syslog TCP connections.",
			ConstLabels: prometheus.Labels{
				"transport": "tcp",
			},
		}),
		parseErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lynxdb_syslog_parse_errors_total",
			Help: "Total syslog parse errors.",
		}, []string{"dialect"}),
	}
	reg.MustRegister(m.received, m.dropped, m.activeConnections, m.parseErrors)
	return m
}

func (m *Metrics) IncReceived(transport, dialect string) {
	if m != nil {
		m.received.WithLabelValues(transport, dialect).Inc()
	}
}

func (m *Metrics) IncDropped(transport, reason string) {
	if m != nil {
		m.dropped.WithLabelValues(transport, reason).Inc()
	}
}

func (m *Metrics) IncParseError(dialect string) {
	if m != nil {
		m.parseErrors.WithLabelValues(dialect).Inc()
	}
}

func (m *Metrics) SetActiveConnections(n int64) {
	if m != nil {
		m.activeConnections.Set(float64(n))
	}
}
