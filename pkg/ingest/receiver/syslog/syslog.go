package syslog

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ingest/pipeline"
	"github.com/lynxbase/lynxdb/pkg/ingest/receiver"
)

var errFrameTooLarge = errors.New("syslog frame too large")

type runtimeConfig struct {
	config.SyslogConfig
	Location *time.Location
}

type Receiver struct {
	cfg       atomic.Value // *runtimeConfig
	sink      receiver.EventSink
	pipeline  *pipeline.Pipeline
	tlsConfig *tls.Config
	logger    *slog.Logger
	metrics   *Metrics

	udpConn   net.PacketConn
	tcpLn     net.Listener
	udpAddr   atomic.Value
	tcpAddr   atomic.Value
	ready     chan struct{}
	readyOnce sync.Once
	startErr  atomic.Value
	stop      chan struct{}
	once      sync.Once
	wg        sync.WaitGroup

	activeConn atomic.Int64
}

func New(
	cfg config.SyslogConfig,
	sink receiver.EventSink,
	pipe *pipeline.Pipeline,
	tlsCfg *tls.Config,
	logger *slog.Logger,
	metrics *Metrics,
) (*Receiver, error) {
	rt, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	r := &Receiver{
		sink:      sink,
		pipeline:  pipe,
		tlsConfig: tlsCfg,
		logger:    logger,
		metrics:   metrics,
		ready:     make(chan struct{}),
		stop:      make(chan struct{}),
	}
	r.cfg.Store(rt)
	return r, nil
}

func (r *Receiver) Start(ctx context.Context) error {
	cfg := r.currentConfig()

	if cfg.UDP != "" {
		var lc net.ListenConfig
		conn, err := lc.ListenPacket(ctx, "udp", cfg.UDP)
		if err != nil {
			r.markReady(err)
			return fmt.Errorf("syslog udp listen: %w", err)
		}
		r.udpConn = conn
		r.udpAddr.Store(conn.LocalAddr().String())
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			if err := r.serveUDP(conn); err != nil && !isClosedNetworkError(err) {
				r.logger.Error("syslog UDP receiver stopped with error", "error", err)
			}
		}()
	}

	if cfg.TCP != "" {
		var lc net.ListenConfig
		ln, err := lc.Listen(ctx, "tcp", cfg.TCP)
		if err != nil {
			if r.udpConn != nil {
				_ = r.udpConn.Close()
			}
			r.markReady(err)
			return fmt.Errorf("syslog tcp listen: %w", err)
		}
		if cfg.TLS {
			if r.tlsConfig == nil {
				_ = ln.Close()
				if r.udpConn != nil {
					_ = r.udpConn.Close()
				}
				r.markReady(fmt.Errorf("syslog tcp tls enabled but server TLS is not configured"))
				return fmt.Errorf("syslog tcp tls enabled but server TLS is not configured")
			}
			ln = tls.NewListener(ln, r.tlsConfig)
		}
		r.tcpLn = ln
		r.tcpAddr.Store(ln.Addr().String())
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			if err := r.serveTCP(ln); err != nil && !isClosedNetworkError(err) {
				r.logger.Error("syslog TCP receiver stopped with error", "error", err)
			}
		}()
	}

	go func() {
		select {
		case <-ctx.Done():
			r.Stop()
		case <-r.stop:
		}
	}()

	r.markReady(nil)
	r.logger.Info("syslog receiver started", "udp", r.UDPAddr(), "tcp", r.TCPAddr(), "tls", cfg.TLS)
	r.wg.Wait()
	return nil
}

func (r *Receiver) Stop() {
	r.once.Do(func() {
		close(r.stop)
		if r.udpConn != nil {
			_ = r.udpConn.Close()
		}
		if r.tcpLn != nil {
			_ = r.tcpLn.Close()
		}
	})
}

func (r *Receiver) WaitReady() {
	<-r.ready
}

func (r *Receiver) ReadyError() error {
	if v := r.startErr.Load(); v != nil {
		return v.(error)
	}
	return nil
}

func (r *Receiver) markReady(err error) {
	if err != nil {
		r.startErr.Store(err)
	}
	r.readyOnce.Do(func() { close(r.ready) })
}

func (r *Receiver) UDPAddr() string {
	if v := r.udpAddr.Load(); v != nil {
		return v.(string)
	}
	return r.currentConfig().UDP
}

func (r *Receiver) TCPAddr() string {
	if v := r.tcpAddr.Load(); v != nil {
		return v.(string)
	}
	return r.currentConfig().TCP
}

func (r *Receiver) ReloadConfig(cfg config.SyslogConfig) error {
	rt, err := normalizeConfig(cfg)
	if err != nil {
		return err
	}
	r.cfg.Store(rt)
	return nil
}

func (r *Receiver) currentConfig() *runtimeConfig {
	return r.cfg.Load().(*runtimeConfig)
}

func (r *Receiver) processBatch(batch []*event.Event) error {
	if len(batch) == 0 {
		return nil
	}
	parseErrors := make([]bool, len(batch))
	for i, e := range batch {
		parseErrors[i] = e.ParseError
	}
	if r.pipeline != nil {
		processed, err := r.pipeline.Process(batch)
		if err != nil {
			return fmt.Errorf("pipeline: %w", err)
		}
		batch = processed
	}
	for i, e := range batch {
		if stringsHasSyslogPrefix(e.SourceType) {
			e.ParseError = parseErrors[i]
		}
	}
	if err := r.sink.Write(batch); err != nil {
		return fmt.Errorf("sink: %w", err)
	}
	return nil
}

func normalizeConfig(cfg config.SyslogConfig) (*runtimeConfig, error) {
	defaults := config.DefaultConfig().Syslog
	if cfg.Parser == "" {
		cfg.Parser = defaults.Parser
	}
	if cfg.Framing == "" {
		cfg.Framing = defaults.Framing
	}
	if cfg.Trailer == "" {
		cfg.Trailer = defaults.Trailer
	}
	if cfg.DefaultTimezone == "" {
		cfg.DefaultTimezone = defaults.DefaultTimezone
	}
	if cfg.Index == "" {
		cfg.Index = defaults.Index
	}
	if cfg.SourceType == "" {
		cfg.SourceType = defaults.SourceType
	}
	if cfg.MaxMessageBytes == 0 {
		cfg.MaxMessageBytes = defaults.MaxMessageBytes
	}
	if cfg.TCPIdleTimeout == 0 {
		cfg.TCPIdleTimeout = defaults.TCPIdleTimeout
	}
	if cfg.TCPMaxConns == 0 {
		cfg.TCPMaxConns = defaults.TCPMaxConns
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = defaults.BatchSize
	}
	if cfg.BatchTimeout == 0 {
		cfg.BatchTimeout = defaults.BatchTimeout
	}
	loc, err := locationForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &runtimeConfig{SyslogConfig: cfg, Location: loc}, nil
}

func stringsHasSyslogPrefix(s string) bool {
	return len(s) >= 6 && s[:6] == "syslog"
}

func isClosedNetworkError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, net.ErrClosed)
}
