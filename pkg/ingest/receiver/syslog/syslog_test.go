package syslog

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ingest/pipeline"
)

type memSink struct {
	mu     sync.Mutex
	events []*event.Event
}

func (s *memSink) Write(events []*event.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, events...)
	return nil
}

func (s *memSink) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}

func (s *memSink) first() *event.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.events) == 0 {
		return nil
	}
	return s.events[0]
}

func TestUDPReceiverLoopback(t *testing.T) {
	cfg := config.DefaultConfig().Syslog
	cfg.UDP = "127.0.0.1:0"
	cfg.TCP = ""
	sink := &memSink{}
	r, err := New(cfg, sink, pipeline.New(), nil, testLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = r.Start(ctx)
	}()
	r.WaitReady()
	if err := r.ReadyError(); err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	conn, err := net.Dial("udp", r.UDPAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := fmt.Fprint(conn, "<13>Feb  5 17:32:18 host app: hello"); err != nil {
		t.Fatal(err)
	}
	waitForEvents(t, sink, 1)
	if got := sink.first().SourceType; got != "syslog:rfc3164" {
		t.Fatalf("sourcetype = %q", got)
	}
}

func TestTCPReceiverLoopback(t *testing.T) {
	cfg := config.DefaultConfig().Syslog
	cfg.UDP = ""
	cfg.TCP = "127.0.0.1:0"
	sink := &memSink{}
	r, err := New(cfg, sink, pipeline.New(), nil, testLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = r.Start(ctx)
	}()
	r.WaitReady()
	if err := r.ReadyError(); err != nil {
		t.Fatal(err)
	}
	defer r.Stop()

	conn, err := net.Dial("tcp", r.TCPAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	msg := "<34>1 2003-10-11T22:14:15Z host app 1 ID47 - hello"
	if _, err := fmt.Fprintf(conn, "%d %s", len(msg), msg); err != nil {
		t.Fatal(err)
	}
	waitForEvents(t, sink, 1)
	if got := sink.first().Host; got != "host" {
		t.Fatalf("host = %q", got)
	}
}

func waitForEvents(t *testing.T, sink *memSink, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if sink.len() >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("events = %d, want %d", sink.len(), want)
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}
