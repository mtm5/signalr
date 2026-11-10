package signalr

import (
	"bytes"
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-kit/log"
)

func TestDebugLoggerOff(t *testing.T) {
	var buf bytes.Buffer
	info, dbg := buildInfoDebugLogger(log.NewLogfmtLogger(&buf), false)
	if debugEnabled(dbg) {
		t.Fatal("debug logging is off")
	}
	if debugEnabled(withDebugPrefix(dbg, "ts", log.DefaultTimestampUTC, "class", "Client")) {
		t.Fatal("a prefix must not turn debug logging on")
	}
	_ = dbg.Log("msg", "dropped")
	_ = info.Log("msg", "kept")
	if out := buf.String(); strings.Contains(out, "dropped") || !strings.Contains(out, "kept") {
		t.Fatalf("got %q", out)
	}
}

func TestDebugLoggerOn(t *testing.T) {
	var buf bytes.Buffer
	_, dbg := buildInfoDebugLogger(log.NewLogfmtLogger(&buf), true)
	dbg = withDebugPrefix(dbg, "class", "Client")
	if !debugEnabled(dbg) {
		t.Fatal("debug logging is on")
	}
	_ = dbg.Log("msg", "written")
	if out := buf.String(); !strings.Contains(out, "written") || !strings.Contains(out, "class=Client") {
		t.Fatalf("got %q", out)
	}
}

var formattedCount atomic.Int32

type formatted struct {
	N int `json:"n"`
}

func (formatted) String() string {
	formattedCount.Add(1)
	return "formatted"
}

type formattingReceiver struct {
	got chan struct{}
}

func (r *formattingReceiver) OnValue(formatted) {
	r.got <- struct{}{}
}

func TestReceivedArgumentsAreFormattedOnlyForDebug(t *testing.T) {
	for _, debug := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		formattedCount.Store(0)
		rec := &formattingReceiver{got: make(chan struct{}, 1)}
		frames := startPipeClient(t, ctx, rec, debug, log.NewNopLogger())
		frames <- []byte(`{"type":1,"target":"OnValue","arguments":[{"n":1}]}` + "\u001e")
		<-rec.got
		close(frames)
		cancel()

		if got := formattedCount.Load(); debug == (got == 0) {
			t.Errorf("debug %v: argument formatted %d times", debug, got)
		}
	}
}
