package signalr

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/go-kit/log"
)

type benchPipeConnection struct {
	*ConnectionBase
	net.Conn
}

type benchReceiver struct {
	got chan int
}

func (r *benchReceiver) OnUpdate(msg []json.RawMessage) {
	r.got <- len(msg)
}

func updatePayload(items int) string {
	var b strings.Builder
	b.WriteString(`{"type":1,"target":"OnUpdate","arguments":[[`)
	for i := 0; i < items; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"id":"01a1bb34-2f70-766d-a59b-55896e0bd5e%d","sportId":11,"markets":[`, i%10)
		for m := 0; m < 20; m++ {
			if m > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `{"id":"[2,5,1,0,[%d.5]]","marketType":2,"outcomes":[{"id":"[4,[]]","price":1.55,"status":1},{"id":"[5,[]]","price":2.23,"status":1}]}`, 150+m)
		}
		b.WriteString(`]}`)
	}
	b.WriteString("]]}\u001e")
	return b.String()
}

func startPipeClient(tb testing.TB, ctx context.Context, receiver interface{}, debug bool, logger log.Logger) chan<- []byte {
	tb.Helper()
	cli, srv := net.Pipe()
	tb.Cleanup(func() { _ = cli.Close(); _ = srv.Close() })

	frames := make(chan []byte)
	go func() {
		r := bufio.NewReader(srv)
		if _, err := r.ReadString('\u001e'); err != nil {
			return
		}
		if _, err := srv.Write([]byte("{}\u001e")); err != nil {
			return
		}
		for f := range frames {
			if _, err := srv.Write(f); err != nil {
				return
			}
		}
	}()

	client, err := NewClient(ctx,
		WithConnection(&benchPipeConnection{NewConnectionBase(ctx, "bench"), cli}),
		WithReceiver(receiver),
		Logger(logger, debug))
	if err != nil {
		tb.Fatal(err)
	}
	client.Start()
	return frames
}

func BenchmarkClientReceive(b *testing.B) {
	for _, items := range []int{1, 10} {
		b.Run(fmt.Sprintf("items=%d", items), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rec := &benchReceiver{got: make(chan int, 1)}
			frames := startPipeClient(b, ctx, rec, false, log.NewNopLogger())
			defer close(frames)

			payload := []byte(updatePayload(items))
			receive := func() {
				frames <- payload
				if n := <-rec.got; n != items {
					b.Fatalf("got %d items, want %d", n, items)
				}
			}
			receive()

			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				receive()
			}
			b.StopTimer()
		})
	}
}
