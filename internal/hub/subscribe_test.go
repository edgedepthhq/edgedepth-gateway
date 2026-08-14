package hub_test

// What a client receives at the moment it subscribes, as opposed to what it
// receives once something happens. The distinction matters because stats only
// flush on a real change: the panel is populated from the 30s REST poll, not
// from the tape, so "wait for the next event" can mean waiting in front of an
// empty strip.

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/edgedepthhq/edgedepth-gateway/pkg/pb"
)

// slowTf is an hourly series. Timeframe choice is what makes these tests mean
// anything: the stub venue never trades and its MarkState is constant, so the
// only thing that can dirty a stat is a bucket roll. On the 1s series that
// happens every second and a late subscriber would be served by the next
// natural flush whether or not priming exists, which is a test that passes
// either way. On an hourly series the next roll is an hour out, so a frame
// arriving inside the read timeout can only have come from priming.
const slowTf = int64(3600)

// A client joining a feed that is already warm must be primed with the current
// stat rather than left waiting for the next change.
func TestStatsSubscriberIsPrimedFromAWarmFeed(t *testing.T) {
	_, srv := newHub(t)

	warm := dial(t, srv, 10*time.Second)
	subscribe(t, warm, stubSymbol, pb.Stream_STREAM_STATS, 1)
	// One 1s frame proves the flush loop has ticked, and a tick calls Current
	// on every series, so the hourly series' opening stat is spent too.
	readFrame(t, warm, pb.Stream_STREAM_STATS)

	late := dial(t, srv, 3*time.Second)
	subscribe(t, late, stubSymbol, pb.Stream_STREAM_STATS, slowTf)

	env := readFrame(t, late, pb.Stream_STREAM_STATS)
	var b pb.Stats
	if err := proto.Unmarshal(env.Data, &b); err != nil || len(b.Values) == 0 {
		t.Fatalf("primed frame is not a populated pb.Stats: %v %+v", err, &b)
	}
	if got := b.Values[0].MarkPrice; got != 64000 {
		t.Fatalf("primed stat carried mark %v, want the venue's 64000", got)
	}
}

// The terminal subscribes STREAM_STATS with seconds, with milliseconds and
// with 0, depending on which panel is asking, and dispatches client side on an
// exact (stream, timeframe) match. Priming has to resolve all three to a
// series and echo the subscriber's own convention back, or the panels that use
// an alias get nothing.
func TestStatsPrimingResolvesEveryTimeframeAlias(t *testing.T) {
	_, srv := newHub(t)

	warm := dial(t, srv, 10*time.Second)
	subscribe(t, warm, stubSymbol, pb.Stream_STREAM_STATS, 1)
	readFrame(t, warm, pb.Stream_STREAM_STATS)

	// All slow enough that no bucket rolls inside the read timeout, so every
	// frame below is a primed one. 0 is covered by the test above's sibling
	// path (it resolves to the 1s series) and is deliberately not asserted
	// here, because a 1s roll would satisfy it without priming.
	for _, tf := range []int64{300, 300_000, slowTf, slowTf * 1000} {
		late := dial(t, srv, 3*time.Second)
		subscribe(t, late, stubSymbol, pb.Stream_STREAM_STATS, tf)

		env := readFrame(t, late, pb.Stream_STREAM_STATS)
		if env.Timeframe != tf {
			t.Errorf("tf=%d: envelope echoed %d; the client matches on the value "+
				"it subscribed with and would drop this", tf, env.Timeframe)
		}
		var b pb.Stats
		if err := proto.Unmarshal(env.Data, &b); err != nil || len(b.Values) == 0 {
			t.Errorf("tf=%d: primed frame is not a populated pb.Stats: %v", tf, err)
			continue
		}
		if got := b.Values[0].MarkPrice; got != 64000 {
			t.Errorf("tf=%d: primed stat carried mark %v, want 64000", tf, got)
		}
	}
}
