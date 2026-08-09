// Package wire encodes the EdgeDepth terminal wire format.
//
// The contract, verified against the terminal client source:
//
//   - Every market-data message is one pb.WSPayload, marshalled to protobuf,
//     sent as a single WebSocket BINARY frame. No length prefix, no stream-id
//     header byte. One frame, one payload.
//
//   - Compression is optional and detected by content. The client sniffs the
//     zstd magic (28 B5 2F FD) and passes anything else through untouched, so
//     this gateway emits plain protobuf. See message_parser.cpp decompress_zstd.
//     The one exception is TickVolumeUpdate.levels_data, which is a zstd blob
//     nested INSIDE the payload and is always compressed.
//
//   - The control plane is JSON TEXT frames keyed on "method" (not "type").
package wire

import (
	"google.golang.org/protobuf/proto"

	"github.com/edgedepthhq/edgedepth-gateway/pkg/pb"
)

// Encode wraps an inner market-data message in the WSPayload envelope and
// returns the bytes for one binary WebSocket frame.
//
// timeframe is 0 for streams that are not timeframed (trades, orderbook,
// liquidations, ticker24h). eventTimeMs is the market event timestamp; the
// client uses it for its replay buffer, and 0 is acceptable but unhelpful.
func Encode(pair *pb.Pair, stream pb.Stream, timeframe int64, eventTimeMs int64, inner proto.Message) ([]byte, error) {
	data, err := proto.Marshal(inner)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&pb.WSPayload{
		Pair:        pair,
		Stream:      stream,
		Timeframe:   timeframe,
		Data:        data,
		EventTimeMs: eventTimeMs,
	})
}

// Key identifies a subscription: a pair plus a stream, plus the timeframe for
// the streams where it is meaningful. It matches the client's StreamKey.
type Key struct {
	Exchange  string
	Symbol    string
	Stream    pb.Stream
	Timeframe int64
}

// Pair rebuilds the protobuf Pair for this key.
func (k Key) Pair() *pb.Pair {
	return &pb.Pair{Exchange: k.Exchange, Symbol: k.Symbol}
}

// Timeframed reports whether the stream carries a meaningful timeframe. For
// every other stream the client sends timeframe 0 and we must echo 0 back,
// otherwise the subscription keys do not match on the client side.
func Timeframed(s pb.Stream) bool {
	switch s {
	case pb.Stream_STREAM_CANDLES, pb.Stream_STREAM_STATS,
		pb.Stream_STREAM_HISTORICAL_CANDLES, pb.Stream_STREAM_TICK_VOLUME:
		return true
	default:
		return false
	}
}
