// Package volume retains observed, closed one-minute trade-volume buckets.
// There is no REST trade backfill. A join/reset discards the partial minute;
// the first later trade closes a bucket. Silence never proves completeness.
package volume

import (
	"github.com/edgedepthhq/edgedepth-gateway/pkg/pb"
	"google.golang.org/protobuf/proto"
	"math"
	"sort"
	"sync"
)

const Minute int64 = 60_000
const Retention int64 = 60 * Minute
const MaxLevels = 50_000 // across current + retained minutes, per active symbol

type History struct {
	mu         sync.Mutex
	acceptFrom int64
	current    int64
	levels     map[float64]*pb.TickVolumeLevel
	closed     []*pb.TickVolumeUpdate
	cells      int
}

func New(now int64) *History { h := &History{}; h.Reset(now); return h }

// Reset preserves older closed buckets but never joins volume across a gap.
func (h *History) Reset(now int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reset(now)
}
func (h *History) reset(now int64) {
	h.cells -= len(h.levels)
	h.levels = nil
	h.current = 0
	h.acceptFrom = (now/Minute + 1) * Minute
}

func (h *History) prune(now int64) {
	for len(h.closed) > 0 && h.closed[0].StartTime < now-Retention {
		h.dropOldest()
	}
}
func (h *History) dropOldest() {
	var levels pb.TickVolumeLevels
	_ = proto.Unmarshal(h.closed[0].LevelsData, &levels)
	h.cells -= len(levels.Levels)
	h.closed[0] = nil
	h.closed = h.closed[1:]
}

// Add returns a newly closed snapshot, if any. Regressing timestamps invalidate
// the open minute rather than publishing a plausible but incomplete snapshot.
func (h *History) Add(t *pb.Trade) *pb.TickVolumeUpdate {
	h.mu.Lock()
	defer h.mu.Unlock()
	if t.TimestampMs <= 0 || !finitePositive(t.Price) || !finitePositive(t.Qty) {
		return nil
	}
	minute := t.TimestampMs / Minute * Minute
	h.prune(minute)
	if t.TimestampMs < h.acceptFrom {
		return nil
	}
	if h.current > 0 && minute < h.current {
		h.reset(h.current)
		return nil
	}
	var out *pb.TickVolumeUpdate
	if h.current > 0 && minute > h.current && len(h.levels) > 0 {
		out = snapshot(h.current, h.levels)
		h.closed = append(h.closed, out)
		h.levels = nil
	}
	h.current = minute
	if h.levels == nil {
		h.levels = make(map[float64]*pb.TickVolumeLevel)
	}
	level := h.levels[t.Price]
	if level == nil {
		for h.cells >= MaxLevels && len(h.closed) > 0 {
			h.dropOldest()
		}
		if h.cells >= MaxLevels {
			h.reset(t.TimestampMs)
			return out
		}
		level = &pb.TickVolumeLevel{Price: t.Price}
		h.levels[t.Price] = level
		h.cells++
	}
	if t.IsBuy {
		level.BuyVolume += t.Qty
	} else {
		level.SellVolume += t.Qty
	}
	level.TotalVolume += t.Qty
	level.TradeCount++ // received messages, not underlying fills or taker orders
	return out
}
func finitePositive(v float64) bool { return v > 0 && !math.IsNaN(v) && !math.IsInf(v, 0) }

func snapshot(start int64, levels map[float64]*pb.TickVolumeLevel) *pb.TickVolumeUpdate {
	vals := make([]*pb.TickVolumeLevel, 0, len(levels))
	for _, level := range levels {
		vals = append(vals, level)
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i].Price < vals[j].Price })
	// The terminal's FootprintManager accepts raw inner protobuf explicitly.
	data, _ := proto.Marshal(&pb.TickVolumeLevels{Levels: vals})
	out := &pb.TickVolumeUpdate{TimestampMs: start, Timeframe: Minute, StartTime: start, EndTime: start + Minute, LevelsData: data}
	var maximum float64
	for _, v := range vals {
		out.BuyVolume += v.BuyVolume
		out.SellVolume += v.SellVolume
		out.TradeCount += v.TradeCount
		if v.TotalVolume > maximum {
			maximum = v.TotalVolume
			out.Poc = v.Price
		}
	}
	out.TotalVolume = out.BuyVolume + out.SellVolume
	out.LowPrice = vals[0].Price
	out.HighPrice = vals[len(vals)-1].Price
	return out
}

// Range returns immutable whole minutes contained in [start,end). Never stamp
// requested bounds onto narrower observed coverage. No empty-minute fabrication.
func (h *History) Range(start, end, now int64) []*pb.TickVolumeUpdate {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.prune(now)
	end = min(end, now)
	var out []*pb.TickVolumeUpdate
	for _, u := range h.closed {
		if u.StartTime >= start && u.EndTime <= end {
			out = append(out, u)
		}
	}
	return out
}

func Profile(minutes []*pb.TickVolumeUpdate, tick float64) *pb.VolumeProfileResponse {
	out := &pb.VolumeProfileResponse{}
	if !finitePositive(tick) {
		return out
	}
	rows := make(map[float64]*pb.VolumeProfileLevel)
	for _, u := range minutes {
		var levels pb.TickVolumeLevels
		if proto.Unmarshal(u.LevelsData, &levels) != nil {
			continue
		}
		if out.StartTime == 0 || u.StartTime < out.StartTime {
			out.StartTime = u.StartTime
		}
		out.EndTime = max(out.EndTime, u.EndTime)
		for _, v := range levels.Levels {
			price := math.Floor(v.Price/tick+1e-9) * tick
			if math.IsInf(price, 0) || math.IsNaN(price) {
				return &pb.VolumeProfileResponse{}
			}
			row := rows[price]
			if row == nil {
				row = &pb.VolumeProfileLevel{Price: price}
				rows[price] = row
			}
			row.BuyVolume += v.BuyVolume
			row.SellVolume += v.SellVolume
			row.TotalVolume += v.TotalVolume
			row.TradeCount += v.TradeCount
		}
	}
	for _, row := range rows {
		out.Levels = append(out.Levels, row)
		out.TotalVolume += row.TotalVolume
	}
	sort.Slice(out.Levels, func(i, j int) bool { return out.Levels[i].Price < out.Levels[j].Price })
	if len(out.Levels) == 0 {
		return out
	}
	poc := 0
	for i, v := range out.Levels {
		if v.TotalVolume > out.Levels[poc].TotalVolume {
			poc = i
		}
	}
	lo, hi := poc, poc
	va := out.Levels[poc].TotalVolume
	for va < out.TotalVolume*.7 && (lo > 0 || hi < len(out.Levels)-1) {
		above, below := -1.0, -1.0
		if hi < len(out.Levels)-1 {
			above = out.Levels[hi+1].TotalVolume
		}
		if lo > 0 {
			below = out.Levels[lo-1].TotalVolume
		}
		if above >= below {
			hi++
			va += out.Levels[hi].TotalVolume
		} else {
			lo--
			va += out.Levels[lo].TotalVolume
		}
	}
	out.Poc = out.Levels[poc].Price
	out.Vah = out.Levels[hi].Price
	out.Val = out.Levels[lo].Price
	out.PocIndex = int32(poc)
	out.VahIndex = int32(hi)
	out.ValIndex = int32(lo)
	out.ValueAreaVolume = va
	for i, v := range out.Levels {
		v.IsPoc = i == poc
		v.InValueArea = i >= lo && i <= hi
		v.VolumePercentage = v.TotalVolume / out.TotalVolume * 100
	}
	return out
}
