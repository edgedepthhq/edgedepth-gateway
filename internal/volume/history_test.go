package volume

import (
	"github.com/edgedepthhq/edgedepth-gateway/pkg/pb"
	"google.golang.org/protobuf/proto"
	"testing"
)

const base int64 = 1788739200000

func trade(ts int64, price, qty float64, buy bool) *pb.Trade {
	return &pb.Trade{TimestampMs: ts, Price: price, Qty: qty, IsBuy: buy}
}
func TestWarmupClosedBoundsAndUnits(t *testing.T) {
	h := New(base + 123)
	h.Add(trade(base+200, 100, 999, true))
	if h.Add(trade(base+Minute, 100, 2, true)) != nil {
		t.Fatal("join minute published")
	}
	h.Add(trade(base+Minute+1, 101, 3, false))
	u := h.Add(trade(base+2*Minute, 99, 1, true))
	if u == nil || u.TotalVolume != 5 || u.BuyVolume != 2 || u.SellVolume != 3 || u.TradeCount != 2 {
		t.Fatalf("bad totals: %v", u)
	}
	var lv pb.TickVolumeLevels
	if err := proto.Unmarshal(u.LevelsData, &lv); err != nil || len(lv.Levels) != 2 || lv.Levels[0].Price != 100 {
		t.Fatalf("inner contract: %v %v", lv.Levels, err)
	}
	if len(h.Range(base, base+2*Minute-1, base+3*Minute)) != 0 {
		t.Fatal("future close included")
	}
	if len(h.Range(base+Minute+1, base+3*Minute, base+3*Minute)) != 0 {
		t.Fatal("partial left minute included")
	}
	p := Profile(h.Range(base, base+3*Minute, base+3*Minute), 1)
	if p.TotalVolume != 5 || p.Poc != 101 || p.StartTime != base+Minute || p.EndTime != base+2*Minute {
		t.Fatalf("bad profile: %v", p)
	}
	if p.ValueAreaVolume != 5 || p.Val != 100 || p.Vah != 101 {
		t.Fatalf("bad value area: %v", p)
	}
}
func TestReconnectRetainsOlderMinutesAndDiscardsOpen(t *testing.T) {
	h := New(base)
	h.Add(trade(base+Minute, 100, 1, true))
	h.Add(trade(base+2*Minute, 100, 50, true))
	h.Reset(base + 2*Minute + 100)
	h.Add(trade(base+2*Minute+200, 100, 999, true))
	h.Add(trade(base+3*Minute, 100, 2, false))
	h.Add(trade(base+4*Minute, 100, 1, true))
	rows := h.Range(base, base+5*Minute, base+5*Minute)
	if len(rows) != 2 || rows[0].TotalVolume != 1 || rows[1].SellVolume != 2 {
		t.Fatalf("reset leaked volume: %v", rows)
	}
	if len(h.Range(base, base+100*Minute, base+100*Minute)) != 0 {
		t.Fatal("retention failed")
	}
}
func TestCellBudgetAndOverload(t *testing.T) {
	h := New(base)
	for i := 0; i < MaxLevels+1; i++ {
		h.Add(trade(base+Minute, intPrice(i), 1, true))
	}
	if h.cells != 0 || h.current != 0 {
		t.Fatalf("overload must discard entire minute: cells=%d", h.cells)
	}
	if h.Add(trade(base+2*Minute, 100, 1, true)) != nil {
		t.Fatal("overflow partial was published")
	}
}
func intPrice(i int) float64 { return float64(i + 1) }
func TestRegressingMinuteAndInvalidValues(t *testing.T) {
	h := New(base)
	h.Add(trade(base+Minute, 100, 1, true))
	h.Add(trade(base+2*Minute, 100, 2, true))
	h.Add(trade(base+Minute+1, 100, 9, true))
	if h.current != 0 {
		t.Fatal("late trade did not discard open minute")
	}
	if p := Profile(nil, 0); len(p.Levels) != 0 {
		t.Fatal("invalid grouping should be empty")
	}
}
