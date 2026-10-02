package chaindet

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

// 资源要求：5000 条规则、200 个进程、50 万条遥测，
// 每种行为类型涉及不超过 100 条规则，任意 60 秒内最多 10 万条遥测。
// 覆盖前两阶段大量重复、最后才出现第三阶段的情况。
// 全部摄入耗时 ≤ 20 秒，峰值内存 ≤ 512 MiB。
func TestPerfScale(t *testing.T) {
	const (
		numRules  = 5000
		numProcs  = 200
		numEvents = 500000
		numTypes  = 200 // 5000 规则 × 3 阶段 / 200 类型 = 每类型 75 条规则 ≤ 100
	)
	types := make([]string, numTypes)
	for i := range types {
		types[i] = fmt.Sprintf("T%d", i)
	}
	rules := make([]Rule, numRules)
	for i := range rules {
		rules[i] = Rule{
			ID:      fmt.Sprintf("R%d", i),
			Types:   [3]string{types[i%numTypes], types[(i+1)%numTypes], types[(i+2)%numTypes]},
			WaitMin: [2]int64{int64(i % 50), int64(i % 40)},
			WaitMax: [2]int64{int64(1000 + i%5000), int64(1000 + i%4000)},
			MaxSpan: 60000,
			Weight:  1 + i%10,
		}
	}
	eng := NewEngine()
	if err := eng.LoadRules(rules); err != nil {
		t.Fatalf("LoadRules: %v", err)
	}

	// 每个进程：前 2400 条为大量重复的前两阶段类型（T0/T1），
	// 最后 100 条才出现第三阶段类型 T2（规则 [T0,T1,T2] 共 25 条）。
	// 全局 50 万条按 ts = i*3/5 毫秒排列，任意 60 秒约 10 万条。
	events := make([]Event, 0, numEvents)
	perProc := numEvents / numProcs
	for i := 0; i < numEvents; i++ {
		proc := fmt.Sprintf("P%d", i%numProcs)
		n := i / numProcs // 该进程内的序号
		typ := "T2"
		if n < perProc-100 {
			if n%2 == 0 {
				typ = "T0"
			} else {
				typ = "T1"
			}
		}
		events = append(events, Event{
			ID:   fmt.Sprintf("E%d", i),
			Proc: proc,
			Obj:  "OBJ",
			Type: typ,
			Ts:   int64(i) * 3 / 5,
		})
	}

	start := time.Now()
	// 分 10 批摄入
	var alerts []Alert
	batch := numEvents / 10
	for lo := 0; lo < numEvents; lo += batch {
		hi := lo + batch
		if hi > numEvents {
			hi = numEvents
		}
		alerts = append(alerts, eng.Ingest(events[lo:hi])...)
	}
	elapsed := time.Since(start)

	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	t.Logf("摄入 %d 条事件耗时 %v, 告警 %d 条, HeapInuse=%.1f MiB",
		numEvents, elapsed, len(alerts), float64(mem.HeapInuse)/1024/1024)

	if elapsed > 20*time.Second {
		t.Fatalf("摄入耗时 %v 超过 20 秒", elapsed)
	}
	if mem.HeapInuse > 512*1024*1024 {
		t.Fatalf("峰值内存超过 512 MiB")
	}
	// 每个进程应命中规则 [T0,T1,T2]（25 条）
	score, hits := eng.Query("P0")
	if len(hits) != 25 {
		t.Fatalf("P0 应命中 25 条规则, got %d (score=%d)", len(hits), score)
	}
}
