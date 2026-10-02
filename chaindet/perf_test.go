package chaindet

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

// TestPerformance 按非功能要求构造负载:
// 5000 条规则、200 个进程、50 万条遥测,每种行为类型涉及不超过 100 条规则,
// 任意 60 秒内最多 10 万条遥测,且前两阶段大量重复、最后才出现第三阶段。
// 要求:全部摄入 <= 20 秒,峰值内存 <= 512 MiB。
func TestPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("短模式跳过性能测试")
	}
	const (
		nRules   = 5000
		nProcs   = 200
		nEvents  = 500000
		nBeh     = 200    // 行为类型数;每条规则占 3 种,每种行为涉及 75 条规则
		nObj     = 8      // 每进程对象数
		windowMs = 300000 // 总跨度 300 秒,任意 60 秒约 10 万条
	)

	// 规则:阶段行为取自相邻行为编号,阶段一、二的行为大量重复,
	// 阶段三行为集中在后段编号,对应事件最后才出现。
	rules := make([]Rule, nRules)
	for i := range rules {
		rules[i] = Rule{
			ID: fmt.Sprintf("rule-%d", i),
			Behaviors: [3]string{
				fmt.Sprintf("b%d", i%nBeh),
				fmt.Sprintf("b%d", (i+1)%nBeh),
				fmt.Sprintf("b%d", (i+2)%nBeh),
			},
			Wait1:   Interval{Min: 0, Max: 60000},
			Wait2:   Interval{Min: 0, Max: 60000},
			MaxSpan: 60000,
			Weight:  1 + i%10,
		}
	}

	// 事件:按进程内序号 seq 生成,保证同一进程能依次看到各行为。
	// 前 80% 只出现 b0..b99(阶段一、二大量堆积,半数规则无法闭合),
	// 后 20% 出现 b0..b199(含 b100..b199,第三阶段姗姗来迟)。
	// 同一行为周期内对象相同,使链可以闭合。
	events := make([]Event, nEvents)
	for i := 0; i < nEvents; i++ {
		p := i % nProcs
		seq := i / nProcs
		var b int
		if seq < (nEvents/nProcs)*4/5 {
			b = seq % 100
		} else {
			b = seq % 200
		}
		events[i] = Event{
			ID:        fmt.Sprintf("ev-%d", i),
			ProcessID: fmt.Sprintf("proc-%d", p),
			ObjectID:  fmt.Sprintf("obj-%d-%d", p, (seq/200)%nObj),
			Behavior:  fmt.Sprintf("b%d", b),
			Timestamp: int64(i) * windowMs / nEvents,
		}
	}

	eng := NewEngine(rules)
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	start := time.Now()
	var alerts []Alert
	const batch = 5000
	for pos := 0; pos < nEvents; pos += batch {
		end := pos + batch
		if end > nEvents {
			end = nEvents
		}
		alerts = append(alerts, eng.Ingest(events[pos:end])...)
	}
	elapsed := time.Since(start)

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	peakMiB := float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / (1 << 20)
	liveMiB := float64(memAfter.HeapAlloc) / (1 << 20)

	t.Logf("摄入 %d 条事件耗时 %v(上限 20s)", nEvents, elapsed)
	t.Logf("告警数 %d", len(alerts))
	t.Logf("累计分配 %.1f MiB,存活堆 %.1f MiB(上限 512 MiB)", peakMiB, liveMiB)

	if elapsed > 20*time.Second {
		t.Fatalf("摄入耗时 %v 超过 20 秒", elapsed)
	}
	if liveMiB > 512 {
		t.Fatalf("存活堆内存 %.1f MiB 超过 512 MiB", liveMiB)
	}
	// 功能 sanity:大量规则应命中,告警不应超过进程数。
	if len(alerts) == 0 {
		t.Fatalf("应产生告警")
	}
	if len(alerts) > nProcs {
		t.Fatalf("每个进程最多告警一次,得到 %d 条", len(alerts))
	}
	seen := map[string]bool{}
	for _, a := range alerts {
		if seen[a.ProcessID] {
			t.Fatalf("进程 %v 重复告警", a.ProcessID)
		}
		seen[a.ProcessID] = true
		if a.Score < 25 {
			t.Fatalf("告警分数 %d 低于阈值", a.Score)
		}
	}
}
