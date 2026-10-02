package chaindet

import "testing"

// 场景一:较早事件符合最小等待时间而最近事件不符合,
// 且有其他进程/对象交错;链须在首个合格末事件处命中,证据不得串进程或对象。
func TestInterleavedProcessesAndObjects(t *testing.T) {
	rule := Rule{
		ID:        "R1",
		Behaviors: [3]string{"A", "B", "C"},
		Wait1:     Interval{Min: 5, Max: 10},
		Wait2:     Interval{Min: 5, Max: 10},
		MaxSpan:   20,
		Weight:    5,
	}
	eng := NewEngine([]Rule{rule})
	events := []Event{
		{ID: "e1", ProcessID: "p1", ObjectID: "o1", Behavior: "A", Timestamp: 0},
		// 其他进程、其他对象的同类型事件,用于干扰。
		{ID: "x1", ProcessID: "p2", ObjectID: "o1", Behavior: "A", Timestamp: 1},
		{ID: "x2", ProcessID: "p1", ObjectID: "o2", Behavior: "A", Timestamp: 2},
		{ID: "x3", ProcessID: "p2", ObjectID: "o1", Behavior: "B", Timestamp: 6},
		{ID: "x4", ProcessID: "p1", ObjectID: "o2", Behavior: "B", Timestamp: 8},
		// p2 的 C 与 x3 差 2,不满足等待区间,p2 不会命中。
		{ID: "x5", ProcessID: "p2", ObjectID: "o1", Behavior: "C", Timestamp: 8},
		// 最近的 A,与即将到来的 B 时间差 1,不满足最小等待 5。
		{ID: "e2", ProcessID: "p1", ObjectID: "o1", Behavior: "A", Timestamp: 9},
		// B 与 e1 差 10(符合),与 e2 差 1(不符合);必须选较早的 e1。
		{ID: "e3", ProcessID: "p1", ObjectID: "o1", Behavior: "B", Timestamp: 10},
		// 末事件但与 e3 差 2,不满足等待区间,不能命中。
		{ID: "e4", ProcessID: "p1", ObjectID: "o1", Behavior: "C", Timestamp: 12},
		// 首个合格末事件:与 e3 差 6,跨度 16 <= 20。
		{ID: "e5", ProcessID: "p1", ObjectID: "o1", Behavior: "C", Timestamp: 16},
	}
	if alerts := eng.Ingest(events); len(alerts) != 0 {
		t.Fatalf("不应产生告警,得到 %v", alerts)
	}
	got := eng.Score("p1")
	if got.TotalScore != 5 || len(got.Hits) != 1 {
		t.Fatalf("p1 应命中一次得 5 分,得到 %+v", got)
	}
	wantIDs := [3]string{"e1", "e3", "e5"}
	if got.Hits[0].Evidence.EventIDs != wantIDs {
		t.Fatalf("证据应为 %v,得到 %v", wantIDs, got.Hits[0].Evidence.EventIDs)
	}
	validateEvidence(t, rule, got.Hits[0].Evidence, events, "p1")
	if s := eng.Score("p2"); s.TotalScore != 0 || len(s.Hits) != 0 {
		t.Fatalf("p2 不应有任何命中,得到 %+v", s)
	}
}

// 场景二(确定性部分):同时间戳、等待区间端点、事件不得重复占用两个阶段。
func TestBoundaryAndSameTimestamp(t *testing.T) {
	// 三个阶段同类型、零等待、零跨度:必须由三个不同事件组成。
	rule := Rule{
		ID:        "R2",
		Behaviors: [3]string{"A", "A", "A"},
		Wait1:     Interval{Min: 0, Max: 0},
		Wait2:     Interval{Min: 0, Max: 0},
		MaxSpan:   0,
		Weight:    10,
	}
	eng := NewEngine([]Rule{rule})
	// 仅两个同时间戳事件,不足以组成链。
	eng.Ingest([]Event{
		{ID: "a1", ProcessID: "p", ObjectID: "o", Behavior: "A", Timestamp: 100},
		{ID: "a2", ProcessID: "p", ObjectID: "o", Behavior: "A", Timestamp: 100},
	})
	if got := eng.Score("p"); got.TotalScore != 0 {
		t.Fatalf("两个事件不得命中(事件不可复用),得到 %+v", got)
	}
	// 第三个同时间戳事件到达,链完成。
	eng.Ingest([]Event{
		{ID: "a3", ProcessID: "p", ObjectID: "o", Behavior: "A", Timestamp: 100},
	})
	got := eng.Score("p")
	if got.TotalScore != 10 || len(got.Hits) != 1 {
		t.Fatalf("三个同时间戳事件应命中,得到 %+v", got)
	}
	if got.Hits[0].Evidence.EventIDs != [3]string{"a1", "a2", "a3"} {
		t.Fatalf("证据应为 a1,a2,a3,得到 %v", got.Hits[0].Evidence.EventIDs)
	}

	// 等待区间端点(含边界)必须命中。
	r2 := Rule{
		ID:        "R3",
		Behaviors: [3]string{"X", "Y", "Z"},
		Wait1:     Interval{Min: 3, Max: 7},
		Wait2:     Interval{Min: 2, Max: 9},
		MaxSpan:   16,
		Weight:    8,
	}
	eng2 := NewEngine([]Rule{r2})
	eng2.Ingest([]Event{
		{ID: "x", ProcessID: "p", ObjectID: "o", Behavior: "X", Timestamp: 10},
		{ID: "y", ProcessID: "p", ObjectID: "o", Behavior: "Y", Timestamp: 17}, // 差 7 = Max
		{ID: "z", ProcessID: "p", ObjectID: "o", Behavior: "Z", Timestamp: 19}, // 差 2 = Min,跨度 9 <= 16
	})
	if got := eng2.Score("p"); got.TotalScore != 8 {
		t.Fatalf("端点边界应命中,得到 %+v", got)
	}
	// 跨度恰好等于 MaxSpan 也必须命中。
	r3 := Rule{
		ID:        "R4",
		Behaviors: [3]string{"X", "Y", "Z"},
		Wait1:     Interval{Min: 0, Max: 60000},
		Wait2:     Interval{Min: 0, Max: 60000},
		MaxSpan:   30,
		Weight:    8,
	}
	eng3 := NewEngine([]Rule{r3})
	eng3.Ingest([]Event{
		{ID: "x", ProcessID: "p", ObjectID: "o", Behavior: "X", Timestamp: 0},
		{ID: "y", ProcessID: "p", ObjectID: "o", Behavior: "Y", Timestamp: 10},
		{ID: "z", ProcessID: "p", ObjectID: "o", Behavior: "Z", Timestamp: 30}, // 跨度 30 = MaxSpan
	})
	if got := eng3.Score("p"); got.TotalScore != 8 {
		t.Fatalf("跨度等于 MaxSpan 应命中,得到 %+v", got)
	}
}

// 场景三:同一末事件命中多条规则使总分 20 -> 30,只产生一条 30 分 Critical 告警;
// 后续重复行为不再加分;改变批次划分结果一致。
func TestMultiRuleSingleAlertAndBatchInvariance(t *testing.T) {
	mkRules := func() []Rule {
		return []Rule{
			{ID: "r1", Behaviors: [3]string{"A", "B", "C"}, Wait1: Interval{0, 10}, Wait2: Interval{0, 10}, MaxSpan: 60, Weight: 10},
			{ID: "r2", Behaviors: [3]string{"A", "B", "D"}, Wait1: Interval{0, 10}, Wait2: Interval{0, 10}, MaxSpan: 60, Weight: 10},
			{ID: "r3", Behaviors: [3]string{"A", "B", "E"}, Wait1: Interval{0, 10}, Wait2: Interval{0, 10}, MaxSpan: 60, Weight: 4},
			{ID: "r4", Behaviors: [3]string{"A", "B", "E"}, Wait1: Interval{0, 20}, Wait2: Interval{0, 20}, MaxSpan: 60, Weight: 6},
		}
	}
	events := []Event{
		{ID: "e1", ProcessID: "p", ObjectID: "o", Behavior: "A", Timestamp: 0},
		{ID: "e2", ProcessID: "p", ObjectID: "o", Behavior: "B", Timestamp: 5},
		{ID: "e3", ProcessID: "p", ObjectID: "o", Behavior: "C", Timestamp: 9},  // 命中 r1 -> 10
		{ID: "e4", ProcessID: "p", ObjectID: "o", Behavior: "D", Timestamp: 12}, // 命中 r2 -> 20
		{ID: "e5", ProcessID: "p", ObjectID: "o", Behavior: "E", Timestamp: 15}, // 命中 r3+r4 -> 30,告警
		// 后续重复行为:不再加分、不再告警。
		{ID: "e6", ProcessID: "p", ObjectID: "o", Behavior: "A", Timestamp: 20},
		{ID: "e7", ProcessID: "p", ObjectID: "o", Behavior: "B", Timestamp: 25},
		{ID: "e8", ProcessID: "p", ObjectID: "o", Behavior: "C", Timestamp: 28},
		{ID: "e9", ProcessID: "p", ObjectID: "o", Behavior: "D", Timestamp: 30},
		{ID: "e10", ProcessID: "p", ObjectID: "o", Behavior: "E", Timestamp: 33},
	}

	run := func(batches [][]Event) ([]Alert, ProcessScore) {
		eng := NewEngine(mkRules())
		var alerts []Alert
		for _, b := range batches {
			alerts = append(alerts, eng.Ingest(b)...)
		}
		return alerts, eng.Score("p")
	}

	split := func(cuts ...int) [][]Event {
		var out [][]Event
		prev := 0
		for _, c := range cuts {
			out = append(out, events[prev:c])
			prev = c
		}
		return append(out, events[prev:])
	}

	for name, batches := range map[string][][]Event{
		"单批":   {events},
		"逐条":   split(1, 2, 3, 4, 5, 6, 7, 8, 9),
		"不规则":  split(3, 4, 7),
		"告警边界": split(4, 5),
	} {
		alerts, score := run(batches)
		if len(alerts) != 1 {
			t.Fatalf("%s: 应只有一条告警,得到 %v", name, alerts)
		}
		if alerts[0].Level != "Critical" || alerts[0].Score != 30 || alerts[0].ProcessID != "p" {
			t.Fatalf("%s: 告警应为 Critical/30 分,得到 %+v", name, alerts[0])
		}
		if score.TotalScore != 30 {
			t.Fatalf("%s: 总分应为 30(重复行为不加分),得到 %d", name, score.TotalScore)
		}
		if len(score.Hits) != 4 {
			t.Fatalf("%s: 应命中 4 条规则,得到 %d", name, len(score.Hits))
		}
		// 命中时点:末事件分别是 e3,e4,e5,e5。
		wantLast := map[string]string{"r1": "e3", "r2": "e4", "r3": "e5", "r4": "e5"}
		for _, h := range score.Hits {
			if h.Evidence.EventIDs[2] != wantLast[h.RuleID] {
				t.Fatalf("%s: 规则 %v 命中时点应为 %v,得到 %v",
					name, h.RuleID, wantLast[h.RuleID], h.Evidence.EventIDs[2])
			}
		}
	}
}
