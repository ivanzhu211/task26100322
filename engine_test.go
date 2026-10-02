package chaindet

import "testing"

// 场景一：较早事件符合最小等待时间而最近事件不符合，并有其他进程
// 或对象交错出现：有效行为链仍须在首个合格末事件处命中，
// 证据不能串用其他进程或对象。
func TestInterleavedProcessesAndObjects(t *testing.T) {
	eng := NewEngine()
	rule := Rule{
		ID:      "R1",
		Types:   [3]string{"A", "B", "C"},
		WaitMin: [2]int64{10, 10},
		WaitMax: [2]int64{100, 100},
		MaxSpan: 1000,
		Weight:  5,
	}
	if err := eng.LoadRules([]Rule{rule}); err != nil {
		t.Fatalf("LoadRules: %v", err)
	}
	events := []Event{
		{ID: "e1", Proc: "P1", Obj: "O1", Type: "A", Ts: 1000},
		{ID: "e2", Proc: "P1", Obj: "O1", Type: "B", Ts: 1010}, // 距 e1 为 10，满足下界
		// 其他进程、其他对象的完美时序干扰项，不得被串用
		{ID: "x1", Proc: "P2", Obj: "O1", Type: "A", Ts: 1012},
		{ID: "x2", Proc: "P2", Obj: "O1", Type: "B", Ts: 1014},
		{ID: "x3", Proc: "P1", Obj: "O2", Type: "A", Ts: 1016},
		{ID: "x4", Proc: "P1", Obj: "O2", Type: "B", Ts: 1018},
		{ID: "e3", Proc: "P1", Obj: "O1", Type: "C", Ts: 1015}, // 距 e2 仅 5，不满足下界
		{ID: "x5", Proc: "P2", Obj: "O1", Type: "C", Ts: 1020}, // P2 自身不成链
		{ID: "e4", Proc: "P1", Obj: "O1", Type: "C", Ts: 1025}, // 距 e2 为 15，首个合格末事件
	}
	alerts := eng.Ingest(events)
	if len(alerts) != 0 {
		t.Fatalf("不应产生告警, got %v", alerts)
	}
	score, hits := eng.Query("P1")
	if score != 5 {
		t.Fatalf("P1 得分应为 5, got %d", score)
	}
	if len(hits) != 1 || hits[0].RuleID != "R1" {
		t.Fatalf("P1 应命中 R1, got %+v", hits)
	}
	want := [3]string{"e1", "e2", "e4"}
	if hits[0].Evidence.EventIDs != want {
		t.Fatalf("证据应为 %v, got %v", want, hits[0].Evidence.EventIDs)
	}
	if score2, hits2 := eng.Query("P2"); score2 != 0 || len(hits2) != 0 {
		t.Fatalf("P2 不应命中, got score=%d hits=%+v", score2, hits2)
	}
	// 后续重复行为不再加分
	eng.Ingest([]Event{
		{ID: "e5", Proc: "P1", Obj: "O1", Type: "A", Ts: 2000},
		{ID: "e6", Proc: "P1", Obj: "O1", Type: "B", Ts: 2010},
		{ID: "e7", Proc: "P1", Obj: "O1", Type: "C", Ts: 2020},
	})
	if score, hits := eng.Query("P1"); score != 5 || len(hits) != 1 {
		t.Fatalf("重复行为不应加分, got score=%d hits=%+v", score, hits)
	}
}

// 场景二：大量重叠前置行为、同时间戳事件与等待区间端点。
func TestOverlappingAndBoundaries(t *testing.T) {
	eng := NewEngine()
	rules := []Rule{
		{ // 三阶段同类型，验证单个事件不能占用两个阶段
			ID:      "R-SAME",
			Types:   [3]string{"X", "X", "X"},
			WaitMin: [2]int64{10, 30},
			WaitMax: [2]int64{20, 40},
			MaxSpan: 60,
			Weight:  3,
		},
		{ // 零等待：同时间戳事件按摄入顺序成链
			ID:      "R-ZERO",
			Types:   [3]string{"A", "B", "C"},
			WaitMin: [2]int64{0, 0},
			WaitMax: [2]int64{0, 0},
			MaxSpan: 0,
			Weight:  4,
		},
	}
	if err := eng.LoadRules(rules); err != nil {
		t.Fatalf("LoadRules: %v", err)
	}
	var events []Event
	// 大量重叠的前置行为：同一 (进程, 对象) 上 200 个相同类型事件
	for i := 0; i < 200; i++ {
		events = append(events, Event{
			ID:   "dup",
			Proc: "P1",
			Obj:  "O1",
			Type: "X",
			Ts:   int64(i % 5), // 时间戳 0..4 大量重复
		})
	}
	// 端点链：t2-t1=10（下界），t3-t2=40（上界），跨度 50 ≤ 60
	events = append(events,
		Event{ID: "c1", Proc: "P1", Obj: "O1", Type: "X", Ts: 100},
		Event{ID: "c2", Proc: "P1", Obj: "O1", Type: "X", Ts: 110},
		Event{ID: "c3", Proc: "P1", Obj: "O1", Type: "X", Ts: 150},
	)
	// 同时间戳、零等待链：按摄入顺序 A→B→C
	events = append(events,
		Event{ID: "z1", Proc: "P1", Obj: "O1", Type: "A", Ts: 500},
		Event{ID: "z2", Proc: "P1", Obj: "O1", Type: "B", Ts: 500},
		Event{ID: "z3", Proc: "P1", Obj: "O1", Type: "C", Ts: 500},
	)
	// 同时间戳但顺序为 C→B→A，不得命中 R-ZERO
	events = append(events,
		Event{ID: "w1", Proc: "P3", Obj: "O3", Type: "C", Ts: 500},
		Event{ID: "w2", Proc: "P3", Obj: "O3", Type: "B", Ts: 500},
		Event{ID: "w3", Proc: "P3", Obj: "O3", Type: "A", Ts: 500},
	)
	eng.Ingest(events)

	score, hits := eng.Query("P1")
	if score != 7 {
		t.Fatalf("P1 得分应为 7, got %d", score)
	}
	byRule := make(map[string]Evidence)
	for _, h := range hits {
		byRule[h.RuleID] = h.Evidence
	}
	evSame, ok := byRule["R-SAME"]
	if !ok {
		t.Fatalf("R-SAME 应命中, hits=%+v", hits)
	}
	ids := evSame.EventIDs
	if ids[0] == ids[1] || ids[1] == ids[2] || ids[0] == ids[2] {
		t.Fatalf("同一事件不得占用两个阶段: %v", ids)
	}
	if ids[2] != "c3" {
		t.Fatalf("R-SAME 应在首个合格末事件 c3 命中, got %v", ids)
	}
	evZero, ok := byRule["R-ZERO"]
	if !ok {
		t.Fatalf("R-ZERO 应命中, hits=%+v", hits)
	}
	if evZero.EventIDs != [3]string{"z1", "z2", "z3"} {
		t.Fatalf("R-ZERO 证据应为 z1,z2,z3, got %v", evZero.EventIDs)
	}
	if score3, hits3 := eng.Query("P3"); score3 != 0 || len(hits3) != 0 {
		t.Fatalf("P3 顺序颠倒不应命中, got score=%d hits=%+v", score3, hits3)
	}
}

// 场景三：同一末事件命中多条规则使总分从 20 升至 30，
// 只产生一条分数为 30 的 Critical 告警；改变批次划分结果一致。
func TestMultiRuleAlertAndBatching(t *testing.T) {
	build := func() []Rule {
		return []Rule{
			{ID: "RA", Types: [3]string{"A", "B", "C"}, WaitMin: [2]int64{0, 0}, WaitMax: [2]int64{100, 100}, MaxSpan: 1000, Weight: 10},
			{ID: "RB", Types: [3]string{"D", "E", "F"}, WaitMin: [2]int64{0, 0}, WaitMax: [2]int64{100, 100}, MaxSpan: 1000, Weight: 10},
			{ID: "RC", Types: [3]string{"G", "H", "I"}, WaitMin: [2]int64{0, 0}, WaitMax: [2]int64{100, 100}, MaxSpan: 1000, Weight: 5},
			{ID: "RD", Types: [3]string{"G", "H", "I"}, WaitMin: [2]int64{0, 0}, WaitMax: [2]int64{100, 100}, MaxSpan: 1000, Weight: 5},
		}
	}
	stream := []Event{
		{ID: "a1", Proc: "P", Obj: "O", Type: "A", Ts: 0},
		{ID: "a2", Proc: "P", Obj: "O", Type: "B", Ts: 10},
		{ID: "a3", Proc: "P", Obj: "O", Type: "C", Ts: 20}, // 命中 RA → 10
		{ID: "b1", Proc: "P", Obj: "O", Type: "D", Ts: 30},
		{ID: "b2", Proc: "P", Obj: "O", Type: "E", Ts: 40},
		{ID: "b3", Proc: "P", Obj: "O", Type: "F", Ts: 50}, // 命中 RB → 20
		{ID: "c1", Proc: "P", Obj: "O", Type: "G", Ts: 60},
		{ID: "c2", Proc: "P", Obj: "O", Type: "H", Ts: 70},
		{ID: "c3", Proc: "P", Obj: "O", Type: "I", Ts: 80}, // 同时命中 RC、RD → 30
		{ID: "d1", Proc: "P", Obj: "O", Type: "G", Ts: 90}, // 重复行为不再加分
		{ID: "d2", Proc: "P", Obj: "O", Type: "H", Ts: 100},
		{ID: "d3", Proc: "P", Obj: "O", Type: "I", Ts: 110},
	}

	run := func(batches [][]Event) ([]Alert, int, []RuleHit) {
		eng := NewEngine()
		if err := eng.LoadRules(build()); err != nil {
			t.Fatalf("LoadRules: %v", err)
		}
		var alerts []Alert
		for _, b := range batches {
			alerts = append(alerts, eng.Ingest(b)...)
		}
		score, hits := eng.Query("P")
		return alerts, score, hits
	}

	// 逐条摄入
	var oneByOne [][]Event
	for _, ev := range stream {
		oneByOne = append(oneByOne, []Event{ev})
	}
	alerts1, score1, hits1 := run(oneByOne)
	// 整批摄入
	alerts2, score2, hits2 := run([][]Event{stream})
	// 不规则分批
	alerts3, score3, hits3 := run([][]Event{stream[:5], stream[5:9], stream[9:]})

	for i, alerts := range [][]Alert{alerts1, alerts2, alerts3} {
		if len(alerts) != 1 {
			t.Fatalf("批次方案 %d: 应只产生一条告警, got %+v", i, alerts)
		}
		if alerts[0].Level != LevelCritical || alerts[0].Score != 30 || alerts[0].Proc != "P" {
			t.Fatalf("批次方案 %d: 告警应为 Critical/30/P, got %+v", i, alerts[0])
		}
	}
	if score1 != 30 || score2 != 30 || score3 != 30 {
		t.Fatalf("得分应为 30, got %d/%d/%d", score1, score2, score3)
	}
	if len(hits1) != 4 {
		t.Fatalf("应命中 4 条规则, got %+v", hits1)
	}
	// 批次划分不得改变命中时点（证据）与得分
	for i := range hits1 {
		if hits1[i] != hits2[i] || hits1[i] != hits3[i] {
			t.Fatalf("批次划分改变了命中证据: %+v / %+v / %+v", hits1[i], hits2[i], hits3[i])
		}
	}
}

func TestLoadRulesValidation(t *testing.T) {
	valid := Rule{ID: "ok", Types: [3]string{"A", "B", "C"}, WaitMax: [2]int64{1, 1}, Weight: 1}
	cases := []struct {
		name  string
		rules []Rule
	}{
		{"空标识", []Rule{{Types: valid.Types, Weight: 1}}},
		{"重复标识", []Rule{valid, valid}},
		{"下界越界", []Rule{{ID: "x", Types: valid.Types, WaitMin: [2]int64{-1, 0}, Weight: 1}}},
		{"上界越界", []Rule{{ID: "x", Types: valid.Types, WaitMax: [2]int64{60001, 0}, Weight: 1}}},
		{"下界大于上界", []Rule{{ID: "x", Types: valid.Types, WaitMin: [2]int64{5, 0}, WaitMax: [2]int64{1, 0}, Weight: 1}}},
		{"跨度越界", []Rule{{ID: "x", Types: valid.Types, MaxSpan: 60001, Weight: 1}}},
		{"权重越界", []Rule{{ID: "x", Types: valid.Types, Weight: 0}}},
		{"权重越界2", []Rule{{ID: "x", Types: valid.Types, Weight: 11}}},
	}
	for _, c := range cases {
		eng := NewEngine()
		if err := eng.LoadRules(c.rules); err == nil {
			t.Fatalf("%s: 应返回错误", c.name)
		}
	}
	eng := NewEngine()
	if err := eng.LoadRules([]Rule{valid}); err != nil {
		t.Fatalf("合法规则应载入成功: %v", err)
	}
	if err := eng.LoadRules([]Rule{{ID: "again", Types: valid.Types, Weight: 1}}); err == nil {
		t.Fatalf("重复载入应返回错误")
	}
}
