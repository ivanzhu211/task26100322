package chaindet

import "testing"

// 空摄入与未知进程查询应返回零值。
func TestEmptyAndUnknown(t *testing.T) {
	eng := NewEngine([]Rule{
		{ID: "r", Behaviors: [3]string{"A", "B", "C"}, Wait1: Interval{0, 10}, Wait2: Interval{0, 10}, MaxSpan: 30, Weight: 5},
	})
	if alerts := eng.Ingest(nil); len(alerts) != 0 {
		t.Fatalf("空摄入不应产生告警: %v", alerts)
	}
	if got := eng.Score("nobody"); got.TotalScore != 0 || len(got.Hits) != 0 {
		t.Fatalf("未知进程应返回零值: %+v", got)
	}
	eng.Ingest([]Event{{ID: "e1", ProcessID: "p", ObjectID: "o", Behavior: "A", Timestamp: 0}})
	if got := eng.Score("p"); got.TotalScore != 0 || len(got.Hits) != 0 {
		t.Fatalf("仅有阶段一不应命中: %+v", got)
	}
}

// 同一事件不得在同一命中中充当两个阶段(阶段类型相同的规则)。
func TestEventNotReusedAcrossStages(t *testing.T) {
	rule := Rule{
		ID:        "r",
		Behaviors: [3]string{"A", "A", "B"},
		Wait1:     Interval{0, 100},
		Wait2:     Interval{0, 100},
		MaxSpan:   60000,
		Weight:    5,
	}
	eng := NewEngine([]Rule{rule})
	// 只有一个 A,不能同时充当阶段一和阶段二。
	eng.Ingest([]Event{
		{ID: "a1", ProcessID: "p", ObjectID: "o", Behavior: "A", Timestamp: 0},
		{ID: "b1", ProcessID: "p", ObjectID: "o", Behavior: "B", Timestamp: 10},
	})
	if got := eng.Score("p"); got.TotalScore != 0 {
		t.Fatalf("单个 A 不得复用为两个阶段: %+v", got)
	}
	eng.Ingest([]Event{
		{ID: "a2", ProcessID: "p", ObjectID: "o", Behavior: "A", Timestamp: 20},
		{ID: "b2", ProcessID: "p", ObjectID: "o", Behavior: "B", Timestamp: 30},
	})
	got := eng.Score("p")
	if got.TotalScore != 5 || len(got.Hits) != 1 {
		t.Fatalf("两个不同 A 应完成链: %+v", got)
	}
	// 命中时点是首个合法末事件 b1(由 a1,a2? 不,b1 时只有一个 A)。
	// 合法链:a1->a2->b2 或 a2->a2 非法;实际应为 [a1 a2 b2]。
	if got.Hits[0].Evidence.EventIDs != [3]string{"a1", "a2", "b2"} {
		t.Fatalf("证据应为 [a1 a2 b2]: %v", got.Hits[0].Evidence.EventIDs)
	}
}
