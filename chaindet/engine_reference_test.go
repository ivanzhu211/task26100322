package chaindet

import (
	"fmt"
	"math/rand"
	"testing"
)

// refResult 为朴素参考实现对一个进程的计算结果。
type refResult struct {
	totalScore int
	hitRules   map[string]bool
	hitEvent   map[string]string // ruleID -> 首个完成事件 ID
	alerted    bool
	alertScore int
}

// bruteForce 按题意直接模拟:逐事件检查每个规则是否在该事件首次完成。
func bruteForce(rules []Rule, events []Event, pid string) refResult {
	res := refResult{hitRules: map[string]bool{}, hitEvent: map[string]string{}}
	score := 0
	for i := 0; i < len(events); i++ {
		if events[i].ProcessID != pid {
			continue
		}
		gain := 0
		for _, r := range rules {
			if res.hitRules[r.ID] {
				continue
			}
			if ev, ok := firstCompletion(r, events, pid, i); ok {
				res.hitRules[r.ID] = true
				res.hitEvent[r.ID] = ev
				gain += r.Weight
			}
		}
		prev := score
		score += gain
		if !res.alerted && prev < alertThreshold && score >= alertThreshold {
			res.alerted = true
			res.alertScore = score
		}
	}
	res.totalScore = score
	return res
}

// firstCompletion 判断事件 i 是否是规则 r 在该进程上的首个合法完成事件。
func firstCompletion(r Rule, events []Event, pid string, i int) (string, bool) {
	e3 := events[i]
	if e3.Behavior != r.Behaviors[2] {
		return "", false
	}
	for j := 0; j < i; j++ {
		e2 := events[j]
		if e2.ProcessID != pid || e2.ObjectID != e3.ObjectID || e2.Behavior != r.Behaviors[1] {
			continue
		}
		d2 := e3.Timestamp - e2.Timestamp
		if d2 < r.Wait2.Min || d2 > r.Wait2.Max {
			continue
		}
		for k := 0; k < j; k++ {
			e1 := events[k]
			if e1.ProcessID != pid || e1.ObjectID != e3.ObjectID || e1.Behavior != r.Behaviors[0] {
				continue
			}
			d1 := e2.Timestamp - e1.Timestamp
			if d1 < r.Wait1.Min || d1 > r.Wait1.Max {
				continue
			}
			if e3.Timestamp-e1.Timestamp > r.MaxSpan {
				continue
			}
			return e3.ID, true
		}
	}
	return "", false
}

// validateEvidence 独立校验证据本身是否合法。
func validateEvidence(t *testing.T, r Rule, ev Evidence, events []Event, pid string) {
	t.Helper()
	byID := make(map[string]int, len(events))
	for i, e := range events {
		byID[e.ID] = i
	}
	i1, ok1 := byID[ev.EventIDs[0]]
	i2, ok2 := byID[ev.EventIDs[1]]
	i3, ok3 := byID[ev.EventIDs[2]]
	if !ok1 || !ok2 || !ok3 {
		t.Fatalf("证据包含未知事件: %v", ev.EventIDs)
	}
	if i1 == i2 || i2 == i3 || i1 == i3 {
		t.Fatalf("同一事件被占用为多个阶段: %v", ev.EventIDs)
	}
	if !(i1 < i2 && i2 < i3) {
		t.Fatalf("证据事件顺序非法: %v", ev.EventIDs)
	}
	e1, e2, e3 := events[i1], events[i2], events[i3]
	for _, e := range []Event{e1, e2, e3} {
		if e.ProcessID != pid || e.ObjectID != e3.ObjectID {
			t.Fatalf("证据串用了其他进程或对象: %v", ev.EventIDs)
		}
	}
	if e1.Behavior != r.Behaviors[0] || e2.Behavior != r.Behaviors[1] || e3.Behavior != r.Behaviors[2] {
		t.Fatalf("证据行为类型不匹配: 规则 %v", r.ID)
	}
	if d := e2.Timestamp - e1.Timestamp; d < r.Wait1.Min || d > r.Wait1.Max {
		t.Fatalf("证据违反等待区间1: %d 不在 [%d,%d]", d, r.Wait1.Min, r.Wait1.Max)
	}
	if d := e3.Timestamp - e2.Timestamp; d < r.Wait2.Min || d > r.Wait2.Max {
		t.Fatalf("证据违反等待区间2: %d 不在 [%d,%d]", d, r.Wait2.Min, r.Wait2.Max)
	}
	if e3.Timestamp-e1.Timestamp > r.MaxSpan {
		t.Fatalf("证据违反最长跨度: 规则=%+v 证据=%v e1=%+v e2=%+v e3=%+v", r, ev.EventIDs, e1, e2, e3)
	}
}

// TestRandomAgainstBruteForce 随机小规模工作负载与朴素实现全量对拍。
func TestRandomAgainstBruteForce(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for iter := 0; iter < 3000; iter++ {
		nBeh := 1 + rng.Intn(3)
		behaviors := make([]string, nBeh)
		for i := range behaviors {
			behaviors[i] = fmt.Sprintf("b%d", i)
		}
		nObj := 1 + rng.Intn(3)
		nProc := 1 + rng.Intn(2)
		nRules := 1 + rng.Intn(5)
		rules := make([]Rule, nRules)
		for i := range rules {
			rules[i] = Rule{
				ID: fmt.Sprintf("r%d", i),
				Behaviors: [3]string{
					behaviors[rng.Intn(nBeh)],
					behaviors[rng.Intn(nBeh)],
					behaviors[rng.Intn(nBeh)],
				},
				Wait1:   randInterval(rng),
				Wait2:   randInterval(rng),
				MaxSpan: int64(rng.Intn(8)),
				Weight:  1 + rng.Intn(10),
			}
		}
		nEv := 1 + rng.Intn(24)
		events := make([]Event, nEv)
		ts := int64(0)
		for i := range events {
			ts += int64(rng.Intn(3)) // 允许相同时间戳
			events[i] = Event{
				ID:        fmt.Sprintf("e%d", i),
				ProcessID: fmt.Sprintf("p%d", rng.Intn(nProc)),
				ObjectID:  fmt.Sprintf("o%d", rng.Intn(nObj)),
				Behavior:  behaviors[rng.Intn(nBeh)],
				Timestamp: ts,
			}
		}

		eng := NewEngine(rules)
		// 随机批次划分,验证批次无关性。
		var alerts []Alert
		for pos := 0; pos < len(events); {
			step := 1 + rng.Intn(4)
			if pos+step > len(events) {
				step = len(events) - pos
			}
			alerts = append(alerts, eng.Ingest(events[pos:pos+step])...)
			pos += step
		}

		alertByProc := map[string]Alert{}
		for _, a := range alerts {
			if a.Level != "Critical" {
				t.Fatalf("iter %d: 告警级别非法: %v", iter, a.Level)
			}
			if _, dup := alertByProc[a.ProcessID]; dup {
				t.Fatalf("iter %d: 进程 %v 重复告警", iter, a.ProcessID)
			}
			alertByProc[a.ProcessID] = a
		}

		ruleByID := map[string]Rule{}
		for _, r := range rules {
			ruleByID[r.ID] = r
		}
		for p := 0; p < nProc; p++ {
			pid := fmt.Sprintf("p%d", p)
			want := bruteForce(rules, events, pid)
			got := eng.Score(pid)
			if got.TotalScore != want.totalScore {
				t.Fatalf("iter %d 进程 %v: 总分=%d, 期望 %d\n规则=%v\n事件=%v",
					iter, pid, got.TotalScore, want.totalScore, rules, events)
			}
			if len(got.Hits) != len(want.hitRules) {
				t.Fatalf("iter %d 进程 %v: 命中规则数=%d, 期望 %d", iter, pid, len(got.Hits), len(want.hitRules))
			}
			for _, h := range got.Hits {
				if !want.hitRules[h.RuleID] {
					t.Fatalf("iter %d 进程 %v: 规则 %v 不应命中", iter, pid, h.RuleID)
				}
				validateEvidence(t, ruleByID[h.RuleID], h.Evidence, events, pid)
				// 证据的末事件必须是首个完成事件。
				if h.Evidence.EventIDs[2] != want.hitEvent[h.RuleID] {
					t.Fatalf("iter %d 进程 %v 规则 %v: 命中时点=%v, 期望首个完成事件 %v",
						iter, pid, h.RuleID, h.Evidence.EventIDs[2], want.hitEvent[h.RuleID])
				}
			}
			a, alerted := alertByProc[pid]
			if alerted != want.alerted {
				t.Fatalf("iter %d 进程 %v: 告警=%v, 期望 %v", iter, pid, alerted, want.alerted)
			}
			if alerted && a.Score != want.alertScore {
				t.Fatalf("iter %d 进程 %v: 告警分数=%d, 期望 %d", iter, pid, a.Score, want.alertScore)
			}
		}
	}
}

func randInterval(rng *rand.Rand) Interval {
	lo := rng.Intn(5)
	hi := lo + rng.Intn(4)
	return Interval{Min: int64(lo), Max: int64(hi)}
}
