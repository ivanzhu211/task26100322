package chaindet

import "sort"

// alertThreshold 为触发 Critical 告警的总分阈值。
const alertThreshold = 25

// Engine 为进程行为链检测引擎,规则、运行状态与证据全部保存在内存。
type Engine struct {
	rules   []Rule
	byStage [3]map[string][]int // 每个阶段:行为类型 -> 规则下标
	procs   map[string]*procState
}

// NewEngine 一次载入规则,运行期间规则不变。
func NewEngine(rules []Rule) *Engine {
	e := &Engine{
		rules: rules,
		procs: make(map[string]*procState),
	}
	for i := range e.byStage {
		e.byStage[i] = make(map[string][]int)
	}
	for idx := range e.rules {
		r := &e.rules[idx]
		for stage := 0; stage < 3; stage++ {
			b := r.Behaviors[stage]
			e.byStage[stage][b] = append(e.byStage[stage][b], idx)
		}
	}
	return e
}

// Ingest 分批摄入遥测,返回本批次新产生的告警。
// 事件整体按时间戳非递减摄入,相同时间戳以摄入顺序为先后。
func (e *Engine) Ingest(events []Event) []Alert {
	var alerts []Alert
	for i := range events {
		if a, ok := e.process(events[i]); ok {
			alerts = append(alerts, a)
		}
	}
	return alerts
}

// Score 查询指定进程的总分、已命中规则及其证据。
func (e *Engine) Score(processID string) ProcessScore {
	ps := e.procs[processID]
	if ps == nil {
		return ProcessScore{}
	}
	hits := make([]RuleHit, len(ps.hits))
	copy(hits, ps.hits)
	return ProcessScore{TotalScore: ps.totalScore, Hits: hits}
}

// process 处理单条事件,若该事件使进程总分首次越阈则返回告警。
func (e *Engine) process(ev Event) (Alert, bool) {
	ps := e.procs[ev.ProcessID]
	if ps == nil {
		ps = newProcState()
		e.procs[ev.ProcessID] = ps
	}
	prevScore := ps.totalScore

	// 顺序:先判定阶段三完成,再生成阶段二部分链,最后登记阶段一候选,
	// 保证同一事件不会在同一次命中中充当两个阶段。
	e.advanceStage3(ps, ev)
	e.advanceStage2(ps, ev)
	e.advanceStage1(ps, ev)

	if !ps.alerted && prevScore < alertThreshold && ps.totalScore >= alertThreshold {
		ps.alerted = true
		return Alert{ProcessID: ev.ProcessID, Score: ps.totalScore, Level: "Critical"}, true
	}
	return Alert{}, false
}

// advanceStage3 用当前事件作为阶段三,尝试完成各规则的行为链。
func (e *Engine) advanceStage3(ps *procState, ev Event) {
	for _, idx := range e.byStage[2][ev.Behavior] {
		if ps.done[idx] {
			continue
		}
		rs := ps.rules[idx]
		if rs == nil {
			continue
		}
		r := &e.rules[idx]
		rs.s2.prune(ev.Timestamp, r.Wait2.Max)
		pairs := rs.s2.active()
		if len(pairs) == 0 {
			continue
		}
		// pairs 按 t2 升序;t2 窗口内首个同对象且满足跨度的 pair 即合法。
		lo := sort.Search(len(pairs), func(i int) bool {
			return pairs[i].t2 >= ev.Timestamp-r.Wait2.Max
		})
		hi := ev.Timestamp - r.Wait2.Min
		spanLo := ev.Timestamp - r.MaxSpan
		for i := lo; i < len(pairs) && pairs[i].t2 <= hi; i++ {
			if pairs[i].obj != ev.ObjectID || pairs[i].t1 < spanLo {
				continue
			}
			p := pairs[i]
			ps.hits = append(ps.hits, RuleHit{
				RuleID:   r.ID,
				Evidence: Evidence{EventIDs: [3]string{p.id1, p.id2, ev.ID}},
			})
			ps.totalScore += r.Weight
			ps.done[idx] = true
			delete(ps.rules, idx) // 同一规则对一个进程终身只计一次分
			break
		}
	}
}

// advanceStage2 用当前事件作为阶段二,与同对象最晚的合法阶段一候选配对。
func (e *Engine) advanceStage2(ps *procState, ev Event) {
	for _, idx := range e.byStage[1][ev.Behavior] {
		if ps.done[idx] {
			continue
		}
		rs := ps.rules[idx]
		if rs == nil {
			continue // 无阶段一候选,不可能配对
		}
		r := &e.rules[idx]
		rs.s1.prune(ev.Timestamp - s1UsefulWindow(r))
		list := rs.s1.active()
		if len(list) == 0 {
			continue
		}
		lower := ev.Timestamp - r.Wait1.Max
		if spanLower := ev.Timestamp - r.MaxSpan; spanLower > lower {
			lower = spanLower
		}
		upper := ev.Timestamp - r.Wait1.Min
		// 自后向前找同对象候选:首个落在窗口内的即最晚合法 e1。
		for i := len(list) - 1; i >= 0; i-- {
			c := &list[i]
			if c.ts > upper {
				continue
			}
			if c.ts < lower {
				break
			}
			if c.obj != ev.ObjectID {
				continue
			}
			rs.s2.push(s2Pair{id1: c.id, id2: ev.ID, obj: ev.ObjectID, t1: c.ts, t2: ev.Timestamp})
			break
		}
	}
}

// advanceStage1 把当前事件登记为各规则的阶段一候选。
func (e *Engine) advanceStage1(ps *procState, ev Event) {
	for _, idx := range e.byStage[0][ev.Behavior] {
		if ps.done[idx] {
			continue
		}
		rs := ps.rules[idx]
		if rs == nil {
			rs = &ruleState{}
			ps.rules[idx] = rs
		}
		r := &e.rules[idx]
		rs.s1.prune(ev.Timestamp - s1UsefulWindow(r))
		rs.s1.push(s1Entry{id: ev.ID, obj: ev.ObjectID, ts: ev.Timestamp})
	}
}

// s1UsefulWindow 返回 e1 事件在 now 之后仍可能用到的宽限时长。
func s1UsefulWindow(r *Rule) int64 {
	w := r.Wait1.Max + r.Wait2.Max
	if r.MaxSpan < w {
		w = r.MaxSpan
	}
	return w
}
