package chaindet

import (
	"errors"
	"fmt"
	"sort"
)

// alertThreshold 总分首次达到或超过该值时产生 Critical 告警。
const alertThreshold = 25

const maxInterval = 60000

// procObj 是 (进程实例, 对象) 窗口的键。
type procObj struct {
	proc string
	obj  string
}

// scanKey 是 (规则, 进程, 对象) 扫描状态的键。
type scanKey struct {
	rule int
	procObj
}

// Engine 是行为链检测引擎。规则、运行状态和命中证据均保存在内存。
// 调用串行，不做持久化。
type Engine struct {
	rules    []Rule
	byStage3 map[string][]int // 末阶段行为类型 → 规则下标
	windows  map[procObj]*objWindow
	checked  map[scanKey]int            // 第二阶段列表的扫描进度
	hits     map[string]map[int]Evidence // 进程 → 规则下标 → 证据
	score    map[string]int
	alerted  map[string]bool
	seq      int64
}

// NewEngine 创建一个空引擎。
func NewEngine() *Engine {
	return &Engine{
		byStage3: make(map[string][]int),
		windows:  make(map[procObj]*objWindow),
		checked:  make(map[scanKey]int),
		hits:     make(map[string]map[int]Evidence),
		score:    make(map[string]int),
		alerted:  make(map[string]bool),
	}
}

// LoadRules 一次载入全部规则，规则运行期间不变。
func (e *Engine) LoadRules(rules []Rule) error {
	seen := make(map[string]bool, len(rules))
	for i, r := range rules {
		if r.ID == "" {
			return fmt.Errorf("规则 #%d: 标识为空", i)
		}
		if seen[r.ID] {
			return fmt.Errorf("规则 %q: 标识重复", r.ID)
		}
		seen[r.ID] = true
		for s := 0; s < 2; s++ {
			if r.WaitMin[s] < 0 || r.WaitMin[s] > maxInterval ||
				r.WaitMax[s] < 0 || r.WaitMax[s] > maxInterval {
				return fmt.Errorf("规则 %q: 等待区间超出 [0, %d]", r.ID, maxInterval)
			}
			if r.WaitMin[s] > r.WaitMax[s] {
				return fmt.Errorf("规则 %q: 等待区间下界大于上界", r.ID)
			}
		}
		if r.MaxSpan < 0 || r.MaxSpan > maxInterval {
			return fmt.Errorf("规则 %q: 最长跨度超出 [0, %d]", r.ID, maxInterval)
		}
		if r.Weight < 1 || r.Weight > 10 {
			return fmt.Errorf("规则 %q: 权重超出 [1, 10]", r.ID)
		}
	}
	if len(e.rules) > 0 {
		return errors.New("规则只能载入一次")
	}
	e.rules = append(e.rules, rules...)
	for i, r := range rules {
		e.byStage3[r.Types[2]] = append(e.byStage3[r.Types[2]], i)
	}
	return nil
}

// Ingest 分批摄入遥测，返回本批次新产生的告警。
// 命中时点、得分与告警不依赖批次划分。
func (e *Engine) Ingest(events []Event) []Alert {
	var alerts []Alert
	for _, ev := range events {
		e.seq++
		key := procObj{proc: ev.Proc, obj: ev.Obj}
		w := e.windows[key]
		if w == nil {
			w = newObjWindow()
			e.windows[key] = w
		}
		// 先把事件作为末事件参与匹配，再加入窗口，
		// 保证前序事件的摄入序号都严格小于它。
		gained := 0
		for _, ri := range e.byStage3[ev.Type] {
			procHits := e.hits[ev.Proc]
			if procHits != nil {
				if _, done := procHits[ri]; done {
					continue // 同一规则对一个进程终身只计一次分
				}
			}
			sk := scanKey{rule: ri, procObj: key}
			evid, next, ok := w.findChain(&e.rules[ri], ev.Ts, ev.ID, e.checked[sk])
			e.checked[sk] = next
			if !ok {
				continue
			}
			if procHits == nil {
				procHits = make(map[int]Evidence)
				e.hits[ev.Proc] = procHits
			}
			procHits[ri] = evid
			gained += e.rules[ri].Weight
		}
		if gained > 0 {
			before := e.score[ev.Proc]
			after := before + gained
			e.score[ev.Proc] = after
			if !e.alerted[ev.Proc] && before < alertThreshold && after >= alertThreshold {
				e.alerted[ev.Proc] = true
				alerts = append(alerts, Alert{Proc: ev.Proc, Level: LevelCritical, Score: after})
			}
		}
		w.append(ev.Type, winEvent{id: ev.ID, seq: e.seq, ts: ev.Ts})
	}
	return alerts
}

// Query 查询指定进程的总分、已命中规则及其证据（按规则标识排序）。
func (e *Engine) Query(proc string) (score int, hits []RuleHit) {
	score = e.score[proc]
	procHits := e.hits[proc]
	hits = make([]RuleHit, 0, len(procHits))
	for ri, evid := range procHits {
		hits = append(hits, RuleHit{RuleID: e.rules[ri].ID, Evidence: evid})
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i].RuleID < hits[j].RuleID })
	return score, hits
}
