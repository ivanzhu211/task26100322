package chaindet

import "sort"

// winEvent 是窗口内的事件，携带全局摄入序号。
// 同一类型列表内按下标顺序即 (Ts, seq) 升序。
type winEvent struct {
	id  string
	seq int64
	ts  int64
}

// objWindow 保存同一 (进程, 对象) 上按行为类型分组的事件序列。
// 所有区间与跨度均不超过 60000 毫秒，匹配只回看当前事件之前的事件，
// 因此无需物理清理，二分查找的时间下界自然排除过期事件。
type objWindow struct {
	byType map[string][]winEvent
}

func newObjWindow() *objWindow {
	return &objWindow{byType: make(map[string][]winEvent)}
}

func (w *objWindow) append(typ string, ev winEvent) {
	w.byType[typ] = append(w.byType[typ], ev)
}

// lowerBound 返回 list 中首个 ts >= t 的下标。
func lowerBound(list []winEvent, t int64) int {
	return sort.Search(len(list), func(i int) bool { return list[i].ts >= t })
}

// upperBound 返回 list 中首个 ts > t 的下标。
func upperBound(list []winEvent, t int64) int {
	return sort.Search(len(list), func(i int) bool { return list[i].ts > t })
}

// findChain 在窗口中寻找规则 r 的一次合法命中，末事件时间戳为 ts
//（末事件尚未加入窗口，故窗口内所有事件摄入序号都小于它）。
// checked 是该 (规则, 进程, 对象) 在第二阶段列表上已扫描到的下标，
// 返回更新后的下标。每个第二阶段事件只会被检查一次：
// 若它在检查时不存在合法首事件，则以后也绝不会有（候选集已固定）；
// 若存在合法首事件但跨度不满足，则未来末事件时间戳更大，跨度只会更不满足。
func (w *objWindow) findChain(r *Rule, ts int64, lastID string, checked int) (Evidence, int, bool) {
	list2 := w.byType[r.Types[1]]
	lo := lowerBound(list2, ts-r.WaitMax[1])
	hi := upperBound(list2, ts-r.WaitMin[1])
	if checked > lo {
		lo = checked
	}
	list1 := w.byType[r.Types[0]]
	for i := lo; i < hi; i++ {
		e2 := list2[i]
		loTs := e2.ts - r.WaitMax[0]
		if spanLo := ts - r.MaxSpan; spanLo > loTs {
			loTs = spanLo
		}
		hiTs := e2.ts - r.WaitMin[0]
		if loTs <= hiTs {
			a := lowerBound(list1, loTs)
			b := upperBound(list1, hiTs)
			// 列表内 seq 随下标递增，区间内最小 seq 在 a 处；
			// 要求首事件摄入序号严格小于第二阶段事件（不同事件）。
			if a < b && list1[a].seq < e2.seq {
				return Evidence{EventIDs: [3]string{list1[a].id, e2.id, lastID}}, hi, true
			}
		}
	}
	return Evidence{}, hi, false
}
