package chaindet

// s1Entry 为阶段一候选事件。
type s1Entry struct {
	id  string
	obj string
	ts  int64
}

// s2Pair 为已完成阶段一、二的部分链,每对只保留对 e2 而言最晚的合法 e1。
type s2Pair struct {
	id1 string
	id2 string
	obj string
	t1  int64
	t2  int64
}

// s1List 为按时间升序的候选队列,头部惰性删除、定期压缩,摊还 O(1)。
type s1List struct {
	items []s1Entry
	head  int
}

func (l *s1List) push(e s1Entry) { l.items = append(l.items, e) }

// prune 丢弃时间戳早于 cutoff 的候选。
func (l *s1List) prune(cutoff int64) {
	for l.head < len(l.items) && l.items[l.head].ts < cutoff {
		l.head++
	}
	if l.head > 64 && l.head*2 >= len(l.items) {
		l.items = append([]s1Entry(nil), l.items[l.head:]...)
		l.head = 0
	}
}

func (l *s1List) active() []s1Entry { return l.items[l.head:] }

// s2List 为按 t2 升序的部分链队列。
// 注意:t1 随对象交错不具备单调性,跨度条件只能在查询时判定。
type s2List struct {
	items []s2Pair
	head  int
}

func (l *s2List) push(p s2Pair) { l.items = append(l.items, p) }

// prune 按 t2 单调性丢弃对当前时间 now 不再有用的部分链。
func (l *s2List) prune(now, wait2Max int64) {
	for l.head < len(l.items) {
		p := &l.items[l.head]
		if p.t2+wait2Max >= now {
			break
		}
		l.head++
	}
	if l.head > 64 && l.head*2 >= len(l.items) {
		l.items = append([]s2Pair(nil), l.items[l.head:]...)
		l.head = 0
	}
}

func (l *s2List) active() []s2Pair { return l.items[l.head:] }

// ruleState 为某进程在某规则上的未命中运行状态。
// 条目内携带对象标识,避免按对象再建 map 的内存开销。
type ruleState struct {
	s1 s1List
	s2 s2List
}

// procState 为单进程运行状态。
type procState struct {
	totalScore int
	alerted    bool
	hits       []RuleHit
	done       map[int]bool       // 已命中规则下标,终身不再计分
	rules      map[int]*ruleState // 仅保存尚未命中的规则状态
}

func newProcState() *procState {
	return &procState{done: make(map[int]bool), rules: make(map[int]*ruleState)}
}
