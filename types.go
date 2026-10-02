// Package chaindet 实现进程行为链检测：从遥测事件中识别
// 同一进程、同一对象上按序发生的三阶段可疑行为链。
package chaindet

// Event 是一条遥测事件。Ts 为毫秒时间戳，整体非递减；
// 时间戳相同时以摄入顺序（Ingest 参数顺序）确定先后。
type Event struct {
	ID   string // 唯一事件标识
	Proc string // 进程实例标识，不复用
	Obj  string // 对象标识（文件、注册表、内存等）
	Type string // 行为类型
	Ts   int64  // 毫秒时间戳
}

// Rule 是一条三阶段行为链规则。运行期间不变。
type Rule struct {
	ID      string    // 唯一规则标识
	Types   [3]string // 按顺序排列的三个行为类型，可以相同
	WaitMin [2]int64  // 相邻阶段等待时间下界（含），0..60000 毫秒
	WaitMax [2]int64  // 相邻阶段等待时间上界（含），0..60000 毫秒
	MaxSpan int64     // 整条链最长跨度（含），0..60000 毫秒
	Weight  int       // 权重 1..10
}

// Evidence 是一次命中的证据：按阶段顺序的三个事件标识。
type Evidence struct {
	EventIDs [3]string
}

// RuleHit 表示某进程已命中的规则及其证据。
type RuleHit struct {
	RuleID   string
	Evidence Evidence
}

// AlertLevel 告警级别。
type AlertLevel string

// LevelCritical 总分首次达到或超过 25 时产生。
const LevelCritical AlertLevel = "Critical"

// Alert 总分首次从低于 25 达到或超过 25 时产生，
// Score 包含触发事件的全部贡献。每个进程最多告警一次。
type Alert struct {
	Proc  string
	Level AlertLevel
	Score int
}
