package chaindet

// Event 是一条遥测事件。
type Event struct {
	ID        string // 唯一事件标识
	ProcessID string // 进程实例标识
	ObjectID  string // 对象标识
	Behavior  string // 行为类型
	Timestamp int64  // 毫秒时间戳
}

// Rule 是一条三阶段行为链规则。
type Rule struct {
	ID        string
	Behaviors [3]string // 按顺序排列的三个行为类型
	Wait1     Interval  // 阶段1->阶段2 等待时间区间(毫秒)
	Wait2     Interval  // 阶段2->阶段3 等待时间区间(毫秒)
	MaxSpan   int64     // 整条链最长跨度(毫秒)
	Weight    int       // 1 至 10
}

// Interval 为闭区间毫秒区间。
type Interval struct {
	Min int64
	Max int64
}

// Evidence 为一次命中的证据:三个事件标识。
type Evidence struct {
	EventIDs [3]string
}

// RuleHit 为某条规则在一个进程上的命中记录。
type RuleHit struct {
	RuleID   string
	Evidence Evidence
}

// Alert 为进程总分首次达到阈值时产生的告警。
type Alert struct {
	ProcessID string
	Score     int
	Level     string // "Critical"
}

// ProcessScore 为查询进程得分与命中证据的结果。
type ProcessScore struct {
	TotalScore int
	Hits       []RuleHit
}
