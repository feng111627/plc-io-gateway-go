// Package gateway 实现工业设备采集与控制网关的核心逻辑。
package gateway

import (
	"errors"
	"fmt"
	"time"
)

// PointType 点位类型。
type PointType string

const (
	PointTypeHoldingRegister PointType = "holding_register" // 保持寄存器，可读写
	PointTypeInputRegister   PointType = "input_register"   // 输入寄存器，只读
	PointTypeCoil            PointType = "coil"             // 线圈，可读写（0/1）
)

// PointConfig 单个点位的静态配置。
type PointConfig struct {
	Name    string    `json:"name"`    // 点位名，设备内唯一
	Address uint16    `json:"address"` // 从站地址
	Type    PointType `json:"type"`
	Min     float64   `json:"min"` // 可写点位的允许下界（含）
	Max     float64   `json:"max"` // 可写点位的允许上界（含）
}

// Writable 报告该点位是否允许下行写。
func (p PointConfig) Writable() bool {
	return p.Type == PointTypeHoldingRegister || p.Type == PointTypeCoil
}

// DeviceConfig 单台设备的配置。
type DeviceConfig struct {
	ID            string        `json:"id"`
	PollInterval  time.Duration `json:"poll_interval"`  // 采集周期
	ReadTimeout   time.Duration `json:"read_timeout"`   // 单次读超时
	WriteTimeout  time.Duration `json:"write_timeout"`  // 单次写超时
	WriteRetries  int           `json:"write_retries"`  // 写失败后的最大重试次数
	FailThreshold int           `json:"fail_threshold"` // 连续失败多少次判定掉线
	StaleAfter    time.Duration `json:"stale_after"`    // 值超过该时长未刷新视为过期
	Points        []PointConfig `json:"points"`
}

// Quality 值的质量位。
type Quality string

const (
	QualityGood    Quality = "good"    // 新鲜且来自设备确认
	QualityStale   Quality = "stale"   // 超过新鲜期未刷新，不能当作有效值
	QualityBad     Quality = "bad"     // 设备返回了坏值/校验失败
	QualityOffline Quality = "offline" // 设备掉线，仅保留最后一次可信值
)

// PointValue 某点位的一次观测值。
type PointValue struct {
	Device    string    `json:"device"`
	Point     string    `json:"point"`
	Value     float64   `json:"value"`
	Quality   Quality   `json:"quality"`
	Timestamp time.Time `json:"timestamp"` // 值产生/确认的时间
	Seq       uint64    `json:"seq"`       // 设备内单调递增序号，用于读写顺序自洽
}

// Valid 报告该值是否可作为有效业务值使用。
func (v PointValue) Valid() bool { return v.Quality == QualityGood }

// 写失败的可区分原因。
var (
	ErrDeviceOffline   = errors.New("device offline")     // 设备离线
	ErrWriteTimeout    = errors.New("write timeout")      // 写超时
	ErrOutOfRange      = errors.New("value out of range") // 越界
	ErrRejectedBySlave = errors.New("rejected by device") // 被设备拒绝
	ErrPointNotFound   = errors.New("point not found")    // 点位不存在
	ErrPointReadOnly   = errors.New("point is read-only") // 点位只读
	ErrDuplicateCmd    = errors.New("duplicate command")  // 重复指令（已应用，幂等命中）
)

// WriteResult 一次下行写的结果。
type WriteResult struct {
	CommandID string    `json:"command_id"` // 调用方提供的幂等键
	Device    string    `json:"device"`
	Point     string    `json:"point"`
	Value     float64   `json:"value"`
	Applied   bool      `json:"applied"`   // 本次调用是否真正写入了设备
	Duplicate bool      `json:"duplicate"` // 是否为重复指令（命中幂等缓存）
	Attempts  int       `json:"attempts"`  // 实际尝试次数（含重试）
	Err       error     `json:"-"`
	Finished  time.Time `json:"finished"`
}

func (r WriteResult) String() string {
	if r.Err != nil {
		return fmt.Sprintf("cmd=%s %s/%s FAILED attempts=%d err=%v",
			r.CommandID, r.Device, r.Point, r.Attempts, r.Err)
	}
	dup := ""
	if r.Duplicate {
		dup = " (duplicate, idempotent-hit)"
	}
	return fmt.Sprintf("cmd=%s %s/%s=%.3f OK attempts=%d%s",
		r.CommandID, r.Device, r.Point, r.Value, r.Attempts, dup)
}
