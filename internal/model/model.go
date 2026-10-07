// Package model 定义网关内部共享的领域类型：点位、值视图、设备与错误分类。
package model

import (
	"errors"
	"time"
)

// DeviceID 设备标识。
type DeviceID string

// PointID 点位标识（全局唯一，形如 "dev1.temp"）。
type PointID string

// DataType 点位数据类型。
type DataType string

const (
	TypeInt    DataType = "int"
	TypeFloat  DataType = "float"
	TypeBool   DataType = "bool"
)

// PointConfig 一个采集/控制点的配置。
type PointConfig struct {
	ID       PointID
	Device   DeviceID
	Address  string
	Type     DataType
	ReadOnly bool
	Min      float64 // 写值下限（数值型）
	Max      float64 // 写值上限（数值型）
}

// Quality 值质量。
type Quality string

const (
	// QualityGood 可信的有效值。
	QualityGood Quality = "good"
	// QualityUncertain 最后一次可信但已过期/设备降级中。
	QualityUncertain Quality = "uncertain"
	// QualityBad 读失败/坏值，不可当有效值使用。
	QualityBad Quality = "bad"
)

// PointValue 统一值视图中的一个点位值。
type PointValue struct {
	ID        PointID
	Device    DeviceID
	Value     float64
	BoolValue bool
	IsBool    bool
	Quality   Quality
	// Ts 该值由设备确认的时间戳；good 时表示采集时间，uncertain 时表示最后可信时间。
	Ts time.Time
	// CollectedAt 本次采集完成时间，用于新鲜度判断。
	CollectedAt time.Time
}

// Fresh 报告值在 now 下是否处于 maxAge 新鲜窗口内。
// 只有质量 good 且时间戳在窗口内才算新鲜；坏值永远不新鲜。
func (v PointValue) Fresh(now time.Time, maxAge time.Duration) bool {
	if v.Quality != QualityGood {
		return false
	}
	if v.Ts.IsZero() || maxAge <= 0 {
		return false
	}
	age := now.Sub(v.Ts)
	return age >= 0 && age <= maxAge
}

// ReadStatus 单个设备/点位的读取结果状态。
type ReadStatus struct {
	Device DeviceID
	Err    error
}

// Device 是网关依赖的从站抽象，内存模拟从站和真实 PLC 驱动都实现它。
type Device interface {
	ID() DeviceID
	// Read 一次读取多个地址；慢/坏/超时由实现返回错误。
	Read(addresses []string) (map[string]float64, error)
	// Write 下发一个地址；返回 nil 表示设备已确认。
	Write(address string, value float64) error
	// Close 释放连接。
	Close() error
}

// 写失败的可区分原因。
var (
	ErrDeviceOffline = errors.New("device offline")
	ErrTimeout       = errors.New("timeout")
	ErrOutOfRange    = errors.New("value out of range")
	ErrRejected      = errors.New("write rejected by device")
	ErrReadOnly      = errors.New("point is read-only")
	ErrUnknownPoint  = errors.New("unknown point")
	ErrBadValue      = errors.New("bad value from device")
)

// WriteStatus 写指令的终态。
type WriteStatus string

const (
	WritePending   WriteStatus = "pending"
	WriteConfirmed WriteStatus = "confirmed"
	WriteFailed    WriteStatus = "failed"
)

// WriteResult 一次写指令的结果。
type WriteResult struct {
	CommandID string
	Point     PointID
	Status    WriteStatus
	Attempts  int
	Err       error
	Ts        time.Time
}

// DeviceState 设备连接状态。
type DeviceState string

const (
	StateOnline  DeviceState = "online"
	StateOffline DeviceState = "offline"
)

// SafeState 掉线后的安全策略。
type SafeState string

const (
	// SafeHold 保持最后一次可信值（标记 uncertain）。
	SafeHold SafeState = "hold"
	// SafeZero 输出零值/断开（这里体现为标记 uncertain 并保持缓存值，由使用方按安全值处理）。
	SafeZero SafeState = "zero"
)
