package gateway

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

// 从站侧错误（会被网关归类为可区分的写/读失败原因）。
var (
	errSlaveOffline  = errors.New("slave: link down")
	errSlaveBadValue = errors.New("slave: bad value (crc/illegal data)")
	errSlaveRejected = errors.New("slave: write rejected")
)

// Slave 是对单个 PLC 从站链路的抽象，内存模拟实现见 SimSlave。
type Slave interface {
	Read(ctx context.Context, addr uint16) (float64, error)
	Write(ctx context.Context, addr uint16, value float64) error
}

// SimSlave 内存模拟从站：保存地址->值，支持注入延迟、掉线、坏值、拒绝等故障。
// 所有方法并发安全。
type SimSlave struct {
	mu           sync.Mutex
	regs         map[uint16]float64
	offline      bool
	latency      time.Duration
	badValues    map[uint16]bool // 这些地址读到坏值
	rejectWrites bool
}

// NewSimSlave 创建模拟从站，initial 为地址初始值。
func NewSimSlave(initial map[uint16]float64) *SimSlave {
	regs := make(map[uint16]float64, len(initial))
	for a, v := range initial {
		regs[a] = v
	}
	return &SimSlave{regs: regs, badValues: make(map[uint16]bool)}
}

// --- 故障注入（测试/演示用） ---

// SetOffline 模拟链路断开/恢复。
func (s *SimSlave) SetOffline(off bool) {
	s.mu.Lock()
	s.offline = off
	s.mu.Unlock()
}

// SetLatency 模拟响应变慢。
func (s *SimSlave) SetLatency(d time.Duration) {
	s.mu.Lock()
	s.latency = d
	s.mu.Unlock()
}

// SetBadValues 指定哪些地址返回坏值（NaN，模拟 CRC 错/非法数据）。
func (s *SimSlave) SetBadValues(addrs map[uint16]bool) {
	s.mu.Lock()
	s.badValues = make(map[uint16]bool, len(addrs))
	for a, b := range addrs {
		s.badValues[a] = b
	}
	s.mu.Unlock()
}

// SetRejectWrites 模拟设备拒绝写（如联锁保护）。
func (s *SimSlave) SetRejectWrites(on bool) {
	s.mu.Lock()
	s.rejectWrites = on
	s.mu.Unlock()
}

// applyLatency 在持有故障配置快照后模拟链路延迟；ctx 取消则返回错误。
func (s *SimSlave) applyLatency(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Read 模拟读寄存器。
func (s *SimSlave) Read(ctx context.Context, addr uint16) (float64, error) {
	s.mu.Lock()
	off, lat, bad := s.offline, s.latency, s.badValues[addr]
	s.mu.Unlock()

	if off {
		return 0, errSlaveOffline
	}
	if err := s.applyLatency(ctx, lat); err != nil {
		return 0, err
	}
	if bad {
		return math.NaN(), errSlaveBadValue
	}
	s.mu.Lock()
	v, ok := s.regs[addr]
	s.mu.Unlock()
	if !ok {
		return 0, errSlaveBadValue
	}
	return v, nil
}

// Write 模拟写寄存器/线圈。
func (s *SimSlave) Write(ctx context.Context, addr uint16, value float64) error {
	s.mu.Lock()
	off, lat, rej := s.offline, s.latency, s.rejectWrites
	s.mu.Unlock()

	if off {
		return errSlaveOffline
	}
	if err := s.applyLatency(ctx, lat); err != nil {
		return err
	}
	if rej {
		return errSlaveRejected
	}
	s.mu.Lock()
	s.regs[addr] = value
	s.mu.Unlock()
	return nil
}

// Snapshot 返回当前从站全部地址的值（测试断言用）。
func (s *SimSlave) Snapshot() map[uint16]float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[uint16]float64, len(s.regs))
	for a, v := range s.regs {
		out[a] = v
	}
	return out
}
