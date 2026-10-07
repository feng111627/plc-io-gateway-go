// Package sim 提供内存里的模拟 PLC 从站，实现 model.Device。
//
// 它可以被测试/演示代码控制：制造延迟、返回坏值、拒绝写入、
// 制造瞬时可恢复故障，以及模拟设备掉线与恢复。
package sim

import (
	"sync"
	"time"

	"github.com/feng111627/plc-io-gateway-go/internal/model"
)

// Device 内存模拟从站。所有方法并发安全。
type Device struct {
	id model.DeviceID
	mu sync.Mutex

	online       bool
	registers    map[string]float64
	readDelay    time.Duration
	writeDelay   time.Duration
	badAddresses map[string]bool
	rejectWrites bool
	writeCount   int
	writeLog     []writeRecord
	// failNextWrites 剩余的“写超时”次数，用于模拟瞬时可恢复故障。
	failNextWrites int
	// readFailNext 剩余的“读超时/坏”次数。
	readFailNext int
}

type writeRecord struct {
	address string
	value   float64
	at      time.Time
}

// New 创建一台模拟从站。registers 为初始寄存器快照（会被复制）。
func New(id model.DeviceID, registers map[string]float64) *Device {
	cp := make(map[string]float64, len(registers))
	for k, v := range registers {
		cp[k] = v
	}
	return &Device{
		id:           id,
		online:       true,
		registers:    cp,
		badAddresses: make(map[string]bool),
	}
}

func (d *Device) ID() model.DeviceID { return d.id }

// Read 一次读取多个地址。
//
// - 设备掉线：返回 model.ErrDeviceOffline；
// - readFailNext>0：本次返回 model.ErrTimeout（瞬时故障）；
// - 某地址在 badAddresses 中：返回 model.ErrBadValue；
// - 未配置地址：返回 model.ErrBadValue（未知地址也视为坏数据）；
// - 其余返回寄存器当前值，并按 readDelay 模拟响应变慢。
func (d *Device) Read(addresses []string) (map[string]float64, error) {
	d.mu.Lock()
	if !d.online {
		d.mu.Unlock()
		return nil, model.ErrDeviceOffline
	}
	delay := d.readDelay
	if d.readFailNext > 0 {
		d.readFailNext--
		d.mu.Unlock()
		if delay > 0 {
			time.Sleep(delay)
		}
		return nil, model.ErrTimeout
	}
	out := make(map[string]float64, len(addresses))
	for _, addr := range addresses {
		if d.badAddresses[addr] {
			d.mu.Unlock()
			return nil, model.ErrBadValue
		}
		v, ok := d.registers[addr]
		if !ok {
			d.mu.Unlock()
			return nil, model.ErrBadValue
		}
		out[addr] = v
	}
	d.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}
	return out, nil
}

// Write 下发一个地址。
//
// - 设备掉线：model.ErrDeviceOffline（调用方不应重试）；
// - failNextWrites>0：model.ErrTimeout（瞬时，可重试）；
// - rejectWrites：model.ErrRejected（设备明确拒绝，不重试）；
// - 成功：真正写寄存器并记录一次写入（用于验证不重复写）。
func (d *Device) Write(address string, value float64) error {
	d.mu.Lock()
	if !d.online {
		d.mu.Unlock()
		return model.ErrDeviceOffline
	}
	delay := d.writeDelay
	if d.failNextWrites > 0 {
		d.failNextWrites--
		d.mu.Unlock()
		if delay > 0 {
			time.Sleep(delay)
		}
		return model.ErrTimeout
	}
	if d.rejectWrites {
		d.mu.Unlock()
		return model.ErrRejected
	}
	d.registers[address] = value
	d.writeCount++
	d.writeLog = append(d.writeLog, writeRecord{address: address, value: value, at: time.Now()})
	d.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}
	return nil
}

func (d *Device) Close() error {
	d.mu.Lock()
	d.online = false
	d.mu.Unlock()
	return nil
}

// ---- 测试/演示用的故障注入与状态切换 ----

func (d *Device) SetOnline(online bool) {
	d.mu.Lock()
	d.online = online
	d.mu.Unlock()
}

func (d *Device) SetReadDelay(delay time.Duration) {
	d.mu.Lock()
	d.readDelay = delay
	d.mu.Unlock()
}

func (d *Device) SetWriteDelay(delay time.Duration) {
	d.mu.Lock()
	d.writeDelay = delay
	d.mu.Unlock()
}

func (d *Device) SetBadAddress(address string, bad bool) {
	d.mu.Lock()
	d.badAddresses[address] = bad
	d.mu.Unlock()
}

func (d *Device) SetRejectWrites(reject bool) {
	d.mu.Lock()
	d.rejectWrites = reject
	d.mu.Unlock()
}

// FailNextWrites 让接下来 n 次写返回超时（可恢复的瞬时故障）。
func (d *Device) FailNextWrites(n int) {
	d.mu.Lock()
	d.failNextWrites = n
	d.mu.Unlock()
}

// FailNextReads 让接下来 n 次读返回超时。
func (d *Device) FailNextReads(n int) {
	d.mu.Lock()
	d.readFailNext = n
	d.mu.Unlock()
}

// SetRegister 从外部直接改寄存器（模拟现场值变化）。
func (d *Device) SetRegister(address string, value float64) {
	d.mu.Lock()
	d.registers[address] = value
	d.mu.Unlock()
}

func (d *Device) WriteCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.writeCount
}

func (d *Device) WriteLog() []WriteEntry {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]WriteEntry, 0, len(d.writeLog))
	for _, r := range d.writeLog {
		out = append(out, WriteEntry{Address: r.address, Value: r.value, At: r.at})
	}
	return out
}

func (d *Device) Online() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.online
}

// WriteEntry 一次实际写入记录。
type WriteEntry struct {
	Address string
	Value   float64
	At      time.Time
}
