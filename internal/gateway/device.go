package gateway

import (
	"context"
	"errors"
	"log"
	"math"
	"sync"
	"time"
)

// deviceState 设备运行状态机。
type deviceState int

const (
	stateOnline  deviceState = iota
	stateOffline             // 掉线降级：进入安全状态，拒绝写，缓存保留最后可信值
)

func (s deviceState) String() string {
	if s == stateOffline {
		return "offline"
	}
	return "online"
}

// Device 管理单台设备：周期采集、状态机、下行写（确认+重试+幂等）。
//
// 并发模型：
//   - ioMu 串行化所有对从站的 I/O（读与写），保证“写确认”与“采集读数”
//     在同一个顺序点上发生，先后关系自洽；
//   - mu 保护状态机、序号与幂等表；
//   - 单台设备的慢/故障只阻塞自己的 ioMu，不影响其它设备。
type Device struct {
	cfg   DeviceConfig
	slave Slave
	cache *valueCache

	mu          sync.Mutex
	state       deviceState
	failCount   int
	seq         uint64
	appliedCmds map[string]WriteResult   // 幂等键 -> 已应用结果
	inFlight    map[string]chan struct{} // 正在执行中的幂等键 -> 完成信号

	ioMu   sync.Mutex
	stopCh chan struct{}
	done   chan struct{}
}

func newDevice(cfg DeviceConfig, slave Slave, cache *valueCache) *Device {
	if cfg.FailThreshold <= 0 {
		cfg.FailThreshold = 3
	}
	if cfg.WriteRetries < 0 {
		cfg.WriteRetries = 0
	}
	return &Device{
		cfg:         cfg,
		slave:       slave,
		cache:       cache,
		state:       stateOnline,
		appliedCmds: make(map[string]WriteResult),
		inFlight:    make(map[string]chan struct{}),
		stopCh:      make(chan struct{}),
		done:        make(chan struct{}),
	}
}

// nextSeq 分配设备内单调递增序号。
func (d *Device) nextSeq() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seq++
	return d.seq
}

// Start 启动周期采集协程。
func (d *Device) Start() {
	go func() {
		defer close(d.done)
		t := time.NewTicker(d.cfg.PollInterval)
		defer t.Stop()
		for {
			select {
			case <-d.stopCh:
				return
			case <-t.C:
				ctx, cancel := context.WithTimeout(context.Background(), d.pollBudget())
				d.pollOnce(ctx)
				cancel()
			}
		}
	}()
}

// pollBudget 一轮采集的总预算：每点位一个读超时。
func (d *Device) pollBudget() time.Duration {
	n := len(d.cfg.Points)
	if n == 0 {
		n = 1
	}
	return time.Duration(n) * d.cfg.ReadTimeout
}

// Stop 停止采集并等待退出。
func (d *Device) Stop() {
	select {
	case <-d.done:
		return
	default:
	}
	close(d.stopCh)
	<-d.done
}

// State 返回当前状态（"online"/"offline"）。
func (d *Device) State() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.state.String()
}

// recordSuccess 一次成功 I/O 后的状态推进：掉线中则恢复在线。
func (d *Device) recordSuccess() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failCount = 0
	if d.state == stateOffline {
		d.state = stateOnline
		log.Printf("[device %s] recovered -> online, resyncing points", d.cfg.ID)
	}
}

// recordFailure 一次失败 I/O 后的状态推进：连续失败达阈值则进入掉线安全状态。
func (d *Device) recordFailure(err error) {
	d.mu.Lock()
	was := d.state
	d.failCount++
	offline := d.failCount >= d.cfg.FailThreshold
	if offline {
		d.state = stateOffline
	}
	fc := d.failCount
	d.mu.Unlock()

	if offline && was != stateOffline {
		// 进入安全状态：缓存中的值保留但标记为 offline（含最后可信时间戳）。
		d.cache.MarkOffline(d.cfg.ID, time.Now())
		log.Printf("[device %s] enter OFFLINE safe state after %d consecutive failures: %v",
			d.cfg.ID, fc, err)
	}
}

// isOffline 供写路径快速失败。
func (d *Device) isOffline() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.state == stateOffline
}

// classifyIOErr 把从站错误归类为可区分的业务原因。
func classifyIOErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return ErrWriteTimeout
	case errors.Is(err, errSlaveOffline):
		return ErrDeviceOffline
	case errors.Is(err, errSlaveRejected):
		return ErrRejectedBySlave
	default:
		return err
	}
}

// pollOnce 执行一轮采集：逐点位带超时读取，成功/失败都推进状态机。
// 坏值（NaN、越界）写入缓存时标记 QualityBad，绝不会被当成有效值。
func (d *Device) pollOnce(ctx context.Context) {
	for _, p := range d.cfg.Points {
		select {
		case <-ctx.Done():
			return
		default:
		}
		d.pollPoint(ctx, p)
	}
}

func (d *Device) pollPoint(ctx context.Context, p PointConfig) {
	rctx, cancel := context.WithTimeout(ctx, d.cfg.ReadTimeout)
	defer cancel()

	d.ioMu.Lock()
	v, err := d.slave.Read(rctx, p.Address)
	d.ioMu.Unlock()

	now := time.Now()
	if err != nil {
		if errors.Is(err, errSlaveBadValue) {
			// 坏值（CRC 错/非法数据）：链路是好的，不计入掉线失败，
			// 但该值必须标记为 Bad，绝不能当作有效值。
			d.cache.Put(PointValue{
				Device: d.cfg.ID, Point: p.Name, Value: math.NaN(),
				Quality: QualityBad, Timestamp: now, Seq: d.nextSeq(),
			})
			d.recordSuccess()
			log.Printf("[device %s] poll %s got BAD value: %v", d.cfg.ID, p.Name, err)
			return
		}
		d.recordFailure(err)
		log.Printf("[device %s] poll %s failed: %v", d.cfg.ID, p.Name, err)
		return
	}
	if math.IsNaN(v) || math.IsInf(v, 0) || v < p.Min || v > p.Max {
		// 坏值：记录为 Bad，不推进失败计数（链路是好的），但绝不作为有效值。
		d.cache.Put(PointValue{
			Device: d.cfg.ID, Point: p.Name, Value: v,
			Quality: QualityBad, Timestamp: now, Seq: d.nextSeq(),
		})
		d.recordSuccess()
		log.Printf("[device %s] poll %s got BAD value %v", d.cfg.ID, p.Name, v)
		return
	}
	d.cache.Put(PointValue{
		Device: d.cfg.ID, Point: p.Name, Value: v,
		Quality: QualityGood, Timestamp: now, Seq: d.nextSeq(),
	})
	d.recordSuccess()
}

// Write 下发写指令：校验 -> 幂等检查 -> 带超时的确认与重试。
//
// 幂等语义：同一 commandID 重复下发，直接返回首次的结果（Duplicate=true），
// 不会造成重复写入。失败原因可区分：离线/超时/越界/被设备拒绝。
func (d *Device) Write(ctx context.Context, commandID, point string, value float64) WriteResult {
	res := WriteResult{
		CommandID: commandID, Device: d.cfg.ID, Point: point, Value: value,
	}

	// 1. 静态校验：点位存在、可写、不越界。
	var pc *PointConfig
	for i := range d.cfg.Points {
		if d.cfg.Points[i].Name == point {
			pc = &d.cfg.Points[i]
			break
		}
	}
	if pc == nil {
		res.Err = ErrPointNotFound
		res.Finished = time.Now()
		return res
	}
	if !pc.Writable() {
		res.Err = ErrPointReadOnly
		res.Finished = time.Now()
		return res
	}
	if math.IsNaN(value) || value < pc.Min || value > pc.Max {
		res.Err = ErrOutOfRange
		res.Finished = time.Now()
		return res
	}

	// 2. 幂等检查（并发安全）：同一指令重复下发，返回首次结果，不重复写。
	// 若同一 commandID 正在执行中，等待其完成后再判定，
	// 否则多个并发重复指令可能同时通过检查造成重复写入。
	for {
		d.mu.Lock()
		if prev, ok := d.appliedCmds[commandID]; ok {
			d.mu.Unlock()
			prev.Duplicate = true
			prev.Applied = false
			log.Printf("[device %s] write cmd=%s duplicate -> idempotent hit", d.cfg.ID, commandID)
			return prev
		}
		if done, ok := d.inFlight[commandID]; ok {
			d.mu.Unlock()
			<-done
			continue
		}
		d.inFlight[commandID] = make(chan struct{})
		d.mu.Unlock()
		break
	}
	defer func() {
		d.mu.Lock()
		if done, ok := d.inFlight[commandID]; ok {
			delete(d.inFlight, commandID)
			close(done)
		}
		d.mu.Unlock()
	}()

	// 3. 掉线安全状态：明确拒绝，不排队、不猜测。
	if d.isOffline() {
		res.Err = ErrDeviceOffline
		res.Finished = time.Now()
		return res
	}

	// 4. 确认 + 重试。设备拒绝是确定性错误，不重试；超时/链路抖动可重试。
	maxAttempts := 1 + d.cfg.WriteRetries
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		res.Attempts = attempt
		wctx, cancel := context.WithTimeout(ctx, d.cfg.WriteTimeout)
		d.ioMu.Lock()
		err := d.slave.Write(wctx, pc.Address, value)
		d.ioMu.Unlock()
		cancel()

		if err == nil {
			seq := d.nextSeq()
			d.cache.Put(PointValue{
				Device: d.cfg.ID, Point: point, Value: value,
				Quality: QualityGood, Timestamp: time.Now(), Seq: seq,
			})
			d.recordSuccess()
			res.Applied = true
			res.Err = nil // 清除前几次尝试留下的错误
			res.Finished = time.Now()
			d.mu.Lock()
			d.appliedCmds[commandID] = res
			d.mu.Unlock()
			log.Printf("[device %s] write cmd=%s %s=%.3f applied (attempt %d)",
				d.cfg.ID, commandID, point, value, attempt)
			return res
		}

		classified := classifyIOErr(err)
		res.Err = classified
		d.recordFailure(err)
		log.Printf("[device %s] write cmd=%s attempt %d failed: %v",
			d.cfg.ID, commandID, attempt, classified)

		if errors.Is(classified, ErrRejectedBySlave) || errors.Is(classified, ErrDeviceOffline) {
			break // 确定性失败，重试无意义
		}
		// 超时类失败：短暂退避后重试。
		select {
		case <-ctx.Done():
			res.Err = ErrWriteTimeout
			res.Finished = time.Now()
			return res
		case <-time.After(time.Duration(attempt) * 10 * time.Millisecond):
		}
	}
	res.Finished = time.Now()
	return res
}
