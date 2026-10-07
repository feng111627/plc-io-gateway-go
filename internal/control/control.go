// Package control 负责下行写指令：参数校验、幂等去重、
// 可重试错误的确认式重试，以及按可区分原因拒绝。
package control

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/feng111627/plc-io-gateway-go/internal/model"
)

// DeviceWriter 控制器看到的设备写接口。
type DeviceWriter interface {
	ID() model.DeviceID
	Write(address string, value float64) error
}

// StateReader 允许控制器查询设备在线状态与最后可信值。
type StateReader interface {
	IsOnline(id model.DeviceID) bool
}

// CacheUpdater 写确认后更新值缓存，保证写命令与读数先后关系自洽。
type CacheUpdater interface {
	ApplyWrite(p model.PointConfig, value float64, at time.Time)
	LastGood(id model.PointID) (model.PointValue, bool)
}

// Command 一条写指令。ID 由调用方提供，用于幂等识别。
type Command struct {
	ID    string
	Point model.PointID
	Value float64
}

// inflight 一条指令的处理状态与终态。
type inflight struct {
	mu     sync.Mutex
	cond   chan struct{} // 完成后关闭，供并发重复请求等待同一份结果
	point  model.PointConfig
	status model.WriteStatus
	result model.WriteResult
}

// Controller 写控制器。
type Controller struct {
	mu         sync.Mutex
	points     map[model.PointID]model.PointConfig
	writers    map[model.DeviceID]DeviceWriter
	state      StateReader
	cache      CacheUpdater
	maxTries   int
	retryDelay time.Duration
	commands   map[string]*inflight
	now        func() time.Time
}

// New 创建写控制器。
func New(points []model.PointConfig, writers map[model.DeviceID]DeviceWriter,
	state StateReader, cache CacheUpdater,
	maxTries int, retryDelay time.Duration) *Controller {
	pm := make(map[model.PointID]model.PointConfig, len(points))
	for _, p := range points {
		pm[p.ID] = p
	}
	if maxTries < 1 {
		maxTries = 1
	}
	return &Controller{
		points:     pm,
		writers:    writers,
		state:      state,
		cache:      cache,
		maxTries:   maxTries,
		retryDelay: retryDelay,
		commands:   make(map[string]*inflight),
		now:        time.Now,
	}
}

// Execute 下发一条写指令。
//
// 语义：
//   - 相同 Command.ID 重复下发：只执行一次，并发重复者等待并拿到同一结果，绝不重复写入；
//   - 未知点位 / 只读 / 越界：立即以对应原因拒绝（不重试）；
//   - 设备离线：ErrDeviceOffline 拒绝（不重试）；
//   - 超时/瞬时错误：最多 maxTries 次；
//   - 设备拒绝 ErrRejected：立即结束（不重试）；
//   - 仅在设备确认成功后更新值缓存（写先于随后读视图）。
func (c *Controller) Execute(ctx context.Context, cmd Command) model.WriteResult {
	entry, isOwner := c.register(cmd)
	if !isOwner {
		// 重复指令：等待首次执行完成，返回同一份终态。
		select {
		case <-entry.cond:
		case <-ctx.Done():
			return model.WriteResult{CommandID: cmd.ID, Point: cmd.Point,
				Status: model.WriteFailed, Err: ctx.Err(), Ts: c.now()}
		}
		return entry.result
	}
	result := c.run(ctx, entry, cmd)
	entry.result = result
	entry.status = result.Status
	close(entry.cond)
	return result
}

// register 登记指令；owner=true 表示当前调用负责真正执行。
func (c *Controller) register(cmd Command) (*inflight, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.commands[cmd.ID]; ok {
		return e, false
	}
	e := &inflight{
		cond:   make(chan struct{}),
		status: model.WritePending,
		result: model.WriteResult{CommandID: cmd.ID, Point: cmd.Point, Status: model.WritePending},
	}
	c.commands[cmd.ID] = e
	return e, true
}

// run 在 owner 身份下执行前置校验与重试循环。
func (c *Controller) run(ctx context.Context, entry *inflight, cmd Command) model.WriteResult {
	p, ok := c.points[cmd.Point]
	if !ok {
		return c.fail(cmd, 0, model.ErrUnknownPoint)
	}
	entry.point = p
	if p.ReadOnly {
		return c.fail(cmd, 0, model.ErrReadOnly)
	}
	if err := validateValue(p, cmd.Value); err != nil {
		return c.fail(cmd, 0, err)
	}
	if c.state != nil && !c.state.IsOnline(p.Device) {
		return c.fail(cmd, 0, model.ErrDeviceOffline)
	}
	writer := c.writers[p.Device]
	if writer == nil {
		return c.fail(cmd, 0, model.ErrDeviceOffline)
	}

	var lastErr error
	for attempt := 1; attempt <= c.maxTries; attempt++ {
		if err := ctx.Err(); err != nil {
			return c.fail(cmd, attempt-1, err)
		}
		// 每次尝试前再确认一次在线状态（可能在重试间隙掉线）。
		if c.state != nil && !c.state.IsOnline(p.Device) {
			return c.fail(cmd, attempt-1, model.ErrDeviceOffline)
		}
		err := writer.Write(p.Address, encodeValue(p, cmd.Value))
		if err == nil {
			at := c.now()
			// 设备确认后才更新缓存：写与读的先后关系因此自洽。
			if c.cache != nil {
				c.cache.ApplyWrite(p, encodeValue(p, cmd.Value), at)
			}
			return model.WriteResult{
				CommandID: cmd.ID, Point: cmd.Point, Status: model.WriteConfirmed,
				Attempts: attempt, Ts: at,
			}
		}
		lastErr = classify(err)
		// 明确拒绝 / 离线：不可重试，立即结束。
		if errors.Is(lastErr, model.ErrRejected) ||
			errors.Is(lastErr, model.ErrDeviceOffline) {
			return c.fail(cmd, attempt, lastErr)
		}
		// 其余（超时等瞬时错误）：退避后重试，除非已达上限或 ctx 取消。
		if attempt == c.maxTries {
			break
		}
		select {
		case <-time.After(c.retryDelay):
		case <-ctx.Done():
			return c.fail(cmd, attempt, ctx.Err())
		}
	}
	return c.fail(cmd, c.maxTries, lastErr)
}

func (c *Controller) fail(cmd Command, attempts int, err error) model.WriteResult {
	return model.WriteResult{
		CommandID: cmd.ID, Point: cmd.Point, Status: model.WriteFailed,
		Attempts: attempts, Err: err, Ts: c.now(),
	}
}

// Result 查询某条指令的终态/进行态（幂等查询，不触发写入）。
func (c *Controller) Result(commandID string) (model.WriteResult, bool) {
	c.mu.Lock()
	e, ok := c.commands[commandID]
	c.mu.Unlock()
	if !ok {
		return model.WriteResult{}, false
	}
	return e.result, true
}

// validateValue 范围/类型校验。
func validateValue(p model.PointConfig, v float64) error {
	switch p.Type {
	case model.TypeBool:
		if v != 0 && v != 1 {
			return model.ErrOutOfRange
		}
	case model.TypeInt:
		if v != float64(int64(v)) {
			return model.ErrOutOfRange
		}
		if v < p.Min || v > p.Max {
			return model.ErrOutOfRange
		}
	case model.TypeFloat:
		if v < p.Min || v > p.Max {
			return model.ErrOutOfRange
		}
	default:
		return model.ErrUnknownPoint
	}
	return nil
}

// encodeValue 把指令数值编码为寄存器值；布尔用 0/1。
func encodeValue(p model.PointConfig, v float64) float64 {
	if p.Type == model.TypeBool {
		if v != 0 {
			return 1
		}
		return 0
	}
	return v
}

// classify 把底层驱动错误归一到可区分的领域原因。
func classify(err error) error {
	switch {
	case errors.Is(err, model.ErrTimeout):
		return model.ErrTimeout
	case errors.Is(err, model.ErrDeviceOffline):
		return model.ErrDeviceOffline
	case errors.Is(err, model.ErrRejected):
		return model.ErrRejected
	case errors.Is(err, model.ErrOutOfRange):
		return model.ErrOutOfRange
	default:
		return err
	}
}
