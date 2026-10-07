// Package gateway 把采集器、写控制器、设备状态管理编排在一起，
// 负责掉线检测、安全降级、恢复重同步，并提供并发安全的统一入口。
package gateway

import (
	"context"
	"sync"
	"time"

	"github.com/feng111627/plc-io-gateway-go/internal/collector"
	"github.com/feng111627/plc-io-gateway-go/internal/config"
	"github.com/feng111627/plc-io-gateway-go/internal/control"
	"github.com/feng111627/plc-io-gateway-go/internal/model"
)

// DeviceStatus 对外暴露的设备状态与新鲜度。
type DeviceStatus struct {
	Device    model.DeviceID
	State     model.DeviceState
	SafeState model.SafeState
	LastSeen  time.Time
	LastError error
}

// Gateway 采集与控制网关。
type Gateway struct {
	mu         sync.RWMutex
	cfg        *config.Config
	devices    map[model.DeviceID]model.Device
	safeState  map[model.DeviceID]model.SafeState
	online     map[model.DeviceID]bool
	lastSeen   map[model.DeviceID]time.Time
	lastErr    map[model.DeviceID]error
	col        *collector.Collector
	ctrl       *control.Controller
	now        func() time.Time
}

// 编译期保证 Gateway 满足 control.StateReader。
var _ control.StateReader = (*Gateway)(nil)

// New 根据配置与一组设备创建网关。
func New(cfg *config.Config, devices map[model.DeviceID]model.Device) *Gateway {
	readers := make(map[model.DeviceID]collector.DeviceReader, len(devices))
	writers := make(map[model.DeviceID]control.DeviceWriter, len(devices))
	safe := make(map[model.DeviceID]model.SafeState, len(cfg.Devices))
	online := make(map[model.DeviceID]bool, len(cfg.Devices))
	lastSeen := make(map[model.DeviceID]time.Time, len(cfg.Devices))
	for _, dc := range cfg.Devices {
		id := dc.ID
		readers[id] = devices[id]
		writers[id] = devices[id]
		s := dc.SafeState
		if s == "" {
			s = model.SafeHold
		}
		safe[id] = s
		online[id] = dc.InitOnline
		if dc.InitOnline {
			lastSeen[id] = time.Now()
		}
	}
	col := collector.New(cfg.Points, readers,
		time.Duration(cfg.ReadTimeout)*time.Millisecond,
		time.Duration(cfg.MaxAge)*time.Millisecond)
	g := &Gateway{
		cfg:       cfg,
		devices:   devices,
		safeState: safe,
		online:    online,
		lastSeen:  lastSeen,
		lastErr:   make(map[model.DeviceID]error),
		col:       col,
		now:       time.Now,
	}
	g.ctrl = control.New(cfg.Points, writers, g, col,
		cfg.WriteMaxAttempts, time.Duration(cfg.WriteRetryDelay)*time.Millisecond)
	return g
}

// IsOnline 实现 control.StateReader。
func (g *Gateway) IsOnline(id model.DeviceID) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.online[id]
}

// Run 启动后台循环：周期性采集，依据采集结果维护设备在线/掉线状态，
// 触发安全降级与恢复重同步，直到 ctx 取消。
func (g *Gateway) Run(ctx context.Context) {
	interval := time.Duration(g.cfg.ReadTimeout) * time.Millisecond
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	ticker := time.NewTicker(interval / 2)
	defer ticker.Stop()
	for {
		g.pollOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// pollOnce 采集一次并依据“设备是否可达”更新在线状态。
//
//   - 读成功：若此前掉线则判定恢复，本次成功值已完成重同步；
//   - ErrDeviceOffline（明确掉线）：进入安全状态；
//   - 超时/坏值：视为瞬时异常，不改变在线状态（可能是响应变慢或坏点）。
func (g *Gateway) pollOnce(ctx context.Context) {
	_, statuses := g.ReadOnce(ctx)
	now := g.now()
	for _, st := range statuses {
		g.mu.Lock()
		if st.Err == nil {
			g.lastSeen[st.Device] = now
			g.lastErr[st.Device] = nil
			if !g.online[st.Device] {
				g.online[st.Device] = true
				// collector 的本次成功采集已经用设备新值重同步了缓存，
				// 因此无需额外动作，View 自此刻起可给出 good。
			}
		} else {
			g.lastErr[st.Device] = st.Err
			if g.online[st.Device] && isOfflineErr(st.Err) {
				g.online[st.Device] = false
				g.col.MarkDeviceOffline(st.Device)
			}
		}
		g.mu.Unlock()
	}
}

// ReadOnce 触发一次并发采集。
//
// 即使设备被标记为掉线也照常尝试读取：这样设备恢复后第一次成功读取
// 即可自动完成重新同步；读失败时采集器不会用错误值覆盖最后可信值。
func (g *Gateway) ReadOnce(ctx context.Context) (collector.Snapshot, []model.ReadStatus) {
	return g.col.Collect(ctx)
}

// View 返回当前缓存值视图（不触发设备 IO）。
func (g *Gateway) View() collector.Snapshot { return g.col.View() }

// Write 下发一条写指令。
func (g *Gateway) Write(ctx context.Context, cmd control.Command) model.WriteResult {
	return g.ctrl.Execute(ctx, cmd)
}

// SetDeviceOnline 供演示/测试注入设备掉线或恢复。
//
//   - 置为 false：立即进入安全状态（uncertain 保留最后可信值）；
//   - 置为 true：标记待重同步，在下一次成功采集前旧值不会被当作正常 good。
func (g *Gateway) SetDeviceOnline(id model.DeviceID, online bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	prev := g.online[id]
	if prev == online {
		return
	}
	g.online[id] = online
	if online {
		g.col.MarkDeviceOnline(id)
	} else {
		g.lastErr[id] = model.ErrDeviceOffline
		g.col.MarkDeviceOffline(id)
	}
}

// Status 返回各设备当前状态快照。
func (g *Gateway) Status() []DeviceStatus {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]DeviceStatus, 0, len(g.cfg.Devices))
	for _, dc := range g.cfg.Devices {
		id := dc.ID
		st := DeviceStatus{
			Device:    id,
			SafeState: g.safeState[id],
			LastSeen:  g.lastSeen[id],
			LastError: g.lastErr[id],
		}
		if g.online[id] {
			st.State = model.StateOnline
		} else {
			st.State = model.StateOffline
		}
		out = append(out, st)
	}
	return out
}

// Close 关闭网关与所有设备。
func (g *Gateway) Close() error {
	var firstErr error
	for _, d := range g.devices {
		if err := d.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// isOfflineErr 判断错误是否表示设备掉线（区别于普通超时/坏值）。
func isOfflineErr(err error) bool {
	return err == model.ErrDeviceOffline
}
