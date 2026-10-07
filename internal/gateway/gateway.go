package gateway

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"
)

// Gateway 多设备编排入口：并发采集汇总 + 统一写入口。
type Gateway struct {
	mu      sync.RWMutex
	devices map[string]*Device
	cache   *valueCache
	now     func() time.Time // 可注入时钟，便于测试新鲜度
}

// NewGateway 创建网关。
func NewGateway() *Gateway {
	return &Gateway{
		devices: make(map[string]*Device),
		cache:   newValueCache(),
		now:     time.Now,
	}
}

// AddDevice 注册设备及其模拟从站。
func (g *Gateway) AddDevice(cfg DeviceConfig, slave Slave) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.devices[cfg.ID] = newDevice(cfg, slave, g.cache)
}

// Start 启动所有设备采集。
func (g *Gateway) Start() {
	g.mu.RLock()
	defer g.mu.RUnlock()
	for _, d := range g.devices {
		d.Start()
	}
}

// Stop 停止所有设备。
func (g *Gateway) Stop() {
	g.mu.RLock()
	defer g.mu.RUnlock()
	for _, d := range g.devices {
		d.Stop()
	}
}

// evaluate 在读取时刻评估新鲜度：Good 但超过 StaleAfter 未刷新 -> Stale。
// 过期值仍然返回（带时间戳），但 Valid() 为 false，调用方不得当作有效值。
func (g *Gateway) evaluate(v PointValue) PointValue {
	g.mu.RLock()
	d, ok := g.devices[v.Device]
	g.mu.RUnlock()
	if !ok || v.Quality != QualityGood {
		return v
	}
	if g.now().Sub(v.Timestamp) > d.cfg.StaleAfter {
		v.Quality = QualityStale
	}
	return v
}

// ReadPoint 读取单个点位当前值（含质量与新鲜度）。
func (g *Gateway) ReadPoint(device, point string) (PointValue, bool) {
	v, ok := g.cache.Get(device, point)
	if !ok {
		return PointValue{}, false
	}
	return g.evaluate(v), true
}

// Snapshot 并发汇总所有设备点位，形成统一值视图。
// 每台设备的汇总在独立协程中完成，单台设备的问题不会拖垮整体。
func (g *Gateway) Snapshot(ctx context.Context) []PointValue {
	g.mu.RLock()
	devs := make([]*Device, 0, len(g.devices))
	for _, d := range g.devices {
		devs = append(devs, d)
	}
	g.mu.RUnlock()

	var wg sync.WaitGroup
	out := make([]PointValue, 0, 64)
	for _, d := range devs {
		// 设备离线时也触发一次重同步尝试，加速恢复发现。
		if d.isOffline() {
			wg.Add(1)
			go func(d *Device) {
				defer wg.Done()
				rctx, cancel := context.WithTimeout(ctx, d.cfg.ReadTimeout)
				d.pollOnce(rctx)
				cancel()
			}(d)
		}
	}
	wg.Wait()

	for _, d := range devs {
		for _, v := range g.cache.Snapshot(d.cfg.ID) {
			out = append(out, g.evaluate(v))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Device != out[j].Device {
			return out[i].Device < out[j].Device
		}
		return out[i].Point < out[j].Point
	})
	return out
}

// Write 向指定设备点位下发写指令（确认+重试+幂等，见 Device.Write）。
func (g *Gateway) Write(ctx context.Context, device, commandID, point string, value float64) WriteResult {
	g.mu.RLock()
	d, ok := g.devices[device]
	g.mu.RUnlock()
	if !ok {
		return WriteResult{
			CommandID: commandID, Device: device, Point: point, Value: value,
			Err: ErrPointNotFound, Finished: time.Now(),
		}
	}
	res := d.Write(ctx, commandID, point, value)
	log.Printf("[gateway] %s", res)
	return res
}

// DeviceState 返回设备状态（"online"/"offline"/"" 不存在）。
func (g *Gateway) DeviceState(device string) string {
	g.mu.RLock()
	d, ok := g.devices[device]
	g.mu.RUnlock()
	if !ok {
		return ""
	}
	return d.State()
}
