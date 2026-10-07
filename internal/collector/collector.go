// Package collector 负责多设备点位的并发采集、地址去重合并、
// 超时/坏值隔离以及统一值视图（新鲜度感知）的维护。
package collector

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/feng111627/plc-io-gateway-go/internal/model"
)

// DeviceReader 采集器看到的设备读接口（model.Device 的子集）。
type DeviceReader interface {
	ID() model.DeviceID
	Read(addresses []string) (map[string]float64, error)
}

// Snapshot 某一时刻的统一值视图。
type Snapshot struct {
	Values map[model.PointID]model.PointValue
	ReadAt time.Time
}

// Good 返回质量为 good 的点位子视图。
func (s Snapshot) Good() map[model.PointID]model.PointValue {
	out := make(map[model.PointID]model.PointValue, len(s.Values))
	for id, v := range s.Values {
		if v.Quality == model.QualityGood {
			out[id] = v
		}
	}
	return out
}

type devicePoints struct {
	reader  DeviceReader
	points  []model.PointConfig
	address []string // 去重后的地址
}

// Collector 并发采集器。零值不可用，请用 New 构造。
type Collector struct {
	mu       sync.RWMutex
	groups   []devicePoints
	pointMap map[model.PointID]model.PointConfig
	cache    map[model.PointID]model.PointValue // 最后一次可信值
	offline  map[model.DeviceID]bool

	readTimeout time.Duration
	maxAge      time.Duration
	now         func() time.Time
}

// New 创建采集器。同一点位配置按设备分组，同设备内地址去重。
func New(points []model.PointConfig, readers map[model.DeviceID]DeviceReader,
	readTimeout, maxAge time.Duration) *Collector {
	c := &Collector{
		pointMap:    make(map[model.PointID]model.PointConfig, len(points)),
		cache:       make(map[model.PointID]model.PointValue),
		offline:     make(map[model.DeviceID]bool),
		readTimeout: readTimeout,
		maxAge:      maxAge,
		now:         time.Now,
	}
	groupIdx := make(map[model.DeviceID]int)
	for _, p := range points {
		c.pointMap[p.ID] = p
		idx, ok := groupIdx[p.Device]
		if !ok {
			c.groups = append(c.groups, devicePoints{reader: readers[p.Device]})
			idx = len(c.groups) - 1
			groupIdx[p.Device] = idx
		}
		g := &c.groups[idx]
		g.points = append(g.points, p)
		// 地址去重：同一物理地址在一次读里只读一次，结果复用给多个点位。
		dup := false
		for _, a := range g.address {
			if a == p.Address {
				dup = true
				break
			}
		}
		if !dup {
			g.address = append(g.address, p.Address)
		}
	}
	return c
}

// deviceResult 单个设备一次读取的内部结果。
type deviceResult struct {
	device model.DeviceID
	values map[string]float64
	err    error
}

// Collect 对所有设备并发采集一次。
//
// 保证：
//   - 各设备独立 goroutine + 独立超时，互不拖垮；
//   - 同一设备同地址只读一次，多点位共享结果（去重合并）；
//   - 设备超时/坏值/离线：该设备点位不产生新 good，也不覆盖最后可信值；
//   - 返回的快照对所有点位给出 good/uncertain/bad 的一致质量标注。
func (c *Collector) Collect(ctx context.Context) (Snapshot, []model.ReadStatus) {
	results := make([]deviceResult, len(c.groups))
	var wg sync.WaitGroup
	for i := range c.groups {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			g := c.groups[i]
			id := g.reader.ID()
			res := deviceResult{device: id}

			done := make(chan struct{})
			var vals map[string]float64
			var err error
			go func() {
				defer close(done)
				vals, err = g.reader.Read(append([]string(nil), g.address...))
			}()

			timer := time.NewTimer(c.readTimeout)
			defer timer.Stop()
			select {
			case <-done:
				res.values, res.err = vals, err
			case <-timer.C:
				res.err = model.ErrTimeout
			case <-ctx.Done():
				res.err = ctx.Err()
			}
			results[i] = res
		}(i)
	}
	wg.Wait()

	now := c.now()
	statuses := make([]model.ReadStatus, 0, len(results))
	for _, r := range results {
		statuses = append(statuses, model.ReadStatus{Device: r.device, Err: r.err})
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	snap := Snapshot{Values: make(map[model.PointID]model.PointValue), ReadAt: now}
	for i := range c.groups {
		g := c.groups[i]
		r := results[i]
		for _, p := range g.points {
			if r.err == nil {
				raw, ok := r.values[p.Address]
				if !ok {
					// 设备没返回该地址：按坏值处理，绝不采信。
					snap.Values[p.ID] = c.degraded(p, now, model.ErrBadValue)
					continue
				}
				pv := model.PointValue{
					ID:          p.ID,
					Device:      p.Device,
					Quality:     model.QualityGood,
					Ts:          now,
					CollectedAt: now,
				}
				if p.Type == model.TypeBool {
					pv.IsBool = true
					pv.BoolValue = raw != 0
				} else {
					pv.Value = raw
				}
				c.cache[p.ID] = pv
				snap.Values[p.ID] = pv
			} else {
				snap.Values[p.ID] = c.degraded(p, now, r.err)
			}
		}
	}
	return snap, statuses
}

// degraded 计算失败/降级时某点位的对外值。
// 调用方需持有 c.mu。
func (c *Collector) degraded(p model.PointConfig, now time.Time, cause error) model.PointValue {
	base := model.PointValue{
		ID:          p.ID,
		Device:      p.Device,
		CollectedAt: now,
	}
	if last, ok := c.cache[p.ID]; ok {
		// 保留最后一次可信值及其时间戳（新鲜度信息），降级为 uncertain。
		base.Value, base.BoolValue, base.IsBool = last.Value, last.BoolValue, last.IsBool
		base.Ts = last.Ts
		// 仅在标记离线或已超过新鲜窗口时降级；短暂瞬时失败但缓存尚新鲜时仍给 good？
		// 需求要求读取失败不能被当成有效值：一律 uncertain/bad，不给 good。
		base.Quality = model.QualityUncertain
		return base
	}
	base.Quality = model.QualityBad
	base.Ts = now
	return base
}

// View 返回当前缓存值视图（不触发 IO）。
//
//   - 设备离线：其点位若有最后可信值则 uncertain（保留 Ts），否则 bad；
//   - 在线但缓存值超过 maxAge：uncertain（过期不当有效）；
//   - 新鲜缓存：good；
//   - 从无成功值：bad。
func (c *Collector) View() Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := c.now()
	snap := Snapshot{Values: make(map[model.PointID]model.PointValue), ReadAt: now}
	for id, p := range c.pointMap {
		v, ok := c.cache[id]
		if !ok {
			snap.Values[id] = model.PointValue{
				ID: id, Device: p.Device, Quality: model.QualityBad, Ts: now, CollectedAt: now,
			}
			continue
		}
		v.CollectedAt = now
		switch {
		case c.offline[p.Device]:
			v.Quality = model.QualityUncertain
		case !v.Fresh(now, c.maxAge):
			v.Quality = model.QualityUncertain
		default:
			v.Quality = model.QualityGood
		}
		snap.Values[id] = v
	}
	return snap
}

// MarkDeviceOffline 通知某设备掉线，其点位进入安全降级。
func (c *Collector) MarkDeviceOffline(id model.DeviceID) {
	c.mu.Lock()
	c.offline[id] = true
	c.mu.Unlock()
}

// MarkDeviceOnline 通知某设备恢复。缓存中的旧值先标记为待重新同步：
// 它们不会立刻变成 good，必须等到下一次成功 Collect 后才重新可信。
func (c *Collector) MarkDeviceOnline(id model.DeviceID) {
	c.mu.Lock()
	c.offline[id] = false
	// 把该设备缓存值的时间戳置为零，使下一次成功采集前 View 判为过期(uncertain)，
	// 从而不会把掉线期间的状态当作正常结果。
	for pid, p := range c.pointMap {
		if p.Device == id {
			if v, ok := c.cache[pid]; ok {
				v.Ts = time.Time{}
				c.cache[pid] = v
			}
		}
	}
	c.mu.Unlock()
}

// IsOffline 报告设备是否被标记为掉线。
func (c *Collector) IsOffline(id model.DeviceID) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.offline[id]
}

// LastGood 返回某点位最后一次可信值与是否存在。
func (c *Collector) LastGood(id model.PointID) (model.PointValue, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.cache[id]
	return v, ok
}

// ApplyWrite 在写指令被设备确认后更新缓存，确保写命令与随后读数自洽。
func (c *Collector) ApplyWrite(p model.PointConfig, value float64, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pv := model.PointValue{
		ID:          p.ID,
		Device:      p.Device,
		Quality:     model.QualityGood,
		Ts:          at,
		CollectedAt: at,
	}
	if p.Type == model.TypeBool {
		pv.IsBool = true
		pv.BoolValue = value != 0
	} else {
		pv.Value = value
	}
	c.cache[p.ID] = pv
}

// IsTimeout 判断读取错误是否为超时类。
func IsTimeout(err error) bool {
	return errors.Is(err, model.ErrTimeout) || errors.Is(err, context.DeadlineExceeded)
}
