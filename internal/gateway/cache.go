package gateway

import (
	"sync"
	"time"
)

// valueCache 线程安全的点位值缓存，记录质量与新鲜度。
// 关键不变量：只有 Seq 更大的观测才能覆盖旧值，
// 因此“写确认后更新的值”不会被“写之前发出的慢读响应”覆盖。
type valueCache struct {
	mu     sync.RWMutex
	values map[string]PointValue // key: device/point
}

func newValueCache() *valueCache {
	return &valueCache{values: make(map[string]PointValue)}
}

func cacheKey(device, point string) string { return device + "/" + point }

// Put 写入一条观测值；仅当序号更新（Seq 更大）或尚无缓存时才覆盖。
// 返回是否真正更新了缓存。
func (c *valueCache) Put(v PointValue) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := cacheKey(v.Device, v.Point)
	if old, ok := c.values[k]; ok && old.Seq > v.Seq {
		return false // 旧值更新（如写确认后的值），拒绝被过期读覆盖
	}
	c.values[k] = v
	return true
}

// Get 读取缓存值；第二个返回值表示是否存在。
// 注意：不做新鲜度判断，新鲜度由读取方结合 StaleAfter 评估，
// 以便调用方能拿到“过期值 + 其年龄”用于展示而非误用。
func (c *valueCache) Get(device, point string) (PointValue, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.values[cacheKey(device, point)]
	return v, ok
}

// MarkOffline 将某设备全部点位标记为离线，但保留最后一次可信值与时间戳，
// 调用方据此可计算“最后可信值距今年龄”。
func (c *valueCache) MarkOffline(device string, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	prefix := device + "/"
	for k, v := range c.values {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix && v.Quality != QualityOffline {
			v.Quality = QualityOffline
			c.values[k] = v
		}
	}
}

// Snapshot 返回某设备全部点位缓存的副本。
func (c *valueCache) Snapshot(device string) []PointValue {
	c.mu.RLock()
	defer c.mu.RUnlock()
	prefix := device + "/"
	out := make([]PointValue, 0, len(c.values))
	for k, v := range c.values {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, v)
		}
	}
	return out
}
