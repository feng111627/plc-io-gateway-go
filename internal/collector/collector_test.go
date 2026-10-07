package collector

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/feng111627/plc-io-gateway-go/internal/model"
)

// fakeReader 是测试用的可控设备读实现。
type fakeReader struct {
	id      model.DeviceID
	mu      sync.Mutex
	vals    map[string]float64
	err     error
	delay   time.Duration
	reads   int
}

func (f *fakeReader) ID() model.DeviceID { return f.id }

func (f *fakeReader) Read(addresses []string) (map[string]float64, error) {
	f.mu.Lock()
	f.reads++
	err, vals, delay := f.err, f.vals, f.delay
	f.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	if err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(addresses))
	for _, a := range addresses {
		out[a] = vals[a]
	}
	return out, nil
}

func (f *fakeReader) set(err error, vals map[string]float64) {
	f.mu.Lock()
	f.err, f.vals = err, vals
	f.mu.Unlock()
}

func (f *fakeReader) readCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reads
}

func points() []model.PointConfig {
	return []model.PointConfig{
		{ID: "a.x", Device: "A", Address: "100", Type: model.TypeFloat},
		{ID: "a.y", Device: "A", Address: "100", Type: model.TypeFloat}, // 同地址：去重
		{ID: "b.x", Device: "B", Address: "200", Type: model.TypeFloat},
	}
}

func newCollectors(t *testing.T, ra, rb *fakeReader, timeout, maxAge time.Duration) *Collector {
	t.Helper()
	return New(points(), map[model.DeviceID]DeviceReader{"A": ra, "B": rb}, timeout, maxAge)
}

func TestCollector(t *testing.T) {
	ctx := context.Background()

	t.Run("业务/并发采集-多设备并发汇总成统一视图", func(t *testing.T) {
		t.Log("用例: 两台设备各自返回值, Collect 后三个点位质量均为 good 且值正确")
		ra := &fakeReader{id: "A", vals: map[string]float64{"100": 1.5}}
		rb := &fakeReader{id: "B", vals: map[string]float64{"200": 2.5}}
		c := newCollectors(t, ra, rb, 200*time.Millisecond, time.Second)
		snap, status := c.Collect(ctx)
		if len(status) != 2 {
			t.Fatalf("应有两台设备状态, 得到 %d", len(status))
		}
		for _, s := range status {
			if s.Err != nil {
				t.Fatalf("设备 %s 不应有错误: %v", s.Device, s.Err)
			}
		}
		if snap.Values["a.x"].Quality != model.QualityGood || snap.Values["a.x"].Value != 1.5 {
			t.Fatalf("a.x 异常: %+v", snap.Values["a.x"])
		}
		if snap.Values["b.x"].Value != 2.5 {
			t.Fatalf("b.x 异常: %+v", snap.Values["b.x"])
		}
	})

	t.Run("业务/并发采集-同地址多点位只读一次并合并", func(t *testing.T) {
		t.Log("用例: a.x 与 a.y 同址, 设备 A 只应被读一次, 两点位共享同一值")
		ra := &fakeReader{id: "A", vals: map[string]float64{"100": 9}}
		rb := &fakeReader{id: "B", vals: map[string]float64{"200": 1}}
		c := newCollectors(t, ra, rb, 200*time.Millisecond, time.Second)
		snap, _ := c.Collect(ctx)
		if ra.readCount() != 1 {
			t.Fatalf("设备 A 应只被读一次, 实际 %d", ra.readCount())
		}
		if snap.Values["a.x"].Value != 9 || snap.Values["a.y"].Value != 9 {
			t.Fatalf("同地址两个点位应共享值 9, 得到 %v %v",
				snap.Values["a.x"].Value, snap.Values["a.y"].Value)
		}
	})

	t.Run("业务/超时隔离-单设备超时不拖垮其它设备", func(t *testing.T) {
		t.Log("用例: 设备 A 响应 120ms 超过 50ms 超时, 设备 B 正常; A 为坏/降级, B 仍 good")
		ra := &fakeReader{id: "A", vals: map[string]float64{"100": 1}, delay: 120 * time.Millisecond}
		rb := &fakeReader{id: "B", vals: map[string]float64{"200": 2}}
		c := newCollectors(t, ra, rb, 50*time.Millisecond, time.Second)
		start := time.Now()
		snap, status := c.Collect(ctx)
		if time.Since(start) > 100*time.Millisecond {
			t.Fatalf("采集不应被慢设备拖到 120ms, 实际 %v", time.Since(start))
		}
		var aErr error
		for _, s := range status {
			if s.Device == "A" {
				aErr = s.Err
			}
		}
		if !errors.Is(aErr, model.ErrTimeout) {
			t.Fatalf("设备 A 应超时, 得到 %v", aErr)
		}
		if snap.Values["b.x"].Quality != model.QualityGood {
			t.Fatalf("设备 B 不应受 A 影响, 质量 %s", snap.Values["b.x"].Quality)
		}
		// a.x 从未成功过 -> bad（失败不能当有效值）
		if snap.Values["a.x"].Quality != model.QualityBad {
			t.Fatalf("a.x 首读失败应为 bad, 得到 %s", snap.Values["a.x"].Quality)
		}
	})

	t.Run("业务/坏数据隔离-坏值不覆盖最后可信值", func(t *testing.T) {
		t.Log("用例: 先成功读到 a.x=1.5, 之后设备 A 返回坏值, 新快照 a.x 降级为 uncertain 且保留 1.5")
		ra := &fakeReader{id: "A", vals: map[string]float64{"100": 1.5}}
		rb := &fakeReader{id: "B", vals: map[string]float64{"200": 2}}
		c := newCollectors(t, ra, rb, 200*time.Millisecond, time.Second)
		snap1, _ := c.Collect(ctx)
		if snap1.Values["a.x"].Value != 1.5 {
			t.Fatalf("首次读数不对")
		}
		ra.set(model.ErrBadValue, nil)
		snap2, _ := c.Collect(ctx)
		if snap2.Values["a.x"].Quality != model.QualityUncertain {
			t.Fatalf("坏值后应降级 uncertain, 得到 %s", snap2.Values["a.x"].Quality)
		}
		if snap2.Values["a.x"].Value != 1.5 {
			t.Fatalf("坏值不能覆盖最后可信值, 得到 %v", snap2.Values["a.x"].Value)
		}
	})

	t.Run("业务/新鲜度-过期缓存不被当作有效值", func(t *testing.T) {
		t.Log("用例: maxAge=40ms, 等待 60ms 后 View 应把该点标 uncertain")
		ra := &fakeReader{id: "A", vals: map[string]float64{"100": 1}}
		rb := &fakeReader{id: "B", vals: map[string]float64{"200": 2}}
		c := newCollectors(t, ra, rb, 200*time.Millisecond, 40*time.Millisecond)
		c.Collect(ctx)
		if v := c.View(); v.Values["a.x"].Quality != model.QualityGood {
			t.Fatalf("刚采集应为 good")
		}
		time.Sleep(60 * time.Millisecond)
		if v := c.View(); v.Values["a.x"].Quality != model.QualityUncertain {
			t.Fatalf("过期后应 uncertain, 得到 %s", v.Values["a.x"].Quality)
		}
	})

	t.Run("业务/掉线降级-离线标记保留最后可信值并在恢复后重同步", func(t *testing.T) {
		t.Log("用例: MarkOffline 后 a 点 uncertain 保留旧值; MarkOnline 后在成功采集前不确定, 采集后恢复 good")
		ra := &fakeReader{id: "A", vals: map[string]float64{"100": 3}}
		rb := &fakeReader{id: "B", vals: map[string]float64{"200": 4}}
		c := newCollectors(t, ra, rb, 200*time.Millisecond, time.Second)
		c.Collect(ctx)
		c.MarkDeviceOffline("A")
		v := c.View()
		if v.Values["a.x"].Quality != model.QualityUncertain || v.Values["a.x"].Value != 3 {
			t.Fatalf("掉线应 uncertain 且保留 3, 得到 %+v", v.Values["a.x"])
		}
		c.MarkDeviceOnline("A")
		// 恢复后但尚未重新采集：旧缓存 Ts 被清零, 判为过期, 不得立即 good
		if v2 := c.View(); v2.Values["a.x"].Quality == model.QualityGood {
			t.Fatalf("重同步完成前不应恢复 good")
		}
		ra.set(nil, map[string]float64{"100": 30})
		c.Collect(ctx)
		v3 := c.View()
		if v3.Values["a.x"].Quality != model.QualityGood || v3.Values["a.x"].Value != 30 {
			t.Fatalf("重同步后应为 good=30, 得到 %+v", v3.Values["a.x"])
		}
	})
}
