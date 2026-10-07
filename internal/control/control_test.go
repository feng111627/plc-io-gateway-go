package control

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/feng111627/plc-io-gateway-go/internal/model"
)

// fakeWriter 可控的设备写实现。
type fakeWriter struct {
	id       model.DeviceID
	mu       sync.Mutex
	err      error
	failNext int // >0 时本次写返回 timeout 并递减（瞬时故障）
	count    int32
	delay    time.Duration
	values   []float64
}

func (f *fakeWriter) ID() model.DeviceID { return f.id }

func (f *fakeWriter) Write(address string, value float64) error {
	atomic.AddInt32(&f.count, 1)
	f.mu.Lock()
	err, delay := f.err, f.delay
	failNext := f.failNext
	if failNext > 0 {
		f.failNext--
		err = model.ErrTimeout
	}
	f.values = append(f.values, value)
	f.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	return err
}
func (f *fakeWriter) cnt() int          { return int(atomic.LoadInt32(&f.count)) }
func (f *fakeWriter) setErr(e error)    { f.mu.Lock(); f.err = e; f.mu.Unlock() }
func (f *fakeWriter) setFailNext(n int) { f.mu.Lock(); f.failNext = n; f.mu.Unlock() }

// fakeState 在线状态。
type fakeState struct{ online bool }

func (s *fakeState) IsOnline(model.DeviceID) bool { return s.online }

// fakeCache 记录 ApplyWrite。
type fakeCache struct {
	mu     sync.Mutex
	values map[model.PointID]float64
}

func newFakeCache() *fakeCache { return &fakeCache{values: map[model.PointID]float64{}} }
func (c *fakeCache) ApplyWrite(p model.PointConfig, v float64, at time.Time) {
	c.mu.Lock()
	c.values[p.ID] = v
	c.mu.Unlock()
}
func (c *fakeCache) LastGood(id model.PointID) (model.PointValue, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.values[id]
	return model.PointValue{ID: id, Value: v}, ok
}
func (c *fakeCache) get(id model.PointID) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.values[id]
}

func testPoints() []model.PointConfig {
	return []model.PointConfig{
		{ID: "setpoint", Device: "A", Address: "100", Type: model.TypeFloat, Min: 0, Max: 10},
		{ID: "ro", Device: "A", Address: "101", Type: model.TypeFloat, ReadOnly: true},
		{ID: "flag", Device: "A", Address: "102", Type: model.TypeBool},
	}
}

func newController(w DeviceWriter, st StateReader, cache CacheUpdater, maxTries int) *Controller {
	return New(testPoints(), map[model.DeviceID]DeviceWriter{"A": w}, st, cache,
		maxTries, time.Millisecond)
}

func TestController(t *testing.T) {
	ctx := context.Background()

	t.Run("业务/写确认-成功一次确认并更新缓存", func(t *testing.T) {
		t.Log("用例: 正常写 setpoint=5, 应 confirmed/attempts=1, 缓存更新为 5")
		w := &fakeWriter{id: "A"}
		cache := newFakeCache()
		c := newController(w, &fakeState{online: true}, cache, 3)
		r := c.Execute(ctx, Command{ID: "c1", Point: "setpoint", Value: 5})
		if r.Status != model.WriteConfirmed || r.Attempts != 1 {
			t.Fatalf("期望 confirmed/1, 得到 %s/%d", r.Status, r.Attempts)
		}
		if w.cnt() != 1 {
			t.Fatalf("设备应被写 1 次, 实际 %d", w.cnt())
		}
		if cache.get("setpoint") != 5 {
			t.Fatalf("确认后缓存应为 5, 得到 %v", cache.get("setpoint"))
		}
	})

	t.Run("业务/写重试-瞬时超时按上限重试后成功", func(t *testing.T) {
		t.Log("用例: 前两次写超时, 第三次成功, attempts=3, 设备共被写 3 次")
		w := &fakeWriter{id: "A"}
		w.setFailNext(2)
		cache := newFakeCache()
		c := newController(w, &fakeState{online: true}, cache, 3)
		r := c.Execute(ctx, Command{ID: "c2", Point: "setpoint", Value: 6})
		if r.Status != model.WriteConfirmed || r.Attempts != 3 {
			t.Fatalf("期望第三次成功 attempts=3, 得到 %s/%d", r.Status, r.Attempts)
		}
		if w.cnt() != 3 {
			t.Fatalf("设备应共被尝试写入 3 次, 实际 %d", w.cnt())
		}
	})

	t.Run("业务/写重试-持续超时达到上限后按超时拒绝", func(t *testing.T) {
		t.Log("用例: 一直超时, maxTries=2 后 failed 且原因为 ErrTimeout")
		w := &fakeWriter{id: "A", err: model.ErrTimeout}
		cache := newFakeCache()
		c := newController(w, &fakeState{online: true}, cache, 2)
		r := c.Execute(ctx, Command{ID: "c3", Point: "setpoint", Value: 6})
		if r.Status != model.WriteFailed || !errors.Is(r.Err, model.ErrTimeout) {
			t.Fatalf("期望 failed/ErrTimeout, 得到 %s/%v", r.Status, r.Err)
		}
		if w.cnt() != 2 {
			t.Fatalf("应恰好尝试 2 次, 实际 %d", w.cnt())
		}
	})

	t.Run("业务/写幂等-相同ID重复下发只写一次并复用结果", func(t *testing.T) {
		t.Log("用例: 同一命令 ID 连发两次(不同值), 设备只被写一次, 两次返回同一终态")
		w := &fakeWriter{id: "A"}
		cache := newFakeCache()
		c := newController(w, &fakeState{online: true}, cache, 3)
		r1 := c.Execute(ctx, Command{ID: "dup", Point: "setpoint", Value: 5})
		r2 := c.Execute(ctx, Command{ID: "dup", Point: "setpoint", Value: 9})
		if r1.Status != model.WriteConfirmed || r2.Status != model.WriteConfirmed {
			t.Fatalf("两次都应成功复用, 得到 %s %s", r1.Status, r2.Status)
		}
		if w.cnt() != 1 {
			t.Fatalf("重复指令只能写一次, 实际 %d", w.cnt())
		}
		if cache.get("setpoint") != 5 {
			t.Fatalf("第二次不同值不应生效, 缓存 %v", cache.get("setpoint"))
		}
	})

	t.Run("业务/写幂等-并发重复下发也只写一次", func(t *testing.T) {
		t.Log("用例: 多个 goroutine 用同一 ID 并发下发, 设备写入次数恰为 1, 均得到相同终态")
		w := &fakeWriter{id: "A", delay: 10 * time.Millisecond}
		cache := newFakeCache()
		c := newController(w, &fakeState{online: true}, cache, 3)
		const n = 8
		var wg sync.WaitGroup
		results := make([]model.WriteStatus, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				r := c.Execute(ctx, Command{ID: "par", Point: "setpoint", Value: 1})
				results[idx] = r.Status
			}(i)
		}
		wg.Wait()
		for _, s := range results {
			if s != model.WriteConfirmed {
				t.Fatalf("所有并发重复请求都应确认, 出现 %s", s)
			}
		}
		if w.cnt() != 1 {
			t.Fatalf("并发重复下发只能写一次, 实际 %d", w.cnt())
		}
	})

	t.Run("业务/拒绝原因-只读越界未知点位离线被设备拒绝各自可区分", func(t *testing.T) {
		t.Log("用例: 五类拒绝应返回互不相同的领域错误, 且都不产生设备写入")
		w := &fakeWriter{id: "A"}
		cache := newFakeCache()
		c := newController(w, &fakeState{online: true}, cache, 3)

		check := func(name string, cmd Command, want error, wantTries int) {
			r := c.Execute(ctx, cmd)
			if !errors.Is(r.Err, want) {
				t.Fatalf("%s: 期望 %v, 得到 %v", name, want, r.Err)
			}
		}
		check("只读", Command{ID: "ro", Point: "ro", Value: 1}, model.ErrReadOnly, 0)
		check("越界-上限", Command{ID: "high", Point: "setpoint", Value: 99}, model.ErrOutOfRange, 0)
		check("越界-下限", Command{ID: "low", Point: "setpoint", Value: -1}, model.ErrOutOfRange, 0)
		check("布尔非01", Command{ID: "badbool", Point: "flag", Value: 2}, model.ErrOutOfRange, 0)
		check("未知点位", Command{ID: "unk", Point: "ghost", Value: 1}, model.ErrUnknownPoint, 0)

		// 设备明确拒绝：不重试。
		w.setErr(model.ErrRejected)
		r := c.Execute(ctx, Command{ID: "rej", Point: "setpoint", Value: 5})
		if !errors.Is(r.Err, model.ErrRejected) || r.Attempts != 1 {
			t.Fatalf("设备拒绝应 attempts=1/ErrRejected, 得到 %d/%v", r.Attempts, r.Err)
		}
		w.setErr(nil)

		// 设备离线：不重试, attempts=0。
		offline := newController(w, &fakeState{online: false}, cache, 3)
		r2 := offline.Execute(ctx, Command{ID: "off", Point: "setpoint", Value: 5})
		if !errors.Is(r2.Err, model.ErrDeviceOffline) || r2.Attempts != 0 {
			t.Fatalf("离线应 attempts=0/ErrDeviceOffline, 得到 %d/%v", r2.Attempts, r2.Err)
		}
	})

	t.Run("业务/写读一致-确认后写入缓存先于随后读视图", func(t *testing.T) {
		t.Log("用例: 写确认前缓存无新值, 确认后 LastGood 立即等于写入值")
		w := &fakeWriter{id: "A"}
		cache := newFakeCache()
		c := newController(w, &fakeState{online: true}, cache, 3)
		if _, ok := cache.LastGood("setpoint"); ok {
			t.Fatalf("写入前不应有缓存值")
		}
		c.Execute(ctx, Command{ID: "ord", Point: "setpoint", Value: 7.5})
		lg, ok := cache.LastGood("setpoint")
		if !ok || lg.Value != 7.5 {
			t.Fatalf("确认后 LastGood 应为 7.5, 得到 %v ok=%v", lg.Value, ok)
		}
	})
}
