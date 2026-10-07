package gateway

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// flakySlave 前 failTimes 次写超时，之后恢复正常（用于验证重试）。
type flakySlave struct {
	*SimSlave
	mu       sync.Mutex
	failLeft int
	writes   int // 实际成功写入次数
}

func newFlakySlave(initial map[uint16]float64, failTimes int) *flakySlave {
	return &flakySlave{SimSlave: NewSimSlave(initial), failLeft: failTimes}
}

func (f *flakySlave) Write(ctx context.Context, addr uint16, value float64) error {
	f.mu.Lock()
	if f.failLeft > 0 {
		f.failLeft--
		f.mu.Unlock()
		// 模拟写超时：挂起直到 ctx 截止。
		<-ctx.Done()
		return ctx.Err()
	}
	f.mu.Unlock()
	if err := f.SimSlave.Write(ctx, addr, value); err != nil {
		return err
	}
	f.mu.Lock()
	f.writes++
	f.mu.Unlock()
	return nil
}

func (f *flakySlave) writeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writes
}

// TestWriteRetryEventuallySucceeds 业务：写重试。
// 场景：从站前 2 次写超时，第 3 次成功；配置 2 次重试，断言最终成功且只写一次。
func TestWriteRetryEventuallySucceeds(t *testing.T) {
	t.Log("业务: 写重试与幂等 | 用例: 前2次超时第3次成功, 重试后 Applied=true 且仅写一次")
	g := NewGateway()
	slave := newFlakySlave(map[uint16]float64{1: 0}, 2)
	g.AddDevice(testConfig("dev", tempPoint("sp", 1)), slave)

	res := g.Write(context.Background(), "dev", "cmd-1", "sp", 55)
	if res.Err != nil || !res.Applied {
		t.Fatalf("写应最终成功, 实际 %+v", res)
	}
	if res.Attempts != 3 {
		t.Fatalf("应尝试 3 次, 实际 %d", res.Attempts)
	}
	if got := slave.Snapshot()[1]; got != 55 {
		t.Fatalf("从站值应为 55, 实际 %v", got)
	}
	if n := slave.writeCount(); n != 1 {
		t.Fatalf("从站应只被成功写入 1 次, 实际 %d", n)
	}
	t.Logf("结果: %s —— 通过", res)
}

// TestWriteIdempotentDuplicate 业务：写幂等。
// 场景：同一 commandID 重复下发（含并发重复），断言从站只被写入一次。
func TestWriteIdempotentDuplicate(t *testing.T) {
	t.Log("业务: 写重试与幂等 | 用例: 同一 commandID 并发重复下发, 从站只写一次")
	g := NewGateway()
	slave := newFlakySlave(map[uint16]float64{1: 0}, 0)
	g.AddDevice(testConfig("dev", tempPoint("sp", 1)), slave)

	const dup = 8
	results := make([]WriteResult, dup)
	var wg sync.WaitGroup
	for i := 0; i < dup; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = g.Write(context.Background(), "dev", "cmd-dup", "sp", 77)
		}(i)
	}
	wg.Wait()

	applied, duplicates := 0, 0
	for _, r := range results {
		if r.Err != nil {
			t.Fatalf("所有重复下发都应成功返回, 实际 %+v", r)
		}
		if r.Duplicate {
			duplicates++
		} else if r.Applied {
			applied++
		}
	}
	if applied != 1 || duplicates != dup-1 {
		t.Fatalf("应 1 次实际写入 + %d 次幂等命中, 实际 applied=%d dup=%d", dup-1, applied, duplicates)
	}
	if n := slave.writeCount(); n != 1 {
		t.Fatalf("从站应只被写入 1 次, 实际 %d", n)
	}
	t.Logf("结果: applied=%d duplicate=%d, 从站写入次数=%d —— 通过", applied, duplicates, slave.writeCount())
}

// TestWriteFailureReasons 业务：可区分的写失败原因。
// 场景：越界 / 只读点位 / 设备拒绝 / 设备离线，分别断言对应错误。
func TestWriteFailureReasons(t *testing.T) {
	t.Log("业务: 写重试与幂等 | 用例: 失败原因可区分(越界/只读/拒绝/离线)")
	g := NewGateway()
	slave := NewSimSlave(map[uint16]float64{1: 0, 2: 0})
	g.AddDevice(testConfig("dev",
		tempPoint("sp", 1),
		PointConfig{Name: "meas", Address: 2, Type: PointTypeInputRegister, Min: 0, Max: 100},
	), slave)
	ctx := context.Background()

	// 越界
	if r := g.Write(ctx, "dev", "c1", "sp", 500); !errors.Is(r.Err, ErrOutOfRange) {
		t.Fatalf("越界应返回 ErrOutOfRange, 实际 %v", r.Err)
	}
	// 只读
	if r := g.Write(ctx, "dev", "c2", "meas", 10); !errors.Is(r.Err, ErrPointReadOnly) {
		t.Fatalf("只读点位应返回 ErrPointReadOnly, 实际 %v", r.Err)
	}
	// 点位不存在
	if r := g.Write(ctx, "dev", "c3", "nope", 10); !errors.Is(r.Err, ErrPointNotFound) {
		t.Fatalf("不存在点位应返回 ErrPointNotFound, 实际 %v", r.Err)
	}
	// 设备拒绝（确定性，不应重试）
	slave.SetRejectWrites(true)
	r := g.Write(ctx, "dev", "c4", "sp", 10)
	if !errors.Is(r.Err, ErrRejectedBySlave) {
		t.Fatalf("设备拒绝应返回 ErrRejectedBySlave, 实际 %v", r.Err)
	}
	if r.Attempts != 1 {
		t.Fatalf("确定性拒绝不应重试, 实际尝试 %d 次", r.Attempts)
	}
	slave.SetRejectWrites(false)

	// 设备离线
	slave.SetOffline(true)
	if r := g.Write(ctx, "dev", "c5", "sp", 10); !errors.Is(r.Err, ErrDeviceOffline) {
		t.Fatalf("设备离线应返回 ErrDeviceOffline, 实际 %v", r.Err)
	}
	slave.SetOffline(false)
	t.Log("结果: out-of-range / read-only / not-found / rejected / offline 全部正确区分 —— 通过")
}

// TestWriteTimeoutExhaustsRetries 业务：写超时。
// 场景：从站持续不响应，断言重试耗尽后返回 ErrWriteTimeout。
func TestWriteTimeoutExhaustsRetries(t *testing.T) {
	t.Log("业务: 写重试与幂等 | 用例: 持续超时, 重试耗尽后返回 ErrWriteTimeout")
	g := NewGateway()
	slave := newFlakySlave(map[uint16]float64{1: 0}, 100) // 一直失败
	cfg := testConfig("dev", tempPoint("sp", 1))
	g.AddDevice(cfg, slave)

	res := g.Write(context.Background(), "dev", "cmd-t", "sp", 5)
	if !errors.Is(res.Err, ErrWriteTimeout) {
		t.Fatalf("应返回 ErrWriteTimeout, 实际 %v", res.Err)
	}
	if res.Attempts != 1+cfg.WriteRetries {
		t.Fatalf("应尝试 %d 次, 实际 %d", 1+cfg.WriteRetries, res.Attempts)
	}
	if res.Applied {
		t.Fatal("超时失败不应标记 Applied")
	}
	t.Logf("结果: %s —— 通过", res)
}

// TestWriteThenReadConsistency 业务：读写顺序自洽。
// 场景：写确认后立刻读，缓存值必须反映写入值（Seq 单调保证不被旧读覆盖）。
func TestWriteThenReadConsistency(t *testing.T) {
	t.Log("业务: 写重试与幂等 | 用例: 写确认后的读必须看到写入值")
	g := NewGateway()
	slave := NewSimSlave(map[uint16]float64{1: 1})
	g.AddDevice(testConfig("dev", tempPoint("sp", 1)), slave)

	g.mu.RLock()
	d := g.devices["dev"]
	g.mu.RUnlock()
	d.pollOnce(context.Background()) // 先采到旧值 1

	res := g.Write(context.Background(), "dev", "cmd-w", "sp", 88)
	if res.Err != nil {
		t.Fatalf("写失败: %v", res.Err)
	}
	v, ok := g.ReadPoint("dev", "sp")
	if !ok || !v.Valid() || v.Value != 88 {
		t.Fatalf("写后读应为有效值 88, 实际 %+v ok=%v", v, ok)
	}
	t.Logf("结果: 写后读值=%.1f quality=%s —— 通过", v.Value, v.Quality)
}

// TestConcurrentReadWriteNoRace 业务：并发安全。
// 场景：多协程并发采集+读写，配合 -race 运行验证无数据竞争。
func TestConcurrentReadWriteNoRace(t *testing.T) {
	t.Log("业务: 并发采集 | 用例: 多协程并发读写采集(-race 验证无竞争)")
	g := NewGateway()
	slave := NewSimSlave(map[uint16]float64{1: 10, 2: 20})
	g.AddDevice(testConfig("dev", tempPoint("a", 1), tempPoint("b", 2)), slave)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	time.AfterFunc(150*time.Millisecond, func() { close(stop) })
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				g.mu.RLock()
				d := g.devices["dev"]
				g.mu.RUnlock()
				d.pollOnce(context.Background())
				g.ReadPoint("dev", "a")
				g.Snapshot(context.Background())
				g.Write(context.Background(), "dev", "cmd-race", "a", 30)
			}
		}(i)
	}
	wg.Wait()
	t.Log("结果: 并发读写采集完成, 无竞争 —— 通过")
}
