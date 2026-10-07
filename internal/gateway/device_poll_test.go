package gateway

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// testConfig 生成测试用设备配置（短周期、短超时）。
func testConfig(id string, points ...PointConfig) DeviceConfig {
	return DeviceConfig{
		ID:            id,
		PollInterval:  20 * time.Millisecond,
		ReadTimeout:   50 * time.Millisecond,
		WriteTimeout:  50 * time.Millisecond,
		WriteRetries:  2,
		FailThreshold: 3,
		StaleAfter:    200 * time.Millisecond,
		Points:        points,
	}
}

func tempPoint(name string, addr uint16) PointConfig {
	return PointConfig{Name: name, Address: addr, Type: PointTypeHoldingRegister, Min: 0, Max: 100}
}

// TestConcurrentPollingIsolation 业务：并发采集。
// 场景：3 台设备并发采集，其中 1 台响应变慢（超过读超时），
// 断言慢设备的故障不影响其它设备采集到有效值。
func TestConcurrentPollingIsolation(t *testing.T) {
	t.Log("业务: 并发采集 | 用例: 慢设备不拖垮其它设备")
	g := NewGateway()

	normal1 := NewSimSlave(map[uint16]float64{1: 42})
	normal2 := NewSimSlave(map[uint16]float64{1: 7})
	slow := NewSimSlave(map[uint16]float64{1: 99})
	slow.SetLatency(10 * time.Second) // 远超读超时

	g.AddDevice(testConfig("normal-1", tempPoint("temp", 1)), normal1)
	g.AddDevice(testConfig("normal-2", tempPoint("temp", 1)), normal2)
	g.AddDevice(testConfig("slow-1", tempPoint("temp", 1)), slow)

	// 并发触发多轮采集（直接调用 pollOnce，等价于采集协程的行为）。
	var wg sync.WaitGroup
	for round := 0; round < 5; round++ {
		for _, id := range []string{"normal-1", "normal-2", "slow-1"} {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				g.mu.RLock()
				d := g.devices[id]
				g.mu.RUnlock()
				ctx, cancel := context.WithTimeout(context.Background(), d.pollBudget())
				d.pollOnce(ctx)
				cancel()
			}(id)
		}
	}
	wg.Wait()

	v1, ok := g.ReadPoint("normal-1", "temp")
	if !ok || !v1.Valid() || v1.Value != 42 {
		t.Fatalf("normal-1 应为有效值 42, 实际 %+v ok=%v", v1, ok)
	}
	v2, ok := g.ReadPoint("normal-2", "temp")
	if !ok || !v2.Valid() || v2.Value != 7 {
		t.Fatalf("normal-2 应为有效值 7, 实际 %+v ok=%v", v2, ok)
	}
	// 慢设备：所有读都超时，不应产生任何有效值。
	if v, ok := g.ReadPoint("slow-1", "temp"); ok && v.Valid() {
		t.Fatalf("slow-1 不应有有效值, 实际 %+v", v)
	}
	t.Log("结果: 正常设备值有效, 慢设备无有效值且未阻塞其它设备 —— 通过")
}

// TestBadValueNotReturnedAsValid 业务：坏数据处理。
// 场景：从站对某地址返回坏值（NaN），断言该点位质量为 Bad 且不被当作有效值。
func TestBadValueNotReturnedAsValid(t *testing.T) {
	t.Log("业务: 超时与坏数据 | 用例: 坏值标记为 Bad 且 Valid()=false")
	g := NewGateway()
	slave := NewSimSlave(map[uint16]float64{1: 50, 2: 60})
	slave.SetBadValues(map[uint16]bool{2: true})
	g.AddDevice(testConfig("dev", tempPoint("good", 1), tempPoint("bad", 2)), slave)

	g.mu.RLock()
	d := g.devices["dev"]
	g.mu.RUnlock()
	d.pollOnce(context.Background())

	good, _ := g.ReadPoint("dev", "good")
	if !good.Valid() || good.Value != 50 {
		t.Fatalf("good 点位应为有效值 50, 实际 %+v", good)
	}
	bad, ok := g.ReadPoint("dev", "bad")
	if !ok {
		t.Fatal("bad 点位应存在缓存记录")
	}
	if bad.Quality != QualityBad || bad.Valid() {
		t.Fatalf("bad 点位质量应为 bad 且无效, 实际 %+v", bad)
	}
	t.Logf("结果: good=%.1f(good), bad 点位 quality=%s valid=%v —— 通过",
		good.Value, bad.Quality, bad.Valid())
}

// TestStaleValueNotValid 业务：数据过期。
// 场景：点位值超过 StaleAfter 未刷新，断言读取时质量降级为 Stale。
func TestStaleValueNotValid(t *testing.T) {
	t.Log("业务: 超时与坏数据 | 用例: 过期值降级为 Stale 且 Valid()=false")
	g := NewGateway()
	slave := NewSimSlave(map[uint16]float64{1: 33})
	g.AddDevice(testConfig("dev", tempPoint("temp", 1)), slave)

	g.mu.RLock()
	d := g.devices["dev"]
	g.mu.RUnlock()
	d.pollOnce(context.Background())

	// 注入时钟：快进到新鲜期之后。
	base := time.Now()
	g.now = func() time.Time { return base.Add(10 * time.Second) }

	v, ok := g.ReadPoint("dev", "temp")
	if !ok {
		t.Fatal("点位应存在")
	}
	if v.Quality != QualityStale || v.Valid() {
		t.Fatalf("过期值应为 stale 且无效, 实际 %+v", v)
	}
	t.Logf("结果: 值 %.1f 保留但 quality=%s valid=%v —— 通过", v.Value, v.Quality, v.Valid())
}

// TestReadTimeoutClassified 业务：超时。
// 场景：从站延迟超过读超时，断言失败被计数并最终触发掉线。
func TestReadTimeoutClassified(t *testing.T) {
	t.Log("业务: 超时与坏数据 | 用例: 连续读超时达到阈值后设备进入 offline")
	g := NewGateway()
	slave := NewSimSlave(map[uint16]float64{1: 1})
	slave.SetLatency(5 * time.Second)
	cfg := testConfig("dev", tempPoint("temp", 1))
	g.AddDevice(cfg, slave)

	g.mu.RLock()
	d := g.devices["dev"]
	g.mu.RUnlock()
	for i := 0; i < cfg.FailThreshold; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), d.pollBudget())
		d.pollOnce(ctx)
		cancel()
	}
	if got := g.DeviceState("dev"); got != "offline" {
		t.Fatalf("连续 %d 次超时后应为 offline, 实际 %s", cfg.FailThreshold, got)
	}
	t.Logf("结果: %d 次连续超时后状态=%s —— 通过", cfg.FailThreshold, g.DeviceState("dev"))
}

// TestSnapshotAggregates 业务：统一值视图。
// 场景：多设备多点位汇总为统一视图，且包含各点位质量信息。
func TestSnapshotAggregates(t *testing.T) {
	t.Log("业务: 并发采集 | 用例: Snapshot 汇总多设备统一值视图")
	g := NewGateway()
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("dev-%d", i)
		slave := NewSimSlave(map[uint16]float64{1: float64(i * 10)})
		g.AddDevice(testConfig(id, tempPoint("temp", 1)), slave)
		g.mu.RLock()
		g.devices[id].pollOnce(context.Background())
		g.mu.RUnlock()
	}
	snap := g.Snapshot(context.Background())
	if len(snap) != 3 {
		t.Fatalf("视图应含 3 个点位, 实际 %d", len(snap))
	}
	for _, v := range snap {
		if !v.Valid() {
			t.Fatalf("所有点位应有效, 实际 %+v", v)
		}
		t.Logf("  视图项: %s/%s=%.1f quality=%s", v.Device, v.Point, v.Value, v.Quality)
	}
	t.Log("结果: 3 台设备全部有效汇总 —— 通过")
}
