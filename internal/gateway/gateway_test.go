package gateway

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestOfflineDegradationKeepsLastGoodValue 业务：掉线降级。
// 场景：设备掉线后进入 offline 安全状态，缓存保留最后可信值及其时间戳，
// 且写指令被明确拒绝（ErrDeviceOffline）。
func TestOfflineDegradationKeepsLastGoodValue(t *testing.T) {
	t.Log("业务: 掉线恢复 | 用例: 掉线进入安全状态, 保留最后可信值, 写被拒绝")
	g := NewGateway()
	slave := NewSimSlave(map[uint16]float64{1: 66})
	cfg := testConfig("dev", tempPoint("temp", 1))
	g.AddDevice(cfg, slave)

	g.mu.RLock()
	d := g.devices["dev"]
	g.mu.RUnlock()
	d.pollOnce(context.Background())
	before, _ := g.ReadPoint("dev", "temp")
	if !before.Valid() || before.Value != 66 {
		t.Fatalf("掉线前应为有效值 66, 实际 %+v", before)
	}

	// 掉线：连续失败达到阈值。
	slave.SetOffline(true)
	for i := 0; i < cfg.FailThreshold; i++ {
		d.pollOnce(context.Background())
	}
	if got := g.DeviceState("dev"); got != "offline" {
		t.Fatalf("应为 offline, 实际 %s", got)
	}

	// 缓存：最后可信值仍在，但质量为 offline，携带最后可信时间戳。
	v, ok := g.ReadPoint("dev", "temp")
	if !ok {
		t.Fatal("掉线后缓存值应保留")
	}
	if v.Quality != QualityOffline || v.Valid() {
		t.Fatalf("掉线后质量应为 offline 且无效, 实际 %+v", v)
	}
	if v.Value != 66 {
		t.Fatalf("最后可信值应为 66, 实际 %v", v.Value)
	}
	age := time.Since(v.Timestamp)
	if age < 0 || age > 5*time.Second {
		t.Fatalf("最后可信时间戳应接近掉线前采集时刻, age=%v", age)
	}

	// 掉线期间写：明确拒绝。
	w := g.Write(context.Background(), "dev", "cmd-off", "temp", 10)
	if !errors.Is(w.Err, ErrDeviceOffline) || w.Applied {
		t.Fatalf("掉线写应返回 ErrDeviceOffline 且不写入, 实际 %+v", w)
	}
	t.Logf("结果: state=offline, 最后可信值=%.1f(age=%v), 写被拒绝 —— 通过", v.Value, age.Round(time.Millisecond))
}

// TestRecoveryResyncsPoints 业务：掉线恢复。
// 场景：掉线期间从站值被外部改变（脏值），恢复后必须重新同步为最新真实值，
// 不能把中间状态当作正常结果；恢复后写功能恢复。
func TestRecoveryResyncsPoints(t *testing.T) {
	t.Log("业务: 掉线恢复 | 用例: 恢复后重新同步点位, 丢弃中间错误值")
	g := NewGateway()
	slave := NewSimSlave(map[uint16]float64{1: 10, 2: 20})
	cfg := testConfig("dev", tempPoint("a", 1), tempPoint("b", 2))
	g.AddDevice(cfg, slave)

	g.mu.RLock()
	d := g.devices["dev"]
	g.mu.RUnlock()
	d.pollOnce(context.Background())

	// 掉线，期间从站寄存器被外部因素改变（网关不可见）。
	slave.SetOffline(true)
	for i := 0; i < cfg.FailThreshold; i++ {
		d.pollOnce(context.Background())
	}
	if g.DeviceState("dev") != "offline" {
		t.Fatal("应已进入 offline")
	}
	mid, _ := g.ReadPoint("dev", "a")
	if mid.Valid() {
		t.Fatalf("掉线期间的值不得作为有效值, 实际 %+v", mid)
	}

	// 恢复链路，从站值已变为 99/88。
	slave.SetOffline(false)
	slave.Write(context.Background(), 1, 99)
	slave.Write(context.Background(), 2, 88)

	// 恢复后第一轮采集：重新同步全部点位。
	d.pollOnce(context.Background())
	if got := g.DeviceState("dev"); got != "online" {
		t.Fatalf("恢复后应为 online, 实际 %s", got)
	}
	a, _ := g.ReadPoint("dev", "a")
	b, _ := g.ReadPoint("dev", "b")
	if !a.Valid() || a.Value != 99 || !b.Valid() || b.Value != 88 {
		t.Fatalf("恢复后应同步到最新值 99/88, 实际 a=%+v b=%+v", a, b)
	}

	// 恢复后写功能正常。
	w := g.Write(context.Background(), "dev", "cmd-after", "a", 50)
	if w.Err != nil || !w.Applied {
		t.Fatalf("恢复后写应成功, 实际 %+v", w)
	}
	t.Logf("结果: 恢复后 a=%.1f b=%.1f(重新同步), 写恢复正常 —— 通过", a.Value, b.Value)
}

// TestOfflineDoesNotAffectOtherDevices 业务：故障隔离。
// 场景：一台设备掉线，其它设备采集与写不受影响。
func TestOfflineDoesNotAffectOtherDevices(t *testing.T) {
	t.Log("业务: 掉线恢复 | 用例: 单设备掉线不影响其它设备")
	g := NewGateway()
	bad := NewSimSlave(map[uint16]float64{1: 1})
	good := NewSimSlave(map[uint16]float64{1: 2})
	cfgBad := testConfig("bad", tempPoint("t", 1))
	g.AddDevice(cfgBad, bad)
	g.AddDevice(testConfig("good", tempPoint("t", 1)), good)

	g.mu.RLock()
	dBad, dGood := g.devices["bad"], g.devices["good"]
	g.mu.RUnlock()

	bad.SetOffline(true)
	for i := 0; i < cfgBad.FailThreshold; i++ {
		dBad.pollOnce(context.Background())
		dGood.pollOnce(context.Background())
	}

	if g.DeviceState("bad") != "offline" || g.DeviceState("good") != "online" {
		t.Fatalf("状态应为 bad=offline good=online, 实际 %s/%s",
			g.DeviceState("bad"), g.DeviceState("good"))
	}
	v, _ := g.ReadPoint("good", "t")
	if !v.Valid() || v.Value != 2 {
		t.Fatalf("健康设备值应有效, 实际 %+v", v)
	}
	w := g.Write(context.Background(), "good", "cmd-g", "t", 42)
	if w.Err != nil {
		t.Fatalf("健康设备写应成功, 实际 %v", w.Err)
	}
	t.Log("结果: bad 掉线, good 采集与写均正常 —— 通过")
}

// TestStartStopPollingLoop 业务：采集循环生命周期。
// 场景：Start 后周期采集自动刷新缓存，Stop 后安全退出。
func TestStartStopPollingLoop(t *testing.T) {
	t.Log("业务: 并发采集 | 用例: Start 周期采集自动刷新, Stop 安全退出")
	g := NewGateway()
	slave := NewSimSlave(map[uint16]float64{1: 5})
	g.AddDevice(testConfig("dev", tempPoint("t", 1)), slave)
	g.Start()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if v, ok := g.ReadPoint("dev", "t"); ok && v.Valid() && v.Value == 5 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("周期采集未在预期时间内刷新缓存")
		}
		time.Sleep(5 * time.Millisecond)
	}
	g.Stop()
	g.Stop() // 重复 Stop 应安全
	t.Log("结果: 周期采集生效, Stop 幂等 —— 通过")
}
