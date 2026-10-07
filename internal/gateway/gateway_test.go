package gateway

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/feng111627/plc-io-gateway-go/internal/config"
	"github.com/feng111627/plc-io-gateway-go/internal/control"
	"github.com/feng111627/plc-io-gateway-go/internal/model"
	"github.com/feng111627/plc-io-gateway-go/internal/sim"
)

func testSetup(t *testing.T) (*config.Config, map[model.DeviceID]model.Device, *Gateway) {
	t.Helper()
	cfg := config.Default()
	devs := map[model.DeviceID]model.Device{
		"line-a": sim.New("line-a", map[string]float64{"40001": 23.5, "40002": 1200}),
		"line-b": sim.New("line-b", map[string]float64{"40010": 3.2, "00001": 1}),
	}
	gw := New(cfg, devs)
	return cfg, devs, gw
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("等待条件超时: %s", msg)
}

func simA(devs map[model.DeviceID]model.Device) *sim.Device { return devs["line-a"].(*sim.Device) }
func simB(devs map[model.DeviceID]model.Device) *sim.Device { return devs["line-b"].(*sim.Device) }

func TestGateway(t *testing.T) {
	t.Run("业务/网关集成-初始采集得到全 good 视图", func(t *testing.T) {
		t.Log("用例: ReadOnce 后四个点位均 good 且值等于模拟寄存器")
		_, _, gw := testSetup(t)
		defer gw.Close()
		snap, status := gw.ReadOnce(context.Background())
		for _, s := range status {
			if s.Err != nil {
				t.Fatalf("设备 %s 初始读取失败: %v", s.Device, s.Err)
			}
		}
		for _, id := range []model.PointID{"a.temp", "a.speed", "b.pressure", "b.motor"} {
			if snap.Values[id].Quality != model.QualityGood {
				t.Fatalf("%s 初始应为 good, 得到 %s", id, snap.Values[id].Quality)
			}
		}
	})

	t.Run("业务/掉线恢复-掉线进入安全状态保留最后可信值恢复后重同步", func(t *testing.T) {
		t.Log("用例: line-b 掉线后其点位 uncertain 且保留旧值; 改寄存器并恢复后值刷新为新值且 good")
		_, devs, gw := testSetup(t)
		defer gw.Close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go gw.Run(ctx)

		waitFor(t, func() bool { return gw.View().Values["b.pressure"].Quality == model.QualityGood },
			time.Second, "初始采集")

		// 掉线
		simB(devs).SetOnline(false)
		gw.SetDeviceOnline("line-b", false)
		waitFor(t, func() bool {
			st := gw.Status()
			for _, s := range st {
				if s.Device == "line-b" {
					return s.State == model.StateOffline
				}
			}
			return false
		}, time.Second, "line-b 掉线状态")
		v := gw.View().Values["b.pressure"]
		if v.Quality != model.QualityUncertain || v.Value != 3.2 {
			t.Fatalf("掉线应 uncertain 且保留 3.2, 得到 %s=%v", v.Quality, v.Value)
		}

		// 掉线期间写被离线原因拒绝
		r := gw.Write(ctx, control.Command{ID: "w-off", Point: "b.pressure", Value: 5})
		if !errors.Is(r.Err, model.ErrDeviceOffline) {
			t.Fatalf("掉线期写应被 ErrDeviceOffline 拒绝, 得到 %v", r.Err)
		}

		// 现场寄存器在掉线期间被改变，恢复后必须重新同步成新值
		simB(devs).SetRegister("40010", 7.5)
		simB(devs).SetOnline(true)
		gw.SetDeviceOnline("line-b", true)
		waitFor(t, func() bool {
			got := gw.View().Values["b.pressure"]
			return got.Quality == model.QualityGood && got.Value == 7.5
		}, time.Second, "恢复后重新同步为 7.5/good")
	})

	t.Run("业务/坏值不采信-设备瞬时返回坏值不污染统一视图", func(t *testing.T) {
		t.Log("用例: line-a 下一次读超时, line-b 仍 good; 恢复后 line-a 重新 good")
		_, devs, gw := testSetup(t)
		defer gw.Close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go gw.Run(ctx)
		waitFor(t, func() bool { return gw.View().Values["a.temp"].Quality == model.QualityGood },
			time.Second, "初始 good")

		simA(devs).FailNextReads(1)
		waitFor(t, func() bool {
			// a 点出现过瞬时失败, 但 b 始终 good, a 有缓存时为 uncertain（绝不会出现错误值）
			return gw.View().Values["b.pressure"].Quality == model.QualityGood
		}, time.Second, "b 不受 a 故障影响")
		waitFor(t, func() bool { return gw.View().Values["a.temp"].Quality == model.QualityGood },
			time.Second, "a 在故障恢复后重新 good")
	})

	t.Run("业务/写后读自洽-确认写入立刻反映到视图", func(t *testing.T) {
		t.Log("用例: 写 a.speed=2000 确认后, View 立即读到 2000/good, 且设备仅写一次")
		_, devs, gw := testSetup(t)
		defer gw.Close()
		ctx := context.Background()
		gw.ReadOnce(ctx)
		r := gw.Write(ctx, control.Command{ID: "w1", Point: "a.speed", Value: 2000})
		if r.Status != model.WriteConfirmed {
			t.Fatalf("写应确认, 得到 %s/%v", r.Status, r.Err)
		}
		v := gw.View().Values["a.speed"]
		if v.Quality != model.QualityGood || v.Value != 2000 {
			t.Fatalf("写后视图应为 good=2000, 得到 %s=%v", v.Quality, v.Value)
		}
		if simA(devs).WriteCount() != 1 {
			t.Fatalf("设备应仅被写一次, 实际 %d", simA(devs).WriteCount())
		}
		// 再次采集不应回退成旧值（寄存器已被设备更新）
		gw.ReadOnce(ctx)
		v2 := gw.View().Values["a.speed"]
		if v2.Value != 2000 {
			t.Fatalf("重新采集应读到设备新值 2000, 得到 %v", v2.Value)
		}
	})

	t.Run("业务/并发读写-多协程并发采集与写入无数据竞争", func(t *testing.T) {
		t.Log("用例: 多个 goroutine 并发 ReadOnce/View/Write/Status/掉线切换, race 检测器下不报错且状态自洽")
		_, devs, gw := testSetup(t)
		defer gw.Close()
		ctx, cancel := context.WithCancel(context.Background())
		go gw.Run(ctx)

		var wg sync.WaitGroup
		stop := make(chan struct{})
		// 并发采集者
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
						gw.ReadOnce(ctx)
					}
				}
			}()
		}
		// 并发读视图/状态
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
						_ = gw.View()
						_ = gw.Status()
					}
				}
			}()
		}
		// 并发写（用不同 ID 避免幂等合并）
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				gw.Write(ctx, control.Command{
					ID:    "par-w-" + itoa(i),
					Point: "a.speed",
					Value: float64(1000 + i),
				})
				time.Sleep(time.Millisecond)
			}
		}()
		// 并发掉线/恢复切换
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				simB(devs).SetOnline(false)
				gw.SetDeviceOnline("line-b", false)
				time.Sleep(3 * time.Millisecond)
				simB(devs).SetOnline(true)
				gw.SetDeviceOnline("line-b", true)
				time.Sleep(3 * time.Millisecond)
			}
		}()

		time.Sleep(120 * time.Millisecond)
		close(stop)
		cancel()
		wg.Wait()

		// 最终一致性：b 恢复在线且值 good；a 速度值在合法范围
		waitFor(t, func() bool {
			pb := gw.View().Values["b.pressure"]
			pa := gw.View().Values["a.speed"]
			return pb.Quality == model.QualityGood && pa.Quality == model.QualityGood
		}, time.Second, "并发结束后视图恢复一致 good")
	})
}

// itoa 避免在测试里再引入 strconv 的小工具。
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
