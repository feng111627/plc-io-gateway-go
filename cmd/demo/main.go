// Command demo 在本地用内存模拟从站演示：多设备并发采集、
// 下行写的确认/重试/幂等，以及设备掉线后的安全降级与恢复重同步。
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/feng111627/plc-io-gateway-go/internal/config"
	"github.com/feng111627/plc-io-gateway-go/internal/control"
	"github.com/feng111627/plc-io-gateway-go/internal/gateway"
	"github.com/feng111627/plc-io-gateway-go/internal/model"
	"github.com/feng111627/plc-io-gateway-go/internal/sim"
)

func main() {
	cfg := config.Default()

	devs := buildDevices(cfg)
	gw := gateway.New(cfg, devs)
	defer gw.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go gw.Run(ctx)
	time.Sleep(120 * time.Millisecond)

	fmt.Println("== 1. 初始统一值视图 ==")
	printView(gw)

	fmt.Println("\n== 2. 写控制：确认成功 ==")
	r := gw.Write(ctx, control.Command{ID: "cmd-1", Point: "a.speed", Value: 1500})
	fmt.Printf("cmd-1 set a.speed=1500 -> status=%s attempts=%d err=%v\n", r.Status, r.Attempts, r.Err)
	printView(gw)

	fmt.Println("\n== 3. 幂等：相同 ID 重复下发（设备只应被写一次） ==")
	r2 := gw.Write(ctx, control.Command{ID: "cmd-1", Point: "a.speed", Value: 999})
	fmt.Printf("重复 cmd-1(=999) -> status=%s attempts=%d err=%v（复用首次结果，不重复写）\n",
		r2.Status, r2.Attempts, r2.Err)

	fmt.Println("\n== 4. 可区分的拒绝原因 ==")
	cases := []control.Command{
		{ID: "cmd-ro", Point: "a.temp", Value: 10},     // 只读
		{ID: "cmd-range", Point: "a.speed", Value: 9999}, // 越界
		{ID: "cmd-unknown", Point: "nope", Value: 1},
	}
	for _, c := range cases {
		rr := gw.Write(ctx, c)
		fmt.Printf("%-12s point=%-10s -> %v\n", c.ID, c.Point, rr.Err)
	}

	fmt.Println("\n== 5. 瞬时故障下的重试 ==")
	mustSim(devs["line-a"]).FailNextWrites(2)
	r5 := gw.Write(ctx, control.Command{ID: "cmd-retry", Point: "a.speed", Value: 800})
	fmt.Printf("cmd-retry -> status=%s attempts=%d err=%v\n", r5.Status, r5.Attempts, r5.Err)

	fmt.Println("\n== 6. 设备掉线：安全降级，保留最后可信值 ==")
	mustSim(devs["line-b"]).SetOnline(false)
	gw.SetDeviceOnline("line-b", false)
	time.Sleep(150 * time.Millisecond)
	printView(gw)
	printStatus(gw)

	fmt.Println("\n== 7. 掉线期间写被明确拒绝（离线） ==")
	r7 := gw.Write(ctx, control.Command{ID: "cmd-offline", Point: "b.pressure", Value: 5})
	fmt.Printf("cmd-offline -> status=%s err=%v attempts=%d\n", r7.Status, r7.Err, r7.Attempts)

	fmt.Println("\n== 8. 恢复：重新同步后值重新可信 ==")
	mustSim(devs["line-b"]).SetRegister("40010", 7.5)
	mustSim(devs["line-b"]).SetOnline(true)
	gw.SetDeviceOnline("line-b", true)
	time.Sleep(150 * time.Millisecond)
	printView(gw)
	printStatus(gw)
}

func buildDevices(cfg *config.Config) map[model.DeviceID]model.Device {
	initial := map[string]map[string]float64{
		"line-a": {"40001": 23.5, "40002": 1200},
		"line-b": {"40010": 3.2, "00001": 1},
	}
	devs := make(map[model.DeviceID]model.Device, len(cfg.Devices))
	for _, dc := range cfg.Devices {
		devs[dc.ID] = sim.New(dc.ID, initial[string(dc.ID)])
	}
	return devs
}

func printView(gw *gateway.Gateway) {
	snap := gw.View()
	for _, p := range []model.PointID{"a.temp", "a.speed", "b.pressure", "b.motor"} {
		v := snap.Values[p]
		val := v.Value
		if v.IsBool {
			val = map[bool]float64{true: 1, false: 0}[v.BoolValue]
		}
		age := time.Since(v.Ts).Round(time.Millisecond)
		fmt.Printf("  %-11s quality=%-9s value=%-6v lastGoodAge=%-8v\n", p, v.Quality, val, age)
	}
}

func printStatus(gw *gateway.Gateway) {
	for _, s := range gw.Status() {
		fmt.Printf("  device=%-7s state=%-8s safe=%-5s lastErr=%v\n",
			s.Device, s.State, s.SafeState, s.LastError)
	}
}

func mustSim(d model.Device) *sim.Device {
	s, ok := d.(*sim.Device)
	if !ok {
		panic("demo requires sim.Device")
	}
	return s
}
