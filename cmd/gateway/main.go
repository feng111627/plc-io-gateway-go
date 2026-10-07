// 本地演示：两台模拟 PLC 设备的采集、写控制、掉线降级与恢复。
package main

import (
	"context"
	"log"
	"time"

	gw "github.com/feng111627/plc-io-gateway-go/internal/gateway"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	g := gw.NewGateway()

	// 两台模拟设备：锅炉与传送线。
	boiler := gw.NewSimSlave(map[uint16]float64{100: 61.5, 101: 1})
	conveyor := gw.NewSimSlave(map[uint16]float64{200: 0, 201: 1200})

	g.AddDevice(gw.DeviceConfig{
		ID: "boiler-1", PollInterval: 200 * time.Millisecond,
		ReadTimeout: 100 * time.Millisecond, WriteTimeout: 100 * time.Millisecond,
		WriteRetries: 2, FailThreshold: 3, StaleAfter: 1 * time.Second,
		Points: []gw.PointConfig{
			{Name: "temperature", Address: 100, Type: gw.PointTypeHoldingRegister, Min: 0, Max: 150},
			{Name: "pump_on", Address: 101, Type: gw.PointTypeCoil, Min: 0, Max: 1},
		},
	}, boiler)
	g.AddDevice(gw.DeviceConfig{
		ID: "conveyor-1", PollInterval: 200 * time.Millisecond,
		ReadTimeout: 100 * time.Millisecond, WriteTimeout: 100 * time.Millisecond,
		WriteRetries: 2, FailThreshold: 3, StaleAfter: 1 * time.Second,
		Points: []gw.PointConfig{
			{Name: "running", Address: 200, Type: gw.PointTypeCoil, Min: 0, Max: 1},
			{Name: "speed_sp", Address: 201, Type: gw.PointTypeHoldingRegister, Min: 0, Max: 3000},
		},
	}, conveyor)

	g.Start()
	defer g.Stop()
	ctx := context.Background()

	printSnapshot := func(tag string) {
		log.Printf("=== %s ===", tag)
		for _, v := range g.Snapshot(ctx) {
			log.Printf("  %s/%s = %.2f quality=%s valid=%v ts=%s",
				v.Device, v.Point, v.Value, v.Quality, v.Valid(),
				v.Timestamp.Format("15:04:05.000"))
		}
	}

	time.Sleep(500 * time.Millisecond)
	printSnapshot("1. 正常采集")

	// 下行写：温度设定 + 幂等重复下发。
	log.Printf("=== 2. 下行写（含幂等重复） ===")
	r1 := g.Write(ctx, "boiler-1", "cmd-001", "temperature", 80.0)
	log.Printf("  %s", r1)
	r2 := g.Write(ctx, "boiler-1", "cmd-001", "temperature", 80.0) // 重复指令
	log.Printf("  %s", r2)
	r3 := g.Write(ctx, "boiler-1", "cmd-002", "temperature", 999) // 越界
	log.Printf("  %s", r3)

	// 掉线降级。
	log.Printf("=== 3. conveyor-1 掉线（进入安全状态） ===")
	conveyor.SetOffline(true)
	time.Sleep(1 * time.Second)
	log.Printf("  conveyor-1 state=%s", g.DeviceState("conveyor-1"))
	w := g.Write(ctx, "conveyor-1", "cmd-003", "running", 1)
	log.Printf("  掉线期间写: %s", w)
	printSnapshot("掉线期间视图（注意 quality 与最后可信时间戳）")

	// 恢复。
	log.Printf("=== 4. conveyor-1 恢复（重新同步） ===")
	conveyor.SetOffline(false)
	time.Sleep(600 * time.Millisecond)
	log.Printf("  conveyor-1 state=%s", g.DeviceState("conveyor-1"))
	printSnapshot("恢复后视图")

	log.Printf("演示结束")
}
