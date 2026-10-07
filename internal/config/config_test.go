package config

import (
	"testing"

	"github.com/feng111627/plc-io-gateway-go/internal/model"
)

func TestConfig(t *testing.T) {
	t.Run("业务/配置-默认配置合法且含读写与只读点位", func(t *testing.T) {
		t.Log("用例: Default() 通过校验, 含 2 台设备 4 个点位, a.temp 为只读")
		c := Default()
		if err := c.Validate(); err != nil {
			t.Fatalf("默认配置应合法: %v", err)
		}
		if len(c.Devices) != 2 || len(c.Points) != 4 {
			t.Fatalf("设备/点位数不符: %d/%d", len(c.Devices), len(c.Points))
		}
		var ro bool
		for _, p := range c.Points {
			if p.ID == "a.temp" {
				ro = p.ReadOnly
			}
		}
		if !ro {
			t.Fatalf("a.temp 应为只读点")
		}
	})

	t.Run("业务/配置-加载JSON并补默认参数", func(t *testing.T) {
		t.Log("用例: JSON 配置可加载, 省略的超时参数取默认值, 点范围被解析")
		js := `{
			"devices": [{"id": "d1", "safeState": "hold", "initOnline": true}],
			"points": [
				{"id": "p1", "device": "d1", "address": "1", "type": "int", "min": 0, "max": 10}
			]
		}`
		path := t.TempDir() + "/cfg.json"
		if err := writeFile(path, []byte(js)); err != nil {
			t.Fatal(err)
		}
		c, err := Load(path)
		if err != nil {
			t.Fatalf("加载失败: %v", err)
		}
		if c.ReadTimeout != defaultReadTimeout || c.WriteMaxAttempts != defaultWriteMaxAttempt {
			t.Fatalf("默认参数未补齐: %+v", c)
		}
		if c.Points[0].Min != 0 || c.Points[0].Max != 10 || c.Points[0].Type != model.TypeInt {
			t.Fatalf("点位解析错误: %+v", c.Points[0])
		}
	})

	t.Run("业务/配置-非法配置被拒绝", func(t *testing.T) {
		t.Log("用例: 重复点位/未知设备/未知类型/max<min 都应校验失败")
		bad := []*Config{
			{ // 点位引用不存在的设备
				Devices: []DeviceConfig{{ID: "d1", SafeState: model.SafeHold}},
				Points:  []model.PointConfig{{ID: "p", Device: "ghost", Type: model.TypeInt}},
			},
			{ // 重复点位
				Devices: []DeviceConfig{{ID: "d1", SafeState: model.SafeHold}},
				Points: []model.PointConfig{
					{ID: "p", Device: "d1", Type: model.TypeInt},
					{ID: "p", Device: "d1", Type: model.TypeInt},
				},
			},
			{ // max < min
				Devices: []DeviceConfig{{ID: "d1", SafeState: model.SafeHold}},
				Points:  []model.PointConfig{{ID: "p", Device: "d1", Type: model.TypeFloat, Min: 10, Max: 1}},
			},
			{ // 未知类型
				Devices: []DeviceConfig{{ID: "d1", SafeState: model.SafeHold}},
				Points:  []model.PointConfig{{ID: "p", Device: "d1", Type: "weird"}},
			},
		}
		for i, c := range bad {
			if err := c.Validate(); err == nil {
				t.Fatalf("非法配置 #%d 应被拒绝", i+1)
			}
		}
	})
}
