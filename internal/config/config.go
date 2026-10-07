// Package config 负责点位与设备配置的加载与校验。
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/feng111627/plc-io-gateway-go/internal/model"
)

// 默认调参与超时（毫秒）。
const (
	defaultReadTimeout     = 200
	defaultMaxAge          = 1500
	defaultWriteMaxAttempt = 3
	defaultWriteRetryDelay = 50
)

// DeviceConfig 一台设备的配置。
type DeviceConfig struct {
	ID         model.DeviceID `json:"id"`
	PollDelay  int            `json:"pollDelay"`  // 模拟从站响应时延（毫秒），0 表示立即
	ReadDelay  int            `json:"readDelay"`  // 读延迟（毫秒）
	WriteDelay int            `json:"writeDelay"` // 写延迟（毫秒）
	SafeState  model.SafeState `json:"safeState"` // 安全状态策略：hold / zero
	InitOnline bool           `json:"initOnline"` // 初始是否在线（模拟用）
}

// pointJSON 点位的 JSON 表示。
type pointJSON struct {
	ID       model.PointID  `json:"id"`
	Device   model.DeviceID `json:"device"`
	Address  string         `json:"address"`
	Type     model.DataType `json:"type"`
	ReadOnly bool           `json:"readOnly"`
	Min      *float64       `json:"min"`
	Max      *float64       `json:"max"`
}

type configJSON struct {
	ReadTimeout      int            `json:"readTimeout"`
	MaxAge           int            `json:"maxAge"`
	WriteMaxAttempts int            `json:"writeMaxAttempts"`
	WriteRetryDelay  int            `json:"writeRetryDelay"`
	Devices          []DeviceConfig `json:"devices"`
	Points           []pointJSON    `json:"points"`
}

// Config 网关整体配置。
type Config struct {
	Devices          []DeviceConfig
	Points           []model.PointConfig
	ReadTimeout      int // 单设备采集超时（毫秒）
	MaxAge           int // 值新鲜度窗口（毫秒）
	WriteMaxAttempts int // 写指令最大尝试次数（含首次）
	WriteRetryDelay  int // 每次写重试间隔（毫秒）
}

// Load 从 JSON 文件加载配置。
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var jc configJSON
	if err := json.Unmarshal(raw, &jc); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	cfg, err := fromJSON(&jc)
	if err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func fromJSON(jc *configJSON) (*Config, error) {
	cfg := &Config{
		ReadTimeout:      orDefault(jc.ReadTimeout, defaultReadTimeout),
		MaxAge:           orDefault(jc.MaxAge, defaultMaxAge),
		WriteMaxAttempts: orDefault(jc.WriteMaxAttempts, defaultWriteMaxAttempt),
		WriteRetryDelay:  orDefault(jc.WriteRetryDelay, defaultWriteRetryDelay),
		Devices:          jc.Devices,
	}
	for _, p := range jc.Points {
		pc := model.PointConfig{
			ID:       p.ID,
			Device:   p.Device,
			Address:  p.Address,
			Type:     p.Type,
			ReadOnly: p.ReadOnly,
		}
		if p.Type == model.TypeInt || p.Type == model.TypeFloat {
			if p.Min != nil {
				pc.Min = *p.Min
			}
			if p.Max != nil {
				pc.Max = *p.Max
			}
		}
		cfg.Points = append(cfg.Points, pc)
	}
	return cfg, nil
}

func orDefault(v, d int) int {
	if v <= 0 {
		return d
	}
	return v
}

// Default 返回一份用于本地演示/测试的内置配置（两台设备、四个点位）。
func Default() *Config {
	mkmin := func(v float64) *float64 { return &v }
	mkmax := func(v float64) *float64 { return &v }
	jc := &configJSON{
		ReadTimeout:      defaultReadTimeout,
		MaxAge:           defaultMaxAge,
		WriteMaxAttempts: defaultWriteMaxAttempt,
		WriteRetryDelay:  defaultWriteRetryDelay,
		Devices: []DeviceConfig{
			{ID: "line-a", ReadDelay: 5, SafeState: model.SafeHold, InitOnline: true},
			{ID: "line-b", ReadDelay: 8, SafeState: model.SafeHold, InitOnline: true},
		},
		Points: []pointJSON{
			{ID: "a.temp", Device: "line-a", Address: "40001", Type: model.TypeFloat, ReadOnly: true, Min: mkmin(-40), Max: mkmax(120)},
			{ID: "a.speed", Device: "line-a", Address: "40002", Type: model.TypeInt, Min: mkmin(0), Max: mkmax(3000)},
			{ID: "b.pressure", Device: "line-b", Address: "40010", Type: model.TypeFloat, Min: mkmin(0), Max: mkmax(10)},
			{ID: "b.motor", Device: "line-b", Address: "00001", Type: model.TypeBool, Min: mkmin(0), Max: mkmax(1)},
		},
	}
	cfg, _ := fromJSON(jc)
	return cfg
}

// Validate 校验配置的完整性与一致性。
func (c *Config) Validate() error {
	if len(c.Devices) == 0 {
		return errors.New("config: no devices")
	}
	if len(c.Points) == 0 {
		return errors.New("config: no points")
	}
	dev := make(map[model.DeviceID]bool, len(c.Devices))
	for _, d := range c.Devices {
		if d.ID == "" {
			return errors.New("config: device with empty id")
		}
		if dev[d.ID] {
			return fmt.Errorf("config: duplicate device %q", d.ID)
		}
		dev[d.ID] = true
		switch d.SafeState {
		case "", model.SafeHold, model.SafeZero:
		default:
			return fmt.Errorf("config: device %q unknown safe state %q", d.ID, d.SafeState)
		}
	}
	seen := make(map[model.PointID]bool, len(c.Points))
	for _, p := range c.Points {
		if p.ID == "" {
			return errors.New("config: point with empty id")
		}
		if seen[p.ID] {
			return fmt.Errorf("config: duplicate point %q", p.ID)
		}
		seen[p.ID] = true
		if !dev[p.Device] {
			return fmt.Errorf("config: point %q references unknown device %q", p.ID, p.Device)
		}
		switch p.Type {
		case model.TypeInt, model.TypeFloat, model.TypeBool:
		default:
			return fmt.Errorf("config: point %q unknown type %q", p.ID, p.Type)
		}
		if p.Type != model.TypeBool && p.Max < p.Min {
			return fmt.Errorf("config: point %q max(%v) < min(%v)", p.ID, p.Max, p.Min)
		}
	}
	return nil
}
