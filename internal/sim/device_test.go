package sim

import (
	"errors"
	"testing"
	"time"

	"github.com/feng111627/plc-io-gateway-go/internal/model"
)

func TestSimDevice(t *testing.T) {
	t.Run("业务/模拟从站-正常读取返回寄存器快照", func(t *testing.T) {
		t.Log("用例: 在线设备批量读取，应返回与寄存器一致的值且无错误")
		d := New("dev1", map[string]float64{"40001": 23.5, "40002": 100})
		got, err := d.Read([]string{"40001", "40002"})
		if err != nil {
			t.Fatalf("期望读取成功, 得到 %v", err)
		}
		if got["40001"] != 23.5 || got["40002"] != 100 {
			t.Fatalf("读取值不匹配: %v", got)
		}
	})

	t.Run("业务/模拟从站-掉线后读取与写入均报离线", func(t *testing.T) {
		t.Log("用例: SetOnline(false) 后 Read/Write 都应返回 ErrDeviceOffline")
		d := New("dev1", map[string]float64{"40001": 1})
		d.SetOnline(false)
		if _, err := d.Read([]string{"40001"}); !errors.Is(err, model.ErrDeviceOffline) {
			t.Fatalf("读离线设备应报 ErrDeviceOffline, 得到 %v", err)
		}
		if err := d.Write("40001", 2); !errors.Is(err, model.ErrDeviceOffline) {
			t.Fatalf("写离线设备应报 ErrDeviceOffline, 得到 %v", err)
		}
	})

	t.Run("业务/模拟从站-坏地址与未知地址返回坏值", func(t *testing.T) {
		t.Log("用例: badAddresses 中的地址或不存在的地址读取应返回 ErrBadValue")
		d := New("dev1", map[string]float64{"40001": 1})
		d.SetBadAddress("40001", true)
		if _, err := d.Read([]string{"40001"}); !errors.Is(err, model.ErrBadValue) {
			t.Fatalf("坏地址应报 ErrBadValue, 得到 %v", err)
		}
		if _, err := d.Read([]string{"99999"}); !errors.Is(err, model.ErrBadValue) {
			t.Fatalf("未知地址应报 ErrBadValue, 得到 %v", err)
		}
	})

	t.Run("业务/模拟从站-瞬时故障按次数恢复", func(t *testing.T) {
		t.Log("用例: FailNextWrites(2) 后前两次写超时, 第三次成功")
		d := New("dev1", map[string]float64{"40001": 0})
		d.FailNextWrites(2)
		for i := 1; i <= 2; i++ {
			if err := d.Write("40001", 1); !errors.Is(err, model.ErrTimeout) {
				t.Fatalf("第 %d 次写应超时, 得到 %v", i, err)
			}
		}
		if err := d.Write("40001", 5); err != nil {
			t.Fatalf("瞬时故障耗尽后写应成功, 得到 %v", err)
		}
		if d.WriteCount() != 1 {
			t.Fatalf("实际写入次数应为 1, 得到 %d", d.WriteCount())
		}
	})

	t.Run("业务/模拟从站-设备拒绝写入", func(t *testing.T) {
		t.Log("用例: SetRejectWrites(true) 后写应返回 ErrRejected 且不计数")
		d := New("dev1", map[string]float64{"40001": 0})
		d.SetRejectWrites(true)
		if err := d.Write("40001", 1); !errors.Is(err, model.ErrRejected) {
			t.Fatalf("应被设备拒绝, 得到 %v", err)
		}
		if d.WriteCount() != 0 {
			t.Fatalf("被拒绝的写不应计数, 得到 %d", d.WriteCount())
		}
	})

	t.Run("业务/模拟从站-读延迟被注入", func(t *testing.T) {
		t.Log("用例: 设置 30ms 读延迟后, Read 至少耗时 30ms")
		d := New("dev1", map[string]float64{"40001": 1})
		d.SetReadDelay(30 * time.Millisecond)
		start := time.Now()
		_, _ = d.Read([]string{"40001"})
		if time.Since(start) < 25*time.Millisecond {
			t.Fatalf("读延迟未生效, 耗时 %v", time.Since(start))
		}
	})
}
