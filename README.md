# plc-io-gateway-go

本地可运行的工业设备采集与控制网关。用内存模拟从站（`SimSlave`）替代真实 PLC，
演示多设备并发采集、下行写控制（确认/重试/幂等）、掉线降级与恢复，
并保证异常情况下结果一致且可解释。

## 结构

```
internal/gateway/
  types.go    点位配置、值与质量位、可区分错误、写结果
  slave.go    内存模拟从站（故障注入：延迟/掉线/坏值/拒绝写）
  cache.go    线程安全值缓存（Seq 单调，写确认不会被旧读覆盖）
  device.go   单设备：采集循环、online/offline 状态机、写重试与幂等
  gateway.go  多设备编排：统一值视图、新鲜度评估、统一写入口
cmd/gateway/  本地演示入口
```

## 点位配置方式

点位通过 `DeviceConfig.Points` 静态声明（见 `cmd/gateway/main.go` 示例）：

```go
gw.PointConfig{
    Name:    "temperature",              // 点位名，设备内唯一
    Address: 100,                        // 从站寄存器地址
    Type:    gw.PointTypeHoldingRegister,// holding_register / input_register / coil
    Min:     0, Max: 150,                // 可读值的合法范围 & 可写值的允许范围
}
```

设备级参数：`PollInterval`（采集周期）、`ReadTimeout`/`WriteTimeout`（单次 I/O 超时）、
`WriteRetries`（写重试次数）、`FailThreshold`（连续失败多少次判定掉线）、
`StaleAfter`（值的新鲜期）。

## 读写语义

### 读（采集 + 统一值视图）

- 每台设备独立协程周期采集；单台设备超时/变慢/坏值只影响自己，不拖垮其它设备。
- 每个观测值带 **质量位** 与 **序号**：
  - `good`：新鲜且经设备确认，`Valid()==true`，唯一可作为业务有效值的质量；
  - `bad`：设备返回坏值（NaN/越界/CRC 错），记录但绝不作为有效值；
  - `stale`：超过 `StaleAfter` 未刷新，读取时降级，不得当作有效值；
  - `offline`：设备掉线，保留最后一次可信值及其时间戳（可计算年龄），仅供参考。
- `Gateway.Snapshot()` 汇总所有设备点位为统一值视图；`Gateway.ReadPoint()` 读单点。
- **读写顺序自洽**：每台设备的读/写 I/O 由同一把锁串行化，且缓存只接受
  Seq 更大的观测——写确认后的值不会被“写之前发出的慢读响应”覆盖。

### 写（确认 + 重试 + 幂等）

`Gateway.Write(ctx, device, commandID, point, value)`：

- **幂等**：`commandID` 是幂等键。同一指令重复下发（包括并发重复）只写入一次，
  后续调用返回首次结果并标记 `Duplicate=true`。
- **确认与重试**：每次尝试有 `WriteTimeout` 超时；超时类失败按指数退避重试
  `WriteRetries` 次；设备拒绝等确定性失败不重试。
- **失败原因可区分**（`WriteResult.Err`）：
  - `ErrDeviceOffline`：设备离线（安全状态，直接拒绝，不排队）；
  - `ErrWriteTimeout`：重试耗尽仍超时；
  - `ErrOutOfRange`：值超出点位 `[Min, Max]`；
  - `ErrRejectedBySlave`：被设备拒绝（如联锁）；
  - `ErrPointNotFound` / `ErrPointReadOnly`：点位不存在 / 只读。

## 降级与安全回退规则

- 连续 `FailThreshold` 次 I/O 失败 → 设备进入 **offline 安全状态**：
  - 缓存中该设备所有点位标记为 `offline`，**保留最后一次可信值与时间戳**；
  - 所有写指令立即以 `ErrDeviceOffline` 拒绝，不排队、不猜测；
  - 采集循环继续运行，用于探测链路恢复。
- 链路恢复后第一轮成功采集 → 回到 online，**全量重新同步点位**；
  掉线期间的任何中间值都不会被当作正常结果。
- 坏值（`bad`）不计入掉线失败次数（链路是好的），但该值不作为有效值。

## 并发安全

所有共享状态（缓存、状态机、幂等表）均由互斥锁保护；`go test -race` 下
有专门的多协程并发读写用例验证无数据竞争。

## 本地验证

```bash
# 运行全部单元测试（竞态检测 + 详细日志，日志标注了每条用例覆盖的业务）
go test ./internal/gateway/ -race -v

# 运行本地演示（正常采集 → 下行写/幂等/越界 → 掉线降级 → 恢复重同步）
go run ./cmd/gateway
```

测试覆盖矩阵：

| 业务 | 用例 |
| --- | --- |
| 并发采集 | 慢设备隔离、Snapshot 汇总、Start/Stop 生命周期、多协程读写无竞争 |
| 超时与坏数据 | 坏值标记 Bad、过期降级 Stale、连续超时判定掉线 |
| 写重试与幂等 | 超时重试后成功、并发重复指令只写一次、失败原因分类、重试耗尽、写后读一致 |
| 掉线恢复 | 掉线安全状态保留最后可信值、恢复后重新同步、故障不影响其它设备 |
