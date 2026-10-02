
# trace-tail-sampler

基于 OTLP/HTTP 的调用链尾部采样后端（Go 1.27.1，OpenTelemetry Proto 1.5.0）。

## 构建与运行

```sh
go build -o trace-tail-sampler .
./trace-tail-sampler
```

仅监听本机回环地址（非 loopback 的 SAMPLER_ADDR 会拒绝启动）。

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
|---|---|---|
| SAMPLER_ADDR | 127.0.0.1:4318 | 监听地址（必须为本机） |
| SAMPLER_WAIT | 10s | 首个 Span 到达后的固定等待截止点 |
| SAMPLER_SLOW_THRESHOLD | 500ms | 慢调用阈值（maxEnd-minStart） |
| SAMPLER_RATIO | 0.1 | 稳定哈希采样比例 [0,1] |
| SAMPLER_MAX_TRACES | 1000 | 同时在途链路数上限 |
| SAMPLER_MAX_SPANS_PER_TRACE | 200 | 每链路 Span 数上限 |
| SAMPLER_DECISION_CACHE_TTL | 5m | 决定缓存期限（迟到 Span 沿用原决定） |
| SAMPLER_MAX_REQUEST_BYTES | 4194304 | 单请求大小上限 |
| SAMPLER_OUTPUT_FILE | kept_traces.jsonl | 保留链路输出文件 |

## HTTP 接口

- POST /v1/traces — OTLP/HTTP Protobuf（Content-Type: application/x-protobuf）。非法批次（ID 长度/全零、缺失开始时间、结束早于开始、超限、超容量）整体拒绝，不改变状态；容量不足返回 429，不驱逐在途链路。
- GET /policy — 查询当前策略（version、wait_duration、slow_threshold、sample_ratio）。
- PUT /policy — 整份替换策略，版本号递增；在途链路沿用首次接收时的策略版本。
- GET /status — 策略、计数器（接收/去重/保留/丢弃/迟到/拒绝）、输出失败次数与最近错误。

## 行为说明

- 跨请求按 traceId 汇聚，按 spanId 去重，重复上报保留首次内容。
- 首个 Span 到达即固定截止点，后续数据不延长，不因根 Span 提前决定。
- 截止时决策：任一 Span 为 ERROR → 保留（reason=error）；否则 maxEnd-minStart ≥ 慢阈值 → 保留（slow）；否则按 traceId 的 FNV-1a 稳定哈希与比例采样（sampled / dropped）。每条链路只决策一次。
- 决定缓存期内：迟到 Span 沿用原决定——保留链路追加未见 Span 并追加 JSONL（late_append=true），丢弃链路直接忽略，迟到错误不翻转决定。缓存过期后同 traceId 可重新成组。
- 保留记录以 JSONL 追加，含 Resource/Scope/Span 完整内容、决定原因与策略版本。

## 限制

- 决定与输出在采样器锁内同步执行，文件写入阻塞会影响接收吞吐。
- 进程退出时不冲刷未到期的在途链路。
- 决定缓存与在途状态均在内存中，重启即丢失。

## 演示

```sh
go build -o genpayload ./cmd/genpayload   # 生成测试用 protobuf 批次
./genpayload 1 1 ok fast > t1.pb          # traceByte spanByte ok|error slow|fast
curl -XPOST -H 'Content-Type: application/x-protobuf' --data-binary `t1.pb http://127.0.0.1:4318/v1/traces
curl http://127.0.0.1:4318/status
cat kept_traces.jsonl
```

## 测试

```sh
go test ./...
```

