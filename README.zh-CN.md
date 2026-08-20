# Reliability Lab — MySQL CDC -> Flink -> Iceberg

这是一个单节点数据管道可靠性实验室。重点不是把组件连起来，而是主动制造故障，
再把最终 MySQL 源快照与 Iceberg 快照逐行对账，并检查事件 ID 集合是否一致。

## 当前已记录运行

`20260711T034018Z-local-mac`（证据提交 `7eab9c3`）是一次已记录的 Apple Silicon
macOS 运行。主机内存为 16 GiB，Docker Desktop 虚拟机报告 10 个 CPU 和约
7.65 GiB 内存。五类故障全部恢复，快照差异为零，事件 ID 审计一致。

| 故障类型 | 已记录结果 | 主要文件 |
| --- | --- | --- |
| 任务崩溃 | 通过 | [`eo_reconciliation-all.json`](docs/workstation-run/20260711T034018Z-local-mac/eo_reconciliation-all.json) |
| 检查点恢复 | 通过 | 同一 JSON 的 `results[1]` |
| JobManager 重启 | 通过 | 同一 JSON 的 `results[2]` |
| 保存点恢复 | 通过 | 同一 JSON 的 `results[3]` |
| sink 提交故障 | 通过 | 同一 JSON 的 `results[4]` |

完整命令、环境与清理记录见[运行摘要](docs/workstation-run/20260711T034018Z-local-mac/SUMMARY.md)。
这证明的是这一次已记录的运行，不代表所有硬件都兼容，也不代表任何环境都能一键复现。

## Path A / Path B 架构

![Path A 与 broker Path B 架构](showcase/media/phase-b1-path-a-b.svg)

- **Path A（保留）**：MySQL GTID/binlog → 内嵌 Debezium 的现有 Flink CDC job →
  Flink checkpoint → Iceberg v2 keyed upsert；既有 job 和故障证据未改动。
- **Path B（Phase B1）**：MySQL GTID/binlog → 单 worker Debezium Connect
  （at-least-once）→ Kafka 3.9.2 offsets → 独立 Flink Kafka-source job →
  同一 Iceberg v2 keyed upsert 模型。

固定 `--events 1000 --seed 17` 的真实远端运行中，两条路径最终快照摘要一致、逐行差异为
`0`；Path B 的分区 offset、completed Flink checkpoint 和 Iceberg snapshot ID 已一起记录在
[`showcase/results/broker_parity.json`](showcase/results/broker_parity.json)（run
`20260820T102311Z-5bbec087`）。这个结论只覆盖 B1 parity，不覆盖后续 broker 故障或 Avro
契约演练。

Phase B2 已把 Debezium key/value producer 和 Flink Path B consumer 切到 Schema
Registry 7.9.8 管理的 Avro；B1 JSON-wire 结果保持不可变。认证运行
`20260820T120104Z-7e40acd6` 中，Registry 对 `event_id long -> string` 变更实际返回 HTTP
`409`，value subject 仍为版本 `1`；旧 schema 流水线继续运行，checkpoint 从 `5` 前进到
`7`，Kafka lag 为 `0`，拒绝后的事件可见，最终 source/Iceberg 逐行差异为 `0`。证据见
[`schema_contract_drill.json`](showcase/results/schema_contract_drill.json)，契约边界见
[`docs/data-contracts.md`](docs/data-contracts.md)。

Phase B3 已在专用 CPU-only Linux VM 上完成五类 broker 故障认证：Kafka 中途重启后，
同一个 Flink job 从已提交 offset 恢复，最终分区 offset 为 `35/35/50`、lag 为 `0`、120 行
逐行差异为 `0`；强制重投产生 36 次可审计重复，changelog 从 36 行增至 72 行，而 keyed
current 表仍为 36 行；正确主键保持同分区 `0,1,2` 顺序，三个受控错误 key 则跨越三个分区，
审计在进入主 topic 前检出一次非单调转移；一条 poison record 被隔离到 DLQ，携带原始字节与
错误元数据，经注册 Avro schema ID `2` 修复重放后以 14 行、差异 `0` 收敛；offset 0 与指定
时间戳的两次全新重建都与原快照逐行一致，并得到同一摘要
`17ed71ec943ec3a57a8635ba90ab6e2bcae0a7a3db34c146adc6c8b36305487e`。证据分别见
[`broker_restart_drill.json`](showcase/results/broker_restart_drill.json)、
[`duplicate_redelivery_drill.json`](showcase/results/duplicate_redelivery_drill.json)、
[`ordering_miskey_drill.json`](showcase/results/ordering_miskey_drill.json)、
[`poison_dlq_drill.json`](showcase/results/poison_dlq_drill.json) 与
[`offset_replay_drill.json`](showcase/results/offset_replay_drill.json)，操作事件见
[`RUNBOOK.md`](RUNBOOK.md)。这些是单节点已记录运行的正确性结论，不是恢复时间、freshness
或吞吐 SLO；后者仍属于 Phase B4。

## 证据如何工作

- Iceberg v2 upsert 表包含 equality delete，因此正确性对账通过 Flink SQL batch 读取；
  pyiceberg 只用于文件、manifest 和 snapshot 等元数据。
- 每个可发布 JSON 都必须包含 `run_id`、`git_sha`、时间、技术栈版本、命令和日志引用。
- `RUNBOOK.md` 按故障记录触发方式、症状、恢复命令、验证结果和文件链接。

## 本地轻量检查

空间有限的笔记本不启动 Docker，只运行：

```bash
make local-verify
```

它会执行 harness 单元测试、lint/type check、Maven 验证和静态证据面板构建，
但不会现场重跑 MySQL/Flink/Iceberg 的完整故障链路。

## 远端重型复现

Mac 只负责轻量检查与证据提交；Docker 运行在专用 Linux VM。五类 B3 演练都使用同一接口，
每次都先做全新 guarded bring-up：

```bash
export P1_REMOTE_ROOT='exactly-once-workstation:/root/autodl-tmp/exactly-once-drills'
make sync-up P1_REMOTE_ROOT="$P1_REMOTE_ROOT"
make remote-broker-up P1_REMOTE_ROOT="$P1_REMOTE_ROOT" \
  ARGS="--phase failures --fresh"
make remote-broker-verify P1_REMOTE_ROOT="$P1_REMOTE_ROOT" \
  ARGS="--failure broker-restart"
make sync-down P1_REMOTE_ROOT="$P1_REMOTE_ROOT"
```

其余名称为 `duplicate-redelivery`、`mis-keying`、`poison-dlq`、`offset-replay`。远端包装器
会记录 load average 与 GPU 状态，并用 tmux 保证 SSH 断连不终止长任务。固定工具链为 Java
11、Maven 3.9、Python 3.11 和 Node 20。具体步骤见
[`docs/broker-remote-execution.md`](docs/broker-remote-execution.md)。

## 已记录画面

![证据面板](showcase/media/phase-1.4-dashboard.jpg)

可在公开的 [Portfolio Phase 2 Review](https://portfolio-site-gpt-review.vercel.app/engineering/p1-reliability-lab)
查看交互式回放。隔离的 Review 部署无需 Vercel 登录或查询参数密钥。

## 范围

- 已验证到 Phase 2.3，并完成 Phase B1 broker ingress parity、Phase B2 data
  contracts 与 Phase B3 broker failure drills。B1 以
  [`broker_parity.json`](showcase/results/broker_parity.json) 为边界；B2
  以 [`schema_contract_drill.json`](showcase/results/schema_contract_drill.json) 中的 Registry
  拒绝与旧 schema 连续流为边界；B3 只以本页链接的五份故障 JSON 为边界，不包含 Phase B4
  的恢复时间、freshness 与吞吐 SLO。
- StarRocks 尚未开始。
- 仅为单节点 Docker Compose，不是云端、多节点或 GPU 系统。
- GitHub Actions 只运行轻量检查；重型 Docker 集成由人工执行并保存可审计文件。
