# IM-learn 红包与消息链路实施交接

> 更新日期：2026-09-23。架构事实以 `redpacket-design.md` 为准，本文件只说明当前代码落点和接下来的开发顺序。

> 2026-09-23 最新变更：用户明确要求立即删除旧消息链路。代码已只保留 Outbox 路径，`MessagePipeline.Mode` 开关、Transfer 服务、旧事件和旧 Topic 配置已移除；尚未进行端到端联调。下文关于保持 `legacy` 和验收后再删除旧链路的段落保留为此前交接快照，不再作为当前操作要求。现有数据库需先执行 `sql/migrations/20260923_remove_legacy_seq.sql`。

## 当前结论

目标链路已经完成机械骨架，但尚未完成学习核心。默认配置继续使用：

```yaml
MessagePipeline:
  Mode: legacy
```

在消息事务和 Outbox Relay 验收前，不得把生产/联调配置切换为 `outbox`，也不得删除旧的 Accepted、Transfer、Sequenced、Offline Store 路径。

## AI 已完成

- `ChatMessageCreateRequestedEvent`、`ChatMessagePersistedEvent` 和统一 fingerprint；
- `senderID + clientMessageID` 已落库结果缓存与 `sessionID + seq` 消息内容缓存契约；
- 普通消息和红包消息共用的 `MessageWriteService` 外壳；
- Message Writer Consumer、Message Cache Consumer、持久化 Push Consumer 接线；
- Message、ConversationSequence、Outbox 三个事务 Model 接口；
- Outbox Relay 的配置、Store、Publisher 和 Worker 接口；
- Realtime/Offline 的 `legacy/outbox` 迁移开关；
- 离线同步读取新版缓存事件和 Seq 范围回源；
- 新 Topic、Consumer Group、SQL 字段和示例配置；
- 相关单元测试，当前 `go test ./...` 通过。

## 开发者需要亲自完成

### 已完成（2026-09-23）

1. `service/message/model/conversation_sequence/model.go` —— 幂等补建会话计数器、`SELECT ... FOR UPDATE`、事务内更新 `last_seq + 1`。
2. `service/message/model/message/model.go` —— 按 `sender_id + client_message_id` 查询，用同一个 `txctx` 插入消息。
3. `service/message/model/outbox_event/model.go` —— 用同一个 `txctx` 插入 `MESSAGE_PERSISTED`（`aggregate_id=sessionID`、`aggregate_seq=message.seq`、`message_key=sessionID`）。
4. `service/message/persistence/writer.go` —— 把上述操作放进一个 MySQL 本地事务；普通消息生成 `messageID`，红包消息复用预生成 ID；唯一键竞争用"回滚 + 新事务重查"处理，COMMIT 结果未知由调用方按 sender/client 键确认。

AI 另外补了 `common/txctx/txctx.go` 的 `SessionConn`（Model 层统一取事务连接，取不到直接报错）和 `redpacket-design.md` §9.5 离线同步时序图。

### 剩余

5. `service/outboxrelay/internal/relay/worker.go`
   - `FOR UPDATE SKIP LOCKED` 领取租约；
   - Broker ACK 后才能标记 `PUBLISHED`；
   - 失败退避重试；
   - 同一 `sessionID` 按 `aggregate_seq` 发布。
   - 注意：`service/outboxrelay/` 目前只有配置、接口和一个 TODO 的 Worker，还缺 `main.go`、`Store` 实现和 `Publisher` 实现。
6. 红包核心
   - `AllocateShareAmounts`；
   - 发送、领取、退款事务；
   - Redis Lua 候选份额；
   - CacheBuild 的版本校验与原子替换；
   - 权限与错误分类。

更细的进度、已确认的设计决策与坑、验收用例见 `progress.md`。

## 切换验收

核心代码完成后：

1. 新建干净 MySQL/Redis/Kafka 环境并执行全部 SQL；
2. 运行 `go test ./...` 和 `go build ./...`；
3. 将 Realtime、Offline 配置切换为 `MessagePipeline.Mode=outbox`；
4. 验证普通消息：重复发送、内容冲突、Redis 故障、COMMIT 未知、离线补齐；
5. 验证红包消息：资金先成功、消息延迟、Outbox 重试、Kafka 重投；
6. 验证同会话 Seq 严格递增，不同会话可并发；
7. 验证 Redis `FLUSHALL` 后消息和红包事实不丢失；
8. 验收通过后再删除旧链路及 `reserved_max_seq` 兼容字段。

## 失败边界

- 当前切换到 `outbox` 后，Redis 已落库幂等缓存命中可以返回原结果；缓存未命中会进入尚未实现的持久化 Writer，并明确返回错误，不会假成功。
- 红包 API 当前也不会形成资金事实；核心发送事务仍返回未实现错误。
- Outbox Relay 只有接口，没有运行中的 Worker；因此目标事件暂时不会自动发布。

## 2026-10-05 负载问题优化计划

已获用户授权，由 AI 实施并记录面试素材。本阶段改动为：

1. 新增 `common/mysqlpool`，统一校验和设置数据库连接池；7 个服务在创建 Model 之前应用额度。示例最大连接数合计 96，省略配置时各进程默认 16，合计 112；需在多实例扩容时重新计算总预算。
2. 新增 Relay `Run` 循环：有任务且整批处理成功则立即再领取，空批和错误等待配置间隔。保留领取 SQL、会话前序阻塞条件、处理租约、同步 Broker ACK 和状态回写。
3. 增加连接池等待/取消及轮询控制测试。压测使用隔离数据库和 Topic，对照轮与优化轮均开启阶段间排空；领取延迟剔除轮间屏障，采样实际数据库连接数，优化轮检查观察连接中按 Seq 的推送顺序。
4. 构建与测试完成后复测 10/50/100 并发消息发送、领取和离线同步，校验余额、流水、份额、Seq、Outbox 和 Kafka lag，记录观察结果及仍然存在的超时/热点行限制。

本阶段不通过延长请求超时解释高并发结果；如小改动后仍有查询瓶颈，先记录执行计划与结果，再决定后续 SQL 优化。

## 2026-10-06 Relay 实时延迟优化

用户已授权第一阶段：分段计时、验证前序检查索引、固定消息数复测。

1. Relay 可选记录 `claim`（查询、写租约、COMMIT）、`publish`（同步 Broker ACK）、`mark_published` 和 `reschedule` 的耗时与错误；默认不记录。
2. 在旧压测数据的独立副本上用实际领取 SQL 做 `EXPLAIN ANALYZE`，比较状态在 Seq 前/后的覆盖索引；通过后为新库 DDL 和旧库迁移提供 `idx_relay_predecessor`。
3. 在独立 MySQL 库验证前序行被锁住、发布失败待重试、过期租约不能回写；对照原索引与新索引。
4. 两轮各 6 组固定 600 条消息，10/50/100 并发，单会话及 20 会话；每组排空后才进入下一组，计时通过在线收到结束。两轮均开启相同日志、使用相同连接池与 Relay 调度，只改变索引。
5. 核查资金、消息数、Seq、观察连接推送顺序、Outbox 和 Kafka lag，保存原始数据与面试讲述。沿用单有效 Relay，不在本轮扩展批内同会话并行发布。

本阶段已完成：两个索引顺序的执行计划比较、真实 MySQL 故障边界测试和两轮各 15,960 请求对照均已记录。新增索引已迁移至本地现有库。热门会话推送改善，发送 ACK 和多会话高并发未全面改善；结论与限制见 `relay-performance-report.md`。没有继续扩展批内发布或多实例设计。

## 2026-10-06 会话连续领取与 ACK 批量确认

用户认可按连续 Seq 批量领取，并要求在保证消息准确性下扩大领取尺度。本轮实施：

1. 全局批次示例 500（配置上限 2000），同事务先锁每个会话最早未发布事件，MVCC读取已提交前缀后按明确ID使用NOWAIT加锁；不锁Writer尚未提交的尾部，已有中间记录锁冲突回滚，不跳过中间事件。未来退避或未过期租约截断前缀。一次 UPDATE 写入整批租约。
2. Kafka 仍按会话 Seq 串行等待 ACK。每 20 条 ACKed 事件在一个事务中确认 PUBLISHED；必须全数持有有效租约，部分有效时整个确认事务回滚。
3. 任一发布或确认错误停止本批。失败事件保留退避；未确认的已 ACK/未尝试记录按 owner/status/有效租约释放，已提交或已重排的记录不改。释放失败由持久租约到期恢复；取消后用独立 5 秒 context 尝试释放。
4. 单元测试模拟 ACK 未知、COMMIT 响应丢失、释放失败、取消与过期；真实 MySQL 验证中间锁冲突、未来重试、有效租约屏障和确认原子性。
5. 固定六组各 600 条消息对照上一轮，并增加 20 条低负载消息观察；核查资金、Seq、观察连接顺序、Outbox 和 Kafka lag。记录本轮包含批量领取、批量回写与批大小调整，不能将收益单独归因于某一项。仍要求单个有效 Relay。

本阶段代码和列出的测试已完成。复测中发现并修复WebSocket业务处理阻塞控制帧读取的问题，采用每连接16条有界业务队列、串行处理。最终复测15,960次请求有1次热门会话写入超时，资金、Seq、成功消息数量与顺序、最终Outbox及lag核查通过；不能标记100并发无错误容量验收。详细边界与中间失败轮见 [连续领取报告](relay-batch-report.md)。
