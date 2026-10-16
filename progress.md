# 消息与红包模块 · 进度交接

> 更新日期：2026-09-23
> 最新变更：用户要求立即删除旧消息链路，现已移除其代码、配置和新库的兼容字段；`go build ./...` 通过，未运行测试或联调。已有数据库仍需手工执行 `sql/migrations/20260923_remove_legacy_seq.sql`。下文的 legacy 切换说明是此前快照。
> 本文件是**源文件**（和 `AGENTS.md`、`redpacket-design.md` 一样，项目根目录有一份同名副本）。
> 本文件只是**进度快照**，不是设计文档。一切实现以 `redpacket-design.md` 为准（§9 消息主链、§9.5 离线同步、§20 评审决议）。
> 接手顺序：`AGENTS.md` → `redpacket-design.md` → `implementation-plan.md` → 本文件。

## 一、当前进度

### 已完成

| 位置 | 内容 |
|---|---|
| `common/txctx/txctx.go` | 新增 `SessionConn(ctx)` 与 `ErrMissingTransaction`：Model 层统一取事务连接，取不到直接报错，绝不回退连接池 |
| `service/message/model/conversation_sequence/model.go` | `EnsureTx`（upsert 补行）、`LockAndIncrementTx`（`FOR UPDATE` 签发 Seq） |
| `service/message/model/message/model.go` | `FindByIdempotencyTx`、`InsertTx`、`IsDuplicateKeyError`、行到 DTO 的映射 |
| `service/message/model/outbox_event/model.go` | `InsertPersistedTx`（写 `MESSAGE_PERSISTED`，status=0） |
| `service/message/persistence/writer.go` | `Persist` 全文（M6.3–M6.12），含幂等判定、唯一键冲突回滚重查、错误分类 |
| `redpacket-design.md` §9.5 | 离线同步（读路径）时序图 + 边界分析表。**这是新增内容，不是既有决议** |

验证状态：`go build ./...`、`go vet`、`go test ./...`（9 个包）全部通过。

> ⚠️ **这些 SQL 还没有在真实 MySQL 上执行过**（截至本文件更新时），只验证到编译层面。`ON DUPLICATE KEY UPDATE session_id = session_id` 的实际行为、`FOR UPDATE` 与唯一键冲突的表现、`sqlx.ErrNotFound` 的触发条件，都需要连库确认。

### 未完成：消息模块（3 处 TODO）

| 位置 | 内容 | 谁写 |
|---|---|---|
| `service/outboxrelay/internal/relay/worker.go:49` | `RunBatch` 轮询循环：租约领取、按会话顺序发布、Broker ACK 后才标 PUBLISHED、失败退避 | 用户主导，AI 陪写 |
| `service/realtime/internal/websocket/message.go:169` | 把 writer 的可重试错误和 COMMIT 未知映射成不同的协议响应 | AI |
| `service/realtime/internal/consumer/message_writer_consumer.go:34` | 永久协议错误进 DLQ 后再提交 offset | AI |

Relay 缺的不只是 `RunBatch`。`service/outboxrelay/` 目录下只有 `etc/outbox-relay.example.yaml`、`internal/config/config.go`、`internal/relay/worker.go`——**没有 `main.go`（进程不存在）、没有 `Store` 的 MySQL 实现、没有 `Publisher` 的 Kafka 实现**。这三样要补：Store（`ClaimBatch` 用 `FOR UPDATE SKIP LOCKED`、`MarkPublished`、`Reschedule`）、Publisher（可参考 `service/realtime/internal/mq/publisher.go`）、进程入口与装配。配置字段（WorkerID / BatchSize / PollIntervalMillis / LeaseSeconds / MaxRetryBackoffSecs）已经给好了。

### 未完成：红包模块（7 处 TODO）

| 位置 | 内容 |
|---|---|
| `service/redpacket/application/send_rules.go:122` | `AllocateShareAmounts`：普通红包等分（总额须被份数整除，不做余数分配）、拼手气红包二倍均值法 |
| `service/redpacket/application/send_service.go:89` | 发红包事务编排、权限错误分类、COMMIT 未知后的查询确认 |
| `service/redpacket/model/user_balance/user_balance_model.go:42` | 条件扣款（单条条件 UPDATE，余额不足影响 0 行） |
| `service/redpacket/model/red_packet/red_packet_model.go:42` | 事务内插入红包主记录 |
| `service/redpacket/model/red_packet_share/red_packet_share_model.go:41` | 事务内批量插入份额 |
| `service/redpacket/model/balance_log/balance_log_model.go:40` | 事务内插入流水，保住业务唯一键 |
| `service/redpacket/model/outbox_event/outbox_event_model.go:40` | 事务内插入红包的 Outbox 事件 |

红包模块还完全没有的：Redis Lua 候选份额脚本、领取事务（含 `SELECT ... FOR UPDATE SKIP LOCKED` 降级路径）、过期退款调度、CacheBuild Consumer、8103 端口的 API 联调。

## 二、工作区状态

```
M  common/txctx/txctx.go
M  service/message/model/conversation_sequence/model.go
M  service/message/model/message/model.go
M  service/message/model/outbox_event/model.go
M  service/message/persistence/writer.go
?? AGENTS.md
?? implementation-plan.md
?? progress.md
?? redpacket-design.md
```

分支 `main`，HEAD 在 `1c0688d feat: scaffold redpacket and outbox message pipeline`。
**用户明确要求暂不提交**，不要擅自 `git add` / `commit` / `push`。

项目根目录的四个 md 文件是交接仓库 `git@github.com:www0527-ux/Transfer_codex.git` 的副本（本仓库自身是源）。`IM` 仓库的 `origin` 已从 HTTPS 改成 SSH，因为 HTTPS 443 在某台机器上连不通。

## 三、协作约定（重要）

1. **核心代码由用户自己写**：Redis Lua、发送/领取/退款事务、关键 SQL、幂等键设计、错误分类。
2. **AI 不直接改文件**，先给参考代码让用户自己敲，加深印象；**只有用户明确说"你改吧"时才动文件**。
3. AI 负责机械部分：model 层、脚手架、DTO、config/wiring、日志、测试骨架、文档与图。
4. 每阶段先验收再进入下一阶段，不要跳过验证。
5. 不要改 `note/readme.md` 和未提交的实验代码，不要顺手重构无关代码。
6. 表达偏好：中文、先结论后理由、尽量短；区分观察/推断/假设，不确定就直说不确定。

## 四、已经确认的设计决策与坑（避免重复推导）

1. **`outbox_event.event_id` 不是自增列**，必须由应用生成；`InsertPersistedTx` 会拒绝 `EventID <= 0`。`messageID` 同理。
2. **`message` 表没有 `receiver_id` 列**。从 MySQL 读回来的消息 `ReceiverID` 恒为 `nil`，而 fingerprint 里**包含** ReceiverID。所以幂等比对只能在 DB 能表达的字段上做（`message_id`、`client_message_id`、`sender_id`、`session_id`、`session_type`、`message_type`、`content`、`red_packet_id`、`reply_id`）。ReceiverID 的真相在 user 模块（`session.single_key` + `user_session`），它只活在事件里，不落库。
3. **`EnsureTx` 用 `last_seq = 0` 初始化**，不用 `MAX(message.seq)`（热路径上多一个子查询）。代价：已有 legacy 消息的会话**必须先回填** `MAX(message.seq)`，否则第一条新消息会撞 `uk_session_seq`。切换前必须跑回填脚本。
4. **唯一键冲突不能靠解析错误文本判断是哪个索引**。做法：`INSERT` 撞 1062 → 返回哨兵错误触发回滚 → 用**新事务**按幂等键重查 → 查到就当作幂等命中返回。
5. **RR 隔离级别的快照在事务第一次普通 `SELECT` 时建立**。`FindByIdempotencyTx` 就是那一次，所以它看不到并发插入的行，这正是需要唯一索引兜底的原因。这也意味着"先锁后查"只有把锁放在第一句才成立。
6. **`LockAndIncrementTx` 里的 `FOR UPDATE` 主要是为了"当前读"**（拿到最新 `last_seq`），加锁是次要的。换成普通 `SELECT` 会读到过期快照值，算出重复的 seq。
7. **`session_id = session_id` 是 upsert-no-op 惯用法**：不存在则插入，已存在则什么都不改。它同时是整条链路上最早的隐式串行点（会对已存在行加 X 锁）。不要改成 `INSERT IGNORE`（吞掉所有错误）或 `REPLACE INTO`（DELETE+INSERT，会把 `last_seq` 重置）。
8. **`segmentio/kafka-go` 没有实现 Kafka 的幂等生产者协议**，生产端重试会重复写入。正确性只能靠消费端幂等，见 §2.3。
9. **`reply_id` 是 legacy 字段**，设计文档从未讨论。现有校验只保证"给了就 > 0"（`service/realtime/internal/protocol/validation.go:114`），不校验被引用消息是否存在或属于同一会话——已知缺口，要不要补属设计决定。
10. **离线同步仍是 `seq` 区间查询**（`service/offline/internal/syncer/reader.go` 用 `afterSeq + limit` 当上界），而 §15.18 要求改成 `seq > ? ORDER BY seq LIMIT ?` 游标查询。当前 Seq 连续所以不会出错，但这是待办。
11. **多实例推送会丢**：`Manager` 是纯进程内 map，推送消费者共用一个消费组，同一条消息只被一个实例消费。属 §16.2 网关阶段，当前单实例部署不受影响。
12. `outbox_event.AggregateTypeSession = "session"` 这个取值是 AI 定的，设计文档只规定了 `aggregate_id` 与 `aggregate_seq`，名字需要确认。

## 五、下一步顺序建议

**消息模块（优先，红包依赖它）**

1. 起环境跑四条验收（见第六节）——会顺带暴露所有配置问题，越早越好。
2. Relay：补 Store / Publisher / main 三个缺失件（AI），用户写 `RunBatch` 轮询与 `SKIP LOCKED` 领取。
3. 错误映射与 DLQ（AI）。
4. 把 `MessagePipeline.Mode` 切成 `outbox`，跑端到端联调。
5. 验收通过后清理旧链路（Accepted / Transfer / Sequenced / Store）与 `reserved_max_seq` 兼容字段。

**红包模块**

6. model 层 7 个（AI 铺，用户读）。
7. `AllocateShareAmounts`（用户写。注意普通红包"不整除直接报错、不做余数分配"，拼手气用二倍均值并修掉"101 分 100 人"这类死循环）。
8. 发送事务 + Lua 候选份额（用户写）。
9. 领取事务（有候选走条件占用，无候选走 `SELECT ... FOR UPDATE SKIP LOCKED`）。
10. 退款调度 + CacheBuild（用户写）。

## 六、验收用例（消息模块）

需要先 `docker compose up -d`，并确认 SQL 已初始化、Kafka topic 已建立。

1. 同一个 `clientMessageID` 连发两次 → `message` 只有一行，两次返回同一个 `messageID` 和 `seq`。
2. 同一个 `clientMessageID` 换内容再发 → 返回 `ErrIdempotencyConflict`，不新增行。
3. 正常发一条 → `message` 与 `outbox_event(status=0)` 同时存在，且 `message.seq = conversation_sequence.last_seq`。
4. **证明同事务**：手工往 `outbox_event` 插一行占掉某个 `event_id`，再让 `InsertPersistedTx` 撞这个主键 → 检查 `message` 表没有这条消息、`last_seq` 没有变化。这是证明"Seq、message、Outbox 一起回滚"最直接的方式。
5. 同一会话并发发两条不同 `clientMessageID` → seq 连续（N、N+1），不重号。
6. 红包命令走一遍 → `messageID` 用的是命令里带的那个，没有被重新生成。

## 七、环境与命令

```powershell
cd IM-learn
docker compose up -d          # MySQL 8.4 / Redis 7 / etcd / Kafka 3.9 / MinIO
go build ./...
go test ./...
.\scripts\chat-demo.ps1       # 后端联调脚本
```

SQL 由 `docker-entrypoint-initdb.d` 按文件名序号自动初始化（14 个文件）；Kafka 的 5 个 topic 由 `kafka-init` 创建，每个 3 分区。

端口：user.rpc 8080、user-api 8104、offline 8105、realtime 9101、红包服务规划 8103。
启动需要 `IM_JWT_SECRET`；user-api 另外需要 SMTP 相关环境变量。
新旧链路开关：`service/realtime/etc/*.yaml` 与 `service/offline/etc/*.yaml` 里的 `MessagePipeline.Mode: legacy | outbox`。**核心事务与 Relay 验收前必须保持 `legacy`。**

## 八、待确认的开放问题

1. 离线同步的"服务器水位"该取 `MAX(message.seq)` 还是 `conversation_sequence.last_seq`（目标链路下两者应一致，出现空洞或计数器先行时需重新确认）。
2. `outbox_event.aggregate_type` 取 `"session"` 是否合适。
3. receiver 的校验缺口要不要补（补法：给 `message` 加列，或把校验挪到 Redis 命中之外的路径）。
4. 离线同步要不要按 §15.18 改成 `LIMIT` 游标查询。
5. 当前 5 个 Go 改动 + 4 个文档要不要提交进 `IM` 仓库（用户暂缓）。
6. `redpacket-design.md` §21 的"下一阶段由开发者完成"清单已部分过期（第 1–4 项已完成），要不要补一条带日期的新进展，还是保持它作为 2026-09-21 的历史快照。

## 九、2026-10-05 验收进展

当前实现已完成单 Realtime、单有效 Relay、静态网关部署的 21 项真实后端验收与两组浏览器验证，Go 测试/构建和前端类型检查/生产构建通过。本轮修复领取/退款数据库时间扫描失败，以及 Kafka 网络 EOF 导致 Realtime 错误退出的问题。12 个红包核查和 16 名用户余额流水核查通过。

本文件前面的阶段说明保留为历史快照；当前范围、证据、重现脚本和未覆盖边界见 [验收报告](acceptance-report.md)。多节点、统一限流和动态发现按当前取舍保留为扩展；DLQ 人工重放工具不在本轮实现范围。

### 红包前端体验入口

已接入聊天工具栏发红包、普通/拼手气选择、红包卡片、领取、个人金额与领取记录、主动刷新和领完状态。发送结果未知时保留原请求供安全重试；发送端通过增量同步补齐聊天消息，继续保持推送不推进同步游标。真实双账号生产浏览器验证与普通聊天回归通过，详见验收报告的红包前端接入部分。

### 本机负载测试

已完成 13 组、17,760 次测量操作，18 个红包与 121 名用户资金核查通过，Seq 与消息数量一致。修正 Relay 同步逐条发布却默认等待 Kafka 攒批的问题；热门会话仍出现分钟级推送积压，50 并发领取触发 MySQL 151 连接上限，100 并发出现超时与 1 次已入账但未成功响应。结果不是容量全面通过；见 [负载报告](load-test-report.md)，后续优先处理连接预算与 Relay 调度，再谈高并发指标。

### 2026-10-06 连接预算和 Relay 调度优化

已完成用户授权的两项改动：统一 MySQL 连接池配置并给 7 个进程分配合计 96 的示例额度；Relay 有任务且整批成功后立即再领取，空批或错误才等待。新增连接池等待/取消和 Relay 调度测试，全部 Go 测试与构建通过。

真实优化复测 13 组、17,541 次请求无服务错误，18 个红包、121 名用户余额流水与 Seq、观察连接推送顺序核查通过；领取 10/50/100 并发各 600 成功、120 已领完。单会话 10/50 并发推送明显改善，100 并发仍有约 47 秒的推送 P95，不能宣称全部实时性能通过。旧版本对照在 50 并发领取因数据库连接耗尽而终止，不能当作完整验收。详情和可复述的面试素材见 [优化复测报告](load-optimization-report.md)。测试服务已停止，隔离数据保留。

### 2026-10-06 Relay 领取索引优化

新增默认关闭的 Relay 分段计时，在旧负载数据副本上确认前序检查的历史扫描成本，添加状态在 Seq 前的覆盖索引。真实 MySQL 测试通过前序锁竞争、失败等待重试和过期租约回写拒绝；原、新索引均保持顺序。

两轮各 15,960 次请求均通过资金、Seq、推送顺序和最终积压核查。热门会话 10/50/100 并发、固定 600 条消息的推送 P95 从约 49/35/47 秒降到 7.3/7.5/9.6 秒；发送 ACK 和高并发多会话未全面改善。全部 Go 测试、构建通过，本地现有库已添加索引，测试业务服务已停止。数据、迁移与面试素材见 [Relay 优化报告](relay-performance-report.md)。

### 2026-10-06 Relay 连续领取与批量确认

示例领取上限改为500、已ACK确认批次20。同会话先锁首条，MVCC读取已提交前缀后按ID加锁；中间锁冲突回滚，不越过重试/有效租约。批内串行Kafka ACK，确认原子回写，异常停止本批；未确认任务释放或等待租约恢复。真实MySQL验证600条中首批领500、确认后再领100，以及前序、中间锁竞争、确认原子性、失效租约和进程消失恢复；Go测试与构建通过。

同时修复压测实际触发的WebSocket心跳读取阻塞，业务处理进入每连接16条有界队列并保持串行。最终15,960次请求有1次热门会话写入超时，20条低负载观察成功；资金、Seq、成功消息数、观察连接推送顺序和最终积压核查通过。100并发热门会话成功消息的推送P95约9.6秒降到4.1秒，20会话约9.8秒降到3.7秒；不能称为100并发无错误容量通过。测试业务服务已停止，证据与面试素材见 [连续领取报告](relay-batch-report.md)。

### 2026-10-06 面试问答第一版

新增 [interview复习目录](interview/README.md)，六类60道主问题、20道必会题及速记，包含普通消息、红包、同步、消费和Relay的编号时序图。重点整理应用跨系统双写、两段Outbox、缓存投影与恢复的取舍，并用当前代码交叉核对历史决议；不把已删除notification、旧消息链路或逐条Relay回写当作当前实现。

材料区分实现、真实验收和未来扩展，保留最终压测1次超时、单Realtime/Relay、DLQ人工重放工具未实现等边界。每题有口述、展开、追问、代码证据与易错点；本轮仅整理文档，没有重跑业务验收或修改运行服务。
