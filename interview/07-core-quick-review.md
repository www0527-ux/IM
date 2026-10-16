# 07 20道必会题速记

先遮住答案口述，再点完整题练追问。下面是答题提纲，完整依据、代码和失败边界见链接。

## 01 项目介绍 · [A01](01-architecture.md#a01)

参考老师、AI协助的Go IM学习项目，普通聊天、离线同步、红包。核心是MySQL事实、事务内Seq、两段Outbox、幂等和故障恢复；当前单Realtime、单有效Relay、静态网关，有真实验收和本机压测。

## 02 普通消息链路 · [A03](01-architecture.md#a03)

WebSocket鉴权与权限→统一Send→已落库缓存/持久化幂等→同事务Seq、message、Outbox→提交确认返回stored→Relay→独立Push/Cache组。**stored不等于对方收到**，未收到由同步补齐。

## 03 发红包链路 · [A04](01-architecture.md#a04)

先查幂等，新请求才权限校验；条件扣款、红包、份额、流水、创建命令与缓存任务同事务。资金提交即可返回，聊天Writer异步复用预生成messageID，之后签发Seq。**资金成功与消息出现不是同一个提交点。**

## 04 领取与退款 · [A05](01-architecture.md#a05)

领取锁红包，复查领取与状态，份额占用、领取记录、余额、流水和统计一起提交。退款锁同一红包，按总额减已领退款。主表锁决定竞争顺序，唯一约束兜底；Redis不能裁决资金。

## 05 同步与实时交接 · [A06](01-architecture.md#a06)

实时只合并消息，不推进syncCursor。同步响应合并并保存后才按nextSeq推进；查询水位之后新提交的消息下一轮补齐，messageID去重。**最大看见Seq不代表连续同步进度。**

## 06 双写窗口 · [B01](02-design-decisions.md#b01)

先提交库、再发Kafka，中间宕机可丢事件；先发Kafka、再提交库，事件可能对应未提交或回滚业务。业务与Outbox同库同事务，发布留给Relay。这里是跨系统双写，不是InnoDB doublewrite buffer。

## 07 为什么选Outbox · [B02](02-design-decisions.md#b02)

内存重试不能覆盖提交后进程消失；Kafka事务不会自动包括MySQL。Outbox持久化传播任务，恢复可扫描，适合接受异步延迟的需求。代价是轮询、存储、重复和待修复任务，不是跨系统同时提交。

## 08 为什么两段 · [B03](02-design-decisions.md#b03)

资金事务保证消息创建命令存在；消息事务保证已落库推送事件存在。共享统一Writer的幂等、Seq和消息规则，接受卡片延迟。红包CacheBuild是独立支线，不是“第二段消息Outbox”。

## 09 为什么不异步入账 · [B05](02-design-decisions.md#b05)

Lua与Kafka之间有窗口，Lua成功不能代表MySQL加款。当前只用Redis给候选，资金同步事务确认；候选丢失可重建，热门红包主表串行吞吐限制明确保留。

## 10 两个入口不同错误策略 · [B06](02-design-decisions.md#b06)

Send统一返回业务结果，普通入口转stored/rejected/unavailable；Writer Consumer成功后提交offset，临时当前重试，永久先可靠DLQ，不变量停止。**恢复主体不同，事实判断一致。**

## 11 Seq为什么进MySQL · [B07](02-design-decisions.md#b07)

锁会话计数器，Seq、message、Outbox同事务，失败一起回滚。不同会话可并发，同会话有串行点；Snowflake负责消息身份，不替代会话Seq。

## 12 幂等键与内容 · [B08](02-design-decisions.md#b08)

发送sender+clientMessageID；领取packet+user；退款业务唯一流水。同键还需同内容，10元改20元是冲突。数据库唯一键保护并发，Redis仅快路径；超时沿用原键查证。

## 13 为什么重复查询 · [B10](02-design-decisions.md#b10)

事务外读详情和原领取结果，准备RPC并快速返回；RPC留在持锁事务外降低等待。事务内拿红包锁后复查，防止事务外查无之后别人提交。**锁外优化成本，锁内裁决并发。**

## 14 ACID项目实例 · [C01](03-project-fundamentals.md#c01)

原子性：领取、余额、流水整体提交。隔离性：锁和唯一约束协调竞争。持久性：数据库及配置。业务一致性：总额守恒和领取唯一，仍靠代码校验和核查；事务不跨Kafka。

## 15 普通读与锁定读 · [C02](03-project-fundamentals.md#c02)

RR普通读沿用首次一致性读快照；FOR UPDATE是锁定读。当前领取新事务先锁红包，再首次普通读领取记录；若提前建立旧快照要重新评估。**主表锁不自动刷新其他表的普通查询快照。**

## 16 offset怎么防越过 · [C05](03-project-fundamentals.md#c05)

失败10之后提交11会越过10。当前Fetch一条→成功或确认DLQ→同步Commit→再Fetch。并行必须按每分区连续完成区间推进，不能只“不提交失败那条”。

## 17 COMMIT未知 · [D01](04-failures-and-concurrency.md#d01)

响应丢失不是失败证明，可能已整体提交。红包即时回查原业务键，消息后续同键重试查证；确认才成功，未确认可重试/unknown。不能换ID重扣款，也不能从Redis缺失推断没有提交。

## 18 Consumer三个重试阶段 · [D03](04-failures-and-concurrency.md#d03)

业务临时失败只重做当前业务；明确永久错误只重试DLQ；业务或DLQ成功后只重试offset提交。进程恢复可能重放，依靠业务幂等；次数只告警，代次失效停止。

## 19 项目迭代 · [E01](05-iterations-and-performance.md#e01)

旧消息Seq/落库/推送拆开→统一MySQL事务与Outbox→显式offset和DLQ→红包同步资金、删除通知→独立同步游标→真实验收→连接池、Relay索引和批量优化。每次讲出原因与代价，AI分工如实说明。

## 20 Relay为什么还要扩大批量 · [E04](05-iterations-and-performance.md#e04)

原总批量100但每会话一条，热门会话不断领取和回写。当前领取已提交有序前缀，上限500，一次写租约；Kafka仍串行ACK，每20条或批尾原子确认。异常停止后续、释放或租约恢复，重复窗口增大。

## 收尾一定能说明的三个事实

- **性能：** 本机固定600次尝试，100并发热门会话599条成功消息推送P95约9.6s→4.1s，仍有1次写入超时；低负载20条观察P95约232ms。
- **正确性：** 最终资金、Seq、成功消息数量及观察连接推送顺序核查通过，Outbox与已报告Kafka分区lag清零；不代替未覆盖故障验证。
- **范围：** 当前不支持跨Realtime实时路由、严格有序多Relay接管，也没做DLQ人工修复重放工具。未来思路能讲，完成情况如实讲。
