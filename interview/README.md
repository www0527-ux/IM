# IM-learn 项目面试问答

版本：2026-10-06。共60道主问题、20道★必会题，分六类，另有必会题速记。用于面试复习和代码走读，不是生产能力声明。

## 从哪里开始

| 次序 | 材料 | 数量 | 阅读目标 |
|---|---|---:|---|
| 1 | [项目介绍与整体架构](01-architecture.md) | 8 | 能画出普通消息、发红包、领取/退款与同步链路 |
| 2 | [业务设计与取舍](02-design-decisions.md) | 14 | 说清双写、Outbox、事务、幂等和产品取舍 |
| 3 | [项目相关八股](03-project-fundamentals.md) | 14 | 用项目实例解释锁、MVCC、Kafka、Redis和Go |
| 4 | [故障与并发边界](04-failures-and-concurrency.md) | 10 | 沿步骤说明已有事实、响应和恢复方式 |
| 5 | [迭代与性能优化](05-iterations-and-performance.md) | 8 | 说明为什么修改、如何测量、结果与限制 |
| 6 | [验证、局限与扩展](06-verification-and-extension.md) | 6 | 接住多节点、网关、DLQ和AI协作追问 |
| 随时 | [20道必会题速记](07-core-quick-review.md) | 20道摘录 | 遮住答案自测，再回完整题补追问 |

每道主问题包含：**口述回答、展开、追问、代码与证据、容易说错**。★题优先练熟，其余用于补追问；编号稳定，避免复习时找不到原题。

## 20道必会题索引

| 编号 | 问题 | 完整回答 |
|---|---|---|
| A01 | 项目如何介绍？ | [A01](01-architecture.md#a01) |
| A03 | 普通消息完整链路？ | [A03](01-architecture.md#a03) |
| A04 | 发红包与普通消息的区别？ | [A04](01-architecture.md#a04) |
| A05 | 领取和退款如何保证资金正确？ | [A05](01-architecture.md#a05) |
| A06 | 同步与实时消息如何交接？ | [A06](01-architecture.md#a06) |
| B01 | 双写问题的具体失败窗口？ | [B01](02-design-decisions.md#b01) |
| B02 | 为什么选Outbox？ | [B02](02-design-decisions.md#b02) |
| B03 | 为什么两段Outbox？ | [B03](02-design-decisions.md#b03) |
| B05 | 为什么领取同步MySQL入账？ | [B05](02-design-decisions.md#b05) |
| B06 | 两入口的错误处理为什么不同？ | [B06](02-design-decisions.md#b06) |
| B07 | 为什么Seq进MySQL事务？ | [B07](02-design-decisions.md#b07) |
| B08 | 幂等键与内容查证？ | [B08](02-design-decisions.md#b08) |
| B10 | 为什么事务前后重复查询？ | [B10](02-design-decisions.md#b10) |
| C01 | ACID怎么对应项目？ | [C01](03-project-fundamentals.md#c01) |
| C02 | 普通读与锁定读的区别？ | [C02](03-project-fundamentals.md#c02) |
| C05 | 如何防止offset越过失败消息？ | [C05](03-project-fundamentals.md#c05) |
| D01 | COMMIT未知怎么办？ | [D01](04-failures-and-concurrency.md#d01) |
| D03 | 业务、DLQ、提交分别重试什么？ | [D03](04-failures-and-concurrency.md#d03) |
| E01 | 架构怎么迭代的？ | [E01](05-iterations-and-performance.md#e01) |
| E04 | 已经批量领取为何还要优化Relay？ | [E04](05-iterations-and-performance.md#e04) |

## 建议复习方式

1. 第一遍读A01和四条核心链路，把服务、表和提交点连起来。
2. 第二遍练20道必会题，先不看答案口述；卡住时回完整题和代码。
3. 第三遍用追问自测，尤其双写、COMMIT未知、领取退款竞争和offset。
4. 第四遍练一次完整模拟：项目介绍→设计选择→故障→优化→扩展。

每题自检：能否不用类名解释业务？能否指出成功提交点？能否说明一种失败后从哪里恢复？能否承认当前方案的代价？

需要重点回看的已讨论疑问：**fingerprint与业务字段比较B09；事务外/内复查B10与C02；时间边界B13；Redis重建来源B04；网关与Vite A07；同步游标A06；批量领取尺度E04。**

## 当前事实清单

| 项目 | 截至本版的事实 |
|---|---|
| 消息写入 | 统一Writer，MySQL事务签发会话Seq、消息与已落库Outbox；旧链路已移除 |
| 红包 | 同步发送/领取/退款事务；候选池是缓存；生命周期notification已移除 |
| 缓存 | 初次构建走Outbox事件；候选池还保留降级成功重建和低频修复 |
| 消费 | 三个消息消费组显式提交、当前消息重试、永久错误可靠DLQ；人工修复重放工具未做 |
| 部署 | 单Realtime、单有效Relay，静态HTTP/WS网关；未完成跨Realtime实时推送 |
| Relay参数 | 示例领取500、确认20、租约120秒；Kafka逐条同步ACK，Publisher BatchSize=1 |
| 同步 | 推送不推进syncCursor；后端区间MGET与MySQL回源；前端保留最近500条 |
| 验证 | 21项真实后端验收与浏览器验证；真实MySQL Relay边界测试；本机负载结果有明确限制 |
| 最终负载 | 15,960次请求有1次写入超时；100并发热门会话599条成功消息推送P95约4.1秒 |

## 证据与历史材料如何使用

- [redpacket-design.md](../redpacket-design.md)：业务规则与D1～D33决议。包含历史快照，当前实现优先看最新决议及代码。
- [acceptance-report.md](../acceptance-report.md)：真实依赖、浏览器验证和未覆盖边界。
- [load-test-report.md](../load-test-report.md)：初轮容量问题，包含失败结果。
- [load-optimization-report.md](../load-optimization-report.md)：连接预算与Relay连续循环。
- [relay-performance-report.md](../relay-performance-report.md)：前序检查索引的对照证据。
- [relay-batch-report.md](../relay-batch-report.md)：最终批量优化、失败轮、超时与测量口径。

本套问答依据当前代码交叉核对，并不将本轮文档核对描述成重新运行了全部验收。以后修改业务字段、SQL顺序或部署方式时同步更新相关答案。

## 面试时的表述边界

“已经编码”“单元测试覆盖”“真实依赖验收通过”“生产能力”分别陈述。不要说：

- Kafka、MySQL和Redis已经同时原子提交；
- 入WebSocket队列等于客户端收到；
- Redis整体停机所有接口仍可用；
- 当前支持多Realtime或多Relay严格有序接管；
- 100并发容量无错误，或本机数字等于生产性能；
- 参考和AI协助完成的实现全部是自己独立手写。

材料中的口述回答可以按自己真实参与修改，但成功条件、验证结果和技术边界要保持一致。
