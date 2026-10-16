# 03 项目相关八股

只整理直接影响本项目设计的基础知识。口述后接一个项目例子；官方文档核对日期为2026-10-06。

<a id="c01"></a>
## C01 ★ ACID在项目中如何体现？

**口述回答：** 原子性体现在余额、份额、领取记录和流水一起提交或回滚；隔离性通过事务、红包行锁和唯一约束处理竞争；持久性依赖数据库提交与其持久化配置；一致性是提交前后仍满足业务约束，例如总额守恒和一人一份。数据库提供基础机制，但不会自动理解红包业务，我还要实现校验、条件更新、唯一键和只读核查。

**展开：** 回调执行完不等于COMMIT已确认。不能用“代码都在一个函数里”证明同事务；所有Model必须用同一事务Session。当前是模拟余额，不包含外部银行或支付渠道事务。

**追问：** 如何证明回滚？验收曾删除领取者余额行让加款失败，确认份额、领取记录与主表统计都回滚，见F01。

**代码与证据：** [Receive](../service/redpacket/application/receive_service.go)、[txctx](../common/txctx/txctx.go)、[验收](../acceptance-report.md)。

**容易说错：** 不说ACID自动解决MySQL与Kafka双写，也不把未实测的磁盘断电持久性作为已验收结果。

<a id="c02"></a>
## C02 ★ 普通SELECT和FOR UPDATE有什么区别？锁后查询是不是一定最新？

**口述回答：** 在InnoDB的RR下，普通一致性读通常沿用本事务第一次一致性读建立的快照；FOR UPDATE是锁定读，会读取可用于当前加锁更新的版本并等待冲突事务。锁红包之后再普通查询领取表，并不会让普通查询变成锁定读。当前顶层领取事务在拿红包锁之前没有做领取表的一致性读，因此锁后的首次查询能看到此前持锁事务已提交的领取；如果增加更早的快照读，就要重新评估。

**展开：** 事务外读取不属于后来新事务的快照；BEGIN本身也不等于所有普通查询的快照必然已经建立。RC下普通一致性读按语句取新快照。项目依赖默认RR和现行SQL顺序，修改隔离级别或加入外层事务时需要复核。

**追问：** 可不可以直接用FOR UPDATE查询领取表？可以作为另一个设计评估，但会改变锁范围；现有主表协议加唯一约束已承担裁决，不能未经分析叠加锁。

**代码与证据：** [Receive](../service/redpacket/application/receive_service.go)，[MySQL一致性读说明](https://dev.mysql.com/doc/refman/8.4/en/innodb-consistent-read.html)、[锁定读说明](https://dev.mysql.com/doc/refman/8.4/en/innodb-locking-reads.html)。

**容易说错：** 不说RR的所有语句都只看同一个旧快照，也不说锁另一张表会自动刷新已有Read View。

<a id="c03"></a>
## C03 条件更新、行锁和唯一索引分别解决什么？RowsAffected是什么？

**口述回答：** 条件更新将判断与修改放进同一SQL，例如balance足够才扣款，避免读余额与扣款之间的竞争。行锁协调多个相关表操作，例如领取和退款都先锁同一个红包。唯一索引保护不能重复的身份，例如一个用户在同一个红包只能有一条领取记录。RowsAffected是某次执行返回结果中的影响行数，不是这个事务所有UPDATE的总和。

**展开：** 扣款影响0行表示条件未匹配，当前按余额不足处理；领取余额行必须存在且正金额加款，应影响1行，否则报不变量错误并回滚。区分“SQL执行错误”和“SQL成功但行数不符合预期”。

**追问：** 已有主表锁还需要唯一键吗？锁协议是代码路径要求，唯一键是数据库的最后防线，也保护异常写入和未来代码变动。

**代码与证据：** [DebitIfEnoughTx](../service/redpacket/model/user_balance/user_balance_model.go)、[Receive.RowsAffected](../service/redpacket/application/receive_service.go)、[领取表](../sql/red_packet_receive.sql)。

**容易说错：** 不说唯一索引会自动核对业务内容，不把RowsAffected当SELECT结果数。

<a id="c04"></a>
## C04 SKIP LOCKED和NOWAIT怎么用？为什么不能任意跳过？

**口述回答：** SKIP LOCKED排除当前不能取得行锁的记录，适合任务领取，但得到的集合不一定完整。NOWAIT遇到行锁冲突立即报错，便于回滚后重试。本项目Relay可以跳过其他被锁会话的首条，却不能因此越过同会话未完成的前序。扩展同会话前缀时按明确ID用NOWAIT锁定，中间冲突整批回滚。资金领取也有SKIP LOCKED，但仍先拿红包主表锁。

**展开：** 不能根据SKIP LOCKED查不到就认定数据库没有记录。当前领取在主表统计仍显示有剩余、所有正常份额写路径先锁主表的前提下，选不到份额被归为不变量故障。

**追问：** Relay怎么避免范围读碰上未提交尾部？先普通快照读出已提交ID，再锁这些ID，见E06。

**代码与证据：** [Relay Store](../service/outboxrelay/internal/relay/store.go)、[领取](../service/redpacket/application/receive_service.go)，[MySQL NOWAIT与SKIP LOCKED](https://dev.mysql.com/doc/refman/8.4/en/innodb-locking-reads.html#innodb-locking-reads-nowait-skip-locked)。

**容易说错：** 不把SKIP LOCKED当排序保证；也不能笼统说这些选项消除了所有类型的数据库等待。

<a id="c05"></a>
## C05 ★ Kafka offset为什么可能越过失败消息？怎么避免？

**口述回答：** 同一分区提交更高offset会推进消费位置，不是独立确认每条消息。如果offset10业务失败但继续处理11并提交11，恢复时可能从12开始，10就被跳过。当前采用FetchMessage取一条，处理成功或可靠入DLQ后同步CommitMessages，提交成功再读下一条。临时失败保留当前消息重试，未来并行化需要每分区只推进连续完成区间。

**展开：** `CommitInterval=0`用于同步提交。仅设置单个Processor或让失败消息“不调用提交”，但继续允许提交后继，不能证明不会越过。

**追问：** 业务成功但提交失败怎么办？当前进程重试提交，重启后可能再做业务，靠业务幂等，见D03。

**代码与证据：** [公共消费循环](../service/realtime/internal/consumer/kafka_reader.go)、[Realtime配置Reader](../service/realtime/realtime.go)，[kafka-go显式提交说明](https://github.com/segmentio/kafka-go#explicit-commits)。

**容易说错：** 不说提交offset自动和MySQL事务原子；offset也不是用户已收到的凭证。

<a id="c06"></a>
## C06 至少一次、至多一次、恰好一次分别意味着什么？ACK=all安全吗？

**口述回答：** 至多一次偏向不重复但允许丢失；至少一次通过重试降低丢失风险但允许重复；恰好一次需要限定在哪些资源和副作用范围成立。当前Outbox到Kafka、Consumer到MySQL都采用至少一次配合幂等。Relay等待同步Broker ACK并配置all，之后才确认发布；ACK未知仍可能重复。它不证明消费者处理完成或客户端收到。

**展开：** all与Topic复制因子、ISR及min.insync.replicas共同决定副本确认条件。本机压测Topic复制因子是1，不能说all因此有多副本容灾。业务唯一约束让重复收敛，也不等于网络只投递一次。

**追问：** Kafka事务能不能原子改MySQL？当前没有外部协调，不能；见B02。

**代码与证据：** [Publisher配置](../service/outboxrelay/internal/mq/publisher.go)、[压测环境](../scripts/relay-batch-results-20261006.json)，[Kafka交付语义](https://kafka.apache.org/39/design/design/#message-delivery-semantics)。

**容易说错：** 不把RequireAll说成所有消费者ACK，不用“端到端exactly once”描述本项目。

<a id="c07"></a>
## C07 Topic、Partition、Key和Consumer Group有什么关系？

**口述回答：** Topic分成多个Partition，顺序主要在分区内讨论。同一会话用sessionID作Key，经当前Hash分配到同一分区。一个组负责消费自己的位置，组内实例分担分区；不同组各自收到同一Topic的数据。因此Push和Cache使用独立组，缓存故障可以独立重试。相同组不会把每条消息广播给所有Realtime节点。

**展开：** 单个Reader当前串行处理会让某条临时失败阻塞这个Reader继续取其他消息，可能影响其负责的多个分区，不只是失败分区。增加分区数可能改变Key映射，后续扩容要评估会话顺序。

**追问：** 多Realtime都用同一Push组能收到所有本地用户的消息吗？不能自动保证，因为持有连接的节点不一定是拿到事件的节点，见F02。

**代码与证据：** [Realtime启动](../service/realtime/realtime.go)、[Publisher Hash](../service/outboxrelay/internal/mq/publisher.go)、[Kafka设计](https://kafka.apache.org/39/design/design/)。

**容易说错：** 不说Kafka提供跨所有Partition的全局消息顺序。

<a id="c08"></a>
## C08 Redis Lua的原子性在这里到底有什么价值？

**口述回答：** 脚本在Redis内执行期间不会与其他命令交错，适合把多个Redis操作组合起来。但这里的弹出脚本只是一个RPOP，RPOP本身就原子；Lua不是当前资金正确性的核心，也不必为了技术亮点夸大脚本复杂度。真正需要数据库保证的是候选份额是否合法、领取唯一性和余额入账，Redis操作成功后宕机仍可能丢一个缓存候选。

**展开：** 替换候选池的另一个脚本将EXISTS、RENAME和EXPIRE组合执行。它只协调Redis，不覆盖MySQL读取与版本复查，更不覆盖Kafka。

**追问：** 多命令Lua出错会自动回滚之前的写吗？不能把脚本不交错等同数据库事务回滚，脚本应自行控制错误与写入顺序。

**代码与证据：** [popCandidateScript与replaceCandidateScript](../service/redpacket/application/candidate_pool.go)，[Redis脚本执行说明](https://redis.io/docs/latest/develop/programmability/eval-intro/)。

**容易说错：** 不说当前Lua会直接扣余额，也不把单条RPOP包装成复杂分布式锁方案。

<a id="c09"></a>
## C09 为什么重建候选池使用临时Key和version？

**口述回答：** 逐项覆盖正式列表会暴露半成品，临时Key构建完再RENAME可以一次替换。构建前后读红包version和状态，若领取或退款造成变化就放弃这次结果，降低旧份额覆盖新状态的机会。由于MySQL复查与Redis替换之间仍有窗口，陈旧候选不能完全消除，最终由领取SQL检查UNCLAIMED和主表状态。

**展开：** 临时Key加TTL并尝试清理；正式候选池TTL按有效期计算。Worker比较未领取数量与Redis长度能发现很多丢候选问题，但数量相同的错误内容未必检测出来。

**追问：** 为什么不锁红包直到缓存写完？会把网络耗时放入资金锁区间，当前通过可丢弃投影和数据库兜底避免这种耦合。

**代码与证据：** [Rebuild与RebuildIfMismatch](../service/redpacket/application/candidate_pool.go)、[Worker修复循环](../service/redpacket/worker/worker.go)。

**容易说错：** 不说version复查与RENAME组成了跨MySQL/Redis的原子事务。

<a id="c10"></a>
## C10 Go的context怎么让多个Model加入同一事务？

**口述回答：** txctx创建事务后把Session放进派生context，Model的Tx方法从它取出Session并生成事务绑定连接，所有SQL都用同一个事务。缺Session就明确报错，不退回普通连接池。context同时传递取消和期限，但把变量放进context本身不会自动让普通Model加入事务，需要每个调用实际使用SessionConn。

**展开：** `%w`包装错误保留errors.Is/As分类。当前嵌套WithTransaction复用已有Session，不创建独立子事务或savepoint；外层提交决定最终结果。

**追问：** context超时是不是数据库一定没提交？不是，提交结果可能未知，需要业务键查证。

**代码与证据：** [txctx](../common/txctx/txctx.go)、[事务Model](../service/redpacket/model/user_balance/user_balance_model.go)。

**容易说错：** 不把context.Value当隐式分布式事务，不在缺事务时偷偷使用池连接。

<a id="c11"></a>
## C11 WebSocket为什么分读循环、写循环和业务处理循环？

**口述回答：** 读循环要持续读取数据和Ping/Pong控制帧，写循环串行发送消息与心跳。原来读循环直接做业务，RPC或数据库慢时无法及时读取Pong，会误判连接超时。现在读循环投递到16条有界业务队列，单业务worker保持每连接请求顺序；写队列上限256。队列满采用明确关闭或入队失败，并依靠原ID重试和同步恢复。

**展开：** 独立业务worker不在连接上直接读写帧，回复送写队列；已经执行的事务可能在连接关闭后提交。关闭信号让循环停止，不能把断线等同业务回滚。

**追问：** 为什么不能无限goroutine处理每帧？会增加资源、破坏每连接处理顺序，也让背压失控。

**代码与证据：** [Client](../service/realtime/internal/websocket/client.go)、[真实WS回归测试](../service/realtime/internal/websocket/client_test.go)，[Gorilla并发与控制帧说明](https://pkg.go.dev/github.com/gorilla/websocket#hdr-Concurrency)。

**容易说错：** 不说多个goroutine可以任意同时WriteMessage；也不把入队成功当网络接收成功。

<a id="c12"></a>
## C12 JWT已经能验签，为什么还查询Redis登录状态？

**口述回答：** JWT验签和期限检查证明令牌有效，但当前还希望校验服务器端登录会话。我从token取得用户、session和jti，再查Redis中的登录记录，检查用户一致、当前jti和会话有效期。因此请求身份来自鉴权结果，不信任客户端提交senderID。网关只是转发Authorization，各服务仍做自己的鉴权。

**展开：** 当前token使用HS256并检查算法白名单、issuer和token类型；签名不等于加密。登录状态引入Redis依赖，因此Redis整体故障会影响入口可用性。

**追问：** 能否完全无状态？可以重新选择过期、撤销和多设备策略，但那不是当前实现。

**代码与证据：** [JWT](../common/auth/jwt.go)、[Authenticator](../common/auth/authenticator.go)、[网关说明](../service/gateway/README.md)。

**容易说错：** 不说有效JWT就可以访问任意会话；身份认证之后还需要会话权限判断。

<a id="c13"></a>
## C13 Snowflake解决什么？不同进程怎样避免ID冲突？

**口述回答：** Snowflake提供服务端ID生成，不承担会话Seq或请求幂等。每个产生ID的进程显式配置独立NodeID，启动时检查范围，避免多个进程使用同一节点身份。普通消息ID由Writer生成，红包先生成消息ID并让Writer复用。客户端clientMessageID表示请求身份，重试不能随意变化。

**展开：** 唯一性仍依赖节点配置、算法对时间的处理和运行条件，数据库主键会拦截碰撞。前端把大整数ID作为字符串传输处理，不能用JavaScript Number随意转换。

**追问：** Snowflake顺序能代替会话Seq吗？不能提供同一业务事务的连续会话排序与同步水位。

**代码与证据：** [Generator](../common/idgen/generator.go)、[Message协议](../service/realtime/internal/protocol/message.go)、[前端数据源](../../webark-im-template/src/imlearn/dataSource.ts)。

**容易说错：** 不承诺错误NodeID配置下仍唯一，也不把ID空洞当资金损失。

<a id="c14"></a>
## C14 联合索引怎么结合项目解释？如何确认SQL慢在哪里？

**口述回答：** 我先看实际查询的过滤、排序和扫描，再用执行计划及运行耗时验证。消息发送依赖sender加clientMessageID唯一索引，同步依赖session加Seq。Relay检查同会话更早未发布事件，原索引反复扫描已发布历史，增加aggregate、eventType、status、Seq的覆盖索引后减少检查成本。索引收益是通过实际数据对照确认，不是只根据最左前缀口诀判断。

**展开：** `status<>2`与`aggregate_seq<当前Seq`涉及范围条件，不能简单说把status放前面一定让后续字段全部变成索引定位；还应看覆盖、扫描行数与实际分布。新增索引也增加写入和存储成本。

**追问：** EXPLAIN好看就代表端到端延迟好了吗？不代表，还要测领取事务、同步Kafka ACK、回写和客户端实际收到时间。

**代码与证据：** [消息DDL](../sql/message.sql)、[Outbox DDL](../sql/outbox_event.sql)、[索引对照报告](../relay-performance-report.md)、[执行计划分析脚本](../scripts/relay-index-analysis.mjs)。

**容易说错：** 不把索引优化归因于消息发送ACK全面改善，实际多会话ACK仍有容量限制。
