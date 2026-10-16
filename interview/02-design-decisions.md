# 02 业务设计与取舍

本章重点是“有哪几种做法→为何选择当前方案→接受什么代价”。链路编号引用[架构图](01-architecture.md)。

<a id="b01"></a>
## B01 ★ 项目里的双写问题具体是什么？

**口述回答：** 双写是一个业务操作需要写两个独立系统，但没有同一个提交点。普通消息需要写MySQL并发Kafka；红包需要扣款并可靠发出消息创建命令。如果先提交数据库，进程在发Kafka前崩溃，业务存在但事件丢失；先发Kafka再写库，则消费者可能处理数据库里尚不存在、甚至最终回滚的业务。我把业务数据和待发送事件放进同一个MySQL事务，再由Relay发布。

**展开：** 本项目主要处理的是应用的跨系统双写，不是InnoDB存储引擎的doublewrite buffer。资金事务和消息事务各有自己的Outbox提交点，缓存更新则允许延迟和重建。

**追问：** Outbox是不是分布式事务？它是本地事务加异步传播，不提供MySQL、Kafka、Redis同时提交。

**代码与证据：** [消息Writer](../service/message/persistence/writer.go)、[红包Send](../service/redpacket/application/send_service.go)，对应M7～M9和S4～S7。

**容易说错：** 不说“写Kafka失败就把已提交扣款回滚”；已提交事务无法靠普通ROLLBACK撤销。

<a id="b02"></a>
## B02 ★ 为什么用Outbox，而不是重试、Kafka事务或分布式事务？

**口述回答：** 进程内重试解决暂时错误，但如果在数据库提交之后、开始重试之前宕机，没有持久任务就无法继续。Outbox将重试意图随业务事务保存，恢复后可继续扫描。Kafka事务主要协调Kafka内部生产和消费，不自动包含外部MySQL事务。引入跨系统协调会增加实施和运维成本；当前需求接受异步延迟，所以选择本地事务、Outbox和下游幂等。

**展开：**

| 做法 | 能覆盖什么 | 本项目取舍 |
|---|---|---|
| 先写库，再直接发送并内存重试 | 暂时网络错误 | 提交后宕机窗口没有持久任务 |
| 先发Kafka，再写库 | 命令先进入消息系统 | 需要另设计消费与事实提交关系，当前不选 |
| 本地事务写业务和Outbox | 业务与发送意图原子提交 | 当前采用，增加表、轮询、延迟和重复 |
| Kafka事务 | Kafka内的生产/offset协调 | 不能自动原子提交外部MySQL |
| CDC读取提交日志 | 从提交事实生成事件 | 可评估的替代方案，当前未实现 |
| 跨系统事务协调 | 显式协调多个资源的提交 | 取决于资源支持，当前没有建设 |

Kafka与外部存储的协调限制可核对[Kafka官方消息语义说明](https://kafka.apache.org/39/design/design/#message-delivery-semantics)。

**追问：** Outbox成本是什么？历史数据与索引维护、轮询开销、至少一次重复、DLQ人工修复，以及单Relay有序发布的容量限制。

**代码与证据：** [Outbox表](../sql/outbox_event.sql)、[Relay Store](../service/outboxrelay/internal/relay/store.go)。

**容易说错：** 不说Outbox无代价，也不说它消除了发布重复。

<a id="b03"></a>
## B03 ★ 为什么有两段Outbox？不可以一次扣款并写消息吗？

**口述回答：** 第一段在红包资金事务里，保证扣款成功时消息创建命令也存在；第二段在统一消息事务里，保证消息落库时推送事件也存在。当前各表在同库，从物理上可以设计一个更大的事务，但我们选择让红包只拥有资金写入逻辑，普通消息与红包消息共用一个Writer签发Seq、做幂等和写消息。代价是扣款与聊天卡片出现之间有延迟。

**展开：** S6中的两条任务是“创建聊天消息”和“预热红包缓存”。第二段Outbox发生于S12，对应M8，是`MESSAGE_PERSISTED`。两段业务提交点不能混为一谈。

**追问：** 这样能保证最终一定出现红包消息吗？正常可重试路径会恢复；永久错误进入DLQ后仍需要修复，当前没有自动重放平台，不能无条件承诺。

**代码与证据：** [SendService](../service/redpacket/application/send_service.go)、[Writer](../service/message/persistence/writer.go)、设计D1、D7～D10。

**容易说错：** “没有message写权限”是架构责任约束，当前没有证明部署了独立数据库账户权限隔离。

<a id="b04"></a>
## B04 数据库与Redis双写怎么处理？所有缓存都走Outbox吗？

**口述回答：** 缓存不决定资金和消息是否存在，因此没有要求它与MySQL原子提交。初次消息缓存由已落库事件构建，初次红包缓存由资金事务中的CacheBuild任务构建。写缓存失败保留消费位置重试，读缓存失败回源。红包候选池还在成功降级领取后直接重建，并由Worker低频修复；这些操作不新写Outbox，也不改变业务成功事实。

**展开：**

| Redis内容 | 初次构建 | 缺失/错误后的恢复 |
|---|---|---|
| 消息内容 | Message Cache消费已落库快照 | Offline回源MySQL，不在Reader中回填 |
| 消息发送结果 | 同一Cache组，从message计算fingerprint | Send回MySQL，当前不直接回填 |
| 红包创建结果 | CacheBuild按ID查已提交红包 | Send回MySQL，当前不直接回填 |
| shareID候选池 | CacheBuild按ID查未领取份额 | MySQL降级成功后尝试重建，Worker比较数量后修复 |

**追问：** 为什么缓存还能有陈旧ID？版本复查与Redis替换之间不是跨系统原子操作；重建也可能重新放回已弹出的候选，所以领取最终检查MySQL。

**代码与证据：** [Cache Consumer](../service/realtime/internal/consumer/message_cache_consumer.go)、[红包CacheBuild](../service/redpacket/worker/cache_consumer.go)、[CandidatePool](../service/redpacket/application/candidate_pool.go)、[Reader](../service/offline/internal/syncer/reader.go)。

**容易说错：** 不将普通Cache-Aside的“写库删缓存”描述为这里所有缓存的现行实现；不把短暂投影差异说成资金丢失。

<a id="b05"></a>
## B05 ★ 为什么领取不采用Lua抢到后发Kafka异步入账？

**口述回答：** Lua成功只修改Redis；如果随后Kafka发送丢失，客户端可能认为抢到了，却没有数据库领取和余额入账。要修补这条路径，需要额外可靠任务、占用状态和资金恢复协议。当前我让Lua只给候选shareID，领取在同步MySQL事务中完成，确认提交才返回金额。这样并发控制和资金事实落在一个提交点，接受热门红包主表锁的吞吐限制。

**展开：** R4前后没有新增资金事实，候选丢失可重建；R8～R10才包含余额和领取记录。删掉生命周期notification后，领取和退款都无需发布这类通知。

**追问：** Redis没减少主表锁吗？没有，主表锁仍存在；候选池提供定位份额和预热价值，不消除资金串行点。

**代码与证据：** [Receive](../service/redpacket/application/receive_service.go)、[Lua候选](../service/redpacket/application/candidate_pool.go)。

**容易说错：** 不把Lua原子性扩展成Redis、Kafka、MySQL三者的原子性。

<a id="b06"></a>
## B06 ★ 两个入口调用同一个Send，错误处理为什么不同？

**口述回答：** Send负责统一的命令校验、幂等和持久化结果，入口负责把结果转成自己的协议。普通WebSocket入口成功返回stored，明确冲突返回rejected，数据库临时或未知错误返回unavailable并要求原ID重试。Writer Consumer成功才允许提交offset，明确永久错误先可靠入DLQ，临时错误重试当前消息，不变量或缺依赖则停止。两者使用同一事实判断，但恢复主体不同。

**展开：** 消费分类是Consumer包装出来的`PermanentError/FatalError`，不是Send直接操作Kafka。普通入口当前没有把所有数据库失败细分成“明确回滚”和“COMMIT未知”，默认报可重试不可用，不能根据错误文案断言消息没有提交。

**追问：** 服务里直接写DLQ可不可以？会把Kafka协议和普通请求耦合，当前选择在适配入口分类，在消费循环执行策略。

**代码与证据：** [Send](../service/message/application/message_write_service.go)、[普通入口](../service/realtime/internal/websocket/message.go)、[Writer Consumer](../service/realtime/internal/consumer/message_writer_consumer.go)、[公共循环](../service/realtime/internal/consumer/kafka_reader.go)。

**容易说错：** 不把数据库故障统一当永久坏事件，不把缺依赖通过DLQ提交掉。

<a id="b07"></a>
## B07 ★ 为什么把Seq从Redis号段移进MySQL事务？

**口述回答：** 当前需要会话内消息顺序，并希望写入失败不产生计数器已经前进、消息却不存在的空洞。我用会话行锁串行签发Seq，把计数器更新、消息插入和已落库Outbox放进一个事务，失败一起回滚。不同会话仍可并发；同一热门会话存在热点行，这是明确接受的代价。Redis号段更适合允许空洞并有独立持久化顺序协议的方案。

**展开：** messageID用于身份与去重，Seq用于会话内排序与同步，不用全局Snowflake替代会话Seq。迁移老会话需要按已有消息最大Seq回填，不允许从0与历史重号。

**追问：** 回滚后的Snowflake ID会回收吗？不会，ID有空洞正常；这里保证的是当前事务签发的会话Seq回滚。

**代码与证据：** [Sequence Model](../service/message/model/conversation_sequence/model.go)、[Writer](../service/message/persistence/writer.go)、[表结构](../sql/conversation_sequence.sql)。

**容易说错：** 不说数据库消息永远不可能有空洞；人工删除、迁移或未来规则变化都需另分析。

<a id="b08"></a>
## B08 ★ 幂等键是什么？为什么不能只看ID相同？

**口述回答：** 发送以senderID加clientMessageID唯一，领取以红包ID加receiverID唯一，退款有业务唯一流水。相同键只能证明请求身份相同，还要确认业务内容相同；同一个clientMessageID第一次发10元、第二次20元，必须报冲突。Redis只是快路径，MySQL唯一约束是并发最后防线。请求或响应丢失时沿用原键，返回原messageID、Seq或领取金额。

**展开：** 消息还有messageID、会话加Seq、红包ID等唯一约束，任意1062不都代表幂等成功。先回滚，再按原发送键查证并比较内容，见D06。

**追问：** 创建缓存过期后会重复扣款吗？不会仅因为缓存过期而失去持久化幂等约束，但数据库查证失败不能继续创建。

**代码与证据：** [发送查证](../service/redpacket/application/send_service.go)、[消息Writer](../service/message/persistence/writer.go)、[领取唯一键](../sql/red_packet_receive.sql)。

**容易说错：** 不说同一幂等ID可以修改金额重试，不说每次超时都生成新ID。

<a id="b09"></a>
## B09 为什么消息用fingerprint，红包缓存保存整个Command？

**口述回答：** 两者目的都是比较稳定业务语义。红包Command字段简单可比较，所以缓存保存原请求与结果，直接比业务字段。消息对象还带Seq、messageID和时间，直接比较整个对象会把服务端变化当作内容冲突，因此抽取业务字段后计算SHA-256指纹。也完全可以保存消息的业务字段逐项比较；指纹是固定大小摘要，不是必须采用的正确性机制。

**展开：** 当前指纹排除clientMessageID、messageID、Seq和时间；clientMessageID已进入幂等Key。SHA-256为32字节，代码使用hex字符串是64个字符；短红包字段的JSON未必比它大，长文本通常更大。带指针的结构体直接`==`比较可能比较指针地址，slice/map也限制结构体可比性。

**追问：** 哈希会碰撞吗？理论上会，不应称数学上绝无碰撞；数据库查证当前用显式业务字段比较。新增字段要同时维护指纹和持久化比较规则。

**代码与证据：** [Fingerprint](../common/messageidentity/fingerprint.go)、[samePersistedMessage](../service/message/persistence/writer.go)、[sendCacheEntry](../service/redpacket/application/send_service.go)。

**容易说错：** 不说字符串无法比较，不说fingerprint是JWT签名或加密。

<a id="b10"></a>
## B10 ★ 领取为什么事务外查一次，事务内再查一次？

**口述回答：** 事务外读红包获得权限RPC需要的会话和接收人字段，读领取记录可以快速返回已成功结果，避免重做RPC和弹Redis。RPC可能慢，放在持锁事务内会延长连接和红包锁占用。事务外查无记录不排除别人随后提交，所以事务内先锁红包，再复查领取和状态，才执行资金操作。前一层负责减少成本，后一层负责并发正确性。

**展开：** 同用户两个请求都在R2查无：第一个在R5拿锁提交；第二个等待锁后在R6发现原记录，直接返回，不再次加余额。领取表查询没有单独FOR UPDATE，但所有资金写路径遵守先锁红包的协议，并有唯一约束兜底。

**追问：** RR下锁后的普通查询必然最新吗？取决于该事务之前是否建立旧快照，见C02；不能仅因为另一张表加锁就说所有SELECT变当前读。

**代码与证据：** [Receive](../service/redpacket/application/receive_service.go)，R2～R6。

**容易说错：** 不说事务外检查保证了并发安全，也不说主表锁自动给领取表所有记录加锁。

<a id="b11"></a>
## B11 权限校验之后退群或解散会话，怎么办？

**口述回答：** 权限RPC与消息或红包事务没有共同锁和提交点。当前取舍是资格检查返回允许后，本次请求仍可继续提交；不是保证提交瞬间仍有成员身份。下游Writer不再次审核红包发送资格，Push查群成员是决定当前接收人。红包已提交的同内容重试返回原资金结果；新领取仍按当前成员资格校验。

**展开：** 普通消息入口目前每次调用Send前都校验权限，因此发送者后来退群时，普通消息重试可能在到达持久化幂等查证前就被拒绝。不能将红包的“先幂等、再权限”顺序套到普通入口。若要统一历史请求查证体验，是后续改动。

**追问：** 提交前再调一次RPC能消除窗口吗？只能缩短窗口；严格排序需要共享原子检查或明确的版本/协调协议。

**代码与证据：** [普通入口](../service/realtime/internal/websocket/message.go)、[红包Send](../service/redpacket/application/send_service.go)、设计D27。

**容易说错：** 不说当前已经实现跨服务的权限与事务原子性。

<a id="b12"></a>
## B12 为什么删除红包notification？详情还可靠吗？

**口述回答：** 领取是否成功、金额和领完状态已有MySQL事实及接口可查询。当前产品允许打开详情或主动刷新再更新，不需要每次领取、领完和退款都实时广播。因此删除生命周期通知Outbox、Topic和Consumer，保留发送的消息创建与缓存任务。详情返回成功领取列表，前端按用户ID展示个人金额，减少一条不参与资金正确性的链路。

**展开：** 详情主表与列表分开读，当前接受短暂展示差异，没有增加统一只读快照或主表锁。删除通知的代价是其他用户的界面不会立即自动更新状态。

**追问：** notification是不是必需组件？取决于产品实时展示需求，当前需求通过查询满足，不是所有系统都应删除。

**代码与证据：** [QueryService.Get](../service/redpacket/application/query_service.go)、[前端详情](../../webark-im-template/src/imlearn/RedPacketDialogs.tsx)、设计D30。

**容易说错：** 不说红包整个模块只有两个HTTP接口；还有发送和我发出的红包查询。

<a id="b13"></a>
## B13 为什么用数据库时间？提前读NOW之后再过期怎么办？

**口述回答：** 数据库时间使领取和退款在同一时间基准判断，减少不同进程时钟偏差。领取先读取NOW快速拒绝明显到期请求，但事务执行期间仍会过去时间，因此最后更新红包主表时还要求expire_at大于NOW。条件不成立就回滚此前份额、领取、余额和流水。时间不是靠行锁冻结的，行锁只协调数据写入。

**展开：** 数据库生成的判断时间和业务允许的边界要说清：当前领取以R9条件更新的时间作为最终到期检查，并不保证R10提交瞬间尚未到期。创建时ExpireAt由应用时间加24小时，当前没有全部改成数据库生成。

**追问：** 必须按提交瞬间到期怎么办？需要重新定义业务语义和提交协调，不能把提前取时间称为完全覆盖。

**代码与证据：** [Receive](../service/redpacket/application/receive_service.go)、[Expire](../service/redpacket/application/expire_service.go)、[创建时间](../service/redpacket/application/send_service.go)。

**容易说错：** 不把“条件更新时有效”说成“提交时一定有效”。

<a id="b14"></a>
## B14 整个红包锁住是不是太慢？为什么暂时保留？

**口述回答：** 主表锁确实把同一红包领取和退款串行化，限制热门红包吞吐。但它让重复领取查证、已领统计、最后一份完成和剩余退款有统一裁决点，逻辑容易审查。不同红包仍能并行。目前先保证这些不变量并控制事务长度；若进一步拆成份额并行领取，需要重新设计统计、到期协调与退款，不能仅删除主表锁。

**展开：** RPC在事务外、不在资金事务写Redis，有助于缩短锁占用。乐观版本更新也需要处理冲突和重试，热点高竞争下不一定更快。当前没有实现拆锁方案。

**追问：** SKIP LOCKED还有效吗？正常路径已先锁同一红包，份额并发竞争有限；它不是当前热门红包吞吐提升的主要来源。

**代码与证据：** [Receive](../service/redpacket/application/receive_service.go)、[Expire](../service/redpacket/application/expire_service.go)、[负载结果](../load-test-report.md)。

**容易说错：** 不声称用了Redis就没有数据库热点，也不为面试临时承诺未实现的高吞吐拆锁设计。
