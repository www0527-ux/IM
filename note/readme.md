建表：
nickname --昵称
avatar --用户头像

    [NOT NULL|NULL]表示是否允许这个列没有值
    [DEFAULT X]设置默认值为NULL,对于允许为空的列本来默认值就是NULL

时间字段的约束：
DEFAULT CURRENT_TIMESTAMP,自动使用当前的时间
ON UPDATE CURRENT_TIMESTAMP --表示这行数据更改时自动更新该字段

字符集：
CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci 决定字符串如何比较与排序

is_delete只是程序上的约束

复合索引是如何优化的？

为何不把两个id作为复合主键？
增设一个apply_friend_id，可以在接口只传一个ID，缓存键也比较简单。

`system_notification`中保存了两个id，可以用来实现幂等，去重

//编译期接口检查
var \_ UserModel = (\*customUserModel)(nil)，防止后面我们在接口里面增加方法但是忘记了实现

关于使用指针作为长期持有的一个资源设计的思想：为什么这个类型的语义更适合 pointer semantics，而不是证明 value semantics 不能工作。

model的字段类型string表示该字段不能为nil

NULLString与string的区分：
可以配合唯一索引，否则没有填入时，会导致两个都是空字符串，而发生插入错误

联合主键goctl生成有误，所以我们自己实现

crud中只有查询没有使用这个ExecCtx
因为sql.result并不能接收查询出来的字段

UserSession 负责描述“数据是什么”，defaultUserSessionModel 负责封装“数据怎么查、怎么改、怎么缓存”，实现数据与数据访问逻辑的职责分离。否则一些数据库连接的配置还需要写到数据里面

cache的两套配置
cause ...error，这种相比于直接传入一个列表错误有什么好处

在函数内部其实就是一个error切片，用来模拟可选参数，但是go没有模拟可选参数，通常必须传入，所以使用这个...error写法来达到目的。

关于这个panic +recover的写法，我现在还没参透

//WrapError 没有直接使用原来的对象，而是返回一个新的ErrorCode就是为了维护这个全局对象
func WrapError(code ErrorCode,cause error)ErrorCode{
return ErrorCode{
Code: code.Code,
Message: code.Message,
Cause: cause,
}
}
直接返回值也没有很大代价，因为string保存的只是一些提示信息

事务处理
txctx transaction context事务上下文

//used for get session from context:
//txCtx := context.WithValue(ctx, txKey{}, session)

    ctx--父context，txctx为其子，继承了取消信号，超时时间，和其他的value，就是增添了新的键值对

//不使用字符串是因为会发生包冲突
type txKey struct{}

func GetSession(ctx context.Context) sqlx.Session {
session, \_ := ctx.Value(txKey{}).(sqlx.Session)
return session
}

func WithTransaction(
ctx context.Context,
conn sqlx.SqlConn,
fn func(ctx context.Context) error,
) error
coon是数据库连接的接口

return conn.TransactCtx(
ctx,
func(ctx context.Context, session sqlx.Session) error {
txCtx := context.WithValue(ctx, txKey{}, session)
return safeRun(func() error {
return fn(txCtx)
})
},
)
第二个参数--回调函数的作用是，在事务开始后的具体执行的业务，session就是该函数在开头创建的事务，作为参数传入

fn 返回 nil
↓
safeRun 返回 nil
↓
TransactCtx 提交事务

fn 返回 error
↓
safeRun 返回 error
↓
TransactCtx 回滚事务

fn 发生 panic
↓
safeRun 通过 recover 转成 error
↓
TransactCtx 回滚事务

tx, err := db.BeginTx(ctx, nil)
sql.DB->sql.Tx开启事务

go-zero定义的接口：
session->sqlx.SqlConn(组成)

Session：
只能执行 SQL

SqlConn：
既能执行 SQL，又能开启事务

commonSqlConn实现了sqlx.SqlConn

type txSession struct {
    *sql.Tx
}实现了这个session接口

数据库的连接是指对于数据库的服务器的连接，不是数据库表的连接

httpx.OkJsonCtx(
r.Context(),
w,
value
)

msg为校验失败的时候显示的提示语

byte为uint8的别名
rune为int32的别名，保存一个unicode码点
码点（code point） 就是编号
rune 字面量就相当于字符字面量，但是未定类型

其中 UTF-8 负责“怎么读这些 byte”，Unicode 负责“读出来的这个编号代表哪个字符”。

internal一般是go中规定好的用来表示服务内部使用的文件

sms short message service 短信服务

反射
dataType := reflect.TypeOf(data)
//判断是否为指针
if dataType.Kind() == reflect.Ptr {
dataType = dataType.Elem()
}
取得请求数据的反射类型信息

防止缓存穿透

Base64Url(Header)

- "."
- Base64Url(Payload)
- "."
- Signature

HMAC-SHA256(
Base64Url(Header) + "." + Base64Url(Payload),
服务端密钥
)

纯JWT验证无法让服务端主动注销JWT，只能等过期

为什么refresh token不设计成jwt，而是随机字符串
jwt验证设计是为了满足无状态验证，然而refresh token若采用jwt则会导致后台无法主动撤销

为什么refresh token不用雪花算法？因为雪花算法是有规律可预测的

SHA-256--单向哈希

    hash := sha256.Sum256([]byte(refreshToken))

原始RefreshToken → 返回给客户端
哈希值 → 保存到Redis

session 一段不独立有上下文的会话

claim 为jwt签发者做出的“声明”
strconv.FormatInt(userID, 10) 是把 int64 转换成十进制字符串。

数据来源 Claims字段 Payload字段
config.Issuer Issuer iss
userID Subject sub
loginSessionID LoginSessionID sid
jti ID jti
TokenTypeAccess TokenType typ
now IssuedAt iat
now + AccessExpire ExpiresAt exp

accessToken 的payload
{
"iss": "infinitechat-user-api",
"sub": "1001",
"sid": "login-A",
"jti": "access-A1",
"typ": "access",
"iat": 1720000000,
"exp": 1720000900
}

完成一个签名需要的变量

    1.签名配置:
    	Issuer
    	AccessSecret
    	AccessExpire

    2.用户身份:
    	userID

    3.登录会话身份:
    	sid
    4.AccessToken 身份:
    	jti

make与new的区别：
var 只产生变量的零值；new 额外产生一个 T 的零值对象并返回地址；make 则把 slice/map/chan 的内部结构真正初始化到可使用状态。
UserID 雪花算法 BIGINT、长期保存、索引友好、大量关联
MessageID 雪花算法 数据量大、需要分布式生成和大致有序
LoginSessionID 128位安全随机数 Redis临时状态、不需排序、不可枚举
JTI 128位安全随机数 Token版本编号、不需排序
RefreshToken 256位安全随机数 属于秘密凭证，必须无法猜测

logout时，若refreshtoken本身失效，就应该正常返回而不是报错

type UserIdRequest {
	UserId int64 `form:"userId,optional" validate:"required" msg:"用户ID不能为空"`
}这样设计主要是为了让错误统一经过我们自己的校验器

func ParseAccessToken(
	config Config,
	tokenString string,
) (*AccessClaims, error)

只负责：
- JWT 格式。
- 签名。
- 算法。
- exp。
- iss。
- typ。
- sub/sid/jti 是否存在。

AuthenticateAccessToken调用前者，查询Redis并比较JTI
返回可信Identity

type identityContextKey struct{}

可以确保不发生冲突


result, err := s.rds.ScriptRunCtx(
		ctx,
		rotateTokenScript,
		[]string{
			oldRefreshKey,
			sessionKey,
			auth.RefreshTokenKey(newRefreshHash),
		},
		sessionID,
		string(newSessionJSON),
		expireSeconds,
	)

这里expireSeconds并不是保存在redis里面的内容，而是外在的时间限制，确保经过轮换之后session存活时间依旧不会增长
也就是对于expireSeconds这个变量不断地通过redis中的expireat的时间来更新。

结构体的零值不是nil

strconv.ParseInt(
	s string,
	base int,//字符串使用的进制
	bitSize int,//结果必须符合的范围如 → 结果必须符合int64范围
) (int64, error)

请求行
请求头
空行
请求体

POST /api/user/loginPassword?source=web HTTP/1.1
Host: 127.0.0.1:8104
Content-Type: application/json
Authorization: Bearer eyJhbGciOi...
User-Agent: Chrome
Accept: application/json

{
  "account": "someone@example.com",
  "password": "123456"
}

MinIo
Bucket: infinitechat
Object: avatars/1001/avatar.png

user-api 换一台机器就找不到文件。
部署多个 user-api 后，各机器文件不一致。
业务服务还要负责存储和传输大量文件。

预签名 URL 可以理解为一张临时许可证：
允许：向指定Bucket上传指定Object
方法：PUT
期限：例如5分钟
签名：由MinIO密钥计算

accessKey：相当于用户名。
secretKey：相当于密码，同时参与请求签名。
sessionToken：临时身份令牌；我们使用固定本地账号，所以传空字符串。

credentials.NewStaticV4(
	accessKey,
	secretKey,
	sessionToken,
)

uploadURL,err:=l.svcCtx.MinIOClient.PresignedPutObject(
		l.ctx,
		l.svcCtx.Config.Minio.Bucket,
		objectName,
		5*time.Minute,
	)

	PresignedPutObject(
	ctx,          // 控制请求取消与超时
	bucketName,   // infinitechat
	objectName,   // avatars/1001/唯一ID.jpg
	expiry,       // URL有效时间
)

BalanceLogItem goctl生成，作为返回响应的一部分

BalanceLog 作为对数据库的接口

第一批 F1：好友只读查询
全部由我实现，属于 [纯CRUD]：
- 获取好友列表
- 获取好友详情
- 获取好友申请列表
- 获取未读好友申请数量
- 所需的 Model 查询和分页方法
- Model 注入 ServiceContext

部分依赖主键会导致数据冗余，以及改动的时候成本大

GET /api/user/friends
GET /api/user/friends/:friendId
GET /api/user/friend-applications
GET /api/user/friend-applications/unread-count

defaultApplyFriendModel struct {
		sqlc.CachedConn
		table string
	}

中数据库操作 + 删除缓存

sqlx.Session

for update锁定的是一行还是一个表

锁的范围取决于查询方式

事务隔离边界：
	一个事务尚未完成时，其他事务不应该随便看到它做到一半的中间状态。
		在提交前，只能看到其他查询只能看到事务开始前的数据
事务隔离性
→ 控制“能够看到什么数据”

FOR UPDATE
→ 控制“谁可以先处理这条数据”

同意好友申请需要在一个事务中完成：

1. 将申请改为已通过
2. 建立好友关系A → B
3. 建立好友关系B → A
4. 创建或复用单聊session
5. 添加A的user_session
6. 添加B的user_session

事务：
保证六步全部成功或全部回滚，并避免向其他普通查询暴露未提交的中间状态。
事务本身不保证所有业务操作自动排队。

FOR UPDATE：
锁住具体的apply_friend记录，使同一个申请只能串行处理。
拿到锁后必须重新检查申请状态。

不同申请：
如果A和B分别存在不同的申请记录，两个事务锁的是不同记录，不会因为
apply_friend锁而互相阻塞。

好友唯一索引：
保证(user_id, friend_id)最终只能存在一条记录。

Upsert：
好友关系不存在则插入；已存在则更新旧记录，适合恢复逻辑删除的好友关系。
唯一索引负责防重复，Upsert负责处理重复键对应的旧记录。

单聊session：
先查询并复用已有session可以避免普通重复，但“先查再创建”本身不能严格
防止并发重复。同一个申请由FOR UPDATE保护；若要保证不同申请并发时也绝对
只有一个单聊，需要为规范化用户对建立数据库唯一索引。

goctl 保证不覆盖自定义代码，但数据库结构改变以后，自定义 SQL、参数顺序、缓存键仍然需要开发者自己同步检查。编译通过也不代表动态 SQL 一定正确。

对于model依赖注入，可以防止在logic层频繁传入redis等参数给函数，减少重复传参，又达到封装的目的，防止logic绕过model直接操控

魔法数字，看不出业务含义的数字

正常好友或已拉黑：不能再次发送申请；能够同意申请
好友关系已逻辑删除：允许重新申请；
没有好友关系：允许申请。

群聊注意：
	session
	→ 群本身：群ID、群名、头像、类型、状态

	user_session
	→ 群成员：谁在哪个群、角色、是否退出

	成员退出应该使用逻辑删除

校验应该放在事务中，实现锁

在同意好友申请时，即使发送申请时已经确认双方用户存在，用户也可能在申请发出后被注销或禁用，因此需要在同一个事务中重新检查双方状态。我们使用 SELECT ... FOR UPDATE 锁定双方用户记录，确认二者都满足 state=0、is_delete=0，并通过 ORDER BY user_id 让并发事务始终按照从小到大的相同顺序加锁，降低双方互相等待造成死锁的概率；检查通过后才更新申请状态、建立双向好友关系和创建单聊会话，否则整个事务回滚。

如何妥善的删除事务后的缓存？
	
rpc的error
codes.InvalidArgument ≈ HTTP 400
codes.Unauthenticated ≈ HTTP 401
codes.PermissionDenied ≈ HTTP 403
codes.NotFound        ≈ HTTP 404
codes.Internal        ≈ HTTP 500

防御性编程
for _,view:=range userViews{
		if view==nil{
			continue
		}
		userById[view.UserId] = &user.BatchUserInfoItem{
			UserId:   view.UserId,
			Nickname: view.Nickname,
			Avatar:   view.Avatar.String,
		}
	}

var users []*UserBasicView
	if err:=m.QueryRowsNoCacheCtx(
		ctx,
		&users,
		query,
		args...,
	);
查询的一行数据原则性是可能全为NULL的。
如果全为NULL，在外层封装上，逐渐扫描，
	只有sql.nullstring,ptr这些才可能接收null，其他直接报错

没有查询到任何行，则len=0,err=nil

结构体保存的是结构体值本身


为什么微服务内部常采用 RPC？

微服务之间本质上仍然需要通过网络通信。虽然 HTTP API 也可以封装成类似函数调用的形式，但对外的 API 层通常还承担 JWT 身份认证、Cookie/Header 解析、CORS、限流、HTTP 状态码转换、前端响应格式等面向外部客户端的职责。内部服务如果只是想调用另一个服务的某项业务能力，再经过这一整套对外 API 层往往没有必要。

RPC 则直接把服务能力定义为类似 `GetUser(req)` 的远程方法，让内部服务通过统一的接口契约直接调用目标服务。它通常配合 Protobuf、代码生成、服务发现等机制，使服务间调用更直接、更高效，也避免把“对外 HTTP 接入逻辑”和“内部业务能力调用”混在一起。

因此可以简单理解为：

**HTTP API 主要解决“外部客户端如何访问服务”，RPC 主要解决“内部服务如何调用另一个服务的业务能力”。**

需要注意，RPC 并不代表内部通信完全不需要安全措施；大型系统内部仍可能使用鉴权、mTLS、权限控制等机制，只是这些机制与面向浏览器或 App 的 HTTP API 接入逻辑不同。


看rpc的链路

1.基础参数校验
	避免无意义的数据库、缓存查询；
	自己不能给自己发送普通单聊消息；
	明确区分参数错误和业务拒绝。
2.发送者账号是否正常
	user_session.status 表示“是否仍是会话成员”，不等于“账号能否使用”。
	发送者可能在建立 WebSocket 后被封禁；
	用户注销后，历史 session 和 user_session 仍可能保留；
	user_session 存在不代表账号当前可用。
3.session是否状态正常，是不是单聊

	防止传入不存在的会话；
	防止拿群聊 Session 调用单聊发送接口；
	已删除的 Session 不应继续发送消息。
4.

Token
├── Raw
│   └── 原始 JWT 字符串
│      header.payload.signature
│
├── Method
│   └── Go 内部真正使用的签名算法对象
│      例如 jwt.SigningMethodHS256
│
├── Header
│   └── JWT 第一部分解析后的内容
│      例如：
│      {
│          "alg": "HS256",
│          "typ": "JWT"
│      }
│
├── Claims
│   └── JWT 第二部分，也就是 payload
│      例如：
│      {
│          "user_id": 123,
│          "exp": ...
│      }
│
├── Signature
│   └── JWT 第三部分，解析后的签名字节
│
└── Valid
    └── 这个 Token 最终是否验证通过

导入 路径而不是包名

%v表示默认显示，如（v1，v2）
%+v则带上字段名

当前项目中，单聊和群聊的消息结构都会携带 sessionId，而且消息历史也是按 sessionId 归档，因此继续传入并验证更直观，也能保持单聊、群聊消息协议一致。

websocket:
	HTTP报文部分：	
	请求行 GET /ws/chat HTTP/1.1
	请求头 
		Host: 127.0.0.1:9101
		Connection: Upgrade表示服务器需要关注upgrade请求头，来切换连接协议
		Upgrade: websocket
		Sec-WebSocket-Version: 13 说明客户端使用的版本
		Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==
		作用是确认服务器理解websocket握手而不是恰巧返回
		Origin: http://127.0.0.1:3000，websocket请求来源
		空行
		请求体（可选）


	响应就会返回
	HTTP/1.1 101 Switching Protocols
	Upgrade: websocket
	Connection: Upgrade
	Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=，握手结果

http中携带host，是为了区分不同的域名服务

var upgrader = ws.Upgrader{
	ReadBufferSize:1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}
协议升级配置加上执行升级方法

conn,err:=upgrader.Upgrade(
		w,
		r,
		nil,
	)

http中带有host的作用：
	找到具体的服务

type Manager struct{
	mu sync.RWMutex
	clients map[int64]map[*Client]struct{}
}
为什么使用map而不是slice，因为在这里主要是用来模拟集合，只需判断一个client是否存在于集合之中，因此使用map让crud都是O(1),而且还有去重的作用。使用空结构体也是因为节省存储空间，也不用包含业务信息

nil map可读，不可写,只有make初始化后才可写
或者使用字面量：
	clients := map[int64]*Client{}
	range遍历得到的是key( _value)

func ParseInt(
	s string,
	base int,
	bitSize int,
) (int64, error)
strconv.ParseInt(text, 10, 64)
text    → 要转换的字符串
10      → 按十进制解释
64      → 按有符号64位整数范围进行检查,例如“200”，10，8就不行，因为200超过了int8的范围

base=0则表示让parseint根据字符串前缀自动判断进制

type MessageResponse struct {
	Type            string `json:"type"`
	ClientMessageID string `json:"clientMessageId"`
	SessionID       int64  `json:"sessionId"`
	SenderID        int64  `json:"senderId"`
	ReceiverID      int64  `json:"receiverId"`
	Content         string `json:"content"`
	SentAt          int64  `json:"sentAt"` //时间戳
}

结构体值装入接口 → 复制结构体值
结构体指针装入接口 → 复制指针，共享原对象

Name: user.rpc --服务的名称
ListenOn: 0.0.0.0:8080
Etcd:
  Hosts:
  - 127.0.0.1:2379 etcd服务的监听端口
  Key: user.rpc 集合的key


  var configFile = flag.String("f", "etc/user.yaml", "the config file")

func main() {
	//更新configFile的指向字符串
	flag.Parse()
	flag内部保存了configFile指向的这个参数的底层，后续还可以通过指针去修改

realtime发出RPC请求
→ Protobuf序列化
→ HTTP/2网络传输
→ user-rpc的grpc.Server接收
→ Protobuf反序列化
→ 根据RPC方法名寻找处理函数
→ 调用UserRpcServer.ValidateSingleMessage

conn, err := upgrader.Upgrade(w, r, nil)
w：写回升级响应，并取得底层 TCP 连接。
r：读取、检查客户端发送的 WebSocket 握手请求。
nil：不添加额外的握手响应头。


GET /ws/chat HTTP/1.1
Host: 127.0.0.1:9101
Connection: Upgrade
Upgrade: websocket
Sec-WebSocket-Version: 13
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==
Authorization: Bearer xxx

HTTP/1.1 101 Switching Protocols
Connection: Upgrade
Upgrade: websocket
Sec-WebSocket-Accept: 计算结果


GUID = 258EAFA5-E914-47DA-95CA-C5AB0DC85B11
Accept = Base64(SHA1(Sec-WebSocket-Key + GUID))

为websocket公用的，固定握手算法

关于http->websocket底层的转化，
	rw, err := listener.Accept()
	首先从监听端口得到底层的tcp连接

	c := server.newConn(rw)
	go c.serve(connCtx)封装为http的连接
	conn的底层
	type conn struct {
		rwc       net.Conn // 底层TCP连接
		hijackedv bool     // 是否已经被接管
	}


	后续升级为websocket之后，w.write就会检查if w.conn.hijacked() {
			return 0, http.ErrHijacked
		}
	

func (c *conn) serve() {
    for {
        request := c.readRequest()

        w := newResponse(c, request)
        handler.ServeHTTP(w, request)

        w.finishRequest()

        if 不能复用连接 {
            return
        }

        // 循环回来，继续从同一个c.rwc读取
    }
}

客户端建立TCP连接
→ 服务端Accept得到net.Conn
→ net/http创建*http.conn并保存net.Conn
→ 从该连接读取HTTP请求1
→ 调用Handler，产生w1
→ Handler结束
→ 连接可以复用
→ 同一个goroutine继续从同一个net.Conn读取请求2
→ 调用Handler，产生w2

ResponseWriter w 是当前 HTTP Response 的写入入口。Handler 可以在生命周期内多次 Write，也可以通过 Flush 提前发送部分数据；只有 Handler 返回后，net/http 才通过 finishRequest() 对这次 Response 进行最终收尾，比如发送response结束信号。

端口号是传输层用于标识“主机上的一个通信端点”的 16 位编号，不是一个进程一个端口号

？？？？

 func(w http.ResponseWriter, r *http.Request)
 {
					Method:  http.MethodGet,
					Path:    "/groups",
					Handler: group.GetJoinedGroupsHandler(serverCtx),
				},
				这种好出就是不用把依赖作为参数传入

websocket 心跳的必然性：
	tcp的keepalive可能时间过长

Ping/Pong + ReadDeadline：判断连接是否仍然存活。
WriteDeadline：避免一次写操作永久阻塞整个发送循环。

c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(
			time.Now().Add(pongWait),
		)
	})
	让readMessage识别pong控制帧

客户端无法确定事务究竟有没有提交。事务保证了“要么全有、要么全无”，但客户端不知道是哪一种。因此即使使用事务，关键业务仍然最好具备幂等性或能够查询最终状态。

回滚不能保证安

业务错误由 Logic 层提前识别；基础设施错误向上返回并包装。事务保证多个数据库操作整体提交或回滚，但提交结果仍可能因网络故障而不确定。因此无论事务还是非事务，凡是可能被重试的重要写操作，都应该通过唯一索引、状态机、请求 ID 等方式实现业务幂等。

Partition 是 Topic 的逻辑有序分片；为了容灾，一个 Partition 可以有多个物理 Replica，分布在不同 Broker 上。其中一个 Replica 是 Leader，负责主要读写，其余 Replica 是 Follower，负责复制 Leader 数据。

突破单日志的串行瓶颈
单 Partition 必须维护一个总顺序；多个 Partition 把全局顺序拆成多个局部顺序，可以并行写入和处理。
支持水平扩展
不同 Partition 可以把 Leader 分布在不同 Broker 上，从而同时利用多台机器的 CPU、磁盘和网络能力。
提高 Consumer 并行度
Consumer Group 以 Partition 为最小并行单位，Partition 越多，组内就可以有更多 Consumer 同时工作。

replicate     动词：复制
replica       名词：副本
replication   名词：复制这个过程

ISR = In-Sync Replicas 当前仍然与 Leader 保持足够同步的 Replica 集合。
Kafka 的 Follower Replica 通过向 Leader

Replica 发送 Fetch 请求，主动拉取 Leader 新追加的日志，并追加到自己的本地日志中，从而完成 Partition 的副本复制。

HW = High Watermark 高水位

HW 取决于 “ISR” 中所有副本都已经复制到的位置，因此可以近似看成 ISR 中各 Replica LEO 的最小值。

Kafka 当前认为已经被 ISR 可靠复制、可以作为已提交日志看待的边界。

Kafka 当前认为“已经足够安全，可以暴露给 Consumer”的日志边界。

position fetch
committed offset

LEO = Log End Offset 每个副本复制到哪里了

ack (0,1,all/-1)
all:要求ISR里同步的副本都确认复制之后才返回成功
如果isr中的relica数量少于min.insync.replicas = 2，就发送失败的ack，不写入

Controller 负责 Kafka 集群的元数据与 Partition Leader 管理。当某个 Broker 或 Partition Leader 故障时，Controller 根据当前副本状态选择新的 Leader，并更新集群元数据。现代 Kafka 使用 KRaft Controller Quorum，通过多个 Controller 节点共同维护一致的控制面状态，避免单个 Controller 成为单点故障。

Group Coordinator Kafka 中专门负责管理某个 Consumer Group 状态的 Broker 角色。

有 key
→ 通常强调路由稳定、局部顺序

无 key
→ 通常强调负载分散、吞吐量

在 Partition 数量和分区算法保持不变的情况下，相同 key 会稳定映射到相同 Partition，否则需要注意依赖陷阱

Producer 发送消息前，需要通过 Partitioner 决定目标 Partition。带 key 的消息通常根据 key 进行稳定映射，使相同 key 在分区数量和算法不变时进入同一个 Partition，从而利用 Partition 内的顺序性；没有 key 时，Producer 通常以提高吞吐和负载均衡为目标把消息分散到多个 Partition。也可以显式指定 Partition，但业务代码通常不应过度依赖具体 Partition 编号。

type ChatMessage struct {
	MessageID int64 `json:"messageId,string"`

	ClientMessageID string `json:"clientMessageId"`//由客户端在发送前生成
		客户端把 ACK 对应到本地消息气泡。
		网络超时后，客户端使用相同 ID 重试。
		-SSA
		服务端识别重复业务请求。
		消息发送前还没有服务端 ID 时，客户端也能标识它。

	SessionID   int64       `json:"sessionId,string"`
	SessionType SessionType `json:"sessionType"`

	SenderID   int64  `json:"senderId,string"`
	ReceiverID *int64 `json:"receiverId,string,omitempty"`

	MessageType MessageType `json:"messageType"`
	Body        MessageBody `json:"body"`

	CreatedAt int64 `json:"createdAt"`
}

落库消费者写 MySQL 成功
        ↓
发送 ChatMessagePersistedEvent 到 Kafka
        ↓
realtime 消费该事件
        ├─ 给发送设备发送 stored ACK
        └─ 给接收设备发送 message.new

Time to live
created, err := r.rds.SetnxExCtx(
		ctx,
		key,
		string(value),
		idempotencyTTLSeconds,
	)

Topic：消息进入哪一类数据流
Key：消息进入 Topic 中的哪个 Partition

flag.String(
    "f",                  // 参数名称
    "etc/offline.yaml",   // 默认值
    "the config file",    // 帮助说明
)
