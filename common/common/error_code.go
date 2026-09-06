package common

//cause保存真实错误，用于作为服务端的日志
//ErrorCode同时保存前端与后端服务端日志所需的内容

// type BaseResponse[T any] struct {
// 	Code    int    `json:"code"`
// 	Data    T      `json:"data"`
// 	Message string `json:"message"`
// }

// cause服务于后端的日志，不作为baseresponse返回
type ErrorCode struct {
	Code    int
	Message string
	Cause   error
}

// 使用值接收者表示，ErrorCode被当成一个“错误信息快照”
// 而不是一个需要共享和修改状态的对象
func (e ErrorCode) Error() string {
	return e.Message
}
func (e ErrorCode) Unwrap() error {
	return e.Cause
}

// 只创建error模板，不放入cause，方便后续拓展使用
func newErrorCode(code int, message string) ErrorCode {
	return ErrorCode{
		Code:    code,
		Message: message,
	}
}

// WrapError 没有直接使用原来的对象，而是返回一个新的ErrorCode就是为了维护这个全局对象
func WrapError(code ErrorCode, cause error) ErrorCode {
	return ErrorCode{
		Code:    code.Code,
		Message: code.Message,
		Cause:   cause,
	}
}

// 40xxx 表示通用请求、权限错误
// 50xxx 表示系统内部错误
// 70xxx 用户业务错误
var (
	ParamsError   = newErrorCode(40000, "请求参数错误")
	NotLoginError = newErrorCode(40100, "未登录")
	NoAuthError   = newErrorCode(40101, "无权限")
	NotFoundError = newErrorCode(40400, "请求数据不存在")

	SystemError    = newErrorCode(50000, "系统内部异常")
	OperationError = newErrorCode(50001, "操作失败")
	SystemBusy     = newErrorCode(50008, "系统繁忙，请稍后重试")

	PhoneEmailError    = newErrorCode(70000, "手机号或邮箱格式错误")
	UserAlreadyExists  = newErrorCode(70001, "用户已存在")
	UserNotExists      = newErrorCode(70002, "用户不存在")
	RegisterError      = newErrorCode(70003, "注册失败")
	VerificationError  = newErrorCode(70004, "验证码错误")
	LoginError         = newErrorCode(70005, "账号或密码错误")
	PasswordMatchError = newErrorCode(70006, "两次密码不一致")

	CannotAddSelfError             = newErrorCode(71000, "不能添加自己为好友")
	AlreadyFriendsError            = newErrorCode(71001, "对方已经是你的好友")
	FriendApplicationPendingError  = newErrorCode(71002, "好友申请正在等待处理")
	FriendApplicationReceivedError = newErrorCode(71003, "对方已经向你发送了好友申请")
	FriendRelationNotFound         = newErrorCode(71004, "好友关系不存在")
	FriendApplicationNotFound      = newErrorCode(71005, "好友申请不存在")
	FriendApplicationStateError    = newErrorCode(71006, "当前好友申请状态不允许此操作")

	GroupMemberInvalid    = newErrorCode(72000, "群成员不存在、状态异常或不是你的好友")
	GroupMemberLimitError = newErrorCode(72001, "群成员数量超过限制")
	GroupNotFound         = newErrorCode(72002, "群聊不存在或已失效")
	GroupInviteNoAuth     = newErrorCode(72003, "只有群主或管理员可以邀请成员")
	GroupKickNoAuth       = newErrorCode(72004, "无权移除该群成员")
	GroupOwnerCannotLeave = newErrorCode(72005, "群主需要先转让群主或解散群聊")
)

// 在某些情况下，直接终止调用栈，统一由外层中间件捕获
func ThrowIf(condition bool, code ErrorCode) {
	if condition {
		Throw(code)
	}
}

func ThrowIfCause(condition bool, code ErrorCode, cause error) {
	if condition {
		ThrowCause(code, cause)
	}
}

func Throw(code ErrorCode) {
	panic(code)
}

func ThrowCause(code ErrorCode, cause error) {
	panic(WrapError(code, cause))
}
