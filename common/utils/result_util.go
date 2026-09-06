package utils

import "infinitechatgo/common/common"

func Success[T any](data T) *common.BaseResponse[T] {
	return common.NewBaseResponse(200, data, "ok")
}

func FailWithCode(code int, message string) *common.BaseResponse[any] {
	return common.NewBaseResponse[any](code, nil, message)
}
func Fail(errorCode common.ErrorCode) *common.BaseResponse[any] {
	return FailWithCode(errorCode.Code, errorCode.Message)
}
