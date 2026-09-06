package common

type BaseResponse[T any] struct {
	Code    int    `json:"code"`
	Data    T      `json:"data"`
	Message string `json:"message"`
}

func NewBaseResponse[T any](
	code int,
	data T,
	message string,
) *BaseResponse[T] {
	return &BaseResponse[T]{
		Code:    code,
		Data:    data,
		Message: message,
	}
}