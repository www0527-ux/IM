package middleware

import (
	"fmt"
	"net/http"

	"infinitechatgo/common/common"
	"infinitechatgo/common/utils"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest/httpx"
)

func RecoverMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			recovered := recover()
			if recovered == nil {
				return
			}

			switch value := recovered.(type) {
			case common.ErrorCode:
				// 系统错误交给 SetErrorHandlerCtx 统一记录和包装。
				if value.Code >= 50000 && value.Code < 60000 {
					httpx.ErrorCtx(r.Context(), w, value)
					return
				}

				// 业务错误已经是失败响应，直接写 JSON，避免再次经过 SetOkHandler。
				httpx.WriteJsonCtx(
					r.Context(),
					w,
					http.StatusOK,
					utils.Fail(value),
				)

			case error:
				logx.WithContext(r.Context()).Errorf("unexpected panic: %v", value)
				httpx.ErrorCtx(
					r.Context(),
					w,
					common.WrapError(common.SystemError, value),
				)

			default:
				cause := fmt.Errorf("panic: %v", value)
				logx.WithContext(r.Context()).Errorf("unexpected panic: %v", value)
				httpx.ErrorCtx(
					r.Context(),
					w,
					common.WrapError(common.SystemError, cause),
				)
			}
		}()

		next(w, r)
	}
}
