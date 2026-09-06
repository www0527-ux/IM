// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package user

import (
	"net/http"

	"github.com/zeromicro/go-zero/rest/httpx"
	"infinitechatgo/service/user/api/internal/logic/user"
	"infinitechatgo/service/user/api/internal/svc"
)

func GetUnreadFriendApplicationCountHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := user.NewGetUnreadFriendApplicationCountLogic(r.Context(), svcCtx)
		resp, err := l.GetUnreadFriendApplicationCount()
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
