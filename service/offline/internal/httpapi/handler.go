package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/common"
	"infinitechatgo/common/utils"
	"infinitechatgo/service/offline/internal/config"
	"infinitechatgo/service/offline/internal/svc"
	"infinitechatgo/service/offline/internal/syncer"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest/httpx"
)

func SyncHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		identity, ok := auth.IdentityFromContext(r.Context())
		if !ok {
			writeError(r, w, http.StatusUnauthorized, common.NotLoginError)
			return
		}

		var request SyncRequest
		decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil {
			writeError(r, w, http.StatusBadRequest, common.ParamsError)
			return
		}
		if err := ensureJSONEnd(decoder); err != nil ||
			!validSyncRequest(request, svcCtx.Config) {
			writeError(r, w, http.StatusBadRequest, common.ParamsError)
			return
		}

		limit := request.Limit
		if limit == 0 {
			limit = svcCtx.Config.Sync.DefaultLimit
		}
		response := SyncResponse{
			Conversations: make([]ConversationSyncResult, 0, len(request.Conversations)),
		}
		for _, cursor := range request.Conversations {
			result, err := svcCtx.SyncReader.Sync(
				r.Context(),
				identity.UserID,
				cursor.SessionID,
				cursor.AfterSeq,
				limit,
			)
			if errors.Is(err, syncer.ErrSessionAccessDenied) {
				writeError(r, w, http.StatusForbidden, common.NoAuthError)
				return
			}
			if err != nil {
				logx.WithContext(r.Context()).Errorf(
					"sync offline messages failed: userId=%d sessionId=%d err=%v",
					identity.UserID,
					cursor.SessionID,
					err,
				)
				writeError(r, w, http.StatusInternalServerError, common.SystemError)
				return
			}

			response.Conversations = append(
				response.Conversations,
				ConversationSyncResult{
					SessionID:    result.SessionID,
					ServerMaxSeq: result.ServerMaxSeq,
					NextSeq:      result.NextSeq,
					HasMore:      result.HasMore,
					Messages:     result.Messages,
				},
			)
		}

		httpx.WriteJsonCtx(
			r.Context(),
			w,
			http.StatusOK,
			utils.Success(response),
		)
	}
}

func ensureJSONEnd(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("request body must contain exactly one JSON value")
	}
	return nil
}

func validSyncRequest(request SyncRequest, c config.Config) bool {
	if len(request.Conversations) == 0 ||
		len(request.Conversations) > c.Sync.MaxConversations ||
		request.Limit < 0 ||
		request.Limit > c.Sync.MaxLimit {
		return false
	}

	seen := make(map[int64]struct{}, len(request.Conversations))
	for _, cursor := range request.Conversations {
		if cursor.SessionID <= 0 || cursor.AfterSeq < 0 {
			return false
		}
		if _, exists := seen[cursor.SessionID]; exists {
			return false
		}
		seen[cursor.SessionID] = struct{}{}
	}
	return true
}
