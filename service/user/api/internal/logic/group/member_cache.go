package group

import (
	"context"
	"log"
	"time"

	"infinitechatgo/service/user/api/internal/svc"
)

func notifyGroupMembersChanged(ctx context.Context, services *svc.ServiceContext, sessionID int64) {
	if services.GroupMemberInvalidator == nil {
		return
	}
	notifyCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	if err := services.GroupMemberInvalidator.Invalidate(notifyCtx, sessionID); err != nil {
		log.Printf("group member cache invalidation failed after transaction sessionId=%d: %v", sessionID, err)
	}
}
