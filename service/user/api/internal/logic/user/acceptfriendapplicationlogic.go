// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package user

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/common"
	"infinitechatgo/common/txctx"
	commonutils "infinitechatgo/common/utils"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"
	"infinitechatgo/service/user/api/internal/types/constants"
	applyFriendModel "infinitechatgo/service/user/model/apply_friend"
	friendModel "infinitechatgo/service/user/model/friend"
	chatSessionModel "infinitechatgo/service/user/model/session"
	userSessionModel "infinitechatgo/service/user/model/user_session"

	"github.com/zeromicro/go-zero/core/logx"
)

type AcceptFriendApplicationLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewAcceptFriendApplicationLogic(ctx context.Context, svcCtx *svc.ServiceContext) *AcceptFriendApplicationLogic {
	return &AcceptFriendApplicationLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *AcceptFriendApplicationLogic) AcceptFriendApplication(req *types.FriendApplicationOperationRequest) (resp *types.AcceptFriendApplicationResponse, err error) {
	identity, ok := auth.IdentityFromContext(l.ctx)
	if !ok {
		return nil, common.NotLoginError
	}
	//
	var (
		application    *applyFriendModel.ApplyFriend
		firstRelation  *friendModel.Friend
		secondRelation *friendModel.Friend
		chatSession    *chatSessionModel.Session
		firstMember    *userSessionModel.UserSession
		secondMember   *userSessionModel.UserSession
	)
	err = txctx.WithTransaction(
		l.ctx,
		l.svcCtx.SqlConn,
		func(ctx context.Context) error {
			//确认申请
			lockedApplication, findErr :=
				l.svcCtx.ApplyFriendModel.FindOneForUpdateTx(
					ctx,
					req.ApplyFriendId,
				)
			if errors.Is(findErr, applyFriendModel.ErrNotFound) {
				return common.FriendApplicationNotFound
			}
			if findErr != nil {
				return common.WrapError(
					common.SystemError,
					findErr,
				)
			}
			if lockedApplication == nil {
				return common.FriendApplicationNotFound
			}

			application = lockedApplication
			if application.ReceiverId != identity.UserID {
				return common.NoAuthError
			}
			if !isPendingFriendApplication(application.Status) {
				return common.FriendApplicationStateError
			}

			activeUserIds, userErr :=
				l.svcCtx.UserModel.FindActiveUserIdsForUpdateTx(
					ctx,
					[]int64{
						application.SenderId,
						application.ReceiverId,
					},
				)
			if userErr != nil {
				return common.WrapError(
					common.SystemError,
					userErr,
				)
			}
			if len(activeUserIds) != 2 {
				return common.UserNotExists
			}

			now := time.Now()

			application.Status = constants.FriendApplicationStatusAccepted
			application.UpdatedTime = now
			//更新申请状态
			if updateErr :=
				l.svcCtx.ApplyFriendModel.UpdateTx(
					ctx,
					application,
				); updateErr != nil {
				return common.WrapError(
					common.SystemError,
					updateErr,
				)
			}
			//确认session与friend关系
			firstUserID := application.SenderId
			secondUserID := application.ReceiverId
			if firstUserID > secondUserID {
				firstUserID, secondUserID =
					secondUserID, firstUserID
			}
			var relationErr error
			firstRelation, relationErr =
				l.svcCtx.FriendModel.UpsertTx(
					ctx,
					&friendModel.Friend{
						UserId:      firstUserID,
						FriendId:    secondUserID,
						Status:      constants.FriendStatusNormal,
						CreatedTime: now,
						UpdatedTime: now,
					},
				)
			if relationErr != nil {
				return common.WrapError(
					common.SystemError,
					relationErr,
				)
			}

			secondRelation, relationErr =
				l.svcCtx.FriendModel.UpsertTx(
					ctx,
					&friendModel.Friend{
						UserId:      secondUserID,
						FriendId:    firstUserID,
						Status:      constants.FriendStatusNormal,
						CreatedTime: now,
						UpdatedTime: now,
					},
				)
			if relationErr != nil {
				return common.WrapError(
					common.SystemError,
					relationErr,
				)
			}

			//雪花算法设置sessionid
			candidateSessionID, idErr := commonutils.NextID()
			if idErr != nil {
				return common.WrapError(
					common.SystemError,
					idErr,
				)
			}
			singleKey := chatSessionModel.BuildSingleKey(
				application.SenderId,
				application.ReceiverId,
			)
			//更新或者插入session，同时获得真实的在数据库中的session id
			chatSession, relationErr =
				l.svcCtx.SessionModel.FindOrCreateSingleTx(
					ctx,
					&chatSessionModel.Session{
						SessionId: candidateSessionID,
						Name:      "",
						Type:      constants.SessionTypeSingle,
						SingleKey: sql.NullString{
							String: singleKey,
							Valid:  true,
						},
						Status: constants.SessionStatusNormal,
						Avatar: sql.NullString{
							Valid: false,
						},
						CreatedTime: now,
						UpdatedTime: now,
					},
				)
			if relationErr != nil {
				return common.WrapError(
					common.SystemError,
					relationErr,
				)
			}
			firstMember = &userSessionModel.UserSession{
				UserId:    firstUserID,
				SessionId: chatSession.SessionId,
				Role:      constants.UserSessionRoleMember,
				Status:    constants.UserSessionStatusNormal,
			}
			secondMember = &userSessionModel.UserSession{
				UserId:    secondUserID,
				SessionId: chatSession.SessionId,
				Role:      constants.UserSessionRoleMember,
				Status:    constants.UserSessionStatusNormal,
			}
			//更新session_user
			if memberErr :=
				l.svcCtx.UserSessionModel.UpsertTx(
					ctx,
					firstMember,
				); memberErr != nil {
				return common.WrapError(
					common.SystemError,
					memberErr,
				)
			}

			if memberErr :=
				l.svcCtx.UserSessionModel.UpsertTx(
					ctx,
					secondMember,
				); memberErr != nil {
				return common.WrapError(
					common.SystemError,
					memberErr,
				)
			}
			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	//事务提交后，清除旧的缓存
	if cacheErr := l.svcCtx.ApplyFriendModel.ClearCache(
		l.ctx,
		application,
	); cacheErr != nil {
		l.Errorf("清理好友申请缓存失败: %v", cacheErr)
	}

	if cacheErr := l.svcCtx.FriendModel.ClearCache(
		l.ctx,
		firstRelation,
	); cacheErr != nil {
		l.Errorf("清理第一条好友缓存失败: %v", cacheErr)
	}

	if cacheErr := l.svcCtx.FriendModel.ClearCache(
		l.ctx,
		secondRelation,
	); cacheErr != nil {
		l.Errorf("清理第二条好友缓存失败: %v", cacheErr)
	}

	if cacheErr := l.svcCtx.SessionModel.ClearCache(
		l.ctx,
		chatSession,
	); cacheErr != nil {
		l.Errorf("清理单聊会话缓存失败: %v", cacheErr)
	}

	if cacheErr := l.svcCtx.UserSessionModel.ClearCache(
		l.ctx,
		firstMember,
	); cacheErr != nil {
		l.Errorf("清理第一个会话成员缓存失败: %v", cacheErr)
	}

	if cacheErr := l.svcCtx.UserSessionModel.ClearCache(
		l.ctx,
		secondMember,
	); cacheErr != nil {
		l.Errorf("清理第二个会话成员缓存失败: %v", cacheErr)
	}
	return &types.AcceptFriendApplicationResponse{
		FriendId:  application.SenderId,
		SessionId: chatSession.SessionId,
	}, nil
}
