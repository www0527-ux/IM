package group

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"infinitechatgo/common/auth"
	"infinitechatgo/common/idgen"
	"infinitechatgo/common/txctx"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"
	"infinitechatgo/service/user/api/internal/types/constants"
	friendmodel "infinitechatgo/service/user/model/friend"
	sessionmodel "infinitechatgo/service/user/model/session"
	membermodel "infinitechatgo/service/user/model/user_session"
)

// These fakes leave existing membership SQL untouched; the tests verify the
// new post-transaction notification placement and failure boundaries only.
type cacheSessionModel struct {
	sessionmodel.SessionModel
	writeErr error
}

func (m cacheSessionModel) FindOneForUpdateTx(context.Context, int64) (*sessionmodel.Session, error) {
	return &sessionmodel.Session{Type: constants.SessionTypeGroup, Status: constants.SessionStatusNormal}, nil
}
func (m cacheSessionModel) InsertTx(context.Context, *sessionmodel.Session) (sql.Result, error) {
	return sqlmock.NewResult(1, 1), m.writeErr
}
func (cacheSessionModel) ClearCache(context.Context, *sessionmodel.Session) error {
	return errors.New("unrelated model cache failure")
}

type cacheMemberModel struct {
	membermodel.UserSessionModel
	writeErr  error
	unchanged bool
	leaving   bool
}

func (m cacheMemberModel) FindOneForUpdateTx(_ context.Context, userID, sessionID int64) (*membermodel.UserSession, error) {
	role := int64(constants.UserSessionRoleMember)
	if userID == 11 && !m.leaving {
		role = constants.UserSessionRoleOwner
	}
	status := int64(constants.UserSessionStatusNormal)
	if m.unchanged && (userID != 11 || m.leaving) {
		status = constants.UserSessionStatusExited
	}
	return &membermodel.UserSession{UserId: userID, SessionId: sessionID, Role: role, Status: status}, nil
}
func (m cacheMemberModel) FindByUserIdsForUpdateTx(context.Context, int64, []int64) ([]*membermodel.UserSession, error) {
	return nil, nil
}
func (m cacheMemberModel) InviteMemberTx(context.Context, *membermodel.UserSession) (bool, error) {
	return !m.unchanged, m.writeErr
}
func (m cacheMemberModel) UpsertTx(context.Context, *membermodel.UserSession) error {
	return m.writeErr
}
func (m cacheMemberModel) UpdateStatusTx(context.Context, int64, int64, int64) error {
	return m.writeErr
}
func (cacheMemberModel) ClearCache(context.Context, *membermodel.UserSession) error {
	return errors.New("unrelated model cache failure")
}

type cacheFriendModel struct{ friendmodel.FriendModel }

func (cacheFriendModel) FindNormalFriendIds(_ context.Context, _ int64, ids []int64) ([]int64, error) {
	return ids, nil
}
func (cacheFriendModel) FindNormalFriendIdsForUpdateTx(_ context.Context, _ int64, ids []int64) ([]int64, error) {
	return ids, nil
}

type cacheNotifier struct {
	db         *sql.DB
	calls      int
	sessionID  int64
	orderErr   error
	publishErr error
}

func (n *cacheNotifier) Invalidate(ctx context.Context, sessionID int64) error {
	n.calls++
	n.sessionID = sessionID
	if txctx.GetSession(ctx) != nil {
		n.orderErr = errors.New("notification inside SQL transaction")
		return n.orderErr
	}
	var value int
	n.orderErr = n.db.QueryRowContext(ctx, "SELECT 1").Scan(&value)
	if n.orderErr != nil {
		return n.orderErr
	}
	return n.publishErr
}

func TestMembershipInvalidationOnlyAfterSuccessfulCommit(t *testing.T) {
	for _, operation := range []string{"create", "invite", "leave", "kick"} {
		for _, outcome := range []string{"commit", "rollback", "unknown-commit", "publish-failed", "unchanged"} {
			if operation == "create" && outcome == "unchanged" {
				continue
			}
			t.Run(operation+"/"+outcome, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				notifier := &cacheNotifier{db: db}
				var writeErr error
				if outcome == "rollback" {
					writeErr = errors.New("membership update failed")
				}
				if outcome == "publish-failed" {
					notifier.publishErr = errors.New("Redis unavailable")
				}
				services := &svc.ServiceContext{SqlConn: sqlx.NewSqlConnFromDB(db), IDGenerator: idgen.MustNew(idgen.Config{NodeID: 91}),
					SessionModel:     cacheSessionModel{writeErr: writeErr},
					UserSessionModel: cacheMemberModel{writeErr: writeErr, unchanged: outcome == "unchanged", leaving: operation == "leave"},
					FriendModel:      cacheFriendModel{}, GroupMemberInvalidator: notifier}
				mock.ExpectBegin()
				switch outcome {
				case "rollback":
					mock.ExpectRollback()
				case "unknown-commit":
					mock.ExpectCommit().WillReturnError(errors.New("commit result unknown"))
				default:
					mock.ExpectCommit()
				}
				wantNotify := outcome == "commit" || outcome == "publish-failed"
				if wantNotify {
					mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
				}
				ctx := auth.ContextWithIdentity(context.Background(), auth.Identity{UserID: 11})
				switch operation {
				case "create":
					_, err = NewCreateGroupLogic(ctx, services).CreateGroup(&types.CreateGroupRequest{MemberIds: []int64{22}})
				case "invite":
					_, err = NewInviteGroupMembersLogic(ctx, services).InviteGroupMembers(&types.InviteGroupMembersRequest{SessionId: 99, MemberIds: []int64{22}})
				case "leave":
					_, err = NewLeaveGroupLogic(ctx, services).LeaveGroup(&types.LeaveGroupRequest{SessionId: 99})
				case "kick":
					_, err = NewKickGroupMemberLogic(ctx, services).KickGroupMember(&types.KickGroupMemberRequest{SessionId: 99, MemberId: 22})
				}
				wantError := outcome == "rollback" || outcome == "unknown-commit"
				if (err != nil) != wantError {
					t.Fatalf("err=%v wantError=%v", err, wantError)
				}
				if (notifier.calls == 1) != wantNotify || notifier.orderErr != nil {
					t.Fatalf("notification calls=%d orderErr=%v", notifier.calls, notifier.orderErr)
				}
				if wantNotify && (notifier.sessionID <= 0 || operation != "create" && notifier.sessionID != 99) {
					t.Fatal(fmt.Sprintf("notified session=%d", notifier.sessionID))
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
