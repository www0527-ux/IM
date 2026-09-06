// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package user

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"infinitechatgo/common/common"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"
	"infinitechatgo/service/user/api/internal/types/constants"
	userModel "infinitechatgo/service/user/model/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type SearchUserLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewSearchUserLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SearchUserLogic {
	return &SearchUserLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *SearchUserLogic) SearchUser(req *types.SearchUserRequest) (resp *types.SearchUserResponse, err error) {
	keyword := strings.TrimSpace(req.Keyword)
	var (
		foundUser *userModel.User
		findErr   error
	)

	switch {
	case constants.EmailRegex.MatchString(keyword):
		keyword = strings.ToLower(keyword)
		foundUser, findErr = l.svcCtx.UserModel.FindOneByEmail(
			l.ctx,
			sql.NullString{
				String: keyword,
				Valid:  true,
			},
		)

	case constants.PhoneRegex.MatchString(keyword):
		foundUser, findErr = l.svcCtx.UserModel.FindOneByPhone(
			l.ctx,
			sql.NullString{
				String: keyword,
				Valid:  true,
			},
		)

	default:
		return nil, common.PhoneEmailError
	}
	if errors.Is(findErr, userModel.ErrNotFound) {
		return nil, common.UserNotExists
	}
	if findErr != nil {
		return nil, common.WrapError(
			common.SystemError,
			findErr,
		)
	}

	if foundUser.IsDelete == 1 || foundUser.State == 2 {
		return nil, common.UserNotExists
	}

	return &types.SearchUserResponse{
		UserId:      foundUser.UserId,
		Nickname:    foundUser.Nickname,
		Avatar:      foundUser.Avatar.String,
		Description: foundUser.Description.String,
		Gender:      foundUser.Gender,
	}, nil
}
