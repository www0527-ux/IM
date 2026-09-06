// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package user

import (
	"context"
	"fmt"
	"time"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/common"
	commonutils "infinitechatgo/common/utils"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetUploadUrlLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetUploadUrlLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUploadUrlLogic {
	return &GetUploadUrlLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetUploadUrlLogic) GetUploadUrl(req *types.GetUploadUrlRequest) (resp *types.UploadUrlResponse, err error) {
	var avatarExtensions = map[string]string{
		"image/jpeg": ".jpg",
		"image/png":  ".png",
		"image/webp": ".webp",
	}
	extension, ok := avatarExtensions[req.ContentType]
	if !ok {
		return nil, common.ParamsError
	}
	identity, ok := auth.IdentityFromContext(l.ctx)
	if !ok {
		return nil, common.NotLoginError
	}
	objectID, err := commonutils.NextID()
	if err != nil {
		return nil, common.WrapError(
			common.SystemError,
			err,
		)
	}
	objectName := fmt.Sprintf(
		"avatars/%d/%d%s",
		identity.UserID,
		objectID,
		extension,
	)
	//生成预签名的URL
	uploadURL, err := l.svcCtx.MinIOClient.PresignedPutObject(
		l.ctx,
		l.svcCtx.Config.Minio.Bucket,
		objectName,
		5*time.Minute,
	)
	if err != nil {
		return nil, common.WrapError(
			common.SystemError,
			err,
		)
	}
	//拼接下载地址
	scheme := "http"
	if l.svcCtx.Config.Minio.UseSSL {
		scheme = "https"
	}

	downloadURL := fmt.Sprintf(
		"%s://%s/%s/%s",
		scheme,
		l.svcCtx.Config.Minio.Endpoint,
		l.svcCtx.Config.Minio.Bucket,
		objectName,
	)
	return &types.UploadUrlResponse{
		UploadUrl:   uploadURL.String(),
		DownloadUrl: downloadURL,
	}, nil
}
