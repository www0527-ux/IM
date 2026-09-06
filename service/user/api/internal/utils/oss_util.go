package utils

import (
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func NewMinioClient(
	endpoint string,
	accessKey string,
	secretKey string,
	useSSL bool,
) (*minio.Client, error) {
	//minio地址,访问凭证，是否使用HTTPS
	return minio.New(
		endpoint,
		&minio.Options{
			Creds: credentials.NewStaticV4(
				accessKey,
				secretKey,
				"",
			),
			Secure: useSSL,
		},
	)
}
