package auth

//签名所需配置
type Config struct {
	Issuer        string
	AccessSecret  string
	AccessExpire  int64
	RefreshExpire int64
}
