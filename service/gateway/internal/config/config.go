package config

type Upstreams struct {
	UserAPI      string
	OfflineAPI   string
	RedPacketAPI string
	Realtime     string
}

type Config struct {
	ListenOn  string
	StaticDir string `json:",optional"`
	Upstreams Upstreams
}
