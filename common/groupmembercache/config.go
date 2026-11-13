// Package groupmembercache caches only chat push recipient lists. It must never
// replace send authorization or red packet claim authorization.
package groupmembercache

import (
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	zeroredis "github.com/zeromicro/go-zero/core/stores/redis"
)

const DefaultChannel = "im:group-members:invalidate:v1"

type InvalidationConfig struct {
	Channel string `json:",optional"`
}

func (c InvalidationConfig) ChannelName() string {
	if strings.TrimSpace(c.Channel) == "" {
		return DefaultChannel
	}
	return strings.TrimSpace(c.Channel)
}

type Config struct {
	Disabled   bool   `json:",optional"`
	TTLSeconds int    `json:",optional"`
	Capacity   int    `json:",optional"`
	Channel    string `json:",optional"`
}

func (c Config) WithDefaults() (Config, error) {
	if c.TTLSeconds == 0 {
		c.TTLSeconds = 5
	}
	if c.Capacity == 0 {
		c.Capacity = 1024
	}
	c.Channel = (InvalidationConfig{Channel: c.Channel}).ChannelName()
	if c.TTLSeconds < 1 || c.TTLSeconds > 300 || c.Capacity < 1 || c.Capacity > 10000 {
		return c, fmt.Errorf("group member cache requires TTLSeconds in 1..300 and Capacity in 1..10000")
	}
	return c, nil
}

// NewRedisClient uses the same endpoint/authentication as the existing service
// Redis. This additional client owns the Pub/Sub connection, not business data.
func NewRedisClient(c zeroredis.RedisConf) (redis.UniversalClient, error) {
	if strings.TrimSpace(c.Host) == "" {
		return nil, fmt.Errorf("group member invalidation requires Redis.Host")
	}
	var tlsConfig *tls.Config
	if c.Tls {
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	switch c.Type {
	case "", zeroredis.NodeType:
		return redis.NewClient(&redis.Options{Addr: c.Host, Username: c.User, Password: c.Pass, TLSConfig: tlsConfig,
			DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second, ContextTimeoutEnabled: true}), nil
	case zeroredis.ClusterType:
		return redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(c.Host, ","), Username: c.User, Password: c.Pass, TLSConfig: tlsConfig,
			DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second, ContextTimeoutEnabled: true}), nil
	default:
		return nil, fmt.Errorf("unsupported group member invalidation Redis type %q", c.Type)
	}
}
