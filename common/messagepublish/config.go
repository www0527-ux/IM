package messagepublish

import "fmt"

type Config struct {
	BatchSize     int `json:",optional"`
	FlushMillis   int `json:",optional"`
	QueueCapacity int `json:",optional"`
}

func (c Config) WithDefaults() (Config, error) {
	if c.BatchSize == 0 {
		c.BatchSize = 32
	}
	if c.FlushMillis == 0 {
		c.FlushMillis = 1
	}
	if c.QueueCapacity == 0 {
		c.QueueCapacity = 512
	}
	if c.BatchSize < 1 || c.BatchSize > 512 || c.FlushMillis < 1 || c.FlushMillis > 100 || c.QueueCapacity < 1 || c.QueueCapacity > 10000 {
		return c, fmt.Errorf("invalid publisher batch size, flush interval or queue capacity")
	}
	return c, nil
}
