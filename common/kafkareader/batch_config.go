package kafkareader

import "fmt"

type BatchCommitConfig struct {
	Disabled       bool `json:",optional"`
	BatchSize      int  `json:",optional"`
	IntervalMillis int  `json:",optional"`
}

func (c BatchCommitConfig) WithDefaults() (BatchCommitConfig, error) {
	if c.BatchSize == 0 {
		c.BatchSize = 32
	}
	if c.IntervalMillis == 0 {
		c.IntervalMillis = 10
	}
	if c.BatchSize < 1 || c.BatchSize > 512 || c.IntervalMillis < 1 || c.IntervalMillis > 1000 {
		return c, fmt.Errorf("invalid consumer batch commit size or interval")
	}
	return c, nil
}
