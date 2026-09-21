package idgen

import (
	"fmt"

	"github.com/bwmarrin/snowflake"
)

const (
	minNodeID int64 = 1
	maxNodeID int64 = 1023
)

// Config identifies one Snowflake generator instance. NodeID must be unique
// among all concurrently running processes that share the same ID namespace.
type Config struct {
	NodeID int64
}

type Generator struct {
	node *snowflake.Node
}

func New(config Config) (*Generator, error) {
	if config.NodeID < minNodeID || config.NodeID > maxNodeID {
		return nil, fmt.Errorf(
			"snowflake node id must be between %d and %d",
			minNodeID,
			maxNodeID,
		)
	}

	node, err := snowflake.NewNode(config.NodeID)
	if err != nil {
		return nil, fmt.Errorf("create snowflake node: %w", err)
	}

	return &Generator{node: node}, nil
}

func MustNew(config Config) *Generator {
	generator, err := New(config)
	if err != nil {
		panic(err)
	}
	return generator
}

func (g *Generator) NextID() (int64, error) {
	if g == nil || g.node == nil {
		return 0, fmt.Errorf("snowflake generator is not initialized")
	}
	return g.node.Generate().Int64(), nil
}
