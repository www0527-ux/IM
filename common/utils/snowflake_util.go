package utils

import (
	"sync"

	"github.com/bwmarrin/snowflake"
)

const snowflakeNodeID int64 = 1

//Node 为雪花序列生成器，once确保懒加载依旧只执行依次
var (
	snowflakeNode *snowflake.Node
	snowflakeErr  error
	snowflakeOnce sync.Once
)

func NextID() (int64, error) {
	snowflakeOnce.Do(func() {
		snowflakeNode, snowflakeErr = snowflake.NewNode(
			snowflakeNodeID,
		)
	})
	if snowflakeErr != nil {
		return 0, snowflakeErr
	}
	return snowflakeNode.Generate().Int64(), nil
}
