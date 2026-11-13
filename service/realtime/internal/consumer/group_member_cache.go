package consumer

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"infinitechatgo/common/groupmembercache"
	"infinitechatgo/service/user/rpc/userrpc"
)

type CachedGroupMembers struct {
	upstream groupMemberProvider
	local    *groupmembercache.Local
}

func NewCachedGroupMembers(upstream groupMemberProvider, local *groupmembercache.Local) *CachedGroupMembers {
	return &CachedGroupMembers{upstream: upstream, local: local}
}
func (c *CachedGroupMembers) GetGroupMemberIds(ctx context.Context, in *userrpc.SessionIdReq, opts ...grpc.CallOption) (*userrpc.GroupMemberIdsResp, error) {
	if c == nil || c.upstream == nil || c.local == nil || in == nil {
		return nil, fmt.Errorf("cached group member provider dependencies are required")
	}
	ids, err := c.local.Get(ctx, in.SessionId, func(ctx context.Context, id int64) ([]int64, error) {
		response, err := c.upstream.GetGroupMemberIds(ctx, &userrpc.SessionIdReq{SessionId: id}, opts...)
		if err != nil {
			return nil, err
		}
		if response == nil {
			return nil, fmt.Errorf("group member RPC returned nil response")
		}
		return response.UserIds, nil
	})
	if err != nil {
		return nil, err
	}
	return &userrpc.GroupMemberIdsResp{UserIds: ids}, nil
}
