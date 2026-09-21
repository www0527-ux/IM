package persistence

import (
	"context"
	"fmt"
	"strings"

	"infinitechatgo/common/idgen"
	messageapplication "infinitechatgo/service/message/application"
	sequencemodel "infinitechatgo/service/message/model/conversation_sequence"
	messagemodel "infinitechatgo/service/message/model/message"
	outboxmodel "infinitechatgo/service/message/model/outbox_event"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type Dependencies struct {
	SqlConn        sqlx.SqlConn
	IDGenerator    *idgen.Generator
	Messages       messagemodel.Model
	Sequences      sequencemodel.Model
	Outbox         outboxmodel.Model
	PersistedTopic string
}

type Writer struct {
	dependencies Dependencies
}

func NewWriter(dependencies Dependencies) (*Writer, error) {
	if dependencies.SqlConn == nil || dependencies.IDGenerator == nil ||
		dependencies.Messages == nil || dependencies.Sequences == nil ||
		dependencies.Outbox == nil ||
		strings.TrimSpace(dependencies.PersistedTopic) == "" {
		return nil, fmt.Errorf("invalid message persistence dependencies")
	}
	return &Writer{dependencies: dependencies}, nil
}

// Persist owns M6.3-M6.12 from redpacket-design.md.
//
// Preconditions:
//   - application.ValidateCommand succeeded;
//   - Redis did not provide an authoritative persisted-result hit;
//   - a red packet command carries the MessageID created by RedPacket API.
//
// Postconditions on success:
//   - an idempotent existing message is returned unchanged; or
//   - conversation_sequence, message and MESSAGE_PERSISTED Outbox commit in
//     one MySQL local transaction.
//
// Failure semantics:
//   - semantic reuse of an idempotency key returns ErrIdempotencyConflict;
//   - a definite rollback may discard generated Snowflake IDs;
//   - an unknown COMMIT result must be resolved by sender/client key before a
//     retry can generate another MessageID or Seq.
func (w *Writer) Persist(
	context.Context,
	messageapplication.Command,
) (messageapplication.Result, error) {
	// TODO(user): implement the learning-critical MySQL transaction here with
	// txctx.WithTransaction and the three transaction-aware models.
	return messageapplication.Result{}, messageapplication.ErrCoreNotImplemented
}
