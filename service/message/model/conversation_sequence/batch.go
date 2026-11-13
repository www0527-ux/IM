package conversation_sequence

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	"infinitechatgo/common/txctx"
)

// LockAndAddManyTx amortizes SQL round trips across a shard's conversations.
// Sorted multi-row upsert takes counter locks first; the final message INSERT
// and these updates still belong to the caller's one transaction.
func (m *model) LockAndAddManyTx(ctx context.Context, counts map[int64]int64) (map[int64]int64, error) {
	if len(counts) == 0 || len(counts) > 128 {
		return nil, fmt.Errorf("invalid conversation counter batch size")
	}
	ids := make([]int64, 0, len(counts))
	for id, count := range counts {
		if id <= 0 || count <= 0 {
			return nil, fmt.Errorf("invalid conversation counter batch entry")
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	conn, err := txctx.SessionConn(ctx)
	if err != nil {
		return nil, err
	}
	values, placeholders := make([]string, len(ids)), make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		values[i] = "(?,0)"
		placeholders[i] = "?"
		args[i] = id
	}
	query := "INSERT INTO conversation_sequence(session_id,last_seq) VALUES " + strings.Join(values, ",") + " ON DUPLICATE KEY UPDATE session_id=session_id"
	if _, err := conn.ExecCtx(ctx, query, args...); err != nil {
		return nil, fmt.Errorf("ensure batch conversation counters: %w", err)
	}
	var rows []struct {
		SessionID int64 `db:"session_id"`
		LastSeq   int64 `db:"last_seq"`
	}
	query = "SELECT session_id,last_seq FROM conversation_sequence WHERE session_id IN (" + strings.Join(placeholders, ",") + ") ORDER BY session_id FOR UPDATE"
	if err := conn.QueryRowsCtx(ctx, &rows, query, args...); err != nil {
		return nil, fmt.Errorf("lock batch conversation counters: %w", err)
	}
	if len(rows) != len(ids) {
		return nil, ErrRowMissing
	}
	result := make(map[int64]int64, len(ids))
	updateArgs := make([]any, 0, len(ids)*3)
	var cases strings.Builder
	for i, row := range rows {
		count := counts[row.SessionID]
		if row.SessionID != ids[i] || count <= 0 || row.LastSeq < 0 || row.LastSeq > math.MaxInt64-count {
			return nil, fmt.Errorf("invalid or exhausted batch conversation counter: session_id=%d", row.SessionID)
		}
		result[row.SessionID] = row.LastSeq + count
		cases.WriteString(" WHEN ? THEN ?")
		updateArgs = append(updateArgs, row.SessionID, row.LastSeq+count)
	}
	updateArgs = append(updateArgs, args...)
	query = "UPDATE conversation_sequence SET last_seq=CASE session_id" + cases.String() + " ELSE last_seq END WHERE session_id IN (" + strings.Join(placeholders, ",") + ")"
	updated, err := conn.ExecCtx(ctx, query, updateArgs...)
	if err != nil {
		return nil, fmt.Errorf("update batch conversation counters: %w", err)
	}
	affected, err := updated.RowsAffected()
	if err != nil {
		return nil, err
	}
	if affected != int64(len(ids)) {
		return nil, fmt.Errorf("batch counters updated %d rows, expected %d", affected, len(ids))
	}
	return result, nil
}
