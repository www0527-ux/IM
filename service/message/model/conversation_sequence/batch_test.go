package conversation_sequence

import (
	"context"
	"math"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"infinitechatgo/common/txctx"
)

func TestBulkCounterRejectsOverflowAndMissingRowsWithoutUpdates(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "overflow", true: "missing"}[missing], func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			conn := sqlx.NewSqlConnFromDB(db)
			model := NewModel(conn)
			counts := map[int64]int64{10: 2, 20: 1}
			if _, err := model.LockAndAddManyTx(context.Background(), counts); err == nil {
				t.Fatal("allocated outside transaction")
			}
			mock.ExpectBegin()
			mock.ExpectExec("INSERT INTO conversation_sequence").WithArgs(int64(10), int64(20)).WillReturnResult(sqlmock.NewResult(0, 0))
			rows := sqlmock.NewRows([]string{"session_id", "last_seq"}).AddRow(int64(10), int64(math.MaxInt64-1))
			if !missing {
				rows.AddRow(int64(20), int64(5))
			}
			mock.ExpectQuery("SELECT session_id,last_seq").WithArgs(int64(10), int64(20)).WillReturnRows(rows)
			mock.ExpectRollback()
			err = txctx.WithTransaction(context.Background(), conn, func(ctx context.Context) error {
				_, err := model.LockAndAddManyTx(ctx, counts)
				return err
			})
			if err == nil {
				t.Fatal("invalid counters accepted")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
