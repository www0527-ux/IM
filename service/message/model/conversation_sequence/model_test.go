package conversation_sequence

import (
	"context"
	"math"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"infinitechatgo/common/txctx"
)

func TestAllocationRequiresTransactionAndRollsBackOverflow(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn := sqlx.NewSqlConnFromDB(db)
	m := NewModel(conn)
	if _, err := m.LockAndAddTx(context.Background(), 1, 2); err == nil {
		t.Fatal("allocated outside a transaction")
	}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT last_seq").WillReturnRows(sqlmock.NewRows([]string{"last_seq"}).AddRow(int64(math.MaxInt64 - 1)))
	mock.ExpectRollback()
	err = txctx.WithTransaction(context.Background(), conn, func(ctx context.Context) error {
		_, err := m.LockAndAddTx(ctx, 1, 2)
		return err
	})
	if err == nil {
		t.Fatal("overflow was accepted")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
