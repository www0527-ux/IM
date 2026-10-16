package mysqlpool

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/core/conf"
)

func TestConfigurationDefaultsAndValidation(t *testing.T) {
	var c struct{ MySQLPool Config }
	if err := conf.LoadFromYamlBytes([]byte("{}"), &c); err != nil {
		t.Fatal(err)
	}
	if c.MySQLPool.MaxOpenConns != 16 || c.MySQLPool.MaxIdleConns != 8 {
		t.Fatalf("missing section should retain a bounded default: %+v", c)
	}
	if err := conf.LoadFromYamlBytes([]byte("MySQLPool:\n  MaxOpenConns: 4\n  MaxIdleConns: 0\n"), &c); err != nil {
		t.Fatal(err)
	}
	if c.MySQLPool.MaxOpenConns != 4 || c.MySQLPool.MaxIdleConns != 0 {
		t.Fatalf("explicit zero idle limit must be preserved: %+v", c)
	}
	for _, invalid := range []Config{{}, {MaxOpenConns: -1}, {MaxOpenConns: 2, MaxIdleConns: 3}, {MaxOpenConns: 2, MaxIdleConns: -1}} {
		if _, err := NewMysql("unused", invalid); err == nil {
			t.Fatalf("must reject invalid budget before opening a connection: %+v", invalid)
		}
	}
}

// A real database/sql pool verifies that excess demand waits and obeys context
// cancellation rather than opening connections beyond the configured budget.
func TestBudgetBoundsConnectionsAndAllowsCancellation(t *testing.T) {
	db := sql.OpenDB(poolTestConnector{})
	defer db.Close()
	Config{MaxOpenConns: 2, MaxIdleConns: 1}.apply(db)
	first, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := db.Conn(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("third request must wait until its deadline, got %v", err)
	}
	stats := db.Stats()
	if stats.MaxOpenConnections != 2 || stats.OpenConnections != 2 || stats.WaitCount != 1 || stats.WaitDuration <= 0 {
		t.Fatalf("unexpected budget or wait metrics: %+v", stats)
	}
	first.Close()
	second.Close()
	if db.Stats().Idle != 1 {
		t.Fatalf("idle connections exceed budget: %+v", db.Stats())
	}
}

type poolTestConnector struct{}

func (poolTestConnector) Connect(context.Context) (driver.Conn, error) { return poolTestConn{}, nil }
func (poolTestConnector) Driver() driver.Driver                        { return poolTestDriver{} }

type poolTestDriver struct{}

func (poolTestDriver) Open(string) (driver.Conn, error) { return poolTestConn{}, nil }

type poolTestConn struct{}

func (poolTestConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unused") }
func (poolTestConn) Close() error                        { return nil }
func (poolTestConn) Begin() (driver.Tx, error)           { return nil, errors.New("unused") }
