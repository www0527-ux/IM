// Package mysqlpool applies a per-process connection budget to go-zero's shared
// pool before models use it. All models in a service must reuse the returned conn.
package mysqlpool

import (
	"database/sql"
	"fmt"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type Config struct {
	MaxOpenConns int `json:",default=16"`
	MaxIdleConns int `json:",default=8"`
}

func (c Config) Validate() error {
	if c.MaxOpenConns <= 0 || c.MaxIdleConns < 0 || c.MaxIdleConns > c.MaxOpenConns {
		return fmt.Errorf("MySQLPool requires MaxOpenConns > 0 and 0 <= MaxIdleConns <= MaxOpenConns")
	}
	return nil
}

func (c Config) apply(db *sql.DB) {
	db.SetMaxOpenConns(c.MaxOpenConns)
	db.SetMaxIdleConns(c.MaxIdleConns)
}

func NewMysql(dataSource string, c Config) (sqlx.SqlConn, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	conn := sqlx.NewMysql(dataSource)
	db, err := conn.RawDB()
	if err != nil {
		return nil, fmt.Errorf("initialize MySQL pool: %w", err)
	}
	c.apply(db)
	return conn, nil
}

func MustNewMysql(dataSource string, c Config) sqlx.SqlConn {
	conn, err := NewMysql(dataSource, c)
	if err != nil {
		panic(err)
	}
	return conn
}
