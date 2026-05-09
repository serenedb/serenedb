package drivers

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	_ "github.com/lib/pq"
	"github.com/serenedb/drivers-go/spec"
)

const libpqDriverKey = "go_libpq"

func libpqOpen(t *testing.T) *sql.DB {
	t.Helper()
	host := getEnvOr("SDB_DRV_HOST", "localhost")
	port := getEnvOr("SDB_DRV_PORT", "5432")
	db := getEnvOr("SDB_DRV_DATABASE", "postgres")
	user := getEnvOr("SDB_DRV_USER", "postgres")
	dsn := fmt.Sprintf("host=%s port=%s dbname=%s user=%s sslmode=disable",
		host, port, db, user)
	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skipf("cannot open: %v", err)
	}
	if err := conn.Ping(); err != nil {
		t.Skipf("cannot ping: %v", err)
	}
	schema := spec.SchemaName(libpqDriverKey)
	if _, err := conn.Exec(fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %q`, schema)); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := conn.Exec(fmt.Sprintf(`SET search_path TO %q, public, pg_catalog`, schema)); err != nil {
		t.Fatalf("set search_path: %v", err)
	}
	return conn
}

func TestLibpqSmokeSelectOne(t *testing.T) {
	c := libpqOpen(t)
	defer c.Close()
	var v int
	if err := c.QueryRowContext(context.Background(), "SELECT 1").Scan(&v); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if v != 1 {
		t.Fatalf("got %d", v)
	}
}

func TestLibpqRoundtrip(t *testing.T) {
	c := libpqOpen(t)
	defer c.Close()
	cases, err := spec.CasesFor(libpqDriverKey)
	if err != nil {
		t.Fatalf("cases: %v", err)
	}
	if len(cases) == 0 {
		t.Skip("no cases")
	}
	for _, cs := range cases {
		cs := cs
		name := fmt.Sprintf("%d-%s-%s-%d",
			cs.Type.OID, cs.Type.Name, cs.Protocol, cs.Idx)
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			expected, isNull := libpqServerText(t, c, cs.Type.PgTypname, cs.Sample)
			var actual sql.NullString
			switch cs.Protocol {
			case "simple", "extended-noparam":
				var sqlStr string
				if cs.Sample == nil {
					sqlStr = fmt.Sprintf("SELECT NULL::%s::text", cs.Type.PgTypname)
				} else {
					lit := strings.ReplaceAll(fmt.Sprintf("%v", cs.Sample), "'", "''")
					sqlStr = fmt.Sprintf("SELECT '%s'::%s::text", lit, cs.Type.PgTypname)
				}
				if err := c.QueryRowContext(ctx, sqlStr).Scan(&actual); err != nil {
					t.Fatalf("inline: %v", err)
				}
			case "extended-text", "extended-binary":
				cast := strings.Replace(cs.Type.CastSQL, "$1", "$1::text", 1)
				sqlStr := fmt.Sprintf("SELECT (%s)::text", cast)
				var arg interface{}
				if cs.Sample != nil {
					arg = fmt.Sprintf("%v", cs.Sample)
				}
				if err := c.QueryRowContext(ctx, sqlStr, arg).Scan(&actual); err != nil {
					t.Fatalf("bound: %v", err)
				}
			}
			if isNull && actual.Valid {
				t.Fatalf("expected NULL got %q", actual.String)
			}
			if !isNull && (!actual.Valid || actual.String != expected) {
				t.Fatalf("expected %q got %q (valid=%v)", expected, actual.String, actual.Valid)
			}
		})
	}
}

func libpqServerText(t *testing.T, c *sql.DB, pgType string, s interface{}) (string, bool) {
	t.Helper()
	if s == nil {
		return "", true
	}
	q := fmt.Sprintf(`SELECT (($1::text)::%s)::text`, pgType)
	var out sql.NullString
	if err := c.QueryRow(q, fmt.Sprintf("%v", s)).Scan(&out); err != nil {
		t.Fatalf("expected: %v", err)
	}
	if !out.Valid {
		return "", true
	}
	return out.String, false
}

func getEnvOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
