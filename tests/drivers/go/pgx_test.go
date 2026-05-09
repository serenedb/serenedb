package drivers

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/serenedb/drivers-go/spec"
)

const driverKey = "go_pgx"

var schemaSetUp string

func setup(t *testing.T) *pgx.Conn {
	t.Helper()
	cfg, err := pgx.ParseConfig(spec.ConnString(false))
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	c, err := pgx.ConnectConfig(context.Background(), cfg)
	if err != nil {
		t.Skipf("cannot connect: %v", err)
	}
	if schemaSetUp == "" {
		schemaSetUp = spec.SchemaName(driverKey)
		ctx := context.Background()
		if _, err := c.Exec(ctx,
			fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %q`, schemaSetUp)); err != nil {
			t.Fatalf("create schema: %v", err)
		}
		if _, err := c.Exec(ctx,
			fmt.Sprintf(`SET search_path TO %q, public, pg_catalog`, schemaSetUp)); err != nil {
			t.Fatalf("set search_path: %v", err)
		}
	}
	return c
}

func TestSmokeSelectOne(t *testing.T) {
	c := setup(t)
	defer c.Close(context.Background())
	var v int
	if err := c.QueryRow(context.Background(), "SELECT 1").Scan(&v); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if v != 1 {
		t.Fatalf("got %d", v)
	}
}

func TestSmokeServerVersion(t *testing.T) {
	c := setup(t)
	defer c.Close(context.Background())
	var s string
	if err := c.QueryRow(context.Background(), "SHOW server_version").Scan(&s); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if s == "" {
		t.Fatalf("empty server_version")
	}
}

func serverText(t *testing.T, c *pgx.Conn, pgTypname string, sample interface{}) (string, bool) {
	t.Helper()
	if sample == nil {
		return "", true
	}
	var s string
	q := fmt.Sprintf(`SELECT ($1::text)::%s::text`, pgTypname)
	if err := c.QueryRow(context.Background(), q, fmt.Sprintf("%v", sample)).Scan(&s); err != nil {
		t.Fatalf("expected: %v", err)
	}
	return s, false
}

func TestRoundtrip(t *testing.T) {
	c := setup(t)
	defer c.Close(context.Background())

	cases, err := spec.CasesFor(driverKey)
	if err != nil {
		t.Fatalf("cases: %v", err)
	}
	if len(cases) == 0 {
		t.Skipf("no cases (SDB_DRV_TYPES=%q?)", os.Getenv("SDB_DRV_TYPES"))
	}

	for _, cs := range cases {
		cs := cs
		name := fmt.Sprintf("%d-%s-%s-%d",
			cs.Type.OID, cs.Type.Name, cs.Protocol, cs.Idx)
		t.Run(name, func(t *testing.T) {
			expected, isNull := serverText(t, c, cs.Type.PgTypname, cs.Sample)
			var actualStr string
			var actualNull bool
			ctx := context.Background()

			runInline := func() {
				var sql string
				if cs.Sample == nil {
					sql = fmt.Sprintf("SELECT NULL::%s::text", cs.Type.PgTypname)
				} else {
					lit := strings.ReplaceAll(fmt.Sprintf("%v", cs.Sample), "'", "''")
					sql = fmt.Sprintf("SELECT '%s'::%s::text", lit, cs.Type.PgTypname)
				}
				var out *string
				if err := c.QueryRow(ctx, sql).Scan(&out); err != nil {
					t.Fatalf("inline: %v", err)
				}
				if out == nil {
					actualNull = true
				} else {
					actualStr = *out
				}
			}

			runBound := func() {
				sql := fmt.Sprintf("SELECT (%s)::text", cs.Type.CastSQL)
				var out *string
				var err error
				if cs.Sample == nil {
					err = c.QueryRow(ctx, sql, nil).Scan(&out)
				} else {
					err = c.QueryRow(ctx, sql, fmt.Sprintf("%v", cs.Sample)).Scan(&out)
				}
				if err != nil {
					t.Fatalf("bound: %v", err)
				}
				if out == nil {
					actualNull = true
				} else {
					actualStr = *out
				}
			}

			switch cs.Protocol {
			case "simple", "extended-noparam":
				runInline()
			case "extended-text", "extended-binary":
				runBound()
			default:
				t.Fatalf("unknown protocol %q", cs.Protocol)
			}

			if isNull != actualNull {
				t.Fatalf("null mismatch: expected=%v got=%v", isNull, actualNull)
			}
			if !isNull && actualStr != expected {
				t.Fatalf("mismatch: expected %q got %q", expected, actualStr)
			}
		})
	}
}
