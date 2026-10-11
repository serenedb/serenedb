package drivers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func pgxConnect(t *testing.T, url string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

const pgxSchema = `
CREATE TABLE heroes (id bigserial PRIMARY KEY, name text NOT NULL, mmr integer, born date, tags text[]);
CREATE TYPE lane AS ENUM ('safe', 'mid', 'off');
CREATE TABLE picks (hero_id bigint, lane lane);
`

func TestPgxCopyFromBatchAndCollect(t *testing.T) {
	conn := pgxConnect(t, freshDatabase(t, "pgx_features"))
	ctx := context.Background()
	if _, err := conn.Exec(ctx, pgxSchema); err != nil {
		t.Fatal(err)
	}
	rows := [][]any{
		{"Pudge", int32(4200), time.Date(2004, 2, 1, 0, 0, 0, 0, time.UTC), []string{"hook", "rot"}},
		{"Axe", nil, nil, []string{}},
		{"Lina", int32(5100), time.Date(2005, 3, 4, 0, 0, 0, 0, time.UTC), nil},
	}
	copied, err := conn.CopyFrom(ctx, pgx.Identifier{"heroes"}, []string{"name", "mmr", "born", "tags"}, pgx.CopyFromRows(rows))
	if err != nil || copied != 3 {
		t.Fatalf("copy from: %d %v", copied, err)
	}
	batch := &pgx.Batch{}
	batch.Queue("UPDATE heroes SET mmr = mmr + 100 WHERE name = $1", "Pudge")
	batch.Queue("SELECT count(*) FROM heroes WHERE mmr IS NULL")
	batch.Queue("INSERT INTO heroes (name) VALUES ($1) RETURNING id", "Lion")
	results := conn.SendBatch(ctx, batch)
	if tag, err := results.Exec(); err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("batch update: %v %v", tag, err)
	}
	var nulls int64
	if err := results.QueryRow().Scan(&nulls); err != nil || nulls != 1 {
		t.Fatalf("batch count: %d %v", nulls, err)
	}
	var id int64
	if err := results.QueryRow().Scan(&id); err != nil || id == 0 {
		t.Fatalf("batch insert: %d %v", id, err)
	}
	if err := results.Close(); err != nil {
		t.Fatal(err)
	}
	type hero struct {
		Name string
		Mmr  *int32
		Tags []string
	}
	collected, err := pgx.CollectRows(must(conn.Query(ctx, "SELECT name, mmr, tags FROM heroes ORDER BY name")),
		pgx.RowToStructByPos[hero])
	if err != nil || len(collected) != 4 || collected[3].Name != "Pudge" || *collected[3].Mmr != 4300 ||
		strings.Join(collected[3].Tags, ",") != "hook,rot" {
		t.Fatalf("collect: %v %+v", err, collected)
	}
}

func must(rows pgx.Rows, err error) pgx.Rows {
	if err != nil {
		panic(err)
	}
	return rows
}

func TestPgxLoadTypeEnum(t *testing.T) {
	conn := pgxConnect(t, freshDatabase(t, "pgx_types"))
	ctx := context.Background()
	if _, err := conn.Exec(ctx, pgxSchema); err != nil {
		t.Fatal(err)
	}
	laneType, err := conn.LoadType(ctx, "lane")
	if err != nil {
		t.Fatal(err)
	}
	conn.TypeMap().RegisterType(laneType)
	arrayType, err := conn.LoadType(ctx, "_lane")
	if err != nil {
		t.Fatal(err)
	}
	conn.TypeMap().RegisterType(arrayType)
	var lanes []string
	if err := conn.QueryRow(ctx, "SELECT ARRAY['mid', 'off']::lane[]").Scan(&lanes); err != nil ||
		strings.Join(lanes, ",") != "mid,off" {
		t.Fatalf("enum array: %v %v", lanes, err)
	}
	if _, err := conn.Exec(ctx, "INSERT INTO picks VALUES ($1, $2)", int64(1), "safe"); err != nil {
		t.Fatal(err)
	}
	var lane string
	if err := conn.QueryRow(ctx, "SELECT lane FROM picks").Scan(&lane); err != nil || lane != "safe" {
		t.Fatalf("enum: %v %v", lane, err)
	}
}

func TestPgxQueryExecModes(t *testing.T) {
	url := freshDatabase(t, "pgx_modes")
	conn := pgxConnect(t, url)
	if _, err := conn.Exec(context.Background(), pgxSchema+"INSERT INTO heroes (name, mmr) VALUES ('Pudge', 4200);"); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeCacheStatement, pgx.QueryExecModeCacheDescribe,
		pgx.QueryExecModeDescribeExec, pgx.QueryExecModeExec, pgx.QueryExecModeSimpleProtocol} {
		var sum int64
		if err := conn.QueryRow(context.Background(), "SELECT $1::int8 + $2::int8", mode, 40, 2).Scan(&sum); err != nil || sum != 42 {
			t.Fatalf("mode %v: %d %v", mode, sum, err)
		}
		var name string
		if err := conn.QueryRow(context.Background(), "SELECT name FROM heroes WHERE mmr > $1", mode, 4000).Scan(&name); err != nil || name != "Pudge" {
			t.Fatalf("mode %v: %q %v", mode, name, err)
		}
	}
}

func TestPgxUntypedParameterInExpression(t *testing.T) {
	conn := pgxConnect(t, freshDatabase(t, "pgx_untyped"))
	ctx := context.Background()
	if _, err := conn.Exec(ctx, pgxSchema+"INSERT INTO heroes (name, mmr) VALUES ('Pudge', 4200);"); err != nil {
		t.Fatal(err)
	}
	xfailOnSereneDB(t, "SereneDB describes $1 in mmr + $1 as text (PG: integer) and the RETURNING list as one 'unknown' column",
		func() error {
			var mmr int32
			if err := conn.QueryRow(ctx, "UPDATE heroes SET mmr = mmr + $1 WHERE name = $2 RETURNING mmr", 100, "Pudge").
				Scan(&mmr); err != nil {
				return err
			}
			if mmr != 4300 {
				return fmt.Errorf("mmr %d", mmr)
			}
			return nil
		})
}

func TestPgxListenNotify(t *testing.T) {
	url := freshDatabase(t, "pgx_listen")
	listener := pgxConnect(t, url)
	notifier := pgxConnect(t, url)
	xfailOnSereneDB(t, "SereneDB has no LISTEN/NOTIFY (#723)", func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := listener.Exec(ctx, "LISTEN drv_channel"); err != nil {
			return err
		}
		if _, err := notifier.Exec(ctx, "SELECT pg_notify('drv_channel', 'hook')"); err != nil {
			return err
		}
		n, err := listener.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		if n.Payload != "hook" {
			return errors.New("payload " + n.Payload)
		}
		return nil
	})
}
