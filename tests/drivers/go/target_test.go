package drivers

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/serenedb/drivers-go/spec"
)

var sereneDB = sync.OnceValues(func() (bool, error) {
	c, err := pgx.Connect(context.Background(), spec.ConnString(false))
	if err != nil {
		return false, err
	}
	defer c.Close(context.Background())
	var version string
	if err := c.QueryRow(context.Background(), "SELECT version()").Scan(&version); err != nil {
		return false, err
	}
	return strings.Contains(version, "SereneDB"), nil
})

func isSereneDB(t *testing.T) bool {
	t.Helper()
	serene, err := sereneDB()
	if err != nil {
		t.Fatalf("detect target: %v", err)
	}
	return serene
}

func compareGolden(t *testing.T, path, text string) error {
	t.Helper()
	ext := filepath.Ext(path)
	twin := strings.TrimSuffix(path, ext) + ".serenedb" + ext
	if os.Getenv("SDB_DRV_UPDATE_GOLDENS") == "1" {
		if !isSereneDB(t) {
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				return err
			}
			return os.WriteFile(path, []byte(text), 0o644)
		}
		golden, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if string(golden) == text {
			if err := os.Remove(twin); err != nil && !os.IsNotExist(err) {
				return err
			}
			return nil
		}
		return os.WriteFile(twin, []byte(text), 0o644)
	}
	expected := path
	if isSereneDB(t) {
		if _, err := os.Stat(twin); err == nil {
			expected = twin
		}
	}
	golden, err := os.ReadFile(expected)
	if err != nil {
		return err
	}
	if string(golden) != text {
		return fmt.Errorf("%s differs:\n--- expected\n%s\n--- actual\n%s", expected, golden, text)
	}
	return nil
}

func checkGolden(t *testing.T, path, text string) {
	t.Helper()
	if err := compareGolden(t, path, text); err != nil {
		t.Fatal(err)
	}
}

func xfailOnSereneDB(t *testing.T, reason string, body func() error) {
	t.Helper()
	err := body()
	if !isSereneDB(t) {
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	if err == nil {
		t.Fatalf("SereneDB passes now, drop the expected failure: %s", reason)
	}
	if os.Getenv("SDB_DRV_DEBUG") == "true" {
		t.Logf("xfail on SereneDB (%s): %v", reason, err)
	}
}

func databaseName(prefix string) string {
	runID := os.Getenv("SDB_DRV_RUN_ID")
	if runID == "" {
		runID = "0"
	}
	return "go_" + prefix + "_" + runID
}

func databaseURL(database string) string {
	u := spec.ConnString(false)
	base := u[:strings.LastIndex(u, "/")+1]
	return base + database
}

func adminExec(t *testing.T, sql string) {
	t.Helper()
	c, err := pgx.Connect(context.Background(), spec.ConnString(false))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(context.Background())
	if _, err := c.Exec(context.Background(), sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func freshDatabase(t *testing.T, prefix string) string {
	t.Helper()
	name := databaseName(prefix)
	adminExec(t, fmt.Sprintf(`DROP DATABASE IF EXISTS %q`, name))
	adminExec(t, fmt.Sprintf(`CREATE DATABASE %q`, name))
	t.Cleanup(func() {
		adminExec(t, fmt.Sprintf(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '%s' AND pid <> pg_backend_pid()`, name))
		adminExec(t, fmt.Sprintf(`DROP DATABASE IF EXISTS %q`, name))
	})
	return databaseURL(name)
}

func openDB(t *testing.T, url string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func fixture(name string) string {
	return filepath.Join("fixtures", name)
}
