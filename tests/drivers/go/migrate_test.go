package drivers

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	migratepgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

//go:embed migrations
var migrationFiles embed.FS

func TestGolangMigrateUpStepsAndDown(t *testing.T) {
	db := openDB(t, freshDatabase(t, "golang_migrate"))
	xfailOnSereneDB(t, "SereneDB has no pg_advisory_lock(), golang-migrate locks the database with it", func() error {
		source, err := iofs.New(migrationFiles, "migrations/golang-migrate")
		if err != nil {
			return err
		}
		driver, err := migratepgx.WithInstance(db, &migratepgx.Config{})
		if err != nil {
			return err
		}
		m, err := migrate.NewWithInstance("iofs", source, "pgx5", driver)
		if err != nil {
			return err
		}
		defer m.Close()
		if err := m.Up(); err != nil {
			return err
		}
		if version, dirty, err := m.Version(); err != nil || version != 2 || dirty {
			return fmt.Errorf("after up: %d %v %v", version, dirty, err)
		}
		if err := m.Steps(-1); err != nil {
			return err
		}
		if version, _, err := m.Version(); err != nil || version != 1 {
			return fmt.Errorf("after step back: %d %v", version, err)
		}
		if err := m.Down(); err != nil {
			return err
		}
		if _, _, err := m.Version(); !errors.Is(err, migrate.ErrNilVersion) {
			return fmt.Errorf("after down: %v", err)
		}
		return nil
	})
}

func gooseProvider(t *testing.T, prefix string, opts ...goose.ProviderOption) (*goose.Provider, error) {
	t.Helper()
	files, err := fs.Sub(migrationFiles, "migrations/goose")
	if err != nil {
		t.Fatal(err)
	}
	return goose.NewProvider(goose.DialectPostgres, openDB(t, freshDatabase(t, prefix)), files, opts...)
}

func runGoose(provider *goose.Provider) error {
	ctx := context.Background()
	results, err := provider.Up(ctx)
	if err != nil {
		return err
	}
	if len(results) != 2 {
		return fmt.Errorf("applied %d migrations", len(results))
	}
	if version, err := provider.GetDBVersion(ctx); err != nil || version != 2 {
		return fmt.Errorf("version after up: %d %v", version, err)
	}
	statuses, err := provider.Status(ctx)
	if err != nil {
		return err
	}
	for _, status := range statuses {
		if status.State != goose.StateApplied {
			return fmt.Errorf("status %+v", status)
		}
	}
	if _, err := provider.Down(ctx); err != nil {
		return err
	}
	if version, err := provider.GetDBVersion(ctx); err != nil || version != 1 {
		return fmt.Errorf("version after down: %d %v", version, err)
	}
	if _, err := provider.DownTo(ctx, 0); err != nil {
		return err
	}
	return nil
}

func TestGooseUpStatusAndDown(t *testing.T) {
	xfailOnSereneDB(t, "SereneDB has no identity columns (#1426), goose_db_version has an IDENTITY key", func() error {
		provider, err := gooseProvider(t, "goose")
		if err != nil {
			return err
		}
		return runGoose(provider)
	})
}

func TestGooseSessionLocker(t *testing.T) {
	xfailOnSereneDB(t, "SereneDB has no identity columns (#1426) for goose_db_version and no pg_try_advisory_lock() "+
		"for the session locker", func() error {
		locker, err := lock.NewPostgresSessionLocker()
		if err != nil {
			return err
		}
		provider, err := gooseProvider(t, "goose_locked", goose.WithSessionLocker(locker))
		if err != nil {
			return err
		}
		return runGoose(provider)
	})
}
