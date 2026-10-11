package drivers

import (
	"context"
	"fmt"
	"os"
	"testing"

	"ariga.io/atlas/sql/postgres"
	"ariga.io/atlas/sql/schema"
)

const atlasDDL = `
CREATE TABLE teams (id serial PRIMARY KEY, name varchar(40) NOT NULL UNIQUE, founded date, budget numeric(12,2) DEFAULT 0);
CREATE TABLE players (id bigserial PRIMARY KEY, team_id integer NOT NULL REFERENCES teams (id), nick text NOT NULL, mmr integer CHECK (mmr >= 0), active boolean DEFAULT true, joined timestamptz DEFAULT now(), UNIQUE (team_id, nick));
CREATE INDEX players_mmr_idx ON players (mmr DESC);
CREATE INDEX players_active_nick_idx ON players (active, nick);
COMMENT ON TABLE teams IS 'pro teams';
COMMENT ON COLUMN players.nick IS 'in-game name';
`

func inspectAtlas(t *testing.T, url string, ddl string) (string, error) {
	db := openDB(t, url)
	if ddl != "" {
		if _, err := db.Exec(ddl); err != nil {
			return "", err
		}
	}
	drv, err := postgres.Open(db)
	if err != nil {
		return "", err
	}
	s, err := drv.InspectSchema(context.Background(), "public", nil)
	if err != nil {
		return "", err
	}
	hcl, err := postgres.MarshalHCL(s)
	return string(hcl), err
}

const atlasInspect = "SereneDB has no jsonb_object_agg() (#613), Atlas aggregates index properties with it"

func TestAtlasInspectMatchesPostgreSQL(t *testing.T) {
	url := freshDatabase(t, "atlas_inspect")
	xfailOnSereneDB(t, atlasInspect, func() error {
		hcl, err := inspectAtlas(t, url, atlasDDL)
		if err != nil {
			return err
		}
		return compareGolden(t, fixture("atlas/inspect.hcl"), hcl)
	})
}

func TestAtlasDeclarativeApply(t *testing.T) {
	desiredHCL, err := os.ReadFile(fixture("atlas/inspect.hcl"))
	if err != nil {
		t.Fatal(err)
	}
	db := openDB(t, freshDatabase(t, "atlas_apply"))
	drv, err := postgres.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	var desired schema.Schema
	if err := postgres.EvalHCLBytes(desiredHCL, &desired, nil); err != nil {
		t.Fatal(err)
	}
	changes, err := drv.SchemaDiff(schema.New("public"), &desired)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := drv.ApplyChanges(ctx, changes); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO teams (name) VALUES ('OG'); INSERT INTO players (team_id, nick, mmr) SELECT id, 'N0tail', 9000 FROM teams`); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{
		`INSERT INTO players (team_id, nick) VALUES (999, 'ghost')`,
		`INSERT INTO players (team_id, nick, mmr) SELECT id, 'negative', -1 FROM teams`,
		`INSERT INTO teams (name) VALUES ('OG')`,
	} {
		if _, err := db.Exec(bad); err == nil {
			t.Fatalf("constraint not enforced: %s", bad)
		}
	}
	xfailOnSereneDB(t, atlasInspect, func() error {
		after, err := drv.InspectSchema(ctx, "public", nil)
		if err != nil {
			return err
		}
		drift, err := drv.SchemaDiff(after, &desired)
		if err != nil {
			return err
		}
		if len(drift) != 0 {
			return fmt.Errorf("schema drifts after apply: %s", fmt.Sprint(drift))
		}
		return nil
	})
}
