// Package spec mirrors tests/drivers/harness/spec_loader.py for Go.
package spec

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

var Protocols = []string{
	"simple", "extended-noparam", "extended-text", "extended-binary",
}

type Type struct {
	OID       int                 `yaml:"oid"`
	Name      string              `yaml:"name"`
	PgTypname string              `yaml:"pg_typname"`
	CastSQL   string              `yaml:"cast_sql"`
	Text      bool                `yaml:"text"`
	Binary    bool                `yaml:"binary"`
	Array     interface{}         `yaml:"array"`
	Samples   []interface{}       `yaml:"samples"`
	Skip      map[string][]string `yaml:"skip"`
}

type Case struct {
	Type     Type
	Protocol string
	Idx      int
	Sample   interface{}
}

func specDir() string {
	if s := os.Getenv("SDB_DRV_SPEC"); s != "" {
		return s
	}
	wd, _ := os.Getwd()
	return filepath.Join(wd, "..", "spec")
}

func LoadTypes() ([]Type, error) {
	b, err := os.ReadFile(filepath.Join(specDir(), "types.yaml"))
	if err != nil {
		return nil, err
	}
	var out []Type
	if err := yaml.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	for i, t := range out {
		if t.Name == "" {
			out[i].Name = t.PgTypname
		}
		if t.PgTypname == "" {
			out[i].PgTypname = t.Name
		}
		if out[i].CastSQL == "" {
			out[i].CastSQL = "$1::" + out[i].PgTypname
		}
	}
	return out, nil
}

func CasesFor(driverKey string) ([]Case, error) {
	types, err := LoadTypes()
	if err != nil {
		return nil, err
	}
	pat := os.Getenv("SDB_DRV_TYPES")
	if pat == "" {
		pat = ".*"
	}
	re := regexp.MustCompile(pat)
	requested := strings.Split(getOr("SDB_DRV_PROTOCOLS", strings.Join(Protocols, ",")), ",")
	var out []Case
	for _, t := range types {
		if !re.MatchString(t.Name) {
			continue
		}
		if !t.Text && !t.Binary {
			continue
		}
		for _, p := range requested {
			if !contains(Protocols, p) {
				continue
			}
			supports := t.Text
			if p == "extended-binary" {
				supports = t.Binary
			}
			if !supports {
				continue
			}
			modes := t.Skip[driverKey]
			if contains(modes, "all") || contains(modes, p) {
				continue
			}
			for i, s := range t.Samples {
				out = append(out, Case{Type: t, Protocol: p, Idx: i, Sample: s})
			}
		}
	}
	return out, nil
}

func ConnString(prefersSimple bool) string {
	host := getOr("SDB_DRV_HOST", "localhost")
	port := getOr("SDB_DRV_PORT", "5432")
	db := getOr("SDB_DRV_DATABASE", "serenedb")
	user := getOr("SDB_DRV_USER", "serenedb")
	q := ""
	if prefersSimple {
		q = "?default_query_exec_mode=simple_protocol"
	}
	return "postgres://" + user + "@" + host + ":" + port + "/" + db + q
}

func SchemaName(driverKey string) string {
	return "drv_" + driverKey + "_" + getOr("SDB_DRV_RUN_ID", "0")
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func getOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
