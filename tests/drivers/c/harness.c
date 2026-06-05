////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

// C / libpq round-trip matrix harness.
//
// Reads SDB_DRV_* env, loads the spec by shelling out to the shared
// Python loader (avoids pulling in a YAML parser), then for every
// (type, protocol, sample) tuple sends the round-trip query via libpq
// and compares the server-canonicalized text form.
//
// Output: a tiny JUnit XML at argv[1] so the suite-level reporter
// picks it up alongside the other languages.

#include <libpq-fe.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/wait.h>
#include <unistd.h>

static const char* getenv_or(const char* k, const char* d) {
  const char* v = getenv(k);
  return (v && *v) ? v : d;
}

// ---- shared spec loader via Python -------------------------------------
//
// We reuse tests/drivers/harness/spec_loader.py (already in the repo) to
// emit cases as a tab-separated stream: oid<TAB>name<TAB>pg_typname<TAB>
// cast_sql<TAB>protocol<TAB>idx<TAB>sample (sample = "<NULL>" for None).
//
// This keeps the C harness small and ensures the matrix interpretation
// stays identical to every other language harness.

typedef struct Case {
  int oid;
  char name[128];
  char pg_typname[128];
  char cast_sql[256];
  char protocol[32];
  int idx;
  char* sample;    // NULL for SQL NULL
  int has_sample;  // 0 if NULL
} Case;

static int load_cases(const char* driver_key, Case** out) {
  char cmd[1024];
  snprintf(cmd, sizeof(cmd),
           "python3 - <<'PY'\n"
           "import sys, os\n"
           "sys.path.insert(0, os.path.join(os.environ.get('SDB_DRV_SPEC',\n"
           "    os.path.dirname(os.path.abspath('%s'))+'/../spec'), '..', "
           "'harness'))\n"
           "from spec_loader import cases_for_driver\n"
           "for ts, p, idx, sample in cases_for_driver('%s'):\n"
           "    s = '<NULL>' if sample is None else "
           "sample.replace('\\n','\\\\n').replace('\\t','\\\\t')\n"
           "    print('\\t'.join([str(ts.oid), ts.name, ts.pg_typname, "
           "ts.cast_sql, p, str(idx), s]))\n"
           "PY\n",
           getenv_or("SDB_DRV_SPEC", "./tests/drivers/spec"), driver_key);
  FILE* f = popen(cmd, "r");
  if (!f) {
    return -1;
  }
  Case* arr = NULL;
  int cap = 0, n = 0;
  char line[8192];
  while (fgets(line, sizeof(line), f)) {
    size_t L = strlen(line);
    if (L && line[L - 1] == '\n') {
      line[--L] = 0;
    }
    if (n >= cap) {
      cap = cap ? cap * 2 : 64;
      arr = realloc(arr, cap * sizeof(Case));
      if (!arr) {
        pclose(f);
        return -1;
      }
    }
    Case* c = &arr[n];
    memset(c, 0, sizeof(*c));
    char* p = line;
    char* tabs[7] = {0};
    int tab_count = 0;
    for (char* s = p; *s && tab_count < 6; ++s) {
      if (*s == '\t') {
        *s = 0;
        tabs[tab_count++] = s + 1;
      }
    }
    if (tab_count != 6) {
      continue;
    }
    c->oid = atoi(p);
    snprintf(c->name, sizeof(c->name), "%s", tabs[0]);
    snprintf(c->pg_typname, sizeof(c->pg_typname), "%s", tabs[1]);
    snprintf(c->cast_sql, sizeof(c->cast_sql), "%s", tabs[2]);
    snprintf(c->protocol, sizeof(c->protocol), "%s", tabs[3]);
    c->idx = atoi(tabs[4]);
    if (strcmp(tabs[5], "<NULL>") == 0) {
      c->has_sample = 0;
      c->sample = NULL;
    } else {
      c->has_sample = 1;
      c->sample = strdup(tabs[5]);
    }
    n++;
  }
  pclose(f);
  *out = arr;
  return n;
}

// ---- libpq helpers -----------------------------------------------------

static char* server_text(PGconn* c, const char* pg_typname, const char* sample,
                         int has_sample) {
  if (!has_sample) {
    return NULL;
  }
  char sql[4096];
  snprintf(sql, sizeof(sql), "SELECT (($1::text)::%s)::text", pg_typname);
  const char* params[1] = {sample};
  PGresult* r = PQexecParams(c, sql, 1, NULL, params, NULL, NULL, 0);
  if (PQresultStatus(r) != PGRES_TUPLES_OK || PQntuples(r) == 0) {
    char* e = strdup(PQerrorMessage(c));
    PQclear(r);
    return (char*)((uintptr_t)e | 1);  // tagged: error
  }
  char* v = NULL;
  if (!PQgetisnull(r, 0, 0)) {
    v = strdup(PQgetvalue(r, 0, 0));
  }
  PQclear(r);
  return v;
}

static int run_inline(PGconn* c, const Case* cs, char** actual_out) {
  // sql holds the escaped sample (up to ESC_MAX) plus the cast wrapper and type
  // name; size it above esc so snprintf can't truncate (-Wformat-truncation).
  enum { ESC_MAX = 4096 };
  char sql[ESC_MAX + 256];
  if (!cs->has_sample) {
    snprintf(sql, sizeof(sql), "SELECT NULL::%s::text AS v", cs->pg_typname);
  } else {
    // Naive single-quote escape (mirrors the python harness).
    char esc[ESC_MAX];
    size_t j = 0;
    for (size_t i = 0; cs->sample[i] && j + 2 < sizeof(esc); i++) {
      if (cs->sample[i] == '\'') {
        esc[j++] = '\'';
      }
      esc[j++] = cs->sample[i];
    }
    esc[j] = 0;
    snprintf(sql, sizeof(sql), "SELECT '%s'::%s::text AS v", esc,
             cs->pg_typname);
  }
  PGresult* r = PQexec(c, sql);
  if (PQresultStatus(r) != PGRES_TUPLES_OK) {
    PQclear(r);
    return -1;
  }
  *actual_out = PQgetisnull(r, 0, 0) ? NULL : strdup(PQgetvalue(r, 0, 0));
  PQclear(r);
  return 0;
}

static int run_bound(PGconn* c, const Case* cs, char** actual_out) {
  // Replace $1 with $1::text in cast_sql to neutralise per-OID strict
  // typing - we always send text-form params and let the server cast
  // server-side. Same trick as the Rust harness.
  char cast[512];
  const char* dollar = strstr(cs->cast_sql, "$1");
  if (!dollar) {
    return -1;
  }
  size_t prefix_len = (size_t)(dollar - cs->cast_sql);
  snprintf(cast, sizeof(cast), "%.*s$1::text%s", (int)prefix_len, cs->cast_sql,
           dollar + 2);
  char sql[4096];
  snprintf(sql, sizeof(sql), "SELECT (%s)::text AS v", cast);
  const char* params[1] = {cs->has_sample ? cs->sample : NULL};
  PGresult* r = PQexecParams(c, sql, 1, NULL, params, NULL, NULL, 0);
  if (PQresultStatus(r) != PGRES_TUPLES_OK) {
    PQclear(r);
    return -1;
  }
  *actual_out = PQgetisnull(r, 0, 0) ? NULL : strdup(PQgetvalue(r, 0, 0));
  PQclear(r);
  return 0;
}

// ---- driver ------------------------------------------------------------

int main(int argc, char** argv) {
  const char* host = getenv_or("SDB_DRV_HOST", "localhost");
  const char* port = getenv_or("SDB_DRV_PORT", "5432");
  const char* db = getenv_or("SDB_DRV_DATABASE", "postgres");
  const char* user = getenv_or("SDB_DRV_USER", "postgres");
  const char* run_id = getenv_or("SDB_DRV_RUN_ID", "0");

  char conninfo[512];
  snprintf(conninfo, sizeof(conninfo), "host=%s port=%s dbname=%s user=%s",
           host, port, db, user);
  PGconn* c = PQconnectdb(conninfo);
  if (PQstatus(c) != CONNECTION_OK) {
    fprintf(stderr, "C harness: connect failed: %s\n", PQerrorMessage(c));
    PQfinish(c);
    return 1;
  }

  char schema_sql[256];
  snprintf(schema_sql, sizeof(schema_sql),
           "CREATE SCHEMA IF NOT EXISTS \"drv_c_libpq_%s\"; "
           "SET search_path TO \"drv_c_libpq_%s\", public, pg_catalog",
           run_id, run_id);
  PGresult* sr = PQexec(c, schema_sql);
  PQclear(sr);

  Case* cases = NULL;
  int n = load_cases("c_libpq", &cases);
  if (n < 0) {
    fprintf(stderr, "C harness: failed to load cases\n");
    PQfinish(c);
    return 1;
  }

  int passed = 0, failed = 0;
  FILE* xml = argc > 1 ? fopen(argv[1], "w") : NULL;
  if (xml) {
    fprintf(xml, "<?xml version=\"1.0\"?>\n<testsuite name=\"c_libpq\">\n");
  }

  // Smoke
  PGresult* sm = PQexec(c, "SELECT 1");
  int smoke_ok =
    PQresultStatus(sm) == PGRES_TUPLES_OK && atoi(PQgetvalue(sm, 0, 0)) == 1;
  PQclear(sm);
  if (smoke_ok) {
    passed++;
  } else {
    failed++;
  }
  if (xml) {
    fprintf(xml,
            "  <testcase classname=\"c_libpq\" name=\"smoke_select_one\"%s/>\n",
            smoke_ok ? "" : "><failure/></testcase>");
  }

  for (int i = 0; i < n; i++) {
    Case* cs = &cases[i];
    char id[256];
    snprintf(id, sizeof(id), "%d-%s-%s-%d", cs->oid, cs->name, cs->protocol,
             cs->idx);

    char* expected = server_text(c, cs->pg_typname, cs->sample, cs->has_sample);
    int err_tag = ((uintptr_t)expected) & 1;
    if (err_tag) {
      failed++;
      char* msg = (char*)((uintptr_t)expected & ~(uintptr_t)1);
      if (xml) {
        fprintf(xml,
                "  <testcase classname=\"c_libpq\" name=\"%s\">"
                "<error>%s</error></testcase>\n",
                id, msg);
      }
      free(msg);
      continue;
    }

    char* actual = NULL;
    int rc;
    if (strcmp(cs->protocol, "simple") == 0 ||
        strcmp(cs->protocol, "extended-noparam") == 0) {
      rc = run_inline(c, cs, &actual);
    } else {
      rc = run_bound(c, cs, &actual);
    }
    int eq = rc == 0 && ((expected == NULL && actual == NULL) ||
                         (expected && actual && strcmp(expected, actual) == 0));
    if (eq) {
      passed++;
      if (xml) {
        fprintf(xml, "  <testcase classname=\"c_libpq\" name=\"%s\"/>\n", id);
      }
    } else {
      failed++;
      if (xml) {
        fprintf(xml,
                "  <testcase classname=\"c_libpq\" name=\"%s\">"
                "<failure>expected=%s actual=%s rc=%d</failure></testcase>\n",
                id, expected ? expected : "<NULL>", actual ? actual : "<NULL>",
                rc);
      }
    }
    free(expected);
    free(actual);
  }

  if (xml) {
    fprintf(xml, "</testsuite>\n");
    fclose(xml);
  }
  fprintf(stderr, "[c_libpq] %d passed, %d failed\n", passed, failed);

  PQfinish(c);
  for (int i = 0; i < n; i++) {
    free(cases[i].sample);
  }
  free(cases);
  return failed > 0 ? 1 : 0;
}
