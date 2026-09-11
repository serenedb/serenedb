////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "docs/docs_loader.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>
#include <absl/time/time.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/prepared_statement.hpp>
#include <duckdb/main/query_result.hpp>
#include <exception>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "basics/duckdb_engine.h"
#include "basics/log.h"
#include "basics/static_strings.h"
#include "catalog/entry.h"
#include "catalog/entry/duckdb_object_entry.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "connector/duckdb_client_state.h"
#include "docs/docs_data.h"
#include "pg/connection_context.h"

namespace sdb::docs {
namespace {

constexpr std::string_view kSchema = StaticStrings::kDocsSchema;
constexpr std::string_view kTable = "sdb_docs.docs";
constexpr std::string_view kMeta = "sdb_docs.meta";
constexpr int kLayout = 13;
constexpr size_t kInsertBatch = 32;

constexpr std::string_view kSchemaToken = "@schema@";

std::string Sql(std::string_view statement) {
  return absl::StrReplaceAll(statement, {{kSchemaToken, kSchema}});
}

constexpr std::string_view kResetSql = R"sql(
DROP FUNCTION IF EXISTS @schema@.search(TEXT, INTEGER);
DROP FUNCTION IF EXISTS @schema@.read(TEXT);
DROP FUNCTION IF EXISTS @schema@.sections(TEXT);
DROP FUNCTION IF EXISTS @schema@.reference(TEXT);
DROP FUNCTION IF EXISTS @schema@.objects();
DROP FUNCTION IF EXISTS @schema@.summary(TEXT);
DROP TABLE IF EXISTS @schema@.docs;
DROP TABLE IF EXISTS @schema@.meta;
DROP TEXT SEARCH DICTIONARY IF EXISTS @schema@.tokenizer;

CREATE TEXT SEARCH DICTIONARY @schema@.tokenizer
  (template = 'segmentation', case = 'lower', break = 'alpha',
   frequency = true, position = true);

CREATE TABLE @schema@.docs (
  path TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  breadcrumb TEXT NOT NULL,
  content TEXT NOT NULL,
  content_text TEXT NOT NULL
) WITH (storage = 'search', compaction_interval = 0);

CREATE INDEX docs_fts ON @schema@.docs USING inverted (
  title @schema@.tokenizer,
  breadcrumb @schema@.tokenizer,
  content_text @schema@.tokenizer);
)sql";

constexpr std::string_view kGrantSql = R"sql(
GRANT USAGE ON SCHEMA @schema@ TO PUBLIC;
GRANT SELECT ON @schema@.docs TO PUBLIC;
GRANT SELECT ON @schema@.meta TO PUBLIC;
)sql";

constexpr std::string_view kFinalizeSql[]{
  R"sql(VACUUM (REFRESH_TABLE) @schema@.docs)sql",
  R"sql(
CREATE FUNCTION @schema@.search(query TEXT, max_hits INTEGER)
RETURNS TABLE(path TEXT, title TEXT, breadcrumb TEXT, snippet TEXT,
              score DOUBLE PRECISION)
LANGUAGE SQL BEGIN ATOMIC
  SELECT d.path, d.title, d.breadcrumb,
         left(regexp_replace(d.content_text, '\s+', ' ', 'g'), 400),
         BM25(d.tableoid)
  FROM @schema@.docs_fts d
  WHERE d.title @@ query OR d.breadcrumb @@ query OR d.content_text @@ query
  ORDER BY BM25(d.tableoid) DESC, d.path
  LIMIT max_hits;
END)sql",
  R"sql(
CREATE FUNCTION @schema@.read(doc_path TEXT) RETURNS TEXT
LANGUAGE SQL BEGIN ATOMIC
  SELECT content FROM @schema@.docs WHERE path = doc_path;
END)sql",
  R"sql(
CREATE FUNCTION @schema@.sections(prefix TEXT)
RETURNS TABLE(path TEXT, title TEXT, breadcrumb TEXT)
LANGUAGE SQL BEGIN ATOMIC
  SELECT path, title, breadcrumb FROM @schema@.docs
  WHERE starts_with(path, prefix) ORDER BY path;
END)sql",
  R"sql(
CREATE FUNCTION @schema@.reference(name TEXT)
RETURNS TABLE(path TEXT, title TEXT, breadcrumb TEXT)
LANGUAGE SQL BEGIN ATOMIC
  SELECT path, title, breadcrumb FROM @schema@.docs
  WHERE lower(title) = lower(name)
     OR starts_with(lower(title), lower(name) || '(')
  ORDER BY path;
END)sql",
  R"sql(
CREATE FUNCTION @schema@.summary(doc_content TEXT) RETURNS TEXT
LANGUAGE SQL BEGIN ATOMIC
SELECT regexp_replace(coalesce(
  nullif(regexp_replace(trim(CASE
    WHEN regexp_matches(regexp_replace(ltrim(doc_content),
                                       '^# [^' || chr(10) || ']*' || chr(10) || '+', ''),
                        '^(\||#|```|> |[-*+] |[0-9]+\. )') THEN ''
    ELSE split_part(regexp_replace(ltrim(doc_content),
                                   '^# [^' || chr(10) || ']*' || chr(10) || '+', ''),
                    chr(10) || chr(10), 1) END), '\s+', ' ', 'g'), ''),
  nullif(trim(regexp_replace(
    regexp_extract(doc_content, '\|\s*\*\*Description\*\*\s*\|([^|]*)\|', 1),
    '\s+', ' ', 'g')), '')), '\[([^\]]*)\]\([^)]*\)', '\1', 'g');
END)sql",
  R"sql(
CREATE FUNCTION @schema@.objects()
RETURNS TABLE(kind TEXT, name TEXT, signature TEXT, summary TEXT, aliases TEXT,
              path TEXT, page TEXT, category TEXT, breadcrumb TEXT)
LANGUAGE SQL BEGIN ATOMIC
WITH d AS (
  SELECT path, title, breadcrumb, content, split_part(path, '#', 1) AS page,
         path = split_part(path, '#', 1) || '#' ||
                replace(replace(title, '#', '\#'), ' ', '_') AS is_title_row
  FROM @schema@.docs
),
rows AS (
  SELECT d.page AS page, d.path AS path, d.breadcrumb AS breadcrumb,
         u.tbl.headers[1] AS first_header,
         CASE WHEN u.tbl.headers[1] = 'Index'
              THEN regexp_extract(v.cells[1], '\]\(\.?/?([^)]*?)(?:/index)?\.mdx?\)', 1)
              ELSE regexp_replace(v.cells[1], '\[([^\]]*)\]\([^)]*\)', '\1', 'g') END AS name,
         CASE WHEN list_contains(u.tbl.headers, 'Aliases')
              THEN nullif(v.cells[list_position(u.tbl.headers, 'Aliases')], '') END AS aliases,
         regexp_replace(CASE WHEN list_contains(u.tbl.headers, 'Description')
                             THEN v.cells[list_position(u.tbl.headers, 'Description')]
                             WHEN list_contains(u.tbl.headers, 'Purpose')
                             THEN v.cells[list_position(u.tbl.headers, 'Purpose')] END,
                        '\[([^\]]*)\]\([^)]*\)', '\1', 'g') AS summary
  FROM d, unnest(md_extract_tables_json(d.content)) AS u(tbl),
          unnest(u.tbl.table_data) AS v(cells)
  WHERE d.is_title_row
),
rows_named AS (
  SELECT page, path, breadcrumb, first_header, name, aliases, summary,
         CASE WHEN regexp_matches(name, '^[A-Za-z_][A-Za-z0-9_]*\(')
              THEN regexp_extract(name, '^([A-Za-z_][A-Za-z0-9_]*)\(', 1)
              ELSE name END AS bare
  FROM rows
)
SELECT 'function' AS kind,
       regexp_extract(title, '^(?:[A-Za-z_][A-Za-z0-9_]*\.)?([A-Za-z_][A-Za-z0-9_]*)\(', 1) AS name,
       title AS signature, @schema@.summary(content) AS summary, NULL AS aliases,
       path AS path, page AS page,
       regexp_replace(regexp_replace(page, '^sql/functions/', ''), '(/index)?\.mdx?$', '') AS category,
       breadcrumb AS breadcrumb
FROM d WHERE starts_with(page, 'sql/functions/') AND position('#' IN path) > 0
   AND regexp_matches(title, '^(?:[A-Za-z_][A-Za-z0-9_]*\.)?[A-Za-z_][A-Za-z0-9_]*\(')
UNION ALL
SELECT 'statement', title, title, @schema@.summary(content), NULL, path, page, NULL, breadcrumb
FROM d WHERE starts_with(page, 'sql/statements/') AND is_title_row
   AND NOT starts_with(page, 'sql/statements/create_text_search_dictionary/')
UNION ALL
SELECT 'tokenizer', title, title, @schema@.summary(content), NULL, path, page, NULL, breadcrumb
FROM d WHERE starts_with(page, 'sql/statements/create_text_search_dictionary/') AND is_title_row
   AND page <> 'sql/statements/create_text_search_dictionary/index.md'
UNION ALL
SELECT 'type', bare, name, summary, aliases, path, page, NULL, breadcrumb
FROM rows_named WHERE page = 'sql/data_types/overview.md' AND first_header = 'Name'
UNION ALL
SELECT 'setting', bare, name, summary, aliases, path, page, NULL, breadcrumb
FROM rows_named WHERE page = 'configuration/overview.md' AND first_header = 'Name'
UNION ALL
SELECT 'index_type', bare, name, summary, NULL, path, page, NULL, breadcrumb
FROM rows_named WHERE page = 'sql/indexes/index.md' AND first_header = 'Index';
END)sql",
  R"sql(CREATE TABLE @schema@.meta (hash TEXT, layout INTEGER))sql",
};

class Loader {
 public:
  Loader(std::string_view database, ObjectId database_id)
    : _conn{DuckDBEngine::Instance().CreateConnection()},
      _ctx{std::make_shared<ConnectionContext>(
        *_conn->context, StaticStrings::kDefaultUser, id::kRootUser, database,
        database_id, nullptr, 0, nullptr)} {
    _ctx->MarkSystemWriter();
    connector::SereneDBClientState::Register(*_conn->context, _ctx);
    _conn->context->session_user = std::string{StaticStrings::kDefaultUser};
    std::vector<duckdb::CatalogSearchEntry> paths{
      duckdb::CatalogSearchEntry{duckdb::Identifier{std::string{database}},
                                 duckdb::Identifier{"$user"}},
      duckdb::CatalogSearchEntry{duckdb::Identifier{std::string{database}},
                                 duckdb::Identifier{"public"}},
    };
    _conn->context->client_data->catalog_search_path->SetDefaultPaths(
      std::vector{paths});
    _conn->context->client_data->catalog_search_path->Set(
      std::move(paths), duckdb::CatalogSetPathType::SET_DIRECTLY);
  }

  ~Loader() {
    _ctx->ConsumeNotices([](auto& notice) {
      SDB_INFO(STARTUP, "embedded docs: ", notice.errmsg);
    });
  }

  bool UpToDate() {
    auto result = _conn->Query(
      absl::StrCat("SELECT hash, layout FROM ", kMeta, " LIMIT 1"));
    if (result->HasError() || result->RowCount() != 1) {
      return false;
    }
    return result->GetValue(0, 0).ToString() == GetDocsHash() &&
           result->GetValue(1, 0).GetValue<int32_t>() == kLayout;
  }

  bool Rebuild() {
    if (!Run(Sql(kResetSql)) || !Insert() || !RunAll(kFinalizeSql)) {
      return false;
    }
    if (!Run(absl::StrCat("INSERT INTO ", kMeta, " VALUES ('", GetDocsHash(),
                          "', ", kLayout, ")"))) {
      return false;
    }
    return Run(Sql(kGrantSql));
  }

  template<size_t N>
  bool RunAll(const std::string_view (&statements)[N]) {
    return absl::c_all_of(statements, [this](std::string_view statement) {
      return Run(Sql(statement));
    });
  }

  bool Run(const std::string& sql) {
    auto result = _conn->Query(sql);
    if (!result->HasError()) {
      return true;
    }
    SDB_WARN(GENERAL, "embedded docs: '", sql,
             "' failed: ", result->GetError());
    return false;
  }

 private:
  bool Insert() {
    constexpr size_t kColumns = 4;
    const auto docs = GetDocs();
    for (size_t begin = 0; begin < docs.size(); begin += kInsertBatch) {
      const auto batch =
        docs.subspan(begin, std::min(kInsertBatch, docs.size() - begin));
      std::string sql = absl::StrCat("INSERT INTO ", kTable, " VALUES ");
      duckdb::vector<duckdb::Value> values;
      values.reserve(batch.size() * kColumns);
      for (size_t i = 0; i < batch.size(); ++i) {
        const auto first = kColumns * i + 1;
        absl::StrAppend(&sql, i == 0 ? "(" : ", (");
        for (size_t column = 0; column < kColumns; ++column) {
          absl::StrAppend(&sql, column == 0 ? "$" : ", $", first + column);
        }
        absl::StrAppend(&sql, ", md_to_text($", first + kColumns - 1, "))");
        const auto& doc = batch[i];
        values.emplace_back(std::string{doc.path});
        values.emplace_back(std::string{doc.title});
        values.emplace_back(std::string{doc.breadcrumb});
        values.emplace_back(std::string{doc.content});
      }
      auto prepared = _conn->Prepare(sql);
      if (prepared->HasError()) {
        SDB_WARN(GENERAL, "embedded docs: prepare insert failed: ",
                 prepared->GetError());
        return false;
      }
      auto result = prepared->Execute(values, /*allow_stream_result=*/false);
      if (result->HasError()) {
        SDB_WARN(GENERAL, "embedded docs: insert failed: ", result->GetError());
        return false;
      }
    }
    return true;
  }

  duckdb::unique_ptr<duckdb::Connection> _conn;
  std::shared_ptr<ConnectionContext> _ctx;
};

bool LoadInto(std::string_view database, ObjectId database_id) {
  const auto begin = std::chrono::steady_clock::now();
  try {
    Loader loader{database, database_id};
    if (!loader.Run(absl::StrCat("CREATE SCHEMA IF NOT EXISTS ",
                                 StaticStrings::kDocsSchema))) {
      return false;
    }
    if (loader.UpToDate()) {
      SDB_INFO(STARTUP, "embedded docs are up to date in database \"", database,
               "\" (", GetDocs().size(), " rows)");
      return true;
    }
    if (!loader.Rebuild()) {
      return false;
    }
    SDB_INFO(STARTUP, "embedded docs loaded: ", GetDocs().size(),
             " rows into database \"", database, "\" in ",
             absl::FormatDuration(
               absl::FromChrono(std::chrono::steady_clock::now() - begin)));
    return true;
  } catch (const std::exception& e) {
    SDB_WARN(GENERAL, "embedded docs: load into database \"", database,
             "\" failed: ", e.what());
    return false;
  }
}

}  // namespace

void LoadEmbeddedDocs() {
  if (GetDocs().empty()) {
    SDB_INFO(STARTUP,
             "embedded docs disabled (built with SDB_EMBEDDED_DOCS=OFF)");
    return;
  }
  const auto* database =
    catalog::FindDatabase(nullptr, StaticStrings::kDefaultDatabase);
  if (database == nullptr) {
    SDB_WARN(GENERAL, "embedded docs: default database not found");
    return;
  }
  LoadInto(database->name.GetIdentifierName(), catalog::IdOf(*database));
}

}  // namespace sdb::docs
