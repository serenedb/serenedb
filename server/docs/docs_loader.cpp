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

#include <absl/strings/str_cat.h>
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
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "catalog/entry.h"
#include "catalog/entry/duckdb_object_entry.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "connector/duckdb_client_state.h"
#include "docs/docs_data.h"
#include "pg/connection_context.h"

namespace sdb::docs {
namespace {

constexpr std::string_view kSchema = irs::StaticStrings::kDocsSchema;
constexpr std::string_view kTable = "sdb_docs.docs";
constexpr std::string_view kIndexRelation = "sdb_docs.docs_fts";
constexpr std::string_view kMeta = "sdb_docs.meta";
constexpr std::string_view kIndex = "docs_fts";
constexpr std::string_view kTokenizer = "sdb_docs.tokenizer";
constexpr int kLayout = 12;
constexpr size_t kInsertBatch = 32;
class Loader {
 public:
  Loader(std::string_view database, ObjectId database_id)
    : _conn{irs::DuckDBEngine::Instance().CreateConnection()},
      _ctx{std::make_shared<ConnectionContext>(
        *_conn->context, irs::StaticStrings::kDefaultUser, id::kRootUser,
        database, database_id, nullptr, 0, nullptr)} {
    _ctx->MarkSystemWriter();
    connector::SereneDBClientState::Register(*_conn->context, _ctx);
    _conn->context->session_user =
      std::string{irs::StaticStrings::kDefaultUser};
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
    for (const auto& sql : {
           absl::StrCat("DROP FUNCTION IF EXISTS ", kSchema,
                        ".search(TEXT, INTEGER)"),
           absl::StrCat("DROP FUNCTION IF EXISTS ", kSchema, ".read(TEXT)"),
           absl::StrCat("DROP FUNCTION IF EXISTS ", kSchema, ".sections(TEXT)"),
           absl::StrCat("DROP FUNCTION IF EXISTS ", kSchema,
                        ".reference(TEXT)"),
           absl::StrCat("DROP TABLE IF EXISTS ", kTable),
           absl::StrCat("DROP TABLE IF EXISTS ", kMeta),
           absl::StrCat("DROP TEXT SEARCH DICTIONARY IF EXISTS ", kTokenizer),
           absl::StrCat("CREATE TEXT SEARCH DICTIONARY ", kTokenizer,
                        " (template = 'segmentation', case = 'lower', "
                        "break = 'alpha', frequency = true, position = true)"),
           absl::StrCat("CREATE TABLE ", kTable,
                        " (path TEXT PRIMARY KEY, title TEXT NOT NULL, "
                        "breadcrumb TEXT NOT NULL, "
                        "content TEXT NOT NULL, content_text TEXT NOT NULL) "
                        "WITH (storage = 'search', compaction_interval = 0)"),
           absl::StrCat("CREATE INDEX ", kIndex, " ON ", kTable,
                        " USING inverted (title ", kTokenizer, ", breadcrumb ",
                        kTokenizer, ", content_text ", kTokenizer, ")"),
         }) {
      if (!Run(sql)) {
        return false;
      }
    }
    if (!Insert()) {
      return false;
    }
    for (const auto& sql : {
           absl::StrCat("VACUUM (REFRESH_TABLE) ", kTable),
           absl::StrCat(
             "CREATE FUNCTION ", kSchema,
             ".search(query TEXT, max_hits INTEGER) RETURNS TABLE(path TEXT, "
             "title TEXT, breadcrumb TEXT, snippet TEXT, score DOUBLE "
             "PRECISION) LANGUAGE SQL BEGIN ATOMIC SELECT d.path, d.title, "
             "d.breadcrumb, left(regexp_replace(d.content_text, '\\s+', ' ', "
             "'g'), 400), BM25(d.tableoid) FROM ",
             kIndexRelation,
             " d WHERE d.title @@ query OR d.breadcrumb @@ query OR "
             "d.content_text @@ query ORDER BY BM25(d.tableoid) DESC, d.path "
             "LIMIT max_hits; END"),
           absl::StrCat("CREATE FUNCTION ", kSchema,
                        ".read(doc_path TEXT) RETURNS TEXT LANGUAGE SQL BEGIN "
                        "ATOMIC SELECT content FROM ",
                        kTable, " WHERE path = doc_path; END"),
           absl::StrCat("CREATE FUNCTION ", kSchema,
                        ".sections(prefix TEXT) RETURNS TABLE(path TEXT, title "
                        "TEXT, breadcrumb TEXT) LANGUAGE SQL BEGIN ATOMIC "
                        "SELECT path, title, breadcrumb FROM ",
                        kTable,
                        " WHERE starts_with(path, prefix) ORDER BY path; END"),
           absl::StrCat("CREATE FUNCTION ", kSchema,
                        ".reference(name TEXT) RETURNS TABLE(path TEXT, title "
                        "TEXT, breadcrumb TEXT) LANGUAGE SQL BEGIN ATOMIC "
                        "SELECT path, title, breadcrumb FROM ",
                        kTable,
                        " WHERE lower(title) = lower(name) OR "
                        "starts_with(lower(title), lower(name) || '(') ORDER "
                        "BY path; END"),
           absl::StrCat("CREATE TABLE ", kMeta, " (hash TEXT, layout INTEGER)"),
           absl::StrCat("INSERT INTO ", kMeta, " VALUES ('", GetDocsHash(),
                        "', ", kLayout, ")"),
           absl::StrCat("GRANT USAGE ON SCHEMA ",
                        irs::StaticStrings::kDocsSchema, " TO PUBLIC"),
           absl::StrCat("GRANT SELECT ON ", kTable, " TO PUBLIC"),
           absl::StrCat("GRANT SELECT ON ", kMeta, " TO PUBLIC"),
         }) {
      if (!Run(sql)) {
        return false;
      }
    }
    return true;
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
                                 irs::StaticStrings::kDocsSchema))) {
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
    catalog::FindDatabase(nullptr, irs::StaticStrings::kDefaultDatabase);
  if (database == nullptr) {
    SDB_WARN(GENERAL, "embedded docs: default database not found");
    return;
  }
  LoadInto(database->name.GetIdentifierName(), catalog::IdOf(*database));
}

}  // namespace sdb::docs
