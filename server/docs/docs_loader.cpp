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

constexpr std::string_view kSchema = "sdb_docs";
constexpr std::string_view kTable = "sdb_docs.docs";
constexpr std::string_view kMeta = "sdb_docs.meta";
constexpr std::string_view kIndex = "docs_fts";
constexpr std::string_view kDictionary = "sdb_docs.english";
constexpr int kLayout = 11;
constexpr size_t kInsertBatch = 32;
constexpr std::string_view kStopWords =
  "\"a\",\"an\",\"also\",\"are\",\"be\",\"been\",\"but\",\"can\","
  "\"do\",\"does\",\"for\",\"has\",\"have\",\"it\",\"its\",\"may\","
  "\"of\",\"should\",\"so\",\"such\",\"than\",\"that\",\"the\","
  "\"their\",\"there\",\"these\",\"they\",\"this\",\"to\",\"was\","
  "\"were\",\"which\",\"will\",\"would\"";

class Loader {
 public:
  explicit Loader(ObjectId database_id)
    : _conn{DuckDBEngine::Instance().CreateConnection()},
      _ctx{std::make_shared<ConnectionContext>(
        *_conn->context, StaticStrings::kDefaultUser, id::kRootUser,
        StaticStrings::kDefaultDatabase, database_id, nullptr, 0, nullptr)} {
    connector::SereneDBClientState::Register(*_conn->context, _ctx);
    _conn->context->session_user = std::string{StaticStrings::kDefaultUser};
    std::vector<duckdb::CatalogSearchEntry> paths{
      duckdb::CatalogSearchEntry{
        duckdb::Identifier{std::string{StaticStrings::kDefaultDatabase}},
        duckdb::Identifier{"$user"}},
      duckdb::CatalogSearchEntry{
        duckdb::Identifier{std::string{StaticStrings::kDefaultDatabase}},
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
           absl::StrCat("DROP TABLE IF EXISTS ", kTable),
           absl::StrCat("DROP TABLE IF EXISTS ", kMeta),
           absl::StrCat("DROP TEXT SEARCH DICTIONARY IF EXISTS ", kDictionary),
           absl::StrCat("CREATE TEXT SEARCH DICTIONARY ", kDictionary,
                        " (template = 'text', locale = 'en_US.UTF-8', "
                        "case = 'lower', stemming = true, accent = false, "
                        "frequency = true, position = true, stopwords = '",
                        kStopWords, "')"),
           absl::StrCat("CREATE TABLE ", kTable,
                        " (path TEXT PRIMARY KEY, title TEXT NOT NULL, "
                        "breadcrumb TEXT NOT NULL, "
                        "content TEXT NOT NULL, content_text TEXT NOT NULL) "
                        "WITH (storage = 'search', compaction_interval = 0)"),
           absl::StrCat("CREATE INDEX ", kIndex, " ON ", kTable,
                        " USING inverted (title ", kDictionary, ", breadcrumb ",
                        kDictionary, ", content_text ", kDictionary, ")"),
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
           absl::StrCat("CREATE TABLE ", kMeta, " (hash TEXT, layout INTEGER)"),
           absl::StrCat("INSERT INTO ", kMeta, " VALUES ('", GetDocsHash(),
                        "', ", kLayout, ")"),
           absl::StrCat("GRANT USAGE ON SCHEMA ", kSchema, " TO PUBLIC"),
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

}  // namespace

void LoadEmbeddedDocs() {
  if (GetDocs().empty()) {
    SDB_INFO(STARTUP,
             "embedded docs disabled (built with SDB_EMBEDDED_DOCS=OFF)");
    return;
  }
  const auto begin = std::chrono::steady_clock::now();
  try {
    const auto* database =
      catalog::FindDatabase(nullptr, StaticStrings::kDefaultDatabase);
    if (database == nullptr) {
      SDB_WARN(GENERAL, "embedded docs: default database not found");
      return;
    }
    Loader loader{catalog::IdOf(*database)};
    if (!loader.Run(absl::StrCat("CREATE SCHEMA IF NOT EXISTS ", kSchema))) {
      // TODO warning?
      return;
    }
    if (loader.UpToDate()) {
      SDB_INFO(STARTUP, "embedded docs are up to date (", GetDocs().size(),
               " rows)");
      return;
    }
    if (!loader.Rebuild()) {
      return;
    }
    SDB_INFO(STARTUP, "embedded docs loaded: ", GetDocs().size(), " rows into ",
             kTable, " in ",
             absl::FormatDuration(
               absl::FromChrono(std::chrono::steady_clock::now() - begin)));
  } catch (const std::exception& e) {
    SDB_WARN(GENERAL, "embedded docs: load failed: ", e.what());
  }
}

}  // namespace sdb::docs
