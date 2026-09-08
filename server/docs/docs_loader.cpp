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

#include <absl/flags/flag.h>
#include <absl/strings/str_cat.h>
#include <absl/synchronization/mutex.h>
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
#include <utility>
#include <vector>

#include "basics/containers/flat_hash_set.h"
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


// TODO: fix when cross database reference will be supported
ABSL_FLAG(std::string, embedded_docs, "all",
          "Where the embedded documentation is loaded: all (every database, on "
          "its first connection), default (the default database only), off.");

namespace sdb::docs {
namespace {

enum class Scope { All, DefaultDatabase, Off };

Scope gScope = Scope::All;

Scope ParseScope() {
  const auto value = absl::GetFlag(FLAGS_embedded_docs);
  if (value == "all") {
    return Scope::All;
  }
  if (value == "default") {
    return Scope::DefaultDatabase;
  }
  if (value == "off") {
    return Scope::Off;
  }
  SDB_FATAL(GENERAL, "--embedded_docs must be all, default or off, got '",
            value, "'");
}

bool InScope(std::string_view database) {
  switch (gScope) {
    case Scope::All:
      return true;
    case Scope::DefaultDatabase:
      return database == StaticStrings::kDefaultDatabase;
    case Scope::Off:
      return false;
  }
  return false;
}

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
           absl::StrCat("GRANT USAGE ON SCHEMA ", StaticStrings::kDocsSchema,
                        " TO PUBLIC"),
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

struct Registry {
  absl::Mutex mu;
  containers::FlatHashSet<ObjectId> done;
  containers::FlatHashSet<ObjectId> loading;
};

Registry& GetRegistry() {
  static Registry registry;
  return registry;
}

struct NotLoading {
  Registry* registry;
  ObjectId id;
  bool Check() const { return !registry->loading.contains(id); }
};

void EnsureIn(std::string_view database, ObjectId database_id) {
  auto& registry = GetRegistry();
  {
    absl::MutexLock lock{&registry.mu};
    const NotLoading wait{&registry, database_id};
    registry.mu.Await(absl::Condition(&wait, &NotLoading::Check));
    if (registry.done.contains(database_id)) {
      return;
    }
    registry.loading.insert(database_id);
  }
  LoadInto(database, database_id);
  absl::MutexLock lock{&registry.mu};
  registry.loading.erase(database_id);
  registry.done.insert(database_id);
}

}  // namespace

void LoadEmbeddedDocs() {
  if (GetDocs().empty()) {
    SDB_INFO(STARTUP,
             "embedded docs disabled (built with SDB_EMBEDDED_DOCS=OFF)");
    return;
  }
  gScope = ParseScope();
  if (gScope == Scope::Off) {
    SDB_INFO(STARTUP, "embedded docs disabled (--embedded_docs=off)");
    return;
  }
  std::vector<std::pair<std::string, ObjectId>> databases;
  catalog::VisitDatabases(nullptr, [&](catalog::SereneDBDatabaseEntry& entry) {
    databases.emplace_back(entry.name.GetIdentifierName(),
                           catalog::IdOf(entry));
  });
  for (const auto& [name, id] : databases) {
    if (InScope(name)) {
      EnsureIn(name, id);
    }
  }
}

void EnsureEmbeddedDocs(ObjectId database_id) {
  if (GetDocs().empty()) {
    return;
  }
  {
    auto& registry = GetRegistry();
    absl::MutexLock lock{&registry.mu};
    if (registry.done.contains(database_id)) {
      return;
    }
  }
  const auto* database = catalog::FindDatabase(nullptr, database_id);
  if (database == nullptr || !InScope(database->name.GetIdentifierName())) {
    return;
  }
  EnsureIn(database->name.GetIdentifierName(), database_id);
}

}  // namespace sdb::docs
