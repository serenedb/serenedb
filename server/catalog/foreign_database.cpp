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

#include "catalog/foreign_database.h"

#include <absl/algorithm/container.h>

#include <duckdb/common/file_system.hpp>
#include <filesystem>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include "catalog/boot.h"
#include "catalog/database_directory.h"
#include "catalog/log.h"
#include "catalog/record.h"
#include "catalog/schema.h"
#include "search/store.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kDatabaseOption = "database";
constexpr std::string_view kDefaultDatabase = "postgres";

std::string DatabaseName(const duckdb::AttachOptions& options) {
  std::string name{kDefaultDatabase};
  for (const auto& [key, value] : options.options) {
    if (key != kDatabaseOption) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FDW_INVALID_OPTION_NAME),
        ERR_MSG("invalid option \"", key, "\""),
        ERR_HINT("A SereneDB data directory takes the options path and ",
                 kDatabaseOption, "."));
    }
    name = value.ToString();
  }
  return name;
}

void OpenForeignStore(search::Store& store, const duckdb::CatalogEntry& entry) {
  if (!store.Opened()) {
    store.Open();
  }
  if (store.Absent()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_FILE),
                    ERR_MSG("the directory \"", store.Path().string(), "\" of ",
                            entry.name.GetIdentifierName(), " (oid ", entry.oid,
                            ") is missing"));
  }
}

}  // namespace

duckdb::unique_ptr<duckdb::Catalog> AttachForeignDatabase(
  duckdb::ClientContext& context, duckdb::AttachedDatabase& db,
  const duckdb::AttachInfo& info, duckdb::AttachOptions& options) {
  const auto name = DatabaseName(options);
  std::error_code ec;
  const auto root = std::filesystem::canonical(info.path, ec);
  if (ec) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_FILE),
                    ERR_MSG("could not open data directory \"", info.path,
                            "\": ", ec.message()));
  }
  const DataDirectory layout{root.string()};
  if (!std::filesystem::is_directory(layout.LogDir(), ec)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_FILE),
      ERR_MSG("\"", info.path, "\" is not a SereneDB data directory"));
  }
  auto& fs = duckdb::FileSystem::GetFileSystem(context);
  ForeignLog foreign{
    .dir = layout.LogDir(),
    .locks = Log::LockChunks(fs, layout.LogDir()),
  };
  LoggedDatabases databases;
  Log::Read(fs, foreign.dir, [&](std::span<uint8_t> payload) {
    ReadDatabases(context, CatalogPart(payload), databases);
  });
  const auto it = absl::c_find_if(databases, [&](const auto& database) {
    return database.second.GetIdentifierName() == name;
  });
  if (it == databases.end()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
      ERR_MSG("database \"", name, "\" does not exist in \"", info.path, "\""));
  }
  foreign.database = it->first;
  return duckdb::make_uniq<SereneDBCatalog>(
    db,
    std::make_shared<DatabaseDirectory>(layout.DatabaseDir(foreign.database)),
    std::move(foreign));
}

void ReplayForeignDatabase(duckdb::ClientContext& context,
                           SereneDBCatalog& catalog,
                           const ForeignLog& foreign) {
  const auto target = [&](duckdb::idx_t id) -> BaseCatalog* {
    return id == foreign.database ? &catalog : nullptr;
  };
  Log::Read(duckdb::FileSystem::GetFileSystem(context), foreign.dir,
            [&](std::span<uint8_t> payload) {
              ReplayRecord(context, target, payload);
            });
  const auto transaction = catalog.GetCatalogTransaction(context);
  catalog.ScanSchemas(context, [&](duckdb::SchemaCatalogEntry& schema) {
    auto& own = schema.Cast<Schema>();
    own.Scan(transaction, duckdb::CatalogType::TABLE_ENTRY,
             [&](duckdb::CatalogEntry& entry) {
               if (!IsSearchTable(entry)) {
                 return;
               }
               auto& store = *entry.Cast<SearchTableEntry>().Storage();
               OpenForeignStore(store, entry);
               store.InvertReplayed(context);
               store.FinishReplay();
             });
    own.Scan(transaction, duckdb::CatalogType::INDEX_ENTRY,
             [&](duckdb::CatalogEntry& entry) {
               if (auto* index = dynamic_cast<InvertedIndexEntry*>(&entry);
                   index && index->Storage()) {
                 OpenForeignStore(*index->Storage(), entry);
               }
             });
  });
}

}  // namespace sdb::catalog
