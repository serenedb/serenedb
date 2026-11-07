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

#include "connector/duckdb_storage_extension.h"

#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>
#include <duckdb/planner/planner_extension.hpp>
#include <duckdb/storage/storage_extension.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <vector>

#include "catalog/boot.h"
#include "catalog/catalog.h"
#include "connector/optimizer/iresearch_plan.h"
#include "connector/optimizer/wrap_unsupported_types.h"
#include "pg/catalog/functions/functions.h"
#include "search/store.h"

namespace sdb::connector {
namespace {

duckdb::unique_ptr<duckdb::Catalog> AttachSereneDB(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo>, duckdb::ClientContext&,
  duckdb::AttachedDatabase&, const duckdb::string&, duckdb::AttachInfo& info,
  duckdb::AttachOptions&) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
    ERR_MSG("cannot attach \"", info.path,
            "\" as a SereneDB database: a SereneDB database is created "
            "with CREATE DATABASE"));
}

}  // namespace

std::vector<std::shared_ptr<search::Store>> InvertedStorages(
  duckdb::AttachedDatabase& db) {
  std::vector<std::shared_ptr<search::Store>> storages;
  catalog::ForEachSearchStorage(
    db.GetCatalog().Cast<catalog::SereneDBCatalog>(),
    [](catalog::SearchTableEntry&) {},
    [&](catalog::InvertedIndexEntry& index) {
      storages.push_back(index.Storage());
    });
  return storages;
}

void RegisterSereneDBStorage(duckdb::DBConfig& config) {
  auto extension = duckdb::make_shared_ptr<duckdb::StorageExtension>();
  extension->attach = AttachSereneDB;
  extension->create_transaction_manager = catalog::CreateTransactionManager;
  duckdb::StorageExtension::Register(config, "serenedb", std::move(extension));
}

void RegisterSereneDBOptimizers(duckdb::DatabaseInstance& db) {
  duckdb::PlannerExtension::Register(
    duckdb::DBConfig::GetConfig(db),
    duckdb::PlannerExtension{
      .post_bind_function = &optimizer::WrapVariantOutputs,
      .get_sql_value_function = &pg::SystemUserValueFunction,
    });
  optimizer::RegisterIResearchPlanOptimizer(db);
}

}  // namespace sdb::connector
