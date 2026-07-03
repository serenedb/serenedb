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

#include "connector/optimizer/copy_to_progress.h"

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/function/table/table_scan.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/optimizer/optimizer_extension.hpp>
#include <duckdb/parser/parsed_data/copy_info.hpp>
#include <duckdb/planner/operator/logical_copy_to_file.hpp>
#include <duckdb/planner/operator/logical_extension_operator.hpp>
#include <duckdb/planner/operator/logical_get.hpp>

#include "catalog/catalog.h"
#include "catalog/database.h"
#include "catalog/store/store.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_physical_progress.h"
#include "connector/duckdb_table_function.h"
#include "pg/connection_context.h"

namespace sdb::optimizer {
namespace {

// Facade scans retarget to the __sdb_store DuckTableEntry at bind, so the
// facade table id has to be recovered from the store-table naming scheme.
ObjectId ResolveStoreScanTableId(duckdb::ClientContext& context,
                                 const duckdb::TableCatalogEntry& store_entry) {
  auto* conn = connector::GetSereneDBContextPtr(context);
  if (!conn) {
    return {};
  }
  const auto& db = conn->GetDatabasePtr();
  const auto& snapshot = conn->CatalogSnapshot();
  if (!db || !snapshot) {
    return {};
  }
  const auto& store_name = store_entry.name.GetIdentifierName();
  for (const auto& schema : snapshot->GetSchemas(db->GetId())) {
    for (const auto& table :
         snapshot->GetTables(db->GetId(), schema->GetName())) {
      if (catalog::StoreTableName(db->GetName(), schema->GetName(),
                                  table->GetName()) == store_name) {
        return table->GetId();
      }
    }
  }
  return {};
}

// PG reports the source relation for `COPY table TO ...` and 0 for the
// query form; mirror that by resolving a plain scan under the sink.
ObjectId FindSourceTableId(duckdb::ClientContext& context,
                           const duckdb::LogicalOperator& op) {
  const duckdb::LogicalOperator* cur = &op;
  while (cur->type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION &&
         cur->children.size() == 1) {
    cur = cur->children[0].get();
  }
  if (cur->type != duckdb::LogicalOperatorType::LOGICAL_GET) {
    return {};
  }
  const auto& get = cur->Cast<duckdb::LogicalGet>();
  if (const auto* bind = dynamic_cast<const connector::SereneDBScanBindData*>(
        get.bind_data.get())) {
    return bind->RelationId();
  }
  if (const auto* bind =
        dynamic_cast<const duckdb::TableScanBindData*>(get.bind_data.get())) {
    return ResolveStoreScanTableId(context, bind->table);
  }
  return {};
}

class LogicalCopyToProgress final : public duckdb::LogicalExtensionOperator {
 public:
  LogicalCopyToProgress(duckdb::unique_ptr<duckdb::LogicalOperator> child,
                        ObjectId table_id, pg::copy_progress::Type type)
    : _table_id(table_id), _type(type) {
    children.emplace_back(std::move(child));
  }

  duckdb::PhysicalOperator& CreatePlan(
    duckdb::ClientContext& context,
    duckdb::PhysicalPlanGenerator& planner) final {
    auto& child = planner.CreatePlan(*children[0]);
    return planner.Make<connector::SereneDBPhysicalProgress>(
      child, _table_id, pg::copy_progress::Command::CopyTo, _type);
  }

  duckdb::vector<duckdb::ColumnBinding> GetColumnBindings() final {
    return children[0]->GetColumnBindings();
  }

  std::string GetName() const final { return "SDB_COPY_TO_PROGRESS"; }
  std::string GetExtensionName() const final { return "serenedb"; }

 protected:
  void ResolveTypes() final { types = children[0]->types; }

 private:
  ObjectId _table_id;
  pg::copy_progress::Type _type;
};

void WrapCopyToWithProgress(duckdb::OptimizerExtensionInput& input,
                            duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  if (!plan ||
      plan->type != duckdb::LogicalOperatorType::LOGICAL_COPY_TO_FILE ||
      plan->children.size() != 1) {
    return;
  }
  auto& copy = plan->Cast<duckdb::LogicalCopyToFile>();
  const auto type = copy.file_path == "/dev/stdout"
                      ? pg::copy_progress::Type::Pipe
                      : pg::copy_progress::Type::File;
  // PG sets relid only for `COPY table TO ...`; the query form reports 0.
  const bool is_table_form =
    copy.copy_info && !copy.copy_info->Table().GetIdentifierName().empty();
  const auto table_id = is_table_form
                          ? FindSourceTableId(input.context, *copy.children[0])
                          : ObjectId{};
  copy.children[0] = duckdb::make_uniq<LogicalCopyToProgress>(
    std::move(copy.children[0]), table_id, type);
}

}  // namespace

void RegisterCopyToProgressOptimizer(duckdb::DatabaseInstance& db) {
  duckdb::OptimizerExtension::Register(
    duckdb::DBConfig::GetConfig(db),
    duckdb::OptimizerExtension{
      .optimize_function = &WrapCopyToWithProgress,
    });
}

}  // namespace sdb::optimizer
