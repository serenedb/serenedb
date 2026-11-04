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

#include "query/engine_host.h"

#include <autocomplete_extension.hpp>
#include <avro_extension.hpp>
#include <azure_extension.hpp>
#include <clickhouse_scanner_extension.hpp>
#include <core_functions_extension.hpp>
#include <duckdb/inet/inet_extension.hpp>
#include <duckdb/optimizer/cte_filter_pusher.hpp>
#include <duckdb/optimizer/optimizer.hpp>
#include <httpfs_extension.hpp>
#include <iceberg_extension.hpp>
#include <icu_extension.hpp>
#include <json_extension.hpp>
#include <markdown_extension.hpp>
#include <parquet_extension.hpp>
#include <planning/iceberg_planner.hpp>
#include <postgres_scanner_extension.hpp>
#include <shell_extension.hpp>
#include <spatial/operators/spatial_join_optimizer.hpp>
#include <spatial/operators/spatial_operator_extension.hpp>
#include <spatial/spatial_extension.hpp>
#include <storage/clickhouse_optimizer.hpp>
#include <storage/postgres_optimizer.hpp>
#include <tpcds_extension.hpp>
#include <tpch_extension.hpp>

#include "connector/functions/ts_query.h"
#include "connector/optimizer/iresearch_plan.h"
#include "connector/optimizer/wrap_unsupported_types.h"
#include "pg/catalog/engine/registry.h"
#include "pg/catalog/functions/functions.h"

namespace sdb::server::query {

void EngineHost::LoadExtensions(duckdb::DuckDB& db) {
  db.LoadStaticExtension<duckdb::CoreFunctionsExtension>();
  db.LoadStaticExtension<duckdb::ParquetExtension>();
  db.LoadStaticExtension<duckdb::TpchExtension>();
  db.LoadStaticExtension<duckdb::TpcdsExtension>();
  db.LoadStaticExtension<duckdb::JsonExtension>();
  db.LoadStaticExtension<duckdb::HttpfsExtension>();
  db.LoadStaticExtension<duckdb::AvroExtension>();
  db.LoadStaticExtension<duckdb::IcebergExtension>();
  db.LoadStaticExtension<PostgresScannerExtension>();
  db.LoadStaticExtension<duckdb::AutocompleteExtension>();
  db.LoadStaticExtension<duckdb::IcuExtension>();
  db.LoadStaticExtension<duckdb::InetExtension>();
  db.LoadStaticExtension<duckdb::AzureExtension>();
  db.LoadStaticExtension<duckdb::SpatialExtension>();
  db.LoadStaticExtension<duckdb::MarkdownExtension>();
  db.LoadStaticExtension<ClickhouseScannerExtension>();
  // Attached iceberg tables read CURRENT metadata: the default txn-start
  // time travel errors out any catalog enumeration that walks a table
  // created after the reader's transaction began.
  duckdb::DBConfig::GetConfig(*db.instance)
    .SetOptionByName("iceberg_use_metadata_log", duckdb::Value::BOOLEAN(false));
}

std::span<const duckdb::DefaultType> EngineHost::GetTypes() {
  return pg::ExternalTypes();
}

void EngineHost::OnConnectionOpened(duckdb::ClientContext& context) {
  PostgresScannerExtension::OnConnectionOpened(context);
  ClickhouseScannerExtension::OnConnectionOpened(context);
}

duckdb::BoundStatement EngineHost::BindStatement(duckdb::ClientContext&,
                                                 duckdb::Binder&,
                                                 duckdb::SQLStatement&) {
  return {};
}

void EngineHost::PostBind(duckdb::ClientContext& context,
                          duckdb::Binder& binder,
                          duckdb::BoundStatement& statement) {
  duckdb::PlannerExtensionInput input{context, binder, nullptr};
  duckdb::IcebergPlanner::PostBind(input, statement);
  switch (_mode) {
    case EngineMode::kServer:
      sdb::optimizer::WrapVariantOutputs(input, statement);
      break;
    case EngineMode::kShell:
      duckdb::ShellExtension::PostBind(input, statement);
      break;
  }
}

duckdb::GetSQLValueFunctionReturnType EngineHost::BindSQLValueFunction(
  duckdb::ClientContext& context, duckdb::Binder& binder,
  const duckdb::string& name,
  duckdb::unique_ptr<duckdb::ParsedExpression>& result) {
  if (_mode != EngineMode::kServer) {
    return duckdb::GetSQLValueFunctionReturnType::CONTINUE_BINDING;
  }
  duckdb::PlannerExtensionInput input{context, binder, nullptr};
  return pg::SystemUserValueFunction(input, name, result);
}

duckdb::unique_ptr<duckdb::LogicalExtensionOperator>
EngineHost::DeserializeOperator(duckdb::ClientContext&,
                                const duckdb::string& extension,
                                duckdb::Deserializer& deserializer) {
  if (extension == duckdb::SpatialOperators::EXTENSION_NAME) {
    return duckdb::SpatialOperators::Deserialize(deserializer);
  }
  return nullptr;
}

void EngineHost::PreOptimize(
  duckdb::Optimizer& optimizer,
  duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  if (_mode != EngineMode::kServer) {
    return;
  }
  optimizer.RunOptimizer(duckdb::OptimizerType::EXTENSION, [&] {
    duckdb::CTEFilterPusher::ClearDependencies(*plan);
    duckdb::OptimizerExtensionInput input{optimizer.GetContext(), optimizer,
                                          nullptr};
    connector::FoldTSQueryConstants(input, plan);
  });
}

void EngineHost::BeforeOptimizer(
  duckdb::OptimizerType type, duckdb::Optimizer& optimizer,
  duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  if (_mode == EngineMode::kServer &&
      type == duckdb::OptimizerType::FILTER_PUSHDOWN) {
    duckdb::OptimizerExtensionInput input{optimizer.GetContext(), optimizer,
                                          nullptr};
    sdb::optimizer::RewriteSearchCallsToColumnRefs(input, plan);
  }
}

void EngineHost::AfterOptimizer(
  duckdb::OptimizerType type, duckdb::Optimizer& optimizer,
  duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  if (_mode == EngineMode::kServer &&
      type == duckdb::OptimizerType::FILTER_PUSHDOWN) {
    duckdb::OptimizerExtensionInput input{optimizer.GetContext(), optimizer,
                                          nullptr};
    sdb::optimizer::LimitTsDictScans(input, plan);
  }
}

void EngineHost::PostOptimize(
  duckdb::Optimizer& optimizer,
  duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  optimizer.RunOptimizer(duckdb::OptimizerType::EXTENSION, [&] {
    duckdb::OptimizerExtensionInput input{optimizer.GetContext(), optimizer,
                                          nullptr};
    duckdb::PostgresOptimizer::Optimize(input, plan);
    duckdb::SpatialJoinOptimizer::Optimize(input, plan);
    duckdb::ClickHouseOptimizer::Optimize(input, plan);
  });
}

bool EngineHost::IsRewriteBarrier(duckdb::ClientContext&,
                                  const duckdb::Expression& parent,
                                  bool parent_frozen) {
  return _mode == EngineMode::kServer &&
         connector::MinMatchRewriteBarrier(parent, parent_frozen);
}

}  // namespace sdb::server::query
