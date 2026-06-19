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

#include "connector/duckdb_physical_search_truncate.h"

#include <cstdint>
#include <duckdb/common/types/data_chunk.hpp>
#include <memory>

#include "catalog/identifiers/object_id.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "query/transaction.h"
#include "search/search_table.h"

namespace sdb::connector {

SereneDBSearchTruncate::SereneDBSearchTruncate(
  duckdb::PhysicalPlan& plan, std::shared_ptr<catalog::Table> table,
  duckdb::idx_t estimated_cardinality)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             {duckdb::LogicalType::BIGINT},
                             estimated_cardinality),
    _table(std::move(table)) {}

duckdb::SourceResultType SereneDBSearchTruncate::GetDataInternal(
  duckdb::ExecutionContext& context, duckdb::DataChunk& /*chunk*/,
  duckdb::OperatorSourceInput& /*input*/) const {
  auto& conn_ctx = GetSereneDBContext(context.client);
  const auto& search = _table->GetData();
  conn_ctx.SearchTxn().AddSearchTruncate(search);

  if (const int64_t current = search->NumRows(); current > 0) {
    conn_ctx.UpdateNumRows(_table->GetId(), -current);
  }
  return duckdb::SourceResultType::FINISHED;
}

}  // namespace sdb::connector
