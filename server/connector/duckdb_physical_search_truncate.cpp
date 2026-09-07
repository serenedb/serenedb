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
#include <iresearch/search/all_filter.hpp>
#include <memory>

#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "query/transaction.h"
#include "search/search_table.h"

namespace sdb::connector {

SereneDBSearchTruncate::SereneDBSearchTruncate(
  duckdb::PhysicalPlan& plan, std::shared_ptr<search::SearchTable> data,
  duckdb::idx_t estimated_cardinality, bool clears_shard)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             {duckdb::LogicalType::BIGINT},
                             estimated_cardinality),
    _data(std::move(data)),
    _clears_shard(clears_shard) {}

duckdb::SourceResultType SereneDBSearchTruncate::GetDataInternal(
  duckdb::ExecutionContext& context, duckdb::DataChunk& /*chunk*/,
  duckdb::OperatorSourceInput& /*input*/) const {
  auto& search_txn = GetSereneDBContext(context.client).SearchTxn();
  if (!_clears_shard) {
    search_txn
      .EnsureSerialSearchTransaction(_data,
                                     [&] { return _data->GetTransaction(); })
      .Remove(std::make_shared<irs::All>());
  }
  search_txn.AddSearchTruncate(_data, _clears_shard);
  return duckdb::SourceResultType::FINISHED;
}

}  // namespace sdb::connector
