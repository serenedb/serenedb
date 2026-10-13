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

#include "connector/copy_from_batches.h"

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/main/settings.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <utility>

#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"

namespace sdb::connector {
namespace {

constexpr size_t kOrderedRun = 16;

duckdb::OperatorPartitionData CopyFromBatchesPartitionData(
  duckdb::ClientContext&, duckdb::TableFunctionGetPartitionInput& input) {
  if (input.partition_info.RequiresPartitionColumns()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("COPY FROM: partition columns are not supported"));
  }
  return duckdb::OperatorPartitionData{
    input.local_state->Cast<CopyFromBatchesLocalState>().batch_index};
}

bool CopyInPreservesOrder(duckdb::ClientContext& context,
                          const duckdb::TableCatalogEntry* table) {
  return table != nullptr && table->IsDuckTable() &&
         duckdb::Settings::Get<duckdb::PreserveInsertionOrderSetting>(context);
}

}  // namespace

pg::CopyInBatches& RequireCopyInBatches(duckdb::ClientContext& context,
                                        std::string_view format) {
  auto* state =
    context.registered_state->Get<SereneDBClientState>(kSereneDBClientStateKey)
      .get();
  if (state == nullptr) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("COPY ... FROM STDIN (FORMAT ", format,
                            ") is only valid on a server-side PostgreSQL wire "
                            "connection"));
  }
  auto* batches =
    state->GetConnectionContext().GetSideChannel<pg::CopyInBatches>();
  if (batches == nullptr) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("COPY ... FROM STDIN (FORMAT ", format,
                            ") requires a PG wire connection (transport not "
                            "attached)"));
  }
  return *batches;
}

CopyFromBatchesGlobalState::CopyFromBatchesGlobalState(
  duckdb::ClientContext& context, pg::CopyInBatches& queue,
  pg::CopyInTarget target, bool ordered)
  : queue{queue},
    target{std::move(target)},
    ordered{ordered},
    max_threads{duckdb::TaskScheduler::QueryThreads(context)} {}

void BindCopyFromBatches(duckdb::ClientContext& context,
                         duckdb::CopyFromFunctionBindInput& input,
                         std::string_view format, bool& ordered) {
  RequireCopyInBatches(context, format);
  const auto table = duckdb::Catalog::GetEntry<duckdb::TableCatalogEntry>(
    context, input.info.GetQualifiedName(),
    duckdb::OnEntryNotFound::RETURN_NULL);
  ordered = CopyInPreservesOrder(context, table.get());
  if (ordered) {
    input.tf.get_partition_data = CopyFromBatchesPartitionData;
  } else {
    input.tf.get_partition_data = nullptr;
    input.tf.order_preservation_type = duckdb::OrderPreservationType::NO_ORDER;
  }
}

void ScanCopyFromBatches(duckdb::ClientContext& context,
                         duckdb::TableFunctionInput& input,
                         duckdb::DataChunk& output) {
  auto& g = input.global_state->Cast<CopyFromBatchesGlobalState>();
  auto& l = input.local_state->Cast<CopyFromBatchesLocalState>();
  if (!l.parser) {
    l.parser.emplace(context, g.target);
  }
  auto& parser = *l.parser;
  duckdb::idx_t row = 0;
  while (row < STANDARD_VECTOR_SIZE) {
    if (!parser.Active()) {
      if (l.next == l.run.size()) {
        l.run.clear();
        l.next = 0;
        if (row != 0) {
          if (g.ordered || !g.queue.TryTake(1, l.run)) {
            break;
          }
        } else if (g.queue.Take(input, g.ordered ? kOrderedRun : 1, l.run) !=
                   pg::CopyTake::Batches) {
          break;
        }
        l.batch_index = l.run.front()->seq;
      }
      parser.Start(*l.run[l.next++]);
    }
    row = parser.Parse(output, row);
    if (parser.Exhausted()) {
      g.queue.Done(parser.Seq());
      parser.Stop();
    }
  }
  output.SetChildCardinality(row);
}

}  // namespace sdb::connector
