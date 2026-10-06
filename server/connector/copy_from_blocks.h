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

#pragma once

#include <cstddef>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/function/copy_function.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "connector/copy_byte_source.h"
#include "pg/deserialize.h"

namespace sdb::connector {

inline constexpr size_t kCopyBlockBytes = 1 << 20;

struct CopyBlock {
  std::string data;
  duckdb::idx_t batch = 0;

  bool Empty() const noexcept { return data.empty(); }
};

struct CopyFromGlobalState : public duckdb::GlobalTableFunctionState {
  std::unique_ptr<ByteSource> source;
  std::vector<pg::DeserializationFunction<pg::VectorSink>> deserializers;
  bool finished = false;

  duckdb::idx_t MaxThreads() const final { return _max_threads; }

  void StartParallel(duckdb::ClientContext& context) {
    _first = Numbered(CutBlock());
    if (!finished) {
      _max_threads =
        duckdb::TaskScheduler::GetScheduler(context).NumberOfThreads();
    }
  }

  CopyBlock NextBlock() {
    std::lock_guard lock{_mu};
    if (!_first.Empty()) {
      return std::exchange(_first, {});
    }
    return Numbered(CutBlock());
  }

 protected:
  virtual std::string CutBlock() = 0;

  std::string_view Pull(std::string& data) {
    const auto view = source->View();
    if (view.empty()) {
      source->DrainToEof();
      finished = true;
      return {};
    }
    const size_t from = data.size();
    data.append(view);
    source->Next(view.size());
    source->View();
    return std::string_view{data}.substr(from);
  }

 private:
  CopyBlock Numbered(std::string data) {
    if (data.empty()) {
      return {};
    }
    return {std::move(data), _next_batch++};
  }

  std::mutex _mu;
  CopyBlock _first;
  duckdb::idx_t _next_batch = 0;
  duckdb::idx_t _max_threads = 1;
};

struct CopyFromLocalState : public duckdb::LocalTableFunctionState {
  CopyBlock block;
  size_t pos = 0;
  pg::DeserializeContext dctx;

  bool Exhausted() const noexcept { return pos >= block.data.size(); }

  bool Claim(CopyFromGlobalState& global) {
    block = global.NextBlock();
    pos = 0;
    return !block.Empty();
  }
};

inline bool ParallelCopyFrom(duckdb::ClientContext& context,
                             duckdb::CopyFromFunctionBindInput& input) {
  const auto table = duckdb::Catalog::GetEntry<duckdb::TableCatalogEntry>(
    context, input.info.GetQualifiedName(),
    duckdb::OnEntryNotFound::RETURN_NULL);
  const bool parallel = table != nullptr && !table->IsDuckTable();
  if (!parallel) {
    input.tf.get_partition_data = nullptr;
  }
  return parallel;
}

inline duckdb::OperatorPartitionData CopyFromPartitionData(
  duckdb::ClientContext&, duckdb::TableFunctionGetPartitionInput& input) {
  if (input.partition_info.RequiresPartitionColumns()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("COPY FROM: partition columns are not supported"));
  }
  return duckdb::OperatorPartitionData{
    input.local_state->Cast<CopyFromLocalState>().block.batch};
}

}  // namespace sdb::connector
