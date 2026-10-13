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
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/function/copy_function.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <optional>
#include <string_view>
#include <vector>

#include "connector/copy_batch_parse.h"
#include "pg/copy_in_batches.h"

namespace sdb::connector {

inline constexpr std::string_view kCopyStdinPath = "/dev/stdin";

pg::CopyInBatches& RequireCopyInBatches(duckdb::ClientContext& context,
                                        std::string_view format);

struct CopyFromBatchesGlobalState final
  : public duckdb::GlobalTableFunctionState {
  CopyFromBatchesGlobalState(duckdb::ClientContext& context,
                             pg::CopyInBatches& queue, pg::CopyInTarget target,
                             bool ordered);

  duckdb::idx_t MaxThreads() const final { return max_threads; }

  pg::CopyInBatches& queue;
  pg::CopyInTarget target;
  bool ordered;
  duckdb::idx_t max_threads;
};

struct CopyFromBatchesLocalState final
  : public duckdb::LocalTableFunctionState {
  std::optional<CopyBatchParser> parser;
  std::vector<pg::CopyBatch*> run;
  size_t next = 0;
  duckdb::idx_t batch_index = 0;
};

void BindCopyFromBatches(duckdb::ClientContext& context,
                         duckdb::CopyFromFunctionBindInput& input,
                         std::string_view format, bool& ordered);

void ScanCopyFromBatches(duckdb::ClientContext& context,
                         duckdb::TableFunctionInput& input,
                         duckdb::DataChunk& output);

}  // namespace sdb::connector
