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

#include <duckdb/common/types/data_chunk.hpp>
#include <memory>
#include <span>
#include <vector>

#include "connector/column_extract.h"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"

namespace sdb::connector {

class FullScanner {
 public:
  FullScanner(const irs::ColReader& reader,
              std::span<const ColumnstoreProjection> projections,
              duckdb::ClientContext* context);

  FullScanner(const FullScanner&) = delete;
  FullScanner& operator=(const FullScanner&) = delete;

  bool HasAny() const noexcept { return !_bound.empty(); }

  void Scan(uint64_t start_row, duckdb::idx_t count, duckdb::DataChunk& output);

 private:
  struct Binding {
    const irs::ColumnReader* reader = nullptr;
    duckdb::idx_t output_slot = 0;
    bool is_list_like = false;
    std::unique_ptr<irs::ColumnReader::ScanState> state;
    std::unique_ptr<ExtractBinding> extract;
  };

  irs::ReadContext _ctx;
  std::vector<Binding> _bound;
};

}  // namespace sdb::connector
