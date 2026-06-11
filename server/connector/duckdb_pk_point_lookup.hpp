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

#include <duckdb.hpp>
#include <span>
#include <string>
#include <vector>

#include "connector/duckdb_rocksdb_reader.h"
#include "connector/duckdb_scan_base.hpp"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace rocksdb {

class PinnableSlice;
}

namespace sdb::connector {

struct PKPointLookupGlobalState : public CommonScanGlobalState {
  size_t point_offset = 0;
  std::vector<std::string> pk_suffixes;
  std::vector<std::string> pk_multiget_key_storage;
  std::vector<rocksdb::Slice> pk_multiget_key_slices;
  std::vector<size_t> pk_found_indices;
};

// Result collector for PK point lookups: deserializes values into the output
// vector and records which batch indices had a hit.
class DuckDBPKResultCollector {
 public:
  void Init(duckdb::LogicalType type, size_t capacity, duckdb::Vector& vec) {
    _type = std::move(type);
    _vec = &vec;
    _found_indices.clear();
    _found_indices.reserve(capacity);
  }

  void Fill(size_t batch_idx, size_t found_idx,
            const rocksdb::PinnableSlice& val) {
    _found_indices.push_back(batch_idx);
    DeserializeValueIntoDuckDB(val.ToStringView(), *_vec, _type, found_idx);
  }

  void Finish(size_t /*found_count*/) {}

  std::span<const size_t> PresentRows() const { return _found_indices; }

 private:
  duckdb::LogicalType _type;
  duckdb::Vector* _vec = nullptr;
  std::vector<size_t> _found_indices;
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> PKPointLookupInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input);

void PKPointLookupFunction(duckdb::ClientContext& context,
                           duckdb::TableFunctionInput& data,
                           duckdb::DataChunk& output);

}  // namespace sdb::connector
