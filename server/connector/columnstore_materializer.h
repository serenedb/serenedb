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

#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <memory>
#include <span>
#include <vector>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/exceptions.h"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/column/scan.hpp"
#include "iresearch/types.hpp"

namespace sdb::connector {

class ColumnstoreMaterializer {
 public:
  ColumnstoreMaterializer(const irs::ColReader& reader,
                          std::span<const irs::field_id> column_ids,
                          std::span<const duckdb::idx_t> output_slots);

  ColumnstoreMaterializer(const ColumnstoreMaterializer&) = delete;
  ColumnstoreMaterializer& operator=(const ColumnstoreMaterializer&) = delete;

  bool HasAny() const noexcept { return !_bound.empty(); }

  size_t BindingCount() const noexcept { return _bound.size(); }
  duckdb::idx_t BindingOutputSlot(size_t i) const noexcept {
    SDB_ASSERT(i < _bound.size());
    return _bound[i].output_slot;
  }
  const irs::ColumnReader& BindingReader(size_t i) const noexcept {
    SDB_ASSERT(i < _bound.size());
    return *_bound[i].reader;
  }

  // Write one binding's slice into `out_vec` starting at `output_start`.
  // Callers that need an unusual output shape (e.g. score-order's per-binding
  // Vector, sliced later through a DICTIONARY_VECTOR) go through this.
  template<typename DocIds>
  void MaterializeBinding(size_t i, const DocIds& doc_ids,
                          duckdb::Vector& out_vec,
                          duckdb::idx_t output_start) const {
    SDB_ASSERT(i < _bound.size());
    SDB_IF_FAILURE("SearchIncludeFetchFault") { SDB_THROW(ERROR_DEBUG); }
    const auto& b = _bound[i];
    irs::MaterializeNode(*b.reader, *b.state, doc_ids, out_vec, output_start);
  }

  template<typename DocIds>
  void SelectByDocIds(const DocIds& doc_ids, duckdb::DataChunk& output,
                      duckdb::idx_t output_start) const {
    if (_bound.empty() || doc_ids.size() == 0) {
      return;
    }
    SDB_IF_FAILURE("SearchIncludeFetchFault") { SDB_THROW(ERROR_DEBUG); }
    for (const auto& b : _bound) {
      auto& out_vec = output.data[b.output_slot];
      const auto type_id = b.reader->Type().id();
      if (output_start == 0 && (type_id == duckdb::LogicalTypeId::LIST ||
                                type_id == duckdb::LogicalTypeId::MAP)) {
        duckdb::ListVector::SetListSize(out_vec, 0);
      }
      irs::MaterializeNode(*b.reader, *b.state, doc_ids, out_vec, output_start);
    }
  }

  void Scan(uint64_t start_doc, duckdb::idx_t count, duckdb::DataChunk& output,
            duckdb::idx_t output_start) const {
    if (_bound.empty() || count == 0) {
      return;
    }
    SDB_IF_FAILURE("SearchIncludeFetchFault") { SDB_THROW(ERROR_DEBUG); }
    for (const auto& b : _bound) {
      auto& out_vec = output.data[b.output_slot];
      const auto type_id = b.reader->Type().id();
      if (output_start == 0 && (type_id == duckdb::LogicalTypeId::LIST ||
                                type_id == duckdb::LogicalTypeId::MAP)) {
        duckdb::ListVector::SetListSize(out_vec, 0);
      }
      irs::MaterializeNode(*b.reader, *b.state,
                           irs::IotaRange{start_doc, count}, out_vec,
                           output_start, output_start == 0);
    }
  }

 private:
  struct Binding {
    const irs::ColumnReader* reader;
    duckdb::idx_t output_slot;
    std::unique_ptr<irs::MaterializeState> state;
  };

  irs::ReadContext _ctx;
  std::vector<Binding> _bound;
};

}  // namespace sdb::connector
