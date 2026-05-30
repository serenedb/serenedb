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
#include "iresearch/columnstore/column_reader.hpp"
#include "iresearch/columnstore/format.hpp"
#include "iresearch/columnstore/read_context.hpp"
#include "iresearch/columnstore/scan.hpp"
#include "iresearch/types.hpp"

namespace sdb::connector {

class ColumnstoreMaterializer {
 public:
  ColumnstoreMaterializer(const irs::columnstore::Reader& reader,
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
  const irs::columnstore::ColumnReader& BindingReader(size_t i) const noexcept {
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
    SDB_IF_FAILURE("SearchIncludeFetchFault") { SDB_THROW(sdb::ERROR_DEBUG); }
    const auto& b = _bound[i];
    irs::columnstore::MaterializeNode(*b.reader, *b.state, doc_ids, out_vec,
                                      output_start);
  }

  void SelectByDocIds(std::span<const irs::doc_id_t> doc_ids,
                      duckdb::DataChunk& output,
                      duckdb::idx_t output_start = 0) const {
    if (_bound.empty() || doc_ids.empty()) {
      return;
    }
    SDB_IF_FAILURE("SearchIncludeFetchFault") { SDB_THROW(sdb::ERROR_DEBUG); }
    for (const auto& b : _bound) {
      auto& out_vec = output.data[b.output_slot];
      const auto type_id = b.reader->Type().id();
      if (output_start == 0 && (type_id == duckdb::LogicalTypeId::LIST ||
                                type_id == duckdb::LogicalTypeId::MAP)) {
        duckdb::ListVector::SetListSize(out_vec, 0);
      }
      irs::columnstore::MaterializeNode(*b.reader, *b.state, doc_ids, out_vec,
                                        output_start);
    }
  }

  void Scan(uint64_t start_doc, duckdb::idx_t count,
            duckdb::DataChunk& output) const {
    if (_bound.empty() || count == 0) {
      return;
    }
    SDB_IF_FAILURE("SearchIncludeFetchFault") { SDB_THROW(sdb::ERROR_DEBUG); }
    for (const auto& b : _bound) {
      auto& out_vec = output.data[b.output_slot];
      const auto type_id = b.reader->Type().id();
      if (type_id == duckdb::LogicalTypeId::LIST ||
          type_id == duckdb::LogicalTypeId::MAP) {
        duckdb::ListVector::SetListSize(out_vec, 0);
      }
      irs::columnstore::MaterializeNode(
        *b.reader, *b.state, irs::columnstore::IotaRange{start_doc, count},
        out_vec, 0, /*may_use_entire=*/true);
    }
  }

 private:
  struct Binding {
    const irs::columnstore::ColumnReader* reader;
    duckdb::idx_t output_slot;
    std::unique_ptr<irs::columnstore::MaterializeState> state;
  };

  irs::columnstore::ReadContext _ctx;
  std::vector<Binding> _bound;
};

}  // namespace sdb::connector
