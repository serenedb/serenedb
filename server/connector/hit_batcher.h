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

#include <array>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <memory>
#include <span>
#include <vector>

#include "connector/column_extract.h"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"

namespace sdb::connector {

class HitBatcher {
 public:
  HitBatcher(std::span<const ColumnstoreProjection> projections, bool fetch_pk,
             bool track_scores);

  HitBatcher(const HitBatcher&) = delete;
  HitBatcher& operator=(const HitBatcher&) = delete;
  ~HitBatcher();

  void BeginSegment(uint32_t seg_idx, const irs::ColReader* col_reader,
                    duckdb::ClientContext* context);

  bool Push(irs::doc_id_t doc);

  duckdb::idx_t OpenWindow(uint64_t row);
  // Batched fill: the current window's ids/scores go to [WindowHead(), ...) /
  // [ScoreHead(), ...), then CommitWindow(n) records how many were emitted.
  irs::doc_id_t* WindowHead() noexcept { return &_docs[_len]; }
  irs::score_t* ScoreHead() noexcept { return &_scores[_len]; }
  void CommitWindow(duckdb::idx_t n) noexcept {
    _len += n;
    SDB_ASSERT(_len <= STANDARD_VECTOR_SIZE);
  }

  std::span<float> ScoreSlots(duckdb::idx_t n) noexcept {
    SDB_ASSERT(n <= _len);
    return {&_scores[_len - n], n};
  }

  void Finalize();

  uint32_t Segment() const noexcept { return _seg_idx; }

  bool Ready() const noexcept { return _ready != Pending::None; }
  bool Empty() const noexcept {
    return _ready == Pending::None && _len == 0 && !_compact;
  }

  struct Batch {
    std::span<const irs::doc_id_t> docs;
    std::span<const float> scores;
    uint32_t seg = 0;
    duckdb::Vector* pk = nullptr;
    duckdb::idx_t count = 0;
  };

  Batch Emit(duckdb::DataChunk& output);

 private:
  struct Column {
    const irs::ColumnReader* reader = nullptr;
    duckdb::idx_t slot = 0;
    bool is_pk = false;
    bool extract = false;
    bool list_like = false;
    duckdb::LogicalType out_type;
    std::unique_ptr<irs::ColumnReader::ScanState> state;
    std::unique_ptr<ExtractBinding> extract_binding;
    std::unique_ptr<duckdb::Vector> scratch;
  };

  enum class Pending : uint8_t { None, Dense, Scratch };

  void CloseGroup();
  void ScatterGroup();
  void Compact();
  void MaterializeColumn(Column& c, uint64_t anchor, duckdb::idx_t span,
                         duckdb::idx_t hits, duckdb::idx_t first,
                         duckdb::Vector& out, duckdb::idx_t at, bool dense);
  duckdb::Vector& Scratch(Column& c);
  uint64_t Row(duckdb::idx_t i) const noexcept {
    return _docs[i] - irs::doc_limits::min();
  }
  // Row-group window end for a group starting at `row`: the segment's row-group
  // boundary (if any) capped to a single output vector.
  uint64_t RgEndFor(uint64_t row) const noexcept;

  std::span<const ColumnstoreProjection> _projections;
  const bool _fetch_pk;
  const bool _track_scores;

  std::unique_ptr<irs::ReadContext> _ctx;
  std::vector<Column> _columns;
  uint32_t _seg_idx = 0;
  const irs::ColumnReader* _rg_col = nullptr;

  std::array<irs::doc_id_t, STANDARD_VECTOR_SIZE + 1> _docs;
  std::array<irs::score_t, STANDARD_VECTOR_SIZE + 1> _scores;
  duckdb::idx_t _len = 0;
  duckdb::idx_t _group = 0;
  duckdb::idx_t _batch = 0;
  uint64_t _group_rg_end = 0;

  std::unique_ptr<duckdb::Vector> _pk_out;

  duckdb::SelectionVector _sel;

  Pending _ready = Pending::None;
  bool _compact = false;
  bool _compact_dense = false;
};

}  // namespace sdb::connector
