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

#include "iresearch/index/hit_batcher.hpp"

#include <bit>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <limits>

#include "basics/assert.h"
#include "iresearch/formats/column/col_reader.hpp"

namespace sdb::connector {
namespace {

constexpr duckdb::idx_t kDenseDensity = 32;
constexpr duckdb::idx_t kMinDenseBatch = 64;

}  // namespace

HitBatcher::HitBatcher(std::span<const ColumnstoreProjection> projections,
                       irs::field_id pk_field_id, bool track_scores)
  : _projections{projections},
    _pk_field_id{pk_field_id},
    _track_scores{track_scores} {
  _sel.Initialize(STANDARD_VECTOR_SIZE);
}

HitBatcher::~HitBatcher() = default;

void HitBatcher::BeginSegment(uint32_t seg_idx,
                              const irs::ColReader* col_reader,
                              duckdb::ClientContext* context) {
  SDB_ASSERT(Empty(), "drain the batcher before switching segments");
  _seg_idx = seg_idx;
  _len = 0;
  _group = 0;
  _batch = 0;
  _compact = false;
  _compact_dense = false;
  _columns.clear();
  _rg_col = nullptr;
  if (col_reader == nullptr) {
    _ctx.reset();
    return;
  }
  if (_ctx) {
    _ctx->Reset(*col_reader);
  } else {
    _ctx = std::make_unique<irs::ReadContext>(*col_reader);
  }

  const auto bind = [&](Column& c, const irs::ColumnReader& r) {
    c.reader = &r;
    c.out_type = r.Type();
    c.list_like = r.Type().id() == duckdb::LogicalTypeId::LIST ||
                  r.Type().id() == duckdb::LogicalTypeId::MAP;
    c.state = std::make_unique<irs::ColumnReader::ScanState>(r.InitScan(*_ctx));
    if (_rg_col == nullptr) {
      _rg_col = &r;
    }
  };

  if (_pk_field_id != irs::field_limits::invalid()) {
    const auto* pk = col_reader->Column(_pk_field_id);
    SDB_ASSERT(pk != nullptr);
    auto& c = _columns.emplace_back();
    c.is_pk = true;
    bind(c, *pk);
    if (!_pk_out) {
      _pk_out = std::make_unique<duckdb::Vector>(
        pk->Type(), static_cast<duckdb::idx_t>(STANDARD_VECTOR_SIZE));
    }
  }
  for (const auto& p : _projections) {
    const auto* r = col_reader->Column(static_cast<irs::field_id>(p.column_id));
    if (r == nullptr) {
      continue;
    }
    auto& c = _columns.emplace_back();
    c.slot = p.output_slot;
    c.extract = p.IsExtract();
    if (c.extract) {
      c.reader = r;
      c.out_type = p.extract_scan_type;
      c.list_like = c.out_type.id() == duckdb::LogicalTypeId::LIST ||
                    c.out_type.id() == duckdb::LogicalTypeId::MAP;
      c.extract_binding = std::make_unique<ExtractBinding>();
      c.extract_binding->Bind(*r, *_ctx, p.extract_path, p.extract_scan_type,
                              context);
      if (_rg_col == nullptr) {
        _rg_col = r;
      }
    } else {
      bind(c, *r);
    }
  }
}

uint64_t HitBatcher::RgEndFor(uint64_t row) const noexcept {
  return std::min(_rg_col != nullptr ? _rg_col->RowGroupEnd(row)
                                     : std::numeric_limits<uint64_t>::max(),
                  row + STANDARD_VECTOR_SIZE);
}

bool HitBatcher::Push(irs::doc_id_t doc) {
  SDB_ASSERT(!Ready(), "emit the pending batch before pushing more");
  if (_compact) {
    Compact();
    if (Ready()) {
      SDB_ASSERT(_len <= STANDARD_VECTOR_SIZE);
      _docs[_len++] = doc;
      return true;
    }
  }
  const uint64_t row = doc - irs::doc_limits::min();
  if (_len != _group &&
      (row >= _group_rg_end || _len == STANDARD_VECTOR_SIZE)) {
    CloseGroup();
  }
  if (_len == _group) {
    _group_rg_end = RgEndFor(row);
  }
  _docs[_len++] = doc;
  return Ready();
}

duckdb::idx_t HitBatcher::OpenWindow(uint64_t row) {
  SDB_ASSERT(!Ready(), "emit the pending batch before pushing more");
  if (_compact) {
    Compact();
    if (Ready()) {
      return 0;
    }
  }
  if (_len != _group &&
      (row >= _group_rg_end || _len == STANDARD_VECTOR_SIZE)) {
    CloseGroup();
    if (Ready()) {
      return 0;
    }
  }
  if (_len == _group) {
    _group_rg_end = RgEndFor(row);
  }
  return std::min<duckdb::idx_t>(
    _group_rg_end - row,
    static_cast<duckdb::idx_t>(STANDARD_VECTOR_SIZE) - _len);
}

void HitBatcher::Finalize() {
  SDB_ASSERT(!Ready(), "emit the pending batch before Finalize");
  if (_compact) {
    Compact();
    if (Ready()) {
      return;
    }
  }
  CloseGroup();
  if (_ready == Pending::None && _len != 0) {
    _ready = Pending::Scratch;
    _group = _len;
    _batch = _len;
    _compact = true;
  }
}

void HitBatcher::Compact() {
  SDB_ASSERT(_compact && !Ready());
  const auto tail = _len - _batch;
  if (tail != 0 && _batch != 0) {
    std::copy_n(_docs.begin() + _batch, tail, _docs.begin());
    if (_track_scores) {
      std::copy_n(_scores.begin() + _batch, tail, _scores.begin());
    }
  }
  _len = tail;
  _group = 0;
  _batch = 0;
  _compact = false;
  if (_compact_dense) {
    _compact_dense = false;
    _ready = Pending::Dense;
    _batch = _len;
  } else if (_len != 0) {
    _group_rg_end = RgEndFor(Row(0));
  }
}

void HitBatcher::CloseGroup() {
  const auto hits = _len - _group;
  if (hits == 0) {
    return;
  }
  const uint64_t anchor = Row(_group);
  const auto span = static_cast<duckdb::idx_t>(Row(_len - 1) - anchor + 1);
  if (hits * kDenseDensity >= span && hits >= kMinDenseBatch) {
    if (_group == 0) {
      _ready = Pending::Dense;
      _batch = _len;
    } else {
      _ready = Pending::Scratch;
      _batch = _group;
      _compact = true;
      _compact_dense = true;
    }
    return;
  }
  ScatterGroup();
  _group = _len;
  if (_len == STANDARD_VECTOR_SIZE) {
    _ready = Pending::Scratch;
    _batch = _len;
    _compact = true;
  }
}

void HitBatcher::ScatterGroup() {
  const auto hits = _len - _group;
  const uint64_t anchor = Row(_group);
  const auto span = static_cast<duckdb::idx_t>(Row(_len - 1) - anchor + 1);
  for (duckdb::idx_t i = 0; i < hits; ++i) {
    _sel.set_index(i, Row(_group + i) - anchor);
  }
  for (auto& c : _columns) {
    auto& out = c.is_pk ? *_pk_out : Scratch(c);
    MaterializeColumn(c, anchor, span, hits, _group, out, _group,
                      /*dense=*/false);
  }
}

duckdb::Vector& HitBatcher::Scratch(Column& c) {
  if (!c.scratch || _group == 0) {
    c.scratch = std::make_unique<duckdb::Vector>(
      c.out_type, static_cast<duckdb::idx_t>(STANDARD_VECTOR_SIZE));
  }
  return *c.scratch;
}

void HitBatcher::MaterializeColumn(Column& c, uint64_t anchor,
                                   duckdb::idx_t span, duckdb::idx_t hits,
                                   duckdb::idx_t first, duckdb::Vector& out,
                                   duckdb::idx_t at, bool dense) {
  if (at == 0 && c.list_like) {
    duckdb::ListVector::SetListSize(out, 0);
  }
  if (c.extract) {
    const DocRows rows{std::span<const irs::doc_id_t>{&_docs[first], hits}};
    c.extract_binding->MaterializeRows(rows, out, at, dense);
    return;
  }
  // A "dense" group only bounds hit *density*, not the doc-id span: sparse
  // hits across a wide row group can give span > STANDARD_VECTOR_SIZE while
  // hits <= STANDARD_VECTOR_SIZE. GatherDense scans the whole span into one
  // vector and requires span <= STANDARD_VECTOR_SIZE; fall back to the
  // run-based GatherScatter otherwise (same guard as GatherRows).
  if (dense && span <= STANDARD_VECTOR_SIZE) {
    c.reader->GatherDense(*c.state, anchor, _sel, hits, span, out);
  } else {
    c.reader->GatherScatter(*c.state, anchor, _sel, hits, out, at);
  }
}

HitBatcher::Batch HitBatcher::Emit(duckdb::DataChunk& output) {
  SDB_ASSERT(Ready());
  Batch batch;
  batch.seg = _seg_idx;
  if (_ready == Pending::Dense) {
    const uint64_t anchor = Row(0);
    const auto span = static_cast<duckdb::idx_t>(Row(_batch - 1) - anchor + 1);
    for (duckdb::idx_t i = 0; i < _batch; ++i) {
      _sel.set_index(i, Row(i) - anchor);
    }
    for (auto& c : _columns) {
      auto& out = c.is_pk ? *_pk_out : output.data[c.slot];
      MaterializeColumn(c, anchor, span, _batch, 0, out, 0, /*dense=*/true);
      if (c.is_pk) {
        batch.pk = _pk_out.get();
      }
    }
  } else {
    for (auto& c : _columns) {
      if (c.is_pk) {
        batch.pk = _pk_out.get();
      } else if (c.scratch) {
        output.data[c.slot].Reference(*c.scratch);
      }
    }
  }
  batch.count = _batch;
  batch.docs = {_docs.data(), _batch};
  if (_track_scores) {
    batch.scores = {_scores.data(), _batch};
  }
  _ready = Pending::None;
  _compact = true;
  return batch;
}

}  // namespace sdb::connector
