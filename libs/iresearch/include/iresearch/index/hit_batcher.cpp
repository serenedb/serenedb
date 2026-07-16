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
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <limits>

#include "basics/assert.h"
#include "iresearch/formats/column/col_reader.hpp"

namespace sdb::connector {
namespace {

constexpr duckdb::idx_t kDenseDensity = 32;
constexpr duckdb::idx_t kMinDenseBatch = 64;

}  // namespace

HitBatcher::HitBatcher(
  std::span<const ColumnstoreProjection> projections, irs::field_id pk_field_id,
  bool track_scores,
  std::span<const TableFilterDocIterator::FilterSpec> filters)
  : _projections{projections},
    _pk_field_id{pk_field_id},
    _track_scores{track_scores},
    _filter_specs{filters.begin(), filters.end()} {
  _sel_data = duckdb::make_buffer<duckdb::SelectionData>(STANDARD_VECTOR_SIZE);
  _sel.Initialize(_sel_data);
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
  _filters.clear();
  _score_filter = nullptr;
  _score_state.reset();
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

  // Pushed table filters for this segment (applied in EmitFiltered). The score
  // filter needs no columnstore reader; each `.col` filter binds its column.
  for (const auto& spec : _filter_specs) {
    if (spec.is_score) {
      if (_track_scores) {
        _score_filter = spec.filter;
        _score_state =
          duckdb::TableFilterState::Initialize(*context, *spec.filter);
      }
      continue;
    }
    const auto* r = col_reader->Column(spec.field);
    if (r == nullptr) {
      continue;
    }
    auto& f = _filters.emplace_back();
    f.reader = r;
    f.field = spec.field;
    f.filter = spec.filter;
    f.state = duckdb::TableFilterState::Initialize(*context, *spec.filter);
    f.scan = std::make_unique<irs::ColumnReader::ScanState>(r->InitScan(*_ctx));
    if (_rg_col == nullptr) {
      _rg_col = r;
    }
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
    // A projected column that is also a `.col` filter column materializes as
    // part of its filter step (decode once into this slot, then Slice) --
    // record the slot on the filter and don't scan it again below.
    if (!p.IsExtract()) {
      FilterCol* fc = nullptr;
      for (auto& f : _filters) {
        if (f.field == p.column_id) {
          fc = &f;
          break;
        }
      }
      if (fc != nullptr) {
        fc->output_slots.push_back(p.output_slot);
        continue;
      }
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

  // Filter-only columns (not projected) decode into a private scratch just to
  // evaluate the predicate.
  for (auto& f : _filters) {
    if (f.output_slots.empty()) {
      f.scratch =
        std::make_unique<irs::ColumnReader::VectorScratch>(f.reader->Type());
    }
  }
}

uint64_t HitBatcher::RgEndFor(uint64_t row) const noexcept {
  return std::min(_rg_col != nullptr ? _rg_col->RowGroupEnd(row)
                                     : std::numeric_limits<uint64_t>::max(),
                  row + STANDARD_VECTOR_SIZE);
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
  if (HasFilters()) {
    // Filtered scans fuse filter+materialize per window in EmitFiltered
    // (RowGroup::Scan-style), so a window is never accumulated into the scratch
    // path -- it closes as its own dense batch straight from offset 0.
    SDB_ASSERT(_group == 0);
    _ready = Pending::Dense;
    _batch = _len;
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

HitBatcher::Batch HitBatcher::EmitFiltered(duckdb::DataChunk& output) {
  SDB_ASSERT(_ready == Pending::Dense,
             "filtered windows close as dense batches");
  Batch batch;
  batch.seg = _seg_idx;
  duckdb::idx_t count = _batch;

  // Phase 1: the score filter is the cheap one -- a comparison on the already
  // computed, in-memory scores, no columnstore read. Run it first so the `.col`
  // pass reads only survivors. Compacts _docs/_scores in place (survivor
  // indices ascend, so writes never clobber unread input).
  if (_score_filter != nullptr && _track_scores && count != 0) {
    duckdb::Vector svec{duckdb::LogicalType::FLOAT,
                        reinterpret_cast<duckdb::data_ptr_t>(_scores.data()),
                        count};
    duckdb::SelectionVector score_sel;
    duckdb::idx_t kept = count;
    duckdb::ColumnSegment::FilterSelection(score_sel, svec, *_score_state,
                                           count, kept);
    for (duckdb::idx_t s = 0; s < kept; ++s) {
      const auto idx = score_sel.get_index(s);
      _docs[s] = _docs[idx];
      _scores[s] = _scores[idx];
    }
    count = kept;
  }

  // The `.col` filters and every column materialization key their span offsets
  // off this anchor/span, so both are fixed before the doc array is compacted.
  duckdb::idx_t survivors = count;
  uint64_t anchor = 0;
  duckdb::idx_t span = 0;
  if (count != 0) {
    anchor = Row(0);
    span = static_cast<duckdb::idx_t>(Row(count - 1) - anchor + 1);
    _sel.Initialize(_sel_data);
    for (duckdb::idx_t i = 0; i < count; ++i) {
      _sel.set_index(i, Row(i) - anchor);
    }
    // Phase 2: `.col` codec filters -- each narrows `_sel`; a projected filter
    // column decodes straight into its output slot (Sliced below), a
    // filter-only column into private scratch. Whole-window zonemap skips a
    // dead group.
    for (auto& f : _filters) {
      if (survivors == 0) {
        break;
      }
      const auto block = f.reader->Locate(anchor).block;
      const auto& expr = f.filter->Cast<duckdb::ExpressionFilter>();
      const auto z = expr.CheckStatistics(f.reader->RowGroupStatistics(block));
      if (z == duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE) {
        survivors = 0;
        break;
      }
      if (z == duckdb::FilterPropagateResult::FILTER_ALWAYS_TRUE &&
          f.output_slots.empty()) {
        continue;
      }
      auto& target = f.output_slots.empty()
                       ? f.scratch->Reset()
                       : output.data[f.output_slots.front()];
      survivors = f.reader->GatherFilter(*f.scan, anchor, span, _sel, survivors,
                                         *f.filter, *f.state, target);
    }
    // Map `_sel[0..survivors)` (surviving span offsets, ascending) back to the
    // doc/score arrays, compacting in place (s <= k, so no clobber).
    if (!_filters.empty()) {
      duckdb::idx_t k = 0;
      for (duckdb::idx_t s = 0; s < survivors; ++s) {
        const uint64_t want = _sel.get_index(s);
        while ((Row(k) - anchor) != want) {
          ++k;
        }
        _docs[s] = _docs[k];
        if (_track_scores) {
          _scores[s] = _scores[k];
        }
      }
    }
    // Slice each projected filter column (decoded over the full span) down to
    // the survivors -- zero-copy dictionary view -- and Reference it into any
    // further slots the same column is projected to.
    for (auto& f : _filters) {
      if (f.output_slots.empty()) {
        continue;
      }
      auto& first = output.data[f.output_slots.front()];
      first.Slice(_sel, survivors);
      for (std::size_t j = 1; j < f.output_slots.size(); ++j) {
        output.data[f.output_slots[j]].Reference(first);
      }
    }
  }

  // Materialize the survivors of the non-filter projected columns (and PK),
  // keyed off the same anchor/span the `_sel` offsets were built against.
  for (auto& c : _columns) {
    auto& out = c.is_pk ? *_pk_out : output.data[c.slot];
    if (survivors != 0) {
      MaterializeColumn(c, anchor, span, survivors, 0, out, 0, /*dense=*/true);
    }
    if (c.is_pk) {
      batch.pk = _pk_out.get();
    }
  }

  batch.count = survivors;
  batch.docs = {_docs.data(), survivors};
  if (_track_scores) {
    batch.scores = {_scores.data(), survivors};
  }
  _ready = Pending::None;
  _compact = true;
  return batch;
}

HitBatcher::Batch HitBatcher::Emit(duckdb::DataChunk& output) {
  SDB_ASSERT(Ready());
  if (HasFilters()) {
    return EmitFiltered(output);
  }
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
