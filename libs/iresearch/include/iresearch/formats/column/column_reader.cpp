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

#include "iresearch/formats/column/column_reader.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/serializer/deserializer.hpp>
#include <duckdb/common/serializer/serializer.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/dictionary_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/function/scalar/variant_utils.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/buffer/buffer_handle.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/checkpoint/string_checkpoint_state.hpp>
#include <duckdb/storage/segment/uncompressed.hpp>
#include <duckdb/storage/statistics/array_stats.hpp>
#include <duckdb/storage/statistics/list_stats.hpp>
#include <duckdb/storage/statistics/struct_stats.hpp>
#include <duckdb/storage/statistics/variant_stats.hpp>
#include <duckdb/storage/table/variant_column_data.hpp>
#include <memory>
#include <optional>
#include <utility>

#include "basics/assert.h"
#include "iresearch/formats/column/array_column_reader.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/internal/gather_arms.hpp"
#include "iresearch/formats/column/internal/overflow_string_io.hpp"
#include "iresearch/formats/column/list_column_reader.hpp"
#include "iresearch/formats/column/struct_column_reader.hpp"
#include "iresearch/formats/column/variant_column_reader.hpp"
#include "iresearch/store/data_input.hpp"
#include "pg/sql_exception_macro.h"

namespace irs {
namespace {

void SerializeColumnBlockMeta(duckdb::Serializer& s, const ColumnBlockMeta& m) {
  s.WriteProperty<uint8_t>(0, "compression_type",
                           static_cast<uint8_t>(m.codec->type));
  s.WriteProperty<uint64_t>(1, "tuple_count", m.tuple_count);
  s.WriteProperty<uint64_t>(2, "file_offset", m.file_offset);
  s.WriteProperty<uint64_t>(3, "byte_size", m.byte_size);
  s.WriteProperty<duckdb::BaseStatistics>(4, "statistics", m.statistics);
}

ColumnBlockMeta DeserializeColumnBlockMeta(duckdb::Deserializer& d,
                                           duckdb::PhysicalType physical) {
  const auto compression_type = static_cast<duckdb::CompressionType>(
    d.ReadProperty<uint8_t>(0, "compression_type"));
  const auto tuple_count = d.ReadProperty<uint64_t>(1, "tuple_count");
  const auto file_offset = d.ReadProperty<uint64_t>(2, "file_offset");
  const auto byte_size = d.ReadProperty<uint64_t>(3, "byte_size");
  auto stats = d.ReadProperty<duckdb::BaseStatistics>(4, "statistics");
  auto& cfg = duckdb::DBConfig::GetConfig(d.Get<duckdb::DatabaseInstance&>());
  auto codec = cfg.TryGetCompressionFunction(compression_type, physical);
  SDB_ENSURE(codec, "ColumnReader: missing compression function for codec ",
             static_cast<uint8_t>(compression_type));
  return ColumnBlockMeta{std::move(stats), tuple_count, file_offset, byte_size,
                         codec.get()};
}

}  // namespace

void SerializeColumnMeta(duckdb::Serializer& s, const ColumnMeta& meta) {
  s.WriteProperty<uint64_t>(0, "id", static_cast<uint64_t>(meta.id));
  s.WriteProperty(1, "type", meta.type);
  s.WriteList(2, "data", meta.data.size(),
              [&](duckdb::Serializer::List& list, duckdb::idx_t j) {
                list.WriteObject([&](duckdb::Serializer& so) {
                  SerializeColumnBlockMeta(so, meta.data[j]);
                });
              });
  s.WriteList(3, "validity", meta.validity.size(),
              [&](duckdb::Serializer::List& list, duckdb::idx_t j) {
                list.WriteObject([&](duckdb::Serializer& so) {
                  SerializeColumnBlockMeta(so, meta.validity[j]);
                });
              });
  s.WriteList(4, "children", meta.children.size(),
              [&](duckdb::Serializer::List& list, duckdb::idx_t j) {
                list.WriteObject([&](duckdb::Serializer& co) {
                  SerializeColumnMeta(co, meta.children[j]);
                });
              });
  s.WriteList(5, "variant_rgs", meta.variant_rgs.size(),
              [&](duckdb::Serializer::List& list, duckdb::idx_t j) {
                const auto& rg = meta.variant_rgs[j];
                list.WriteObject([&](duckdb::Serializer& ro) {
                  ro.WriteProperty<uint64_t>(0, "row_count", rg.row_count);
                  ro.WriteObject(1, "unshredded", [&](duckdb::Serializer& uo) {
                    SerializeColumnMeta(uo, *rg.unshredded);
                  });
                  const bool has_shredded = rg.shredded != nullptr;
                  ro.WriteProperty<bool>(2, "has_shredded", has_shredded);
                  if (has_shredded) {
                    ro.WriteObject(3, "shredded", [&](duckdb::Serializer& so) {
                      SerializeColumnMeta(so, *rg.shredded);
                    });
                  }
                });
              });
  s.WritePropertyWithDefault<duckdb::shared_ptr<duckdb::HyperLogLog>>(
    6, "hyperloglog", meta.hyperloglog);
}

ColumnMeta DeserializeColumnMeta(duckdb::Deserializer& d) {
  ColumnMeta meta;
  meta.id = static_cast<field_id>(d.ReadProperty<uint64_t>(0, "id"));
  meta.type = d.ReadProperty<duckdb::LogicalType>(1, "type");
  const bool is_list_like = meta.type.id() == duckdb::LogicalTypeId::LIST ||
                            meta.type.id() == duckdb::LogicalTypeId::MAP;
  const duckdb::LogicalType data_stats_type =
    is_list_like ? duckdb::LogicalType::UBIGINT : meta.type;
  const auto data_physical = meta.type.InternalType();
  d.Set<const duckdb::LogicalType&>(data_stats_type);
  d.ReadList(
    2, "data", [&](duckdb::Deserializer::List& list, duckdb::idx_t /*j*/) {
      list.ReadObject([&](duckdb::Deserializer& so) {
        meta.data.push_back(DeserializeColumnBlockMeta(so, data_physical));
      });
    });
  d.Unset<const duckdb::LogicalType>();
  const duckdb::LogicalType validity_type{duckdb::LogicalTypeId::VALIDITY};
  const auto validity_physical = validity_type.InternalType();
  d.Set<const duckdb::LogicalType&>(validity_type);
  d.ReadList(3, "validity",
             [&](duckdb::Deserializer::List& list, duckdb::idx_t /*j*/) {
               list.ReadObject([&](duckdb::Deserializer& so) {
                 meta.validity.push_back(
                   DeserializeColumnBlockMeta(so, validity_physical));
               });
             });
  d.Unset<const duckdb::LogicalType>();
  d.ReadList(4, "children",
             [&](duckdb::Deserializer::List& list, duckdb::idx_t /*j*/) {
               list.ReadObject([&](duckdb::Deserializer& co) {
                 meta.children.push_back(DeserializeColumnMeta(co));
               });
             });
  d.ReadList(5, "variant_rgs",
             [&](duckdb::Deserializer::List& list, duckdb::idx_t /*j*/) {
               list.ReadObject([&](duckdb::Deserializer& ro) {
                 VariantRgMeta rg;
                 rg.row_count = ro.ReadProperty<uint64_t>(0, "row_count");
                 ro.ReadObject(1, "unshredded", [&](duckdb::Deserializer& uo) {
                   rg.unshredded =
                     std::make_unique<ColumnMeta>(DeserializeColumnMeta(uo));
                 });
                 const bool has_shredded =
                   ro.ReadProperty<bool>(2, "has_shredded");
                 if (has_shredded) {
                   ro.ReadObject(3, "shredded", [&](duckdb::Deserializer& so) {
                     rg.shredded =
                       std::make_unique<ColumnMeta>(DeserializeColumnMeta(so));
                   });
                 }
                 meta.variant_rgs.push_back(std::move(rg));
               });
             });
  meta.hyperloglog =
    d.ReadPropertyWithDefault<duckdb::shared_ptr<duckdb::HyperLogLog>>(
      6, "hyperloglog");
  return meta;
}

ColumnReader::ScanState::ScanState() = default;
ColumnReader::ScanState::ScanState(ScanState&&) = default;
ColumnReader::ScanState& ColumnReader::ScanState::operator=(ScanState&&) =
  default;
ColumnReader::ScanState::~ScanState() = default;

ColumnReader::ColumnReader(field_id id, duckdb::LogicalType type,
                           std::vector<ColumnBlockMeta> segments,
                           std::unique_ptr<ColumnReader> validity,
                           std::vector<std::unique_ptr<ColumnReader>> children)
  : _id{id},
    _type{std::move(type)},
    _segments{std::move(segments)},
    _validity{std::move(validity)},
    _children{std::move(children)},
    _array_size{_type.id() == duckdb::LogicalTypeId::ARRAY
                  ? duckdb::ArrayType::GetSize(_type)
                  : 0} {
  auto stats = duckdb::BaseStatistics::CreateEmpty(
    _segments.empty() ? _type : _segments.front().statistics.GetType());
  _offsets.reserve(_segments.size() + 1);
  _offsets.push_back(0);
  for (const auto& m : _segments) {
    _row_count += m.tuple_count;
    _offsets.push_back(_row_count);
    stats.Merge(m.statistics);
  }
  FinishStats(std::move(stats));
}

bool ColumnReader::NullsInData() const noexcept {
  return absl::c_any_of(_segments, [](const ColumnBlockMeta& m) {
    return m.codec->validity ==
           duckdb::CompressionValidity::NO_VALIDITY_REQUIRED;
  });
}

void ColumnReader::FinishStats(duckdb::BaseStatistics stats) {
  if (_validity) {
    stats.Merge(_validity->MergedStatistics());
  } else if (_row_count > 0 && _type.id() != duckdb::LogicalTypeId::VALIDITY &&
             (!_children.empty() || !NullsInData())) {
    // No validity payload means the writer counted every row valid
    // (ColumnWriter::SealValidity): the column has non-null values. A nested
    // parent's row validity never hides in its data codec; a scalar's can
    // (dict_fsst), and there the data block stats already carry the flags.
    SDB_ASSERT(!stats.CanHaveNull(),
               "column without a validity payload carries null-bearing stats");
    stats.SetHasNoNull();
  }
  _stats = stats.ToUnique();
}

uint64_t ColumnReader::RowGroupEnd(uint64_t row) const noexcept {
  SDB_ASSERT(row < _row_count);
  if (!_segments.empty()) {
    return Locate(row).end;
  }
  if (!_children.empty()) {
    return _children.front()->RowGroupEnd(row);
  }
  SDB_ASSERT(_validity);
  return _validity->RowGroupEnd(row);
}

BlockWindow ColumnReader::Locate(uint64_t row,
                                 BlockWindow hint) const noexcept {
  SDB_ASSERT(!_segments.empty(), "Locate on a column without own blocks");
  if (hint.end != 0) {
    if (row >= hint.begin && row < hint.end) {
      return hint;
    }
    const auto next = hint.block + 1;
    if (next < _segments.size() && row >= _offsets[next] &&
        row < _offsets[next + 1]) {
      return BlockWindow{next, _offsets[next], _offsets[next + 1]};
    }
  }
  SDB_ASSERT(row < _row_count);
  const auto it = std::upper_bound(_offsets.begin(), _offsets.end(), row);
  const auto rg = static_cast<size_t>(it - _offsets.begin()) - 1;
  return BlockWindow{rg, _offsets[rg], _offsets[rg + 1]};
}

bool ColumnReader::NextSegment(BlockWindow& w) const noexcept {
  const auto next = w.block + 1;
  if (next >= _segments.size()) {
    return false;
  }
  w.block = next;
  w.begin = _offsets[next];
  w.end = _offsets[next + 1];
  return true;
}

std::unique_ptr<duckdb::ColumnSegment> ColumnReader::Open(
  const BlockWindow& w, ReadContext& ctx) const {
  const auto& m = _segments[w.block];
  auto& db = ctx.Database();
  const auto& codec = *m.codec;
  auto stats = m.statistics.Copy();
  const auto byte_size = static_cast<duckdb::idx_t>(m.byte_size);

  if (byte_size == 0) {
    return std::make_unique<duckdb::ColumnSegment>(
      db, /*block=*/nullptr, duckdb::ColumnSegmentType::PERSISTENT,
      static_cast<duckdb::idx_t>(m.tuple_count), codec, std::move(stats),
      /*block_id=*/0, /*offset=*/0, byte_size, /*segment_state=*/nullptr);
  }

  auto handle = ctx.RegisterColBlock(m.file_offset, byte_size);
  auto segment = std::make_unique<duckdb::ColumnSegment>(
    db, std::move(handle), duckdb::ColumnSegmentType::PERSISTENT,
    static_cast<duckdb::idx_t>(m.tuple_count), codec, std::move(stats),
    /*block_id=*/0, /*offset=*/0, byte_size, /*segment_state=*/nullptr);
  if (_type.InternalType() == duckdb::PhysicalType::VARCHAR) {
    if (auto seg_state = segment->GetSegmentState()) {
      seg_state->Cast<duckdb::UncompressedStringSegmentState>()
        .overflow_reader =
        duckdb::make_uniq<IndexInputOverflowReader>(ctx.In());
    }
  }
  return segment;
}

ColumnReader::ScanState ColumnReader::InitScan(ReadContext& ctx) const {
  ScanState s;
  s.ctx = &ctx;
  if (!_segments.empty()) {
    s.window = BlockWindow{0, _offsets[0], _offsets[1]};
  }
  s.st.offset_in_column = 0;
  s.st.internal_index = 0;
  s.initialized = false;
  s.child_states.reserve(_children.size() + 1);
  s.child_states.push_back(_validity ? _validity->InitScan(ctx) : ScanState{});
  for (const auto& child : _children) {
    s.child_states.push_back(child->InitScan(ctx));
  }
  return s;
}

void ColumnReader::BeginScanVector(ScanState& s) const {
  if (s.st.offset_in_column == s.window.end - s.window.begin &&
      NextSegment(s.window)) {
    s.initialized = false;
    s.st.offset_in_column = 0;
    s.st.internal_index = 0;
  }
  if (!s.initialized) {
    if (s.st.scan_state) {
      s.st.previous_states.emplace_back(std::move(s.st.scan_state));
    }
    s.segments.emplace_back(Open(s.window, *s.ctx));
    s.segments.back()->InitializeScan(s.st);
    s.st.internal_index = 0;
    s.initialized = true;
  }
  SDB_ASSERT(s.st.internal_index <= s.st.offset_in_column);
  if (s.st.internal_index < s.st.offset_in_column) {
    s.segments.back()->Skip(s.st);
  }
}

void ColumnReader::NewOutputVector(ScanState& s) const {
  s.st.previous_states.clear();
  if (s.segments.size() > 1) {
    s.segments.erase(s.segments.begin(), s.segments.end() - 1);
  }
  if (_validity) {
    _validity->ColumnReader::NewOutputVector(s.child_states[0]);
  }
  for (size_t i = 0; i < _children.size(); ++i) {
    _children[i]->NewOutputVector(s.child_states[i + 1]);
  }
}

duckdb::ScanVectorType ColumnReader::GetVectorScanType(
  ScanState& s, duckdb::idx_t count, duckdb::Vector& result) const {
  if (result.GetVectorType() != duckdb::VectorType::FLAT_VECTOR) {
    return duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR;
  }
  if ((s.window.end - s.window.begin) - s.st.offset_in_column < count) {
    return duckdb::ScanVectorType::SCAN_FLAT_VECTOR;
  }
  auto scan_type = duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR;
  if (_validity &&
      _validity->GetVectorScanType(s.child_states[0], count, result) ==
        duckdb::ScanVectorType::SCAN_FLAT_VECTOR) {
    scan_type = duckdb::ScanVectorType::SCAN_FLAT_VECTOR;
  }
  return scan_type;
}

duckdb::idx_t ColumnReader::ScanVector(ScanState& s, duckdb::Vector& result,
                                       duckdb::idx_t remaining,
                                       duckdb::ScanVectorType scan_type,
                                       duckdb::idx_t base_result_offset) const {
  BeginScanVector(s);
  const auto initial = remaining;
  while (remaining > 0) {
    const auto scan_count = std::min<duckdb::idx_t>(
      remaining, (s.window.end - s.window.begin) - s.st.offset_in_column);
    const auto result_offset = base_result_offset + (initial - remaining);
    if (scan_count > 0) {
      s.segments.back()->Scan(s.st, scan_count, result, result_offset,
                              scan_type);
      s.st.offset_in_column += scan_count;
      remaining -= scan_count;
    }
    if (remaining > 0) {
      if (!NextSegment(s.window)) {
        break;
      }
      s.st.previous_states.emplace_back(std::move(s.st.scan_state));
      s.segments.emplace_back(Open(s.window, *s.ctx));
      s.segments.back()->InitializeScan(s.st);
      s.st.offset_in_column = 0;
      s.st.internal_index = 0;
    }
  }
  s.st.internal_index = s.st.offset_in_column;
  return initial - remaining;
}

duckdb::idx_t ColumnReader::Scan(ScanState& s, duckdb::Vector& result,
                                 duckdb::idx_t count) const {
  NewOutputVector(s);
  const auto scan_type = GetVectorScanType(s, count, result);
  const auto n = ScanVector(s, result, count, scan_type);
  if (_validity) {
    SDB_ASSERT(!s.child_states.empty());
    _validity->ScanVector(s.child_states[0], result, count, scan_type);
  }
  return n;
}

duckdb::idx_t ColumnReader::ScanCount(ScanState& s, duckdb::Vector& result,
                                      duckdb::idx_t count,
                                      duckdb::idx_t result_offset) const {
  const auto scan_type = duckdb::ScanVectorType::SCAN_FLAT_VECTOR;
  const auto n = ScanVector(s, result, count, scan_type, result_offset);
  if (_validity) {
    SDB_ASSERT(!s.child_states.empty());
    _validity->ScanVector(s.child_states[0], result, count, scan_type,
                          result_offset);
  }
  return n;
}

void ColumnReader::Skip(ScanState& s, duckdb::idx_t count) const {
  SkipRows(s, count);
  if (_validity) {
    _validity->SkipRows(s.child_states[0], count);
  }
}

void ColumnReader::GatherScatter(ScanState& s, uint64_t anchor,
                                 const duckdb::SelectionVector& sel,
                                 duckdb::idx_t hits, duckdb::Vector& out,
                                 duckdb::idx_t at) const {
  column_internal::ScatterRuns(*this, s, anchor, sel, hits, out, at);
}

void ColumnReader::GatherDense(ScanState& s, uint64_t anchor,
                               const duckdb::SelectionVector& sel,
                               duckdb::idx_t hits, duckdb::idx_t span,
                               duckdb::Vector& out) const {
  SDB_ASSERT(hits > 0 && hits <= span && span <= STANDARD_VECTOR_SIZE);
  NewOutputVector(s);
  const uint64_t cur = ColumnReader::GatherCursor(s);
  SDB_ASSERT(anchor >= cur, "GatherDense requires ascending rows");
  if (anchor > cur) {
    ColumnReader::Skip(s, anchor - cur);
  }
  if (hits == span) {
    ColumnReader::Scan(s, out, span);
    return;
  }
  BeginScanVector(s);
  if ((s.window.end - s.window.begin) - s.st.offset_in_column >= span) {
    const auto codec = _segments[s.window.block].codec->type;
    const auto bands = column_internal::BandsFor(codec, _type);
    const auto permille = hits * 1000;
    if (permille <= bands.flat * span) {
      column_internal::ScatterRuns(*this, s, anchor, sel, hits, out, 0);
      return;
    }
    if (permille <= bands.native * span &&
        _segments[s.window.block].codec->select != nullptr) {
      bool native = true;
      if (_validity) {
        auto& vs = s.child_states[0];
        _validity->BeginScanVector(vs);
        native =
          (vs.window.end - vs.window.begin) - vs.st.offset_in_column >= span &&
          _validity->_segments[vs.window.block].codec->select != nullptr;
      }
      if (native) {
        s.segments.back()->Select(s.st, span, out, sel, hits);
        s.st.offset_in_column += span;
        s.st.internal_index = s.st.offset_in_column;
        if (_validity) {
          auto& vs = s.child_states[0];
          vs.segments.back()->Select(vs.st, span, out, sel, hits);
          vs.st.offset_in_column += span;
          vs.st.internal_index = vs.st.offset_in_column;
        }
        return;
      }
    }
  }
  ColumnReader::ScanCount(s, out, span, 0);
  out.Slice(sel, hits);
}

duckdb::idx_t ColumnReader::GatherFilter(
  ScanState& s, uint64_t anchor, duckdb::idx_t span,
  duckdb::SelectionVector& sel, duckdb::idx_t sel_count,
  const duckdb::TableFilter& filter, duckdb::TableFilterState& filter_state,
  NullCheckKind null_check, duckdb::Vector& result) const {
  const uint64_t cur = ColumnReader::GatherCursor(s);
  SDB_ASSERT(anchor >= cur, "GatherFilter requires ascending rows");
  if (anchor > cur) {
    ColumnReader::Skip(s, anchor - cur);
  }
  BeginScanVector(s);
  auto& seg = *s.segments.back();
  const auto& codec = seg.GetCompressionFunction();
  duckdb::idx_t approved = sel_count;
  // The codec filter operates on one segment; a span that crosses the block
  // boundary (blocks are not vector-aligned, e.g. dictionary-full splits on
  // string columns) takes the decode path below -- ScanVector walks segments.
  const bool within_segment =
    (s.window.end - s.window.begin) - s.st.offset_in_column >= span;
  const bool self_valid =
    codec.validity == duckdb::CompressionValidity::NO_VALIDITY_REQUIRED;
  // The codec filter needs the data segment alone to be validity-complete for
  // the span: either the codec self-describes nulls (dict_fsst evaluates its
  // dictionary once and filters rows by code), or the span's validity block is
  // the all-valid EMPTY codec (rle evaluates its run values once and filters
  // rows by run flag). Bare null checks keep the validity-only arm below, and
  // null-bearing spans keep the decode arm.
  const bool codec_filter =
    within_segment && codec.filter &&
    (self_valid || (null_check == NullCheckKind::None &&
                    ValiditySpanAllValid(s, anchor, span)));
  if (codec_filter) {
    if (!self_valid) {
      result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
      // The codec filter's evaluation reads the mask: reset it if an earlier
      // null-bearing window dirtied it. A buffer-less mask is already
      // all-valid.
      auto& mask = duckdb::FlatVector::ValidityMutable(result);
      if (!mask.AllValid()) {
        mask.SetAllValid(span);
      }
    }
    seg.Filter(s.st, span, result, sel, approved, filter, filter_state);
    s.st.offset_in_column += span;
    s.st.internal_index = s.st.offset_in_column;
    // Codecs are picked per block: a later block of the same column may need
    // the validity child (decode path below), whose cursor only moves
    // relatively -- keep it in step with the data cursor.
    if (_validity) {
      SDB_ASSERT(!s.child_states.empty());
      _validity->SkipRows(s.child_states[0], span);
    }
  } else if (within_segment && null_check != NullCheckKind::None &&
             !self_valid) {
    // Bare IS [NOT] NULL: the validity child alone answers it -- skip the
    // data decode entirely (the data cursor still advances to stay in step).
    SkipRows(s, span);
    result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    auto& mask = duckdb::FlatVector::ValidityMutable(result);
    mask.SetAllValid(span);
    if (_validity) {
      SDB_ASSERT(!s.child_states.empty());
      _validity->ScanVector(s.child_states[0], result, span,
                            duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
    }
    const bool want_valid = null_check == NullCheckKind::IsNotNull;
    duckdb::idx_t kept = 0;
    for (duckdb::idx_t k = 0; k < sel_count; ++k) {
      const auto idx = sel.get_index(k);
      if (mask.RowIsValid(idx) == want_valid) {
        sel.set_index(kept++, idx);
      }
    }
    approved = kept;
  } else {
    // Decode the span into `scratch`, then narrow the selection natively.
    // When the span's validity block is the all-valid EMPTY codec, decoding
    // the validity child would AND-combine nothing: scan the data alone and
    // keep the validity cursor in step.
    const bool all_valid = ValiditySpanAllValid(s, anchor, span);
    result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    duckdb::FlatVector::ValidityMutable(result).SetAllValid(span);
    if (all_valid) {
      ScanVector(s, result, span, duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
      if (_validity) {
        SDB_ASSERT(!s.child_states.empty());
        _validity->SkipRows(s.child_states[0], span);
      }
    } else {
      ColumnReader::ScanCount(s, result, span, 0);
    }
    duckdb::ColumnSegment::FilterSelection(sel, result, filter_state, span,
                                           approved);
  }
  return approved;
}

bool ColumnReader::ValiditySpanAllValid(ScanState& s, uint64_t anchor,
                                        duckdb::idx_t span) const {
  if (!_validity) {
    return true;
  }
  SDB_ASSERT(!s.child_states.empty());
  const auto w = _validity->Locate(anchor, s.child_states[0].window);
  return anchor + span <= w.end && _validity->_segments[w.block].codec->type ==
                                     duckdb::CompressionType::COMPRESSION_EMPTY;
}

void ColumnReader::SkipRows(ScanState& s, duckdb::idx_t count) const {
  duckdb::idx_t remaining = count;
  while (remaining > 0) {
    const auto avail = (s.window.end - s.window.begin) - s.st.offset_in_column;
    if (remaining < avail) {
      s.st.offset_in_column += remaining;
      break;
    }
    remaining -= avail;
    if (!NextSegment(s.window)) {
      s.st.offset_in_column += avail;
      break;
    }
    s.initialized = false;
    s.st.offset_in_column = 0;
    s.st.internal_index = 0;
  }
}

std::unique_ptr<ColumnReader> ColumnReader::Make(ColumnMeta&& meta) {
  std::unique_ptr<ColumnReader> validity;
  if (absl::c_any_of(meta.validity, [](const ColumnBlockMeta& m) {
        return m.codec->type != duckdb::CompressionType::COMPRESSION_EMPTY;
      })) {
    validity.reset(new ColumnReader{
      meta.id,
      duckdb::LogicalTypeId::VALIDITY,
      std::move(meta.validity),
      nullptr,
      {},
    });
  }

  std::vector<std::unique_ptr<ColumnReader>> children;
  children.reserve(meta.children.size());
  for (auto& c : meta.children) {
    children.push_back(Make(std::move(c)));
  }

  std::unique_ptr<ColumnReader> col;
  switch (meta.type.id()) {
    case duckdb::LogicalTypeId::VARIANT:
      SDB_ASSERT(children.empty());
      col = std::make_unique<VariantColumnReader>(meta.id, std::move(meta.type),
                                                  std::move(validity),
                                                  std::move(meta.variant_rgs));
      break;
    case duckdb::LogicalTypeId::STRUCT:
      col = std::make_unique<StructColumnReader>(meta.id, std::move(meta.type),
                                                 std::move(validity),
                                                 std::move(children));
      break;
    case duckdb::LogicalTypeId::ARRAY:
      col = std::make_unique<ArrayColumnReader>(meta.id, std::move(meta.type),
                                                std::move(validity),
                                                std::move(children));
      break;
    case duckdb::LogicalTypeId::LIST:
    case duckdb::LogicalTypeId::MAP:
      col = std::make_unique<ListColumnReader>(
        meta.id, std::move(meta.type), std::move(meta.data),
        std::move(validity), std::move(children));
      break;
    default:
      col.reset(new ColumnReader{
        meta.id,
        std::move(meta.type),
        std::move(meta.data),
        std::move(validity),
        std::move(children),
      });
      break;
  }
  col->_hyperloglog = std::move(meta.hyperloglog);
  return col;
}

ColumnReader::PointReader::PointReader(const ColReader& col_reader,
                                       const ColumnReader& col)
  : _ctx{col_reader}, _reader{&col} {}

bool ColumnReader::PointReader::FetchRow(uint64_t row, duckdb::Vector& out,
                                         duckdb::idx_t out_offset) {
  if (row >= _reader->RowCount()) {
    duckdb::FlatVector::ValidityMutable(out).SetInvalid(out_offset);
    return false;
  }
  duckdb::FlatVector::ValidityMutable(out).SetValid(out_offset);
  const auto* validity = _reader->_validity.get();
  if (validity) {
    _validity_window = validity->Locate(row, _validity_window);
    if (_validity_window.block != _cached_validity_block) {
      _validity_block = validity->Open(_validity_window, _ctx);
      _validity_fetch_state = duckdb::ColumnFetchState{};
      _cached_validity_block = _validity_window.block;
    }
    _validity_block->FetchRow(
      _validity_fetch_state,
      static_cast<duckdb::row_t>(row - _validity_window.begin), out,
      out_offset);
    if (!duckdb::FlatVector::Validity(out).RowIsValid(out_offset)) {
      return false;
    }
  }
  _window = _reader->Locate(row, _window);
  if (_window.block != _cached_block) {
    _block = _reader->Open(_window, _ctx);
    _fetch_state = duckdb::ColumnFetchState{};
    _cached_block = _window.block;
  }
  _block->FetchRow(_fetch_state,
                   static_cast<duckdb::row_t>(row - _window.begin), out,
                   out_offset);
  return duckdb::FlatVector::Validity(out).RowIsValid(out_offset);
}

}  // namespace irs
