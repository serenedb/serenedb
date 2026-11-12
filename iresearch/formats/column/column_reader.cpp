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
#include <atomic>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/vector/dictionary_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/buffer/buffer_handle.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <functional>
#include <memory>
#include <utility>

#include "iresearch/formats/column/array_column_reader.hpp"
#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/internal/gather_arms.hpp"
#include "iresearch/formats/column/list_column_reader.hpp"
#include "iresearch/formats/column/struct_column_reader.hpp"
#include "iresearch/formats/column/variant_column_reader.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/file_utils_ext.hpp"
#include "iresearch/utils/lower_bound.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

constexpr duckdb::idx_t kMaxFetchHits = 64;

void SerializeColumnBlockMeta(duckdb::BinarySerializer& s,
                              const ColumnBlockMeta& m) {
  s.WriteProperty(0, "compression_type", static_cast<uint8_t>(m.codec->type));
  s.WriteProperty(1, "tuple_count", m.tuple_count);
  s.WriteProperty(2, "file_offset", m.file_offset);
  s.WriteProperty(3, "byte_size", m.byte_size);
  s.WriteProperty(4, "statistics", m.statistics);
}

ColumnBlockMeta DeserializeColumnBlockMeta(duckdb::BinaryDeserializer& d,
                                           duckdb::PhysicalType physical) {
  const auto compression_type = d.ReadProperty<uint8_t>(0, "compression_type");
  const auto tuple_count = d.ReadProperty<uint64_t>(1, "tuple_count");
  const auto file_offset = d.ReadProperty<uint64_t>(2, "file_offset");
  const auto byte_size = d.ReadProperty<uint64_t>(3, "byte_size");
  auto stats = d.ReadProperty<duckdb::BaseStatistics>(4, "statistics");
  const auto* codec =
    codecs::GetCodec(codecs::CodecType{compression_type}, physical);
  if (!codec) [[unlikely]] {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("Column block uses compression ",
              static_cast<uint32_t>(compression_type),
              ", which this release of SereneDB does not have; it was written "
              "by a newer release"));
  }
  return ColumnBlockMeta{std::move(stats), tuple_count, file_offset, byte_size,
                         codec};
}

template<typename T, typename Write>
void WriteObjects(duckdb::BinarySerializer& s, duckdb::field_id_t id,
                  const char* tag, const std::vector<T>& items, Write&& write) {
  if (items.empty()) {
    return;
  }
  s.WriteList(id, tag, items.size(),
              [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
                list.WriteObject(
                  [&](duckdb::BinarySerializer& obj) { write(obj, items[i]); });
              });
}

template<typename Read>
void ReadObjects(duckdb::BinaryDeserializer& d, duckdb::field_id_t id,
                 const char* tag, Read&& read) {
  d.ReadOptionalList(
    id, tag, [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
      list.ReadObject(read);
    });
}

void ReadBlockMetas(duckdb::BinaryDeserializer& d, duckdb::field_id_t id,
                    const char* tag, const duckdb::LogicalType& type,
                    std::vector<ColumnBlockMeta>& out) {
  d.Set<const duckdb::LogicalType&>(type);
  ReadObjects(d, id, tag, [&](duckdb::BinaryDeserializer& obj) {
    out.push_back(DeserializeColumnBlockMeta(obj, type.InternalType()));
  });
  d.Unset<const duckdb::LogicalType>();
}

bool BorrowsBlockStrings(const codecs::ColumnSegment& segment) {
  return segment.type.InternalType() == duckdb::PhysicalType::VARCHAR &&
         !segment.function.framed;
}

void ScanSegment(const codecs::ColumnSegment& segment,
                 codecs::ColumnScanState& st, duckdb::idx_t count,
                 duckdb::Vector& result, duckdb::idx_t result_offset,
                 duckdb::ScanVectorType scan_type) {
  if (scan_type == duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR) {
    SDB_ASSERT(result_offset == 0);
    segment.function.scan_vector(segment, st, count, result);
    return;
  }
  SDB_ASSERT(result.GetVectorType() == duckdb::VectorType::FLAT_VECTOR);
  segment.function.scan_partial(segment, st, count, result, result_offset);
}

void SkipSegment(const codecs::ColumnSegment& segment,
                 codecs::ColumnScanState& st) {
  segment.function.skip(segment, st, st.offset_in_column - st.internal_index);
  st.internal_index = st.offset_in_column;
}

void FetchSegmentRows(const codecs::ColumnSegment& segment,
                      codecs::ColumnFetchState& state,
                      const duckdb::idx_t* rows, duckdb::idx_t count,
                      duckdb::Vector& result,
                      const duckdb::SelectionVector& result_sel,
                      duckdb::idx_t result_offset) {
  if (count == 0) {
    return;
  }
  SDB_ASSERT(rows[count - 1] < segment.count);
  segment.function.fetch_rows(segment, state, rows, count, result, result_sel,
                              result_offset);
}

struct GatherBands {
  uint32_t flat;
  uint32_t native;
};

GatherBands BandsFor(codecs::CodecType codec,
                     const duckdb::LogicalType& type) noexcept {
  switch (codec) {
    case codecs::CodecType::Constant:
      return {0, 1000};
    case codecs::CodecType::Rle:
      return {2, 70};
    case codecs::CodecType::Numeric:
      return {10, 1000};
    default:
      return type.InternalType() == duckdb::PhysicalType::VARCHAR
               ? GatherBands{10, 1000}
               : GatherBands{10, 0};
  }
}

bool FewerRuns(const duckdb::SelectionVector& sel, duckdb::idx_t hits,
               uint64_t limit) noexcept {
  if (hits < limit) {
    return true;
  }
  uint64_t runs = 1;
  for (duckdb::idx_t i = 1; i < hits; ++i) {
    runs += sel.get_index(i) != sel.get_index(i - 1) + 1;
    if (runs >= limit) {
      return false;
    }
  }
  return runs < limit;
}

bool FewStringRuns(const duckdb::LogicalType& type,
                   const duckdb::SelectionVector& sel, duckdb::idx_t hits,
                   duckdb::idx_t span) noexcept {
  return type.InternalType() == duckdb::PhysicalType::VARCHAR &&
         FewerRuns(sel, hits, (120 * (span - hits) + 999) / 1000);
}

bool ScatterWins(const ColumnBlockMeta& block, const duckdb::LogicalType& type,
                 const duckdb::SelectionVector& sel, duckdb::idx_t hits,
                 duckdb::idx_t span) noexcept {
  if (hits * 1000 <= BandsFor(block.codec->type, type).flat * span) {
    return true;
  }
  return !block.codec->select && FewStringRuns(type, sel, hits, span);
}

bool SelectsNatively(const ColumnBlockMeta& block,
                     const duckdb::LogicalType& type, duckdb::idx_t hits,
                     duckdb::idx_t span) noexcept {
  return block.codec->select &&
         hits * 1000 <= BandsFor(block.codec->type, type).native * span;
}

duckdb::ValidityMask& ResetAllValid(duckdb::Vector& v, duckdb::idx_t count) {
  v.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto& mask = duckdb::FlatVector::ValidityMutable(v);
  if (mask.CanHaveNull()) {
    mask.SetAllValid(count);
  }
  return mask;
}

}  // namespace

void SerializeColumnMeta(duckdb::BinarySerializer& s, const ColumnMeta& meta) {
  s.WriteProperty(0, "id", meta.id);
  s.WriteProperty(1, "type", meta.type);
  WriteObjects(s, 2, "data", meta.data, SerializeColumnBlockMeta);
  WriteObjects(s, 3, "validity", meta.validity, SerializeColumnBlockMeta);
  WriteObjects(s, 4, "children", meta.children, SerializeColumnMeta);
  WriteObjects(s, 5, "variant_rgs", meta.variant_rgs,
               [](duckdb::BinarySerializer& obj, const VariantRgMeta& rg) {
                 obj.WriteProperty(0, "row_count", rg.row_count);
                 obj.WriteObject(
                   1, "unshredded", [&](duckdb::BinarySerializer& unshredded) {
                     SerializeColumnMeta(unshredded, *rg.unshredded);
                   });
                 if (rg.shredded) {
                   obj.WriteObject(
                     2, "shredded", [&](duckdb::BinarySerializer& shredded) {
                       SerializeColumnMeta(shredded, *rg.shredded);
                     });
                 }
               });
  s.WritePropertyWithDefault(6, "hyperloglog", meta.hyperloglog);
}

ColumnMeta DeserializeColumnMeta(duckdb::BinaryDeserializer& d) {
  ColumnMeta meta;
  d.ReadProperty(0, "id", meta.id);
  d.ReadProperty(1, "type", meta.type);
  ReadBlockMetas(d, 2, "data", meta.type, meta.data);
  ReadBlockMetas(d, 3, "validity", duckdb::LogicalTypeId::VALIDITY,
                 meta.validity);
  ReadObjects(d, 4, "children", [&](duckdb::BinaryDeserializer& obj) {
    meta.children.push_back(DeserializeColumnMeta(obj));
  });
  ReadObjects(d, 5, "variant_rgs", [&](duckdb::BinaryDeserializer& obj) {
    auto& rg = meta.variant_rgs.emplace_back();
    obj.ReadProperty(0, "row_count", rg.row_count);
    obj.ReadObject(1, "unshredded", [&](duckdb::BinaryDeserializer& in) {
      rg.unshredded = std::make_unique<ColumnMeta>(DeserializeColumnMeta(in));
    });
    obj.ReadOptionalObject(2, "shredded", [&](duckdb::BinaryDeserializer& in) {
      rg.shredded = std::make_unique<ColumnMeta>(DeserializeColumnMeta(in));
    });
  });
  d.ReadPropertyWithDefault(6, "hyperloglog", meta.hyperloglog);
  return meta;
}

uint64_t BlockBytes(std::span<const ColumnBlockMeta> blocks) noexcept {
  uint64_t bytes = 0;
  for (const auto& block : blocks) {
    bytes += block.byte_size;
  }
  return bytes;
}

uint64_t StoredBytes(const ColumnReader& column) {
  uint64_t bytes = BlockBytes(column.DataBlocks());
  column.VisitSubColumns([&](const ColumnReader::SubColumn& sub) {
    bytes += StoredBytes(sub.column);
  });
  return bytes;
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
  _touched = std::make_unique<std::atomic<bool>[]>(_segments.size());
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

void ColumnReader::FinishStats(duckdb::BaseStatistics stats) {
  if (_validity) {
    stats.Merge(_validity->MergedStatistics());
  } else if (_row_count > 0 && !_children.empty()) {
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
  const auto it =
    BranchlessPartitionPoint(_offsets.data(), _offsets.size(),
                             [row](uint64_t offset) { return offset <= row; });
  const auto rg = static_cast<size_t>(it - _offsets.data()) - 1;
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

std::unique_ptr<codecs::ColumnSegment> ColumnReader::Open(const BlockWindow& w,
                                                          ReadContext& ctx,
                                                          ScanState* s) const {
  Readahead(w.block, ctx, s);
  const auto& m = _segments[w.block];
  const auto& codec = *m.codec;
  auto segment = std::make_unique<codecs::ColumnSegment>(codecs::ColumnSegment{
    .function = codec,
    .type = m.statistics.GetType(),
    .stats = m.statistics,
    .ctx = ctx,
    .count = m.tuple_count,
    .size = m.byte_size,
  });
  if (m.byte_size == 0) {
    if (codec.type != codecs::CodecType::Constant &&
        codec.type != codecs::CodecType::Empty) [[unlikely]] {
      codecs::ThrowCorrupted(codecs::NameOf(codec.type),
                             "segment has no bytes");
    }
    return segment;
  }
  if (!codec.framed && m.byte_size > codecs::kBlockSize) [[unlikely]] {
    codecs::ThrowCorrupted(codecs::NameOf(codec.type),
                           "segment is larger than a block");
  }
  if (codec.framed) {
    auto& slot = segment->cache_slot;
    slot.file_id = _file_id;
    slot.file_offset = m.file_offset;
    if (auto* cache = ctx.Cache()) {
      slot.touched = &_touched[w.block];
      slot.cache = cache;
    }
  }
  segment->block = ctx.RegisterColBlock(m.file_offset, m.byte_size);
  return segment;
}

void ColumnReader::Readahead(size_t block, ReadContext& ctx,
                             ScanState* s) const noexcept {
  if (ctx.RandomAccess()) {
    return;
  }
  auto& in = ctx.In();
  const auto& first = _segments[block];
  if (s == nullptr) {
    file_utils::Hint(in, first.file_offset, first.byte_size);
    return;
  }
  const bool sequential = block != 0 && s->opened_block + 1 == block;
  s->opened_block = block;
  if (block < s->advised_end) {
    return;
  }
  if (in.Resident(first.file_offset, first.byte_size)) {
    s->advised_end = block + 1;
    return;
  }
  uint64_t budget = sequential ? file_utils::kMaxReadahead : 0;
  auto b = block;
  do {
    const auto& m = _segments[b++];
    in.Prefetch(m.file_offset, m.byte_size);
    const auto cost = std::max<uint64_t>(m.byte_size, file_utils::kPage);
    if (budget <= cost) {
      break;
    }
    budget -= cost;
  } while (b < _segments.size());
  s->advised_end = b;
}

ColumnReader::ScanState ColumnReader::InitScan(
  std::shared_ptr<ReadContext> ctx) const {
  ScanState s;
  s.ctx = std::move(ctx);
  if (!_segments.empty()) {
    s.window = BlockWindow{0, _offsets[0], _offsets[1]};
  }
  s.child_states.reserve(_children.size() + 1);
  s.child_states.push_back(_validity ? _validity->InitScan(s.ctx)
                                     : ScanState{});
  for (const auto& child : _children) {
    s.child_states.push_back(child->InitScan(s.ctx));
  }
  return s;
}

namespace {

struct ScannedStrings final : duckdb::AuxiliaryDataHolder {
  ScannedStrings(std::shared_ptr<ReadContext> ctx,
                 duckdb::BufferHandle pin) noexcept
    : ctx{std::move(ctx)}, pin{std::move(pin)} {}

  bool CertifiesImmutablePayloads() const final { return true; }

  std::shared_ptr<ReadContext> ctx;
  duckdb::BufferHandle pin;
};

}  // namespace

void ColumnReader::CertifyStrings(ScanState& s,
                                  const codecs::ColumnSegment& segment,
                                  duckdb::Vector& result) const {
  if (s.ctx.use_count() == 0 || !BorrowsBlockStrings(segment) ||
      !segment.block) {
    return;
  }
  auto* target = &result;
  if (result.GetVectorType() == duckdb::VectorType::DICTIONARY_VECTOR) {
    if (s.certified_dictionary) {
      return;
    }
    s.certified_dictionary = true;
    target = &duckdb::DictionaryVector::Child(result);
  }
  duckdb::StringVector::AddAuxiliaryData(
    *target, duckdb::make_uniq<ScannedStrings>(s.ctx, segment.Pin()));
}

void ColumnReader::OpenScanSegment(ScanState& s) const {
  if (s.st.scan_state) {
    s.st.previous_states.emplace_back(std::move(s.st.scan_state));
  }
  const auto& segment = *s.segments.emplace_back(Open(s.window, *s.ctx, &s));
  s.st.scan_state = segment.function.init_scan(segment);
  s.certified_dictionary = false;
  s.st.internal_index = 0;
  s.initialized = true;
}

void ColumnReader::BeginScanVector(ScanState& s) const {
  if (s.st.offset_in_column == s.window.end - s.window.begin &&
      NextSegment(s.window)) {
    s.initialized = false;
    s.st.offset_in_column = 0;
  }
  if (!s.initialized) {
    OpenScanSegment(s);
  }
  SDB_ASSERT(s.st.internal_index <= s.st.offset_in_column);
  if (s.st.internal_index < s.st.offset_in_column) {
    SkipSegment(*s.segments.back(), s.st);
  }
}

void ColumnReader::NewOutputVector(ScanState& s) const {
  s.st.previous_states.clear();
  s.retired_fetches.clear();
  s.retired_states.clear();
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
  if (!_validity) {
    return duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR;
  }
  return _validity->GetVectorScanType(s.child_states[0], count, result);
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
      ScanSegment(*s.segments.back(), s.st, scan_count, result, result_offset,
                  scan_type);
      CertifyStrings(s, *s.segments.back(), result);
      s.st.offset_in_column += scan_count;
      remaining -= scan_count;
    }
    if (remaining > 0) {
      if (!NextSegment(s.window)) {
        break;
      }
      s.st.offset_in_column = 0;
      OpenScanSegment(s);
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
  SkipValidity(s, count);
}

void ColumnReader::SkipValidity(ScanState& s, duckdb::idx_t count) const {
  if (_validity) {
    _validity->SkipRows(s.child_states[0], count);
  }
}

void ColumnReader::SkipTo(ScanState& s, uint64_t row) const {
  const uint64_t cur = ColumnReader::GatherCursor(s);
  SDB_ASSERT(row >= cur);
  if (row > cur) {
    ColumnReader::Skip(s, row - cur);
  }
}

void ColumnReader::FetchInto(ScanState& s, std::span<const uint64_t> rows,
                             duckdb::sel_t* positions, duckdb::Vector& out,
                             duckdb::idx_t at) const {
  SDB_ASSERT(rows.size() <= kMaxFetchHits);
  std::array<duckdb::idx_t, kMaxFetchHits> in_block;
  for (duckdb::idx_t i = 0; i < rows.size();) {
    if (!s.fetch.segment || rows[i] < s.fetch_window.begin ||
        rows[i] >= s.fetch_window.end) {
      if (s.fetch.segment) {
        s.retired_fetches.push_back(std::move(s.fetch));
        s.fetch = {};
      }
      s.fetch_window = Locate(rows[i], s.window);
      s.fetch.segment = Open(s.fetch_window, *s.ctx);
    }
    auto end = i;
    for (; end < rows.size() && rows[end] < s.fetch_window.end; ++end) {
      in_block[end - i] = rows[end] - s.fetch_window.begin;
    }
    FetchSegmentRows(*s.fetch.segment, s.fetch.state, in_block.data(), end - i,
                     out, duckdb::SelectionVector{positions + i, end - i}, at);
    i = end;
  }
}

bool ColumnReader::FetchSparse(ScanState& s, uint64_t anchor,
                               const duckdb::SelectionVector& sel,
                               duckdb::idx_t hits, duckdb::Vector& out,
                               duckdb::idx_t at) const {
  const uint64_t first = anchor + sel.get_index(0);
  const uint64_t last = anchor + sel.get_index(hits - 1);
  if (hits >= kMaxFetchHits || last >= _row_count) {
    return false;
  }
  const auto window = Locate(first, s.window);
  if (last >= window.end || (s.initialized && window.block == s.window.block) ||
      (s.fetch.segment && s.fetch_window.block == window.block)) {
    return false;
  }
  SkipTo(s, first);
  if (at == 0) {
    out.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  }
  auto& mask = duckdb::FlatVector::ValidityMutable(out);
  std::array<uint64_t, kMaxFetchHits> rows;
  std::array<duckdb::sel_t, kMaxFetchHits> positions;
  for (duckdb::idx_t i = 0; i < hits; ++i) {
    rows[i] = anchor + sel.get_index(i);
    positions[i] = static_cast<duckdb::sel_t>(i);
    mask.SetValid(at + i);
  }
  auto valid = hits;
  if (_validity) {
    _validity->FetchInto(s.child_states[0], {rows.data(), hits},
                         positions.data(), out, at);
    valid = 0;
    for (duckdb::idx_t i = 0; i < hits; ++i) {
      if (mask.RowIsValid(at + i)) {
        rows[valid] = rows[i];
        positions[valid] = static_cast<duckdb::sel_t>(i);
        ++valid;
      }
    }
  }
  FetchInto(s, {rows.data(), valid}, positions.data(), out, at);
  ColumnReader::Skip(s, last + 1 - first);
  return true;
}

void ColumnReader::GatherScatter(ScanState& s, uint64_t anchor,
                                 const duckdb::SelectionVector& sel,
                                 duckdb::idx_t hits, duckdb::Vector& out,
                                 duckdb::idx_t at) const {
  if (at == 0) {
    NewOutputVector(s);
  }
  if (FetchSparse(s, anchor, sel, hits, out, at)) {
    return;
  }
  const auto span = sel.get_index(hits - 1) - sel.get_index(0) + 1;
  for (duckdb::idx_t i = 0; i < hits;) {
    const auto first = sel.get_index(i);
    if (anchor + first >= _row_count) {
      column_internal::ScatterRuns(*this, s, anchor, sel, i, hits, out, at);
      return;
    }
    auto end = i + 1;
    if (const auto block = Locate(anchor + first, s.window);
        !SelectsNatively(_segments[block.block], _type, hits, span)) {
      if (anchor + sel.get_index(hits - 1) < block.end) {
        end = hits;
      }
      while (end < hits && anchor + sel.get_index(end) < block.end) {
        ++end;
      }
      column_internal::ScatterRuns(*this, s, anchor, sel, i, end, out, at);
      i = end;
      continue;
    }
    while (end < hits && sel.get_index(end) - first < STANDARD_VECTOR_SIZE) {
      ++end;
    }
    const auto* window = &sel;
    if (first != 0) {
      if (!s.window_sel.IsSet()) {
        s.window_sel.Initialize(STANDARD_VECTOR_SIZE);
      }
      for (auto k = i; k < end; ++k) {
        s.window_sel.set_index(k - i, sel.get_index(k) - first);
      }
      window = &s.window_sel;
    }
    if ((i == 0 && end == hits) ||
        !FetchSparse(s, anchor + first, *window, end - i, out, at + i)) {
      GatherWindow(s, anchor + first, *window, end - i,
                   sel.get_index(end - 1) - first + 1, out, at + i);
    }
    i = end;
  }
}

void ColumnReader::GatherWindow(ScanState& s, uint64_t anchor,
                                const duckdb::SelectionVector& sel,
                                duckdb::idx_t hits, duckdb::idx_t span,
                                duckdb::Vector& out, duckdb::idx_t at) const {
  SDB_ASSERT(hits > 0 && hits <= span && span <= STANDARD_VECTOR_SIZE);
  if (hits != span && anchor < _row_count &&
      !ScatterWins(_segments[Locate(anchor, s.window).block], _type, sel, hits,
                   span)) {
    SkipTo(s, anchor);
    if (SelectNative(s, sel, hits, span, out, at,
                     duckdb::ScanVectorType::SCAN_FLAT_VECTOR)) {
      return;
    }
  }
  column_internal::ScatterRuns(*this, s, anchor, sel, 0, hits, out, at);
}

void ColumnReader::GatherDense(ScanState& s, uint64_t anchor,
                               const duckdb::SelectionVector& sel,
                               duckdb::idx_t hits, duckdb::idx_t span,
                               duckdb::Vector& out) const {
  SDB_ASSERT(hits > 0 && hits <= span && span <= STANDARD_VECTOR_SIZE);
  NewOutputVector(s);
  if (FetchSparse(s, anchor, sel, hits, out, 0)) {
    return;
  }
  SkipTo(s, anchor);
  if (hits == span) {
    ColumnReader::Scan(s, out, span);
    return;
  }
  if (anchor < _row_count &&
      ScatterWins(_segments[Locate(anchor, s.window).block], _type, sel, hits,
                  span)) {
    column_internal::ScatterRuns(*this, s, anchor, sel, 0, hits, out, 0);
    return;
  }
  if (SelectNative(s, sel, hits, span, out, 0,
                   duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR)) {
    return;
  }
  if (FewStringRuns(_type, sel, hits, span)) {
    column_internal::ScatterRuns(*this, s, anchor, sel, 0, hits, out, 0);
    return;
  }
  ColumnReader::ScanCount(s, out, span, 0);
  out.Slice(sel, hits);
}

bool ColumnReader::SelectsAcross(const ScanState& s,
                                 duckdb::idx_t span) const noexcept {
  const uint64_t end = s.window.begin + s.st.offset_in_column + span;
  if (end > _offsets.back()) {
    return false;
  }
  for (size_t block = s.window.block;
       block < _segments.size() && _offsets[block] < end; ++block) {
    const auto* choice = codecs::ChoiceOf(_segments[block].codec->type);
    if (!choice || choice->shape != codecs::Shape::Plain) {
      return false;
    }
  }
  return true;
}

void ColumnReader::SelectAcross(ScanState& s,
                                const duckdb::SelectionVector& sel,
                                duckdb::idx_t hits, duckdb::idx_t span,
                                duckdb::Vector& out, duckdb::idx_t at) const {
  out.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  if (!s.piece.IsSet()) {
    s.piece.Initialize(STANDARD_VECTOR_SIZE);
  }
  duckdb::idx_t row = 0;
  for (duckdb::idx_t i = 0; i < hits;) {
    SkipRows(s, sel.get_index(i) - row);
    row = sel.get_index(i);
    BeginScanVector(s);
    const auto rows = std::min<duckdb::idx_t>(
      span - row, (s.window.end - s.window.begin) - s.st.offset_in_column);
    auto end = i + 1;
    while (end < hits && sel.get_index(end) - row < rows) {
      ++end;
    }
    for (auto k = i; k < end; ++k) {
      s.piece.set_index(k - i, sel.get_index(k) - row);
    }
    const auto& segment = *s.segments.back();
    segment.function.select(segment, s.st, rows, out, at + i, s.piece, end - i,
                            duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
    const auto taken = sel.get_index(end - 1) + 1 - row;
    s.st.offset_in_column += taken;
    s.st.internal_index = s.st.offset_in_column;
    row += taken;
    i = end;
  }
  SkipRows(s, span - row);
}

bool ColumnReader::SelectNative(ScanState& s,
                                const duckdb::SelectionVector& sel,
                                duckdb::idx_t hits, duckdb::idx_t span,
                                duckdb::Vector& out, duckdb::idx_t at,
                                duckdb::ScanVectorType scan_type) const {
  BeginScanVector(s);
  const bool within =
    (s.window.end - s.window.begin) - s.st.offset_in_column >= span;
  const auto& codec = *_segments[s.window.block].codec;
  if (hits * 1000 > BandsFor(codec.type, _type).native * span ||
      !codec.select || (!within && !SelectsAcross(s, span))) {
    return false;
  }
  const bool self_valid =
    codec.validity == codecs::CompressionValidity::NoValidityRequired;
  if (_validity && !self_valid) {
    auto& vs = s.child_states[0];
    _validity->BeginScanVector(vs);
    if ((vs.window.end - vs.window.begin) - vs.st.offset_in_column < span ||
        !_validity->_segments[vs.window.block].codec->select) {
      return false;
    }
  }
  if (within) {
    const auto& segment = *s.segments.back();
    segment.function.select(segment, s.st, span, out, at, sel, hits, scan_type);
    s.st.offset_in_column += span;
    s.st.internal_index = s.st.offset_in_column;
  } else {
    SelectAcross(s, sel, hits, span, out, at);
  }
  if (self_valid) {
    SkipValidity(s, span);
  } else if (_validity) {
    auto& vs = s.child_states[0];
    const auto& validity = *vs.segments.back();
    validity.function.select(validity, vs.st, span, out, at, sel, hits,
                             scan_type);
    vs.st.offset_in_column += span;
    vs.st.internal_index = vs.st.offset_in_column;
  }
  return true;
}

duckdb::idx_t ColumnReader::GatherFilter(
  ScanState& s, uint64_t anchor, duckdb::idx_t span,
  duckdb::SelectionVector& sel, duckdb::idx_t sel_count,
  const duckdb::TableFilter& filter, duckdb::TableFilterState& filter_state,
  NullCheckKind null_check, duckdb::Vector& result) const {
  NewOutputVector(s);
  SDB_ASSERT(span <= STANDARD_VECTOR_SIZE);
  SkipTo(s, anchor);
  BeginScanVector(s);
  const auto& seg = *s.segments.back();
  const auto& codec = seg.function;
  duckdb::idx_t approved = sel_count;
  const bool within_segment =
    (s.window.end - s.window.begin) - s.st.offset_in_column >= span;
  const bool self_valid =
    codec.validity == codecs::CompressionValidity::NoValidityRequired;
  // The codec filter needs the data segment alone to be validity-complete for
  // the span: either the codec self-describes nulls (dict_fsst evaluates its
  // dictionary once and filters rows by code), or the span's validity block is
  // the all-valid EMPTY codec (rle evaluates its run values once and filters
  // rows by run flag). Bare null checks keep the validity-only arm below, and
  // null-bearing spans keep the decode arm.
  const bool codec_filter =
    within_segment && codec.filter && !filter_state.can_throw &&
    (self_valid || (null_check == NullCheckKind::None &&
                    ValiditySpanAllValid(s, anchor, span)));
  if (codec_filter) {
    if (!self_valid) {
      ResetAllValid(result, span);
    }
    codec.filter(seg, s.st, span, result, sel, approved, filter, filter_state);
    s.st.offset_in_column += span;
    s.st.internal_index = s.st.offset_in_column;
    // Codecs are picked per block: a later block of the same column may need
    // the validity child (decode path below), whose cursor only moves
    // relatively -- keep it in step with the data cursor.
    SkipValidity(s, span);
  } else if (within_segment && null_check != NullCheckKind::None &&
             !self_valid) {
    // Bare IS [NOT] NULL: the validity child alone answers it -- skip the
    // data decode entirely (the data cursor still advances to stay in step).
    SkipRows(s, span);
    auto& mask = ResetAllValid(result, span);
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
    ResetAllValid(result, span);
    if (all_valid) {
      ScanVector(s, result, span, duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
      SkipValidity(s, span);
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
  return anchor + span <= w.end &&
         _validity->_segments[w.block].codec->type == codecs::CodecType::Empty;
}

void ColumnReader::SkipRows(ScanState& s, duckdb::idx_t count) const {
  if (count < (s.window.end - s.window.begin) - s.st.offset_in_column) {
    s.st.offset_in_column += count;
    return;
  }
  if (count == 0 || _segments.empty()) {
    return;
  }
  const uint64_t end = _offsets.back();
  const uint64_t target =
    std::min<uint64_t>(s.window.begin + s.st.offset_in_column + count, end);
  const auto window =
    target < end
      ? Locate(target, s.window)
      : BlockWindow{_segments.size() - 1, _offsets[_segments.size() - 1], end};
  if (window.block != s.window.block) {
    s.window = window;
    s.initialized = false;
  }
  s.st.offset_in_column = target - s.window.begin;
}

std::unique_ptr<ColumnReader> ColumnReader::Make(ColumnMeta&& meta,
                                                 uint64_t file_id) {
  std::unique_ptr<ColumnReader> validity;
  if (absl::c_any_of(meta.validity, [](const ColumnBlockMeta& m) {
        return m.codec->type != codecs::CodecType::Empty;
      })) {
    validity.reset(new ColumnReader{
      meta.id,
      duckdb::LogicalTypeId::VALIDITY,
      std::move(meta.validity),
      nullptr,
      {},
    });
    validity->_file_id = file_id;
  }

  std::vector<std::unique_ptr<ColumnReader>> children;
  children.reserve(meta.children.size());
  for (auto& c : meta.children) {
    children.push_back(Make(std::move(c), file_id));
  }

  std::unique_ptr<ColumnReader> col;
  switch (meta.type.id()) {
    case duckdb::LogicalTypeId::VARIANT:
      SDB_ASSERT(children.empty());
      col = std::make_unique<VariantColumnReader>(
        meta.id, std::move(meta.type), std::move(validity),
        std::move(meta.variant_rgs), file_id);
      break;
    case duckdb::LogicalTypeId::UNION:
    case duckdb::LogicalTypeId::STRUCT:
    case duckdb::LogicalTypeId::TUPLE:
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
  col->_file_id = file_id;
  return col;
}

ColumnReader::PointReader::PointReader(const ColReader& col_reader,
                                       const ColumnReader& col,
                                       bool random_access)
  : _ctx{col_reader, random_access},
    _reader{&col},
    _blocks(col.DataRgCount()),
    _validity_blocks(col.Validity() ? col.Validity()->DataRgCount() : 0) {}

ColumnReader::PointReader::OpenBlock& ColumnReader::PointReader::Block(
  std::vector<OpenBlock>& blocks, const ColumnReader& reader,
  BlockWindow& window, uint64_t row) {
  if (row < window.begin || row >= window.end) {
    window = reader.Locate(row, window);
  }
  auto& block = blocks[window.block];
  block.last_use = ++_uses;
  if (!block.segment) {
    const auto bytes = reader._segments[window.block].byte_size;
    Evict(bytes);
    block.segment = reader.Open(window, _ctx);
    block.bytes = bytes;
    _open_bytes += bytes;
    _open.push_back(&block);
  }
  return block;
}

void ColumnReader::PointReader::Evict(uint64_t incoming) {
  constexpr uint64_t kOpenBytes = uint64_t{64} << 20;
  while (!_open.empty() && _open_bytes + incoming > kOpenBytes) {
    const auto oldest =
      absl::c_min_element(_open, [](const OpenBlock* l, const OpenBlock* r) {
        return l->last_use < r->last_use;
      });
    auto& block = **oldest;
    block.state = codecs::ColumnFetchState{};
    block.segment.reset();
    _open_bytes -= block.bytes;
    block.bytes = 0;
    *oldest = _open.back();
    _open.pop_back();
  }
}

IRS_FORCE_INLINE bool ColumnReader::PointReader::FetchOne(
  std::vector<OpenBlock>& blocks, const ColumnReader& reader,
  BlockWindow& window, uint64_t row, duckdb::Vector& out,
  duckdb::idx_t out_offset) {
  auto& block = Block(blocks, reader, window, row);
  const duckdb::idx_t local = row - window.begin;
  FetchSegmentRows(*block.segment, block.state, &local, 1, out,
                   *duckdb::FlatVector::IncrementalSelectionVector(),
                   out_offset);
  return duckdb::FlatVector::Validity(out).RowIsValid(out_offset);
}

bool ColumnReader::PointReader::FetchRow(uint64_t row, duckdb::Vector& out,
                                         duckdb::idx_t out_offset) {
  if (row >= _reader->RowCount()) {
    duckdb::FlatVector::ValidityMutable(out).SetInvalid(out_offset);
    return false;
  }
  duckdb::FlatVector::ValidityMutable(out).SetValid(out_offset);
  const auto* validity = _reader->_validity.get();
  if (validity && !FetchOne(_validity_blocks, *validity, _validity_window, row,
                            out, out_offset)) {
    return false;
  }
  return FetchOne(_blocks, *_reader, _window, row, out, out_offset);
}

void ColumnReader::PointReader::FetchRows(std::span<const uint32_t> ids,
                                          uint32_t first_id,
                                          duckdb::Vector& out,
                                          duckdb::idx_t out_offset) {
  SDB_ASSERT(out_offset + ids.size() <= STANDARD_VECTOR_SIZE);
  SDB_ASSERT(ids.empty() || ids.front() >= first_id);
  SDB_ASSERT(absl::c_adjacent_find(ids, std::greater_equal<>{}) == ids.end());
  auto& mask = duckdb::FlatVector::ValidityMutable(out);
  auto count = ids.size();
  for (size_t i = 0; i < count; ++i) {
    mask.SetValid(out_offset + i);
  }
  const uint64_t stored = _reader->RowCount() + first_id;
  while (count != 0 && ids[count - 1] >= stored) {
    mask.SetInvalid(out_offset + --count);
  }
  if (const auto* validity = _reader->_validity.get()) {
    for (size_t i = 0; i < count;) {
      auto& block =
        Block(_validity_blocks, *validity, _validity_window, ids[i] - first_id);
      const uint64_t begin = _validity_window.begin + first_id;
      const uint64_t end = _validity_window.end + first_id;
      _local.clear();
      size_t j = i;
      for (; j < count; ++j) {
        const uint64_t id = ids[j];
        if (id >= end) {
          break;
        }
        _local.push_back(id - begin);
      }
      FetchSegmentRows(
        *block.segment, block.state, _local.data(), _local.size(), out,
        *duckdb::FlatVector::IncrementalSelectionVector(), out_offset + i);
      i = j;
    }
  }
  _fetched.clear();
  for (size_t i = 0; i < count; ++i) {
    if (mask.RowIsValid(out_offset + i)) {
      _fetched.push_back(out_offset + i);
    }
  }
  for (size_t i = 0; i < _fetched.size();) {
    auto& block = Block(_blocks, *_reader, _window,
                        ids[_fetched[i] - out_offset] - first_id);
    const uint64_t begin = _window.begin + first_id;
    const uint64_t end = _window.end + first_id;
    _local.clear();
    size_t j = i;
    for (; j < _fetched.size(); ++j) {
      const auto at = _fetched[j];
      const uint64_t id = ids[at - out_offset];
      if (id >= end) {
        break;
      }
      _sel.set_index(_local.size(), at);
      _local.push_back(id - begin);
    }
    FetchSegmentRows(*block.segment, block.state, _local.data(), _local.size(),
                     out, _sel, 0);
    if (BorrowsBlockStrings(*block.segment)) {
      auto* data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(out);
      for (size_t k = 0; k < _local.size(); ++k) {
        auto& value = data[_sel.get_index(k)];
        if (!value.IsInlined()) {
          value = duckdb::StringVector::AddStringOrBlob(out, value);
        }
      }
    }
    i = j;
  }
}

bool ColumnReader::PointReader::ValidRow(uint64_t row,
                                         duckdb::Vector& scratch) {
  if (row >= _reader->RowCount()) {
    return false;
  }
  const auto* validity = _reader->_validity.get();
  if (!validity) {
    return true;
  }
  duckdb::FlatVector::ValidityMutable(scratch).SetValid(0);
  return FetchOne(_validity_blocks, *validity, _validity_window, row, scratch,
                  0);
}

}  // namespace irs
