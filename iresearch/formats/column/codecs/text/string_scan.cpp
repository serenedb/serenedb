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

#include "iresearch/formats/column/codecs/text/string_scan.hpp"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/immutable_strings.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/storage/buffer/buffer_handle.hpp>

#include "iresearch/formats/column/codecs/bytes/byte_codec.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/utils/enum_name.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs::text {

using duckdb::string_t;

duckdb::shared_ptr<DecodedDictionary> CachedDictionary(
  const ReadContext::CacheSlot& slot) {
  if (!slot.touched || !Touched(slot)) {
    return nullptr;
  }
  return slot.cache->Get<DecodedDictionary>(
    DictionaryCacheKey(slot.file_id, slot.file_offset));
}

std::string_view SymbolTable(const ColumnSegment& segment, const Header& h,
                             duckdb::const_data_ptr_t base, std::string& copy) {
  if (h.symtab_size != 0 || segment.shared_size == 0) {
    return {reinterpret_cast<const char*>(base + h.off_symtab), h.symtab_size};
  }
  if (const auto* stable =
        segment.ctx.TryReadStable(segment.shared_offset, segment.shared_size)) {
    return {reinterpret_cast<const char*>(stable), segment.shared_size};
  }
  copy.resize(segment.shared_size);
  segment.ctx.Read(segment.shared_offset, duckdb::data_ptr_cast(copy.data()),
                   segment.shared_size);
  return copy;
}

bool HasRoom(const ReadContext::CacheSlot& slot, uint64_t bytes) {
  return slot.touched && slot.cache->GetCurrentMemoryRacy() + bytes <=
                           slot.cache->GetMaxMemoryRacy();
}

Header SegmentHeader(duckdb::const_data_ptr_t base,
                     const ColumnSegment& segment, Shape shape,
                     ByteCodec codec) {
  SDB_ASSERT(segment.codec == CodecOf(shape, codec));
  const auto h = Header::Parse(base, segment.size, segment.codec);
  const uint64_t first_entry = shape == Shape::Dictionary ? 1 : 0;
  SDB_ENSURE(h.row_count == segment.count &&
               uint64_t{h.entry_count} <= uint64_t{h.row_count} + first_entry &&
               (shape == Shape::Dictionary || h.entry_count == h.row_count),
             "col codec: corrupted segment header");
  return h;
}

EntryRanges Normalize(EntryRanges ranges) {
  absl::c_sort(ranges);
  size_t kept = 0;
  for (const auto [lo, hi] : ranges) {
    if (lo >= hi) {
      continue;
    }
    if (kept != 0 && lo <= ranges[kept - 1].hi) {
      ranges[kept - 1].hi = std::max(ranges[kept - 1].hi, hi);
    } else {
      ranges[kept++] = {lo, hi};
    }
  }
  ranges.resize(kept);
  return ranges;
}

EntryRanges Intersect(const EntryRanges& a, const EntryRanges& b) {
  EntryRanges out;
  size_t i = 0;
  size_t j = 0;
  while (i < a.size() && j < b.size()) {
    const auto lo = std::max(a[i].lo, b[j].lo);
    const auto hi = std::min(a[i].hi, b[j].hi);
    if (lo < hi) {
      out.emplace_back(lo, hi);
    }
    if (a[i].hi < b[j].hi) {
      ++i;
    } else {
      ++j;
    }
  }
  return out;
}

const duckdb::Value* StringConstant(const duckdb::Expression& expr) noexcept {
  if (expr.GetExpressionType() != duckdb::ExpressionType::VALUE_CONSTANT) {
    return nullptr;
  }
  const auto& value = expr.Cast<duckdb::BoundConstantExpression>().GetValue();
  if (value.type().id() != duckdb::LogicalTypeId::VARCHAR) {
    return nullptr;
  }
  return &value;
}

uint64_t TouchedBytes(const Header& h, duckdb::const_data_ptr_t base,
                      std::span<const EntryAt> order) {
  const auto* table = base + h.off_frames;
  uint64_t bytes = 0;
  uint32_t counted = h.frame_count;
  for (const auto& [entry, at] : order) {
    if (h.frame_count == 0 || (counted != h.frame_count &&
                               FrameAt(table, counted).first_entry <= entry)) {
      continue;
    }
    counted = FrameOfEntry(table, counted, entry);
    bytes += FrameAt(table, counted).raw_len;
  }
  return bytes;
}

uint64_t DistinctEntries(std::span<const EntryAt> order) noexcept {
  uint64_t distinct = 0;
  for (size_t i = 0; i < order.size(); ++i) {
    distinct += i == 0 || order[i].entry != order[i - 1].entry;
  }
  return distinct;
}

void ReferenceOnce(HeapReference& reference, const duckdb::Vector& source,
                   duckdb::Vector& result) {
  const auto& heap = source.GetBufferRef()->GetAuxiliaryData();
  if (const auto& held = result.GetBufferRef()->GetAuxiliaryData();
      heap.get() == reference.source && held &&
      held.get() == reference.target && !reference.target_weak.expired()) {
    return;
  }
  duckdb::StringVector::AddAuxiliaryData(
    result, duckdb::ImmutableStrings::Reference(heap));
  const auto& held = result.GetBufferRef()->GetAuxiliaryData();
  reference.target_weak = held;
  reference.target = held.get();
  reference.source = heap.get();
}

void EmitDecoded(const DecodedDictionary& decoded,
                 std::span<const EntryAt> order, duckdb::Vector& result,
                 HeapReference& reference) {
  const auto& dictionary = decoded.Dictionary()->data;
  ReferenceOnce(reference, dictionary, result);
  const auto* values = duckdb::FlatVector::GetData<string_t>(dictionary);
  auto* out = duckdb::FlatVector::GetDataMutable<string_t>(result);
  for (size_t i = 0; i < order.size(); ++i) {
    if (i + kFetchAhead < order.size()) {
      __builtin_prefetch(values + order[i + kFetchAhead].entry);
    }
    out[order[i].at] = values[order[i].entry];
  }
}

duckdb::InsertionOrderPreservingMap<std::string> SegmentInfo(
  const ColumnSegment& segment) {
  auto handle = segment.Pin();
  const auto* base = handle.Ptr();
  const auto h = Header::Parse(base, segment.size, segment.codec);
  const auto shape = ShapeOf(segment.codec);
  duckdb::InsertionOrderPreservingMap<std::string> info;
  info["shape"] = std::string{SnakeCaseName(shape)};
  info["codec"] = std::string{SnakeCaseName(LeafOf(segment.codec))};
  info["level"] = absl::StrCat(static_cast<uint32_t>(h.level));
  if (shape == Shape::Dictionary) {
    info["codes_transform"] = std::string{
      TransformName(ShapeOf(h.codes),
                    ParseFrames(base + h.off_codes, h.off_symtab - h.off_codes,
                                h.row_count, h.codes))};
  }
  info["entries"] = absl::StrCat(h.entry_count);
  info["frames"] = absl::StrCat(h.frame_count);
  if (h.flags != FrameDictionary::None) {
    info["dictionary"] = std::string{SnakeCaseName(h.flags)};
  }
  info["raw_bytes"] = absl::StrCat(h.raw_bytes);
  info["data_bytes"] = absl::StrCat(h.data_size);
  return info;
}

}  // namespace irs::codecs::text
