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

#include "iresearch/formats/column/codecs/string_scan.hpp"

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
#include <duckdb/storage/buffer_manager.hpp>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
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

NumericHeader CodesHeader(duckdb::const_data_ptr_t base, const Header& h) {
  const auto codes =
    NumericHeader::Parse(base + h.off_codes, h.off_symtab - h.off_codes);
  SDB_ENSURE(codes.row_count == h.row_count && codes.leaf == NumericLeaf::None,
             "col codec: corrupted row codes");
  return codes;
}

const TrainedDictionary* TrainedOf(duckdb::ColumnSegment& segment,
                                   const Header& header) {
  if (header.flags != FrameDictionary::Trained) {
    return nullptr;
  }
  const auto* dictionaries = CacheSlotOf(segment).dictionaries;
  SDB_ENSURE(dictionaries && header.dictionary <= dictionaries->size(),
             "col codec: missing trained dictionary ", header.dictionary);
  return (*dictionaries)[header.dictionary - 1].get();
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

void ReferenceOnce(HeapReference& reference, const duckdb::Vector& source,
                   duckdb::Vector& result) {
  const auto& heap = source.GetBufferRef()->GetAuxiliaryData();
  if (heap.get() == reference.source &&
      reference.target.Holds(result.GetBufferRef()->GetAuxiliaryData())) {
    return;
  }
  duckdb::StringVector::AddAuxiliaryData(
    result, duckdb::ImmutableStrings::Reference(heap));
  reference.target.Set(result.GetBufferRef()->GetAuxiliaryData());
  reference.source = heap.get();
}

void ReferenceFrame(FrameLink& link,
                    const duckdb::shared_ptr<const FrameBytes>& frame,
                    duckdb::Vector& result) {
  if (frame.get() == link.frame &&
      link.target.Holds(result.GetBufferRef()->GetAuxiliaryData())) {
    return;
  }
  duckdb::StringVector::AddAuxiliaryData(
    result, duckdb::make_uniq<FrameReference>(frame));
  link.target.Set(result.GetBufferRef()->GetAuxiliaryData());
  link.frame = frame.get();
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
  duckdb::QueryContext, duckdb::ColumnSegment& segment) {
  auto& buffer_manager =
    duckdb::BufferManager::GetBufferManager(segment.GetDatabase());
  auto handle = buffer_manager.Pin(segment.GetBlockHandle());
  const auto* base = handle.Ptr() + segment.GetBlockOffset();
  const auto h = Header::Parse(base, segment.SegmentSize());
  duckdb::InsertionOrderPreservingMap<std::string> info;
  info["shape"] = std::string{SnakeCaseName(h.shape)};
  info["codec"] = std::string{SnakeCaseName(h.codec)};
  info["level"] = absl::StrCat(static_cast<uint32_t>(h.level));
  if (h.shape == Shape::Dedup) {
    const auto codes = CodesHeader(base, h);
    info["codes_transform"] = std::string{SnakeCaseName(codes.transform)};
  }
  info["entries"] = absl::StrCat(h.entry_count);
  info["frames"] = absl::StrCat(h.frame_count);
  if (h.flags == FrameDictionary::FirstFrame) {
    info["dictionary"] = "first_frame";
  } else if (h.flags == FrameDictionary::Trained) {
    info["dictionary"] = absl::StrCat("trained:", h.dictionary);
  }
  info["raw_bytes"] = absl::StrCat(h.raw_bytes);
  info["data_bytes"] = absl::StrCat(h.data_size);
  return info;
}

}  // namespace irs::codecs::text
