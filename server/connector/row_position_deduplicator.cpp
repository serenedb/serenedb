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

#include "connector/row_position_deduplicator.h"

#include <bit>
#include <duckdb/common/types/selection_vector.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/bit_utils.hpp>
#include <iresearch/utils/type_limits.hpp>

#include "connector/row_position.h"

namespace sdb::connector {
namespace {

constexpr uint32_t kWordBits = irs::BitsRequired<uint64_t>();
constexpr uint32_t kPageShift = 16;
constexpr uint32_t kPageWords = (uint32_t{1} << kPageShift) / kWordBits;
static_assert(kPageShift <= kRowPositionDocBits);

}  // namespace

RowPositionDeduplicator::RowPositionDeduplicator(
  std::span<const irs::doc_id_t> segment_docs) {
  _segments.reserve(segment_docs.size());
  for (const auto docs : segment_docs) {
    _segments.emplace_back(
      docs, std::make_unique<std::atomic<Word*>[]>((docs >> kPageShift) + 1));
  }
}

RowPositionDeduplicator::RowPositionDeduplicator(
  const irs::DirectoryReader& reader)
  : RowPositionDeduplicator{[&] {
      std::vector<irs::doc_id_t> docs(reader.size());
      for (size_t i = 0; i != docs.size(); ++i) {
        docs[i] = static_cast<irs::doc_id_t>(reader[i].docs_count());
      }
      return docs;
    }()} {}

RowPositionDeduplicator::~RowPositionDeduplicator() {
  for (auto& segment : _segments) {
    for (uint32_t i = 0, n = (segment.docs >> kPageShift) + 1; i != n; ++i) {
      delete[] segment.pages[i].load(std::memory_order_relaxed);
    }
  }
}

RowPositionDeduplicator::Word& RowPositionDeduplicator::WordOf(
  uint32_t index, irs::doc_id_t doc) {
  SDB_ASSERT(index < _segments.size());
  auto& segment = _segments[index];
  SDB_ASSERT(irs::doc_limits::min() <= doc && doc <= segment.docs);
  auto& slot = segment.pages[doc >> kPageShift];
  auto* page = slot.load(std::memory_order_acquire);
  if (!page) {
    auto* fresh = new Word[kPageWords]{};
    if (slot.compare_exchange_strong(page, fresh, std::memory_order_acq_rel,
                                     std::memory_order_acquire)) {
      page = fresh;
    } else {
      delete[] fresh;
    }
  }
  return page[(doc & ((uint32_t{1} << kPageShift) - 1)) / kWordBits];
}

duckdb::idx_t RowPositionDeduplicator::KeepFirst(duckdb::DataChunk& chunk,
                                                 duckdb::idx_t rowid) {
  const auto count = chunk.size();
  auto places = chunk.data[rowid].Values<int64_t>();
  duckdb::SelectionVector keep;
  duckdb::idx_t kept = 0;
  for (duckdb::idx_t i = 0; i < count;) {
    const auto first = static_cast<uint64_t>(places.GetValueUnsafe(i));
    const auto doc = RowPositionDoc(first);
    const auto shift = doc % kWordBits;
    duckdb::idx_t run = 1;
    while (i + run < count && shift + run < kWordBits &&
           static_cast<uint64_t>(places.GetValueUnsafe(i + run)) ==
             first + run) {
      ++run;
    }
    const auto mask =
      (run == kWordBits ? ~uint64_t{0} : (uint64_t{1} << run) - 1) << shift;
    const auto seen = mask & WordOf(RowPositionIndex(first), doc)
                               .fetch_or(mask, std::memory_order_relaxed);
    if (seen == 0 && !keep.IsSet()) {
      kept += run;
    } else {
      if (!keep.IsSet()) {
        keep.Initialize(count);
        for (duckdb::idx_t j = 0; j < kept; ++j) {
          keep.set_index(j, j);
        }
      }
      auto fresh = (mask & ~seen) >> shift;
      while (fresh != 0) {
        keep.set_index(kept++, i + std::countr_zero(fresh));
        fresh &= fresh - 1;
      }
    }
    i += run;
  }
  if (kept != count) {
    chunk.Slice(keep, kept);
  }
  return kept;
}

}  // namespace sdb::connector
