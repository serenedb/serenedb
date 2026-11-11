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
#include <limits>

#include "connector/row_position.h"

namespace sdb::connector {
namespace {

constexpr uint32_t kWordBits = irs::BitsRequired<uint64_t>();
constexpr uint32_t kPageShift = irs::DocumentMask::kChunkShift;
constexpr uint32_t kPageLow = (uint32_t{1} << kPageShift) - 1;
static_assert(kPageShift <= kRowPositionDocBits);

}  // namespace

RowPositionDeduplicator::RowPositionDeduplicator(const irs::IndexReader& view)
  : _units{static_cast<uint32_t>(view.size())},
    _first_page{std::make_unique<uint32_t[]>(_units + 1)} {
  for (uint32_t unit = 0; unit != _units; ++unit) {
    _first_page[unit + 1] =
      _first_page[unit] +
      static_cast<uint32_t>(view[unit].docs_count() >> kPageShift) + 1;
  }
  _pages = std::make_unique<std::atomic<Bitset*>[]>(_first_page[_units]);
}

RowPositionDeduplicator::~RowPositionDeduplicator() {
  for (uint32_t page = 0; page != _first_page[_units]; ++page) {
    if (auto* bits = _pages[page].load(std::memory_order_relaxed)) {
      irs::DocumentMaskBuilder::BitsetFree{}(bits);
    }
  }
}

uint64_t* RowPositionDeduplicator::WordsOf(uint32_t unit, irs::doc_id_t doc) {
  SDB_ASSERT(unit < _units);
  const auto page = _first_page[unit] + (doc >> kPageShift);
  SDB_ASSERT(page < _first_page[unit + 1]);
  auto& slot = _pages[page];
  auto* bits = slot.load(std::memory_order_acquire);
  if (!bits) {
    auto fresh = irs::DocumentMaskBuilder::NewBitset();
    if (slot.compare_exchange_strong(bits, fresh.get(),
                                     std::memory_order_acq_rel,
                                     std::memory_order_acquire)) {
      bits = fresh.release();
    }
  }
  return irs::DocumentMaskBuilder::Words(*bits);
}

duckdb::idx_t RowPositionDeduplicator::KeepFirst(duckdb::DataChunk& chunk,
                                                 duckdb::idx_t rowid) {
  const auto count = chunk.size();
  auto places = chunk.data[rowid].Values<int64_t>();
  duckdb::SelectionVector keep;
  duckdb::idx_t kept = 0;
  auto cached = std::numeric_limits<uint64_t>::max();
  uint64_t* words = nullptr;
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
    const auto index = RowPositionIndex(first);
    if (const auto key = uint64_t{index} << kPageShift | doc >> kPageShift;
        key != cached) {
      words = WordsOf(index, doc);
      cached = key;
    }
    const auto seen =
      mask & std::atomic_ref{words[(doc & kPageLow) / kWordBits]}.fetch_or(
               mask, std::memory_order_relaxed);
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

irs::DocRemovalBuilder RowPositionDeduplicator::Positions() && {
  irs::DocRemovalBuilder positions;
  for (uint32_t unit = 0; unit != _units; ++unit) {
    const auto first = _first_page[unit];
    for (auto page = first; page != _first_page[unit + 1]; ++page) {
      if (irs::DocumentMaskBuilder::BitsetPtr bits{
            _pages[page].exchange(nullptr, std::memory_order_relaxed)}) {
        positions.Group(unit).AppendBitset(static_cast<uint16_t>(page - first),
                                           std::move(bits));
      }
    }
  }
  return positions;
}

}  // namespace sdb::connector
