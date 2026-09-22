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

#include <algorithm>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/detail/window.hpp>
#include <iresearch/utils/bit_utils.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "connector/full_scanner.h"
#include "connector/scan/scan_state.h"

namespace sdb::connector {
namespace {

void OpenScanner(ScanGlobalState& g, ColScanLocalState& l) {
  const auto& reader = *g.reader;
  if (l.full_scanners.size() < reader.size()) {
    l.full_scanners.resize(reader.size());
  }
  auto& slot = l.full_scanners[l.unit.seg];
  if (slot && l.doc_cursor < slot->ScannedEnd()) {
    slot.reset();
  }
  if (!slot) {
    const auto* col_reader = reader[l.unit.seg].GetColReader();
    SDB_ENSURE(col_reader != nullptr,
               "bulk cs scan: segment has no columnstore reader");
    slot = std::make_unique<FullScanner>(*col_reader, g.cs_projections,
                                         l.seg_cls.active, g.client_context,
                                         l.filter_states);
  }
  l.scanner = slot.get();
}

constexpr uint32_t kMaskBits = 64;

uint64_t DeadWord(const uint64_t* words, uint32_t count, int64_t at,
                  uint32_t len) noexcept {
  const auto word = irs::detail::WordAt(words, count, at);
  return len == kMaskBits ? word : word & ((uint64_t{1} << len) - 1);
}

duckdb::idx_t LiveRows(const ColScanLocalState& l, irs::doc_id_t first,
                       duckdb::idx_t take, duckdb::SelectionVector& sel) {
  const auto base = static_cast<int64_t>(first - irs::doc_limits::min());
  auto* const out = sel.data();
  duckdb::idx_t live = 0;
  for (duckdb::idx_t at = 0; at < take; at += kMaskBits) {
    const auto len =
      static_cast<uint32_t>(std::min<duckdb::idx_t>(kMaskBits, take - at));
    const auto dead = DeadWord(l.mask_words, l.mask_word_count,
                               base + static_cast<int64_t>(at), len);
    const auto alive =
      ~dead & (len == kMaskBits ? ~uint64_t{0} : ((uint64_t{1} << len) - 1));
    live = static_cast<duckdb::idx_t>(
      irs::MaterializeWord(static_cast<uint32_t>(at), alive, out + live) - out);
  }
  return live;
}

bool HasDead(const ColScanLocalState& l, irs::doc_id_t first,
             duckdb::idx_t take) noexcept {
  const auto base = static_cast<int64_t>(first - irs::doc_limits::min());
  for (duckdb::idx_t at = 0; at < take; at += kMaskBits) {
    const auto len =
      static_cast<uint32_t>(std::min<duckdb::idx_t>(kMaskBits, take - at));
    if (DeadWord(l.mask_words, l.mask_word_count,
                 base + static_cast<int64_t>(at), len) != 0) {
      return true;
    }
  }
  return false;
}

duckdb::idx_t EmitFromUnit(ScanGlobalState& g, ColScanLocalState& l,
                           duckdb::DataChunk& output) {
  auto& scanner = *l.scanner;
  while (l.doc_cursor < l.doc_end) {
    const auto dead_end = scanner.DeadUntil(l.doc_cursor);
    if (dead_end > l.doc_cursor) {
      l.doc_cursor = std::min<uint64_t>(dead_end, l.doc_end);
      continue;
    }
    const auto take = static_cast<duckdb::idx_t>(
      std::min<uint64_t>(STANDARD_VECTOR_SIZE, l.doc_end - l.doc_cursor));
    const auto first =
      irs::doc_limits::min() + static_cast<irs::doc_id_t>(l.doc_cursor);
    duckdb::idx_t produced;
    if (l.mask_words != nullptr && HasDead(l, first, take)) {
      l.live_sel.Initialize(l.live_sel_data);
      const auto live = LiveRows(l, first, take, l.live_sel);
      produced = scanner.Scan(l.doc_cursor, take, output, &l.live_sel, live);
    } else {
      produced = scanner.Scan(l.doc_cursor, take, output);
    }
    l.doc_cursor += take;
    if (produced != 0) {
      AccountAndWriteVirtualColumns(g, produced, nullptr, output);
      return produced;
    }
    output.Reset();
  }
  return 0;
}

}  // namespace

void RunColScan(duckdb::ClientContext&, duckdb::TableFunctionInput&,
                ScanGlobalState& g, ColScanLocalState& l,
                duckdb::DataChunk& output) {
  if (!l.live_sel_data) {
    // MaterializeWord writes up to 8 slots past the produced count.
    l.live_sel_data =
      duckdb::make_buffer<duckdb::SelectionData>(STANDARD_VECTOR_SIZE + 8);
  }
  for (;;) {
    if (l.has_unit) {
      const auto added = EmitFromUnit(g, l, output);
      if (added != 0) {
        output.SetChildCardinality(added);
        return;
      }
      if (FinishUnit(g, l)) {
        FinishSegments(g, 1);
      }
    }
    if (!NextLiveUnit(g, l)) {
      break;
    }
    const auto& sub = (*g.reader)[l.unit.seg];
    const auto docs = std::min<uint64_t>(
      sub.docs_count(), sub.Meta().uncommitted_begin - irs::doc_limits::min());
    if (l.unit.whole) {
      l.doc_cursor = 0;
      l.doc_end = docs;
    } else {
      l.doc_cursor =
        std::min<uint64_t>(docs, uint64_t{l.unit.rg_begin} * g.rg_size);
      l.doc_end = std::min<uint64_t>(docs, uint64_t{l.unit.rg_end} * g.rg_size);
    }
    const auto* mask = sub.docs_mask();
    l.mask_words = mask != nullptr ? mask->Words() : nullptr;
    l.mask_word_count =
      mask != nullptr ? static_cast<uint32_t>(mask->WordCount()) : 0;
    OpenScanner(g, l);
  }
  output.SetChildCardinality(0);
}

}  // namespace sdb::connector
