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
#include <iresearch/search/fill/docs_mask.hpp>
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
    duckdb::idx_t produced;
    if (l.has_mask) {
      const auto first =
        irs::doc_limits::min() + static_cast<irs::doc_id_t>(l.doc_cursor);
      l.live_sel.Initialize(l.live_sel_data);
      const auto live =
        l.mask.FillLive(first, static_cast<uint32_t>(take), l.live_sel.data());
      produced = live == take ? scanner.Scan(l.doc_cursor, take, output)
                              : scanner.Scan(l.doc_cursor, take, output,
                                             &l.live_sel, live);
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
    l.live_sel_data = duckdb::make_buffer<duckdb::SelectionData>(
      STANDARD_VECTOR_SIZE + irs::doc_limits::kDocsSlack);
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
    const auto docs = irs::VisibleCount(sub.Meta());
    if (l.unit.whole) {
      l.doc_cursor = 0;
      l.doc_end = docs;
    } else {
      l.doc_cursor =
        std::min<uint64_t>(docs, uint64_t{l.unit.rg_begin} * g.rg_size);
      l.doc_end = std::min<uint64_t>(docs, uint64_t{l.unit.rg_end} * g.rg_size);
    }
    l.has_mask = sub.docs_mask() != nullptr;
    l.mask = irs::fill::DocsMask{sub};
    OpenScanner(g, l);
  }
  output.SetChildCardinality(0);
}

}  // namespace sdb::connector
