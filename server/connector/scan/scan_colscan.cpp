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
#include <iresearch/index/docs_mask/docs_mask.hpp>
#include <iresearch/index/index_reader.hpp>
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
    std::vector<MultiColumnCheck> multi;
    BuildMultiColumnChecks(g, *col_reader, multi);
    slot = std::make_unique<FullScanner>(
      *col_reader, g.cs_projections, l.seg_cls.active, multi, g.client_context,
      l.filter_states, g.Bind().share_payloads);
  }
  l.scanner = slot.get();
}

duckdb::idx_t SampleRows(const ScanGlobalState& g, ColScanLocalState& l,
                         duckdb::idx_t take, duckdb::idx_t live) {
  const bool every_row = live == take;
  if (every_row) {
    l.live_sel.Initialize(l.live_sel_data);
  }
  duckdb::idx_t kept = 0;
  for (duckdb::idx_t i = 0; i != live; ++i) {
    const auto row = every_row ? i : l.live_sel.get_index(i);
    if (g.sampling.KeepsRow(l.doc_cursor + row)) {
      l.live_sel.set_index(kept++, row);
    }
  }
  return kept;
}

duckdb::idx_t EmitFromUnit(ScanGlobalState& g, ColScanLocalState& l,
                           duckdb::DataChunk& output) {
  auto& scanner = *l.scanner;
  const bool sampled = g.sampling.do_system_sample;
  const bool sample_rows =
    sampled && !g.sampling.is_percentage && g.sampling.sample_rate < 1;
  while (l.doc_cursor < l.doc_end) {
    const auto dead_end = scanner.DeadUntil(l.doc_cursor);
    if (dead_end > l.doc_cursor) {
      l.doc_cursor = std::min<uint64_t>(dead_end, l.doc_end);
      continue;
    }
    if (l.doc_cursor >= l.ahead_check) [[unlikely]] {
      MoveReadAhead(g, l, l.doc_cursor);
    }
    const auto take = static_cast<duckdb::idx_t>(
      std::min<uint64_t>(STANDARD_VECTOR_SIZE, l.doc_end - l.doc_cursor));
    if (sampled && g.sampling.is_percentage &&
        !g.sampling.KeepsVector(SampleRandom(g, l))) {
      l.doc_cursor += take;
      continue;
    }
    const auto first =
      irs::doc_limits::min() + static_cast<irs::doc_id_t>(l.doc_cursor);
    const duckdb::SelectionVector* live = nullptr;
    duckdb::idx_t live_count = take;
    if (l.has_mask) {
      l.live_sel.Initialize(l.live_sel_data);
      live_count =
        l.mask.FillLive(first, static_cast<uint32_t>(take), l.live_sel.data());
      live = &l.live_sel;
    }
    if (sample_rows) {
      live_count = SampleRows(g, l, take, live_count);
      if (live_count == 0) {
        l.doc_cursor += take;
        continue;
      }
      live = &l.live_sel;
    }
    const auto produced =
      scanner.Filter(l.doc_cursor, take, output, live, live_count);
    if (produced == 0) {
      l.doc_cursor += take;
      output.Reset();
      continue;
    }
    if (l.doc_cursor >= l.projected_until) [[unlikely]] {
      DeclareProjected(g, l, l.doc_cursor);
    }
    scanner.Project(l.doc_cursor, take, produced, output);
    l.doc_cursor += take;
    WriteRowPositions(g, l.unit.seg, first, scanner.Selection(), produced,
                      output);
    WriteVirtualColumns(g, produced, nullptr, output);
    return produced;
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
      FinishUnit(g, l);
    }
    if (!NextLiveUnit(g, l)) {
      break;
    }
    const auto& sub = (*g.reader)[l.unit.seg];
    const auto rows = g.RowsOf(l.unit);
    l.doc_cursor = rows.begin;
    l.doc_end = rows.end;
    l.has_mask = sub.docs_mask() != nullptr;
    l.mask = irs::MakeGenericDocsMask(sub);
    OpenScanner(g, l);
  }
  output.SetChildCardinality(0);
}

}  // namespace sdb::connector
