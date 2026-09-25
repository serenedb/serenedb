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

#include "connector/scan/col_filter_verify.h"

#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "connector/scan/scan_state.h"

namespace sdb::connector {

void ColFilterVerify::Begin(const irs::SubReader& seg,
                            std::span<const irs::ColFilterSpec> active,
                            duckdb::ClientContext& context,
                            irs::ColFilterStateCache& states) {
  _chain.Clear();
  _score_filter = nullptr;
  _score_state = nullptr;
  bool any_col = false;
  for (const auto& spec : active) {
    if (spec.is_score) {
      _score_filter = spec.filter;
      _score_state = &states.State(context, *spec.filter);
    } else {
      any_col = true;
    }
  }
  if (!any_col) {
    _ctx.reset();
    return;
  }
  const auto* col_reader = seg.GetColReader();
  SDB_ENSURE(col_reader != nullptr,
             "`.col` table filter requires a columnstore segment");
  if (_ctx) {
    _ctx->Reset(*col_reader);
  } else {
    _ctx = std::make_unique<irs::ReadContext>(*col_reader);
  }
  _chain.Bind(*col_reader, *_ctx, active, context, states);
  _chain.FinishBind();
}

uint32_t ColFilterVerify::Narrow(irs::doc_id_t* docs, irs::score_t* scores,
                                 uint32_t n) {
  if (n == 0) {
    return 0;
  }
  SDB_ASSERT(_score_filter == nullptr || scores != nullptr);
  duckdb::idx_t left = n;
  if (_score_filter != nullptr) {
    left = irs::ColFilterChain::FilterDocsScores(*_score_filter, *_score_state,
                                                 docs, scores, left);
  }
  return static_cast<uint32_t>(_chain.FilterDocs(docs, scores, left));
}

uint32_t ColFilterVerify::Narrow(irs::doc_id_t base, uint64_t* mask,
                                 irs::score_t* scores, uint32_t words) {
  SDB_ASSERT(_score_filter == nullptr || scores != nullptr);
  if (_score_filter != nullptr) {
    irs::ColFilterChain::FilterMaskScores(*_score_filter, *_score_state, mask,
                                          scores, words);
  }
  return static_cast<uint32_t>(_chain.FilterMask(base, mask, words));
}

uint64_t ColFilterVerify::CountAndClear(irs::doc_id_t base, uint64_t* mask,
                                        uint32_t words) {
  SDB_ASSERT(_score_filter == nullptr);
  return static_cast<uint64_t>(_chain.CountMask(base, mask, words));
}

void ClassifySegmentColFilters(const irs::SubReader& seg, ScanGlobalState& g,
                               irs::ColFilterStateCache& states,
                               irs::ColFilterClassification& out) {
  out.segment_dead = false;
  out.active.clear();
  if (g.col_filters.empty()) {
    return;
  }
  const auto* col_reader = seg.GetColReader();
  for (const auto& cf : g.col_filters) {
    irs::ColFilterSpec spec{
      .field = cf.field,
      .filter = cf.filter,
      .is_score = cf.is_score,
      .is_dynamic = cf.is_dynamic,
      .zonemap_only = cf.zonemap_only,
      .null_check = cf.null_check,
      .not_null = cf.not_null.get(),
      .extract_path = cf.extract_path,
      .extract_type = &cf.type,
    };
    if (spec.is_score) {
      out.active.push_back(spec);
      continue;
    }
    const auto* column =
      col_reader != nullptr ? col_reader->Column(spec.field) : nullptr;
    const auto* reader =
      column != nullptr && !cf.extract_path.empty()
        ? irs::DirectExtractLeaf(*column, cf.extract_path, cf.type)
        : column;
    if (column != nullptr && reader == nullptr) {
      out.active.push_back(spec);
      continue;
    }
    if (reader == nullptr) {
      duckdb::Vector null_row{duckdb::Value{cf.type}, duckdb::count_t{1}};
      duckdb::SelectionVector sel;
      duckdb::idx_t approved = 1;
      duckdb::ColumnSegment::FilterSelection(
        sel, null_row, states.State(*g.client_context, *spec.filter), 1,
        approved);
      if (approved == 0) {
        out.segment_dead = true;
        out.active.clear();
        return;
      }
      continue;
    }
    const auto& stats = reader->MergedStatistics();
    const auto verdict =
      spec.filter->Cast<duckdb::ExpressionFilter>().CheckStatistics(
        stats, states.State(*g.client_context, *spec.filter));
    switch (verdict) {
      case duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE:
      case duckdb::FilterPropagateResult::FILTER_FALSE_OR_NULL:
        out.segment_dead = true;
        out.active.clear();
        return;
      case duckdb::FilterPropagateResult::FILTER_ALWAYS_TRUE:
        if (spec.zonemap_only) {
          out.active.push_back(spec);
        }
        break;
      case duckdb::FilterPropagateResult::FILTER_TRUE_OR_NULL:
        if (!spec.zonemap_only && spec.not_null != nullptr) {
          spec.filter = spec.not_null;
          spec.null_check = irs::NullCheckKind::IsNotNull;
        }
        out.active.push_back(spec);
        break;
      case duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE:
        out.active.push_back(spec);
        break;
    }
  }
}

void ScanLocalState::Classify(ScanGlobalState& g, uint32_t seg) {
  if (classified_seg == seg) {
    return;
  }
  ClassifySegmentColFilters((*g.reader)[seg], g, filter_states, seg_cls);
  classified_seg = seg;
}

irs::detail::TableFilter* BeginVerify(ColFilterVerify& verify,
                                      const irs::SubReader& seg,
                                      ScanGlobalState& g, ScanLocalState& l) {
  verify.Begin(seg, l.seg_cls.active, *g.client_context, l.filter_states);
  return verify.Empty() ? nullptr : &verify;
}

}  // namespace sdb::connector
