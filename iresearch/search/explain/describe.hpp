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

#pragma once

#include <cstddef>

#include "iresearch/search/detail/collectors.hpp"
#include "iresearch/search/detail/multiterm_collector.hpp"
#include "iresearch/search/explain/stats.hpp"
#include "iresearch/utils/down_cast.hpp"

namespace irs::explain {

inline SlotStats FieldStats(const FieldPrepareCollector& collector,
                            field_id field) {
  const auto total = collector.FieldTotal();
  return {
    .field = field,
    .docs_with_field = total.docs_with_field,
    .total_term_freq = total.total_term_freq,
  };
}

inline TermStats TermOf(bytes_view term, const TermCollector& counts) {
  return {
    .term = bstring{term},
    .docs_with_term = counts.docs_with_term,
    .total_term_freq = counts.total_term_freq,
  };
}

inline void DescribeTerm(const ByTermsCollector& collector, size_t i,
                         field_id field, bytes_view term, StatsIndex& out) {
  auto stats = FieldStats(collector, field);
  stats.terms.push_back(TermOf(term, collector.TermTotal(i)));
  out.Add(collector.Record(i).stats, std::move(stats));
}

inline void DescribeSlot(const PrepareCollector* collector, field_id field,
                         StatsIndex& out) {
  if (collector != nullptr) {
    out.Add(collector->Record().stats, {.field = field});
  }
}

inline void DescribeMultiTerm(const PrepareCollector* collector, field_id field,
                              StatsIndex& out) {
  if (collector == nullptr) {
    return;
  }
  if (!ScoresPerDoc(collector->GetScorer())) {
    DescribeSlot(collector, field, out);
    return;
  }
  const auto& terms = utils::downCast<MultiTermCollector>(*collector);
  const auto base = FieldStats(terms, field);
  terms.VisitTerms(
    [&](bytes_view term, const TermCollector& counts, const byte_type* slot) {
      auto stats = base;
      stats.terms.push_back(TermOf(term, counts));
      out.Add(slot, std::move(stats));
    });
}

inline void DescribeBlended(const PrepareCollector* collector, field_id field,
                            StatsIndex& out) {
  if (collector == nullptr) {
    return;
  }
  const auto& terms = utils::downCast<BlendedTermsCollector>(*collector);
  auto stats = FieldStats(terms, field);
  stats.terms.push_back(TermOf({}, terms.Blended()));
  out.Add(terms.Record().stats, std::move(stats));
}

}  // namespace irs::explain
