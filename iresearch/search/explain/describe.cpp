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

#include <utility>

#include "iresearch/search/detail/collectors.hpp"
#include "iresearch/search/detail/multiterm_collector.hpp"

namespace irs {
namespace {

explain::SlotStats FieldTotalOf(const FieldPrepareCollector& collector,
                                field_id field) {
  const auto total = collector.FieldTotal();
  return {
    .field = field,
    .docs_with_field = total.docs_with_field,
    .total_term_freq = total.total_term_freq,
  };
}

explain::TermStats TermOf(bytes_view term, const TermCollector& counts) {
  return {
    .term = bstring{term},
    .docs_with_term = counts.docs_with_term,
    .total_term_freq = counts.total_term_freq,
  };
}

}  // namespace

void ByTermsCollector::Describe(field_id field, TermAt term,
                                explain::StatsIndex& out) const {
  const auto base = FieldTotalOf(*this, field);
  for (size_t i = 0; i != _size; ++i) {
    auto stats = base;
    stats.terms.push_back(TermOf(term(i), TermTotal(i)));
    out.insert_or_assign(Record(i).stats, std::move(stats));
  }
}

void SlotsCollector::Describe(field_id field, TermAt term,
                              explain::StatsIndex& out) const {
  auto stats = FieldTotalOf(*this, field);
  for (size_t i = 0; i != _counters.Terms(); ++i) {
    const auto counts = TermTotal(i);
    if (counts.docs_with_term != 0) {
      stats.terms.push_back(TermOf(term(i), counts));
    }
  }
  out.insert_or_assign(Record().stats, std::move(stats));
}

void ExpandedSlotsCollector::Describe(field_id field, TermAt term,
                                      explain::StatsIndex& out) const {
  SlotsCollector::Describe(field, term, out);
  auto& terms = out[Record().stats].terms;
  VisitExpanded([&](bytes_view expanded, const TermCollector& counts) {
    terms.push_back(TermOf(expanded, counts));
  });
}

void PrepareCollector::Describe(field_id field, TermAt,
                                explain::StatsIndex& out) const {
  out.insert_or_assign(Record().stats, explain::SlotStats{.field = field});
}

void MultiTermCollector::Describe(field_id field, TermAt,
                                  explain::StatsIndex& out) const {
  const auto base = FieldTotalOf(*this, field);
  VisitTerms(
    [&](bytes_view term, const TermCollector& counts, const byte_type* slot) {
      auto stats = base;
      stats.terms.push_back(TermOf(term, counts));
      out.insert_or_assign(slot, std::move(stats));
    });
}

void BlendedTermsCollector::Describe(field_id field, TermAt,
                                     explain::StatsIndex& out) const {
  auto stats = FieldTotalOf(*this, field);
  stats.blended = true;
  stats.terms.push_back(TermOf({}, Blended()));
  out.insert_or_assign(Record().stats, std::move(stats));
}

}  // namespace irs
