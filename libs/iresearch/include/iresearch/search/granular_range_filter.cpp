////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "granular_range_filter.hpp"

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/limited_sample_selector.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/range_filter.hpp"
#include "iresearch/search/term_filter.hpp"

namespace irs {
namespace {

enum class RangeKind {
  Term,
  Empty,
  Range,
};

RangeKind Classify(const ByGranularRangeOptions& options) noexcept {
  const auto& rng = options.range;
  if (!rng.min.empty() && !rng.max.empty() &&
      rng.min.front() == rng.max.front()) {
    if (rng.min_type == rng.max_type && rng.min_type == BoundType::Inclusive) {
      return RangeKind::Term;
    }
    return RangeKind::Empty;
  }
  return RangeKind::Range;
}

bytes_view MaskGranularity(bytes_view term, size_t prefix_size) noexcept {
  return term.size() > prefix_size ? bytes_view{term.data(), prefix_size}
                                   : term;
}

bytes_view MaskValue(bytes_view term, size_t prefix_size) noexcept {
  if (IsNull(term)) {
    return term;
  }

  return term.size() > prefix_size
           ? bytes_view{term.data() + prefix_size, term.size() - prefix_size}
           : bytes_view{};
}

template<typename Visitor, typename Comparer>
void CollectTerms(const SubReader& segment, const TermReader& field,
                  SeekTermIterator& terms, Visitor& visitor,
                  const Comparer& cmp) {
  visitor.Prepare(segment, field, terms);

  do {
    if (!cmp(terms)) {
      break;
    }

    visitor.Visit(kNoBoost);
  } while (terms.next());
}

template<typename Visitor>
void CollectTermsBetween(const SubReader& segment, const TermReader& field,
                         SeekTermIterator& terms, size_t prefix_size,
                         bytes_view begin_term, bytes_view end_term,
                         bool include_begin_term, bool include_end_term,
                         Visitor& visitor) {
  bstring tmp;
  bytes_view masked_begin_level;

  if (!IsNull(begin_term)) {
    const auto res = terms.seek_ge(begin_term);

    if (SeekResult::End == res) {
      return;
    }

    if (SeekResult::Found == res) {
      if (!include_begin_term) {
        if (!terms.next()) {
          return;
        }
      } else if (!include_end_term && !IsNull(end_term) &&
                 !(MaskValue(begin_term, prefix_size) <
                   MaskValue(end_term, prefix_size))) {
        return;
      }
    }

    masked_begin_level = MaskGranularity(begin_term, prefix_size);
  } else if (!include_begin_term && !terms.next()) {
    return;
  } else {
    tmp = static_cast<bstring>(terms.value());
    masked_begin_level = MaskGranularity(tmp, prefix_size);
  }

  const auto& masked_end_term = MaskValue(end_term, prefix_size);

  CollectTerms(
    segment, field, terms, visitor,
    [&prefix_size, masked_begin_level, masked_end_term,
     include_end_term](const TermIterator& itr) -> bool {
      const auto& value = itr.value();
      const auto masked_current_level = MaskGranularity(value, prefix_size);
      const auto masked_current_term = MaskValue(value, prefix_size);

      return masked_current_level == masked_begin_level &&
             (IsNull(masked_end_term) ||
              (include_end_term && masked_current_term <= masked_end_term) ||
              (!include_end_term && masked_current_term < masked_end_term));
    });
}

template<typename Visitor>
void CollectTermsFrom(const SubReader& segment, const TermReader& field,
                      SeekTermIterator& terms, size_t prefix_size,
                      const ByGranularRange::options_type::terms& min_term,
                      bool min_term_inclusive, Visitor& visitor) {
  auto min_term_itr = min_term.rbegin();

  if (min_term_itr == min_term.rend()) {
    CollectTermsBetween(segment, field, terms, prefix_size, bytes_view{},
                        bytes_view{}, true, true, visitor);

    return;
  }

  auto* exact_min_term = &(*min_term.begin());

  CollectTermsBetween(
    segment, field, terms, prefix_size, *min_term_itr, bytes_view{},
    min_term_inclusive && exact_min_term == &(*min_term_itr), true, visitor);

  for (auto current_min_term_itr = min_term_itr, end = min_term.rend();
       ++current_min_term_itr != end; ++min_term_itr) {
    auto res = terms.seek_ge(*min_term_itr);

    if (SeekResult::End == res) {
      continue;
    }

    auto end_term = (SeekResult::NotFound == res ||
                     (SeekResult::Found == res && terms.next())) &&
                        MaskGranularity(terms.value(), prefix_size) ==
                          MaskGranularity(*min_term_itr, prefix_size)
                      ? terms.value()
                      : bytes_view{};
    bstring end_term_copy;
    auto is_most_granular_term = exact_min_term == &(*current_min_term_itr);

    if (!IsNull(end_term)) {
      end_term_copy.assign(end_term.data(), end_term.size());
      end_term = bytes_view(end_term_copy);
    }

    CollectTermsBetween(segment, field, terms, prefix_size,
                        *current_min_term_itr, end_term,
                        min_term_inclusive && is_most_granular_term,
                        IsNull(end_term) && is_most_granular_term, visitor);
  }
}

template<typename Visitor>
void CollectTermsUntil(const SubReader& segment, const TermReader& field,
                       SeekTermIterator& terms, size_t prefix_size,
                       const ByGranularRange::options_type::terms& max_term,
                       bool max_term_inclusive, Visitor& visitor) {
  auto max_term_itr = max_term.rbegin();

  if (max_term_itr == max_term.rend()) {
    CollectTermsBetween(segment, field, terms, prefix_size, bytes_view{},
                        bytes_view{}, true, true, visitor);

    return;
  }

  {
    const auto& current_level = MaskGranularity(terms.value(), prefix_size);

    for (auto end = max_term.rend();
         current_level != MaskGranularity(*max_term_itr, prefix_size);) {
      if (++max_term_itr == end) {
        return;
      }
    }
  }

  auto* exact_max_term = &(*max_term.begin());

  CollectTermsBetween(
    segment, field, terms, prefix_size, bytes_view{}, *max_term_itr, true,
    max_term_inclusive && exact_max_term == &(*max_term_itr), visitor);

  bstring tmp_term;

  for (auto current_max_term_itr = max_term_itr, end = max_term.rend();
       ++current_max_term_itr != end; ++max_term_itr) {
    tmp_term = *max_term_itr;

    if (max_term_itr->size() > prefix_size) {
      tmp_term.replace(0, prefix_size, *current_max_term_itr, 0, prefix_size);
    }

    CollectTermsBetween(
      segment, field, terms, prefix_size, tmp_term, *current_max_term_itr, true,
      max_term_inclusive && exact_max_term == &(*current_max_term_itr),
      visitor);
  }
}

template<typename Visitor>
void CollectTermsWithin(const SubReader& segment, const TermReader& field,
                        SeekTermIterator& terms, size_t prefix_size,
                        const ByGranularRange::options_type::terms& min_term,
                        const ByGranularRange::options_type::terms& max_term,
                        bool min_term_inclusive, bool max_term_inclusive,
                        Visitor& visitor) {
  auto min_term_itr = min_term.rbegin();

  if (min_term_itr == min_term.rend()) {
    CollectTermsUntil(segment, field, terms, prefix_size, max_term,
                      max_term_inclusive, visitor);

    return;
  }

  if (min_term_inclusive && !min_term.empty()) {
    auto& exact_min_term = min_term.front();
    bool single_term = !max_term.empty() && exact_min_term == max_term.front();

    if ((!single_term || max_term_inclusive) &&
        exact_min_term > max_term.front()) {
      return;
    }

    if (single_term && min_term_inclusive != max_term_inclusive) {
      min_term_inclusive = false;
    }
  }

  auto* exact_min_term = min_term.empty() ? nullptr : &(min_term.front());
  auto max_term_itr = max_term.rbegin();

  if (!max_term.empty()) {
    auto min_end = min_term.rend();
    auto max_end = max_term.rend();

    for (;;) {
      auto& min_term_value = *min_term_itr;
      auto& max_term_value = *max_term_itr;
      const auto& min_term_level = MaskGranularity(min_term_value, prefix_size);
      const auto& max_term_level = MaskGranularity(max_term_value, prefix_size);

      if (min_term_level == max_term_level) {
        if (min_term_value != max_term_value ||
            exact_min_term == &min_term_value) {
          break;
        }

        ++min_term_itr;
        ++max_term_itr;
      } else if (min_term_level > max_term_level && ++min_term_itr == min_end) {
        return;
      } else if (min_term_level < max_term_level && ++max_term_itr == max_end) {
        return;
      }
    }
  }

  CollectTermsBetween(
    segment, field, terms, prefix_size, *min_term_itr,
    max_term.empty() ? bytes_view{} : bytes_view(*max_term_itr),
    min_term_inclusive && exact_min_term == &(*min_term_itr), false, visitor);

  for (auto current_min_term_itr = min_term_itr, end = min_term.rend();
       ++current_min_term_itr != end; ++min_term_itr) {
    auto res = terms.seek_ge(*min_term_itr);

    if (SeekResult::End == res) {
      continue;
    }

    auto end_term = (SeekResult::NotFound == res ||
                     (SeekResult::Found == res && terms.next())) &&
                        MaskGranularity(terms.value(), prefix_size) ==
                          MaskGranularity(*min_term_itr, prefix_size)
                      ? terms.value()
                      : bytes_view{};
    bstring end_term_copy;

    if (!IsNull(end_term)) {
      end_term_copy.assign(end_term.data(), end_term.size());
      end_term = bytes_view(end_term_copy);
    }

    CollectTermsBetween(
      segment, field, terms, prefix_size, *current_min_term_itr, end_term,
      min_term_inclusive && exact_min_term == &(*current_min_term_itr), false,
      visitor);
  }

  if (!max_term.empty() && terms.seek(*max_term_itr)) {
    CollectTermsUntil(segment, field, terms, prefix_size, max_term,
                      max_term_inclusive, visitor);
  }
}

template<typename Visitor>
void VisitImpl(const SubReader& segment, const TermReader& reader,
               const ByGranularRange::options_type& options, Visitor& visitor) {
  auto terms = reader.iterator();

  if (!terms) [[unlikely]] {
    return;
  }
  if (!terms->next()) {
    return;
  }

  const size_t prefix_size = options.is_granular;
  const auto& rng = options.range;

  SDB_ASSERT(!rng.min.empty() || BoundType::Unbounded == rng.min_type);
  SDB_ASSERT(!rng.max.empty() || BoundType::Unbounded == rng.max_type);

  if (rng.min.empty()) {
    if (rng.max.empty()) {
      static const ByGranularRange::options_type::terms kEmpty;
      CollectTermsFrom(segment, reader, *terms, prefix_size, kEmpty, true,
                       visitor);
      return;
    }

    auto& max_term = *rng.max.rbegin();

    const bytes_view smallest_term{max_term.c_str(),
                                   std::min(max_term.size(), prefix_size)};

    if (SeekResult::End != terms->seek_ge(smallest_term)) {
      CollectTermsUntil(segment, reader, *terms, prefix_size, rng.max,
                        BoundType::Inclusive == rng.max_type, visitor);
    }

    return;
  }

  if (rng.max.empty()) {
    CollectTermsFrom(segment, reader, *terms, prefix_size, rng.min,
                     BoundType::Inclusive == rng.min_type, visitor);
    return;
  }

  CollectTermsWithin(segment, reader, *terms, prefix_size, rng.min, rng.max,
                     BoundType::Inclusive == rng.min_type,
                     BoundType::Inclusive == rng.max_type, visitor);
}

}  // namespace

QueryBuilder::ptr ByGranularRange::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  auto sub_ctx = ctx;
  sub_ctx.boost *= GetBoost();
  return PrepareSegment(segment, sub_ctx, field_id(), options());
}

QueryBuilder::ptr ByGranularRange::PrepareSegment(const SubReader& segment,
                                                  const PrepareContext& ctx,
                                                  const irs::field_id field,
                                                  const options_type& options) {
  switch (Classify(options)) {
    case RangeKind::Term:
      return ByTerm::PrepareSegment(segment, ctx, field,
                                    options.range.min.front());
    case RangeKind::Empty:
      return QueryBuilder::Empty();
    case RangeKind::Range:
      break;
  }

  const auto* reader = segment.field(field);
  if (!reader) {
    return QueryBuilder::Empty();
  }

  auto query = memory::make_tracked<MultiTermQuery>(
    ctx.memory, segment, ctx.memory, ctx.boost, ScoreMergeType::Sum);
  auto* collector =
    ctx.collector
      ? &sdb::basics::downCast<LimitedTermsCollector>(*ctx.collector)
      : nullptr;
  if (collector) {
    collector->Field(ctx.thread).Collect(*reader);
    if (collector->Limited(ctx.thread).Samples()) {
      query->Pin();
    }
  }
  SampledMultiTermVisitor mtv{
    collector ? &collector->Limited(ctx.thread) : nullptr, query->State()};
  VisitImpl(segment, *reader, options, mtv);
  return MultiTermQuery::Finish(std::move(query), ctx);
}

PrepareCollector::ptr ByGranularRange::MakeCollectorImpl(
  const Scorer* scorer, StatsArena& stats, uint32_t threads) const {
  if (Classify(options()) == RangeKind::Term) {
    return std::make_unique<ByTermsCollector>(scorer, 1, stats, threads);
  }
  return std::make_unique<LimitedTermsCollector>(
    scorer, options().scored_terms_limit, stats, threads);
}

}  // namespace irs
