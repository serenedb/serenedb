////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "phrase_query.hpp"

#include "iresearch/index/field_meta.hpp"
#include "iresearch/search/phrase_filter.hpp"

namespace irs {
namespace {

// Get index features required for offsets
constexpr IndexFeatures kRequireOffs =
  FixedPhraseQuery::kRequiredFeatures | IndexFeatures::Offs;

template<bool OneShot, bool HasFreq, bool HasIntervals>
using FixedPhraseIterator =
  PhraseIterator<Conjunction<ScoreAdapter<>, NoopAggregator>,
                 FixedPhraseFrequency<OneShot, HasFreq, HasIntervals>>;

// FIXME add proper handling of overlapped case
template<typename Adapter, bool VolatileBoost, bool OneShot, bool HasFreq,
         bool HasIntervals>
using VariadicPhraseIterator =
  PhraseIterator<Conjunction<ScoreAdapter<>, NoopAggregator>,
                 VariadicPhraseFrequency<Adapter, VolatileBoost, OneShot,
                                         HasFreq, HasIntervals>>;

}  // namespace

DocIterator::ptr FixedPhraseQuery::execute(const ExecutionContext& ctx) const {
  auto& rdr = ctx.segment;
  auto& ord = ctx.scorers;

  // get phrase state for the specified reader
  auto phrase_state = states.find(rdr);

  if (!phrase_state) {
    // invalid state
    return DocIterator::empty();
  }

  auto* reader = phrase_state->reader;
  SDB_ASSERT(reader);

  if (kRequiredFeatures !=
      (reader->meta().index_features & kRequiredFeatures)) {
    return DocIterator::empty();
  }

  // get index features required for query & order
  const IndexFeatures features = ord.features() | kRequiredFeatures;

  ScoreAdapters itrs;
  itrs.reserve(phrase_state->terms.size());

  std::vector<FixedTermPosition> positions;
  positions.reserve(phrase_state->terms.size());

  auto position = std::begin(this->positions);

  for (const auto& term_state : phrase_state->terms) {
    SDB_ASSERT(term_state.first);

    // get postings using cached state
    auto& docs =
      itrs.emplace_back(reader->postings(*term_state.first, features));

    if (!docs) [[unlikely]] {
      return DocIterator::empty();
    }

    auto* pos = irs::GetMutable<irs::PosAttr>(docs.it.get());

    if (!pos) {
      // positions not found
      return DocIterator::empty();
    }

    positions.emplace_back(std::ref(*pos), *position);

    ++position;
  }
  const bool has_intervals = absl::c_any_of(
    this->positions,
    [](const auto& pos) { return pos.offs_max != pos.offs_min; });
  if (ord.empty()) {
    return ResolveBool(has_intervals,
                       [&]<bool HasIntervals> -> DocIterator::ptr {
                         return memory::make_managed<
                           FixedPhraseIterator<true, false, HasIntervals>>(
                           std::move(itrs), std::move(positions));
                       });
  }
  return ResolveBool(has_intervals, [&]<bool HasIntervals> -> DocIterator::ptr {
    return memory::make_managed<FixedPhraseIterator<false, true, HasIntervals>>(
      std::move(itrs), std::move(positions), rdr, *phrase_state->reader,
      stats.c_str(), ord, boost);
  });
}

DocIterator::ptr FixedPhraseQuery::ExecuteWithOffsets(
  const SubReader& rdr) const {
  // get phrase state for the specified reader
  auto phrase_state = states.find(rdr);

  if (!phrase_state) {
    // invalid state
    return DocIterator::empty();
  }

  const bool has_intervals = absl::c_any_of(
    this->positions,
    [](const auto& pos) { return pos.offs_max != pos.offs_min; });

  return ResolveBool(has_intervals, [&]<bool HasIntervals> -> DocIterator::ptr {
    using FixedPhraseIterator = PhraseIterator<
      Conjunction<ScoreAdapter<>, NoopAggregator>,
      PhrasePosition<FixedPhraseFrequency<true, false, HasIntervals>>>;

    ScoreAdapters itrs;
    itrs.reserve(phrase_state->terms.size());

    std::vector<typename FixedPhraseIterator::TermPosition> positions;
    positions.reserve(phrase_state->terms.size());

    auto* reader = phrase_state->reader;
    SDB_ASSERT(reader);

    if (kRequireOffs != (reader->meta().index_features & kRequireOffs)) {
      return DocIterator::empty();
    }

    auto position = std::begin(this->positions);
    SDB_ASSERT(!phrase_state->terms.empty());

    auto term_state = std::begin(phrase_state->terms);

    auto add_iterator = [&](IndexFeatures features) {
      SDB_ASSERT(term_state->first);

      // get postings using cached state
      auto& docs =
        itrs.emplace_back(reader->postings(*term_state->first, features));

      if (!docs) [[unlikely]] {
        return false;
      }

      auto* pos = irs::GetMutable<irs::PosAttr>(docs.it.get());

      if (!pos) {
        return false;
      }

      positions.emplace_back(std::ref(*pos), *position);

      if (IndexFeatures::Offs == (features & IndexFeatures::Offs)) {
        if (!irs::get<irs::OffsAttr>(*pos)) {
          return false;
        }
      }

      ++position;
      ++term_state;
      return true;
    };

    if (!add_iterator(kRequireOffs)) {
      return DocIterator::empty();
    }

    if (term_state != std::end(phrase_state->terms)) {
      auto back = std::prev(std::end(phrase_state->terms));

      while (term_state != back) {
        if (!add_iterator(kRequiredFeatures)) {
          return DocIterator::empty();
        }
      }

      if (!add_iterator(kRequireOffs)) {
        return DocIterator::empty();
      }
    }

    return memory::make_managed<FixedPhraseIterator>(std::move(itrs),
                                                     std::move(positions));
  });
}

DocIterator::ptr VariadicPhraseQuery::execute(
  const ExecutionContext& ctx) const {
  using Adapter = VariadicPhraseAdapter;
  using CompoundDocIterator = irs::CompoundDocIterator<Adapter>;
  using Disjunction = Disjunction<DocIterator::ptr, NoopAggregator, Adapter>;
  auto& rdr = ctx.segment;

  // get phrase state for the specified reader
  auto phrase_state = states.find(rdr);

  if (!phrase_state) {
    // invalid state
    return DocIterator::empty();
  }

  // find term using cached state
  auto* reader = phrase_state->reader;
  SDB_ASSERT(reader);

  if (kRequiredFeatures !=
      (reader->meta().index_features & kRequiredFeatures)) {
    return DocIterator::empty();
  }

  auto& ord = ctx.scorers;

  // get features required for query & order
  const IndexFeatures features = ord.features() | kRequiredFeatures;

  ScoreAdapters conj_itrs;
  conj_itrs.reserve(phrase_state->terms.size());

  const auto phrase_size = phrase_state->num_terms.size();

  std::vector<VariadicTermPosition<Adapter>> positions;
  positions.resize(phrase_size);

  auto position = std::begin(this->positions);

  auto term_state = std::begin(phrase_state->terms);
  for (size_t i = 0; i < phrase_size; ++i) {
    const auto num_terms = phrase_state->num_terms[i];
    auto& pos = positions[i];
    pos.second = *position;

    std::vector<Adapter> disj_itrs;
    disj_itrs.reserve(num_terms);
    for (const auto end = term_state + num_terms; term_state != end;
         ++term_state) {
      SDB_ASSERT(term_state->first);

      auto it = reader->postings(*term_state->first, features);

      if (!it) [[unlikely]] {
        continue;
      }

      Adapter docs{std::move(it), term_state->second};

      if (!docs.position) {
        // positions not found
        continue;
      }

      disj_itrs.emplace_back(std::move(docs));
    }

    if (disj_itrs.empty()) {
      return DocIterator::empty();
    }

    // TODO(mbkkt) VariadicPhrase wand support
    auto disj =
      MakeDisjunction<Disjunction>({}, std::move(disj_itrs), NoopAggregator{});
    pos.first = sdb::basics::downCast<CompoundDocIterator>(disj.get());
    conj_itrs.emplace_back(std::move(disj));
    ++position;
  }
  SDB_ASSERT(term_state == std::end(phrase_state->terms));

  const bool has_intervals = absl::c_any_of(
    this->positions,
    [](const auto& pos) { return pos.offs_max != pos.offs_min; });

  if (ord.empty()) {
    return ResolveBool(
      has_intervals, [&]<bool HasIntervals> -> DocIterator::ptr {
        return memory::make_managed<
          VariadicPhraseIterator<Adapter, false, true, false, HasIntervals>>(
          std::move(conj_itrs), std::move(positions));
      });
  }

  if (phrase_state->volatile_boost) {
    return ResolveBool(
      has_intervals, [&]<bool HasIntervals> -> DocIterator::ptr {
        return memory::make_managed<
          VariadicPhraseIterator<Adapter, true, false, true, HasIntervals>>(
          std::move(conj_itrs), std::move(positions), rdr,
          *phrase_state->reader, stats.c_str(), ord, boost);
      });
  }
  return ResolveBool(has_intervals, [&]<bool HasIntervals> -> DocIterator::ptr {
    return memory::make_managed<
      VariadicPhraseIterator<Adapter, false, false, true, HasIntervals>>(
      std::move(conj_itrs), std::move(positions), rdr, *phrase_state->reader,
      stats.c_str(), ord, boost);
  });
}

DocIterator::ptr VariadicPhraseQuery::ExecuteWithOffsets(
  const irs::SubReader& rdr) const {
  using Adapter = VariadicPhraseOffsetAdapter;
  using CompundDocIterator = irs::CompoundDocIterator<Adapter>;
  using Disjunction = Disjunction<DocIterator::ptr, NoopAggregator, Adapter>;

  // get phrase state for the specified reader
  auto phrase_state = states.find(rdr);

  if (!phrase_state) {
    // invalid state
    return DocIterator::empty();
  }

  ScoreAdapters conj_itrs;
  conj_itrs.reserve(phrase_state->terms.size());

  const auto phrase_size = phrase_state->num_terms.size();

  // find term using cached state
  auto* reader = phrase_state->reader;
  SDB_ASSERT(reader);

  if (kRequireOffs != (reader->meta().index_features & kRequireOffs)) {
    return DocIterator::empty();
  }

  const bool has_intervals = absl::c_any_of(
    this->positions,
    [](const auto& pos) { return pos.offs_max != pos.offs_min; });

  return ResolveBool(has_intervals, [&]<bool HasIntervals> -> DocIterator::ptr {
    using VariadicPhraseIterator =
      PhraseIterator<Conjunction<ScoreAdapter<>, NoopAggregator>,
                     PhrasePosition<VariadicPhraseFrequency<
                       Adapter, false, true, false, HasIntervals>>>;

    std::vector<typename VariadicPhraseIterator::TermPosition> positions;
    positions.resize(phrase_size);

    auto position = std::begin(this->positions);

    auto term_state = std::begin(phrase_state->terms);

    size_t i = 0;
    auto add_iterator = [&](IndexFeatures features) {
      const auto num_terms = phrase_state->num_terms[i];
      auto& pos = positions[i];
      pos.second = *position;

      std::vector<Adapter> disj_itrs;
      disj_itrs.reserve(num_terms);
      for (const auto end = term_state + num_terms; term_state != end;
           ++term_state) {
        SDB_ASSERT(term_state->first);

        auto it = reader->postings(*term_state->first, features);

        if (!it) [[unlikely]] {
          continue;
        }

        Adapter docs{std::move(it), term_state->second};

        if (!docs.position) {
          // positions not found
          continue;
        }

        if (IndexFeatures::Offs == (features & IndexFeatures::Offs)) {
          if (!irs::get<irs::OffsAttr>(*docs.position)) {
            continue;
          }
        }

        disj_itrs.emplace_back(std::move(docs));
      }

      if (disj_itrs.empty()) {
        return false;
      }

      // TODO(mbkkt) VariadicPhrase wand support
      auto disj = MakeDisjunction<Disjunction>({}, std::move(disj_itrs),
                                               NoopAggregator{});
      pos.first = sdb::basics::downCast<CompundDocIterator>(disj.get());
      conj_itrs.emplace_back(std::move(disj));
      ++position;
      ++i;
      return true;
    };

    if (!add_iterator(kRequireOffs)) {
      return DocIterator::empty();
    }

    if (i < phrase_size) {
      for (auto size = phrase_size - 1; i < size;) {
        if (!add_iterator(kRequiredFeatures)) {
          return DocIterator::empty();
        }
      }

      if (!add_iterator(kRequireOffs)) {
        return DocIterator::empty();
      }
    }
    SDB_ASSERT(term_state == std::end(phrase_state->terms));

    return memory::make_managed<VariadicPhraseIterator>(std::move(conj_itrs),
                                                        std::move(positions));
  });
}

}  // namespace irs
