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

#include "iresearch/formats/posting/iterator_doc.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/search/make_disjunction.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {
namespace {

// Get index features required for offsets
constexpr IndexFeatures kRequireOffs =
  FixedPhraseQuery::kRequiredFeatures | IndexFeatures::Offs;

// FIXME add proper handling of overlapped case
template<typename Adapter, bool HasBoost, bool HasFreq, bool HasIntervals>
using VariadicPhraseIterator = PhraseIterator<
  Conjunction<ScoreAdapter>,
  VariadicPhraseFrequency<Adapter, HasBoost, HasFreq, HasIntervals>>;

}  // namespace

DocIterator::ptr FixedPhraseQuery::Execute(const ExecutionContext& ctx,
                                           const StatsBuffer& stats) const {
  auto& rdr = _segment;
  const auto* phrase_state = &state;

  auto* reader = phrase_state->reader;
  if (!reader) {
    return DocIterator::empty();
  }

  if (kRequiredFeatures !=
      (reader->meta().index_features & kRequiredFeatures)) {
    return DocIterator::empty();
  }

  const auto* scorer = stats.GetScorer();

  // get index features required for query & order
  const IndexFeatures features = GetFeatures(scorer) | kRequiredFeatures;

  using Adapter = PostingAdapter<PostingIteratorBase<FixedTermTraits<false>>>;

  std::vector<Adapter> itrs;
  itrs.reserve(phrase_state->terms.size());

  std::vector<FixedTermPosition<false>> positions;
  positions.reserve(phrase_state->terms.size());

  auto position = std::begin(this->positions);

  for (const auto& term_state : phrase_state->terms) {
    SDB_ASSERT(term_state.first);

    auto docs = reader->Iterator(features, {.cookie = term_state.first.get()});
    if (!docs) [[unlikely]] {
      return DocIterator::empty();
    }
    auto* pos = irs::GetMutable<PosAttr>(docs.get());
    if (!pos) [[unlikely]] {
      return DocIterator::empty();
    }
    itrs.emplace_back(std::move(docs));
    positions.emplace_back(
      sdb::basics::downCast<FixedTermPositionImpl<false>>(pos), *position++);
  }
  if (!scorer) {
    return ResolveBool(
      has_intervals, [&]<bool HasIntervals> -> DocIterator::ptr {
        using FixedPhraseIterator =
          PhraseIterator<Conjunction<Adapter>,
                         FixedPhraseFrequency<false, false, HasIntervals>>;
        return memory::make_managed<FixedPhraseIterator>(
          static_cast<doc_id_t>(rdr.docs_count()), std::move(itrs),
          std::move(positions));
      });
  }
  const auto& all_stats = stats.GetAllStats();
  const auto* stats_data =
    all_stats.empty() ? nullptr : all_stats.back().c_str();
  return ResolveBool(has_intervals, [&]<bool HasIntervals> -> DocIterator::ptr {
    using FixedPhraseIterator =
      PhraseIterator<Conjunction<Adapter>,
                     FixedPhraseFrequency<false, true, HasIntervals>>;
    return memory::make_managed<FixedPhraseIterator>(
      static_cast<doc_id_t>(rdr.docs_count()), std::move(itrs),
      std::move(positions), phrase_state->reader->meta(), stats_data, boost);
  });
}

DocIterator::ptr FixedPhraseQuery::ExecuteWithOffsets(
  const SubReader& segment) const {
  const auto* phrase_state = &state;

  if (!phrase_state->reader) {
    return DocIterator::empty();
  }

  return ResolveBool(has_intervals, [&]<bool HasIntervals> -> DocIterator::ptr {
    using Adapter = PostingAdapter<PostingIteratorBase<FixedTermTraits<true>>>;
    using FixedPhraseIterator = PhraseIterator<
      Conjunction<Adapter>,
      PhrasePosition<FixedPhraseFrequency<true, false, HasIntervals>>>;

    std::vector<Adapter> itrs;
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

      auto docs =
        reader->Iterator(features, {.cookie = term_state->first.get()});
      if (!docs) [[unlikely]] {
        return false;
      }
      auto* pos = irs::GetMutable<PosAttr>(docs.get());
      if (!pos) [[unlikely]] {
        return false;
      }
      if (IndexFeatures::Offs == (features & IndexFeatures::Offs) &&
          !irs::get<OffsAttr>(*pos)) [[unlikely]] {
        return false;
      }
      itrs.emplace_back(std::move(docs));
      positions.emplace_back(
        sdb::basics::downCast<FixedTermPositionImpl<true>>(pos), *position++);

      ++term_state;
      return true;
    };

    if (!add_iterator(kRequireOffs)) {
      return DocIterator::empty();
    }

    if (term_state != std::end(phrase_state->terms)) {
      auto back = std::prev(std::end(phrase_state->terms));

      while (term_state != back) {
        // TODO(Dronplane) Make Adapter accept itarators without offs
        // We will not need ones from middle iterators.
        if (!add_iterator(kRequireOffs)) {
          return DocIterator::empty();
        }
      }

      if (!add_iterator(kRequireOffs)) {
        return DocIterator::empty();
      }
    }

    return memory::make_managed<FixedPhraseIterator>(
      static_cast<doc_id_t>(segment.docs_count()), std::move(itrs),
      std::move(positions));
  });
}

DocIterator::ptr VariadicPhraseQuery::Execute(const ExecutionContext& ctx,
                                              const StatsBuffer& stats) const {
  using Adapter = VariadicPhraseAdapter;
  using CompoundDocIterator = CompoundDocIterator<Adapter>;
  using Disjunction = Disjunction<VariadicPhraseAdapter>;
  auto& rdr = _segment;

  const auto* phrase_state = &state;

  // find term using cached state
  auto* reader = phrase_state->reader;
  if (!reader) {
    return DocIterator::empty();
  }

  if (kRequiredFeatures !=
      (reader->meta().index_features & kRequiredFeatures)) {
    return DocIterator::empty();
  }

  const auto* scorer = stats.GetScorer();

  // get features required for query & order
  const IndexFeatures features = GetFeatures(scorer) | kRequiredFeatures;

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

      auto it = reader->Iterator(features, {.cookie = term_state->first.get()});
      if (!it) [[unlikely]] {
        continue;
      }

      Adapter docs{std::move(it), term_state->second};
      if (!docs.position) [[unlikely]] {
        continue;
      }

      disj_itrs.emplace_back(std::move(docs));
    }

    if (disj_itrs.empty()) {
      return DocIterator::empty();
    }

    // TODO(mbkkt) VariadicPhrase wand support
    auto disj = MakeDisjunction<Disjunction>(
      {}, static_cast<doc_id_t>(rdr.docs_count()), std::move(disj_itrs));
    pos.first = sdb::basics::downCast<CompoundDocIterator>(disj.get());
    conj_itrs.emplace_back(std::move(disj));
    ++position;
  }
  SDB_ASSERT(term_state == std::end(phrase_state->terms));

  if (!scorer) {
    return ResolveBool(
      has_intervals, [&]<bool HasIntervals> -> DocIterator::ptr {
        return memory::make_managed<
          VariadicPhraseIterator<Adapter, false, false, HasIntervals>>(
          static_cast<doc_id_t>(rdr.docs_count()), std::move(conj_itrs),
          std::move(positions));
      });
  }

  const auto& all_stats = stats.GetAllStats();
  const auto* stats_data =
    all_stats.empty() ? nullptr : all_stats.back().c_str();

  if (phrase_state->volatile_boost) {
    return ResolveBool(
      has_intervals, [&]<bool HasIntervals> -> DocIterator::ptr {
        return memory::make_managed<
          VariadicPhraseIterator<Adapter, true, true, HasIntervals>>(
          static_cast<doc_id_t>(rdr.docs_count()), std::move(conj_itrs),
          std::move(positions), phrase_state->reader->meta(), stats_data,
          boost);
      });
  }
  return ResolveBool(has_intervals, [&]<bool HasIntervals> -> DocIterator::ptr {
    return memory::make_managed<
      VariadicPhraseIterator<Adapter, false, true, HasIntervals>>(
      static_cast<doc_id_t>(rdr.docs_count()), std::move(conj_itrs),
      std::move(positions), phrase_state->reader->meta(), stats_data, boost);
  });
}

DocIterator::ptr VariadicPhraseQuery::ExecuteWithOffsets(
  const SubReader& segment) const {
  using Adapter = VariadicPhraseOffsetAdapter;
  using CompundDocIterator = CompoundDocIterator<Adapter>;
  using Disjunction = Disjunction<Adapter>;

  const auto* phrase_state = &state;

  ScoreAdapters conj_itrs;
  conj_itrs.reserve(phrase_state->terms.size());

  const auto phrase_size = phrase_state->num_terms.size();

  // find term using cached state
  auto* reader = phrase_state->reader;
  SDB_ASSERT(reader);

  if (kRequireOffs != (reader->meta().index_features & kRequireOffs)) {
    return DocIterator::empty();
  }

  return ResolveBool(has_intervals, [&]<bool HasIntervals> -> DocIterator::ptr {
    std::vector<VariadicTermPosition<Adapter>> positions;
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

        auto it =
          reader->Iterator(features, {.cookie = term_state->first.get()});
        if (!it) [[unlikely]] {
          continue;
        }

        Adapter docs{std::move(it), term_state->second};
        if (!docs.position) [[unlikely]] {
          continue;
        }
        if (IndexFeatures::Offs == (features & IndexFeatures::Offs) &&
            !irs::get<OffsAttr>(*docs.position)) [[unlikely]] {
          continue;
        }

        disj_itrs.emplace_back(std::move(docs));
      }

      if (disj_itrs.empty()) {
        return false;
      }

      // TODO(mbkkt) VariadicPhrase wand support
      auto disj = MakeDisjunction<Disjunction>(
        {}, static_cast<doc_id_t>(segment.docs_count()), std::move(disj_itrs));
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

    return memory::make_managed<
      VariadicPhraseIterator<Adapter, false, false, HasIntervals>>(
      static_cast<doc_id_t>(segment.docs_count()), std::move(conj_itrs),
      std::move(positions));
  });
}

}  // namespace irs
