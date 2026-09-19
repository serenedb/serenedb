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

#include <algorithm>
#include <span>
#include <tuple>
#include <type_traits>
#include <utility>

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/detail/exclude_block.hpp"
#include "iresearch/search/detail/posting_batch.hpp"
#include "iresearch/search/hits/root.hpp"
#include "iresearch/search/lead/posting_scored.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::hits {

template<typename InputType>
class BoostTerm {
 public:
  explicit BoostTerm(ColumnArgsFetcher& fetcher) noexcept : _fetcher{fetcher} {}

  BoostTerm(BoostTerm&&) = delete;
  BoostTerm& operator=(BoostTerm&&) = delete;

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               const SubReader& segment, const TermReader& field,
               const irs::detail::ScoreArgs& args) {
    _leaf.Prepare(meta, doc_in, segment, field, args);
    _score = _leaf.PrepareScore();
    _doc = doc_limits::invalid();
  }

  void Apply(const doc_id_t* IRS_RESTRICT docs, score_t* IRS_RESTRICT scores,
             uint32_t len) {
    const doc_id_t last = docs[len - 1];
    auto doc = _doc;
    if (doc > last) {
      return;
    }
    uint32_t at = 0;
    uint32_t held = 0;
    for (;;) {
      if (doc < docs[at]) {
        doc = _leaf.Seek(docs[at]);
        if (doc > last) {
          break;
        }
      }
      at = static_cast<uint32_t>(std::lower_bound(docs + at, docs + len, doc) -
                                 docs);
      if (docs[at] != doc) {
        continue;
      }
      _cand[held] = doc;
      _pos[held] = at;
      _leaf.FetchScoreArgs(held);
      if (++held == kScoreBlock) {
        Flush(scores, held);
        held = 0;
      }
      doc = _leaf.Next();
      if (doc > last) {
        break;
      }
      ++at;
    }
    _doc = doc;
    if (held != 0) {
      Flush(scores, held);
    }
  }

 private:
  void Flush(score_t* IRS_RESTRICT scores, uint32_t held) {
    std::fill(_cand + held, _cand + kScoreBlock, _cand[held - 1]);
    _fetcher.FetchScoreBlock(
      std::span<const doc_id_t, kScoreBlock>{_cand, kScoreBlock});
    _score.ScoreBlock(_boosts);
    for (uint32_t i = 0; i != held; ++i) {
      scores[_pos[i]] += _boosts[i];
    }
  }

  ColumnArgsFetcher& _fetcher;
  irs::detail::PostingLeadScored<InputType> _leaf;
  doc_id_t _doc = doc_limits::invalid();
  ScoreFunction _score;
  ABSL_CACHELINE_ALIGNED doc_id_t _cand[kScoreBlock];
  ABSL_CACHELINE_ALIGNED score_t _boosts[kScoreBlock];
  uint32_t _pos[kScoreBlock];
};

template<typename InputType, typename Boost, typename Excludes>
class Posting : public Root, public irs::detail::PostingBatch<InputType, true> {
  using Base = irs::detail::PostingBatch<InputType, true>;

  using Base::_last;
  using Base::_left_in_list;
  using Base::kBlock;
  using Base::ReadDocs;
  using Base::ScoreBlock;
  using Base::ScoreTail;

 public:
  static constexpr bool kBoost = !std::is_same_v<Boost, utils::Empty>;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;

  template<typename BoostArgs, typename ExcludesArgs>
  Posting(std::piecewise_construct_t, BoostArgs&& boost,
          ExcludesArgs&& excludes)
    : _boost{std::make_from_tuple<Boost>(std::forward<BoostArgs>(boost))},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))} {}

  Posting(Posting&&) = delete;
  Posting& operator=(Posting&&) = delete;

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               const SubReader& segment, const TermReader& field,
               const irs::detail::ScoreArgs& args, IndexFeatures layout,
               bool bounds) {
    SDB_ASSERT(meta.docs_count > 1, "a single document has its own root");
    this->OpenInput(meta, doc_in, bounds);
    this->ArmWalk(meta, layout, bounds);
    this->MakeScore(segment, field, args);
  }

  Boost& Optional() noexcept { return _boost; }

  uint32_t Run(doc_id_t min, doc_id_t max, doc_id_t* IRS_RESTRICT docs,
               score_t* IRS_RESTRICT scores) final {
    uint32_t emitted = Drain(docs, scores, min, max);
    if (_at != _len) {
      return emitted;
    }
    if (!this->Start(min)) {
      return emitted;
    }
    const auto capacity = static_cast<uint32_t>(max - min);
    while (_left_in_list != 0) {
      const auto len = std::min(_left_in_list, kBlock);
      if (emitted + kBlock > capacity) [[unlikely]] {
        _at = 0;
        _len = Fill(_block.data(), _scores.data(), len);
        emitted += Drain(docs + emitted, scores + emitted, min, max);
        if (_at != _len) {
          break;
        }
        continue;
      }
      auto* const dest = docs + emitted;
      auto* const out = scores + emitted;
      const auto kept = Fill(dest, out, len);
      if (kept == 0) [[unlikely]] {
        continue;
      }
      if (dest[kept - 1] < max) [[likely]] {
        emitted += kept;
        continue;
      }
      auto* stop = dest + kept;
      do {
        --stop;
      } while (stop != dest && stop[-1] >= max);
      const auto keep = static_cast<uint32_t>(stop - dest);
      _len = kept - keep;
      _at = 0;
      std::copy_n(stop, _len, _block.data());
      std::copy_n(out + keep, _len, _scores.data());
      return emitted + keep;
    }
    return emitted;
  }

 private:
  IRS_FORCE_INLINE uint32_t Fill(doc_id_t* IRS_RESTRICT dest,
                                 score_t* IRS_RESTRICT out, uint32_t len) {
    ReadDocs(dest, len);
    if (len == kBlock) {
      ScoreBlock(dest, out);
    } else {
      ScoreTail(dest, out, len);
    }
    if constexpr (kBoost) {
      _boost.Apply(dest, out, len);
    }
    if constexpr (kExcludes) {
      len = irs::detail::ExcludeBlock(_excludes, dest, out, len);
    }
    return len;
  }

  IRS_FORCE_INLINE uint32_t Drain(doc_id_t* IRS_RESTRICT docs,
                                  score_t* IRS_RESTRICT scores, doc_id_t min,
                                  doc_id_t max) noexcept {
    if (_at == _len) {
      return 0;
    }
    const doc_id_t* const begin = _block.data();
    const auto* first = begin + _at;
    const auto* last = begin + _len;
    if (*first < min) [[unlikely]] {
      do {
        ++first;
      } while (first != last && *first < min);
    }
    if (first != last && last[-1] >= max) [[unlikely]] {
      do {
        --last;
      } while (last != first && last[-1] >= max);
    }
    const auto at = static_cast<uint32_t>(first - begin);
    const auto n = static_cast<uint32_t>(last - first);
    std::copy_n(first, n, docs);
    std::copy_n(_scores.data() + at, n, scores);
    _at = at + n;
    return n;
  }

  DocsBuf _block;
  SlackBuf<score_t, doc_limits::kBlockSize, doc_limits::kScoresSlack> _scores;
  uint32_t _len = 0;
  uint32_t _at = 0;
  [[no_unique_address]] Boost _boost;
  [[no_unique_address]] Excludes _excludes;
};

}  // namespace irs::hits
