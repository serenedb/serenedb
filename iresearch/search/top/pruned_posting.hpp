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

#include <tuple>
#include <type_traits>
#include <utility>

#include "iresearch/index/iterators.hpp"
#include "iresearch/search/detail/exclude_block.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/pruned_leaf.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/utils/empty.hpp"

namespace irs::top {

template<typename InputType, typename Excludes, typename Table>
class PrunedPosting : public Root, public PrunedLeafBase<InputType, true> {
  using Base = PrunedLeafBase<InputType, true>;

  using Base::_cursor;
  using Base::_doc;
  using Base::_docs;
  using Base::_fetcher;
  using Base::_freqs;
  using Base::_left_in_leaf;
  using Base::_left_in_list;
  using Base::_max_in_leaf;
  using Base::_provider;
  using Base::_score;
  using Base::_threshold;
  using Base::BoundScore;
  using Base::Emit;
  using Base::In;
  using Base::ReadLeaf;
  using Base::SeekCursor;

 public:
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;

  template<typename ExcludesArgs, typename... Args>
  PrunedPosting(Table table, ExcludesArgs&& excludes, Args&&... args)
    : _excludes{std::make_from_tuple<Excludes>(
        std::forward<ExcludesArgs>(excludes))},
      _admit{table} {
    Prepare(std::forward<Args>(args)...);
  }

  PrunedPosting(PrunedPosting&&) = delete;
  PrunedPosting& operator=(PrunedPosting&&) = delete;

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               IndexFeatures layout, const SubReader& segment,
               const TermReader& field, const irs::detail::ScoreArgs& args) {
    if (Base::PrepareCommon(meta, doc_in, layout, segment, field, args)) {
      _left_in_leaf = 0;
      _doc = doc_limits::min() + meta.doc_delta;
    }
    _least_score = BoundScore({.freq = 1, .norm = 1});
    if (_cursor.Armed()) {
      _most_freq = _cursor.Index().Root().freq;
    }
  }

  void Run(doc_id_t min, doc_id_t max, LoserScoreCollector& collector) final {
    const auto emit = [&](doc_id_t* IRS_RESTRICT docs, uint32_t len,
                          score_t* IRS_RESTRICT scores) IRS_FORCE_INLINE {
      if constexpr (kExcludes) {
        len = irs::detail::FilterScores(docs, scores, len,
                                        collector.ScoreThreshold());
        len = irs::detail::ExcludeBlock(_excludes, docs, scores, len);
      }
      if (len != 0) {
        _admit.AddDocs(collector, docs, len, scores);
      }
      _threshold = collector.ScoreThreshold();
    };

    if (_left_in_list == 0 && _left_in_leaf == 0) {
      if (doc_limits::valid(_doc) && _doc >= min && _doc < max) {
        Emit(std::end(_docs) - 1, 1, emit);
      }
      _doc = doc_limits::eof();
      _admit.Flush(collector);
      return;
    }
    if (_least_freq != 1 || Blocks<false>(min, max, emit)) {
      FilteredBlocks(min, max, emit);
    }
    _doc = doc_limits::eof();
    _admit.Flush(collector);
  }

 private:
  template<typename Visitor>
  IRS_NO_INLINE void FilteredBlocks(doc_id_t min, doc_id_t max, Visitor& emit) {
    Blocks<true>(min, max, emit);
  }

  template<bool Filter, typename Visitor>
  IRS_FORCE_INLINE bool Blocks(doc_id_t min, doc_id_t max, Visitor& emit) {
    auto* const end = std::end(_docs);
    for (;;) {
      if constexpr (!Filter) {
        if (_least_score <= _threshold) {
          return true;
        }
      }
      if (_left_in_leaf == 0) {
        if (_left_in_list == 0) {
          break;
        }
        auto last = _max_in_leaf;
        const auto target = std::max<doc_id_t>(last + 1, min);
        if (target >= max) {
          break;
        }
        if (target > _cursor.UpperBound()) {
          _left_in_list = SeekCursor(target);
          if (_left_in_list == 0) {
            break;
          }
          const auto state = _cursor.Landing();
          In().Seek(state.doc_ptr);
          last = state.doc;
        }
        ReadLeaf(last);
      }
      auto* first = end - _left_in_leaf;
      auto* stop = end;
      if (*first < min) [[unlikely]] {
        do {
          ++first;
        } while (first != stop && *first < min);
      }
      const bool straddles = _max_in_leaf >= max;
      if (straddles) [[unlikely]] {
        while (stop != first && stop[-1] >= max) {
          --stop;
        }
      }
      _left_in_leaf = static_cast<uint32_t>(end - stop);
      if (stop != first) {
        if constexpr (Filter) {
          EmitKept(first, static_cast<uint32_t>(stop - first), emit);
        } else {
          Emit(first, static_cast<uint32_t>(stop - first), emit);
        }
      }
      if (straddles) {
        break;
      }
    }
    return false;
  }

  template<typename Visitor>
  IRS_FORCE_INLINE void EmitKept(doc_id_t* docs, uint32_t len, Visitor& visit) {
    const auto least =
      _least_score > _threshold ? _least_freq : RaiseLeastFreq();
    ABSL_CACHELINE_ALIGNED doc_id_t kept_docs[doc_limits::kBlockSize];
    ABSL_CACHELINE_ALIGNED uint32_t kept_freqs[doc_limits::kBlockSize];
    ABSL_CACHELINE_ALIGNED score_t kept_scores[doc_limits::kBlockSize];
    const auto* const freqs = _freqs.data + (docs - std::begin(_docs));
    const auto kept =
      irs::detail::FilterFreqs(docs, freqs, len, least, kept_docs, kept_freqs);
    if (kept == 0) {
      return;
    }
    _fetcher->Fetch(std::span<const doc_id_t>{kept_docs, kept});
    _provider.freq.value = kept_freqs;
    _score.Score(kept_scores, static_cast<scores_size_t>(kept));
    _provider.freq.value = _freqs.data;
    visit(kept_docs, kept, kept_scores);
  }

  IRS_NO_INLINE uint32_t RaiseLeastFreq() {
    while (_least_freq <= _most_freq) {
      ++_least_freq;
      _least_score = BoundScore({.freq = _least_freq, .norm = _least_freq});
      if (_least_score > _threshold) {
        break;
      }
    }
    return _least_freq;
  }

  [[no_unique_address]] Excludes _excludes;
  [[no_unique_address]] Admit<Table> _admit;
  score_t _least_score = std::numeric_limits<score_t>::lowest();
  uint32_t _least_freq = 1;
  uint32_t _most_freq = 0;
};

}  // namespace irs::top
