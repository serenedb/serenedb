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

#include <utility>

#include "iresearch/index/iterators.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/detail/prune_leaf.hpp"
#include "iresearch/search/top/root.hpp"

namespace irs::top {

template<typename InputType, typename Table>
class PrunedPosting : public Root,
                      public search::PruneLeafBase<InputType, true> {
  using Base = search::PruneLeafBase<InputType, true>;

  using Base::_doc;
  using Base::_docs;
  using Base::_left_in_leaf;
  using Base::_left_in_list;
  using Base::_skip;
  using Base::Emit;
  using Base::In;
  using Base::ReadLeaf;

 public:
  template<typename... Args>
  explicit PrunedPosting(Table table, Args&&... args) : _admit{table} {
    Prepare(std::forward<Args>(args)...);
  }

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               IndexFeatures layout, const SubReader& segment,
               const TermReader& field, const search::ScoreArgs& args) {
    if (Base::PrepareCommon(meta, doc_in, layout, segment, field, args)) {
      _left_in_leaf = 0;
      _doc = doc_limits::min() + meta.doc_delta;
    }
  }

  void Run(LoserScoreCollector& collector) final {
    const auto emit = [&](const doc_id_t* IRS_RESTRICT docs, uint32_t len,
                          const score_t* IRS_RESTRICT scores) IRS_FORCE_INLINE {
      _admit.AddDocs(collector, docs, len, scores);
      _skip.Reader().Threshold() = collector.ScoreThreshold();
    };

    if (_left_in_list == 0) {
      if (doc_limits::valid(_doc)) {
        Emit(std::end(_docs) - 1, 1, emit);
      }
      _doc = doc_limits::eof();
      _admit.Flush(collector);
      return;
    }
    *(std::end(_docs) - 1) = doc_limits::invalid();
    while (_left_in_list != 0) {
      auto last = *(std::end(_docs) - 1);
      if (last + 1 > _skip.Reader().UpperBound()) {
        _left_in_list = _skip.Seek(last + 1);
        auto& state = _skip.Reader().State();
        if (state.doc_ptr != 0) [[likely]] {
          In().Seek(state.doc_ptr);
        }
        last = state.doc;
        if (_left_in_list == 0) {
          break;
        }
      }
      ReadLeaf(last);
      Emit(std::end(_docs) - _left_in_leaf, _left_in_leaf, emit);
      _left_in_leaf = 0;
    }
    _doc = doc_limits::eof();
    _admit.Flush(collector);
  }

 private:
  [[no_unique_address]] Admit<Table> _admit;
};

}  // namespace irs::top
