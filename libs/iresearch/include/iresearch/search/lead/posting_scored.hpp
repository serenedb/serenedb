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

#include "iresearch/formats/posting/skip_list.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/posting_leaf.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

template<typename InputType>
class PostingLeadScored : public PostingLeaf<InputType, kCursorScoredShape> {
  using Base = PostingLeaf<InputType, kCursorScoredShape>;

  using Base::_freqs;
  using Base::_gather;
  using Base::_left_in_leaf;
  using Base::kBlock;

 public:
  PostingLeadScored() = default;

  PostingLeadScored(const PostingMeta& meta, const IndexInput& doc_in,
                    const SubReader& segment, const TermReader& field,
                    const ScoreArgs& args) {
    Prepare(meta, doc_in, segment, field, args);
  }

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               const SubReader& segment, const TermReader& field,
               const ScoreArgs& args) {
    SDB_ASSERT(meta.docs_count != 0);
    SDB_ASSERT(FeaturesHaveFreq(field.meta().index_features));
    this->SetRecipe(segment, field, args);

    if (meta.docs_count == 1) {
      this->SetSingle(meta);
      return;
    }

    const auto bounds = field.HasScoreBounds();
    this->OpenInput(meta, doc_in, bounds);
    this->ArmWalk(meta, field.meta().index_features, bounds);
  }

  ScoreFunction PrepareScore() { return this->MakeDeferredScore(); }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) noexcept {
    _gather.data[slot] = _freqs.data[kBlock - _left_in_leaf - 1];
  }
};

}  // namespace irs::search
