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
#include <bit>
#include <vector>

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/masked_leaf.hpp"
#include "iresearch/search/detail/posting_leaf.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

template<typename InputType>
class PostingProbeScored : public PostingLeaf<InputType, kProbeScoredShape> {
  using Base = PostingLeaf<InputType, kProbeScoredShape>;

  using Base::_cursor;
  using Base::_doc;
  using Base::_docs;
  using Base::_freqs;
  using Base::_gather;
  using Base::_last;
  using Base::ReadLeafFill;
  using Base::SeekToLeaf;

 public:
  PostingProbeScored() = default;

  PostingProbeScored(const PostingMeta& meta, const IndexInput& doc_in,
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
      const auto doc = this->SetSingle(meta);
      _cursor.base = doc - 1;
      _leaf.Single();
      return;
    }

    const auto bounds = field.HasScoreBounds();
    this->OpenInput(meta, doc_in, bounds);
    this->ArmWalk(meta, field.meta().index_features, bounds);
  }

  ScoreFunction PrepareScore() { return this->MakeDeferredScore(); }

  void CollectScorers(std::vector<ScoreFunction>& out) {
    AppendScorer(out, PrepareScore());
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) noexcept {
    _gather.data[slot] = _freqs.data[_leaf.Index()];
  }

  IRS_FORCE_INLINE uint32_t Freq() const noexcept {
    return _freqs.data[_leaf.Index()];
  }

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }

    if (_last < target && !ReadTo(target)) [[unlikely]] {
      return _doc = doc_limits::eof();
    }

    return _doc = _leaf.Find(std::begin(_docs), _cursor.base, target);
  }

 private:
  void ReadLeaf(doc_id_t prev) {
    const auto read = ReadLeafFill(prev);
    _leaf.Reset(read.leaf, read.bitset, read.len);
  }

  IRS_FORCE_INLINE bool ReadTo(doc_id_t target) {
    return SeekToLeaf(
      target, [this](doc_id_t prev) IRS_FORCE_INLINE { ReadLeaf(prev); });
  }

  MaskedLeaf _leaf;
};

}  // namespace irs::detail
