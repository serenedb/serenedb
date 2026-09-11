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

#include <span>
#include <type_traits>
#include <vector>

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/common/conjunction_leaves.hpp"
#include "iresearch/search/ngram_matcher.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

template<typename Leaf, size_t N = 0, bool Scored = false, bool Offs = false>
class NGramAllSlots {
 public:
  static constexpr bool kOffsets = Offs;
  static_assert(!Offs || Leaf::kOffsets);

  NGramAllSlots(std::span<const PostingMeta> metas, const IndexInput& doc_in,
                IndexFeatures layout, const IndexInput& pos_in,
                const IndexInput* pay_in, size_t total_terms)
    : _leaves(metas.size()),
      _checker{metas.size(), total_terms, static_cast<uint32_t>(metas.size())} {
    _leaves.Open(
      metas,
      [&](Leaf& leaf, const PostingMeta& meta) {
        leaf.Prepare(meta, doc_in, layout, pos_in, pay_in);
      },
      [&](uint32_t j, typename Leaves::Slot& slot) {
        _checker.Slot(j) = {slot.leaf.ValueRef(), slot.leaf.Positions()};
      });
  }

  NGramAllSlots(NGramAllSlots&&) = delete;
  NGramAllSlots& operator=(NGramAllSlots&&) = delete;

  doc_id_t Seek(doc_id_t from) { return _leaves.Seek(from); }

  doc_id_t Next(doc_id_t) { return _leaves.Next(); }

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    return _leaves.Probe(target);
  }

  bool Match(doc_id_t doc) { return _checker.Match(_leaves.Size(), doc); }

  uint32_t Freq() const noexcept
    requires(Scored)
  {
    return _checker.GetFreq();
  }

  score_t Boost() const noexcept
    requires(Scored)
  {
    return _checker.GetBoost();
  }

  std::span<const OffsAttr> Offsets() const noexcept
    requires(Offs)
  {
    return _checker.Offsets();
  }

 private:
  using Leaves = ConjunctionLeaves<Leaf, N>;
  using Base = std::conditional_t<Offs, ngram::NGramPosition, ngram::Dummy>;
  using Checker = ngram::SerialPositionsChecker<Base, Scored || Offs, N>;

  Leaves _leaves;
  Checker _checker;
};

}  // namespace irs::search
