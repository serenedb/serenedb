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
#include <tuple>
#include <type_traits>

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/detail/conjunction_leaves.hpp"
#include "iresearch/search/detail/ngram_matcher.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

template<typename Leaf, size_t N = 0, bool Scored = false, bool Offs = false>
class NGramAllSlots {
 public:
  static constexpr bool kOffsets = Offs;
  static_assert(!Offs || Leaf::kOffsets);

  NGramAllSlots(std::span<const PostingMeta> metas, const IndexInput& doc_in,
                IndexFeatures layout, const IndexInput& pos_in,
                const IndexInput* pay_in, size_t total_terms)
    : _matcher{metas.size(), total_terms, static_cast<uint32_t>(metas.size())},
      _leaves{metas.size(), [&](uint32_t j) { return metas[j].docs_count; },
              [&](uint32_t j) {
                return std::forward_as_tuple(metas[j], doc_in, layout, pos_in,
                                             pay_in);
              },
              [&](uint32_t j, Leaf& leaf) {
                _matcher.Slot(j) = {leaf.ValueRef(), leaf.Positions()};
              }} {
    SDB_ASSERT(metas.size() > 1);
  }

  NGramAllSlots(NGramAllSlots&&) = delete;
  NGramAllSlots& operator=(NGramAllSlots&&) = delete;

  doc_id_t Seek(doc_id_t from) { return _leaves.Seek(from); }

  doc_id_t Next(doc_id_t) { return _leaves.Next(); }

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    return _leaves.Probe(target);
  }

  bool Match(doc_id_t doc) { return _matcher.Match(_leaves.Size(), doc); }

  uint32_t Freq() const noexcept
    requires(Scored)
  {
    return _matcher.GetFreq();
  }

  score_t Scale() const noexcept
    requires(Scored)
  {
    return _matcher.GetScale();
  }

  std::span<const OffsAttr> Offsets() const noexcept
    requires(Offs)
  {
    return _matcher.Offsets();
  }

 private:
  using Leaves = ConjunctionLeaves<Leaf, N>;
  using Base = std::conditional_t<Offs, ngram::NGramPosition, ngram::Dummy>;
  using Matcher = ngram::NGramMatcher<Base, Scored || Offs, N>;

  Matcher _matcher;
  Leaves _leaves;
};

}  // namespace irs::detail
