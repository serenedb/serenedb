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

#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

#include "iresearch/search/detail/node_of.hpp"
#include "iresearch/search/detail/phrase_verify.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/queries/verified_phrase_query.hpp"
#include "iresearch/utils/memory.hpp"

namespace irs::detail {

template<typename Approx, bool Sloppy>
class VerifiedPhraseSlots {
 public:
  template<typename ApproxArgs>
  VerifiedPhraseSlots(std::piecewise_construct_t, ApproxArgs&& approx,
                      const VerifiedPhraseQuery::Recipe& recipe, bool count)
    : _approx{std::make_from_tuple<Approx>(std::forward<ApproxArgs>(approx))},
      _source{recipe.source->Open(*recipe.segment)},
      _kernel{recipe.kernel},
      _count{count} {}

  VerifiedPhraseSlots(VerifiedPhraseSlots&&) = delete;
  VerifiedPhraseSlots& operator=(VerifiedPhraseSlots&&) = delete;

  doc_id_t Seek(doc_id_t target)
    requires requires(Approx& approx, doc_id_t doc) { approx.Seek(doc); }
  {
    return _approx.Seek(target);
  }

  doc_id_t Next(doc_id_t)
    requires requires(Approx& approx) { approx.Next(); }
  {
    return _approx.Next();
  }

  doc_id_t Probe(doc_id_t target)
    requires requires(Approx& approx, doc_id_t doc) { approx.Probe(doc); }
  {
    return _approx.Probe(target);
  }

  bool Match(doc_id_t doc) {
    return _source->Load(doc, _tokens) &&
           _kernel->Match(_tokens, _count, _scratch, _verdict);
  }

  uint32_t Freq() const noexcept { return _verdict.freq; }

  score_t Scale() const noexcept
    requires(Sloppy)
  {
    return _verdict.scale;
  }

 private:
  Approx _approx;
  std::unique_ptr<PhraseTokenSource> _source;
  const PhraseVerifyKernel* _kernel;
  PhraseDocTokens _tokens;
  PhraseVerifyScratch _scratch;
  PhraseVerdict _verdict;
  bool _count;
};

template<template<typename> class Impl, typename Result, bool Scored = false,
         template<typename> class Wrap = DeducedNode, typename... Prefix>
Result MakeVerifiedPhrase(const VerifiedPhraseQuery& query,
                          uint64_t interrogations, Prefix&&... prefix) {
  constexpr bool kProbed = std::is_same_v<Result, ProbeNode::ptr>;
  const auto recipe = query.MakeRecipe();
  const auto make = [&]<bool Sloppy> -> Result {
    auto node = [&] {
      if constexpr (kProbed) {
        return query.Approx().PlanProbe({}, interrogations);
      } else {
        return query.Approx().PlanLead({});
      }
    }();
    if (!node) {
      return {};
    }
    using Approx = std::conditional_t<kProbed, probe::Erased, lead::Erased>;
    using Slots = VerifiedPhraseSlots<Approx, Sloppy>;
    return memory::make_managed<Impl<NodeOf<Wrap, Result, Slots>>>(
      std::forward<Prefix>(prefix)..., std::piecewise_construct,
      std::forward_as_tuple(std::move(node)), recipe, Scored);
  };
  if (query.Sloppy()) {
    return make.template operator()<true>();
  }
  return make.template operator()<false>();
}

}  // namespace irs::detail
