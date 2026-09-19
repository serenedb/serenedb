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

#include <cstdint>
#include <span>
#include <vector>

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/detail/bitset_of.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/phrase_of.hpp"
#include "iresearch/search/docs/boolean_bitset.hpp"
#include "iresearch/search/docs/make.hpp"
#include "iresearch/search/docs/walk.hpp"
#include "iresearch/search/fill/set_leaves.hpp"

namespace irs::docs {

template<template<typename...> class Shape, typename... Parts, typename... Args>
Root::ptr MakeShape(const Context&, Args&&... args) {
  return memory::make_managed<Shape<Parts...>>(std::forward<Args>(args)...);
}

template<detail::PhraseMatch M>
Root::ptr MakeFixedPhraseWalk(const FixedPhraseQuery& query, const Context&) {
  return detail::MakeFixedPhraseOf<M, Walk, Root::ptr>(query);
}

template<detail::PhraseMatch M>
Root::ptr MakeVariadicPhraseWalk(const VariadicPhraseQuery& query,
                                 const Context&) {
  return detail::MakeVariadicPhraseOf<M, Walk, Root::ptr>(query);
}

Root::ptr MakeFixedPhrase(const FixedPhraseQuery& query, const Context& ctx);
Root::ptr MakeFixedPhraseIntervals(const FixedPhraseQuery& query,
                                   const Context& ctx);
Root::ptr MakeFixedPhraseSlop(const FixedPhraseQuery& query,
                              const Context& ctx);
Root::ptr MakeVariadicPhrase(const VariadicPhraseQuery& query,
                             const Context& ctx);
Root::ptr MakeVariadicPhraseIntervals(const VariadicPhraseQuery& query,
                                      const Context& ctx);
Root::ptr MakeVariadicPhraseSlop(const VariadicPhraseQuery& query,
                                 const Context& ctx);

Root::ptr MakeNGram(const NGramSimilarityQuery& query, const Context& ctx);
Root::ptr MakeNGramAll(const NGramSimilarityQuery& query, const Context& ctx);

}  // namespace irs::docs
