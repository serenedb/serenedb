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
#include "iresearch/search/common/bitset_of.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/offsets/phrase_of.hpp"
#include "iresearch/search/count/boolean_bitset.hpp"
#include "iresearch/search/count/make.hpp"
#include "iresearch/search/count/walk.hpp"
#include "iresearch/search/fill/set_leaves.hpp"

namespace irs::count {

template<template<typename...> class Shape, typename... Parts, typename... Args>
Root::ptr MakeShape(const Context& ctx, Args&&... args) {
  if (ctx.table != nullptr) {
    return memory::make_managed<Shape<Parts..., search::TableFilter*>>(
      ctx.table, std::forward<Args>(args)...);
  }
  return memory::make_managed<Shape<Parts..., utils::Empty>>(
    utils::Empty{}, std::forward<Args>(args)...);
}

template<typename Node>
using PlainWalk = Walk<Node, utils::Empty>;
template<typename Node>
using FilteredWalk = Walk<Node, search::TableFilter*>;

template<search::PhraseMatch M>
Root::ptr MakeFixedPhraseWalk(const FixedPhraseQuery& query,
                              const Context& ctx) {
  if (ctx.table != nullptr) {
    return search::MakeFixedPhraseOf<M, FilteredWalk, Root::ptr>(query,
                                                                 ctx.table);
  }
  return search::MakeFixedPhraseOf<M, PlainWalk, Root::ptr>(query,
                                                            utils::Empty{});
}

template<search::PhraseMatch M>
Root::ptr MakeVariadicPhraseWalk(const VariadicPhraseQuery& query,
                                 const Context& ctx) {
  if (ctx.table != nullptr) {
    return search::MakeVariadicPhraseOf<M, FilteredWalk, Root::ptr>(query,
                                                                    ctx.table);
  }
  return search::MakeVariadicPhraseOf<M, PlainWalk, Root::ptr>(query,
                                                               utils::Empty{});
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

}  // namespace irs::count
