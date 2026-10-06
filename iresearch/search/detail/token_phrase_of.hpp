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
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/search/detail/node_of.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/token_phrase.hpp"
#include "iresearch/search/queries/token_phrase_query.hpp"
#include "iresearch/utils/memory.hpp"

namespace irs::detail {

template<typename Approx, bool Sloppy>
class TokenPhraseSlots {
 public:
  static constexpr size_t kMaxBatch = 512;

  template<typename ApproxArgs>
  TokenPhraseSlots(std::piecewise_construct_t, ApproxArgs&& approx,
                   const TokenPhraseQuery::Recipe& recipe, bool count)
    : _approx{std::make_from_tuple<Approx>(std::forward<ApproxArgs>(approx))},
      _reader{*recipe.col_reader, recipe.columns, recipe.tokens->tokenizer(),
              *recipe.matcher,
              recipe.tokens->text.expression ? recipe.tokens->text.expression()
                                             : nullptr},
      _count{count} {}

  TokenPhraseSlots(TokenPhraseSlots&&) = delete;
  TokenPhraseSlots& operator=(TokenPhraseSlots&&) = delete;

  doc_id_t Seek(doc_id_t target)
    requires requires(Approx& approx, doc_id_t doc) { approx.Seek(doc); }
  {
    while (_pos < _docs.size() && _docs[_pos] < target) {
      ++_pos;
    }
    if (_pos < _docs.size()) {
      return _docs[_pos];
    }
    _batch = 1;
    return Fill(_end ? doc_limits::eof() : _approx.Seek(target));
  }

  doc_id_t Next(doc_id_t)
    requires requires(Approx& approx) { approx.Next(); }
  {
    if (++_pos < _docs.size()) {
      return _docs[_pos];
    }
    _batch = std::min(_batch * 2, kMaxBatch);
    return Fill(_end ? doc_limits::eof() : _approx.Next());
  }

  doc_id_t Probe(doc_id_t target)
    requires requires(Approx& approx, doc_id_t doc) { approx.Probe(doc); }
  {
    return _approx.Probe(target);
  }

  bool Match(doc_id_t doc) {
    if (_pos < _docs.size() && _docs[_pos] == doc) {
      _verdict = _verdicts[_pos];
      return _matched[_pos] != 0;
    }
    return _reader.Match(doc, _count, _verdict);
  }

  uint32_t Freq() const noexcept { return _verdict.freq; }

  score_t Scale() const noexcept
    requires(Sloppy)
  {
    return _verdict.scale;
  }

 private:
  doc_id_t Fill(doc_id_t first) {
    _docs.clear();
    _pos = 0;
    if (doc_limits::eof(first)) {
      _end = true;
      return first;
    }
    _docs.push_back(first);
    while (_docs.size() < _batch) {
      const auto doc = _approx.Next();
      if (doc_limits::eof(doc)) {
        _end = true;
        break;
      }
      _docs.push_back(doc);
    }
    _verdicts.resize(_docs.size());
    _matched.resize(_docs.size());
    _reader.Match(_docs, _count, _verdicts, _matched);
    return first;
  }

  Approx _approx;
  TokenPhraseReader _reader;
  PhraseVerdict _verdict;
  std::vector<doc_id_t> _docs;
  std::vector<PhraseVerdict> _verdicts;
  std::vector<uint8_t> _matched;
  size_t _pos = 0;
  size_t _batch = 1;
  bool _count;
  bool _end = false;
};

template<template<typename> class Impl, typename Result, bool Scored = false,
         template<typename> class Wrap = DeducedNode, typename... Prefix>
Result MakeTokenPhrase(const TokenPhraseQuery& query, uint64_t interrogations,
                       Prefix&&... prefix) {
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
    using Slots = TokenPhraseSlots<Approx, Sloppy>;
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
