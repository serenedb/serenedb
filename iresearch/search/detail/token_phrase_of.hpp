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
#include <limits>
#include <memory>
#include <span>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/search/detail/node_of.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/resolve.hpp"
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
                   const TokenPhraseQuery& query, bool count)
    : _approx{std::make_from_tuple<Approx>(std::forward<ApproxArgs>(approx))},
      _ctx{*query.Segment().GetColReader()},
      _column{&query.Column()},
      _state{_column->InitScan(_ctx)},
      _out{_column->Type()},
      _sel{STANDARD_VECTOR_SIZE},
      _check{query.Compiled(), query.Tokens(), count} {}

  TokenPhraseSlots(TokenPhraseSlots&&) = delete;
  TokenPhraseSlots& operator=(TokenPhraseSlots&&) = delete;

  doc_id_t Seek(doc_id_t target)
    requires requires(Approx& approx, doc_id_t doc) { approx.Seek(doc); }
  {
    const auto it = std::lower_bound(_docs.begin() + _pos, _docs.end(), target);
    _pos = static_cast<size_t>(it - _docs.begin());
    if (it != _docs.end()) {
      return *it;
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
    } else {
      Check({&doc, 1}, {&_verdict, 1});
    }
    return _verdict.freq != 0;
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
    Check(_docs, _verdicts);
    return first;
  }

  void Check(std::span<const doc_id_t> docs,
             std::span<PhraseVerdict> verdicts) {
    SDB_ASSERT(docs.size() <= STANDARD_VECTOR_SIZE);
    auto n = docs.size();
    while (n != 0 && docs[n - 1] - doc_limits::min() >= _column->RowCount()) {
      --n;
    }
    std::ranges::fill(verdicts.subspan(n), PhraseVerdict{});
    if (n == 0) {
      return;
    }
    const uint64_t anchor = docs.front() - doc_limits::min();
    for (size_t i = 0; i != n; ++i) {
      _sel.set_index(i, docs[i] - docs.front());
    }
    auto& out = _out.Reset();
    _column->GatherScatter(_state, anchor, _sel, n, out, 0);
    _check.Bind(out, n);
    for (size_t i = 0; i != n; ++i) {
      _check.Check(i, verdicts[i]);
    }
  }

  Approx _approx;
  ReadContext _ctx;
  const ColumnReader* _column;
  ColumnReader::ScanState _state;
  ColumnReader::VectorScratch _out;
  duckdb::SelectionVector _sel;
  PhraseCheck _check;
  PhraseVerdict _verdict;
  std::vector<doc_id_t> _docs;
  std::vector<PhraseVerdict> _verdicts;
  size_t _pos = 0;
  size_t _batch = 1;
  bool _end = false;
};

template<template<typename> class Impl, typename Result, bool Scored = false,
         template<typename> class Wrap = DeducedNode, typename... Prefix>
Result MakeTokenPhrase(const TokenPhraseQuery& query, uint64_t interrogations,
                       Prefix&&... prefix) {
  constexpr bool kProbed = std::is_same_v<Result, ProbeNode::ptr>;
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
  const auto make = [&]<bool Sloppy> -> Result {
    using Slots = TokenPhraseSlots<Approx, Sloppy>;
    return memory::make_managed<Impl<NodeOf<Wrap, Result, Slots>>>(
      std::forward<Prefix>(prefix)..., std::piecewise_construct,
      std::forward_as_tuple(std::move(node)), query, Scored);
  };
  if constexpr (Scored) {
    return ResolveBool(query.Compiled().slop.max != 0, make);
  } else {
    return make.template operator()<false>();
  }
}

}  // namespace irs::detail
