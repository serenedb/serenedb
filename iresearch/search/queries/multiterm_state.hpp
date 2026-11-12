////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <span>

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/queries/term_state.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/query_memory.hpp"

namespace irs {

struct TermReader;

class MultiTermState {
 public:
  struct Entry {
    PostingMeta cookie;
    score_t boost = kNoBoost;
    const byte_type* stats = nullptr;
  };

  explicit MultiTermState(QueryMemory& memory)
    : _terms{memory.Arena()}, _walked{memory.Scratch()} {}

  void Prepare(const TermReader* reader) {
    SDB_ASSERT(reader);
    SDB_ASSERT(!_reader || _reader == reader);
    _reader = reader;
  }

  bool Empty() const noexcept { return _terms.empty() && _walked.empty(); }
  const auto* Reader() const noexcept { return _reader; }

  void Push(const PostingMeta& cookie, score_t boost,
            const byte_type* stats = nullptr) {
    _walked.emplace_back(cookie, boost, stats);
  }

  auto& Walked() noexcept { return _walked; }

  void Seal() {
    _terms.reserve(_walked.size());
    _terms.assign(_walked.begin(), _walked.end());
    _walked = memory::ArenaVector<Entry>{_walked.get_allocator()};
  }

  const auto& Terms() const noexcept { return _terms; }
  auto TermsSize() const { return _terms.size(); }

 private:
  const TermReader* _reader = nullptr;

  memory::ArenaVector<Entry> _terms;
  memory::ArenaVector<Entry> _walked;
};

}  // namespace irs
