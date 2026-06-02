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

#include <limits>

#include "iresearch/formats/seek_cookie.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {

struct TermReader;

class MultiTermState {
 public:
  static constexpr uint32_t kUnscored = std::numeric_limits<uint32_t>::max();

  struct Entry {
    SeekCookie::ptr cookie;
    uint32_t docs_count = 0;
    score_t boost = kNoBoost;
    uint32_t stat_offset = kUnscored;
  };

  explicit MultiTermState(IResourceManager& memory) noexcept
    : _terms{{memory}} {}

  void Prepare(const TermReader* reader) {
    SDB_ASSERT(reader);
    SDB_ASSERT(!_reader || _reader == reader);
    _reader = reader;
  }

  // Return true if state is empty
  bool Empty() const noexcept { return _terms.empty(); }
  const auto* Reader() const noexcept { return _reader; }

  void Push(Entry&& entry) {
    _terms.emplace_back(std::move(entry));
    _estimation += _terms.back().docs_count;
  }

  auto& Terms() noexcept { return _terms; }
  const auto& Terms() const noexcept { return _terms; }
  auto TermsSize() const { return _terms.size(); }

 private:
  // Reader using for iterate over the terms
  const TermReader* _reader = nullptr;

  ManagedVector<Entry> _terms;
  CostAttr::Type _estimation = 0;
};

}  // namespace irs
