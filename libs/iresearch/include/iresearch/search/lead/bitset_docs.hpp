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

#include <bit>
#include <utility>

#include "iresearch/search/common/bitset_storage.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

class BitsetDocs {
 public:
  static constexpr auto kBits = search::BitsetStorage::kBits;

  explicit BitsetDocs(search::BitsetStorage&& set) noexcept
    : _set{std::move(set)},
      _words{_set.Words()},
      _count{_set.WordCount()},
      _rest{_words[0]} {}

  doc_id_t Value() const noexcept { return _doc; }

  doc_id_t Advance() {
    if (doc_limits::eof(_doc)) [[unlikely]] {
      return _doc;
    }
    return Next();
  }

  doc_id_t Seek(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    const auto word = static_cast<uint32_t>(target / kBits);
    if (word != _word) {
      if (word >= _count) [[unlikely]] {
        _word = _count;
        _rest = 0;
        return _doc = doc_limits::eof();
      }
      _word = word;
      _rest = _words[word];
    }
    _rest &= ~uint64_t{0} << (target % kBits);
    return Next();
  }

 private:
  doc_id_t Next() noexcept {
    while (_rest == 0) {
      if (++_word >= _count) [[unlikely]] {
        _word = _count;
        return _doc = doc_limits::eof();
      }
      _rest = _words[_word];
    }
    _doc = static_cast<doc_id_t>(size_t{_word} * kBits +
                                 static_cast<size_t>(std::countr_zero(_rest)));
    _rest &= _rest - 1;
    return _doc;
  }

  search::BitsetStorage _set;
  const uint64_t* _words;
  uint32_t _count;
  uint32_t _word = 0;
  uint64_t _rest;
  doc_id_t _doc = doc_limits::invalid();
};

}  // namespace irs::lead
