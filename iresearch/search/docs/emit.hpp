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

#include "iresearch/search/detail/window.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::docs {

class Emit {
 public:
  IRS_FORCE_INLINE void Opened(doc_id_t base, doc_id_t max,
                               uint64_t* words) noexcept {
    _words = words;
    _base = base;
    _word = 0;
    _words_end = static_cast<uint32_t>(detail::WindowWords(base, max));
  }

  IRS_FORCE_INLINE bool Pending() const noexcept { return _word != _words_end; }

  IRS_FORCE_INLINE void SkipTo(doc_id_t begin) noexcept {
    if (begin <= _base) {
      return;
    }
    const uint64_t off = begin - _base;
    const auto word = static_cast<uint32_t>(off / detail::kWindowBits);
    if (word >= _words_end) {
      for (; _word != _words_end; ++_word) {
        _words[_word] = 0;
      }
      return;
    }
    for (; _word < word; ++_word) {
      _words[_word] = 0;
    }
    if (_word == word) {
      if (const auto bit = off % detail::kWindowBits; bit != 0) {
        _words[word] &= ~((uint64_t{1} << bit) - 1);
      }
    }
  }

  IRS_FORCE_INLINE void Drain(doc_id_t* IRS_RESTRICT out, uint32_t& n,
                              doc_id_t end) noexcept {
    const uint64_t avail = end > _base ? end - _base : 0;
    const auto limit = static_cast<uint32_t>(
      std::min<uint64_t>(_words_end, avail / detail::kWindowBits));
    [[clang::code_align(64)]] for (; _word != limit; ++_word) {
      const auto word = _words[_word];
      if (word == 0) {
        continue;
      }
      _words[_word] = 0;
      const auto base = _base + _word * detail::kWindowBits;
      n = static_cast<uint32_t>(MaterializeWord(base, word, out + n) - out);
    }
    if (_word == _words_end) {
      return;
    }
    const auto tail = avail % detail::kWindowBits;
    if (tail == 0) {
      return;
    }
    const auto base = _base + _word * detail::kWindowBits;
    auto word = _words[_word] & ((uint64_t{1} << tail) - 1);
    if (word != 0) {
      _words[_word] ^= word;
      n = static_cast<uint32_t>(MaterializeWord(base, word, out + n) - out);
    }
  }

 private:
  uint64_t* _words = nullptr;
  uint32_t _word = 0;
  uint32_t _words_end = 0;
  doc_id_t _base = 0;
};

}  // namespace irs::docs
