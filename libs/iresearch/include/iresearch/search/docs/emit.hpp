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
#include <cstdint>

#include "basics/bit_utils.hpp"
#include "iresearch/search/common/table_filter.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::docs {

template<typename Table>
class Emit {
 public:
  explicit Emit(Table table) noexcept : _table{table} {}

  IRS_FORCE_INLINE void Opened(doc_id_t base, uint64_t* words) noexcept {
    _words = words;
    _base = base;
    _word = 0;
  }

  IRS_FORCE_INLINE bool Skip(doc_id_t& min) const { return _table.Skip(min); }

  IRS_FORCE_INLINE bool Drain(doc_id_t* IRS_RESTRICT out, uint32_t capacity,
                              uint32_t& n) noexcept {
    [[clang::code_align(64)]] for (; _word != search::kWindowWords; ++_word) {
      const auto word = _words[_word];
      if (word == 0) {
        continue;
      }
      if (n + search::kWindowBits > capacity) [[unlikely]] {
        if (n + static_cast<uint32_t>(std::popcount(word)) > capacity) {
          return false;
        }
      }
      _words[_word] = 0;
      n = static_cast<uint32_t>(
        MaterializeWord(_base + _word * search::kWindowBits, word, out + n) -
        out);
    }
    return true;
  }

 private:
  uint64_t* _words = nullptr;
  uint32_t _word = search::kWindowWords;
  doc_id_t _base = 0;
  [[no_unique_address]] search::Narrowing<Table> _table;
};

}  // namespace irs::docs
