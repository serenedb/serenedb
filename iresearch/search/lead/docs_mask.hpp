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
#include <bit>

#include "iresearch/index/document_mask.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/window.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

class DocsMask {
 public:
  DocsMask(const DocumentMask* mask, doc_id_t visible_end,
           doc_id_t docs_count) noexcept
    : _words{mask != nullptr ? mask->Words() : nullptr},
      _count{mask != nullptr ? static_cast<uint32_t>(mask->WordCount()) : 0},
      _end{std::min(static_cast<doc_id_t>(kMin + docs_count), visible_end)},
      _rest{Live(0)} {}

  explicit DocsMask(const SubReader& segment) noexcept
    : DocsMask{segment.docs_mask(), segment.Meta().visible_end,
               static_cast<doc_id_t>(segment.docs_count())} {}

  doc_id_t Next() noexcept {
    if (doc_limits::eof(_doc)) [[unlikely]] {
      return _doc;
    }
    return Scan();
  }

  doc_id_t Seek(doc_id_t target) noexcept {
    if (target <= _doc) {
      return _doc;
    }
    if (target >= _end) [[unlikely]] {
      return _doc = doc_limits::eof();
    }
    const auto offset = target - kMin;
    const auto word = static_cast<uint32_t>(offset / kBits);
    if (word != _word) {
      _word = word;
      _rest = Live(word);
    }
    _rest &= ~uint64_t{0} << (offset % kBits);
    return Scan();
  }

 private:
  static constexpr auto kBits = detail::kWindowBits;
  static constexpr doc_id_t kMin = doc_limits::min();

  uint64_t Live(uint32_t word) const noexcept {
    return word < _count ? ~_words[word] : ~uint64_t{0};
  }

  doc_id_t Scan() noexcept {
    while (_rest == 0) {
      if (kMin + (size_t{_word} + 1) * kBits >= _end) [[unlikely]] {
        return _doc = doc_limits::eof();
      }
      _rest = Live(++_word);
    }
    const auto doc =
      static_cast<doc_id_t>(kMin + size_t{_word} * kBits +
                            static_cast<size_t>(std::countr_zero(_rest)));
    if (doc >= _end) [[unlikely]] {
      return _doc = doc_limits::eof();
    }
    _rest &= _rest - 1;
    return _doc = doc;
  }

  const uint64_t* _words;
  uint32_t _count;
  doc_id_t _end;
  uint32_t _word = 0;
  uint64_t _rest;
  doc_id_t _doc = doc_limits::invalid();
};

}  // namespace irs::lead
