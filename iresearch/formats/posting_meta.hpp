////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <array>
#include <cstdint>
#include <cstring>

#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

struct PostingMeta {
  static constexpr uint32_t kInlineBytes = 32;
  static constexpr uint32_t kInlineSlack = 16;

  using InlineBytes = std::array<byte_type, kInlineBytes + kInlineSlack>;

  void clear() noexcept {
    docs_count = freq = 0;
    doc_start = pos_start = pay_start = 0;
    doc_delta = 0;
    pos_extent = pay_extent = 0;
    pos_offset = 0;
    inline_size = 0;
  }

  bytes_view Inline() const noexcept {
    return {inline_bytes.data(), inline_size};
  }
  void SetInline(bytes_view bytes) noexcept {
    SDB_ASSERT(bytes.size() <= kInlineBytes);
    inline_size = static_cast<uint8_t>(bytes.size());
    if (!bytes.empty()) {
      std::memcpy(inline_bytes.data(), bytes.data(), bytes.size());
    }
  }

  bool operator==(const PostingMeta& other) const noexcept {
    return docs_count == other.docs_count && freq == other.freq &&
           doc_start == other.doc_start && pos_start == other.pos_start &&
           pay_start == other.pay_start && doc_delta == other.doc_delta &&
           pos_extent == other.pos_extent && pay_extent == other.pay_extent &&
           pos_offset == other.pos_offset && Inline() == other.Inline();
  }

  uint32_t docs_count = 0;  // How many documents a particular term contains
  uint32_t freq = 0;  // How many times a particular term occur in documents
  uint64_t doc_start = 0;  // where this term's postings start in the .doc file
  uint64_t pos_start = 0;  // where this term's postings start in the .pos file
  uint64_t pay_start = 0;  // where this term's postings start in the .pay file
  uint32_t doc_delta = 0;
  uint32_t pos_extent = 0;
  uint32_t pay_extent = 0;
  uint16_t pos_offset = 0;
  uint8_t inline_size = 0;
  InlineBytes inline_bytes{};
};

// What a query over a term this segment does not have stands on.
inline constexpr PostingMeta kNoPosting;

}  // namespace irs
