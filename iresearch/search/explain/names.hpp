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

#include <array>
#include <cstdint>
#include <utility>

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/utils/containers/node_hash_map.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::explain {

struct PostingKey {
  PostingKey(const TermReader& reader, const PostingMeta& meta) noexcept;

  bool operator==(const PostingKey&) const noexcept = default;

  const TermReader* reader;
  uint64_t doc_start;
  uint64_t pos_start;
  uint64_t pay_start;
  uint32_t docs_count;
  uint32_t freq;
  uint32_t doc_delta;
  uint32_t pos_extent;
  uint32_t pay_extent;
  uint16_t pos_offset;
  uint8_t inline_size;
  std::array<byte_type, PostingMeta::kInlineBytes> inline_bytes{};
};

template<typename H>
H AbslHashValue(H state, const PostingKey& key) {
  return H::combine(std::move(state), key.reader, key.doc_start, key.pos_start,
                    key.pay_start, key.docs_count, key.freq, key.doc_delta,
                    key.pos_extent, key.pay_extent, key.pos_offset,
                    key.inline_size, key.inline_bytes);
}

class TermNames {
 public:
  void Add(const TermReader& reader, const PostingMeta& meta, bytes_view term);

  void AddAll(const TermReader& reader, TermIterator& terms);

  const bstring* Find(const TermReader& reader, const PostingMeta& meta) const;

 private:
  struct Name {
    bstring term;
    bool ambiguous = false;
  };

  containers::NodeHashMap<PostingKey, Name> _names;
};

}  // namespace irs::explain
