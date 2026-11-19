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

#include "iresearch/search/explain/names.hpp"

namespace irs::explain {

PostingKey::PostingKey(const TermReader& reader,
                       const PostingMeta& meta) noexcept
  : reader{&reader},
    doc_start{meta.doc_start},
    inline_data{meta.inline_size != 0 ? meta.inline_data : nullptr},
    pos_start{meta.pos_start},
    doc_delta{meta.doc_delta},
    freq{meta.freq},
    pos_offset{meta.pos_offset} {}

void TermNames::Add(const TermReader& reader, const PostingMeta& meta,
                    bytes_view term) {
  auto [it, added] = _names.try_emplace(PostingKey{reader, meta});
  if (added) {
    it->second.term = term;
  } else if (it->second.term != term) {
    it->second.ambiguous = true;
  }
}

void TermNames::AddAll(const TermReader& reader, TermIterator& terms) {
  while (terms.next()) {
    Add(reader, terms.cookie(), terms.value());
  }
}

const bstring* TermNames::Find(const TermReader& reader,
                               const PostingMeta& meta) const {
  const auto it = _names.find(PostingKey{reader, meta});
  if (it == _names.end() || it->second.ambiguous) {
    return nullptr;
  }
  return &it->second.term;
}

}  // namespace irs::explain
