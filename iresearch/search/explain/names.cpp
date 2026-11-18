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

#include <absl/algorithm/container.h>

#include "iresearch/utils/assert.hpp"

namespace irs::explain {

PostingKey::PostingKey(const TermReader& reader,
                       const PostingMeta& meta) noexcept
  : reader{&reader},
    doc_start{meta.doc_start},
    pos_start{meta.pos_start},
    pay_start{meta.pay_start},
    docs_count{meta.docs_count},
    freq{meta.freq},
    doc_delta{meta.doc_delta},
    pos_extent{meta.pos_extent},
    pay_extent{meta.pay_extent},
    pos_offset{meta.pos_offset},
    inline_size{meta.inline_size} {
  SDB_ASSERT(meta.inline_size <= PostingMeta::kInlineBytes);
  absl::c_copy(meta.Inline(), inline_bytes.begin());
}

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
