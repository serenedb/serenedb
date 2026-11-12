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
#include <utility>
#include <vector>

#include "iresearch/search/docs/root.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::docs {

class SortedDocs : public Root {
 public:
  explicit SortedDocs(std::vector<doc_id_t>&& docs) noexcept
    : _docs{std::move(docs)} {
    SDB_ASSERT(!_docs.empty() && doc_limits::eof(_docs.back()));
  }

  uint32_t Run(doc_id_t min, doc_id_t max, doc_id_t* IRS_RESTRICT out) final {
    const auto* const begin = _docs.data();
    const auto* first = begin + _at;
    if (*first < min) {
      first = std::lower_bound(first, begin + _docs.size(), min);
    }
    const auto* last = first;
    while (*last < max) {
      ++last;
    }
    std::copy(first, last, out);
    _at = static_cast<size_t>(last - begin);
    return static_cast<uint32_t>(last - first);
  }

  doc_id_t NextDoc() const noexcept final { return _docs[_at]; }

 private:
  std::vector<doc_id_t> _docs;
  size_t _at = 0;
};

}  // namespace irs::docs
