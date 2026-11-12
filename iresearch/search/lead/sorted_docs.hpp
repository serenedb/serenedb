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

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

class SortedDocs {
 public:
  explicit SortedDocs(std::vector<doc_id_t>&& docs) noexcept
    : _docs{std::move(docs)} {
    SDB_ASSERT(!_docs.empty() && doc_limits::eof(_docs.back()));
  }

  SortedDocs(SortedDocs&&) = delete;
  SortedDocs& operator=(SortedDocs&&) = delete;

  doc_id_t Next() noexcept {
    if (doc_limits::eof(_doc)) [[unlikely]] {
      return _doc;
    }
    return _doc = _docs[_at++];
  }

  doc_id_t Seek(doc_id_t target) noexcept {
    if (target <= _doc) {
      return _doc;
    }
    const auto* const begin = _docs.data();
    const auto* const it =
      std::lower_bound(begin + _at, begin + _docs.size(), target);
    _at = static_cast<size_t>(it - begin) + 1;
    return _doc = *it;
  }

 private:
  std::vector<doc_id_t> _docs;
  size_t _at = 0;
  doc_id_t _doc = doc_limits::invalid();
};

}  // namespace irs::lead
