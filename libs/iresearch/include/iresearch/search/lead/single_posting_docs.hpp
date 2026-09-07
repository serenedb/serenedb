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

#include "basics/assert.h"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

class SinglePostingDocs {
 public:
  explicit SinglePostingDocs(doc_id_t only) noexcept : _only{only} {
    SDB_ASSERT(doc_limits::valid(only));
  }

  SinglePostingDocs(const SinglePostingDocs&) = delete;
  SinglePostingDocs& operator=(const SinglePostingDocs&) = delete;
  SinglePostingDocs(SinglePostingDocs&&) = delete;
  SinglePostingDocs& operator=(SinglePostingDocs&&) = delete;

  doc_id_t Advance() noexcept {
    return _doc = _doc < _only ? _only : doc_limits::eof();
  }

  doc_id_t Seek(doc_id_t target) noexcept {
    if (target <= _doc) {
      return _doc;
    }
    return _doc = target <= _only ? _only : doc_limits::eof();
  }

 private:
  doc_id_t _only;
  doc_id_t _doc = doc_limits::invalid();
};

}  // namespace irs::lead
