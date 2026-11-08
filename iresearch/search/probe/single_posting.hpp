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

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::probe {

class SinglePostingDocs {
 public:
  explicit SinglePostingDocs(const PostingMeta& meta) noexcept
    : _doc{doc_limits::min() + meta.doc_delta} {
    SDB_ASSERT(meta.docs_count == 1);
  }

  SinglePostingDocs(const SinglePostingDocs&) = delete;
  SinglePostingDocs& operator=(const SinglePostingDocs&) = delete;
  SinglePostingDocs(SinglePostingDocs&&) = delete;
  SinglePostingDocs& operator=(SinglePostingDocs&&) = delete;

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) const noexcept {
    return target <= _doc ? _doc : doc_limits::eof();
  }

 private:
  doc_id_t _doc;
};

}  // namespace irs::probe
