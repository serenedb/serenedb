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

#include "iresearch/search/docs/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::docs {

class SinglePosting : public Root {
 public:
  explicit SinglePosting(doc_id_t doc) noexcept : _doc{doc} {
    SDB_ASSERT(doc_limits::valid(doc));
  }

  uint32_t Run(doc_id_t* out, uint32_t capacity) final {
    SDB_ASSERT(capacity != 0);
    if (!doc_limits::valid(_doc)) {
      return 0;
    }
    out[0] = _doc;
    _doc = doc_limits::invalid();
    return 1;
  }

 private:
  doc_id_t _doc;
};

}  // namespace irs::docs
