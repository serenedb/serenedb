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

#include <utility>

#include "iresearch/search/docs/root.hpp"
#include "iresearch/search/lead/concept.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::docs {

template<lead::Type Node>
class Walk : public Root {
 public:
  template<typename... Args>
  explicit Walk(Args&&... args) : _node{std::forward<Args>(args)...} {}

  uint32_t Run(doc_id_t min, doc_id_t max, doc_id_t* IRS_RESTRICT out) final {
    uint32_t n = 0;
    auto doc = _node.Seek(min);
    while (doc < max) {
      out[n++] = doc;
      doc = _node.Next();
    }
    return n;
  }

 private:
  Node _node;
};

}  // namespace irs::docs
