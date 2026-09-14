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

#include <span>
#include <utility>

#include "iresearch/search/offsets/root.hpp"

namespace irs::offsets {

template<typename Leaf>
class Impl : public Root {
 public:
  template<typename... Args>
  explicit Impl(Args&&... args) : _leaf{std::forward<Args>(args)...} {}

  uint32_t Run(doc_id_t doc, std::span<Range> out) final {
    return _leaf.Run(doc, out);
  }

 private:
  Leaf _leaf;
};

}  // namespace irs::offsets
