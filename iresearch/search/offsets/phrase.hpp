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
#include "iresearch/utils/type_limits.hpp"

namespace irs::offsets {

template<typename Slots>
class Phrase {
 public:
  template<typename... Args>
  explicit Phrase(Args&&... args) : _slots{std::forward<Args>(args)...} {}

  uint32_t Run(doc_id_t doc, std::span<Range> out) {
    if (doc != _doc) {
      _doc = doc;
      _standing = _slots.Seek(doc) == doc && _slots.Match(doc);
    }
    return Read(out);
  }

 private:
  uint32_t Read(std::span<Range> out) {
    uint32_t count = 0;
    while (_standing && count != out.size()) {
      const auto [start, end] = _slots.Offsets();
      out[count++] = {start, end};
      _standing = _slots.NextAlignment();
    }
    return count;
  }

  Slots _slots;
  doc_id_t _doc = doc_limits::invalid();
  bool _standing = false;
};

}  // namespace irs::offsets
