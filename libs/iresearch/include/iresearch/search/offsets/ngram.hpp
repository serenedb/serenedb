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
#include <span>
#include <utility>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/search/offsets/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::offsets {

template<typename Slots>
class NGram {
 public:
  template<typename... Args>
  explicit NGram(Args&&... args) : _slots{std::forward<Args>(args)...} {}

  uint32_t Run(doc_id_t doc, std::span<Range> out) {
    if (doc != _doc) {
      _doc = doc;
      _found = {};
      _read = 0;
      if (_slots.Seek(doc) == doc && _slots.Match(doc)) {
        _found = _slots.Offsets();
      }
    }
    return Read(out);
  }

 private:
  uint32_t Read(std::span<Range> out) {
    const auto count =
      static_cast<uint32_t>(std::min(_found.size() - _read, out.size()));
    for (uint32_t i = 0; i != count; ++i) {
      const auto& found = _found[_read + i];
      out[i] = {found.start, found.end};
    }
    _read += count;
    return count;
  }

  Slots _slots;
  doc_id_t _doc = doc_limits::invalid();
  std::span<const OffsAttr> _found;
  size_t _read = 0;
};

}  // namespace irs::offsets
