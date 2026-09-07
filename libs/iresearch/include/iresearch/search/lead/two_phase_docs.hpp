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

#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

template<typename Slots>
class TwoPhaseDocs {
 public:
  template<typename... Args>
  explicit TwoPhaseDocs(Args&&... args) : _slots{std::forward<Args>(args)...} {}

  doc_id_t Value() const noexcept { return _doc; }

  doc_id_t Advance() { return Converge(_slots.Next(_doc)); }

  doc_id_t Seek(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    return Converge(_slots.Seek(target));
  }

 private:
  doc_id_t Converge(doc_id_t target) {
    while (!doc_limits::eof(target)) {
      if (_slots.Match(target)) {
        return _doc = target;
      }
      target = _slots.Next(target);
    }
    return _doc = target;
  }

  Slots _slots;
  doc_id_t _doc = doc_limits::invalid();
};

}  // namespace irs::lead
