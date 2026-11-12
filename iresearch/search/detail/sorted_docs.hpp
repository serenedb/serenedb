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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <span>

#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

class SortedDocs {
 public:
  explicit SortedDocs(std::span<const doc_id_t> docs) noexcept : _docs{docs} {}

  doc_id_t Next() noexcept {
    if (_at < _docs.size()) {
      return _doc = _docs[_at++];
    }
    return _doc = doc_limits::eof();
  }

  doc_id_t Seek(doc_id_t target) noexcept {
    if (target <= _doc) {
      return _doc;
    }
    size_t step = 1;
    auto low = _at;
    while (low + step < _docs.size() && _docs[low + step] < target) {
      low += step;
      step *= 2;
    }
    const auto window = _docs.subspan(low, std::min(step, _docs.size() - low));
    _at = low + static_cast<size_t>(absl::c_lower_bound(window, target) -
                                    window.begin());
    return Next();
  }

  doc_id_t Probe(doc_id_t target) noexcept { return Seek(target); }

 private:
  std::span<const doc_id_t> _docs;
  size_t _at = 0;
  doc_id_t _doc = doc_limits::invalid();
};

}  // namespace irs::detail
