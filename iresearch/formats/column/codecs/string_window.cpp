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

#include "iresearch/formats/column/codecs/string_window.hpp"

#include <absl/algorithm/container.h>

namespace irs::codecs::text {

uint64_t StringWindow::Price(const Config& config) {
  if (const auto* seg = Encoded(config)) {
    return seg->Size();
  }
  return _encoder.Price(config, _acc, _begin, _end);
}

Segment& StringWindow::Encode(const Config& config) {
  if (auto* seg = Encoded(config)) {
    return *seg;
  }
  return *_encoded.emplace_back(_encoder.Encode(config, _acc, _begin, _end));
}

Segment* StringWindow::Encoded(const Config& config) const noexcept {
  const auto it = absl::c_find_if(
    _encoded, [&](const Segment* seg) { return seg->config == config; });
  return it == _encoded.end() ? nullptr : *it;
}

void StringWindow::Keep(const Segment& kept) {
  for (auto* seg : _encoded) {
    if (seg != &kept) {
      _encoder.Release(seg);
    }
  }
  _encoded.clear();
}

}  // namespace irs::codecs::text
