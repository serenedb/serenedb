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

#include "iresearch/formats/column/codecs/string_compressor.hpp"

namespace irs::codecs::text {

PooledSegment StringCompressor::Acquire() {
  if (_free.empty()) {
    auto* seg = _pool.emplace_back(std::make_unique<Segment>(_buffers)).get();
    _free.reserve(_pool.size());
    return {seg, {&_free}};
  }
  auto* seg = _free.back();
  _free.pop_back();
  return {seg, {&_free}};
}

}  // namespace irs::codecs::text
