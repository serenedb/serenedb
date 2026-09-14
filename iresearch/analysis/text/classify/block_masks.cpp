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

#include "iresearch/analysis/text/classify/block_masks.hpp"

#include "iresearch/utils/utf8_utils.hpp"

namespace irs::analysis::classify {
namespace {

IRS_FORCE_INLINE inline uint32_t ClassifyUtf8LeadBlock(
  const byte_type* block) noexcept {
  const auto b = Load(block);
  return MoveMask((b & uint8_t{0xC0}) != uint8_t{0x80});
}

}  // namespace

size_t BuildUtf8CpBounds(const byte_type* data, size_t size, bool valid_utf8,
                         std::vector<uint32_t>& out) {
  if (out.size() < size + 1) {
    out.resize(size + 1);
  }
  uint32_t* bounds = out.data();
  size_t n = 0;
  if (valid_utf8) {
    DrainClassified(
      data, size, true,
      [](const byte_type* block)
        IRS_FORCE_INLINE { return ClassifyUtf8LeadBlock(block); },
      [](byte_type c) IRS_FORCE_INLINE { return (c & 0xC0) != 0x80; },
      [&](size_t pos)
        IRS_FORCE_INLINE { bounds[n++] = static_cast<uint32_t>(pos); });
  } else {
    const auto* end = data + size;
    for (const auto* it = data; it != end; it = utf8_utils::Next(it, end)) {
      bounds[n++] = static_cast<uint32_t>(it - data);
    }
  }
  bounds[n] = static_cast<uint32_t>(size);
  return n;
}

}  // namespace irs::analysis::classify
