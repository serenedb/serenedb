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

#include "iresearch/formats/column/codecs/onpair_encoder.hpp"

#include <algorithm>

#include "iresearch/formats/column/codecs/string_layout.hpp"

namespace irs::codecs::text {

EncodedFrames OnPairEncoder::Finish(const SegmentEntries& rows,
                                    std::span<uint32_t> lengths) {
  const auto entries = rows.entries;
  const auto first_entry = rows.first_entry;
  _codec.Train(entries, rows.raw);
  uint64_t total = 0;
  for (const auto entry : entries) {
    total += entry.size();
  }
  _codes.resize(total);
  _codec.Encode(entries, _codes, lengths);
  _frames.clear();
  _data.clear();
  uint32_t max_codes = 0;
  uint64_t frame_raw = 0;
  size_t frame_first = 0;
  uint64_t frame_region = 0;
  uint64_t region = 0;
  const auto close = [&](size_t end) {
    const auto at = _data.size();
    Pack(entries, lengths, frame_first, end, frame_region);
    _frames.emplace_back(static_cast<uint32_t>(frame_first + first_entry),
                         static_cast<uint32_t>(frame_raw),
                         static_cast<uint32_t>(at),
                         static_cast<uint32_t>(_data.size() - at));
  };
  for (size_t i = 0; i < entries.size(); ++i) {
    if (frame_raw != 0 && frame_raw + entries[i].size() > kFsstFrameRawBytes) {
      close(i);
      frame_raw = 0;
      frame_first = i;
      frame_region = region;
    }
    max_codes = std::max(max_codes, lengths[i]);
    frame_raw += entries[i].size();
    region += entries[i].size();
  }
  if (!entries.empty()) {
    close(entries.size());
  }
  _codec.Export(_side);
  RecordRatio(_history[static_cast<size_t>(_shape)], rows.raw,
              _data.size() + _side.size(), _target);
  return {
    .codec = kCodec,
    .frames = _frames,
    .max_length = max_codes,
    .symtab = _side,
  };
}

void OnPairEncoder::Pack(std::span<const std::string_view> entries,
                         std::span<const uint32_t> lengths, size_t first,
                         size_t end, uint64_t region) {
  const auto bits = _codec.Bits();
  uint64_t acc = 0;
  uint32_t fill = 0;
  for (auto e = first; e < end; ++e) {
    const auto* codes = _codes.data() + region;
    for (uint32_t c = 0; c < lengths[e]; ++c) {
      acc |= uint64_t{codes[c]} << fill;
      fill += bits;
      while (fill >= 8) {
        _data.push_back(static_cast<char>(acc & 0xFF));
        acc >>= 8;
        fill -= 8;
      }
    }
    region += entries[e].size();
  }
  if (fill != 0) {
    _data.push_back(static_cast<char>(acc & 0xFF));
  }
  _data.append(kOnPairPadding, '\0');
}

}  // namespace irs::codecs::text
