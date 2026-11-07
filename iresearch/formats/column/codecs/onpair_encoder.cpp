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

EncodedFrames OnPairEncoder::Finish(const SegmentBuilder& rows,
                                    std::span<uint32_t> lengths) {
  const auto entries = rows.Entries();
  const auto first_entry = rows.FirstEntry();
  _codec.Train(entries, rows.Raw());
  _codes.clear();
  _frames.clear();
  _data.clear();
  uint32_t max_codes = 0;
  uint64_t frame_raw = 0;
  size_t frame_first = 0;
  size_t frame_code = 0;
  const auto close = [&](size_t end_code) {
    const auto at = _data.size();
    Pack(frame_code, end_code);
    _frames.emplace_back(static_cast<uint32_t>(frame_first + first_entry),
                         static_cast<uint32_t>(frame_raw),
                         static_cast<uint32_t>(at),
                         static_cast<uint32_t>(_data.size() - at));
  };
  for (size_t i = 0; i < entries.size(); ++i) {
    if (frame_raw != 0 && frame_raw + entries[i].size() > kFsstFrameRawBytes) {
      close(_codes.size());
      frame_raw = 0;
      frame_first = i;
      frame_code = _codes.size();
    }
    const auto before = _codes.size();
    _codec.Encode(entries[i], _codes);
    lengths[i] = static_cast<uint32_t>(_codes.size() - before);
    max_codes = std::max(max_codes, lengths[i]);
    frame_raw += entries[i].size();
  }
  if (!entries.empty()) {
    close(_codes.size());
  }
  _codec.Export(_side);
  RecordRatio(_history[static_cast<size_t>(_shape)], rows.Raw(),
              _data.size() + _side.size(), _target);
  return {
    .codec = kCodec,
    .frames = _frames,
    .max_length = max_codes,
    .symtab = _side,
  };
}

void OnPairEncoder::Pack(size_t first, size_t end) {
  const auto bits = _codec.Bits();
  uint64_t acc = 0;
  uint32_t fill = 0;
  for (auto i = first; i < end; ++i) {
    acc |= uint64_t{_codes[i]} << fill;
    fill += bits;
    while (fill >= 8) {
      _data.push_back(static_cast<char>(acc & 0xFF));
      acc >>= 8;
      fill -= 8;
    }
  }
  if (fill != 0) {
    _data.push_back(static_cast<char>(acc & 0xFF));
  }
  _data.append(kOnPairPadding, '\0');
}

}  // namespace irs::codecs::text
