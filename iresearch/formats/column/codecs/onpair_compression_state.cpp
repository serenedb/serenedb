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

#include "iresearch/formats/column/codecs/onpair_compression_state.hpp"

#include <algorithm>
#include <cstring>

#include "iresearch/formats/column/codecs/string_compression_function.hpp"
#include "iresearch/formats/column/codecs/string_compression_loops.tpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"

namespace irs::codecs::text {

void OnPairCompressionState::FinalizeCompress() {
  if (_rows.SegmentShape() == Shape::Dedup) {
    _rows.Sort(_sort);
  }
  const auto lengths = _rows.Lengths();
  const auto rows = _rows.View();
  const auto entries = rows.entries;
  const auto first_entry = rows.first_entry;
  _codec.Train(entries, rows.raw);
  uint64_t total = 0;
  for (const auto& entry : entries) {
    total += entry.GetSize();
  }
  _codes.resize(total);
  _codec.Encode(entries, _codes, lengths);
  _frames.clear();
  uint32_t max_codes = 0;
  uint64_t frame_raw = 0;
  size_t frame_first = 0;
  uint64_t frame_region = 0;
  uint64_t region = 0;
  const auto close = [&](size_t end) {
    const auto at = Written();
    Pack(entries, lengths, frame_first, end, frame_region);
    _frames.emplace_back(static_cast<uint32_t>(frame_first + first_entry),
                         static_cast<uint32_t>(frame_raw),
                         static_cast<uint32_t>(at),
                         static_cast<uint32_t>(Written() - at));
  };
  for (size_t i = 0; i < entries.size(); ++i) {
    const auto size = entries[i].GetSize();
    if (frame_raw != 0 && frame_raw + size > kFsstFrameRawBytes) {
      close(i);
      frame_raw = 0;
      frame_first = i;
      frame_region = region;
    }
    max_codes = std::max(max_codes, lengths[i]);
    frame_raw += size;
    region += size;
  }
  if (!entries.empty()) {
    close(entries.size());
  }
  _codec.Export(_side);
  RecordRatio(_history[static_cast<size_t>(_rows.SegmentShape())], rows.raw,
              Written() + _side.size(), _target);
  _rows.Write(
    {
      .codec = kCodec,
      .frames = _frames,
      .max_length = max_codes,
      .symtab = _side,
    },
    *_out);
}

void OnPairCompressionState::Pack(std::span<const duckdb::string_t> entries,
                                  std::span<const uint32_t> lengths,
                                  size_t first, size_t end, uint64_t region) {
  const auto bits = _codec.Bits();
  uint64_t count = 0;
  for (auto e = first; e < end; ++e) {
    count += lengths[e];
  }
  const uint64_t bytes = (count * bits + 7) / 8;
  auto* out = _out->bytes.Extend(bytes + kOnPairPadding);
  uint64_t acc = 0;
  uint32_t fill = 0;
  for (auto e = first; e < end; ++e) {
    const auto* codes = _codes.data() + region;
    for (uint32_t c = 0; c < lengths[e]; ++c) {
      acc |= uint64_t{codes[c]} << fill;
      fill += bits;
      while (fill >= 8) {
        *out++ = static_cast<char>(acc & 0xFF);
        acc >>= 8;
        fill -= 8;
      }
    }
    region += entries[e].GetSize();
  }
  if (fill != 0) {
    *out++ = static_cast<char>(acc & 0xFF);
  }
  std::memset(out, 0, kOnPairPadding);
}

const StringCompressionFunction& OnPairCompressionFunction() {
  static constexpr auto kFunction =
    CompressionFunctionOf<OnPairCompressionState>(ByteCodec::OnPair);
  return kFunction;
}

}  // namespace irs::codecs::text
