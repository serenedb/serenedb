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

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/blob_frames.hpp"
#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/numeric.hpp"
#include "iresearch/formats/column/codecs/segment_rows.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/codecs/string_tuning.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::codecs::text {

template<typename Codec>
class BlobPricer {
 public:
  static constexpr ByteCodec kCodec = Codec::kCodec;
  static_assert(!Sorted(kCodec));

  uint64_t Price(const Profile& p, std::string_view bytes,
                 const FrameSpans& spans, uint64_t codes_bytes, uint8_t level,
                 FrameLayout layout, uint64_t begin) {
    if (!_codec || _level != level) {
      _codec.emplace(level);
      _level = level;
    } else {
      _codec->Clear();
    }
    uint64_t data = 0;
    if (!spans.empty()) {
      const bool whole =
        layout == FrameLayout::Local && spans.size() >= kLocalMinFrames;
      SDB_ASSERT(!whole || bytes.size() == p.raw);
      const auto frame = [&](size_t i) {
        return whole ? bytes.substr(spans[i].offset, spans[i].raw)
                     : Assemble(p, spans[i].first, spans[i].end);
      };
      bool trained = false;
      if (whole) {
        _sizes.clear();
        for (const auto& span : spans.All()) {
          _sizes.push_back(span.raw);
        }
        trained = _codec->Train(bytes, _sizes);
      }
      if (trained) {
        data += _codec->Stored().size();
      }
      const auto first = frame(0);
      data += CompressedSize<Codec>(first, _scratch, *_codec);
      if (!trained && spans.Shape().first != 0 && !first.empty() &&
          spans.size() > 1) {
        _codec->Use(first);
      }
      uint64_t sampled_raw = 0;
      uint64_t sampled = 0;
      spans.Sample(
        kPriceFrames, [&](size_t j) { return Jitter(begin, j); },
        [&](size_t i) {
          const auto raw = frame(i);
          sampled_raw += raw.size();
          sampled += CompressedSize<Codec>(raw, _scratch, *_codec);
        });
      data += Scale(sampled, sampled_raw, p.raw - spans[0].raw);
    }
    const auto entry_count = p.codes.size() + (p.shape == Shape::Dedup ? 1 : 0);
    Header h;
    return LayOut(h, data, spans.size(), Packed(entry_count, p.max_len), 0,
                  codes_bytes, 0);
  }

 private:
  static double Jitter(uint64_t seed, uint64_t stratum) noexcept {
    uint64_t z = seed * 0x9E3779B97F4A7C15ULL + stratum + 1;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    z ^= z >> 31;
    return static_cast<double>(z >> 11) * 0x1.0p-53;
  }

  std::string_view Assemble(const Profile& p, size_t first, size_t end) {
    _frame.clear();
    for (auto k = first; k < end; ++k) {
      const auto entry = EntryOf(p, k);
      _frame.append(entry.GetData(), entry.GetSize());
    }
    return _frame;
  }

  std::optional<typename Codec::Compressor> _codec;
  uint8_t _level = 0;
  std::string _frame;
  std::string _scratch;
  std::vector<size_t> _sizes;
};

}  // namespace irs::codecs::text
