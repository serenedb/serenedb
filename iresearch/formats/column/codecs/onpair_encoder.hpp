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
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/leaf_encoder.hpp"
#include "iresearch/formats/column/codecs/onpair.hpp"
#include "iresearch/formats/column/codecs/string_tuning.hpp"

namespace irs::codecs::text {

class OnPairEncoder {
 public:
  static constexpr ByteCodec kCodec = ByteCodec::OnPair;

  explicit OnPairEncoder(StringTuning& tuning, uint32_t target) noexcept
    : _history{tuning.history[static_cast<size_t>(kCodec)]}, _target{target} {}

  void Begin(Shape shape, uint8_t, FrameLayout, SegmentBuffer& out) noexcept {
    _shape = shape;
    _segment = &out;
    _base = out.Size();
  }

  void Retrain() noexcept {}

  void Append(const duckdb::string_t&, uint32_t) noexcept {}

  uint64_t Estimate(const SegmentEntries& rows) const noexcept {
    return EstimateEntries(rows, _history[static_cast<size_t>(_shape)]);
  }

  EncodedFrames Finish(const SegmentEntries& rows, std::span<uint32_t> lengths);

 private:
  uint64_t Written() const noexcept { return _segment->Size() - _base; }

  void Pack(std::span<const duckdb::string_t> entries,
            std::span<const uint32_t> lengths, size_t first, size_t end,
            uint64_t region);

  RatioHistory* _history;
  uint32_t _target;
  Shape _shape = Shape::Dedup;
  OnPairCodec _codec;
  SegmentBuffer* _segment = nullptr;
  size_t _base = 0;
  std::vector<FrameMeta> _frames;
  std::string _side;
  std::vector<uint16_t> _codes;
};

}  // namespace irs::codecs::text
