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
#include "iresearch/formats/column/codecs/fsst.hpp"
#include "iresearch/formats/column/codecs/leaf_encoder.hpp"
#include "iresearch/formats/column/codecs/string_tuning.hpp"

namespace irs::codecs::text {

class FsstChainEncoder {
 public:
  static constexpr ByteCodec kCodec = ByteCodec::Fsst;

  explicit FsstChainEncoder(StringTuning& tuning, uint32_t target) noexcept
    : _history{tuning.history[static_cast<size_t>(kCodec)]}, _target{target} {}

  void Begin(Shape shape, uint8_t, FrameLayout) noexcept {
    _shape = shape;
    _data.clear();
  }

  void Retrain() noexcept { _codec.Reset(); }

  void Append(const duckdb::string_t&, uint32_t) noexcept {}

  uint64_t Estimate(const SegmentEntries& rows) const noexcept;

  EncodedFrames Finish(const SegmentEntries& rows, std::span<uint32_t> lengths);

  std::string& Data() noexcept { return _data; }

 private:
  RatioHistory* _history;
  uint32_t _target;
  Shape _shape = Shape::Dedup;
  FsstEncoder _codec;
  std::vector<FrameMeta> _frames;
  std::string _data;
  std::vector<uint32_t> _entry_lengths;
  std::vector<std::string_view> _suffixes;
  std::vector<uint32_t> _lcp_stream;
};

}  // namespace irs::codecs::text
