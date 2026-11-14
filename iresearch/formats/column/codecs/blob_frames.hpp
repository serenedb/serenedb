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

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/codecs/string_tuning.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs::text {

inline constexpr size_t kPriceFrames = 8;
inline constexpr size_t kProbeFrames = 1;
inline constexpr size_t kLocalMinFrames = 8;

struct FrameShape {
  size_t frame;
  size_t first;
};

template<typename Codec>
constexpr FrameShape FrameShapeOf(FrameLayout layout) noexcept {
  if (layout == FrameLayout::Wide) {
    return {kFrameRawBytes, 0};
  }
  if constexpr (Codec::kCodec == ByteCodec::Zstd) {
    return {kZstdDictionaryFrameBytes, kFrameDictionaryBytes};
  } else {
    return {kDictionaryFrameBytes, kFrameDictionaryBytes};
  }
}

class FrameSpans {
 public:
  struct Span {
    uint64_t first = 0;
    uint64_t end = 0;
    uint64_t offset = 0;
    uint64_t raw = 0;
  };

  void Reset(FrameShape shape) noexcept {
    _shape = shape;
    _spans.clear();
    _open = {};
  }

  IRS_FORCE_INLINE bool Add(uint64_t entry, uint64_t size) {
    const auto limit =
      _spans.empty() && _shape.first != 0 ? _shape.first : _shape.frame;
    const bool closed = _open.end != _open.first && _open.raw + size > limit;
    if (closed) {
      _spans.push_back(_open);
      _open = {.offset = _open.offset + _open.raw};
    }
    if (_open.end == _open.first) {
      _open.first = entry;
    }
    _open.end = entry + 1;
    _open.raw += size;
    return closed;
  }

  void Close() {
    if (Open()) {
      _spans.push_back(_open);
    }
    _open = {};
  }

  bool Open() const noexcept { return _open.end != _open.first; }

  FrameShape Shape() const noexcept { return _shape; }

  size_t size() const noexcept { return _spans.size(); }

  bool empty() const noexcept { return _spans.empty(); }

  const Span& operator[](size_t i) const noexcept { return _spans[i]; }

  std::span<const Span> All() const noexcept { return _spans; }

  template<typename Offset, typename Visit>
  void Sample(size_t count, Offset&& offset, Visit&& visit) const {
    const size_t rest = _spans.size() - 1;
    if (rest <= count) {
      for (size_t i = 1; i < _spans.size(); ++i) {
        visit(i);
      }
      return;
    }
    size_t last = 0;
    for (size_t j = 0; j < count; ++j) {
      const auto i =
        1 + static_cast<size_t>((static_cast<double>(j) + offset(j)) *
                                static_cast<double>(rest) /
                                static_cast<double>(count));
      if (i != last) {
        last = i;
        visit(i);
      }
    }
  }

 private:
  FrameShape _shape{kFrameRawBytes, 0};
  Span _open;
  std::vector<Span> _spans;
};

inline uint64_t Scale(uint64_t sampled, uint64_t sampled_raw,
                      uint64_t raw) noexcept {
  return sampled_raw == 0
           ? 0
           : static_cast<uint64_t>(static_cast<double>(sampled) *
                                   static_cast<double>(raw) /
                                   static_cast<double>(sampled_raw));
}

template<typename Codec>
size_t CompressedSize(std::string_view raw, std::string& scratch,
                      typename Codec::Compressor& codec) {
  const auto bound = Codec::Bound(raw.size());
  size_t n = 0;
  scratch.resize_and_overwrite(bound, [&](char* buf, size_t) {
    n = codec.Compress(raw.data(), raw.size(), buf, bound);
    return n;
  });
  return n;
}

}  // namespace irs::codecs::text
