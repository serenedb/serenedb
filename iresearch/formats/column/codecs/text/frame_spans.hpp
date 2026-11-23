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
#include <vector>

#include "iresearch/formats/column/codecs/codec_type.hpp"
#include "iresearch/formats/column/codecs/text/string_layout.hpp"
#include "iresearch/formats/column/codecs/text/string_tuning.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs::text::blob {

struct FrameShape {
  size_t frame;
  size_t first;
};

template<typename Codec>
constexpr FrameShape FrameShapeOf(FrameDictionary layout) noexcept {
  if (layout == FrameDictionary::None) {
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
    const bool closed = _open.raw + size > limit && Cut();
    if (_open.end == _open.first) {
      _open.first = entry;
    }
    _open.end = entry + 1;
    _open.raw += size;
    return closed;
  }

  bool Cut() {
    if (!Open()) {
      return false;
    }
    _spans.push_back(_open);
    _open = {.offset = _open.offset + _open.raw};
    return true;
  }

  void Close() {
    if (Open()) {
      _spans.push_back(_open);
    }
    _open = {};
  }

  bool Open() const noexcept { return _open.end != _open.first; }

  const Span& OpenSpan() const noexcept { return _open; }

  FrameShape Shape() const noexcept { return _shape; }

  size_t size() const noexcept { return _spans.size(); }

  bool empty() const noexcept { return _spans.empty(); }

  const Span& operator[](size_t i) const noexcept { return _spans[i]; }

  void Sizes(std::vector<size_t>& sizes) const {
    sizes.clear();
    for (const auto& span : _spans) {
      sizes.push_back(span.raw);
    }
  }

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

}  // namespace irs::codecs::text::blob
