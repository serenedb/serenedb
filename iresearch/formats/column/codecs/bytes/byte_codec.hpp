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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

#include "iresearch/formats/column/codecs/bytes/segment_dictionary.hpp"
#include "iresearch/formats/column/codecs/codec_type.hpp"
#include "iresearch/utils/tracked.hpp"

namespace irs::codecs {

template<typename Codec>
constexpr uint8_t EffectiveLevel(uint8_t level) noexcept {
  return level ? std::min(level, Codec::kMaxLevel) : Codec::kDefaultLevel;
}

template<typename Codec>
constexpr bool HighCompression(uint8_t level) noexcept {
  return EffectiveLevel<Codec>(level) > Codec::kDefaultLevel;
}

class DictionaryCompressor {
 public:
  template<typename Self>
  bool Train(this Self& self, std::string_view frames,
             std::span<const size_t> sizes) {
    DictionaryCompressor& base = self;
    TrainDictionary(frames, sizes, base._cap, base._dictionary);
    base._stored.clear();
    if (base._dictionary.empty()) {
      return false;
    }
    PackDictionary(
      base._dictionary, self.StoredBound(base._dictionary.size()), base._stored,
      [&](const char* src, size_t size, char* dst, size_t capacity) {
        return self.CompressStored(src, size, dst, capacity);
      });
    self.Attach();
    return true;
  }

  template<typename Self>
  void Use(this Self& self, std::string_view raw) {
    DictionaryCompressor& base = self;
    base._dictionary.assign(raw);
    base._stored.clear();
    self.Attach();
  }

  void Clear() noexcept {
    _dictionary.clear();
    _stored.clear();
  }

  std::string_view Dictionary() const noexcept { return _dictionary; }

  std::string_view Stored() const noexcept { return _stored; }

 protected:
  explicit DictionaryCompressor(TrackedAllocator<> alloc, size_t cap) noexcept
    : _cap{cap}, _dictionary{alloc}, _stored{alloc} {}

  TrackedAllocator<> Allocator() const noexcept {
    return _dictionary.get_allocator();
  }

 private:
  size_t _cap;
  TrackedString _dictionary;
  TrackedString _stored;
};

class DictionaryDecompressor {
 public:
  template<typename Self>
  bool Load(this Self& self, std::string_view stored, size_t size) {
    DictionaryDecompressor& base = self;
    return FillDictionary(base._window, base._dictionary, size, [&](char* buf) {
      return UnpackDictionary(
        stored, buf, size,
        [&](const char* src, size_t n, char* dst, size_t raw_size) {
          return self.DecompressStored(src, n, dst, raw_size);
        });
    });
  }

  template<typename Self>
  bool LoadFrame(this Self& self, const char* src, size_t size,
                 size_t raw_size) {
    DictionaryDecompressor& base = self;
    return FillDictionary(
      base._window, base._dictionary, raw_size,
      [&](char* buf) { return self.Decompress(src, size, buf, raw_size); });
  }

  std::string_view Dictionary() const noexcept {
    return {_window.data(), _dictionary};
  }

 protected:
  explicit DictionaryDecompressor(TrackedAllocator<> alloc) noexcept
    : _window{alloc} {}

  TrackedAllocator<> Allocator() const noexcept {
    return _window.get_allocator();
  }

  bool HasDictionary() const noexcept { return _dictionary != 0; }

  char* FrameAfterDictionary(size_t size) {
    return codecs::FrameAfterDictionary(_window, _dictionary, size);
  }

 private:
  TrackedString _window;
  size_t _dictionary = 0;
};

}  // namespace irs::codecs
