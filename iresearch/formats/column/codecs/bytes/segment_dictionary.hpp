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
#include <cstring>
#include <span>
#include <string>
#include <string_view>

namespace irs::codecs {

inline constexpr uint64_t kSegmentDictionaryShare = 25;
inline constexpr uint64_t kSegmentDictionaryMinBytes = 256;

constexpr size_t SegmentDictionaryBytes(uint64_t raw, uint64_t cap) noexcept {
  return static_cast<size_t>(
    std::clamp(raw / kSegmentDictionaryShare, kSegmentDictionaryMinBytes, cap));
}

void TrainDictionary(std::string_view frames, std::span<const size_t> sizes,
                     size_t cap, std::string& dictionary);

template<typename Compress>
void PackDictionary(std::string_view dictionary, size_t bound,
                    std::string& stored, Compress&& compress) {
  stored.resize_and_overwrite(bound, [&](char* buf, size_t capacity) {
    const auto n =
      compress(dictionary.data(), dictionary.size(), buf, capacity);
    return n < dictionary.size() ? n : 0;
  });
  if (stored.empty()) {
    stored.assign(dictionary);
  }
}

template<typename Decode>
bool FillDictionary(std::string& window, size_t& dictionary, size_t size,
                    Decode&& decode) {
  bool ok = false;
  window.resize_and_overwrite(size, [&](char* buf, size_t n) {
    ok = decode(buf);
    return ok ? n : 0;
  });
  dictionary = window.size();
  return ok;
}

template<typename Decompress>
bool UnpackDictionary(std::string_view stored, char* dst, size_t size,
                      Decompress&& decompress) {
  if (stored.size() == size) {
    std::memcpy(dst, stored.data(), size);
    return true;
  }
  return decompress(stored.data(), stored.size(), dst, size);
}

inline char* FrameAfterDictionary(std::string& window, size_t dictionary,
                                  size_t size) {
  window.resize_and_overwrite(dictionary + size,
                              [](char*, size_t n) { return n; });
  return window.data() + dictionary;
}

}  // namespace irs::codecs
