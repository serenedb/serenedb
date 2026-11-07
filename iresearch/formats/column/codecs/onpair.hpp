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

#include <absl/container/flat_hash_map.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace irs::codecs {

inline constexpr size_t kOnPairMaxToken = 16;
inline constexpr size_t kOnPairSideHeader = 8;
inline constexpr size_t kOnPairPadding = 2;
inline constexpr uint8_t kOnPairDictionaryBits = 12;

class OnPairTable {
 public:
  bool Open(std::string_view side);

  uint8_t Bits() const noexcept { return _bits; }
  uint64_t Bytes() const noexcept { return _bytes.size(); }
  uint64_t Capacity(uint64_t codes) const noexcept {
    return codes * kOnPairMaxToken;
  }

  uint64_t Decode(const uint8_t* stream, uint64_t first, uint64_t count,
                  char* out, uint64_t capacity) const;

 private:
  std::string _bytes;
  std::vector<uint32_t> _starts;
  uint32_t _count = 0;
  uint32_t _mask = 0;
  uint8_t _bits = 0;
};

class OnPairCodec {
 public:
  OnPairCodec();

  void Train(std::span<const std::string_view> entries, uint64_t raw);

  void Encode(std::string_view entry, std::vector<uint16_t>& codes) const;

  void Export(std::string& side) const;

  uint8_t Bits() const noexcept { return _bits; }

 private:
  struct Match {
    uint32_t id;
    uint32_t len;
  };

  void Reset();
  uint32_t Insert(std::string_view token);
  Match Longest(std::string_view rest) const;
  void Finalize();

  absl::flat_hash_map<std::string, uint32_t> _ids;
  std::unique_ptr<std::array<uint16_t, 65536>> _lengths;
  std::vector<std::string> _tokens;
  std::vector<uint32_t> _order;
  absl::flat_hash_map<uint32_t, uint8_t> _pairs;
  uint8_t _bits = 8;
};

}  // namespace irs::codecs
