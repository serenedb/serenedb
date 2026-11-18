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
#include <duckdb/common/types/string_type.hpp>
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
                  char* out, uint64_t capacity) const {
    if (count * kOnPairMaxToken <= capacity) {
      return Copy<false>(stream, first, count, out, capacity);
    }
    return Copy<true>(stream, first, count, out, capacity);
  }

 private:
  template<bool kTail>
  uint64_t Copy(const uint8_t* stream, uint64_t first, uint64_t count,
                char* out, uint64_t capacity) const {
    const auto* tokens = _tokens.data();
    const auto* bytes = _bytes.data();
    const uint64_t bits = _bits;
    const auto mask = _mask;
    const auto limit = _count;
    uint64_t off = 0;
    auto bit = first * bits;
    const auto end = (first + count) * bits;
    if constexpr (!kTail) {
      const uint64_t batch = (64 - 7) / bits;
      const auto readable = (end + 7) / 8 + kOnPairPadding;
      while (bit + batch * bits <= end &&
             bit / 8 + sizeof(uint64_t) <= readable) {
        uint64_t word;
        std::memcpy(&word, stream + bit / 8, sizeof(word));
        word >>= bit % 8;
        for (uint64_t k = 0; k < batch; ++k) {
          const auto code = static_cast<uint32_t>(word) & mask;
          word >>= bits;
          if (code >= limit) [[unlikely]] {
            Corrupted();
          }
          const auto token = tokens[code];
          std::memcpy(out + off, bytes + (token & 0xFFFFFF), kOnPairMaxToken);
          off += token >> 24;
        }
        bit += batch * bits;
      }
    }
    for (; bit != end; bit += bits) {
      uint32_t word = 0;
      std::memcpy(&word, stream + bit / 8, 3);
      const auto code = (word >> (bit % 8)) & mask;
      if (code >= limit) [[unlikely]] {
        Corrupted();
      }
      const auto token = tokens[code];
      const auto len = token >> 24;
      const auto* from = bytes + (token & 0xFFFFFF);
      if constexpr (kTail) {
        if (off + len > capacity) [[unlikely]] {
          Corrupted();
        }
        if (off + kOnPairMaxToken > capacity) {
          std::memcpy(out + off, from, len);
          off += len;
          continue;
        }
      }
      std::memcpy(out + off, from, kOnPairMaxToken);
      off += len;
    }
    return off;
  }

  [[noreturn]] static void Corrupted();

  std::string _bytes;
  std::vector<uint32_t> _tokens;
  uint32_t _count = 0;
  uint32_t _mask = 0;
  uint8_t _bits = 0;
};

class OnPairCodec {
 public:
  OnPairCodec();

  void Train(std::span<const duckdb::string_t> entries, uint64_t raw);

  void Encode(std::span<const duckdb::string_t> entries,
              std::span<uint16_t> codes, std::span<uint32_t> counts) const;

  void Export(std::string& side) const;

  uint8_t Bits() const noexcept { return _bits; }

 private:
  struct Match {
    uint32_t id;
    uint32_t len;
  };

  struct Chain {
    uint16_t lens;
    uint16_t ids[kOnPairMaxToken];
  };

  using Key = unsigned __int128;

  static Key KeyOf(std::string_view bytes) noexcept;

  void Reset();
  uint32_t Insert(std::string_view token);
  Match Longest(const char* rest, size_t left) const;
  void Finalize();

  absl::flat_hash_map<std::string, uint32_t> _ids;
  std::unique_ptr<std::array<uint16_t, 65536>> _lengths;
  std::vector<std::string> _tokens;
  std::vector<uint32_t> _order;
  absl::flat_hash_map<uint32_t, uint8_t> _pairs;
  std::vector<Key> _keys;
  std::vector<uint32_t> _firsts;
  std::vector<Chain> _chains;
  std::vector<uint32_t> _stack;
  uint8_t _bits = 8;
};

}  // namespace irs::codecs
