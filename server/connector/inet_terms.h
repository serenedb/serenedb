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

#include <absl/base/internal/endian.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <duckdb/common/types/string_type.hpp>
#include <iresearch/utils/shared.hpp>
#include <iresearch/utils/string.hpp>
#include <limits>
#include <optional>
#include <string>
#include <vector>

namespace duckdb {

class Value;

}  // namespace duckdb
namespace sdb::connector::inet_terms {

inline constexpr size_t kMaxTermSize = 1 + 16 + 2 + 16;
inline constexpr size_t kMinTermsPerValue = 4;
inline constexpr size_t kMinV6TermsPerValue = 8;
inline constexpr size_t kMaxTermsPerValue = 9;
inline constexpr size_t kV6LongTermBytes = 35 + 15 + 13;

inline constexpr uint8_t kV4Type = 1;
inline constexpr uint8_t kV6Type = 2;
inline constexpr uint8_t kV4Exact = 0x40;
inline constexpr uint8_t kV6Exact = 0x60;
inline constexpr uint8_t kMaskOffset = 8;

using Bytes = std::array<uint8_t, 16>;

struct Inet {
  bool v6 = false;
  uint16_t mask = 0;
  Bytes address{};

  uint32_t Width() const noexcept { return v6 ? 16 : 4; }
  uint32_t MaxBits() const noexcept { return v6 ? 128 : 32; }
  uint32_t NetworkBits() const noexcept {
    return std::min<uint32_t>(mask, MaxBits());
  }
};

std::optional<Inet> FromAddress(uint8_t ip_type, uint64_t upper, uint64_t lower,
                                uint16_t mask);
std::optional<Inet> FromValue(const duckdb::Value& value);

Inet FamilyMin(bool v6);
Inet FamilyMax(bool v6);

Bytes Network(const Inet& value, uint32_t bits);
Bytes Broadcast(const Inet& value, uint32_t bits);

uint32_t LevelCount(bool v6);
uint32_t LevelBits(bool v6, uint32_t level);

irs::bstring ExactTermOf(const Inet& value);
std::vector<irs::bstring> GranularTermsOf(const Inet& value);
irs::bstring LevelTermOf(bool v6, uint32_t level, const Bytes& network);
irs::bstring MaskTermOf(bool v6, uint8_t mask);
irs::bstring ExactPrefixOf(bool v6, const Bytes& network);
irs::bstring ExactPrefixOf(bool v6, const Bytes& network, uint16_t mask);
irs::bstring ExactUpperOf(bool v6, const Bytes& network);
irs::bstring CoarsestLevelPrefixOf(bool v6);

std::string Describe(irs::bytes_view term);

IRS_FORCE_INLINE inline uint8_t* InlineTerm(duckdb::string_t& slot,
                                            uint32_t size) noexcept {
  std::memset(&slot, 0, sizeof slot);
  std::memcpy(&slot, &size, sizeof size);
  return reinterpret_cast<uint8_t*>(&slot) + sizeof size;
}

IRS_FORCE_INLINE inline uint32_t WriteV4Terms(uint32_t address, uint16_t mask,
                                              duckdb::string_t* out) noexcept {
  const uint32_t host =
    mask >= 32 ? 0 : std::numeric_limits<uint32_t>::max() >> mask;
  const uint32_t network = address & ~host;
  const bool has_host_bits = address != network;
  auto* term = InlineTerm(out[0], has_host_bits ? 11 : 7);
  term[0] = kV4Exact;
  absl::big_endian::Store32(term + 1, network);
  absl::big_endian::Store16(term + 5, mask);
  if (has_host_bits) {
    absl::big_endian::Store32(term + 7, address);
  }
  for (uint32_t level = 1; level <= 3; ++level) {
    term = InlineTerm(out[level], 5 - level);
    term[0] = static_cast<uint8_t>(kV4Exact + level);
    absl::big_endian::Store32(
      term + 1,
      network & (std::numeric_limits<uint32_t>::max() << (8 * level)));
  }
  if (mask >= 32) {
    return 4;
  }
  term = InlineTerm(out[4], 2);
  term[0] = kV4Exact + kMaskOffset;
  term[1] = static_cast<uint8_t>(mask);
  return 5;
}

IRS_FORCE_INLINE inline uint32_t WriteV6Terms(uint64_t upper, uint64_t lower,
                                              uint16_t mask,
                                              duckdb::string_t* out,
                                              uint8_t*& bytes) noexcept {
  constexpr auto kAll = std::numeric_limits<uint64_t>::max();
  const uint64_t host_upper = mask >= 64 ? 0 : kAll >> mask;
  const uint64_t host_lower =
    mask >= 128 ? 0 : (mask <= 64 ? kAll : kAll >> (mask - 64));
  const uint64_t network_upper = upper & ~host_upper;
  const uint64_t network_lower = lower & ~host_lower;
  const bool has_host_bits = network_upper != upper || network_lower != lower;
  uint8_t network[16];
  absl::big_endian::Store64(network, network_upper);
  absl::big_endian::Store64(network + 8, network_lower);
  auto* term = bytes;
  term[0] = kV6Exact;
  std::memcpy(term + 1, network, 16);
  absl::big_endian::Store16(term + 17, mask);
  uint32_t exact_size = 19;
  if (has_host_bits) {
    absl::big_endian::Store64(term + 19, upper);
    absl::big_endian::Store64(term + 27, lower);
    exact_size = 35;
  }
  out[0] = duckdb::string_t{reinterpret_cast<const char*>(term), exact_size};
  term += exact_size;
  for (uint32_t level = 1; level <= 2; ++level) {
    const uint32_t size = 17 - 2 * level;
    term[0] = static_cast<uint8_t>(kV6Exact + level);
    std::memcpy(term + 1, network, size - 1);
    out[level] = duckdb::string_t{reinterpret_cast<const char*>(term), size};
    term += size;
  }
  bytes = term;
  for (uint32_t level = 3; level <= 7; ++level) {
    const uint32_t size = 17 - 2 * level;
    term = InlineTerm(out[level], size);
    term[0] = static_cast<uint8_t>(kV6Exact + level);
    std::memcpy(term + 1, network, size - 1);
  }
  if (mask >= 128) {
    return 8;
  }
  term = InlineTerm(out[8], 2);
  term[0] = kV6Exact + kMaskOffset;
  term[1] = static_cast<uint8_t>(mask);
  return 9;
}

IRS_FORCE_INLINE inline uint32_t WriteTerms(uint8_t ip_type, uint64_t upper,
                                            uint64_t lower, uint16_t mask,
                                            duckdb::string_t* out,
                                            uint8_t*& bytes) noexcept {
  if (ip_type == kV4Type) {
    if (upper != 0 || lower > std::numeric_limits<uint32_t>::max()) {
      return 0;
    }
    return WriteV4Terms(static_cast<uint32_t>(lower), mask, out);
  }
  if (ip_type == kV6Type) {
    return WriteV6Terms(upper, lower, mask, out, bytes);
  }
  return 0;
}

}  // namespace sdb::connector::inet_terms
