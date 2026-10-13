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
#include <climits>
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

inline constexpr uint8_t kV4Type = 1;
inline constexpr uint8_t kV6Type = 2;

inline constexpr uint32_t kV4Width = sizeof(uint32_t);
inline constexpr uint32_t kV6Width = 2 * sizeof(uint64_t);
inline constexpr uint32_t kV4Bits = kV4Width * CHAR_BIT;
inline constexpr uint32_t kV6Bits = kV6Width * CHAR_BIT;

inline constexpr uint8_t kV4Exact = 0x40;
inline constexpr uint8_t kV6Exact = 0x60;
inline constexpr uint8_t kMaskOffset = 8;

inline constexpr uint32_t kV4Levels = 3;
inline constexpr uint32_t kV6Levels = 7;
inline constexpr uint32_t kV4LevelStep = CHAR_BIT;
inline constexpr uint32_t kV6LevelStep = 2 * CHAR_BIT;

inline constexpr uint32_t kTagSize = sizeof(uint8_t);
inline constexpr uint32_t kMaskSize = sizeof(uint16_t);
inline constexpr uint32_t kMaskTermSize = kTagSize + sizeof(uint8_t);

inline constexpr uint32_t kV4BaseTerms = 1 + kV4Levels;
inline constexpr uint32_t kV6BaseTerms = 1 + kV6Levels;
inline constexpr uint32_t kMaxTermsPerValue = kV6BaseTerms + 1;

static_assert(kV6Levels < kMaskOffset);
static_assert(kV4Exact + kMaskOffset < kV6Exact);
static_assert(kV4Levels * kV4LevelStep < kV4Bits);
static_assert(kV6Levels * kV6LevelStep < kV6Bits);

constexpr uint32_t Width(bool v6) noexcept { return v6 ? kV6Width : kV4Width; }

constexpr uint32_t MaxBits(bool v6) noexcept { return v6 ? kV6Bits : kV4Bits; }

constexpr uint32_t LevelCount(bool v6) noexcept {
  return v6 ? kV6Levels : kV4Levels;
}

constexpr uint32_t LevelBits(bool v6, uint32_t level) noexcept {
  return MaxBits(v6) - level * (v6 ? kV6LevelStep : kV4LevelStep);
}

constexpr uint8_t ExactTag(bool v6) noexcept {
  return v6 ? kV6Exact : kV4Exact;
}

constexpr uint8_t LevelTag(bool v6, uint32_t level) noexcept {
  return static_cast<uint8_t>(ExactTag(v6) + level);
}

constexpr uint8_t MaskTag(bool v6) noexcept {
  return ExactTag(v6) + kMaskOffset;
}

constexpr uint32_t ShortExactSize(bool v6) noexcept {
  return kTagSize + Width(v6) + kMaskSize;
}

constexpr uint32_t LongExactSize(bool v6) noexcept {
  return ShortExactSize(v6) + Width(v6);
}

constexpr uint32_t LevelTermSize(bool v6, uint32_t level) noexcept {
  return kTagSize + LevelBits(v6, level) / CHAR_BIT;
}

inline constexpr size_t kMaxTermSize = LongExactSize(true);

inline constexpr uint32_t kV6FirstInlineLevel = 3;

static_assert(LongExactSize(false) <= duckdb::string_t::INLINE_LENGTH);
static_assert(ShortExactSize(true) > duckdb::string_t::INLINE_LENGTH);
static_assert(LevelTermSize(true, kV6FirstInlineLevel - 1) >
              duckdb::string_t::INLINE_LENGTH);
static_assert(LevelTermSize(true, kV6FirstInlineLevel) <=
              duckdb::string_t::INLINE_LENGTH);

constexpr size_t V6OutOfLineBytes() noexcept {
  size_t bytes = LongExactSize(true);
  for (uint32_t level = 1; level < kV6FirstInlineLevel; ++level) {
    bytes += LevelTermSize(true, level);
  }
  return bytes;
}

inline constexpr size_t kV6LongTermBytes = V6OutOfLineBytes();

using Bytes = std::array<uint8_t, kV6Width>;

struct Inet {
  bool v6 = false;
  uint16_t mask = 0;
  Bytes address{};

  uint32_t Width() const noexcept { return inet_terms::Width(v6); }
  uint32_t MaxBits() const noexcept { return inet_terms::MaxBits(v6); }
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
uint32_t MinMaskOf(bool v6, const Bytes& network);

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

IRS_FORCE_INLINE inline void WriteMaskTerm(bool v6, uint16_t mask,
                                           duckdb::string_t& slot) noexcept {
  auto* term = InlineTerm(slot, kMaskTermSize);
  term[0] = MaskTag(v6);
  term[kTagSize] = static_cast<uint8_t>(mask);
}

IRS_FORCE_INLINE inline uint32_t WriteV4Terms(uint32_t address, uint16_t mask,
                                              duckdb::string_t* out) noexcept {
  constexpr auto kAll = std::numeric_limits<uint32_t>::max();
  const uint32_t host = mask >= kV4Bits ? 0 : kAll >> mask;
  const uint32_t network = address & ~host;
  const bool has_host_bits = address != network;
  auto* term = InlineTerm(
    out[0], has_host_bits ? LongExactSize(false) : ShortExactSize(false));
  term[0] = kV4Exact;
  term += kTagSize;
  absl::big_endian::Store32(term, network);
  absl::big_endian::Store16(term + kV4Width, mask);
  if (has_host_bits) {
    absl::big_endian::Store32(term + kV4Width + kMaskSize, address);
  }
  for (uint32_t level = 1; level <= kV4Levels; ++level) {
    term = InlineTerm(out[level], LevelTermSize(false, level));
    term[0] = LevelTag(false, level);
    absl::big_endian::Store32(term + kTagSize,
                              network & (kAll << (level * kV4LevelStep)));
  }
  if (mask >= kV4Bits) {
    return kV4BaseTerms;
  }
  WriteMaskTerm(false, mask, out[kV4BaseTerms]);
  return kV4BaseTerms + 1;
}

IRS_FORCE_INLINE inline uint32_t WriteV6Terms(uint64_t upper, uint64_t lower,
                                              uint16_t mask,
                                              duckdb::string_t* out,
                                              uint8_t*& bytes) noexcept {
  constexpr auto kAll = std::numeric_limits<uint64_t>::max();
  constexpr uint32_t kHalfBits = std::numeric_limits<uint64_t>::digits;
  const uint64_t host_upper = mask >= kHalfBits ? 0 : kAll >> mask;
  const uint64_t host_lower =
    mask >= kV6Bits ? 0
                    : (mask <= kHalfBits ? kAll : kAll >> (mask - kHalfBits));
  const uint64_t network_upper = upper & ~host_upper;
  const uint64_t network_lower = lower & ~host_lower;
  const bool has_host_bits = network_upper != upper || network_lower != lower;
  uint8_t network[kV6Width];
  absl::big_endian::Store64(network, network_upper);
  absl::big_endian::Store64(network + sizeof(uint64_t), network_lower);
  auto* term = bytes;
  term[0] = kV6Exact;
  std::memcpy(term + kTagSize, network, kV6Width);
  absl::big_endian::Store16(term + kTagSize + kV6Width, mask);
  uint32_t exact_size = ShortExactSize(true);
  if (has_host_bits) {
    absl::big_endian::Store64(term + exact_size, upper);
    absl::big_endian::Store64(term + exact_size + sizeof(uint64_t), lower);
    exact_size = LongExactSize(true);
  }
  out[0] = duckdb::string_t{reinterpret_cast<const char*>(term), exact_size};
  term += exact_size;
  for (uint32_t level = 1; level < kV6FirstInlineLevel; ++level) {
    const uint32_t size = LevelTermSize(true, level);
    term[0] = LevelTag(true, level);
    std::memcpy(term + kTagSize, network, size - kTagSize);
    out[level] = duckdb::string_t{reinterpret_cast<const char*>(term), size};
    term += size;
  }
  bytes = term;
  for (uint32_t level = kV6FirstInlineLevel; level <= kV6Levels; ++level) {
    const uint32_t size = LevelTermSize(true, level);
    term = InlineTerm(out[level], size);
    term[0] = LevelTag(true, level);
    std::memcpy(term + kTagSize, network, size - kTagSize);
  }
  if (mask >= kV6Bits) {
    return kV6BaseTerms;
  }
  WriteMaskTerm(true, mask, out[kV6BaseTerms]);
  return kV6BaseTerms + 1;
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
