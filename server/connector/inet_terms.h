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

#include <array>
#include <climits>
#include <cstdint>
#include <cstring>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/value.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/shared.hpp>
#include <iresearch/utils/string.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <limits>
#include <optional>
#include <string>
#include <vector>

namespace sdb::connector::inet_terms {

// Index terms of one INET/CIDR value. Integers are big-endian; addresses are
// the real address bytes; network = address with the bits past the netmask
// cleared; W = 4 bytes for IPv4, 16 for IPv6.
//
//   exact   [tag 1B][network W][mask 2B]            address == network
//           [tag 1B][network W][mask 2B][address W] host bits set
//           IPv4 7 or 11 bytes, IPv6 19 or 35 bytes; always emitted.
//   level   [tag+l 1B][first L_l bits of network]
//           IPv4 l=1..3: /24 /16 /8      (4, 3, 2 bytes)
//           IPv6 l=1..7: /112 /96 ... /16 (15, 13, ..., 3 bytes)
//           always emitted.
//   mask    [tag+8 1B][mask 1B]
//           emitted only when mask < 32 (IPv4) or < 128 (IPv6).
//
//   tags    IPv4: 0x40 exact, 0x41-0x43 levels, 0x48 mask
//           IPv6: 0x60 exact, 0x61-0x67 levels, 0x68 mask
//
// Byte order of exact terms is PostgreSQL inet order (IPv4 before IPv6, then
// network, mask, address), so comparisons are byte ranges. The bytes after a
// level tag are a prefix of the exact term's network bytes, so the values
// sharing a level term form one contiguous run of exact terms. Mask terms let
// "contained by" queries exclude values whose mask is shorter than theirs.

enum class Family : uint8_t {
  V4 = 1,
  V6 = 2,
};

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

inline constexpr uint32_t kV4MinTerms = 1 + kV4Levels;
inline constexpr uint32_t kV6MinTerms = 1 + kV6Levels;
inline constexpr uint32_t kMaxTerms = kV6MinTerms + 1;

static_assert(kV6Levels < kMaskOffset);
static_assert(kV4Exact + kMaskOffset < kV6Exact);
static_assert(kV4Levels * kV4LevelStep < kV4Bits);
static_assert(kV6Levels * kV6LevelStep < kV6Bits);

constexpr uint32_t Width(Family family) noexcept {
  return family == Family::V6 ? kV6Width : kV4Width;
}

constexpr uint32_t MaxBits(Family family) noexcept {
  return family == Family::V6 ? kV6Bits : kV4Bits;
}

constexpr uint32_t LevelCount(Family family) noexcept {
  return family == Family::V6 ? kV6Levels : kV4Levels;
}

constexpr uint32_t LevelBits(Family family, uint32_t level) noexcept {
  return MaxBits(family) -
         level * (family == Family::V6 ? kV6LevelStep : kV4LevelStep);
}

constexpr uint8_t ExactTag(Family family) noexcept {
  return family == Family::V6 ? kV6Exact : kV4Exact;
}

constexpr uint8_t LevelTag(Family family, uint32_t level) noexcept {
  return static_cast<uint8_t>(ExactTag(family) + level);
}

constexpr uint8_t MaskTag(Family family) noexcept {
  return ExactTag(family) + kMaskOffset;
}

constexpr uint32_t ShortExactSize(Family family) noexcept {
  return kTagSize + Width(family) + kMaskSize;
}

constexpr uint32_t LongExactSize(Family family) noexcept {
  return ShortExactSize(family) + Width(family);
}

constexpr uint32_t LevelTermSize(Family family, uint32_t level) noexcept {
  return kTagSize + LevelBits(family, level) / CHAR_BIT;
}

inline constexpr uint32_t kV6FirstInlineLevel = 3;

static_assert(LongExactSize(Family::V4) <= duckdb::string_t::INLINE_LENGTH);
static_assert(ShortExactSize(Family::V6) > duckdb::string_t::INLINE_LENGTH);
static_assert(LevelTermSize(Family::V6, kV6FirstInlineLevel - 1) >
              duckdb::string_t::INLINE_LENGTH);
static_assert(LevelTermSize(Family::V6, kV6FirstInlineLevel) <=
              duckdb::string_t::INLINE_LENGTH);

constexpr size_t V6OutOfLineBytes() noexcept {
  size_t bytes = LongExactSize(Family::V6);
  for (uint32_t level = 1; level < kV6FirstInlineLevel; ++level) {
    bytes += LevelTermSize(Family::V6, level);
  }
  return bytes;
}

inline constexpr size_t kV6LongTermBytes = V6OutOfLineBytes();

using Bytes = std::array<uint8_t, kV6Width>;

struct Inet {
  Family family = Family::V4;
  uint16_t mask = 0;
  Bytes address{};
};

std::optional<Inet> FromAddress(Family family, uint64_t upper, uint64_t lower,
                                uint16_t mask);
std::optional<Inet> FromValue(const duckdb::Value& value);

Inet FamilyMin(Family family);
Inet FamilyMax(Family family);

Bytes Network(const Inet& value, uint32_t bits);
Bytes Broadcast(const Inet& value, uint32_t bits);
uint32_t MinMaskOf(Family family, const Bytes& network);

irs::bstring ExactTermOf(const Inet& value);
irs::bstring FirstExactTermOf(Family family, const Bytes& network);
irs::bstring LastExactTermOf(Family family, const Bytes& network);

struct NetworkPiece {
  uint32_t level;
  Bytes first;
  Bytes last;
};

std::vector<NetworkPiece> SplitNetworks(Family family, const Bytes& first,
                                        const Bytes& last);
irs::bstring LevelTermOf(Family family, uint32_t level, const Bytes& network);
irs::bstring MaskTermOf(Family family, uint8_t mask);
irs::bstring ExactPrefixOf(Family family, const Bytes& network);
irs::bstring ExactPrefixOf(Family family, const Bytes& network, uint16_t mask);
irs::bstring ExactUpperOf(Family family, const Bytes& network);
irs::bstring CoarsestLevelPrefixOf(Family family);

std::string Describe(irs::bytes_view term);

IRS_FORCE_INLINE inline uint8_t* InlineTerm(duckdb::string_t& slot,
                                            uint32_t size) noexcept {
  std::memset(&slot, 0, sizeof slot);
  std::memcpy(&slot, &size, sizeof size);
  return reinterpret_cast<uint8_t*>(&slot) + sizeof size;
}

IRS_FORCE_INLINE inline void WriteMaskTerm(Family family, uint16_t mask,
                                           duckdb::string_t& slot) noexcept {
  auto* term = InlineTerm(slot, kMaskTermSize);
  term[0] = MaskTag(family);
  term[kTagSize] = static_cast<uint8_t>(mask);
}

// Writes the exact, 3 level and, for a mask below /32, the mask term; all fit
// inline. Returns how many were written.
IRS_FORCE_INLINE inline uint32_t WriteV4Terms(uint32_t address, uint16_t mask,
                                              duckdb::string_t* out) noexcept {
  SDB_ASSERT(mask <= kV4Bits);
  constexpr auto kOnes = std::numeric_limits<uint32_t>::max();
  const uint32_t host_bits = mask == kV4Bits ? 0 : kOnes >> mask;
  const uint32_t network = address & ~host_bits;
  const bool has_host_bits = address != network;

  auto* term = InlineTerm(out[0], has_host_bits ? LongExactSize(Family::V4)
                                                : ShortExactSize(Family::V4));
  term[0] = kV4Exact;
  term += kTagSize;
  absl::big_endian::Store32(term, network);
  absl::big_endian::Store16(term + kV4Width, mask);
  if (has_host_bits) {
    absl::big_endian::Store32(term + kV4Width + kMaskSize, address);
  }

  for (uint32_t level = 1; level <= kV4Levels; ++level) {
    term = InlineTerm(out[level], LevelTermSize(Family::V4, level));
    term[0] = LevelTag(Family::V4, level);
    absl::big_endian::Store32(term + kTagSize,
                              network & (kOnes << (level * kV4LevelStep)));
  }

  if (mask == kV4Bits) {
    return kV4MinTerms;
  }
  WriteMaskTerm(Family::V4, mask, out[kV4MinTerms]);
  return kV4MinTerms + 1;
}

// Writes the exact, 7 level and, for a mask below /128, the mask term. Terms
// longer than string_t's 12 inline bytes (the exact, /112 and /96 terms) go to
// `bytes`, which is advanced past them.
IRS_FORCE_INLINE inline uint32_t WriteV6Terms(uint64_t upper, uint64_t lower,
                                              uint16_t mask,
                                              duckdb::string_t* out,
                                              uint8_t*& bytes) noexcept {
  SDB_ASSERT(mask <= kV6Bits);
  constexpr auto kOnes = std::numeric_limits<uint64_t>::max();
  constexpr uint32_t kBitsPerHalf = std::numeric_limits<uint64_t>::digits;
  const uint64_t upper_host_bits = mask >= kBitsPerHalf ? 0 : kOnes >> mask;
  const uint64_t lower_host_bits =
    mask <= kBitsPerHalf
      ? kOnes
      : (mask == kV6Bits ? 0 : kOnes >> (mask - kBitsPerHalf));
  const uint64_t network_upper = upper & ~upper_host_bits;
  const uint64_t network_lower = lower & ~lower_host_bits;
  const bool has_host_bits = network_upper != upper || network_lower != lower;
  uint8_t network[kV6Width];
  absl::big_endian::Store64(network, network_upper);
  absl::big_endian::Store64(network + sizeof(uint64_t), network_lower);

  auto* term = bytes;
  term[0] = kV6Exact;
  std::memcpy(term + kTagSize, network, kV6Width);
  absl::big_endian::Store16(term + kTagSize + kV6Width, mask);
  uint32_t exact_size = ShortExactSize(Family::V6);
  if (has_host_bits) {
    absl::big_endian::Store64(term + exact_size, upper);
    absl::big_endian::Store64(term + exact_size + sizeof(uint64_t), lower);
    exact_size = LongExactSize(Family::V6);
  }
  out[0] = duckdb::string_t{reinterpret_cast<const char*>(term), exact_size};
  term += exact_size;

  for (uint32_t level = 1; level < kV6FirstInlineLevel; ++level) {
    const uint32_t size = LevelTermSize(Family::V6, level);
    term[0] = LevelTag(Family::V6, level);
    std::memcpy(term + kTagSize, network, size - kTagSize);
    out[level] = duckdb::string_t{reinterpret_cast<const char*>(term), size};
    term += size;
  }
  bytes = term;

  for (uint32_t level = kV6FirstInlineLevel; level <= kV6Levels; ++level) {
    const uint32_t size = LevelTermSize(Family::V6, level);
    term = InlineTerm(out[level], size);
    term[0] = LevelTag(Family::V6, level);
    std::memcpy(term + kTagSize, network, size - kTagSize);
  }

  if (mask == kV6Bits) {
    return kV6MinTerms;
  }
  WriteMaskTerm(Family::V6, mask, out[kV6MinTerms]);
  return kV6MinTerms + 1;
}

// Writes the terms of a valid INET value; returns how many.
IRS_FORCE_INLINE inline uint32_t WriteTerms(Family family, uint64_t upper,
                                            uint64_t lower, uint16_t mask,
                                            duckdb::string_t* out,
                                            uint8_t*& bytes) noexcept {
  switch (family) {
    case Family::V4:
      SDB_ASSERT(upper == 0 && lower <= std::numeric_limits<uint32_t>::max());
      return WriteV4Terms(static_cast<uint32_t>(lower), mask, out);
    case Family::V6:
      return WriteV6Terms(upper, lower, mask, out, bytes);
  }
  SDB_UNREACHABLE();
}

}  // namespace sdb::connector::inet_terms
