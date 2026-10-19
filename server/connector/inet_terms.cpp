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

#include "connector/inet_terms.h"

#include <absl/algorithm/container.h>
#include <absl/base/internal/endian.h>
#include <absl/numeric/int128.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <bit>
#include <cstring>
#include <duckdb/common/types/value.hpp>
#include <duckdb/inet/inet_ipaddress.hpp>
#include <duckdb/inet/inet_type.hpp>
#include <iresearch/utils/assert.hpp>
#include <utility>

#include "connector/pg_logical_types.h"

namespace sdb::connector::inet_terms {
namespace {

static_assert(std::to_underlying(Family::V4) == INET_IP_ADDRESS_V4);
static_assert(std::to_underlying(Family::V6) == INET_IP_ADDRESS_V6);
static_assert(kV4Bits == IPV4_DEFAULT_MASK);
static_assert(kV6Bits == IPV6_DEFAULT_MASK);
static_assert(LongExactSize(Family::V4) == duckdb::INET_SORT_KEY_V4_SIZE);
static_assert(LongExactSize(Family::V6) == duckdb::INET_SORT_KEY_V6_SIZE);

constexpr uint8_t kAllBits = std::numeric_limits<uint8_t>::max();
constexpr size_t kMaxTextSize = 64;

Bytes ApplyHostBits(const Inet& value, uint32_t bits, bool set) {
  Bytes out = value.address;
  for (uint32_t i = 0; i < Width(value.family); ++i) {
    const uint32_t first = i * CHAR_BIT;
    if (first + CHAR_BIT <= bits) {
      continue;
    }
    const auto keep =
      first >= bits
        ? uint8_t{0}
        : static_cast<uint8_t>(kAllBits << (CHAR_BIT - (bits - first)));
    out[i] = set ? static_cast<uint8_t>(out[i] | static_cast<uint8_t>(~keep))
                 : static_cast<uint8_t>(out[i] & keep);
  }
  return out;
}

irs::bstring ToBString(const uint8_t* data, size_t size) {
  return irs::bstring{reinterpret_cast<const irs::byte_type*>(data), size};
}

std::string Format(Family family, const uint8_t* address, uint32_t mask) {
  INET_IPAddress inet;
  inet.type = family == Family::V6 ? INET_IP_ADDRESS_V6 : INET_IP_ADDRESS_V4;
  if (family == Family::V6) {
    inet.address.upper = absl::big_endian::Load64(address);
    inet.address.lower = absl::big_endian::Load64(address + sizeof(uint64_t));
  } else {
    inet.address.upper = 0;
    inet.address.lower = absl::big_endian::Load32(address);
  }
  inet.mask = static_cast<uint16_t>(mask);
  char buf[kMaxTextSize];
  const auto size = ipaddress_to_string(&inet, buf, sizeof(buf));
  return std::string{buf, size};
}

std::optional<Inet> FromSortKey(std::string_view key) {
  const auto family =
    key.size() == LongExactSize(Family::V6) ? Family::V6 : Family::V4;
  if (key.size() != LongExactSize(family)) {
    return std::nullopt;
  }
  const auto* data = reinterpret_cast<const uint8_t*>(key.data());
  if (data[0] != std::to_underlying(family)) {
    return std::nullopt;
  }
  const auto* mask = data + kTagSize + Width(family);
  Inet value{.family = family, .mask = absl::big_endian::Load16(mask)};
  if (value.mask > MaxBits(family)) {
    return std::nullopt;
  }
  std::memcpy(value.address.data(), mask + kMaskSize, Width(family));
  return value;
}

irs::bstring ToBString(const duckdb::string_t& term) {
  return ToBString(reinterpret_cast<const uint8_t*>(term.GetData()),
                   term.GetSize());
}

class EncodedTerms {
 public:
  explicit EncodedTerms(const Inet& value) {
    const auto* address = value.address.data();
    uint64_t upper = 0;
    uint64_t lower = 0;
    switch (value.family) {
      case Family::V4:
        lower = absl::big_endian::Load32(address);
        break;
      case Family::V6:
        upper = absl::big_endian::Load64(address);
        lower = absl::big_endian::Load64(address + sizeof(uint64_t));
        break;
    }
    auto* bytes = _bytes;
    _count = WriteTerms(value.family, upper, lower, value.mask, _terms, bytes);
  }

  EncodedTerms(const EncodedTerms&) = delete;
  EncodedTerms& operator=(const EncodedTerms&) = delete;

  irs::bstring operator[](uint32_t i) const {
    SDB_ASSERT(i < _count);
    return ToBString(_terms[i]);
  }

 private:
  duckdb::string_t _terms[kMaxTerms];
  uint8_t _bytes[kV6LongTermBytes];
  uint32_t _count = 0;
};

Inet NetworkValue(Family family, const Bytes& network, uint32_t mask) {
  return {
    .family = family, .mask = static_cast<uint16_t>(mask), .address = network};
}

using NetworkBits = absl::uint128;

NetworkBits ToBits(Family family, const Bytes& bytes) {
  NetworkBits bits = 0;
  for (uint32_t i = 0; i < Width(family); ++i) {
    bits = (bits << CHAR_BIT) | bytes[i];
  }
  return bits;
}

Bytes ToBytes(Family family, NetworkBits bits) {
  Bytes bytes{};
  for (uint32_t i = Width(family); i != 0; --i) {
    bytes[i - 1] = static_cast<uint8_t>(bits & kAllBits);
    bits >>= CHAR_BIT;
  }
  return bytes;
}

}  // namespace

std::optional<Inet> FromAddress(Family family, uint64_t upper, uint64_t lower,
                                uint16_t mask) {
  Inet value{.family = family, .mask = mask};
  switch (family) {
    case Family::V4:
      if (upper != 0 || lower > std::numeric_limits<uint32_t>::max() ||
          mask > kV4Bits) {
        return std::nullopt;
      }
      absl::big_endian::Store32(value.address.data(),
                                static_cast<uint32_t>(lower));
      return value;
    case Family::V6:
      if (mask > kV6Bits) {
        return std::nullopt;
      }
      absl::big_endian::Store64(value.address.data(), upper);
      absl::big_endian::Store64(value.address.data() + sizeof(uint64_t), lower);
      return value;
  }
  return std::nullopt;
}

std::optional<Inet> FromValue(const duckdb::Value& value) {
  if (value.IsNull()) {
    return std::nullopt;
  }
  if (value.type().id() == duckdb::LogicalTypeId::BLOB) {
    return FromSortKey(duckdb::StringValue::Get(value));
  }
  if (value.type().id() != duckdb::LogicalTypeId::STRUCT) {
    return std::nullopt;
  }
  const auto& children = duckdb::StructValue::GetChildren(value);
  if (children.size() != pg::kInetChildCount) {
    return std::nullopt;
  }
  for (const auto& child : children) {
    if (child.IsNull()) {
      return std::nullopt;
    }
  }
  const auto family =
    static_cast<Family>(children[pg::kInetIpTypeChild].GetValue<uint8_t>());
  const auto address =
    children[pg::kInetAddressChild].GetValue<duckdb::hugeint_t>();
  auto upper = static_cast<uint64_t>(address.upper);
  if (family == Family::V6) {
    upper ^= pg::kInetV6SortBit;
  }
  return FromAddress(family, upper, address.lower,
                     children[pg::kInetMaskChild].GetValue<uint16_t>());
}

Inet FamilyMin(Family family) { return Inet{.family = family}; }

Inet FamilyMax(Family family) {
  Inet value{.family = family, .mask = static_cast<uint16_t>(MaxBits(family))};
  value.address.fill(kAllBits);
  return value;
}

Bytes Network(const Inet& value, uint32_t bits) {
  return ApplyHostBits(value, bits, false);
}

Bytes Broadcast(const Inet& value, uint32_t bits) {
  return ApplyHostBits(value, bits, true);
}

uint32_t MinMaskOf(Family family, const Bytes& network) {
  for (uint32_t i = Width(family); i != 0; --i) {
    if (const auto byte = network[i - 1]) {
      return i * CHAR_BIT - static_cast<uint32_t>(std::countr_zero(byte));
    }
  }
  return 0;
}

irs::bstring ExactTermOf(const Inet& value) { return EncodedTerms{value}[0]; }

irs::bstring FirstExactTermOf(Family family, const Bytes& network) {
  return ExactPrefixOf(family, network,
                       static_cast<uint16_t>(MinMaskOf(family, network)));
}

irs::bstring LastExactTermOf(Family family, const Bytes& network) {
  return ExactPrefixOf(family, network, static_cast<uint16_t>(MaxBits(family)));
}

std::vector<NetworkPiece> SplitNetworks(Family family, const Bytes& first,
                                        const Bytes& last) {
  auto lo = ToBits(family, first);
  auto hi = ToBits(family, last);
  SDB_ASSERT(lo <= hi);
  const uint32_t step = MaxBits(family) - LevelBits(family, 1);
  std::vector<NetworkPiece> pieces;
  const auto add = [&](uint32_t level, NetworkBits from, NetworkBits to) {
    pieces.push_back({level, ToBytes(family, from), ToBytes(family, to)});
  };
  for (uint32_t level = 0;; ++level) {
    if (level == LevelCount(family)) {
      add(level, lo, hi);
      break;
    }
    const uint32_t shift = level * step;
    const NetworkBits mask = ((NetworkBits{1} << step) - 1) << shift;
    const NetworkBits bucket = NetworkBits{1} << (shift + step);
    const bool has_head = (lo & mask) != 0;
    const bool has_tail = (hi & mask) != mask;
    const NetworkBits next_lo = (has_head ? lo + bucket : lo) & ~mask;
    const NetworkBits next_hi = (has_tail ? hi - bucket : hi) & ~mask;
    if (next_lo > next_hi || next_lo < lo || next_hi > hi) {
      add(level, lo, hi);
      break;
    }
    if (has_head) {
      add(level, lo, lo | mask);
    }
    if (has_tail) {
      add(level, hi & ~mask, hi);
    }
    lo = next_lo;
    hi = next_hi;
  }
  absl::c_sort(pieces, [](const NetworkPiece& lhs, const NetworkPiece& rhs) {
    return lhs.first < rhs.first;
  });
  return pieces;
}

irs::bstring LevelTermOf(Family family, uint32_t level, const Bytes& network) {
  return EncodedTerms{NetworkValue(family, network, MaxBits(family))}[level];
}

irs::bstring MaskTermOf(Family family, uint8_t mask) {
  duckdb::string_t term;
  WriteMaskTerm(family, mask, term);
  return ToBString(term);
}

irs::bstring ExactPrefixOf(Family family, const Bytes& network) {
  auto prefix = ExactTermOf(NetworkValue(family, network, MaxBits(family)));
  prefix.resize(kTagSize + Width(family));
  return prefix;
}

irs::bstring ExactPrefixOf(Family family, const Bytes& network, uint16_t mask) {
  auto prefix = ExactTermOf(NetworkValue(family, network, mask));
  SDB_ASSERT(prefix.size() == ShortExactSize(family));
  return prefix;
}

irs::bstring ExactUpperOf(Family family, const Bytes& network) {
  auto upper = ExactPrefixOf(family, network);
  upper.append(LongExactSize(family) - upper.size(), irs::byte_type{kAllBits});
  return upper;
}

irs::bstring CoarsestLevelPrefixOf(Family family) {
  return irs::bstring(1, LevelTag(family, LevelCount(family)));
}

std::string Describe(irs::bytes_view term) {
  if (term.empty() || term[0] < kV4Exact || term[0] > MaskTag(Family::V6)) {
    return {};
  }
  const auto family = term[0] >= kV6Exact ? Family::V6 : Family::V4;
  const uint8_t base = ExactTag(family);
  const auto width = Width(family);
  const auto tag = term[0];
  if (tag == MaskTag(family) && term.size() == kMaskTermSize) {
    return absl::StrCat("/", term[kTagSize]);
  }
  if (term.size() == kTagSize) {
    return family == Family::V6 ? "ipv6" : "ipv4";
  }
  const auto* network = term.data() + kTagSize;
  if (tag > base && tag <= LevelTag(family, LevelCount(family))) {
    Bytes level_network{};
    std::memcpy(level_network.data(), network,
                std::min<size_t>(term.size() - kTagSize, width));
    return Format(family, level_network.data(), LevelBits(family, tag - base));
  }
  if (tag != base) {
    return {};
  }
  if (term.size() == kTagSize + width) {
    return Format(family, network, MaxBits(family));
  }
  if (term.size() != ShortExactSize(family) &&
      term.size() != LongExactSize(family)) {
    return family == Family::V6 ? "ipv6" : "ipv4";
  }
  const auto mask = absl::big_endian::Load16(network + width);
  if (term.size() == ShortExactSize(family)) {
    return Format(family, network, std::min<uint32_t>(mask, MaxBits(family)));
  }
  if (mask > MaxBits(family)) {
    return Format(family, network, MaxBits(family));
  }
  return Format(family, term.data() + ShortExactSize(family), mask);
}

}  // namespace sdb::connector::inet_terms
