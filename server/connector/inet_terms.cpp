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

#include <absl/base/internal/endian.h>
#include <absl/strings/str_cat.h>

#include <bit>
#include <cstring>
#include <duckdb/common/types/value.hpp>
#include <duckdb/inet/inet_ipaddress.hpp>
#include <iresearch/utils/assert.hpp>

#include "connector/pg_logical_types.h"

namespace sdb::connector::inet_terms {
namespace {

static_assert(kV4Type == INET_IP_ADDRESS_V4);
static_assert(kV6Type == INET_IP_ADDRESS_V6);
static_assert(kV4Bits == IPV4_DEFAULT_MASK);
static_assert(kV6Bits == IPV6_DEFAULT_MASK);

constexpr uint8_t kAllBits = std::numeric_limits<uint8_t>::max();
constexpr size_t kMaxTextSize = 64;

Bytes ApplyHostBits(const Inet& value, uint32_t bits, bool set) {
  Bytes out = value.address;
  for (uint32_t i = 0; i < value.Width(); ++i) {
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

std::string Format(bool v6, const uint8_t* address, uint32_t mask) {
  INET_IPAddress inet;
  inet.type = v6 ? INET_IP_ADDRESS_V6 : INET_IP_ADDRESS_V4;
  if (v6) {
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
  const bool v6 = key.size() == LongExactSize(true);
  if (!v6 && key.size() != LongExactSize(false)) {
    return std::nullopt;
  }
  const auto* data = reinterpret_cast<const uint8_t*>(key.data());
  if (data[0] != (v6 ? kV6Type : kV4Type)) {
    return std::nullopt;
  }
  const auto* mask = data + kTagSize + Width(v6);
  Inet value{.v6 = v6, .mask = absl::big_endian::Load16(mask)};
  std::memcpy(value.address.data(), mask + kMaskSize, Width(v6));
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
    const uint64_t upper =
      value.v6 ? absl::big_endian::Load64(address) : uint64_t{0};
    const uint64_t lower =
      value.v6 ? absl::big_endian::Load64(address + sizeof(uint64_t))
               : absl::big_endian::Load32(address);
    auto* bytes = _bytes;
    _count = WriteTerms(value.v6 ? kV6Type : kV4Type, upper, lower, value.mask,
                        _terms, bytes);
  }

  EncodedTerms(const EncodedTerms&) = delete;
  EncodedTerms& operator=(const EncodedTerms&) = delete;

  irs::bstring operator[](uint32_t i) const {
    SDB_ASSERT(i < _count);
    return ToBString(_terms[i]);
  }

 private:
  duckdb::string_t _terms[kMaxTermsPerValue];
  uint8_t _bytes[kV6LongTermBytes];
  uint32_t _count = 0;
};

Inet NetworkValue(bool v6, const Bytes& network, uint32_t mask) {
  return {.v6 = v6, .mask = static_cast<uint16_t>(mask), .address = network};
}

}  // namespace

std::optional<Inet> FromAddress(uint8_t ip_type, uint64_t upper, uint64_t lower,
                                uint16_t mask) {
  Inet value;
  value.mask = mask;
  if (ip_type == kV4Type) {
    if (upper != 0 || lower > std::numeric_limits<uint32_t>::max()) {
      return std::nullopt;
    }
    absl::big_endian::Store32(value.address.data(),
                              static_cast<uint32_t>(lower));
    return value;
  }
  if (ip_type == kV6Type) {
    value.v6 = true;
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
  const auto ip_type = children[pg::kInetIpTypeChild].GetValue<uint8_t>();
  const auto address =
    children[pg::kInetAddressChild].GetValue<duckdb::hugeint_t>();
  auto upper = static_cast<uint64_t>(address.upper);
  if (ip_type == kV6Type) {
    upper ^= pg::kInetV6SortBit;
  }
  return FromAddress(ip_type, upper, address.lower,
                     children[pg::kInetMaskChild].GetValue<uint16_t>());
}

Inet FamilyMin(bool v6) { return Inet{.v6 = v6}; }

Inet FamilyMax(bool v6) {
  Inet value{.v6 = v6, .mask = std::numeric_limits<uint16_t>::max()};
  value.address.fill(kAllBits);
  return value;
}

Bytes Network(const Inet& value, uint32_t bits) {
  return ApplyHostBits(value, bits, false);
}

Bytes Broadcast(const Inet& value, uint32_t bits) {
  return ApplyHostBits(value, bits, true);
}

uint32_t MinMaskOf(bool v6, const Bytes& network) {
  for (uint32_t i = Width(v6); i != 0; --i) {
    if (const auto byte = network[i - 1]) {
      return i * CHAR_BIT - static_cast<uint32_t>(std::countr_zero(byte));
    }
  }
  return 0;
}

irs::bstring ExactTermOf(const Inet& value) { return EncodedTerms{value}[0]; }

std::vector<irs::bstring> GranularTermsOf(const Inet& value) {
  const EncodedTerms encoded{value};
  std::vector<irs::bstring> terms;
  terms.reserve(1 + LevelCount(value.v6));
  for (uint32_t i = 0; i <= LevelCount(value.v6); ++i) {
    terms.push_back(encoded[i]);
  }
  return terms;
}

irs::bstring LevelTermOf(bool v6, uint32_t level, const Bytes& network) {
  return EncodedTerms{NetworkValue(v6, network, MaxBits(v6))}[level];
}

irs::bstring MaskTermOf(bool v6, uint8_t mask) {
  duckdb::string_t term;
  WriteMaskTerm(v6, mask, term);
  return ToBString(term);
}

irs::bstring ExactPrefixOf(bool v6, const Bytes& network) {
  auto prefix = ExactTermOf(NetworkValue(v6, network, MaxBits(v6)));
  prefix.resize(kTagSize + Width(v6));
  return prefix;
}

irs::bstring ExactPrefixOf(bool v6, const Bytes& network, uint16_t mask) {
  auto prefix = ExactTermOf(NetworkValue(v6, network, mask));
  SDB_ASSERT(prefix.size() == ShortExactSize(v6));
  return prefix;
}

irs::bstring ExactUpperOf(bool v6, const Bytes& network) {
  auto upper = ExactPrefixOf(v6, network);
  upper.append(LongExactSize(v6) - upper.size(), irs::byte_type{kAllBits});
  return upper;
}

irs::bstring CoarsestLevelPrefixOf(bool v6) {
  return irs::bstring(1, LevelTag(v6, LevelCount(v6)));
}

std::string Describe(irs::bytes_view term) {
  if (term.empty() || term[0] < kV4Exact || term[0] > MaskTag(true)) {
    return {};
  }
  const bool v6 = term[0] >= kV6Exact;
  const uint8_t base = ExactTag(v6);
  const auto width = Width(v6);
  const auto tag = term[0];
  if (tag == MaskTag(v6) && term.size() == kMaskTermSize) {
    return absl::StrCat("/", term[kTagSize]);
  }
  if (term.size() == kTagSize) {
    return v6 ? "ipv6" : "ipv4";
  }
  const auto* network = term.data() + kTagSize;
  if (tag > base && tag <= LevelTag(v6, LevelCount(v6))) {
    Bytes level_network{};
    std::memcpy(level_network.data(), network,
                std::min<size_t>(term.size() - kTagSize, width));
    return Format(v6, level_network.data(), LevelBits(v6, tag - base));
  }
  if (tag != base) {
    return {};
  }
  if (term.size() == kTagSize + width) {
    return Format(v6, network, MaxBits(v6));
  }
  if (term.size() != ShortExactSize(v6) && term.size() != LongExactSize(v6)) {
    return v6 ? "ipv6" : "ipv4";
  }
  const auto mask = absl::big_endian::Load16(network + width);
  if (term.size() == ShortExactSize(v6)) {
    return Format(v6, network, std::min<uint32_t>(mask, MaxBits(v6)));
  }
  if (mask > MaxBits(v6)) {
    return Format(v6, network, MaxBits(v6));
  }
  return Format(v6, term.data() + ShortExactSize(v6), mask);
}

}  // namespace sdb::connector::inet_terms
