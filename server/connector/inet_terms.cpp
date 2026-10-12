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

#include <cstring>
#include <duckdb/common/types/value.hpp>
#include <duckdb/inet/inet_ipaddress.hpp>

namespace sdb::connector::inet_terms {
namespace {

constexpr uint8_t kV4Exact = 0x40;
constexpr uint8_t kV6Exact = 0x60;
constexpr uint8_t kMaskOffset = 8;
constexpr uint8_t kV4Type = 1;
constexpr uint8_t kV6Type = 2;

uint8_t ExactTag(bool v6) noexcept { return v6 ? kV6Exact : kV4Exact; }
uint8_t MaskTag(bool v6) noexcept { return ExactTag(v6) + kMaskOffset; }
uint32_t Width(bool v6) noexcept { return v6 ? 16 : 4; }
uint32_t LevelStep(bool v6) noexcept { return v6 ? 16 : 8; }

Bytes ApplyHostBits(const Inet& value, uint32_t bits, bool set) {
  Bytes out = value.address;
  for (uint32_t i = 0; i < value.Width(); ++i) {
    const uint32_t first = i * 8;
    if (first + 8 <= bits) {
      continue;
    }
    const auto keep = first >= bits
                        ? uint8_t{0}
                        : static_cast<uint8_t>(0xFF << (8 - (bits - first)));
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
    inet.address.lower = absl::big_endian::Load64(address + 8);
  } else {
    inet.address.upper = 0;
    inet.address.lower = absl::big_endian::Load32(address);
  }
  inet.mask = static_cast<uint16_t>(mask);
  char buf[64];
  const auto size = ipaddress_to_string(&inet, buf, sizeof(buf));
  return std::string{buf, size};
}

}  // namespace

std::optional<Inet> FromAddress(uint8_t ip_type, uint64_t upper, uint64_t lower,
                                uint16_t mask) {
  Inet value;
  value.mask = mask;
  if (ip_type == kV4Type) {
    if (upper != 0 || lower > 0xFFFFFFFF) {
      return std::nullopt;
    }
    absl::big_endian::Store32(value.address.data(),
                              static_cast<uint32_t>(lower));
    return value;
  }
  if (ip_type == kV6Type) {
    value.v6 = true;
    absl::big_endian::Store64(value.address.data(), upper);
    absl::big_endian::Store64(value.address.data() + 8, lower);
    return value;
  }
  return std::nullopt;
}

std::optional<Inet> FromValue(const duckdb::Value& value) {
  if (value.IsNull()) {
    return std::nullopt;
  }
  if (value.type().id() == duckdb::LogicalTypeId::BLOB) {
    const auto& key = duckdb::StringValue::Get(value);
    const auto* data = reinterpret_cast<const uint8_t*>(key.data());
    if (key.size() == 11 && data[0] == kV4Type) {
      return FromAddress(kV4Type, 0, absl::big_endian::Load32(data + 7),
                         absl::big_endian::Load16(data + 5));
    }
    if (key.size() == 35 && data[0] == kV6Type) {
      return FromAddress(kV6Type, absl::big_endian::Load64(data + 19),
                         absl::big_endian::Load64(data + 27),
                         absl::big_endian::Load16(data + 17));
    }
    return std::nullopt;
  }
  if (value.type().id() != duckdb::LogicalTypeId::STRUCT) {
    return std::nullopt;
  }
  const auto& children = duckdb::StructValue::GetChildren(value);
  for (const auto& child : children) {
    if (child.IsNull()) {
      return std::nullopt;
    }
  }
  if (children.size() == 3) {
    const auto ip_type = children[0].GetValue<uint8_t>();
    const auto address = children[1].GetValue<duckdb::hugeint_t>();
    auto upper = static_cast<uint64_t>(address.upper);
    if (ip_type == kV6Type) {
      upper ^= uint64_t{1} << 63;
    }
    return FromAddress(ip_type, upper, address.lower,
                       children[2].GetValue<uint16_t>());
  }
  return std::nullopt;
}

Inet FamilyMin(bool v6) { return Inet{.v6 = v6}; }

Inet FamilyMax(bool v6) {
  Inet value{.v6 = v6, .mask = 0xFFFF};
  value.address.fill(0xFF);
  return value;
}

Bytes Network(const Inet& value, uint32_t bits) {
  return ApplyHostBits(value, bits, false);
}

Bytes Broadcast(const Inet& value, uint32_t bits) {
  return ApplyHostBits(value, bits, true);
}

uint32_t LevelCount(bool v6) { return v6 ? 7 : 3; }

uint32_t LevelBits(bool v6, uint32_t level) {
  return Width(v6) * 8 - level * LevelStep(v6);
}

size_t ExactTerm(const Inet& value, uint8_t* out) {
  const auto width = value.Width();
  const auto network = Network(value, value.NetworkBits());
  out[0] = ExactTag(value.v6);
  std::memcpy(out + 1, network.data(), width);
  absl::big_endian::Store16(out + 1 + width, value.mask);
  std::memcpy(out + 3 + width, value.address.data(), width);
  return 3 + 2 * width;
}

size_t LevelTerm(bool v6, uint32_t level, const Bytes& network, uint8_t* out) {
  const auto size = LevelBits(v6, level) / 8;
  out[0] = static_cast<uint8_t>(ExactTag(v6) + level);
  std::memcpy(out + 1, network.data(), size);
  return 1 + size;
}

size_t MaskTerm(bool v6, uint8_t mask, uint8_t* out) {
  out[0] = MaskTag(v6);
  out[1] = mask;
  return 2;
}

irs::bstring ExactTermOf(const Inet& value) {
  uint8_t buf[kMaxTermSize];
  return ToBString(buf, ExactTerm(value, buf));
}

std::vector<irs::bstring> GranularTermsOf(const Inet& value) {
  std::vector<irs::bstring> terms;
  terms.reserve(1 + LevelCount(value.v6));
  terms.push_back(ExactTermOf(value));
  const auto network = Network(value, value.NetworkBits());
  for (uint32_t level = 1; level <= LevelCount(value.v6); ++level) {
    terms.push_back(LevelTermOf(value.v6, level, network));
  }
  return terms;
}

irs::bstring LevelTermOf(bool v6, uint32_t level, const Bytes& network) {
  uint8_t buf[kMaxTermSize];
  return ToBString(buf, LevelTerm(v6, level, network, buf));
}

irs::bstring MaskTermOf(bool v6, uint8_t mask) {
  uint8_t buf[2];
  return ToBString(buf, MaskTerm(v6, mask, buf));
}

irs::bstring ExactPrefixOf(bool v6, const Bytes& network) {
  uint8_t buf[kMaxTermSize];
  buf[0] = ExactTag(v6);
  std::memcpy(buf + 1, network.data(), Width(v6));
  return ToBString(buf, 1 + Width(v6));
}

irs::bstring ExactPrefixOf(bool v6, const Bytes& network, uint16_t mask) {
  auto prefix = ExactPrefixOf(v6, network);
  uint8_t buf[2];
  absl::big_endian::Store16(buf, mask);
  prefix.append(reinterpret_cast<const irs::byte_type*>(buf), sizeof(buf));
  return prefix;
}

irs::bstring ExactUpperOf(bool v6, const Bytes& network) {
  auto upper = ExactPrefixOf(v6, network);
  upper.append(2 + Width(v6), irs::byte_type{0xFF});
  return upper;
}

irs::bstring CoarsestLevelPrefixOf(bool v6) {
  return irs::bstring(
    1, static_cast<irs::byte_type>(ExactTag(v6) + LevelCount(v6)));
}

std::string Describe(irs::bytes_view term) {
  if (term.empty() || term[0] < kV4Exact || term[0] > MaskTag(true)) {
    return {};
  }
  const bool v6 = term[0] >= kV6Exact;
  const uint8_t base = ExactTag(v6);
  const auto width = Width(v6);
  const auto tag = term[0];
  if (tag == MaskTag(v6) && term.size() == 2) {
    return absl::StrCat("/", term[1]);
  }
  if (term.size() == 1) {
    return v6 ? "ipv6" : "ipv4";
  }
  if (tag > base && tag <= base + LevelCount(v6)) {
    Bytes network{};
    std::memcpy(network.data(), term.data() + 1,
                std::min<size_t>(term.size() - 1, width));
    return Format(v6, network.data(), LevelBits(v6, tag - base));
  }
  if (tag != base) {
    return {};
  }
  if (term.size() == 3 + 2 * width) {
    const auto mask = absl::big_endian::Load16(term.data() + 1 + width);
    if (mask > width * 8) {
      return Format(v6, term.data() + 1, width * 8);
    }
    return Format(v6, term.data() + 3 + width, mask);
  }
  if (term.size() == 3 + width) {
    return Format(v6, term.data() + 1,
                  absl::big_endian::Load16(term.data() + 1 + width));
  }
  if (term.size() == 1 + width) {
    return Format(v6, term.data() + 1, width * 8);
  }
  return v6 ? "ipv6" : "ipv4";
}

}  // namespace sdb::connector::inet_terms
