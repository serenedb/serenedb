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

#include "pg/inet.h"

#include <absl/algorithm/container.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <fast_float/fast_float.h>

#include <algorithm>
#include <cstring>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <string>

namespace sdb::pg {
namespace {

std::string Unabbreviate(std::string_view text) {
  const auto slash = text.find('/');
  const auto address = text.substr(0, slash);
  const auto octets = static_cast<int>(absl::c_count(address, '.')) + 1;
  if (address.empty() || octets >= 4 || absl::StrContains(text, ':')) {
    return std::string{text};
  }
  std::string out{address};
  for (auto i = octets; i < 4; ++i) {
    absl::StrAppend(&out, ".0");
  }
  if (slash != std::string_view::npos) {
    absl::StrAppend(&out, text.substr(slash));
    return out;
  }
  uint32_t first = 0;
  if (fast_float::from_chars(address.data(), address.data() + address.size(),
                             first)
        .ec != std::errc{}) {
    return std::string{text};
  }
  auto bits = first >= 240   ? 32
              : first >= 224 ? 8
              : first >= 192 ? 24
              : first >= 128 ? 16
                             : 8;
  bits = std::max(bits, octets * 8);
  if (bits == 8 && first == 224) {
    bits = 4;
  }
  absl::StrAppend(&out, "/", bits);
  return out;
}

}  // namespace

void StoreInet(duckdb::Vector& vec, duckdb::idx_t row, INET_IPAddressType type,
               uint64_t addr_hi, uint64_t addr_lo, uint16_t mask) {
  if (type == INET_IP_ADDRESS_V6) {
    addr_hi ^= (uint64_t{1} << 63);  // re-apply IPv6 sort-order flip
  }
  auto& entries = duckdb::StructVector::GetEntries(vec);
  duckdb::FlatVector::GetDataMutable<uint8_t>(entries[0])[row] =
    static_cast<uint8_t>(type);
  duckdb::hugeint_t stored;
  stored.lower = addr_lo;
  stored.upper = static_cast<int64_t>(addr_hi);
  duckdb::FlatVector::GetDataMutable<duckdb::hugeint_t>(entries[1])[row] =
    stored;
  duckdb::FlatVector::GetDataMutable<uint16_t>(entries[2])[row] = mask;
}

bool IsNetwork(const INET_IPAddress& inet) {
  const auto network = ipaddress_network(&inet);
  return network.address.upper == inet.address.upper &&
         network.address.lower == inet.address.lower;
}

INET_IPAddress ParseCidr(std::string_view text) {
  const auto input = Unabbreviate(text);
  INET_IPAddress inet{};
  try {
    inet = ipaddress_from_string(input.data(), input.size());
  } catch (const std::exception&) {
    inet.type = INET_IP_ADDRESS_INVALID;
  }
  if (inet.type == INET_IP_ADDRESS_INVALID) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
      ERR_MSG("invalid input syntax for type cidr: \"", text, "\""));
  }
  if (!IsNetwork(inet)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    ERR_MSG("invalid cidr value: \"", text, "\""),
                    ERR_DETAIL("Value has bits set to right of mask."));
  }
  return inet;
}

size_t CidrToString(const INET_IPAddress& inet, char* buffer, size_t size) {
  auto written = ipaddress_to_string(&inet, buffer, size);
  const std::string_view full =
    inet.type == INET_IP_ADDRESS_V6 ? "/128" : "/32";
  if (!std::string_view{buffer, written}.contains('/') &&
      written + full.size() <= size) {
    std::memcpy(buffer + written, full.data(), full.size());
    written += full.size();
  }
  return written;
}

}  // namespace sdb::pg
