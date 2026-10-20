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

#include <cstdint>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/inet/inet_ipaddress.hpp>
#include <iresearch/utils/shared.hpp>

#include "pg/types/logical.h"

namespace sdb::pg {

inline constexpr uint8_t kPgsqlAfInet = 2;
inline constexpr uint8_t kPgsqlAfInet6 = 3;
inline constexpr uint8_t kInetWireV4Size = sizeof(uint32_t);
inline constexpr uint8_t kInetWireV6Size = 2 * sizeof(uint64_t);

struct InetWireHeader {
  uint8_t family;
  uint8_t bits;
  uint8_t is_cidr;
  uint8_t size;
};

// STRUCT entries: [0]=ip_type (1=IPv4, 2=IPv6), [1]=address (signed hugeint,
// IPv6 top bit flipped for sort order), [2]=mask.
IRS_FORCE_INLINE inline void ReadInet(
  const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t row,
  INET_IPAddress& inet) {
  const auto& c0 = vdata.children[kInetIpTypeChild].unified;
  const auto& c1 = vdata.children[kInetAddressChild].unified;
  const auto& c2 = vdata.children[kInetMaskChild].unified;
  const auto ip_type =
    duckdb::UnifiedVectorFormat::GetData<uint8_t>(c0)[c0.sel->get_index(row)];
  const auto stored = duckdb::UnifiedVectorFormat::GetData<duckdb::hugeint_t>(
    c1)[c1.sel->get_index(row)];
  const auto mask =
    duckdb::UnifiedVectorFormat::GetData<uint16_t>(c2)[c2.sel->get_index(row)];
  inet.type = static_cast<INET_IPAddressType>(ip_type);
  inet.address.lower = stored.lower;
  inet.address.upper = static_cast<uint64_t>(stored.upper);
  if (inet.type == INET_IP_ADDRESS_V6) {
    inet.address.upper ^= kInetV6SortBit;
  }
  inet.mask = mask;
}

void StoreInet(duckdb::Vector& vec, duckdb::idx_t row,
               const INET_IPAddress& inet);

}  // namespace sdb::pg
