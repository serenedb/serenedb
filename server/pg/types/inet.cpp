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

#include "pg/types/inet.h"

#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>

namespace sdb::pg {

void StoreInet(duckdb::Vector& vec, duckdb::idx_t row,
               const INET_IPAddress& inet) {
  auto upper = inet.address.upper;
  if (inet.type == INET_IP_ADDRESS_V6) {
    upper ^= kInetV6SortBit;
  }
  auto& entries = duckdb::StructVector::GetEntries(vec);
  duckdb::FlatVector::GetDataMutable<uint8_t>(entries[kInetIpTypeChild])[row] =
    static_cast<uint8_t>(inet.type);
  duckdb::hugeint_t stored;
  stored.lower = inet.address.lower;
  stored.upper = static_cast<int64_t>(upper);
  duckdb::FlatVector::GetDataMutable<duckdb::hugeint_t>(
    entries[kInetAddressChild])[row] = stored;
  duckdb::FlatVector::GetDataMutable<uint16_t>(entries[kInetMaskChild])[row] =
    inet.mask;
}

}  // namespace sdb::pg
