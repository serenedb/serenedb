////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <span>
#include <string>
#include <type_traits>

#include "basics/assert.h"
#include "basics/key_concat.h"

namespace sdb::connector::primary_key {

template<typename T>
void AppendSigned(std::string& key, T value) {
  SDB_ASSERT(std::is_signed_v<T>,
             "Cannot correctly store unsigned value due to sign bit flipping");
  const auto base_size = key.size();
  basics::StrAppend(key, sizeof(T));
  absl::big_endian::Store(key.data() + base_size, value);
  key[base_size] = static_cast<uint8_t>(key[base_size]) ^ 0x80;
}

template<typename T>
T ReadSigned(std::string_view buf) {
  SDB_ASSERT(std::is_signed_v<T>,
             "Cannot correctly read unsigned value due to sign bit flipping");
  SDB_ASSERT(buf.size() >= sizeof(T));
  using Unsigned = std::make_unsigned_t<T>;
  static constexpr int kBits = sizeof(T) * CHAR_BIT;

  auto val = absl::big_endian::Load<Unsigned>(buf.data());
  val ^= static_cast<Unsigned>(1) << (kBits - 1);

  return std::bit_cast<T>(val);
}

}  // namespace sdb::connector::primary_key
