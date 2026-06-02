////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/base/internal/endian.h>

#include "basics/common.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"

namespace sdb::rocksutils {

template<typename T>
inline T UintFromPersistentLittleEndian(const char* p) {
  static_assert(std::is_unsigned_v<T>);
  T value;
  memcpy(&value, p, sizeof(T));
  return absl::little_endian::ToHost<T>(value);
}

template<typename T>
inline T UintFromPersistentBigEndian(const char* p) {
  static_assert(std::is_unsigned_v<T>);
  T value;
  memcpy(&value, p, sizeof(T));
  return absl::big_endian::ToHost<T>(value);
}

template<typename T>
inline void UintToPersistentLittleEndian(std::string& p, T value) {
  static_assert(std::is_unsigned_v<T>);
  value = absl::little_endian::FromHost<T>(value);
  p.append(reinterpret_cast<const char*>(&value), sizeof(T));
}

template<typename T>
inline void UintToPersistentBigEndian(std::string& p, T value) {
  static_assert(std::is_unsigned_v<T>);
  value = absl::big_endian::FromHost<T>(value);
  p.append(reinterpret_cast<const char*>(&value), sizeof(T));
}

template<typename T>
inline void UintToPersistentRawLe(char* p, T value) {
  static_assert(std::is_unsigned_v<T>);
  value = absl::little_endian::FromHost<T>(value);
  memcpy(p, &value, sizeof(T));
}

template<typename T>
inline void UintToPersistentRawBe(char* p, T value) {
  static_assert(std::is_unsigned_v<T>);
  value = absl::big_endian::FromHost<T>(value);
  memcpy(p, &value, sizeof(T));
}

// TODO(mbkkt) we probably need big endian format only for ability to specify
// ranges for some keys. I think for most writes it's unnecessary,
// so needs to change it to little endian
constexpr uint16_t Uint16FromPersistent(const char* p) {
  return UintFromPersistentBigEndian<uint16_t>(p);
}
constexpr uint32_t Uint32FromPersistent(const char* p) {
  return UintFromPersistentBigEndian<uint32_t>(p);
}
constexpr uint64_t Uint64FromPersistent(const char* p) {
  return UintFromPersistentBigEndian<uint64_t>(p);
}
constexpr void Uint16ToPersistent(std::string& p, uint16_t value) {
  UintToPersistentBigEndian<uint16_t>(p, value);
}
constexpr void Uint32ToPersistent(std::string& p, uint32_t value) {
  UintToPersistentBigEndian<uint32_t>(p, value);
}
constexpr void Uint64ToPersistent(std::string& p, uint64_t value) {
  UintToPersistentBigEndian<uint64_t>(p, value);
}
constexpr void Uint64ToPersistentRaw(char* p, uint64_t value) {
  UintToPersistentRawBe<uint64_t>(p, value);
}

}  // namespace sdb::rocksutils
