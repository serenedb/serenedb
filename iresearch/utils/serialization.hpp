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

#include <cstdint>
#include <duckdb/common/serializer/serializer.hpp>
#include <duckdb/common/storage_compatibility.hpp>
#include <duckdb/storage/storage_info.hpp>
#include <string_view>

namespace duckdb {

inline SerializationOptions VersionStorageOptions() {
  SerializationOptions opts;
  opts.storage_compatibility =
    StorageCompatibility::FromIndex(SERENEDB_VERSION_DEFAULT);
  return opts;
}

inline std::string_view StorageVersionError(uint64_t version) {
  if (version > static_cast<uint64_t>(SERENEDB_VERSION_UPPER)) {
    return "it was written by a newer release of SereneDB";
  }
  if (version < static_cast<uint64_t>(SERENEDB_VERSION_LOWER)) {
    return "it is older than this release of SereneDB reads; upgrade it "
           "through an earlier release first";
  }
  return {};
}

}  // namespace duckdb
