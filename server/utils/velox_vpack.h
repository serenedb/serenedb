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

#include <vpack/vpack.h>

#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/storage_compatibility.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/storage/storage_info.hpp>

#include "basics/errors.h"
#include "basics/exceptions.h"

namespace duckdb {

inline duckdb::SerializationOptions VersionStorageOptions() {
  duckdb::SerializationOptions opts;
  opts.storage_compatibility =
    duckdb::StorageCompatibility::FromIndex(duckdb::StorageVersion::V2_0_0);
  return opts;
}

void VPackWrite(auto ctx, const duckdb::LogicalType& type) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer::Serialize(type, stream, VersionStorageOptions());
  auto data = stream.GetData();
  auto size = stream.GetPosition();
  ctx.vpack().add(std::string_view{reinterpret_cast<const char*>(data), size});
}

void VPackRead(auto ctx, duckdb::LogicalType& type) {
  auto vpack = ctx.vpack();
  if (vpack.isString()) {
    auto str = vpack.stringViewUnchecked();
    duckdb::MemoryStream stream(
      const_cast<duckdb::data_t*>(
        reinterpret_cast<const duckdb::data_t*>(str.data())),
      str.size());
    duckdb::BinaryDeserializer deserializer(stream);
    type = duckdb::LogicalType::Deserialize(deserializer);
  } else {
    type = duckdb::LogicalType::VARCHAR;
  }
}

}  // namespace duckdb
