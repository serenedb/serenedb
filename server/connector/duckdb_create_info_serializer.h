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

#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/parser/parsed_data/create_info.hpp>
#include <string>

namespace sdb::connector {

// Serialize a DuckDB CreateInfo (CreateMacroInfo, CreateViewInfo, etc.)
// to a binary string for storage in RocksDB.
inline std::string SerializeCreateInfo(const duckdb::CreateInfo& info) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer(stream);
  info.Serialize(serializer);
  return std::string(reinterpret_cast<const char*>(stream.GetData()),
                     stream.GetPosition());
}

// Deserialize a DuckDB CreateInfo from a binary string.
inline duckdb::unique_ptr<duckdb::CreateInfo> DeserializeCreateInfo(
  const std::string& data) {
  duckdb::MemoryStream stream(
    reinterpret_cast<duckdb::data_ptr_t>(const_cast<char*>(data.data())),
    data.size());
  duckdb::BinaryDeserializer deserializer(stream);
  return duckdb::CreateInfo::Deserialize(deserializer);
}

}  // namespace sdb::connector
