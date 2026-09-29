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

#include <duckdb/common/error_data.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/serialization.hpp>
#include <iresearch/utils/serializer.hpp>
#include <string>
#include <string_view>

namespace sdb::catalog::persistence {

template<typename T>
std::string Pack(const T& value) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer{stream, duckdb::DatabaseStorageOptions()};
  irs::utils::WriteTuple(serializer, value);
  return std::string{reinterpret_cast<const char*>(stream.GetData()),
                     stream.GetPosition()};
}

template<typename T>
T Unpack(std::string_view object, std::string_view name,
         std::string_view bytes) {
  duckdb::MemoryStream stream{
    const_cast<duckdb::data_ptr_t>(
      reinterpret_cast<duckdb::const_data_ptr_t>(bytes.data())),
    bytes.size()};
  duckdb::BinaryDeserializer deserializer{stream};
  T value;
  std::string error;
  try {
    irs::utils::ReadTuple(deserializer, value);
    if (stream.GetPosition() != bytes.size()) {
      error = "unexpected bytes after the end of the definition";
    }
  } catch (const std::exception& e) {
    error = duckdb::ErrorData{e}.RawMessage();
  }
  if (!error.empty()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DATA_CORRUPTED),
      ERR_MSG("cannot read ", object, " \"", name,
              "\": it was stored by a newer release of SereneDB or is corrupt"),
      ERR_DETAIL(error));
  }
  return value;
}

}  // namespace sdb::catalog::persistence
