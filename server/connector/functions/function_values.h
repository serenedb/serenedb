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
#include <duckdb/common/identifier.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/function/table_function.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <string>
#include <string_view>

namespace sdb::connector {

inline std::string ReadRelation(const duckdb::TableFunctionBindInput& input,
                                std::string_view function) {
  SDB_ASSERT(!input.inputs.empty());
  if (input.inputs[0].IsNull()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(function, " requires a relation name"));
  }
  return input.inputs[0].GetValue<std::string>();
}

inline const duckdb::Value& NamedParameter(
  const duckdb::TableFunctionBindInput& input, std::string_view name) {
  const auto entry = input.named_parameters.find(duckdb::Identifier{name});
  SDB_ASSERT(entry != input.named_parameters.end());
  return entry->second;
}

inline duckdb::Value Bigint(uint64_t value) {
  return duckdb::Value::BIGINT(static_cast<int64_t>(value));
}

}  // namespace sdb::connector
