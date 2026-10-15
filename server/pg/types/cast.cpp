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

#include "pg/types/cast.h"

#include <duckdb/common/operator/cast_operators.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

namespace sdb::pg {

void InputError(irs::pg::SqlErrorData&& error,
                duckdb::CastParameters& parameters) {
  if (!parameters.error_message) {
    THROW_SQL_ERROR_FROM_DATA(std::move(error));
  }
  duckdb::HandleCastError::AssignError(error.errmsg, parameters);
}

void RegisterTextInput(duckdb::CastFunctionSet& casts,
                       const duckdb::LogicalType& type,
                       duckdb::cast_function_t input,
                       duckdb::init_cast_local_state_t init_local_state) {
  for (const auto text : {duckdb::LogicalTypeId::VARCHAR,
                          duckdb::LogicalTypeId::STRING_LITERAL}) {
    casts.RegisterCastFunction(
      duckdb::LogicalType{text}, type,
      duckdb::BoundCastInfo{input, nullptr, init_local_state}, 50);
  }
}

}  // namespace sdb::pg
