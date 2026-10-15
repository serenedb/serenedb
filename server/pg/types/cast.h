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

#include <cstddef>
#include <cstdint>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/function/cast/cast_function_set.hpp>
#include <duckdb/function/cast/default_casts.hpp>
#include <iresearch/utils/pg/sql_error.hpp>
#include <optional>
#include <string_view>

namespace sdb::pg {

struct ParsedNumber {
  uint64_t magnitude;
  bool negative;
  bool out_of_range;
  size_t end;
};

ParsedNumber ParseNumber(std::string_view text, size_t start, int base);

void InputError(irs::pg::SqlErrorData&& error,
                duckdb::CastParameters& parameters);

template<typename T, auto Parse>
bool TextInput(duckdb::Vector& source, duckdb::Vector& result,
               duckdb::idx_t count, duckdb::CastParameters& parameters) {
  bool converted = true;
  duckdb::UnaryExecutor::Execute<duckdb::string_t, T>(
    source, result, count, [&](duckdb::string_t text) -> std::optional<T> {
      T value{};
      auto error =
        Parse(std::string_view{text.GetData(), text.GetSize()}, value);
      if (!error) {
        return value;
      }
      InputError(std::move(*error), parameters);
      converted = false;
      return std::nullopt;
    });
  return converted;
}

void RegisterTextInput(duckdb::CastFunctionSet& casts,
                       const duckdb::LogicalType& type,
                       duckdb::cast_function_t input,
                       duckdb::init_cast_local_state_t init_local_state);

void ReplaceTextInput(duckdb::CastFunctionSet& casts,
                      const duckdb::LogicalType& type,
                      duckdb::cast_function_t input);

}  // namespace sdb::pg
