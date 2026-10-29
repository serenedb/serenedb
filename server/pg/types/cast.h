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
#include <duckdb/common/types/vector_cache.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/function/cast/cast_function_set.hpp>
#include <duckdb/function/cast/default_casts.hpp>
#include <duckdb/main/client_context.hpp>
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

ParsedNumber ParseNumber(std::string_view text, size_t start);

struct InputState : duckdb::FunctionLocalState {
  std::optional<irs::pg::SqlErrorData> error;
};

duckdb::unique_ptr<duckdb::FunctionLocalState> InitInput(
  duckdb::CastLocalStateParameters& parameters);

void InputError(irs::pg::SqlErrorData&& error,
                duckdb::CastParameters& parameters);

template<typename T, auto Parse>
bool TextInputCast(duckdb::Vector& source, duckdb::Vector& result,
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

class TextInput final {
 public:
  TextInput(duckdb::ClientContext& context, const duckdb::LogicalType& type);

  bool Valid(duckdb::string_t text);
  std::optional<irs::pg::SqlErrorData> Error(duckdb::string_t text);

 private:
  bool Cast(duckdb::string_t text, std::string* message);

  bool _reports;
  duckdb::BoundCastInfo _cast;
  duckdb::unique_ptr<duckdb::FunctionLocalState> _state;
  duckdb::VectorCache _cache;
  duckdb::Vector _source;
  duckdb::Vector _result;
};

void RegisterTextInput(duckdb::CastFunctionSet& casts,
                       const duckdb::LogicalType& type,
                       duckdb::cast_function_t input,
                       duckdb::init_cast_local_state_t init_local_state);

void RegisterTextInput(duckdb::CastFunctionSet& casts,
                       const duckdb::LogicalType& type,
                       duckdb::bind_cast_function_t bind);

void ReplaceTextInput(duckdb::CastFunctionSet& casts,
                      const duckdb::LogicalType& type,
                      duckdb::cast_function_t input);

duckdb::BoundCastInfo BindStorageCast(duckdb::BindCastInput& input,
                                      const duckdb::LogicalType& source,
                                      const duckdb::LogicalType& target,
                                      duckdb::cast_function_t check);

void RegisterStorageCasts(duckdb::CastFunctionSet& casts,
                          const duckdb::LogicalType& storage,
                          const duckdb::LogicalType& type,
                          duckdb::cast_function_t check,
                          duckdb::bind_cast_function_t bind);

template<duckdb::cast_function_t Check>
void RegisterStorageCast(duckdb::CastFunctionSet& casts,
                         const duckdb::LogicalType& storage,
                         const duckdb::LogicalType& type) {
  RegisterStorageCasts(
    casts, storage, type, Check,
    [](duckdb::BindCastInput& input, const duckdb::LogicalType& source,
       const duckdb::LogicalType& target) {
      return BindStorageCast(input, source, target, Check);
    });
}

}  // namespace sdb::pg
