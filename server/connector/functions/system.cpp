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

#include "connector/functions/system.h"

#include <duckdb/common/vector_operations/generic_executor.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/extension/extension_loader.hpp>

namespace sdb::connector {

// current_setting(name, missing_ok) -> text
// Ported from server/pg/functions/system.cpp CurrentSettingMissingOkFunction.
void CurrentSetting2Function(duckdb::DataChunk& args,
                             duckdb::ExpressionState& state,
                             duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto count = args.size();

  duckdb::BinaryExecutor::ExecuteWithNulls<duckdb::string_t, bool,
                                           duckdb::string_t>(
    args.data[0], args.data[1], result, count,
    [&](duckdb::string_t name, bool missing_ok, duckdb::ValidityMask& mask,
        duckdb::idx_t idx) -> duckdb::string_t {
      std::string key{name.GetData(), name.GetSize()};
      duckdb::Value value;
      if (context.TryGetCurrentSetting(key, value)) {
        auto str = value.ToString();
        return duckdb::StringVector::AddString(result, str);
      }
      if (missing_ok) {
        mask.SetInvalid(idx);
        return duckdb::string_t();
      }
      throw duckdb::InvalidInputException(
        "unrecognized configuration parameter \"%s\"", key);
    });
}

// set_config(name, value, is_local) -> text
// Ported from server/pg/functions/system.cpp SetConfigFunction.
void SetConfigFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                       duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto& db_config = duckdb::DBConfig::GetConfig(*context.db);

  duckdb::TernaryExecutor::Execute<duckdb::string_t, duckdb::string_t, bool,
                                   duckdb::string_t>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t name, duckdb::string_t value,
        bool /*is_local*/) -> duckdb::string_t {
      std::string key{name.GetData(), name.GetSize()};
      std::string val{value.GetData(), value.GetSize()};
      // Set via TryGetSettingIndex + SetUserSetting on client config
      duckdb::optional_ptr<const duckdb::ConfigurationOption> option;
      auto setting_index = db_config.TryGetSettingIndex(key, option);
      if (setting_index.IsValid()) {
        context.config.user_settings.SetUserSetting(setting_index.GetIndex(),
                                                    duckdb::Value(val));
      } else {
        throw duckdb::InvalidInputException(
          "unrecognized configuration parameter \"%s\"", key);
      }
      return duckdb::StringVector::AddString(result, val);
    });
}

// num_nonnulls(...) -> int
// Ported from PG: counts non-null arguments.
void NumNonNullsFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                         duckdb::Vector& result) {
  auto count = args.size();
  auto result_data = duckdb::FlatVector::GetData<int32_t>(result);

  for (duckdb::idx_t row = 0; row < count; row++) {
    int32_t non_nulls = 0;
    for (duckdb::idx_t col = 0; col < args.ColumnCount(); col++) {
      duckdb::UnifiedVectorFormat vdata;
      args.data[col].ToUnifiedFormat(count, vdata);
      auto idx = vdata.sel->get_index(row);
      if (vdata.validity.RowIsValid(idx)) {
        non_nulls++;
      }
    }
    result_data[row] = non_nulls;
  }
}

// num_nulls(...) -> int
// Ported from PG: counts null arguments.
void NumNullsFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                      duckdb::Vector& result) {
  auto count = args.size();
  auto result_data = duckdb::FlatVector::GetData<int32_t>(result);

  for (duckdb::idx_t row = 0; row < count; row++) {
    int32_t nulls = 0;
    for (duckdb::idx_t col = 0; col < args.ColumnCount(); col++) {
      duckdb::UnifiedVectorFormat vdata;
      args.data[col].ToUnifiedFormat(count, vdata);
      auto idx = vdata.sel->get_index(row);
      if (!vdata.validity.RowIsValid(idx)) {
        nulls++;
      }
    }
    result_data[row] = nulls;
  }
}

void RegisterPgSystemFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};

  // current_setting(name, missing_ok) -> text
  {
    duckdb::ScalarFunction func{
      "current_setting",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BOOLEAN},
      duckdb::LogicalType::VARCHAR,
      CurrentSetting2Function};
    func.null_handling = duckdb::FunctionNullHandling::SPECIAL_HANDLING;
    loader.RegisterFunction(func);
  }

  // set_config(name, value, is_local) -> text
  loader.RegisterFunction(duckdb::ScalarFunction{
    "set_config",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
     duckdb::LogicalType::BOOLEAN},
    duckdb::LogicalType::VARCHAR,
    SetConfigFunction});

  // num_nonnulls(...) -> int
  {
    duckdb::ScalarFunction func{"num_nonnulls",
                                {duckdb::LogicalType::ANY},
                                duckdb::LogicalType::INTEGER,
                                NumNonNullsFunction};
    func.varargs = duckdb::LogicalType::ANY;
    func.null_handling = duckdb::FunctionNullHandling::SPECIAL_HANDLING;
    loader.RegisterFunction(func);
  }

  // num_nulls(...) -> int
  {
    duckdb::ScalarFunction func{"num_nulls",
                                {duckdb::LogicalType::ANY},
                                duckdb::LogicalType::INTEGER,
                                NumNullsFunction};
    func.varargs = duckdb::LogicalType::ANY;
    func.null_handling = duckdb::FunctionNullHandling::SPECIAL_HANDLING;
    loader.RegisterFunction(func);
  }
}

}  // namespace sdb::connector
