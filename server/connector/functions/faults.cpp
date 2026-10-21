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

#include "connector/functions/faults.h"

#ifdef SDB_FAULT_INJECTION
#include <absl/time/time.h>

#include <duckdb/function/table_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <string>
#include <utility>

#include "auth/role_closure.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#endif

namespace sdb::connector {

#ifdef SDB_FAULT_INJECTION
namespace {

struct WaitFaultData final : duckdb::TableFunctionData {
  explicit WaitFaultData(std::string name_p) : name{std::move(name_p)} {}

  std::string name;
};

duckdb::unique_ptr<duckdb::FunctionData> WaitFaultBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::Identifier>& names) {
  auto* conn = GetSereneDBContextPtr(context);
  if (conn && !auth::ClosureFor(&context, conn->GetRoleId())->is_superuser) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied for function sdb_wait_fault"));
  }
  if (input.inputs[0].IsNull()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    ERR_MSG("Failure point name cannot be NULL"));
  }
  return_types.emplace_back(duckdb::LogicalType::BOOLEAN);
  names.emplace_back("Success");
  return duckdb::make_uniq<WaitFaultData>(
    duckdb::StringValue::Get(input.inputs[0]));
}

void WaitFaultExecute(duckdb::ClientContext&, duckdb::TableFunctionInput& input,
                      duckdb::DataChunk&) {
  const auto& name = input.bind_data->Cast<WaitFaultData>().name;
  if (!irs::AwaitParkedFailurePointDebugging(name, absl::Seconds(60))) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_QUERY_CANCELED),
                    ERR_MSG("failure point '", name, "' was not reached"));
  }
}

}  // namespace
#endif

void RegisterFaultFunctions(duckdb::DatabaseInstance& db) {
#ifdef SDB_FAULT_INJECTION
  duckdb::ExtensionLoader loader{db, "serenedb"};
  loader.RegisterFunction(duckdb::TableFunction{"sdb_wait_fault",
                                                {duckdb::LogicalType::VARCHAR},
                                                WaitFaultExecute,
                                                WaitFaultBind});
#endif
}

}  // namespace sdb::connector
