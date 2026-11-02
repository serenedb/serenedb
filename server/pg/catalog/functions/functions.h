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

#include <algorithm>
#include <duckdb/catalog/permissions.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/function_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/parsed_expression.hpp>
#include <duckdb/planner/planner_extension.hpp>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "auth/role_closure.h"
#include "pg/catalog/utils/session.h"

namespace sdb::pg {

struct SystemMacro {
  std::string_view schema;
  std::string_view name;
  std::string_view macro_definition;
};

extern const std::span<const SystemMacro> kPgCatalogMacros;
extern const std::span<const SystemMacro> kInformationSchemaMacros;

struct NamedScalar {
  std::string_view name;
  duckdb::scalar_function_t function;
};

void Register(duckdb::ExtensionLoader& loader, std::string_view schema,
              duckdb::FunctionStability stability,
              duckdb::ScalarFunctionSet set);
void RegisterScalar(duckdb::ExtensionLoader& loader, std::string_view schema,
                    std::string_view name, duckdb::FunctionStability stability,
                    duckdb::init_local_state_t init,
                    std::span<const duckdb::ScalarFunction> functions);
void RegisterTable(duckdb::ExtensionLoader& loader, std::string_view schema,
                   duckdb::TableFunction function);

duckdb::unique_ptr<duckdb::Expression> BindNull(
  duckdb::FunctionBindExpressionInput& input);

inline duckdb::unique_ptr<duckdb::FunctionLocalState> InitSession(
  duckdb::ExpressionState& state, const duckdb::BoundFunctionExpression&,
  duckdb::FunctionData*) {
  return duckdb::make_uniq<SessionState>(&state.GetContext());
}

inline const Session& SessionOf(duckdb::ExpressionState& state) {
  return duckdb::ExecuteFunctionState::GetFunctionState(state)
    ->Cast<SessionState>()
    .session;
}

void WriteAclTexts(
  duckdb::VectorWriter<duckdb::VectorListType<duckdb::string_t>>& writer,
  std::span<const duckdb::AclItem> items, const auth::RoleGraph& roles,
  std::string& text);

template<typename Row>
struct RowsState final : duckdb::LocalTableFunctionState {
  std::vector<Row> rows;
  size_t next = 0;
  bool loaded = false;
};

template<typename Row>
duckdb::unique_ptr<duckdb::LocalTableFunctionState> InitRows(
  duckdb::ExecutionContext&, duckdb::TableFunctionInitInput&,
  duckdb::GlobalTableFunctionState*) {
  return duckdb::make_uniq<RowsState<Row>>();
}

template<typename Row, auto Load, auto Write>
duckdb::OperatorResultType RowsInOut(duckdb::ExecutionContext& context,
                                     duckdb::TableFunctionInput& data,
                                     duckdb::DataChunk& input,
                                     duckdb::DataChunk& output) {
  auto& state = data.local_state->Cast<RowsState<Row>>();
  if (!state.loaded) {
    state.rows.clear();
    state.next = 0;
    state.loaded = true;
    Load(context.client, input, state.rows);
  }
  if (state.next == state.rows.size()) {
    state.loaded = false;
    output.SetCardinality(0);
    return duckdb::OperatorResultType::NEED_MORE_INPUT;
  }
  const auto count =
    std::min<size_t>(state.rows.size() - state.next, STANDARD_VECTOR_SIZE);
  Write(std::span<const Row>{state.rows}.subspan(state.next, count), output);
  state.next += count;
  output.SetCardinality(count);
  return duckdb::OperatorResultType::HAVE_MORE_OUTPUT;
}

void RegisterAclFunctions(duckdb::ExtensionLoader& loader);
void RegisterDbsizeFunctions(duckdb::ExtensionLoader& loader);
void RegisterFormatTypeFunctions(duckdb::ExtensionLoader& loader);
void RegisterRegprocFunctions(duckdb::ExtensionLoader& loader);
void RegisterRuleutilsFunctions(duckdb::ExtensionLoader& loader);
void RegisterSequenceFunctions(duckdb::ExtensionLoader& loader);
void RegisterMiscFunctions(duckdb::ExtensionLoader& loader);
void RegisterObjectAddressFunctions(duckdb::ExtensionLoader& loader);
void RegisterSubscriptionFunctions(duckdb::ExtensionLoader& loader);
void RegisterInformationSchemaFunctions(duckdb::ExtensionLoader& loader);

void RegisterFunctions(duckdb::DatabaseInstance& db);

duckdb::GetSQLValueFunctionReturnType SystemUserValueFunction(
  duckdb::PlannerExtensionInput& input, const std::string& column_name,
  duckdb::unique_ptr<duckdb::ParsedExpression>& result);

}  // namespace sdb::pg
