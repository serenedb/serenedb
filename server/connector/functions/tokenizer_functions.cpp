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

#include "connector/functions/tokenizer_functions.h"

#include <absl/strings/str_cat.h>

#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/function_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <iresearch/analysis/tokenizer_pool.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <string>
#include <string_view>

#include "catalog/tokenizer.h"
#include "connector/duckdb_client_state.h"
#include "connector/functions/list_token_sink.hpp"
#include "pg/commands/create_tsdictionary.h"
#include "pg/connection_context.h"
#include "pg/tokenizer_options.h"

namespace sdb::connector {
namespace {

constexpr char kListSeparator = '\x1F';

duckdb::LogicalType ParameterType(const pg::OptionInfo& info) {
  switch (info.type) {
    case pg::OptionInfo::Type::Boolean:
      return duckdb::LogicalType::BOOLEAN;
    case pg::OptionInfo::Type::Integer:
      return duckdb::LogicalType::INTEGER;
    case pg::OptionInfo::Type::Double:
      return duckdb::LogicalType::DOUBLE;
    case pg::OptionInfo::Type::String:
    case pg::OptionInfo::Type::Character:
      return duckdb::LogicalType::VARCHAR;
    case pg::OptionInfo::Type::StringList:
      return duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  }
  return duckdb::LogicalType::VARCHAR;
}

duckdb::Value DefaultValue(const pg::OptionInfo& info) {
  switch (info.type) {
    case pg::OptionInfo::Type::Boolean:
      return duckdb::Value::BOOLEAN(info.GetDefaultValue<bool>());
    case pg::OptionInfo::Type::Integer:
      return duckdb::Value::INTEGER(info.GetDefaultValue<int>());
    case pg::OptionInfo::Type::Double:
      return duckdb::Value::DOUBLE(info.GetDefaultValue<double>());
    case pg::OptionInfo::Type::String:
      return duckdb::Value{info.GetDefaultValue<std::string>()};
    case pg::OptionInfo::Type::Character:
      return duckdb::Value{std::string(1, info.GetDefaultValue<char>())};
    case pg::OptionInfo::Type::StringList:
      return duckdb::Value::LIST(duckdb::LogicalType::VARCHAR, {});
  }
  return duckdb::Value{};
}

const pg::OptionGroup& FindGroup(std::string_view function) {
  for (const auto& group : pg::tokenizer_options::kTokenizerSubgroups) {
    if (group.function == function) {
      return group;
    }
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                  ERR_MSG("no tokenizer function named \"", function, "\""));
}

void Put(pg::Options& options, std::string_view key, duckdb::Value value) {
  options.try_emplace(std::string{key},
                      std::make_unique<duckdb::Value>(std::move(value)));
}

struct TokenizerFunctionBindData final : public duckdb::FunctionData {
  std::string key;
  irs::analysis::TokenizerConfig config;
  duckdb::shared_ptr<irs::analysis::TokenizerPool> pool;

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    auto copy = duckdb::make_uniq<TokenizerFunctionBindData>();
    copy->key = key;
    copy->config = irs::analysis::Clone(config);
    copy->pool = pool;
    return copy;
  }

  bool Equals(const duckdb::FunctionData& other) const final {
    return key == other.Cast<TokenizerFunctionBindData>().key;
  }
};

struct TokenizerFunctionLocalState final : public duckdb::FunctionLocalState {
  catalog::Tokenizer::TokenizerWrapper wrapper;
};

duckdb::unique_ptr<duckdb::FunctionLocalState> InitLocalState(
  duckdb::ExpressionState& state, const duckdb::BoundFunctionExpression& expr,
  duckdb::FunctionData* bind_data) {
  auto& data = bind_data->Cast<TokenizerFunctionBindData>();
  auto analyzer = data.pool->Acquire();
  if (!analyzer) {
    analyzer = irs::analysis::CreateTokenizer(
      irs::analysis::Clone(data.config),
      irs::DuckDBEngine::Instance().instance().GetSharedObjectCache());
  }
  auto local = duckdb::make_uniq<TokenizerFunctionLocalState>();
  local->wrapper = catalog::Tokenizer::TokenizerWrapper{
    analyzer.release(), catalog::Tokenizer::Deleter{data.pool}};
  if (state.HasContext()) {
    local->wrapper->Bind(state.GetContext());
  }
  return local;
}

irs::analysis::Tokenizer& LocalTokenizer(duckdb::ExpressionState& state) {
  return *duckdb::ExecuteFunctionState::GetFunctionState(state)
            ->Cast<TokenizerFunctionLocalState>()
            .wrapper;
}

void TokenizeValues(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                    duckdb::Vector& result) {
  const auto count = args.size();
  duckdb::UnifiedVectorFormat value_format;
  args.data[0].ToUnifiedFormat(count, value_format);
  const auto* values =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(value_format);

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::ListVector::SetListSize(result, 0);
  auto* entries =
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
  auto& validity = duckdb::FlatVector::ValidityMutable(result);
  ListTokenSink sink{result};
  sink.Bind(LocalTokenizer(state));

  for (duckdb::idx_t i = 0; i < count; i++) {
    const auto idx = value_format.sel->get_index(i);
    if (!value_format.validity.RowIsValid(idx)) {
      validity.SetInvalid(i);
      entries[i] = {sink.Offset(), 0};
      continue;
    }
    const auto row_offset = sink.Offset();
    sink.Tokenize(values[idx]);
    entries[i] = {row_offset, sink.Offset() - row_offset};
  }
}

template<bool Joined>
void TokenizeLists(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                   duckdb::Vector& result) {
  const auto count = args.size();
  duckdb::UnifiedVectorFormat list_format;
  args.data[0].ToUnifiedFormat(count, list_format);
  const auto* lists =
    duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(list_format);

  auto& child = duckdb::ListVector::GetEntry(args.data[0]);
  const auto child_size = duckdb::ListVector::GetListSize(args.data[0]);
  duckdb::UnifiedVectorFormat child_format;
  child.ToUnifiedFormat(child_size, child_format);
  const auto* elements =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(child_format);

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::ListVector::SetListSize(result, 0);
  auto* entries =
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
  auto& validity = duckdb::FlatVector::ValidityMutable(result);
  ListTokenSink sink{result};
  sink.Bind(LocalTokenizer(state));
  std::string joined;

  for (duckdb::idx_t i = 0; i < count; i++) {
    const auto idx = list_format.sel->get_index(i);
    if (!list_format.validity.RowIsValid(idx)) {
      validity.SetInvalid(i);
      entries[i] = {sink.Offset(), 0};
      continue;
    }
    const auto row_offset = sink.Offset();
    const auto& entry = lists[idx];
    joined.clear();
    for (duckdb::idx_t k = 0; k < entry.length; k++) {
      const auto child_idx = child_format.sel->get_index(entry.offset + k);
      if (!child_format.validity.RowIsValid(child_idx)) {
        continue;
      }
      if constexpr (Joined) {
        if (!joined.empty()) {
          joined.push_back(kListSeparator);
        }
        joined.append(elements[child_idx].GetData(),
                      elements[child_idx].GetSize());
      } else {
        sink.Tokenize(elements[child_idx]);
      }
    }
    if constexpr (Joined) {
      sink.Tokenize(
        duckdb::string_t{joined.data(), static_cast<uint32_t>(joined.size())});
    }
    entries[i] = {row_offset, sink.Offset() - row_offset};
  }
}

duckdb::unique_ptr<duckdb::FunctionData> Bind(
  duckdb::BindScalarFunctionInput& input) {
  auto& context = input.GetClientContext();
  auto& fn = input.GetBoundFunction();
  const std::string name = fn.GetName().GetIdentifierName();
  const auto& group = FindGroup(name);
  const auto flat = group.FlatOptions();
  auto& args = input.GetArguments();
  SDB_ASSERT(args.size() == flat.size() + 1);
  const bool list_input =
    args[0]->GetReturnType().id() == duckdb::LogicalTypeId::LIST;
  const bool wrapper = group.kind == pg::TemplateKind::Wrapper;

  pg::Options options;
  Put(options, pg::tokenizer_options::kTemplate.name,
      duckdb::Value{std::string{group.name}});
  std::string key = absl::StrCat("fn:", name, list_input ? "[]" : "", "(");
  for (size_t i = 0; i < flat.size(); ++i) {
    auto& arg = *args[i + 1];
    if (arg.HasParameter() || !arg.IsFoldable()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG(name, "(): option \"", flat[i].name, "\" must be a constant"));
    }
    auto value = duckdb::ExpressionExecutor::EvaluateScalar(context, arg);
    if (value.IsNull()) {
      continue;
    }
    if (!flat[i].IsRequired() && value == DefaultValue(flat[i])) {
      continue;
    }
    absl::StrAppend(&key, flat[i].name, "=", value.ToString(), ";");
    Put(options, flat[i].name, std::move(value));
  }
  key += ")";
  if (wrapper && list_input) {
    Put(
      options, "tokenizer_template",
      duckdb::Value{std::string{pg::tokenizer_options::kDelimiterGroup.name}});
    Put(options, "tokenizer_delimiter",
        duckdb::Value{std::string(1, kListSeparator)});
  } else if (wrapper) {
    Put(options, "tokenizer_template",
        duckdb::Value{std::string{pg::tokenizer_options::kKeywordGroup.name}});
  }

  auto* conn_ctx = GetSereneDBContextPtr(context);
  auto config = pg::BuildTokenizerConfig(
    context, conn_ctx ? conn_ctx->GetDatabaseId() : ObjectId{},
    conn_ctx ? conn_ctx->GetCurrentSchema() : std::string{}, std::move(options),
    absl::StrCat(name, "()"));

  auto& db = duckdb::DatabaseInstance::GetDatabase(context);
  auto probe = irs::analysis::CreateTokenizer(irs::analysis::Clone(config),
                                              db.GetSharedObjectCache());
  SDB_ASSERT(probe);
  probe->Bind(context);
  const auto output = probe->Traits().output;
  probe->Unbind();

  fn.SetReturnType(duckdb::LogicalType::LIST(output));
  if (!list_input) {
    fn.SetFunctionCallback(TokenizeValues);
  } else if (wrapper) {
    fn.SetFunctionCallback(TokenizeLists<true>);
  } else {
    fn.SetFunctionCallback(TokenizeLists<false>);
  }

  auto bind = duckdb::make_uniq<TokenizerFunctionBindData>();
  bind->pool = irs::analysis::TokenizerPool::Get(db, key);
  bind->key = std::move(key);
  bind->config = std::move(config);
  return bind;
}

duckdb::ScalarFunction MakeFunction(const pg::OptionGroup& group,
                                    duckdb::LogicalType value_type) {
  duckdb::ScalarFunction fn{
    {},
    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
    TokenizeValues,
    Bind,
    nullptr,
    InitLocalState,
  };
  auto& signature = fn.GetSignature();
  signature.AddParameter(duckdb::Identifier{"value"}, std::move(value_type));
  for (const auto& info : group.FlatOptions()) {
    if (info.IsRequired()) {
      signature.AddParameter(duckdb::Identifier{info.name},
                             ParameterType(info));
      continue;
    }
    signature.AddParameter(duckdb::Identifier{info.name}, ParameterType(info),
                           DefaultValue(info));
  }
  fn.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
  return fn;
}

}  // namespace

void RegisterTokenizerFunctions(duckdb::ExtensionLoader& loader) {
  for (const auto& group : pg::tokenizer_options::kTokenizerSubgroups) {
    if (group.function.empty()) {
      continue;
    }
    duckdb::ScalarFunctionSet set{duckdb::Identifier{group.function}};
    if (group.input == pg::TemplateInput::Json) {
      set.AddFunction(MakeFunction(group, duckdb::LogicalType::JSON()));
    } else {
      set.AddFunction(MakeFunction(group, duckdb::LogicalType::VARCHAR));
      set.AddFunction(MakeFunction(
        group, duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR)));
    }
    loader.RegisterFunction(std::move(set));
  }
}

}  // namespace sdb::connector
