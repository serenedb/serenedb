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

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/function_binder.hpp>
#include <duckdb/function/function_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_lambda_expression.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <iresearch/analysis/tokenizer_pool.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <string>
#include <string_view>
#include <vector>

#include "connector/duckdb_client_state.h"
#include "connector/functions/list_token_sink.hpp"
#include "pg/commands/create_tsdictionary.h"
#include "pg/connection_context.h"
#include "pg/tokenizer_options.h"

namespace sdb::connector {
namespace {

constexpr std::string_view kListSeparator{"\x1E", 1};

const duckdb::LogicalType& StringListType() {
  static const duckdb::LogicalType type = duckdb::LogicalType::UNION(
    {{"str", duckdb::LogicalType::VARCHAR},
     {"list", duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR)}});
  return type;
}

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
      return StringListType();
    case pg::OptionInfo::Type::Lambda:
      return duckdb::LogicalType::LAMBDA;
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
    case pg::OptionInfo::Type::Lambda:
      return duckdb::Value{};
  }
  return duckdb::Value{};
}

std::vector<pg::OptionInfo> SignatureOptions(const pg::OptionGroup& group) {
  auto options = group.FlatOptions();
  std::erase_if(options, [](const pg::OptionInfo& info) {
    return info.type == pg::OptionInfo::Type::Lambda;
  });
  return options;
}

bool TakesLambda(const pg::OptionGroup& group) {
  return absl::c_any_of(group.FlatOptions(), [](const pg::OptionInfo& info) {
    return info.type == pg::OptionInfo::Type::Lambda;
  });
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

struct PoolDeleter {
  duckdb::shared_ptr<irs::analysis::TokenizerPool> pool;

  void operator()(irs::analysis::Tokenizer* analyzer) const {
    pool->Release(irs::analysis::Tokenizer::ptr{analyzer});
  }
};

using PooledTokenizer = std::unique_ptr<irs::analysis::Tokenizer, PoolDeleter>;

struct TokenizerFunctionLocalState final : public duckdb::FunctionLocalState {
  PooledTokenizer wrapper;
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
  local->wrapper = PooledTokenizer{analyzer.release(), PoolDeleter{data.pool}};
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
  auto values = args.data[0].Values<duckdb::string_t>();

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::ListVector::SetListSize(result, 0);
  auto entries =
    duckdb::FlatVector::Writer<duckdb::list_entry_t>(result, count);
  ListTokenSink sink{result};
  sink.Bind(LocalTokenizer(state));

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto value = values[i];
    if (!value.IsValid()) {
      entries.WriteNull({sink.Offset(), 0});
      continue;
    }
    const auto row_offset = sink.Offset();
    sink.Tokenize(value.GetValue());
    entries.WriteValue({row_offset, sink.Offset() - row_offset});
  }
}

template<bool Joined>
void TokenizeLists(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                   duckdb::Vector& result) {
  const auto count = args.size();
  auto lists = args.data[0].Values<duckdb::VectorListType<duckdb::string_t>>();

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::ListVector::SetListSize(result, 0);
  auto entries =
    duckdb::FlatVector::Writer<duckdb::list_entry_t>(result, count);
  ListTokenSink sink{result};
  sink.Bind(LocalTokenizer(state));
  std::vector<std::string_view> members;
  std::string joined;

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto list = lists[i];
    if (!list.IsValid()) {
      entries.WriteNull({sink.Offset(), 0});
      continue;
    }
    const auto row_offset = sink.Offset();
    members.clear();
    for (auto element : list.GetChildValues()) {
      if (!element.IsValid()) {
        continue;
      }
      const auto& term = element.GetValue();
      if constexpr (Joined) {
        members.emplace_back(term.GetData(), term.GetSize());
      } else {
        sink.Tokenize(term);
      }
    }
    if constexpr (Joined) {
      if (!members.empty()) {
        joined = absl::StrJoin(members, kListSeparator);
        sink.Tokenize(duckdb::string_t{joined.data(),
                                       static_cast<uint32_t>(joined.size())});
      }
    }
    entries.WriteValue({row_offset, sink.Offset() - row_offset});
  }
}

duckdb::unique_ptr<duckdb::FunctionData> Bind(
  duckdb::BindScalarFunctionInput& input) {
  auto& context = input.GetClientContext();
  auto& fn = input.GetBoundFunction();
  const std::string name = fn.GetName().GetIdentifierName();
  const auto& group = FindGroup(name);
  const auto flat = SignatureOptions(group);
  auto& args = input.GetArguments();
  SDB_ASSERT(args.size() == flat.size() + 1);
  const bool list_input =
    fn.GetArguments()[0].id() == duckdb::LogicalTypeId::LIST;
  const bool wrapper = group.kind == pg::TemplateKind::Wrapper;

  pg::Options options;
  std::string key = absl::StrCat("fn:", name, list_input ? "[]" : "", "(");
  for (size_t i = 0; i < flat.size(); ++i) {
    auto& arg = *args[i + 1];
    if (arg.HasParameter() || !arg.IsFoldable()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG(name, "(): option \"", flat[i].name, "\" must be a constant"));
    }
    auto value = duckdb::ExpressionExecutor::EvaluateScalar(context, arg);
    if (value.type().id() == duckdb::LogicalTypeId::UNION && !value.IsNull()) {
      value = duckdb::UnionValue::GetValue(value);
    }
    if (value.IsNull()) {
      continue;
    }
    if (!flat[i].IsRequired() && value == DefaultValue(flat[i])) {
      continue;
    }
    absl::StrAppend(&key, flat[i].name, "=", value.ToString(), ";");
    Put(options, flat[i].name, std::move(value));
  }
  absl::StrAppend(&key, ")");
  const auto operation = absl::StrCat(name, "()");

  pg::TokenizerConfigs children;
  if (wrapper) {
    using namespace pg::tokenizer_options;
    pg::Options nested;
    const auto& base = list_input ? kDelimiterGroup : kKeywordGroup;
    if (list_input) {
      Put(nested, kDelimiter.name, duckdb::Value{std::string{kListSeparator}});
    }
    children.emplace_back(std::make_unique<irs::analysis::TokenizerConfig>(
      pg::BuildStage(context, base.name, std::move(nested), {}, operation)));
  }

  auto config = pg::BuildStage(context, group.name, std::move(options),
                               std::move(children), operation);

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

duckdb::LogicalType BindTokenLambda(
  duckdb::ClientContext& /*context*/,
  const duckdb::vector<duckdb::LogicalType>& /*function_child_types*/,
  duckdb::idx_t parameter_idx,
  duckdb::optional_ptr<duckdb::BindLambdaContext> /*bind_lambda_context*/) {
  if (parameter_idx != 0) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG("the lambda of a token filter takes exactly one parameter"));
  }
  return duckdb::LogicalType::VARCHAR;
}

duckdb::unique_ptr<duckdb::Expression> RewriteLambdaCall(
  duckdb::FunctionBindExpressionInput& input) {
  auto& children = input.children;
  SDB_ASSERT(children.size() == 2);
  const std::string name = input.bound_function.GetName().GetIdentifierName();
  const auto& type = children[1]
                       ->Cast<duckdb::BoundLambdaExpression>()
                       .LambdaExpr()
                       ->GetReturnType();
  if (type != duckdb::LogicalType::BOOLEAN) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(name, "(): the lambda must return BOOLEAN, got ",
                            type.ToString()));
  }
  duckdb::FunctionBinder binder{input.context};
  duckdb::ErrorData error;
  auto filtered = binder.BindScalarFunction(duckdb::Identifier{DEFAULT_SCHEMA},
                                            duckdb::Identifier{"list_filter"},
                                            std::move(children), error);
  if (!filtered) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(name, "(): ", error.RawMessage()));
  }
  return filtered;
}

void UnrewrittenLambdaCall(duckdb::DataChunk& /*args*/,
                           duckdb::ExpressionState& /*state*/,
                           duckdb::Vector& /*result*/) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                  ERR_MSG("a lambda token filter call was not rewritten"));
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
  for (const auto& info : SignatureOptions(group)) {
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

duckdb::ScalarFunction MakeLambdaFunction() {
  duckdb::ScalarFunction fn{
    {},
    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
    UnrewrittenLambdaCall,
    nullptr,
  };
  auto& signature = fn.GetSignature();
  signature.AddParameter(
    duckdb::Identifier{"value"},
    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR));
  signature.AddParameter(duckdb::Identifier{"predicate"},
                         duckdb::LogicalType::LAMBDA);
  fn.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
  fn.SetBindLambdaCallback(BindTokenLambda);
  fn.SetBindExpressionCallback(RewriteLambdaCall);
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
      if (TakesLambda(group)) {
        set.AddFunction(MakeLambdaFunction());
      }
      set.AddFunction(MakeFunction(group, duckdb::LogicalType::VARCHAR));
      set.AddFunction(MakeFunction(
        group, duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR)));
    }
    loader.RegisterFunction(std::move(set));
  }
}

}  // namespace sdb::connector
