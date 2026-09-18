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

#include "connector/functions/ts_lexize.h"

#include <duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/function_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/string.hpp>
#include <variant>

#include "catalog/ddl/catalog.h"
#include "catalog/entry/duckdb_object_entry.h"
#include "catalog/tokenizer.h"
#include "connector/common.h"
#include "connector/duckdb_client_state.h"
#include "connector/functions/list_token_sink.hpp"
#include "connector/functions/search.h"
#include "connector/functions/ts_common.hpp"
#include "pg/connection_context.h"
#include "pg/sql_utils.h"

namespace sdb::connector {
namespace {

catalog::TokenizerRef LookupTokenizerDict(duckdb::ClientContext& context,
                                          std::string_view dict_name) {
  auto dict = ResolveCatalogTokenizer(context, dict_name);
  if (!dict) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("text search dictionary \"", dict_name, "\" does not exist"));
  }
  return dict;
}

catalog::Tokenizer::TokenizerWrapper AcquireTokenizer(
  duckdb::ClientContext& ctx, const catalog::Tokenizer& dict) {
  return dict.GetTokenizer(ctx);
}

catalog::Tokenizer::TokenizerWrapper AcquireTextTokenizer(
  duckdb::ClientContext& ctx, const catalog::Tokenizer& dict,
  std::string_view dict_name) {
  auto tokenizer = AcquireTokenizer(ctx, dict);
  const auto output = tokenizer->Traits().output;
  if (output != duckdb::LogicalTypeId::VARCHAR) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
      ERR_MSG("text search dictionary \"", dict_name, "\" produces ",
              duckdb::LogicalTypeIdToString(output),
              " tokens; pass the dictionary name as a constant"));
  }
  return tokenizer;
}

struct DynamicCtx {
  sdb::ObjectId db_id;
  std::string current_schema;

  bool operator==(const DynamicCtx& rhs) const {
    return db_id == rhs.db_id && current_schema == rhs.current_schema;
  }
};

struct TsLexizeBindData final : public duckdb::FunctionData {
  std::variant<DynamicCtx, catalog::TokenizerRef> state;

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    return duckdb::make_uniq<TsLexizeBindData>(*this);
  }
  bool Equals(const duckdb::FunctionData& other) const final {
    return state == other.Cast<TsLexizeBindData>().state;
  }
};

struct TsLexizeLocalState final : public duckdb::FunctionLocalState {
  catalog::Tokenizer::TokenizerWrapper wrapper;
};

duckdb::unique_ptr<duckdb::FunctionLocalState> InitTsLexizeLocalState(
  duckdb::ExpressionState& state, const duckdb::BoundFunctionExpression& expr,
  duckdb::FunctionData* bind_data) {
  auto& dict =
    std::get<catalog::TokenizerRef>(bind_data->Cast<TsLexizeBindData>().state);
  auto local = duckdb::make_uniq<TsLexizeLocalState>();
  local->wrapper = AcquireTokenizer(state.GetContext(), *dict);
  return local;
}

[[maybe_unused]] const TsLexizeBindData& GetBindData(
  duckdb::ExpressionState& state) {
  return state.expr.Cast<duckdb::BoundFunctionExpression>()
    .BindInfo()
    ->Cast<TsLexizeBindData>();
}

template<duckdb::idx_t ValueArg>
void TsLexizeFunctionConstant(duckdb::DataChunk& args,
                              duckdb::ExpressionState& state,
                              duckdb::Vector& result) {
  const auto count = args.size();
  auto& tokenizer = *duckdb::ExecuteFunctionState::GetFunctionState(state)
                       ->Cast<TsLexizeLocalState>()
                       .wrapper;
  auto texts = args.data[ValueArg].Values<duckdb::string_t>();

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::ListVector::SetListSize(result, 0);
  auto entries =
    duckdb::FlatVector::Writer<duckdb::list_entry_t>(result, count);
  ListTokenSink sink{result};
  sink.Bind(tokenizer);

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto text = texts[i];
    if (!text.IsValid()) {
      entries.WriteNull({sink.Offset(), 0});
      continue;
    }
    const auto row_offset = sink.Offset();
    sink.Tokenize(text.GetValue());
    entries.WriteValue({row_offset, sink.Offset() - row_offset});
  }
}

template<duckdb::idx_t ValueArg>
void TsLexizeArrayFunctionConstant(duckdb::DataChunk& args,
                                   duckdb::ExpressionState& state,
                                   duckdb::Vector& result) {
  const auto count = args.size();
  auto& tokenizer = *duckdb::ExecuteFunctionState::GetFunctionState(state)
                       ->Cast<TsLexizeLocalState>()
                       .wrapper;
  auto lists =
    args.data[ValueArg].Values<duckdb::VectorListType<duckdb::string_t>>();

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::ListVector::SetListSize(result, 0);
  auto entries =
    duckdb::FlatVector::Writer<duckdb::list_entry_t>(result, count);
  ListTokenSink sink{result};
  sink.Bind(tokenizer);

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto list = lists[i];
    if (!list.IsValid()) {
      entries.WriteNull({sink.Offset(), 0});
      continue;
    }
    const auto row_offset = sink.Offset();
    for (auto element : list.GetChildValues()) {
      if (!element.IsValid()) {
        continue;
      }
      sink.Tokenize(element.GetValue());
    }
    entries.WriteValue({row_offset, sink.Offset() - row_offset});
  }
}

void TsLexizeFunctionDynamic(duckdb::DataChunk& args,
                             duckdb::ExpressionState& state,
                             duckdb::Vector& result) {
  const auto count = args.size();
  SDB_ASSERT(std::holds_alternative<DynamicCtx>(GetBindData(state).state));
  auto dicts = args.data[0].Values<duckdb::string_t>();
  auto texts = args.data[1].Values<duckdb::string_t>();

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::ListVector::SetListSize(result, 0);
  auto entries =
    duckdb::FlatVector::Writer<duckdb::list_entry_t>(result, count);
  ListTokenSink sink{result};

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto dict_value = dicts[i];
    auto text = texts[i];
    if (!dict_value.IsValid() || !text.IsValid()) {
      entries.WriteNull({sink.Offset(), 0});
      continue;
    }
    const auto dict_name = AsView(dict_value.GetValue());
    auto dict = LookupTokenizerDict(state.GetContext(), dict_name);
    auto tokenizer = AcquireTextTokenizer(state.GetContext(), *dict, dict_name);
    const auto row_offset = sink.Offset();
    sink.Tokenize(*tokenizer, text.GetValue());
    entries.WriteValue({row_offset, sink.Offset() - row_offset});
  }
}

void TsLexizeArrayFunctionDynamic(duckdb::DataChunk& args,
                                  duckdb::ExpressionState& state,
                                  duckdb::Vector& result) {
  const auto count = args.size();
  SDB_ASSERT(std::holds_alternative<DynamicCtx>(GetBindData(state).state));
  auto dicts = args.data[0].Values<duckdb::string_t>();
  auto lists = args.data[1].Values<duckdb::VectorListType<duckdb::string_t>>();

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::ListVector::SetListSize(result, 0);
  auto entries =
    duckdb::FlatVector::Writer<duckdb::list_entry_t>(result, count);
  ListTokenSink sink{result};

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto dict_value = dicts[i];
    auto list = lists[i];
    if (!dict_value.IsValid() || !list.IsValid()) {
      entries.WriteNull({sink.Offset(), 0});
      continue;
    }
    const auto dict_name = AsView(dict_value.GetValue());
    auto dict = LookupTokenizerDict(state.GetContext(), dict_name);
    auto tokenizer = AcquireTextTokenizer(state.GetContext(), *dict, dict_name);
    const auto row_offset = sink.Offset();
    for (auto element : list.GetChildValues()) {
      if (!element.IsValid()) {
        continue;
      }
      sink.Tokenize(*tokenizer, element.GetValue());
    }
    entries.WriteValue({row_offset, sink.Offset() - row_offset});
  }
}

using ScalarFnPtr = void (*)(duckdb::DataChunk&, duckdb::ExpressionState&,
                             duckdb::Vector&);

template<ScalarFnPtr ConstantFn>
duckdb::unique_ptr<duckdb::FunctionData> TsLexizeBind(
  duckdb::BindScalarFunctionInput& input) {
  auto& context = input.GetClientContext();
  auto& conn_ctx = GetSereneDBContext(context);
  DynamicCtx ctx{
    .db_id = conn_ctx.GetDatabaseId(),
    .current_schema = conn_ctx.GetCurrentSchema(),
  };

  auto bind = duckdb::make_uniq<TsLexizeBindData>();
  auto& args = input.GetArguments();
  if (args[0]->IsFoldable()) {
    auto val = duckdb::ExpressionExecutor::EvaluateScalar(context, *args[0]);
    if (!val.IsNull()) {
      auto dict = LookupTokenizerDict(context, duckdb::StringValue::Get(val));
      const auto output = AcquireTokenizer(context, *dict)->Traits().output;
      bind->state = std::move(dict);
      auto& fn = input.GetBoundFunction();
      fn.SetReturnType(duckdb::LogicalType::LIST(output));
      fn.SetFunctionCallback(ConstantFn);
      fn.SetInitStateCallback(InitTsLexizeLocalState);
      return bind;
    }
  }
  bind->state = std::move(ctx);
  return bind;
}

duckdb::ScalarFunction MakeFn(duckdb::vector<duckdb::LogicalType> args,
                              ScalarFnPtr dynamic_fn,
                              duckdb::bind_scalar_function_t bind) {
  duckdb::ScalarFunction f{
    std::move(args),
    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
    dynamic_fn,
    bind,
  };
  f.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
  return f;
}

}  // namespace

void RegisterTsLexize(duckdb::ExtensionLoader& loader) {
  duckdb::ScalarFunctionSet set{"ts_lexize"};
  set.AddFunction(
    MakeFn({duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
           TsLexizeFunctionDynamic, TsLexizeBind<TsLexizeFunctionConstant<1>>));
  set.AddFunction(
    MakeFn({duckdb::LogicalType::VARCHAR,
            duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR)},
           TsLexizeArrayFunctionDynamic,
           TsLexizeBind<TsLexizeArrayFunctionConstant<1>>));
  loader.RegisterFunction(std::move(set));
}

}  // namespace sdb::connector
