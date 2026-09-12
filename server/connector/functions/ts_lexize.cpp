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

#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/common/vector_operations/binary_executor.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/function_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <iresearch/analysis/token_sinks.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/string.hpp>
#include <span>
#include <variant>

#include "catalog/ddl/catalog.h"
#include "catalog/tokenizer.h"
#include "connector/common.h"
#include "connector/duckdb_client_state.h"
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

class ListTokenSink {
 public:
  explicit ListTokenSink(duckdb::Vector& result_list)
    : _result_list(result_list) {}
  ~ListTokenSink() { Finalize(); }

  duckdb::idx_t Offset() const noexcept { return _offset; }

  void Bind(irs::analysis::Tokenizer& tokenizer) { _stream = &tokenizer; }

  void Tokenize(duckdb::string_t text) {
    SDB_ASSERT(_stream);
    if (!_analyzer.Analyze(*_stream, text, _tokens)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                      ERR_MSG("error while preparing tokenizer"));
    }
    Append(_tokens.terms());
  }

  void Tokenize(irs::analysis::Tokenizer& tokenizer, duckdb::string_t text) {
    Bind(tokenizer);
    Tokenize(text);
  }

 private:
  void Append(std::span<const duckdb::string_t> terms) {
    auto& child = duckdb::ListVector::GetEntry(_result_list);
    const auto needed = _offset + terms.size();
    if (needed > duckdb::ListVector::GetListCapacity(_result_list)) {
      duckdb::ListVector::SetListSize(_result_list, _offset);
      duckdb::ListVector::Reserve(_result_list, needed * 2);
    }
    auto* data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(child);
    for (const auto& term : terms) {
      data[_offset++] = duckdb::StringVector::AddStringOrBlob(
        child, term.GetData(), term.GetSize());
    }
  }

  void Finalize() noexcept {
    duckdb::ListVector::SetListSize(_result_list, _offset);
  }

  duckdb::Vector& _result_list;
  duckdb::idx_t _offset = 0;
  irs::analysis::Tokenizer* _stream = nullptr;
  irs::ValueAnalyzer _analyzer;
  irs::ValueTokens<> _tokens;
};

[[maybe_unused]] const TsLexizeBindData& GetBindData(
  duckdb::ExpressionState& state) {
  return state.expr.Cast<duckdb::BoundFunctionExpression>()
    .BindInfo()
    ->Cast<TsLexizeBindData>();
}

// The elements of a LIST argument, read once so a per-row lambda can walk them.
class ListElements {
 public:
  explicit ListElements(duckdb::Vector& list) {
    auto& child = duckdb::ListVector::GetEntry(list);
    child.ToUnifiedFormat(duckdb::ListVector::GetListSize(list), _format);
    _data = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(_format);
  }

  void TokenizeInto(ListTokenSink& sink, duckdb::list_entry_t entry) const {
    for (duckdb::idx_t k = 0; k < entry.length; ++k) {
      const auto idx = _format.sel->get_index(entry.offset + k);
      if (_format.validity.RowIsValid(idx)) {
        sink.Tokenize(_data[idx]);
      }
    }
  }

 private:
  duckdb::UnifiedVectorFormat _format;
  const duckdb::string_t* _data = nullptr;
};

// A row's tokens occupy the span of the child vector the sink filled for it.
template<typename Tokenize>
duckdb::list_entry_t SinkRow(ListTokenSink& sink, Tokenize&& tokenize) {
  const auto offset = sink.Offset();
  tokenize();
  return {offset, sink.Offset() - offset};
}

auto AcquireDynamicTokenizer(duckdb::ExpressionState& state,
                             duckdb::string_t dict_column) {
  const auto dict_name = AsView(dict_column);
  auto dict = LookupTokenizerDict(state.GetContext(), dict_name);
  return AcquireTextTokenizer(state.GetContext(), *dict, dict_name);
}

void TsLexizeFunctionConstant(duckdb::DataChunk& args,
                              duckdb::ExpressionState& state,
                              duckdb::Vector& result) {
  auto& tokenizer = *duckdb::ExecuteFunctionState::GetFunctionState(state)
                       ->Cast<TsLexizeLocalState>()
                       .wrapper;

  duckdb::ListVector::SetListSize(result, 0);
  ListTokenSink sink{result};
  sink.Bind(tokenizer);

  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::list_entry_t>(
    args.data[1], result, args.size(), [&](duckdb::string_t text) {
      return SinkRow(sink, [&] { sink.Tokenize(text); });
    });
}

void TsLexizeArrayFunctionConstant(duckdb::DataChunk& args,
                                   duckdb::ExpressionState& state,
                                   duckdb::Vector& result) {
  auto& tokenizer = *duckdb::ExecuteFunctionState::GetFunctionState(state)
                       ->Cast<TsLexizeLocalState>()
                       .wrapper;
  const ListElements elements{args.data[1]};

  duckdb::ListVector::SetListSize(result, 0);
  ListTokenSink sink{result};
  sink.Bind(tokenizer);

  duckdb::UnaryExecutor::Execute<duckdb::list_entry_t, duckdb::list_entry_t>(
    args.data[1], result, args.size(), [&](duckdb::list_entry_t entry) {
      return SinkRow(sink, [&] { elements.TokenizeInto(sink, entry); });
    });
}

void TsLexizeFunctionDynamic(duckdb::DataChunk& args,
                             duckdb::ExpressionState& state,
                             duckdb::Vector& result) {
  SDB_ASSERT(std::holds_alternative<DynamicCtx>(GetBindData(state).state));

  duckdb::ListVector::SetListSize(result, 0);
  ListTokenSink sink{result};

  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                  duckdb::list_entry_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t dict_name, duckdb::string_t text) {
      auto tokenizer = AcquireDynamicTokenizer(state, dict_name);
      return SinkRow(sink, [&] { sink.Tokenize(*tokenizer, text); });
    });
}

void TsLexizeArrayFunctionDynamic(duckdb::DataChunk& args,
                                  duckdb::ExpressionState& state,
                                  duckdb::Vector& result) {
  SDB_ASSERT(std::holds_alternative<DynamicCtx>(GetBindData(state).state));
  const ListElements elements{args.data[1]};

  duckdb::ListVector::SetListSize(result, 0);
  ListTokenSink sink{result};

  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::list_entry_t,
                                  duckdb::list_entry_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t dict_name, duckdb::list_entry_t entry) {
      auto tokenizer = AcquireDynamicTokenizer(state, dict_name);
      sink.Bind(*tokenizer);
      return SinkRow(sink, [&] { elements.TokenizeInto(sink, entry); });
    });
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
           TsLexizeFunctionDynamic, TsLexizeBind<TsLexizeFunctionConstant>));
  set.AddFunction(MakeFn(
    {duckdb::LogicalType::VARCHAR,
     duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR)},
    TsLexizeArrayFunctionDynamic, TsLexizeBind<TsLexizeArrayFunctionConstant>));
  loader.RegisterFunction(std::move(set));
}

}  // namespace sdb::connector
