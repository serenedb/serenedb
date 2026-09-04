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
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/function_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <iresearch/analysis/token_list_sink.hpp>
#include <iresearch/utils/string.hpp>
#include <variant>

#include "catalog/ddl/catalog.h"
#include "catalog/tokenizer.h"
#include "connector/common.h"
#include "connector/duckdb_client_state.h"
#include "connector/functions/search.h"
#include "connector/functions/ts_common.hpp"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
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

// The dictionary is named per row, so this arm resolves at execution time. What
// it carries is only what makes two bindings interchangeable: the database and
// the schema an unqualified name resolves against.
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
    : _result_list(result_list), _sink(result_list, 0) {}
  ~ListTokenSink() { Finalize(); }

  duckdb::idx_t Offset() const noexcept { return _offset; }

  void Bind(irs::analysis::Tokenizer& tokenizer) { _stream = &tokenizer; }

  void Tokenize(duckdb::string_t text) {
    SDB_ASSERT(_stream);
    if (!_stream->Fill(text, _sink.writer, {irs::TokenLayout::Terms})) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                      ERR_MSG("error while preparing tokenizer"));
    }
    _sink.writer.Finish();
    _offset = _sink.offset();
  }

  void Tokenize(irs::analysis::Tokenizer& tokenizer, duckdb::string_t text) {
    Bind(tokenizer);
    Tokenize(text);
  }

 private:
  void Finalize() noexcept {
    duckdb::ListVector::SetListSize(_result_list, _offset);
  }

  duckdb::Vector& _result_list;
  duckdb::idx_t _offset = 0;
  irs::analysis::Tokenizer* _stream = nullptr;
  irs::ListVectorSink _sink;
};

[[maybe_unused]] const TsLexizeBindData& GetBindData(
  duckdb::ExpressionState& state) {
  return state.expr.Cast<duckdb::BoundFunctionExpression>()
    .BindInfo()
    ->Cast<TsLexizeBindData>();
}

void TsLexizeFunctionConstant(duckdb::DataChunk& args,
                              duckdb::ExpressionState& state,
                              duckdb::Vector& result) {
  auto count = args.size();
  auto& tokenizer = *duckdb::ExecuteFunctionState::GetFunctionState(state)
                       ->Cast<TsLexizeLocalState>()
                       .wrapper;

  duckdb::UnifiedVectorFormat text_format;
  args.data[1].ToUnifiedFormat(count, text_format);
  auto* text_data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(text_format);

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::ListVector::SetListSize(result, 0);
  auto* list_entries =
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);
  ListTokenSink sink{result};
  sink.Bind(tokenizer);

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto text_idx = text_format.sel->get_index(i);
    if (!text_format.validity.RowIsValid(text_idx)) {
      result_validity.SetInvalid(i);
      list_entries[i] = {sink.Offset(), 0};
      continue;
    }
    const auto row_offset = sink.Offset();
    sink.Tokenize(text_data[text_idx]);
    list_entries[i] = {row_offset, sink.Offset() - row_offset};
  }
}

void TsLexizeArrayFunctionConstant(duckdb::DataChunk& args,
                                   duckdb::ExpressionState& state,
                                   duckdb::Vector& result) {
  auto count = args.size();
  auto& tokenizer = *duckdb::ExecuteFunctionState::GetFunctionState(state)
                       ->Cast<TsLexizeLocalState>()
                       .wrapper;

  duckdb::UnifiedVectorFormat list_format;
  args.data[1].ToUnifiedFormat(count, list_format);
  auto* list_entries_in =
    duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(list_format);

  auto& list_child = duckdb::ListVector::GetEntry(args.data[1]);
  const auto child_size = duckdb::ListVector::GetListSize(args.data[1]);
  duckdb::UnifiedVectorFormat child_format;
  list_child.ToUnifiedFormat(child_size, child_format);
  auto* child_data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(child_format);

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::ListVector::SetListSize(result, 0);
  auto* list_entries_out =
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);
  ListTokenSink sink{result};
  sink.Bind(tokenizer);

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto list_idx = list_format.sel->get_index(i);
    if (!list_format.validity.RowIsValid(list_idx)) {
      result_validity.SetInvalid(i);
      list_entries_out[i] = {sink.Offset(), 0};
      continue;
    }
    const auto row_offset = sink.Offset();
    const auto& entry = list_entries_in[list_idx];
    for (duckdb::idx_t k = 0; k < entry.length; k++) {
      auto child_idx = child_format.sel->get_index(entry.offset + k);
      if (!child_format.validity.RowIsValid(child_idx)) {
        continue;
      }
      sink.Tokenize(child_data[child_idx]);
    }
    list_entries_out[i] = {row_offset, sink.Offset() - row_offset};
  }
}

void TsLexizeFunctionDynamic(duckdb::DataChunk& args,
                             duckdb::ExpressionState& state,
                             duckdb::Vector& result) {
  auto count = args.size();
  SDB_ASSERT(std::holds_alternative<DynamicCtx>(GetBindData(state).state));

  duckdb::UnifiedVectorFormat dict_format, text_format;
  args.data[0].ToUnifiedFormat(count, dict_format);
  args.data[1].ToUnifiedFormat(count, text_format);
  auto* dict_data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(dict_format);
  auto* text_data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(text_format);

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::ListVector::SetListSize(result, 0);
  auto* list_entries =
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);
  ListTokenSink sink{result};

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto dict_idx = dict_format.sel->get_index(i);
    auto text_idx = text_format.sel->get_index(i);
    if (!dict_format.validity.RowIsValid(dict_idx) ||
        !text_format.validity.RowIsValid(text_idx)) {
      result_validity.SetInvalid(i);
      list_entries[i] = {sink.Offset(), 0};
      continue;
    }
    const auto dict_name = AsView(dict_data[dict_idx]);
    auto dict = LookupTokenizerDict(state.GetContext(), dict_name);
    auto tokenizer = AcquireTextTokenizer(state.GetContext(), *dict, dict_name);
    const auto row_offset = sink.Offset();
    sink.Tokenize(*tokenizer, text_data[text_idx]);
    list_entries[i] = {row_offset, sink.Offset() - row_offset};
  }
}

void TsLexizeArrayFunctionDynamic(duckdb::DataChunk& args,
                                  duckdb::ExpressionState& state,
                                  duckdb::Vector& result) {
  auto count = args.size();
  SDB_ASSERT(std::holds_alternative<DynamicCtx>(GetBindData(state).state));

  duckdb::UnifiedVectorFormat dict_format, list_format;
  args.data[0].ToUnifiedFormat(count, dict_format);
  args.data[1].ToUnifiedFormat(count, list_format);
  auto* dict_data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(dict_format);
  auto* list_entries_in =
    duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(list_format);

  auto& list_child = duckdb::ListVector::GetEntry(args.data[1]);
  const auto child_size = duckdb::ListVector::GetListSize(args.data[1]);
  duckdb::UnifiedVectorFormat child_format;
  list_child.ToUnifiedFormat(child_size, child_format);
  auto* child_data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(child_format);

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::ListVector::SetListSize(result, 0);
  auto* list_entries_out =
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);
  ListTokenSink sink{result};

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto dict_idx = dict_format.sel->get_index(i);
    auto list_idx = list_format.sel->get_index(i);
    if (!dict_format.validity.RowIsValid(dict_idx) ||
        !list_format.validity.RowIsValid(list_idx)) {
      result_validity.SetInvalid(i);
      list_entries_out[i] = {sink.Offset(), 0};
      continue;
    }
    const auto dict_name = AsView(dict_data[dict_idx]);
    auto dict = LookupTokenizerDict(state.GetContext(), dict_name);
    auto tokenizer = AcquireTextTokenizer(state.GetContext(), *dict, dict_name);
    const auto row_offset = sink.Offset();
    const auto& entry = list_entries_in[list_idx];
    for (duckdb::idx_t k = 0; k < entry.length; k++) {
      auto child_idx = child_format.sel->get_index(entry.offset + k);
      if (!child_format.validity.RowIsValid(child_idx)) {
        continue;
      }
      sink.Tokenize(*tokenizer, child_data[child_idx]);
    }
    list_entries_out[i] = {row_offset, sink.Offset() - row_offset};
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
           TsLexizeFunctionDynamic, TsLexizeBind<TsLexizeFunctionConstant>));
  set.AddFunction(MakeFn(
    {duckdb::LogicalType::VARCHAR,
     duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR)},
    TsLexizeArrayFunctionDynamic, TsLexizeBind<TsLexizeArrayFunctionConstant>));
  loader.RegisterFunction(std::move(set));
}

}  // namespace sdb::connector
