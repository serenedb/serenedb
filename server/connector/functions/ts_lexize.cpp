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

#include <duckdb/catalog/catalog.hpp>
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
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/string.hpp>
#include <variant>

#include "catalog/catalog.h"
#include "catalog/entry/tokenizer.h"
#include "connector/common.h"
#include "connector/duckdb_client_state.h"
#include "connector/functions/list_token_sink.hpp"
#include "connector/functions/search.h"
#include "connector/functions/ts_common.hpp"
#include "pg/connection_context.h"
#include "pg/sql_utils.h"

namespace sdb::connector {
namespace {

duckdb::optional_ptr<const catalog::TokenizerCatalogEntry> LookupTokenizerDict(
  duckdb::ClientContext& context, std::string_view dict_name) {
  auto dict = duckdb::Catalog::GetEntry<catalog::TokenizerCatalogEntry>(
    context, duckdb::QualifiedName::Parse(std::string{dict_name}),
    duckdb::OnEntryNotFound::RETURN_NULL);
  if (!dict) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("text search dictionary \"", dict_name, "\" does not exist"));
  }
  return dict.get();
}

catalog::Tokenizer::TokenizerWrapper AcquireTokenizer(
  duckdb::ClientContext& ctx, const catalog::TokenizerCatalogEntry& dict) {
  return dict.Acquire(ctx);
}

catalog::Tokenizer::TokenizerWrapper AcquireTextTokenizer(
  duckdb::ClientContext& ctx, const catalog::TokenizerCatalogEntry& dict,
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
  duckdb::idx_t db_id;

  bool operator==(const DynamicCtx& rhs) const = default;
};

struct TsLexizeBindData final : public duckdb::FunctionData {
  std::variant<DynamicCtx,
               duckdb::optional_ptr<const catalog::TokenizerCatalogEntry>>
    state;

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    return duckdb::make_uniq<TsLexizeBindData>(*this);
  }
  bool Equals(const duckdb::FunctionData& other) const final {
    return state == other.Cast<TsLexizeBindData>().state;
  }
};

struct TsLexizeLocalState final : public duckdb::FunctionLocalState {
  catalog::Tokenizer::TokenizerWrapper wrapper;
  irs::TokenSink writer;
};

duckdb::unique_ptr<duckdb::FunctionLocalState> InitTsLexizeLocalState(
  duckdb::ExpressionState& state, const duckdb::BoundFunctionExpression& expr,
  duckdb::FunctionData* bind_data) {
  auto& dict =
    std::get<duckdb::optional_ptr<const catalog::TokenizerCatalogEntry>>(
      bind_data->Cast<TsLexizeBindData>().state);
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
  auto& local = duckdb::ExecuteFunctionState::GetFunctionState(state)
                  ->Cast<TsLexizeLocalState>();
  TokenizeRows(*local.wrapper, local.writer, args.data[ValueArg], args.size(),
               result);
}

template<duckdb::idx_t ValueArg>
void TsLexizeArrayFunctionConstant(duckdb::DataChunk& args,
                                   duckdb::ExpressionState& state,
                                   duckdb::Vector& result) {
  auto& local = duckdb::ExecuteFunctionState::GetFunctionState(state)
                  ->Cast<TsLexizeLocalState>();
  TokenizeListRows(*local.wrapper, local.writer, args.data[ValueArg],
                   args.size(), result);
}

struct DictionaryGroups {
  std::vector<std::string_view> names;
  std::vector<std::vector<uint32_t>> rows;
};

template<typename HasValue>
DictionaryGroups GroupByDictionary(const duckdb::UnifiedVectorFormat& dicts,
                                   uint32_t count, ListTokenSink& sink,
                                   HasValue has_value) {
  DictionaryGroups groups;
  irs::containers::FlatHashMap<std::string_view, uint32_t> index;
  const auto* names =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(dicts);
  for (uint32_t r = 0; r < count; ++r) {
    const auto idx = dicts.sel->get_index(r);
    if (!dicts.validity.RowIsValid(idx) || !has_value(r)) {
      sink.SetNull(r);
      continue;
    }
    const auto name = AsView(names[idx]);
    const auto [it, inserted] =
      index.try_emplace(name, static_cast<uint32_t>(groups.names.size()));
    if (inserted) {
      groups.names.push_back(name);
      groups.rows.emplace_back();
    }
    groups.rows[it->second].push_back(r);
  }
  return groups;
}

void TsLexizeFunctionDynamic(duckdb::DataChunk& args,
                             duckdb::ExpressionState& state,
                             duckdb::Vector& result) {
  const auto count = static_cast<uint32_t>(args.size());
  SDB_ASSERT(std::holds_alternative<DynamicCtx>(GetBindData(state).state));
  duckdb::UnifiedVectorFormat dicts;
  args.data[0].ToUnifiedFormat(dicts);
  duckdb::UnifiedVectorFormat texts;
  args.data[1].ToUnifiedFormat(texts);

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::ListVector::SetListSize(result, 0);
  irs::TokenSink writer;
  ListTokenSink sink{
    result, duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result),
    duckdb::FlatVector::ValidityMutable(result), writer};
  sink.ResetRows(count);
  const auto groups = GroupByDictionary(dicts, count, sink, [&](uint32_t r) {
    return texts.validity.RowIsValid(texts.sel->get_index(r));
  });
  for (size_t g = 0; g < groups.names.size(); ++g) {
    const auto& rows = groups.rows[g];
    auto dict = LookupTokenizerDict(state.GetContext(), groups.names[g]);
    auto tokenizer =
      AcquireTextTokenizer(state.GetContext(), *dict, groups.names[g]);
    duckdb::SelectionVector sel{rows.size()};
    for (size_t i = 0; i < rows.size(); ++i) {
      sel.set_index(i, texts.sel->get_index(rows[i]));
    }
    sink.FillGroup(*tokenizer, args.data[1], SliceFormat(texts, sel),
                   static_cast<uint32_t>(rows.size()), rows.data(), true);
  }
}

void TsLexizeArrayFunctionDynamic(duckdb::DataChunk& args,
                                  duckdb::ExpressionState& state,
                                  duckdb::Vector& result) {
  const auto count = static_cast<uint32_t>(args.size());
  SDB_ASSERT(std::holds_alternative<DynamicCtx>(GetBindData(state).state));
  duckdb::UnifiedVectorFormat dicts;
  args.data[0].ToUnifiedFormat(dicts);
  auto& input = args.data[1];
  duckdb::UnifiedVectorFormat lists;
  input.ToUnifiedFormat(lists);
  const auto* entries =
    duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(lists);
  const auto& child = duckdb::ListVector::GetChild(input);
  duckdb::UnifiedVectorFormat elements;
  child.ToUnifiedFormat(elements);

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::ListVector::SetListSize(result, 0);
  irs::TokenSink writer;
  ListTokenSink sink{
    result, duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result),
    duckdb::FlatVector::ValidityMutable(result), writer};
  sink.ResetRows(count);
  const auto groups = GroupByDictionary(dicts, count, sink, [&](uint32_t r) {
    return lists.validity.RowIsValid(lists.sel->get_index(r));
  });
  std::vector<uint32_t> owners;
  for (size_t g = 0; g < groups.names.size(); ++g) {
    owners.clear();
    duckdb::idx_t total = 0;
    for (const auto r : groups.rows[g]) {
      total += entries[lists.sel->get_index(r)].length;
    }
    if (total == 0) {
      continue;
    }
    auto dict = LookupTokenizerDict(state.GetContext(), groups.names[g]);
    auto tokenizer =
      AcquireTextTokenizer(state.GetContext(), *dict, groups.names[g]);
    duckdb::SelectionVector sel{total};
    for (const auto r : groups.rows[g]) {
      const auto& entry = entries[lists.sel->get_index(r)];
      for (duckdb::idx_t k = 0; k < entry.length; ++k) {
        sel.set_index(owners.size(), elements.sel->get_index(entry.offset + k));
        owners.push_back(r);
      }
    }
    sink.FillGroup(*tokenizer, child, SliceFormat(elements, sel),
                   static_cast<uint32_t>(owners.size()), owners.data(), false);
  }
}

using ScalarFnPtr = void (*)(duckdb::DataChunk&, duckdb::ExpressionState&,
                             duckdb::Vector&);

template<ScalarFnPtr ConstantFn>
duckdb::unique_ptr<duckdb::FunctionData> TsLexizeBind(
  duckdb::BindScalarFunctionInput& input) {
  auto& context = input.GetClientContext();
  auto& conn_ctx = GetSereneDBContext(context);
  DynamicCtx ctx{.db_id = conn_ctx.GetDatabaseId()};

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
