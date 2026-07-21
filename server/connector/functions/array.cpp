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

#include "connector/functions/array.h"

#include <absl/strings/str_cat.h>

#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>

namespace sdb::connector {
namespace {

// Count nesting depth of LIST types at bind time.
int32_t CountListDepth(const duckdb::LogicalType& type) {
  int32_t depth = 0;
  auto* t = &type;
  while (t->id() == duckdb::LogicalTypeId::LIST) {
    ++depth;
    t = &duckdb::ListType::GetChildType(*t);
  }
  return depth;
}

struct SimpleIntBindData : public duckdb::FunctionData {
  int32_t ndims;

  explicit SimpleIntBindData(int32_t ndims) : ndims{ndims} {}

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    return duckdb::make_uniq<SimpleIntBindData>(ndims);
  }

  bool Equals(const duckdb::FunctionData& other) const final {
    return ndims == other.Cast<SimpleIntBindData>().ndims;
  }
};

// --- array_ndims ---

duckdb::unique_ptr<duckdb::FunctionData> ArrayNdimsBind(
  duckdb::BindScalarFunctionInput& input) {
  const auto& type = input.GetArguments()[0]->GetReturnType();
  return duckdb::make_uniq<SimpleIntBindData>(CountListDepth(type));
}

void ArrayNdimsFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                        duckdb::Vector& result) {
  auto& bind_data = state.expr.Cast<duckdb::BoundFunctionExpression>()
                      .BindInfo()
                      ->Cast<SimpleIntBindData>();
  auto count = args.size();
  auto& input = args.data[0];

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto* result_data = duckdb::FlatVector::GetDataMutable<int32_t>(result);
  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);

  duckdb::UnifiedVectorFormat input_data;
  input.ToUnifiedFormat(count, input_data);

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto idx = input_data.sel->get_index(i);
    if (!input_data.validity.RowIsValid(idx)) {
      result_validity.SetInvalid(i);
    } else {
      // PG: empty array returns NULL
      auto list_size = input_data.GetData<duckdb::list_entry_t>()[idx].length;
      if (list_size == 0) {
        result_validity.SetInvalid(i);
      } else {
        result_data[i] = bind_data.ndims;
      }
    }
  }
}

// --- array_dims ---

duckdb::unique_ptr<duckdb::FunctionData> ArrayDimsBind(
  duckdb::BindScalarFunctionInput& input) {
  const auto& type = input.GetArguments()[0]->GetReturnType();
  return duckdb::make_uniq<SimpleIntBindData>(CountListDepth(type));
}

void ArrayDimsFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                       duckdb::Vector& result) {
  auto& bind_data = state.expr.Cast<duckdb::BoundFunctionExpression>()
                      .BindInfo()
                      ->Cast<SimpleIntBindData>();
  auto ndims = bind_data.ndims;
  auto count = args.size();

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto* result_data =
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);

  // Build UnifiedVectorFormat for each nesting level.
  // Level 0 = input vector, level 1 = its child, etc.
  std::vector<duckdb::UnifiedVectorFormat> formats(ndims);
  {
    auto* vec = &args.data[0];
    for (int32_t d = 0; d < ndims; d++) {
      vec->ToUnifiedFormat(count, formats[d]);
      if (d + 1 < ndims) {
        vec = &duckdb::ListVector::GetEntry(*vec);
      }
    }
  }

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto idx = formats[0].sel->get_index(i);
    if (!formats[0].validity.RowIsValid(idx)) {
      result_validity.SetInvalid(i);
      continue;
    }

    auto& top_entry = formats[0].GetData<duckdb::list_entry_t>()[idx];
    if (top_entry.length == 0) {
      result_validity.SetInvalid(i);
      continue;
    }

    std::string dims;
    absl::StrAppend(&dims, "[1:", top_entry.length, "]");

    // Walk into first element of each deeper dimension
    auto child_idx = top_entry.offset;
    for (int32_t d = 1; d < ndims; d++) {
      auto mapped = formats[d].sel->get_index(child_idx);
      auto& entry = formats[d].GetData<duckdb::list_entry_t>()[mapped];
      absl::StrAppend(&dims, "[1:", entry.length, "]");
      child_idx = entry.offset;
    }

    result_data[i] = duckdb::StringVector::AddString(result, dims);
  }
}

}  // namespace

void RegisterPgArrayFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};

  auto list_any = duckdb::LogicalType::LIST(duckdb::LogicalType::ANY);

  duckdb::ScalarFunction array_ndims{
    "array_ndims",
    {
      list_any,
    },
    duckdb::LogicalType::INTEGER,
    ArrayNdimsFunction,
    ArrayNdimsBind,
  };
  loader.RegisterFunction(array_ndims);

  duckdb::ScalarFunction array_dims{
    "array_dims",
    {
      list_any,
    },
    duckdb::LogicalType::VARCHAR,
    ArrayDimsFunction,
    ArrayDimsBind,
  };
  loader.RegisterFunction(array_dims);
}

}  // namespace sdb::connector
