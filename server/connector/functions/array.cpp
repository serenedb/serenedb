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

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <duckdb/common/optional.hpp>
#include <duckdb/common/random_engine.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/main/settings.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <optional>
#include <span>
#include <vector>

#include "connector/functions/random.h"

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
  duckdb::UnaryExecutor::Execute<duckdb::list_entry_t, int32_t>(
    args.data[0], result, args.size(),
    [&](duckdb::list_entry_t list) -> duckdb::optional<int32_t> {
      // PG: empty array returns NULL
      if (list.length == 0) {
        return duckdb::nullopt;
      }
      return bind_data.ndims;
    });
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

void CheckPolymorphicType(const duckdb::LogicalType& type) {
  if (type.id() == duckdb::LogicalTypeId::SQLNULL) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
                    ERR_MSG("could not determine polymorphic type because "
                            "input has type unknown"));
  }
}

constexpr int64_t kMaxArraySize = 134217727;

size_t BoundDimensionCount(duckdb::BindScalarFunctionInput& input) {
  const auto* dims = input.GetArguments()[1].get();
  if (dims->IsFoldable() || dims->HasParameter()) {
    const auto value = input.GetConstant(1);
    if (value.IsNull()) {
      return 1;
    }
    const auto list =
      value.CastAs(input.GetClientContext(),
                   duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER));
    return std::max<size_t>(duckdb::ListValue::GetChildren(list).size(), 1);
  }
  while (duckdb::BoundCastExpression::IsCast(*dims)) {
    dims = &duckdb::BoundCastExpression::Child(
      dims->Cast<duckdb::BoundFunctionExpression>());
  }
  if (dims->GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION) {
    const auto& function = dims->Cast<duckdb::BoundFunctionExpression>();
    if (function.Function().GetName().GetIdentifierName() == "list_value") {
      return std::max<size_t>(function.GetChildren().size(), 1);
    }
  }
  return 1;
}

duckdb::unique_ptr<duckdb::FunctionData> ArrayFillBind(
  duckdb::BindScalarFunctionInput& input) {
  const auto element = input.GetArguments()[0]->GetReturnType();
  CheckPolymorphicType(element);
  const auto ndims = BoundDimensionCount(input);
  const auto max_dims =
    duckdb::Settings::Get<duckdb::MaxExpressionDepthSetting>(
      input.GetClientContext());
  if (ndims > max_dims) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    ERR_MSG("number of array dimensions (", ndims,
                            ") exceeds the maximum allowed (", max_dims, ")"));
  }
  auto type = element;
  for (size_t i = 0; i < ndims; ++i) {
    type = duckdb::LogicalType::LIST(type);
  }
  auto& function = input.GetBoundFunction();
  function.GetArguments()[0] = element;
  function.SetReturnType(type);
  return duckdb::make_uniq<SimpleIntBindData>(static_cast<int32_t>(ndims));
}

struct IntegerList {
  duckdb::UnifiedVectorFormat lists;
  duckdb::UnifiedVectorFormat values;

  IntegerList(const duckdb::Vector& vector, duckdb::idx_t count) {
    vector.ToUnifiedFormat(count, lists);
    duckdb::ListVector::GetChild(vector).ToUnifiedFormat(
      duckdb::ListVector::GetListSize(vector), values);
  }
};

void CheckDimensionsNotNull(const IntegerList& list, duckdb::idx_t row) {
  if (!list.lists.validity.RowIsValid(list.lists.sel->get_index(row))) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_NULL_VALUE_NOT_ALLOWED),
      ERR_MSG("dimension array or low bound array cannot be null"));
  }
}

void ReadDimensions(const IntegerList& list, duckdb::idx_t row,
                    std::vector<int32_t>& out) {
  const auto index = list.lists.sel->get_index(row);
  const auto entry = list.lists.GetData<duckdb::list_entry_t>()[index];
  out.clear();
  for (auto i = entry.offset; i < entry.offset + entry.length; ++i) {
    const auto value = list.values.sel->get_index(i);
    if (!list.values.validity.RowIsValid(value)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                      ERR_MSG("dimension values cannot be null"));
    }
    out.push_back(list.values.GetData<int32_t>()[value]);
  }
}

[[noreturn]] void ThrowArrayTooLarge() {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
    ERR_MSG("array size exceeds the maximum allowed (", kMaxArraySize, ")"));
}

int64_t ArrayItemCount(std::span<const int32_t> dims) {
  int64_t items = dims.empty() ? 0 : 1;
  for (const auto dim : dims) {
    if (dim < 0) {
      ThrowArrayTooLarge();
    }
    items *= dim;
    if (items > kMaxArraySize) {
      ThrowArrayTooLarge();
    }
  }
  return items;
}

void FillEntries(duckdb::Vector& list, duckdb::list_entry_t* entries,
                 duckdb::idx_t count, std::span<const int32_t> dims,
                 const duckdb::Vector& value, duckdb::idx_t row,
                 std::vector<duckdb::sel_t>& indexes) {
  const auto length = static_cast<duckdb::idx_t>(dims.front());
  const auto offset = duckdb::ListVector::GetListSize(list);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    entries[i] = {offset + i * length, length};
  }
  const auto total = count * length;
  if (dims.size() == 1) {
    indexes.assign(total, static_cast<duckdb::sel_t>(row));
    duckdb::ListVector::Append(
      list, value, duckdb::SelectionVector{indexes.data(), total}, total);
    return;
  }
  duckdb::ListVector::Reserve(list, offset + total);
  duckdb::ListVector::SetListSize(list, offset + total);
  auto& child = duckdb::ListVector::GetChildMutable(list);
  FillEntries(
    child,
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(child) + offset,
    total, dims.subspan(1), value, row, indexes);
}

void ArrayFillFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                       duckdb::Vector& result) {
  const auto ndims =
    static_cast<size_t>(state.expr.Cast<duckdb::BoundFunctionExpression>()
                          .BindInfo()
                          ->Cast<SimpleIntBindData>()
                          .ndims);
  const auto count = args.size();
  const IntegerList dims_list{args.data[1], count};
  std::optional<IntegerList> lbs_list;
  if (args.ColumnCount() > 2) {
    lbs_list.emplace(args.data[2], count);
  }
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto* entries =
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
  std::vector<int32_t> dims;
  std::vector<int32_t> lbs;
  std::vector<duckdb::sel_t> indexes;
  for (duckdb::idx_t row = 0; row < count; ++row) {
    CheckDimensionsNotNull(dims_list, row);
    if (lbs_list) {
      CheckDimensionsNotNull(*lbs_list, row);
    }
    ReadDimensions(dims_list, row, dims);
    lbs.assign(dims.size(), 1);
    if (lbs_list) {
      ReadDimensions(*lbs_list, row, lbs);
      if (lbs.size() != dims.size()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                        ERR_MSG("wrong number of array subscripts"),
                        ERR_DETAIL("Low bound array has different size than "
                                   "dimensions array."));
      }
    }
    const auto items = ArrayItemCount(dims);
    for (size_t i = 0; i < dims.size(); ++i) {
      if (static_cast<int64_t>(dims[i]) + lbs[i] >
          std::numeric_limits<int32_t>::max()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        ERR_MSG("array lower bound is too large: ", lbs[i]));
      }
    }
    if (items == 0) {
      entries[row] = {duckdb::ListVector::GetListSize(result), 0};
      continue;
    }
    if (absl::c_any_of(lbs, [](int32_t lb) { return lb != 1; })) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("array lower bounds other than 1 are not "
                              "supported"));
    }
    if (dims.size() != ndims) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("array_fill() cannot produce ", dims.size(),
                " dimensions where the query expects ", ndims),
        ERR_HINT("Pass the dimensions as a constant or an ARRAY[...] "
                 "constructor."));
    }
    FillEntries(result, entries + row, 1, dims, args.data[0], row, indexes);
  }
}

duckdb::unique_ptr<duckdb::FunctionData> ShuffleBind(
  duckdb::BindScalarFunctionInput& input) {
  const auto type = input.GetArguments()[0]->GetReturnType();
  CheckPolymorphicType(type);
  auto& function = input.GetBoundFunction();
  function.GetArguments()[0] = type;
  function.SetReturnType(type);
  return nullptr;
}

void ShuffleFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                     duckdb::Vector& result) {
  auto& random = duckdb::ExecuteFunctionState::GetFunctionState(state)
                   ->Cast<RandomState>()
                   .random;
  const auto count = args.size();
  duckdb::UnifiedVectorFormat lists;
  args.data[0].ToUnifiedFormat(count, lists);
  std::optional<duckdb::VectorIterator<int32_t>> samples;
  if (args.ColumnCount() > 1) {
    samples.emplace(args.data[1]);
  }
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto* entries =
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
  auto& validity = duckdb::FlatVector::ValidityMutable(result);
  std::vector<duckdb::sel_t> indexes;
  auto offset = duckdb::ListVector::GetListSize(result);
  for (duckdb::idx_t row = 0; row < count; ++row) {
    const auto index = lists.sel->get_index(row);
    if (!lists.validity.RowIsValid(index) ||
        (samples && !(*samples)[row].IsValid())) {
      validity.SetInvalid(row);
      continue;
    }
    const auto entry = lists.GetData<duckdb::list_entry_t>()[index];
    auto picks = entry.length;
    if (samples) {
      const auto sample = (*samples)[row].GetValue();
      if (sample < 0 || static_cast<duckdb::idx_t>(sample) > entry.length) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("sample size must be between 0 and ", entry.length));
      }
      picks = static_cast<duckdb::idx_t>(sample);
    }
    const auto first = indexes.size();
    for (auto i = entry.offset; i < entry.offset + entry.length; ++i) {
      indexes.push_back(static_cast<duckdb::sel_t>(i));
    }
    const auto items = std::span{indexes}.subspan(first);
    for (duckdb::idx_t i = 0; i < picks; ++i) {
      const auto j = i + random.NextRandomInteger(
                           0, static_cast<uint32_t>(entry.length - i));
      std::swap(items[i], items[j]);
    }
    indexes.resize(first + picks);
    entries[row] = {offset, picks};
    offset += picks;
  }
  duckdb::ListVector::Append(
    result, duckdb::ListVector::GetChild(args.data[0]),
    duckdb::SelectionVector{indexes.data(), indexes.size()}, indexes.size());
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

  {
    const auto int_list =
      duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER);
    duckdb::ScalarFunctionSet set{"array_fill"};
    for (const auto& arguments :
         {duckdb::vector<duckdb::LogicalType>{duckdb::LogicalType::ANY,
                                              int_list},
          duckdb::vector<duckdb::LogicalType>{duckdb::LogicalType::ANY,
                                              int_list, int_list}}) {
      duckdb::ScalarFunction function{set.name, arguments, list_any,
                                      ArrayFillFunction, ArrayFillBind};
      function.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
      set.AddFunction(std::move(function));
    }
    set.SetFallible();
    loader.RegisterFunction(set);
  }

  for (auto function : {
         duckdb::ScalarFunction{"array_shuffle",
                                {list_any},
                                list_any,
                                ShuffleFunction,
                                ShuffleBind,
                                nullptr,
                                InitRandomState},
         duckdb::ScalarFunction{"array_sample",
                                {list_any, duckdb::LogicalType::INTEGER},
                                list_any,
                                ShuffleFunction,
                                ShuffleBind,
                                nullptr,
                                InitRandomState},
       }) {
    function.SetVolatile();
    function.SetFallible();
    loader.RegisterFunction(std::move(function));
  }
}

}  // namespace sdb::connector
