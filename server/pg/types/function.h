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

#include <absl/container/btree_map.h>

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector_operations/binary_executor.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/function/cast/default_casts.hpp>
#include <duckdb/function/function_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/database.hpp>
#include <optional>
#include <span>
#include <string_view>

namespace sdb::pg {

template<typename T>
class ScalarArg {
 public:
  explicit ScalarArg(const duckdb::Vector& vector) {
    vector.ToUnifiedFormat(_format);
  }

  std::optional<T> operator[](duckdb::idx_t row) const {
    const auto index = _format.sel->get_index(row);
    if (!_format.validity.RowIsValid(index)) {
      return std::nullopt;
    }
    return duckdb::UnifiedVectorFormat::GetData<T>(_format)[index];
  }

 private:
  duckdb::UnifiedVectorFormat _format;
};

template<typename T, auto Read>
class RecursiveArg {
 public:
  explicit RecursiveArg(const duckdb::Vector& vector) {
    duckdb::Vector::RecursiveToUnifiedFormat(vector, _format);
  }

  std::optional<T> operator[](duckdb::idx_t row) const {
    const auto index = _format.unified.sel->get_index(row);
    if (!_format.unified.validity.RowIsValid(index)) {
      return std::nullopt;
    }
    return Read(_format, index);
  }

 private:
  duckdb::RecursiveUnifiedVectorFormat _format;
};

template<typename T>
void StoreValue(duckdb::Vector& result, duckdb::idx_t row, const T& value) {
  duckdb::FlatVector::GetDataMutable<T>(result)[row] = value;
}

template<typename T>
void StoreList(duckdb::Vector& list, duckdb::idx_t row,
               std::span<const T> values) {
  const auto offset = duckdb::ListVector::GetListSize(list);
  duckdb::ListVector::Reserve(list, offset + values.size());
  auto& child = duckdb::ListVector::GetChildMutable(list);
  for (size_t i = 0; i < values.size(); ++i) {
    StoreValue(child, offset + i, values[i]);
  }
  duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(list)[row] = {
    offset, values.size()};
  duckdb::ListVector::SetListSize(list, offset + values.size());
}

template<typename T>
void Store(duckdb::Vector& result, duckdb::idx_t row, const T& value) {
  StoreValue(result, row, value);
}

template<typename T>
void Store(duckdb::Vector& result, duckdb::idx_t row,
           const std::optional<T>& value) {
  if (value) {
    StoreValue(result, row, *value);
  } else {
    duckdb::FlatVector::SetNull(result, row, true);
  }
}

template<typename Arg, auto Apply>
void Map(const duckdb::Vector& source, duckdb::Vector& result,
         duckdb::idx_t count) {
  const Arg arg{source};
  for (duckdb::idx_t row = 0; row < count; ++row) {
    if (const auto value = arg[row]) {
      Store(result, row, Apply(*value));
    } else {
      duckdb::FlatVector::SetNull(result, row, true);
    }
  }
}

template<typename Arg, auto Apply>
void Unary(duckdb::DataChunk& args, duckdb::ExpressionState&,
           duckdb::Vector& result) {
  Map<Arg, Apply>(args.data[0], result, args.size());
}

template<typename Arg, auto Apply>
bool UnaryCast(duckdb::Vector& source, duckdb::Vector& result,
               duckdb::idx_t count, duckdb::CastParameters&) {
  Map<Arg, Apply>(source, result, count);
  return true;
}

template<typename Left, typename Right, auto Apply>
void Binary(duckdb::DataChunk& args, duckdb::ExpressionState&,
            duckdb::Vector& result) {
  const Left left{args.data[0]};
  const Right right{args.data[1]};
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    const auto l = left[row];
    const auto r = right[row];
    if (l && r) {
      Store(result, row, Apply(*l, *r));
    } else {
      duckdb::FlatVector::SetNull(result, row, true);
    }
  }
}

template<typename In, typename Out, auto Apply>
void UnaryExecute(duckdb::DataChunk& args, duckdb::ExpressionState&,
                  duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<In, Out>(args.data[0], result, args.size(),
                                          Apply);
}

template<typename Left, typename Right, typename Out, auto Apply>
void BinaryExecute(duckdb::DataChunk& args, duckdb::ExpressionState&,
                   duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<Left, Right, Out>(args.data[0], args.data[1],
                                                    result, args.size(), Apply);
}

inline duckdb::ScalarFunction Fallible(duckdb::ScalarFunction function) {
  function.SetFallible();
  return function;
}

struct Functions {
  absl::btree_map<std::string_view, duckdb::ScalarFunctionSet> scalars;
  absl::btree_map<std::string_view, duckdb::AggregateFunctionSet> aggregates;
};

inline void Add(Functions& functions, std::string_view name,
                duckdb::ScalarFunction function) {
  functions.scalars.try_emplace(name, duckdb::Identifier{name})
    .first->second.AddFunction(std::move(function));
}

inline void Add(Functions& functions, std::string_view name,
                duckdb::AggregateFunction function) {
  function.SetName(duckdb::Identifier{name});
  functions.aggregates.try_emplace(name, duckdb::Identifier{name})
    .first->second.AddFunction(std::move(function));
}

inline void AddDivision(Functions& functions,
                        const duckdb::ScalarFunction& function) {
  Add(functions, "/", function);
  Add(functions, "//", function);
}

void RegisterFunctions(duckdb::DatabaseInstance& db, Functions functions);

}  // namespace sdb::pg
