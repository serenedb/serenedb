////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "connector/functions/vector.h"

#include <cmath>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/function_set.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/planner/expression.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/utils/vector.hpp>
#include <vector>

#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

enum class Distance {
  L1 = 0,
  L2,
  L2Sqr,
  Cosine,
  IP,
};

template<typename T, typename R>
R ComputeL2Sqr(const T* l, const T* r, size_t size) {
  return irs::vector::L2Space<T, T, R>::Dist(
    reinterpret_cast<const irs::byte_type*>(l),
    reinterpret_cast<const irs::byte_type*>(r), static_cast<uint16_t>(size));
}

template<typename T, typename R>
R ComputeL2(const T* l, const T* r, size_t size) {
  auto res = ComputeL2Sqr<T, R>(l, r, size);
  return std::sqrt(res);
}

template<typename T, typename R>
R ComputeL1(const T* l, const T* r, size_t size) {
  return irs::vector::L1Space<T, T, R>::Dist(
    reinterpret_cast<const irs::byte_type*>(l),
    reinterpret_cast<const irs::byte_type*>(r), static_cast<uint16_t>(size));
}

template<typename T, typename R>
R ComputeCosine(const T* l, const T* r, size_t size) {
  const auto [ll, lr, rr] = irs::vector::CosineDistanceImpl<T, T, R>::Compute(
    reinterpret_cast<const irs::byte_type*>(l),
    reinterpret_cast<const irs::byte_type*>(r), static_cast<uint16_t>(size));
  const R denom = std::sqrt(ll * rr);
  if (denom == 0.0) {
    return 0.0;
  }
  return lr / denom;
}

template<typename T, typename R>
R ComputeDotProduct(const T* l, const T* r, size_t size) {
  return -irs::vector::DotProductImpl<T, R>::Compute(
    reinterpret_cast<const irs::byte_type*>(l),
    reinterpret_cast<const irs::byte_type*>(r), static_cast<uint16_t>(size));
}

template<Distance D, typename T, typename R>
void Execute(R& result, const T* l, const T* r, size_t size) {
  if constexpr (D == Distance::L1) {
    result = ComputeL1<T, R>(l, r, size);
  } else if constexpr (D == Distance::L2) {
    result = ComputeL2<T, R>(l, r, size);
  } else if constexpr (D == Distance::Cosine) {
    result = ComputeCosine<T, R>(l, r, size);
  } else if constexpr (D == Distance::IP) {
    result = ComputeDotProduct<T, R>(l, r, size);
  } else if constexpr (D == Distance::L2Sqr) {
    result = ComputeL2Sqr<T, R>(l, r, size);
  } else {
    SDB_UNREACHABLE();
  }
}

template<Distance D, typename Elem, typename Res>
static void ArrayDistanceExecutor(duckdb::DataChunk& args,
                                  duckdb::ExpressionState& state,
                                  duckdb::Vector& result) {
  auto& left_vector = args.data[0];
  auto& right_vector = args.data[1];
  duckdb::idx_t batch_size = args.size();

  duckdb::idx_t array_size = duckdb::ArrayType::GetSize(left_vector.GetType());
  duckdb::idx_t right_array_size =
    duckdb::ArrayType::GetSize(right_vector.GetType());
  if (array_size == 0) {
    throw duckdb::InvalidInputException(
      "Distance operators require non-empty arrays");
  }
  if (array_size != right_array_size) {
    throw duckdb::InvalidInputException(
      "Array dimensions must be equal: left has %llu, right has %llu",
      array_size, right_array_size);
  }

  duckdb::UnifiedVectorFormat left_vdata, right_vdata;
  left_vector.ToUnifiedFormat(batch_size, left_vdata);
  right_vector.ToUnifiedFormat(batch_size, right_vdata);

  auto& left_child = duckdb::ArrayVector::GetEntry(left_vector);
  auto& right_child = duckdb::ArrayVector::GetEntry(right_vector);
  const Elem* left_data = duckdb::FlatVector::GetData<Elem>(left_child);
  const Elem* right_data = duckdb::FlatVector::GetData<Elem>(right_child);
  auto& left_child_validity = duckdb::FlatVector::Validity(left_child);
  auto& right_child_validity = duckdb::FlatVector::Validity(right_child);

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  Res* result_data = duckdb::FlatVector::GetDataMutable<Res>(result);
  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);

  for (duckdb::idx_t row = 0; row < batch_size; row++) {
    auto left_idx = left_vdata.sel->get_index(row);
    auto right_idx = right_vdata.sel->get_index(row);

    if (!left_vdata.validity.RowIsValid(left_idx) ||
        !right_vdata.validity.RowIsValid(right_idx)) {
      result_validity.SetInvalid(row);
      continue;
    }

    bool has_null = false;
    for (duckdb::idx_t i = 0; i < array_size; i++) {
      if (!left_child_validity.RowIsValid(left_idx * array_size + i) ||
          !right_child_validity.RowIsValid(right_idx * array_size + i)) {
        has_null = true;
        break;
      }
    }
    if (has_null) {
      result_validity.SetInvalid(row);
      continue;
    }

    Execute<D, Elem, Res>(result_data[row], left_data + (left_idx * array_size),
                          right_data + (right_idx * array_size), array_size);
  }
}

template<Distance D>
void RegisterDistance(duckdb::ExtensionLoader& loader) {
  std::string name;
  std::string op_name;
  if constexpr (D == Distance::L1) {
    name = kL1Distance;
    op_name = kL1DistanceOp;
  } else if constexpr (D == Distance::L2) {
    name = kL2Distance;
    op_name = kL2DistanceOp;
  } else if constexpr (D == Distance::L2Sqr) {
    name = kL2SqrDistance;
  } else if constexpr (D == Distance::Cosine) {
    name = kCosineDistance;
    op_name = kCosineDistanceOp;
  } else if constexpr (D == Distance::IP) {
    name = kInnerProduct;
    op_name = kIPDistanceOp;
  } else {
    SDB_UNREACHABLE();
  }
  const duckdb::ScalarFunction float_fn(
    {duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT,
                                duckdb::optional_idx{}),
     duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT,
                                duckdb::optional_idx{})},
    duckdb::LogicalType::FLOAT, ArrayDistanceExecutor<D, float, float>);
  const duckdb::ScalarFunction double_fn(
    {duckdb::LogicalType::ARRAY(duckdb::LogicalType::DOUBLE,
                                duckdb::optional_idx{}),
     duckdb::LogicalType::ARRAY(duckdb::LogicalType::DOUBLE,
                                duckdb::optional_idx{})},
    duckdb::LogicalType::DOUBLE, ArrayDistanceExecutor<D, double, double>);
  duckdb::ScalarFunctionSet distance{name};
  distance.AddFunction(float_fn);
  distance.AddFunction(double_fn);
  loader.RegisterFunction(std::move(distance));
  if (!op_name.empty()) {
    duckdb::ScalarFunctionSet op{op_name};
    op.AddFunction(float_fn);
    op.AddFunction(double_fn);
    loader.RegisterFunction(std::move(op));
  }
}

}  // namespace

void RegisterVectorFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");
  RegisterDistance<Distance::L1>(loader);
  RegisterDistance<Distance::L2>(loader);
  RegisterDistance<Distance::Cosine>(loader);
  RegisterDistance<Distance::IP>(loader);
  RegisterDistance<Distance::L2Sqr>(loader);
}

}  // namespace sdb::connector
