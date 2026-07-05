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
#include <duckdb/function/scalar/nested_functions.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/planner/expression.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/utils/vector.hpp>
#include <vector>

#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

enum class Distance {
  L1 = 0,
  L2,
  L2Sqr,
  Cosine,
  CosineSimilarity,
  IP,
  NegativeIP,
};

enum class Norm {
  L1 = 0,
  L2,
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
R ComputeL2Norm(const T* x, size_t size) {
  auto res = irs::vector::L2Space<T, T, R>::Norm(
    reinterpret_cast<const irs::byte_type*>(x), static_cast<int16_t>(size));
  return std::sqrt(res);
}

template<typename T, typename R>
R ComputeL1(const T* l, const T* r, size_t size) {
  return irs::vector::L1Space<T, T, R>::Dist(
    reinterpret_cast<const irs::byte_type*>(l),
    reinterpret_cast<const irs::byte_type*>(r), static_cast<uint16_t>(size));
}

template<typename T, typename R>
R ComputeL1Norm(const T* x, size_t size) {
  return irs::vector::L1Space<T, T, R>::Norm(
    reinterpret_cast<const irs::byte_type*>(x), static_cast<int16_t>(size));
}

template<typename T, typename R>
R ComputeCos(const T* l, const T* r, size_t size) {
  const auto [ll, lr, rr] = irs::vector::CosineDistanceImpl<T, T, R>::Compute(
    reinterpret_cast<const irs::byte_type*>(l),
    reinterpret_cast<const irs::byte_type*>(r), static_cast<uint16_t>(size));
  const R denom = std::sqrt(ll * rr);
  if (denom == 0.0) {
    return 0.;
  }
  return lr / denom;
}

template<typename T, typename R>
R ComputeInnerProduct(const T* l, const T* r, size_t size) {
  auto res = irs::vector::DotProductImpl<T, R>::Compute(
    reinterpret_cast<const irs::byte_type*>(l),
    reinterpret_cast<const irs::byte_type*>(r), static_cast<uint16_t>(size));
  return res;
}

template<Distance D, typename T, typename R>
void ExecuteDistance(R& result, const T* l, const T* r, size_t size) {
  if constexpr (D == Distance::L1) {
    result = ComputeL1<T, R>(l, r, size);
  } else if constexpr (D == Distance::L2) {
    result = ComputeL2<T, R>(l, r, size);
  } else if constexpr (D == Distance::Cosine) {
    result = 1. - ComputeCos<T, R>(l, r, size);
  } else if constexpr (D == Distance::CosineSimilarity) {
    result = ComputeCos<T, R>(l, r, size);
  } else if constexpr (D == Distance::IP) {
    result = ComputeInnerProduct<T, R>(l, r, size);
  } else if constexpr (D == Distance::L2Sqr) {
    result = ComputeL2Sqr<T, R>(l, r, size);
  } else if constexpr (D == Distance::NegativeIP) {
    result = -ComputeInnerProduct<T, R>(l, r, size);
  } else {
    SDB_UNREACHABLE();
  }
}

template<Norm N, typename T, typename R>
void ExecuteNorm(R& result, const T* x, size_t size) {
  if constexpr (N == Norm::L1) {
    result = ComputeL1Norm<T, R>(x, size);
  } else if constexpr (N == Norm::L2) {
    result = ComputeL2Norm<T, R>(x, size);
  } else {
    SDB_UNREACHABLE();
  }
}

template<Norm N, typename T>
void ExecuteNormalize(T* out, const T* x, size_t size) {
  if constexpr (N == Norm::L1) {
    irs::vector::L1Space<T, T, T>::Normalize(
      reinterpret_cast<const irs::byte_type*>(x), static_cast<uint16_t>(size),
      out);
  } else if constexpr (N == Norm::L2) {
    irs::vector::L2Space<T, T, T>::Normalize(
      reinterpret_cast<const irs::byte_type*>(x), static_cast<uint16_t>(size),
      out);
  } else {
    SDB_UNREACHABLE();
  }
}

template<typename Elem>
struct ArrayInput {
  duckdb::UnifiedVectorFormat vdata;
  const Elem* data;
  const duckdb::ValidityMask* child_validity;
  duckdb::idx_t array_size;

  static ArrayInput Make(duckdb::Vector& v, duckdb::idx_t batch_size) {
    ArrayInput in;
    in.array_size = duckdb::ArrayType::GetSize(v.GetType());
    v.ToUnifiedFormat(batch_size, in.vdata);
    auto& child = duckdb::ArrayVector::GetEntry(v);
    in.data = duckdb::FlatVector::GetData<Elem>(child);
    in.child_validity = &duckdb::FlatVector::Validity(child);
    return in;
  }

  const Elem* RowData(duckdb::idx_t row) const {
    auto idx = vdata.sel->get_index(row);
    if (!vdata.validity.RowIsValid(idx)) {
      return nullptr;
    }
    auto base = idx * array_size;
    for (duckdb::idx_t i = 0; i < array_size; ++i) {
      if (!child_validity->RowIsValid(base + i)) {
        return nullptr;
      }
    }
    return data + base;
  }
};

template<Distance D, typename Elem, typename Res>
static void ArrayDistanceExecutor(duckdb::DataChunk& args,
                                  duckdb::ExpressionState& state,
                                  duckdb::Vector& result) {
  duckdb::idx_t batch_size = args.size();
  auto left = ArrayInput<Elem>::Make(args.data[0], batch_size);
  auto right = ArrayInput<Elem>::Make(args.data[1], batch_size);
  if (left.array_size == 0) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Distance operators require non-empty arrays"));
  }
  if (left.array_size != right.array_size) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Array dimensions must be equal: left has ",
                            left.array_size, ", right has ", right.array_size));
  }

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  Res* result_data = duckdb::FlatVector::GetDataMutable<Res>(result);
  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);

  for (duckdb::idx_t row = 0; row < batch_size; row++) {
    const Elem* l = left.RowData(row);
    const Elem* r = right.RowData(row);
    if (l == nullptr || r == nullptr) {
      result_validity.SetInvalid(row);
      continue;
    }
    ExecuteDistance<D, Elem, Res>(result_data[row], l, r, left.array_size);
  }
}

template<Norm N, typename Elem, typename Res>
static void ArrayNormExecutor(duckdb::DataChunk& args,
                              duckdb::ExpressionState& state,
                              duckdb::Vector& result) {
  duckdb::idx_t batch_size = args.size();
  auto in = ArrayInput<Elem>::Make(args.data[0], batch_size);
  if (in.array_size == 0) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Norm operators require non-empty arrays"));
  }

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  Res* result_data = duckdb::FlatVector::GetDataMutable<Res>(result);
  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);

  for (duckdb::idx_t row = 0; row < batch_size; row++) {
    const Elem* x = in.RowData(row);
    if (x == nullptr) {
      result_validity.SetInvalid(row);
      continue;
    }
    ExecuteNorm<N, Elem, Res>(result_data[row], x, in.array_size);
  }
}

template<Norm N, typename Elem>
static void ArrayNormalizeExecutor(duckdb::DataChunk& args,
                                   duckdb::ExpressionState& state,
                                   duckdb::Vector& result) {
  duckdb::idx_t batch_size = args.size();
  auto in = ArrayInput<Elem>::Make(args.data[0], batch_size);
  if (in.array_size == 0) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Normalize operators require non-empty arrays"));
  }

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);
  auto& result_child = duckdb::ArrayVector::GetEntry(result);
  Elem* result_data = duckdb::FlatVector::GetDataMutable<Elem>(result_child);
  auto& result_child_validity =
    duckdb::FlatVector::ValidityMutable(result_child);

  for (duckdb::idx_t row = 0; row < batch_size; row++) {
    const Elem* x = in.RowData(row);
    if (x == nullptr) {
      result_validity.SetInvalid(row);
      for (duckdb::idx_t i = 0; i < in.array_size; i++) {
        result_child_validity.SetInvalid(row * in.array_size + i);
      }
      continue;
    }
    ExecuteNormalize<N, Elem>(result_data + (row * in.array_size), x,
                              in.array_size);
  }
}

static void ValidateArrayArgs(duckdb::BindScalarFunctionInput& input) {
  auto& bound = input.GetBoundFunction();
  auto& args = input.GetArguments();
  for (auto& a : args) {
    if (a->GetReturnType().id() != duckdb::LogicalTypeId::ARRAY) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
                      ERR_MSG(bound.GetName().GetIdentifierName(),
                              " requires ARRAY arguments"));
    }
  }
}

static duckdb::unique_ptr<duckdb::FunctionData> NormalizeBind(
  duckdb::BindScalarFunctionInput& input) {
  ValidateArrayArgs(input);
  auto& bound = input.GetBoundFunction();
  auto& args = input.GetArguments();
  bound.SetReturnType(args[0]->GetReturnType());
  return duckdb::make_uniq<duckdb::VariableReturnBindData>(
    bound.GetReturnType());
}

static duckdb::unique_ptr<duckdb::FunctionData> ScalarArrayBind(
  duckdb::BindScalarFunctionInput& input) {
  ValidateArrayArgs(input);
  return nullptr;
}

template<Norm N>
void RegisterNormalize(duckdb::ExtensionLoader& loader) {
  std::string name;
  if constexpr (N == Norm::L1) {
    name = kL1Normalize;
  } else if constexpr (N == Norm::L2) {
    name = kL2Normalize;
  } else {
    SDB_UNREACHABLE();
  }
  const duckdb::ScalarFunction float_fn(
    {duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT,
                                duckdb::optional_idx{})},
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT,
                               duckdb::optional_idx{}),
    ArrayNormalizeExecutor<N, float>, NormalizeBind);
  const duckdb::ScalarFunction double_fn(
    {duckdb::LogicalType::ARRAY(duckdb::LogicalType::DOUBLE,
                                duckdb::optional_idx{})},
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::DOUBLE,
                               duckdb::optional_idx{}),
    ArrayNormalizeExecutor<N, double>, NormalizeBind);
  duckdb::ScalarFunctionSet fn{duckdb::Identifier{name}};
  fn.AddFunction(float_fn);
  fn.AddFunction(double_fn);
  loader.RegisterFunction(std::move(fn));
}

template<Norm N>
void RegisterNorm(duckdb::ExtensionLoader& loader) {
  std::string name;
  irs::HNSWMetric metric;
  ScoreEmit score_emit = ScoreEmit::Identity;
  if constexpr (N == Norm::L1) {
    name = kL1Norm;
    metric = irs::HNSWMetric::L1;
  } else if constexpr (N == Norm::L2) {
    name = kL2Norm;
    metric = irs::HNSWMetric::L2Sqr;
    score_emit = ScoreEmit::Sqrt;
  } else {
    SDB_UNREACHABLE();
  }
  duckdb::ScalarFunction float_fn(
    {duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT,
                                duckdb::optional_idx{})},
    duckdb::LogicalType::FLOAT, ArrayNormExecutor<N, float, float>,
    ScalarArrayBind);
  duckdb::ScalarFunction double_fn(
    {duckdb::LogicalType::ARRAY(duckdb::LogicalType::DOUBLE,
                                duckdb::optional_idx{})},
    duckdb::LogicalType::DOUBLE, ArrayNormExecutor<N, double, double>,
    ScalarArrayBind);
  float_fn.SetExtraFunctionInfo<AnnFunctionInfo>(
    metric, duckdb::OrderType::ASCENDING, /*is_norm=*/true, score_emit);
  double_fn.SetExtraFunctionInfo<AnnFunctionInfo>(
    metric, duckdb::OrderType::ASCENDING, /*is_norm=*/true, score_emit);
  duckdb::ScalarFunctionSet norm{duckdb::Identifier{name}};
  norm.AddFunction(float_fn);
  norm.AddFunction(double_fn);
  loader.RegisterFunction(std::move(norm));
}

template<Distance D>
void RegisterDistance(duckdb::ExtensionLoader& loader) {
  std::string name;
  std::string op_name;
  irs::HNSWMetric metric;
  duckdb::OrderType order = duckdb::OrderType::ASCENDING;
  ScoreEmit score_emit = ScoreEmit::Identity;
  if constexpr (D == Distance::L1) {
    name = kL1Distance;
    op_name = kL1DistanceOp;
    metric = irs::HNSWMetric::L1;
  } else if constexpr (D == Distance::L2) {
    name = kL2Distance;
    op_name = kL2DistanceOp;
    metric = irs::HNSWMetric::L2Sqr;
    score_emit = ScoreEmit::Sqrt;
  } else if constexpr (D == Distance::L2Sqr) {
    name = kL2SqrDistance;
    metric = irs::HNSWMetric::L2Sqr;
  } else if constexpr (D == Distance::Cosine) {
    name = kCosineDistance;
    op_name = kCosineDistanceOp;
    metric = irs::HNSWMetric::Cosine;
  } else if constexpr (D == Distance::CosineSimilarity) {
    name = kCosineSimilarity;
    metric = irs::HNSWMetric::Cosine;
    order = duckdb::OrderType::DESCENDING;
    score_emit = ScoreEmit::OneMinus;
  } else if constexpr (D == Distance::IP) {
    name = kIP;
    metric = irs::HNSWMetric::NegativeIP;
    order = duckdb::OrderType::DESCENDING;
    score_emit = ScoreEmit::Negate;
  } else if constexpr (D == Distance::NegativeIP) {
    name = kNegativeIP;
    op_name = kNegativeIPDistanceOp;
    metric = irs::HNSWMetric::NegativeIP;
  } else {
    SDB_UNREACHABLE();
  }
  duckdb::ScalarFunction float_fn(
    {duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT,
                                duckdb::optional_idx{}),
     duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT,
                                duckdb::optional_idx{})},
    duckdb::LogicalType::FLOAT, ArrayDistanceExecutor<D, float, float>,
    ScalarArrayBind);
  duckdb::ScalarFunction double_fn(
    {duckdb::LogicalType::ARRAY(duckdb::LogicalType::DOUBLE,
                                duckdb::optional_idx{}),
     duckdb::LogicalType::ARRAY(duckdb::LogicalType::DOUBLE,
                                duckdb::optional_idx{})},
    duckdb::LogicalType::DOUBLE, ArrayDistanceExecutor<D, double, double>,
    ScalarArrayBind);
  float_fn.SetExtraFunctionInfo<AnnFunctionInfo>(metric, order,
                                                 /*is_norm=*/false, score_emit);
  double_fn.SetExtraFunctionInfo<AnnFunctionInfo>(metric, order,
                                                  /*is_norm=*/false,
                                                  score_emit);
  duckdb::ScalarFunctionSet distance{duckdb::Identifier{name}};
  distance.AddFunction(float_fn);
  distance.AddFunction(double_fn);
  loader.RegisterFunction(std::move(distance));
  if (!op_name.empty()) {
    duckdb::ScalarFunctionSet op{duckdb::Identifier{op_name}};
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
  RegisterDistance<Distance::CosineSimilarity>(loader);
  RegisterDistance<Distance::IP>(loader);
  RegisterDistance<Distance::NegativeIP>(loader);
  RegisterDistance<Distance::L2Sqr>(loader);
  RegisterNorm<Norm::L1>(loader);
  RegisterNorm<Norm::L2>(loader);
  RegisterNormalize<Norm::L1>(loader);
  RegisterNormalize<Norm::L2>(loader);
}

}  // namespace sdb::connector
