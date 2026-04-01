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

#include "pg/sdb_functions/vector.h"

#include <velox/expression/ComplexViewTypes.h>
#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <velox/type/SimpleFunctionApi.h>

#include <cmath>
#include <iresearch/utils/vector.hpp>
#include <vector>

#include "basics/fwd.h"
#include "pg/sql_exception_macro.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg::functions {
namespace {

// From velox
template<typename T>
const T* GetArrayDataOrCopy(const velox::exec::ArrayView<false, T>& array,
                            std::vector<T>& buffer) {
  if (array.isFlatElements()) {
    auto flatVec = dynamic_cast<const facebook::velox::FlatVector<T>*>(
      array.elementsVectorBase());
    if (flatVec) {
      return flatVec->template rawValues<T>() + array.offset();
    }
  }
  // Not flat: must copy
  buffer.resize(array.size());
  for (size_t i = 0; i < array.size(); ++i) {
    buffer[i] = array[i];
  }
  return buffer.data();
}

template<typename T>
void CheckVectors(const velox::exec::ArrayView<false, T>& l,
                  const velox::exec::ArrayView<false, T>& r) {
  if (l.size() != r.size() || l.size() == 0) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Vectors must be of same non-zero size"));
  }
}

template<typename T, typename R>
struct ComputeDistanceBase {
  R ComputeL2(const velox::exec::ArrayView<false, T>& l,
              const velox::exec::ArrayView<false, T>& r, size_t size) {
    const auto* l_data = GetArrayDataOrCopy(l, lbuf);
    const auto* r_data = GetArrayDataOrCopy(r, rbuf);
    return irs::vector::L2Space<T, T, R>::Dist(
      reinterpret_cast<const irs::byte_type*>(l_data),
      reinterpret_cast<const irs::byte_type*>(r_data),
      static_cast<uint16_t>(l.size()));
  }

  R ComputeL1(const velox::exec::ArrayView<false, T>& l,
              const velox::exec::ArrayView<false, T>& r, size_t size) {
    const auto* l_data = GetArrayDataOrCopy(l, lbuf);
    const auto* r_data = GetArrayDataOrCopy(r, rbuf);
    return irs::vector::L1Space<T, T, R>::Dist(
      reinterpret_cast<const irs::byte_type*>(l_data),
      reinterpret_cast<const irs::byte_type*>(r_data),
      static_cast<uint16_t>(l.size()));
  }

  R ComputeCosine(const velox::exec::ArrayView<false, T>& l,
                  const velox::exec::ArrayView<false, T>& r, size_t size) {
    const auto* l_data = GetArrayDataOrCopy(l, lbuf);
    const auto* r_data = GetArrayDataOrCopy(r, rbuf);
    const auto [ll, lr, rr] = irs::vector::CosineDistanceImpl<T, T, R>::Compute(
      reinterpret_cast<const irs::byte_type*>(l_data),
      reinterpret_cast<const irs::byte_type*>(r_data),
      static_cast<uint16_t>(l.size()));
    const T denom = std::sqrt(ll * rr);
    if (denom == 0.0) {
      return 0.0;
    }
    return lr / denom;
  }

  R ComputeDotProduct(const velox::exec::ArrayView<false, T>& l,
                      const velox::exec::ArrayView<false, T>& r, size_t size) {
    const auto* l_data = GetArrayDataOrCopy(l, lbuf);
    const auto* r_data = GetArrayDataOrCopy(r, rbuf);
    return irs::vector::DotProductImpl<T, R>::Compute(
      reinterpret_cast<const irs::byte_type*>(l_data),
      reinterpret_cast<const irs::byte_type*>(r_data),
      static_cast<uint16_t>(l.size()));
  }

  std::vector<T> lbuf, rbuf;
};

template<typename T, typename Elem, typename Res>
struct L2Squared : public ComputeDistanceBase<Elem, Res> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void callNullFree(
    out_type<Res>& result, const velox::exec::ArrayView<false, Elem>& l,
    const velox::exec::ArrayView<false, Elem>& r) {
    CheckVectors(l, r);
    result = this->ComputeL2(l, r, l.size());
  }
};

template<typename T>
using L2SquaredDouble = L2Squared<T, double, double>;

template<typename T>
using L2SquaredFloat = L2Squared<T, float, float>;

template<typename T>
using L2SquaredInt32 = L2Squared<T, int32_t, int64_t>;

template<typename T, typename Elem, typename Res>
struct L1Distance : public ComputeDistanceBase<Elem, Res> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void callNullFree(
    out_type<Res>& result, const velox::exec::ArrayView<false, Elem>& l,
    const velox::exec::ArrayView<false, Elem>& r) {
    CheckVectors(l, r);
    result = this->ComputeL1(l, r, l.size());
  }
};

template<typename T>
using L1DistanceFloat = L1Distance<T, float, float>;

template<typename T>
using L1DistanceDouble = L1Distance<T, double, double>;

template<typename T>
using L1DistanceInt32 = L1Distance<T, int32_t, int64_t>;

template<typename T, typename Elem, typename Res>
struct CosineSimilarity : public ComputeDistanceBase<Elem, Res> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void callNullFree(
    out_type<Res>& result, const velox::exec::ArrayView<false, Elem>& l,
    const velox::exec::ArrayView<false, Elem>& r) {
    CheckVectors(l, r);
    result = this->ComputeCosine(l, r, l.size());
  }
};

template<typename T>
using CosineSimilarityFloat = CosineSimilarity<T, float, float>;

template<typename T>
using CosineSimilarityDouble = CosineSimilarity<T, double, double>;

template<typename T, typename Elem, typename Res>
struct DotProduct : public ComputeDistanceBase<Elem, Res> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void callNullFree(
    out_type<Res>& result, const velox::exec::ArrayView<false, Elem>& l,
    const velox::exec::ArrayView<false, Elem>& r) {
    CheckVectors(l, r);
    result = this->ComputeDotProduct(l, r, l.size());
  }
};

template<typename T>
using DotProductFloat = DotProduct<T, float, float>;

template<typename T>
using DotProductDouble = DotProduct<T, double, double>;

template<typename T>
using DotProductInt32 = DotProduct<T, int32_t, int64_t>;

}  // namespace

void RegisterVectorFunctions(const std::string& prefix) {
  velox::registerFunction<L2SquaredFloat, float, velox::Array<float>,
                          velox::Array<float>>({prefix + kL2Distance});
  velox::registerFunction<L2SquaredDouble, double, velox::Array<double>,
                          velox::Array<double>>({prefix + kL2Distance});
  velox::registerFunction<L2SquaredInt32, int64_t, velox::Array<int32_t>,
                          velox::Array<int32_t>>({prefix + kL2Distance});

  velox::registerFunction<L1DistanceFloat, float, velox::Array<float>,
                          velox::Array<float>>({prefix + kL1Distance});
  velox::registerFunction<L1DistanceDouble, double, velox::Array<double>,
                          velox::Array<double>>({prefix + kL1Distance});
  velox::registerFunction<L1DistanceInt32, int64_t, velox::Array<int32_t>,
                          velox::Array<int32_t>>({prefix + kL1Distance});

  velox::registerFunction<CosineSimilarityFloat, float, velox::Array<float>,
                          velox::Array<float>>({prefix + kCosineDistance});
  velox::registerFunction<CosineSimilarityDouble, double, velox::Array<double>,
                          velox::Array<double>>({prefix + kCosineDistance});

  velox::registerFunction<DotProductFloat, float, velox::Array<float>,
                          velox::Array<float>>({prefix + kInnerProduct});
  velox::registerFunction<DotProductDouble, double, velox::Array<double>,
                          velox::Array<double>>({prefix + kInnerProduct});
  velox::registerFunction<DotProductInt32, int64_t, velox::Array<int32_t>,
                          velox::Array<int32_t>>({prefix + kInnerProduct});
}

}  // namespace sdb::pg::functions
