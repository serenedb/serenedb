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

template<typename T>
T ComputeL2(const velox::exec::ArrayView<false, T>& l,
            const velox::exec::ArrayView<false, T>& r, size_t size) {
  std::vector<T> lbuf, rbuf;
  const auto* l_data = GetArrayDataOrCopy(l, lbuf);
  const auto* r_data = GetArrayDataOrCopy(r, rbuf);
  return irs::vector::L2Space<T, T, T>::Dist(
    reinterpret_cast<const irs::byte_type*>(l_data),
    reinterpret_cast<const irs::byte_type*>(r_data),
    static_cast<uint16_t>(l.size()));
}

template<typename T>
T ComputeL1(const velox::exec::ArrayView<false, T>& l,
            const velox::exec::ArrayView<false, T>& r, size_t size) {
  std::vector<T> lbuf, rbuf;
  const auto* l_data = GetArrayDataOrCopy(l, lbuf);
  const auto* r_data = GetArrayDataOrCopy(r, rbuf);
  return irs::vector::L1Space<T, T, T>::Dist(
    reinterpret_cast<const irs::byte_type*>(l_data),
    reinterpret_cast<const irs::byte_type*>(r_data),
    static_cast<uint16_t>(l.size()));
}

template<typename T>
T ComputeCosine(const velox::exec::ArrayView<false, T>& l,
                const velox::exec::ArrayView<false, T>& r, size_t size) {
  std::vector<T> lbuf, rbuf;
  const auto* l_data = GetArrayDataOrCopy(l, lbuf);
  const auto* r_data = GetArrayDataOrCopy(r, rbuf);
  const auto [ll, lr, rr] =
    irs::vector::CosineDistanceImpl<T, T, double>::Compute(
      reinterpret_cast<const irs::byte_type*>(l_data),
      reinterpret_cast<const irs::byte_type*>(r_data),
      static_cast<uint16_t>(l.size()));
  const T denom = std::sqrt(ll * rr);
  if (denom == 0.0) {
    return 0.0;
  }
  return lr / denom;
}

template<typename T>
T ComputeDotProduct(const velox::exec::ArrayView<false, T>& l,
                    const velox::exec::ArrayView<false, T>& r, size_t size) {
  std::vector<T> lbuf, rbuf;
  const auto* l_data = GetArrayDataOrCopy(l, lbuf);
  const auto* r_data = GetArrayDataOrCopy(r, rbuf);
  return irs::vector::DotProductImpl<T, T>::Compute(
    reinterpret_cast<const irs::byte_type*>(l_data),
    reinterpret_cast<const irs::byte_type*>(r_data),
    static_cast<uint16_t>(l.size()));
}

template<typename T, typename Elem>
struct L2Squared {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool callNullFree(  // NOLINT
    out_type<Elem>& result, const velox::exec::ArrayView<false, Elem>& l,
    const velox::exec::ArrayView<false, Elem>& r) {
    CheckVectors(l, r);
    result = ComputeL2(l, r, l.size());
    return true;
  }
};

template<typename T>
using L2SquaredDouble = L2Squared<T, double>;

template<typename T>
using L2SquaredFloat = L2Squared<T, float>;

template<typename T, typename Elem>
struct L1Distance {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool callNullFree(  // NOLINT
    out_type<Elem>& result, const velox::exec::ArrayView<false, Elem>& l,
    const velox::exec::ArrayView<false, Elem>& r) {
    CheckVectors(l, r);
    result = ComputeL1(l, r, l.size());
    return true;
  }
};

template<typename T>
using L1DistanceFloat = L1Distance<T, float>;

template<typename T>
using L1DistanceDouble = L1Distance<T, double>;

template<typename T, typename Elem>
struct CosineSimilarity {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool callNullFree(  // NOLINT
    out_type<Elem>& result, const velox::exec::ArrayView<false, Elem>& l,
    const velox::exec::ArrayView<false, Elem>& r) {
    CheckVectors(l, r);
    result = ComputeCosine(l, r, l.size());
    return true;
  }
};

template<typename T>
using CosineSimilarityFloat = CosineSimilarity<T, float>;

template<typename T>
using CosineSimilarityDouble = CosineSimilarity<T, double>;

template<typename T, typename Elem>
struct DotProduct {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool callNullFree(  // NOLINT
    out_type<Elem>& result, const velox::exec::ArrayView<false, Elem>& l,
    const velox::exec::ArrayView<false, Elem>& r) {
    CheckVectors(l, r);
    result = ComputeDotProduct(l, r, l.size());
    return true;
  }
};

template<typename T>
using DotProductFloat = DotProduct<T, float>;

template<typename T>
using DotProductDouble = DotProduct<T, double>;

}  // namespace

void RegisterVectorFunctions(const std::string& prefix) {
  velox::registerFunction<L2SquaredFloat, float, velox::Array<float>,
                          velox::Array<float>>({prefix + "l2_squared"});
  velox::registerFunction<L2SquaredDouble, double, velox::Array<double>,
                          velox::Array<double>>({prefix + "l2_squared"});
  velox::registerFunction<L1DistanceFloat, float, velox::Array<float>,
                          velox::Array<float>>({prefix + "l1_distance"});
  velox::registerFunction<L1DistanceDouble, double, velox::Array<double>,
                          velox::Array<double>>({prefix + "l1_distance"});
  velox::registerFunction<CosineSimilarityFloat, float, velox::Array<float>,
                          velox::Array<float>>({prefix + "cosine_similarity"});
  velox::registerFunction<CosineSimilarityDouble, double, velox::Array<double>,
                          velox::Array<double>>({prefix + "cosine_similarity"});
  velox::registerFunction<DotProductFloat, float, velox::Array<float>,
                          velox::Array<float>>({prefix + "dot_product"});
  velox::registerFunction<DotProductDouble, double, velox::Array<double>,
                          velox::Array<double>>({prefix + "dot_product"});
}

}  // namespace sdb::pg::functions
