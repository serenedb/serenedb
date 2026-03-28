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

#pragma once

#include <velox/expression/ComplexViewTypes.h>
#include <velox/functions/Macros.h>
#include <velox/type/SimpleFunctionApi.h>

#include <cmath>
#include <iresearch/utils/vector.hpp>
#include <vector>

#include "basics/fwd.h"

namespace sdb::pg {
namespace {

// From velox
template<typename T>
const T* GetArrayDataOrCopy(
  const facebook::velox::exec::ArrayView<false, T>& array,
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

}  // namespace

// Returns the squared L2 (Euclidean) distance between two float arrays.
template<typename T>
struct L2Squared {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool callNullFree(  // NOLINT
    out_type<float>& result, const velox::exec::ArrayView<false, float>& l,
    const velox::exec::ArrayView<false, float>& r) {
    if (l.size() != r.size() || l.size() == 0) {
      return false;
    }
    std::vector<float> lbuf, rbuf;
    const auto* l_data = GetArrayDataOrCopy(l, lbuf);
    const auto* r_data = GetArrayDataOrCopy(r, rbuf);
    result = irs::vector::L2Space<float, float, float>::Dist(
      reinterpret_cast<const irs::byte_type*>(l_data),
      reinterpret_cast<const irs::byte_type*>(r_data),
      static_cast<uint16_t>(l.size()));
    return true;
  }
};

// Returns the L1 (Manhattan) distance between two float arrays.
template<typename T>
struct L1Distance {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool callNullFree(  // NOLINT
    out_type<float>& result, const velox::exec::ArrayView<false, float>& l,
    const velox::exec::ArrayView<false, float>& r) {
    if (l.size() != r.size() || l.size() == 0) {
      return false;
    }
    std::vector<float> lbuf, rbuf;
    const auto* l_data = GetArrayDataOrCopy(l, lbuf);
    const auto* r_data = GetArrayDataOrCopy(r, rbuf);
    result = irs::vector::L1Space<float, float, float>::Dist(
      reinterpret_cast<const irs::byte_type*>(l_data),
      reinterpret_cast<const irs::byte_type*>(r_data),
      static_cast<uint16_t>(l.size()));
    return true;
  }
};

// Returns the cosine similarity between two float arrays.
// Result is in [-1, 1]: 1 means identical direction, 0 orthogonal, -1 opposite.
template<typename T>
struct CosineSimilarity {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool callNullFree(  // NOLINT
    out_type<float>& result, const velox::exec::ArrayView<false, float>& l,
    const velox::exec::ArrayView<false, float>& r) {
    if (l.size() != r.size() || l.size() == 0) {
      return false;
    }
    std::vector<float> lbuf, rbuf;
    const auto* l_data = GetArrayDataOrCopy(l, lbuf);
    const auto* r_data = GetArrayDataOrCopy(r, rbuf);
    const auto [ll, lr, rr] =
      irs::vector::CosineDistanceImpl<float, float, double>::Compute(
        reinterpret_cast<const irs::byte_type*>(l_data),
        reinterpret_cast<const irs::byte_type*>(r_data),
        static_cast<uint16_t>(l.size()));
    const float denom = std::sqrtf(ll * rr);
    if (denom == 0.0) {
      return false;
    }
    result = lr / denom;
    return true;
  }
};

}  // namespace sdb::pg
