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
#include <velox/functions/prestosql/DistanceFunctions.h>

#include "faiss/utils/distances.h"
#include "pg/sql_exception_macro.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {

template<typename T>
struct L1Distance {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(  // NOLINT
    out_type<float>& result,
    const arg_type<velox::exec::ArrayView<false, float>>& left,
    const arg_type<velox::exec::ArrayView<false, float>>& right) {
    if (left.size() != right.size()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("Cannot evaluate distance function on vectors "
                              "with different dimensions"));
    }
    size_t d = left.size();
    if (d == 0) {
      result = std::numeric_limits<float>::quiet_NaN();
      return;
    }
    std::vector<float> left_buffer, right_buffer;
    const float* left_data = velox::functions::getArrayDataOrCopy(left, left_buffer);
    const float* right_data = velox::functions::getArrayDataOrCopy(right, right_buffer);
    const auto* left_data = left.data();
    const auto* right_data = right.data();
    auto res = faiss::fvec_L1(left_data, right_data, left.size());
    result = res;
  }
};
}  // namespace sdb::pg
