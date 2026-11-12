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

#include <algorithm>
#include <cmath>
#include <limits>
#include <optional>
#include <span>

#include "iresearch/index/column_info.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {

struct ScalarVectorLimits {
  double max_component;
  double max_norm2;

  static std::optional<ScalarVectorLimits> For(VectorQuantization quant,
                                               uint32_t dimensions) {
    if (!IsScalar(quant) && !IsHalfFloat(quant)) {
      return std::nullopt;
    }
    return ScalarVectorLimits{
      quant == VectorQuantization::FP16 ? 65504.0
                                        : std::numeric_limits<float>::max(),
      std::numeric_limits<float>::max() / (16.0 * dimensions)};
  }

  void Merge(const ScalarVectorLimits& other) noexcept {
    max_component = std::min(max_component, other.max_component);
    max_norm2 = std::min(max_norm2, other.max_norm2);
  }

  void Check(double component, double norm2) const {
    if (max_component == 65504.0 &&
        (!(component <= max_component) || !std::isfinite(norm2))) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                      ERR_MSG("FP16 vector components must be finite and "
                              "between -65504 and 65504"));
    }
    if (!(norm2 <= max_norm2)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                      ERR_MSG("Vector magnitude exceeds the range supported by "
                              "scalar quantization"));
    }
  }

  void Check(std::span<const float> vector) const {
    double component = 0;
    double norm2 = 0;
    for (const double value : vector) {
      component = std::max(component, std::abs(value));
      norm2 += value * value;
    }
    Check(component, norm2);
  }
};

inline void CheckScalarQuery(std::span<const float> query) {
  ScalarVectorLimits::For(VectorQuantization::SQ8, query.size())->Check(query);
}

}  // namespace irs
