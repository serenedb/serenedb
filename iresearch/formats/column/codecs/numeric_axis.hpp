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

#include <cstdint>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/hugeint.hpp>
#include <duckdb/common/types/uhugeint.hpp>
#include <utility>

#include "iresearch/utils/dispatch.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs {

template<duckdb::PhysicalType K>
struct NumericOf;

template<>
struct NumericOf<duckdb::PhysicalType::INT8> {
  using Type = int8_t;
};

template<>
struct NumericOf<duckdb::PhysicalType::INT16> {
  using Type = int16_t;
};

template<>
struct NumericOf<duckdb::PhysicalType::INT32> {
  using Type = int32_t;
};

template<>
struct NumericOf<duckdb::PhysicalType::INT64> {
  using Type = int64_t;
};

template<>
struct NumericOf<duckdb::PhysicalType::UINT8> {
  using Type = uint8_t;
};

template<>
struct NumericOf<duckdb::PhysicalType::UINT16> {
  using Type = uint16_t;
};

template<>
struct NumericOf<duckdb::PhysicalType::UINT32> {
  using Type = uint32_t;
};

template<>
struct NumericOf<duckdb::PhysicalType::UINT64> {
  using Type = uint64_t;
};

template<>
struct NumericOf<duckdb::PhysicalType::INT128> {
  using Type = duckdb::hugeint_t;
};

template<>
struct NumericOf<duckdb::PhysicalType::UINT128> {
  using Type = duckdb::uhugeint_t;
};

template<>
struct NumericOf<duckdb::PhysicalType::FLOAT> {
  using Type = float;
};

template<>
struct NumericOf<duckdb::PhysicalType::DOUBLE> {
  using Type = double;
};

struct NumericAxis
  : Axis<duckdb::PhysicalType::INT8, duckdb::PhysicalType::INT16,
         duckdb::PhysicalType::INT32, duckdb::PhysicalType::INT64,
         duckdb::PhysicalType::UINT8, duckdb::PhysicalType::UINT16,
         duckdb::PhysicalType::UINT32, duckdb::PhysicalType::UINT64,
         duckdb::PhysicalType::INT128, duckdb::PhysicalType::UINT128,
         duckdb::PhysicalType::FLOAT, duckdb::PhysicalType::DOUBLE> {
  static constexpr duckdb::PhysicalType Of(
    duckdb::PhysicalType physical) noexcept {
    return physical;
  }

  template<duckdb::PhysicalType K, typename F>
  IRS_FORCE_INLINE static decltype(auto) Call(F&& f) {
    return std::forward<F>(f)
      .template operator()<typename NumericOf<K>::Type>();
  }

  template<typename F>
  IRS_FORCE_INLINE static auto Dispatch(duckdb::PhysicalType physical, F&& f)
    -> decltype(f.template operator()<duckdb::PhysicalType::INT8>()) {
    using enum duckdb::PhysicalType;
    switch (physical) {
      case BOOL:
      case INT8:
        return f.template operator()<INT8>();
      case INT16:
        return f.template operator()<INT16>();
      case INT32:
        return f.template operator()<INT32>();
      case INT64:
        return f.template operator()<INT64>();
      case UINT8:
        return f.template operator()<UINT8>();
      case UINT16:
        return f.template operator()<UINT16>();
      case UINT32:
        return f.template operator()<UINT32>();
      case UINT64:
      case LIST:
        return f.template operator()<UINT64>();
      case INT128:
        return f.template operator()<INT128>();
      case UINT128:
        return f.template operator()<UINT128>();
      case FLOAT:
        return f.template operator()<FLOAT>();
      case DOUBLE:
        return f.template operator()<DOUBLE>();
      default:
        return {};
    }
  }
};

}  // namespace irs::codecs
