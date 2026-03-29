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

#include "pg/functions/array_extra.h"

#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <velox/type/SimpleFunctionApi.h>

#include "basics/fwd.h"

namespace sdb::pg::functions {
namespace {

// array_position(anyarray, element) -> bigint
// Returns the 1-based position of the first occurrence, or NULL if not found.
template<typename T>
struct PgArrayPosition {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
    int64_t& result, const arg_type<velox::Array<velox::Varchar>>& arr,
    const arg_type<velox::Varchar>& elem) {
    for (auto i = 0; i < arr.size(); ++i) {
      if (arr[i].has_value() && arr[i].value() == elem) {
        result = i + 1;
        return true;
      }
    }
    return false;
  }

  FOLLY_ALWAYS_INLINE bool call(int64_t& result,
                                const arg_type<velox::Array<int64_t>>& arr,
                                const int64_t& elem) {
    for (auto i = 0; i < arr.size(); ++i) {
      if (arr[i].has_value() && arr[i].value() == elem) {
        result = i + 1;
        return true;
      }
    }
    return false;
  }
};

// array_positions(anyarray, element) -> int[]
// Returns an array of the 1-based positions of all occurrences of the element.
template<typename T>
struct PgArrayPositions {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
    out_type<velox::Array<int32_t>>& result,
    const arg_type<velox::Array<velox::Varchar>>& arr,
    const arg_type<velox::Varchar>& elem) {
    for (auto i = 0; i < arr.size(); ++i) {
      if (arr[i].has_value() && arr[i].value() == elem) {
        result.add_item() = i + 1;
      }
    }
    return true;
  }

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Array<int32_t>>& result,
                                const arg_type<velox::Array<int64_t>>& arr,
                                const int64_t& elem) {
    for (auto i = 0; i < arr.size(); ++i) {
      if (arr[i].has_value() && arr[i].value() == elem) {
        result.add_item() = i + 1;
      }
    }
    return true;
  }
};

// array_replace(anyarray, from, to) -> anyarray
// Replace all occurrences of `from` with `to`.
template<typename T>
struct PgArrayReplace {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
    out_type<velox::Array<velox::Varchar>>& result,
    const arg_type<velox::Array<velox::Varchar>>& arr,
    const arg_type<velox::Varchar>& from_val,
    const arg_type<velox::Varchar>& to_val) {
    for (const auto& item : arr) {
      if (item) {
        result.add_item().copy_from(*item == from_val ? to_val : *item);
      } else {
        result.add_null();
      }
    }
    return true;
  }

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Array<int64_t>>& result,
                                const arg_type<velox::Array<int64_t>>& arr,
                                const int64_t& from_val,
                                const int64_t& to_val) {
    for (const auto& item : arr) {
      if (item) {
        result.add_item() = (*item == from_val) ? to_val : *item;
      } else {
        result.add_null();
      }
    }
    return true;
  }
};

}  // namespace

void registerArrayExtraFunctions(const std::string& prefix) {
  velox::registerFunction<PgArrayPosition, int64_t,
                          velox::Array<velox::Varchar>, velox::Varchar>(
    {prefix + "array_position"});
  velox::registerFunction<PgArrayPosition, int64_t, velox::Array<int64_t>,
                          int64_t>({prefix + "array_position"});
  velox::registerFunction<PgArrayPositions, velox::Array<int32_t>,
                          velox::Array<velox::Varchar>, velox::Varchar>(
    {prefix + "array_positions"});
  velox::registerFunction<PgArrayPositions, velox::Array<int32_t>,
                          velox::Array<int64_t>, int64_t>(
    {prefix + "array_positions"});
  velox::registerFunction<PgArrayReplace, velox::Array<velox::Varchar>,
                          velox::Array<velox::Varchar>, velox::Varchar,
                          velox::Varchar>({prefix + "array_replace"});
  velox::registerFunction<PgArrayReplace, velox::Array<int64_t>,
                          velox::Array<int64_t>, int64_t, int64_t>(
    {prefix + "array_replace"});
}

}  // namespace sdb::pg::functions
