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

using namespace velox;

// array_positions(anyarray, anyelement) -> integer[]
// Returns an array of the 1-based positions of all occurrences of the element.
template<typename T>
struct PgArrayPositions {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<Array<int32_t>>& result,
                                const arg_type<Array<Varchar>>& arr,
                                const arg_type<Varchar>& elem) {
    for (auto i = 0; i < arr.size(); ++i) {
      if (arr[i].has_value() && arr[i].value() == elem) {
        result.add_item() = i + 1;
      }
    }
    return true;
  }
};

// int64 overload
template<typename T>
struct PgArrayPositionsInt {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<Array<int32_t>>& result,
                                const arg_type<Array<int64_t>>& arr,
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

  FOLLY_ALWAYS_INLINE bool call(out_type<Array<Varchar>>& result,
                                const arg_type<Array<Varchar>>& arr,
                                const arg_type<Varchar>& from_val,
                                const arg_type<Varchar>& to_val) {
    for (auto i = 0; i < arr.size(); ++i) {
      if (arr[i].has_value()) {
        if (arr[i].value() == from_val) {
          result.add_item().copy_from(to_val);
        } else {
          result.add_item().copy_from(arr[i].value());
        }
      } else {
        result.add_null();
      }
    }
    return true;
  }
};

template<typename T>
struct PgArrayReplaceInt {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<Array<int64_t>>& result,
                                const arg_type<Array<int64_t>>& arr,
                                const int64_t& from_val,
                                const int64_t& to_val) {
    for (auto i = 0; i < arr.size(); ++i) {
      if (arr[i].has_value()) {
        result.add_item() =
          (arr[i].value() == from_val) ? to_val : arr[i].value();
      } else {
        result.add_null();
      }
    }
    return true;
  }
};

// array_cat(anyarray, anyarray) -> anyarray
// Concatenate two arrays.
template<typename T>
struct PgArrayCat {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<Array<Varchar>>& result,
                                const arg_type<Array<Varchar>>& a,
                                const arg_type<Array<Varchar>>& b) {
    for (auto i = 0; i < a.size(); ++i) {
      if (a[i].has_value()) {
        result.add_item().copy_from(a[i].value());
      } else {
        result.add_null();
      }
    }
    for (auto i = 0; i < b.size(); ++i) {
      if (b[i].has_value()) {
        result.add_item().copy_from(b[i].value());
      } else {
        result.add_null();
      }
    }
    return true;
  }
};

template<typename T>
struct PgArrayCatInt {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<Array<int64_t>>& result,
                                const arg_type<Array<int64_t>>& a,
                                const arg_type<Array<int64_t>>& b) {
    for (auto i = 0; i < a.size(); ++i) {
      if (a[i].has_value()) {
        result.add_item() = a[i].value();
      } else {
        result.add_null();
      }
    }
    for (auto i = 0; i < b.size(); ++i) {
      if (b[i].has_value()) {
        result.add_item() = b[i].value();
      } else {
        result.add_null();
      }
    }
    return true;
  }
};

// array_prepend(element, anyarray) -> anyarray
template<typename T>
struct PgArrayPrepend {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<Array<Varchar>>& result,
                                const arg_type<Varchar>& elem,
                                const arg_type<Array<Varchar>>& arr) {
    result.add_item().copy_from(elem);
    for (auto i = 0; i < arr.size(); ++i) {
      if (arr[i].has_value()) {
        result.add_item().copy_from(arr[i].value());
      } else {
        result.add_null();
      }
    }
    return true;
  }
};

template<typename T>
struct PgArrayPrependInt {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<Array<int64_t>>& result,
                                const int64_t& elem,
                                const arg_type<Array<int64_t>>& arr) {
    result.add_item() = elem;
    for (auto i = 0; i < arr.size(); ++i) {
      if (arr[i].has_value()) {
        result.add_item() = arr[i].value();
      } else {
        result.add_null();
      }
    }
    return true;
  }
};

}  // namespace

void registerArrayExtraFunctions(const std::string& prefix) {
  registerFunction<PgArrayPositions, Array<int32_t>, Array<Varchar>, Varchar>(
    {prefix + "array_positions"});
  registerFunction<PgArrayPositionsInt, Array<int32_t>, Array<int64_t>,
                   int64_t>({prefix + "array_positions"});
  registerFunction<PgArrayReplace, Array<Varchar>, Array<Varchar>, Varchar,
                   Varchar>({prefix + "array_replace"});
  registerFunction<PgArrayReplaceInt, Array<int64_t>, Array<int64_t>, int64_t,
                   int64_t>({prefix + "array_replace"});
  registerFunction<PgArrayCat, Array<Varchar>, Array<Varchar>, Array<Varchar>>(
    {prefix + "array_cat"});
  registerFunction<PgArrayCatInt, Array<int64_t>, Array<int64_t>,
                   Array<int64_t>>({prefix + "array_cat"});
  registerFunction<PgArrayPrepend, Array<Varchar>, Varchar, Array<Varchar>>(
    {prefix + "array_prepend"});
  registerFunction<PgArrayPrependInt, Array<int64_t>, int64_t, Array<int64_t>>(
    {prefix + "array_prepend"});
}

}  // namespace sdb::pg::functions
