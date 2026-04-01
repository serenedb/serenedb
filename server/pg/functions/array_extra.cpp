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
#include "pg/parse_array.h"
#include "pg/sql_exception_macro.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

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

// Parses a PostgreSQL array literal into ARRAY<VARCHAR>.
// The caller casts the varchar elements to the target type via CAST.
//
// Format: {elem1,"elem 2",NULL,...}  (delimiter is ',' for all standard types)
// Rules (from pg arrayfuncs.c / ReadArrayToken):
//   - Unquoted: strip leading/trailing whitespace; backslash escapes apply;
//     unescaped "NULL" (case-insensitive) -> SQL NULL.
//   - Quoted ("..."): backslash escapes apply; never treated as NULL.
template<typename T>
struct PgArrayTextIn {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Array<velox::Varchar>>& result,
                                const arg_type<velox::Varchar>& input) {
    sdb::pg::ParsePgTextArray(
      std::string_view{input.data(), input.size()},
      [&](std::string_view token, bool is_null) {
        if (is_null) {
          result.add_null();
        } else {
          result.add_item().copy_from(token);
        }
      },
      [](std::string_view msg) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        ERR_MSG(msg));
      });
  }
};

using facebook::velox::T1;

// Returns the length of the requested array dimension.
// Produces NULL for empty arrays or missing dimensions.
template<typename T>
struct ArrayLengthFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
    const std::vector<velox::TypePtr>& inputTypes,
    const velox::core::QueryConfig&,
    const arg_type<velox::Array<velox::Generic<T1>>>*,
    const arg_type<int32_t>*) {
    SDB_ASSERT(_depth == 0, "initialize should be called only once");
    auto type = inputTypes[0];
    while (type->isArray()) {
      ++_depth;
      type = type->childAt(0);
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
    out_type<int32_t>& out,
    const arg_type<velox::Array<velox::Generic<T1>>>& input,
    const arg_type<int32_t>& dim) {
    if (dim < 1 || dim > _depth || input.size() == 0) {
      return false;
    }
    if (dim == 1) {
      out = static_cast<int32_t>(input.size());
      return true;
    }
    // Walk into nested arrays via first element at each level
    auto first = input.at(0);
    if (!first.has_value()) {
      return false;
    }
    const auto* base = first->base();
    auto idx = first->decodedIndex();
    for (int32_t d = 2; d <= dim; ++d) {
      const auto* arr = base->template as<velox::ArrayVector>();
      auto size = arr->sizeAt(idx);
      if (d == dim) {
        if (size == 0) {
          return false;
        }
        out = static_cast<int32_t>(size);
        return true;
      }
      if (size == 0) {
        return false;
      }
      idx = arr->offsetAt(idx);
      base = arr->elements().get();
    }
    return false;
  }

 private:
  int32_t _depth = 0;
};

// Returns the number of dimensions of the array.
template<typename T>
struct ArrayNdimsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
    const std::vector<velox::TypePtr>& inputTypes,
    const velox::core::QueryConfig&,
    const arg_type<velox::Array<velox::Generic<T1>>>*) {
    SDB_ASSERT(_depth == 0, "initialize should be called only once");
    auto type = inputTypes[0];
    while (type->isArray()) {
      ++_depth;
      type = type->childAt(0);
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
    out_type<int32_t>& out,
    const arg_type<velox::Array<velox::Generic<T1>>>& input) {
    if (input.size() == 0) {
      return false;
    }
    out = _depth;
    return true;
  }

 private:
  int32_t _depth = 0;
};

// Returns the lower bound of the requested dimension (always 1 in PG).
template<typename T>
struct ArrayLowerFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
    const std::vector<velox::TypePtr>& inputTypes,
    const velox::core::QueryConfig&,
    const arg_type<velox::Array<velox::Generic<T1>>>*,
    const arg_type<int32_t>*) {
    SDB_ASSERT(_depth == 0, "initialize should be called only once");
    auto type = inputTypes[0];
    while (type->isArray()) {
      ++_depth;
      type = type->childAt(0);
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
    out_type<int32_t>& out,
    const arg_type<velox::Array<velox::Generic<T1>>>& input,
    const arg_type<int32_t>& dim) {
    if (dim < 1 || dim > _depth || input.size() == 0) {
      return false;
    }
    out = 1;  // PG arrays are always 1-based
    return true;
  }

 private:
  int32_t _depth = 0;
};

// Returns the upper bound (= length) of the requested dimension.
template<typename T>
struct ArrayUpperFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
    const std::vector<velox::TypePtr>& inputTypes,
    const velox::core::QueryConfig&,
    const arg_type<velox::Array<velox::Generic<T1>>>*,
    const arg_type<int32_t>*) {
    SDB_ASSERT(_depth == 0, "initialize should be called only once");
    auto type = inputTypes[0];
    while (type->isArray()) {
      ++_depth;
      type = type->childAt(0);
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
    out_type<int32_t>& out,
    const arg_type<velox::Array<velox::Generic<T1>>>& input,
    const arg_type<int32_t>& dim) {
    if (dim < 1 || dim > _depth || input.size() == 0) {
      return false;
    }
    if (dim == 1) {
      out = static_cast<int32_t>(input.size());
      return true;
    }
    // Same logic as ArrayLengthFunction for deeper dims.
    auto first = input.at(0);
    if (!first.has_value()) {
      return false;
    }
    const auto* base = first->base();
    auto idx = first->decodedIndex();
    for (int32_t d = 2; d <= dim; ++d) {
      const auto* arr = base->template as<velox::ArrayVector>();
      auto size = arr->sizeAt(idx);
      if (d == dim) {
        if (size == 0) {
          return false;
        }
        out = static_cast<int32_t>(size);
        return true;
      }
      if (size == 0) {
        return false;
      }
      idx = arr->offsetAt(idx);
      base = arr->elements().get();
    }
    return false;
  }

 private:
  int32_t _depth = 0;
};

// Returns text representation of array dimensions, e.g. "[1:3][1:2]".
template<typename T>
struct ArrayDimsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
    const std::vector<velox::TypePtr>& inputTypes,
    const velox::core::QueryConfig&,
    const arg_type<velox::Array<velox::Generic<T1>>>*) {
    SDB_ASSERT(_depth == 0, "initialize should be called only once");
    auto type = inputTypes[0];
    while (type->isArray()) {
      ++_depth;
      type = type->childAt(0);
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
    out_type<velox::Varchar>& out,
    const arg_type<velox::Array<velox::Generic<T1>>>& input) {
    if (input.size() == 0) {
      return false;
    }

    std::string result;
    // First dimension.
    result += "[1:" + std::to_string(input.size()) + "]";

    if (_depth > 1 && input.size() > 0) {
      auto first = input.at(0);
      if (first.has_value()) {
        const auto* base = first->base();
        auto idx = first->decodedIndex();
        for (int32_t d = 2; d <= _depth; ++d) {
          const auto* arr = base->template as<velox::ArrayVector>();
          auto size = arr->sizeAt(idx);
          result += "[1:" + std::to_string(size) + "]";
          if (d < _depth && size > 0) {
            idx = arr->offsetAt(idx);
            base = arr->elements().get();
          }
        }
      }
    }

    out.resize(result.size());
    std::memcpy(out.data(), result.data(), result.size());
    return true;
  }

 private:
  int32_t _depth = 0;
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
  velox::registerFunction<ArrayLengthFunction, int32_t,
                          velox::Array<velox::Generic<T1>>, int32_t>(
    {prefix + "array_length"});
  velox::registerFunction<ArrayNdimsFunction, int32_t,
                          velox::Array<velox::Generic<T1>>>(
    {prefix + "array_ndims"});
  velox::registerFunction<ArrayLowerFunction, int32_t,
                          velox::Array<velox::Generic<T1>>, int32_t>(
    {prefix + "array_lower"});
  velox::registerFunction<ArrayUpperFunction, int32_t,
                          velox::Array<velox::Generic<T1>>, int32_t>(
    {prefix + "array_upper"});
  velox::registerFunction<ArrayDimsFunction, velox::Varchar,
                          velox::Array<velox::Generic<T1>>>(
    {prefix + "array_dims"});

  // serenedb custom function
  velox::registerFunction<PgArrayTextIn, velox::Array<velox::Varchar>,
                          velox::Varchar>({prefix + "array_text_in"});
}

}  // namespace sdb::pg::functions
