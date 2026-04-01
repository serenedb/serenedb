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

#include "pg/functions/generate.h"

#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <velox/expression/DecodedArgs.h>
#include <velox/expression/FunctionMetadata.h>
#include <velox/expression/VectorFunction.h>
#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <velox/functions/prestosql/DateTimeImpl.h>
#include <velox/type/SimpleFunctionApi.h>
#include <velox/vector/ComplexVector.h>
#include <velox/vector/RangeVector.h>

#include "basics/down_cast.h"
#include "basics/fwd.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg::functions {
namespace {

class GenerateSeriesFunction final : public velox::exec::VectorFunction {
 public:
  void apply(const velox::SelectivityVector& rows,
             std::vector<velox::VectorPtr>& args,
             const velox::TypePtr& output_type, velox::exec::EvalCtx& context,
             velox::VectorPtr& result) const final {
    velox::exec::DecodedArgs decoded_args{rows, args, context};
    const auto* start_vector = decoded_args.at(0);
    const auto* stop_vector = decoded_args.at(1);
    velox::DecodedVector* step_vector = nullptr;
    if (args.size() == 3) {
      step_vector = decoded_args.at(2);
    }

    const auto num_rows = rows.end();
    auto* pool = context.pool();

    const auto sizes = velox::allocateSizes(num_rows, pool);
    const auto offsets = velox::allocateOffsets(num_rows, pool);
    auto* raw_sizes = sizes->asMutable<velox::vector_size_t>();
    auto* raw_offsets = offsets->asMutable<velox::vector_size_t>();

    std::vector<velox::RangeVector::RowMeta> metas(num_rows);
    velox::vector_size_t total_elements = 0;

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      auto start = start_vector->valueAt<int64_t>(row);
      auto stop = stop_vector->valueAt<int64_t>(row);
      int64_t step = step_vector ? step_vector->valueAt<int64_t>(row) : 1;

      if (step == 0) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("step size cannot equal zero"));
      }

      int64_t count = 0;
      if ((step > 0 && stop >= start) || (step < 0 && stop <= start)) {
        count = (stop - start) / step + 1;
      }

      raw_offsets[row] = total_elements;
      raw_sizes[row] = static_cast<velox::vector_size_t>(count);
      metas[row] = {start, step};
      total_elements += raw_sizes[row];
    });

    auto range_vector = std::make_shared<velox::RangeVector>(
      pool, output_type, nullptr, num_rows, std::move(offsets),
      std::move(sizes), std::move(metas));

    context.moveOrCopyResult(std::move(range_vector), rows, result);
  }
};

class GenerateSubscriptsFunction final : public velox::exec::VectorFunction {
 public:
  void apply(const velox::SelectivityVector& rows,
             std::vector<velox::VectorPtr>& args,
             const velox::TypePtr& output_type, velox::exec::EvalCtx& context,
             velox::VectorPtr& result) const final {
    velox::exec::DecodedArgs decoded_args{rows, args, context};
    const auto* array_vector = decoded_args.at(0);
    const auto* dim_vector = decoded_args.at(1);
    velox::DecodedVector* reverse_vector = nullptr;
    if (args.size() == 3) {
      reverse_vector = decoded_args.at(2);
    }

    const auto num_rows = rows.end();
    auto* pool = context.pool();

    const auto sizes = velox::allocateSizes(num_rows, pool);
    const auto offsets = velox::allocateOffsets(num_rows, pool);
    auto* raw_sizes = sizes->asMutable<velox::vector_size_t>();
    auto* raw_offsets = offsets->asMutable<velox::vector_size_t>();

    std::vector<velox::RangeVector::RowMeta> metas(num_rows);
    velox::vector_size_t total_elements = 0;

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      const auto dim = dim_vector->valueAt<int64_t>(row);
      const bool reverse = reverse_vector && reverse_vector->valueAt<bool>(row);

      int64_t count = 0;
      if (dim >= 1 && !array_vector->isNullAt(row)) {
        auto& base_array =
          basics::downCast<velox::ArrayVector>(*array_vector->base());
        auto* cur = &base_array;
        auto idx = array_vector->index(row);
        bool valid = true;

        for (int64_t d = 1; d < dim; ++d) {
          if (cur->sizeAt(idx) == 0) {
            valid = false;
            break;
          }
          auto* inner =
            dynamic_cast<velox::ArrayVector*>(cur->elements().get());
          if (!inner) {
            valid = false;
            break;
          }
          idx = cur->offsetAt(idx);
          cur = inner;
        }

        if (valid) {
          count = cur->sizeAt(idx);
        }
      }

      raw_offsets[row] = total_elements;
      raw_sizes[row] = static_cast<velox::vector_size_t>(count);
      if (reverse) {
        metas[row] = {count, -1};
      } else {
        metas[row] = {1, 1};
      }
      total_elements += raw_sizes[row];
    });

    auto range_vector = std::make_shared<velox::RangeVector>(
      pool, output_type, nullptr, num_rows, std::move(offsets),
      std::move(sizes), std::move(metas));

    context.moveOrCopyResult(std::move(range_vector), rows, result);
  }
};

}  // namespace

void registerGenerateFunctions(const std::string& prefix) {
  velox::exec::registerVectorFunction(
    prefix + "generate_series",
    {
      velox::exec::FunctionSignatureBuilder()
        .returnType("array(bigint)")
        .argumentType("bigint")
        .argumentType("bigint")
        .build(),
      velox::exec::FunctionSignatureBuilder()
        .returnType("array(bigint)")
        .argumentType("bigint")
        .argumentType("bigint")
        .argumentType("bigint")
        .build(),
    },
    std::make_unique<GenerateSeriesFunction>(),
    velox::exec::VectorFunctionMetadataBuilder().deterministic(false).build());

  velox::exec::registerVectorFunction(
    prefix + "generate_subscripts",
    {
      velox::exec::FunctionSignatureBuilder()
        .typeVariable("T")
        .returnType("array(bigint)")
        .argumentType("array(T)")
        .argumentType("bigint")
        .build(),
      velox::exec::FunctionSignatureBuilder()
        .typeVariable("T")
        .returnType("array(bigint)")
        .argumentType("array(T)")
        .argumentType("bigint")
        .argumentType("boolean")
        .build(),
    },
    std::make_unique<GenerateSubscriptsFunction>(),
    velox::exec::VectorFunctionMetadataBuilder().deterministic(false).build());
}

}  // namespace sdb::pg::functions
