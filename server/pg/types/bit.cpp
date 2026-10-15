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

#include "pg/types/bit.h"

#include <algorithm>
#include <array>
#include <duckdb/common/types/bit.hpp>
#include <duckdb/common/vector_operations/binary_executor.hpp>
#include <duckdb/common/vector_operations/generic_executor.hpp>
#include <duckdb/common/vector_operations/ternary_executor.hpp>
#include <duckdb/function/scalar/string_common.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <span>

namespace sdb::pg {
namespace {

struct BitSlice {
  duckdb::string_t bits;
  duckdb::idx_t begin;
  duckdb::idx_t end;
};

BitSlice Whole(duckdb::string_t bits) {
  return {bits, 0, duckdb::Bit::BitLength(bits)};
}

duckdb::string_t WriteBits(duckdb::Vector& result,
                           std::span<const BitSlice> slices) {
  duckdb::idx_t length = 0;
  for (const auto& slice : slices) {
    length += slice.end - slice.begin;
  }
  if (length == 0) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("empty bit strings are not supported"));
  }
  auto bits = duckdb::StringVector::EmptyString(
    result, duckdb::Bit::ComputeBitstringLen(length));
  duckdb::Bit::SetEmptyBitString(bits, length);
  duckdb::idx_t position = 0;
  for (const auto& slice : slices) {
    for (auto i = slice.begin; i < slice.end; ++i) {
      if (duckdb::Bit::GetBit(slice.bits, i)) {
        duckdb::Bit::SetBit(bits, position, 1);
      }
      ++position;
    }
  }
  return bits;
}

duckdb::string_t Substring(duckdb::Vector& result, duckdb::string_t bits,
                           int64_t offset, int64_t length) {
  duckdb::PostgresSubstringBounds(offset, length);
  const auto size = duckdb::Bit::BitLength(bits);
  const auto begin = std::min<uint64_t>(offset - 1, size);
  const std::array slices{
    BitSlice{bits, begin, begin + std::min<uint64_t>(length, size - begin)}};
  return WriteBits(result, slices);
}

void SubstringFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                       duckdb::Vector& result) {
  if (args.ColumnCount() == 3) {
    duckdb::TernaryExecutor::Execute<duckdb::string_t, int64_t, int64_t,
                                     duckdb::string_t>(
      args.data[0], args.data[1], args.data[2], result, args.size(),
      [&](duckdb::string_t bits, int64_t offset, int64_t length) {
        return Substring(result, bits, offset, length);
      });
    return;
  }
  duckdb::BinaryExecutor::Execute<duckdb::string_t, int64_t, duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t bits, int64_t offset) {
      return Substring(result, bits, std::max<int64_t>(offset, 1),
                       duckdb::Bit::BitLength(bits));
    });
}

duckdb::string_t Overlay(duckdb::Vector& result, duckdb::string_t bits,
                         duckdb::string_t placing, int64_t start,
                         int64_t count) {
  const auto tail = duckdb::PostgresOverlayEnd(start, count);
  const auto size = duckdb::Bit::BitLength(bits);
  const std::array slices{
    BitSlice{bits, 0, std::min<uint64_t>(start - 1, size)},
    Whole(placing),
    BitSlice{bits,
             static_cast<duckdb::idx_t>(
               std::clamp<int64_t>(tail - 1, 0, static_cast<int64_t>(size))),
             size},
  };
  return WriteBits(result, slices);
}

void OverlayFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                     duckdb::Vector& result) {
  if (args.ColumnCount() == 4) {
    duckdb::GenericExecutor::ExecuteQuaternary<
      duckdb::PrimitiveType<duckdb::string_t>,
      duckdb::PrimitiveType<duckdb::string_t>, duckdb::PrimitiveType<int64_t>,
      duckdb::PrimitiveType<int64_t>, duckdb::PrimitiveType<duckdb::string_t>>(
      args.data[0], args.data[1], args.data[2], args.data[3], result,
      args.size(),
      [&](duckdb::PrimitiveType<duckdb::string_t> bits,
          duckdb::PrimitiveType<duckdb::string_t> placing,
          duckdb::PrimitiveType<int64_t> start,
          duckdb::PrimitiveType<int64_t> count)
        -> duckdb::PrimitiveType<duckdb::string_t> {
        return {Overlay(result, bits.val, placing.val, start.val, count.val)};
      });
    return;
  }
  duckdb::TernaryExecutor::Execute<duckdb::string_t, duckdb::string_t, int64_t,
                                   duckdb::string_t>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t bits, duckdb::string_t placing, int64_t start) {
      return Overlay(result, bits, placing, start,
                     duckdb::Bit::BitLength(placing));
    });
}

void PositionFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                      duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t, int32_t>(
    args.data[0], args.data[1], result, args.size(),
    [](duckdb::string_t bits, duckdb::string_t substring) {
      return static_cast<int32_t>(duckdb::Bit::BitPosition(substring, bits));
    });
}

void ConcatFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                    duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                  duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t lhs, duckdb::string_t rhs) {
      const std::array slices{Whole(lhs), Whole(rhs)};
      return WriteBits(result, slices);
    });
}

}  // namespace

void RegisterBit(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  const auto bit = duckdb::LogicalType::BIT;
  const auto bigint = duckdb::LogicalType::BIGINT;
  for (auto arguments :
       {duckdb::vector<duckdb::LogicalType>{bit, bigint},
        duckdb::vector<duckdb::LogicalType>{bit, bigint, bigint}}) {
    duckdb::ScalarFunction substring{"substring", std::move(arguments), bit,
                                     SubstringFunction};
    substring.SetFallible();
    loader.AddFunctionOverload(std::move(substring));
  }
  for (auto arguments :
       {duckdb::vector<duckdb::LogicalType>{bit, bit, bigint},
        duckdb::vector<duckdb::LogicalType>{bit, bit, bigint, bigint}}) {
    duckdb::ScalarFunction overlay{"overlay", std::move(arguments), bit,
                                   OverlayFunction};
    overlay.SetFallible();
    loader.AddFunctionOverload(std::move(overlay));
  }
  loader.AddFunctionOverload(duckdb::ScalarFunction{
    "position", {bit, bit}, duckdb::LogicalType::INTEGER, PositionFunction});
  loader.AddFunctionOverload(
    duckdb::ScalarFunction{"||", {bit, bit}, bit, ConcatFunction});
}

}  // namespace sdb::pg
