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
#include <duckdb/common/types/vector.hpp>
#include <duckdb/function/cast/cast_function_set.hpp>
#include <iresearch/utils/pg/sql_error.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "pg/types/function.h"

namespace sdb::pg {

inline constexpr std::string_view kInt4rangeAlias = "int4range";
inline constexpr std::string_view kInt8rangeAlias = "int8range";
inline constexpr std::string_view kNumrangeAlias = "numrange";
inline constexpr std::string_view kDaterangeAlias = "daterange";
inline constexpr std::string_view kTsrangeAlias = "tsrange";
inline constexpr std::string_view kTstzrangeAlias = "tstzrange";

inline constexpr std::string_view kInt4multirangeAlias = "int4multirange";
inline constexpr std::string_view kInt8multirangeAlias = "int8multirange";
inline constexpr std::string_view kNummultirangeAlias = "nummultirange";
inline constexpr std::string_view kDatemultirangeAlias = "datemultirange";
inline constexpr std::string_view kTsmultirangeAlias = "tsmultirange";
inline constexpr std::string_view kTstzmultirangeAlias = "tstzmultirange";

inline constexpr duckdb::idx_t kRangeNonemptyChild = 0;
inline constexpr duckdb::idx_t kRangeLowerFiniteChild = 1;
inline constexpr duckdb::idx_t kRangeLowerChild = 2;
inline constexpr duckdb::idx_t kRangeLowerExclusiveChild = 3;
inline constexpr duckdb::idx_t kRangeUpperInfiniteChild = 4;
inline constexpr duckdb::idx_t kRangeUpperChild = 5;
inline constexpr duckdb::idx_t kRangeUpperInclusiveChild = 6;

inline constexpr uint8_t kRangeEmpty = 0x01;
inline constexpr uint8_t kRangeLowerInclusive = 0x02;
inline constexpr uint8_t kRangeUpperInclusive = 0x04;
inline constexpr uint8_t kRangeLowerInfinite = 0x08;
inline constexpr uint8_t kRangeUpperInfinite = 0x10;

duckdb::LogicalType INT4RANGE();
duckdb::LogicalType INT8RANGE();
duckdb::LogicalType NUMRANGE();
duckdb::LogicalType DATERANGE();
duckdb::LogicalType TSRANGE();
duckdb::LogicalType TSTZRANGE();

duckdb::LogicalType INT4MULTIRANGE();
duckdb::LogicalType INT8MULTIRANGE();
duckdb::LogicalType NUMMULTIRANGE();
duckdb::LogicalType DATEMULTIRANGE();
duckdb::LogicalType TSMULTIRANGE();
duckdb::LogicalType TSTZMULTIRANGE();

bool IsRange(const duckdb::LogicalType& type);
bool IsMultirange(const duckdb::LogicalType& type);

uint8_t ReadRangeFlags(const duckdb::RecursiveUnifiedVectorFormat& range,
                       duckdb::idx_t row);

struct ParsedRange {
  uint8_t flags;
  std::string lower;
  std::string upper;
};

std::optional<irs::pg::SqlErrorData> ParseRange(std::string_view text,
                                                ParsedRange& range);

bool FinishRange(duckdb::Vector& range, duckdb::idx_t row, uint8_t flags);

std::optional<irs::pg::SqlErrorData> SplitMultirange(
  std::string_view text, std::vector<std::string_view>& ranges);

void FinishMultirange(duckdb::Vector& multirange, duckdb::idx_t row);

void RegisterRange(duckdb::CastFunctionSet& casts, Functions& functions);

}  // namespace sdb::pg
