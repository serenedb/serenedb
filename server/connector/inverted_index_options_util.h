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

#include <absl/algorithm/container.h>

#include <array>
#include <duckdb/common/types/value.hpp>
#include <limits>
#include <string_view>

#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "query/config_variable_names.h"

namespace sdb::connector {

// Numeric inverted-index options shared by CREATE INDEX ... WITH and
// ALTER INDEX ... SET/RESET. The persisted value 0 is reserved as the
// internal "unset, use the built-in default" sentinel (an omitted WITH
// option or a RESET stores it), so an explicit 0 from the user is rejected
// everywhere by ValidateInvertedIndexOptionValue.

inline constexpr std::array<std::string_view, 8> kAlterableInvertedOptions = {
  kRefreshIntervalSetting,
  kCompactionIntervalSetting,
  kCleanupIntervalStepSetting,
  kSegmentMemoryMaxSetting,
  kSegmentDocsMaxSetting,
  kCompactionMaxSegmentsSetting,
  kCompactionMaxSegmentsBytesSetting,
  kCompactionFloorSegmentBytesSetting,
};

inline constexpr std::array<std::string_view, 10> kNumericInvertedOptions = {
  kRowGroupSizeSetting,
  kNormRowGroupSizeSetting,
  kRefreshIntervalSetting,
  kCompactionIntervalSetting,
  kCleanupIntervalStepSetting,
  kSegmentMemoryMaxSetting,
  kSegmentDocsMaxSetting,
  kCompactionMaxSegmentsSetting,
  kCompactionMaxSegmentsBytesSetting,
  kCompactionFloorSegmentBytesSetting,
};

// Everything CREATE INDEX ... WITH accepts: the numeric options plus the
// create-time-only string options.
inline constexpr auto kCreateInvertedOptions = [] {
  std::array<std::string_view, kNumericInvertedOptions.size() + 2> all{};
  for (size_t i = 0; i < kNumericInvertedOptions.size(); ++i) {
    all[i] = kNumericInvertedOptions[i];
  }
  all[kNumericInvertedOptions.size()] = "optimize_top_k";
  all[kNumericInvertedOptions.size() + 1] = "store_pk";
  return all;
}();

inline bool IsUint32InvertedOption(std::string_view name) {
  constexpr std::array<std::string_view, 7> kUint32Options = {
    kRowGroupSizeSetting,          kNormRowGroupSizeSetting,
    kRefreshIntervalSetting,       kCompactionIntervalSetting,
    kCleanupIntervalStepSetting,   kSegmentDocsMaxSetting,
    kCompactionMaxSegmentsSetting,
  };
  return absl::c_contains(kUint32Options, name);
}

// Options where 0 is a real value, not a rejected degenerate: iresearch
// defines segment_docs_max 0 == unlimited, and a maintenance interval (or
// cleanup step) of 0 disables that background task -- the established idiom
// deterministic tests rely on.
inline bool IsZeroAllowedInvertedOption(std::string_view name) {
  constexpr std::array<std::string_view, 4> kZeroAllowed = {
    kSegmentDocsMaxSetting,
    kRefreshIntervalSetting,
    kCompactionIntervalSetting,
    kCleanupIntervalStepSetting,
  };
  return absl::c_contains(kZeroAllowed, name);
}

// Validates a value for a numeric inverted-index option; the one validator
// both the CREATE WITH path and the ALTER INDEX SET/RESET path go through.
inline uint64_t ValidateInvertedIndexOptionValue(std::string_view name,
                                                 const duckdb::Value& raw) {
  auto value = raw;
  if (!value.DefaultTryCastAs(duckdb::LogicalType::UBIGINT)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("invalid value for parameter \"", name, "\": \"",
                            raw.ToString(), "\""));
  }
  const auto result = value.GetValue<uint64_t>();
  if (result == 0 && !IsZeroAllowedInvertedOption(name)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("invalid value for parameter \"", name, "\": \"0\""));
  }
  if (IsUint32InvertedOption(name) &&
      result > std::numeric_limits<uint32_t>::max()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("value for option \"", name, "\" is out of range"));
  }
  return result;
}

}  // namespace sdb::connector
