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

#include "pg/catalog/functions/format_type.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>

#include "pg/types.h"

namespace sdb::pg {

using enum PgTypeOID;

const IntervalRange* FindIntervalRange(int32_t typmod) {
  const auto range = (typmod >> 16) & kIntervalFullRange;
  const auto it = absl::c_find_if(
    kIntervalRanges, [&](const IntervalRange& r) { return r.mask == range; });
  return it == kIntervalRanges.end() ? nullptr : &*it;
}

std::optional<int32_t> CharMaxLength(int64_t typid, int32_t typmod) {
  if (typmod == -1) {
    return std::nullopt;
  }
  if (typid == kBpchar || typid == kVarchar) {
    return typmod - kVarHdrSz;
  }
  if (typid == kBit || typid == kVarbit) {
    return typmod;
  }
  return std::nullopt;
}

std::optional<int32_t> CharOctetLength(int64_t typid, int32_t typmod) {
  if (typid != kText && typid != kBpchar && typid != kVarchar) {
    return std::nullopt;
  }
  if (typmod == -1) {
    return int32_t{1} << 30;
  }
  const auto length = CharMaxLength(typid, typmod);
  if (!length) {
    return std::nullopt;
  }
  return *length * 4;
}

std::optional<int32_t> NumericPrecision(int64_t typid, int32_t typmod) {
  if (typid == kInt2) {
    return 16;
  }
  if (typid == kInt4) {
    return 32;
  }
  if (typid == kInt8) {
    return 64;
  }
  if (typid == kFloat4) {
    return 24;
  }
  if (typid == kFloat8) {
    return 53;
  }
  if (typid == kNumeric && typmod != -1) {
    return NumericTypmodPrecision(typmod);
  }
  return std::nullopt;
}

std::optional<int32_t> NumericPrecisionRadix(int64_t typid, int32_t) {
  if (typid == kInt2 || typid == kInt4 || typid == kInt8 || typid == kFloat4 ||
      typid == kFloat8) {
    return 2;
  }
  if (typid == kNumeric) {
    return 10;
  }
  return std::nullopt;
}

std::optional<int32_t> NumericScale(int64_t typid, int32_t typmod) {
  if (typid == kInt2 || typid == kInt4 || typid == kInt8) {
    return 0;
  }
  if (typid == kNumeric && typmod != -1) {
    return (typmod - kVarHdrSz) & 0xFFFF;
  }
  return std::nullopt;
}

std::optional<int32_t> DatetimePrecision(int64_t typid, int32_t typmod) {
  if (typid == kDate) {
    return 0;
  }
  if (typid == kTime || typid == kTimestamp || typid == kTimestamptz ||
      typid == kTimetz) {
    return typmod < 0 ? 6 : typmod;
  }
  if (typid == kInterval) {
    return typmod < 0 ||
               (typmod & kIntervalFullPrecision) == kIntervalFullPrecision
             ? 6
             : typmod & kIntervalFullPrecision;
  }
  return std::nullopt;
}

std::optional<std::string> IntervalType(int64_t typid, int32_t typmod) {
  if (typid != kInterval || typmod < 0) {
    return std::nullopt;
  }
  const auto* range = FindIntervalRange(typmod);
  if (!range) {
    return std::nullopt;
  }
  auto fields = absl::AsciiStrToUpper(range->fields);
  const auto precision = typmod & kIntervalFullPrecision;
  if (precision == kIntervalFullPrecision) {
    return fields;
  }
  return absl::StrCat(fields, "(", precision, ")");
}

}  // namespace sdb::pg
