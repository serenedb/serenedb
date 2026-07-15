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

#include "pg/deserialize.h"

#include <absl/base/internal/endian.h>
#include <absl/numeric/int128.h>
#include <absl/strings/escaping.h>
#include <absl/strings/match.h>
#include <fast_float/fast_float.h>

#include <cstdint>
#include <duckdb/common/operator/cast_operators.hpp>
#include <duckdb/common/operator/decimal_cast_operators.hpp>
#include <duckdb/common/types/bit.hpp>
#include <duckdb/common/types/date.hpp>
#include <duckdb/common/types/decimal.hpp>
#include <duckdb/common/types/interval.hpp>
#include <duckdb/common/types/time.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/types/uuid.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/map_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/function/cast/default_casts.hpp>
#include <duckdb/inet/inet_ipaddress.hpp>
#include <duckdb/main/client_context.hpp>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "connector/pg_logical_types.h"
#include "icu-datefunc.hpp"
#include "icu-helpers.hpp"
#include "pg/pg_types.h"

namespace sdb::pg {
namespace {

// Decode cores are the deserialize twin of the serialize framework's leaf
// cores: each is a struct exposing
//   template<typename Sink> static bool Decode(DeserializeContext&,
//                                              std::string_view, Sink&)
// that reads one wire field into the sink and returns false on malformed input.
// SelectDecoder picks the binary or text core by wire format -- the twin of
// serialize's SelectFieldSerializer.

// ===========================================================================
// Binary leaf cores
// ===========================================================================

// Fixed-width big-endian load with an optional narrowing/widening cast: the
// int/float family. Wire is the on-wire width (PG sends the small/unsigned
// types in the next-larger signed slot); Logical is what the column stores.
template<typename Logical, typename Wire = Logical>
struct LoadBin {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    if (data.size() != sizeof(Wire)) {
      return false;
    }
    sink.Fixed(static_cast<Logical>(absl::big_endian::Load<Wire>(data.data())));
    return true;
  }
};

struct BoolBin {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    if (data.size() != 1) {
      return false;
    }
    sink.Fixed(data[0] != 0);
    return true;
  }
};

struct VarcharBin {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    sink.Varchar(data);
    return true;
  }
};

struct BlobBin {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    sink.Blob(
      duckdb::string_t{data.data(), static_cast<uint32_t>(data.size())});
    return true;
  }
};

int64_t TimestampMicrosFromWire(int64_t us) {
  if (us == std::numeric_limits<int64_t>::max()) {
    return us;
  }
  if (us == std::numeric_limits<int64_t>::min()) {
    return -std::numeric_limits<int64_t>::max();
  }
  return us + kGapUs;
}

int64_t TimestampSecondsFromWire(int64_t us) {
  if (us == std::numeric_limits<int64_t>::max()) {
    return us;
  }
  if (us == std::numeric_limits<int64_t>::min()) {
    return -std::numeric_limits<int64_t>::max();
  }
  return us / 1'000'000 + kGapSec;
}

int64_t TimestampMillisFromWire(int64_t us) {
  if (us == std::numeric_limits<int64_t>::max()) {
    return us;
  }
  if (us == std::numeric_limits<int64_t>::min()) {
    return -std::numeric_limits<int64_t>::max();
  }
  return us / 1000 + kGapMs;
}

int64_t TimestampNanosFromWire(int64_t us) {
  if (us == std::numeric_limits<int64_t>::max()) {
    return us;
  }
  if (us == std::numeric_limits<int64_t>::min()) {
    return -std::numeric_limits<int64_t>::max();
  }
  return (us + kGapUs) * 1000;
}

struct TimestampBin {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    if (data.size() != 8) {
      return false;
    }
    const auto us = absl::big_endian::Load<int64_t>(data.data());
    sink.Fixed(duckdb::timestamp_t{TimestampMicrosFromWire(us)});
    return true;
  }
};

struct TimestampTzBin {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    if (data.size() != 8) {
      return false;
    }
    const auto us = absl::big_endian::Load<int64_t>(data.data());
    sink.Fixed(duckdb::timestamp_tz_t{TimestampMicrosFromWire(us)});
    return true;
  }
};

struct TimeBin {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    if (data.size() != 8) {
      return false;
    }
    const auto us = absl::big_endian::Load<int64_t>(data.data());
    sink.Fixed(duckdb::dtime_t{us});
    return true;
  }
};

struct TimeTzBin {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    if (data.size() != 12) {
      return false;
    }
    const auto us = absl::big_endian::Load<int64_t>(data.data());
    // PG zone is seconds WEST; DuckDB offset() is seconds EAST -- negate.
    const auto pg_zone = absl::big_endian::Load<int32_t>(data.data() + 8);
    sink.Fixed(duckdb::dtime_tz_t{duckdb::dtime_t{us}, -pg_zone});
    return true;
  }
};

struct DateBin {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    if (data.size() != 4) {
      return false;
    }
    const auto days = absl::big_endian::Load<int32_t>(data.data());
    duckdb::date_t result;
    if (days == std::numeric_limits<int32_t>::max()) {
      result = duckdb::date_t::infinity();
    } else if (days == std::numeric_limits<int32_t>::min()) {
      result = duckdb::date_t::ninfinity();
    } else {
      result = duckdb::date_t(days + kGapDays);
    }
    sink.Fixed(result);
    return true;
  }
};

// Scaled timestamp/time variants normalize to PG micros on the wire, so reverse
// the serializer's scaling. Text reuses DuckDB's string casts, which parse the
// column's exact precision (the generic varchar cast would lose sub-second).
struct TimestampSecBin {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    if (data.size() != 8) {
      return false;
    }
    const auto us = absl::big_endian::Load<int64_t>(data.data());
    sink.Fixed(duckdb::timestamp_sec_t(TimestampSecondsFromWire(us)));
    return true;
  }
};

struct TimestampMsBin {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    if (data.size() != 8) {
      return false;
    }
    const auto us = absl::big_endian::Load<int64_t>(data.data());
    sink.Fixed(duckdb::timestamp_ms_t(TimestampMillisFromWire(us)));
    return true;
  }
};

struct TimestampNsBin {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    if (data.size() != 8) {
      return false;
    }
    const auto us = absl::big_endian::Load<int64_t>(data.data());
    sink.Fixed(duckdb::timestamp_ns_t(TimestampNanosFromWire(us)));
    return true;
  }
};

struct TimestampTzNsBin {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    if (data.size() != 8) {
      return false;
    }
    const auto us = absl::big_endian::Load<int64_t>(data.data());
    sink.Fixed(duckdb::timestamp_tz_ns_t(TimestampNanosFromWire(us)));
    return true;
  }
};

struct TimeNsBin {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    if (data.size() != 8) {
      return false;
    }
    const auto us = absl::big_endian::Load<int64_t>(data.data());
    sink.Fixed(duckdb::dtime_ns_t(us * 1000));
    return true;
  }
};

struct UuidBin {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    if (data.size() != 16) {
      return false;
    }
    duckdb::hugeint_t val;
    const uint64_t raw_high = absl::big_endian::Load64(data.data());
    val.upper = static_cast<int64_t>(raw_high ^ (uint64_t{1} << 63));
    val.lower = absl::big_endian::Load64(data.data() + 8);
    sink.Uuid(val);
    return true;
  }
};

struct IntervalBin {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    if (data.size() != 16) {
      return false;
    }
    duckdb::interval_t interval;
    interval.micros =
      static_cast<int64_t>(absl::big_endian::Load64(data.data()));
    interval.days =
      static_cast<int32_t>(absl::big_endian::Load32(data.data() + 8));
    interval.months =
      static_cast<int32_t>(absl::big_endian::Load32(data.data() + 12));
    sink.Fixed(interval);
    return true;
  }
};

// PG NUMERIC binary -> an unsigned magnitude (a base-10 integer scaled by
// `scale`) plus a sign flag; the inverse of WriteAsNumericBinary. The wire is
// base-10000 digit groups, most significant first, at power-of-10000 `weight`.
// uint128 holds the full range -- a signed int128 cannot represent 2^127, the
// magnitude of HUGEINT's minimum. false on NaN or malformed.
bool PgNumericMagnitude(std::string_view data, int32_t scale, bool& negative,
                        absl::uint128& magnitude) {
  if (data.size() < 8) {
    return false;
  }
  const auto ndigits =
    static_cast<int16_t>(absl::big_endian::Load16(data.data()));
  const auto weight =
    static_cast<int16_t>(absl::big_endian::Load16(data.data() + 2));
  const uint16_t sign = absl::big_endian::Load16(data.data() + 4);
  if (sign == 0xC000) {  // NaN has no integer/decimal representation
    return false;
  }
  negative = sign == 0x4000;
  if (ndigits < 0 || data.size() < static_cast<size_t>(8 + ndigits * 2)) {
    return false;
  }
  // Accumulate value*10^scale MSB-first, taking only the decimal digits at
  // exponent >= -scale; the running total never exceeds the final magnitude
  // (concat-then-divide would overflow uint128 on a full-precision
  // DECIMAL(38)). The first digit below -scale feeds half-away-from-zero
  // rounding.
  static constexpr int kPow10[] = {1, 10, 100, 1000};
  absl::uint128 acc = 0;
  int round_digit = 0;
  bool have_round = false;
  for (int16_t i = 0; i < ndigits; ++i) {
    const uint16_t group = absl::big_endian::Load16(data.data() + 8 + i * 2);
    for (int k = 3; k >= 0; --k) {
      const int target = 4 * (weight - i) + k + scale;
      if (target >= 0) {
        acc = acc * 10 + (group / kPow10[k]) % 10;
      } else if (target == -1 && !have_round) {
        round_digit = (group / kPow10[k]) % 10;
        have_round = true;
      }
    }
  }
  // Trailing zeros when the least significant group digit sits above -scale.
  for (int z = 4 * (weight - ndigits + 1) + scale; z > 0; --z) {
    acc *= 10;
  }
  if (have_round && round_digit >= 5) {
    ++acc;
  }
  magnitude = acc;
  return true;
}

// Two's-complement 128-bit pattern, range-checked: a positive magnitude must
// fit 2^127-1, a negative one 2^127 (HUGEINT's minimum).
bool MagnitudeToHugeint(bool negative, absl::uint128 magnitude,
                        duckdb::hugeint_t& out) {
  const absl::uint128 limit = absl::uint128{1} << 127;
  if (negative ? magnitude > limit : magnitude >= limit) {
    return false;
  }
  const absl::uint128 bits =
    negative ? absl::uint128{0} - magnitude : magnitude;
  out = duckdb::hugeint_t{static_cast<int64_t>(absl::Uint128High64(bits)),
                          absl::Uint128Low64(bits)};
  return true;
}

template<typename Phys>
struct DecimalIntBin {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    const auto& type = sink.Type();
    const auto scale = duckdb::DecimalType::GetScale(type);
    bool negative;
    absl::uint128 magnitude;
    if (!PgNumericMagnitude(data, scale, negative, magnitude) ||
        magnitude > absl::uint128{static_cast<uint64_t>(
                      std::numeric_limits<int64_t>::max())}) {
      return false;
    }
    int64_t value = static_cast<int64_t>(absl::Uint128Low64(magnitude));
    if (negative) {
      value = -value;
    }
    if (value < std::numeric_limits<Phys>::min() ||
        value > std::numeric_limits<Phys>::max()) {
      return false;
    }
    sink.template Decimal<Phys>(static_cast<Phys>(value),
                                duckdb::DecimalType::GetWidth(type), scale);
    return true;
  }
};

struct DecimalHugeintBin {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    const auto& type = sink.Type();
    bool negative;
    absl::uint128 magnitude;
    duckdb::hugeint_t value;
    if (!PgNumericMagnitude(data, duckdb::DecimalType::GetScale(type), negative,
                            magnitude) ||
        !MagnitudeToHugeint(negative, magnitude, value)) {
      return false;
    }
    sink.template Decimal<duckdb::hugeint_t>(
      value, duckdb::DecimalType::GetWidth(type),
      duckdb::DecimalType::GetScale(type));
    return true;
  }
};

struct HugeintBin {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    bool negative;
    absl::uint128 magnitude;
    duckdb::hugeint_t value;
    if (!PgNumericMagnitude(data, 0, negative, magnitude) ||
        !MagnitudeToHugeint(negative, magnitude, value)) {
      return false;
    }
    sink.Fixed(value);
    return true;
  }
};

struct UhugeintBin {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    bool negative;
    absl::uint128 magnitude;
    if (!PgNumericMagnitude(data, 0, negative, magnitude) || negative) {
      return false;
    }
    sink.Fixed(duckdb::uhugeint_t{absl::Uint128High64(magnitude),
                                  absl::Uint128Low64(magnitude)});
    return true;
  }
};

struct UbigintBin {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    bool negative;
    absl::uint128 magnitude;
    if (!PgNumericMagnitude(data, 0, negative, magnitude) || negative ||
        magnitude > absl::uint128{std::numeric_limits<uint64_t>::max()}) {
      return false;
    }
    sink.Fixed(static_cast<uint64_t>(absl::Uint128Low64(magnitude)));
    return true;
  }
};

// ===========================================================================
// Text leaf cores
// ===========================================================================

struct BoolText {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    if (data == "t" || data == "1" || data == "on" || data == "yes" ||
        data == "true") {
      sink.Fixed(true);
      return true;
    }
    if (data == "f" || data == "0" || data == "off" || data == "no" ||
        data == "false") {
      sink.Fixed(false);
      return true;
    }
    return false;
  }
};

// All fixed-width numbers (int*/uint*/float/double) parse through fast_float.
// PG's numeric input accepts surrounding whitespace, a leading '+', and (for
// floats) inf/nan: the flags cover the leading whitespace + plus, `general`
// keeps inf/nan, and the field must otherwise be fully consumed (trailing
// whitespace is skip-only at the head, so drop it here) to reject junk.
template<typename T>
struct NumberText {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    static constexpr fast_float::parse_options kOptions{
      fast_float::chars_format::general |
      fast_float::chars_format::allow_leading_plus |
      fast_float::chars_format::skip_white_space};
    T value;
    auto res = fast_float::from_chars_advanced(
      data.data(), data.data() + data.size(), value, kOptions);
    if (res.ec != std::errc{}) {
      return false;
    }
    const auto* end = data.data() + data.size();
    while (res.ptr != end && (*res.ptr == ' ' || *res.ptr == '\t')) {
      ++res.ptr;
    }
    if (res.ptr != end) {
      return false;
    }
    sink.Fixed(value);
    return true;
  }
};

struct VarcharText {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    sink.Varchar(data);
    return true;
  }
};

struct BlobText {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    if (data.size() > 2 && data.starts_with("\\x")) {
      std::string bytes;
      if (!absl::HexStringToBytes(data.substr(2), &bytes)) {
        return false;
      }
      sink.Blob(
        duckdb::string_t{bytes.data(), static_cast<uint32_t>(bytes.size())});
      return true;
    }
    sink.Blob(
      duckdb::string_t{data.data(), static_cast<uint32_t>(data.size())});
    return true;
  }
};

std::unique_ptr<icu::Calendar> MakeCalendar(std::string tz_name) {
  auto tz = duckdb::ICUHelpers::TryGetTimeZone(tz_name);
  if (!tz) {
    return nullptr;
  }
  UErrorCode status = U_ZERO_ERROR;
  std::unique_ptr<icu::Calendar> calendar{
    icu::Calendar::createInstance(tz.release(), status)};
  if (U_FAILURE(status)) {
    return nullptr;
  }
  return calendar;
}

}  // namespace

icu::Calendar* DeserializeContext::CalendarFor(std::string_view tz_name) {
  auto it = named_calendars.try_emplace(tz_name, nullptr).first;
  if (it->second) {
    return it->second.get();
  }
  it->second = MakeCalendar(std::string{tz_name});
  return it->second.get();
}

void FillDeserializeContext(duckdb::ClientContext& client,
                            DeserializeContext& context) {
  duckdb::Value value;
  if (!client.TryGetCurrentSetting("TimeZone", value) || value.IsNull()) {
    context.session_calendar.reset();
    return;
  }
  const auto tz_name = value.ToString();
  if (IsUtcTimeZoneName(tz_name)) {
    context.session_calendar.reset();
  } else {
    context.session_calendar = MakeCalendar(tz_name);
  }
}

namespace {

inline bool ConvertTimestampTzText(DeserializeContext& ctx,
                                   std::string_view data,
                                   duckdb::timestamp_t& result,
                                   int32_t* nanos = nullptr) {
  bool has_offset = false;
  duckdb::string_t tz_name{};
  if (duckdb::Timestamp::TryConvertTimestampTZ(
        data.data(), data.size(), result, /*use_offset=*/true, has_offset,
        tz_name, nanos) != duckdb::TimestampCastResult::SUCCESS) {
    return false;
  }
  if (has_offset || !result.IsFinite()) {
    return true;
  }
  icu::Calendar* calendar = nullptr;
  if (tz_name.GetSize() != 0) {
    calendar = ctx.CalendarFor({tz_name.GetData(), tz_name.GetSize()});
    if (!calendar) {
      return false;
    }
  } else {
    // No zone in the text: interpret in the session zone (null = UTC).
    calendar = ctx.session_calendar.get();
  }
  if (calendar) {
    try {
      result = duckdb::timestamp_t{
        duckdb::ICUDateFunc::FromNaive(calendar, result).value};
    } catch (const std::exception&) {
      return false;
    }
  }
  return true;
}

struct TimestampTzText {
  template<typename Sink>
  static bool Decode(DeserializeContext& ctx, std::string_view data,
                     Sink& sink) {
    duckdb::timestamp_t result;
    if (!ConvertTimestampTzText(ctx, data, result)) {
      return false;
    }
    sink.Fixed(duckdb::timestamp_tz_t{result});
    return true;
  }
};

struct TimestampTzNsText {
  template<typename Sink>
  static bool Decode(DeserializeContext& ctx, std::string_view data,
                     Sink& sink) {
    duckdb::timestamp_t result;
    int32_t nanos = 0;
    if (!ConvertTimestampTzText(ctx, data, result, &nanos)) {
      return false;
    }
    duckdb::timestamp_ns_t ns;
    if (!duckdb::Timestamp::TryFromTimestampNanos(result, nanos, ns)) {
      return false;
    }
    sink.Fixed(duckdb::timestamp_tz_ns_t{ns});
    return true;
  }
};

struct TimeTzText {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    duckdb::dtime_tz_t result;
    duckdb::idx_t pos = 0;
    bool has_offset = false;
    if (!duckdb::Time::TryConvertTimeTZ(data.data(), data.size(), pos, result,
                                        has_offset)) {
      return false;
    }
    sink.Fixed(result);
    return true;
  }
};

struct UuidText {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    duckdb::hugeint_t result;
    if (!duckdb::UUID::FromCString(data.data(), data.size(), result)) {
      return false;
    }
    sink.Uuid(result);
    return true;
  }
};

// 128-bit has no fast_float; DuckDB's string->hugeint cast is the parser.
struct HugeintText {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    duckdb::hugeint_t result;
    if (!duckdb::TryCast::Operation<duckdb::string_t, duckdb::hugeint_t>(
          duckdb::string_t{data.data(), static_cast<uint32_t>(data.size())},
          result, /*strict=*/true)) {
      return false;
    }
    sink.Fixed(result);
    return true;
  }
};

struct UhugeintText {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    duckdb::uhugeint_t result;
    if (!duckdb::TryCast::Operation<duckdb::string_t, duckdb::uhugeint_t>(
          duckdb::string_t{data.data(), static_cast<uint32_t>(data.size())},
          result, /*strict=*/true)) {
      return false;
    }
    sink.Fixed(result);
    return true;
  }
};

template<typename Phys>
struct DecimalText {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    const auto& type = sink.Type();
    const auto width = duckdb::DecimalType::GetWidth(type);
    const auto scale = duckdb::DecimalType::GetScale(type);
    duckdb::CastParameters params(/*strict=*/true, nullptr);
    Phys result;
    if (!duckdb::TryCastToDecimal::Operation<duckdb::string_t, Phys>(
          duckdb::string_t{data.data(), static_cast<uint32_t>(data.size())},
          result, params, width, scale)) {
      return false;
    }
    sink.template Decimal<Phys>(result, width, scale);
    return true;
  }
};

// Types without a dedicated binary recv (scaled time/timestamp text): route
// through DuckDB's string->type cast, which parses the column's exact
// precision.
template<typename T>
struct CastText {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    T result;
    if (!duckdb::TryCast::Operation<duckdb::string_t, T>(
          duckdb::string_t{data.data(), static_cast<uint32_t>(data.size())},
          result, /*strict=*/true)) {
      return false;
    }
    sink.Fixed(result);
    return true;
  }
};

struct IntervalText {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    duckdb::interval_t result;
    if (!duckdb::Interval::FromCString(data.data(), data.size(), result,
                                       nullptr, /*strict=*/true)) {
      return false;
    }
    sink.Fixed(result);
    return true;
  }
};

// ===========================================================================
// Format-shared cores
// ===========================================================================

// ENUM binary and text are identical on the wire: the label bytes, resolved to
// the dictionary ordinal and stored at the enum's physical width.
struct EnumCore {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    const auto& type = sink.Type();
    const auto pos = duckdb::EnumType::GetPos(
      type, duckdb::string_t{data.data(), static_cast<uint32_t>(data.size())});
    if (pos < 0) {
      return false;
    }
    switch (duckdb::EnumType::GetPhysicalType(type)) {
      case duckdb::PhysicalType::UINT8:
        sink.template Enum<uint8_t>(static_cast<uint8_t>(pos));
        return true;
      case duckdb::PhysicalType::UINT16:
        sink.template Enum<uint16_t>(static_cast<uint16_t>(pos));
        return true;
      case duckdb::PhysicalType::UINT32:
        sink.template Enum<uint32_t>(static_cast<uint32_t>(pos));
        return true;
      default:
        return false;
    }
  }
};

struct BitText {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    return sink.Bit(data);
  }
};

struct BitBin {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink) {
    // PG bit binary: int32 nbits + ceil(nbits/8) bytes, bits MSB-first from
    // bit 0 with trailing zero padding in the last byte. Read them out into a
    // "0101" string and let the bit cast build the DuckDB bitstring.
    if (data.size() < 4) {
      return false;
    }
    const auto nbits = absl::big_endian::Load<int32_t>(data.data());
    if (nbits < 0) {
      return false;
    }
    const size_t nbytes = (static_cast<size_t>(nbits) + 7) / 8;
    if (data.size() < 4 + nbytes) {
      return false;
    }
    std::string bits;
    bits.reserve(static_cast<size_t>(nbits));
    for (int32_t b = 0; b < nbits; ++b) {
      const auto byte = static_cast<uint8_t>(data[4 + b / 8]);
      bits += ((byte >> (7 - b % 8)) & 1) ? '1' : '0';
    }
    return sink.Bit(bits);
  }
};

// Types without a dedicated decoder (e.g. VARIANT): text routes through the
// generic string->type cast, the same path PG's input functions take for exotic
// types. Binary has no portable recv format here, so it is rejected -- matching
// PG, which errors rather than coercing on a binary type mismatch.
bool DeserializeTextDefaultInto(std::string_view data,
                                const duckdb::LogicalType& type,
                                duckdb::Value& out) {
  duckdb::Value value{std::string{data}};
  duckdb::Value casted;
  std::string error;
  if (!value.DefaultTryCastAs(type, casted, &error, /*strict=*/true)) {
    return false;
  }
  out = std::move(casted);
  return true;
}

struct DefaultText {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view data, Sink& sink);
};

template<>
bool DefaultText::Decode<VectorSink>(DeserializeContext&, std::string_view data,
                                     VectorSink& sink) {
  duckdb::Value casted;
  if (!DeserializeTextDefaultInto(data, sink.vec.GetType(), casted)) {
    return false;
  }
  sink.vec.SetValue(sink.row, std::move(casted));
  return true;
}

template<>
bool DefaultText::Decode<ValueSink>(DeserializeContext&, std::string_view data,
                                    ValueSink& sink) {
  return DeserializeTextDefaultInto(data, sink.type, sink.out);
}

struct DefaultBin {
  template<typename Sink>
  static bool Decode(DeserializeContext&, std::string_view, Sink&) {
    return false;
  }
};

// Pick the binary or text core by wire format -- the twin of serialize's
// SelectFieldSerializer.
template<typename BinCore, typename TextCore, typename Sink>
DeserializationFunction<Sink> SelectDecoder(bool binary) {
  return binary ? &BinCore::template Decode<Sink>
                : &TextCore::template Decode<Sink>;
}

// ===========================================================================
// Nested decoding (list / struct / map)
// ===========================================================================
//
// The leaf cores above land each scalar field; the machinery below rebuilds the
// PG array/composite wire forms into the column's Vector children, resolving
// the child decoders through GetDeserialization (the deserialize twin of how
// serialize walks to the leaf core). The thin nested cores at the end wrap this
// so the GetDeserialization switch dispatches LIST/STRUCT/MAP the same way as
// the scalars.

template<typename T>
T* Out(duckdb::Vector& vec) {
  return duckdb::FlatVector::GetDataMutable<T>(vec);
}

// Decode one element body into the list's child vector at `index`, recursing on
// the child type's decoder; -1 length (binary) or is_null (text) -> NULL slot.
// Nested decoding always targets a Vector child, so it routes through the
// VectorSink decoders.
bool DecodeChild(DeserializeContext& ctx, const duckdb::LogicalType& child_type,
                 VarFormat format, std::string_view body, duckdb::Vector& child,
                 duckdb::idx_t index) {
  const auto fn = GetDeserialization<VectorSink>(child_type, format);
  if (fn == nullptr) {
    return false;
  }
  VectorSink sink{child, index};
  return fn(ctx, body, sink);
}

// Resolve (and cache, per type+format) the field deserializers of a record-like
// type -- the deserialize twin of serialize's GetSerializersCache. Built once
// on the first decode of the type, then reused for every row/element instead of
// re-resolving per field. STRUCT uses its field types; MAP its key/value.
const std::vector<DeserializationFunction<VectorSink>>& GetRecordFieldDecoders(
  DeserializeContext& ctx, const duckdb::LogicalType& type, VarFormat format) {
  if (ctx.record_cache == nullptr) {
    ctx.record_cache = std::make_unique<RecordDeserializers>();
  }
  const auto key = reinterpret_cast<uintptr_t>(&type) |
                   static_cast<uintptr_t>(format == VarFormat::Binary ? 1 : 0);
  auto [it, inserted] = ctx.record_cache->fields.try_emplace(key);
  auto& fns = it->second;
  if (inserted) {
    if (type.id() == duckdb::LogicalTypeId::MAP) {
      fns.push_back(
        GetDeserialization<VectorSink>(duckdb::MapType::KeyType(type), format));
      fns.push_back(GetDeserialization<VectorSink>(
        duckdb::MapType::ValueType(type), format));
    } else {
      const auto& children = duckdb::StructType::GetChildTypes(type);
      fns.reserve(children.size());
      for (const auto& [name, child_type] : children) {
        (void)name;
        fns.push_back(GetDeserialization<VectorSink>(child_type, format));
      }
    }
  }
  return fns;
}

// Reconstruct one LIST dimension of a PG multidimensional array into `vec` at
// `index`. The serializer (FlattenArray) emits a single row-major leaf stream:
// dims `[depth..ndim)` describe the remaining nesting, so a list of dims[depth]
// children is written, each either a deeper sub-list or a leaf element read
// from the stream (int32 len, -1 = NULL, then len bytes). `offset` advances
// through the leaf stream. The DuckDB column type mirrors the dimension count.
bool DecodeBinaryDim(DeserializeContext& ctx, std::string_view data,
                     size_t& offset, const int32_t* dims, int32_t ndim,
                     int32_t depth, duckdb::Vector& vec, duckdb::idx_t index,
                     DeserializationFunction<VectorSink> leaf_fn) {
  const auto count = static_cast<duckdb::idx_t>(dims[depth]);
  const auto cur = duckdb::ListVector::GetListSize(vec);
  duckdb::ListVector::Reserve(vec, cur + count);
  auto& child = duckdb::ListVector::GetEntry(vec);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    const auto child_index = cur + i;
    if (depth + 1 < ndim) {
      if (!DecodeBinaryDim(ctx, data, offset, dims, ndim, depth + 1, child,
                           child_index, leaf_fn)) {
        return false;
      }
      continue;
    }
    if (offset + 4 > data.size()) {
      return false;
    }
    const auto len = absl::big_endian::Load<int32_t>(data.data() + offset);
    offset += 4;
    if (len == -1) {
      duckdb::FlatVector::SetNull(child, child_index, true);
      continue;
    }
    if (len < 0 || static_cast<size_t>(len) > data.size() - offset) {
      return false;
    }
    // leaf_fn is resolved once per array (the element type is fixed) instead of
    // re-resolving the deserializer for every element.
    VectorSink sink{child, child_index};
    if (!leaf_fn(ctx, data.substr(offset, len), sink)) {
      return false;
    }
    offset += len;
  }
  Out<duckdb::list_entry_t>(vec)[index] = {cur, count};
  duckdb::ListVector::SetListSize(vec, cur + count);
  return true;
}

// PG array binary: int32 ndim, int32 flags, int32 elem_oid, then
// ndim x (int32 size, int32 lower_bound), then the flattened leaf stream of
// int32 len (-1 = NULL) + bytes. The column type is authoritative and the
// dimensions drive the multi-dim rebuild. Each LIST/MAP level is one wire
// dimension (MAP is physically a LIST of key/value structs); the serialize side
// rejects ragged nestings, so a well-formed array is always rectangular here.
bool DeserializeBinaryList(DeserializeContext& ctx, std::string_view data,
                           duckdb::Vector& vec, duckdb::idx_t row) {
  if (data.size() < 12) {
    return false;
  }
  const auto ndim = absl::big_endian::Load<int32_t>(data.data());
  const auto cur = duckdb::ListVector::GetListSize(vec);
  if (ndim == 0) {
    Out<duckdb::list_entry_t>(vec)[row] = {cur, 0};
    return true;
  }
  if (ndim < 0 || data.size() < 12 + static_cast<size_t>(ndim) * 8) {
    return false;
  }
  std::vector<int32_t> dims(static_cast<size_t>(ndim));
  for (int32_t d = 0; d < ndim; ++d) {
    const auto size = absl::big_endian::Load<int32_t>(data.data() + 12 + d * 8);
    if (size < 0) {
      return false;
    }
    dims[static_cast<size_t>(d)] = size;
  }
  // Resolve the leaf element deserializer once (the element type is fixed for
  // the whole array). LIST and MAP each contribute one dimension; ARRAY targets
  // aren't reconstructed by the multi-dim path.
  const duckdb::LogicalType* leaf = &vec.GetType();
  for (int32_t d = 0; d < ndim; ++d) {
    const auto id = leaf->id();
    if (id == duckdb::LogicalTypeId::LIST || id == duckdb::LogicalTypeId::MAP) {
      leaf = &duckdb::ListType::GetChildType(*leaf);
    } else if (id == duckdb::LogicalTypeId::ARRAY) {
      return false;
    } else {
      break;
    }
  }
  const auto leaf_fn = GetDeserialization<VectorSink>(*leaf, VarFormat::Binary);
  if (leaf_fn == nullptr) {
    return false;
  }
  size_t offset = 12 + static_cast<size_t>(ndim) * 8;
  return DecodeBinaryDim(ctx, data, offset, dims.data(), ndim, 0, vec, row,
                         leaf_fn);
}

// PG composite binary: int32 nfields, then per field int32 type_oid,
// int32 len (-1 NULL), len bytes. The column type is authoritative; the wire
// OIDs are ignored. Each field decodes into its own child vector at `row`.
bool DeserializeBinaryStruct(DeserializeContext& ctx, std::string_view data,
                             duckdb::Vector& vec, duckdb::idx_t row) {
  if (data.size() < 4) {
    return false;
  }
  const auto nfields = absl::big_endian::Load<int32_t>(data.data());
  const auto& field_fns =
    GetRecordFieldDecoders(ctx, vec.GetType(), VarFormat::Binary);
  if (nfields < 0 || static_cast<size_t>(nfields) != field_fns.size()) {
    return false;
  }
  auto& entries = duckdb::StructVector::GetEntries(vec);
  size_t offset = 4;
  for (size_t f = 0; f < field_fns.size(); ++f) {
    if (offset + 8 > data.size()) {
      return false;
    }
    offset += 4;  // field type OID (ignored)
    const auto len = absl::big_endian::Load<int32_t>(data.data() + offset);
    offset += 4;
    if (len == -1) {
      duckdb::FlatVector::SetNull(entries[f], row, true);
      continue;
    }
    if (len < 0 || static_cast<size_t>(len) > data.size() - offset) {
      return false;
    }
    VectorSink sink{entries[f], row};
    if (!field_fns[f](ctx, data.substr(offset, len), sink)) {
      return false;
    }
    offset += len;
  }
  return true;
}

// ---- Zero-copy PG text composite decoding ----------------------------------
//
// PG renders nested values as `{a,b}` arrays and `(a,b)` records, recursively
// quoting any element/field that holds a delimiter, bracket, quote, space, the
// word NULL (arrays) or is empty. The two dialects diverge: arrays escape an
// embedded quote as `\"` only and treat a bare case-insensitive `NULL` as SQL
// NULL; records escape it as `""` (and also accept `\"`), treat an *empty*
// field as SQL NULL, and never trim whitespace. The scanners split on top-level
// commas in a single pass without allocating; the resolvers unescape into a
// caller-owned scratch buffer only when an escape is actually present and
// otherwise hand back a view straight into the input.

// Strip surrounding whitespace and one matching `open`/`close` bracket pair.
// Returns false (leaving `s` untouched) when the wrapper is absent.
bool StripWrap(std::string_view& s, char open, char close) {
  while (!s.empty() && absl::ascii_isspace(s.front())) {
    s.remove_prefix(1);
  }
  while (!s.empty() && absl::ascii_isspace(s.back())) {
    s.remove_suffix(1);
  }
  if (s.size() < 2 || s.front() != open || s.back() != close) {
    return false;
  }
  s.remove_prefix(1);
  s.remove_suffix(1);
  return true;
}

// Scan an array body (inside `{}`), calling emit(raw, group) per top-level
// element. `group` marks a bare `{...}` sub-array kept verbatim for recursion;
// otherwise `raw` is the leaf incl. any surrounding quotes (surrounding
// whitespace trimmed). Returns false on malformed nesting/quoting.
template<typename Emit>
bool ScanArrayItems(std::string_view body, Emit&& emit) {
  size_t i = 0;
  const size_t n = body.size();
  while (i < n && absl::ascii_isspace(body[i])) {
    ++i;
  }
  if (i == n) {
    return true;  // empty array
  }
  while (true) {
    while (i < n && absl::ascii_isspace(body[i])) {
      ++i;
    }
    const size_t start = i;
    bool group = false;
    if (i < n && body[i] == '{') {
      group = true;
      int depth = 0;
      bool in_quote = false;
      for (; i < n; ++i) {
        const char c = body[i];
        if (in_quote) {
          if (c == '\\' && i + 1 < n) {
            ++i;
          } else if (c == '"') {
            in_quote = false;
          }
        } else if (c == '"') {
          in_quote = true;
        } else if (c == '{') {
          ++depth;
        } else if (c == '}' && --depth == 0) {
          ++i;
          break;
        }
      }
      if (depth != 0) {
        return false;
      }
    } else if (i < n && body[i] == '"') {
      ++i;
      bool closed = false;
      for (; i < n; ++i) {
        if (body[i] == '\\' && i + 1 < n) {
          ++i;
        } else if (body[i] == '"') {
          ++i;
          closed = true;
          break;
        }
      }
      if (!closed) {
        return false;
      }
    } else {
      for (; i < n && body[i] != ','; ++i) {
        if (body[i] == '\\' && i + 1 < n) {
          ++i;
        }
      }
    }
    size_t end = i;
    while (end > start && absl::ascii_isspace(body[end - 1])) {
      --end;
    }
    emit(body.substr(start, end - start), group);
    while (i < n && absl::ascii_isspace(body[i])) {
      ++i;
    }
    if (i >= n) {
      return true;
    }
    if (body[i] != ',') {
      return false;
    }
    ++i;
  }
}

// Resolve a non-group array leaf to its value, viewing into `raw` when no
// unescaping is needed and into `scratch` otherwise. `is_null` is set for a
// bare, unescaped, case-insensitive NULL.
std::string_view ResolveArrayLeaf(std::string_view raw, std::string& scratch,
                                  bool& is_null) {
  is_null = false;
  if (!raw.empty() && raw.front() == '"') {
    const std::string_view inner = raw.substr(1, raw.size() - 2);
    if (inner.find('\\') == std::string_view::npos) {
      return inner;
    }
    scratch.clear();
    for (size_t i = 0; i < inner.size(); ++i) {
      if (inner[i] == '\\' && i + 1 < inner.size()) {
        scratch += inner[++i];
      } else {
        scratch += inner[i];
      }
    }
    return scratch;
  }
  if (raw.find('\\') == std::string_view::npos) {
    is_null = absl::EqualsIgnoreCase(raw, "NULL");
    return raw;
  }
  scratch.clear();
  for (size_t i = 0; i < raw.size(); ++i) {
    if (raw[i] == '\\' && i + 1 < raw.size()) {
      scratch += raw[++i];
    } else {
      scratch += raw[i];
    }
  }
  return scratch;
}

// Scan a record body (inside `()`), calling emit(raw) per top-level field
// (verbatim bytes incl. any quotes; whitespace is part of the value). Double
// quotes use `""` and `\` escapes. An empty body is a zero-field record.
// Returns false on malformed quoting.
template<typename Emit>
bool ScanRecordFields(std::string_view body, Emit&& emit) {
  if (body.empty()) {
    return true;
  }
  size_t i = 0;
  const size_t n = body.size();
  while (true) {
    const size_t start = i;
    bool in_quote = false;
    for (; i < n; ++i) {
      const char c = body[i];
      if (in_quote) {
        if (c == '\\' && i + 1 < n) {
          ++i;
        } else if (c == '"') {
          if (i + 1 < n && body[i + 1] == '"') {
            ++i;
          } else {
            in_quote = false;
          }
        }
      } else if (c == '"') {
        in_quote = true;
      } else if (c == ',') {
        break;
      }
    }
    if (in_quote) {
      return false;
    }
    emit(body.substr(start, i - start));
    if (i >= n) {
      return true;
    }
    ++i;  // skip comma
    if (i == n) {
      emit(std::string_view{});  // empty field after a trailing comma
      return true;
    }
  }
}

// Resolve one record field, viewing into `raw` when no unescaping is needed and
// into `scratch` otherwise. An empty unquoted field is SQL NULL.
std::string_view ResolveRecordField(std::string_view raw, std::string& scratch,
                                    bool& is_null) {
  is_null = false;
  if (raw.find('"') == std::string_view::npos &&
      raw.find('\\') == std::string_view::npos) {
    is_null = raw.empty();
    return raw;
  }
  scratch.clear();
  bool in_quote = false;
  for (size_t i = 0; i < raw.size(); ++i) {
    const char c = raw[i];
    if (in_quote) {
      if (c == '\\' && i + 1 < raw.size()) {
        scratch += raw[++i];
      } else if (c == '"') {
        if (i + 1 < raw.size() && raw[i + 1] == '"') {
          scratch += '"';
          ++i;
        } else {
          in_quote = false;
        }
      } else {
        scratch += c;
      }
    } else if (c == '\\' && i + 1 < raw.size()) {
      scratch += raw[++i];
    } else if (c == '"') {
      in_quote = true;
    } else {
      scratch += c;
    }
  }
  return scratch;
}

// Decode a record body (inside `()`) into its fields, writing field f into
// dst(f) at `index`. Shared by STRUCT and the STRUCT(key,value) of a MAP entry.
// Returns false on a field-count mismatch or a child failure.
template<typename DstFn>
bool DecodeRecordFields(
  DeserializeContext& ctx, std::string_view body,
  const std::vector<DeserializationFunction<VectorSink>>& field_fns,
  DstFn&& dst, std::string& scratch, duckdb::idx_t index) {
  if (body.empty()) {
    // PG renders a single NULL-field record as `()`; for a 1-column record that
    // empty body is one NULL field (not zero), so it round-trips. A 0-column
    // record stays empty; anything wider is too few columns.
    if (field_fns.size() == 1) {
      duckdb::FlatVector::SetNull(dst(0), index, true);
      return true;
    }
    return field_fns.empty();
  }
  size_t f = 0;
  bool ok = true;
  if (!ScanRecordFields(body, [&](std::string_view raw) {
        if (!ok) {
          return;
        }
        if (f >= field_fns.size()) {
          ok = false;
          return;
        }
        bool is_null = false;
        const std::string_view val = ResolveRecordField(raw, scratch, is_null);
        duckdb::Vector& d = dst(f);
        if (is_null) {
          duckdb::FlatVector::SetNull(d, index, true);
        } else {
          VectorSink sink{d, index};
          if (!field_fns[f](ctx, val, sink)) {
            ok = false;
            return;
          }
        }
        ++f;
      })) {
    return false;
  }
  return ok && f == field_fns.size();
}

bool DeserializeTextList(DeserializeContext& ctx, std::string_view data,
                         duckdb::Vector& vec, duckdb::idx_t row) {
  std::string_view body = data;
  if (!StripWrap(body, '{', '}')) {
    return false;
  }
  const auto& child_type = duckdb::ListType::GetChildType(vec.GetType());
  const bool nested = child_type.id() == duckdb::LogicalTypeId::LIST;
  const auto cur = duckdb::ListVector::GetListSize(vec);
  duckdb::idx_t count = 0;
  if (!ScanArrayItems(body, [&](std::string_view, bool) { ++count; })) {
    return false;
  }
  duckdb::ListVector::Reserve(vec, cur + count);
  auto& child = duckdb::ListVector::GetEntry(vec);
  std::string scratch;
  duckdb::idx_t i = 0;
  bool ok = true;
  ScanArrayItems(body, [&](std::string_view raw, bool) {
    if (!ok) {
      return;
    }
    const auto index = cur + i;
    ++i;
    if (nested) {
      // A sub-array element keeps its `{...}` braces; recurse on the child
      // type.
      if (!DecodeChild(ctx, child_type, VarFormat::Text, raw, child, index)) {
        ok = false;
      }
      return;
    }
    bool is_null = false;
    const std::string_view val = ResolveArrayLeaf(raw, scratch, is_null);
    if (is_null) {
      duckdb::FlatVector::SetNull(child, index, true);
    } else if (!DecodeChild(ctx, child_type, VarFormat::Text, val, child,
                            index)) {
      ok = false;
    }
  });
  if (!ok) {
    return false;
  }
  Out<duckdb::list_entry_t>(vec)[row] = {cur, count};
  duckdb::ListVector::SetListSize(vec, cur + count);
  return true;
}

bool DeserializeTextStruct(DeserializeContext& ctx, std::string_view data,
                           duckdb::Vector& vec, duckdb::idx_t row) {
  std::string_view body = data;
  if (!StripWrap(body, '(', ')')) {
    return false;
  }
  const auto& field_fns =
    GetRecordFieldDecoders(ctx, vec.GetType(), VarFormat::Text);
  auto& entries = duckdb::StructVector::GetEntries(vec);
  std::string scratch;
  return DecodeRecordFields(
    ctx, body, field_fns,
    [&](size_t f) -> duckdb::Vector& { return entries[f]; }, scratch, row);
}

// Decode the count map entries (each a STRUCT(key,value)) into the list child,
// then point this row's list_entry at the slice. `decode_entry` writes one
// entry STRUCT into the child struct vector at the given index.
template<typename DecodeEntry>
bool FinishMap(duckdb::Vector& vec, duckdb::idx_t row, duckdb::idx_t cur,
               duckdb::idx_t count, DecodeEntry&& decode_entry) {
  auto& key_child = duckdb::MapVector::GetKeys(vec);
  auto& value_child = duckdb::MapVector::GetValues(vec);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    if (!decode_entry(i, cur + i, key_child, value_child)) {
      return false;
    }
  }
  Out<duckdb::list_entry_t>(vec)[row] = {cur, count};
  duckdb::ListVector::SetListSize(vec, cur + count);
  return true;
}

bool DeserializeBinaryMap(DeserializeContext& ctx, std::string_view data,
                          duckdb::Vector& vec, duckdb::idx_t row) {
  if (data.size() < 12) {
    return false;
  }
  const auto ndims = absl::big_endian::Load<int32_t>(data.data());
  const auto& field_fns =
    GetRecordFieldDecoders(ctx, vec.GetType(), VarFormat::Binary);
  const auto cur = duckdb::ListVector::GetListSize(vec);
  if (ndims == 0) {
    Out<duckdb::list_entry_t>(vec)[row] = {cur, 0};
    return true;
  }
  if (ndims != 1 || data.size() < 12 + 8) {
    return false;
  }
  const auto count = absl::big_endian::Load<int32_t>(data.data() + 12);
  if (count < 0) {
    return false;
  }
  duckdb::ListVector::Reserve(vec, cur + static_cast<duckdb::idx_t>(count));
  size_t offset = 12 + 8;
  return FinishMap(
    vec, row, cur, static_cast<duckdb::idx_t>(count),
    [&](duckdb::idx_t, duckdb::idx_t index, duckdb::Vector& key_child,
        duckdb::Vector& value_child) {
      // Each entry is a non-null STRUCT(key,value) on the wire: int32 elem len
      // then the binary record body { int32 nfields, per field oid+len+bytes }.
      if (offset + 4 > data.size()) {
        return false;
      }
      const auto elem_len =
        absl::big_endian::Load<int32_t>(data.data() + offset);
      offset += 4;
      if (elem_len < 4 ||
          static_cast<size_t>(elem_len) > data.size() - offset) {
        return false;
      }
      const auto entry = data.substr(offset, elem_len);
      offset += elem_len;
      if (absl::big_endian::Load<int32_t>(entry.data()) != 2) {
        return false;
      }
      size_t fo = 4;
      auto field = [&](DeserializationFunction<VectorSink> fn,
                       duckdb::Vector& dst) {
        if (fo + 8 > entry.size()) {
          return false;
        }
        fo += 4;  // field OID (ignored)
        const auto len = absl::big_endian::Load<int32_t>(entry.data() + fo);
        fo += 4;
        if (len == -1) {
          duckdb::FlatVector::SetNull(dst, index, true);
          return true;
        }
        if (len < 0 || static_cast<size_t>(len) > entry.size() - fo) {
          return false;
        }
        VectorSink sink{dst, index};
        if (!fn(ctx, entry.substr(fo, len), sink)) {
          return false;
        }
        fo += len;
        return true;
      };
      return field(field_fns[0], key_child) && field(field_fns[1], value_child);
    });
}

bool DeserializeTextMap(DeserializeContext& ctx, std::string_view data,
                        duckdb::Vector& vec, duckdb::idx_t row) {
  // A MAP serializes exactly like LIST(STRUCT(key,value)): `{"(k,v)",...}`.
  // Each array element is a quoted record, so it decodes through the array
  // scanner (to unquote the entry) and the record decoder (key + value).
  std::string_view body = data;
  if (!StripWrap(body, '{', '}')) {
    return false;
  }
  const auto& field_fns =
    GetRecordFieldDecoders(ctx, vec.GetType(), VarFormat::Text);
  const auto cur = duckdb::ListVector::GetListSize(vec);
  duckdb::idx_t count = 0;
  if (!ScanArrayItems(body, [&](std::string_view, bool) { ++count; })) {
    return false;
  }
  duckdb::ListVector::Reserve(vec, cur + count);
  auto& key_child = duckdb::MapVector::GetKeys(vec);
  auto& value_child = duckdb::MapVector::GetValues(vec);
  std::string entry_scratch;
  std::string field_scratch;
  duckdb::idx_t i = 0;
  bool ok = true;
  ScanArrayItems(body, [&](std::string_view raw, bool) {
    if (!ok) {
      return;
    }
    const auto index = cur + i;
    ++i;
    bool is_null = false;
    std::string_view entry = ResolveArrayLeaf(raw, entry_scratch, is_null);
    if (is_null || !StripWrap(entry, '(', ')')) {
      ok = false;  // a NULL or non-record map entry is malformed
      return;
    }
    if (!DecodeRecordFields(
          ctx, entry, field_fns,
          [&](size_t f) -> duckdb::Vector& {
            return f == 0 ? key_child : value_child;
          },
          field_scratch, index)) {
      ok = false;
    }
  });
  if (!ok) {
    return false;
  }
  Out<duckdb::list_entry_t>(vec)[row] = {cur, count};
  duckdb::ListVector::SetListSize(vec, cur + count);
  return true;
}

// The nested decoders rebuild into a Vector child structure. ValueSink decodes
// into a one-row scratch Vector through that same machinery, then lifts the
// Value out. Nested bind params are rare, so the scratch+GetValue cost there is
// fine; the win is the common scalar/temporal/decimal/uuid path going direct.
using NestedVectorFn = bool (*)(DeserializeContext&, std::string_view,
                                duckdb::Vector&, duckdb::idx_t);

template<typename Sink>
struct NestedAdapter;

template<>
struct NestedAdapter<VectorSink> {
  template<NestedVectorFn Fn>
  static bool Run(DeserializeContext& ctx, std::string_view data,
                  VectorSink& sink) {
    return Fn(ctx, data, sink.vec, sink.row);
  }
};

template<>
struct NestedAdapter<ValueSink> {
  template<NestedVectorFn Fn>
  static bool Run(DeserializeContext& ctx, std::string_view data,
                  ValueSink& sink) {
    duckdb::Vector scratch{sink.type, 1};
    if (!Fn(ctx, data, scratch, 0)) {
      return false;
    }
    sink.out = scratch.GetValue(0);
    return true;
  }
};

// STRUCT vector entries: [0]=ip_type (1=IPv4, 2=IPv6), [1]=address (signed
// hugeint, IPv6 top bit flipped for sort order), [2]=mask. Inverse of ReadInet.
void StoreInet(duckdb::Vector& vec, duckdb::idx_t row, INET_IPAddressType type,
               uint64_t addr_hi, uint64_t addr_lo, uint16_t mask) {
  if (type == INET_IP_ADDRESS_V6) {
    addr_hi ^= (uint64_t{1} << 63);  // re-apply IPv6 sort-order flip
  }
  auto& entries = duckdb::StructVector::GetEntries(vec);
  Out<uint8_t>(entries[0])[row] = static_cast<uint8_t>(type);
  duckdb::hugeint_t stored;
  stored.lower = addr_lo;
  stored.upper = static_cast<int64_t>(addr_hi);
  Out<duckdb::hugeint_t>(entries[1])[row] = stored;
  Out<uint16_t>(entries[2])[row] = mask;
}

bool DeserializeBinaryInet(DeserializeContext&, std::string_view data,
                           duckdb::Vector& vec, duckdb::idx_t row) {
  // PG binary inet bytes: [0]=family, [1]=bits, [2]=is_cidr, [3]=nb (4 or 16),
  // then nb big-endian addr bytes; only bits and nb are read.
  if (data.size() < 4) {
    return false;
  }
  const auto nb = static_cast<uint8_t>(data[3]);
  const auto bits = static_cast<uint8_t>(data[1]);
  const bool is_v6 = nb == 16;
  if ((!is_v6 && nb != 4) || data.size() != static_cast<size_t>(4) + nb) {
    return false;
  }
  const char* addr = data.data() + 4;
  if (is_v6) {
    StoreInet(vec, row, INET_IP_ADDRESS_V6,
              absl::big_endian::Load<uint64_t>(addr),
              absl::big_endian::Load<uint64_t>(addr + 8), bits);
  } else {
    StoreInet(vec, row, INET_IP_ADDRESS_V4, 0,
              absl::big_endian::Load<uint32_t>(addr), bits);
  }
  return true;
}

bool DeserializeTextInet(DeserializeContext&, std::string_view data,
                         duckdb::Vector& vec, duckdb::idx_t row) {
  INET_IPAddress inet;
  try {
    inet = ipaddress_from_string(data.data(), data.size());
  } catch (...) {
    return false;
  }
  if (inet.type == INET_IP_ADDRESS_INVALID) {
    return false;
  }
  StoreInet(vec, row, inet.type, inet.address.upper, inet.address.lower,
            inet.mask);
  return true;
}

// Thin nested cores: present the LIST/STRUCT/MAP rebuilders with the same
// Decode interface as the scalar cores, so the GetDeserialization switch
// dispatches them through SelectDecoder uniformly.
struct InetBin {
  template<typename Sink>
  static bool Decode(DeserializeContext& ctx, std::string_view data,
                     Sink& sink) {
    return NestedAdapter<Sink>::template Run<DeserializeBinaryInet>(ctx, data,
                                                                    sink);
  }
};

struct InetText {
  template<typename Sink>
  static bool Decode(DeserializeContext& ctx, std::string_view data,
                     Sink& sink) {
    return NestedAdapter<Sink>::template Run<DeserializeTextInet>(ctx, data,
                                                                  sink);
  }
};

struct ListBin {
  template<typename Sink>
  static bool Decode(DeserializeContext& ctx, std::string_view data,
                     Sink& sink) {
    return NestedAdapter<Sink>::template Run<DeserializeBinaryList>(ctx, data,
                                                                    sink);
  }
};

struct ListText {
  template<typename Sink>
  static bool Decode(DeserializeContext& ctx, std::string_view data,
                     Sink& sink) {
    return NestedAdapter<Sink>::template Run<DeserializeTextList>(ctx, data,
                                                                  sink);
  }
};

struct StructBin {
  template<typename Sink>
  static bool Decode(DeserializeContext& ctx, std::string_view data,
                     Sink& sink) {
    return NestedAdapter<Sink>::template Run<DeserializeBinaryStruct>(ctx, data,
                                                                      sink);
  }
};

struct StructText {
  template<typename Sink>
  static bool Decode(DeserializeContext& ctx, std::string_view data,
                     Sink& sink) {
    return NestedAdapter<Sink>::template Run<DeserializeTextStruct>(ctx, data,
                                                                    sink);
  }
};

struct MapBin {
  template<typename Sink>
  static bool Decode(DeserializeContext& ctx, std::string_view data,
                     Sink& sink) {
    return NestedAdapter<Sink>::template Run<DeserializeBinaryMap>(ctx, data,
                                                                   sink);
  }
};

struct MapText {
  template<typename Sink>
  static bool Decode(DeserializeContext& ctx, std::string_view data,
                     Sink& sink) {
    return NestedAdapter<Sink>::template Run<DeserializeTextMap>(ctx, data,
                                                                 sink);
  }
};

}  // namespace

template<typename Sink>
DeserializationFunction<Sink> GetDeserialization(
  const duckdb::LogicalType& type, VarFormat format) {
  const bool binary = format == VarFormat::Binary;
  switch (type.id()) {
    using enum duckdb::LogicalTypeId;
    case BOOLEAN:
      return SelectDecoder<BoolBin, BoolText, Sink>(binary);
    case TINYINT:
      return SelectDecoder<LoadBin<int8_t, int16_t>, NumberText<int8_t>, Sink>(
        binary);
    case SMALLINT:
      return SelectDecoder<LoadBin<int16_t>, NumberText<int16_t>, Sink>(binary);
    case INTEGER:
      return SelectDecoder<LoadBin<int32_t>, NumberText<int32_t>, Sink>(binary);
    case BIGINT:
      if (binary) {
        return IsOidLike(type)
                 ? &LoadBin<int64_t, uint32_t>::template Decode<Sink>
                 : &LoadBin<int64_t>::template Decode<Sink>;
      }
      return &NumberText<int64_t>::template Decode<Sink>;
    case UTINYINT:
      return SelectDecoder<LoadBin<uint8_t, int16_t>, NumberText<uint8_t>,
                           Sink>(binary);
    case USMALLINT:
      return SelectDecoder<LoadBin<uint16_t, int32_t>, NumberText<uint16_t>,
                           Sink>(binary);
    case UINTEGER:
      return SelectDecoder<LoadBin<uint32_t, int64_t>, NumberText<uint32_t>,
                           Sink>(binary);
    case UBIGINT:
      return SelectDecoder<UbigintBin, NumberText<uint64_t>, Sink>(binary);
    case HUGEINT:
      return SelectDecoder<HugeintBin, HugeintText, Sink>(binary);
    case UHUGEINT:
      return SelectDecoder<UhugeintBin, UhugeintText, Sink>(binary);
    case DECIMAL: {
      const auto width = duckdb::DecimalType::GetWidth(type);
      if (width <= duckdb::Decimal::MAX_WIDTH_INT16) {
        return SelectDecoder<DecimalIntBin<int16_t>, DecimalText<int16_t>,
                             Sink>(binary);
      }
      if (width <= duckdb::Decimal::MAX_WIDTH_INT32) {
        return SelectDecoder<DecimalIntBin<int32_t>, DecimalText<int32_t>,
                             Sink>(binary);
      }
      if (width <= duckdb::Decimal::MAX_WIDTH_INT64) {
        return SelectDecoder<DecimalIntBin<int64_t>, DecimalText<int64_t>,
                             Sink>(binary);
      }
      return SelectDecoder<DecimalHugeintBin, DecimalText<duckdb::hugeint_t>,
                           Sink>(binary);
    }
    case FLOAT:
      return SelectDecoder<LoadBin<float>, NumberText<float>, Sink>(binary);
    case DOUBLE:
      return SelectDecoder<LoadBin<double>, NumberText<double>, Sink>(binary);
    case VARCHAR:
    case CHAR:
      return SelectDecoder<VarcharBin, VarcharText, Sink>(binary);
    case BLOB:
      return SelectDecoder<BlobBin, BlobText, Sink>(binary);
    case DATE:
      return SelectDecoder<DateBin, CastText<duckdb::date_t>, Sink>(binary);
    case TIME:
      return SelectDecoder<TimeBin, CastText<duckdb::dtime_t>, Sink>(binary);
    case TIME_NS:
      return SelectDecoder<TimeNsBin, CastText<duckdb::dtime_ns_t>, Sink>(
        binary);
    case TIME_TZ:
      return SelectDecoder<TimeTzBin, TimeTzText, Sink>(binary);
    case TIMESTAMP:
      return SelectDecoder<TimestampBin, CastText<duckdb::timestamp_t>, Sink>(
        binary);
    case TIMESTAMP_SEC:
      return SelectDecoder<TimestampSecBin, CastText<duckdb::timestamp_sec_t>,
                           Sink>(binary);
    case TIMESTAMP_MS:
      return SelectDecoder<TimestampMsBin, CastText<duckdb::timestamp_ms_t>,
                           Sink>(binary);
    case TIMESTAMP_NS:
      return SelectDecoder<TimestampNsBin, CastText<duckdb::timestamp_ns_t>,
                           Sink>(binary);
    case TIMESTAMP_TZ:
      return SelectDecoder<TimestampTzBin, TimestampTzText, Sink>(binary);
    case TIMESTAMP_TZ_NS:
      return SelectDecoder<TimestampTzNsBin, TimestampTzNsText, Sink>(binary);
    case INTERVAL:
      return SelectDecoder<IntervalBin, IntervalText, Sink>(binary);
    case UUID:
      return SelectDecoder<UuidBin, UuidText, Sink>(binary);
    case ENUM:
      return &EnumCore::template Decode<Sink>;
    case BIT:
      return SelectDecoder<BitBin, BitText, Sink>(binary);
    case LIST:
      return SelectDecoder<ListBin, ListText, Sink>(binary);
    case STRUCT:
      if (IsInet(type)) {
        return SelectDecoder<InetBin, InetText, Sink>(binary);
      }
      return SelectDecoder<StructBin, StructText, Sink>(binary);
    case MAP:
      return SelectDecoder<MapBin, MapText, Sink>(binary);
    default:
      return SelectDecoder<DefaultBin, DefaultText, Sink>(binary);
  }
}

template DeserializationFunction<VectorSink> GetDeserialization<VectorSink>(
  const duckdb::LogicalType&, VarFormat);
template DeserializationFunction<ValueSink> GetDeserialization<ValueSink>(
  const duckdb::LogicalType&, VarFormat);

}  // namespace sdb::pg
