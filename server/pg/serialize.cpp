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

#include "pg/serialize.h"

#include <absl/algorithm/container.h>
#include <absl/base/internal/endian.h>
#include <absl/strings/ascii.h>
#include <absl/strings/escaping.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_format.h>

#include <algorithm>
#include <bit>
#include <cctype>
#include <cfloat>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <duckdb/common/types/bit.hpp>
#include <duckdb/common/types/hugeint.hpp>
#include <duckdb/common/types/time.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/types/uhugeint.hpp>
#include <duckdb/common/types/uuid.hpp>
#include <limits>
#include <string_view>
#include <type_traits>

#define SDB_PG_LOGICAL_TYPES_NO_FACTORY

#include "basics/assert.h"
#include "basics/dtoa.h"
#include "basics/logger/logger.h"
#include "basics/misc.hpp"
#include "connector/pg_logical_types.h"
#include "pg/errcodes.h"
#include "pg/pg_types.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "query/config.h"

namespace sdb::pg {
namespace {

#define RETURN_SERIALIZATION(serialize_text, serialize_binary)         \
  return format == VarFormat::Text ? SerializeNullable<serialize_text> \
                                   : SerializeNullable<serialize_binary>

enum class ArrayKind {
  ListSingleDimension,
  ArraySingleDimension,
  MultiDimensions,
};

#define RETURN_ARRAY_SERIALIZATION(serialize_text, serialize_binary, oid)     \
  switch (kind) {                                                             \
    case ArrayKind::ListSingleDimension:                                      \
      return format == VarFormat::Text                                        \
               ? SerializeNullable<                                           \
                   SerializeOneDimArray<serialize_text, oid, VarFormat::Text, \
                                        ArrayKind::ListSingleDimension>>      \
               : SerializeNullable<SerializeOneDimArray<                      \
                   serialize_binary, oid, VarFormat::Binary,                  \
                   ArrayKind::ListSingleDimension>>;                          \
    case ArrayKind::ArraySingleDimension:                                     \
      return format == VarFormat::Text                                        \
               ? SerializeNullable<                                           \
                   SerializeOneDimArray<serialize_text, oid, VarFormat::Text, \
                                        ArrayKind::ArraySingleDimension>>     \
               : SerializeNullable<SerializeOneDimArray<                      \
                   serialize_binary, oid, VarFormat::Binary,                  \
                   ArrayKind::ArraySingleDimension>>;                         \
    case ArrayKind::MultiDimensions:                                          \
      return format == VarFormat::Text                                        \
               ? SerializeNullable<                                           \
                   SerializeArray<serialize_text, oid, VarFormat::Text>>      \
               : SerializeNullable<                                           \
                   SerializeArray<serialize_binary, oid, VarFormat::Binary>>; \
  }

template<SerializationFunction ValueSerialization>
void SerializeNullable(SerializationContext context,
                       const duckdb::RecursiveUnifiedVectorFormat& vdata,
                       duckdb::idx_t row) {
  auto* length_data = context.buffer->GetContiguousData(4);
  if (!vdata.unified.validity.RowIsValid(vdata.unified.sel->get_index(row))) {
    absl::big_endian::Store32(length_data, -1);
  } else {
    const auto uncommitted_size = context.buffer->GetUncommittedSize();
    ValueSerialization(context, vdata, row);
    absl::big_endian::Store32(
      length_data, context.buffer->GetUncommittedSize() - uncommitted_size);
  }
}

void SerializeNull(SerializationContext context,
                   const duckdb::RecursiveUnifiedVectorFormat&, duckdb::idx_t) {
  absl::big_endian::Store32(context.buffer->GetContiguousData(4), -1);
}

template<VarFormat Format, typename T, bool Precise = true>
void SerializeFloat(SerializationContext context,
                    const duckdb::RecursiveUnifiedVectorFormat& vdata,
                    duckdb::idx_t row) {
  static_assert(std::is_same_v<T, float> || std::is_same_v<T, double>);

  auto value = vdata.unified.GetData<T>()[vdata.unified.sel->get_index(row)];
  // Postgres converts -0.0 as 0.0
  if (value == 0) {
    value = 0;
  }

  if constexpr (Format == VarFormat::Text) {
    context.buffer->WriteContiguousData(
      basics::kNumberStrMaxLen, [&](auto* data) {
        char* buf = reinterpret_cast<char*>(data);

        if (char* ptr =
              basics::dtoa_literals<basics::kPgDtoaLiterals>(value, buf)) {
          return static_cast<size_t>(ptr - buf);
        }

        if constexpr (Precise) {
          char* ptr = basics::dtoa_fast(value, buf);
          return static_cast<size_t>(ptr - buf);
        } else {
          int num_of_digits =
            std::numeric_limits<T>::digits10 + context.extra_float_digits;
          if constexpr (std::is_same_v<float, T>) {
            num_of_digits = std::max(0, num_of_digits);
          } else {
            SDB_ASSERT(num_of_digits >= 0);
          }

          const auto r =
            std::to_chars(buf, buf + basics::kNumberStrMaxLen, value,
                          std::chars_format::general, num_of_digits);
          SDB_ASSERT(r);
          return static_cast<size_t>(r.ptr - buf);
        }
      });
  } else {
    absl::big_endian::Store(context.buffer->GetContiguousData(sizeof(T)),
                            value);
  }
}

template<VarFormat Format, typename Read, typename Wire = Read>
void SerializeInt(SerializationContext context,
                  const duckdb::RecursiveUnifiedVectorFormat& vdata,
                  duckdb::idx_t row) {
  const auto value =
    vdata.unified.GetData<Read>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    context.buffer->WriteContiguousData(basics::kIntStrMaxLen, [&](auto* data) {
      char* buf = reinterpret_cast<char*>(data);
      char* ptr = absl::numbers_internal::FastIntToBuffer(value, buf);
      return static_cast<size_t>(ptr - buf);
    });
  } else {
    absl::big_endian::Store(context.buffer->GetContiguousData(sizeof(Wire)),
                            static_cast<Wire>(value));
  }
}

// See https://www.postgresql.org/docs/current/arrays.html#ARRAYS-IO
// The array output routine will put double quotes around element value if it
// * is empty string
// * equals to NULL (case insensitive)
// Or contains
// * curly braces
// * delimiter characters(comma)
// * double quotes
// * backslashes
// * space
bool ArrayItemNeedQuotesAndEscape(std::string_view data) {
  return data.empty() || absl::EqualsIgnoreCase(data, "null") ||
         absl::c_any_of(data, [](char c) {
           return c == '{' || c == '}' || c == ',' || c == '"' || c == '\\' ||
                  absl::ascii_isspace(c);
         });
}

void WriteArrayItemQuotedAndEscaped(std::string_view item,
                                    SerializationContext context) {
  const auto required_size =
    2                   // quotes in the beginning and in the end
    + item.size() * 2;  // in worst case each symbol may be escaped
  context.buffer->WriteContiguousData(required_size, [&](uint8_t* data) {
    char* buf = reinterpret_cast<char*>(data);
    *buf++ = '"';
    for (char c : item) {
      if (c == '"' || c == '\\') [[unlikely]] {
        *buf++ = '\\';
      }
      *buf++ = c;
    }
    *buf++ = '"';
    return buf - reinterpret_cast<char*>(data);
  });
}

template<VarFormat Format, bool InArray>
void SerializeVarchar(SerializationContext context,
                      const duckdb::RecursiveUnifiedVectorFormat& vdata,
                      duckdb::idx_t row) {
  auto raw = vdata.unified
               .GetData<duckdb::string_t>()[vdata.unified.sel->get_index(row)];
  auto value = std::string_view{raw.GetData(), raw.GetSize()};
  if constexpr (Format == VarFormat::Text && InArray) {
    if (ArrayItemNeedQuotesAndEscape(value)) {
      WriteArrayItemQuotedAndEscaped(value, context);
    } else {
      context.buffer->WriteUncommitted(value);
    }
  } else {
    context.buffer->WriteUncommitted(value);
  }
}

template<VarFormat Format, bool InArray, typename T>
void SerializeEnumLabel(SerializationContext context,
                        const duckdb::RecursiveUnifiedVectorFormat& vdata,
                        duckdb::idx_t row) {
  auto idx = vdata.unified.sel->get_index(row);
  auto ordinal = duckdb::UnifiedVectorFormat::GetData<T>(vdata.unified)[idx];
  auto label = duckdb::EnumType::GetString(vdata.logical_type, ordinal);
  auto value = std::string_view{label.GetData(), label.GetSize()};
  if constexpr (Format == VarFormat::Text && InArray) {
    if (ArrayItemNeedQuotesAndEscape(value)) {
      WriteArrayItemQuotedAndEscaped(value, context);
      return;
    }
  }
  context.buffer->WriteUncommitted(value);
}

// Encode a value into PG numeric binary format.
// value is the unscaled integer (e.g. 12345 for 123.45 with scale=2).
// Use scale=0 for integer types. Caller must convert duckdb::hugeint_t /
// uhugeint_t to absl::int128 / absl::uint128 before calling.
template<typename T>
void WriteAsNumericBinary(SerializationContext context, T value,
                          int32_t scale) {
  static constexpr int32_t kBase = 10'000;
  static constexpr int16_t kPositive = 0x0000;
  static constexpr int16_t kNegative = 0x4000;
  static constexpr int16_t kPowersOfTen[] = {1, 10, 100, 1000};

  int16_t extra_digits = static_cast<int16_t>((4 - (scale % 4)) % 4);
  auto extra_base = kPowersOfTen[extra_digits];

  int16_t sign = kPositive;
  if constexpr (std::numeric_limits<T>::is_signed) {
    if (value < T{0}) {
      sign = kNegative;
      value = -value;
    }
  }

  int16_t ndigits = [extra_base](auto v) -> int16_t {
    if (v == T{0}) {
      return 0;
    }
    int16_t n = 0;
    if (extra_base != 1) {
      ++n;
      v /= static_cast<T>(kBase / extra_base);
    }
    for (; v != T{0}; v /= static_cast<T>(kBase)) {
      ++n;
    }
    return n;
  }(value);

  auto weight = static_cast<int16_t>(ndigits - ((scale + 3) / 4) - 1);
  auto* data = context.buffer->GetContiguousData(8 + ndigits * 2);
  absl::big_endian::Store16(data, ndigits);
  absl::big_endian::Store16(data + 2, weight);
  absl::big_endian::Store16(data + 4, sign);
  absl::big_endian::Store16(data + 6, static_cast<int16_t>(scale));
  data += 8 + ndigits * 2;

  if (extra_base != 1 && value != T{0}) {
    data -= 2;
    ndigits--;
    auto digit =
      (value % static_cast<T>(kBase / extra_base)) * static_cast<T>(extra_base);
    absl::big_endian::Store16(data, static_cast<int16_t>(digit));
    value /= static_cast<T>(kBase / extra_base);
  }
  while (value != T{0}) {
    data -= 2;
    ndigits--;
    absl::big_endian::Store16(
      data, static_cast<int16_t>(value % static_cast<T>(kBase)));
    value /= static_cast<T>(kBase);
  }
  SDB_ASSERT(ndigits == 0);
}

template<VarFormat Format, typename PhysicalType>
void SerializeDecimal(SerializationContext context,
                      const duckdb::RecursiveUnifiedVectorFormat& vdata,
                      duckdb::idx_t row) {
  const auto& type = vdata.logical_type;
  auto precision = duckdb::DecimalType::GetWidth(type);
  auto scale = duckdb::DecimalType::GetScale(type);
  auto value =
    vdata.unified.GetData<PhysicalType>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    auto str = duckdb::Value::DECIMAL(value, precision, scale).ToString();
    context.buffer->WriteUncommitted(str);
  } else {
    if constexpr (std::is_same_v<PhysicalType, duckdb::hugeint_t>) {
      WriteAsNumericBinary(context, absl::MakeInt128(value.upper, value.lower),
                           scale);
    } else {
      WriteAsNumericBinary(context, value, scale);
    }
  }
}

template<VarFormat Format>
void SerializeUbigint(SerializationContext context,
                      const duckdb::RecursiveUnifiedVectorFormat& vdata,
                      duckdb::idx_t row) {
  const auto value =
    vdata.unified.GetData<uint64_t>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    context.buffer->WriteContiguousData(basics::kIntStrMaxLen, [&](auto* data) {
      char* buf = reinterpret_cast<char*>(data);
      char* ptr = absl::numbers_internal::FastIntToBuffer(value, buf);
      return static_cast<size_t>(ptr - buf);
    });
  } else {
    WriteAsNumericBinary(context, value, 0);
  }
}

template<VarFormat Format>
void SerializeHugeint(SerializationContext context,
                      const duckdb::RecursiveUnifiedVectorFormat& vdata,
                      duckdb::idx_t row) {
  auto value =
    vdata.unified
      .GetData<duckdb::hugeint_t>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    context.buffer->WriteContiguousData(
      absl::numbers_internal::kFastToBuffer128Size, [&](auto* data) {
        char* buf = reinterpret_cast<char*>(data);
        char* ptr = absl::numbers_internal::FastIntToBuffer(
          absl::MakeInt128(value.upper, value.lower), buf);
        return static_cast<size_t>(ptr - buf);
      });
  } else {
    WriteAsNumericBinary(context, absl::MakeInt128(value.upper, value.lower),
                         0);
  }
}

template<VarFormat Format>
void SerializeUhugeint(SerializationContext context,
                       const duckdb::RecursiveUnifiedVectorFormat& vdata,
                       duckdb::idx_t row) {
  const auto value =
    vdata.unified
      .GetData<duckdb::uhugeint_t>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    context.buffer->WriteContiguousData(
      absl::numbers_internal::kFastToBuffer128Size, [&](auto* data) {
        char* buf = reinterpret_cast<char*>(data);
        char* ptr = absl::numbers_internal::FastIntToBuffer(
          absl::MakeUint128(value.upper, value.lower), buf);
        return static_cast<size_t>(ptr - buf);
      });
  } else {
    WriteAsNumericBinary(context, absl::MakeUint128(value.upper, value.lower),
                         0);
  }
}

template<bool InArray>
void SerializeByteaTextHex(SerializationContext context,
                           const duckdb::RecursiveUnifiedVectorFormat& vdata,
                           duckdb::idx_t row) {
  auto raw = vdata.unified
               .GetData<duckdb::string_t>()[vdata.unified.sel->get_index(row)];
  auto value = std::string_view{raw.GetData(), raw.GetSize()};
  const auto required_size = (InArray ? 3 : 0) + 2 + 2 * value.size();
  auto* buf =
    reinterpret_cast<char*>(context.buffer->GetContiguousData(required_size));

  ByteaOutHex<InArray>(buf, value);
}

template<bool InArray>
void SerializeByteaTextEscape(SerializationContext context,
                              const duckdb::RecursiveUnifiedVectorFormat& vdata,
                              duckdb::idx_t row) {
  auto raw = vdata.unified
               .GetData<duckdb::string_t>()[vdata.unified.sel->get_index(row)];
  auto value = std::string_view{raw.GetData(), raw.GetSize()};

  const auto required_size = ByteaOutEscapeLength<InArray>(value);
  auto* buf =
    reinterpret_cast<char*>(context.buffer->GetContiguousData(required_size));

  irs::ResolveBool(InArray && required_size != value.size(),
                   [&]<bool NeedArrayEscaping> {
                     ByteaOutEscape<NeedArrayEscaping>(buf, value);
                   });
}

void SerializeByteaBinary(SerializationContext context,
                          const duckdb::RecursiveUnifiedVectorFormat& vdata,
                          duckdb::idx_t row) {
  auto raw = vdata.unified
               .GetData<duckdb::string_t>()[vdata.unified.sel->get_index(row)];
  auto value = std::string_view{raw.GetData(), raw.GetSize()};
  context.buffer->WriteUncommitted(value);
}

template<VarFormat Format>
void SerializeBool(SerializationContext context,
                   const duckdb::RecursiveUnifiedVectorFormat& vdata,
                   duckdb::idx_t row) {
  auto value = vdata.unified.GetData<bool>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    context.buffer->WriteUncommitted(value ? "t" : "f");
  } else {
    auto* ptr =
      reinterpret_cast<bool*>(context.buffer->GetContiguousData(sizeof(bool)));
    *ptr = value;
  }
}

template<VarFormat Format>
void SerializeTimestampSec(SerializationContext context,
                           const duckdb::RecursiveUnifiedVectorFormat& vdata,
                           duckdb::idx_t row) {
  const auto timestamp =
    vdata.unified
      .GetData<duckdb::timestamp_sec_t>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    auto str = duckdb::Timestamp::ToString(
      duckdb::Timestamp::FromEpochSeconds(timestamp.value));
    context.buffer->WriteUncommitted(str);
  } else {
    absl::big_endian::Store64(context.buffer->GetContiguousData(8),
                              (timestamp.value - kGapSec) * 1'000'000);
  }
}

template<VarFormat Format>
void SerializeTimestampMs(SerializationContext context,
                          const duckdb::RecursiveUnifiedVectorFormat& vdata,
                          duckdb::idx_t row) {
  const auto timestamp =
    vdata.unified
      .GetData<duckdb::timestamp_ms_t>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    auto str = duckdb::Timestamp::ToString(
      duckdb::Timestamp::FromEpochMicroSeconds(timestamp.value));
    context.buffer->WriteUncommitted(str);
  } else {
    absl::big_endian::Store64(context.buffer->GetContiguousData(8),
                              (timestamp.value - kGapMs) * 1000);
  }
}

template<VarFormat Format>
void SerializeTimestamp(SerializationContext context,
                        const duckdb::RecursiveUnifiedVectorFormat& vdata,
                        duckdb::idx_t row) {
  const auto timestamp =
    vdata.unified
      .GetData<duckdb::timestamp_t>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    auto str = duckdb::Timestamp::ToString(timestamp);
    context.buffer->WriteUncommitted(str);
  } else {
    absl::big_endian::Store64(context.buffer->GetContiguousData(8),
                              timestamp.value - kGapUs);
  }
}

template<VarFormat Format>
void SerializeTimestampNs(SerializationContext context,
                          const duckdb::RecursiveUnifiedVectorFormat& vdata,
                          duckdb::idx_t row) {
  const auto timestamp =
    vdata.unified
      .GetData<duckdb::timestamp_ns_t>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    auto str = duckdb::Timestamp::ToString(
      duckdb::Timestamp::FromEpochNanoSeconds(timestamp.value));
    context.buffer->WriteUncommitted(str);
  } else {
    absl::big_endian::Store64(context.buffer->GetContiguousData(8),
                              (timestamp.value - kGapNs) / 1000);
  }
}

template<VarFormat Format>
void SerializeTimestampTz(SerializationContext context,
                          const duckdb::RecursiveUnifiedVectorFormat& vdata,
                          duckdb::idx_t row) {
  const auto ts =
    vdata.unified
      .GetData<duckdb::timestamp_tz_t>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    auto str = duckdb::Timestamp::ToString(ts);
    context.buffer->WriteUncommitted(str);
    context.buffer->WriteUncommitted("+00");
  } else {
    absl::big_endian::Store64(context.buffer->GetContiguousData(8),
                              ts.value - kGapUs);
  }
}

template<VarFormat Format>
void SerializeTime(SerializationContext context,
                   const duckdb::RecursiveUnifiedVectorFormat& vdata,
                   duckdb::idx_t row) {
  const auto time =
    vdata.unified.GetData<duckdb::dtime_t>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    auto str = duckdb::Time::ToString(time);
    context.buffer->WriteUncommitted(str);
  } else {
    absl::big_endian::Store64(context.buffer->GetContiguousData(8),
                              time.micros);
  }
}

template<VarFormat Format>
void SerializeTimeNs(SerializationContext context,
                     const duckdb::RecursiveUnifiedVectorFormat& vdata,
                     duckdb::idx_t row) {
  const auto time =
    vdata.unified
      .GetData<duckdb::dtime_ns_t>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    auto str = duckdb::Time::ToString(time.time());
    context.buffer->WriteUncommitted(str);
  } else {
    absl::big_endian::Store64(context.buffer->GetContiguousData(8),
                              time.time().micros);
  }
}

template<VarFormat Format>
void SerializeTimeTz(SerializationContext context,
                     const duckdb::RecursiveUnifiedVectorFormat& vdata,
                     duckdb::idx_t row) {
  const auto tz =
    vdata.unified
      .GetData<duckdb::dtime_tz_t>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    // Format: HH:MM:SS[.mmm][±HH:MM]
    auto time_str = duckdb::Time::ToString(tz.time());
    context.buffer->WriteUncommitted(time_str);
    const auto offset_secs = tz.offset();
    const bool negative = offset_secs < 0;
    const auto abs_offset = negative ? -offset_secs : offset_secs;
    const auto offset_h = abs_offset / 3600;
    const auto offset_m = (abs_offset % 3600) / 60;
    context.buffer->WriteContiguousData(6, [&](auto* data) {
      char* buf = reinterpret_cast<char*>(data);
      *buf++ = negative ? '-' : '+';
      *buf++ = '0' + offset_h / 10;
      *buf++ = '0' + offset_h % 10;
      *buf++ = ':';
      *buf++ = '0' + offset_m / 10;
      *buf++ = '0' + offset_m % 10;
      return size_t{6};
    });
  } else {
    // PG binary: int64 time_micros + int32 zone (seconds WEST of UTC).
    // DuckDB offset() is seconds EAST, so negate.
    auto* data = context.buffer->GetContiguousData(12);
    absl::big_endian::Store64(data, tz.time().micros);
    absl::big_endian::Store32(data + 8, -tz.offset());
  }
}

template<VarFormat Format>
void SerializeBit(SerializationContext context,
                  const duckdb::RecursiveUnifiedVectorFormat& vdata,
                  duckdb::idx_t row) {
  const auto raw =
    vdata.unified
      .GetData<duckdb::string_t>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    // DuckDB Bit::ToString gives "01001..." string
    auto str = duckdb::Bit::ToString(raw);
    context.buffer->WriteUncommitted(str);
  } else {
    // PG binary: int32 nBits + ceil(nBits/8) packed bytes MSB-first
    // DuckDB internal: byte[0]=padding count, byte[1..N]=packed bits MSB-first
    const auto n_bits = static_cast<int32_t>(duckdb::Bit::BitLength(raw));
    const auto n_bytes = (n_bits + 7) / 8;
    auto* data = context.buffer->GetContiguousData(4 + n_bytes);
    absl::big_endian::Store32(data, n_bits);
    // raw.GetData()[0] is padding, [1..n_bytes] are the bit data
    memcpy(data + 4, raw.GetData() + 1, n_bytes);
  }
}

template<SerializationFunction ElementSerialization, int32_t ElementOID,
         VarFormat Format, ArrayKind Kind>
void SerializeOneDimArray(SerializationContext context,
                          const duckdb::RecursiveUnifiedVectorFormat& vdata,
                          duckdb::idx_t row) {
  duckdb::idx_t array_size;
  duckdb::idx_t array_offset;
  if constexpr (Kind == ArrayKind::ArraySingleDimension) {
    array_size = duckdb::ArrayType::GetSize(vdata.logical_type);
    array_offset = row * array_size;
  } else {
    auto list_data =
      vdata.unified
        .GetData<duckdb::list_entry_t>()[vdata.unified.sel->get_index(row)];
    array_size = list_data.length;
    array_offset = list_data.offset;
  }
  auto& child_vdata = vdata.children[0];
  if constexpr (Format == VarFormat::Text) {
    context.buffer->WriteUncommitted("{");
    for (duckdb::idx_t i = 0; i < array_size; ++i) {
      if (i > 0) {
        context.buffer->WriteUncommitted(",");
      }
      const auto element_row = array_offset + i;
      if (!child_vdata.unified.validity.RowIsValid(
            child_vdata.unified.sel->get_index(element_row))) {
        context.buffer->WriteUncommitted("NULL");
      } else {
        ElementSerialization(context, child_vdata, element_row);
      }
    }
    context.buffer->WriteUncommitted("}");
  } else {
    // dimensions (4) - amount of array dims
    // flags(4) - 0(no nulls), 1(have nulls)
    // element_oid (4) - oid of an array element
    // dim1 size (4) - size of first(and only) dim
    // lower_bound (4) - begin offset(0 by default)
    auto* prefix_data = context.buffer->GetContiguousData(20);
    absl::big_endian::Store32(prefix_data, 1);
    absl::big_endian::Store32(prefix_data + 4, 1);
    absl::big_endian::Store32(prefix_data + 8, ElementOID);
    absl::big_endian::Store32(prefix_data + 12, array_size);
    absl::big_endian::Store32(prefix_data + 16, 0);
    for (duckdb::idx_t i = 0; i < array_size; ++i) {
      const auto element_row = array_offset + i;
      SerializeNullable<ElementSerialization>(context, child_vdata,
                                              element_row);
    }
  }
}

// Multi-dim array serialization (text only for now, binary uses FlattenArray)
template<SerializationFunction ElementSerialization, VarFormat Format,
         bool First = true>
int32_t FlattenArray(SerializationContext context,
                     const duckdb::RecursiveUnifiedVectorFormat& vdata,
                     duckdb::idx_t row) {
  const auto lid = vdata.logical_type.id();
  if (lid != duckdb::LogicalTypeId::LIST &&
      lid != duckdb::LogicalTypeId::ARRAY) {
    SerializeNullable<ElementSerialization>(context, vdata, row);
    return 0;
  }
  duckdb::idx_t array_size;
  duckdb::idx_t array_offset;
  if (lid == duckdb::LogicalTypeId::ARRAY) {
    array_size = duckdb::ArrayType::GetSize(vdata.logical_type);
    array_offset = row * array_size;
  } else {
    auto list_data =
      vdata.unified
        .GetData<duckdb::list_entry_t>()[vdata.unified.sel->get_index(row)];
    array_size = list_data.length;
    array_offset = list_data.offset;
  }
  auto& child_vdata = vdata.children[0];
  if constexpr (First) {
    auto* prefix_data = context.buffer->GetContiguousData(8);
    absl::big_endian::Store32(prefix_data + 4, 0);
    absl::big_endian::Store32(prefix_data, array_size);
  }
  if (array_size == 0) {
    return 1;
  }
  duckdb::idx_t i = 0;
  int32_t dims = -1;
  if constexpr (First) {
    dims = FlattenArray<ElementSerialization, Format>(context, child_vdata,
                                                      array_offset + i) +
           1;
    i++;
  }
  for (; i < array_size; ++i) {
    auto element_row = array_offset + i;
    const auto inner_dim = FlattenArray<ElementSerialization, Format, false>(
      context, child_vdata, element_row);
    SDB_ASSERT(dims == -1 || dims == inner_dim + 1);
    dims = inner_dim + 1;
  }
  SDB_ASSERT(dims > 0);
  return dims;
}

template<SerializationFunction ElementSerialization, int32_t ElementOID,
         VarFormat Format>
void SerializeArray(SerializationContext context,
                    const duckdb::RecursiveUnifiedVectorFormat& vdata,
                    duckdb::idx_t row) {
  if constexpr (Format == VarFormat::Text) {
    const auto lid = vdata.logical_type.id();
    if (lid != duckdb::LogicalTypeId::LIST &&
        lid != duckdb::LogicalTypeId::ARRAY) {
      if (!vdata.unified.validity.RowIsValid(
            vdata.unified.sel->get_index(row))) {
        context.buffer->WriteUncommitted("NULL");
      } else {
        ElementSerialization(context, vdata, row);
      }
      return;
    }
    duckdb::idx_t array_size;
    duckdb::idx_t array_offset;
    if (lid == duckdb::LogicalTypeId::ARRAY) {
      array_size = duckdb::ArrayType::GetSize(vdata.logical_type);
      array_offset = row * array_size;
    } else {
      auto list_data =
        vdata.unified
          .GetData<duckdb::list_entry_t>()[vdata.unified.sel->get_index(row)];
      array_size = list_data.length;
      array_offset = list_data.offset;
    }
    auto& child_vdata = vdata.children[0];
    context.buffer->WriteUncommitted("{");
    for (duckdb::idx_t i = 0; i < array_size; ++i) {
      if (i > 0) {
        context.buffer->WriteUncommitted(",");
      }
      const auto element_row = array_offset + i;
      SerializeArray<ElementSerialization, ElementOID, Format>(
        context, child_vdata, element_row);
    }
    context.buffer->WriteUncommitted("}");
  } else {
    auto* prefix_data = context.buffer->GetContiguousData(12);
    absl::big_endian::Store32(prefix_data + 4, 0);
    absl::big_endian::Store32(prefix_data + 8, ElementOID);
    const auto dims =
      FlattenArray<ElementSerialization, Format>(context, vdata, row);
    absl::big_endian::Store32(prefix_data, dims);
  }
}

template<VarFormat Format>
void SerializeDate(SerializationContext context,
                   const duckdb::RecursiveUnifiedVectorFormat& vdata,
                   duckdb::idx_t row) {
  // days from 1970-01-01
  auto days =
    vdata.unified.GetData<duckdb::date_t>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    // TODO(mkornaukhov) support BC date and add some validation for dates
    // Format is "%04d-%02d-%02d", max year is 5874897
    static constexpr size_t kMaxDateStrSize = 7 + 1 + 2 + 1 + 2;

    absl::CivilDay date{1970, 1, 1};
    date += days.days;

    context.buffer->WriteContiguousData(kMaxDateStrSize, [&](auto* data) {
      char* buf = reinterpret_cast<char*>(data);
      char year_buf[absl::numbers_internal::kFastToBufferSize];
      auto* const year_end =
        absl::numbers_internal::FastIntToBuffer(date.year(), year_buf);

      const auto year_digits = year_end - year_buf;
      const auto extra_pad = 4 - year_digits;
      if (extra_pad > 0) {
        std::memset(buf, '0', extra_pad);
        buf += extra_pad;
      }

      std::memcpy(buf, year_buf, year_digits);
      buf += year_digits;
      *buf++ = '-';

      const auto month_div = std::div(date.month(), 10);
      *buf++ = '0' + month_div.quot;
      *buf++ = '0' + month_div.rem;
      *buf++ = '-';

      const auto day_div = std::div(date.day(), 10);
      *buf++ = '0' + day_div.quot;
      *buf++ = '0' + day_div.rem;

      return buf - reinterpret_cast<char*>(data);
    });
  } else {
    absl::big_endian::Store32(context.buffer->GetContiguousData(4),
                              static_cast<int32_t>(days.days - kGapDays));
  }
}

void SerializeRegtypeText(SerializationContext context,
                          const duckdb::RecursiveUnifiedVectorFormat& vdata,
                          duckdb::idx_t row) {
  const auto oid =
    vdata.unified.GetData<int64_t>()[vdata.unified.sel->get_index(row)];
  context.buffer->WriteUncommitted(RegtypeOut(oid));
}

void SerializeRegclassText(SerializationContext context,
                           const duckdb::RecursiveUnifiedVectorFormat& vdata,
                           duckdb::idx_t row) {
  const auto oid =
    vdata.unified.GetData<int64_t>()[vdata.unified.sel->get_index(row)];
  context.buffer->WriteUncommitted(RegclassOut(*context.snapshot, oid));
}

void SerializeRegnamespaceText(
  SerializationContext context,
  const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t row) {
  const auto oid =
    vdata.unified.GetData<int64_t>()[vdata.unified.sel->get_index(row)];
  context.buffer->WriteUncommitted(RegnamespaceOut(*context.snapshot, oid));
}

// Binary serialization for oid-like types:
// truncate 64-bit OID to 32-bit for PG wire protocol compatibility.
void SerializeOidBinary(SerializationContext context,
                        const duckdb::RecursiveUnifiedVectorFormat& vdata,
                        duckdb::idx_t row) {
  const auto oid =
    vdata.unified.GetData<int64_t>()[vdata.unified.sel->get_index(row)];
  if (oid != static_cast<int32_t>(oid)) {
    SDB_WARN("xxxxx", Logger::COMMUNICATION, "reg* OID ", oid,
             " truncated to 32-bit for binary wire protocol");
  }
  absl::big_endian::Store32(context.buffer->GetContiguousData(4),
                            static_cast<int32_t>(oid));
}

template<VarFormat Format>
void SerializeInterval(SerializationContext context,
                       const duckdb::RecursiveUnifiedVectorFormat& vdata,
                       duckdb::idx_t row) {
  const auto interval =
    vdata.unified
      .GetData<duckdb::interval_t>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    auto str = duckdb::Interval::ToString(interval);
    context.buffer->WriteUncommitted(str);
  } else {
    // PG binary: microseconds(8) + days(4) + months(4)
    auto* data = context.buffer->GetContiguousData(16);
    absl::big_endian::Store64(data, interval.micros);
    absl::big_endian::Store32(data + 8, interval.days);
    absl::big_endian::Store32(data + 12, interval.months);
  }
}

template<VarFormat Format>
void SerializeUuid(SerializationContext context,
                   const duckdb::RecursiveUnifiedVectorFormat& vdata,
                   duckdb::idx_t row) {
  const auto uuid =
    vdata.unified
      .GetData<duckdb::hugeint_t>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    static constexpr size_t kUUIDStrSize = 36;  // 8-4-4-4-12
    auto* data = context.buffer->GetContiguousData(kUUIDStrSize);
    duckdb::BaseUUID::ToString(uuid, reinterpret_cast<char*>(data));
  } else {
    // Binary format: flip top bit back to get original UUID bytes
    auto* data = context.buffer->GetContiguousData(16);
    const uint64_t high =
      static_cast<uint64_t>(uuid.upper) ^ (uint64_t{1} << 63);
    absl::big_endian::Store64(data, high);
    absl::big_endian::Store64(data + 8, uuid.lower);
  }
}

template<VarFormat Format, bool InArray>
void SerializeJson(SerializationContext context,
                   const duckdb::RecursiveUnifiedVectorFormat& vdata,
                   duckdb::idx_t row) {
  const auto str =
    vdata.unified
      .GetData<duckdb::string_t>()[vdata.unified.sel->get_index(row)];
  auto value = std::string_view{str.GetData(), str.GetSize()};
  if constexpr (InArray && Format == VarFormat::Text) {
    if (ArrayItemNeedQuotesAndEscape(value)) {
      WriteArrayItemQuotedAndEscaped(value, context);
      return;
    }
  }
  context.buffer->WriteUncommitted(value);
}

SerializationFunction GetArraySerialization(const duckdb::LogicalType& type,
                                            VarFormat format,
                                            SerializationContext& context,
                                            ArrayKind kind) {
  switch (type.id()) {
    using enum duckdb::LogicalTypeId;
    using enum PgTypeOID;
    case BOOLEAN:
      RETURN_ARRAY_SERIALIZATION(SerializeBool<VarFormat::Text>,
                                 SerializeBool<VarFormat::Binary>, kBool);
    case TINYINT: {
      static constexpr auto kSerializeText =
        SerializeInt<VarFormat::Text, int8_t, int16_t>;
      static constexpr auto kSerializeBinary =
        SerializeInt<VarFormat::Binary, int8_t, int16_t>;
      RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, kInt2);
    }
    case UTINYINT: {
      static constexpr auto kSerializeText =
        SerializeInt<VarFormat::Text, uint8_t, int16_t>;
      static constexpr auto kSerializeBinary =
        SerializeInt<VarFormat::Binary, uint8_t, int16_t>;
      RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, kInt2);
    }
    case SMALLINT: {
      static constexpr auto kSerializeText =
        SerializeInt<VarFormat::Text, int16_t>;
      static constexpr auto kSerializeBinary =
        SerializeInt<VarFormat::Binary, int16_t>;
      RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, kInt2);
    }
    case USMALLINT: {
      static constexpr auto kSerializeText =
        SerializeInt<VarFormat::Text, uint16_t, int32_t>;
      static constexpr auto kSerializeBinary =
        SerializeInt<VarFormat::Binary, uint16_t, int32_t>;
      RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, kInt4);
    }
    case INTEGER: {
      static constexpr auto kSerializeText =
        SerializeInt<VarFormat::Text, int32_t>;
      static constexpr auto kSerializeBinary =
        SerializeInt<VarFormat::Binary, int32_t>;
      RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, kInt4);
    }
    case UINTEGER: {
      static constexpr auto kSerializeText =
        SerializeInt<VarFormat::Text, uint32_t, int64_t>;
      static constexpr auto kSerializeBinary =
        SerializeInt<VarFormat::Binary, uint32_t, int64_t>;
      RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, kInt8);
    }
    case BIGINT: {
      if (IsRegtype(type)) {
        RETURN_ARRAY_SERIALIZATION(SerializeRegtypeText, SerializeOidBinary,
                                   kRegtype);
      }
      if (IsRegclass(type)) {
        RETURN_ARRAY_SERIALIZATION(SerializeRegclassText, SerializeOidBinary,
                                   kRegclass);
      }
      if (IsRegnamespace(type)) {
        RETURN_ARRAY_SERIALIZATION(SerializeRegnamespaceText,
                                   SerializeOidBinary, kRegnamespace);
      }
      static constexpr auto kSerializeText =
        SerializeInt<VarFormat::Text, int64_t>;
      if (IsOid(type)) {
        RETURN_ARRAY_SERIALIZATION(kSerializeText, SerializeOidBinary, kOid);
      }
      if (IsRegproc(type)) {
        RETURN_ARRAY_SERIALIZATION(kSerializeText, SerializeOidBinary,
                                   kRegproc);
      }
      if (IsRegprocedure(type)) {
        RETURN_ARRAY_SERIALIZATION(kSerializeText, SerializeOidBinary,
                                   kRegprocedure);
      }
      if (IsRegoper(type)) {
        RETURN_ARRAY_SERIALIZATION(kSerializeText, SerializeOidBinary,
                                   kRegoper);
      }
      if (IsRegoperator(type)) {
        RETURN_ARRAY_SERIALIZATION(kSerializeText, SerializeOidBinary,
                                   kRegoperator);
      }
      if (IsRegrole(type)) {
        RETURN_ARRAY_SERIALIZATION(kSerializeText, SerializeOidBinary,
                                   kRegrole);
      }
      if (IsRegconfig(type)) {
        RETURN_ARRAY_SERIALIZATION(kSerializeText, SerializeOidBinary,
                                   kRegconfig);
      }
      if (IsRegdictionary(type)) {
        RETURN_ARRAY_SERIALIZATION(kSerializeText, SerializeOidBinary,
                                   kRegdictionary);
      }
      if (IsRegcollation(type)) {
        RETURN_ARRAY_SERIALIZATION(kSerializeText, SerializeOidBinary,
                                   kRegcollation);
      }
      if (IsXid(type)) {
        RETURN_ARRAY_SERIALIZATION(kSerializeText, SerializeOidBinary, kXid);
      }
      if (IsCid(type)) {
        RETURN_ARRAY_SERIALIZATION(kSerializeText, SerializeOidBinary, kCid);
      }
      if (IsTid(type)) {
        RETURN_ARRAY_SERIALIZATION(kSerializeText, SerializeOidBinary, kTid);
      }
      static constexpr auto kSerializeBinary =
        SerializeInt<VarFormat::Binary, int64_t>;
      // XID8 or BIGINT
      RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, kInt8);
    }
    case UBIGINT:
      RETURN_ARRAY_SERIALIZATION(SerializeUbigint<VarFormat::Text>,
                                 SerializeUbigint<VarFormat::Binary>, kNumeric);
    case HUGEINT:
      RETURN_ARRAY_SERIALIZATION(SerializeHugeint<VarFormat::Text>,
                                 SerializeHugeint<VarFormat::Binary>, kNumeric);
    case UHUGEINT:
      RETURN_ARRAY_SERIALIZATION(SerializeUhugeint<VarFormat::Text>,
                                 SerializeUhugeint<VarFormat::Binary>,
                                 kNumeric);
    case FLOAT: {
      static constexpr auto kSerializeBinary =
        SerializeFloat<VarFormat::Binary, float>;
      return irs::ResolveBool(
        context.extra_float_digits > 0, [&]<bool Precise> {
          static constexpr auto kSerializeText =
            SerializeFloat<VarFormat::Text, float, Precise>;
          RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, kFloat4);
        });
    }
    case DOUBLE: {
      static constexpr auto kSerializeBinary =
        SerializeFloat<VarFormat::Binary, double>;
      return irs::ResolveBool(
        context.extra_float_digits > 0, [&]<bool Precise> {
          static constexpr auto kSerializeText =
            SerializeFloat<VarFormat::Text, double, Precise>;
          RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, kFloat8);
        });
    }
    case DECIMAL: {
      switch (type.InternalType()) {
        using enum duckdb::PhysicalType;
        case INT16: {
          static constexpr auto kSerializeText =
            SerializeDecimal<VarFormat::Text, int16_t>;
          static constexpr auto kSerializeBinary =
            SerializeDecimal<VarFormat::Binary, int16_t>;
          RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary,
                                     kNumeric);
        }
        case INT32: {
          static constexpr auto kSerializeText =
            SerializeDecimal<VarFormat::Text, int32_t>;
          static constexpr auto kSerializeBinary =
            SerializeDecimal<VarFormat::Binary, int32_t>;
          RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary,
                                     kNumeric);
        }
        case INT64: {
          static constexpr auto kSerializeText =
            SerializeDecimal<VarFormat::Text, int64_t>;
          static constexpr auto kSerializeBinary =
            SerializeDecimal<VarFormat::Binary, int64_t>;
          RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary,
                                     kNumeric);
        }
        case INT128: {
          static constexpr auto kSerializeText =
            SerializeDecimal<VarFormat::Text, duckdb::hugeint_t>;
          static constexpr auto kSerializeBinary =
            SerializeDecimal<VarFormat::Binary, duckdb::hugeint_t>;
          RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary,
                                     kNumeric);
        }
        default:
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
            ERR_MSG("Unsupported decimal internal type in array"));
      }
    }
    case CHAR:
    case VARCHAR: {
      if (type.IsJSONType()) {
        static constexpr auto kSerializeText =
          SerializeJson<VarFormat::Text, true>;
        static constexpr auto kSerializeBinary =
          SerializeJson<VarFormat::Binary, false>;
        RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, kJson);
      }
      if (IsName(type)) {
        static constexpr auto kSerializeText =
          SerializeVarchar<VarFormat::Text, true>;
        static constexpr auto kSerializeBinary =
          SerializeVarchar<VarFormat::Binary, false>;
        RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, kName);
      }
      static constexpr auto kSerializeText =
        SerializeVarchar<VarFormat::Text, true>;
      static constexpr auto kSerializeBinary =
        SerializeVarchar<VarFormat::Binary, false>;
      RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, kText);
    }
    case BLOB: {
      if (context.bytea_output == ByteaOutput::Hex) {
        static constexpr auto kSerializeText = SerializeByteaTextHex<true>;
        RETURN_ARRAY_SERIALIZATION(kSerializeText, SerializeByteaBinary,
                                   kBytea);
      } else {
        SDB_ASSERT(context.bytea_output == ByteaOutput::Escape);
        static constexpr auto kSerializeText = SerializeByteaTextEscape<true>;
        RETURN_ARRAY_SERIALIZATION(kSerializeText, SerializeByteaBinary,
                                   kBytea);
      }
    }
    case DATE:
      RETURN_ARRAY_SERIALIZATION(SerializeDate<VarFormat::Text>,
                                 SerializeDate<VarFormat::Binary>, kDate);
    case TIME:
      RETURN_ARRAY_SERIALIZATION(SerializeTime<VarFormat::Text>,
                                 SerializeTime<VarFormat::Binary>, kTime);
    case TIME_NS:
      RETURN_ARRAY_SERIALIZATION(SerializeTimeNs<VarFormat::Text>,
                                 SerializeTimeNs<VarFormat::Binary>, kTime);
    case TIME_TZ:
      RETURN_ARRAY_SERIALIZATION(SerializeTimeTz<VarFormat::Text>,
                                 SerializeTimeTz<VarFormat::Binary>, kTimeTz);
    case TIMESTAMP_SEC:
      RETURN_ARRAY_SERIALIZATION(SerializeTimestampSec<VarFormat::Text>,
                                 SerializeTimestampSec<VarFormat::Binary>,
                                 kTimestamp);
    case TIMESTAMP_MS:
      RETURN_ARRAY_SERIALIZATION(SerializeTimestampMs<VarFormat::Text>,
                                 SerializeTimestampMs<VarFormat::Binary>,
                                 kTimestamp);
    case TIMESTAMP:
      RETURN_ARRAY_SERIALIZATION(SerializeTimestamp<VarFormat::Text>,
                                 SerializeTimestamp<VarFormat::Binary>,
                                 kTimestamp);
    case TIMESTAMP_NS:
      RETURN_ARRAY_SERIALIZATION(SerializeTimestampNs<VarFormat::Text>,
                                 SerializeTimestampNs<VarFormat::Binary>,
                                 kTimestamp);
    case TIMESTAMP_TZ:
      RETURN_ARRAY_SERIALIZATION(SerializeTimestampTz<VarFormat::Text>,
                                 SerializeTimestampTz<VarFormat::Binary>,
                                 kTimestampTz);
    case INTERVAL:
      RETURN_ARRAY_SERIALIZATION(SerializeInterval<VarFormat::Text>,
                                 SerializeInterval<VarFormat::Binary>,
                                 kInterval);
    case UUID:
      RETURN_ARRAY_SERIALIZATION(SerializeUuid<VarFormat::Text>,
                                 SerializeUuid<VarFormat::Binary>, kUuid);
    case BIT:
      RETURN_ARRAY_SERIALIZATION(SerializeBit<VarFormat::Text>,
                                 SerializeBit<VarFormat::Binary>, kVarbit);
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("Array element type not supported"));
  }
}

}  // namespace

template<bool NeedArrayEscaping>
void ByteaOutHex(char* buf, std::string_view value) {
  if constexpr (NeedArrayEscaping) {
    *buf++ = '"';
    *buf++ = '\\';
    *(buf + 2 + 2 * value.size()) = '"';
  }
  *buf++ = '\\';
  *buf++ = 'x';

  absl::BytesToHexStringInternal(
    reinterpret_cast<const unsigned char*>(value.data()), buf, value.size());
}

template<bool InArray>
size_t ByteaOutEscapeLength(std::string_view value) {
  size_t backslash_cnt = 0;
  size_t non_printable_cnt = 0;
  for (size_t i = 0; i < value.size(); ++i) {
    if (value[i] == '\\') {
      ++backslash_cnt;
    } else if (!absl::ascii_isprint(value[i])) {
      ++non_printable_cnt;
    }
  }

  size_t required_size = value.size() + backslash_cnt + non_printable_cnt * 3;
  const bool need_escaping =
    InArray && (value.empty() || backslash_cnt > 0 || non_printable_cnt > 0);
  if constexpr (InArray) {
    if (need_escaping) {
      required_size += 2 + backslash_cnt * 2 +
                       non_printable_cnt;  // Quotes and additional backslashes
    }
  }
  return required_size;
}

template<bool NeedArrayEscaping>
void ByteaOutEscape(char* buf, std::string_view value) {
  if constexpr (NeedArrayEscaping) {
    *buf++ = '"';
  }
  for (const char* in = value.data(), *in_end = value.data() + value.size();
       in != in_end; ++in) {
    if (*in == '\\') {
      *buf++ = '\\';
      *buf++ = '\\';
      if constexpr (NeedArrayEscaping) {
        *buf++ = '\\';
        *buf++ = '\\';
      }
    } else if (!absl::ascii_isprint(*in)) {
      // As octal
      unsigned char c = *in;
      buf[0] = '\\';
      if constexpr (NeedArrayEscaping) {
        *++buf = '\\';
      }
      buf[3] = '0' + (c & 07);
      c >>= 3;
      buf[2] = '0' + (c & 07);
      c >>= 3;
      buf[1] = '0' + (c & 03);
      buf += 4;
    } else {
      *buf++ = *in;
    }
  }
  if constexpr (NeedArrayEscaping) {
    *buf++ = '"';
  }
}

template size_t ByteaOutEscapeLength<true>(std::string_view value);
template size_t ByteaOutEscapeLength<false>(std::string_view value);
template void ByteaOutHex<true>(char* buf, std::string_view value);
template void ByteaOutHex<false>(char* buf, std::string_view value);
template void ByteaOutEscape<true>(char* buf, std::string_view value);
template void ByteaOutEscape<false>(char* buf, std::string_view value);

void FillContext(const Config& config, SerializationContext& context) {
  context.extra_float_digits = config.GetExtraFloatDigits();
  context.bytea_output = config.GetByteaOutput();
  context.snapshot = config.EnsureCatalogSnapshot().get();
}

SerializationFunction GetSerialization(const duckdb::LogicalType& type,
                                       VarFormat format,
                                       SerializationContext& context) {
  switch (type.id()) {
    using enum duckdb::LogicalTypeId;
    case SQLNULL:
      return SerializeNull;
    case BOOLEAN:
      RETURN_SERIALIZATION(SerializeBool<VarFormat::Text>,
                           SerializeBool<VarFormat::Binary>);
    case TINYINT: {
      static constexpr auto kSerializeText =
        SerializeInt<VarFormat::Text, int8_t, int16_t>;
      static constexpr auto kSerializeBinary =
        SerializeInt<VarFormat::Binary, int8_t, int16_t>;
      RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
    }
    case SMALLINT: {
      static constexpr auto kSerializeText =
        SerializeInt<VarFormat::Text, int16_t>;
      static constexpr auto kSerializeBinary =
        SerializeInt<VarFormat::Binary, int16_t>;
      RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
    }
    case INTEGER: {
      static constexpr auto kSerializeText =
        SerializeInt<VarFormat::Text, int32_t>;
      static constexpr auto kSerializeBinary =
        SerializeInt<VarFormat::Binary, int32_t>;
      RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
    }
    case BIGINT: {
      if (IsRegtype(type)) {
        RETURN_SERIALIZATION(SerializeRegtypeText, SerializeOidBinary);
      }
      if (IsRegclass(type)) {
        RETURN_SERIALIZATION(SerializeRegclassText, SerializeOidBinary);
      }
      if (IsRegnamespace(type)) {
        RETURN_SERIALIZATION(SerializeRegnamespaceText, SerializeOidBinary);
      }
      static constexpr auto kSerializeText =
        SerializeInt<VarFormat::Text, int64_t>;
      if (IsOidLike(type)) {
        RETURN_SERIALIZATION(kSerializeText, SerializeOidBinary);
      }
      static constexpr auto kSerializeBinary =
        SerializeInt<VarFormat::Binary, int64_t>;
      RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
    }
    case UTINYINT: {
      static constexpr auto kSerializeText =
        SerializeInt<VarFormat::Text, uint8_t, int16_t>;
      static constexpr auto kSerializeBinary =
        SerializeInt<VarFormat::Binary, uint8_t, int16_t>;
      RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
    }
    case USMALLINT: {
      static constexpr auto kSerializeText =
        SerializeInt<VarFormat::Text, uint16_t, int32_t>;
      static constexpr auto kSerializeBinary =
        SerializeInt<VarFormat::Binary, uint16_t, int32_t>;
      RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
    }
    case UINTEGER: {
      static constexpr auto kSerializeText =
        SerializeInt<VarFormat::Text, uint32_t, int64_t>;
      static constexpr auto kSerializeBinary =
        SerializeInt<VarFormat::Binary, uint32_t, int64_t>;
      RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
    }
    case UBIGINT: {
      static constexpr auto kSerializeText = SerializeUbigint<VarFormat::Text>;
      static constexpr auto kSerializeBinary =
        SerializeUbigint<VarFormat::Binary>;
      RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
    }
    case HUGEINT: {
      static constexpr auto kSerializeText = SerializeHugeint<VarFormat::Text>;
      static constexpr auto kSerializeBinary =
        SerializeHugeint<VarFormat::Binary>;
      RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
    }
    case UHUGEINT: {
      static constexpr auto kSerializeText = SerializeUhugeint<VarFormat::Text>;
      static constexpr auto kSerializeBinary =
        SerializeUhugeint<VarFormat::Binary>;
      RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
    }
    case FLOAT: {
      static constexpr auto kSerializeBinary =
        SerializeFloat<VarFormat::Binary, float>;
      return irs::ResolveBool(
        context.extra_float_digits > 0, [&]<bool Precise> {
          static constexpr auto kSerializeText =
            SerializeFloat<VarFormat::Text, float, Precise>;
          RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
        });
    }
    case DOUBLE: {
      static constexpr auto kSerializeBinary =
        SerializeFloat<VarFormat::Binary, double>;
      return irs::ResolveBool(
        context.extra_float_digits > 0, [&]<bool Precise> {
          static constexpr auto kSerializeText =
            SerializeFloat<VarFormat::Text, double, Precise>;
          RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
        });
    }
    case DECIMAL:
      switch (type.InternalType()) {
        using enum duckdb::PhysicalType;
        case INT16: {
          static constexpr auto kSerializeText =
            SerializeDecimal<VarFormat::Text, int16_t>;
          static constexpr auto kSerializeBinary =
            SerializeDecimal<VarFormat::Binary, int16_t>;
          RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
        }
        case INT32: {
          static constexpr auto kSerializeText =
            SerializeDecimal<VarFormat::Text, int32_t>;
          static constexpr auto kSerializeBinary =
            SerializeDecimal<VarFormat::Binary, int32_t>;
          RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
        }
        case INT64: {
          static constexpr auto kSerializeText =
            SerializeDecimal<VarFormat::Text, int64_t>;
          static constexpr auto kSerializeBinary =
            SerializeDecimal<VarFormat::Binary, int64_t>;
          RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
        }
        case INT128: {
          static constexpr auto kSerializeText =
            SerializeDecimal<VarFormat::Text, duckdb::hugeint_t>;
          static constexpr auto kSerializeBinary =
            SerializeDecimal<VarFormat::Binary, duckdb::hugeint_t>;
          RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
        }
        default:
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                          ERR_MSG("Unsupported decimal internal type"));
      }
    case CHAR:
    case VARCHAR: {
      if (type.IsJSONType()) {
        static constexpr auto kSerializeText =
          SerializeJson<VarFormat::Text, false>;
        static constexpr auto kSerializeBinary =
          SerializeJson<VarFormat::Binary, false>;
        RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
      }
      static constexpr auto kSerializeText =
        SerializeVarchar<VarFormat::Text, false>;
      static constexpr auto kSerializeBinary =
        SerializeVarchar<VarFormat::Binary, false>;
      RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
    }
    case BLOB: {
      if (context.bytea_output == ByteaOutput::Hex) {
        static constexpr auto kSerializeText = SerializeByteaTextHex<false>;
        RETURN_SERIALIZATION(kSerializeText, SerializeByteaBinary);
      } else {
        SDB_ASSERT(context.bytea_output == ByteaOutput::Escape);
        static constexpr auto kSerializeText = SerializeByteaTextEscape<false>;
        RETURN_SERIALIZATION(kSerializeText, SerializeByteaBinary);
      }
    }
    case DATE:
      RETURN_SERIALIZATION(SerializeDate<VarFormat::Text>,
                           SerializeDate<VarFormat::Binary>);
    case TIME:
      RETURN_SERIALIZATION(SerializeTime<VarFormat::Text>,
                           SerializeTime<VarFormat::Binary>);
    case TIME_NS:
      RETURN_SERIALIZATION(SerializeTimeNs<VarFormat::Text>,
                           SerializeTimeNs<VarFormat::Binary>);
    case TIME_TZ:
      RETURN_SERIALIZATION(SerializeTimeTz<VarFormat::Text>,
                           SerializeTimeTz<VarFormat::Binary>);
    case TIMESTAMP_SEC:
      RETURN_SERIALIZATION(SerializeTimestampSec<VarFormat::Text>,
                           SerializeTimestampSec<VarFormat::Binary>);
    case TIMESTAMP_MS:
      RETURN_SERIALIZATION(SerializeTimestampMs<VarFormat::Text>,
                           SerializeTimestampMs<VarFormat::Binary>);
    case TIMESTAMP:
      RETURN_SERIALIZATION(SerializeTimestamp<VarFormat::Text>,
                           SerializeTimestamp<VarFormat::Binary>);
    case TIMESTAMP_NS:
      RETURN_SERIALIZATION(SerializeTimestampNs<VarFormat::Text>,
                           SerializeTimestampNs<VarFormat::Binary>);
    case TIMESTAMP_TZ:
      RETURN_SERIALIZATION(SerializeTimestampTz<VarFormat::Text>,
                           SerializeTimestampTz<VarFormat::Binary>);
    case INTERVAL:
      RETURN_SERIALIZATION(SerializeInterval<VarFormat::Text>,
                           SerializeInterval<VarFormat::Binary>);
    case UUID:
      RETURN_SERIALIZATION(SerializeUuid<VarFormat::Text>,
                           SerializeUuid<VarFormat::Binary>);
    case BIT:
      RETURN_SERIALIZATION(SerializeBit<VarFormat::Text>,
                           SerializeBit<VarFormat::Binary>);
    case ENUM: {
      auto phys = duckdb::EnumType::GetPhysicalType(type);
      switch (phys) {
        using enum duckdb::PhysicalType;
        case UINT8: {
          static constexpr auto kText =
            SerializeEnumLabel<VarFormat::Text, false, uint8_t>;
          static constexpr auto kBinary =
            SerializeEnumLabel<VarFormat::Binary, false, uint8_t>;
          RETURN_SERIALIZATION(kText, kBinary);
        }
        case UINT16: {
          static constexpr auto kText =
            SerializeEnumLabel<VarFormat::Text, false, uint16_t>;
          static constexpr auto kBinary =
            SerializeEnumLabel<VarFormat::Binary, false, uint16_t>;
          RETURN_SERIALIZATION(kText, kBinary);
        }
        case UINT32: {
          static constexpr auto kText =
            SerializeEnumLabel<VarFormat::Text, false, uint32_t>;
          static constexpr auto kBinary =
            SerializeEnumLabel<VarFormat::Binary, false, uint32_t>;
          RETURN_SERIALIZATION(kText, kBinary);
        }
        default:
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                          ERR_MSG("Unsupported ENUM physical type"));
      }
    }
    case LIST:
    case ARRAY: {
      const auto* element_type = &type;
      size_t dims = 0;
      while (true) {
        if (element_type->id() == LIST) {
          element_type = &duckdb::ListType::GetChildType(*element_type);
        } else if (element_type->id() == ARRAY) {
          element_type = &duckdb::ArrayType::GetChildType(*element_type);
        } else {
          break;
        }
        ++dims;
      }
      const auto kind = [&] {
        if (dims > 1) {
          return ArrayKind::MultiDimensions;
        } else if (type.id() == ARRAY) {
          return ArrayKind::ArraySingleDimension;
        } else {
          return ArrayKind::ListSingleDimension;
        }
      }();
      return GetArraySerialization(*element_type, format, context, kind);
    }
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("Such type is not supported"));
  }
}

}  // namespace sdb::pg
