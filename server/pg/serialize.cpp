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
#include <velox/functions/prestosql/json/JsonStringUtil.h>
#include <velox/functions/prestosql/types/IPAddressType.h>
#include <velox/functions/prestosql/types/IPPrefixType.h>
#include <velox/functions/prestosql/types/JsonType.h>
#include <velox/functions/prestosql/types/TimeWithTimezoneType.h>
#include <velox/functions/prestosql/types/TimestampWithTimeZoneType.h>
#include <velox/functions/prestosql/types/UuidType.h>
#include <velox/type/DecimalUtil.h>
#include <velox/type/Timestamp.h>
#include <velox/type/Type.h>
#include <velox/vector/ComplexVector.h>

#include <algorithm>
#include <bit>
#include <cctype>
#include <cfloat>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <string_view>
#include <type_traits>

#include "basics/assert.h"
#include "basics/dtoa.h"
#include "basics/logger/logger.h"
#include "basics/misc.hpp"
#include "pg/interval.h"
#include "pg/pg_types.h"
#include "query/config.h"
#include "query/types.h"

namespace sdb::pg {
namespace {

#define SERIALIZE_PRIMITIVE(kind)                  \
  static constexpr auto kSerializeText =           \
    SerializePrimitiveType<kind, VarFormat::Text>; \
  static constexpr auto kSerializeBinary =         \
    SerializePrimitiveType<kind, VarFormat::Binary>

#define RETURN_SERIALIZATION(serialize_text, serialize_binary)         \
  return format == VarFormat::Text ? SerializeNullable<serialize_text> \
                                   : SerializeNullable<serialize_binary>

#define CASE_SERIALIZATION(kind)                            \
  case kind: {                                              \
    SERIALIZE_PRIMITIVE(kind);                              \
    RETURN_SERIALIZATION(kSerializeText, kSerializeBinary); \
  }

#define RETURN_ARRAY_SERIALIZATION(serialize_text, serialize_binary, oid)    \
  if (dims == 1) {                                                           \
    return format == VarFormat::Text                                         \
             ? SerializeNullable<                                            \
                 SerializeOneDimArray<serialize_text, oid, VarFormat::Text>> \
             : SerializeNullable<SerializeOneDimArray<serialize_binary, oid, \
                                                      VarFormat::Binary>>;   \
  }                                                                          \
  return format == VarFormat::Text                                           \
           ? SerializeNullable<                                              \
               SerializeArray<serialize_text, oid, VarFormat::Text>>         \
           : SerializeNullable<                                              \
               SerializeArray<serialize_binary, oid, VarFormat::Binary>>

#define CASE_ARRAY_SERIALIZATION(kind)                                  \
  case kind: {                                                          \
    SERIALIZE_PRIMITIVE(kind);                                          \
    static constexpr auto kOid = GetPrimitiveTypeOID(kind, true);       \
    RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, kOid); \
  }

template<SerializationFunction ValueSerialization>
void SerializeNullable(SerializationContext context,
                       const velox::DecodedVector& decoded_vector,
                       velox::vector_size_t row) {
  auto* length_data = context.buffer->GetContiguousData(4);
  if (decoded_vector.isNullAt(row)) {
    absl::big_endian::Store32(length_data, -1);
  } else {
    const auto uncommitted_size = context.buffer->GetUncommittedSize();
    ValueSerialization(context, decoded_vector, row);
    absl::big_endian::Store32(
      length_data, context.buffer->GetUncommittedSize() - uncommitted_size);
  }
}

template<typename T, VarFormat Format, bool Precise = true>
void SerializeFloat(SerializationContext context,
                    const velox::DecodedVector& decoded_vector,
                    velox::vector_size_t row) {
  static_assert(std::is_same_v<T, float> || std::is_same_v<T, double>);

  auto value = decoded_vector.valueAt<T>(row);
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

template<typename T, VarFormat Format>
void SerializeInt(SerializationContext context,
                  const velox::DecodedVector& decoded_vector,
                  velox::vector_size_t row) {
  const auto value = decoded_vector.valueAt<T>(row);

  if constexpr (Format == VarFormat::Text) {
    context.buffer->WriteContiguousData(basics::kIntStrMaxLen, [&](auto* data) {
      char* buf = reinterpret_cast<char*>(data);
      char* ptr = absl::numbers_internal::FastIntToBuffer(value, buf);
      return static_cast<size_t>(ptr - buf);
    });
  } else {
    absl::big_endian::Store(context.buffer->GetContiguousData(sizeof(T)),
                            value);
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
  return data.empty() ||
         absl::c_any_of(data,
                        [](char c) {
                          return c == '{' || c == '}' || c == ',' || c == '"' ||
                                 c == '\\' || absl::ascii_isspace(c);
                        }) ||
         absl::EqualsIgnoreCase(data, "null");
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
                      const velox::DecodedVector& decoded_vector,
                      velox::vector_size_t row) {
  auto raw = decoded_vector.valueAt<velox::StringView>(row);
  auto value = static_cast<std::string_view>(raw);
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

template<VarFormat Format, typename UnscaledType>
void SerializeDecimal(SerializationContext context,
                      const velox::DecodedVector& decoded_vector,
                      velox::vector_size_t row) {
  const auto& type = decoded_vector.base()->type();
  const auto [precision, scale] = velox::getDecimalPrecisionScale(*type);
  auto value = decoded_vector.valueAt<UnscaledType>(row);
  if constexpr (Format == VarFormat::Text) {
    const auto max_size =
      velox::DecimalUtil::maxStringViewSize(precision, scale);
    context.buffer->WriteContiguousData(max_size, [&](auto* data) {
      char* buf = reinterpret_cast<char*>(data);
      return velox::DecimalUtil::castToString<UnscaledType>(
        value, scale, static_cast<int32_t>(max_size), buf);
    });
  } else {
    // Well, here we go...
    // Postgre numeric(decimal) type has special binary layout:
    // int16_t ndigits;  number of digits
    // int16_t weight;  weight of first digit(the exponent of the first digit)
    // int16_t sign;  sign of the number
    // int16_t dscale;  display scale
    // int16_t digits[ndigits];  base 10000 digits

    // So, in velox value = real_value * 10^scale
    // However, in Postgres each digit represents 4 decimal digits,
    // i.e. base 10000. So we need to convert velox decimal representation
    // to Postgres numeric representation.

    // E.g. value = 12345 with scale = 2 means real_value = 123.45
    // In Postgres representation it will be:
    // ndigits = 2
    // weight = 0
    // sign = 0x0000
    // dscale = 2
    // digits[0] = 123
    // digits[1] = 4500
    static constexpr size_t kBaseSystem = 10'000;
    static constexpr int16_t kPositive = 0x0000;
    static constexpr int16_t kNegative = 0x4000;
    int16_t extra_digits = (4 - (scale % 4)) % 4;
    auto extra_base =
      static_cast<int16_t>(velox::DecimalUtil::kPowersOfTen[extra_digits]);

    auto sign = (value < 0) ? kNegative : kPositive;
    value = value < 0 ? -value : value;

    int16_t ndigits = [extra_base](auto value) -> int16_t {
      if (value == 0) {
        return 0;
      }
      int16_t ndigits = 0;
      if (extra_base != 1) {
        ndigits++;
        value /= (kBaseSystem / extra_base);
      }
      for (; value != 0; value /= kBaseSystem) {
        ++ndigits;
      }
      return ndigits;
    }(value);

    auto weight = static_cast<int16_t>(ndigits - ((scale + 3) / 4) - 1);
    auto dscale = static_cast<int16_t>(scale);
    auto* data = context.buffer->GetContiguousData(8 + ndigits * 2);
    absl::big_endian::Store16(data, ndigits);
    absl::big_endian::Store16(data + 2, weight);
    absl::big_endian::Store16(data + 4, sign);
    absl::big_endian::Store16(data + 6, dscale);
    data += 8 + ndigits * 2;

    // Adjust dscale to be multiple of 4 for ndigits
    if (extra_base != 1 && value != 0) {
      data -= 2;
      ndigits--;
      int16_t extra_value = (value % (kBaseSystem / extra_base)) * extra_base;
      value /= (kBaseSystem / extra_base);
      absl::big_endian::Store16(data, extra_value);
    }

    while (value != 0) {
      data -= 2;
      ndigits--;
      absl::big_endian::Store16(data,
                                static_cast<int16_t>(value % kBaseSystem));
      value /= kBaseSystem;
    }
    SDB_ASSERT(ndigits == 0);
  }
}

template<bool InArray>
void SerializeByteaTextHex(SerializationContext context,
                           const velox::DecodedVector& decoded_vector,
                           velox::vector_size_t row) {
  auto raw = decoded_vector.valueAt<velox::StringView>(row);
  auto value = static_cast<std::string_view>(raw);
  const auto required_size = (InArray ? 3 : 0) + 2 + 2 * value.size();
  auto* buf =
    reinterpret_cast<char*>(context.buffer->GetContiguousData(required_size));

  ByteaOutHex<InArray>(buf, value);
}

template<bool InArray>
void SerializeByteaTextEscape(SerializationContext context,
                              const velox::DecodedVector& decoded_vector,
                              velox::vector_size_t row) {
  auto raw = decoded_vector.valueAt<velox::StringView>(row);
  auto value = static_cast<std::string_view>(raw);

  const auto required_size = ByteaOutEscapeLength<InArray>(value);
  auto* buf =
    reinterpret_cast<char*>(context.buffer->GetContiguousData(required_size));

  irs::ResolveBool(InArray && required_size != value.size(),
                   [&]<bool NeedArrayEscaping> {
                     ByteaOutEscape<NeedArrayEscaping>(buf, value);
                   });
}

void SerializeByteaBinary(SerializationContext context,
                          const velox::DecodedVector& decoded_vector,
                          velox::vector_size_t row) {
  auto raw = decoded_vector.valueAt<velox::StringView>(row);
  auto value = static_cast<std::string_view>(raw);
  context.buffer->WriteUncommitted(value);
}

// Postgres stores days from 2000-01-01
constexpr auto kGapDays =
  absl::CivilDay{2000, 1, 1} - absl::CivilDay{1970, 1, 1};

template<velox::TypeKind Kind, VarFormat Format>
void SerializePrimitiveType(SerializationContext context,
                            const velox::DecodedVector& decoded_vector,
                            velox::vector_size_t row) {
  if constexpr (Kind == velox::TypeKind::UNKNOWN) {
    SDB_ASSERT(false);
  } else if constexpr (Kind == velox::TypeKind::TINYINT) {
    SerializeInt<int8_t, Format>(context, decoded_vector, row);
  } else if constexpr (Kind == velox::TypeKind::SMALLINT) {
    SerializeInt<int16_t, Format>(context, decoded_vector, row);
  } else if constexpr (Kind == velox::TypeKind::INTEGER) {
    SerializeInt<int32_t, Format>(context, decoded_vector, row);
  } else if constexpr (Kind == velox::TypeKind::BIGINT) {
    SerializeInt<int64_t, Format>(context, decoded_vector, row);
  } else if constexpr (Kind == velox::TypeKind::BOOLEAN) {
    if constexpr (Format == VarFormat::Text) {
      auto value = decoded_vector.valueAt<bool>(row);
      context.buffer->WriteUncommitted(value ? "t" : "f");
    } else {
      auto* ptr = reinterpret_cast<bool*>(
        context.buffer->GetContiguousData(sizeof(bool)));
      *ptr = decoded_vector.valueAt<bool>(row);
    }
  } else if constexpr (Kind == velox::TypeKind::TIMESTAMP) {
    const auto timestamp = decoded_vector.valueAt<velox::Timestamp>(row);
    if constexpr (Format == VarFormat::Text) {
      static constexpr auto kOptions = velox::TimestampToStringOptions{
        .skipTrailingZeros = true, .dateTimeSeparator = ' '};
      static constexpr auto kMaxLen = velox::getMaxStringLength(kOptions);
      context.buffer->WriteContiguousData(kMaxLen, [&](auto* data) {
        const auto buf = reinterpret_cast<char*>(data);
        const auto r =
          velox::Timestamp::tsToStringView(timestamp, kOptions, buf);
        return r.size();
      });
    } else {
      static constexpr auto kGapUs =
        absl::FromUnixSeconds(kGapDays * 24 * 60 * 60);
      const auto time = absl::FromUnixSeconds(timestamp.getSeconds()) +
                        absl::Nanoseconds(timestamp.getNanos());
      const auto time_us = absl::ToInt64Microseconds(time - kGapUs);
      absl::big_endian::Store64(context.buffer->GetContiguousData(8), time_us);
    }
  }
}

template<SerializationFunction ElementSerialization, int32_t ElemenOID,
         VarFormat Format>
void SerializeOneDimArray(SerializationContext context,
                          const velox::DecodedVector& decoded_vector,
                          velox::vector_size_t row) {
  const auto* array_vector = decoded_vector.base()->as<velox::ArrayVector>();
  const auto array_row = decoded_vector.index(row);
  const auto array_size = array_vector->sizeAt(array_row);
  const auto array_offset = array_vector->offsetAt(array_row);
  const auto& child_vector = array_vector->elements();
  velox::DecodedVector decoded_child;
  decoded_child.decode(*child_vector, false);
  if constexpr (Format == VarFormat::Text) {
    context.buffer->WriteUncommitted("{");
    for (velox::vector_size_t i = 0; i < array_size; ++i) {
      if (i > 0) {
        context.buffer->WriteUncommitted(",");
      }
      const auto element_row = array_offset + i;
      if (decoded_child.isNullAt(element_row)) {
        context.buffer->WriteUncommitted("NULL");
      } else {
        ElementSerialization(context, decoded_child, element_row);
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
    absl::big_endian::Store32(prefix_data + 8, ElemenOID);
    absl::big_endian::Store32(prefix_data + 12, array_size);
    absl::big_endian::Store32(prefix_data + 16, 0);
    for (velox::vector_size_t i = 0; i < array_size; ++i) {
      const auto element_row = array_offset + i;
      SerializeNullable<ElementSerialization>(context, decoded_child,
                                              element_row);
    }
  }
}

template<SerializationFunction ElementSerialization, VarFormat Format,
         bool First = true>
int32_t FlattenArray(SerializationContext context,
                     const velox::DecodedVector& decoded_vector,
                     velox::vector_size_t row) {
  const auto* array_vector = decoded_vector.base()->as<velox::ArrayVector>();
  if (!array_vector) {
    SerializeNullable<ElementSerialization>(context, decoded_vector, row);
    return 0;
  }

  const auto array_row = decoded_vector.index(row);
  const auto array_size = array_vector->sizeAt(array_row);
  const auto array_offset = array_vector->offsetAt(array_row);
  const auto& child_vector = array_vector->elements();

  if constexpr (First) {
    // dimension size(4) + lower_bound(4)
    auto* prefix_data = context.buffer->GetContiguousData(8);
    absl::big_endian::Store32(prefix_data + 4, 0);
    absl::big_endian::Store32(prefix_data, array_size);
  }
  if (array_size == 0) {
    return 1;
  }
  velox::DecodedVector decoded_child;
  decoded_child.decode(*child_vector, false);
  velox::vector_size_t i = 0;
  int32_t dims = -1;
  if constexpr (First) {
    dims = FlattenArray<ElementSerialization, Format>(context, decoded_child,
                                                      array_offset + i) +
           1;
    i++;
  }
  for (; i < array_size; ++i) {
    auto element_row = array_offset + i;
    const auto inner_dim = FlattenArray<ElementSerialization, Format, false>(
      context, decoded_child, element_row);
    SDB_ASSERT(dims == -1 || dims == inner_dim + 1);
    dims = inner_dim + 1;
  }
  SDB_ASSERT(dims > 0);
  return dims;
}

template<SerializationFunction ElementSerialization, int32_t ElemenOID,
         VarFormat Format>
void SerializeArray(SerializationContext context,
                    const velox::DecodedVector& decoded_vector,
                    velox::vector_size_t row) {
  if constexpr (Format == VarFormat::Text) {
    const auto* array_vector = decoded_vector.base()->as<velox::ArrayVector>();
    if (!array_vector) {
      // the last layer of multi-dim array
      if (decoded_vector.isNullAt(row)) {
        context.buffer->WriteUncommitted("NULL");
      } else {
        ElementSerialization(context, decoded_vector, row);
      }
      return;
    }
    const auto array_row = decoded_vector.index(row);
    const auto array_size = array_vector->sizeAt(array_row);
    const auto array_offset = array_vector->offsetAt(array_row);
    const auto& child_vector = array_vector->elements();
    velox::DecodedVector decoded_child;
    decoded_child.decode(*child_vector, false);
    context.buffer->WriteUncommitted("{");
    for (velox::vector_size_t i = 0; i < array_size; ++i) {
      if (i > 0) {
        context.buffer->WriteUncommitted(",");
      }
      const auto element_row = array_offset + i;
      SerializeArray<ElementSerialization, ElemenOID, Format>(
        context, decoded_child, element_row);
    }
    context.buffer->WriteUncommitted("}");
  } else {
    // dimensions(4) + flags(4) + element_oid(4)
    auto* prefix_data = context.buffer->GetContiguousData(12);
    absl::big_endian::Store32(prefix_data + 4, 0);
    absl::big_endian::Store32(prefix_data + 8, ElemenOID);
    const auto dims =
      FlattenArray<ElementSerialization, Format>(context, decoded_vector, row);
    absl::big_endian::Store32(prefix_data, dims);
  }
}

template<VarFormat Format>
void SerializeDate(SerializationContext context,
                   const velox::DecodedVector& decoded_vector,
                   velox::vector_size_t row) {
  // days from 1970-01-01
  auto days = decoded_vector.valueAt<int32_t>(row);
  if constexpr (Format == VarFormat::Text) {
    // TODO(mkornaukhov) support BC date and add some validation for dates
    // Format is "%04d-%02d-%02d", max year is 5874897
    static constexpr size_t kMaxDateStrSize = 7 + 1 + 2 + 1 + 2;

    absl::CivilDay date{1970, 1, 1};
    date += days;

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
    days -= kGapDays;
    absl::big_endian::Store32(context.buffer->GetContiguousData(4), days);
  }
}

template<VarFormat Format>
void SerializeInterval(SerializationContext context,
                       const velox::DecodedVector& decoded_vector,
                       velox::vector_size_t row) {
  const auto interval = decoded_vector.valueAt<velox::int128_t>(row);
  if constexpr (Format == VarFormat::Text) {
    const auto interval_str = pg::IntervalOut(interval);
    context.buffer->WriteUncommitted(interval_str);
  } else {
    auto* data = context.buffer->GetContiguousData(16);
    absl::big_endian::Store128(data, interval);
  }
}

template<VarFormat Format>
void SerializeUuid(SerializationContext context,
                   const velox::DecodedVector& decoded_vector,
                   velox::vector_size_t row) {
  const auto uuid = decoded_vector.valueAt<velox::int128_t>(row);
  if constexpr (Format == VarFormat::Text) {
    // Format is "%08x-%04x-%04x-%04x-%012x"
    static constexpr size_t kUUIDStrSize = 8 + 1 + 4 + 1 + 4 + 1 + 4 + 1 + 12;
    auto* data = context.buffer->GetContiguousData(kUUIDStrSize);
    char* buf = reinterpret_cast<char*>(data);

    static constexpr size_t kMaxHexSize = 16;
    char hex_buf[kMaxHexSize];
    char* const hex_buf_end = hex_buf + kMaxHexSize;
    auto write_hex = [&](uint64_t value, uint8_t pad) {
      absl::numbers_internal::FastHexToBufferZeroPad16(value, hex_buf);
      std::memcpy(buf, hex_buf_end - pad, pad);
      buf += pad;
    };

    const uint64_t high = (uuid >> 64);
    const uint64_t low = (uuid & std::numeric_limits<uint64_t>::max());

    write_hex(high >> 32, 8);
    *buf++ = '-';
    write_hex((high >> 16) & 0xFFFF, 4);
    *buf++ = '-';
    write_hex(high & 0xFFFF, 4);
    *buf++ = '-';
    write_hex(low >> 48, 4);
    *buf++ = '-';
    write_hex(low & 0xFFFF'FFFF'FFFF, 12);
  } else {
    auto* data = context.buffer->GetContiguousData(16);
    absl::big_endian::Store128(data, uuid);
  }
}

template<VarFormat Format, bool InArray>
void SerializeJson(SerializationContext context,
                   const velox::DecodedVector& decoded_vector,
                   velox::vector_size_t row) {
  const auto value_quoted = decoded_vector.valueAt<velox::StringView>(row);

  // By default, only unicode("\u") and slashes("\/") are unescaped.
  // Should be true if you want to unescape conventional symbols
  // ("\r", "\n", "\\"" etc)
  bool fully_unescape = true;

  SDB_ASSERT(value_quoted.size() >= 2);
  SDB_ASSERT(*value_quoted.begin() == '"');
  SDB_ASSERT(*(value_quoted.end() - 1) == '"');
  auto value =
    std::string_view(value_quoted.begin() + 1, value_quoted.end() - 1);
  if constexpr (InArray && Format == VarFormat::Text) {
    if (ArrayItemNeedQuotesAndEscape(value)) {
      value = static_cast<std::string_view>(value_quoted);
      fully_unescape = false;
    }
  }

  const size_t value_size = value.size();
  context.buffer->WriteContiguousData(value_size, [&](uint8_t* data) {
    char* buf = reinterpret_cast<char*>(data);
    const auto written = velox::unescapeForJsonFunctions(
      value.data(), value_size, buf, fully_unescape);
    SDB_ASSERT(written <= value_size);
    return written;
  });
}

SerializationFunction GetArraySerialization(const velox::TypePtr& type,
                                            VarFormat format,
                                            SerializationContext& context,
                                            size_t dims) {
  if (isUuidType(type)) {
    RETURN_ARRAY_SERIALIZATION(SerializeUuid<VarFormat::Text>,
                               SerializeUuid<VarFormat::Binary>, 2950);
  }

  if (isJsonType(type)) {
    static constexpr auto kSerializeText = SerializeJson<VarFormat::Text, true>;
    static constexpr auto kSerializeBinary =
      SerializeJson<VarFormat::Binary, true>;
    RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, 199);
  }

  if (isIPAddressType(type)) {
    SDB_ASSERT(false,
               "TODO(mkornaukhov): Array of IPAddress is not supported yet");
    return nullptr;
  }

  if (isIPPrefixType(type)) {
    SDB_ASSERT(false,
               "TODO(mkornaukhov): Array of IPPrefix is not supported yet");
    return nullptr;
  }

  if (isTimestampWithTimeZoneType(type)) {
    SDB_ASSERT(false,
               "TODO(mkornaukhov): Array of TimestampTZ is not supported yet");
    return nullptr;
  }

  if (isTimeWithTimeZone(type)) {
    SDB_ASSERT(false,
               "TODO(mkornaukhov): Array of TimeTZ is not supported yet");
    return nullptr;
  }

  if (type->isShortDecimal()) {
    static constexpr auto kSerializeText =
      SerializeDecimal<VarFormat::Text, int64_t>;
    static constexpr auto kSerializeBinary =
      SerializeDecimal<VarFormat::Binary, int64_t>;
    RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, 1231);
  }

  if (type->isLongDecimal()) {
    static constexpr auto kSerializeText =
      SerializeDecimal<VarFormat::Text, velox::int128_t>;
    static constexpr auto kSerializeBinary =
      SerializeDecimal<VarFormat::Binary, velox::int128_t>;
    RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, 1231);
  }

  if (type->isIntervalYearMonth()) {
    SDB_ASSERT(
      false,
      "TODO(mkornaukhov): Array of IntervalYearMonth is not supported yet");
    return nullptr;
  }

  if (type->isIntervalDayTime()) {
    SDB_ASSERT(
      false,
      "TODO(mkornaukhov): Array of IntervalDayTime is not supported yet");
    return nullptr;
  }

  if (type->isTime()) {
    SDB_ASSERT(false, "TODO(mkornaukhov): Array of Time is not supported yet");
    return nullptr;
  }

  if (type->isDate()) {
    RETURN_ARRAY_SERIALIZATION(SerializeDate<VarFormat::Text>,
                               SerializeDate<VarFormat::Binary>, 1082);
  }

  switch (type->kind()) {
    CASE_ARRAY_SERIALIZATION(velox::TypeKind::UNKNOWN)
    CASE_ARRAY_SERIALIZATION(velox::TypeKind::TINYINT)
    CASE_ARRAY_SERIALIZATION(velox::TypeKind::SMALLINT)
    CASE_ARRAY_SERIALIZATION(velox::TypeKind::INTEGER)
    CASE_ARRAY_SERIALIZATION(velox::TypeKind::BIGINT)
    CASE_ARRAY_SERIALIZATION(velox::TypeKind::BOOLEAN)
    CASE_ARRAY_SERIALIZATION(velox::TypeKind::TIMESTAMP)
    case velox::TypeKind::REAL: {
      static constexpr auto kSerializeBinary =
        SerializeFloat<float, VarFormat::Binary>;
      static constexpr auto kOid =
        GetPrimitiveTypeOID(velox::TypeKind::REAL, true);
      return irs::ResolveBool(
        context.extra_float_digits > 0, [&]<bool Precise> {
          static constexpr auto kSerializeText =
            SerializeFloat<float, VarFormat::Text, Precise>;
          RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, kOid);
        });
    }
    case velox::TypeKind::DOUBLE: {
      static constexpr auto kSerializeBinary =
        SerializeFloat<double, VarFormat::Binary>;
      static constexpr auto kOid =
        GetPrimitiveTypeOID(velox::TypeKind::DOUBLE, true);
      return irs::ResolveBool(
        context.extra_float_digits > 0, [&]<bool Precise> {
          static constexpr auto kSerializeText =
            SerializeFloat<double, VarFormat::Text, Precise>;
          RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, kOid);
        });
    }
    case velox::TypeKind::VARCHAR: {
      static constexpr auto kOid =
        GetPrimitiveTypeOID(velox::TypeKind::VARCHAR, true);
      static constexpr auto kSerializeText =
        SerializeVarchar<VarFormat::Text, true>;
      static constexpr auto kSerializeBinary =
        SerializeVarchar<VarFormat::Binary, true>;
      RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, kOid);
    }
    case velox::TypeKind::VARBINARY: {
      static constexpr auto kOid =
        GetPrimitiveTypeOID(velox::TypeKind::VARBINARY, true);
      if (context.bytea_output == ByteaOutput::Hex) {
        static constexpr auto kSerializeText = SerializeByteaTextHex<true>;
        RETURN_ARRAY_SERIALIZATION(kSerializeText, SerializeByteaBinary, kOid);
      } else {
        SDB_ASSERT(context.bytea_output == ByteaOutput::Escape);
        static constexpr auto kSerializeText = SerializeByteaTextEscape<true>;
        RETURN_ARRAY_SERIALIZATION(kSerializeText, SerializeByteaBinary, kOid);
      }
    }
    case velox::TypeKind::ARRAY:
      // This can happens for ARRAY<ROW/MAP<... , ARRAY<...>, ...>>
      SDB_ASSERT(
        false, "TODO(mkornaukhov): Other complex types are not supported yet");
      return nullptr;
    case velox::TypeKind::MAP:
      SDB_ASSERT(false, "TODO(mkornaukhov): Array of Map is not supported yet");
      return nullptr;
    case velox::TypeKind::ROW:
      SDB_ASSERT(false, "TODO(mkornaukhov): Array of Row is not supported yet");
      return nullptr;
    default:
      SDB_ASSERT(false);
      return nullptr;
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
  context.extra_float_digits =
    config.Get<VariableType::PgExtraFloatDigits>("extra_float_digits");
  context.bytea_output =
    config.Get<VariableType::PgByteaOutput>("bytea_output");
}

SerializationFunction GetSerialization(const velox::TypePtr& type,
                                       VarFormat format,
                                       SerializationContext& context) {
  if (isUuidType(type)) {
    RETURN_SERIALIZATION(SerializeUuid<VarFormat::Text>,
                         SerializeUuid<VarFormat::Binary>);
  }

  if (isJsonType(type)) {
    static constexpr auto kSerializeText =
      SerializeJson<VarFormat::Text, false>;
    static constexpr auto kSerializeBinary =
      SerializeJson<VarFormat::Binary, false>;
    RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
  }

  if (isIPAddressType(type)) {
    SDB_ASSERT(false, "TODO(mkornaukhov): IPAddress is not supported yet");
    return nullptr;
  }

  if (isIPPrefixType(type)) {
    SDB_ASSERT(false, "TODO(mkornaukhov): IPPrefix is not supported yet");
    return nullptr;
  }

  if (isTimestampWithTimeZoneType(type)) {
    SDB_ASSERT(false, "TODO(mkornaukhov): TimestampTZ is not supported yet");
    return nullptr;
  }

  if (isTimeWithTimeZone(type)) {
    SDB_ASSERT(false, "TODO(mkornaukhov): TimeTZ is not supported yet");
    return nullptr;
  }

  if (type->isShortDecimal()) {
    static constexpr auto kSerializeText =
      SerializeDecimal<VarFormat::Text, int64_t>;
    static constexpr auto kSerializeBinary =
      SerializeDecimal<VarFormat::Binary, int64_t>;
    RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
  }

  if (type->isLongDecimal()) {
    static constexpr auto kSerializeText =
      SerializeDecimal<VarFormat::Text, velox::int128_t>;
    static constexpr auto kSerializeBinary =
      SerializeDecimal<VarFormat::Binary, velox::int128_t>;
    RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
  }

  if (type->isIntervalYearMonth()) {
    SDB_ASSERT(false,
               "TODO(mkornaukhov): IntervalYearMonth is not supported yet");
    return nullptr;
  }

  if (type->isIntervalDayTime()) {
    SDB_ASSERT(false,
               "TODO(mkornaukhov): IntervalDayTime is not supported yet");
    return nullptr;
  }

  if (type->isTime()) {
    SDB_ASSERT(false, "TODO(mkornaukhov): Time is not supported yet");
    return nullptr;
  }

  if (type->isDate()) {
    RETURN_SERIALIZATION(SerializeDate<VarFormat::Text>,
                         SerializeDate<VarFormat::Binary>);
  }

  if (pg::IsInterval(type)) {
    RETURN_SERIALIZATION(SerializeInterval<VarFormat::Text>,
                         SerializeInterval<VarFormat::Binary>);
  }

  switch (type->kind()) {
    CASE_SERIALIZATION(velox::TypeKind::UNKNOWN)
    CASE_SERIALIZATION(velox::TypeKind::TINYINT)
    CASE_SERIALIZATION(velox::TypeKind::SMALLINT)
    CASE_SERIALIZATION(velox::TypeKind::INTEGER)
    CASE_SERIALIZATION(velox::TypeKind::BIGINT)
    CASE_SERIALIZATION(velox::TypeKind::BOOLEAN)
    CASE_SERIALIZATION(velox::TypeKind::TIMESTAMP)
    case velox::TypeKind::REAL: {
      static constexpr auto kSerializeBinary =
        SerializeFloat<float, VarFormat::Binary>;
      return irs::ResolveBool(
        context.extra_float_digits > 0, [&]<bool Precise> {
          static constexpr auto kSerializeText =
            SerializeFloat<float, VarFormat::Text, Precise>;
          RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
        });
    }
    case velox::TypeKind::DOUBLE: {
      static constexpr auto kSerializeBinary =
        SerializeFloat<double, VarFormat::Binary>;
      return irs::ResolveBool(
        context.extra_float_digits > 0, [&]<bool Precise> {
          static constexpr auto kSerializeText =
            SerializeFloat<double, VarFormat::Text, Precise>;
          RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
        });
    }
    case velox::TypeKind::VARCHAR: {
      static constexpr auto kSerializeText =
        SerializeVarchar<VarFormat::Text, false>;
      static constexpr auto kSerializeBinary =
        SerializeVarchar<VarFormat::Binary, false>;
      RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
    }
    case velox::TypeKind::VARBINARY: {
      if (context.bytea_output == ByteaOutput::Hex) {
        static constexpr auto kSerializeText = SerializeByteaTextHex<false>;
        RETURN_SERIALIZATION(kSerializeText, SerializeByteaBinary);
      } else {
        SDB_ASSERT(context.bytea_output == ByteaOutput::Escape);
        static constexpr auto kSerializeText = SerializeByteaTextEscape<false>;
        RETURN_SERIALIZATION(kSerializeText, SerializeByteaBinary);
      }
    }
    case velox::TypeKind::ARRAY: {
      auto element_type = type->asArray().elementType();
      size_t dims = 1;
      while (element_type->isArray()) {
        element_type = element_type->asArray().elementType();
        dims++;
      }
      return GetArraySerialization(element_type, format, context, dims);
    }
    case velox::TypeKind::MAP:
      SDB_ASSERT(false, "TODO(mkornaukhov): Map is not supported yet");
      return nullptr;
    case velox::TypeKind::ROW:
      SDB_ASSERT(false, "TODO(mkornaukhov): Row is not supported yet");
      return nullptr;
    default:
      SDB_ASSERT(false);
      return nullptr;
  }
}

}  // namespace sdb::pg
