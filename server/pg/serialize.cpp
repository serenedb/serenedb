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
#include <duckdb/common/types/hugeint.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/types/uuid.hpp>
#include <limits>
#include <string_view>
#include <type_traits>

#include "basics/assert.h"
#include "basics/dtoa.h"
#include "basics/logger/logger.h"
#include "basics/misc.hpp"
#include "pg/functions/interval.h"
#include "query/config.h"

namespace sdb::pg {
namespace {

#define SERIALIZE_PRIMITIVE(kind)                  \
  static constexpr auto kSerializeText =           \
    SerializePrimitiveType<kind, VarFormat::Text>; \
  static constexpr auto kSerializeBinary =         \
    SerializePrimitiveType<kind, VarFormat::Binary>

// Map DuckDB LogicalTypeId to PG OID (for array element types)
constexpr int32_t Kind2Oid(duckdb::LogicalTypeId kind,
                           bool /*is_array*/ = false) {
  switch (kind) {
    case duckdb::LogicalTypeId::BOOLEAN:
      return 16;
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
      return 21;
    case duckdb::LogicalTypeId::INTEGER:
      return 23;
    case duckdb::LogicalTypeId::BIGINT:
      return 20;
    case duckdb::LogicalTypeId::FLOAT:
      return 700;
    case duckdb::LogicalTypeId::DOUBLE:
      return 701;
    case duckdb::LogicalTypeId::VARCHAR:
      return 25;
    case duckdb::LogicalTypeId::BLOB:
      return 17;
    case duckdb::LogicalTypeId::TIMESTAMP:
      return 1114;
    case duckdb::LogicalTypeId::DATE:
      return 1082;
    case duckdb::LogicalTypeId::INTERVAL:
      return 1186;
    case duckdb::LogicalTypeId::UUID:
      return 2950;
    case duckdb::LogicalTypeId::DECIMAL:
      return 1700;
    default:
      return 25;  // TEXT fallback
  }
}

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
    static constexpr auto kOid = Kind2Oid(kind, true);                  \
    RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, kOid); \
  }

template<DuckDBSerializationFunction ValueSerialization>
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

template<typename T, VarFormat Format, bool Precise = true>
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

template<typename T, VarFormat Format>
void SerializeInt(SerializationContext context,
                  const duckdb::RecursiveUnifiedVectorFormat& vdata,
                  duckdb::idx_t row) {
  const auto value =
    vdata.unified.GetData<T>()[vdata.unified.sel->get_index(row)];

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

template<VarFormat Format>
void SerializeHugeint(SerializationContext context,
                      const duckdb::RecursiveUnifiedVectorFormat& vdata,
                      duckdb::idx_t row) {
  // HUGEINT is equivalent to DECIMAL(38, 0) -- same physical storage
  auto value =
    vdata.unified
      .GetData<duckdb::hugeint_t>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    auto str = duckdb::Hugeint::ToString(value);
    context.buffer->WriteContiguousData(str.size(), [&](auto* data) {
      memcpy(data, str.data(), str.size());
      return str.size();
    });
  } else {
    // Reuse PG numeric binary layout (scale=0)
    static constexpr size_t kBaseSystem = 10'000;
    static constexpr int16_t kPositive = 0x0000;
    static constexpr int16_t kNegative = 0x4000;

    auto sign = (value < 0) ? kNegative : kPositive;
    value = value < 0 ? -value : value;

    int16_t ndigits = [](auto value) -> int16_t {
      if (value == 0) {
        return 0;
      }
      int16_t ndigits = 0;
      for (; value != 0; value /= kBaseSystem) {
        ++ndigits;
      }
      return ndigits;
    }(value);

    auto weight = static_cast<int16_t>(ndigits - 1);
    auto* data = context.buffer->GetContiguousData(8 + ndigits * 2);
    absl::big_endian::Store16(data, ndigits);
    absl::big_endian::Store16(data + 2, weight);
    absl::big_endian::Store16(data + 4, sign);
    absl::big_endian::Store16(data + 6, 0);  // dscale = 0
    data += 8 + ndigits * 2;

    while (value != 0) {
      data -= 2;
      ndigits--;
      absl::big_endian::Store16(
        data, static_cast<int16_t>(static_cast<int64_t>(value % kBaseSystem)));
      value /= kBaseSystem;
    }
    SDB_ASSERT(ndigits == 0);
  }
}

template<VarFormat Format, typename UnscaledType>
void SerializeDecimal(SerializationContext context,
                      const duckdb::RecursiveUnifiedVectorFormat& vdata,
                      duckdb::idx_t row) {
  const auto& type = vdata.logical_type;
  auto precision = duckdb::DecimalType::GetWidth(type);
  auto scale = duckdb::DecimalType::GetScale(type);
  auto value =
    vdata.unified.GetData<UnscaledType>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    // Use DuckDB's Value::ToString for text decimal formatting.
    // Must use the correct DECIMAL overload matching the physical storage type.
    auto dec_value = duckdb::Value::DECIMAL(value, precision, scale);
    auto str = dec_value.ToString();
    context.buffer->WriteContiguousData(str.size(), [&](auto* data) {
      memcpy(data, str.data(), str.size());
      return str.size();
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
    static constexpr int16_t kPowersOfTen[] = {1, 10, 100, 1000};
    auto extra_base = kPowersOfTen[extra_digits];

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
      int16_t extra_value = static_cast<int16_t>(static_cast<int64_t>(
        (value % (kBaseSystem / extra_base)) * extra_base));
      value /= (kBaseSystem / extra_base);
      absl::big_endian::Store16(data, extra_value);
    }

    while (value != 0) {
      data -= 2;
      ndigits--;
      absl::big_endian::Store16(
        data, static_cast<int16_t>(static_cast<int64_t>(value % kBaseSystem)));
      value /= kBaseSystem;
    }
    SDB_ASSERT(ndigits == 0);
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

// Postgres stores days from 2000-01-01
constexpr auto kGapDays =
  absl::CivilDay{2000, 1, 1} - absl::CivilDay{1970, 1, 1};

template<duckdb::LogicalTypeId Kind, VarFormat Format>
void SerializePrimitiveType(SerializationContext context,
                            const duckdb::RecursiveUnifiedVectorFormat& vdata,
                            duckdb::idx_t row) {
  if constexpr (Kind == duckdb::LogicalTypeId::SQLNULL) {
    SDB_ASSERT(false);
  } else if constexpr (Kind == duckdb::LogicalTypeId::TINYINT) {
    SerializeInt<int8_t, Format>(context, vdata, row);
  } else if constexpr (Kind == duckdb::LogicalTypeId::SMALLINT) {
    SerializeInt<int16_t, Format>(context, vdata, row);
  } else if constexpr (Kind == duckdb::LogicalTypeId::INTEGER) {
    SerializeInt<int32_t, Format>(context, vdata, row);
  } else if constexpr (Kind == duckdb::LogicalTypeId::BIGINT) {
    SerializeInt<int64_t, Format>(context, vdata, row);
  } else if constexpr (Kind == duckdb::LogicalTypeId::BOOLEAN) {
    if constexpr (Format == VarFormat::Text) {
      auto value =
        vdata.unified.GetData<bool>()[vdata.unified.sel->get_index(row)];
      context.buffer->WriteUncommitted(value ? "t" : "f");
    } else {
      auto* ptr = reinterpret_cast<bool*>(
        context.buffer->GetContiguousData(sizeof(bool)));
      *ptr = vdata.unified.GetData<bool>()[vdata.unified.sel->get_index(row)];
    }
  } else if constexpr (Kind == duckdb::LogicalTypeId::TIMESTAMP) {
    const auto timestamp =
      vdata.unified
        .GetData<duckdb::timestamp_t>()[vdata.unified.sel->get_index(row)];
    if constexpr (Format == VarFormat::Text) {
      auto str = duckdb::Timestamp::ToString(timestamp);
      context.buffer->WriteContiguousData(str.size(), [&](auto* data) {
        memcpy(data, str.data(), str.size());
        return str.size();
      });
    } else {
      // PG binary: microseconds since 2000-01-01
      static constexpr int64_t kGapUs =
        static_cast<int64_t>(kGapDays) * 24 * 60 * 60 * 1000000LL;
      // DuckDB timestamp is microseconds since epoch
      int64_t pg_us = timestamp.value - kGapUs;
      absl::big_endian::Store64(context.buffer->GetContiguousData(8), pg_us);
    }
  }
}

template<DuckDBSerializationFunction ElementSerialization, int32_t ElementOID,
         VarFormat Format>
void SerializeOneDimArray(SerializationContext context,
                          const duckdb::RecursiveUnifiedVectorFormat& vdata,
                          duckdb::idx_t row) {
  auto list_data =
    vdata.unified
      .GetData<duckdb::list_entry_t>()[vdata.unified.sel->get_index(row)];
  auto& child_vdata = vdata.children[0];
  auto array_size = list_data.length;
  auto array_offset = list_data.offset;
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
template<DuckDBSerializationFunction ElementSerialization, VarFormat Format,
         bool First = true>
int32_t FlattenArray(SerializationContext context,
                     const duckdb::RecursiveUnifiedVectorFormat& vdata,
                     duckdb::idx_t row) {
  if (vdata.logical_type.id() != duckdb::LogicalTypeId::LIST) {
    SerializeNullable<ElementSerialization>(context, vdata, row);
    return 0;
  }
  auto list_data =
    vdata.unified
      .GetData<duckdb::list_entry_t>()[vdata.unified.sel->get_index(row)];
  auto& child_vdata = vdata.children[0];
  auto array_size = list_data.length;
  auto array_offset = list_data.offset;
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

template<DuckDBSerializationFunction ElementSerialization, int32_t ElementOID,
         VarFormat Format>
void SerializeArray(SerializationContext context,
                    const duckdb::RecursiveUnifiedVectorFormat& vdata,
                    duckdb::idx_t row) {
  if constexpr (Format == VarFormat::Text) {
    if (vdata.logical_type.id() != duckdb::LogicalTypeId::LIST) {
      if (!vdata.unified.validity.RowIsValid(
            vdata.unified.sel->get_index(row))) {
        context.buffer->WriteUncommitted("NULL");
      } else {
        ElementSerialization(context, vdata, row);
      }
      return;
    }
    auto list_data =
      vdata.unified
        .GetData<duckdb::list_entry_t>()[vdata.unified.sel->get_index(row)];
    auto& child_vdata = vdata.children[0];
    auto array_size = list_data.length;
    auto array_offset = list_data.offset;
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
    vdata.unified.GetData<int32_t>()[vdata.unified.sel->get_index(row)];
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

// TODO: These will be used when custom OID types are registered in DuckDB
[[maybe_unused]]
void SerializeRegtypeText(SerializationContext context,
                          const duckdb::RecursiveUnifiedVectorFormat& vdata,
                          duckdb::idx_t row) {
  const auto oid =
    vdata.unified.GetData<int64_t>()[vdata.unified.sel->get_index(row)];
  // TODO: implement RegtypeOut for DuckDB custom types
  context.buffer->WriteUncommitted(std::to_string(oid));
}

[[maybe_unused]]
void SerializeRegclassText(SerializationContext context,
                           const duckdb::RecursiveUnifiedVectorFormat& vdata,
                           duckdb::idx_t row) {
  const auto oid =
    vdata.unified.GetData<int64_t>()[vdata.unified.sel->get_index(row)];
  // TODO: implement RegclassOut for DuckDB custom types
  context.buffer->WriteUncommitted(std::to_string(oid));
}

[[maybe_unused]]
void SerializeRegnamespaceText(
  SerializationContext context,
  const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t row) {
  const auto oid =
    vdata.unified.GetData<int64_t>()[vdata.unified.sel->get_index(row)];
  // TODO: implement RegnamespaceOut for DuckDB custom types
  context.buffer->WriteUncommitted(std::to_string(oid));
}

// Binary serialization for oid-like types:
// truncate 64-bit OID to 32-bit for PG wire protocol compatibility.
[[maybe_unused]]
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

DuckDBSerializationFunction GetArraySerialization(
  const duckdb::LogicalType& type, VarFormat format,
  SerializationContext& context, size_t dims) {
  // TODO: Add custom SereneDB types (OID, Reg*, etc.) when implemented as
  // DuckDB custom logical types. For now, only standard DuckDB types.

  if (type.id() == duckdb::LogicalTypeId::UUID) {
    RETURN_ARRAY_SERIALIZATION(SerializeUuid<VarFormat::Text>,
                               SerializeUuid<VarFormat::Binary>,
                               2950);  // UUIDOID
  }

  if (type.id() == duckdb::LogicalTypeId::DATE) {
    RETURN_ARRAY_SERIALIZATION(SerializeDate<VarFormat::Text>,
                               SerializeDate<VarFormat::Binary>,
                               1082);  // DATEOID
  }

  if (type.id() == duckdb::LogicalTypeId::DECIMAL) {
    auto width = duckdb::DecimalType::GetWidth(type);
    if (width <= 4) {
      static constexpr auto kSerializeText =
        SerializeDecimal<VarFormat::Text, int16_t>;
      static constexpr auto kSerializeBinary =
        SerializeDecimal<VarFormat::Binary, int16_t>;
      RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, 1700);
    } else if (width <= 9) {
      static constexpr auto kSerializeText =
        SerializeDecimal<VarFormat::Text, int32_t>;
      static constexpr auto kSerializeBinary =
        SerializeDecimal<VarFormat::Binary, int32_t>;
      RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, 1700);
    } else if (width <= 18) {
      static constexpr auto kSerializeText =
        SerializeDecimal<VarFormat::Text, int64_t>;
      static constexpr auto kSerializeBinary =
        SerializeDecimal<VarFormat::Binary, int64_t>;
      RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, 1700);
    } else {
      static constexpr auto kSerializeText =
        SerializeDecimal<VarFormat::Text, duckdb::hugeint_t>;
      static constexpr auto kSerializeBinary =
        SerializeDecimal<VarFormat::Binary, duckdb::hugeint_t>;
      RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, 1700);
    }
  }

  switch (type.id()) {
    CASE_ARRAY_SERIALIZATION(duckdb::LogicalTypeId::SQLNULL)
    CASE_ARRAY_SERIALIZATION(duckdb::LogicalTypeId::TINYINT)
    CASE_ARRAY_SERIALIZATION(duckdb::LogicalTypeId::SMALLINT)
    CASE_ARRAY_SERIALIZATION(duckdb::LogicalTypeId::INTEGER)
    CASE_ARRAY_SERIALIZATION(duckdb::LogicalTypeId::BIGINT)
    CASE_ARRAY_SERIALIZATION(duckdb::LogicalTypeId::BOOLEAN)
    CASE_ARRAY_SERIALIZATION(duckdb::LogicalTypeId::TIMESTAMP)
    case duckdb::LogicalTypeId::FLOAT: {
      static constexpr auto kSerializeBinary =
        SerializeFloat<float, VarFormat::Binary>;
      static constexpr auto kOid =
        Kind2Oid(duckdb::LogicalTypeId::FLOAT, false);
      return irs::ResolveBool(
        context.extra_float_digits > 0, [&]<bool Precise> {
          static constexpr auto kSerializeText =
            SerializeFloat<float, VarFormat::Text, Precise>;
          RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, kOid);
        });
    }
    case duckdb::LogicalTypeId::DOUBLE: {
      static constexpr auto kSerializeBinary =
        SerializeFloat<double, VarFormat::Binary>;
      static constexpr auto kOid =
        Kind2Oid(duckdb::LogicalTypeId::DOUBLE, false);
      return irs::ResolveBool(
        context.extra_float_digits > 0, [&]<bool Precise> {
          static constexpr auto kSerializeText =
            SerializeFloat<double, VarFormat::Text, Precise>;
          RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, kOid);
        });
    }
    case duckdb::LogicalTypeId::VARCHAR: {
      static constexpr auto kOid =
        Kind2Oid(duckdb::LogicalTypeId::VARCHAR, false);
      static constexpr auto kSerializeText =
        SerializeVarchar<VarFormat::Text, true>;
      static constexpr auto kSerializeBinary =
        SerializeVarchar<VarFormat::Binary, true>;
      RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, kOid);
    }
    case duckdb::LogicalTypeId::BLOB: {
      static constexpr auto kOid = Kind2Oid(duckdb::LogicalTypeId::BLOB, false);
      if (context.bytea_output == ByteaOutput::Hex) {
        static constexpr auto kSerializeText = SerializeByteaTextHex<true>;
        RETURN_ARRAY_SERIALIZATION(kSerializeText, SerializeByteaBinary, kOid);
      } else {
        SDB_ASSERT(context.bytea_output == ByteaOutput::Escape);
        static constexpr auto kSerializeText = SerializeByteaTextEscape<true>;
        RETURN_ARRAY_SERIALIZATION(kSerializeText, SerializeByteaBinary, kOid);
      }
    }
    case duckdb::LogicalTypeId::LIST:
      // This can happens for ARRAY<ROW/MAP<... , ARRAY<...>, ...>>
      SDB_ASSERT(
        false, "TODO(mkornaukhov): Other complex types are not supported yet");
      return nullptr;
    case duckdb::LogicalTypeId::MAP:
      SDB_ASSERT(false, "TODO(mkornaukhov): Array of Map is not supported yet");
      return nullptr;
    case duckdb::LogicalTypeId::STRUCT:
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

DuckDBSerializationFunction GetDuckDBSerialization(
  const duckdb::LogicalType& type, VarFormat format) {
  // Handle decimal first (needs width check)
  if (type.id() == duckdb::LogicalTypeId::DECIMAL) {
    auto width = duckdb::DecimalType::GetWidth(type);
    // DuckDB stores decimals with different physical types based on width.
    // Must match: width <= 4 -> int16_t, <= 9 -> int32_t, <= 18 -> int64_t,
    // else hugeint_t
    if (width <= 4) {
      static constexpr auto kSerializeText =
        SerializeDecimal<VarFormat::Text, int16_t>;
      static constexpr auto kSerializeBinary =
        SerializeDecimal<VarFormat::Binary, int16_t>;
      RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
    } else if (width <= 9) {
      static constexpr auto kSerializeText =
        SerializeDecimal<VarFormat::Text, int32_t>;
      static constexpr auto kSerializeBinary =
        SerializeDecimal<VarFormat::Binary, int32_t>;
      RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
    } else if (width <= 18) {
      static constexpr auto kSerializeText =
        SerializeDecimal<VarFormat::Text, int64_t>;
      static constexpr auto kSerializeBinary =
        SerializeDecimal<VarFormat::Binary, int64_t>;
      RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
    } else {
      static constexpr auto kSerializeText =
        SerializeDecimal<VarFormat::Text, duckdb::hugeint_t>;
      static constexpr auto kSerializeBinary =
        SerializeDecimal<VarFormat::Binary, duckdb::hugeint_t>;
      RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
    }
  }

  switch (type.id()) {
    CASE_SERIALIZATION(duckdb::LogicalTypeId::SQLNULL)
    CASE_SERIALIZATION(duckdb::LogicalTypeId::TINYINT)
    CASE_SERIALIZATION(duckdb::LogicalTypeId::SMALLINT)
    CASE_SERIALIZATION(duckdb::LogicalTypeId::INTEGER)
    CASE_SERIALIZATION(duckdb::LogicalTypeId::BIGINT)
    case duckdb::LogicalTypeId::HUGEINT: {
      RETURN_SERIALIZATION(SerializeHugeint<VarFormat::Text>,
                           SerializeHugeint<VarFormat::Binary>);
    }
      CASE_SERIALIZATION(duckdb::LogicalTypeId::BOOLEAN)
      CASE_SERIALIZATION(duckdb::LogicalTypeId::TIMESTAMP)
    case duckdb::LogicalTypeId::FLOAT: {
      // TODO: respect extra_float_digits from context
      static constexpr auto kSerializeText =
        SerializeFloat<float, VarFormat::Text, true>;
      static constexpr auto kSerializeBinary =
        SerializeFloat<float, VarFormat::Binary>;
      RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
    }
    case duckdb::LogicalTypeId::DOUBLE: {
      static constexpr auto kSerializeText =
        SerializeFloat<double, VarFormat::Text, true>;
      static constexpr auto kSerializeBinary =
        SerializeFloat<double, VarFormat::Binary>;
      RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
    }
    case duckdb::LogicalTypeId::VARCHAR: {
      static constexpr auto kSerializeText =
        SerializeVarchar<VarFormat::Text, false>;
      static constexpr auto kSerializeBinary =
        SerializeVarchar<VarFormat::Binary, false>;
      RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
    }
    case duckdb::LogicalTypeId::BLOB: {
      // TODO: respect bytea_output from context
      static constexpr auto kSerializeText = SerializeByteaTextHex<false>;
      RETURN_SERIALIZATION(kSerializeText, SerializeByteaBinary);
    }
    case duckdb::LogicalTypeId::DATE: {
      RETURN_SERIALIZATION(SerializeDate<VarFormat::Text>,
                           SerializeDate<VarFormat::Binary>);
    }
    case duckdb::LogicalTypeId::INTERVAL: {
      RETURN_SERIALIZATION(SerializeInterval<VarFormat::Text>,
                           SerializeInterval<VarFormat::Binary>);
    }
    case duckdb::LogicalTypeId::UUID: {
      RETURN_SERIALIZATION(SerializeUuid<VarFormat::Text>,
                           SerializeUuid<VarFormat::Binary>);
    }
    case duckdb::LogicalTypeId::LIST: {
      auto& child_type = duckdb::ListType::GetChildType(type);
      size_t dims = 1;
      auto* inner = &child_type;
      while (inner->id() == duckdb::LogicalTypeId::LIST) {
        inner = &duckdb::ListType::GetChildType(*inner);
        dims++;
      }
      // TODO: pass proper SerializationContext for
      // extra_float_digits/bytea_output
      SerializationContext dummy_ctx{};
      return GetArraySerialization(*inner, format, dummy_ctx, dims);
    }
    case duckdb::LogicalTypeId::MAP:
      SDB_ASSERT(false, "TODO: Map serialization not yet supported");
      return nullptr;
    case duckdb::LogicalTypeId::STRUCT:
      SDB_ASSERT(false, "TODO: Struct serialization not yet supported");
      return nullptr;
    default:
      // Fallback: serialize as text via ToString
      static constexpr auto kSerializeText =
        SerializeVarchar<VarFormat::Text, false>;
      static constexpr auto kSerializeBinary =
        SerializeVarchar<VarFormat::Binary, false>;
      RETURN_SERIALIZATION(kSerializeText, kSerializeBinary);
  }
}

}  // namespace sdb::pg
