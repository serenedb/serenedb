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
#include <absl/strings/str_cat.h>
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
#include <duckdb/inet/inet_ipaddress.hpp>
#include <limits>
#include <string_view>
#include <type_traits>

#define SDB_PG_LOGICAL_TYPES_NO_FACTORY

#include "basics/assert.h"
#include "basics/dtoa.h"
#include "basics/log.h"
#include "basics/misc.hpp"
#include "connector/pg_logical_types.h"
#include "pg/errcodes.h"
#include "pg/pg_types.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "query/config.h"

namespace sdb::pg {
namespace {

#define RETURN_SERIALIZATION(serialize_text, serialize_binary) \
  return format != VarFormat::Text                             \
           ? SerializeNullable<serialize_binary>               \
           : (context.in_record ? serialize_text               \
                                : SerializeNullable<serialize_text>)

#define RETURN_IN_RECORD_SERIALIZATION(text, text_in_record, binary) \
  return format != VarFormat::Text                                   \
           ? SerializeNullable<binary>                               \
           : (context.in_record ? text_in_record : SerializeNullable<text>)

enum class ArrayKind {
  ListSingleDimension,
  ArraySingleDimension,
  MultiDimensions,
};

inline constexpr int32_t kDynamicOid = -2;

enum class WrapContext : uint8_t {
  None,
  Array,
  Record,
};

bool RecordItemNeedsQuoting(std::string_view s);
template<WrapContext InContainer>
bool NeedsQuotingIn(const duckdb::LogicalType& type,
                    const duckdb::RecursiveUnifiedVectorFormat& vdata,
                    duckdb::idx_t row);

struct ArraySlice {
  duckdb::idx_t size;
  duckdb::idx_t offset;
};

ArraySlice GetArraySlice(const duckdb::RecursiveUnifiedVectorFormat& vdata,
                         duckdb::idx_t row) {
  const auto source_row = vdata.unified.sel->get_index(row);
  if (vdata.logical_type.id() == duckdb::LogicalTypeId::ARRAY) {
    auto size = duckdb::ArrayType::GetSize(vdata.logical_type);
    return {size, source_row * size};
  }
  auto list_data = vdata.unified.GetData<duckdb::list_entry_t>()[source_row];
  return {list_data.length, list_data.offset};
}

void EmitEscaped(SerializationContext& ctx, std::string_view s) {
  if (ctx.quote_seq.size() == 1 && ctx.backslash_count == 1) {
    ctx.buffer->WriteUncommitted(s);
    return;
  }
  size_t out_size = 0;
  for (char c : s) {
    if (c == '"') {
      out_size += ctx.quote_seq.size();
    } else if (c == '\\') {
      out_size += ctx.backslash_count;
    } else {
      ++out_size;
    }
  }
  ctx.buffer->WriteContiguousData(out_size, [&](uint8_t* data) {
    char* p = reinterpret_cast<char*>(data);
    for (char c : s) {
      if (c == '"') {
        std::memcpy(p, ctx.quote_seq.data(), ctx.quote_seq.size());
        p += ctx.quote_seq.size();
      } else if (c == '\\') {
        std::memset(p, '\\', ctx.backslash_count);
        p += ctx.backslash_count;
      } else {
        *p++ = c;
      }
    }
    return out_size;
  });
}

// Array escape rule:
//   source '"'  -> previous '\\' encoding + previous '"' encoding
//   source '\\' -> two previous '\\' encodings (count doubles)
void EnterArrayWrap(SerializationContext& ctx, std::string& q_buf) {
  q_buf.clear();
  q_buf.append(ctx.backslash_count, '\\');
  q_buf.append(ctx.quote_seq);
  ctx.quote_seq = q_buf;
  ctx.backslash_count *= 2;
}

// Record escape rule:
//   source '"'  -> two previous '"' encodings
//   source '\\' -> two previous '\\' encodings (count doubles)
void EnterRecordWrap(SerializationContext& ctx, std::string& q_buf) {
  q_buf.clear();
  absl::StrAppend(&q_buf, ctx.quote_seq, ctx.quote_seq);
  ctx.quote_seq = q_buf;
  ctx.backslash_count *= 2;
}

template<WrapContext Wrap, typename Body>
void WriteWrapped(SerializationContext& ctx, Body&& body) {
  static_assert(Wrap != WrapContext::None);
  ctx.buffer->WriteUncommitted(ctx.quote_seq);
  const auto saved_q = ctx.quote_seq;
  const auto saved_b = ctx.backslash_count;
  std::string q_buf;
  if constexpr (Wrap == WrapContext::Array) {
    EnterArrayWrap(ctx, q_buf);
  } else {
    EnterRecordWrap(ctx, q_buf);
  }
  body();
  ctx.quote_seq = saved_q;
  ctx.backslash_count = saved_b;
  ctx.buffer->WriteUncommitted(ctx.quote_seq);
}

#define RETURN_ARRAY_SERIALIZATION(serialize_text, serialize_binary, oid) \
  switch (kind) {                                                         \
    case ArrayKind::ListSingleDimension: {                                \
      static constexpr auto kElemText =                                   \
        SerializeOneDimArray<serialize_text, oid, VarFormat::Text,        \
                             ArrayKind::ListSingleDimension,              \
                             WrapContext::None>;                          \
      static constexpr auto kElemTextInRecord =                           \
        SerializeOneDimArray<serialize_text, oid, VarFormat::Text,        \
                             ArrayKind::ListSingleDimension,              \
                             WrapContext::Record>;                        \
      static constexpr auto kElemBinary =                                 \
        SerializeOneDimArray<serialize_binary, oid, VarFormat::Binary,    \
                             ArrayKind::ListSingleDimension,              \
                             WrapContext::None>;                          \
      return format != VarFormat::Text                                    \
               ? SerializeNullable<kElemBinary>                           \
               : (context.in_record ? kElemTextInRecord                   \
                                    : SerializeNullable<kElemText>);      \
    }                                                                     \
    case ArrayKind::ArraySingleDimension: {                               \
      static constexpr auto kElemText =                                   \
        SerializeOneDimArray<serialize_text, oid, VarFormat::Text,        \
                             ArrayKind::ArraySingleDimension,             \
                             WrapContext::None>;                          \
      static constexpr auto kElemTextInRecord =                           \
        SerializeOneDimArray<serialize_text, oid, VarFormat::Text,        \
                             ArrayKind::ArraySingleDimension,             \
                             WrapContext::Record>;                        \
      static constexpr auto kElemBinary =                                 \
        SerializeOneDimArray<serialize_binary, oid, VarFormat::Binary,    \
                             ArrayKind::ArraySingleDimension,             \
                             WrapContext::None>;                          \
      return format != VarFormat::Text                                    \
               ? SerializeNullable<kElemBinary>                           \
               : (context.in_record ? kElemTextInRecord                   \
                                    : SerializeNullable<kElemText>);      \
    }                                                                     \
    case ArrayKind::MultiDimensions: {                                    \
      static constexpr auto kElemText =                                   \
        SerializeArray<serialize_text, oid, VarFormat::Text,              \
                       WrapContext::None>;                                \
      static constexpr auto kElemTextInRecord =                           \
        SerializeArray<serialize_text, oid, VarFormat::Text,              \
                       WrapContext::Record>;                              \
      static constexpr auto kElemBinary =                                 \
        SerializeArray<serialize_binary, oid, VarFormat::Binary,          \
                       WrapContext::None>;                                \
      return format != VarFormat::Text                                    \
               ? SerializeNullable<kElemBinary>                           \
               : (context.in_record ? kElemTextInRecord                   \
                                    : SerializeNullable<kElemText>);      \
    }                                                                     \
  }

template<SerializationFunction ValueSerialization>
void SerializeNullable(SerializationContext& context,
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

void SerializeNull(SerializationContext& context,
                   const duckdb::RecursiveUnifiedVectorFormat&, duckdb::idx_t) {
  absl::big_endian::Store32(context.buffer->GetContiguousData(4), -1);
}

template<VarFormat Format, typename T, bool Precise = true>
void SerializeFloat(SerializationContext& context,
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
void SerializeInt(SerializationContext& context,
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

template<VarFormat Format, WrapContext InContainer>
void SerializeVarchar(SerializationContext& context,
                      const duckdb::RecursiveUnifiedVectorFormat& vdata,
                      duckdb::idx_t row) {
  auto raw = vdata.unified
               .GetData<duckdb::string_t>()[vdata.unified.sel->get_index(row)];
  auto value = std::string_view{raw.GetData(), raw.GetSize()};
  if constexpr (Format == VarFormat::Text) {
    if constexpr (InContainer == WrapContext::Array) {
      if (ArrayItemNeedQuotesAndEscape(value)) {
        WriteWrapped<WrapContext::Array>(context,
                                         [&] { EmitEscaped(context, value); });
        return;
      }
    } else if constexpr (InContainer == WrapContext::Record) {
      if (RecordItemNeedsQuoting(value)) {
        WriteWrapped<WrapContext::Record>(context,
                                          [&] { EmitEscaped(context, value); });
        return;
      }
    }
    context.buffer->WriteUncommitted(value);
  } else {
    context.buffer->WriteUncommitted(value);
  }
}

template<VarFormat Format, WrapContext InContainer, typename T>
void SerializeEnumLabel(SerializationContext& context,
                        const duckdb::RecursiveUnifiedVectorFormat& vdata,
                        duckdb::idx_t row) {
  auto idx = vdata.unified.sel->get_index(row);
  auto ordinal = duckdb::UnifiedVectorFormat::GetData<T>(vdata.unified)[idx];
  auto label = duckdb::EnumType::GetString(vdata.logical_type, ordinal);
  auto value = std::string_view{label.GetData(), label.GetSize()};
  if constexpr (Format == VarFormat::Text) {
    if constexpr (InContainer == WrapContext::Array) {
      if (ArrayItemNeedQuotesAndEscape(value)) {
        WriteWrapped<WrapContext::Array>(context,
                                         [&] { EmitEscaped(context, value); });
        return;
      }
    } else if constexpr (InContainer == WrapContext::Record) {
      if (RecordItemNeedsQuoting(value)) {
        WriteWrapped<WrapContext::Record>(context,
                                          [&] { EmitEscaped(context, value); });
        return;
      }
    }
    context.buffer->WriteUncommitted(value);
  } else {
    context.buffer->WriteUncommitted(value);
  }
}

template<VarFormat Format, WrapContext InContainer>
void SerializeEnum(SerializationContext& context,
                   const duckdb::RecursiveUnifiedVectorFormat& vdata,
                   duckdb::idx_t row) {
  switch (duckdb::EnumType::GetPhysicalType(vdata.logical_type)) {
    using enum duckdb::PhysicalType;
    case UINT8:
      return SerializeEnumLabel<Format, InContainer, uint8_t>(context, vdata,
                                                              row);
    case UINT16:
      return SerializeEnumLabel<Format, InContainer, uint16_t>(context, vdata,
                                                               row);
    case UINT32:
      return SerializeEnumLabel<Format, InContainer, uint32_t>(context, vdata,
                                                               row);
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("Unsupported ENUM physical type"));
  }
}

// Encode a value into PG numeric binary format.
// value is the unscaled integer (e.g. 12345 for 123.45 with scale=2).
// Use scale=0 for integer types. Caller must convert duckdb::hugeint_t /
// uhugeint_t to absl::int128 / absl::uint128 before calling.
template<typename T>
void WriteAsNumericBinary(SerializationContext& context, T value,
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
void SerializeDecimal(SerializationContext& context,
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
void SerializeUbigint(SerializationContext& context,
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
void SerializeHugeint(SerializationContext& context,
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
void SerializeUhugeint(SerializationContext& context,
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

// The leading '\\' goes through ctx.backslash_count so nested wraps emit
// the depth-correct escape sequence; the rest is hex-only and never triggers.
void ByteaOutHex(SerializationContext& ctx, std::string_view value) {
  EmitEscaped(ctx, "\\");
  const auto body_size = 1 + 2 * value.size();
  auto* data = ctx.buffer->GetContiguousData(body_size);
  char* p = reinterpret_cast<char*>(data);
  *p++ = 'x';
  absl::BytesToHexStringInternal(
    reinterpret_cast<const unsigned char*>(value.data()), p, value.size());
}

template<WrapContext InContainer>
void SerializeByteaTextHex(SerializationContext& context,
                           const duckdb::RecursiveUnifiedVectorFormat& vdata,
                           duckdb::idx_t row) {
  auto raw = vdata.unified
               .GetData<duckdb::string_t>()[vdata.unified.sel->get_index(row)];
  auto value = std::string_view{raw.GetData(), raw.GetSize()};

  // '\\xnnnn' always starts with '\\' which triggers wrap in both array and
  // record contexts.
  if constexpr (InContainer == WrapContext::None) {
    ByteaOutHex(context, value);
  } else {
    WriteWrapped<InContainer>(context, [&] { ByteaOutHex(context, value); });
  }
}

void ByteaOutEscape(SerializationContext& ctx, std::string_view value) {
  size_t backslash_cnt = 0;
  size_t non_printable_cnt = 0;
  for (char c : value) {
    if (c == '\\') {
      ++backslash_cnt;
    } else if (!absl::ascii_isprint(c)) {
      ++non_printable_cnt;
    }
  }
  const auto bs_n = ctx.backslash_count;
  const size_t body_size = (value.size() - backslash_cnt - non_printable_cnt) +
                           backslash_cnt * 2 * bs_n +
                           non_printable_cnt * (bs_n + 3);
  auto* data = ctx.buffer->GetContiguousData(body_size);
  char* p = reinterpret_cast<char*>(data);
  for (unsigned char c : value) {
    if (c == '\\') {
      std::memset(p, '\\', bs_n);
      p += bs_n;
      std::memset(p, '\\', bs_n);
      p += bs_n;
    } else if (!absl::ascii_isprint(c)) {
      std::memset(p, '\\', bs_n);
      p += bs_n;
      unsigned char ch = c;
      p[2] = '0' + (ch & 07);
      ch >>= 3;
      p[1] = '0' + (ch & 07);
      ch >>= 3;
      p[0] = '0' + (ch & 03);
      p += 3;
    } else {
      *p++ = static_cast<char>(c);
    }
  }
}

template<WrapContext InContainer>
void SerializeByteaTextEscape(SerializationContext& context,
                              const duckdb::RecursiveUnifiedVectorFormat& vdata,
                              duckdb::idx_t row) {
  auto raw = vdata.unified
               .GetData<duckdb::string_t>()[vdata.unified.sel->get_index(row)];
  auto value = std::string_view{raw.GetData(), raw.GetSize()};

  if constexpr (InContainer == WrapContext::None) {
    ByteaOutEscape(context, value);
  } else {
    // Wrap iff the rendered escape form has a wrap-trigger char: empty,
    // any source '\\' (becomes '\\' in the body), or any non-printable byte
    // (becomes '\\nnn').
    const bool has_special = value.empty() || absl::c_any_of(value, [](char c) {
                               return c == '\\' || !absl::ascii_isprint(c);
                             });
    if (has_special) {
      WriteWrapped<InContainer>(context,
                                [&] { ByteaOutEscape(context, value); });
    } else {
      ByteaOutEscape(context, value);
    }
  }
}

void SerializeByteaBinary(SerializationContext& context,
                          const duckdb::RecursiveUnifiedVectorFormat& vdata,
                          duckdb::idx_t row) {
  auto raw = vdata.unified
               .GetData<duckdb::string_t>()[vdata.unified.sel->get_index(row)];
  auto value = std::string_view{raw.GetData(), raw.GetSize()};
  context.buffer->WriteUncommitted(value);
}

template<VarFormat Format>
void SerializeBool(SerializationContext& context,
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

// Wrap text output in current-depth quote_seq when emitting inside an
// array or record. Safe only for types whose textual form never contains
// '"' or '\\' (timestamps, intervals) -- no internal escaping needed.
template<WrapContext InContainer, typename Body>
void WithWrapIfNested(SerializationContext& context, Body&& body) {
  if constexpr (InContainer == WrapContext::None) {
    body();
  } else {
    WriteWrapped<InContainer>(context, [&] { body(); });
  }
}

template<VarFormat Format, WrapContext InContainer>
void SerializeTimestampSec(SerializationContext& context,
                           const duckdb::RecursiveUnifiedVectorFormat& vdata,
                           duckdb::idx_t row) {
  const auto timestamp =
    vdata.unified
      .GetData<duckdb::timestamp_sec_t>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    auto str = duckdb::Timestamp::ToString(
      duckdb::Timestamp::FromEpochSeconds(timestamp.value));
    WithWrapIfNested<InContainer>(
      context, [&] { context.buffer->WriteUncommitted(str); });
  } else {
    absl::big_endian::Store64(context.buffer->GetContiguousData(8),
                              (timestamp.value - kGapSec) * 1'000'000);
  }
}

template<VarFormat Format, WrapContext InContainer>
void SerializeTimestampMs(SerializationContext& context,
                          const duckdb::RecursiveUnifiedVectorFormat& vdata,
                          duckdb::idx_t row) {
  const auto timestamp =
    vdata.unified
      .GetData<duckdb::timestamp_ms_t>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    auto str = duckdb::Timestamp::ToString(
      duckdb::Timestamp::FromEpochMicroSeconds(timestamp.value));
    WithWrapIfNested<InContainer>(
      context, [&] { context.buffer->WriteUncommitted(str); });
  } else {
    absl::big_endian::Store64(context.buffer->GetContiguousData(8),
                              (timestamp.value - kGapMs) * 1000);
  }
}

template<VarFormat Format, WrapContext InContainer>
void SerializeTimestamp(SerializationContext& context,
                        const duckdb::RecursiveUnifiedVectorFormat& vdata,
                        duckdb::idx_t row) {
  const auto timestamp =
    vdata.unified
      .GetData<duckdb::timestamp_t>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    auto str = duckdb::Timestamp::ToString(timestamp);
    WithWrapIfNested<InContainer>(
      context, [&] { context.buffer->WriteUncommitted(str); });
  } else {
    absl::big_endian::Store64(context.buffer->GetContiguousData(8),
                              timestamp.value - kGapUs);
  }
}

template<VarFormat Format, WrapContext InContainer>
void SerializeTimestampNs(SerializationContext& context,
                          const duckdb::RecursiveUnifiedVectorFormat& vdata,
                          duckdb::idx_t row) {
  const auto timestamp =
    vdata.unified
      .GetData<duckdb::timestamp_ns_t>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    auto str = duckdb::Timestamp::ToString(
      duckdb::Timestamp::FromEpochNanoSeconds(timestamp.value));
    WithWrapIfNested<InContainer>(
      context, [&] { context.buffer->WriteUncommitted(str); });
  } else {
    absl::big_endian::Store64(context.buffer->GetContiguousData(8),
                              (timestamp.value - kGapNs) / 1000);
  }
}

template<VarFormat Format, WrapContext InContainer>
void SerializeTimestampTz(SerializationContext& context,
                          const duckdb::RecursiveUnifiedVectorFormat& vdata,
                          duckdb::idx_t row) {
  const auto ts =
    vdata.unified
      .GetData<duckdb::timestamp_tz_t>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    auto str = duckdb::Timestamp::ToString(duckdb::timestamp_t{ts});
    WithWrapIfNested<InContainer>(context, [&] {
      context.buffer->WriteUncommitted(str);
      context.buffer->WriteUncommitted("+00");
    });
  } else {
    absl::big_endian::Store64(context.buffer->GetContiguousData(8),
                              ts.value - kGapUs);
  }
}

template<VarFormat Format>
void SerializeTime(SerializationContext& context,
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
void SerializeTimeNs(SerializationContext& context,
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
void SerializeTimeTz(SerializationContext& context,
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
void SerializeBit(SerializationContext& context,
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

// PG `inet` (OID 869). The source is the inet extension's STRUCT
// {ip_type UTINYINT, address HUGEINT, mask USMALLINT}: ip_type is 1 for IPv4,
// 2 for IPv6; `address` is the unsigned IP stored as a signed hugeint with the
// IPv6 top bit flipped for sort order. PG's wire family is 2 (IPv4) / 3 (IPv6).
template<VarFormat Format>
void SerializeInet(SerializationContext& context,
                   const duckdb::RecursiveUnifiedVectorFormat& vdata,
                   duckdb::idx_t row) {
  const auto& ip_type_d = vdata.children[0].unified;
  const auto& addr_d = vdata.children[1].unified;
  const auto& mask_d = vdata.children[2].unified;
  const auto ip_type =
    ip_type_d.GetData<uint8_t>()[ip_type_d.sel->get_index(row)];
  const auto stored =
    addr_d.GetData<duckdb::hugeint_t>()[addr_d.sel->get_index(row)];
  const auto mask = mask_d.GetData<uint16_t>()[mask_d.sel->get_index(row)];

  const bool is_v6 = ip_type == 2;
  uint64_t hi = static_cast<uint64_t>(stored.upper);
  const uint64_t lo = stored.lower;
  if (is_v6) {
    hi ^= (uint64_t{1} << 63);  // undo the IPv6 sort-order flip
  }

  if constexpr (Format == VarFormat::Text) {
    // Reuse the extension's formatter (handles IPv6 canonicalization and the
    // "/mask" suffix rules) so output is identical to host()/::varchar.
    INET_IPAddress inet;
    inet.type = static_cast<INET_IPAddressType>(ip_type);
    inet.address.lower = lo;
    inet.address.upper = hi;
    inet.mask = mask;
    char buf[64];
    const size_t len = ipaddress_to_string(&inet, buf, sizeof(buf));
    context.buffer->WriteUncommitted(std::string_view{buf, len});
  } else {
    // PG binary: family(1), bits(1), is_cidr(1), nb(1), then nb address bytes.
    const uint8_t nb = is_v6 ? 16 : 4;
    auto* data = context.buffer->GetContiguousData(4 + nb);
    data[0] = is_v6 ? 3 : 2;
    data[1] = static_cast<uint8_t>(mask);
    data[2] = 0;
    data[3] = nb;
    if (is_v6) {
      absl::big_endian::Store64(data + 4, hi);
      absl::big_endian::Store64(data + 12, lo);
    } else {
      absl::big_endian::Store32(data + 4, static_cast<uint32_t>(lo));
    }
  }
}

template<SerializationFunction ElementSerialization, int32_t ElementOID,
         VarFormat Format, ArrayKind Kind, WrapContext InContainer>
void SerializeOneDimArray(SerializationContext& context,
                          const duckdb::RecursiveUnifiedVectorFormat& vdata,
                          duckdb::idx_t row) {
  auto [array_size, array_offset] = GetArraySlice(vdata, row);
  auto& child_vdata = vdata.children[0];
  if constexpr (Format == VarFormat::Text) {
    auto emit_inside = [&] {
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
    };

    if constexpr (InContainer == WrapContext::Record) {
      // Wrap the rendered '{...}' as a record field iff its content has
      // any record trigger char ('(', ')', ',', '"', '\\', whitespace)
      // or is empty. '{' / '}' are NOT record triggers.
      const bool needs_wrap = [&] {
        if (array_size == 0) {
          return false;  // bare '{}'
        }
        if (array_size > 1) {
          return true;  // ',' between elements
        }
        return NeedsQuotingIn<WrapContext::Record>(child_vdata.logical_type,
                                                   child_vdata, array_offset);
      }();
      if (needs_wrap) {
        WriteWrapped<WrapContext::Record>(context, emit_inside);
      } else {
        emit_inside();
      }
    } else {
      emit_inside();
    }
  } else {
    // dimensions (4) - amount of array dims
    // flags(4) - 0(no nulls), 1(have nulls)
    // element_oid (4) - oid of an array element
    // dim1 size (4) - size of first(and only) dim
    // lower_bound (4) - begin offset(0 by default)
    int32_t element_oid;
    if constexpr (ElementOID == kDynamicOid) {
      element_oid = Type2Oid(child_vdata.logical_type, false);
    } else {
      element_oid = ElementOID;
    }
    auto* prefix_data = context.buffer->GetContiguousData(20);
    absl::big_endian::Store32(prefix_data, 1);
    absl::big_endian::Store32(prefix_data + 4, 1);
    absl::big_endian::Store32(prefix_data + 8, element_oid);
    absl::big_endian::Store32(prefix_data + 12, array_size);
    absl::big_endian::Store32(prefix_data + 16, 0);
    for (duckdb::idx_t i = 0; i < array_size; ++i) {
      const auto element_row = array_offset + i;
      SerializeNullable<ElementSerialization>(context, child_vdata,
                                              element_row);
    }
  }
}

template<SerializationFunction ElementSerialization, VarFormat Format>
void FlattenArray(SerializationContext& context,
                  const duckdb::RecursiveUnifiedVectorFormat& vdata,
                  duckdb::idx_t row, int32_t& leaf_oid, uint8_t* dim_sizes_data,
                  int32_t depth) {
  const auto lid = vdata.logical_type.id();
  if (lid != duckdb::LogicalTypeId::LIST &&
      lid != duckdb::LogicalTypeId::ARRAY) {
    leaf_oid = Type2Oid(vdata.logical_type, false);
    SerializeNullable<ElementSerialization>(context, vdata, row);
    return;
  }
  auto [array_size, array_offset] = GetArraySlice(vdata, row);
  absl::big_endian::Store32(dim_sizes_data + depth * 8,
                            static_cast<int32_t>(array_size));
  absl::big_endian::Store32(dim_sizes_data + depth * 8 + 4, 0);
  auto& child_vdata = vdata.children[0];
  if (array_size == 0) {
    const auto* leaf = &child_vdata.logical_type;
    int32_t deeper = depth + 1;
    while (leaf->id() == duckdb::LogicalTypeId::LIST ||
           leaf->id() == duckdb::LogicalTypeId::MAP ||
           leaf->id() == duckdb::LogicalTypeId::ARRAY) {
      absl::big_endian::Store32(dim_sizes_data + deeper * 8, 0);
      absl::big_endian::Store32(dim_sizes_data + deeper * 8 + 4, 0);
      ++deeper;
      leaf = leaf->id() == duckdb::LogicalTypeId::ARRAY
               ? &duckdb::ArrayType::GetChildType(*leaf)
               : &duckdb::ListType::GetChildType(*leaf);
    }
    leaf_oid = Type2Oid(*leaf, false);
    return;
  }
  for (duckdb::idx_t i = 0; i < array_size; ++i) {
    FlattenArray<ElementSerialization, Format>(context, child_vdata,
                                               array_offset + i, leaf_oid,
                                               dim_sizes_data, depth + 1);
  }
}

template<SerializationFunction ElementSerialization, int32_t ElementOID,
         VarFormat Format, WrapContext InContainer>
void SerializeArray(SerializationContext& context,
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

    auto [array_size, array_offset] = GetArraySlice(vdata, row);
    auto& child_vdata = vdata.children[0];

    auto emit_inside = [&] {
      context.buffer->WriteUncommitted("{");
      for (duckdb::idx_t i = 0; i < array_size; ++i) {
        if (i > 0) {
          context.buffer->WriteUncommitted(",");
        }
        const auto element_row = array_offset + i;
        SerializeArray<ElementSerialization, ElementOID, Format,
                       WrapContext::None>(context, child_vdata, element_row);
      }
      context.buffer->WriteUncommitted("}");
    };

    if constexpr (InContainer == WrapContext::Record) {
      const bool needs_wrap = [&] {
        if (array_size == 0) {
          return false;  // bare '{}' (or '{...}' with no triggers).
        }
        if (array_size > 1) {
          return true;  // ',' between elements
        }
        return NeedsQuotingIn<WrapContext::Record>(child_vdata.logical_type,
                                                   child_vdata, array_offset);
      }();
      if (needs_wrap) {
        WriteWrapped<WrapContext::Record>(context, emit_inside);
      } else {
        emit_inside();
      }
    } else {
      emit_inside();
    }
  } else {
    // Walk the type to find the leaf OID and count ndim.
    const auto* t = &vdata.logical_type;
    int32_t ndim = 0;
    while (t->id() == duckdb::LogicalTypeId::LIST ||
           t->id() == duckdb::LogicalTypeId::ARRAY) {
      ++ndim;
      t = t->id() == duckdb::LogicalTypeId::ARRAY
            ? &duckdb::ArrayType::GetChildType(*t)
            : &duckdb::ListType::GetChildType(*t);
    }
    // PG binary array: 12-byte top header (ndim, flags, elemtype) followed
    // by ndim*8 bytes of {dim_size, lbound} pairs, then element bytes.
    // FlattenArray fills each dim's size into its slot as it descends.
    auto* prefix_data = context.buffer->GetContiguousData(12 + ndim * 8);
    int32_t leaf_oid = ElementOID;
    FlattenArray<ElementSerialization, Format>(context, vdata, row, leaf_oid,
                                               prefix_data + 12, 0);
    absl::big_endian::Store32(prefix_data, ndim);
    absl::big_endian::Store32(prefix_data + 4, 0);
    absl::big_endian::Store32(prefix_data + 8, leaf_oid);
  }
}

template<VarFormat Format>
void SerializeDate(SerializationContext& context,
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

void SerializeRegtypeText(SerializationContext& context,
                          const duckdb::RecursiveUnifiedVectorFormat& vdata,
                          duckdb::idx_t row) {
  const auto oid =
    vdata.unified.GetData<int64_t>()[vdata.unified.sel->get_index(row)];
  context.buffer->WriteUncommitted(RegtypeOut(oid));
}

void SerializeRegclassText(SerializationContext& context,
                           const duckdb::RecursiveUnifiedVectorFormat& vdata,
                           duckdb::idx_t row) {
  const auto oid =
    vdata.unified.GetData<int64_t>()[vdata.unified.sel->get_index(row)];
  context.buffer->WriteUncommitted(RegclassOut(*context.snapshot, oid));
}

void SerializeRegnamespaceText(
  SerializationContext& context,
  const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t row) {
  const auto oid =
    vdata.unified.GetData<int64_t>()[vdata.unified.sel->get_index(row)];
  context.buffer->WriteUncommitted(RegnamespaceOut(*context.snapshot, oid));
}

// Binary serialization for oid-like types:
// truncate 64-bit OID to 32-bit for PG wire protocol compatibility.
void SerializeOidBinary(SerializationContext& context,
                        const duckdb::RecursiveUnifiedVectorFormat& vdata,
                        duckdb::idx_t row) {
  const auto oid =
    vdata.unified.GetData<int64_t>()[vdata.unified.sel->get_index(row)];
  if (oid != static_cast<int32_t>(oid)) {
    SDB_WARN(HTTP, "reg* OID ", oid,
             " truncated to 32-bit for binary wire protocol");
  }
  absl::big_endian::Store32(context.buffer->GetContiguousData(4),
                            static_cast<int32_t>(oid));
}

template<VarFormat Format, WrapContext InContainer>
void SerializeInterval(SerializationContext& context,
                       const duckdb::RecursiveUnifiedVectorFormat& vdata,
                       duckdb::idx_t row) {
  const auto interval =
    vdata.unified
      .GetData<duckdb::interval_t>()[vdata.unified.sel->get_index(row)];
  if constexpr (Format == VarFormat::Text) {
    auto str = duckdb::Interval::ToString(interval);
    std::string_view value{str};
    if constexpr (InContainer == WrapContext::Array) {
      if (ArrayItemNeedQuotesAndEscape(value)) {
        WriteWrapped<WrapContext::Array>(context,
                                         [&] { EmitEscaped(context, value); });
        return;
      }
    } else if constexpr (InContainer == WrapContext::Record) {
      if (RecordItemNeedsQuoting(value)) {
        WriteWrapped<WrapContext::Record>(context,
                                          [&] { EmitEscaped(context, value); });
        return;
      }
    }
    context.buffer->WriteUncommitted(value);
  } else {
    // PG binary: microseconds(8) + days(4) + months(4)
    auto* data = context.buffer->GetContiguousData(16);
    absl::big_endian::Store64(data, interval.micros);
    absl::big_endian::Store32(data + 8, interval.days);
    absl::big_endian::Store32(data + 12, interval.months);
  }
}

template<VarFormat Format>
void SerializeUuid(SerializationContext& context,
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

template<VarFormat Format, WrapContext InContainer>
void SerializeJson(SerializationContext& context,
                   const duckdb::RecursiveUnifiedVectorFormat& vdata,
                   duckdb::idx_t row) {
  const auto str =
    vdata.unified
      .GetData<duckdb::string_t>()[vdata.unified.sel->get_index(row)];
  auto value = std::string_view{str.GetData(), str.GetSize()};
  if constexpr (Format == VarFormat::Text) {
    if constexpr (InContainer == WrapContext::Array) {
      if (ArrayItemNeedQuotesAndEscape(value)) {
        WriteWrapped<WrapContext::Array>(context,
                                         [&] { EmitEscaped(context, value); });
        return;
      }
    } else if constexpr (InContainer == WrapContext::Record) {
      if (RecordItemNeedsQuoting(value)) {
        WriteWrapped<WrapContext::Record>(context,
                                          [&] { EmitEscaped(context, value); });
        return;
      }
    }
    context.buffer->WriteUncommitted(value);
  } else {
    context.buffer->WriteUncommitted(value);
  }
}

bool RecordItemNeedsQuoting(std::string_view s) {
  if (s.empty()) {
    return true;
  }
  for (char c : s) {
    if (c == ',' || c == '(' || c == ')' || c == '"' || c == '\\' ||
        absl::ascii_isspace(static_cast<unsigned char>(c))) {
      return true;
    }
  }
  return false;
}

bool RecordHasArrayTrigger(const duckdb::LogicalType& type,
                           const duckdb::RecursiveUnifiedVectorFormat& vdata,
                           duckdb::idx_t row);

template<WrapContext InContainer>
bool NeedsQuotingIn(const duckdb::LogicalType& type,
                    const duckdb::RecursiveUnifiedVectorFormat& vdata,
                    duckdb::idx_t row) {
  static_assert(InContainer == WrapContext::Array ||
                InContainer == WrapContext::Record);
  using enum duckdb::LogicalTypeId;
  switch (type.id()) {
    case BOOLEAN:
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
    case UTINYINT:
    case USMALLINT:
    case UINTEGER:
    case UBIGINT:
    case HUGEINT:
    case UHUGEINT:
    case FLOAT:
    case DOUBLE:
    case DECIMAL:
    case DATE:
    case TIME:
    case TIME_NS:
    case TIME_TZ:
    case UUID:
    case BIT:
      return false;
    case TIMESTAMP_SEC:
    case TIMESTAMP_MS:
    case TIMESTAMP:
    case TIMESTAMP_NS:
    case TIMESTAMP_TZ:
    case BLOB:
    case STRUCT:
      return true;
    case INTERVAL: {
      const auto interval =
        vdata.unified
          .GetData<duckdb::interval_t>()[vdata.unified.sel->get_index(row)];
      auto str = duckdb::Interval::ToString(interval);
      std::string_view v{str};
      return RecordItemNeedsQuoting(v) || ArrayItemNeedQuotesAndEscape(v);
    }
    case CHAR:
    case VARCHAR: {
      const auto& raw =
        vdata.unified
          .GetData<duckdb::string_t>()[vdata.unified.sel->get_index(row)];
      std::string_view v{raw.GetData(), raw.GetSize()};
      return RecordItemNeedsQuoting(v) || ArrayItemNeedQuotesAndEscape(v);
    }
    case ENUM: {
      auto idx = vdata.unified.sel->get_index(row);
      auto check = [&](auto ord) {
        const auto& label = duckdb::EnumType::GetString(type, ord);
        std::string_view v{label.GetData(), label.GetSize()};
        return RecordItemNeedsQuoting(v) || ArrayItemNeedQuotesAndEscape(v);
      };
      switch (duckdb::EnumType::GetPhysicalType(type)) {
        using enum duckdb::PhysicalType;
        case UINT8:
          return check(
            duckdb::UnifiedVectorFormat::GetData<uint8_t>(vdata.unified)[idx]);
        case UINT16:
          return check(
            duckdb::UnifiedVectorFormat::GetData<uint16_t>(vdata.unified)[idx]);
        case UINT32:
          return check(
            duckdb::UnifiedVectorFormat::GetData<uint32_t>(vdata.unified)[idx]);
        default:
          return true;
      }
    }
    case LIST:
    case ARRAY:
    case MAP:
      // Rendered `{...}`. `{`/`}` are array triggers (always wraps inside an
      // array). For records: walk the singleton path -- any size>1 contributes
      // `,` (record trigger), size 0 collapses to `{}` (no trigger), else
      // recurse to the leaf and ask the same question there.
      if constexpr (InContainer == WrapContext::Array) {
        return true;
      } else {
        static_assert(InContainer == WrapContext::Record);
        const auto* cur = &vdata;
        duckdb::idx_t cur_row = row;
        while (true) {
          const auto lid = cur->logical_type.id();
          if (lid != duckdb::LogicalTypeId::LIST &&
              lid != duckdb::LogicalTypeId::ARRAY) {
            return NeedsQuotingIn<WrapContext::Record>(cur->logical_type, *cur,
                                                       cur_row);
          }
          auto [array_size, array_offset] = GetArraySlice(*cur, cur_row);
          if (array_size > 1) {
            return true;
          }
          if (array_size == 0) {
            return false;
          }
          cur = &cur->children[0];
          cur_row = array_offset;
        }
      }
    default:
      return true;
  }
}

bool RecordHasArrayTrigger(const duckdb::LogicalType& type,
                           const duckdb::RecursiveUnifiedVectorFormat& vdata,
                           duckdb::idx_t row) {
  const auto& children = duckdb::StructType::GetChildTypes(type);
  if (children.size() != 1) {
    // 0 fields -> '()' (no trigger). N>1 -> ',' is always present.
    return children.size() > 1;
  }
  const auto& child = vdata.children[0];
  if (!child.unified.validity.RowIsValid(child.unified.sel->get_index(row))) {
    return false;  // '()' for the lone NULL field
  }
  return NeedsQuotingIn<WrapContext::Array>(children[0].second, child, row);
}

template<VarFormat Format>
const RecordSerializers& GetSerializersCache(
  SerializationContext& context, const duckdb::LogicalType& struct_type) {
  SDB_ASSERT(context.types_cache != nullptr);
  auto [it, inserted] = context.types_cache->try_emplace(&struct_type);
  auto& cached = it->second;
  if (inserted) {
    const auto& children = duckdb::StructType::GetChildTypes(struct_type);
    cached.functions.reserve(children.size());
    if constexpr (Format == VarFormat::Text) {
      const bool saved = std::exchange(context.in_record, true);
      for (const auto& [_, child_type] : children) {
        cached.functions.push_back(
          GetSerialization(child_type, VarFormat::Text, context));
      }
      context.in_record = saved;
    } else {
      cached.oids.reserve(children.size());
      for (const auto& [_, child_type] : children) {
        cached.functions.push_back(
          GetSerialization(child_type, VarFormat::Binary, context));
        cached.oids.push_back(Type2Oid(child_type, false));
      }
    }
  }
  return cached;
}

template<VarFormat Format, WrapContext InContainer>
void SerializeRecord(SerializationContext& context,
                     const duckdb::RecursiveUnifiedVectorFormat& vdata,
                     duckdb::idx_t row) {
  const auto& cache = GetSerializersCache<Format>(context, vdata.logical_type);

  if constexpr (Format == VarFormat::Text) {
    auto emit_inside = [&] {
      context.buffer->WriteUncommitted("(");
      for (size_t i = 0; i < cache.functions.size(); ++i) {
        if (i > 0) {
          context.buffer->WriteUncommitted(",");
        }
        const auto& child = vdata.children[i];
        if (!child.unified.validity.RowIsValid(
              child.unified.sel->get_index(row))) {
          continue;  // empty between commas means NULL in record text
        }
        cache.functions[i](context, child, row);
      }
      context.buffer->WriteUncommitted(")");
    };

    if constexpr (InContainer == WrapContext::Array) {
      if (RecordHasArrayTrigger(vdata.logical_type, vdata, row)) {
        WriteWrapped<WrapContext::Array>(context, emit_inside);
      } else {
        emit_inside();
      }
    } else if constexpr (InContainer == WrapContext::Record) {
      // '(' is a record trigger, so a record-in-record always wraps.
      WriteWrapped<WrapContext::Record>(context, emit_inside);
    } else {
      emit_inside();
    }
  } else {
    // PG record binary format: int32 nfields; for each field { int32 OID,
    // int32 length (-1 for NULL) followed by length bytes }.
    auto* nfields_data = context.buffer->GetContiguousData(4);
    absl::big_endian::Store32(nfields_data,
                              static_cast<int32_t>(cache.functions.size()));
    for (size_t i = 0; i < cache.functions.size(); ++i) {
      absl::big_endian::Store32(context.buffer->GetContiguousData(4),
                                cache.oids[i]);
      cache.functions[i](context, vdata.children[i], row);
    }
  }
}

SerializationFunction GetArraySerialization(const duckdb::LogicalType& type,
                                            VarFormat format,
                                            SerializationContext& context,
                                            ArrayKind kind) {
  SDB_ASSERT(!context.in_record || format == VarFormat::Text);
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
          SerializeJson<VarFormat::Text, WrapContext::Array>;
        static constexpr auto kSerializeBinary =
          SerializeJson<VarFormat::Binary, WrapContext::None>;
        RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, kJson);
      }
      if (IsName(type)) {
        static constexpr auto kSerializeText =
          SerializeVarchar<VarFormat::Text, WrapContext::Array>;
        static constexpr auto kSerializeBinary =
          SerializeVarchar<VarFormat::Binary, WrapContext::None>;
        RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, kName);
      }
      static constexpr auto kSerializeText =
        SerializeVarchar<VarFormat::Text, WrapContext::Array>;
      static constexpr auto kSerializeBinary =
        SerializeVarchar<VarFormat::Binary, WrapContext::None>;
      RETURN_ARRAY_SERIALIZATION(kSerializeText, kSerializeBinary, kText);
    }
    case BLOB: {
      if (context.bytea_output == ByteaOutput::Hex) {
        static constexpr auto kSerializeText =
          SerializeByteaTextHex<WrapContext::Array>;
        RETURN_ARRAY_SERIALIZATION(kSerializeText, SerializeByteaBinary,
                                   kBytea);
      } else {
        SDB_ASSERT(context.bytea_output == ByteaOutput::Escape);
        static constexpr auto kSerializeText =
          SerializeByteaTextEscape<WrapContext::Array>;
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
    case TIMESTAMP_SEC: {
      static constexpr auto kText =
        SerializeTimestampSec<VarFormat::Text, WrapContext::Array>;
      static constexpr auto kBinary =
        SerializeTimestampSec<VarFormat::Binary, WrapContext::Array>;
      RETURN_ARRAY_SERIALIZATION(kText, kBinary, kTimestamp);
    }
    case TIMESTAMP_MS: {
      static constexpr auto kText =
        SerializeTimestampMs<VarFormat::Text, WrapContext::Array>;
      static constexpr auto kBinary =
        SerializeTimestampMs<VarFormat::Binary, WrapContext::Array>;
      RETURN_ARRAY_SERIALIZATION(kText, kBinary, kTimestamp);
    }
    case TIMESTAMP: {
      static constexpr auto kText =
        SerializeTimestamp<VarFormat::Text, WrapContext::Array>;
      static constexpr auto kBinary =
        SerializeTimestamp<VarFormat::Binary, WrapContext::Array>;
      RETURN_ARRAY_SERIALIZATION(kText, kBinary, kTimestamp);
    }
    case TIMESTAMP_NS: {
      static constexpr auto kText =
        SerializeTimestampNs<VarFormat::Text, WrapContext::Array>;
      static constexpr auto kBinary =
        SerializeTimestampNs<VarFormat::Binary, WrapContext::Array>;
      RETURN_ARRAY_SERIALIZATION(kText, kBinary, kTimestamp);
    }
    case TIMESTAMP_TZ: {
      static constexpr auto kText =
        SerializeTimestampTz<VarFormat::Text, WrapContext::Array>;
      static constexpr auto kBinary =
        SerializeTimestampTz<VarFormat::Binary, WrapContext::Array>;
      RETURN_ARRAY_SERIALIZATION(kText, kBinary, kTimestampTz);
    }
    case INTERVAL: {
      static constexpr auto kText =
        SerializeInterval<VarFormat::Text, WrapContext::Array>;
      static constexpr auto kBinary =
        SerializeInterval<VarFormat::Binary, WrapContext::Array>;
      RETURN_ARRAY_SERIALIZATION(kText, kBinary, kInterval);
    }
    case UUID:
      RETURN_ARRAY_SERIALIZATION(SerializeUuid<VarFormat::Text>,
                                 SerializeUuid<VarFormat::Binary>, kUuid);
    case BIT:
      RETURN_ARRAY_SERIALIZATION(SerializeBit<VarFormat::Text>,
                                 SerializeBit<VarFormat::Binary>, kVarbit);
    case STRUCT: {
      if (IsInet(type)) {
        RETURN_ARRAY_SERIALIZATION(SerializeInet<VarFormat::Text>,
                                   SerializeInet<VarFormat::Binary>, kInet);
      }
      // Element OID is resolved per-row by Type2Oid: anonymous ROW(...) yields
      // kRecord, named record types yield their pg_type OID.
      static constexpr auto kText =
        SerializeRecord<VarFormat::Text, WrapContext::Array>;
      static constexpr auto kBinary =
        SerializeRecord<VarFormat::Binary, WrapContext::Array>;
      RETURN_ARRAY_SERIALIZATION(kText, kBinary, kDynamicOid);
    }
    case ENUM: {
      static constexpr auto kText =
        SerializeEnum<VarFormat::Text, WrapContext::Array>;
      static constexpr auto kBinary =
        SerializeEnum<VarFormat::Binary, WrapContext::Array>;
      RETURN_ARRAY_SERIALIZATION(kText, kBinary, kDynamicOid);
    }
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("Array element type not supported"));
  }
}

}  // namespace

void ByteaOutHex(char* buf, std::string_view value) {
  *buf++ = '\\';
  *buf++ = 'x';

  absl::BytesToHexStringInternal(
    reinterpret_cast<const unsigned char*>(value.data()), buf, value.size());
}

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
  return value.size() + backslash_cnt + non_printable_cnt * 3;
}

void ByteaOutEscape(char* buf, std::string_view value) {
  for (const char* in = value.data(), *in_end = value.data() + value.size();
       in != in_end; ++in) {
    if (*in == '\\') {
      *buf++ = '\\';
      *buf++ = '\\';
    } else if (!absl::ascii_isprint(*in)) {
      // As octal
      unsigned char c = *in;
      buf[0] = '\\';
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
}

void FillContext(const Config& config, SerializationContext& context) {
  context.extra_float_digits = config.GetExtraFloatDigits();
  context.bytea_output = config.GetByteaOutput();
  context.snapshot = config.EnsureCatalogSnapshot().get();
  context.types_cache = std::make_unique<TypesSerializationCache>();
}

SerializationFunction GetSerialization(const duckdb::LogicalType& type,
                                       VarFormat format,
                                       SerializationContext& context) {
  SDB_ASSERT(!context.in_record || format == VarFormat::Text);
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
        static constexpr auto kText =
          SerializeJson<VarFormat::Text, WrapContext::None>;
        static constexpr auto kTextInRecord =
          SerializeJson<VarFormat::Text, WrapContext::Record>;
        static constexpr auto kBinary =
          SerializeJson<VarFormat::Binary, WrapContext::None>;
        RETURN_IN_RECORD_SERIALIZATION(kText, kTextInRecord, kBinary);
      }
      static constexpr auto kText =
        SerializeVarchar<VarFormat::Text, WrapContext::None>;
      static constexpr auto kTextInRecord =
        SerializeVarchar<VarFormat::Text, WrapContext::Record>;
      static constexpr auto kBinary =
        SerializeVarchar<VarFormat::Binary, WrapContext::None>;
      RETURN_IN_RECORD_SERIALIZATION(kText, kTextInRecord, kBinary);
    }
    case BLOB: {
      if (context.bytea_output == ByteaOutput::Hex) {
        static constexpr auto kText = SerializeByteaTextHex<WrapContext::None>;
        static constexpr auto kTextInRecord =
          SerializeByteaTextHex<WrapContext::Record>;
        RETURN_IN_RECORD_SERIALIZATION(kText, kTextInRecord,
                                       SerializeByteaBinary);
      } else {
        SDB_ASSERT(context.bytea_output == ByteaOutput::Escape);
        static constexpr auto kText =
          SerializeByteaTextEscape<WrapContext::None>;
        static constexpr auto kTextInRecord =
          SerializeByteaTextEscape<WrapContext::Record>;
        RETURN_IN_RECORD_SERIALIZATION(kText, kTextInRecord,
                                       SerializeByteaBinary);
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
    case TIMESTAMP_SEC: {
      static constexpr auto kText =
        SerializeTimestampSec<VarFormat::Text, WrapContext::None>;
      static constexpr auto kTextInRecord =
        SerializeTimestampSec<VarFormat::Text, WrapContext::Record>;
      static constexpr auto kBinary =
        SerializeTimestampSec<VarFormat::Binary, WrapContext::None>;
      RETURN_IN_RECORD_SERIALIZATION(kText, kTextInRecord, kBinary);
    }
    case TIMESTAMP_MS: {
      static constexpr auto kText =
        SerializeTimestampMs<VarFormat::Text, WrapContext::None>;
      static constexpr auto kTextInRecord =
        SerializeTimestampMs<VarFormat::Text, WrapContext::Record>;
      static constexpr auto kBinary =
        SerializeTimestampMs<VarFormat::Binary, WrapContext::None>;
      RETURN_IN_RECORD_SERIALIZATION(kText, kTextInRecord, kBinary);
    }
    case TIMESTAMP: {
      static constexpr auto kText =
        SerializeTimestamp<VarFormat::Text, WrapContext::None>;
      static constexpr auto kTextInRecord =
        SerializeTimestamp<VarFormat::Text, WrapContext::Record>;
      static constexpr auto kBinary =
        SerializeTimestamp<VarFormat::Binary, WrapContext::None>;
      RETURN_IN_RECORD_SERIALIZATION(kText, kTextInRecord, kBinary);
    }
    case TIMESTAMP_NS: {
      static constexpr auto kText =
        SerializeTimestampNs<VarFormat::Text, WrapContext::None>;
      static constexpr auto kTextInRecord =
        SerializeTimestampNs<VarFormat::Text, WrapContext::Record>;
      static constexpr auto kBinary =
        SerializeTimestampNs<VarFormat::Binary, WrapContext::None>;
      RETURN_IN_RECORD_SERIALIZATION(kText, kTextInRecord, kBinary);
    }
    case TIMESTAMP_TZ: {
      static constexpr auto kText =
        SerializeTimestampTz<VarFormat::Text, WrapContext::None>;
      static constexpr auto kTextInRecord =
        SerializeTimestampTz<VarFormat::Text, WrapContext::Record>;
      static constexpr auto kBinary =
        SerializeTimestampTz<VarFormat::Binary, WrapContext::None>;
      RETURN_IN_RECORD_SERIALIZATION(kText, kTextInRecord, kBinary);
    }
    case INTERVAL: {
      static constexpr auto kText =
        SerializeInterval<VarFormat::Text, WrapContext::None>;
      static constexpr auto kTextInRecord =
        SerializeInterval<VarFormat::Text, WrapContext::Record>;
      static constexpr auto kBinary =
        SerializeInterval<VarFormat::Binary, WrapContext::None>;
      RETURN_IN_RECORD_SERIALIZATION(kText, kTextInRecord, kBinary);
    }
    case UUID:
      RETURN_SERIALIZATION(SerializeUuid<VarFormat::Text>,
                           SerializeUuid<VarFormat::Binary>);
    case BIT:
      RETURN_SERIALIZATION(SerializeBit<VarFormat::Text>,
                           SerializeBit<VarFormat::Binary>);
    case ENUM: {
      static constexpr auto kText =
        SerializeEnum<VarFormat::Text, WrapContext::None>;
      static constexpr auto kTextInRecord =
        SerializeEnum<VarFormat::Text, WrapContext::Record>;
      static constexpr auto kBinary =
        SerializeEnum<VarFormat::Binary, WrapContext::None>;
      RETURN_IN_RECORD_SERIALIZATION(kText, kTextInRecord, kBinary);
    }
    case STRUCT: {
      if (IsInet(type)) {
        RETURN_SERIALIZATION(SerializeInet<VarFormat::Text>,
                             SerializeInet<VarFormat::Binary>);
      }
      static constexpr auto kText =
        SerializeRecord<VarFormat::Text, WrapContext::None>;
      static constexpr auto kTextInRecord =
        SerializeRecord<VarFormat::Text, WrapContext::Record>;
      static constexpr auto kBinary =
        SerializeRecord<VarFormat::Binary, WrapContext::None>;
      RETURN_IN_RECORD_SERIALIZATION(kText, kTextInRecord, kBinary);
    }
    case MAP:
    case LIST:
    case ARRAY: {
      const auto* element_type = &type;
      size_t dims = 0;
      while (true) {
        if (element_type->id() == LIST || element_type->id() == MAP) {
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
