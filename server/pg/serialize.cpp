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
#ifdef __AVX2__
#include <immintrin.h>
#endif
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/operator/string_cast.hpp>
#include <duckdb/common/types/bit.hpp>
#include <duckdb/common/types/cast_helpers.hpp>
#include <duckdb/common/types/hugeint.hpp>
#include <duckdb/common/types/string_heap.hpp>
#include <duckdb/common/types/time.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/types/uhugeint.hpp>
#include <duckdb/common/types/uuid.hpp>
#include <limits>
#include <string_view>
#include <type_traits>

#define SDB_PG_LOGICAL_TYPES_NO_FACTORY

#include <duckdb/inet/inet_ipaddress.hpp>

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

enum class ArrayKind {
  // A 1-D array/list (LIST or ARRAY are identical on the wire: ndim=1; a
  // top-level MAP is a 1-D list of key/value structs).
  SingleDimension,
  // >=2-D nesting, emitted as a PG multi-dimensional flatten. The value must be
  // rectangular: a ragged or NULL inner LIST/MAP throws in binary (text renders
  // it via nested braces).
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

ArraySlice GetSliceResolved(const duckdb::RecursiveUnifiedVectorFormat& vdata,
                            duckdb::idx_t source_row) {
  if (vdata.logical_type.id() == duckdb::LogicalTypeId::ARRAY) {
    auto size = duckdb::ArrayType::GetSize(vdata.logical_type);
    return {size, source_row * size};
  }
  SDB_ASSERT(vdata.logical_type.id() == duckdb::LogicalTypeId::LIST ||
             vdata.logical_type.id() == duckdb::LogicalTypeId::MAP);
  auto list_data =
    duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::list_entry_t>(
      vdata.unified)[source_row];
  return {list_data.length, list_data.offset};
}

ArraySlice GetArraySlice(const duckdb::RecursiveUnifiedVectorFormat& vdata,
                         duckdb::idx_t row) {
  return GetSliceResolved(vdata, vdata.unified.sel->get_index(row));
}

IRS_FORCE_INLINE size_t NextEscape(const char* p, size_t from, size_t n,
                                   bool escape_quote, bool escape_backslash,
                                   bool copy_text, char copy_delim) {
  size_t i = from;
#ifdef __AVX2__
  if (i + 32 <= n) {
    const __m256i vq = _mm256_set1_epi8('"');
    const __m256i vb = _mm256_set1_epi8('\\');
    const __m256i v8 = _mm256_set1_epi8(8);
    const __m256i v5 = _mm256_set1_epi8(5);
    const __m256i vd = _mm256_set1_epi8(copy_delim);
    do {
      const __m256i v =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p + i));
      __m256i m = _mm256_setzero_si256();
      if (escape_quote) {
        m = _mm256_or_si256(m, _mm256_cmpeq_epi8(v, vq));
      }
      if (escape_backslash) {
        m = _mm256_or_si256(m, _mm256_cmpeq_epi8(v, vb));
      }
      if (copy_text) {
        const __m256i lo = _mm256_sub_epi8(v, v8);
        m = _mm256_or_si256(m, _mm256_cmpeq_epi8(_mm256_max_epu8(lo, v5), v5));
        m = _mm256_or_si256(m, _mm256_cmpeq_epi8(v, vd));
      }
      const auto bits = static_cast<uint32_t>(_mm256_movemask_epi8(m));
      if (bits != 0) {
        return i + static_cast<size_t>(std::countr_zero(bits));
      }
      i += 32;
    } while (i + 32 <= n);
  }
#endif
  for (; i < n; ++i) {
    const auto c = static_cast<unsigned char>(p[i]);
    if ((escape_quote && c == '"') || (escape_backslash && c == '\\') ||
        (copy_text && (static_cast<unsigned char>(c - 8) <= 5 ||
                       c == static_cast<unsigned char>(copy_delim)))) {
      return i;
    }
  }
  return n;
}

void EmitEscaped(SerializationContext& ctx, std::string_view s) {
  const auto quote_seq_size = ctx.quote_seq.size();
  const auto backslash_count = ctx.backslash_count;
  auto& writer = *ctx.writer;
  if (!ctx.copy_text && quote_seq_size == 1 && backslash_count == 1) {
    writer.Write(s);
    return;
  }
  const bool copy_text = ctx.copy_text;
  const char copy_delim = ctx.copy_delim;
  const char* p = s.data();
  const size_t n = s.size();
  size_t start = 0;
  size_t i = NextEscape(p, 0, n, quote_seq_size != 1, backslash_count != 1,
                        copy_text, copy_delim);
  while (i < n) {
    const char c = p[i];
    const size_t run = i - start;
    if (c == '"') {
      char* buf = reinterpret_cast<char*>(writer.Alloc(run + quote_seq_size));
      std::memcpy(buf, p + start, run);
      std::memcpy(buf + run, ctx.quote_seq.data(), quote_seq_size);
    } else if (c == '\\') {
      char* buf = reinterpret_cast<char*>(writer.Alloc(run + backslash_count));
      std::memcpy(buf, p + start, run);
      std::memset(buf + run, '\\', backslash_count);
    } else {
      char* buf = reinterpret_cast<char*>(writer.Alloc(run + 2));
      std::memcpy(buf, p + start, run);
      buf[run] = '\\';
      char& letter = buf[run + 1];
      switch (c) {
        case '\b':
          letter = 'b';
          break;
        case '\t':
          letter = 't';
          break;
        case '\n':
          letter = 'n';
          break;
        case '\v':
          letter = 'v';
          break;
        case '\f':
          letter = 'f';
          break;
        case '\r':
          letter = 'r';
          break;
        default:
          letter = c;
          break;
      }
    }
    start = i + 1;
    i = NextEscape(p, start, n, quote_seq_size != 1, backslash_count != 1,
                   copy_text, copy_delim);
  }
  writer.Write(s.substr(start));
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
  ctx.writer->Write(ctx.quote_seq);
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
  ctx.writer->Write(ctx.quote_seq);
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

// Emit `value` in text form, wrapping/escaping per the container context. Used
// by every streamed text core that renders an escapable string (varchar, enum
// labels, json, interval).
template<WrapContext InContainer>
IRS_FORCE_INLINE void EmitTextItem(SerializationContext& ctx,
                                   std::string_view value) {
  if constexpr (InContainer == WrapContext::Array) {
    if (ArrayItemNeedQuotesAndEscape(value)) {
      WriteWrapped<WrapContext::Array>(ctx, [&] { EmitEscaped(ctx, value); });
      return;
    }
  } else if constexpr (InContainer == WrapContext::Record) {
    if (RecordItemNeedsQuoting(value)) {
      WriteWrapped<WrapContext::Record>(ctx, [&] { EmitEscaped(ctx, value); });
      return;
    }
  }
  EmitEscaped(ctx, value);
}

template<WrapContext InContainer>
struct VarcharTextCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto raw =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::string_t>(
        vdata.unified)[idx];
    EmitTextItem<InContainer>(ctx,
                              std::string_view{raw.GetData(), raw.GetSize()});
  }
};

struct VarcharBinCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto raw =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::string_t>(
        vdata.unified)[idx];
    ctx.writer->Write(std::string_view{raw.GetData(), raw.GetSize()});
  }
};

template<WrapContext InContainer, typename T>
IRS_FORCE_INLINE void EnumTextLabel(
  SerializationContext& ctx, const duckdb::RecursiveUnifiedVectorFormat& vdata,
  duckdb::idx_t idx) {
  const auto ordinal =
    duckdb::UnifiedVectorFormat::GetData<T>(vdata.unified)[idx];
  const auto label = duckdb::EnumType::GetString(vdata.logical_type, ordinal);
  EmitTextItem<InContainer>(ctx,
                            std::string_view{label.GetData(), label.GetSize()});
}

template<WrapContext InContainer>
struct EnumTextCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    switch (duckdb::EnumType::GetPhysicalType(vdata.logical_type)) {
      using enum duckdb::PhysicalType;
      case UINT8:
        return EnumTextLabel<InContainer, uint8_t>(ctx, vdata, idx);
      case UINT16:
        return EnumTextLabel<InContainer, uint16_t>(ctx, vdata, idx);
      case UINT32:
        return EnumTextLabel<InContainer, uint32_t>(ctx, vdata, idx);
      default:
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                        ERR_MSG("Unsupported ENUM physical type"));
    }
  }
};

template<typename T>
IRS_FORCE_INLINE void EnumBinLabel(
  SerializationContext& ctx, const duckdb::RecursiveUnifiedVectorFormat& vdata,
  duckdb::idx_t idx) {
  const auto ordinal =
    duckdb::UnifiedVectorFormat::GetData<T>(vdata.unified)[idx];
  const auto label = duckdb::EnumType::GetString(vdata.logical_type, ordinal);
  ctx.writer->Write(std::string_view{label.GetData(), label.GetSize()});
}

struct EnumBinCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    switch (duckdb::EnumType::GetPhysicalType(vdata.logical_type)) {
      using enum duckdb::PhysicalType;
      case UINT8:
        return EnumBinLabel<uint8_t>(ctx, vdata, idx);
      case UINT16:
        return EnumBinLabel<uint16_t>(ctx, vdata, idx);
      case UINT32:
        return EnumBinLabel<uint32_t>(ctx, vdata, idx);
      default:
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                        ERR_MSG("Unsupported ENUM physical type"));
    }
  }
};

// Encode a value into PG numeric binary format.
// value is the unscaled integer (e.g. 12345 for 123.45 with scale=2).
// Use scale=0 for integer types. Caller must convert duckdb::hugeint_t /
// uhugeint_t to absl::int128 / absl::uint128 before calling.
// The unsigned counterpart of a numeric backing type (absl::int128 is not a
// standard integral, so std::make_unsigned does not cover it).
template<typename T>
struct NumericUnsigned {
  using Type = std::make_unsigned_t<T>;
};
template<>
struct NumericUnsigned<absl::int128> {
  using Type = absl::uint128;
};
template<>
struct NumericUnsigned<absl::uint128> {
  using Type = absl::uint128;
};

template<typename T>
void WriteAsNumericBinary(SerializationContext& context, T value,
                          int32_t scale) {
  static constexpr int32_t kBase = 10'000;
  static constexpr int16_t kPositive = 0x0000;
  static constexpr int16_t kNegative = 0x4000;
  static constexpr int16_t kPowersOfTen[] = {1, 10, 100, 1000};

  int16_t extra_digits = static_cast<int16_t>((4 - (scale % 4)) % 4);
  auto extra_base = kPowersOfTen[extra_digits];

  // The magnitude lives in the unsigned counterpart of T: negating a signed T
  // overflows at its minimum (e.g. INT128_MIN, a valid HUGEINT), but the
  // unsigned negate -static_cast<U>(value) is well-defined and yields |value|.
  // U matches T's width (uint32/uint64/uint128), so the decimal paths keep
  // their native-width division.
  using U = typename NumericUnsigned<T>::Type;
  int16_t sign = kPositive;
  U mag;
  if constexpr (std::numeric_limits<T>::is_signed) {
    if (value < T{0}) {
      sign = kNegative;
      mag = -static_cast<U>(value);
    } else {
      mag = static_cast<U>(value);
    }
  } else {
    mag = static_cast<U>(value);
  }

  int16_t ndigits = [extra_base](U v) -> int16_t {
    if (v == U{0}) {
      return 0;
    }
    int16_t n = 0;
    if (extra_base != 1) {
      ++n;
      v /= static_cast<U>(kBase / extra_base);
    }
    for (; v != U{0}; v /= static_cast<U>(kBase)) {
      ++n;
    }
    return n;
  }(mag);

  auto weight = static_cast<int16_t>(ndigits - ((scale + 3) / 4) - 1);
  auto* data = context.writer->Alloc(8 + ndigits * 2);
  absl::big_endian::Store16(data, ndigits);
  absl::big_endian::Store16(data + 2, weight);
  absl::big_endian::Store16(data + 4, sign);
  absl::big_endian::Store16(data + 6, static_cast<int16_t>(scale));
  data += 8 + ndigits * 2;

  if (extra_base != 1 && mag != U{0}) {
    data -= 2;
    ndigits--;
    auto digit =
      (mag % static_cast<U>(kBase / extra_base)) * static_cast<U>(extra_base);
    absl::big_endian::Store16(data, static_cast<int16_t>(digit));
    mag /= static_cast<U>(kBase / extra_base);
  }
  while (mag != U{0}) {
    data -= 2;
    ndigits--;
    absl::big_endian::Store16(
      data, static_cast<int16_t>(mag % static_cast<U>(kBase)));
    mag /= static_cast<U>(kBase);
  }
  SDB_ASSERT(ndigits == 0);
}

template<typename PhysicalType>
struct DecimalTextCore {
  // Worst case is the all-fractional negative ("-0." + scale digits): the digit
  // budget is the physical type's max width, plus sign + leading "0" + ".".
  static constexpr uint32_t kMaxBytes =
    uint32_t{duckdb::DecimalWidth<PhysicalType>::max} + 3;
  IRS_FORCE_INLINE static size_t Render(
    uint8_t* dst, SerializationContext&,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto& type = vdata.logical_type;
    const auto precision = duckdb::DecimalType::GetWidth(type);
    const auto scale = duckdb::DecimalType::GetScale(type);
    const auto value = duckdb::UnifiedVectorFormat::GetDataUnsafe<PhysicalType>(
      vdata.unified)[idx];
    // In-place, PG-compatible (leading zero, always an integer part): no
    // intermediate duckdb::Value or std::string per row.
    const auto len = static_cast<duckdb::idx_t>(
      duckdb::DecimalToString::DecimalLength<PhysicalType>(value, precision,
                                                           scale));
    duckdb::DecimalToString::FormatDecimal<PhysicalType>(
      value, precision, scale, reinterpret_cast<char*>(dst), len);
    return len;
  }
};

template<typename PhysicalType>
struct DecimalBinCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto scale = duckdb::DecimalType::GetScale(vdata.logical_type);
    const auto value = duckdb::UnifiedVectorFormat::GetDataUnsafe<PhysicalType>(
      vdata.unified)[idx];
    if constexpr (std::is_same_v<PhysicalType, duckdb::hugeint_t>) {
      WriteAsNumericBinary(ctx, absl::MakeInt128(value.upper, value.lower),
                           scale);
    } else {
      WriteAsNumericBinary(ctx, value, scale);
    }
  }
};

struct UbigintBinCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    WriteAsNumericBinary(
      ctx,
      duckdb::UnifiedVectorFormat::GetDataUnsafe<uint64_t>(vdata.unified)[idx],
      0);
  }
};

struct HugeintTextCore {
  static constexpr uint32_t kMaxBytes =
    absl::numbers_internal::kFastToBuffer128Size;
  IRS_FORCE_INLINE static size_t Render(
    uint8_t* dst, SerializationContext&,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto value =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::hugeint_t>(
        vdata.unified)[idx];
    char* buf = reinterpret_cast<char*>(dst);
    char* ptr = absl::numbers_internal::FastIntToBuffer(
      absl::MakeInt128(value.upper, value.lower), buf);
    return static_cast<size_t>(ptr - buf);
  }
};

struct HugeintBinCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto value =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::hugeint_t>(
        vdata.unified)[idx];
    WriteAsNumericBinary(ctx, absl::MakeInt128(value.upper, value.lower), 0);
  }
};

struct UhugeintTextCore {
  static constexpr uint32_t kMaxBytes =
    absl::numbers_internal::kFastToBuffer128Size;
  IRS_FORCE_INLINE static size_t Render(
    uint8_t* dst, SerializationContext&,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto value =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::uhugeint_t>(
        vdata.unified)[idx];
    char* buf = reinterpret_cast<char*>(dst);
    char* ptr = absl::numbers_internal::FastIntToBuffer(
      absl::MakeUint128(value.upper, value.lower), buf);
    return static_cast<size_t>(ptr - buf);
  }
};

struct UhugeintBinCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto value =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::uhugeint_t>(
        vdata.unified)[idx];
    WriteAsNumericBinary(ctx, absl::MakeUint128(value.upper, value.lower), 0);
  }
};

// The leading '\\' goes through ctx.backslash_count so nested wraps emit
// the depth-correct escape sequence; the rest is hex-only and never triggers.
void ByteaOutHex(SerializationContext& ctx, std::string_view value) {
  EmitEscaped(ctx, "\\");
  const auto body_size = 1 + 2 * value.size();
  auto* data = ctx.writer->Alloc(body_size);
  char* p = reinterpret_cast<char*>(data);
  *p++ = 'x';
  absl::BytesToHexStringInternal(
    reinterpret_cast<const unsigned char*>(value.data()), p, value.size());
}

template<WrapContext InContainer>
struct ByteaHexTextCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto raw =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::string_t>(
        vdata.unified)[idx];
    const auto value = std::string_view{raw.GetData(), raw.GetSize()};

    // '\\xnnnn' always starts with '\\' which triggers wrap in both array and
    // record contexts.
    if constexpr (InContainer == WrapContext::None) {
      ByteaOutHex(ctx, value);
    } else {
      WriteWrapped<InContainer>(ctx, [&] { ByteaOutHex(ctx, value); });
    }
  }
};

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
  auto* data = ctx.writer->Alloc(body_size);
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
struct ByteaEscapeTextCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto raw =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::string_t>(
        vdata.unified)[idx];
    const auto value = std::string_view{raw.GetData(), raw.GetSize()};

    if constexpr (InContainer == WrapContext::None) {
      ByteaOutEscape(ctx, value);
    } else {
      // Wrap iff the rendered escape form has a wrap-trigger char: empty,
      // any source '\\' (becomes '\\' in the body), or any non-printable byte
      // (becomes '\\nnn').
      const bool has_special =
        value.empty() || absl::c_any_of(value, [](char c) {
          return c == '\\' || !absl::ascii_isprint(c);
        });
      if (has_special) {
        WriteWrapped<InContainer>(ctx, [&] { ByteaOutEscape(ctx, value); });
      } else {
        ByteaOutEscape(ctx, value);
      }
    }
  }
};

struct ByteaBinCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto raw =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::string_t>(
        vdata.unified)[idx];
    ctx.writer->Write(std::string_view{raw.GetData(), raw.GetSize()});
  }
};

struct BoolTextCore {
  using Value = bool;
  static constexpr uint32_t kMaxBytes = 1;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, bool v) {
    *dst = v ? 't' : 'f';
    return 1;
  }
};

struct BoolBinCore {
  using Value = bool;
  static constexpr uint32_t kMaxBytes = 1;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, bool v) {
    *reinterpret_cast<bool*>(dst) = v;
    return 1;
  }
};

template<typename Read, typename Wire = Read>
struct IntBinCore {
  using Value = Read;
  static constexpr uint32_t kMaxBytes = sizeof(Wire);
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, const Read& v) {
    absl::big_endian::Store(dst, static_cast<Wire>(v));
    return sizeof(Wire);
  }
};

template<typename Read>
struct IntTextCore {
  using Value = Read;
  static constexpr uint32_t kMaxBytes = basics::kIntStrMaxLen;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, const Read& v) {
    char* buf = reinterpret_cast<char*>(dst);
    char* end = absl::numbers_internal::FastIntToBuffer(v, buf);
    return static_cast<size_t>(end - buf);
  }
};

struct OidBinCore {
  using Value = int64_t;
  static constexpr uint32_t kMaxBytes = 4;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, int64_t oid) {
    if (oid != static_cast<int32_t>(oid)) {
      SDB_WARN(HTTP, "reg* OID ", oid,
               " truncated to 32-bit for binary wire protocol");
    }
    absl::big_endian::Store32(dst, static_cast<int32_t>(oid));
    return 4;
  }
};

template<typename T>
struct FloatBinCore {
  using Value = T;
  static constexpr uint32_t kMaxBytes = sizeof(T);
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, T value) {
    if (value == 0) {  // postgres renders -0.0 as 0.0
      value = 0;
    }
    absl::big_endian::Store(dst, value);
    return sizeof(T);
  }
};

template<typename T, bool Precise>
struct FloatTextCore {
  using Value = T;
  static constexpr uint32_t kMaxBytes = basics::kNumberStrMaxLen;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, SerializationContext& ctx,
                                        T value) {
    static_assert(std::is_same_v<T, float> || std::is_same_v<T, double>);
    if (value == 0) {  // Postgres converts -0.0 as 0.0
      value = 0;
    }
    char* buf = reinterpret_cast<char*>(dst);
    if (char* ptr =
          basics::dtoa_literals<basics::kPgDtoaLiterals>(value, buf)) {
      return static_cast<size_t>(ptr - buf);
    }
    if constexpr (Precise) {
      char* ptr = basics::dtoa_fast(value, buf);
      return static_cast<size_t>(ptr - buf);
    } else {
      int num_of_digits =
        std::numeric_limits<T>::digits10 + ctx.extra_float_digits;
      if constexpr (std::is_same_v<float, T>) {
        num_of_digits = std::max(0, num_of_digits);
      } else {
        SDB_ASSERT(num_of_digits >= 0);
      }
      const auto r = std::to_chars(buf, buf + basics::kNumberStrMaxLen, value,
                                   std::chars_format::general, num_of_digits);
      SDB_ASSERT(r);
      return static_cast<size_t>(r.ptr - buf);
    }
  }
};

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

template<WrapContext InContainer>
struct TimestampSecTextCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto timestamp =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::timestamp_sec_t>(
        vdata.unified)[idx];
    auto str = duckdb::Timestamp::ToString(
      duckdb::Timestamp::FromEpochSeconds(timestamp.value));
    WithWrapIfNested<InContainer>(ctx, [&] { ctx.writer->Write(str); });
  }
};

struct TimestampSecBinCore {
  using Value = duckdb::timestamp_sec_t;
  static constexpr uint32_t kMaxBytes = 8;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value timestamp) {
    absl::big_endian::Store64(dst, (timestamp.value - kGapSec) * 1'000'000);
    return 8;
  }
};

template<WrapContext InContainer>
struct TimestampMsTextCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto timestamp =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::timestamp_ms_t>(
        vdata.unified)[idx];
    auto str = duckdb::Timestamp::ToString(
      duckdb::Timestamp::FromEpochMs(timestamp.value));
    WithWrapIfNested<InContainer>(ctx, [&] { ctx.writer->Write(str); });
  }
};

struct TimestampMsBinCore {
  using Value = duckdb::timestamp_ms_t;
  static constexpr uint32_t kMaxBytes = 8;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value timestamp) {
    absl::big_endian::Store64(dst, (timestamp.value - kGapMs) * 1000);
    return 8;
  }
};

int64_t TimestampMicrosToWire(int64_t value) {
  if (value == std::numeric_limits<int64_t>::max()) {
    return value;
  }
  if (value == -std::numeric_limits<int64_t>::max()) {
    return std::numeric_limits<int64_t>::min();
  }
  return value - kGapUs;
}

int32_t DateDaysToWire(int32_t days) {
  if (days == std::numeric_limits<int32_t>::max()) {
    return days;
  }
  if (days == -std::numeric_limits<int32_t>::max()) {
    return std::numeric_limits<int32_t>::min();
  }
  return static_cast<int32_t>(days - kGapDays);
}

template<WrapContext InContainer>
struct TimestampTextCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto timestamp =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::timestamp_t>(
        vdata.unified)[idx];
    auto str = duckdb::Timestamp::ToString(timestamp);
    WithWrapIfNested<InContainer>(ctx, [&] { ctx.writer->Write(str); });
  }
};

struct TimestampBinCore {
  using Value = duckdb::timestamp_t;
  static constexpr uint32_t kMaxBytes = 8;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value timestamp) {
    absl::big_endian::Store64(dst, TimestampMicrosToWire(timestamp.value));
    return 8;
  }
};

template<WrapContext InContainer>
struct TimestampNsTextCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto timestamp =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::timestamp_ns_t>(
        vdata.unified)[idx];
    duckdb::StringHeap heap{duckdb::Allocator::DefaultAllocator()};
    const auto str = duckdb::StringCast::Operation(timestamp, heap);
    WithWrapIfNested<InContainer>(ctx, [&] {
      ctx.writer->Write(std::string_view{str.GetData(), str.GetSize()});
    });
  }
};

struct TimestampNsBinCore {
  using Value = duckdb::timestamp_ns_t;
  static constexpr uint32_t kMaxBytes = 8;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value timestamp) {
    absl::big_endian::Store64(dst, (timestamp.value - kGapNs) / 1000);
    return 8;
  }
};

template<WrapContext InContainer>
struct TimestampTzTextCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto ts =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::timestamp_tz_t>(
        vdata.unified)[idx];
    auto str = duckdb::Timestamp::ToString(duckdb::timestamp_t{ts});
    WithWrapIfNested<InContainer>(ctx, [&] {
      ctx.writer->Write(str);
      ctx.writer->Write("+00");
    });
  }
};

struct TimestampTzBinCore {
  using Value = duckdb::timestamp_tz_t;
  static constexpr uint32_t kMaxBytes = 8;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value ts) {
    absl::big_endian::Store64(dst, TimestampMicrosToWire(ts.value));
    return 8;
  }
};

struct TimeTextCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto time =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::dtime_t>(
        vdata.unified)[idx];
    ctx.writer->Write(duckdb::Time::ToString(time));
  }
};

struct TimeBinCore {
  using Value = duckdb::dtime_t;
  static constexpr uint32_t kMaxBytes = 8;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value time) {
    absl::big_endian::Store64(dst, time.micros);
    return 8;
  }
};

struct TimeNsTextCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto time =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::dtime_ns_t>(
        vdata.unified)[idx];
    duckdb::StringHeap heap{duckdb::Allocator::DefaultAllocator()};
    const auto str = duckdb::StringCast::Operation(time, heap);
    ctx.writer->Write(std::string_view{str.GetData(), str.GetSize()});
  }
};

struct TimeNsBinCore {
  using Value = duckdb::dtime_ns_t;
  static constexpr uint32_t kMaxBytes = 8;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value time) {
    absl::big_endian::Store64(dst, time.time().micros);
    return 8;
  }
};

struct TimeTzTextCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto tz =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::dtime_tz_t>(
        vdata.unified)[idx];
    // Format: HH:MM:SS[.mmm][±HH:MM]
    ctx.writer->Write(duckdb::Time::ToString(tz.time()));
    const auto offset_secs = tz.offset();
    const bool negative = offset_secs < 0;
    const auto abs_offset = negative ? -offset_secs : offset_secs;
    const auto offset_h = abs_offset / 3600;
    const auto offset_m = (abs_offset % 3600) / 60;
    ctx.writer->Write(6, [&](uint8_t* dst) {
      char* buf = reinterpret_cast<char*>(dst);
      *buf++ = negative ? '-' : '+';
      *buf++ = '0' + offset_h / 10;
      *buf++ = '0' + offset_h % 10;
      *buf++ = ':';
      *buf++ = '0' + offset_m / 10;
      *buf++ = '0' + offset_m % 10;
      return size_t{6};
    });
  }
};

struct TimeTzBinCore {
  using Value = duckdb::dtime_tz_t;
  static constexpr uint32_t kMaxBytes = 12;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value tz) {
    // PG binary: int64 time_micros + int32 zone (seconds WEST of UTC).
    // DuckDB offset() is seconds EAST, so negate.
    absl::big_endian::Store64(dst, tz.time().micros);
    absl::big_endian::Store32(dst + 8, -tz.offset());
    return 12;
  }
};

struct BitTextCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto raw =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::string_t>(
        vdata.unified)[idx];
    // DuckDB Bit::ToString gives "01001..." string
    ctx.writer->Write(duckdb::Bit::ToString(raw));
  }
};

struct BitBinCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto raw =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::string_t>(
        vdata.unified)[idx];
    // PG bit binary: int32 nBits + ceil(nBits/8) bytes, bits MSB-first from bit
    // 0 with trailing ZERO padding. DuckDB instead pads at the front of byte 0
    // with one-bits, so re-pack bit by bit rather than copying the raw bytes.
    const auto n_bits = static_cast<int32_t>(duckdb::Bit::BitLength(raw));
    const auto n_bytes = (n_bits + 7) / 8;
    auto* data = ctx.writer->Alloc(4 + n_bytes);
    absl::big_endian::Store32(data, n_bits);
    auto* bits = reinterpret_cast<uint8_t*>(data) + 4;
    std::memset(bits, 0, static_cast<size_t>(n_bytes));
    for (int32_t i = 0; i < n_bits; ++i) {
      if (duckdb::Bit::GetBit(raw, static_cast<duckdb::idx_t>(i)) != 0) {
        bits[i / 8] |= static_cast<uint8_t>(0x80U >> (i % 8));
      }
    }
  }
};

struct DateTextCore {
  using Value = duckdb::date_t;
  static constexpr uint32_t kMaxBytes = 13;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value days) {
    // days from 1970-01-01
    char* buf = reinterpret_cast<char*>(dst);
    if (days.days == std::numeric_limits<int32_t>::max()) {
      std::memcpy(buf, "infinity", 8);
      return 8;
    }
    if (days.days == -std::numeric_limits<int32_t>::max()) {
      std::memcpy(buf, "-infinity", 9);
      return 9;
    }
    // TODO(mkornaukhov) support BC date and add some validation for dates
    // Format is "%04d-%02d-%02d", max year is 5874897
    absl::CivilDay date{1970, 1, 1};
    date += days.days;

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

    return static_cast<size_t>(buf - reinterpret_cast<char*>(dst));
  }
};

struct DateBinCore {
  using Value = duckdb::date_t;
  static constexpr uint32_t kMaxBytes = 4;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value days) {
    absl::big_endian::Store32(dst, DateDaysToWire(days.days));
    return 4;
  }
};

struct RegtypeTextCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto oid =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<int64_t>(vdata.unified)[idx];
    EmitEscaped(ctx, RegtypeOut(oid));
  }
};

struct RegclassTextCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto oid =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<int64_t>(vdata.unified)[idx];
    EmitEscaped(ctx, RegclassOut(*ctx.snapshot, oid));
  }
};

struct RegnamespaceTextCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto oid =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<int64_t>(vdata.unified)[idx];
    EmitEscaped(ctx, RegnamespaceOut(*ctx.snapshot, oid));
  }
};

template<WrapContext InContainer>
struct IntervalTextCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto interval =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::interval_t>(
        vdata.unified)[idx];
    auto str = duckdb::Interval::ToString(interval);
    EmitTextItem<InContainer>(ctx, std::string_view{str});
  }
};

struct IntervalBinCore {
  using Value = duckdb::interval_t;
  static constexpr uint32_t kMaxBytes = 16;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value interval) {
    // PG binary: microseconds(8) + days(4) + months(4)
    absl::big_endian::Store64(dst, interval.micros);
    absl::big_endian::Store32(dst + 8, interval.days);
    absl::big_endian::Store32(dst + 12, interval.months);
    return 16;
  }
};

struct UuidTextCore {
  using Value = duckdb::hugeint_t;
  static constexpr uint32_t kMaxBytes = 36;  // 8-4-4-4-12
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, const Value& uuid) {
    duckdb::BaseUUID::ToString(uuid, reinterpret_cast<char*>(dst));
    return 36;
  }
};

struct UuidBinCore {
  using Value = duckdb::hugeint_t;
  static constexpr uint32_t kMaxBytes = 16;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, const Value& uuid) {
    // Binary format: flip top bit back to get original UUID bytes
    const uint64_t high =
      static_cast<uint64_t>(uuid.upper) ^ (uint64_t{1} << 63);
    absl::big_endian::Store64(dst, high);
    absl::big_endian::Store64(dst + 8, uuid.lower);
    return 16;
  }
};

template<WrapContext InContainer>
struct JsonTextCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto& str =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::string_t>(
        vdata.unified)[idx];
    EmitTextItem<InContainer>(ctx,
                              std::string_view{str.GetData(), str.GetSize()});
  }
};

struct JsonBinCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto str =
      duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::string_t>(
        vdata.unified)[idx];
    ctx.writer->Write(std::string_view{str.GetData(), str.GetSize()});
  }
};

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
        duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::interval_t>(
          vdata.unified)[vdata.unified.sel->get_index(row)];
      auto str = duckdb::Interval::ToString(interval);
      std::string_view v{str};
      return RecordItemNeedsQuoting(v) || ArrayItemNeedQuotesAndEscape(v);
    }
    case CHAR:
    case VARCHAR: {
      const auto& raw =
        duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::string_t>(
          vdata.unified)[vdata.unified.sel->get_index(row)];
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
    case ARRAY:
    case LIST:
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
          if (lid != duckdb::LogicalTypeId::ARRAY &&
              lid != duckdb::LogicalTypeId::LIST &&
              lid != duckdb::LogicalTypeId::MAP) {
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
  // Lazy: only record-typed results pay for the cache; flat-typed queries
  // (the overwhelming majority) skip a per-query map allocation.
  if (context.types_cache == nullptr) {
    context.types_cache = std::make_unique<TypesSerializationCache>();
  }
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

template<WrapContext InContainer>
struct RecordTextCore {
  static void Render(SerializationContext& context,
                     const duckdb::RecursiveUnifiedVectorFormat& vdata,
                     duckdb::idx_t row) {
    const auto& cache =
      GetSerializersCache<VarFormat::Text>(context, vdata.logical_type);
    auto emit_inside = [&] {
      context.writer->Write("(");
      for (size_t i = 0; i < cache.functions.size(); ++i) {
        if (i > 0) {
          context.writer->Write(",");
        }
        const auto& child = vdata.children[i];
        // empty between commas means NULL in record text; the cached in-record
        // text serializer renders nothing on a NULL field.
        cache.functions[i](context, child, row);
      }
      context.writer->Write(")");
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
  }
};

struct RecordBinCore {
  static void Render(SerializationContext& context,
                     const duckdb::RecursiveUnifiedVectorFormat& vdata,
                     duckdb::idx_t row) {
    const auto& cache =
      GetSerializersCache<VarFormat::Binary>(context, vdata.logical_type);
    // PG record binary format: int32 nfields; for each field { int32 OID,
    // int32 length (-1 for NULL) followed by length bytes }.
    auto* nfields_data = context.writer->Alloc(4);
    absl::big_endian::Store32(nfields_data,
                              static_cast<int32_t>(cache.functions.size()));
    for (size_t i = 0; i < cache.functions.size(); ++i) {
      absl::big_endian::Store32(context.writer->Alloc(4), cache.oids[i]);
      cache.functions[i](context, vdata.children[i], row);
    }
  }
};

// PG `inet` (OID 869). STRUCT entries: [0]=ip_type (1=IPv4, 2=IPv6),
// [1]=address (signed hugeint, IPv6 top bit flipped for sort order), [2]=mask.
IRS_FORCE_INLINE void ReadInet(
  const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t row,
  INET_IPAddress& inet) {
  const auto& c0 = vdata.children[0].unified;
  const auto& c1 = vdata.children[1].unified;
  const auto& c2 = vdata.children[2].unified;
  const auto ip_type =
    duckdb::UnifiedVectorFormat::GetData<uint8_t>(c0)[c0.sel->get_index(row)];
  const auto stored = duckdb::UnifiedVectorFormat::GetData<duckdb::hugeint_t>(
    c1)[c1.sel->get_index(row)];
  const auto mask =
    duckdb::UnifiedVectorFormat::GetData<uint16_t>(c2)[c2.sel->get_index(row)];
  inet.type = static_cast<INET_IPAddressType>(ip_type);
  inet.address.lower = stored.lower;
  inet.address.upper = static_cast<uint64_t>(stored.upper);
  if (inet.type == INET_IP_ADDRESS_V6) {
    inet.address.upper ^= (uint64_t{1} << 63);  // undo IPv6 sort-order flip
  }
  inet.mask = mask;
}

template<WrapContext InContainer>
struct InetTextCore {
  static void Render(SerializationContext& context,
                     const duckdb::RecursiveUnifiedVectorFormat& vdata,
                     duckdb::idx_t row) {
    INET_IPAddress inet;
    ReadInet(vdata, row, inet);
    // Reuse the extension's formatter (IPv6 canonicalization + "/mask" rules)
    // so output matches host()/::varchar.
    char buf[64];
    const size_t len = ipaddress_to_string(&inet, buf, sizeof(buf));
    EmitTextItem<InContainer>(context, std::string_view{buf, len});
  }
};

struct InetBinCore {
  static void Render(SerializationContext& context,
                     const duckdb::RecursiveUnifiedVectorFormat& vdata,
                     duckdb::idx_t row) {
    INET_IPAddress inet;
    ReadInet(vdata, row, inet);
    // PG binary inet bytes: [0]=family, [1]=bits, [2]=is_cidr, [3]=nb (4 or
    // 16), then nb big-endian addr bytes.
    const bool is_v6 = inet.type == INET_IP_ADDRESS_V6;
    const uint8_t nb = is_v6 ? 16 : 4;
    auto* data = context.writer->Alloc(4 + nb);
    data[0] = is_v6 ? 3 : 2;
    data[1] = static_cast<uint8_t>(inet.mask);
    data[2] = 0;
    data[3] = nb;
    if (is_v6) {
      absl::big_endian::Store64(data + 4, inet.address.upper);
      absl::big_endian::Store64(data + 12, inet.address.lower);
    } else {
      absl::big_endian::Store32(data + 4,
                                static_cast<uint32_t>(inet.address.lower));
    }
  }
};

using RUVF = duckdb::RecursiveUnifiedVectorFormat;

template<typename C>
concept SizedCore = requires { C::kMaxBytes; };

// A core that consumes only the resolved physical value (no type/context). It
// declares `Value` (the unified-format element type) and renders straight from
// it, so the GetDataUnsafe boilerplate lives here once instead of in every
// core.
template<typename C>
concept ValueCore = requires { typename C::Value; };

template<typename T>
IRS_FORCE_INLINE const T& ReadValue(const RUVF& v, duckdb::idx_t idx) {
  return duckdb::UnifiedVectorFormat::GetDataUnsafe<T>(v.unified)[idx];
}

// Invoke a sized core into `dst`, returning the byte count. A value core
// renders straight from the extracted value -- `Render(dst, v)`, or
// `Render(dst, ctx, v)` if it also reads context; everything else reads
// type/children from (ctx, v, idx).
template<typename Core>
IRS_FORCE_INLINE size_t RenderSized(uint8_t* dst, SerializationContext& ctx,
                                    const RUVF& v, duckdb::idx_t idx) {
  if constexpr (ValueCore<Core>) {
    const auto& value = ReadValue<typename Core::Value>(v, idx);
    if constexpr (requires { Core::Render(dst, value); }) {
      return Core::Render(dst, value);
    } else {
      return Core::Render(dst, ctx, value);
    }
  } else {
    return Core::Render(dst, ctx, v, idx);
  }
}

// Invoke a streamed core (writes directly into ctx.buffer, variable length).
template<typename Core>
IRS_FORCE_INLINE void RenderStreamed(SerializationContext& ctx, const RUVF& v,
                                     duckdb::idx_t idx) {
  if constexpr (ValueCore<Core>) {
    Core::Render(ctx, ReadValue<typename Core::Value>(v, idx));
  } else {
    Core::Render(ctx, v, idx);
  }
}

// Render a single value (no null marker, no length prefix) into ctx.buffer.
// `idx` is already resolved through the vector's selection.
template<typename Core>
IRS_FORCE_INLINE void RenderValue(SerializationContext& ctx, const RUVF& v,
                                  duckdb::idx_t idx) {
  if constexpr (SizedCore<Core>) {
    ctx.writer->Write(Core::kMaxBytes, [&](uint8_t* d) {
      return RenderSized<Core>(d, ctx, v, idx);
    });
  } else {
    RenderStreamed<Core>(ctx, v, idx);
  }
}

enum class Framing {
  BinaryField,
  CopyTextField,
};

template<Framing F, typename Core>
bool SerializeField(SerializationContext& ctx, const RUVF& v,
                    duckdb::idx_t row) {
  const auto idx = v.unified.sel->get_index(row);
  const bool valid = v.unified.validity.RowIsValid(idx);
  if constexpr (F == Framing::CopyTextField) {
    if (!valid) {
      ctx.writer->Write(ctx.copy_null);
      return true;
    }
    RenderValue<Core>(ctx, v, idx);
    return false;
  } else if constexpr (SizedCore<Core>) {
    // BinaryField + sized: one reservation holds the fused [int32 len][body].
    if (!valid) {
      absl::big_endian::Store32(ctx.writer->Alloc(4), -1);
      return true;
    }
    ctx.writer->Write(4 + Core::kMaxBytes, [&](uint8_t* d) {
      const auto n = RenderSized<Core>(d + 4, ctx, v, idx);
      absl::big_endian::Store32(d, static_cast<int32_t>(n));
      return 4 + n;
    });
    return false;
  } else {
    // BinaryField + streamed: measure the body then backpatch its length.
    auto* len = ctx.writer->Alloc(4);
    if (!valid) {
      absl::big_endian::Store32(len, -1);
      return true;
    }
    const auto before = ctx.writer->Written();
    RenderStreamed<Core>(ctx, v, idx);
    absl::big_endian::Store32(
      len, static_cast<int32_t>(ctx.writer->Written() - before));
    return false;
  }
}

bool SerializeNullColumn(SerializationContext& ctx, const RUVF&,
                         duckdb::idx_t) {
  absl::big_endian::Store32(ctx.writer->Alloc(4), -1);
  return true;
}

// In-record TEXT field: no length prefix, NULL renders nothing (the empty
// stretch between record commas means NULL). Returns true on a NULL field.
template<typename Core>
bool RecordTextField(SerializationContext& ctx, const RUVF& v,
                     duckdb::idx_t row) {
  const auto idx = v.unified.sel->get_index(row);
  if (!v.unified.validity.RowIsValid(idx)) {
    return true;
  }
  RenderValue<Core>(ctx, v, idx);
  return false;
}

// Emit the element run of a one-dimensional array (no surrounding braces /
// binary header -- the array core writes those). `off`/`count` come from the
// resolved array slice. Returns whether any element was NULL.
template<typename Core, VarFormat Format>
IRS_FORCE_INLINE bool EmitArrayElems(SerializationContext& ctx, const RUVF& cv,
                                     duckdb::idx_t off, duckdb::idx_t count) {
  bool has_null = false;
  if constexpr (Format == VarFormat::Binary) {
    if constexpr (SizedCore<Core>) {
      ctx.writer->Write(
        count * (4 + size_t{Core::kMaxBytes}), [&](uint8_t* out) {
          uint8_t* p = out;
          for (duckdb::idx_t i = 0; i < count; ++i) {
            const auto idx = cv.unified.sel->get_index(off + i);
            if (!cv.unified.validity.RowIsValid(idx)) {
              absl::big_endian::Store32(p, -1);
              p += 4;
              has_null = true;
            } else {
              const auto n = RenderSized<Core>(p + 4, ctx, cv, idx);
              absl::big_endian::Store32(p, static_cast<int32_t>(n));
              p += 4 + n;
            }
          }
          return static_cast<size_t>(p - out);
        });
    } else {
      for (duckdb::idx_t i = 0; i < count; ++i) {
        has_null |=
          SerializeField<Framing::BinaryField, Core>(ctx, cv, off + i);
      }
    }
  } else if constexpr (SizedCore<Core>) {
    // Bounded text leaves (numerics/dates/uuid/bool) never need quoting, so the
    // whole comma-joined run fits one reservation; NULL is the 4-byte literal.
    constexpr size_t kElem = (Core::kMaxBytes > 4 ? Core::kMaxBytes : 4) + 1;
    ctx.writer->Write(count * kElem, [&](uint8_t* out) {
      uint8_t* p = out;
      for (duckdb::idx_t i = 0; i < count; ++i) {
        if (i > 0) {
          *p++ = ',';
        }
        const auto idx = cv.unified.sel->get_index(off + i);
        if (!cv.unified.validity.RowIsValid(idx)) {
          std::memcpy(p, "NULL", 4);
          p += 4;
          has_null = true;
        } else {
          p += RenderSized<Core>(p, ctx, cv, idx);
        }
      }
      return static_cast<size_t>(p - out);
    });
  } else {
    for (duckdb::idx_t i = 0; i < count; ++i) {
      if (i > 0) {
        ctx.writer->Write(",");
      }
      const auto idx = cv.unified.sel->get_index(off + i);
      if (!cv.unified.validity.RowIsValid(idx)) {
        ctx.writer->Write("NULL");
        has_null = true;
      } else {
        RenderValue<Core>(ctx, cv, idx);
      }
    }
  }
  return has_null;
}

template<typename Core, int32_t ElementOID, VarFormat Format,
         WrapContext InContainer>
struct OneDimArrayCore {
  IRS_FORCE_INLINE static void Render(SerializationContext& context,
                                      const RUVF& vdata, duckdb::idx_t row) {
    // `row` is the source row already resolved by SerializeField; resolving
    // again would double-apply the selection (wrong for CONSTANT/DICTIONARY
    // arrays).
    auto [array_size, array_offset] = GetSliceResolved(vdata, row);
    auto& child_vdata = vdata.children[0];
    if constexpr (Format == VarFormat::Text) {
      auto emit_inside = [&] {
        context.writer->Write("{");
        EmitArrayElems<Core, VarFormat::Text>(context, child_vdata,
                                              array_offset, array_size);
        context.writer->Write("}");
      };

      if constexpr (InContainer == WrapContext::Record) {
        // Wrap the rendered '{...}' as a record field iff its content has any
        // record trigger char ('(', ')', ',', '"', '\\', whitespace) or is
        // empty. '{' / '}' are NOT record triggers.
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
      int32_t element_oid;
      if constexpr (ElementOID == kDynamicOid) {
        element_oid = Type2Oid(child_vdata.logical_type, false);
      } else {
        element_oid = ElementOID;
      }
      if (array_size == 0) {
        auto* prefix_data = context.writer->Alloc(12);
        absl::big_endian::Store32(prefix_data, /*dims*/ 0);
        absl::big_endian::Store32(prefix_data + 4, /*array_size*/ 0);
        absl::big_endian::Store32(prefix_data + 8, element_oid);
        return;
      }
      auto* prefix_data = context.writer->Alloc(20);
      absl::big_endian::Store32(prefix_data, /*dims*/ 1);
      absl::big_endian::Store32(prefix_data + 8, element_oid);
      absl::big_endian::Store32(prefix_data + 12, array_size);
      absl::big_endian::Store32(prefix_data + 16, 1);
      const bool has_null = EmitArrayElems<Core, VarFormat::Binary>(
        context, child_vdata, array_offset, array_size);
      absl::big_endian::Store32(prefix_data + 4, has_null ? 1 : 0);
    }
  }
};

template<typename Core>
void FlattenArray(SerializationContext& context, const RUVF& vdata,
                  duckdb::idx_t source_row, int32_t& leaf_oid, bool& has_null,
                  uint8_t* dim_sizes_data, int32_t depth) {
  auto [array_size, array_offset] = GetSliceResolved(vdata, source_row);
  absl::big_endian::Store32(dim_sizes_data + depth * 8,
                            static_cast<int32_t>(array_size));
  absl::big_endian::Store32(dim_sizes_data + depth * 8 + 4, 1);
  auto& child_vdata = vdata.children[0];
  if (array_size == 0) {
    const auto* leaf = &child_vdata.logical_type;
    int32_t deeper = depth + 1;
    while (leaf->id() == duckdb::LogicalTypeId::ARRAY ||
           leaf->id() == duckdb::LogicalTypeId::LIST ||
           leaf->id() == duckdb::LogicalTypeId::MAP) {
      absl::big_endian::Store32(dim_sizes_data + deeper * 8, 0);
      absl::big_endian::Store32(dim_sizes_data + deeper * 8 + 4, 1);
      ++deeper;
      leaf = leaf->id() == duckdb::LogicalTypeId::ARRAY
               ? &duckdb::ArrayType::GetChildType(*leaf)
               : &duckdb::ListType::GetChildType(*leaf);
    }
    leaf_oid = Type2Oid(*leaf, false);
    return;
  }
  const auto child_lid = child_vdata.logical_type.id();
  if (child_lid != duckdb::LogicalTypeId::ARRAY &&
      child_lid != duckdb::LogicalTypeId::LIST &&
      child_lid != duckdb::LogicalTypeId::MAP) {
    leaf_oid = Type2Oid(child_vdata.logical_type, false);
    has_null |= EmitArrayElems<Core, VarFormat::Binary>(
      context, child_vdata, array_offset, array_size);
    return;
  }
  // A PG multi-dimensional array is rectangular and has no NULL sub-arrays:
  // every sibling sub-collection at this depth must be present and the same
  // length. A ragged or NULL-holding inner LIST/MAP is only representable in
  // the text format.
  duckdb::idx_t sub_size = 0;
  for (duckdb::idx_t i = 0; i < array_size; ++i) {
    const auto child_row = child_vdata.unified.sel->get_index(array_offset + i);
    if (!child_vdata.unified.validity.RowIsValid(child_row)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("cannot represent a NULL sub-array in a binary "
                              "multi-dimensional array; use the text format"));
    }
    const auto cur_size = GetSliceResolved(child_vdata, child_row).size;
    if (i == 0) {
      sub_size = cur_size;
    } else if (cur_size != sub_size) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("cannot represent a ragged array in the binary format; inner "
                "dimensions must all have the same length"));
    }
    FlattenArray<Core>(context, child_vdata, child_row, leaf_oid, has_null,
                       dim_sizes_data, depth + 1);
  }
}

template<typename Core, int32_t ElementOID, VarFormat Format,
         WrapContext InContainer>
struct MultiDimArrayCore {
  IRS_FORCE_INLINE static void Render(SerializationContext& context,
                                      const RUVF& vdata, duckdb::idx_t row) {
    if constexpr (Format == VarFormat::Text) {
      const auto lid = vdata.logical_type.id();
      if (lid != duckdb::LogicalTypeId::ARRAY &&
          lid != duckdb::LogicalTypeId::LIST &&
          lid != duckdb::LogicalTypeId::MAP) {
        // `row` is already the resolved source row.
        if (!vdata.unified.validity.RowIsValid(row)) {
          context.writer->Write("NULL");
        } else {
          RenderValue<Core>(context, vdata, row);
        }
        return;
      }

      auto [array_size, array_offset] = GetSliceResolved(vdata, row);
      auto& child_vdata = vdata.children[0];

      auto emit_inside = [&] {
        context.writer->Write("{");
        for (duckdb::idx_t i = 0; i < array_size; ++i) {
          if (i > 0) {
            context.writer->Write(",");
          }
          // A NULL sub-list/element must not be resolved further (its
          // list_entry is undefined); render it as the literal NULL like the
          // leaf branch.
          const auto child_row =
            child_vdata.unified.sel->get_index(array_offset + i);
          if (!child_vdata.unified.validity.RowIsValid(child_row)) {
            context.writer->Write("NULL");
            continue;
          }
          MultiDimArrayCore<Core, ElementOID, Format,
                            WrapContext::None>::Render(context, child_vdata,
                                                       child_row);
        }
        context.writer->Write("}");
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
      while (t->id() == duckdb::LogicalTypeId::ARRAY ||
             t->id() == duckdb::LogicalTypeId::LIST ||
             t->id() == duckdb::LogicalTypeId::MAP) {
        ++ndim;
        t = t->id() == duckdb::LogicalTypeId::ARRAY
              ? &duckdb::ArrayType::GetChildType(*t)
              : &duckdb::ListType::GetChildType(*t);
      }
      // `row` is already the resolved source row.
      if (GetSliceResolved(vdata, row).size == 0) {
        // PG sends an empty array as ndim=0 with no dimension descriptors.
        int32_t leaf_oid;
        if constexpr (ElementOID == kDynamicOid) {
          leaf_oid = Type2Oid(*t, false);
        } else {
          leaf_oid = ElementOID;
        }
        auto* prefix_data = context.writer->Alloc(12);
        absl::big_endian::Store32(prefix_data, 0);
        absl::big_endian::Store32(prefix_data + 4, 0);
        absl::big_endian::Store32(prefix_data + 8, leaf_oid);
        return;
      }
      // PG binary array: 12-byte top header (ndim, flags, elemtype) followed
      // by ndim*8 bytes of {dim_size, lbound} pairs, then element bytes.
      // FlattenArray fills each dim's size into its slot as it descends.
      auto* prefix_data = context.writer->Alloc(12 + ndim * 8);
      int32_t leaf_oid = ElementOID;
      bool has_null = false;
      FlattenArray<Core>(context, vdata, row, leaf_oid, has_null,
                         prefix_data + 12, 0);
      absl::big_endian::Store32(prefix_data, ndim);
      absl::big_endian::Store32(prefix_data + 4, has_null ? 1 : 0);
      absl::big_endian::Store32(prefix_data + 8, leaf_oid);
    }
  }
};

// Select the column-level (framed) serializer for the wire format: binary and
// top-level text are both int32-length-framed (BinaryField; top-level text just
// wraps the text core), COPY text uses \N framing, an in-record field uses the
// bare record-text framing.
template<typename TextCore, typename TextRecCore, typename BinaryCore>
SerializationFunction SelectFieldSerializer(VarFormat format,
                                            SerializationContext& context) {
  if (format == VarFormat::Binary) {
    return SerializeField<Framing::BinaryField, BinaryCore>;
  }
  if (format == VarFormat::CopyText) {
    return SerializeField<Framing::CopyTextField, TextCore>;
  }
  if (context.in_record) {
    return RecordTextField<TextRecCore>;
  }
  return SerializeField<Framing::BinaryField, TextCore>;
}

template<typename TextCore, typename BinaryCore, int32_t Oid>
SerializationFunction MakeArraySerializer(VarFormat format,
                                          SerializationContext& context,
                                          ArrayKind kind) {
  switch (kind) {
    case ArrayKind::SingleDimension: {
      using Text =
        OneDimArrayCore<TextCore, Oid, VarFormat::Text, WrapContext::None>;
      using TextRec =
        OneDimArrayCore<TextCore, Oid, VarFormat::Text, WrapContext::Record>;
      using Binary =
        OneDimArrayCore<BinaryCore, Oid, VarFormat::Binary, WrapContext::None>;
      return SelectFieldSerializer<Text, TextRec, Binary>(format, context);
    }
    case ArrayKind::MultiDimensions: {
      using Text =
        MultiDimArrayCore<TextCore, Oid, VarFormat::Text, WrapContext::None>;
      using TextRec =
        MultiDimArrayCore<TextCore, Oid, VarFormat::Text, WrapContext::Record>;
      using Binary = MultiDimArrayCore<BinaryCore, Oid, VarFormat::Binary,
                                       WrapContext::None>;
      return SelectFieldSerializer<Text, TextRec, Binary>(format, context);
    }
  }
  SDB_UNREACHABLE();
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
      return MakeArraySerializer<BoolTextCore, BoolBinCore, kBool>(
        format, context, kind);
    case TINYINT:
      return MakeArraySerializer<IntTextCore<int8_t>,
                                 IntBinCore<int8_t, int16_t>, kInt2>(
        format, context, kind);
    case UTINYINT:
      return MakeArraySerializer<IntTextCore<uint8_t>,
                                 IntBinCore<uint8_t, int16_t>, kInt2>(
        format, context, kind);
    case SMALLINT:
      return MakeArraySerializer<IntTextCore<int16_t>, IntBinCore<int16_t>,
                                 kInt2>(format, context, kind);
    case USMALLINT:
      return MakeArraySerializer<IntTextCore<uint16_t>,
                                 IntBinCore<uint16_t, int32_t>, kInt4>(
        format, context, kind);
    case INTEGER:
      return MakeArraySerializer<IntTextCore<int32_t>, IntBinCore<int32_t>,
                                 kInt4>(format, context, kind);
    case UINTEGER:
      return MakeArraySerializer<IntTextCore<uint32_t>,
                                 IntBinCore<uint32_t, int64_t>, kInt8>(
        format, context, kind);
    case BIGINT: {
      if (IsRegtype(type)) {
        return MakeArraySerializer<RegtypeTextCore, OidBinCore, kRegtype>(
          format, context, kind);
      }
      if (IsRegclass(type)) {
        return MakeArraySerializer<RegclassTextCore, OidBinCore, kRegclass>(
          format, context, kind);
      }
      if (IsRegnamespace(type)) {
        return MakeArraySerializer<RegnamespaceTextCore, OidBinCore,
                                   kRegnamespace>(format, context, kind);
      }
      if (IsOid(type)) {
        return MakeArraySerializer<IntTextCore<int64_t>, OidBinCore, kOid>(
          format, context, kind);
      }
      if (IsRegproc(type)) {
        return MakeArraySerializer<IntTextCore<int64_t>, OidBinCore, kRegproc>(
          format, context, kind);
      }
      if (IsRegprocedure(type)) {
        return MakeArraySerializer<IntTextCore<int64_t>, OidBinCore,
                                   kRegprocedure>(format, context, kind);
      }
      if (IsRegoper(type)) {
        return MakeArraySerializer<IntTextCore<int64_t>, OidBinCore, kRegoper>(
          format, context, kind);
      }
      if (IsRegoperator(type)) {
        return MakeArraySerializer<IntTextCore<int64_t>, OidBinCore,
                                   kRegoperator>(format, context, kind);
      }
      if (IsRegrole(type)) {
        return MakeArraySerializer<IntTextCore<int64_t>, OidBinCore, kRegrole>(
          format, context, kind);
      }
      if (IsRegconfig(type)) {
        return MakeArraySerializer<IntTextCore<int64_t>, OidBinCore,
                                   kRegconfig>(format, context, kind);
      }
      if (IsRegdictionary(type)) {
        return MakeArraySerializer<IntTextCore<int64_t>, OidBinCore,
                                   kRegdictionary>(format, context, kind);
      }
      if (IsRegcollation(type)) {
        return MakeArraySerializer<IntTextCore<int64_t>, OidBinCore,
                                   kRegcollation>(format, context, kind);
      }
      if (IsXid(type)) {
        return MakeArraySerializer<IntTextCore<int64_t>, OidBinCore, kXid>(
          format, context, kind);
      }
      if (IsCid(type)) {
        return MakeArraySerializer<IntTextCore<int64_t>, OidBinCore, kCid>(
          format, context, kind);
      }
      if (IsTid(type)) {
        return MakeArraySerializer<IntTextCore<int64_t>, OidBinCore, kTid>(
          format, context, kind);
      }
      // XID8 or BIGINT
      return MakeArraySerializer<IntTextCore<int64_t>, IntBinCore<int64_t>,
                                 kInt8>(format, context, kind);
    }
    case UBIGINT:
      return MakeArraySerializer<IntTextCore<uint64_t>, UbigintBinCore,
                                 kNumeric>(format, context, kind);
    case HUGEINT:
      return MakeArraySerializer<HugeintTextCore, HugeintBinCore, kNumeric>(
        format, context, kind);
    case UHUGEINT:
      return MakeArraySerializer<UhugeintTextCore, UhugeintBinCore, kNumeric>(
        format, context, kind);
    case FLOAT:
      return irs::ResolveBool(
        context.extra_float_digits > 0, [&]<bool Precise> {
          return MakeArraySerializer<FloatTextCore<float, Precise>,
                                     FloatBinCore<float>, kFloat4>(
            format, context, kind);
        });
    case DOUBLE:
      return irs::ResolveBool(
        context.extra_float_digits > 0, [&]<bool Precise> {
          return MakeArraySerializer<FloatTextCore<double, Precise>,
                                     FloatBinCore<double>, kFloat8>(
            format, context, kind);
        });
    case DECIMAL:
      switch (type.InternalType()) {
        using enum duckdb::PhysicalType;
        case INT16:
          return MakeArraySerializer<DecimalTextCore<int16_t>,
                                     DecimalBinCore<int16_t>, kNumeric>(
            format, context, kind);
        case INT32:
          return MakeArraySerializer<DecimalTextCore<int32_t>,
                                     DecimalBinCore<int32_t>, kNumeric>(
            format, context, kind);
        case INT64:
          return MakeArraySerializer<DecimalTextCore<int64_t>,
                                     DecimalBinCore<int64_t>, kNumeric>(
            format, context, kind);
        case INT128:
          return MakeArraySerializer<DecimalTextCore<duckdb::hugeint_t>,
                                     DecimalBinCore<duckdb::hugeint_t>,
                                     kNumeric>(format, context, kind);
        default:
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
            ERR_MSG("Unsupported decimal internal type in array"));
      }
    case CHAR:
    case VARCHAR:
      if (type.IsJSONType()) {
        return MakeArraySerializer<JsonTextCore<WrapContext::Array>,
                                   JsonBinCore, kJson>(format, context, kind);
      }
      if (IsName(type)) {
        return MakeArraySerializer<VarcharTextCore<WrapContext::Array>,
                                   VarcharBinCore, kName>(format, context,
                                                          kind);
      }
      return MakeArraySerializer<VarcharTextCore<WrapContext::Array>,
                                 VarcharBinCore, kText>(format, context, kind);
    case BLOB:
      if (context.bytea_output == ByteaOutput::Hex) {
        return MakeArraySerializer<ByteaHexTextCore<WrapContext::Array>,
                                   ByteaBinCore, kBytea>(format, context, kind);
      }
      SDB_ASSERT(context.bytea_output == ByteaOutput::Escape);
      return MakeArraySerializer<ByteaEscapeTextCore<WrapContext::Array>,
                                 ByteaBinCore, kBytea>(format, context, kind);
    case DATE:
      return MakeArraySerializer<DateTextCore, DateBinCore, kDate>(
        format, context, kind);
    case TIME:
      return MakeArraySerializer<TimeTextCore, TimeBinCore, kTime>(
        format, context, kind);
    case TIME_NS:
      return MakeArraySerializer<TimeNsTextCore, TimeNsBinCore, kTime>(
        format, context, kind);
    case TIME_TZ:
      return MakeArraySerializer<TimeTzTextCore, TimeTzBinCore, kTimeTz>(
        format, context, kind);
    case TIMESTAMP_SEC:
      return MakeArraySerializer<TimestampSecTextCore<WrapContext::Array>,
                                 TimestampSecBinCore, kTimestamp>(
        format, context, kind);
    case TIMESTAMP_MS:
      return MakeArraySerializer<TimestampMsTextCore<WrapContext::Array>,
                                 TimestampMsBinCore, kTimestamp>(format,
                                                                 context, kind);
    case TIMESTAMP:
      return MakeArraySerializer<TimestampTextCore<WrapContext::Array>,
                                 TimestampBinCore, kTimestamp>(format, context,
                                                               kind);
    case TIMESTAMP_NS:
      return MakeArraySerializer<TimestampNsTextCore<WrapContext::Array>,
                                 TimestampNsBinCore, kTimestamp>(format,
                                                                 context, kind);
    case TIMESTAMP_TZ:
      return MakeArraySerializer<TimestampTzTextCore<WrapContext::Array>,
                                 TimestampTzBinCore, kTimestampTz>(
        format, context, kind);
    case INTERVAL:
      return MakeArraySerializer<IntervalTextCore<WrapContext::Array>,
                                 IntervalBinCore, kInterval>(format, context,
                                                             kind);
    case UUID:
      return MakeArraySerializer<UuidTextCore, UuidBinCore, kUuid>(
        format, context, kind);
    case BIT:
      return MakeArraySerializer<BitTextCore, BitBinCore, kVarbit>(
        format, context, kind);
    case STRUCT:
      if (IsInet(type)) {
        return MakeArraySerializer<InetTextCore<WrapContext::Array>,
                                   InetBinCore, kInet>(format, context, kind);
      }
      // Element OID is resolved per-row by Type2Oid: anonymous ROW(...) yields
      // kRecord, named record types yield their pg_type OID.
      return MakeArraySerializer<RecordTextCore<WrapContext::Array>,
                                 RecordBinCore, kDynamicOid>(format, context,
                                                             kind);
    case ENUM:
      return MakeArraySerializer<EnumTextCore<WrapContext::Array>, EnumBinCore,
                                 kDynamicOid>(format, context, kind);
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
  // types_cache stays lazy (GetSerializersCache); record results only.
  context.types_cache.reset();
}

SerializationFunction GetSerialization(const duckdb::LogicalType& type,
                                       VarFormat format,
                                       SerializationContext& context) {
  SDB_ASSERT(!context.in_record || format == VarFormat::Text);
  switch (type.id()) {
    using enum duckdb::LogicalTypeId;
    case SQLNULL:
      return SerializeNullColumn;
    case BOOLEAN:
      return SelectFieldSerializer<BoolTextCore, BoolTextCore, BoolBinCore>(
        format, context);
    case TINYINT:
      return SelectFieldSerializer<IntTextCore<int8_t>, IntTextCore<int8_t>,
                                   IntBinCore<int8_t, int16_t>>(format,
                                                                context);
    case SMALLINT:
      return SelectFieldSerializer<IntTextCore<int16_t>, IntTextCore<int16_t>,
                                   IntBinCore<int16_t>>(format, context);
    case INTEGER:
      return SelectFieldSerializer<IntTextCore<int32_t>, IntTextCore<int32_t>,
                                   IntBinCore<int32_t>>(format, context);
    case BIGINT: {
      if (IsRegtype(type)) {
        return SelectFieldSerializer<RegtypeTextCore, RegtypeTextCore,
                                     OidBinCore>(format, context);
      }
      if (IsRegclass(type)) {
        return SelectFieldSerializer<RegclassTextCore, RegclassTextCore,
                                     OidBinCore>(format, context);
      }
      if (IsRegnamespace(type)) {
        return SelectFieldSerializer<RegnamespaceTextCore, RegnamespaceTextCore,
                                     OidBinCore>(format, context);
      }
      if (IsOidLike(type)) {
        return SelectFieldSerializer<IntTextCore<int64_t>, IntTextCore<int64_t>,
                                     OidBinCore>(format, context);
      }
      return SelectFieldSerializer<IntTextCore<int64_t>, IntTextCore<int64_t>,
                                   IntBinCore<int64_t>>(format, context);
    }
    case UTINYINT:
      return SelectFieldSerializer<IntTextCore<uint8_t>, IntTextCore<uint8_t>,
                                   IntBinCore<uint8_t, int16_t>>(format,
                                                                 context);
    case USMALLINT:
      return SelectFieldSerializer<IntTextCore<uint16_t>, IntTextCore<uint16_t>,
                                   IntBinCore<uint16_t, int32_t>>(format,
                                                                  context);
    case UINTEGER:
      return SelectFieldSerializer<IntTextCore<uint32_t>, IntTextCore<uint32_t>,
                                   IntBinCore<uint32_t, int64_t>>(format,
                                                                  context);
    case UBIGINT:
      return SelectFieldSerializer<IntTextCore<uint64_t>, IntTextCore<uint64_t>,
                                   UbigintBinCore>(format, context);
    case HUGEINT:
      return SelectFieldSerializer<HugeintTextCore, HugeintTextCore,
                                   HugeintBinCore>(format, context);
    case UHUGEINT:
      return SelectFieldSerializer<UhugeintTextCore, UhugeintTextCore,
                                   UhugeintBinCore>(format, context);
    case FLOAT:
      return irs::ResolveBool(
        context.extra_float_digits > 0, [&]<bool Precise> {
          return SelectFieldSerializer<FloatTextCore<float, Precise>,
                                       FloatTextCore<float, Precise>,
                                       FloatBinCore<float>>(format, context);
        });
    case DOUBLE:
      return irs::ResolveBool(
        context.extra_float_digits > 0, [&]<bool Precise> {
          return SelectFieldSerializer<FloatTextCore<double, Precise>,
                                       FloatTextCore<double, Precise>,
                                       FloatBinCore<double>>(format, context);
        });
    case DECIMAL:
      switch (type.InternalType()) {
        using enum duckdb::PhysicalType;
        case INT16:
          return SelectFieldSerializer<DecimalTextCore<int16_t>,
                                       DecimalTextCore<int16_t>,
                                       DecimalBinCore<int16_t>>(format,
                                                                context);
        case INT32:
          return SelectFieldSerializer<DecimalTextCore<int32_t>,
                                       DecimalTextCore<int32_t>,
                                       DecimalBinCore<int32_t>>(format,
                                                                context);
        case INT64:
          return SelectFieldSerializer<DecimalTextCore<int64_t>,
                                       DecimalTextCore<int64_t>,
                                       DecimalBinCore<int64_t>>(format,
                                                                context);
        case INT128:
          return SelectFieldSerializer<DecimalTextCore<duckdb::hugeint_t>,
                                       DecimalTextCore<duckdb::hugeint_t>,
                                       DecimalBinCore<duckdb::hugeint_t>>(
            format, context);
        default:
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                          ERR_MSG("Unsupported decimal internal type"));
      }
    case CHAR:
    case VARCHAR:
      if (type.IsJSONType()) {
        return SelectFieldSerializer<JsonTextCore<WrapContext::None>,
                                     JsonTextCore<WrapContext::Record>,
                                     JsonBinCore>(format, context);
      }
      return SelectFieldSerializer<VarcharTextCore<WrapContext::None>,
                                   VarcharTextCore<WrapContext::Record>,
                                   VarcharBinCore>(format, context);
    case BLOB:
      if (context.bytea_output == ByteaOutput::Hex) {
        return SelectFieldSerializer<ByteaHexTextCore<WrapContext::None>,
                                     ByteaHexTextCore<WrapContext::Record>,
                                     ByteaBinCore>(format, context);
      }
      SDB_ASSERT(context.bytea_output == ByteaOutput::Escape);
      return SelectFieldSerializer<ByteaEscapeTextCore<WrapContext::None>,
                                   ByteaEscapeTextCore<WrapContext::Record>,
                                   ByteaBinCore>(format, context);
    case DATE:
      return SelectFieldSerializer<DateTextCore, DateTextCore, DateBinCore>(
        format, context);
    case TIME:
      return SelectFieldSerializer<TimeTextCore, TimeTextCore, TimeBinCore>(
        format, context);
    case TIME_NS:
      return SelectFieldSerializer<TimeNsTextCore, TimeNsTextCore,
                                   TimeNsBinCore>(format, context);
    case TIME_TZ:
      return SelectFieldSerializer<TimeTzTextCore, TimeTzTextCore,
                                   TimeTzBinCore>(format, context);
    case TIMESTAMP_SEC:
      return SelectFieldSerializer<TimestampSecTextCore<WrapContext::None>,
                                   TimestampSecTextCore<WrapContext::Record>,
                                   TimestampSecBinCore>(format, context);
    case TIMESTAMP_MS:
      return SelectFieldSerializer<TimestampMsTextCore<WrapContext::None>,
                                   TimestampMsTextCore<WrapContext::Record>,
                                   TimestampMsBinCore>(format, context);
    case TIMESTAMP:
      return SelectFieldSerializer<TimestampTextCore<WrapContext::None>,
                                   TimestampTextCore<WrapContext::Record>,
                                   TimestampBinCore>(format, context);
    case TIMESTAMP_NS:
      return SelectFieldSerializer<TimestampNsTextCore<WrapContext::None>,
                                   TimestampNsTextCore<WrapContext::Record>,
                                   TimestampNsBinCore>(format, context);
    case TIMESTAMP_TZ:
      return SelectFieldSerializer<TimestampTzTextCore<WrapContext::None>,
                                   TimestampTzTextCore<WrapContext::Record>,
                                   TimestampTzBinCore>(format, context);
    case INTERVAL:
      return SelectFieldSerializer<IntervalTextCore<WrapContext::None>,
                                   IntervalTextCore<WrapContext::Record>,
                                   IntervalBinCore>(format, context);
    case UUID:
      return SelectFieldSerializer<UuidTextCore, UuidTextCore, UuidBinCore>(
        format, context);
    case BIT:
      return SelectFieldSerializer<BitTextCore, BitTextCore, BitBinCore>(
        format, context);
    case ENUM:
      return SelectFieldSerializer<EnumTextCore<WrapContext::None>,
                                   EnumTextCore<WrapContext::Record>,
                                   EnumBinCore>(format, context);
    case STRUCT:
      if (IsInet(type)) {
        return SelectFieldSerializer<InetTextCore<WrapContext::None>,
                                     InetTextCore<WrapContext::Record>,
                                     InetBinCore>(format, context);
      }
      return SelectFieldSerializer<RecordTextCore<WrapContext::None>,
                                   RecordTextCore<WrapContext::Record>,
                                   RecordBinCore>(format, context);
    case ARRAY:
    case LIST:
    case MAP: {
      const auto* element_type = &type;
      size_t dims = 0;
      while (true) {
        if (element_type->id() == ARRAY) {
          element_type = &duckdb::ArrayType::GetChildType(*element_type);
        } else if (element_type->id() == LIST || element_type->id() == MAP) {
          element_type = &duckdb::ListType::GetChildType(*element_type);
        } else {
          break;
        }
        ++dims;
      }
      // >=2-D nesting -> PG multi-dim flatten (rectangular; a ragged inner
      // LIST/MAP throws in binary). 1-D LIST/ARRAY and a top-level MAP (a 1-D
      // list of key/value structs) all serialize the same single-dimension way.
      const auto kind =
        dims > 1 ? ArrayKind::MultiDimensions : ArrayKind::SingleDimension;
      return GetArraySerialization(*element_type, format, context, kind);
    }
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("Such type is not supported"));
  }
}

}  // namespace sdb::pg
