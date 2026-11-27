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
#include <absl/strings/charset.h>
#include <absl/strings/escaping.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/time/civil_time.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cfloat>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/operator/add.hpp>
#include <duckdb/common/operator/string_cast.hpp>
#include <duckdb/common/types/bit.hpp>
#include <duckdb/common/types/cast_helpers.hpp>
#include <duckdb/common/types/hugeint.hpp>
#include <duckdb/common/types/string_heap.hpp>
#include <duckdb/common/types/time.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/types/uhugeint.hpp>
#include <duckdb/common/types/uuid.hpp>
#include <duckdb/inet/inet_ipaddress.hpp>
#include <icu-helpers.hpp>
#include <icu-zone-lut.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/dispatch.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <limits>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>

#include "connector/functions/ts_query_codec.h"
#include "pg/catalog/functions/pg_catalog/regproc.h"
#include "pg/catalog/utils/session.h"
#include "pg/connection_context.h"
#include "pg/datetime_text.h"
#include "pg/types/char.h"
#include "pg/types/float.h"
#include "pg/types/geometric.h"
#include "pg/types/inet.h"
#include "pg/types/logical.h"
#include "pg/types/mac.h"
#include "pg/types/mapping.h"
#include "pg/types/money.h"
#include "pg/types/pg_lsn.h"
#include "pg/types/range.h"
#include "pg/types/tid.h"
#include "pg/types/xml.h"
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

inline constexpr uint64_t kDynamicOid = std::numeric_limits<uint64_t>::max();

enum class WrapContext : uint8_t {
  None,
  Array,
  Record,
};

bool RecordItemNeedsQuoting(std::string_view s);
template<WrapContext InContainer>
bool NeedsQuotingIn(SerializationContext& ctx, const duckdb::LogicalType& type,
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

IRS_FORCE_INLINE bool EscapesNothing(const SerializationContext& ctx) {
  return !ctx.copy_text && ctx.quote_seq.size() == 1 &&
         ctx.backslash_count == 1;
}

struct WriterSink {
  message::Writer& writer;

  IRS_FORCE_INLINE char* Alloc(size_t n) {
    CheckMessageSize(writer.Written() + n);
    return reinterpret_cast<char*>(writer.Alloc(n));
  }
  IRS_FORCE_INLINE void Write(std::string_view s) { writer.Write(s); }
};

struct RawSink {
  uint8_t* out;

  IRS_FORCE_INLINE char* Alloc(size_t n) {
    return reinterpret_cast<char*>(std::exchange(out, out + n));
  }
  IRS_FORCE_INLINE void Write(std::string_view s) {
    std::memcpy(out, s.data(), s.size());
    out += s.size();
  }
};

template<typename Sink>
IRS_FORCE_INLINE void EscapeTo(const SerializationContext& ctx,
                               std::string_view s, Sink& sink) {
  const auto quote_seq_size = ctx.quote_seq.size();
  const auto backslash_count = ctx.backslash_count;
  const auto& escapes = ctx.text_escapes[(quote_seq_size != 1 ? 2 : 0) +
                                         (backslash_count != 1 ? 1 : 0)];
  const char* p = s.data();
  const size_t n = s.size();
  size_t start = 0;
  size_t i = escapes.Find(s, 0);
  while (i < n) {
    const char c = p[i];
    const size_t run = i - start;
    if (c == '"') {
      char* buf = sink.Alloc(run + quote_seq_size);
      std::memcpy(buf, p + start, run);
      std::memcpy(buf + run, ctx.quote_seq.data(), quote_seq_size);
    } else if (c == '\\') {
      char* buf = sink.Alloc(run + backslash_count);
      std::memcpy(buf, p + start, run);
      std::memset(buf + run, '\\', backslash_count);
    } else {
      char* buf = sink.Alloc(run + 2);
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
    i = escapes.Find(s, start);
  }
  sink.Write(s.substr(start));
}

void EmitEscaped(SerializationContext& ctx, std::string_view s) {
  if (EscapesNothing(ctx)) {
    ctx.writer->Write(s);
    return;
  }
  WriterSink sink{*ctx.writer};
  EscapeTo(ctx, s, sink);
}

void WriteCopyDelimEscaped(SerializationContext& ctx, std::string_view text) {
  const char delim = ctx.copy_delim;
  auto& writer = *ctx.writer;
  size_t start = 0;
  for (auto pos = text.find(delim); pos != std::string_view::npos;
       pos = text.find(delim, start)) {
    const auto run = pos - start;
    CheckMessageSize(writer.Written() + run + 2);
    char* buf = reinterpret_cast<char*>(writer.Alloc(run + 2));
    std::memcpy(buf, text.data() + start, run);
    buf[run] = '\\';
    buf[run + 1] = delim;
    start = pos + 1;
  }
  writer.Write(text.substr(start));
}

template<bool kCopyDelim>
IRS_FORCE_INLINE void WriteText(SerializationContext& ctx,
                                std::string_view text) {
  if constexpr (kCopyDelim) {
    WriteCopyDelimEscaped(ctx, text);
  } else {
    ctx.writer->Write(text);
  }
}

template<bool kCopyDelim, size_t N>
IRS_FORCE_INLINE void WriteText(SerializationContext& ctx,
                                const char (&text)[N]) {
  if constexpr (kCopyDelim) {
    WriteCopyDelimEscaped(ctx, {text, N - 1});
  } else {
    ctx.writer->Write(text);
  }
}

size_t EscapeCopyDelimInPlace(uint8_t* data, size_t size, char delim) {
  const auto byte = static_cast<uint8_t>(delim);
  const auto escapes =
    static_cast<size_t>(absl::c_count(std::span{data, size}, byte));
  auto* in = data + size;
  auto* out = in + escapes;
  while (in != out) {
    const auto c = *--in;
    *--out = c;
    if (c == byte) {
      *--out = '\\';
    }
  }
  return size + escapes;
}

// Array escape rule:
//   source '"'  -> previous '\\' encoding + previous '"' encoding
//   source '\\' -> two previous '\\' encodings (count doubles)
void EnterArrayWrap(SerializationContext& ctx, std::string& q_buf) {
  CheckMessageSize(size_t{ctx.backslash_count} * 2 + ctx.quote_seq.size());
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
  CheckMessageSize(size_t{ctx.backslash_count} * 2 + ctx.quote_seq.size() * 2);
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
  using Value = duckdb::string_t;
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx, Value raw) {
    EmitTextItem<InContainer>(ctx,
                              std::string_view{raw.GetData(), raw.GetSize()});
  }
};

struct VarcharBinCore {
  using Value = duckdb::string_t;
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx, Value raw) {
    ctx.writer->Write(std::string_view{raw.GetData(), raw.GetSize()});
  }
};

template<WrapContext InContainer>
struct XmlTextCore {
  using Value = duckdb::string_t;
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx, Value raw) {
    const auto xml = XmlOut({raw.GetData(), raw.GetSize()});
    if (xml.declaration.empty()) {
      EmitTextItem<InContainer>(ctx, xml.body);
    } else {
      EmitTextItem<InContainer>(ctx, absl::StrCat(xml.declaration, xml.body));
    }
  }
};

struct XmlBinCore {
  using Value = duckdb::string_t;
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx, Value raw) {
    const auto xml = XmlOut({raw.GetData(), raw.GetSize()});
    ctx.writer->Write(xml.declaration);
    ctx.writer->Write(xml.body);
  }
};

struct CharBinCore {
  using Value = duckdb::string_t;
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx, Value raw) {
    const auto value =
      static_cast<char>(CharIn({raw.GetData(), raw.GetSize()}));
    ctx.writer->Write(std::string_view{&value, 1});
  }
};

template<WrapContext InContainer, bool kCopyDelim = false>
struct GeometryTextCore {
  using Value = duckdb::string_t;
  using CopyDelimEscaped = GeometryTextCore<InContainer, true>;
  static constexpr const char* kArrayDelim = ":";
  static constexpr absl::CharSet kCopyDelims{"ABCDEF"};
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx, Value raw) {
    static constexpr char kHexUpper[] = "0123456789ABCDEF";
    const std::string_view value{raw.GetData(), raw.GetSize()};
    if constexpr (kCopyDelim) {
      const char delim = ctx.copy_delim;
      ctx.writer->Write(4 * value.size(), [&](uint8_t* data) {
        char* out = reinterpret_cast<char*>(data);
        for (const char c : value) {
          const auto byte = static_cast<unsigned char>(c);
          for (const char hex :
               {kHexUpper[byte >> 4U], kHexUpper[byte & 0x0FU]}) {
            if (hex == delim) {
              *out++ = '\\';
            }
            *out++ = hex;
          }
        }
        return static_cast<size_t>(out - reinterpret_cast<char*>(data));
      });
      return;
    }
    auto* data = ctx.writer->Alloc(2 * value.size());
    char* out = reinterpret_cast<char*>(data);
    for (const char c : value) {
      const auto byte = static_cast<unsigned char>(c);
      *out++ = kHexUpper[byte >> 4U];
      *out++ = kHexUpper[byte & 0x0FU];
    }
  }
};

struct GeometryBinCore {
  using Value = duckdb::string_t;
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx, Value raw) {
    ctx.writer->Write(std::string_view{raw.GetData(), raw.GetSize()});
  }
};

IRS_FORCE_INLINE duckdb::string_t EnumLabel(
  const duckdb::LogicalType& type, const duckdb::UnifiedVectorFormat& unified,
  duckdb::idx_t idx) {
  switch (duckdb::EnumType::GetPhysicalType(type)) {
    using enum duckdb::PhysicalType;
    case UINT8:
      return duckdb::EnumType::GetString(
        type, duckdb::UnifiedVectorFormat::GetData<uint8_t>(unified)[idx]);
    case UINT16:
      return duckdb::EnumType::GetString(
        type, duckdb::UnifiedVectorFormat::GetData<uint16_t>(unified)[idx]);
    case UINT32:
      return duckdb::EnumType::GetString(
        type, duckdb::UnifiedVectorFormat::GetData<uint32_t>(unified)[idx]);
    default:
      SDB_UNREACHABLE();
  }
}

template<WrapContext InContainer>
struct EnumTextCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto label = EnumLabel(vdata.logical_type, vdata.unified, idx);
    EmitTextItem<InContainer>(
      ctx, std::string_view{label.GetData(), label.GetSize()});
  }
};

struct EnumBinCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto label = EnumLabel(vdata.logical_type, vdata.unified, idx);
    ctx.writer->Write(std::string_view{label.GetData(), label.GetSize()});
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
inline constexpr uint32_t kNumericBinaryMaxBytes =
  8 +
  2 * ((std::numeric_limits<typename NumericUnsigned<T>::Type>::digits10 + 4) /
         4 +
       1);

template<typename T>
size_t RenderNumericBinary(uint8_t* data, T value, int32_t scale) {
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
      mag = static_cast<U>(-static_cast<U>(value));
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
  const auto size = static_cast<size_t>(8 + ndigits * 2);
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
  return size;
}

template<typename T>
void WriteAsNumericBinary(SerializationContext& context, T value,
                          int32_t scale) {
  context.writer->Write(kNumericBinaryMaxBytes<T>, [&](uint8_t* data) {
    return RenderNumericBinary(data, value, scale);
  });
}

template<typename PhysicalType>
struct DecimalTextCore {
  // Worst case is the all-fractional negative ("-0." + scale digits): the digit
  // budget is the physical type's max width, plus sign + leading "0" + ".".
  static constexpr uint32_t kMaxBytes =
    uint32_t{duckdb::DecimalWidth<PhysicalType>::max} + 3;
  static constexpr absl::CharSet kCopyDelims{"-"};
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
using NumericOf =
  std::conditional_t<std::is_same_v<PhysicalType, duckdb::hugeint_t>,
                     absl::int128, PhysicalType>;

template<typename PhysicalType>
struct DecimalBinCore {
  static constexpr uint32_t kMaxBytes =
    kNumericBinaryMaxBytes<NumericOf<PhysicalType>>;
  IRS_FORCE_INLINE static size_t Render(
    uint8_t* dst, SerializationContext&,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    const auto scale = duckdb::DecimalType::GetScale(vdata.logical_type);
    const auto value = duckdb::UnifiedVectorFormat::GetDataUnsafe<PhysicalType>(
      vdata.unified)[idx];
    if constexpr (std::is_same_v<PhysicalType, duckdb::hugeint_t>) {
      return RenderNumericBinary(
        dst, absl::MakeInt128(value.upper, value.lower), scale);
    } else {
      return RenderNumericBinary(dst, value, scale);
    }
  }
};

struct UbigintBinCore {
  using Value = uint64_t;
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx, Value v) {
    WriteAsNumericBinary(ctx, v, 0);
  }
};

struct HugeintTextCore {
  using Value = duckdb::hugeint_t;
  static constexpr uint32_t kMaxBytes =
    absl::numbers_internal::kFastToBuffer128Size;
  static constexpr absl::CharSet kCopyDelims{"-"};
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value v) {
    char* buf = reinterpret_cast<char*>(dst);
    char* ptr = absl::numbers_internal::FastIntToBuffer(
      absl::MakeInt128(v.upper, v.lower), buf);
    return static_cast<size_t>(ptr - buf);
  }
};

struct HugeintBinCore {
  using Value = duckdb::hugeint_t;
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx, Value v) {
    WriteAsNumericBinary(ctx, absl::MakeInt128(v.upper, v.lower), 0);
  }
};

struct UhugeintTextCore {
  using Value = duckdb::uhugeint_t;
  static constexpr uint32_t kMaxBytes =
    absl::numbers_internal::kFastToBuffer128Size;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value v) {
    char* buf = reinterpret_cast<char*>(dst);
    char* ptr = absl::numbers_internal::FastIntToBuffer(
      absl::MakeUint128(v.upper, v.lower), buf);
    return static_cast<size_t>(ptr - buf);
  }
};

struct UhugeintBinCore {
  using Value = duckdb::hugeint_t;
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx, Value v) {
    WriteAsNumericBinary(ctx, absl::MakeUint128(v.upper, v.lower), 0);
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
  using Value = duckdb::string_t;
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx, Value raw) {
    const std::string_view value{raw.GetData(), raw.GetSize()};
    // '\\xnnnn' always starts with '\\' which triggers wrap in both array and
    // record contexts.
    if constexpr (InContainer == WrapContext::None) {
      ByteaOutHex(ctx, value);
    } else {
      WriteWrapped<InContainer>(ctx, [&] { ByteaOutHex(ctx, value); });
    }
  }
};

template<bool kCopyDelim>
void ByteaOutEscape(SerializationContext& ctx, std::string_view value) {
  const char delim = ctx.copy_delim;
  size_t backslash_cnt = 0;
  size_t non_printable_cnt = 0;
  size_t delim_cnt = 0;
  for (char c : value) {
    if (c == '\\') {
      ++backslash_cnt;
    } else if (!absl::ascii_isprint(c)) {
      ++non_printable_cnt;
    } else if (kCopyDelim && c == delim) {
      ++delim_cnt;
    }
  }
  const auto bs_n = ctx.backslash_count;
  const size_t body_size = (value.size() - backslash_cnt - non_printable_cnt) +
                           backslash_cnt * 2 * bs_n +
                           non_printable_cnt * (bs_n + 3) + delim_cnt;
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
      if constexpr (kCopyDelim) {
        if (c == static_cast<unsigned char>(delim)) {
          *p++ = '\\';
        }
      }
      *p++ = static_cast<char>(c);
    }
  }
}

template<WrapContext InContainer, bool kCopyDelim = false>
struct ByteaEscapeTextCore {
  using Value = duckdb::string_t;
  using CopyDelimEscaped = ByteaEscapeTextCore<InContainer, true>;
  static constexpr absl::CharSet kCopyDelims = absl::CharSet::AsciiPrintable();
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx, Value raw) {
    const std::string_view value{raw.GetData(), raw.GetSize()};

    if constexpr (InContainer == WrapContext::None) {
      ByteaOutEscape<kCopyDelim>(ctx, value);
    } else {
      const bool has_special =
        absl::c_any_of(value, [](char c) { return !absl::ascii_isprint(c); }) ||
        (InContainer == WrapContext::Array ? ArrayItemNeedQuotesAndEscape(value)
                                           : RecordItemNeedsQuoting(value));
      if (has_special) {
        WriteWrapped<InContainer>(
          ctx, [&] { ByteaOutEscape<kCopyDelim>(ctx, value); });
      } else {
        ByteaOutEscape<kCopyDelim>(ctx, value);
      }
    }
  }
};

struct ByteaBinCore {
  using Value = duckdb::string_t;
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx, Value raw) {
    ctx.writer->Write(std::string_view{raw.GetData(), raw.GetSize()});
  }
};

struct BoolTextCore {
  using Value = bool;
  static constexpr uint32_t kMaxBytes = 1;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value v) {
    *dst = v ? 't' : 'f';
    return 1;
  }
};

struct BoolBinCore {
  using Value = bool;
  static constexpr uint32_t kMaxBytes = 1;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value v) {
    *reinterpret_cast<bool*>(dst) = v;
    return 1;
  }
};

template<typename Read, typename Wire = Read>
struct IntBinCore {
  using Value = Read;
  static constexpr uint32_t kMaxBytes = sizeof(Wire);
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value v) {
    absl::big_endian::Store(dst, static_cast<Wire>(v));
    return sizeof(Wire);
  }
};

template<typename Read>
struct IntTextCore {
  using Value = Read;
  static constexpr uint32_t kMaxBytes = kIntStrMaxLen;
  static constexpr absl::CharSet kCopyDelims{std::is_signed_v<Read> ? "-" : ""};
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value v) {
    char* buf = reinterpret_cast<char*>(dst);
    char* end = absl::numbers_internal::FastIntToBuffer(v, buf);
    return static_cast<size_t>(end - buf);
  }
};

struct LsnTextCore {
  using Value = uint64_t;
  static constexpr uint32_t kMaxBytes = kLsnBufferSize;
  static constexpr absl::CharSet kCopyDelims{"/ABCDEF"};
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value lsn) {
    return WriteLsn(lsn, reinterpret_cast<char*>(dst));
  }
};

struct TidBinCore {
  using Value = int64_t;
  static constexpr uint32_t kMaxBytes = kTidBytes;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value tid) {
    TidSend(tid, dst);
    return kTidBytes;
  }
};

template<bool kMac8>
struct MacTextCore {
  using Value = uint64_t;
  static constexpr uint32_t kMaxBytes = kMacaddr8Text;
  static constexpr absl::CharSet kCopyDelims{":"};
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value mac) {
    auto* text = reinterpret_cast<char*>(dst);
    return kMac8 ? WriteMacaddr8(mac, text) : WriteMacaddr(mac, text);
  }
};

template<bool kMac8>
struct MacBinCore {
  using Value = uint64_t;
  static constexpr uint32_t kMaxBytes = kMac8 ? kMacaddr8Bytes : kMacaddrBytes;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value mac) {
    MacSend(mac, kMaxBytes, dst);
    return kMaxBytes;
  }
};

struct OidBinCore {
  using Value = int64_t;
  static constexpr uint32_t kMaxBytes = 4;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value value) {
    const auto oid = static_cast<uint64_t>(value);
    if (oid != WireOid(oid)) {
      SDB_WARN(HTTP, "reg* OID ", oid,
               " truncated to 32-bit for binary wire protocol");
    }
    absl::big_endian::Store32(dst, WireOid(oid));
    return 4;
  }
};

template<typename T>
struct FloatBinCore {
  using Value = T;
  static constexpr uint32_t kMaxBytes = sizeof(T);
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value v) {
    absl::big_endian::Store(dst, v);
    return sizeof(T);
  }
};

template<typename T, bool Precise>
struct FloatTextCore {
  using Value = T;
  static constexpr uint32_t kMaxBytes = kNumberStrMaxLen;
  static constexpr absl::CharSet kCopyDelims{"-+IN"};
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, SerializationContext& ctx,
                                        Value v) {
    return WriteFloat<Precise>(v, ctx.extra_float_digits,
                               reinterpret_cast<char*>(dst));
  }
};

// Wrap text output in current-depth quote_seq when emitting inside an
// array or record. Safe only for types whose textual form never contains
// '"' or '\\' (timestamps, intervals) -- no internal escaping needed.
template<WrapContext InContainer, typename Body>
void WithWrapIfNested(SerializationContext& context, bool wrap, Body&& body) {
  if constexpr (InContainer != WrapContext::None) {
    if (wrap) {
      WriteWrapped<InContainer>(context, [&] { body(); });
      return;
    }
  }
  body();
}

inline constexpr absl::CharSet kTimestampCopyDelims{"- :()BC"};

template<WrapContext InContainer, bool kCopyDelim = false>
struct TimestampSecTextCore {
  using Value = duckdb::timestamp_sec_t;
  using CopyDelimEscaped = TimestampSecTextCore<InContainer, true>;
  static constexpr absl::CharSet kCopyDelims = kTimestampCopyDelims;
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx,
                                      Value timestamp) {
    auto str = duckdb::Timestamp::ToString(
      timestamp.IsFinite()
        ? duckdb::Timestamp::FromEpochSeconds(timestamp.value)
        : duckdb::timestamp_t(timestamp.value));
    WithWrapIfNested<InContainer>(ctx, timestamp.IsFinite(),
                                  [&] { WriteText<kCopyDelim>(ctx, str); });
  }
};

template<WrapContext InContainer, bool kCopyDelim = false>
struct TidTextCore {
  using Value = int64_t;
  using CopyDelimEscaped = TidTextCore<InContainer, true>;
  static constexpr absl::CharSet kCopyDelims{"(,)"};
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx, Value tid) {
    char buffer[kTidBufferSize];
    const std::string_view text{buffer, WriteTid(tid, buffer)};
    WithWrapIfNested<InContainer>(ctx, true,
                                  [&] { WriteText<kCopyDelim>(ctx, text); });
  }
};

struct MoneyTextCore {
  using Value = duckdb::hugeint_t;
  static constexpr uint32_t kMaxBytes = kMoneyText;
  static constexpr absl::CharSet kCopyDelims{"$,-"};
  IRS_FORCE_INLINE static bool NeedsQuoting(Value money) {
    return money <= -kMoneyThousand || money >= kMoneyThousand;
  }
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value money) {
    return WriteMoney(money, reinterpret_cast<char*>(dst));
  }
};

struct MoneyBinCore {
  using Value = duckdb::hugeint_t;
  static constexpr uint32_t kMaxBytes = sizeof(int64_t);
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value money) {
    absl::big_endian::Store(dst, MoneyCents(money));
    return sizeof(int64_t);
  }
};

int64_t TimestampSecondsToWire(int64_t value) {
  if (value == std::numeric_limits<int64_t>::max()) {
    return value;
  }
  if (value == -std::numeric_limits<int64_t>::max()) {
    return std::numeric_limits<int64_t>::min();
  }
  return (value - kGapSec) * 1'000'000;
}

int64_t TimestampMillisToWire(int64_t value) {
  if (value == std::numeric_limits<int64_t>::max()) {
    return value;
  }
  if (value == -std::numeric_limits<int64_t>::max()) {
    return std::numeric_limits<int64_t>::min();
  }
  return (value - kGapMs) * 1000;
}

int64_t TimestampNanosToWire(int64_t value) {
  if (value == std::numeric_limits<int64_t>::max()) {
    return value;
  }
  if (value == -std::numeric_limits<int64_t>::max()) {
    return std::numeric_limits<int64_t>::min();
  }
  return value / 1000 - kGapUs;
}

struct TimestampSecBinCore {
  using Value = duckdb::timestamp_sec_t;
  static constexpr uint32_t kMaxBytes = 8;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value timestamp) {
    absl::big_endian::Store64(dst, TimestampSecondsToWire(timestamp.value));
    return 8;
  }
};

template<WrapContext InContainer, bool kCopyDelim = false>
struct TimestampMsTextCore {
  using Value = duckdb::timestamp_ms_t;
  using CopyDelimEscaped = TimestampMsTextCore<InContainer, true>;
  static constexpr absl::CharSet kCopyDelims = kTimestampCopyDelims;
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx,
                                      Value timestamp) {
    auto str = duckdb::Timestamp::ToString(
      timestamp.IsFinite() ? duckdb::Timestamp::FromEpochMs(timestamp.value)
                           : duckdb::timestamp_t(timestamp.value));
    WithWrapIfNested<InContainer>(ctx, timestamp.IsFinite(),
                                  [&] { WriteText<kCopyDelim>(ctx, str); });
  }
};

struct TimestampMsBinCore {
  using Value = duckdb::timestamp_ms_t;
  static constexpr uint32_t kMaxBytes = 8;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value timestamp) {
    absl::big_endian::Store64(dst, TimestampMillisToWire(timestamp.value));
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

template<WrapContext InContainer, bool kCopyDelim = false>
struct TimestampTextCore {
  using Value = duckdb::timestamp_t;
  using CopyDelimEscaped = TimestampTextCore<InContainer, true>;
  static constexpr absl::CharSet kCopyDelims = kTimestampCopyDelims;
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx,
                                      Value timestamp) {
    char buf[kMaxTimestampText];
    if (const auto size = FormatTimestamp(buf, timestamp.value)) {
      WithWrapIfNested<InContainer>(ctx, true, [&] {
        WriteText<kCopyDelim>(ctx, std::string_view{buf, size});
      });
      return;
    }
    auto str = duckdb::Timestamp::ToString(timestamp);
    WithWrapIfNested<InContainer>(ctx, timestamp.IsFinite(),
                                  [&] { WriteText<kCopyDelim>(ctx, str); });
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

template<WrapContext InContainer, bool kCopyDelim = false>
struct TimestampNsTextCore {
  using Value = duckdb::timestamp_ns_t;
  using CopyDelimEscaped = TimestampNsTextCore<InContainer, true>;
  static constexpr absl::CharSet kCopyDelims = kTimestampCopyDelims;
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx,
                                      Value timestamp) {
    duckdb::StringHeap heap{duckdb::Allocator::DefaultAllocator()};
    const auto str = duckdb::StringCast::Operation(timestamp, heap);
    WithWrapIfNested<InContainer>(ctx, timestamp.IsFinite(), [&] {
      WriteText<kCopyDelim>(ctx,
                            std::string_view{str.GetData(), str.GetSize()});
    });
  }
};

struct TimestampNsBinCore {
  using Value = duckdb::timestamp_ns_t;
  static constexpr uint32_t kMaxBytes = 8;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value timestamp) {
    absl::big_endian::Store64(dst, TimestampNanosToWire(timestamp.value));
    return 8;
  }
};

inline constexpr size_t kTzOffsetText = 9;

// PG EncodeTimezone shape: ±HH[:MM[:SS]], parts only when nonzero.
size_t RenderTzOffset(char* buf, int32_t offset_secs) {
  const bool negative = offset_secs < 0;
  const auto abs_offset = negative ? -offset_secs : offset_secs;
  const auto offset_h = abs_offset / 3600;
  const auto offset_m = (abs_offset % 3600) / 60;
  const auto offset_s = abs_offset % 60;
  const size_t len = offset_s != 0 ? 9 : (offset_m != 0 ? 6 : 3);
  *buf++ = negative ? '-' : '+';
  *buf++ = '0' + offset_h / 10;
  *buf++ = '0' + offset_h % 10;
  if (len > 3) {
    *buf++ = ':';
    *buf++ = '0' + offset_m / 10;
    *buf++ = '0' + offset_m % 10;
    if (len > 6) {
      *buf++ = ':';
      *buf++ = '0' + offset_s / 10;
      *buf++ = '0' + offset_s % 10;
    }
  }
  return len;
}

template<bool kCopyDelim>
void WriteTzOffsetSuffix(SerializationContext& ctx, int32_t offset_secs) {
  ctx.writer->Write(
    kCopyDelim ? 2 * kTzOffsetText : kTzOffsetText, [&](uint8_t* dst) {
      const auto len =
        RenderTzOffset(reinterpret_cast<char*>(dst), offset_secs);
      if constexpr (kCopyDelim) {
        return EscapeCopyDelimInPlace(dst, len, ctx.copy_delim);
      }
      return len;
    });
}

int32_t SessionTzOffsetSeconds(const duckdb::TimeZone& tz, int64_t utc_ms) {
  int32_t raw_ms = 0;
  int32_t dst_ms = 0;
  tz.GetOffset(utc_ms, raw_ms, dst_ms);
  return (raw_ms + dst_ms) / 1000;
}

int32_t SessionOffsetSeconds(const SerializationContext& ctx, int64_t utc_us) {
  int64_t offset_us = 0;
  if (ctx.zone_lut && ctx.zone_lut->TryOffset(utc_us, offset_us)) {
    return static_cast<int32_t>(offset_us / 1'000'000);
  }
  return SessionTzOffsetSeconds(*ctx.time_zone, utc_us / 1000);
}

inline constexpr absl::CharSet kTimestampTzCopyDelims =
  kTimestampCopyDelims | absl::CharSet{"+"};

template<WrapContext InContainer, bool kCopyDelim = false>
struct TimestampTzTextCore {
  using Value = duckdb::timestamp_tz_t;
  using CopyDelimEscaped = TimestampTzTextCore<InContainer, true>;
  static constexpr absl::CharSet kCopyDelims = kTimestampTzCopyDelims;
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx, Value tz) {
    const auto ts = duckdb::timestamp_t{tz};
    if (ctx.time_zone && ts.IsFinite()) {
      const auto offset_secs = SessionOffsetSeconds(ctx, ts.value);
      int64_t local_us = 0;
      if (duckdb::TryAddOperator::Operation(
            ts.value, int64_t{offset_secs} * 1'000'000, local_us)) {
        char buf[kMaxTimestampText];
        if (const auto size = FormatTimestamp(buf, local_us)) {
          WithWrapIfNested<InContainer>(ctx, true, [&] {
            WriteText<kCopyDelim>(ctx, std::string_view{buf, size});
            WriteTzOffsetSuffix<kCopyDelim>(ctx, offset_secs);
          });
          return;
        }
        auto str = duckdb::Timestamp::ToString(duckdb::timestamp_t{local_us});
        WithWrapIfNested<InContainer>(ctx, true, [&] {
          WriteText<kCopyDelim>(ctx, str);
          WriteTzOffsetSuffix<kCopyDelim>(ctx, offset_secs);
        });
        return;
      }
    }
    char buf[kMaxTimestampText];
    if (const auto size = FormatTimestamp(buf, ts.value)) {
      WithWrapIfNested<InContainer>(ctx, true, [&] {
        WriteText<kCopyDelim>(ctx, std::string_view{buf, size});
        WriteText<kCopyDelim>(ctx, "+00");
      });
      return;
    }
    auto str = duckdb::Timestamp::ToString(ts);
    WithWrapIfNested<InContainer>(ctx, ts.IsFinite(), [&] {
      WriteText<kCopyDelim>(ctx, str);
      if (ts.IsFinite()) {
        WriteText<kCopyDelim>(ctx, "+00");
      }
    });
  }
};

inline constexpr uint32_t kTimestampTextBound = 64;

IRS_FORCE_INLINE size_t RenderTimestampText(char* buf, int64_t micros) {
  if (const auto size = FormatTimestamp(buf, micros)) {
    return size;
  }
  const auto text = duckdb::Timestamp::ToString(duckdb::timestamp_t{micros});
  SDB_ASSERT(text.size() <= kTimestampTextBound);
  std::memcpy(buf, text.data(), text.size());
  return text.size();
}

struct TimestampTextSized {
  using Value = duckdb::timestamp_t;
  static constexpr uint32_t kMaxBytes = kTimestampTextBound;
  static constexpr absl::CharSet kCopyDelims = kTimestampCopyDelims;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value timestamp) {
    return RenderTimestampText(reinterpret_cast<char*>(dst), timestamp.value);
  }
};

struct TimestampTzTextSized {
  using Value = duckdb::timestamp_tz_t;
  static constexpr uint32_t kMaxBytes = kTimestampTextBound + kTzOffsetText;
  static constexpr absl::CharSet kCopyDelims = kTimestampTzCopyDelims;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, SerializationContext& ctx,
                                        Value tz) {
    auto* buf = reinterpret_cast<char*>(dst);
    const auto ts = duckdb::timestamp_t{tz};
    if (ctx.time_zone && ts.IsFinite()) {
      const auto offset_secs = SessionOffsetSeconds(ctx, ts.value);
      int64_t local_us = 0;
      if (duckdb::TryAddOperator::Operation(
            ts.value, int64_t{offset_secs} * 1'000'000, local_us)) {
        const auto size = RenderTimestampText(buf, local_us);
        return size + RenderTzOffset(buf + size, offset_secs);
      }
    }
    const auto size = RenderTimestampText(buf, ts.value);
    if (!ts.IsFinite()) {
      return size;
    }
    std::memcpy(buf + size, "+00", 3);
    return size + 3;
  }
};

struct TimestampTzBinCore {
  using Value = duckdb::timestamp_tz_t;
  static constexpr uint32_t kMaxBytes = 8;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value tz) {
    absl::big_endian::Store64(dst, TimestampMicrosToWire(tz.value));
    return 8;
  }
};

template<WrapContext InContainer, bool kCopyDelim = false>
struct TimestampTzNsTextCore {
  using Value = duckdb::timestamp_tz_ns_t;
  using CopyDelimEscaped = TimestampTzNsTextCore<InContainer, true>;
  static constexpr absl::CharSet kCopyDelims = kTimestampTzCopyDelims;
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx, Value tz) {
    duckdb::StringHeap heap{duckdb::Allocator::DefaultAllocator()};
    if (ctx.time_zone && tz.IsFinite()) {
      const auto offset_secs = SessionOffsetSeconds(ctx, tz.value / 1'000);
      int64_t local_ns = 0;
      if (duckdb::TryAddOperator::Operation(
            tz.value, int64_t{offset_secs} * 1'000'000'000, local_ns)) {
        const auto str =
          duckdb::StringCast::Operation(duckdb::timestamp_ns_t{local_ns}, heap);
        WithWrapIfNested<InContainer>(ctx, true, [&] {
          WriteText<kCopyDelim>(ctx,
                                std::string_view{str.GetData(), str.GetSize()});
          WriteTzOffsetSuffix<kCopyDelim>(ctx, offset_secs);
        });
        return;
      }
    }
    const auto str = duckdb::StringCast::Operation(tz, heap);
    WithWrapIfNested<InContainer>(ctx, tz.IsFinite(), [&] {
      WriteText<kCopyDelim>(ctx,
                            std::string_view{str.GetData(), str.GetSize()});
    });
  }
};

struct TimestampTzNsBinCore {
  using Value = duckdb::timestamp_tz_ns_t;
  static constexpr uint32_t kMaxBytes = 8;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value tz) {
    absl::big_endian::Store64(dst, TimestampNanosToWire(tz.value));
    return 8;
  }
};

template<bool kCopyDelim = false>
struct TimeTextCore {
  using Value = duckdb::dtime_t;
  using CopyDelimEscaped = TimeTextCore<true>;
  static constexpr absl::CharSet kCopyDelims{":"};
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx, Value time) {
    WriteText<kCopyDelim>(ctx, duckdb::Time::ToString(time));
  }
};

struct TimeBinCore {
  using Value = duckdb::dtime_t;
  static constexpr uint32_t kMaxBytes = 8;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value time) {
    absl::big_endian::Store64(dst, time.value);
    return 8;
  }
};

template<bool kCopyDelim = false>
struct TimeNsTextCore {
  using Value = duckdb::dtime_ns_t;
  using CopyDelimEscaped = TimeNsTextCore<true>;
  static constexpr absl::CharSet kCopyDelims{":"};
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx, Value time) {
    duckdb::StringHeap heap{duckdb::Allocator::DefaultAllocator()};
    const auto str = duckdb::StringCast::Operation(time, heap);
    WriteText<kCopyDelim>(ctx, std::string_view{str.GetData(), str.GetSize()});
  }
};

struct TimeNsBinCore {
  using Value = duckdb::dtime_ns_t;
  static constexpr uint32_t kMaxBytes = 8;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value time) {
    absl::big_endian::Store64(dst, time.value / (duckdb::dtime_ns_t::PRECISION /
                                                 duckdb::dtime_t::PRECISION));
    return 8;
  }
};

template<bool kCopyDelim = false>
struct TimeTzTextCore {
  using Value = duckdb::dtime_tz_t;
  using CopyDelimEscaped = TimeTzTextCore<true>;
  static constexpr absl::CharSet kCopyDelims{":+-"};
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx, Value tz) {
    WriteText<kCopyDelim>(ctx, duckdb::Time::ToString(tz.time()));
    WriteTzOffsetSuffix<kCopyDelim>(ctx, tz.offset());
  }
};

struct TimeTzBinCore {
  using Value = duckdb::dtime_tz_t;
  static constexpr uint32_t kMaxBytes = 12;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value tz) {
    // PG binary: int64 time_micros + int32 zone (seconds WEST of UTC).
    // DuckDB offset() is seconds EAST, so negate.
    absl::big_endian::Store64(dst, tz.time().value);
    absl::big_endian::Store32(dst + 8, -tz.offset());
    return 12;
  }
};

template<WrapContext InContainer>
struct BitTextCore {
  using Value = duckdb::string_t;
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx, Value raw) {
    const auto n_bits = duckdb::Bit::BitLength(raw);
    if constexpr (InContainer != WrapContext::None) {
      if (n_bits == 0) {
        WriteWrapped<InContainer>(ctx, [] {});
        return;
      }
    }
    duckdb::Bit::ToString(raw,
                          reinterpret_cast<char*>(ctx.writer->Alloc(n_bits)));
  }
};

struct BitBinCore {
  using Value = duckdb::string_t;
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx, Value raw) {
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
  static constexpr uint32_t kMaxBytes = sizeof("-5877641-06-25") - 1;
  static constexpr absl::CharSet kCopyDelims{"-"};
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value date) {
    // days from 1970-01-01
    char* buf = reinterpret_cast<char*>(dst);
    if (date.days == std::numeric_limits<int32_t>::max()) {
      std::memcpy(buf, "infinity", 8);
      return 8;
    }
    if (date.days == -std::numeric_limits<int32_t>::max()) {
      std::memcpy(buf, "-infinity", 9);
      return 9;
    }
    if (date.days >= kMinFastDays && date.days <= kMaxFastDays) {
      return FormatDate(buf, date.days);
    }
    // TODO(mkornaukhov) support BC date and add some validation for dates
    // Format is "%04d-%02d-%02d"
    absl::CivilDay days{1970, 1, 1};
    days += date.days;

    char year_buf[absl::numbers_internal::kFastToBufferSize];
    auto* const year_end =
      absl::numbers_internal::FastIntToBuffer(days.year(), year_buf);

    const auto year_digits = year_end - year_buf;
    const auto extra_pad = 4 - year_digits;
    if (extra_pad > 0) {
      std::memset(buf, '0', extra_pad);
      buf += extra_pad;
    }

    std::memcpy(buf, year_buf, year_digits);
    buf += year_digits;
    *buf++ = '-';

    const auto month_div = std::div(days.month(), 10);
    *buf++ = '0' + month_div.quot;
    *buf++ = '0' + month_div.rem;
    *buf++ = '-';

    const auto day_div = std::div(days.day(), 10);
    *buf++ = '0' + day_div.quot;
    *buf++ = '0' + day_div.rem;

    return static_cast<size_t>(buf - reinterpret_cast<char*>(dst));
  }
};

struct DateBinCore {
  using Value = duckdb::date_t;
  static constexpr uint32_t kMaxBytes = 4;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value date) {
    absl::big_endian::Store32(dst, DateDaysToWire(date.days));
    return 4;
  }
};

template<RegKind Kind, WrapContext InContainer>
struct RegTextCore {
  using Value = int64_t;
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx, Value oid) {
    EmitTextItem<InContainer>(
      ctx, RegOut(*ctx.caches->session, Kind, static_cast<uint64_t>(oid)));
  }
};

template<WrapContext InContainer>
struct IntervalTextCore {
  using Value = duckdb::interval_t;
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx,
                                      Value interval) {
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
  static constexpr absl::CharSet kCopyDelims{"-"};
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value uuid) {
    duckdb::BaseUUID::ToString(uuid, reinterpret_cast<char*>(dst));
    return 36;
  }
};

struct UuidBinCore {
  using Value = duckdb::hugeint_t;
  static constexpr uint32_t kMaxBytes = 16;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, Value uuid) {
    // Binary format: flip top bit back to get original UUID bytes
    const uint64_t high =
      static_cast<uint64_t>(uuid.upper) ^ (uint64_t{1} << 63);
    absl::big_endian::Store64(dst, high);
    absl::big_endian::Store64(dst + 8, uuid.lower);
    return 16;
  }
};

template<WrapContext InContainer>
using JsonTextCore = VarcharTextCore<InContainer>;

struct JsonBinCore {
  using Value = duckdb::string_t;
  IRS_FORCE_INLINE static void Render(SerializationContext& ctx, Value raw) {
    ctx.writer->Write(std::string_view{raw.GetData(), raw.GetSize()});
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

bool RecordHasArrayTrigger(SerializationContext& ctx,
                           const duckdb::LogicalType& type,
                           const duckdb::RecursiveUnifiedVectorFormat& vdata,
                           duckdb::idx_t row);

std::string RenderTsqueryRow(const duckdb::RecursiveUnifiedVectorFormat& vdata,
                             duckdb::idx_t row);

template<WrapContext InContainer>
bool TextNeedsQuotingIn(std::string_view text) {
  if constexpr (InContainer == WrapContext::Array) {
    return RecordItemNeedsQuoting(text) ||
           text.find_first_of("{}") != std::string_view::npos;
  } else {
    return ArrayItemNeedQuotesAndEscape(text) ||
           text.find_first_of("()") != std::string_view::npos;
  }
}

template<WrapContext InContainer>
bool NeedsQuotingIn(SerializationContext& ctx, const duckdb::LogicalType& type,
                    const duckdb::RecursiveUnifiedVectorFormat& vdata,
                    duckdb::idx_t row) {
  static_assert(InContainer == WrapContext::Array ||
                InContainer == WrapContext::Record);
  const auto idx = vdata.unified.sel->get_index(row);
  if (!vdata.unified.validity.RowIsValid(idx)) {
    return false;
  }
  using enum duckdb::LogicalTypeId;
  switch (type.id()) {
    case BIGINT:
      if (const auto kind = RegKindOf(type)) {
        const auto text =
          RegOut(*ctx.caches->session, *kind,
                 static_cast<uint64_t>(
                   duckdb::UnifiedVectorFormat::GetDataUnsafe<int64_t>(
                     vdata.unified)[idx]));
        return TextNeedsQuotingIn<InContainer>(text);
      }
      return IsTid(type);
    case DECIMAL:
      return IsMoney(type) &&
             MoneyTextCore::NeedsQuoting(
               duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::hugeint_t>(
                 vdata.unified)[idx]);
    case DATE:
    case BOOLEAN:
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case UTINYINT:
    case USMALLINT:
    case UINTEGER:
    case UBIGINT:
    case HUGEINT:
    case UHUGEINT:
    case FLOAT:
    case DOUBLE:
    case TIME:
    case TIME_NS:
    case TIME_TZ:
    case UUID:
    case GEOMETRY:
      return false;
    case BIT:
      return duckdb::Bit::BitLength(
               duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::string_t>(
                 vdata.unified)[idx]) == 0;
    case TIMESTAMP_SEC:
    case TIMESTAMP_MS:
    case TIMESTAMP:
    case TIMESTAMP_NS:
    case TIMESTAMP_TZ:
    case TIMESTAMP_TZ_NS:
      return duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::timestamp_t>(
               vdata.unified)[idx]
        .IsFinite();
    case STRUCT:
      if (IsInetOrCidr(type)) {
        return false;
      }
      if (IsRange(type)) {
        return !(ReadRangeFlags(vdata, idx) & kRangeEmpty);
      }
      if (connector::IsTSQueryStructType(type)) {
        return TextNeedsQuotingIn<InContainer>(RenderTsqueryRow(vdata, idx));
      }
      return true;
    case BLOB: {
      if (ctx.bytea_output == ByteaOutput::Hex) {
        return true;
      }
      const auto& raw =
        duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::string_t>(
          vdata.unified)[idx];
      const std::string_view v{raw.GetData(), raw.GetSize()};
      return absl::c_any_of(v,
                            [](char c) { return !absl::ascii_isprint(c); }) ||
             TextNeedsQuotingIn<InContainer>(v);
    }
    case TUPLE:
      return true;
    case INTERVAL: {
      const auto interval =
        duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::interval_t>(
          vdata.unified)[idx];
      return TextNeedsQuotingIn<InContainer>(
        duckdb::Interval::ToString(interval));
    }
    case CHAR:
    case VARCHAR: {
      const auto& raw =
        duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::string_t>(
          vdata.unified)[idx];
      std::string_view v{raw.GetData(), raw.GetSize()};
      if (IsXml(type)) {
        const auto xml = XmlOut(v);
        if (!xml.declaration.empty()) {
          return true;
        }
        v = xml.body;
      }
      return TextNeedsQuotingIn<InContainer>(v);
    }
    case ENUM: {
      const auto label = EnumLabel(type, vdata.unified, idx);
      return TextNeedsQuotingIn<InContainer>(
        std::string_view{label.GetData(), label.GetSize()});
    }
    case ARRAY:
    case LIST:
    case MAP:
      if (IsInt2vector(type) || IsOidvector(type)) {
        return GetArraySlice(vdata, row).size != 1;
      }
      SDB_ASSERT(InContainer == WrapContext::Array || IsMultirange(type));
      return true;
    default:
      return true;
  }
}

bool RecordHasArrayTrigger(SerializationContext& ctx,
                           const duckdb::LogicalType& type,
                           const duckdb::RecursiveUnifiedVectorFormat& vdata,
                           duckdb::idx_t row) {
  const auto& children = duckdb::StructType::GetChildTypes(type);
  if (children.size() != 1) {
    // 0 fields -> '()' (no trigger). N>1 -> ',' is always present.
    return children.size() > 1;
  }
  return NeedsQuotingIn<WrapContext::Array>(ctx, children[0].second,
                                            vdata.children[0], row);
}

ArrayWrap ArrayWrapOf(
  const duckdb::child_list_t<duckdb::LogicalType>& children) {
  if (children.size() != 1) {
    return children.empty() ? ArrayWrap::Never : ArrayWrap::Always;
  }
  const auto& type = children[0].second;
  switch (type.id()) {
    using enum duckdb::LogicalTypeId;
    case BIGINT:
      return IsTid(type) || RegKindOf(type) ? ArrayWrap::ByValue
                                            : ArrayWrap::Never;
    case DECIMAL:
      return IsMoney(type) ? ArrayWrap::ByValue : ArrayWrap::Never;
    case DATE:
    case BOOLEAN:
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case UTINYINT:
    case USMALLINT:
    case UINTEGER:
    case UBIGINT:
    case HUGEINT:
    case UHUGEINT:
    case FLOAT:
    case DOUBLE:
    case TIME:
    case TIME_NS:
    case TIME_TZ:
    case UUID:
    case GEOMETRY:
      return ArrayWrap::Never;
    default:
      return ArrayWrap::ByValue;
  }
}

template<VarFormat Format>
IRS_NO_INLINE void FillSerializers(SerializationContext& context,
                                   const duckdb::LogicalType& struct_type,
                                   RecordSerializers& cached) {
  const auto& children = duckdb::StructType::GetChildTypes(struct_type);
  cached.functions.reserve(children.size());
  if constexpr (Format == VarFormat::Text) {
    const bool saved = std::exchange(context.in_record, true);
    for (const auto& [_, child_type] : children) {
      cached.functions.push_back(
        GetSerialization(child_type, VarFormat::Text, context));
    }
    context.in_record = saved;
    cached.array_wrap = ArrayWrapOf(children);
  } else {
    cached.oids.reserve(children.size());
    for (const auto& [_, child_type] : children) {
      cached.functions.push_back(
        GetSerialization(child_type, VarFormat::Binary, context));
      cached.oids.emplace_back(Type2Oid(child_type));
    }
  }
}

IRS_FORCE_INLINE SerializationCaches& Caches(SerializationContext& context) {
  if (!context.caches) {
    context.caches = std::make_unique<SerializationCaches>();
  }
  return *context.caches;
}

template<VarFormat Format>
const RecordSerializers& GetSerializersCache(
  SerializationContext& context, const duckdb::LogicalType& struct_type) {
  auto [it, inserted] = Caches(context).types.try_emplace(&struct_type);
  if (inserted) {
    FillSerializers<Format>(context, struct_type, it->second);
  }
  return it->second;
}

template<WrapContext InContainer, bool kCopyDelim = false>
struct RecordTextCore {
  using CopyDelimEscaped = RecordTextCore<InContainer, true>;
  static constexpr absl::CharSet kCopyDelims{"(,)"};
  static void Render(SerializationContext& context,
                     const duckdb::RecursiveUnifiedVectorFormat& vdata,
                     duckdb::idx_t row) {
    const auto& cache =
      GetSerializersCache<VarFormat::Text>(context, vdata.logical_type);
    auto emit_inside = [&] {
      WriteText<kCopyDelim>(context, "(");
      for (size_t i = 0; i < cache.functions.size(); ++i) {
        if (i > 0) {
          WriteText<kCopyDelim>(context, ",");
        }
        const auto& child = vdata.children[i];
        // empty between commas means NULL in record text; the cached in-record
        // text serializer renders nothing on a NULL field.
        cache.functions[i](context, child, row);
      }
      WriteText<kCopyDelim>(context, ")");
    };

    if constexpr (InContainer == WrapContext::Array) {
      SDB_ASSERT(
        cache.array_wrap == ArrayWrap::ByValue ||
        RecordHasArrayTrigger(context, vdata.logical_type, vdata, row) ==
          (cache.array_wrap == ArrayWrap::Always));
      if (cache.array_wrap == ArrayWrap::Always ||
          (cache.array_wrap == ArrayWrap::ByValue &&
           RecordHasArrayTrigger(context, vdata.logical_type, vdata, row))) {
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
      absl::big_endian::Store32(context.writer->Alloc(4),
                                WireOid(cache.oids[i]));
      cache.functions[i](context, vdata.children[i], row);
    }
  }
};

template<WrapContext InContainer, bool kCopyDelim = false>
struct RangeTextCore {
  using CopyDelimEscaped = RangeTextCore<InContainer, true>;
  static constexpr absl::CharSet kCopyDelims{"[(,)]"};
  static void Render(SerializationContext& context,
                     const duckdb::RecursiveUnifiedVectorFormat& vdata,
                     duckdb::idx_t row) {
    const auto flags = ReadRangeFlags(vdata, row);
    if (flags & kRangeEmpty) {
      context.writer->Write("empty");
      return;
    }
    const auto& cache =
      GetSerializersCache<VarFormat::Text>(context, vdata.logical_type);
    const auto bound = [&](duckdb::idx_t child, uint8_t infinite) {
      if (!(flags & infinite)) {
        cache.functions[child](context, vdata.children[child], row);
      }
    };
    WithWrapIfNested<InContainer>(context, true, [&] {
      WriteText<kCopyDelim>(context, flags & kRangeLowerInclusive ? "[" : "(");
      bound(kRangeLowerChild, kRangeLowerInfinite);
      WriteText<kCopyDelim>(context, ",");
      bound(kRangeUpperChild, kRangeUpperInfinite);
      WriteText<kCopyDelim>(context, flags & kRangeUpperInclusive ? "]" : ")");
    });
  }
};

struct RangeBinCore {
  static void Render(SerializationContext& context,
                     const duckdb::RecursiveUnifiedVectorFormat& vdata,
                     duckdb::idx_t row) {
    const auto flags = ReadRangeFlags(vdata, row);
    *context.writer->Alloc(1) = flags;
    const auto& cache =
      GetSerializersCache<VarFormat::Binary>(context, vdata.logical_type);
    const auto bound = [&](duckdb::idx_t child, uint8_t infinite) {
      if (!(flags & (kRangeEmpty | infinite))) {
        cache.functions[child](context, vdata.children[child], row);
      }
    };
    bound(kRangeLowerChild, kRangeLowerInfinite);
    bound(kRangeUpperChild, kRangeUpperInfinite);
  }
};

template<WrapContext InContainer, bool kCopyDelim = false>
struct MultirangeTextCore {
  using CopyDelimEscaped = MultirangeTextCore<InContainer, true>;
  static constexpr absl::CharSet kCopyDelims =
    absl::CharSet{"{,}"} | RangeTextCore<WrapContext::None>::kCopyDelims;
  static void Render(SerializationContext& context,
                     const duckdb::RecursiveUnifiedVectorFormat& vdata,
                     duckdb::idx_t row) {
    const auto [size, offset] = GetSliceResolved(vdata, row);
    const auto& ranges = vdata.children[0];
    const auto emit = [&] {
      WriteText<kCopyDelim>(context, "{");
      for (duckdb::idx_t i = 0; i < size; ++i) {
        if (i != 0) {
          WriteText<kCopyDelim>(context, ",");
        }
        RangeTextCore<WrapContext::None, kCopyDelim>::Render(
          context, ranges, ranges.unified.sel->get_index(offset + i));
      }
      WriteText<kCopyDelim>(context, "}");
    };
    if constexpr (InContainer == WrapContext::Record) {
      if (size == 0) {
        emit();
        return;
      }
    }
    WithWrapIfNested<InContainer>(context, true, emit);
  }
};

struct MultirangeBinCore {
  static void Render(SerializationContext& context,
                     const duckdb::RecursiveUnifiedVectorFormat& vdata,
                     duckdb::idx_t row) {
    const auto [size, offset] = GetSliceResolved(vdata, row);
    absl::big_endian::Store32(context.writer->Alloc(4),
                              static_cast<int32_t>(size));
    const auto& ranges = vdata.children[0];
    for (duckdb::idx_t i = 0; i < size; ++i) {
      auto* length = context.writer->Alloc(4);
      const auto before = context.writer->Written();
      RangeBinCore::Render(context, ranges,
                           ranges.unified.sel->get_index(offset + i));
      absl::big_endian::Store32(
        length, static_cast<int32_t>(context.writer->Written() - before));
    }
  }
};

template<Geometric kKind, WrapContext InContainer, bool kCopyDelim = false>
struct GeometricTextCore {
  using CopyDelimEscaped = GeometricTextCore<kKind, InContainer, true>;
  static constexpr const char* kArrayDelim =
    kKind == Geometric::Box ? ";" : ",";
  static constexpr absl::CharSet kCopyDelims{"()<>[]{},-+IN"};
  static void Render(SerializationContext& context,
                     const duckdb::RecursiveUnifiedVectorFormat& vdata,
                     duckdb::idx_t row) {
    const auto write = [&] {
      WriteGeometric(
        kKind, vdata, row, context.extra_float_digits,
        [&](std::string_view text) { WriteText<kCopyDelim>(context, text); });
    };
    if constexpr (kKind == Geometric::Box &&
                  InContainer == WrapContext::Array) {
      write();
    } else {
      WithWrapIfNested<InContainer>(context, true, write);
    }
  }
};

template<Geometric kKind>
struct GeometricBinCore {
  static void Render(SerializationContext& context,
                     const duckdb::RecursiveUnifiedVectorFormat& vdata,
                     duckdb::idx_t row) {
    SendGeometric(kKind, vdata, row,
                  [&](size_t size) { return context.writer->Alloc(size); });
  }
};

// PG `inet` (OID 869). STRUCT entries: [0]=ip_type (1=IPv4, 2=IPv6),
// [1]=address (signed hugeint, IPv6 top bit flipped for sort order), [2]=mask.
IRS_FORCE_INLINE void ReadInet(
  const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t row,
  INET_IPAddress& inet) {
  const auto& c0 = vdata.children[kInetIpTypeChild].unified;
  const auto& c1 = vdata.children[kInetAddressChild].unified;
  const auto& c2 = vdata.children[kInetMaskChild].unified;
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
    inet.address.upper ^= kInetV6SortBit;
  }
  inet.mask = mask;
}

template<bool Cidr>
struct InetTextCore {
  static constexpr uint32_t kMaxBytes = 64;
  static constexpr absl::CharSet kCopyDelims{":/"};
  IRS_FORCE_INLINE static size_t Render(
    uint8_t* dst, SerializationContext&,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t row) {
    INET_IPAddress inet;
    ReadInet(vdata, row, inet);
    // Reuse the extension's formatter (IPv6 canonicalization + "/mask" rules)
    // so output matches host()/::varchar.
    auto* out = reinterpret_cast<char*>(dst);
    return Cidr ? cidr_to_string(&inet, out, kMaxBytes)
                : ipaddress_to_string(&inet, out, kMaxBytes);
  }
};

std::string RenderTsqueryRow(const duckdb::RecursiveUnifiedVectorFormat& vdata,
                             duckdb::idx_t row) {
  const auto read_string = [&](duckdb::idx_t child) -> std::string {
    const auto& unified = vdata.children[child].unified;
    const auto idx = unified.sel->get_index(row);
    if (!unified.validity.RowIsValid(idx)) {
      return {};
    }
    const auto& raw =
      duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(unified)[idx];
    return std::string{raw.GetData(), raw.GetSize()};
  };
  const auto read_flat = [&]<typename T>(duckdb::idx_t child, T fallback) {
    const auto& unified = vdata.children[child].unified;
    const auto idx = unified.sel->get_index(row);
    if (!unified.validity.RowIsValid(idx)) {
      return fallback;
    }
    return duckdb::UnifiedVectorFormat::GetData<T>(unified)[idx];
  };
  connector::TSQueryParts parts;
  parts.text = read_string(connector::kTSQueryTextChild);
  parts.tokenizer = read_string(connector::kTSQueryTokenizerChild);
  parts.scorer = read_string(connector::kTSQueryScorerChild);
  parts.boost = read_flat(connector::kTSQueryBoostChild, parts.boost);
  parts.slop = read_flat(connector::kTSQuerySlopChild, parts.slop);
  parts.merge = static_cast<connector::TSQueryMerge>(read_flat(
    connector::kTSQueryMergeChild, static_cast<uint8_t>(parts.merge)));
  parts.min_match =
    read_flat(connector::kTSQueryMinMatchChild, parts.min_match);
  return connector::RenderTSQueryValueText(parts);
}

template<WrapContext InContainer>
struct TsqueryTextCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    EmitTextItem<InContainer>(ctx, RenderTsqueryRow(vdata, idx));
  }
};

struct TsqueryBinCore {
  IRS_FORCE_INLINE static void Render(
    SerializationContext& ctx,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t idx) {
    ctx.writer->Write(RenderTsqueryRow(vdata, idx));
  }
};

template<bool Cidr>
struct InetBinCore {
  static constexpr uint32_t kMaxBytes =
    sizeof(InetWireHeader) + kInetWireV6Size;
  IRS_FORCE_INLINE static size_t Render(
    uint8_t* dst, SerializationContext&,
    const duckdb::RecursiveUnifiedVectorFormat& vdata, duckdb::idx_t row) {
    INET_IPAddress inet;
    ReadInet(vdata, row, inet);
    const bool is_v6 = inet.type == INET_IP_ADDRESS_V6;
    const InetWireHeader header{
      .family = is_v6 ? kPgsqlAfInet6 : kPgsqlAfInet,
      .bits = static_cast<uint8_t>(inet.mask),
      .is_cidr = Cidr,
      .size = is_v6 ? kInetWireV6Size : kInetWireV4Size,
    };
    std::memcpy(dst, &header, sizeof(header));
    auto* address = dst + sizeof(header);
    if (is_v6) {
      absl::big_endian::Store64(address, inet.address.upper);
      absl::big_endian::Store64(address + sizeof(uint64_t), inet.address.lower);
    } else {
      absl::big_endian::Store32(address,
                                static_cast<uint32_t>(inet.address.lower));
    }
    return sizeof(header) + header.size;
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

template<typename Core>
consteval absl::CharSet CopyDelimsOf() {
  if constexpr (requires { Core::kCopyDelims; }) {
    return Core::kCopyDelims;
  } else {
    return {};
  }
}

template<typename Core>
struct CopyDelimSized {
  using Plain = Core;
  static constexpr uint32_t kMaxBytes = 2 * Core::kMaxBytes;
  IRS_FORCE_INLINE static size_t Render(uint8_t* dst, SerializationContext& ctx,
                                        const RUVF& v, duckdb::idx_t idx) {
    return EscapeCopyDelimInPlace(dst, RenderSized<Core>(dst, ctx, v, idx),
                                  ctx.copy_delim);
  }
};

template<typename Core>
struct CopyDelimVariant {
  using Type = Core;
};

template<typename Core>
  requires requires { typename Core::CopyDelimEscaped; }
struct CopyDelimVariant<Core> {
  using Type = typename Core::CopyDelimEscaped;
};

template<typename Core>
  requires(SizedCore<Core> && !CopyDelimsOf<Core>().empty())
struct CopyDelimVariant<Core> {
  using Type = CopyDelimSized<Core>;
};

template<typename Core>
using CopyDelimVariantOf = typename CopyDelimVariant<Core>::Type;

template<typename Core>
IRS_FORCE_INLINE bool EscapesCopyDelim(const SerializationContext& context) {
  static_assert(CopyDelimsOf<Core>().empty() ||
                !std::is_same_v<CopyDelimVariantOf<Core>, Core>);
  return context.copy_text && CopyDelimsOf<Core>().contains(context.copy_delim);
}

template<typename C>
concept QuotedCore =
  requires(const typename C::Value& value) { C::NeedsQuoting(value); };

template<typename Core>
IRS_FORCE_INLINE bool ItemNeedsQuoting(const RUVF& v, duckdb::idx_t idx) {
  if constexpr (requires { typename Core::Plain; }) {
    return ItemNeedsQuoting<typename Core::Plain>(v, idx);
  } else if constexpr (QuotedCore<Core>) {
    return Core::NeedsQuoting(ReadValue<typename Core::Value>(v, idx));
  } else {
    return false;
  }
}

template<typename Core>
IRS_FORCE_INLINE void RenderItem(SerializationContext& ctx, const RUVF& v,
                                 duckdb::idx_t idx) {
  if (ItemNeedsQuoting<Core>(v, idx)) {
    ctx.writer->Write(ctx.quote_seq);
    RenderValue<Core>(ctx, v, idx);
    ctx.writer->Write(ctx.quote_seq);
    return;
  }
  RenderValue<Core>(ctx, v, idx);
}

constexpr absl::CharSet kCopyTextReserved{
  "\\.abcdefghijklmnopqrstuvwxyz0123456789"};

template<typename Core>
bool CsvScanNeeded(const SerializationContext& context) {
  constexpr auto kAlphabet = kCopyTextReserved | CopyDelimsOf<Core>();
  return kAlphabet.contains(context.copy_delim) ||
         kAlphabet.contains(context.csv_quote);
}

bool CsvNeedsQuotes(const SerializationContext& ctx, std::string_view field) {
  return field == ctx.copy_null || (ctx.csv_single_column && field == "\\.") ||
         ctx.csv_specials.In(field);
}

void WriteCsvQuoted(message::Writer& writer, std::string_view field, char quote,
                    char escape) {
  const size_t size =
    field.size() + duckdb::CsvEscapes(field, quote, escape) + 2;
  CheckMessageSize(writer.Written() + size);
  writer.Write(size, [&](uint8_t* out) {
    return duckdb::CsvWriteQuoted(reinterpret_cast<char*>(out), field, quote,
                                  escape);
  });
}

void FinishCsvField(SerializationContext& ctx,
                    const message::Writer::Mark& mark, bool force) {
  auto& writer = *ctx.writer;
  std::string_view field;
  if (writer.Contiguous(mark)) [[likely]] {
    field = writer.Since(mark);
  } else {
    ctx.csv_scratch.clear();
    writer.AppendSince(mark, ctx.csv_scratch);
    field = ctx.csv_scratch;
  }
  if (!force && !CsvNeedsQuotes(ctx, field)) {
    return;
  }
  if (field.data() != ctx.csv_scratch.data()) {
    ctx.csv_scratch.assign(field);
    field = ctx.csv_scratch;
  }
  writer.Truncate(mark);
  WriteCsvQuoted(writer, field, ctx.csv_quote, ctx.csv_escape);
}

enum class Framing {
  BinaryField,
  CopyTextField,
  CsvField,
  CsvPlainField,
  CsvQuotedField,
};

template<Framing F>
constexpr bool kCsvFraming =
  F == Framing::CsvField || F == Framing::CsvPlainField ||
  F == Framing::CsvQuotedField;

template<Framing F, typename Core>
IRS_FORCE_INLINE void RenderCsvValue(SerializationContext& ctx, const RUVF& v,
                                     duckdb::idx_t idx) {
  if constexpr (SizedCore<Core>) {
    ctx.writer->Write(2 * Core::kMaxBytes + 2, [&](uint8_t* d) {
      const auto n = RenderSized<Core>(d, ctx, v, idx);
      const std::string_view field{reinterpret_cast<const char*>(d), n};
      bool quote = true;
      if constexpr (F == Framing::CsvPlainField) {
        quote = field == ctx.copy_null;
      } else if constexpr (F == Framing::CsvField) {
        quote = CsvNeedsQuotes(ctx, field);
      }
      return quote ? duckdb::CsvQuoteInPlace(reinterpret_cast<char*>(d), n,
                                             ctx.csv_quote, ctx.csv_escape)
                   : n;
    });
  } else {
    const auto mark = ctx.writer->Position();
    RenderStreamed<Core>(ctx, v, idx);
    FinishCsvField(ctx, mark, F == Framing::CsvQuotedField);
  }
}

template<Framing F, typename Core>
bool SerializeField(SerializationContext& ctx, const RUVF& v,
                    duckdb::idx_t row) {
  const auto idx = v.unified.sel->get_index(row);
  const bool valid = v.unified.validity.RowIsValid(idx);
  if constexpr (F == Framing::CopyTextField || kCsvFraming<F>) {
    if (!valid) {
      ctx.writer->Write(ctx.copy_null);
      return true;
    }
    if constexpr (kCsvFraming<F>) {
      RenderCsvValue<F, Core>(ctx, v, idx);
    } else {
      RenderValue<Core>(ctx, v, idx);
    }
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

bool SerializeCopyTextNullColumn(SerializationContext& ctx, const RUVF&,
                                 duckdb::idx_t) {
  ctx.writer->Write(ctx.copy_null);
  return true;
}

bool SerializeRecordNullColumn(SerializationContext&, const RUVF&,
                               duckdb::idx_t) {
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
  RenderItem<Core>(ctx, v, idx);
  return false;
}

// Emit the element run of a one-dimensional array (no surrounding braces /
// binary header -- the array core writes those). `off`/`count` come from the
// resolved array slice. Returns whether any element was NULL.
template<typename Core, typename = void>
struct ArrayDelim {
  static constexpr const char* kValue = ",";
};
template<typename Core>
struct ArrayDelim<Core, std::void_t<decltype(Core::kArrayDelim)>> {
  static constexpr const char* kValue = Core::kArrayDelim;
};

template<typename Core, VarFormat Format, bool kCopyDelim = false>
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
  } else if constexpr (SizedCore<Core> && !kCopyDelim) {
    constexpr size_t kElem = (Core::kMaxBytes > 4 ? Core::kMaxBytes : 4) + 1;
    const auto quotes = QuotedCore<Core> ? 2 * ctx.quote_seq.size() : 0;
    ctx.writer->Write(count * (kElem + quotes), [&](uint8_t* out) {
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
        } else if (ItemNeedsQuoting<Core>(cv, idx)) {
          std::memcpy(p, ctx.quote_seq.data(), ctx.quote_seq.size());
          p += ctx.quote_seq.size();
          p += RenderSized<Core>(p, ctx, cv, idx);
          std::memcpy(p, ctx.quote_seq.data(), ctx.quote_seq.size());
          p += ctx.quote_seq.size();
        } else {
          p += RenderSized<Core>(p, ctx, cv, idx);
        }
      }
      return static_cast<size_t>(p - out);
    });
  } else {
    using Elem = std::conditional_t<kCopyDelim, CopyDelimVariantOf<Core>, Core>;
    for (duckdb::idx_t i = 0; i < count; ++i) {
      if (i > 0) {
        WriteText<kCopyDelim>(ctx, std::string_view{ArrayDelim<Core>::kValue});
      }
      const auto idx = cv.unified.sel->get_index(off + i);
      if (!cv.unified.validity.RowIsValid(idx)) {
        WriteText<kCopyDelim>(ctx, "NULL");
        has_null = true;
      } else {
        RenderItem<Elem>(ctx, cv, idx);
      }
    }
  }
  return has_null;
}

inline constexpr absl::CharSet kArrayCopyDelims{"{,}NUL"};

template<typename Core>
bool ArrayNeedsQuotingInRecord(SerializationContext& ctx, const RUVF& vdata,
                               duckdb::idx_t row);

template<typename Core, uint64_t ElementOID, VarFormat Format,
         WrapContext InContainer, bool kCopyDelim = false>
struct OneDimArrayCore {
  using CopyDelimEscaped =
    OneDimArrayCore<Core, ElementOID, Format, InContainer, true>;
  static constexpr absl::CharSet kCopyDelims =
    kArrayCopyDelims | absl::CharSet{ArrayDelim<Core>::kValue} |
    CopyDelimsOf<Core>();
  IRS_FORCE_INLINE static void Render(SerializationContext& context,
                                      const RUVF& vdata, duckdb::idx_t row) {
    // `row` is the source row already resolved by SerializeField; resolving
    // again would double-apply the selection (wrong for CONSTANT/DICTIONARY
    // arrays).
    auto [array_size, array_offset] = GetSliceResolved(vdata, row);
    auto& child_vdata = vdata.children[0];
    if constexpr (Format == VarFormat::Text) {
      auto emit_inside = [&] {
        WriteText<kCopyDelim>(context, "{");
        EmitArrayElems<Core, VarFormat::Text, kCopyDelim>(
          context, child_vdata, array_offset, array_size);
        WriteText<kCopyDelim>(context, "}");
      };

      if constexpr (InContainer == WrapContext::Record) {
        // Wrap the rendered '{...}' as a record field iff its content has any
        // record trigger char ('(', ')', ',', '"', '\\', whitespace) or is
        // empty. '{' / '}' are NOT record triggers.
        if (ArrayNeedsQuotingInRecord<Core>(context, vdata, row)) {
          WriteWrapped<WrapContext::Record>(context, emit_inside);
        } else {
          emit_inside();
        }
      } else {
        emit_inside();
      }
    } else {
      uint64_t element_oid;
      if constexpr (ElementOID == kDynamicOid) {
        element_oid = Type2Oid(child_vdata.logical_type);
      } else {
        element_oid = ElementOID;
      }
      if (array_size == 0) {
        auto* prefix_data = context.writer->Alloc(12);
        absl::big_endian::Store32(prefix_data, /*dims*/ 0);
        absl::big_endian::Store32(prefix_data + 4, /*array_size*/ 0);
        absl::big_endian::Store32(prefix_data + 8, WireOid(element_oid));
        return;
      }
      auto* prefix_data = context.writer->Alloc(20);
      absl::big_endian::Store32(prefix_data, /*dims*/ 1);
      absl::big_endian::Store32(prefix_data + 8, WireOid(element_oid));
      absl::big_endian::Store32(prefix_data + 12, array_size);
      absl::big_endian::Store32(prefix_data + 16, 1);
      const bool has_null = EmitArrayElems<Core, VarFormat::Binary>(
        context, child_vdata, array_offset, array_size);
      absl::big_endian::Store32(prefix_data + 4, has_null ? 1 : 0);
    }
  }
};

template<typename Core, uint64_t ElementOID, VarFormat Format,
         WrapContext InContainer, bool kCopyDelim = false>
struct VectorArrayCore {
  using CopyDelimEscaped =
    VectorArrayCore<Core, ElementOID, Format, InContainer, true>;
  static constexpr absl::CharSet kCopyDelims =
    absl::CharSet{" "} | CopyDelimsOf<Core>();
  IRS_FORCE_INLINE static void Render(SerializationContext& context,
                                      const RUVF& vdata, duckdb::idx_t row) {
    auto [array_size, array_offset] = GetSliceResolved(vdata, row);
    auto& child_vdata = vdata.children[0];
    if constexpr (Format == VarFormat::Text) {
      using Elem =
        std::conditional_t<kCopyDelim, CopyDelimVariantOf<Core>, Core>;
      auto emit_inside = [&] {
        for (duckdb::idx_t i = 0; i < array_size; ++i) {
          if (i > 0) {
            WriteText<kCopyDelim>(context, " ");
          }
          const auto idx = child_vdata.unified.sel->get_index(array_offset + i);
          RenderValue<Elem>(context, child_vdata, idx);
        }
      };
      if constexpr (InContainer != WrapContext::None) {
        if (array_size != 1) {
          WriteWrapped<InContainer>(context, emit_inside);
          return;
        }
      }
      emit_inside();
    } else {
      auto* prefix_data = context.writer->Alloc(20);
      absl::big_endian::Store32(prefix_data, 1);
      absl::big_endian::Store32(prefix_data + 4, 0);
      absl::big_endian::Store32(prefix_data + 8, WireOid(ElementOID));
      absl::big_endian::Store32(prefix_data + 12, array_size);
      absl::big_endian::Store32(prefix_data + 16, 0);
      EmitArrayElems<Core, VarFormat::Binary>(context, child_vdata,
                                              array_offset, array_size);
    }
  }
};

template<typename Core>
constexpr bool kListValued = false;

template<typename Core, uint64_t ElementOID, VarFormat Format,
         WrapContext InContainer, bool kCopyDelim>
constexpr bool kListValued<
  VectorArrayCore<Core, ElementOID, Format, InContainer, kCopyDelim>> = true;

template<WrapContext InContainer, bool kCopyDelim>
constexpr bool kListValued<MultirangeTextCore<InContainer, kCopyDelim>> = true;

template<>
constexpr bool kListValued<MultirangeBinCore> = true;

template<typename Core>
IRS_FORCE_INLINE bool IsDimensionOf(const duckdb::LogicalType& type) {
  if constexpr (kListValued<Core>) {
    return IsArrayDimension(type);
  } else {
    const auto id = type.id();
    return id == duckdb::LogicalTypeId::ARRAY ||
           id == duckdb::LogicalTypeId::LIST ||
           id == duckdb::LogicalTypeId::MAP;
  }
}

template<typename Core>
bool ArrayNeedsQuotingInRecord(SerializationContext& ctx, const RUVF& vdata,
                               duckdb::idx_t row) {
  const auto [array_size, array_offset] = GetSliceResolved(vdata, row);
  if constexpr (*ArrayDelim<Core>::kValue == ',') {
    if (array_size > 1) {
      return true;
    }
  }
  const auto& child = vdata.children[0];
  if (IsDimensionOf<Core>(child.logical_type)) {
    for (duckdb::idx_t i = 0; i < array_size; ++i) {
      const auto child_row = child.unified.sel->get_index(array_offset + i);
      if (child.unified.validity.RowIsValid(child_row) &&
          ArrayNeedsQuotingInRecord<Core>(ctx, child, child_row)) {
        return true;
      }
    }
    return false;
  }
  for (duckdb::idx_t i = 0; i < array_size; ++i) {
    if (NeedsQuotingIn<WrapContext::Record>(ctx, child.logical_type, child,
                                            array_offset + i)) {
      return true;
    }
  }
  return false;
}

template<typename Core>
void FlattenArray(SerializationContext& context, const RUVF& vdata,
                  duckdb::idx_t source_row, uint64_t& leaf_oid, bool& has_null,
                  uint8_t* dim_sizes_data, int32_t depth) {
  auto [array_size, array_offset] = GetSliceResolved(vdata, source_row);
  absl::big_endian::Store32(dim_sizes_data + depth * 8,
                            static_cast<int32_t>(array_size));
  absl::big_endian::Store32(dim_sizes_data + depth * 8 + 4, 1);
  auto& child_vdata = vdata.children[0];
  if (array_size == 0) {
    const auto* leaf = &child_vdata.logical_type;
    int32_t deeper = depth + 1;
    while (IsDimensionOf<Core>(*leaf)) {
      absl::big_endian::Store32(dim_sizes_data + deeper * 8, 0);
      absl::big_endian::Store32(dim_sizes_data + deeper * 8 + 4, 1);
      ++deeper;
      leaf = leaf->id() == duckdb::LogicalTypeId::ARRAY
               ? &duckdb::ArrayType::GetChildType(*leaf)
               : &duckdb::ListType::GetChildType(*leaf);
    }
    leaf_oid = Type2Oid(*leaf);
    return;
  }
  if (!IsDimensionOf<Core>(child_vdata.logical_type)) {
    leaf_oid = Type2Oid(child_vdata.logical_type);
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

template<typename Core, uint64_t ElementOID, VarFormat Format,
         WrapContext InContainer, bool kCopyDelim = false>
struct MultiDimArrayCore {
  using CopyDelimEscaped =
    MultiDimArrayCore<Core, ElementOID, Format, InContainer, true>;
  static constexpr absl::CharSet kCopyDelims =
    kArrayCopyDelims | absl::CharSet{ArrayDelim<Core>::kValue} |
    CopyDelimsOf<Core>();
  IRS_FORCE_INLINE static void Render(SerializationContext& context,
                                      const RUVF& vdata, duckdb::idx_t row) {
    if constexpr (Format == VarFormat::Text) {
      if (!IsDimensionOf<Core>(vdata.logical_type)) {
        // `row` is already the resolved source row.
        if (!vdata.unified.validity.RowIsValid(row)) {
          WriteText<kCopyDelim>(context, "NULL");
        } else {
          RenderItem<
            std::conditional_t<kCopyDelim, CopyDelimVariantOf<Core>, Core>>(
            context, vdata, row);
        }
        return;
      }

      auto [array_size, array_offset] = GetSliceResolved(vdata, row);
      auto& child_vdata = vdata.children[0];

      auto emit_inside = [&] {
        WriteText<kCopyDelim>(context, "{");
        for (duckdb::idx_t i = 0; i < array_size; ++i) {
          if (i > 0) {
            WriteText<kCopyDelim>(context,
                                  std::string_view{ArrayDelim<Core>::kValue});
          }
          // A NULL sub-list/element must not be resolved further (its
          // list_entry is undefined); render it as the literal NULL like the
          // leaf branch.
          const auto child_row =
            child_vdata.unified.sel->get_index(array_offset + i);
          if (!child_vdata.unified.validity.RowIsValid(child_row)) {
            WriteText<kCopyDelim>(context, "NULL");
            continue;
          }
          MultiDimArrayCore<Core, ElementOID, Format, WrapContext::None,
                            kCopyDelim>::Render(context, child_vdata,
                                                child_row);
        }
        WriteText<kCopyDelim>(context, "}");
      };

      if constexpr (InContainer == WrapContext::Record) {
        if (ArrayNeedsQuotingInRecord<Core>(context, vdata, row)) {
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
      while (IsDimensionOf<Core>(*t)) {
        ++ndim;
        t = t->id() == duckdb::LogicalTypeId::ARRAY
              ? &duckdb::ArrayType::GetChildType(*t)
              : &duckdb::ListType::GetChildType(*t);
      }
      // `row` is already the resolved source row.
      if (GetSliceResolved(vdata, row).size == 0) {
        // PG sends an empty array as ndim=0 with no dimension descriptors.
        uint64_t leaf_oid;
        if constexpr (ElementOID == kDynamicOid) {
          leaf_oid = Type2Oid(*t);
        } else {
          leaf_oid = ElementOID;
        }
        auto* prefix_data = context.writer->Alloc(12);
        absl::big_endian::Store32(prefix_data, 0);
        absl::big_endian::Store32(prefix_data + 4, 0);
        absl::big_endian::Store32(prefix_data + 8, WireOid(leaf_oid));
        return;
      }
      // PG binary array: 12-byte top header (ndim, flags, elemtype) followed
      // by ndim*8 bytes of {dim_size, lbound} pairs, then element bytes.
      // FlattenArray fills each dim's size into its slot as it descends.
      auto* prefix_data = context.writer->Alloc(12 + ndim * 8);
      uint64_t leaf_oid = ElementOID;
      bool has_null = false;
      FlattenArray<Core>(context, vdata, row, leaf_oid, has_null,
                         prefix_data + 12, 0);
      absl::big_endian::Store32(prefix_data, ndim);
      absl::big_endian::Store32(prefix_data + 4, has_null ? 1 : 0);
      absl::big_endian::Store32(prefix_data + 8, WireOid(leaf_oid));
    }
  }
};

// Select the column-level (framed) serializer for the wire format: binary and
// top-level text are both int32-length-framed (BinaryField; top-level text just
// wraps the text core), COPY text uses \N framing, COPY csv quotes the field,
// an in-record field uses the bare record-text framing.
template<typename TextCore, typename TextRecCore, typename BinaryCore>
SerializationFunction SelectFieldSerializer(VarFormat format,
                                            SerializationContext& context) {
  if (format == VarFormat::Binary) {
    return SerializeField<Framing::BinaryField, BinaryCore>;
  }
  if ((format == VarFormat::CopyCsv || format == VarFormat::CopyCsvQuoted) &&
      context.csv_quote == '\0') {
    return SerializeField<Framing::CopyTextField, TextCore>;
  }
  if (format == VarFormat::CopyCsvQuoted) {
    return SerializeField<Framing::CsvQuotedField, TextCore>;
  }
  if (format == VarFormat::CopyCsv) {
    if constexpr (SizedCore<TextCore>) {
      if (!CsvScanNeeded<TextCore>(context)) {
        return SerializeField<Framing::CsvPlainField, TextCore>;
      }
    }
    return SerializeField<Framing::CsvField, TextCore>;
  }
  if (format == VarFormat::CopyText) {
    if constexpr (!CopyDelimsOf<TextCore>().empty()) {
      if (EscapesCopyDelim<TextCore>(context)) {
        return SerializeField<Framing::CopyTextField,
                              CopyDelimVariantOf<TextCore>>;
      }
    }
    return SerializeField<Framing::CopyTextField, TextCore>;
  }
  if (context.in_record) {
    if constexpr (!CopyDelimsOf<TextRecCore>().empty()) {
      if (EscapesCopyDelim<TextRecCore>(context)) {
        return RecordTextField<CopyDelimVariantOf<TextRecCore>>;
      }
    }
    return RecordTextField<TextRecCore>;
  }
  return SerializeField<Framing::BinaryField, TextCore>;
}

template<Framing F>
IRS_FORCE_INLINE uint8_t* WriteNullField(const SerializationContext& ctx,
                                         uint8_t* d) {
  if constexpr (F == Framing::BinaryField) {
    absl::big_endian::Store32(d, -1);
    return d + 4;
  } else {
    std::memcpy(d, ctx.copy_null.data(), ctx.copy_null.size());
    return d + ctx.copy_null.size();
  }
}

template<Framing F>
IRS_FORCE_INLINE uint8_t* FinishField(const SerializationContext& ctx,
                                      uint8_t* d, size_t n) {
  if constexpr (F == Framing::BinaryField) {
    absl::big_endian::Store32(d, static_cast<int32_t>(n));
    return d + 4 + n;
  } else if constexpr (kCsvFraming<F>) {
    bool quote = true;
    const std::string_view field{reinterpret_cast<const char*>(d), n};
    if constexpr (F == Framing::CsvPlainField) {
      quote = field == ctx.copy_null;
    } else if constexpr (F == Framing::CsvField) {
      quote = CsvNeedsQuotes(ctx, field);
    }
    return d + (quote ? duckdb::CsvQuoteInPlace(reinterpret_cast<char*>(d), n,
                                                ctx.csv_quote, ctx.csv_escape)
                      : n);
  } else {
    return d + n;
  }
}

template<Framing F>
constexpr size_t kBodyOffset = F == Framing::BinaryField ? 4 : 0;

template<Framing F>
size_t FieldBound(size_t body) {
  if constexpr (F == Framing::BinaryField) {
    return 4 + body;
  } else if constexpr (kCsvFraming<F>) {
    return 2 * body + 2;
  } else {
    return body;
  }
}

template<Framing F>
size_t NullBound(const SerializationContext& ctx) {
  return F == Framing::BinaryField ? 4 : ctx.copy_null.size();
}

template<Framing F, typename Core>
void WriteSizedColumn(SerializationContext& ctx, const RUVF& v,
                      duckdb::idx_t begin, duckdb::idx_t end,
                      uint8_t** cursors) {
  const auto& unified = v.unified;
  const auto render = [&](duckdb::idx_t idx, uint8_t* d) {
    return FinishField<F>(ctx, d,
                          RenderSized<Core>(d + kBodyOffset<F>, ctx, v, idx));
  };
  if (unified.validity.AllValid()) {
    for (auto row = begin; row < end; ++row, ++cursors) {
      *cursors = render(unified.sel->get_index(row), *cursors);
    }
    return;
  }
  for (auto row = begin; row < end; ++row, ++cursors) {
    const auto idx = unified.sel->get_index(row);
    *cursors = unified.validity.RowIsValid(idx)
                 ? render(idx, *cursors)
                 : WriteNullField<F>(ctx, *cursors);
  }
}

template<Framing F, typename Core>
ColumnSerializer SizedColumn(const SerializationContext& ctx) {
  return {
    .write = WriteSizedColumn<F, Core>,
    .fixed = std::max(FieldBound<F>(Core::kMaxBytes), NullBound<F>(ctx)),
  };
}

template<typename TextCore, typename BinaryCore>
ColumnSerializer SelectSizedColumn(VarFormat format,
                                   const SerializationContext& context) {
  if (format == VarFormat::Binary) {
    return SizedColumn<Framing::BinaryField, BinaryCore>(context);
  }
  if ((format == VarFormat::CopyCsv || format == VarFormat::CopyCsvQuoted) &&
      context.csv_quote == '\0') {
    return SizedColumn<Framing::CopyTextField, TextCore>(context);
  }
  if (format == VarFormat::CopyCsvQuoted) {
    return SizedColumn<Framing::CsvQuotedField, TextCore>(context);
  }
  if (format == VarFormat::CopyCsv) {
    if (!CsvScanNeeded<TextCore>(context)) {
      return SizedColumn<Framing::CsvPlainField, TextCore>(context);
    }
    return SizedColumn<Framing::CsvField, TextCore>(context);
  }
  if (format == VarFormat::CopyText) {
    if constexpr (!CopyDelimsOf<TextCore>().empty()) {
      if (EscapesCopyDelim<TextCore>(context)) {
        return SizedColumn<Framing::CopyTextField,
                           CopyDelimVariantOf<TextCore>>(context);
      }
    }
    return SizedColumn<Framing::CopyTextField, TextCore>(context);
  }
  SDB_ASSERT(format == VarFormat::Text);
  return SizedColumn<Framing::BinaryField, TextCore>(context);
}

struct RawBody {
  IRS_FORCE_INLINE static size_t Bound(const SerializationContext&,
                                       size_t size) {
    return size;
  }
  IRS_FORCE_INLINE static size_t Render(const SerializationContext&,
                                        std::string_view value, uint8_t* d) {
    std::memcpy(d, value.data(), value.size());
    return value.size();
  }
};

IRS_FORCE_INLINE size_t EscapedBound(const SerializationContext& ctx,
                                     size_t size) {
  if (EscapesNothing(ctx)) {
    return size;
  }
  return size * std::max({size_t{2}, ctx.quote_seq.size(),
                          size_t{ctx.backslash_count}});
}

IRS_FORCE_INLINE size_t RenderEscaped(const SerializationContext& ctx,
                                      std::string_view value, uint8_t* d) {
  if (EscapesNothing(ctx)) {
    return RawBody::Render(ctx, value, d);
  }
  RawSink sink{d};
  EscapeTo(ctx, value, sink);
  return static_cast<size_t>(sink.out - d);
}

struct VarcharBody {
  IRS_FORCE_INLINE static size_t Bound(const SerializationContext& ctx,
                                       size_t size) {
    return EscapedBound(ctx, size);
  }
  IRS_FORCE_INLINE static size_t Render(const SerializationContext& ctx,
                                        std::string_view value, uint8_t* d) {
    return RenderEscaped(ctx, value, d);
  }
};

struct ByteaHexBody {
  IRS_FORCE_INLINE static size_t Bound(const SerializationContext& ctx,
                                       size_t size) {
    return EscapedBound(ctx, 1) + 1 + 2 * size;
  }
  IRS_FORCE_INLINE static size_t Render(const SerializationContext& ctx,
                                        std::string_view value, uint8_t* d) {
    auto* p = d + RenderEscaped(ctx, "\\", d);
    *p++ = 'x';
    absl::BytesToHexStringInternal(
      reinterpret_cast<const unsigned char*>(value.data()),
      reinterpret_cast<char*>(p), value.size());
    return static_cast<size_t>(p - d) + 2 * value.size();
  }
};

template<Framing F, typename Body>
void BoundStringColumn(const SerializationContext& ctx, const RUVF& v,
                       duckdb::idx_t begin, duckdb::idx_t end, size_t* bounds) {
  const auto& unified = v.unified;
  const auto* data =
    duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::string_t>(unified);
  const auto null_bound = NullBound<F>(ctx);
  for (auto row = begin; row < end; ++row, ++bounds) {
    const auto idx = unified.sel->get_index(row);
    *bounds += unified.validity.RowIsValid(idx)
                 ? FieldBound<F>(Body::Bound(ctx, data[idx].GetSize()))
                 : null_bound;
  }
}

template<Framing F, typename Body>
void WriteStringColumn(SerializationContext& ctx, const RUVF& v,
                       duckdb::idx_t begin, duckdb::idx_t end,
                       uint8_t** cursors) {
  const auto& unified = v.unified;
  const auto* data =
    duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::string_t>(unified);
  if constexpr (F == Framing::CsvField && std::is_same_v<Body, VarcharBody>) {
    if (EscapesNothing(ctx)) {
      for (auto row = begin; row < end; ++row, ++cursors) {
        const auto idx = unified.sel->get_index(row);
        auto* d = *cursors;
        if (!unified.validity.RowIsValid(idx)) {
          *cursors = WriteNullField<F>(ctx, d);
          continue;
        }
        const std::string_view field{data[idx].GetData(), data[idx].GetSize()};
        auto* out = reinterpret_cast<char*>(d);
        if (CsvNeedsQuotes(ctx, field)) {
          *cursors = d + duckdb::CsvWriteQuoted(out, field, ctx.csv_quote,
                                                ctx.csv_escape);
        } else {
          std::memcpy(out, field.data(), field.size());
          *cursors = d + field.size();
        }
      }
      return;
    }
  }
  for (auto row = begin; row < end; ++row, ++cursors) {
    const auto idx = unified.sel->get_index(row);
    auto* d = *cursors;
    if (!unified.validity.RowIsValid(idx)) {
      *cursors = WriteNullField<F>(ctx, d);
      continue;
    }
    const auto& value = data[idx];
    *cursors =
      FinishField<F>(ctx, d,
                     Body::Render(ctx, {value.GetData(), value.GetSize()},
                                  d + kBodyOffset<F>));
  }
}

template<Framing F, typename Body>
ColumnSerializer StringColumn() {
  return {
    .write = WriteStringColumn<F, Body>,
    .bound = BoundStringColumn<F, Body>,
  };
}

template<typename TextBody>
ColumnSerializer SelectStringColumn(VarFormat format,
                                    const SerializationContext& context) {
  if (format == VarFormat::Binary) {
    return StringColumn<Framing::BinaryField, RawBody>();
  }
  if ((format == VarFormat::CopyCsv || format == VarFormat::CopyCsvQuoted) &&
      context.csv_quote == '\0') {
    return StringColumn<Framing::CopyTextField, TextBody>();
  }
  if (format == VarFormat::CopyCsvQuoted) {
    return StringColumn<Framing::CsvQuotedField, TextBody>();
  }
  if (format == VarFormat::CopyCsv) {
    return StringColumn<Framing::CsvField, TextBody>();
  }
  if (format == VarFormat::CopyText) {
    return StringColumn<Framing::CopyTextField, TextBody>();
  }
  SDB_ASSERT(format == VarFormat::Text);
  return StringColumn<Framing::BinaryField, TextBody>();
}

ColumnSerializer GetColumnSerialization(const duckdb::LogicalType& type,
                                        VarFormat format,
                                        SerializationContext& context) {
  if (!type.HasAlias()) {
    switch (type.id()) {
      using enum duckdb::LogicalTypeId;
      case BOOLEAN:
        return SelectSizedColumn<BoolTextCore, BoolBinCore>(format, context);
      case TINYINT:
        return SelectSizedColumn<IntTextCore<int8_t>,
                                 IntBinCore<int8_t, int16_t>>(format, context);
      case SMALLINT:
        return SelectSizedColumn<IntTextCore<int16_t>, IntBinCore<int16_t>>(
          format, context);
      case INTEGER:
        return SelectSizedColumn<IntTextCore<int32_t>, IntBinCore<int32_t>>(
          format, context);
      case BIGINT:
        return SelectSizedColumn<IntTextCore<int64_t>, IntBinCore<int64_t>>(
          format, context);
      case UTINYINT:
        return SelectSizedColumn<IntTextCore<uint8_t>,
                                 IntBinCore<uint8_t, int16_t>>(format, context);
      case USMALLINT:
        return SelectSizedColumn<IntTextCore<uint16_t>,
                                 IntBinCore<uint16_t, int32_t>>(format,
                                                                context);
      case UINTEGER:
        return SelectSizedColumn<IntTextCore<uint32_t>,
                                 IntBinCore<uint32_t, int64_t>>(format,
                                                                context);
      case FLOAT:
        return irs::Resolve(context.extra_float_digits > 0, [&]<bool Precise> {
          return SelectSizedColumn<FloatTextCore<float, Precise>,
                                   FloatBinCore<float>>(format, context);
        });
      case DOUBLE:
        return irs::Resolve(context.extra_float_digits > 0, [&]<bool Precise> {
          return SelectSizedColumn<FloatTextCore<double, Precise>,
                                   FloatBinCore<double>>(format, context);
        });
      case DECIMAL:
        switch (type.InternalType()) {
          using enum duckdb::PhysicalType;
          case INT16:
            return SelectSizedColumn<DecimalTextCore<int16_t>,
                                     DecimalBinCore<int16_t>>(format, context);
          case INT32:
            return SelectSizedColumn<DecimalTextCore<int32_t>,
                                     DecimalBinCore<int32_t>>(format, context);
          case INT64:
            return SelectSizedColumn<DecimalTextCore<int64_t>,
                                     DecimalBinCore<int64_t>>(format, context);
          case INT128:
            return SelectSizedColumn<DecimalTextCore<duckdb::hugeint_t>,
                                     DecimalBinCore<duckdb::hugeint_t>>(
              format, context);
          default:
            break;
        }
        break;
      case DATE:
        return SelectSizedColumn<DateTextCore, DateBinCore>(format, context);
      case TIMESTAMP:
        return SelectSizedColumn<TimestampTextSized, TimestampBinCore>(format,
                                                                       context);
      case TIMESTAMP_TZ:
        return SelectSizedColumn<TimestampTzTextSized, TimestampTzBinCore>(
          format, context);
      case UUID:
        return SelectSizedColumn<UuidTextCore, UuidBinCore>(format, context);
      case CHAR:
      case VARCHAR:
        return SelectStringColumn<VarcharBody>(format, context);
      case BLOB:
        if (context.bytea_output == ByteaOutput::Hex) {
          return SelectStringColumn<ByteaHexBody>(format, context);
        }
        break;
      default:
        break;
    }
  }
  return {.field = GetSerialization(type, format, context)};
}

constexpr size_t kRowTagBytes = 5;
constexpr size_t kMinRowGroupBytes = 4 << 10;
constexpr size_t kMaxRowGroupBytes = 128 << 10;

IRS_FORCE_INLINE uint8_t* Append(uint8_t* d, std::string_view bytes) {
  if (!bytes.empty()) {
    std::memcpy(d, bytes.data(), bytes.size());
  }
  return d + bytes.size();
}

uint8_t* CopyScratch(message::Chunk* chunk, size_t offset, size_t size,
                     uint8_t* d) {
  while (size != 0) {
    const auto available = chunk->GetEnd() - offset;
    if (available == 0) {
      chunk = chunk->Next();
      offset = chunk->GetBegin();
      continue;
    }
    const auto n = std::min(available, size);
    std::memcpy(d, chunk->WritableData() + offset, n);
    d += n;
    offset += n;
    size -= n;
  }
  return d;
}

void AttachSession(VarFormat format, SerializationContext& context) {
  if (format == VarFormat::Binary) {
    return;
  }
  auto& caches = Caches(context);
  if (!caches.session) {
    caches.session =
      std::make_unique<const Session>(MakeSession(context.client));
  }
}

template<RegKind Kind>
SerializationFunction MakeRegSerializer(VarFormat format,
                                        SerializationContext& context) {
  AttachSession(format, context);
  return SelectFieldSerializer<RegTextCore<Kind, WrapContext::None>,
                               RegTextCore<Kind, WrapContext::Record>,
                               OidBinCore>(format, context);
}

template<typename TextCore, typename BinaryCore, uint64_t Oid>
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

template<typename TextCore, typename BinaryCore, uint64_t Oid>
SerializationFunction MakeVectorSerializer(VarFormat format,
                                           SerializationContext& context) {
  using Text =
    VectorArrayCore<TextCore, Oid, VarFormat::Text, WrapContext::None>;
  using TextRec =
    VectorArrayCore<TextCore, Oid, VarFormat::Text, WrapContext::Record>;
  using Binary =
    VectorArrayCore<BinaryCore, Oid, VarFormat::Binary, WrapContext::None>;
  return SelectFieldSerializer<Text, TextRec, Binary>(format, context);
}

template<RegKind Kind, uint64_t Oid>
SerializationFunction MakeRegArraySerializer(VarFormat format,
                                             SerializationContext& context,
                                             ArrayKind kind) {
  AttachSession(format, context);
  return MakeArraySerializer<RegTextCore<Kind, WrapContext::Array>, OidBinCore,
                             Oid>(format, context, kind);
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
        return MakeRegArraySerializer<RegKind::Type, kRegtype>(format, context,
                                                               kind);
      }
      if (IsRegclass(type)) {
        return MakeRegArraySerializer<RegKind::Class, kRegclass>(format,
                                                                 context, kind);
      }
      if (IsRegnamespace(type)) {
        return MakeRegArraySerializer<RegKind::Namespace, kRegnamespace>(
          format, context, kind);
      }
      if (IsRegproc(type)) {
        return MakeRegArraySerializer<RegKind::Proc, kRegproc>(format, context,
                                                               kind);
      }
      if (IsRegprocedure(type)) {
        return MakeRegArraySerializer<RegKind::Procedure, kRegprocedure>(
          format, context, kind);
      }
      if (IsRegoper(type)) {
        return MakeRegArraySerializer<RegKind::Oper, kRegoper>(format, context,
                                                               kind);
      }
      if (IsRegoperator(type)) {
        return MakeRegArraySerializer<RegKind::Operator, kRegoperator>(
          format, context, kind);
      }
      if (IsRegrole(type)) {
        return MakeRegArraySerializer<RegKind::Role, kRegrole>(format, context,
                                                               kind);
      }
      if (IsRegconfig(type)) {
        return MakeRegArraySerializer<RegKind::Config, kRegconfig>(
          format, context, kind);
      }
      if (IsRegdictionary(type)) {
        return MakeRegArraySerializer<RegKind::Dictionary, kRegdictionary>(
          format, context, kind);
      }
      if (IsRegcollation(type)) {
        return MakeRegArraySerializer<RegKind::Collation, kRegcollation>(
          format, context, kind);
      }
      if (IsOid(type)) {
        return MakeArraySerializer<IntTextCore<int64_t>, OidBinCore, kOid>(
          format, context, kind);
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
        return MakeArraySerializer<TidTextCore<WrapContext::Array>, TidBinCore,
                                   kTid>(format, context, kind);
      }
      return MakeArraySerializer<IntTextCore<int64_t>, IntBinCore<int64_t>,
                                 kInt8>(format, context, kind);
    }
    case UBIGINT:
      if (IsXid8(type)) {
        return MakeArraySerializer<IntTextCore<uint64_t>, IntBinCore<uint64_t>,
                                   kXid8>(format, context, kind);
      }
      if (IsPgLsn(type)) {
        return MakeArraySerializer<LsnTextCore, IntBinCore<uint64_t>, kPgLsn>(
          format, context, kind);
      }
      if (IsMacaddr(type)) {
        return MakeArraySerializer<MacTextCore<false>, MacBinCore<false>,
                                   kMacaddr>(format, context, kind);
      }
      if (IsMacaddr8(type)) {
        return MakeArraySerializer<MacTextCore<true>, MacBinCore<true>,
                                   kMacaddr8>(format, context, kind);
      }
      return MakeArraySerializer<IntTextCore<uint64_t>, UbigintBinCore,
                                 kNumeric>(format, context, kind);
    case HUGEINT:
      return MakeArraySerializer<HugeintTextCore, HugeintBinCore, kNumeric>(
        format, context, kind);
    case UHUGEINT:
      return MakeArraySerializer<UhugeintTextCore, UhugeintBinCore, kNumeric>(
        format, context, kind);
    case FLOAT:
      return irs::Resolve(context.extra_float_digits > 0, [&]<bool Precise> {
        return MakeArraySerializer<FloatTextCore<float, Precise>,
                                   FloatBinCore<float>, kFloat4>(format,
                                                                 context, kind);
      });
    case DOUBLE:
      return irs::Resolve(context.extra_float_digits > 0, [&]<bool Precise> {
        return MakeArraySerializer<FloatTextCore<double, Precise>,
                                   FloatBinCore<double>, kFloat8>(
          format, context, kind);
      });
    case DECIMAL:
      if (IsMoney(type)) {
        return MakeArraySerializer<MoneyTextCore, MoneyBinCore, kMoney>(
          format, context, kind);
      }
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
      if (IsChar(type)) {
        return MakeArraySerializer<VarcharTextCore<WrapContext::Array>,
                                   CharBinCore, kChar>(format, context, kind);
      }
      if (IsXml(type)) {
        return MakeArraySerializer<XmlTextCore<WrapContext::Array>, XmlBinCore,
                                   kXml>(format, context, kind);
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
    case GEOMETRY:
      return MakeArraySerializer<GeometryTextCore<WrapContext::Array>,
                                 GeometryBinCore, kGeometry>(format, context,
                                                             kind);
    case DATE:
      return MakeArraySerializer<DateTextCore, DateBinCore, kDate>(
        format, context, kind);
    case TIME:
      return MakeArraySerializer<TimeTextCore<>, TimeBinCore, kTime>(
        format, context, kind);
    case TIME_NS:
      return MakeArraySerializer<TimeNsTextCore<>, TimeNsBinCore, kTime>(
        format, context, kind);
    case TIME_TZ:
      return MakeArraySerializer<TimeTzTextCore<>, TimeTzBinCore, kTimetz>(
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
                                 TimestampTzBinCore, kTimestamptz>(
        format, context, kind);
    case TIMESTAMP_TZ_NS:
      return MakeArraySerializer<TimestampTzNsTextCore<WrapContext::Array>,
                                 TimestampTzNsBinCore, kTimestamptz>(
        format, context, kind);
    case INTERVAL:
      return MakeArraySerializer<IntervalTextCore<WrapContext::Array>,
                                 IntervalBinCore, kInterval>(format, context,
                                                             kind);
    case UUID:
      return MakeArraySerializer<UuidTextCore, UuidBinCore, kUuid>(
        format, context, kind);
    case BIT:
      return MakeArraySerializer<BitTextCore<WrapContext::Array>, BitBinCore,
                                 kVarbit>(format, context, kind);
    case TUPLE:
    case STRUCT:
      if (IsInet(type)) {
        return MakeArraySerializer<InetTextCore<false>, InetBinCore<false>,
                                   kInet>(format, context, kind);
      }
      if (IsCidr(type)) {
        return MakeArraySerializer<InetTextCore<true>, InetBinCore<true>,
                                   kCidr>(format, context, kind);
      }
      if (connector::IsTSQueryStructType(type)) {
        return MakeArraySerializer<TsqueryTextCore<WrapContext::Array>,
                                   TsqueryBinCore, kText>(format, context,
                                                          kind);
      }
      if (IsRange(type)) {
        return MakeArraySerializer<RangeTextCore<WrapContext::Array>,
                                   RangeBinCore, kDynamicOid>(format, context,
                                                              kind);
      }
      if (const auto geometric = GeometricOf(type)) {
        return irs::Resolve(*geometric, [&]<Geometric Kind>() {
          return MakeArraySerializer<
            GeometricTextCore<Kind, WrapContext::Array>, GeometricBinCore<Kind>,
            kDynamicOid>(format, context, kind);
        });
      }
      // Element OID is resolved per-row by Type2Oid: anonymous ROW(...) yields
      // kRecord, named record types yield their pg_type OID.
      return MakeArraySerializer<RecordTextCore<WrapContext::Array>,
                                 RecordBinCore, kDynamicOid>(format, context,
                                                             kind);
    case ENUM:
      return MakeArraySerializer<EnumTextCore<WrapContext::Array>, EnumBinCore,
                                 kDynamicOid>(format, context, kind);
    case LIST:
      if (IsInt2vector(type)) {
        return MakeArraySerializer<
          VectorArrayCore<IntTextCore<int16_t>, kInt2, VarFormat::Text,
                          WrapContext::Array>,
          VectorArrayCore<IntBinCore<int16_t>, kInt2, VarFormat::Binary,
                          WrapContext::None>,
          kInt2Vector>(format, context, kind);
      }
      if (IsOidvector(type)) {
        return MakeArraySerializer<
          VectorArrayCore<IntTextCore<int64_t>, kOid, VarFormat::Text,
                          WrapContext::Array>,
          VectorArrayCore<OidBinCore, kOid, VarFormat::Binary,
                          WrapContext::None>,
          kOidvector>(format, context, kind);
      }
      SDB_ASSERT(IsMultirange(type));
      return MakeArraySerializer<MultirangeTextCore<WrapContext::Array>,
                                 MultirangeBinCore, kDynamicOid>(format,
                                                                 context, kind);
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("Array element type not supported"));
  }
}

}  // namespace

void ThrowMessageTooLarge() {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                  ERR_MSG("row exceeds maximum allowed message length (",
                          kMaxMessageSize, " bytes)"));
}

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

void FillContext(duckdb::ClientContext& client, SerializationContext& context) {
  auto& config = ConnectionContext::Get(client);
  context.extra_float_digits = config.GetExtraFloatDigits();
  context.bytea_output = config.GetByteaOutput();
  std::string tz_name{config.GetTimeZone()};
  context.time_zone = IsUtcTimeZoneName(tz_name)
                        ? nullptr
                        : duckdb::ICUHelpers::TryGetTimeZone(tz_name);
  if (context.time_zone) {
    context.zone_lut = duckdb::ZoneLUT::Get(*context.time_zone);
  } else {
    context.zone_lut.reset();
  }
  context.client = &client;
  context.caches.reset();
}

std::array<duckdb::CsvSpecials, 4> TextEscapes(bool copy_text, char delim) {
  std::array<duckdb::CsvSpecials, 4> escapes;
  for (size_t i = 0; i < escapes.size(); ++i) {
    auto& set = escapes[i];
    if (i & 1) {
      set.Add('\\');
    }
    if (i & 2) {
      set.Add('"');
    }
    if (copy_text) {
      for (char c = '\b'; c <= '\r'; ++c) {
        set.Add(c);
      }
      set.Add(delim);
    }
  }
  return escapes;
}

void SetCopyText(SerializationContext& context, char delim,
                 std::string null_str) {
  context.copy_text = true;
  context.backslash_count = 2;
  context.quote_seq = delim == '"' ? "\\\"" : "\"";
  context.copy_delim = delim;
  context.copy_null = std::move(null_str);
  context.text_escapes = TextEscapes(true, delim);
}

void SerializeCopyTextHeader(SerializationContext& context,
                             std::span<const duckdb::Identifier> names) {
  bool first = true;
  for (const auto& name : names) {
    if (!std::exchange(first, false)) {
      context.writer->Write({&context.copy_delim, 1});
    }
    EmitEscaped(context, name.GetIdentifierName());
  }
  context.writer->Write("\n");
}

void SetCopyCsv(SerializationContext& context, char delim, char quote,
                char escape, std::string null_str, std::string new_line,
                bool single_column) {
  context.copy_text = false;
  context.backslash_count = 1;
  context.quote_seq = "\"";
  context.copy_delim = delim;
  context.text_escapes = TextEscapes(false, delim);
  context.copy_null = std::move(null_str);
  context.copy_newline = std::move(new_line);
  context.csv_quote = quote;
  context.csv_escape = escape;
  context.csv_single_column = single_column;
  context.csv_specials = {};
  context.csv_specials.Add(delim);
  context.csv_specials.Add(quote);
  context.csv_specials.Add('\n');
  context.csv_specials.Add('\r');
}

void SerializeCopyCsvHeader(SerializationContext& context,
                            std::span<const duckdb::Identifier> names) {
  auto& writer = *context.writer;
  bool first = true;
  for (const auto& name : names) {
    if (!std::exchange(first, false)) {
      writer.Write({&context.copy_delim, 1});
    }
    const std::string_view text = name.GetIdentifierName();
    if (context.csv_quote != '\0' && CsvNeedsQuotes(context, text)) {
      WriteCsvQuoted(writer, text, context.csv_quote, context.csv_escape);
    } else {
      writer.Write(text);
    }
  }
  writer.Write(context.copy_newline);
}

SerializationFunction GetSerialization(const duckdb::LogicalType& type,
                                       VarFormat format,
                                       SerializationContext& context) {
  SDB_ASSERT(!context.in_record || format == VarFormat::Text);
  switch (type.id()) {
    using enum duckdb::LogicalTypeId;
    case SQLNULL:
      if (format == VarFormat::CopyText || format == VarFormat::CopyCsv ||
          format == VarFormat::CopyCsvQuoted) {
        return SerializeCopyTextNullColumn;
      }
      return context.in_record ? SerializeRecordNullColumn
                               : SerializeNullColumn;
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
      if (!type.GetTypeInfo().alias.empty()) {
        if (IsRegtype(type)) {
          return MakeRegSerializer<RegKind::Type>(format, context);
        }
        if (IsRegclass(type)) {
          return MakeRegSerializer<RegKind::Class>(format, context);
        }
        if (IsRegnamespace(type)) {
          return MakeRegSerializer<RegKind::Namespace>(format, context);
        }
        if (IsRegproc(type)) {
          return MakeRegSerializer<RegKind::Proc>(format, context);
        }
        if (IsRegprocedure(type)) {
          return MakeRegSerializer<RegKind::Procedure>(format, context);
        }
        if (IsRegoper(type)) {
          return MakeRegSerializer<RegKind::Oper>(format, context);
        }
        if (IsRegoperator(type)) {
          return MakeRegSerializer<RegKind::Operator>(format, context);
        }
        if (IsRegrole(type)) {
          return MakeRegSerializer<RegKind::Role>(format, context);
        }
        if (IsRegconfig(type)) {
          return MakeRegSerializer<RegKind::Config>(format, context);
        }
        if (IsRegdictionary(type)) {
          return MakeRegSerializer<RegKind::Dictionary>(format, context);
        }
        if (IsRegcollation(type)) {
          return MakeRegSerializer<RegKind::Collation>(format, context);
        }
        if (IsTid(type)) {
          return SelectFieldSerializer<TidTextCore<WrapContext::None>,
                                       TidTextCore<WrapContext::Record>,
                                       TidBinCore>(format, context);
        }
        if (IsOidLike(type)) {
          return SelectFieldSerializer<IntTextCore<int64_t>,
                                       IntTextCore<int64_t>, OidBinCore>(
            format, context);
        }
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
      if (!type.GetTypeInfo().alias.empty()) {
        if (IsXid8(type)) {
          return SelectFieldSerializer<
            IntTextCore<uint64_t>, IntTextCore<uint64_t>, IntBinCore<uint64_t>>(
            format, context);
        }
        if (IsPgLsn(type)) {
          return SelectFieldSerializer<LsnTextCore, LsnTextCore,
                                       IntBinCore<uint64_t>>(format, context);
        }
        if (IsMacaddr(type)) {
          return SelectFieldSerializer<MacTextCore<false>, MacTextCore<false>,
                                       MacBinCore<false>>(format, context);
        }
        if (IsMacaddr8(type)) {
          return SelectFieldSerializer<MacTextCore<true>, MacTextCore<true>,
                                       MacBinCore<true>>(format, context);
        }
      }
      return SelectFieldSerializer<IntTextCore<uint64_t>, IntTextCore<uint64_t>,
                                   UbigintBinCore>(format, context);
    case HUGEINT:
      return SelectFieldSerializer<HugeintTextCore, HugeintTextCore,
                                   HugeintBinCore>(format, context);
    case UHUGEINT:
      return SelectFieldSerializer<UhugeintTextCore, UhugeintTextCore,
                                   UhugeintBinCore>(format, context);
    case FLOAT:
      return irs::Resolve(context.extra_float_digits > 0, [&]<bool Precise> {
        return SelectFieldSerializer<FloatTextCore<float, Precise>,
                                     FloatTextCore<float, Precise>,
                                     FloatBinCore<float>>(format, context);
      });
    case DOUBLE:
      return irs::Resolve(context.extra_float_digits > 0, [&]<bool Precise> {
        return SelectFieldSerializer<FloatTextCore<double, Precise>,
                                     FloatTextCore<double, Precise>,
                                     FloatBinCore<double>>(format, context);
      });
    case DECIMAL:
      if (IsMoney(type)) {
        return SelectFieldSerializer<MoneyTextCore, MoneyTextCore,
                                     MoneyBinCore>(format, context);
      }
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
      if (!type.GetTypeInfo().alias.empty()) {
        if (type.IsJSONType()) {
          return SelectFieldSerializer<JsonTextCore<WrapContext::None>,
                                       JsonTextCore<WrapContext::Record>,
                                       JsonBinCore>(format, context);
        }
        if (IsChar(type)) {
          return SelectFieldSerializer<VarcharTextCore<WrapContext::None>,
                                       VarcharTextCore<WrapContext::Record>,
                                       CharBinCore>(format, context);
        }
        if (IsXml(type)) {
          return SelectFieldSerializer<XmlTextCore<WrapContext::None>,
                                       XmlTextCore<WrapContext::Record>,
                                       XmlBinCore>(format, context);
        }
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
    case GEOMETRY:
      return SelectFieldSerializer<GeometryTextCore<WrapContext::None>,
                                   GeometryTextCore<WrapContext::Record>,
                                   GeometryBinCore>(format, context);
    case DATE:
      return SelectFieldSerializer<DateTextCore, DateTextCore, DateBinCore>(
        format, context);
    case TIME:
      return SelectFieldSerializer<TimeTextCore<>, TimeTextCore<>, TimeBinCore>(
        format, context);
    case TIME_NS:
      return SelectFieldSerializer<TimeNsTextCore<>, TimeNsTextCore<>,
                                   TimeNsBinCore>(format, context);
    case TIME_TZ:
      return SelectFieldSerializer<TimeTzTextCore<>, TimeTzTextCore<>,
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
    case TIMESTAMP_TZ_NS:
      return SelectFieldSerializer<TimestampTzNsTextCore<WrapContext::None>,
                                   TimestampTzNsTextCore<WrapContext::Record>,
                                   TimestampTzNsBinCore>(format, context);
    case INTERVAL:
      return SelectFieldSerializer<IntervalTextCore<WrapContext::None>,
                                   IntervalTextCore<WrapContext::Record>,
                                   IntervalBinCore>(format, context);
    case UUID:
      return SelectFieldSerializer<UuidTextCore, UuidTextCore, UuidBinCore>(
        format, context);
    case BIT:
      return SelectFieldSerializer<BitTextCore<WrapContext::None>,
                                   BitTextCore<WrapContext::Record>,
                                   BitBinCore>(format, context);
    case ENUM:
      return SelectFieldSerializer<EnumTextCore<WrapContext::None>,
                                   EnumTextCore<WrapContext::Record>,
                                   EnumBinCore>(format, context);
    case TUPLE:
    case STRUCT:
      if (IsInet(type)) {
        return SelectFieldSerializer<InetTextCore<false>, InetTextCore<false>,
                                     InetBinCore<false>>(format, context);
      }
      if (IsCidr(type)) {
        return SelectFieldSerializer<InetTextCore<true>, InetTextCore<true>,
                                     InetBinCore<true>>(format, context);
      }
      if (connector::IsTSQueryStructType(type)) {
        return SelectFieldSerializer<TsqueryTextCore<WrapContext::None>,
                                     TsqueryTextCore<WrapContext::Record>,
                                     TsqueryBinCore>(format, context);
      }
      if (IsRange(type)) {
        return SelectFieldSerializer<RangeTextCore<WrapContext::None>,
                                     RangeTextCore<WrapContext::Record>,
                                     RangeBinCore>(format, context);
      }
      if (const auto geometric = GeometricOf(type)) {
        return irs::Resolve(*geometric, [&]<Geometric Kind>() {
          return SelectFieldSerializer<
            GeometricTextCore<Kind, WrapContext::None>,
            GeometricTextCore<Kind, WrapContext::Record>,
            GeometricBinCore<Kind>>(format, context);
        });
      }
      return SelectFieldSerializer<RecordTextCore<WrapContext::None>,
                                   RecordTextCore<WrapContext::Record>,
                                   RecordBinCore>(format, context);
    case ARRAY:
    case LIST:
    case MAP: {
      if (IsInt2vector(type)) {
        return MakeVectorSerializer<IntTextCore<int16_t>, IntBinCore<int16_t>,
                                    kInt2>(format, context);
      }
      if (IsOidvector(type)) {
        return MakeVectorSerializer<IntTextCore<int64_t>, OidBinCore, kOid>(
          format, context);
      }
      if (IsMultirange(type)) {
        return SelectFieldSerializer<MultirangeTextCore<WrapContext::None>,
                                     MultirangeTextCore<WrapContext::Record>,
                                     MultirangeBinCore>(format, context);
      }
      const auto* element_type = &type;
      size_t dims = 0;
      while (IsArrayDimension(*element_type)) {
        element_type = element_type->id() == ARRAY
                         ? &duckdb::ArrayType::GetChildType(*element_type)
                         : &duckdb::ListType::GetChildType(*element_type);
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
                      ERR_MSG("Type '", type.ToString(), "' is not supported"));
  }
}

ChunkSerializer::ChunkSerializer(std::span<const duckdb::LogicalType> types,
                                 std::span<const VarFormat> formats,
                                 SerializationContext& context) {
  SDB_ASSERT(types.size() == formats.size());
  _columns.reserve(types.size());
  for (size_t column = 0; column < types.size(); ++column) {
    _fixed += _columns
                .emplace_back(GetColumnSerialization(types[column],
                                                     formats[column], context))
                .fixed;
  }
}

void ChunkSerializer::Write(message::Buffer& out, SerializationContext& context,
                            const duckdb::DataChunk& chunk, duckdb::idx_t begin,
                            duckdb::idx_t end, const RowShape& shape) {
  if (begin >= end) {
    return;
  }
  const auto columns = _columns.size();
  SDB_ASSERT(chunk.ColumnCount() == columns);
  auto& decoded = context.decoded;
  decoded.clear();
  decoded.resize(columns);
  for (size_t column = 0; column < columns; ++column) {
    duckdb::Vector::RecursiveToUnifiedFormat(chunk.data[column], chunk.size(),
                                             decoded[column]);
  }
  const auto count = static_cast<size_t>(end - begin);
  const auto framing = (shape.tag != '\0' ? kRowTagBytes : 0) +
                       (shape.field_count ? 2 : 0) + shape.before.size() +
                       shape.after.size() +
                       (shape.delimited && columns > 0 ? columns - 1 : 0);
  _bounds.assign(count, framing + _fixed);
  _bounds.front() += shape.first.size();
  _cursors.resize(count);
  _fields.clear();
  _scratch.Clear();
  for (size_t column = 0; column < columns; ++column) {
    const auto& serializer = _columns[column];
    if (serializer.bound) {
      serializer.bound(context, decoded[column], begin, end, _bounds.data());
    } else if (serializer.field) {
      for (auto row = begin; row < end; ++row) {
        message::Writer writer{_scratch};
        context.writer = &writer;
        const auto mark = writer.Position();
        serializer.field(context, decoded[column], row);
        _fields.push_back({mark.chunk, mark.end, writer.Written()});
        _bounds[row - begin] += writer.Written();
        writer.Commit(false);
      }
    }
  }
  context.writer = nullptr;
  for (size_t first = 0; first < count;) {
    message::Writer writer{out};
    const auto room = writer.Room();
    const auto budget = room < kMinRowGroupBytes
                          ? kMaxRowGroupBytes
                          : std::min(room, kMaxRowGroupBytes);
    auto last = first + 1;
    auto total = _bounds[first];
    for (; last < count && total + _bounds[last] <= budget; ++last) {
      total += _bounds[last];
    }
    writer.Write(total, [&](uint8_t* base) {
      return WriteRows(context, shape, base, begin, first, last, count);
    });
    writer.Commit(false);
    first = last;
  }
}

size_t ChunkSerializer::WriteRows(SerializationContext& context,
                                  const RowShape& shape, uint8_t* base,
                                  duckdb::idx_t begin, size_t first,
                                  size_t last, size_t count) {
  const auto columns = _columns.size();
  auto* slot = base;
  for (auto i = first; i < last; ++i) {
    auto* d = slot + (shape.tag != '\0' ? kRowTagBytes : 0);
    if (i == 0) {
      d = Append(d, shape.first);
    }
    if (shape.field_count) {
      absl::big_endian::Store16(d, static_cast<uint16_t>(columns));
      d += 2;
    }
    _cursors[i] = Append(d, shape.before);
    slot += _bounds[i];
  }
  auto** cursors = _cursors.data() + first;
  const auto rows = last - first;
  size_t scratch = 0;
  for (size_t column = 0; column < columns; ++column) {
    if (column > 0 && shape.delimited) {
      for (size_t i = 0; i < rows; ++i) {
        *cursors[i]++ = static_cast<uint8_t>(context.copy_delim);
      }
    }
    const auto& serializer = _columns[column];
    if (serializer.write) {
      serializer.write(context, context.decoded[column], begin + first,
                       begin + last, cursors);
      continue;
    }
    const auto* field = _fields.data() + scratch++ * count + first;
    for (size_t i = 0; i < rows; ++i) {
      cursors[i] =
        CopyScratch(field[i].chunk, field[i].offset, field[i].size, cursors[i]);
    }
  }
  auto* out = base;
  slot = base;
  for (auto i = first; i < last; ++i) {
    const auto size =
      static_cast<size_t>(Append(_cursors[i], shape.after) - slot);
    SDB_ASSERT(size <= _bounds[i]);
    if (shape.tag != '\0') {
      CheckMessageSize(size - 1);
      slot[0] = static_cast<uint8_t>(shape.tag);
      absl::big_endian::Store32(slot + 1, static_cast<uint32_t>(size - 1));
    }
    if (out != slot) {
      std::memmove(out, slot, size);
    }
    out += size;
    slot += _bounds[i];
  }
  return static_cast<size_t>(out - base);
}

}  // namespace sdb::pg
