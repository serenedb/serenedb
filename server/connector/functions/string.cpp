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

#include "connector/functions/string.h"

#include <absl/algorithm/container.h>
#include <absl/crc/crc32c.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <fast_float/fast_float.h>
#include <openssl/evp.h>
#include <re2/re2.h>
#include <zlib.h>

#include <array>
#include <duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp>
#include <duckdb/common/types/blob.hpp>
#include <duckdb/common/vector_operations/generic_executor.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/scalar/string_common.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/utf8_utils.hpp>
#include <span>
#include <utf8proc.hpp>

#include "connector/common.h"
#include "pg/encoding.h"
#include "pg/serialize.h"
#include "pg/sql_utils.h"
#include "pg/types/bytea.h"
#include "pg/types/logical.h"

namespace sdb::connector {
namespace {

// PostgreSQL initcap: capitalize first letter of each word, lowercase the rest.
// Word boundaries are any non-alphanumeric character.
void InitcapFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                     duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    args.data[0], result, args.size(),
    [&](duckdb::string_t input) -> duckdb::string_t {
      auto len = input.GetSize();
      auto data = input.GetData();
      auto target = duckdb::StringVector::EmptyString(result, len);
      auto output = target.GetDataWriteable();

      bool new_word = true;
      for (duckdb::idx_t i = 0; i < len; i++) {
        auto c = static_cast<unsigned char>(data[i]);
        if (std::isalpha(c)) {
          output[i] =
            static_cast<char>(new_word ? std::toupper(c) : std::tolower(c));
          new_word = false;
        } else {
          output[i] = data[i];
          new_word = true;
        }
      }
      target.Finalize();
      return target;
    });
}

// to_bin(int32/int64) -> text -- ported from PgToBin
template<typename T>
void ToBinFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                   duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<T, duckdb::string_t>(
    args.data[0], result, args.size(), [&](T value) -> duckdb::string_t {
      using U = std::make_unsigned_t<T>;
      U uval = static_cast<U>(value);
      if (uval == 0) {
        return duckdb::StringVector::AddString(result, "0");
      }
      char buf[sizeof(U) * 8];
      int pos = sizeof(buf);
      while (uval > 0) {
        buf[--pos] = '0' + (uval & 1);
        uval >>= 1;
      }
      return duckdb::StringVector::AddString(result, buf + pos,
                                             sizeof(buf) - pos);
    });
}

// to_oct(int32/int64) -> text -- ported from PgToOct
template<typename T>
void ToOctFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                   duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<T, duckdb::string_t>(
    args.data[0], result, args.size(), [&](T value) -> duckdb::string_t {
      using U = std::make_unsigned_t<T>;
      U uval = static_cast<U>(value);
      if (uval == 0) {
        return duckdb::StringVector::AddString(result, "0");
      }
      char buf[sizeof(U) * 3];
      int pos = sizeof(buf);
      while (uval > 0) {
        buf[--pos] = '0' + (uval & 7);
        uval >>= 3;
      }
      return duckdb::StringVector::AddString(result, buf + pos,
                                             sizeof(buf) - pos);
    });
}

// get_byte(bytea, offset) -> integer -- ported from PgGetByte
void BlobPositionFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                          duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t, int64_t>(
    args.data[0], args.data[1], result, args.size(),
    [](duckdb::string_t data, duckdb::string_t search) -> int64_t {
      const auto location = duckdb::FindStrInStr(data, search);
      return location == duckdb::DConstants::INVALID_INDEX
               ? 0
               : static_cast<int64_t>(location) + 1;
    });
}

void GetByteFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                     duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<duckdb::string_t, int32_t, int32_t>(
    args.data[0], args.data[1], result, args.size(),
    [](duckdb::string_t data, int32_t offset) -> int32_t {
      if (offset < 0 || offset >= static_cast<int32_t>(data.GetSize())) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        ERR_MSG("index ", offset, " out of valid range, 0..",
                                static_cast<int32_t>(data.GetSize()) - 1));
      }
      return static_cast<uint8_t>(data.GetData()[offset]);
    });
}

// set_byte(bytea, offset, value) -> bytea -- ported from PgSetByte
void SetByteFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                     duckdb::Vector& result) {
  duckdb::TernaryExecutor::Execute<duckdb::string_t, int32_t, int32_t,
                                   duckdb::string_t>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t data, int32_t offset,
        int32_t value) -> duckdb::string_t {
      if (offset < 0 || offset >= static_cast<int32_t>(data.GetSize())) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        ERR_MSG("index ", offset, " out of valid range, 0..",
                                static_cast<int32_t>(data.GetSize()) - 1));
      }
      auto target = duckdb::StringVector::EmptyString(result, data.GetSize());
      auto out = target.GetDataWriteable();
      memcpy(out, data.GetData(), data.GetSize());
      out[offset] = static_cast<char>(value & 0xff);
      target.Finalize();
      return target;
    });
}

// get_bit(bytea, offset) -> integer -- ported from PgGetBit
void GetBitFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                    duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<duckdb::string_t, int64_t, int32_t>(
    args.data[0], args.data[1], result, args.size(),
    [](duckdb::string_t data, int64_t bit_offset) -> int32_t {
      if (bit_offset < 0 ||
          bit_offset >= static_cast<int64_t>(data.GetSize()) * 8) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
          ERR_MSG("index ", bit_offset, " out of valid range, 0..",
                  static_cast<int64_t>(data.GetSize()) * 8 - 1));
      }
      int64_t byte_idx = bit_offset / 8;
      int bit_idx = static_cast<int>(bit_offset % 8);
      return (static_cast<uint8_t>(data.GetData()[byte_idx]) >> bit_idx) & 1;
    });
}

// set_bit(bytea, offset, value) -> bytea -- ported from PgSetBit
void SetBitFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                    duckdb::Vector& result) {
  duckdb::TernaryExecutor::Execute<duckdb::string_t, int64_t, int32_t,
                                   duckdb::string_t>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t data, int64_t bit_offset,
        int32_t value) -> duckdb::string_t {
      if (bit_offset < 0 ||
          bit_offset >= static_cast<int64_t>(data.GetSize()) * 8) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
          ERR_MSG("index ", bit_offset, " out of valid range, 0..",
                  static_cast<int64_t>(data.GetSize()) * 8 - 1));
      }
      int64_t byte_idx = bit_offset / 8;
      int bit_idx = static_cast<int>(bit_offset % 8);
      auto target = duckdb::StringVector::EmptyString(result, data.GetSize());
      auto out = target.GetDataWriteable();
      memcpy(out, data.GetData(), data.GetSize());
      auto& byte = out[byte_idx];
      if (value) {
        byte |= (1 << bit_idx);
      } else {
        byte &= ~(1 << bit_idx);
      }
      target.Finalize();
      return target;
    });
}

int32_t Encoding(std::string_view name, std::string_view direction) {
  const auto encoding = pg::EncodingByName(name);
  if (encoding < 0) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("invalid ", direction, " encoding name \"", name, "\""));
  }
  return encoding;
}

void ConvertFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                     duckdb::Vector& result) {
  std::string buffer;
  duckdb::TernaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                   duckdb::string_t, duckdb::string_t>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t data, duckdb::string_t from, duckdb::string_t to) {
      const auto source = Encoding(AsView(from), "source");
      const auto converted = pg::ConvertEncoding(
        AsView(data), source, Encoding(AsView(to), "destination"), buffer);
      return duckdb::StringVector::AddStringOrBlob(result, converted.data(),
                                                   converted.size());
    });
}

void ConvertFromFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                         duckdb::Vector& result) {
  std::string buffer;
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                  duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t data, duckdb::string_t encoding) {
      const auto converted =
        pg::ConvertEncoding(AsView(data), Encoding(AsView(encoding), "source"),
                            pg::kUtf8Encoding, buffer);
      return duckdb::StringVector::AddString(result, converted.data(),
                                             converted.size());
    });
}

void ConvertToFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                       duckdb::Vector& result) {
  std::string buffer;
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                  duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t data, duckdb::string_t encoding) {
      const auto converted =
        pg::ConvertEncoding(AsView(data), pg::kUtf8Encoding,
                            Encoding(AsView(encoding), "destination"), buffer);
      return duckdb::StringVector::AddStringOrBlob(result, converted.data(),
                                                   converted.size());
    });
}

// pg_client_encoding() -> name -- ported from PgClientEncoding
void PgClientEncodingFunction(duckdb::DataChunk&, duckdb::ExpressionState&,
                              duckdb::Vector& result) {
  result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
  duckdb::ConstantVector::GetData<duckdb::string_t>(result)[0] =
    duckdb::StringVector::AddString(result, "UTF8");
}

// quote_ident(text) -> text -- ported from QuoteIdentFunction
void QuoteIdentFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                        duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    args.data[0], result, args.size(),
    [&](duckdb::string_t input) -> duckdb::string_t {
      const std::string_view str(input.GetData(), input.GetSize());
      if (!pg::RequiresQuotes(str)) {
        return input;
      }
      return duckdb::StringVector::AddString(result, pg::QuoteIdentifier(str));
    });
  duckdb::StringVector::AddHeapReference(result, args.data[0]);
}

// octet_length(text) -> int: byte length (DuckDB has it for BLOB/BIT, not
// VARCHAR)
void OctetLengthFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                         duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, int64_t>(
    args.data[0], result, args.size(), [](duckdb::string_t input) -> int64_t {
      return static_cast<int64_t>(input.GetSize());
    });
}

// string_to_array(text, delimiter, null_string) -> text[]
// 3-arg form: splits then replaces null_string matches with NULL.
// When null_string is NULL, behaves like 2-arg form (no NULL replacement).
void StringToArray3Function(duckdb::DataChunk& args, duckdb::ExpressionState&,
                            duckdb::Vector& result) {
  const auto count = args.size();
  auto strings = args.data[0].Values<duckdb::string_t>();
  auto delims = args.data[1].Values<duckdb::string_t>();
  auto null_strs = args.data[2].Values<duckdb::string_t>();
  auto rows =
    duckdb::FlatVector::Writer<duckdb::VectorListType<duckdb::string_t>>(result,
                                                                         count);

  std::vector<std::string_view> parts;
  for (duckdb::idx_t i = 0; i < count; i++) {
    auto str = strings[i];
    auto delim = delims[i];
    if (!str.IsValid()) {
      rows.WriteNull();
      continue;
    }
    auto null_str = null_strs[i];
    const bool has_null_str = null_str.IsValid();
    std::string_view ns;
    if (has_null_str) {
      ns = {null_str.GetValue().GetData(), null_str.GetValue().GetSize()};
    }
    const std::string_view s{str.GetValue().GetData(),
                             str.GetValue().GetSize()};

    parts.clear();
    if (s.empty()) {
      rows.WriteList(0);
      continue;
    }
    if (!delim.IsValid()) {
      const auto* begin = reinterpret_cast<const irs::byte_type*>(s.data());
      const auto* end = begin + s.size();
      for (const auto* it = begin; it != end;) {
        const auto* next = irs::utf8_utils::Next(it, end);
        parts.emplace_back(reinterpret_cast<const char*>(it),
                           static_cast<size_t>(next - it));
        it = next;
      }
    } else if (delim.GetValue().GetSize() == 0) {
      parts.push_back(s);
    } else {
      const std::string_view d{delim.GetValue().GetData(),
                               delim.GetValue().GetSize()};
      size_t pos = 0;
      while (pos <= s.size()) {
        auto found = s.find(d, pos);
        if (found == std::string_view::npos) {
          parts.push_back(s.substr(pos));
          break;
        }
        parts.push_back(s.substr(pos, found - pos));
        pos = found + d.size();
      }
    }

    size_t j = 0;
    for (auto& child : rows.WriteList(parts.size())) {
      if (has_null_str && parts[j] == ns) {
        child.WriteNull();
      } else {
        child.WriteValue(duckdb::string_t{
          parts[j].data(), static_cast<uint32_t>(parts[j].size())});
      }
      ++j;
    }
  }
}

// PG encode(bytea, format) -> text
void EncodeFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                    duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                  duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t data, duckdb::string_t fmt) -> duckdb::string_t {
      std::string_view format{fmt.GetData(), fmt.GetSize()};
      std::string_view input{data.GetData(), data.GetSize()};
      if (format == "hex") {
        std::string hex;
        hex.reserve(input.size() * 2);
        for (auto c : input) {
          static constexpr char kHex[] = "0123456789abcdef";
          hex += kHex[(static_cast<unsigned char>(c) >> 4) & 0xf];
          hex += kHex[static_cast<unsigned char>(c) & 0xf];
        }
        return duckdb::StringVector::AddString(result, hex);
      }
      if (format == "base64") {
        auto encoded = duckdb::Blob::ToBase64(data);
        return duckdb::StringVector::AddString(result, encoded);
      }
      if (format == "escape") {
        // PG escape format -- same as byteaout escape
        auto required = pg::ByteaOutEscapeLength(input);
        auto target = duckdb::StringVector::EmptyString(result, required);
        pg::ByteaOutEscape(target.GetDataWriteable(), input);
        target.Finalize();
        return target;
      }
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("unrecognized encoding: \"", format, "\""));
    });
}

// PG decode(text, format) -> bytea
void DecodeFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                    duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                  duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t data, duckdb::string_t fmt) -> duckdb::string_t {
      std::string_view format{fmt.GetData(), fmt.GetSize()};
      std::string_view input{data.GetData(), data.GetSize()};
      if (format == "hex") {
        auto blob_size = input.size() / 2;
        auto target = duckdb::StringVector::EmptyString(result, blob_size);
        auto out = target.GetDataWriteable();
        for (size_t i = 0; i < input.size(); i += 2) {
          auto hex_val = [](char c) -> int {
            if (c >= '0' && c <= '9') {
              return c - '0';
            }
            if (c >= 'a' && c <= 'f') {
              return 10 + c - 'a';
            }
            if (c >= 'A' && c <= 'F') {
              return 10 + c - 'A';
            }
            return -1;
          };
          int h1 = hex_val(input[i]);
          int h2 = (i + 1 < input.size()) ? hex_val(input[i + 1]) : -1;
          if (h1 < 0 || h2 < 0) {
            THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                            ERR_MSG("invalid hexadecimal data"));
          }
          *out++ = static_cast<char>((h1 << 4) | h2);
        }
        target.Finalize();
        return duckdb::StringVector::AddStringOrBlob(
          result, duckdb::string_t(target.GetDataWriteable(), blob_size));
      }
      if (format == "base64") {
        auto blob = duckdb::Blob::FromBase64(data);
        return duckdb::StringVector::AddStringOrBlob(result, blob);
      }
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("unrecognized encoding: \"", format, "\""));
    });
}

// PG format() accepts varargs of any type and stringifies each one via its
// standard text output. Coerce non-VARCHAR varargs to VARCHAR at bind time so
// the executor body can read every input as `string_t` unconditionally.
duckdb::unique_ptr<duckdb::FunctionData> PgFormatBind(
  duckdb::BindScalarFunctionInput& input) {
  auto& arguments = input.GetArguments();
  auto& context = input.GetClientContext();
  for (auto& arg : arguments) {
    if (arg->GetReturnType().id() != duckdb::LogicalTypeId::VARCHAR) {
      arg = duckdb::BoundCastExpression::AddCastToType(
        context, std::move(arg), duckdb::LogicalType::VARCHAR);
    }
  }
  return nullptr;
}

// PG format(text, ...) -- implements %s, %I, %L, %%, positional %n$s, width
void PgFormatFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                      duckdb::Vector& result) {
  auto count = args.size();
  auto ncols = args.ColumnCount();

  // Flatten all vectors
  std::vector<duckdb::UnifiedVectorFormat> vdata(ncols);
  for (duckdb::idx_t c = 0; c < ncols; c++) {
    args.data[c].ToUnifiedFormat(count, vdata[c]);
  }

  auto result_data =
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);

  for (duckdb::idx_t row = 0; row < count; row++) {
    // Get format string
    auto fmt_idx = vdata[0].sel->get_index(row);
    if (!vdata[0].validity.RowIsValid(fmt_idx)) {
      result_validity.SetInvalid(row);
      continue;
    }
    auto fmt_str =
      duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(vdata[0])[fmt_idx];
    std::string_view fmt{fmt_str.GetData(), fmt_str.GetSize()};

    // Helper to get arg as string
    auto get_arg = [&](duckdb::idx_t arg_idx) -> std::optional<std::string> {
      if (arg_idx >= ncols) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("too few arguments for format()"));
      }
      auto idx = vdata[arg_idx].sel->get_index(row);
      if (!vdata[arg_idx].validity.RowIsValid(idx)) {
        return std::nullopt;
      }
      auto val = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(
        vdata[arg_idx])[idx];
      return std::string{val.GetData(), val.GetSize()};
    };

    // Helper: quote literal
    auto quote_literal =
      [](const std::optional<std::string>& s) -> std::string {
      if (!s) {
        return "NULL";
      }
      bool has_backslash = s->find('\\') != std::string::npos;
      std::string out;
      if (has_backslash) {
        out += "E";
      }
      out += '\'';
      for (auto c : *s) {
        if (c == '\'') {
          out += "''";
        } else if (c == '\\') {
          out += "\\\\";
        } else {
          out += c;
        }
      }
      out += '\'';
      return out;
    };

    std::string out;
    duckdb::idx_t next_arg =
      1;  // next sequential arg (1-based, 0 is format string)

    for (size_t i = 0; i < fmt.size();) {
      if (fmt[i] != '%') {
        out += fmt[i++];
        continue;
      }
      i++;  // skip %
      if (i >= fmt.size()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("unterminated format() conversion specifier"));
      }
      if (fmt[i] == '%') {
        out += '%';
        i++;
        continue;
      }

      // Parse optional position: %n$
      duckdb::idx_t arg_pos = 0;
      bool has_position = false;
      size_t save_i = i;
      while (i < fmt.size() && fmt[i] >= '0' && fmt[i] <= '9') {
        i++;
      }
      if (i < fmt.size() && fmt[i] == '$' && i > save_i) {
        if (fast_float::from_chars(fmt.data() + save_i, fmt.data() + i, arg_pos)
              .ec != std::errc{}) {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                          ERR_MSG("number is out of range"));
        }
        has_position = true;
        i++;  // skip $
      } else {
        i = save_i;  // not a position, rewind
      }

      // Parse optional flags and width: [-][width]
      bool left_align = false;
      int width = 0;
      if (i < fmt.size() && fmt[i] == '-') {
        left_align = true;
        i++;
      }
      while (i < fmt.size() && fmt[i] >= '0' && fmt[i] <= '9') {
        width = width * 10 + (fmt[i] - '0');
        i++;
      }

      if (i >= fmt.size()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("unterminated format() conversion specifier"));
      }

      char spec = fmt[i++];
      duckdb::idx_t arg_idx = has_position ? arg_pos : next_arg++;

      std::string formatted;
      switch (spec) {
        case 's': {
          auto val = get_arg(arg_idx);
          formatted = val.value_or("");
        } break;
        case 'I': {
          auto val = get_arg(arg_idx);
          if (!val) {
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_NULL_VALUE_NOT_ALLOWED),
              ERR_MSG("null values cannot be formatted as an SQL identifier"));
          }
          formatted = pg::QuoteIdentifier(*val);
        } break;
        case 'L': {
          auto val = get_arg(arg_idx);
          formatted = quote_literal(val);
          break;
        }
        default:
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                          ERR_MSG("unrecognized format() type specifier \"",
                                  std::string_view{&spec, 1}, "\""));
      }

      // Apply width
      if (width > 0 && formatted.size() < static_cast<size_t>(width)) {
        auto pad = width - formatted.size();
        if (left_align) {
          formatted.append(pad, ' ');
        } else {
          formatted.insert(0, pad, ' ');
        }
      }

      out += formatted;
    }

    result_data[row] = duckdb::StringVector::AddString(result, out);
  }
}

// PG quote_literal: wraps in single quotes, doubles embedded quotes.
// Uses E'...' prefix when backslashes are present (PG escape string syntax).
void QuoteLiteralFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                          duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    args.data[0], result, args.size(), [&](duckdb::string_t input) {
      auto str = input.GetString();
      bool has_backslash = str.find('\\') != std::string::npos;
      std::string quoted;
      quoted.reserve(str.size() + 3);
      if (has_backslash) {
        quoted += "E'";
      } else {
        quoted += '\'';
      }
      for (char c : str) {
        if (c == '\'') {
          quoted += "''";
        } else if (c == '\\') {
          quoted += "\\\\";
        } else {
          quoted += c;
        }
      }
      quoted += '\'';
      return duckdb::StringVector::AddString(result, quoted);
    });
}

// PG quote_nullable: like quote_literal but returns 'NULL' for NULL input
void QuoteNullableFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                           duckdb::Vector& result) {
  const auto count = args.size();
  auto inputs = args.data[0].Values<duckdb::string_t>();
  auto out = duckdb::FlatVector::Writer<duckdb::string_t>(result, count);

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto input = inputs[i];
    if (!input.IsValid()) {
      out.WriteValue(duckdb::string_t{"NULL", 4});
      continue;
    }
    const auto str = input.GetValue().GetString();
    const bool has_backslash = str.find('\\') != std::string::npos;
    std::string quoted;
    quoted.reserve(str.size() + 3);
    if (has_backslash) {
      quoted += "E'";
    } else {
      quoted += '\'';
    }
    for (char c : str) {
      if (c == '\'') {
        quoted += "''";
      } else if (c == '\\') {
        quoted += "\\\\";
      } else {
        quoted += c;
      }
    }
    quoted += '\'';
    out.WriteValue(
      duckdb::string_t{quoted.data(), static_cast<uint32_t>(quoted.size())});
  }
}

void LikeEscapeFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                        duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                  duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t pattern, duckdb::string_t escape) -> duckdb::string_t {
      std::string_view esc_sv{escape.GetData(), escape.GetSize()};
      if (esc_sv.size() != 1) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_ESCAPE_SEQUENCE),
          ERR_MSG("invalid escape string: must be one character"));
      }
      auto out =
        LikeEscapePattern({pattern.GetData(), pattern.GetSize()}, esc_sv[0]);
      return duckdb::StringVector::AddString(result, out);
    });
}

size_t Utf8CharCount(const char* begin, size_t byte_len) {
  auto* it = reinterpret_cast<const irs::byte_type*>(begin);
  auto* end = it + byte_len;
  size_t count = 0;
  while (it < end) {
    it = irs::utf8_utils::Next(it, end);
    ++count;
  }
  return count;
}

size_t Utf8NextCharPos(const char* begin, size_t byte_len, size_t pos) {
  if (pos >= byte_len) {
    return pos + 1;  // past end - just bump to terminate loop
  }
  auto* it = reinterpret_cast<const irs::byte_type*>(begin) + pos;
  auto* end = reinterpret_cast<const irs::byte_type*>(begin) + byte_len;
  it = irs::utf8_utils::Next(it, end);
  return static_cast<size_t>(it -
                             reinterpret_cast<const irs::byte_type*>(begin));
}

size_t Utf8Advance(const char* begin, size_t byte_len, size_t n) {
  auto* it = reinterpret_cast<const irs::byte_type*>(begin);
  auto* end = it + byte_len;
  for (size_t i = 0; i < n; ++i) {
    if (it == end) {
      return byte_len + 1;
    }
    it = irs::utf8_utils::Next(it, end);
  }
  return static_cast<size_t>(it -
                             reinterpret_cast<const irs::byte_type*>(begin));
}

re2::RE2::Options OwnedRegexOptions() {
  re2::RE2::Options options{re2::RE2::Quiet};
  options.set_thread_safe(false);
  options.set_valid_utf8(true);
  return options;
}

struct PgRegexpFlags {
  bool global = false;
  bool case_insensitive = false;
  bool newline_stop = false;
  bool newline_anchor = false;
  bool literal = false;
};

PgRegexpFlags ParsePgRegexpFlags(std::string_view flags) {
  PgRegexpFlags parsed;
  for (const auto& option : flags) {
    if (option == 'g') {
      parsed.global = true;
    } else if (option == 'c' || option == 'i') {
      parsed.case_insensitive = option == 'i';
    } else if (option == 'm' || option == 'n') {
      parsed.newline_stop = true;
      parsed.newline_anchor = true;
    } else if (option == 'p' || option == 'w' || option == 's') {
      parsed.newline_stop = option == 'p';
      parsed.newline_anchor = option == 'w';
    } else if (option == 'q' || option == 'e') {
      parsed.literal = option == 'q';
    } else if (option != 't') {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("invalid regular expression option: \"",
                              std::string_view{&option, 1}, "\""));
    }
  }
  return parsed;
}

enum class PgRegexpKind : uint8_t {
  Count,
  Instr,
  Substr,
  Replace,
};

struct PgRegexpState final : duckdb::FunctionLocalState {
  std::string pattern;
  std::string flags_text;
  PgRegexpFlags flags;
  std::unique_ptr<re2::RE2> re;
  std::vector<re2::StringPiece> groups;

  const re2::RE2& Compile(std::string_view new_pattern,
                          std::string_view new_flags) {
    if (re && pattern == new_pattern && flags_text == new_flags) {
      return *re;
    }
    flags = ParsePgRegexpFlags(new_flags);
    auto options = OwnedRegexOptions();
    options.set_case_sensitive(!flags.case_insensitive);
    options.set_dot_nl(!flags.newline_stop);
    options.set_literal(flags.literal);
    re = std::make_unique<re2::RE2>(flags.newline_anchor && !flags.literal
                                      ? absl::StrCat("(?m)", new_pattern)
                                      : std::string{new_pattern},
                                    options);
    if (!re->ok()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_REGULAR_EXPRESSION),
                      ERR_MSG("invalid regular expression: ", re->error()));
    }
    groups.resize(re->NumberOfCapturingGroups() + 1);
    pattern = new_pattern;
    flags_text = new_flags;
    return *re;
  }
};

duckdb::unique_ptr<duckdb::FunctionLocalState> PgRegexpInitLocalState(
  duckdb::ExpressionState&, const duckdb::BoundFunctionExpression&,
  duckdb::FunctionData*) {
  return duckdb::make_uniq<PgRegexpState>();
}

void CheckPgRegexpParameter(std::string_view name, int32_t value, bool valid) {
  if (!valid) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("invalid value for parameter \"", name, "\": ", value));
  }
}

struct PgRegexpMatcher {
  const re2::RE2& re;
  re2::StringPiece text;
  size_t pos;
  std::span<re2::StringPiece> groups;

  PgRegexpMatcher(const re2::RE2& re, std::string_view text, int32_t start,
                  std::span<re2::StringPiece> groups)
    : re{re},
      text{text.data(), text.size()},
      pos{
        Utf8Advance(text.data(), text.size(), static_cast<size_t>(start - 1))},
      groups{groups} {}

  bool Next() {
    if (pos > text.size() ||
        !re.Match(text, pos, text.size(), re2::RE2::UNANCHORED, groups.data(),
                  static_cast<int>(groups.size()))) {
      return false;
    }
    pos = End(0);
    if (groups[0].empty()) {
      pos = Utf8NextCharPos(text.data(), text.size(), pos);
    }
    return true;
  }

  bool NextNth(int32_t n) {
    for (int32_t count = 0; count < n; ++count) {
      if (!Next()) {
        return false;
      }
    }
    return true;
  }

  size_t Begin(size_t group) const {
    return groups[group].data() - text.data();
  }
  size_t End(size_t group) const { return Begin(group) + groups[group].size(); }
  int32_t Position(size_t offset) const {
    return static_cast<int32_t>(Utf8CharCount(text.data(), offset)) + 1;
  }
};

void AppendPgReplacement(std::string& out, std::string_view replacement,
                         const PgRegexpMatcher& match) {
  for (size_t i = 0; i < replacement.size(); ++i) {
    const auto c = replacement[i];
    if (c != '\\' || i + 1 == replacement.size()) {
      out.push_back(c);
      continue;
    }
    const auto next = replacement[++i];
    if (next >= '1' && next <= '9') {
      const auto group = static_cast<size_t>(next - '0');
      if (group < match.groups.size() && match.groups[group].data()) {
        out.append(match.groups[group].data(), match.groups[group].size());
      }
    } else if (next == '&') {
      out.append(match.groups[0].data(), match.groups[0].size());
    } else if (next == '\\') {
      out.push_back('\\');
    } else {
      out.push_back('\\');
      out.push_back(next);
    }
  }
}

size_t ReplacementGroups(std::string_view replacement) {
  size_t groups = 1;
  for (size_t i = 0; i + 1 < replacement.size(); ++i) {
    if (replacement[i] != '\\') {
      continue;
    }
    const auto next = replacement[++i];
    if (next >= '1' && next <= '9') {
      groups = std::max(groups, static_cast<size_t>(next - '0') + 1);
    }
  }
  return groups;
}

template<PgRegexpKind Kind>
void PgRegexpFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                      duckdb::Vector& result) {
  using Result = std::conditional_t<Kind == PgRegexpKind::Count ||
                                      Kind == PgRegexpKind::Instr,
                                    int32_t, duckdb::string_t>;
  constexpr std::string_view kName =
    Kind == PgRegexpKind::Count   ? "regexp_count"
    : Kind == PgRegexpKind::Instr ? "regexp_instr"
                                  : "regexp_substr";
  const auto arity = static_cast<int>(args.ColumnCount());
  const int replacement_arg = Kind == PgRegexpKind::Replace ? 2 : -1;
  const int start_arg = Kind == PgRegexpKind::Replace ? 3 : 2;
  const int n_arg = Kind == PgRegexpKind::Count ? -1 : start_arg + 1;
  const int endoption_arg = Kind == PgRegexpKind::Instr ? 4 : -1;
  const int flags_arg = Kind == PgRegexpKind::Count   ? 3
                        : Kind == PgRegexpKind::Instr ? 5
                                                      : n_arg + 1;
  const int subexpr_arg = Kind == PgRegexpKind::Instr    ? 6
                          : Kind == PgRegexpKind::Substr ? 5
                                                         : -1;

  auto& cache = duckdb::ExecuteFunctionState::GetFunctionState(state)
                  ->Cast<PgRegexpState>();
  std::vector<duckdb::UnifiedVectorFormat> columns(args.ColumnCount());
  for (duckdb::idx_t c = 0; c < args.ColumnCount(); ++c) {
    args.data[c].ToUnifiedFormat(args.size(), columns[c]);
  }
  const auto text_at = [&](int column, duckdb::idx_t row) {
    const auto& value = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(
      columns[column])[columns[column].sel->get_index(row)];
    return std::string_view{value.GetData(), value.GetSize()};
  };
  const auto int_at = [&](int column, duckdb::idx_t row, int32_t fallback) {
    if (column < 0 || column >= arity) {
      return fallback;
    }
    return duckdb::UnifiedVectorFormat::GetData<int32_t>(
      columns[column])[columns[column].sel->get_index(row)];
  };

  const auto constant = [&](int column) {
    return column >= arity ||
           (args.data[column].GetVectorType() ==
              duckdb::VectorType::CONSTANT_VECTOR &&
            !duckdb::ConstantVector::IsNull(args.data[column]));
  };
  const bool constant_pattern = constant(1) && constant(flags_arg);
  const re2::RE2* constant_re = nullptr;
  auto writer = duckdb::FlatVector::Writer<Result>(result, args.size());
  std::string replaced;
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    if (absl::c_any_of(columns, [&](const duckdb::UnifiedVectorFormat& column) {
          return !column.validity.RowIsValid(column.sel->get_index(row));
        })) {
      writer.WriteNull();
      continue;
    }
    const auto start = int_at(start_arg, row, 1);
    CheckPgRegexpParameter("start", start, start > 0);
    const auto n = int_at(n_arg, row, 1);
    CheckPgRegexpParameter("n", n,
                           Kind == PgRegexpKind::Replace ? n >= 0 : n > 0);
    const auto endoption = int_at(endoption_arg, row, 0);
    CheckPgRegexpParameter("endoption", endoption,
                           endoption == 0 || endoption == 1);
    const auto subexpr = int_at(subexpr_arg, row, 0);
    CheckPgRegexpParameter("subexpr", subexpr, subexpr >= 0);
    const auto& re =
      constant_re
        ? *constant_re
        : cache.Compile(text_at(1, row),
                        flags_arg < arity ? text_at(flags_arg, row) : "");
    if (constant_pattern) {
      constant_re = &re;
    }
    if constexpr (Kind != PgRegexpKind::Replace) {
      if (cache.flags.global) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG(kName, "() does not support the \"global\" option"));
      }
    }
    const auto text = text_at(0, row);
    std::string_view replacement;
    size_t groups = 1;
    if constexpr (Kind == PgRegexpKind::Instr || Kind == PgRegexpKind::Substr) {
      if (static_cast<size_t>(subexpr) < cache.groups.size()) {
        groups = static_cast<size_t>(subexpr) + 1;
      }
    } else if constexpr (Kind == PgRegexpKind::Replace) {
      replacement = text_at(replacement_arg, row);
      groups = std::min(ReplacementGroups(replacement), cache.groups.size());
    }
    PgRegexpMatcher match{re, text, start,
                          std::span{cache.groups}.first(groups)};
    if constexpr (Kind == PgRegexpKind::Count) {
      int32_t count = 0;
      while (match.Next()) {
        ++count;
      }
      writer.WriteValue(count);
    } else if constexpr (Kind == PgRegexpKind::Instr) {
      const auto group = static_cast<size_t>(subexpr);
      if (!match.NextNth(n) || group >= match.groups.size() ||
          !match.groups[group].data()) {
        writer.WriteValue(0);
        continue;
      }
      writer.WriteValue(
        match.Position(endoption == 1 ? match.End(group) : match.Begin(group)));
    } else if constexpr (Kind == PgRegexpKind::Substr) {
      const auto group = static_cast<size_t>(subexpr);
      if (!match.NextNth(n) || group >= match.groups.size() ||
          !match.groups[group].data()) {
        writer.WriteNull();
        continue;
      }
      writer.WriteValue(duckdb::StringVector::AddString(
        result, match.groups[group].data(), match.groups[group].size()));
    } else {
      const auto target = n_arg < arity ? n : (cache.flags.global ? 0 : 1);
      replaced.clear();
      size_t copied = 0;
      for (int32_t count = 1; match.Next(); ++count) {
        if (target != 0 && count != target) {
          continue;
        }
        replaced.append(text, copied, match.Begin(0) - copied);
        AppendPgReplacement(replaced, replacement, match);
        copied = match.End(0);
        if (target != 0) {
          break;
        }
      }
      replaced.append(text, copied);
      writer.WriteValue(duckdb::StringVector::AddString(result, replaced));
    }
  }
}

// regexp_match(text, pattern) -> text[]
// Returns capture groups from the first match. If no capture groups,
// returns the full match. Returns NULL if no match.
// Ported from Velox PgRegexpMatch.
void RegexpMatchFunction(duckdb::DataChunk& args,
                         duckdb::ExpressionState& state,
                         duckdb::Vector& result) {
  const auto count = args.size();
  auto texts = args.data[0].Values<duckdb::string_t>();
  auto patterns = args.data[1].Values<duckdb::string_t>();
  auto rows =
    duckdb::FlatVector::Writer<duckdb::VectorListType<duckdb::string_t>>(result,
                                                                         count);
  auto& cache = duckdb::ExecuteFunctionState::GetFunctionState(state)
                  ->Cast<PgRegexpState>();
  std::optional<duckdb::VectorIterator<duckdb::string_t>> flags;
  if (args.ColumnCount() > 2) {
    flags.emplace(args.data[2]);
  }
  const auto constant = [&](duckdb::idx_t column) {
    return column >= args.ColumnCount() ||
           (args.data[column].GetVectorType() ==
              duckdb::VectorType::CONSTANT_VECTOR &&
            !duckdb::ConstantVector::IsNull(args.data[column]));
  };
  const bool constant_pattern = constant(1) && constant(2);
  const re2::RE2* constant_re = nullptr;

  for (duckdb::idx_t row = 0; row < count; row++) {
    auto text_value = texts[row];
    auto pattern_value = patterns[row];
    if (!text_value.IsValid() || !pattern_value.IsValid() ||
        (flags && !(*flags)[row].IsValid())) {
      rows.WriteNull();
      continue;
    }
    const auto& text = text_value.GetValue();
    const auto& pattern = pattern_value.GetValue();
    const auto& re = constant_re
                       ? *constant_re
                       : cache.Compile(AsView(pattern),
                                       flags ? AsView((*flags)[row].GetValue())
                                             : std::string_view{});
    if (constant_pattern) {
      constant_re = &re;
    }
    if (cache.flags.global) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("regexp_match() does not support the \"global\" option"),
        ERR_HINT("Use the regexp_matches function instead."));
    }
    auto& groups = cache.groups;
    if (!re.Match(re2::StringPiece{text.GetData(), text.GetSize()}, 0,
                  text.GetSize(), re2::RE2::UNANCHORED, groups.data(),
                  static_cast<int>(groups.size()))) {
      rows.WriteNull();
      continue;
    }

    size_t group = groups.size() == 1 ? 0 : 1;
    for (auto& child : rows.WriteList(groups.size() - group)) {
      const auto& captured = groups[group++];
      if (captured.data()) {
        child.WriteValue(duckdb::string_t{
          captured.data(), static_cast<uint32_t>(captured.size())});
      } else {
        child.WriteNull();
      }
    }
  }
}

template<bool Left, bool Right>
std::string_view TrimBytes(std::string_view bytes, std::string_view set) {
  if (set.empty()) {
    return bytes;
  }
  if constexpr (Left) {
    bytes.remove_prefix(std::min(bytes.find_first_not_of(set), bytes.size()));
  }
  if constexpr (Right) {
    bytes = bytes.substr(0, bytes.find_last_not_of(set) + 1);
  }
  return bytes;
}

template<bool Left, bool Right>
void ByteaTrimFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                       duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                  duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t data, duckdb::string_t set) {
      const auto bytes = TrimBytes<Left, Right>(AsView(data), AsView(set));
      return duckdb::StringVector::AddStringOrBlob(result, bytes.data(),
                                                   bytes.size());
    });
}

template<bool Left, bool Right>
void SpaceTrimFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                       duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    args.data[0], result, args.size(), [&](duckdb::string_t text) {
      return duckdb::StringVector::AddString(
        result, TrimBytes<Left, Right>(AsView(text), " "));
    });
}

enum class LikeMatch : uint8_t {
  False,
  True,
  Abort,
};

[[noreturn]] void ThrowLikeEndsWithEscape() {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_ESCAPE_SEQUENCE),
                  ERR_MSG("LIKE pattern must not end with escape character"));
}

LikeMatch MatchBytes(std::string_view text, std::string_view pattern) {
  if (pattern == "%") {
    return LikeMatch::True;
  }
  while (!text.empty() && !pattern.empty()) {
    if (pattern.front() == '%') {
      pattern.remove_prefix(1);
      while (!pattern.empty()) {
        if (pattern.front() == '%') {
          pattern.remove_prefix(1);
        } else if (pattern.front() == '_') {
          if (text.empty()) {
            return LikeMatch::Abort;
          }
          text.remove_prefix(1);
          pattern.remove_prefix(1);
        } else {
          break;
        }
      }
      if (pattern.empty()) {
        return LikeMatch::True;
      }
      if (pattern.front() == '\\' && pattern.size() < 2) {
        ThrowLikeEndsWithEscape();
      }
      const auto first = pattern.front() == '\\' ? pattern[1] : pattern.front();
      for (; !text.empty(); text.remove_prefix(1)) {
        if (text.front() == first) {
          if (const auto matched = MatchBytes(text, pattern);
              matched != LikeMatch::False) {
            return matched;
          }
        }
      }
      return LikeMatch::Abort;
    }
    if (pattern.front() == '\\') {
      pattern.remove_prefix(1);
      if (pattern.empty()) {
        ThrowLikeEndsWithEscape();
      }
      if (pattern.front() != text.front()) {
        return LikeMatch::False;
      }
    } else if (pattern.front() != '_' && pattern.front() != text.front()) {
      return LikeMatch::False;
    }
    text.remove_prefix(1);
    pattern.remove_prefix(1);
  }
  if (!text.empty()) {
    return LikeMatch::False;
  }
  while (!pattern.empty() && pattern.front() == '%') {
    pattern.remove_prefix(1);
  }
  return pattern.empty() ? LikeMatch::True : LikeMatch::Abort;
}

template<bool Negate>
void ByteaLikeFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                       duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t, bool>(
    args.data[0], args.data[1], result, args.size(),
    [](duckdb::string_t data, duckdb::string_t pattern) {
      return (MatchBytes(AsView(data), AsView(pattern)) == LikeMatch::True) !=
             Negate;
    });
}

void Crc32Function(duckdb::DataChunk& args, duckdb::ExpressionState&,
                   duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, int64_t>(
    args.data[0], result, args.size(), [](duckdb::string_t data) {
      return static_cast<int64_t>(crc32_z(
        0, reinterpret_cast<const Bytef*>(data.GetData()), data.GetSize()));
    });
}

void Crc32cFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                    duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, int64_t>(
    args.data[0], result, args.size(), [](duckdb::string_t data) {
      return static_cast<int64_t>(
        static_cast<uint32_t>(absl::ComputeCrc32c(AsView(data))));
    });
}

template<const EVP_MD* (*Digest)()>
void ShaFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                 duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    args.data[0], result, args.size(), [&](duckdb::string_t data) {
      std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
      unsigned int size = 0;
      if (EVP_Digest(data.GetData(), data.GetSize(), digest.data(), &size,
                     Digest(), nullptr) != 1) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INTERNAL_ERROR),
          ERR_MSG("could not compute ", EVP_MD_get0_name(Digest()), " hash"));
      }
      return duckdb::StringVector::AddStringOrBlob(
        result, reinterpret_cast<const char*>(digest.data()), size);
    });
}

constexpr std::string_view kLatin1Ascii =
  R"(  cL Y  "Ca  -R     'u .,      ?AAAAAAACEEEEIIII NOOOOOxOUUUUYTBaaaaaaaceeeeiiii nooooo/ouuuuyty)";
constexpr std::string_view kLatin2Ascii =
  R"( A L LS "SSTZ-ZZ a,l'ls ,sstz"zzRAAAALCCCEEEEIIDDNNOOOOxRUUUUYTBraaaalccceeeeiiddnnoooo/ruuuuyt.)";
constexpr std::string_view kLatin9Ascii =
  R"(  cL YS sCa  -R     Zu .z   EeY?AAAAAAACEEEEIIII NOOOOOxOUUUUYTBaaaaaaaceeeeiiii nooooo/ouuuuyty)";
constexpr std::string_view kWin1250Ascii =
  R"(  ' "    %S<STZZ `'"".--  s>stzz   L A  "CS  -RZ  ,l'u .,as L"lzRAAAALCCCEEEEIIDDNNOOOOxRUUUUYTBraaaalccceeeeiiddnnoooo/ruuuuyt )";

struct AsciiTable {
  std::string_view chars;
  unsigned char first;
};

AsciiTable AsciiTableFor(int32_t encoding) {
  if (encoding == pg::kLatin1Encoding) {
    return {.chars = kLatin1Ascii, .first = 160};
  }
  if (encoding == pg::kLatin2Encoding) {
    return {.chars = kLatin2Ascii, .first = 160};
  }
  if (encoding == pg::kLatin9Encoding) {
    return {.chars = kLatin9Ascii, .first = 160};
  }
  if (encoding == pg::kWin1250Encoding) {
    return {.chars = kWin1250Ascii, .first = 128};
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
    ERR_MSG("encoding conversion from ", pg::EncodingName(encoding),
            " to ASCII not supported"));
}

duckdb::string_t ToAscii(duckdb::Vector& result, duckdb::string_t text,
                         int32_t encoding) {
  const auto table = AsciiTableFor(encoding);
  auto target = duckdb::StringVector::EmptyString(result, text.GetSize());
  auto* out = target.GetDataWriteable();
  for (const auto c : AsView(text)) {
    const auto byte = static_cast<unsigned char>(c);
    *out++ = byte < 128           ? c
             : byte < table.first ? ' '
                                  : table.chars[byte - table.first];
  }
  target.Finalize();
  return target;
}

void ToAsciiFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                     duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    args.data[0], result, args.size(), [&](duckdb::string_t text) {
      return ToAscii(result, text, pg::kUtf8Encoding);
    });
}

void ToAsciiByNameFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                           duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                  duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t text, duckdb::string_t name) {
      const auto encoding = pg::EncodingByName(AsView(name));
      if (encoding < 0) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                        ERR_MSG(AsView(name), " is not a valid encoding name"));
      }
      return ToAscii(result, text, encoding);
    });
}

void ToAsciiByCodeFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                           duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<duckdb::string_t, int32_t, duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t text, int32_t encoding) {
      if (pg::EncodingName(encoding).empty()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                        ERR_MSG(encoding, " is not a valid encoding code"));
      }
      return ToAscii(result, text, encoding);
    });
}

duckdb::utf8proc_option_t NormalFormOptions(std::string_view form) {
  int options = 0;
  if (absl::EqualsIgnoreCase(form, "NFC")) {
    options = duckdb::UTF8PROC_COMPOSE;
  } else if (absl::EqualsIgnoreCase(form, "NFD")) {
    options = duckdb::UTF8PROC_DECOMPOSE;
  } else if (absl::EqualsIgnoreCase(form, "NFKC")) {
    options = duckdb::UTF8PROC_COMPOSE | duckdb::UTF8PROC_COMPAT;
  } else if (absl::EqualsIgnoreCase(form, "NFKD")) {
    options = duckdb::UTF8PROC_DECOMPOSE | duckdb::UTF8PROC_COMPAT;
  } else {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("invalid normalization form: ", form));
  }
  return static_cast<duckdb::utf8proc_option_t>(options |
                                                duckdb::UTF8PROC_STABLE);
}

struct NormalizeState final : duckdb::FunctionLocalState {
  std::vector<utf8proc_int32_t> buffer;

  std::string_view Normalize(std::string_view text,
                             duckdb::utf8proc_option_t options) {
    const auto* data = reinterpret_cast<const utf8proc_uint8_t*>(text.data());
    const auto size = static_cast<utf8proc_ssize_t>(text.size());
    auto length = duckdb::utf8proc_decompose(
      data, size, buffer.data(), static_cast<utf8proc_ssize_t>(buffer.size()),
      options);
    if (length >= static_cast<utf8proc_ssize_t>(buffer.size())) {
      buffer.resize(static_cast<size_t>(length) + 1);
      length =
        duckdb::utf8proc_decompose(data, size, buffer.data(), length, options);
    }
    if (length >= 0) {
      length = duckdb::utf8proc_reencode(buffer.data(), length, options);
    }
    if (length < 0) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                      ERR_MSG("could not normalize string: ",
                              duckdb::utf8proc_errmsg(length)));
    }
    return {reinterpret_cast<const char*>(buffer.data()),
            static_cast<size_t>(length)};
  }
};

duckdb::unique_ptr<duckdb::FunctionLocalState> NormalizeInitLocalState(
  duckdb::ExpressionState&, const duckdb::BoundFunctionExpression&,
  duckdb::FunctionData*) {
  return duckdb::make_uniq<NormalizeState>();
}

template<typename Result>
void NormalizeFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                       duckdb::Vector& result) {
  auto& local = duckdb::ExecuteFunctionState::GetFunctionState(state)
                  ->Cast<NormalizeState>();
  const auto apply = [&](duckdb::string_t text,
                         std::string_view form) -> Result {
    const auto normalized =
      local.Normalize(AsView(text), NormalFormOptions(form));
    if constexpr (std::is_same_v<Result, bool>) {
      return normalized == AsView(text);
    } else {
      return duckdb::StringVector::AddString(result, normalized.data(),
                                             normalized.size());
    }
  };
  if (args.ColumnCount() == 1) {
    duckdb::UnaryExecutor::Execute<duckdb::string_t, Result>(
      args.data[0], result, args.size(),
      [&](duckdb::string_t text) { return apply(text, "NFC"); });
  } else {
    duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t, Result>(
      args.data[0], args.data[1], result, args.size(),
      [&](duckdb::string_t text, duckdb::string_t form) {
        return apply(text, AsView(form));
      });
  }
}

bool IsIdentStart(unsigned char c) {
  return c == '_' || absl::ascii_isalpha(c) || c >= 0x80;
}

bool IsIdentContinuation(unsigned char c) {
  return absl::ascii_isdigit(c) || c == '$' || IsIdentStart(c);
}

bool IsScannerSpace(char c) {
  return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' ||
         c == '\f';
}

[[noreturn]] void ThrowInvalidIdentifier(std::string_view text,
                                         std::string_view detail) {
  if (detail.empty()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("string is not a valid identifier: \"", text, "\""));
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                  ERR_MSG("string is not a valid identifier: \"", text, "\""),
                  ERR_DETAIL(detail));
}

size_t ParseIdent(std::string_view text, bool strict,
                  std::vector<std::string>& parts) {
  size_t count = 0;
  const auto next_part = [&]() -> std::string& {
    if (count == parts.size()) {
      parts.emplace_back();
    }
    auto& part = parts[count++];
    part.clear();
    return part;
  };
  size_t pos = 0;
  const auto skip_spaces = [&] {
    while (pos < text.size() && IsScannerSpace(text[pos])) {
      ++pos;
    }
  };
  skip_spaces();
  bool after_dot = false;
  while (true) {
    const auto c = pos < text.size() ? text[pos] : '\0';
    if (c == '"') {
      auto& part = next_part();
      ++pos;
      while (true) {
        const auto quote = text.find('"', pos);
        if (quote == std::string_view::npos) {
          ThrowInvalidIdentifier(text, "String has unclosed double quotes.");
        }
        part.append(text.substr(pos, quote - pos));
        pos = quote + 1;
        if (pos == text.size() || text[pos] != '"') {
          break;
        }
        part.push_back('"');
        ++pos;
      }
      if (part.empty()) {
        ThrowInvalidIdentifier(text, "Quoted identifier must not be empty.");
      }
    } else if (IsIdentStart(static_cast<unsigned char>(c))) {
      auto& part = next_part();
      for (; pos < text.size() &&
             IsIdentContinuation(static_cast<unsigned char>(text[pos]));
           ++pos) {
        part.push_back(absl::ascii_tolower(text[pos]));
      }
    } else if (c == '.') {
      ThrowInvalidIdentifier(text, "No valid identifier before \".\".");
    } else if (after_dot) {
      ThrowInvalidIdentifier(text, "No valid identifier after \".\".");
    } else {
      ThrowInvalidIdentifier(text, {});
    }
    skip_spaces();
    if (pos == text.size()) {
      break;
    }
    if (text[pos] != '.') {
      if (strict) {
        ThrowInvalidIdentifier(text, {});
      }
      break;
    }
    after_dot = true;
    ++pos;
    skip_spaces();
  }
  return count;
}

void ParseIdentFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                        duckdb::Vector& result) {
  const auto count = args.size();
  auto texts = args.data[0].Values<duckdb::string_t>();
  std::optional<duckdb::VectorIterator<bool>> stricts;
  if (args.ColumnCount() > 1) {
    stricts.emplace(args.data[1]);
  }
  auto rows =
    duckdb::FlatVector::Writer<duckdb::VectorListType<duckdb::string_t>>(result,
                                                                         count);
  std::vector<std::string> parts;
  for (duckdb::idx_t row = 0; row < count; ++row) {
    const auto text = texts[row];
    if (!text.IsValid() || (stricts && !(*stricts)[row].IsValid())) {
      rows.WriteNull();
      continue;
    }
    const auto size = ParseIdent(AsView(text.GetValue()),
                                 !stricts || (*stricts)[row].GetValue(), parts);
    size_t i = 0;
    for (auto& child : rows.WriteList(size)) {
      const auto& part = parts[i++];
      child.WriteValue(
        duckdb::string_t{part.data(), static_cast<uint32_t>(part.size())});
    }
  }
}

bool IsHexDigits(std::string_view text, size_t pos, size_t count) {
  return pos + count <= text.size() &&
         absl::c_all_of(text.substr(pos, count),
                        [](char c) { return absl::ascii_isxdigit(c); });
}

uint32_t HexValue(std::string_view digits) {
  uint32_t value = 0;
  for (const auto c : digits) {
    value =
      value * 16 +
      (absl::ascii_isdigit(c) ? c - '0' : absl::ascii_tolower(c) - 'a' + 10);
  }
  return value;
}

[[noreturn]] void ThrowInvalidSurrogatePair() {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                  ERR_MSG("invalid Unicode surrogate pair"));
}

void Unistr(std::string_view text, std::string& out) {
  uint32_t pair_first = 0;
  const auto append = [&](uint32_t code) {
    if (code == 0 || code > 0x10FFFF) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("invalid Unicode code point: ", absl::StrFormat("%04X", code)));
    }
    const bool second = code >= 0xDC00 && code <= 0xDFFF;
    if (pair_first) {
      if (!second) {
        ThrowInvalidSurrogatePair();
      }
      code = ((pair_first & 0x3FF) << 10) + 0x10000 + (code & 0x3FF);
      pair_first = 0;
    } else if (second) {
      ThrowInvalidSurrogatePair();
    }
    if (code >= 0xD800 && code <= 0xDBFF) {
      pair_first = code;
      return;
    }
    std::array<irs::byte_type, irs::utf8_utils::kMaxCharSize> bytes{};
    const auto size = irs::utf8_utils::FromChar32(code, bytes.data());
    out.append(reinterpret_cast<const char*>(bytes.data()), size);
  };
  for (size_t pos = 0; pos < text.size();) {
    if (text[pos] != '\\') {
      if (pair_first) {
        ThrowInvalidSurrogatePair();
      }
      out.push_back(text[pos++]);
      continue;
    }
    const auto next = pos + 1 < text.size() ? text[pos + 1] : '\0';
    if (next == '\\') {
      if (pair_first) {
        ThrowInvalidSurrogatePair();
      }
      out.push_back('\\');
      pos += 2;
    } else if (IsHexDigits(text, pos + 1, 4)) {
      append(HexValue(text.substr(pos + 1, 4)));
      pos += 5;
    } else if (next == 'u' && IsHexDigits(text, pos + 2, 4)) {
      append(HexValue(text.substr(pos + 2, 4)));
      pos += 6;
    } else if (next == '+' && IsHexDigits(text, pos + 2, 6)) {
      append(HexValue(text.substr(pos + 2, 6)));
      pos += 8;
    } else if (next == 'U' && IsHexDigits(text, pos + 2, 8)) {
      append(HexValue(text.substr(pos + 2, 8)));
      pos += 10;
    } else {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                      ERR_MSG("invalid Unicode escape"),
                      ERR_HINT("Unicode escapes must be \\XXXX, \\+XXXXXX, "
                               "\\uXXXX, or \\UXXXXXXXX."));
    }
  }
  if (pair_first) {
    ThrowInvalidSurrogatePair();
  }
}

void UnistrFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                    duckdb::Vector& result) {
  std::string out;
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    args.data[0], result, args.size(), [&](duckdb::string_t text) {
      out.clear();
      Unistr(AsView(text), out);
      return duckdb::StringVector::AddString(result, out);
    });
}

}  // namespace

std::string LikeEscapePattern(std::string_view pattern, char escape_char) {
  if (escape_char == '\\') {
    return std::string{pattern};
  }
  std::string result;
  result.reserve(pattern.size());
  bool afterescape = false;
  for (char c : pattern) {
    if (c == escape_char && !afterescape) {
      result += '\\';
      afterescape = true;
      continue;
    } else if (c == '\\' && !afterescape) {
      result += '\\';
    }
    result += c;
    afterescape = false;
  }
  return result;
}

bool LikePatternEndsWithEscape(std::string_view pattern) {
  const auto escapes_begin = pattern.find_last_not_of('\\') + 1;
  return (pattern.size() - escapes_begin) % 2 == 1;
}

void RegisterPgStringFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};

  duckdb::ScalarFunction initcap{
    "initcap",
    {duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::VARCHAR,
    InitcapFunction,
  };
  loader.RegisterFunction(initcap);

  // string_to_array(text, delim, null_string) -> text[]
  {
    duckdb::ScalarFunction func{
      "string_to_array",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
      StringToArray3Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }

  // format(text, ...) -> text  (PG-compatible, overrides DuckDB's fmt-style
  // format)
  {
    duckdb::ScalarFunction func{"format",
                                {duckdb::LogicalType::VARCHAR},
                                duckdb::LogicalType::VARCHAR,
                                PgFormatFunction,
                                PgFormatBind};
    func.GetSignature().AddArgs("args", duckdb::LogicalType::ANY);
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    duckdb::CreateScalarFunctionInfo info{std::move(func)};
    info.on_conflict = duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
    loader.RegisterFunction(std::move(info));
  }

  {
    const auto add = [&](std::string_view name,
                         const duckdb::LogicalType& returns,
                         duckdb::scalar_function_t function) {
      duckdb::ScalarFunctionSet set{duckdb::Identifier{name}};
      for (const auto arity : {1, 2}) {
        set.AddFunction(duckdb::ScalarFunction{
          set.name,
          duckdb::vector<duckdb::LogicalType>(arity,
                                              duckdb::LogicalType::VARCHAR),
          returns, function, nullptr, nullptr, NormalizeInitLocalState});
      }
      set.SetFallible();
      loader.RegisterFunction(set);
    };
    add("normalize", duckdb::LogicalType::VARCHAR,
        NormalizeFunction<duckdb::string_t>);
    add("is_normalized", duckdb::LogicalType::BOOLEAN, NormalizeFunction<bool>);
  }

  // encode(bytea, text) -> text
  duckdb::ScalarFunction encode{
    "encode",
    {duckdb::LogicalType::BLOB, duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::VARCHAR,
    EncodeFunction};
  encode.SetFallible();
  loader.RegisterFunction(std::move(encode));

  // decode(text, text) -> bytea
  duckdb::ScalarFunction decode{
    "decode",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BLOB,
    DecodeFunction};
  decode.SetFallible();
  loader.RegisterFunction(std::move(decode));

  // octet_length(varchar) -> bigint
  loader.RegisterFunction(duckdb::ScalarFunction{"octet_length",
                                                 {duckdb::LogicalType::VARCHAR},
                                                 duckdb::LogicalType::BIGINT,
                                                 OctetLengthFunction});
  loader.RegisterFunction(duckdb::ScalarFunction{"length",
                                                 {duckdb::LogicalType::BLOB},
                                                 duckdb::LogicalType::BIGINT,
                                                 OctetLengthFunction});

  // to_bin(int32), to_bin(int64)
  loader.RegisterFunction(duckdb::ScalarFunction{"to_bin",
                                                 {duckdb::LogicalType::INTEGER},
                                                 duckdb::LogicalType::VARCHAR,
                                                 ToBinFunction<int32_t>});
  loader.RegisterFunction(duckdb::ScalarFunction{"to_bin",
                                                 {duckdb::LogicalType::BIGINT},
                                                 duckdb::LogicalType::VARCHAR,
                                                 ToBinFunction<int64_t>});

  // to_oct(int32), to_oct(int64)
  loader.RegisterFunction(duckdb::ScalarFunction{"to_oct",
                                                 {duckdb::LogicalType::INTEGER},
                                                 duckdb::LogicalType::VARCHAR,
                                                 ToOctFunction<int32_t>});
  loader.RegisterFunction(duckdb::ScalarFunction{"to_oct",
                                                 {duckdb::LogicalType::BIGINT},
                                                 duckdb::LogicalType::VARCHAR,
                                                 ToOctFunction<int64_t>});

  loader.RegisterFunction(duckdb::ScalarFunction{
    "position",
    {duckdb::LogicalType::BLOB, duckdb::LogicalType::BLOB},
    duckdb::LogicalType::BIGINT,
    BlobPositionFunction});

  // get_byte(bytea, int) -> int
  loader.RegisterFunction(duckdb::ScalarFunction{
    "get_byte",
    {duckdb::LogicalType::BLOB, duckdb::LogicalType::INTEGER},
    duckdb::LogicalType::INTEGER,
    GetByteFunction});

  // set_byte(bytea, int, int) -> bytea
  loader.RegisterFunction(duckdb::ScalarFunction{
    "set_byte",
    {duckdb::LogicalType::BLOB, duckdb::LogicalType::INTEGER,
     duckdb::LogicalType::INTEGER},
    duckdb::LogicalType::BLOB,
    SetByteFunction});

  // get_bit(bytea, bigint) -> int
  loader.RegisterFunction(duckdb::ScalarFunction{
    "get_bit",
    {duckdb::LogicalType::BLOB, duckdb::LogicalType::BIGINT},
    duckdb::LogicalType::INTEGER,
    GetBitFunction});

  // set_bit(bytea, bigint, int) -> bytea
  loader.RegisterFunction(duckdb::ScalarFunction{
    "set_bit",
    {duckdb::LogicalType::BLOB, duckdb::LogicalType::BIGINT,
     duckdb::LogicalType::INTEGER},
    duckdb::LogicalType::BLOB,
    SetBitFunction});

  for (auto function : {
         duckdb::ScalarFunction{
           "convert",
           {duckdb::LogicalType::BLOB, duckdb::LogicalType::VARCHAR,
            duckdb::LogicalType::VARCHAR},
           duckdb::LogicalType::BLOB,
           ConvertFunction},
         duckdb::ScalarFunction{
           "convert_from",
           {duckdb::LogicalType::BLOB, duckdb::LogicalType::VARCHAR},
           duckdb::LogicalType::VARCHAR,
           ConvertFromFunction},
         duckdb::ScalarFunction{
           "convert_to",
           {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
           duckdb::LogicalType::BLOB,
           ConvertToFunction},
       }) {
    function.SetStability(duckdb::FunctionStability::CONSISTENT_WITHIN_QUERY);
    function.SetFallible();
    loader.RegisterFunction(std::move(function));
  }

  // pg_client_encoding() -> name
  {
    duckdb::ScalarFunction func{
      "pg_client_encoding", {}, pg::NAME(), PgClientEncodingFunction};
    func.SetStability(duckdb::FunctionStability::CONSISTENT_WITHIN_QUERY);
    loader.RegisterFunction(func);
  }

  // quote_ident(text) -> text
  loader.RegisterFunction(duckdb::ScalarFunction{"quote_ident",
                                                 {duckdb::LogicalType::VARCHAR},
                                                 duckdb::LogicalType::VARCHAR,
                                                 QuoteIdentFunction});

  // quote_literal(text) -> text
  loader.RegisterFunction(duckdb::ScalarFunction{"quote_literal",
                                                 {duckdb::LogicalType::VARCHAR},
                                                 duckdb::LogicalType::VARCHAR,
                                                 QuoteLiteralFunction});

  // quote_nullable(text) -> text (returns 'NULL' for NULL input)
  {
    duckdb::ScalarFunction func{"quote_nullable",
                                {duckdb::LogicalType::VARCHAR},
                                duckdb::LogicalType::VARCHAR,
                                QuoteNullableFunction};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }

  // like_escape(pattern, escape) -> varchar
  loader.RegisterFunction(duckdb::ScalarFunction{
    "like_escape",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::VARCHAR,
    LikeEscapeFunction});

  {
    duckdb::ScalarFunctionSet set{"regexp_match"};
    for (const auto arity : {2, 3}) {
      duckdb::ScalarFunction func{
        set.name,
        duckdb::vector<duckdb::LogicalType>(arity,
                                            duckdb::LogicalType::VARCHAR),
        duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
        RegexpMatchFunction,
        nullptr,
        nullptr,
        PgRegexpInitLocalState};
      func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
      set.AddFunction(std::move(func));
    }
    set.SetFallible();
    loader.RegisterFunction(set);
  }

  {
    using Arguments = duckdb::vector<duckdb::LogicalType>;
    const auto text = duckdb::LogicalType::VARCHAR;
    const auto int4 = duckdb::LogicalType::INTEGER;
    const auto add = [&](std::string_view name, size_t required,
                         const Arguments& arguments,
                         const duckdb::LogicalType& returns,
                         duckdb::scalar_function_t function) {
      duckdb::ScalarFunctionSet set{duckdb::Identifier{name}};
      for (auto count = required; count <= arguments.size(); ++count) {
        set.AddFunction(duckdb::ScalarFunction{
          set.name, Arguments(arguments.begin(), arguments.begin() + count),
          returns, function, nullptr, nullptr, PgRegexpInitLocalState});
      }
      loader.RegisterFunction(set);
    };
    add("regexp_count", 2, {text, text, int4, text}, int4,
        PgRegexpFunction<PgRegexpKind::Count>);
    add("regexp_instr", 2, {text, text, int4, int4, int4, text, int4}, int4,
        PgRegexpFunction<PgRegexpKind::Instr>);
    add("regexp_substr", 2, {text, text, int4, int4, text, int4}, text,
        PgRegexpFunction<PgRegexpKind::Substr>);
    add("regexp_replace", 4, {text, text, text, int4, int4, text}, text,
        PgRegexpFunction<PgRegexpKind::Replace>);
  }

  // similar_to_escape -- lives in duckdb (src/function/scalar/string/like.cpp),
  // next to like_escape and to the parser rewrite that emits the call.

  const auto blob = duckdb::LogicalType::BLOB;
  const auto text = duckdb::LogicalType::VARCHAR;
  {
    duckdb::ScalarFunctionSet set{"btrim"};
    set.AddFunction(duckdb::ScalarFunction{
      set.name, {text}, text, SpaceTrimFunction<true, true>});
    for (const auto& function :
         loader.GetFunction(duckdb::Identifier{"trim"}).functions.functions) {
      if (function->GetSignature().GetParameterCount() == 2) {
        set.AddFunction(function);
      }
    }
    set.AddFunction(duckdb::ScalarFunction{
      set.name, {blob, blob}, blob, ByteaTrimFunction<true, true>});
    loader.RegisterFunction(set);
  }

  struct Trim {
    std::string_view name;
    duckdb::scalar_function_t text;
    duckdb::scalar_function_t bytea;
  };
  for (const auto& [name, text_trim, bytea_trim] : {
         Trim{"ltrim", SpaceTrimFunction<true, false>,
              ByteaTrimFunction<true, false>},
         Trim{"rtrim", SpaceTrimFunction<false, true>,
              ByteaTrimFunction<false, true>},
         Trim{"trim", SpaceTrimFunction<true, true>,
              ByteaTrimFunction<true, true>},
       }) {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{name}};
    set.AddFunction(duckdb::ScalarFunction{set.name, {text}, text, text_trim});
    set.AddFunction(
      duckdb::ScalarFunction{set.name, {blob, blob}, blob, bytea_trim});
    loader.RegisterFunction(set);
  }

  struct OperatorAlias {
    std::string_view name;
    std::string_view source;
    duckdb::scalar_function_t bytea;
  };
  for (const auto& [name, source, bytea_like] : {
         OperatorAlias{"like", "~~", ByteaLikeFunction<false>},
         OperatorAlias{"notlike", "!~~", ByteaLikeFunction<true>},
       }) {
    const duckdb::Identifier source_name{source};
    duckdb::ScalarFunction like{
      source_name, {blob, blob}, duckdb::LogicalType::BOOLEAN, bytea_like};
    like.SetFallible();
    loader.RegisterFunction(std::move(like));
    duckdb::ScalarFunctionSet set{duckdb::Identifier{name}};
    for (const auto& function :
         loader.GetFunction(source_name).functions.functions) {
      set.AddFunction(function);
    }
    loader.RegisterFunction(set);
  }

  loader.RegisterFunction(duckdb::ScalarFunction{
    "crc32", {blob}, duckdb::LogicalType::BIGINT, Crc32Function});
  loader.RegisterFunction(duckdb::ScalarFunction{
    "crc32c", {blob}, duckdb::LogicalType::BIGINT, Crc32cFunction});
  loader.RegisterFunction(
    duckdb::ScalarFunction{"sha224", {blob}, blob, ShaFunction<EVP_sha224>});
  loader.RegisterFunction(
    duckdb::ScalarFunction{"sha384", {blob}, blob, ShaFunction<EVP_sha384>});
  loader.RegisterFunction(
    duckdb::ScalarFunction{"sha512", {blob}, blob, ShaFunction<EVP_sha512>});

  {
    duckdb::ScalarFunctionSet set{"to_ascii"};
    set.AddFunction(
      duckdb::ScalarFunction{set.name, {text}, text, ToAsciiFunction});
    set.AddFunction(duckdb::ScalarFunction{
      set.name, {text, text}, text, ToAsciiByNameFunction});
    set.AddFunction(duckdb::ScalarFunction{set.name,
                                           {text, duckdb::LogicalType::INTEGER},
                                           text,
                                           ToAsciiByCodeFunction});
    set.SetFallible();
    loader.RegisterFunction(set);
  }

  {
    duckdb::ScalarFunctionSet set{"parse_ident"};
    for (const auto& arguments : {duckdb::vector<duckdb::LogicalType>{text},
                                  duckdb::vector<duckdb::LogicalType>{
                                    text, duckdb::LogicalType::BOOLEAN}}) {
      duckdb::ScalarFunction func{set.name, arguments,
                                  duckdb::LogicalType::LIST(text),
                                  ParseIdentFunction};
      func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
      set.AddFunction(std::move(func));
    }
    set.SetFallible();
    loader.RegisterFunction(set);
  }

  duckdb::ScalarFunction unistr{"unistr", {text}, text, UnistrFunction};
  unistr.SetFallible();
  loader.RegisterFunction(std::move(unistr));
}

}  // namespace sdb::connector
