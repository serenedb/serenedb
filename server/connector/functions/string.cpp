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

#include <re2/re2.h>

#include <duckdb/common/types/blob.hpp>
#include <duckdb/common/vector_operations/generic_executor.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>

#include "connector/pg_logical_types.h"
#include "iresearch/utils/utf8_utils.hpp"
#include "pg/errcodes.h"
#include "pg/serialize.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"

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

// to_hex(int32/int64) -> text -- PG-compatible lowercase hex
template<typename T>
void ToHexFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                   duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<T, duckdb::string_t>(
    args.data[0], result, args.size(), [&](T value) -> duckdb::string_t {
      using U = std::make_unsigned_t<T>;
      U uval = static_cast<U>(value);
      if (uval == 0) {
        return duckdb::StringVector::AddString(result, "0");
      }
      char buf[sizeof(U) * 2];
      int pos = sizeof(buf);
      while (uval > 0) {
        static constexpr char kHexDigits[] = "0123456789abcdef";
        buf[--pos] = kHexDigits[uval & 0xf];
        uval >>= 4;
      }
      return duckdb::StringVector::AddString(result, buf + pos,
                                             sizeof(buf) - pos);
    });
}

// get_byte(bytea, offset) -> integer -- ported from PgGetByte
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

// convert_from(bytea, encoding) -> text -- ported from PgConvertFrom
void ConvertFromFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                         duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                  duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t data, duckdb::string_t encoding) -> duckdb::string_t {
      std::string_view enc(encoding.GetData(), encoding.GetSize());
      if (enc != "UTF8" && enc != "UTF-8" && enc != "utf8" && enc != "utf-8") {
        throw duckdb::InvalidInputException(
          "conversion from %s is not supported", std::string{enc});
      }
      return duckdb::StringVector::AddString(result, data);
    });
}

// convert_to(text, encoding) -> bytea -- ported from PgConvertTo
void ConvertToFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                       duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                  duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t data, duckdb::string_t encoding) -> duckdb::string_t {
      std::string_view enc(encoding.GetData(), encoding.GetSize());
      if (enc != "UTF8" && enc != "UTF-8" && enc != "utf8" && enc != "utf-8") {
        throw duckdb::InvalidInputException("conversion to %s is not supported",
                                            std::string{enc});
      }
      return duckdb::StringVector::AddStringOrBlob(result, data);
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
      std::string_view str(input.GetData(), input.GetSize());
      bool needs_quoting = str.empty() || (str[0] >= '0' && str[0] <= '9');
      if (!needs_quoting) {
        for (auto c : str) {
          if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_')) {
            needs_quoting = true;
            break;
          }
        }
      }
      if (!needs_quoting) {
        if (duckdb::KeywordHelper::IsKeyword(std::string{str})) {
          needs_quoting = true;
        }
      }
      if (!needs_quoting) {
        return input;
      }
      std::string out;
      out.reserve(str.size() + 2);
      out += '"';
      for (auto c : str) {
        if (c == '"') {
          out += "\"\"";
        } else {
          out += c;
        }
      }
      out += '"';
      return duckdb::StringVector::AddString(result, out);
    });
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
  auto count = args.size();
  auto& str_data = args.data[0];
  auto& delim_data = args.data[1];
  auto& null_str_data = args.data[2];

  duckdb::UnifiedVectorFormat str_fmt, delim_fmt, null_str_fmt;
  str_data.ToUnifiedFormat(count, str_fmt);
  delim_data.ToUnifiedFormat(count, delim_fmt);
  null_str_data.ToUnifiedFormat(count, null_str_fmt);

  auto& list_validity = duckdb::FlatVector::ValidityMutable(result);
  auto list_entries =
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto str_idx = str_fmt.sel->get_index(i);
    auto delim_idx = delim_fmt.sel->get_index(i);
    auto null_str_idx = null_str_fmt.sel->get_index(i);

    if (!str_fmt.validity.RowIsValid(str_idx) ||
        !delim_fmt.validity.RowIsValid(delim_idx)) {
      list_validity.SetInvalid(i);
      continue;
    }

    auto str =
      duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(str_fmt)[str_idx];
    auto delim = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(
      delim_fmt)[delim_idx];
    bool has_null_str = null_str_fmt.validity.RowIsValid(null_str_idx);
    duckdb::string_t null_str;
    std::string_view ns;
    if (has_null_str) {
      null_str = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(
        null_str_fmt)[null_str_idx];
      ns = {null_str.GetData(), null_str.GetSize()};
    }

    std::string_view s{str.GetData(), str.GetSize()};
    std::string_view d{delim.GetData(), delim.GetSize()};

    auto& child = duckdb::ListVector::GetEntry(result);
    auto current_size = duckdb::ListVector::GetListSize(result);

    std::vector<std::string_view> parts;
    if (d.empty()) {
      parts.push_back(s);
    } else {
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

    duckdb::ListVector::Reserve(result, current_size + parts.size());
    auto& child_validity = duckdb::FlatVector::ValidityMutable(child);
    auto child_data =
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(child);
    for (size_t j = 0; j < parts.size(); j++) {
      if (has_null_str && parts[j] == ns) {
        child_validity.SetInvalid(current_size + j);
        child_data[current_size + j] = duckdb::string_t();
      } else {
        child_data[current_size + j] = duckdb::StringVector::AddString(
          child, parts[j].data(), parts[j].size());
      }
    }
    duckdb::ListVector::SetListSize(result, current_size + parts.size());
    list_entries[i] = {current_size, parts.size()};
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
      throw duckdb::InvalidInputException("unrecognized encoding: \"%s\"",
                                          std::string{format});
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
            throw duckdb::InvalidInputException("invalid hexadecimal data");
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
      throw duckdb::InvalidInputException("unrecognized encoding: \"%s\"",
                                          std::string{format});
    });
}

// normalize(text [, form]) -> text: Unicode normalization
// TODO: call ICU nfc_normalize when available
void NormalizeFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                       duckdb::Vector& result) {
  // Both 1-arg and 2-arg forms just do NFC (the only form we support for now).
  // The 2-arg form accepts the form name but currently ignores it since
  // the implementation only does identity for ASCII / NFC passthrough.
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    args.data[0], result, args.size(),
    [&](duckdb::string_t input) -> duckdb::string_t {
      return duckdb::StringVector::AddString(result, input);
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
        throw duckdb::InvalidInputException("too few arguments for format()");
      }
      auto idx = vdata[arg_idx].sel->get_index(row);
      if (!vdata[arg_idx].validity.RowIsValid(idx)) {
        return std::nullopt;
      }
      auto val = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(
        vdata[arg_idx])[idx];
      return std::string{val.GetData(), val.GetSize()};
    };

    // Helper: quote identifier (same as quote_ident)
    auto quote_ident = [](const std::string& s) -> std::string {
      bool needs_quoting = s.empty();
      if (!needs_quoting) {
        for (auto c : s) {
          if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_')) {
            needs_quoting = true;
            break;
          }
        }
      }
      if (!needs_quoting) {
        return s;
      }
      std::string out = "\"";
      for (auto c : s) {
        if (c == '"') {
          out += "\"\"";
        } else {
          out += c;
        }
      }
      out += '"';
      return out;
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
        throw duckdb::InvalidInputException(
          "unterminated format() conversion specifier");
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
        arg_pos = std::stoul(std::string{fmt.substr(save_i, i - save_i)});
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
        throw duckdb::InvalidInputException(
          "unterminated format() conversion specifier");
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
            throw duckdb::InvalidInputException(
              "null values cannot be formatted as an SQL identifier");
          }
          formatted = quote_ident(*val);
        } break;
        case 'L': {
          auto val = get_arg(arg_idx);
          formatted = quote_literal(val);
          break;
        }
        default:
          throw duckdb::InvalidInputException(
            "unrecognized format() type specifier \"%c\"", spec);
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
  auto& input = args.data[0];
  auto count = args.size();
  duckdb::UnifiedVectorFormat input_data;
  input.ToUnifiedFormat(count, input_data);
  auto* result_data =
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto idx = input_data.sel->get_index(i);
    if (!input_data.validity.RowIsValid(idx)) {
      result_data[i] = duckdb::StringVector::AddString(result, "NULL");
      continue;
    }
    auto str =
      duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(input_data)[idx]
        .GetString();
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
    result_data[i] = duckdb::StringVector::AddString(result, quoted);
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

void SimilarToEscapeFunction2(duckdb::DataChunk& args, duckdb::ExpressionState&,
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
      auto out = SimilarToEscapePattern({pattern.GetData(), pattern.GetSize()},
                                        esc_sv[0]);
      return duckdb::StringVector::AddString(result, out);
    });
}

void SimilarToEscapeFunction1(duckdb::DataChunk& args, duckdb::ExpressionState&,
                              duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    args.data[0], result, args.size(),
    [&](duckdb::string_t pattern) -> duckdb::string_t {
      auto out =
        SimilarToEscapePattern({pattern.GetData(), pattern.GetSize()}, '\\');
      return duckdb::StringVector::AddString(result, out);
    });
}

// ---------------------------------------------------------------------------
// UTF-8 helpers for regexp_instr (same as server/pg/functions/regexp.cpp)
// ---------------------------------------------------------------------------

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
  for (size_t i = 0; i < n && it < end; ++i) {
    it = irs::utf8_utils::Next(it, end);
  }
  return static_cast<size_t>(it -
                             reinterpret_cast<const irs::byte_type*>(begin));
}

// ---------------------------------------------------------------------------
// regexp_instr -- no DuckDB equivalent, needs C++
// Ported from Velox PgRegexpInstr / PgRegexpInstr4.
// ---------------------------------------------------------------------------

// Bind data: caches the compiled RE2 when the pattern is a constant.
struct RegexpInstrBindData : public duckdb::FunctionData {
  std::unique_ptr<re2::RE2> compiled;  // non-null when pattern is constant

  explicit RegexpInstrBindData(std::unique_ptr<re2::RE2> re)
    : compiled(std::move(re)) {}

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    if (compiled) {
      return duckdb::make_uniq<RegexpInstrBindData>(
        std::make_unique<re2::RE2>(compiled->pattern(), compiled->options()));
    }
    return duckdb::make_uniq<RegexpInstrBindData>(nullptr);
  }

  bool Equals(const duckdb::FunctionData& other_p) const final {
    auto& other = other_p.Cast<RegexpInstrBindData>();
    if (!compiled && !other.compiled) {
      return true;
    }
    if (!compiled || !other.compiled) {
      return false;
    }
    return compiled->pattern() == other.compiled->pattern();
  }
};

duckdb::unique_ptr<duckdb::FunctionData> RegexpInstrBind(
  duckdb::BindScalarFunctionInput& input) {
  auto& context = input.GetClientContext();
  auto& arguments = input.GetArguments();
  // Check if pattern argument is a constant
  if (arguments[1]->IsFoldable()) {
    auto val =
      duckdb::ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
    if (!val.IsNull()) {
      auto pattern_str = val.ToString();
      auto re = std::make_unique<re2::RE2>(pattern_str, re2::RE2::Quiet);
      if (!re->ok()) {
        throw duckdb::InvalidInputException("invalid regular expression: %s",
                                            re->error());
      }
      return duckdb::make_uniq<RegexpInstrBindData>(std::move(re));
    }
  }
  return duckdb::make_uniq<RegexpInstrBindData>(nullptr);
}

void RegexpInstrFunction(duckdb::DataChunk& args,
                         duckdb::ExpressionState& state,
                         duckdb::Vector& result) {
  bool has_start_n = args.ColumnCount() == 4;

  auto& func_expr = state.expr.Cast<duckdb::BoundFunctionExpression>();
  auto& info = func_expr.BindInfo()->Cast<RegexpInstrBindData>();

  std::vector<duckdb::UnifiedVectorFormat> vdata(args.ColumnCount());
  for (duckdb::idx_t c = 0; c < args.ColumnCount(); c++) {
    args.data[c].ToUnifiedFormat(args.size(), vdata[c]);
  }

  auto result_data = duckdb::FlatVector::GetDataMutable<int64_t>(result);

  for (duckdb::idx_t row = 0; row < args.size(); row++) {
    auto text_idx = vdata[0].sel->get_index(row);
    auto pat_idx = vdata[1].sel->get_index(row);
    if (!vdata[0].validity.RowIsValid(text_idx) ||
        !vdata[1].validity.RowIsValid(pat_idx)) {
      duckdb::FlatVector::ValidityMutable(result).SetInvalid(row);
      continue;
    }

    auto text = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(
      vdata[0])[text_idx];
    auto pattern =
      duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(vdata[1])[pat_idx];

    int64_t start_char = 1;
    int64_t n = 1;
    if (has_start_n) {
      auto s_idx = vdata[2].sel->get_index(row);
      auto n_idx = vdata[3].sel->get_index(row);
      if (!vdata[2].validity.RowIsValid(s_idx) ||
          !vdata[3].validity.RowIsValid(n_idx)) {
        duckdb::FlatVector::ValidityMutable(result).SetInvalid(row);
        continue;
      }
      start_char =
        duckdb::UnifiedVectorFormat::GetData<int64_t>(vdata[2])[s_idx];
      n = duckdb::UnifiedVectorFormat::GetData<int64_t>(vdata[3])[n_idx];
    }

    // Use precompiled from bind data, or compile per-row
    std::optional<re2::RE2> row_re;
    re2::RE2* re = info.compiled.get();
    if (!re) {
      row_re.emplace(re2::StringPiece(pattern.GetData(), pattern.GetSize()),
                     re2::RE2::Quiet);
      if (!row_re->ok()) {
        throw duckdb::InvalidInputException("invalid regular expression: %s",
                                            row_re->error());
      }
      re = &*row_re;
    }

    re2::StringPiece input(text.GetData(), text.GetSize());
    re2::StringPiece match;
    size_t pos = start_char > 1
                   ? Utf8Advance(text.GetData(), text.GetSize(),
                                 static_cast<size_t>(start_char - 1))
                   : 0;
    int64_t count = 0;

    while (
      re->Match(input, pos, text.GetSize(), re2::RE2::UNANCHORED, &match, 1)) {
      ++count;
      if (count == n) {
        size_t byte_offset = match.data() - input.data();
        result_data[row] =
          static_cast<int64_t>(Utf8CharCount(text.GetData(), byte_offset)) + 1;
        goto next_row;
      }
      pos = match.data() + match.size() - input.data();
      if (match.size() == 0) {
        pos = Utf8NextCharPos(text.GetData(), text.GetSize(), pos);
      }
    }
    result_data[row] = 0;
  next_row:;
  }
}

// regexp_match(text, pattern) -> text[]
// Returns capture groups from the first match. If no capture groups,
// returns the full match. Returns NULL if no match.
// Ported from Velox PgRegexpMatch.
void RegexpMatchFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                         duckdb::Vector& result) {
  duckdb::UnifiedVectorFormat tdata, pdata;
  args.data[0].ToUnifiedFormat(tdata);
  args.data[1].ToUnifiedFormat(pdata);
  const auto* text_ptr =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(tdata);
  const auto* pat_ptr =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(pdata);
  auto* result_ptr =
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);
  for (duckdb::idx_t row = 0; row < args.size(); row++) {
    auto t_idx = tdata.sel->get_index(row);
    auto p_idx = pdata.sel->get_index(row);
    if (!tdata.validity.RowIsValid(t_idx) ||
        !pdata.validity.RowIsValid(p_idx)) {
      result_validity.SetInvalid(row);
      continue;
    }
    const auto& text = text_ptr[t_idx];
    const auto& pattern = pat_ptr[p_idx];
    re2::RE2 re(re2::StringPiece(pattern.GetData(), pattern.GetSize()),
                re2::RE2::Quiet);
    if (!re.ok()) {
      throw duckdb::InvalidInputException("invalid regular expression: %s",
                                          re.error());
    }

    re2::StringPiece input(text.GetData(), text.GetSize());
    int num_groups = re.NumberOfCapturingGroups();

    auto& child = duckdb::ListVector::GetChildMutable(result);
    auto current_size = duckdb::ListVector::GetListSize(result);

    if (num_groups == 0) {
      re2::StringPiece match;
      if (!re.Match(input, 0, text.GetSize(), re2::RE2::UNANCHORED, &match,
                    1)) {
        result_validity.SetInvalid(row);
        continue;
      }
      duckdb::ListVector::Reserve(result, current_size + 1);
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(
        child)[current_size] =
        duckdb::StringVector::AddString(child, match.data(), match.size());
      duckdb::ListVector::SetListSize(result, current_size + 1);
      result_ptr[row] = duckdb::list_entry_t{current_size, 1};
      continue;
    }

    std::vector<re2::StringPiece> groups(num_groups + 1);
    if (!re.Match(input, 0, text.GetSize(), re2::RE2::UNANCHORED, groups.data(),
                  num_groups + 1)) {
      result_validity.SetInvalid(row);
      continue;
    }

    duckdb::ListVector::Reserve(result, current_size + num_groups);
    auto& child_validity = duckdb::FlatVector::ValidityMutable(child);
    for (int i = 1; i <= num_groups; ++i) {
      if (groups[i].data()) {
        duckdb::FlatVector::GetDataMutable<duckdb::string_t>(
          child)[current_size + i - 1] =
          duckdb::StringVector::AddString(child, groups[i].data(),
                                          groups[i].size());
      } else {
        child_validity.SetInvalid(current_size + i - 1);
      }
    }
    duckdb::ListVector::SetListSize(result, current_size + num_groups);
    result_ptr[row] = duckdb::list_entry_t{
      current_size, static_cast<duckdb::idx_t>(num_groups)};
  }
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

std::string SimilarToEscapePattern(std::string_view pattern, char escape_char) {
  std::string result;
  result.reserve(pattern.size() + 6);

  bool afterescape = false;
  int nquotes = 0;
  int bracket_depth = 0;
  int charclass_pos = 0;

  result += "^(?:";

  for (size_t i = 0; i < pattern.size(); ++i) {
    char pchar = pattern[i];

    if (afterescape) {
      if (pchar == '"' && bracket_depth < 1) {
        if (nquotes == 0) {
          result += "){1,1}?(";
        } else if (nquotes == 1) {
          result += "){1,1}(?:";
        } else {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_INVALID_USE_OF_ESCAPE_CHARACTER),
            ERR_MSG("SQL regular expression may not contain more than "
                    "two escape-double-quote separators"));
        }
        nquotes++;
      } else {
        result += '\\';
        result += pchar;
        charclass_pos = 3;
      }
      afterescape = false;
    } else if (pchar == escape_char) {
      afterescape = true;
    } else if (bracket_depth > 0) {
      if (pchar == '\\') {
        result += '\\';
      }
      result += pchar;
      if (pchar == ']' && charclass_pos > 2) {
        bracket_depth--;
      } else if (pchar == '[') {
        bracket_depth++;
        charclass_pos = 3;
      } else if (pchar == '^') {
        charclass_pos++;
      } else {
        charclass_pos = 3;
      }
    } else if (pchar == '[') {
      result += pchar;
      bracket_depth = 1;
      charclass_pos = 1;
    } else if (pchar == '%') {
      result += ".*";
    } else if (pchar == '_') {
      result += '.';
    } else if (pchar == '(') {
      result += "(?:";
    } else if (pchar == '\\' || pchar == '.' || pchar == '^' || pchar == '$') {
      result += '\\';
      result += pchar;
    } else {
      result += pchar;
    }
  }

  result += ")$";
  return result;
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
    func.SetVarArgs(duckdb::LogicalType::ANY);
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }

  // normalize(text [, form]) -> text
  {
    duckdb::ScalarFunctionSet normalize_set("normalize");
    normalize_set.AddFunction(
      duckdb::ScalarFunction{"normalize",
                             {duckdb::LogicalType::VARCHAR},
                             duckdb::LogicalType::VARCHAR,
                             NormalizeFunction});
    normalize_set.AddFunction(duckdb::ScalarFunction{
      "normalize",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::VARCHAR,
      NormalizeFunction});
    loader.RegisterFunction(normalize_set);
  }

  // encode(bytea, text) -> text
  loader.RegisterFunction(duckdb::ScalarFunction{
    "encode",
    {duckdb::LogicalType::BLOB, duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::VARCHAR,
    EncodeFunction});

  // decode(text, text) -> bytea
  loader.RegisterFunction(duckdb::ScalarFunction{
    "decode",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BLOB,
    DecodeFunction});

  // octet_length(varchar) -> bigint
  loader.RegisterFunction(duckdb::ScalarFunction{"octet_length",
                                                 {duckdb::LogicalType::VARCHAR},
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

  // to_hex(int32), to_hex(int64)
  loader.RegisterFunction(duckdb::ScalarFunction{"to_hex",
                                                 {duckdb::LogicalType::INTEGER},
                                                 duckdb::LogicalType::VARCHAR,
                                                 ToHexFunction<int32_t>});
  loader.RegisterFunction(duckdb::ScalarFunction{"to_hex",
                                                 {duckdb::LogicalType::BIGINT},
                                                 duckdb::LogicalType::VARCHAR,
                                                 ToHexFunction<int64_t>});

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

  // convert_from(bytea, encoding) -> text
  loader.RegisterFunction(duckdb::ScalarFunction{
    "convert_from",
    {duckdb::LogicalType::BLOB, duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::VARCHAR,
    ConvertFromFunction});

  // convert_to(text, encoding) -> bytea
  loader.RegisterFunction(duckdb::ScalarFunction{
    "convert_to",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BLOB,
    ConvertToFunction});

  // pg_client_encoding() -> name
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_client_encoding", {}, pg::NAME(), PgClientEncodingFunction});

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

  // regexp_match(text, pattern) -> text[]
  {
    duckdb::ScalarFunction func{
      "regexp_match",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
      RegexpMatchFunction};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }

  // regexp_count -- implemented as macro in default_functions.cpp

  // regexp_instr(text, pattern) / regexp_instr(text, pattern, start, n)
  {
    duckdb::ScalarFunctionSet regexp_instr_set("regexp_instr");
    regexp_instr_set.AddFunction(duckdb::ScalarFunction{
      "regexp_instr",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BIGINT,
      RegexpInstrFunction,
      RegexpInstrBind});
    regexp_instr_set.AddFunction(duckdb::ScalarFunction{
      "regexp_instr",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT},
      duckdb::LogicalType::BIGINT,
      RegexpInstrFunction,
      RegexpInstrBind});
    loader.RegisterFunction(regexp_instr_set);
  }

  // regexp_substr, regexp_count -- implemented as macros in
  // default_functions.cpp

  // similar_to_escape(pattern, escape) -> varchar
  loader.RegisterFunction(duckdb::ScalarFunction{
    "similar_to_escape",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::VARCHAR,
    SimilarToEscapeFunction2});

  // similar_to_escape(pattern) -> varchar (default escape = '\')
  loader.RegisterFunction(duckdb::ScalarFunction{"similar_to_escape",
                                                 {duckdb::LogicalType::VARCHAR},
                                                 duckdb::LogicalType::VARCHAR,
                                                 SimilarToEscapeFunction1});
}

}  // namespace sdb::connector
