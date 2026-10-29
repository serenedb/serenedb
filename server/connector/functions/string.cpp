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
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <fast_float/fast_float.h>
#include <libpq-fe.h>
#include <openssl/evp.h>
#include <re2/re2.h>
#include <simdutf.h>
#include <zlib.h>

#include <array>
#include <duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/common/types/blob.hpp>
#include <duckdb/common/vector_operations/generic_executor.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/scalar/regexp.hpp>
#include <duckdb/function/scalar/string_common.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/utf8_utils.hpp>

#include "connector/common.h"
#include "pg/catalog/oids.h"
#include "pg/serialize.h"
#include "pg/sql_utils.h"
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

struct EncodingName {
  std::string name;
  int32_t encoding = -1;
};

int32_t Encoding(EncodingName& last, duckdb::string_t name,
                 std::string_view role) {
  if (last.encoding < 0 || AsView(name) != last.name) {
    last.name = AsView(name);
    last.encoding = pg_char_to_encoding(last.name.c_str());
    if (last.encoding < 0) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("invalid ", role, " encoding name \"", last.name, "\""));
    }
  }
  return last.encoding;
}

[[noreturn]] void ThrowNoConversion(int32_t from, int32_t to) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
                  ERR_MSG("default conversion function for encoding \"",
                          pg_encoding_to_char(from), "\" to \"",
                          pg_encoding_to_char(to), "\" does not exist"));
}

std::string ByteSequence(std::string_view bytes, size_t length) {
  std::string sequence;
  for (size_t i = 0; i != std::min({length, bytes.size(), size_t{8}}); ++i) {
    absl::StrAppendFormat(&sequence, "%s0x%02x", i == 0 ? "" : " ",
                          static_cast<unsigned char>(bytes[i]));
  }
  return sequence;
}

size_t Utf8SequenceLength(char lead) {
  return std::max<size_t>(
    irs::utf8_utils::LengthFromChar8<0>(static_cast<irs::byte_type>(lead)), 1);
}

void CheckEncoding(int32_t encoding, std::string_view bytes) {
  auto end = std::min(bytes.find('\0'), bytes.size());
  size_t length = 1;
  if (encoding == pg::kUtf8Encoding) {
    if (const auto checked =
          simdutf::validate_utf8_with_errors(bytes.data(), end);
        checked.error != simdutf::error_code::SUCCESS) {
      end = checked.count;
      length = Utf8SequenceLength(bytes[end]);
    }
  }
  if (end != bytes.size()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
                    ERR_MSG("invalid byte sequence for encoding \"",
                            pg_encoding_to_char(encoding),
                            "\": ", ByteSequence(bytes.substr(end), length)));
  }
}

void ConvertFromFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                         duckdb::Vector& result) {
  EncodingName last;
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                  duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t data, duckdb::string_t name) -> duckdb::string_t {
      const auto encoding = Encoding(last, name, "source");
      const auto bytes = AsView(data);
      switch (encoding) {
        case pg::kSqlAsciiEncoding:
          CheckEncoding(encoding, bytes);
          [[fallthrough]];
        case pg::kUtf8Encoding:
          CheckEncoding(pg::kUtf8Encoding, bytes);
          return duckdb::StringVector::AddString(result, data);
        case pg::kLatin1Encoding: {
          CheckEncoding(encoding, bytes);
          auto text = duckdb::StringVector::EmptyString(
            result,
            simdutf::utf8_length_from_latin1(bytes.data(), bytes.size()));
          const auto written = simdutf::convert_latin1_to_utf8(
            bytes.data(), bytes.size(), text.GetDataWriteable());
          SDB_ASSERT(written == text.GetSize());
          text.Finalize();
          return text;
        }
        default:
          ThrowNoConversion(encoding, pg::kUtf8Encoding);
      }
    });
}

void ConvertToFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                       duckdb::Vector& result) {
  EncodingName last;
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                  duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t data, duckdb::string_t name) -> duckdb::string_t {
      const auto encoding = Encoding(last, name, "destination");
      switch (encoding) {
        case pg::kSqlAsciiEncoding:
        case pg::kUtf8Encoding:
          return duckdb::StringVector::AddStringOrBlob(result, data);
        case pg::kLatin1Encoding: {
          const auto text = AsView(data);
          auto bytes = duckdb::StringVector::EmptyString(
            result, simdutf::latin1_length_from_utf8(text.data(), text.size()));
          if (const auto converted =
                simdutf::convert_utf8_to_latin1_with_errors(
                  text.data(), text.size(), bytes.GetDataWriteable());
              converted.error != simdutf::error_code::SUCCESS) {
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_UNTRANSLATABLE_CHARACTER),
              ERR_MSG("character with byte sequence ",
                      ByteSequence(text.substr(converted.count),
                                   Utf8SequenceLength(text[converted.count])),
                      " in encoding \"UTF8\" has no equivalent in encoding "
                      "\"LATIN1\""));
          }
          bytes.Finalize();
          return bytes;
        }
        default:
          ThrowNoConversion(pg::kUtf8Encoding, encoding);
      }
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
      return duckdb::StringVector::AddString(
        result, duckdb::KeywordHelper::WriteQuotedAndEscaped(str, '"'));
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
    if (!str.IsValid() || !delim.IsValid()) {
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
    const std::string_view d{delim.GetValue().GetData(),
                             delim.GetValue().GetSize()};

    parts.clear();
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
      const auto escape_text = AsView(escape);
      if (!escape_text.empty() &&
          irs::utf8_utils::LengthFromChar8<1>(static_cast<irs::byte_type>(
            escape_text.front())) != escape_text.size()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_ESCAPE_SEQUENCE),
          ERR_MSG("invalid escape string"),
          ERR_HINT("Escape string must be empty or one character."));
      }
      return duckdb::StringVector::AddString(
        result, LikeEscapePattern(AsView(pattern), escape_text));
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
struct RegexpBindData : public duckdb::FunctionData {
  std::unique_ptr<re2::RE2> compiled;  // non-null when pattern is constant

  explicit RegexpBindData(std::unique_ptr<re2::RE2> re)
    : compiled(std::move(re)) {}

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    if (compiled) {
      return duckdb::make_uniq<RegexpBindData>(
        std::make_unique<re2::RE2>(compiled->pattern(), compiled->options()));
    }
    return duckdb::make_uniq<RegexpBindData>(nullptr);
  }

  bool Equals(const duckdb::FunctionData& other_p) const final {
    auto& other = other_p.Cast<RegexpBindData>();
    if (!compiled && !other.compiled) {
      return true;
    }
    if (!compiled || !other.compiled) {
      return false;
    }
    return compiled->pattern() == other.compiled->pattern();
  }
};

struct RegexpLocalState final : duckdb::FunctionLocalState {
  explicit RegexpLocalState(const re2::RE2& compiled)
    : re{compiled.pattern(), compiled.options()} {}

  re2::RE2 re;
};

re2::RE2::Options OwnedRegexOptions() {
  re2::RE2::Options options{re2::RE2::Quiet};
  options.set_thread_safe(false);
  options.set_valid_utf8(true);
  return options;
}

duckdb::unique_ptr<duckdb::FunctionLocalState> RegexpInitLocalState(
  duckdb::ExpressionState&, const duckdb::BoundFunctionExpression&,
  duckdb::FunctionData* bind_data) {
  auto& info = bind_data->Cast<RegexpBindData>();
  if (!info.compiled) {
    return nullptr;
  }
  return duckdb::make_uniq<RegexpLocalState>(*info.compiled);
}

std::unique_ptr<re2::RE2> CompileConstantPattern(
  duckdb::BindScalarFunctionInput& input) {
  if (auto val = input.TryGetConstant(1); val && !val->IsNull()) {
    return std::make_unique<re2::RE2>(val->ToString(), OwnedRegexOptions());
  }
  return nullptr;
}

duckdb::unique_ptr<duckdb::FunctionData> RegexpInstrBind(
  duckdb::BindScalarFunctionInput& input) {
  auto re = CompileConstantPattern(input);
  if (re && !re->ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_REGULAR_EXPRESSION),
                    ERR_MSG("invalid regular expression: ", re->error()));
  }
  return duckdb::make_uniq<RegexpBindData>(std::move(re));
}

duckdb::unique_ptr<duckdb::FunctionData> RegexpMatchBind(
  duckdb::BindScalarFunctionInput& input) {
  auto re = CompileConstantPattern(input);
  if (re && !re->ok()) {
    re.reset();
  }
  return duckdb::make_uniq<RegexpBindData>(std::move(re));
}

void RegexpInstrFunction(duckdb::DataChunk& args,
                         duckdb::ExpressionState& state,
                         duckdb::Vector& result) {
  bool has_start_n = args.ColumnCount() == 4;

  auto local = duckdb::ExecuteFunctionState::GetFunctionState(state);
  re2::RE2* compiled = local ? &local->Cast<RegexpLocalState>().re : nullptr;
  const auto row_options = OwnedRegexOptions();

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

    std::optional<re2::RE2> row_re;
    re2::RE2* re = compiled;
    if (!re) {
      row_re.emplace(re2::StringPiece(pattern.GetData(), pattern.GetSize()),
                     row_options);
      if (!row_re->ok()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_REGULAR_EXPRESSION),
          ERR_MSG("invalid regular expression: ", row_re->error()));
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

bool IsCharBoundary(const char* it, const char* end) {
  return it == end || (static_cast<irs::byte_type>(*it) & 0xC0) != 0x80;
}

void VerifyWholeCharacters(re2::StringPiece part, re2::StringPiece input) {
  const auto* end = input.data() + input.size();
  if (IsCharBoundary(part.data(), end) &&
      IsCharBoundary(part.data() + part.size(), end)) [[likely]] {
    return;
  }
  duckdb::regexp_util::VerifyUTF8Result(part.data(), part.size());
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
  auto local = duckdb::ExecuteFunctionState::GetFunctionState(state);
  re2::RE2* compiled = local ? &local->Cast<RegexpLocalState>().re : nullptr;
  const auto row_options = OwnedRegexOptions();
  std::vector<re2::StringPiece> groups;

  for (duckdb::idx_t row = 0; row < count; row++) {
    auto text_value = texts[row];
    auto pattern_value = patterns[row];
    if (!text_value.IsValid() || !pattern_value.IsValid()) {
      rows.WriteNull();
      continue;
    }
    const auto& text = text_value.GetValue();
    std::optional<re2::RE2> row_re;
    re2::RE2* re = compiled;
    if (!re) {
      const auto& pattern = pattern_value.GetValue();
      row_re.emplace(re2::StringPiece(pattern.GetData(), pattern.GetSize()),
                     row_options);
      if (!row_re->ok()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_REGULAR_EXPRESSION),
          ERR_MSG("invalid regular expression: ", row_re->error()));
      }
      re = &*row_re;
    }

    re2::StringPiece input(text.GetData(), text.GetSize());
    const int num_groups = re->NumberOfCapturingGroups();

    if (num_groups == 0) {
      re2::StringPiece match;
      if (!re->Match(input, 0, text.GetSize(), re2::RE2::UNANCHORED, &match,
                     1)) {
        rows.WriteNull();
        continue;
      }
      VerifyWholeCharacters(match, input);
      for (auto& child : rows.WriteList(1)) {
        child.WriteValue(
          duckdb::string_t{match.data(), static_cast<uint32_t>(match.size())});
      }
      continue;
    }

    groups.resize(static_cast<size_t>(num_groups) + 1);
    if (!re->Match(input, 0, text.GetSize(), re2::RE2::UNANCHORED,
                   groups.data(), num_groups + 1)) {
      rows.WriteNull();
      continue;
    }

    int group = 1;
    for (auto& child : rows.WriteList(static_cast<duckdb::idx_t>(num_groups))) {
      if (groups[group].data()) {
        VerifyWholeCharacters(groups[group], input);
        child.WriteValue(duckdb::string_t{
          groups[group].data(), static_cast<uint32_t>(groups[group].size())});
      } else {
        child.WriteNull();
      }
      ++group;
    }
  }
}

std::string_view TrimBytes(std::string_view bytes, std::string_view set) {
  if (set.empty()) {
    return bytes;
  }
  bytes.remove_prefix(std::min(bytes.find_first_not_of(set), bytes.size()));
  return bytes.substr(0, bytes.find_last_not_of(set) + 1);
}

void ByteaTrimFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                       duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                  duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t data, duckdb::string_t set) {
      const auto bytes = TrimBytes(AsView(data), AsView(set));
      return duckdb::StringVector::AddStringOrBlob(result, bytes.data(),
                                                   bytes.size());
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

bool IsIdentStart(unsigned char c) {
  return c == '_' || absl::ascii_isalpha(c) || c >= 0x80;
}

bool IsIdentContinuation(unsigned char c) {
  return absl::ascii_isdigit(c) || c == '$' || IsIdentStart(c);
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
    return parts[count++];
  };
  size_t pos = 0;
  const auto skip_spaces = [&] {
    while (pos < text.size() && absl::ascii_isspace(text[pos])) {
      ++pos;
    }
  };
  skip_spaces();
  bool after_dot = false;
  while (true) {
    const auto c = pos < text.size() ? text[pos] : '\0';
    if (c == '"') {
      auto& part = next_part();
      if (!duckdb::StringUtil::TryParseQuotedString(text, pos, part)) {
        ThrowInvalidIdentifier(text, "String has unclosed double quotes.");
      }
      if (part.empty()) {
        ThrowInvalidIdentifier(text, "Quoted identifier must not be empty.");
      }
    } else if (IsIdentStart(static_cast<unsigned char>(c))) {
      const auto start = pos;
      while (pos < text.size() &&
             IsIdentContinuation(static_cast<unsigned char>(text[pos]))) {
        ++pos;
      }
      next_part() = absl::AsciiStrToLower(text.substr(start, pos - start));
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

std::optional<uint32_t> UnicodeEscape(std::string_view escape,
                                      std::string_view tag, size_t digits) {
  if (!escape.starts_with(tag) || escape.size() < tag.size() + digits) {
    return std::nullopt;
  }
  const auto* begin = escape.data() + tag.size();
  uint32_t code = 0;
  const auto [end, error] =
    fast_float::from_chars(begin, begin + digits, code, 16);
  if (error != std::errc{} || end != begin + digits) {
    return std::nullopt;
  }
  return code;
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
    const auto escape = text.substr(pos + 1);
    if (escape.starts_with('\\')) {
      if (pair_first) {
        ThrowInvalidSurrogatePair();
      }
      out.push_back('\\');
      pos += 2;
    } else if (const auto code = UnicodeEscape(escape, "", 4)) {
      append(*code);
      pos += 5;
    } else if (const auto code = UnicodeEscape(escape, "u", 4)) {
      append(*code);
      pos += 6;
    } else if (const auto code = UnicodeEscape(escape, "+", 6)) {
      append(*code);
      pos += 8;
    } else if (const auto code = UnicodeEscape(escape, "U", 8)) {
      append(*code);
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

std::string LikeEscapePattern(std::string_view pattern,
                              std::string_view escape) {
  if (escape == "\\") {
    return std::string{pattern};
  }
  std::string result;
  result.reserve(pattern.size());
  bool afterescape = false;
  for (size_t i = 0; i != pattern.size();) {
    if (!afterescape && !escape.empty() && pattern[i] == escape.front() &&
        pattern.substr(i).starts_with(escape)) {
      result += '\\';
      i += escape.size();
      afterescape = true;
      continue;
    }
    if (pattern[i] == '\\' && !afterescape) {
      result += '\\';
    }
    result += pattern[i++];
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

  {
    duckdb::ScalarFunction convert_from{
      "convert_from",
      {duckdb::LogicalType::BLOB, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::VARCHAR,
      ConvertFromFunction};
    convert_from.SetFallible();
    loader.RegisterFunction(convert_from);
    duckdb::ScalarFunction convert_to{
      "convert_to",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BLOB,
      ConvertToFunction};
    convert_to.SetFallible();
    loader.RegisterFunction(convert_to);
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

  // regexp_match(text, pattern) -> text[]
  {
    duckdb::ScalarFunction func{
      "regexp_match",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
      RegexpMatchFunction,
      RegexpMatchBind,
      nullptr,
      RegexpInitLocalState};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    func.SetFallible();
    loader.RegisterFunction(func);
  }

  // regexp_count -- implemented as macro in default_functions.cpp

  // regexp_instr(text, pattern) / regexp_instr(text, pattern, start, n)
  {
    duckdb::ScalarFunctionSet regexp_instr_set("regexp_instr");
    duckdb::ScalarFunction instr{
      "regexp_instr",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BIGINT,
      RegexpInstrFunction,
      RegexpInstrBind,
      nullptr,
      RegexpInitLocalState};
    instr.SetFallible();
    regexp_instr_set.AddFunction(instr);
    duckdb::ScalarFunction instr_from{
      "regexp_instr",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT},
      duckdb::LogicalType::BIGINT,
      RegexpInstrFunction,
      RegexpInstrBind,
      nullptr,
      RegexpInitLocalState};
    instr_from.SetFallible();
    regexp_instr_set.AddFunction(instr_from);
    loader.RegisterFunction(regexp_instr_set);
  }

  // regexp_substr, regexp_count -- implemented as macros in
  // default_functions.cpp

  // similar_to_escape -- lives in duckdb (src/function/scalar/string/like.cpp),
  // next to like_escape and to the parser rewrite that emits the call.

  const auto blob = duckdb::LogicalType::BLOB;
  const auto text = duckdb::LogicalType::VARCHAR;
  {
    duckdb::ScalarFunctionSet set{"btrim"};
    for (const auto& function :
         loader.GetFunction(duckdb::Identifier{"trim"}).functions.functions) {
      set.AddFunction(function);
    }
    set.AddFunction(
      duckdb::ScalarFunction{set.name, {blob, blob}, blob, ByteaTrimFunction});
    loader.RegisterFunction(set);
  }

  struct OperatorAlias {
    std::string_view name;
    std::string_view source;
  };
  for (const auto& [name, source] : {
         OperatorAlias{"like", "~~"},
         OperatorAlias{"notlike", "!~~"},
       }) {
    duckdb::ScalarFunctionSet set{duckdb::Identifier{name}};
    for (const auto& function :
         loader.GetFunction(duckdb::Identifier{source}).functions.functions) {
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
