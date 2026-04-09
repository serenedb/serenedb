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

#include <duckdb/common/vector_operations/generic_executor.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include "pg/sql_utils.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "common/keywords.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::connector {

// PostgreSQL initcap: capitalize first letter of each word, lowercase the rest.
// Word boundaries are any non-alphanumeric character.
static void InitcapFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
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
          output[i] = static_cast<char>(new_word ? std::toupper(c)
                                                 : std::tolower(c));
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

// to_bin(int32/int64) -> text — ported from PgToBin
template<typename T>
static void ToBinFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
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

// to_oct(int32/int64) -> text — ported from PgToOct
template<typename T>
static void ToOctFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
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

// get_byte(bytea, offset) -> integer — ported from PgGetByte
static void GetByteFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                            duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<duckdb::string_t, int32_t, int32_t>(
    args.data[0], args.data[1], result, args.size(),
    [](duckdb::string_t data, int32_t offset) -> int32_t {
      if (offset < 0 || offset >= static_cast<int32_t>(data.GetSize())) {
        throw duckdb::InvalidInputException(
          "index %d out of valid range, 0..%d", offset,
          static_cast<int32_t>(data.GetSize()) - 1);
      }
      return static_cast<uint8_t>(data.GetData()[offset]);
    });
}

// set_byte(bytea, offset, value) -> bytea — ported from PgSetByte
static void SetByteFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                            duckdb::Vector& result) {
  duckdb::TernaryExecutor::Execute<duckdb::string_t, int32_t, int32_t,
                                   duckdb::string_t>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t data, int32_t offset, int32_t value)
      -> duckdb::string_t {
      if (offset < 0 || offset >= static_cast<int32_t>(data.GetSize())) {
        throw duckdb::InvalidInputException(
          "index %d out of valid range, 0..%d", offset,
          static_cast<int32_t>(data.GetSize()) - 1);
      }
      auto target =
        duckdb::StringVector::EmptyString(result, data.GetSize());
      auto out = target.GetDataWriteable();
      memcpy(out, data.GetData(), data.GetSize());
      out[offset] = static_cast<char>(value & 0xff);
      target.Finalize();
      return target;
    });
}

// convert_from(bytea, encoding) -> text — ported from PgConvertFrom
static void ConvertFromFunction(duckdb::DataChunk& args,
                                duckdb::ExpressionState&,
                                duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                  duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t data, duckdb::string_t encoding)
      -> duckdb::string_t {
      std::string_view enc(encoding.GetData(), encoding.GetSize());
      if (enc != "UTF8" && enc != "UTF-8" && enc != "utf8" &&
          enc != "utf-8") {
        throw duckdb::InvalidInputException(
          "conversion from %s is not supported", std::string{enc});
      }
      return duckdb::StringVector::AddString(result, data);
    });
}

// convert_to(text, encoding) -> bytea — ported from PgConvertTo
static void ConvertToFunction(duckdb::DataChunk& args,
                              duckdb::ExpressionState&,
                              duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                  duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t data, duckdb::string_t encoding)
      -> duckdb::string_t {
      std::string_view enc(encoding.GetData(), encoding.GetSize());
      if (enc != "UTF8" && enc != "UTF-8" && enc != "utf8" &&
          enc != "utf-8") {
        throw duckdb::InvalidInputException(
          "conversion to %s is not supported", std::string{enc});
      }
      return duckdb::StringVector::AddStringOrBlob(result, data);
    });
}

// pg_client_encoding() -> name — ported from PgClientEncoding
static void PgClientEncodingFunction(duckdb::DataChunk&,
                                     duckdb::ExpressionState&,
                                     duckdb::Vector& result) {
  result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
  duckdb::ConstantVector::GetData<duckdb::string_t>(result)[0] =
    duckdb::StringVector::AddString(result, "UTF8");
}

// quote_ident(text) -> text — ported from QuoteIdentFunction
// Uses libpg_query's ScanKeywordLookup for reserved word detection.
static void QuoteIdentFunction(duckdb::DataChunk& args,
                               duckdb::ExpressionState&,
                               duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    args.data[0], result, args.size(),
    [&](duckdb::string_t input) -> duckdb::string_t {
      std::string_view str(input.GetData(), input.GetSize());
      bool needs_quoting = str.empty() || (str[0] >= '0' && str[0] <= '9');
      if (!needs_quoting) {
        for (auto c : str) {
          if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
                c == '_')) {
            needs_quoting = true;
            break;
          }
        }
      }
      if (!needs_quoting) {
        std::string null_terminated{str};
        int kwnum =
          ScanKeywordLookup(null_terminated.c_str(), &ScanKeywords);
        if (kwnum >= 0 &&
            ScanKeywordCategories[kwnum] != UNRESERVED_KEYWORD) {
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

void RegisterPgStringFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};

  duckdb::ScalarFunction initcap{
    "initcap",
    {duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::VARCHAR,
    InitcapFunction,
  };
  loader.RegisterFunction(initcap);

  // to_bin(int32), to_bin(int64)
  loader.RegisterFunction(duckdb::ScalarFunction{
    "to_bin", {duckdb::LogicalType::INTEGER}, duckdb::LogicalType::VARCHAR,
    ToBinFunction<int32_t>});
  loader.RegisterFunction(duckdb::ScalarFunction{
    "to_bin", {duckdb::LogicalType::BIGINT}, duckdb::LogicalType::VARCHAR,
    ToBinFunction<int64_t>});

  // to_oct(int32), to_oct(int64)
  loader.RegisterFunction(duckdb::ScalarFunction{
    "to_oct", {duckdb::LogicalType::INTEGER}, duckdb::LogicalType::VARCHAR,
    ToOctFunction<int32_t>});
  loader.RegisterFunction(duckdb::ScalarFunction{
    "to_oct", {duckdb::LogicalType::BIGINT}, duckdb::LogicalType::VARCHAR,
    ToOctFunction<int64_t>});

  // get_byte(bytea, int) -> int
  loader.RegisterFunction(duckdb::ScalarFunction{
    "get_byte",
    {duckdb::LogicalType::BLOB, duckdb::LogicalType::INTEGER},
    duckdb::LogicalType::INTEGER, GetByteFunction});

  // set_byte(bytea, int, int) -> bytea
  loader.RegisterFunction(duckdb::ScalarFunction{
    "set_byte",
    {duckdb::LogicalType::BLOB, duckdb::LogicalType::INTEGER,
     duckdb::LogicalType::INTEGER},
    duckdb::LogicalType::BLOB, SetByteFunction});

  // convert_from(bytea, encoding) -> text
  loader.RegisterFunction(duckdb::ScalarFunction{
    "convert_from",
    {duckdb::LogicalType::BLOB, duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::VARCHAR, ConvertFromFunction});

  // convert_to(text, encoding) -> bytea
  loader.RegisterFunction(duckdb::ScalarFunction{
    "convert_to",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BLOB, ConvertToFunction});

  // pg_client_encoding() -> text
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_client_encoding", {}, duckdb::LogicalType::VARCHAR,
    PgClientEncodingFunction});

  // quote_ident(text) -> text
  loader.RegisterFunction(duckdb::ScalarFunction{
    "quote_ident", {duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::VARCHAR, QuoteIdentFunction});

}

}  // namespace sdb::connector
