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

#include "pg/types/bytea.h"

#include <absl/strings/ascii.h>
#include <absl/strings/escaping.h>
#include <fast_float/fast_float.h>

#include <algorithm>
#include <duckdb/common/string_util.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/main/client_context.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <string>
#include <utf8proc_wrapper.hpp>

#include "pg/types/cast.h"

namespace sdb::pg {
namespace {

irs::pg::SqlErrorData InvalidHexDigit(std::string_view text) {
  int length = 1;
  duckdb::Utf8Proc::UTF8ToCodepoint(text.data(), length, text.size());
  return SQL_ERROR_DATA(
    ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
    ERR_MSG("invalid hexadecimal digit: \"",
            text.substr(0, static_cast<size_t>(std::max(length, 1))), "\""));
}

irs::pg::SqlErrorData InvalidByteaSyntax() {
  return SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        ERR_MSG("invalid input syntax for type bytea"));
}

std::optional<irs::pg::SqlErrorData> ByteaIn(std::string_view input,
                                             duckdb::Vector& result_vec,
                                             duckdb::string_t& bytes) {
  if (input.starts_with("\\x")) {
    std::string_view payload{input.begin() + 2, input.end()};
    auto target =
      duckdb::StringVector::EmptyString(result_vec, payload.size() / 2);
    char* out = target.GetDataWriteable();

    for (size_t i = 0; i < payload.size();) {
      char c = payload[i];
      if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
        ++i;
        continue;
      }
      const auto h1 = absl::kHexValueStrict[static_cast<unsigned char>(c)];
      if (h1 == -1) {
        return InvalidHexDigit(payload.substr(i));
      }
      if (i + 1 >= payload.size()) {
        return SQL_ERROR_DATA(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("invalid hexadecimal data: odd number of digits"));
      }
      const auto h2 =
        absl::kHexValueStrict[static_cast<unsigned char>(payload[i + 1])];
      if (h2 == -1) {
        return InvalidHexDigit(payload.substr(i + 1));
      }
      *out++ = static_cast<char>((h1 << 4) + h2);
      i += 2;
    }
    auto new_size = static_cast<duckdb::idx_t>(out - target.GetDataWriteable());
    target.Finalize();
    bytes = duckdb::StringVector::AddStringOrBlob(
      result_vec, duckdb::string_t(target.GetDataWriteable(), new_size));
    return std::nullopt;
  }

  auto target = duckdb::StringVector::EmptyString(result_vec, input.size());
  char* out = target.GetDataWriteable();

  for (size_t i = 0; i < input.size();) {
    char c = input[i];
    if (c != '\\') {
      *out++ = c;
      ++i;
      continue;
    }
    if (i + 1 >= input.size()) {
      return InvalidByteaSyntax();
    }
    if (input[i + 1] == '\\') {
      *out++ = '\\';
      i += 2;
      continue;
    }
    if (i + 3 >= input.size()) {
      return InvalidByteaSyntax();
    }
    const char* octal = input.data() + i + 1;
    unsigned value = 0;
    const auto res = fast_float::from_chars(octal, octal + 3, value, 8);
    if (res.ec != std::errc() || res.ptr != octal + 3 || value > 0xFFU) {
      return InvalidByteaSyntax();
    }
    *out++ = static_cast<char>(value);
    i += 4;
  }
  auto new_size = static_cast<duckdb::idx_t>(out - target.GetDataWriteable());
  target.Finalize();
  bytes = duckdb::StringVector::AddStringOrBlob(
    result_vec, duckdb::string_t(target.GetDataWriteable(), new_size));
  return std::nullopt;
}

bool VarcharToByteaCast(duckdb::Vector& source, duckdb::Vector& result,
                        duckdb::idx_t count,
                        duckdb::CastParameters& parameters) {
  bool converted = true;
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    source, result, count,
    [&](duckdb::string_t input) -> std::optional<duckdb::string_t> {
      duckdb::string_t bytes;
      if (auto error =
            ByteaIn({input.GetData(), input.GetSize()}, result, bytes)) {
        InputError(std::move(*error), parameters);
        converted = false;
        return std::nullopt;
      }
      return bytes;
    });
  return converted;
}

struct ByteaOutCastData final : public duckdb::BoundCastData {
  bool use_escape;
  explicit ByteaOutCastData(bool use_escape) : use_escape(use_escape) {}
  duckdb::unique_ptr<duckdb::BoundCastData> Copy() const final {
    return duckdb::make_uniq<ByteaOutCastData>(use_escape);
  }
};

bool ByteaToVarcharCast(duckdb::Vector& source, duckdb::Vector& result,
                        duckdb::idx_t count,
                        duckdb::CastParameters& parameters) {
  bool use_escape = false;
  if (parameters.cast_data) {
    use_escape = parameters.cast_data->Cast<ByteaOutCastData>().use_escape;
  }

  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    source, result, count, [&](duckdb::string_t input) -> duckdb::string_t {
      std::string_view value{input.GetData(), input.GetSize()};
      if (use_escape) {
        const auto required_size = ByteaOutEscapeLength(value);
        auto target = duckdb::StringVector::EmptyString(result, required_size);
        ByteaOutEscape(target.GetDataWriteable(), value);
        target.Finalize();
        return target;
      }
      const auto required_size = 2 + 2 * value.size();
      auto target = duckdb::StringVector::EmptyString(result, required_size);
      ByteaOutHex(target.GetDataWriteable(), value);
      target.Finalize();
      return target;
    });
  return true;
}

duckdb::BoundCastInfo ByteaToVarcharBind(duckdb::BindCastInput& input,
                                         const duckdb::LogicalType&,
                                         const duckdb::LogicalType&) {
  bool use_escape = false;
  if (input.context) {
    duckdb::Value value;
    if (input.context->TryGetCurrentSetting("bytea_output", value)) {
      auto str = duckdb::StringUtil::Lower(value.ToString());
      use_escape = (str == "escape");
    }
  }
  return duckdb::BoundCastInfo(ByteaToVarcharCast,
                               duckdb::make_uniq<ByteaOutCastData>(use_escape));
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

void RegisterBytea(duckdb::CastFunctionSet& casts) {
  casts.RegisterCastFunction(
    duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BLOB,
    duckdb::BoundCastInfo{VarcharToByteaCast, nullptr, InitInput}, 100);
  casts.RegisterCastFunction(duckdb::LogicalType::BLOB,
                             duckdb::LogicalType::VARCHAR, ByteaToVarcharBind,
                             100);
}

}  // namespace sdb::pg
