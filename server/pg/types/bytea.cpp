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

#include <algorithm>
#include <duckdb/common/string_util.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/main/client_context.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <utf8proc_wrapper.hpp>

#include "pg/deserialize.h"
#include "pg/serialize.h"
#include "pg/types/cast.h"
#include "query/config.h"

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
  const auto capacity = ByteaTextCapacity(input);
  auto target = duckdb::StringVector::EmptyString(result_vec, capacity);
  const auto decoded = DecodeByteaText(input, target.GetDataWriteable());
  switch (decoded.error) {
    case ByteaTextError::None:
      break;
    case ByteaTextError::InvalidHexDigit:
      return InvalidHexDigit(input.substr(decoded.error_pos));
    case ByteaTextError::OddHexDigits:
      return SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("invalid hexadecimal data: odd number of digits"));
    case ByteaTextError::InvalidEscape:
      return InvalidByteaSyntax();
  }
  target.SetSizeAndFinalize(static_cast<uint32_t>(decoded.size), capacity);
  bytes = target;
  return std::nullopt;
}

bool TextToBytea(duckdb::Vector& source, duckdb::Vector& result,
                 duckdb::idx_t count, duckdb::CastParameters& parameters) {
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

bool ByteaToText(duckdb::Vector& source, duckdb::Vector& result,
                 duckdb::idx_t count, duckdb::CastParameters& parameters) {
  const auto use_escape =
    parameters.cast_data->Cast<ByteaOutCastData>().use_escape;
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

duckdb::BoundCastInfo BindByteaToText(duckdb::BindCastInput& input,
                                      const duckdb::LogicalType&,
                                      const duckdb::LogicalType&) {
  const auto output = magic_enum::enum_value<ByteaOutput>(GetEnumSetting(
    *input.context, "bytea_output", magic_enum::enum_names<ByteaOutput>()));
  return duckdb::BoundCastInfo(ByteaToText, duckdb::make_uniq<ByteaOutCastData>(
                                              output == ByteaOutput::Escape));
}

void RegisterCasts(duckdb::CastFunctionSet& casts) {
  casts.RegisterCastFunction(
    duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BLOB,
    duckdb::BoundCastInfo{TextToBytea, nullptr, InitInput}, 100);
  casts.RegisterCastFunction(duckdb::LogicalType::BLOB,
                             duckdb::LogicalType::VARCHAR, BindByteaToText,
                             100);
}

}  // namespace

void RegisterBytea(duckdb::CastFunctionSet& casts) { RegisterCasts(casts); }

}  // namespace sdb::pg
