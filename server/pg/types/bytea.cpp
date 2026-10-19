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

#include <algorithm>
#include <bit>
#include <cstring>
#include <duckdb/common/string_util.hpp>
#include <duckdb/common/vector_operations/binary_executor.hpp>
#include <duckdb/common/vector_operations/generic_executor.hpp>
#include <duckdb/common/vector_operations/ternary_executor.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/function/scalar/string_common.hpp>
#include <duckdb/main/client_context.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <string>
#include <utf8proc_wrapper.hpp>

#include "pg/deserialize.h"
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

duckdb::BoundCastInfo BindByteaToText(duckdb::BindCastInput& input,
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
  return duckdb::BoundCastInfo(ByteaToText,
                               duckdb::make_uniq<ByteaOutCastData>(use_escape));
}

void SubstringFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                       duckdb::Vector& result) {
  if (args.ColumnCount() == 3) {
    duckdb::TernaryExecutor::Execute<duckdb::string_t, int64_t, int64_t,
                                     duckdb::string_t>(
      args.data[0], args.data[1], args.data[2], result, args.size(),
      [&](duckdb::string_t bytes, int64_t offset, int64_t length) {
        duckdb::PostgresSubstringBounds(offset, length);
        return duckdb::SubstringASCII(result, bytes, offset, length);
      });
    return;
  }
  duckdb::BinaryExecutor::Execute<duckdb::string_t, int64_t, duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t bytes, int64_t offset) {
      return duckdb::SubstringASCII(result, bytes, std::max<int64_t>(offset, 1),
                                    duckdb::NumericLimits<uint32_t>::Maximum());
    });
}

duckdb::string_t Overlay(duckdb::Vector& result, duckdb::string_t bytes,
                         duckdb::string_t placing, int64_t start,
                         int64_t count) {
  const auto tail = duckdb::PostgresOverlayEnd(start, count);
  const auto size = bytes.GetSize();
  const auto head = std::min<uint64_t>(start - 1, size);
  const auto rest = static_cast<size_t>(
    std::clamp<int64_t>(tail - 1, 0, static_cast<int64_t>(size)));
  auto target = duckdb::StringVector::EmptyString(
    result, head + placing.GetSize() + size - rest);
  auto* out = target.GetDataWriteable();
  std::memcpy(out, bytes.GetData(), head);
  std::memcpy(out + head, placing.GetData(), placing.GetSize());
  std::memcpy(out + head + placing.GetSize(), bytes.GetData() + rest,
              size - rest);
  target.Finalize();
  return target;
}

void OverlayFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                     duckdb::Vector& result) {
  if (args.ColumnCount() == 4) {
    duckdb::GenericExecutor::ExecuteQuaternary<
      duckdb::PrimitiveType<duckdb::string_t>,
      duckdb::PrimitiveType<duckdb::string_t>, duckdb::PrimitiveType<int64_t>,
      duckdb::PrimitiveType<int64_t>, duckdb::PrimitiveType<duckdb::string_t>>(
      args.data[0], args.data[1], args.data[2], args.data[3], result,
      args.size(),
      [&](duckdb::PrimitiveType<duckdb::string_t> bytes,
          duckdb::PrimitiveType<duckdb::string_t> placing,
          duckdb::PrimitiveType<int64_t> start,
          duckdb::PrimitiveType<int64_t> count)
        -> duckdb::PrimitiveType<duckdb::string_t> {
        return {Overlay(result, bytes.val, placing.val, start.val, count.val)};
      });
    return;
  }
  duckdb::TernaryExecutor::Execute<duckdb::string_t, duckdb::string_t, int64_t,
                                   duckdb::string_t>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t bytes, duckdb::string_t placing, int64_t start) {
      return Overlay(result, bytes, placing, start,
                     static_cast<int64_t>(placing.GetSize()));
    });
}

void BitCountFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                      duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, int64_t>(
    args.data[0], result, args.size(), [](duckdb::string_t bytes) {
      int64_t count = 0;
      for (const auto byte :
           std::string_view{bytes.GetData(), bytes.GetSize()}) {
        count += std::popcount(static_cast<uint8_t>(byte));
      }
      return count;
    });
}

void RegisterCasts(duckdb::CastFunctionSet& casts) {
  casts.RegisterCastFunction(
    duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BLOB,
    duckdb::BoundCastInfo{TextToBytea, nullptr, InitInput}, 100);
  casts.RegisterCastFunction(duckdb::LogicalType::BLOB,
                             duckdb::LogicalType::VARCHAR, BindByteaToText,
                             100);
}

void RegisterFunctions(Functions& functions) {
  const auto blob = duckdb::LogicalType::BLOB;
  const auto bigint = duckdb::LogicalType::BIGINT;
  for (const auto name : {"substring", "substr"}) {
    for (auto arguments :
         {duckdb::vector<duckdb::LogicalType>{blob, bigint},
          duckdb::vector<duckdb::LogicalType>{blob, bigint, bigint}}) {
      Add(functions, name,
          Fallible({std::move(arguments), blob, SubstringFunction}));
    }
  }
  for (auto arguments :
       {duckdb::vector<duckdb::LogicalType>{blob, blob, bigint},
        duckdb::vector<duckdb::LogicalType>{blob, blob, bigint, bigint}}) {
    Add(functions, "overlay",
        Fallible({std::move(arguments), blob, OverlayFunction}));
  }
  Add(functions, "bit_count",
      {{blob}, duckdb::LogicalType::BIGINT, BitCountFunction});
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

void RegisterBytea(duckdb::CastFunctionSet& casts, Functions& functions) {
  RegisterCasts(casts);
  RegisterFunctions(functions);
}

}  // namespace sdb::pg
