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

#include "connector/functions/inout.h"

#include <absl/strings/escaping.h>
#include <fast_float/fast_float.h>

#include <duckdb/common/vector_operations/generic_executor.hpp>
#include <duckdb/function/cast/cast_function_set.hpp>
#include <duckdb/function/cast/default_casts.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "connector/pg_logical_types.h"
#include "pg/catalog/functions/reg_types.h"
#include "pg/catalog/lookup.h"
#include "pg/serialize.h"

namespace sdb::connector {
namespace {

// PG-compatible byteain -- ported from server/pg/functions/inout.cpp
// ByteaInFunction. Handles \x hex format (whitespace between pairs ignored)
// and PG escape format (\\ and \NNN octal).
duckdb::string_t PgByteaIn(std::string_view input, duckdb::Vector& result_vec) {
  if (input.starts_with("\\x")) {
    std::string_view payload{input.begin() + 2, input.end()};
    // Worst case: every 2 chars = 1 byte
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
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
          ERR_MSG("invalid hexadecimal digit: \"", std::string(1, c), "\""));
      }
      if (i + 1 >= payload.size()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
          ERR_MSG("invalid hexadecimal data: odd number of digits"));
      }
      const auto h2 =
        absl::kHexValueStrict[static_cast<unsigned char>(payload[i + 1])];
      if (h2 == -1) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        ERR_MSG("invalid hexadecimal digit: \"",
                                std::string(1, payload[i + 1]), "\""));
      }
      *out++ = static_cast<char>((h1 << 4) + h2);
      i += 2;
    }
    auto new_size = static_cast<duckdb::idx_t>(out - target.GetDataWriteable());
    target.Finalize();
    return duckdb::StringVector::AddStringOrBlob(
      result_vec, duckdb::string_t(target.GetDataWriteable(), new_size));
  }

  // Escape format: \\ -> backslash, \NNN -> octal byte, rest literal
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
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                      ERR_MSG("invalid input syntax for type bytea"));
    }
    if (input[i + 1] == '\\') {
      *out++ = '\\';
      i += 2;
      continue;
    }
    if (i + 3 >= input.size()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                      ERR_MSG("invalid input syntax for type bytea"));
    }
    const char* octal = input.data() + i + 1;
    unsigned value = 0;
    const auto res = fast_float::from_chars(octal, octal + 3, value, 8);
    if (res.ec != std::errc() || res.ptr != octal + 3 || value > 0xFFU) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                      ERR_MSG("invalid input syntax for type bytea"));
    }
    *out++ = static_cast<char>(value);
    i += 4;
  }
  auto new_size = static_cast<duckdb::idx_t>(out - target.GetDataWriteable());
  target.Finalize();
  return duckdb::StringVector::AddStringOrBlob(
    result_vec, duckdb::string_t(target.GetDataWriteable(), new_size));
}

bool PgVarcharToBlobCast(duckdb::Vector& source, duckdb::Vector& result,
                         duckdb::idx_t count, duckdb::CastParameters&) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    source, result, count, [&](duckdb::string_t input) -> duckdb::string_t {
      return PgByteaIn({input.GetData(), input.GetSize()}, result);
    });
  return true;
}

struct ByteaOutCastData : public duckdb::BoundCastData {
  bool use_escape;
  explicit ByteaOutCastData(bool use_escape) : use_escape(use_escape) {}
  duckdb::unique_ptr<duckdb::BoundCastData> Copy() const final {
    return duckdb::make_uniq<ByteaOutCastData>(use_escape);
  }
};

// PG-compatible byteaout -- ported from server/pg/functions/inout.cpp
// ByteaOutFunction. Respects bytea_output setting (hex or escape).
bool PgBlobToVarcharCast(duckdb::Vector& source, duckdb::Vector& result,
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
        const auto required_size = pg::ByteaOutEscapeLength(value);
        auto target = duckdb::StringVector::EmptyString(result, required_size);
        pg::ByteaOutEscape(target.GetDataWriteable(), value);
        target.Finalize();
        return target;
      }
      // Hex format: \x prefix + 2 hex chars per byte
      const auto required_size = 2 + 2 * value.size();
      auto target = duckdb::StringVector::EmptyString(result, required_size);
      pg::ByteaOutHex(target.GetDataWriteable(), value);
      target.Finalize();
      return target;
    });
  return true;
}

duckdb::BoundCastInfo PgBlobToVarcharBind(duckdb::BindCastInput& input,
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
  return duckdb::BoundCastInfo(PgBlobToVarcharCast,
                               duckdb::make_uniq<ByteaOutCastData>(use_escape));
}

struct RegCastData : public duckdb::BoundCastData {
  duckdb::ClientContext* ctx;
  explicit RegCastData(duckdb::ClientContext* ctx) : ctx{ctx} {}
  duckdb::unique_ptr<duckdb::BoundCastData> Copy() const final {
    return duckdb::make_uniq<RegCastData>(ctx);
  }
};

template<pg::RegKind Kind>
bool PgVarcharToRegCast(duckdb::Vector& source, duckdb::Vector& result,
                        duckdb::idx_t count, duckdb::CastParameters& params) {
  const auto& data = params.cast_data->Cast<RegCastData>();
  SDB_ASSERT(data.ctx);
  const auto session = pg::MakeSession(data.ctx);
  auto src = source.Values<duckdb::string_t>();
  auto* dst_data = duckdb::FlatVector::GetDataMutable<int64_t>(result);
  auto& dst_validity = duckdb::FlatVector::ValidityMutable(result);
  for (duckdb::idx_t i = 0; i < count; i++) {
    auto value = src[i];
    if (!value.IsValid()) {
      dst_validity.SetInvalid(i);
      continue;
    }
    const auto& text = value.GetValue();
    dst_data[i] = static_cast<int64_t>(*pg::RegIn<Kind>(
      session, std::string_view{text.GetData(), text.GetSize()}, false));
  }
  return true;
}

template<pg::RegKind Kind>
bool PgRegToVarcharCast(duckdb::Vector& source, duckdb::Vector& result,
                        duckdb::idx_t count, duckdb::CastParameters& params) {
  const auto session =
    pg::MakeSession(params.cast_data->Cast<RegCastData>().ctx);
  auto src = source.Values<int64_t>();
  auto* dst_data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
  auto& dst_validity = duckdb::FlatVector::ValidityMutable(result);
  for (duckdb::idx_t i = 0; i < count; i++) {
    auto value = src[i];
    if (!value.IsValid()) {
      dst_validity.SetInvalid(i);
      continue;
    }
    const auto text =
      pg::RegOut<Kind>(session, static_cast<uint64_t>(value.GetValue()));
    dst_data[i] =
      duckdb::StringVector::AddString(result, text.data(), text.size());
  }
  return true;
}

template<duckdb::cast_function_t Cast>
duckdb::BoundCastInfo RegCastBind(duckdb::BindCastInput& input,
                                  const duckdb::LogicalType&,
                                  const duckdb::LogicalType&) {
  return duckdb::BoundCastInfo(
    Cast, duckdb::make_uniq<RegCastData>(input.context.get()));
}

template<pg::RegKind Kind>
void RegisterRegCasts(duckdb::CastFunctionSet& casts,
                      const duckdb::LogicalType& reg) {
  casts.RegisterCastFunction(duckdb::LogicalType::VARCHAR, reg,
                             RegCastBind<PgVarcharToRegCast<Kind>>, 50);
  casts.RegisterCastFunction(
    duckdb::LogicalType(duckdb::LogicalTypeId::STRING_LITERAL), reg,
    RegCastBind<PgVarcharToRegCast<Kind>>, 50);
  casts.RegisterCastFunction(reg, duckdb::LogicalType::VARCHAR,
                             RegCastBind<PgRegToVarcharCast<Kind>>, 50);
  casts.RegisterCastFunction(
    pg::OID(), reg,
    duckdb::BoundCastInfo(duckdb::DefaultCasts::ReinterpretCast), 1);
  casts.RegisterCastFunction(
    reg, pg::OID(),
    duckdb::BoundCastInfo(duckdb::DefaultCasts::ReinterpretCast), 1);
}

}  // namespace

void RegisterPgInOutFunctions(duckdb::DatabaseInstance& db) {
  auto& config = duckdb::DBConfig::GetConfig(db);
  auto& casts = config.GetCastFunctions();

  [&]<size_t... I>(std::index_sequence<I...>) {
    (RegisterRegCasts<pg::kRegTypes[I].kind>(
       casts, pg::RegLogicalType(pg::kRegTypes[I])),
     ...);
  }(std::make_index_sequence<pg::kRegTypes.size()>{});

  // VARCHAR -> BLOB / BLOB -> VARCHAR (bytea)
  casts.RegisterCastFunction(duckdb::LogicalType::VARCHAR,
                             duckdb::LogicalType::BLOB, PgVarcharToBlobCast,
                             100);
  casts.RegisterCastFunction(duckdb::LogicalType::BLOB,
                             duckdb::LogicalType::VARCHAR, PgBlobToVarcharBind,
                             100);
}

}  // namespace sdb::connector
