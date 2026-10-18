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

#include <duckdb/common/vector_operations/generic_executor.hpp>
#include <duckdb/function/cast/cast_function_set.hpp>
#include <duckdb/function/cast/default_casts.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/utf8_utils.hpp>

#include "connector/duckdb_client_state.h"
#include "connector/pg_logical_types.h"
#include "pg/connection_context.h"
#include "pg/deserialize.h"
#include "pg/pg_types.h"
#include "pg/serialize.h"

namespace sdb::connector {
namespace {

duckdb::string_t PgByteaIn(std::string_view input, duckdb::Vector& result_vec) {
  const auto capacity = pg::ByteaTextCapacity(input);
  auto target = duckdb::StringVector::EmptyString(result_vec, capacity);
  const auto decoded = pg::DecodeByteaText(input, target.GetDataWriteable());
  switch (decoded.error) {
    case pg::ByteaTextError::None:
      break;
    case pg::ByteaTextError::InvalidHexDigit: {
      const auto* begin = reinterpret_cast<const irs::byte_type*>(input.data());
      const auto* digit = begin + decoded.error_pos;
      const auto* next = irs::utf8_utils::Next(digit, begin + input.size());
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("invalid hexadecimal digit: \"",
                input.substr(decoded.error_pos, next - digit), "\""));
    }
    case pg::ByteaTextError::OddHexDigits:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("invalid hexadecimal data: odd number of digits"));
    case pg::ByteaTextError::InvalidEscape:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                      ERR_MSG("invalid input syntax for type bytea"));
  }
  target.SetSizeAndFinalize(static_cast<uint32_t>(decoded.size), capacity);
  return target;
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

// --- Shared cast data for reg* types needing catalog context ---

struct RegCastData : public duckdb::BoundCastData {
  duckdb::ClientContext* ctx;
  explicit RegCastData(duckdb::ClientContext* ctx) : ctx(ctx) {}
  duckdb::unique_ptr<duckdb::BoundCastData> Copy() const final {
    return duckdb::make_uniq<RegCastData>(ctx);
  }
};

// --- VARCHAR -> reg* (name to OID) ---

template<typename InFn>
bool PgVarcharToOidCast(duckdb::Vector& source, duckdb::Vector& result,
                        duckdb::idx_t count, InFn&& in_fn) {
  auto src = source.Values<duckdb::string_t>();
  auto* dst_data = duckdb::FlatVector::GetDataMutable<int64_t>(result);
  auto& dst_validity = duckdb::FlatVector::ValidityMutable(result);

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto value = src[i];
    if (!value.IsValid()) {
      dst_validity.SetInvalid(i);
      continue;
    }
    const auto& name = value.GetValue();
    dst_data[i] = in_fn(std::string_view{name.GetData(), name.GetSize()});
  }
  return true;
}

bool PgVarcharToRegnamespaceCast(duckdb::Vector& source, duckdb::Vector& result,
                                 duckdb::idx_t count,
                                 duckdb::CastParameters& params) {
  auto& conn_ctx =
    GetSereneDBContext(*params.cast_data->Cast<RegCastData>().ctx);
  return PgVarcharToOidCast(
    source, result, count, [&](std::string_view name) -> int64_t {
      auto oid = pg::RegnamespaceIn(conn_ctx, name);
      if (oid == pg::kInvalidOid) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                        ERR_MSG("namespace \"", name, "\" does not exist"));
      }
      return oid;
    });
}

duckdb::BoundCastInfo PgVarcharToRegnamespaceBind(duckdb::BindCastInput& input,
                                                  const duckdb::LogicalType&,
                                                  const duckdb::LogicalType&) {
  return duckdb::BoundCastInfo(
    PgVarcharToRegnamespaceCast,
    duckdb::make_uniq<RegCastData>(input.context.get()));
}

bool PgVarcharToRegclassCast(duckdb::Vector& source, duckdb::Vector& result,
                             duckdb::idx_t count,
                             duckdb::CastParameters& params) {
  auto& conn_ctx =
    GetSereneDBContext(*params.cast_data->Cast<RegCastData>().ctx);
  return PgVarcharToOidCast(
    source, result, count, [&](std::string_view name) -> int64_t {
      auto oid = pg::RegclassIn(conn_ctx, name);
      if (oid == pg::kInvalidOid) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                        ERR_MSG("relation \"", name, "\" does not exist"));
      }
      return oid;
    });
}

duckdb::BoundCastInfo PgVarcharToRegclassBind(duckdb::BindCastInput& input,
                                              const duckdb::LogicalType&,
                                              const duckdb::LogicalType&) {
  return duckdb::BoundCastInfo(
    PgVarcharToRegclassCast,
    duckdb::make_uniq<RegCastData>(input.context.get()));
}

bool PgVarcharToRegtypeCast(duckdb::Vector& source, duckdb::Vector& result,
                            duckdb::idx_t count, duckdb::CastParameters&) {
  return PgVarcharToOidCast(
    source, result, count, [](std::string_view name) -> int64_t {
      auto oid = pg::RegtypeIn(name);
      if (oid == pg::kInvalidOid) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                        ERR_MSG("type \"", name, "\" does not exist"));
      }
      return oid;
    });
}

duckdb::BoundCastInfo PgVarcharToRegtypeBind(duckdb::BindCastInput&,
                                             const duckdb::LogicalType&,
                                             const duckdb::LogicalType&) {
  return duckdb::BoundCastInfo(PgVarcharToRegtypeCast);
}

// --- reg* -> VARCHAR (OID to name) ---

template<typename OutFn>
bool PgOidToVarcharCast(duckdb::Vector& source, duckdb::Vector& result,
                        duckdb::idx_t count, OutFn&& out_fn) {
  auto src = source.Values<int64_t>();
  auto& dst_validity = duckdb::FlatVector::ValidityMutable(result);

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto value = src[i];
    if (!value.IsValid()) {
      dst_validity.SetInvalid(i);
      continue;
    }
    auto name = out_fn(pg::OidFromSql(value.GetValue()));
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result)[i] =
      duckdb::StringVector::AddString(result, name);
  }
  return true;
}

bool PgRegtypeToVarcharCast(duckdb::Vector& source, duckdb::Vector& result,
                            duckdb::idx_t count,
                            duckdb::CastParameters& params) {
  auto* context = params.cast_data->Cast<RegCastData>().ctx;
  return PgOidToVarcharCast(source, result, count, [&](uint64_t oid) {
    return pg::RegtypeOut(context, oid);
  });
}

duckdb::BoundCastInfo PgRegtypeToVarcharBind(duckdb::BindCastInput& input,
                                             const duckdb::LogicalType&,
                                             const duckdb::LogicalType&) {
  return duckdb::BoundCastInfo(
    PgRegtypeToVarcharCast,
    duckdb::make_uniq<RegCastData>(input.context.get()));
}

bool PgRegclassToVarcharCast(duckdb::Vector& source, duckdb::Vector& result,
                             duckdb::idx_t count,
                             duckdb::CastParameters& params) {
  auto* context = params.cast_data->Cast<RegCastData>().ctx;
  return PgOidToVarcharCast(source, result, count, [&](uint64_t oid) {
    return pg::RegclassOut(context, oid);
  });
}

duckdb::BoundCastInfo PgRegclassToVarcharBind(duckdb::BindCastInput& input,
                                              const duckdb::LogicalType&,
                                              const duckdb::LogicalType&) {
  return duckdb::BoundCastInfo(
    PgRegclassToVarcharCast,
    duckdb::make_uniq<RegCastData>(input.context.get()));
}

bool PgRegnamespaceToVarcharCast(duckdb::Vector& source, duckdb::Vector& result,
                                 duckdb::idx_t count,
                                 duckdb::CastParameters& params) {
  auto* context = params.cast_data->Cast<RegCastData>().ctx;
  return PgOidToVarcharCast(source, result, count, [&](uint64_t oid) {
    return pg::RegnamespaceOut(context, oid);
  });
}

duckdb::BoundCastInfo PgRegnamespaceToVarcharBind(duckdb::BindCastInput& input,
                                                  const duckdb::LogicalType&,
                                                  const duckdb::LogicalType&) {
  return duckdb::BoundCastInfo(
    PgRegnamespaceToVarcharCast,
    duckdb::make_uniq<RegCastData>(input.context.get()));
}

}  // namespace

void RegisterPgInOutFunctions(duckdb::DatabaseInstance& db) {
  // PG reg* type casts -- all handled via implicit casts,
  // no scalar functions needed
  auto& config = duckdb::DBConfig::GetConfig(db);
  auto& casts = config.GetCastFunctions();

  // VARCHAR/STRING_LITERAL -> regnamespace
  casts.RegisterCastFunction(duckdb::LogicalType::VARCHAR, pg::REGNAMESPACE(),
                             PgVarcharToRegnamespaceBind, 50);
  casts.RegisterCastFunction(
    duckdb::LogicalType(duckdb::LogicalTypeId::STRING_LITERAL),
    pg::REGNAMESPACE(), PgVarcharToRegnamespaceBind, 50);

  // VARCHAR/STRING_LITERAL -> regclass
  casts.RegisterCastFunction(duckdb::LogicalType::VARCHAR, pg::REGCLASS(),
                             PgVarcharToRegclassBind, 50);
  casts.RegisterCastFunction(
    duckdb::LogicalType(duckdb::LogicalTypeId::STRING_LITERAL), pg::REGCLASS(),
    PgVarcharToRegclassBind, 50);

  // VARCHAR/STRING_LITERAL -> regtype
  casts.RegisterCastFunction(duckdb::LogicalType::VARCHAR, pg::REGTYPE(),
                             PgVarcharToRegtypeBind, 50);
  casts.RegisterCastFunction(
    duckdb::LogicalType(duckdb::LogicalTypeId::STRING_LITERAL), pg::REGTYPE(),
    PgVarcharToRegtypeBind, 50);

  // regtype -> VARCHAR
  casts.RegisterCastFunction(pg::REGTYPE(), duckdb::LogicalType::VARCHAR,
                             PgRegtypeToVarcharBind, 50);

  // regclass -> VARCHAR
  casts.RegisterCastFunction(pg::REGCLASS(), duckdb::LogicalType::VARCHAR,
                             PgRegclassToVarcharBind, 50);

  // regnamespace -> VARCHAR
  casts.RegisterCastFunction(pg::REGNAMESPACE(), duckdb::LogicalType::VARCHAR,
                             PgRegnamespaceToVarcharBind, 50);

  for (const auto& reg :
       {pg::REGCLASS(), pg::REGTYPE(), pg::REGPROC(), pg::REGPROCEDURE(),
        pg::REGNAMESPACE(), pg::REGROLE(), pg::REGOPER(), pg::REGOPERATOR(),
        pg::REGCONFIG(), pg::REGDICTIONARY(), pg::REGCOLLATION()}) {
    casts.RegisterCastFunction(
      pg::OID(), reg,
      duckdb::BoundCastInfo(duckdb::DefaultCasts::ReinterpretCast), 1);
    casts.RegisterCastFunction(
      reg, pg::OID(),
      duckdb::BoundCastInfo(duckdb::DefaultCasts::ReinterpretCast), 1);
  }

  // VARCHAR -> BLOB / BLOB -> VARCHAR (bytea)
  casts.RegisterCastFunction(duckdb::LogicalType::VARCHAR,
                             duckdb::LogicalType::BLOB, PgVarcharToBlobCast,
                             100);
  casts.RegisterCastFunction(duckdb::LogicalType::BLOB,
                             duckdb::LogicalType::VARCHAR, PgBlobToVarcharBind,
                             100);
}

}  // namespace sdb::connector
