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

#include "connector/duckdb_client_state.h"
#include "connector/pg_logical_types.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/pg_types.h"
#include "pg/serialize.h"
#include "pg/sql_exception_macro.h"

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
  duckdb::UnifiedVectorFormat src_fmt;
  source.ToUnifiedFormat(count, src_fmt);
  auto* src_data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(src_fmt);
  auto* dst_data = duckdb::FlatVector::GetDataMutable<int64_t>(result);
  auto& dst_validity = duckdb::FlatVector::ValidityMutable(result);

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto src_idx = src_fmt.sel->get_index(i);
    if (!src_fmt.validity.RowIsValid(src_idx)) {
      dst_validity.SetInvalid(i);
      continue;
    }
    std::string_view name{src_data[src_idx].GetData(),
                          src_data[src_idx].GetSize()};
    dst_data[i] = in_fn(name);
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

bool PgVarcharToRegroleCast(duckdb::Vector& source, duckdb::Vector& result,
                            duckdb::idx_t count,
                            duckdb::CastParameters& params) {
  auto snap = GetSereneDBContext(*params.cast_data->Cast<RegCastData>().ctx)
                .CatalogSnapshot();
  return PgVarcharToOidCast(
    source, result, count, [&](std::string_view name) -> int64_t {
      auto role = snap->GetRole(name);
      if (!role) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                        ERR_MSG("role \"", name, "\" does not exist"));
      }
      return static_cast<int64_t>(role->GetId().id());
    });
}

duckdb::BoundCastInfo PgVarcharToRegroleBind(duckdb::BindCastInput& input,
                                             const duckdb::LogicalType&,
                                             const duckdb::LogicalType&) {
  return duckdb::BoundCastInfo(
    PgVarcharToRegroleCast,
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
  duckdb::UnifiedVectorFormat src_fmt;
  source.ToUnifiedFormat(count, src_fmt);
  auto* src_data = duckdb::UnifiedVectorFormat::GetData<int64_t>(src_fmt);
  auto& dst_validity = duckdb::FlatVector::ValidityMutable(result);

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto src_idx = src_fmt.sel->get_index(i);
    if (!src_fmt.validity.RowIsValid(src_idx)) {
      dst_validity.SetInvalid(i);
      continue;
    }
    auto name = out_fn(static_cast<uint64_t>(src_data[src_idx]));
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result)[i] =
      duckdb::StringVector::AddString(result, name);
  }
  return true;
}

bool PgRegtypeToVarcharCast(duckdb::Vector& source, duckdb::Vector& result,
                            duckdb::idx_t count, duckdb::CastParameters&) {
  return PgOidToVarcharCast(source, result, count, pg::RegtypeOut);
}

duckdb::BoundCastInfo PgRegtypeToVarcharBind(duckdb::BindCastInput&,
                                             const duckdb::LogicalType&,
                                             const duckdb::LogicalType&) {
  return duckdb::BoundCastInfo(PgRegtypeToVarcharCast);
}

bool PgRegclassToVarcharCast(duckdb::Vector& source, duckdb::Vector& result,
                             duckdb::idx_t count,
                             duckdb::CastParameters& params) {
  auto snap = GetSereneDBContext(*params.cast_data->Cast<RegCastData>().ctx)
                .CatalogSnapshot();
  return PgOidToVarcharCast(source, result, count, [&](uint64_t oid) {
    return pg::RegclassOut(*snap, oid);
  });
}

duckdb::BoundCastInfo PgRegclassToVarcharBind(duckdb::BindCastInput& input,
                                              const duckdb::LogicalType&,
                                              const duckdb::LogicalType&) {
  return duckdb::BoundCastInfo(
    PgRegclassToVarcharCast,
    duckdb::make_uniq<RegCastData>(input.context.get()));
}

bool PgRegroleToVarcharCast(duckdb::Vector& source, duckdb::Vector& result,
                            duckdb::idx_t count,
                            duckdb::CastParameters& params) {
  auto snap = GetSereneDBContext(*params.cast_data->Cast<RegCastData>().ctx)
                .CatalogSnapshot();
  return PgOidToVarcharCast(source, result, count, [&](uint64_t oid) {
    auto role = snap->GetObject<catalog::Role>(sdb::ObjectId{oid});
    // PostgreSQL renders a dropped role's oid numerically.
    return role ? std::string{role->GetName()} : std::to_string(oid);
  });
}

duckdb::BoundCastInfo PgRegroleToVarcharBind(duckdb::BindCastInput& input,
                                             const duckdb::LogicalType&,
                                             const duckdb::LogicalType&) {
  return duckdb::BoundCastInfo(
    PgRegroleToVarcharCast,
    duckdb::make_uniq<RegCastData>(input.context.get()));
}

bool PgRegnamespaceToVarcharCast(duckdb::Vector& source, duckdb::Vector& result,
                                 duckdb::idx_t count,
                                 duckdb::CastParameters& params) {
  auto snap = GetSereneDBContext(*params.cast_data->Cast<RegCastData>().ctx)
                .CatalogSnapshot();
  return PgOidToVarcharCast(source, result, count, [&](uint64_t oid) {
    return pg::RegnamespaceOut(*snap, oid);
  });
}

duckdb::BoundCastInfo PgRegnamespaceToVarcharBind(duckdb::BindCastInput& input,
                                                  const duckdb::LogicalType&,
                                                  const duckdb::LogicalType&) {
  return duckdb::BoundCastInfo(
    PgRegnamespaceToVarcharCast,
    duckdb::make_uniq<RegCastData>(input.context.get()));
}

// regprocedure -> oid: both are BIGINT-backed, so the cast is a no-op.
duckdb::BoundCastInfo PgRegprocedureToOidBind(duckdb::BindCastInput&,
                                              const duckdb::LogicalType&,
                                              const duckdb::LogicalType&) {
  return duckdb::BoundCastInfo(duckdb::DefaultCasts::NopCast);
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

  // VARCHAR/STRING_LITERAL -> regrole
  casts.RegisterCastFunction(duckdb::LogicalType::VARCHAR, pg::REGROLE(),
                             PgVarcharToRegroleBind, 50);
  casts.RegisterCastFunction(
    duckdb::LogicalType(duckdb::LogicalTypeId::STRING_LITERAL), pg::REGROLE(),
    PgVarcharToRegroleBind, 50);

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

  // regrole -> VARCHAR
  casts.RegisterCastFunction(pg::REGROLE(), duckdb::LogicalType::VARCHAR,
                             PgRegroleToVarcharBind, 50);

  // regnamespace -> VARCHAR
  casts.RegisterCastFunction(pg::REGNAMESPACE(), duckdb::LogicalType::VARCHAR,
                             PgRegnamespaceToVarcharBind, 50);

  // regprocedure -> oid: implicit, like PostgreSQL's reg* -> oid coercion; lets
  // a regprocedure flow into the oid-typed has_function_privilege overloads.
  casts.RegisterCastFunction(pg::REGPROCEDURE(), pg::OID(),
                             PgRegprocedureToOidBind, 50);

  // VARCHAR -> BLOB / BLOB -> VARCHAR (bytea)
  casts.RegisterCastFunction(duckdb::LogicalType::VARCHAR,
                             duckdb::LogicalType::BLOB, PgVarcharToBlobCast,
                             100);
  casts.RegisterCastFunction(duckdb::LogicalType::BLOB,
                             duckdb::LogicalType::VARCHAR, PgBlobToVarcharBind,
                             100);
}

}  // namespace sdb::connector
