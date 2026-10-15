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

#include "pg/types/reg.h"

#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/function/cast/default_casts.hpp>
#include <iresearch/utils/assert.hpp>
#include <optional>
#include <string_view>

#include "pg/catalog/functions/pg_catalog/regproc.h"
#include "pg/catalog/utils/session.h"
#include "pg/types/cast.h"
#include "pg/types/logical.h"

namespace sdb::pg {
namespace {

struct RegInputState final : InputState {
  explicit RegInputState(duckdb::ClientContext* context)
    : session{MakeSession(context)} {}

  Session session;
};

duckdb::unique_ptr<duckdb::FunctionLocalState> InitRegInput(
  duckdb::CastLocalStateParameters& parameters) {
  return duckdb::make_uniq<RegInputState>(parameters.context.get());
}

duckdb::unique_ptr<duckdb::FunctionLocalState> InitRegCast(
  duckdb::CastLocalStateParameters& parameters) {
  return duckdb::make_uniq<SessionState>(parameters.context.get());
}

template<RegKind Kind>
bool TextToReg(duckdb::Vector& source, duckdb::Vector& result,
               duckdb::idx_t count, duckdb::CastParameters& params) {
  const auto& session = params.local_state->Cast<RegInputState>().session;
  SDB_ASSERT(session.context);
  irs::pg::SqlErrorData error;
  bool converted = true;
  duckdb::UnaryExecutor::Execute<duckdb::string_t, int64_t>(
    source, result, count,
    [&](duckdb::string_t text) -> std::optional<int64_t> {
      if (const auto oid =
            RegIn(session, Kind, {text.GetData(), text.GetSize()},
                  params.error_message ? &error : nullptr)) {
        return static_cast<int64_t>(*oid);
      }
      InputError(std::move(error), params);
      converted = false;
      return std::nullopt;
    });
  return converted;
}

template<RegKind Kind>
bool RegToText(duckdb::Vector& source, duckdb::Vector& result,
               duckdb::idx_t count, duckdb::CastParameters& params) {
  const auto& session = params.local_state->Cast<SessionState>().session;
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
      RegOut(session, Kind, static_cast<uint64_t>(value.GetValue()));
    dst_data[i] =
      duckdb::StringVector::AddString(result, text.data(), text.size());
  }
  return true;
}

template<RegKind Kind>
void RegisterRegCasts(duckdb::CastFunctionSet& casts,
                      const duckdb::LogicalType& reg) {
  RegisterTextInput(casts, reg, TextToReg<Kind>, InitRegInput);
  casts.RegisterCastFunction(
    reg, duckdb::LogicalType::VARCHAR,
    duckdb::BoundCastInfo(RegToText<Kind>, nullptr, InitRegCast), 50);
  casts.RegisterCastFunction(
    OID(), reg, duckdb::BoundCastInfo(duckdb::DefaultCasts::ReinterpretCast),
    1);
  casts.RegisterCastFunction(
    reg, OID(), duckdb::BoundCastInfo(duckdb::DefaultCasts::ReinterpretCast),
    1);
}

}  // namespace

void RegisterRegTypes(duckdb::CastFunctionSet& casts) {
  RegisterRegCasts<RegKind::Proc>(casts, REGPROC());
  RegisterRegCasts<RegKind::Procedure>(casts, REGPROCEDURE());
  RegisterRegCasts<RegKind::Oper>(casts, REGOPER());
  RegisterRegCasts<RegKind::Operator>(casts, REGOPERATOR());
  RegisterRegCasts<RegKind::Class>(casts, REGCLASS());
  RegisterRegCasts<RegKind::Type>(casts, REGTYPE());
  RegisterRegCasts<RegKind::Collation>(casts, REGCOLLATION());
  RegisterRegCasts<RegKind::Config>(casts, REGCONFIG());
  RegisterRegCasts<RegKind::Dictionary>(casts, REGDICTIONARY());
  RegisterRegCasts<RegKind::Role>(casts, REGROLE());
  RegisterRegCasts<RegKind::Namespace>(casts, REGNAMESPACE());
}

}  // namespace sdb::pg
