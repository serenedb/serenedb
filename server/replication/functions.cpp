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

#include "replication/functions.h"

#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>

#include "auth/role_closure.h"
#include "connector/duckdb_client_state.h"
#include "connector/pg_logical_types.h"
#include "pg/connection_context.h"
#include "replication/subscription_engine.h"

namespace sdb::replication {
namespace {

void RequireSuperuser(duckdb::ClientContext& context, std::string_view name) {
  auto& conn = connector::GetSereneDBContext(context);
  if (!auth::ClosureFor(&context, conn.GetRoleId())->is_superuser) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied for function ", name));
  }
}

void ResetSubscriptionStats(duckdb::DataChunk& args,
                            duckdb::ExpressionState& state,
                            duckdb::Vector& result) {
  RequireSuperuser(state.GetContext(), "pg_stat_reset_subscription_stats");
  auto ids = args.data[0].Values<int64_t>();
  for (duckdb::idx_t i = 0; i < args.size(); ++i) {
    std::optional<duckdb::idx_t> subscription;
    if (const auto id = ids[i]; id.IsValid()) {
      if (id.GetValue() == 0) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                        ERR_MSG("invalid subscription OID 0"));
      }
      subscription = static_cast<duckdb::idx_t>(id.GetValue());
    }
    if (auto* engine = SubscriptionEngine::gInstance) {
      engine->ResetStats(subscription);
    }
  }
  result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
  duckdb::ConstantVector::SetNull(result, true);
}

}  // namespace

void RegisterReplicationFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");
  duckdb::ScalarFunction reset{"pg_stat_reset_subscription_stats",
                               {pg::OID()},
                               pg::VOID(),
                               ResetSubscriptionStats};
  reset.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
  reset.SetVolatile();
  loader.RegisterFunction(reset);
}

}  // namespace sdb::replication
