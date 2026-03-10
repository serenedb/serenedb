////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#pragma once

#include <memory>

#include "catalog/fwd.h"
#include "pg/sql_utils.h"
#include "utils/exec_context.h"
#include "yaclib/async/future.hpp"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::query {
class Query;
}  // namespace sdb::query

namespace sdb::pg {

yaclib::Future<> CreateDatabase(ExecContext& ctx, const CreatedbStmt& stmt);

yaclib::Future<> CreateSchema(ExecContext& ctx, const CreateSchemaStmt& stmt);

yaclib::Future<> DropDatabase(ExecContext& ctx, const DropdbStmt& stmt);

yaclib::Future<> CreateTable(ExecContext& ctx, const CreateStmt& stmt);

yaclib::Future<> CreateIndex(ExecContext& ctx, const IndexStmt& stmt);

yaclib::Future<> CreateView(const ExecContext& ctx, const ViewStmt& stmt);

std::shared_ptr<catalog::View> CreateSystemView(const ViewStmt& stmt);

yaclib::Future<> DropObject(ExecContext& ctx, const DropStmt& stmt);

yaclib::Future<> Transaction(ExecContext& ctx, const TransactionStmt& stmt);

yaclib::Future<> VariableSet(ExecContext& ctx, const VariableSetStmt& stmt);

yaclib::Future<> CreateFunction(ExecContext& ctx,
                                const CreateFunctionStmt& stmt);

std::shared_ptr<catalog::Function> CreateSystemFunction(
  const CreateFunctionStmt& stmt);

yaclib::Future<> CreateTableCTAS(ExecContext& ctx, query::Query& query,
                                 const IntoClause& into, bool if_not_exists);

yaclib::Future<> RemoveTombstone(ExecContext& ctx, const RangeVar& rel);

yaclib::Future<> Vacuum(ExecContext& ctx, const VacuumStmt& stmt);

yaclib::Future<> CreateTSDictionary(ExecContext& ctx, const DefineStmt& stmt);

}  // namespace sdb::pg
