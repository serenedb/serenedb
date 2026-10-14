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

#include "connector/functions/backup.h"

#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <string_view>
#include <utility>

#include "auth/role_closure.h"
#include "backup/backup_label.h"
#include "backup/backup_session.h"
#include "connector/duckdb_client_state.h"
#include "connector/pg_logical_types.h"
#include "pg/connection_context.h"

namespace sdb::connector {
namespace {

ConnectionContext& RequireSuperuser(duckdb::ClientContext& context,
                                    std::string_view function) {
  auto& conn = GetSereneDBContext(context);
  if (!auth::ClosureFor(&context, conn.GetRoleId())->is_superuser) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied for function ", function));
  }
  return conn;
}

void PgBackupStartFunction(duckdb::DataChunk& args,
                           duckdb::ExpressionState& state,
                           duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto& conn = RequireSuperuser(context, "pg_backup_start");
  if (!conn.GetSendBuffer()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("pg_backup_start requires a PG wire connection"));
  }
  auto out = duckdb::FlatVector::Writer<int64_t>(result, args.size());
  for (auto label : args.data[0].Values<duckdb::string_t>()) {
    if (!label.IsValid()) {
      out.WriteNull();
      continue;
    }
    const auto& text = label.GetValue();
    out.WriteValue(static_cast<int64_t>(backup::BackupSession::Instance().Begin(
      {&conn, conn.GetBackendPid()},
      std::string_view{text.GetData(), text.GetSize()},
      [&] { return context.IsInterrupted(); })));
  }
}

void PgWalfileNameFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                           duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<int64_t, duckdb::string_t>(
    args.data[0], result, args.size(), [&](int64_t lsn) {
      return duckdb::StringVector::AddString(
        result, backup::WalFileName(static_cast<uint64_t>(lsn)));
    });
}

void WriteFinishedBackups(duckdb::Vector& lsn, duckdb::Vector& labelfile,
                          duckdb::Vector& spcmapfile, duckdb::idx_t count,
                          ConnectionContext& conn) {
  auto lsn_out = duckdb::FlatVector::Writer<int64_t>(lsn, count);
  auto label_out =
    duckdb::FlatVector::Writer<duckdb::string_t>(labelfile, count);
  auto map_out =
    duckdb::FlatVector::Writer<duckdb::string_t>(spcmapfile, count);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    const auto finished = backup::BackupSession::Instance().Finish(&conn);
    lsn_out.WriteValue(static_cast<int64_t>(finished.lsn));
    label_out.WriteValue(
      duckdb::string_t{finished.label_file.data(),
                       static_cast<uint32_t>(finished.label_file.size())});
    map_out.WriteValue(duckdb::string_t{"", 0});
  }
}

void PgBackupStopFunction(duckdb::DataChunk& args,
                          duckdb::ExpressionState& state,
                          duckdb::Vector& result) {
  auto& conn = RequireSuperuser(state.GetContext(), "pg_backup_stop");
  auto& entries = duckdb::StructVector::GetEntries(result);
  WriteFinishedBackups(entries[0], entries[1], entries[2], args.size(), conn);
}

duckdb::unique_ptr<duckdb::FunctionData> PgBackupStopBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput&,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::Identifier>& names) {
  RequireSuperuser(context, "pg_backup_stop");
  names.emplace_back("lsn");
  return_types.emplace_back(pg::PG_LSN());
  names.emplace_back("labelfile");
  return_types.emplace_back(duckdb::LogicalType::VARCHAR);
  names.emplace_back("spcmapfile");
  return_types.emplace_back(duckdb::LogicalType::VARCHAR);
  return duckdb::make_uniq<duckdb::TableFunctionData>();
}

struct PgBackupStopState final : duckdb::GlobalTableFunctionState {
  bool emitted = false;

  static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> Init(
    duckdb::ClientContext&, duckdb::TableFunctionInitInput&) {
    return duckdb::make_uniq<PgBackupStopState>();
  }
};

void PgBackupStopExecute(duckdb::ClientContext& context,
                         duckdb::TableFunctionInput& data,
                         duckdb::DataChunk& output) {
  auto& state = data.global_state->Cast<PgBackupStopState>();
  if (std::exchange(state.emitted, true)) {
    output.SetCardinalityUnsafe(0);
    return;
  }
  WriteFinishedBackups(output.data[0], output.data[1], output.data[2], 1,
                       GetSereneDBContext(context));
  output.SetCardinalityUnsafe(1);
}

}  // namespace

void RegisterBackupFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  duckdb::ScalarFunctionSet start{"pg_backup_start"};
  for (const auto& arguments :
       {duckdb::vector<duckdb::LogicalType>{duckdb::LogicalType::VARCHAR},
        duckdb::vector<duckdb::LogicalType>{duckdb::LogicalType::VARCHAR,
                                            duckdb::LogicalType::BOOLEAN}}) {
    duckdb::ScalarFunction function{arguments, pg::PG_LSN(),
                                    PgBackupStartFunction};
    function.SetVolatile();
    start.AddFunction(std::move(function));
  }
  loader.RegisterFunction(std::move(start));
  duckdb::TableFunctionSet stop{"pg_backup_stop"};
  for (const auto& arguments :
       {duckdb::vector<duckdb::LogicalType>{},
        duckdb::vector<duckdb::LogicalType>{duckdb::LogicalType::BOOLEAN}}) {
    stop.AddFunction(duckdb::TableFunction{arguments, PgBackupStopExecute,
                                           PgBackupStopBind,
                                           PgBackupStopState::Init});
  }
  loader.RegisterFunction(std::move(stop));
  const auto record =
    duckdb::LogicalType::STRUCT({{"lsn", pg::PG_LSN()},
                                 {"labelfile", duckdb::LogicalType::VARCHAR},
                                 {"spcmapfile", duckdb::LogicalType::VARCHAR}});
  duckdb::ScalarFunctionSet stop_record{"pg_backup_stop"};
  for (const auto& arguments :
       {duckdb::vector<duckdb::LogicalType>{},
        duckdb::vector<duckdb::LogicalType>{duckdb::LogicalType::BOOLEAN}}) {
    duckdb::ScalarFunction function{arguments, record, PgBackupStopFunction};
    function.SetVolatile();
    stop_record.AddFunction(std::move(function));
  }
  loader.RegisterFunction(std::move(stop_record));
  loader.RegisterFunction(duckdb::ScalarFunction{"pg_walfile_name",
                                                 {pg::PG_LSN()},
                                                 duckdb::LogicalType::VARCHAR,
                                                 PgWalfileNameFunction});
}

}  // namespace sdb::connector
