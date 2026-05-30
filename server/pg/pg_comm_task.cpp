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

#include "pg_comm_task.h"

#include <absl/base/internal/endian.h>
#include <absl/cleanup/cleanup.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>

#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/common/case_insensitive_map.hpp>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/prepared_statement_data.hpp>
#include <duckdb/parser/parsed_data/alter_info.hpp>
#include <duckdb/parser/parsed_data/create_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/parser/parsed_data/transaction_info.hpp>
#include <duckdb/parser/query_node/delete_query_node.hpp>
#include <duckdb/parser/sql_statement.hpp>
#include <duckdb/parser/statement/alter_statement.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/parser/statement/delete_statement.hpp>
#include <duckdb/parser/statement/drop_statement.hpp>
#include <duckdb/parser/statement/execute_statement.hpp>
#include <duckdb/parser/statement/transaction_statement.hpp>
#include <string_view>

#include "app/app_server.h"
#include "basics/application-exit.h"
#include "basics/assert.h"
#include "basics/dtoa.h"
#include "basics/endian.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "connector/duckdb_client_state.h"
#include "general_server/general_server_feature.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/hba.h"
#include "pg/pg_feature.h"
#include "pg/pg_types.h"
#include "pg/protocol.h"
#include "pg/serialize.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "query/duckdb_engine.h"
#include "search/inverted_index_shard.h"

#define SDB_LOG_PGSQL(...) SDB_PRINT_IF(false, __VA_ARGS__)

namespace sdb::pg {
namespace {

// Possible connection parameters from client
// The database user name to connect as. Required; there is no default.
constexpr std::string_view kUserParameter{"user"};
//  The database to connect to. Defaults to the user name.
constexpr std::string_view kDatabaseParameter{"database"};
// Command-line arguments for the backend. (This is deprecated in favor of
// setting individual run-time parameters.) Spaces within this string are
// considered to separate arguments, unless //escaped with a backslash (\);
// write \\ to represent a literal backslash.
[[maybe_unused]] constexpr std::string_view kOptionsParameter{"options"};
// Used to connect in streaming replication mode, where a small set of
// replication commands can be issued instead of SQL statements. Value can be
// true, false, or database, and the default //is false.
[[maybe_unused]] constexpr std::string_view kReplicationParameter{
  "replication"};

// as soon as this assert breaks we need to implement protocol negotiation
static_assert(PG_PROTOCOL_EARLIEST == PG_PROTOCOL_LATEST);

// clang-format off
// Pre-defined packets
constexpr std::array<char, 1> kNo{'N'};
constexpr std::array<char, 1> kYes{'S'};

constexpr std::array<char, 5> kNoData{
  PQ_MSG_NO_DATA, 0x00, 0x00, 0x00, 0x04,
};

constexpr std::array<char, 5> kParseComplete{
  PQ_MSG_PARSE_COMPLETE, 0x00, 0x00, 0x00, 0x04,
};
constexpr std::array<char, 5> kBindComplete{
  PQ_MSG_BIND_COMPLETE, 0x00, 0x00, 0x00, 0x04,
};
constexpr std::array<char, 5> kCloseComplete{
  PQ_MSG_CLOSE_COMPLETE, 0x00, 0x00, 0x00, 0x04,
};

constexpr std::array<char, 9> kAuthOk{
  PQ_MSG_AUTHENTICATION_REQUEST, 0x00, 0x00, 0x00, 0x8, 0x00, 0x00, 0x00, 0x00,
};

constexpr std::array<char, 6> kReadyForQuery{
  PQ_MSG_READY_FOR_QUERY, 0x00, 0x00, 0x00, 0x5, 'I',
};

constexpr std::array<char, 58> kPacketCancelled{PQ_MSG_ERROR_RESPONSE,
  0x00, 0x00, 0x00, 0x39,
  'S', 'E', 'R', 'R', 'O', 'R', 0x00,
  'V', 'E', 'R', 'R', 'O', 'R', 0x00,
  'C', '5', '7', '0', '1', '4', 0x00,
  'M', 'C', 'a', 'n', 'c', 'e', 'l',
  'i', 'n', 'g', ' ', 'd', 'u', 'e',
  ' ', 't', 'o', ' ', 'u', 's', 'e',
  'r', ' ', 'r', 'e', 'q', 'u', 'e', 's', 't', 0x00, 0x00,
};

constexpr std::array<char, 47> kTimeoutTermination{PQ_MSG_ERROR_RESPONSE,
  0x00, 0x00, 0x00, 0x2E,
  'S', 'E', 'R', 'R', 'O', 'R', 0x00,
  'V', 'E', 'R', 'R', 'O', 'R', 0x00,
  'C', '5', '7', 'P', '0', '5', 0x00,
  'M', 'K', 'e', 'e', 'p', '-', 'A',
  'l', 'i', 'v', 'e', ' ', 'T', 'i',
  'm', 'e', 'o', 'u', 't', 0x00, 0x00,
};

// clang-format on

duckdb::LogicalType ResolveExpectedType(const auto& value_map, uint16_t id) {
  auto type_it = value_map.find(absl::StrCat(id + 1));
  return (type_it != value_map.end() &&
          type_it->second->GetValue().type().id() !=
            duckdb::LogicalTypeId::UNKNOWN &&
          type_it->second->GetValue().type().id() !=
            duckdb::LogicalTypeId::INVALID)
           ? type_it->second->GetValue().type()
           : duckdb::LogicalTypeId::VARCHAR;
}

}  // namespace

PgSQLCommTaskBase::PgSQLCommTaskBase(rest::GeneralServer& server,
                                     ConnectionInfo info)
  : rest::CommTask(server, std::move(info)),
    _feature{PostgresFeature::instance()},
    _copy_queue{_queue_mutex},
    _send{64, 4096, 4096,
          [this](message::SequenceView data) { this->SendAsync(data); }} {}

PgSQLCommTaskBase::~PgSQLCommTaskBase() {
  if (_connection_ctx) {
    // Rollback unconditionally: even for auto-commit connections
    // (IsExplicitTransaction() == false), Config::_snapshot may be set and must
    // be released via Destroy() to avoid unreleased RocksDB snapshots at
    // shutdown.
    std::ignore = _connection_ctx->Rollback();
  }
  if (_key != 0) {
    _feature.UnregisterTask(_key);
  }
}

template<typename Func>
void PgSQLCommTaskBase::SafeCall(Func&& func) noexcept try {
  try {
    func();
  } catch (const duckdb::Exception& e) {
    duckdb::ErrorData error(e);
    SendError(error.RawMessage(), ERRCODE_INTERNAL_ERROR);
  }
} catch (const SqlException& e) {
  SendNotice(PQ_MSG_ERROR_RESPONSE, e.error());
} catch (const std::exception& e) {
  SendError(e.what(), ERRCODE_INTERNAL_ERROR);
} catch (...) {
  SendError("Unhandled exception", ERRCODE_INTERNAL_ERROR);
}

void PgSQLCommTaskBase::ProcessFirstRoot() noexcept {
  std::lock_guard lock{_execution_mutex};
  SDB_ASSERT(_state != State::Processing);
  const auto packet = StartPacket();
  SafeCall([&] {
    switch (_state) {
      case State::ClientHello:
        HandleClientHello(packet);
        break;
      case State::Idle:
        HandleClientPacket(packet);
        break;
      default:
        break;
    }
  });
  if (_pop_packet) {
    FinishPacket();
  }
}

void PgSQLCommTaskBase::ProcessNextRoot() noexcept {
  std::lock_guard lock{_execution_mutex};
  SDB_ASSERT(!_pop_packet);
  _pop_packet = true;
  SafeCall([&] {
    SDB_ASSERT(_current_portal);
    auto& stmt = *_current_portal->stmt;
    ++stmt.current_stmt_idx;
    if (stmt.current_stmt_idx >= stmt.extracted.size()) {
      // All statements done
      _success_packet = true;
      return;
    }
    _anonymous_portal.rows = 0;
    ExecuteNextSimpleStatement();
  });
  if (_pop_packet) {
    FinishPacket();
  }
}

void PgSQLCommTaskBase::ProcessWakeup(yaclib::Result<> r) noexcept {
  std::lock_guard lock{_execution_mutex};
  if (!_current_portal) {
    return;
  }
  SDB_ASSERT(!_pop_packet);
  _pop_packet = true;
  SafeCall([&] {
    SDB_ASSERT(_current_portal);
    auto state = ProcessState::DonePacket;
    do {
      state = ProcessQueryResult();
    } while (state == ProcessState::More);
    if (state == ProcessState::DonePacket) {
      // If simple protocol multi-statement, continue with next statement
      auto& stmt = *_current_portal->stmt;
      if (_success_packet &&
          stmt.current_stmt_idx + 1 < stmt.extracted.size()) {
        _feature.ScheduleProcessNext(weak_from_this());
        _pop_packet = false;
        return;
      }
    }
    _pop_packet = state == ProcessState::DonePacket;
  });
  if (_pop_packet) {
    FinishPacket();
  }
}

void PgSQLCommTaskBase::HandleClientHello(std::string_view packet) {
  // We don't send error in case of incorrect format here
  // or direct reply in case of cancel request,
  // because some security reasons in the protocol
  absl::Cleanup cleanup = [&]() noexcept { Stop(); };
  if (packet.size() < 8) {
    return;
  }
  static_assert(sizeof(ProtocolVersion) == sizeof(uint32_t));
  ProtocolVersion protocol_ver = absl::big_endian::Load32(packet.data() + 4);
  if (protocol_ver == NEGOTIATE_GSS_CODE ||
      protocol_ver == NEGOTIATE_SSL_CODE) {
    if (_ssl_handshake_passed) {
      // something nasty is happening. Double SSL switch is not expected.
      // better abort connection
      return;
    }
    // TODO: Add GSS handling.
    _send.Write(ToBuffer(kNo), true);
    std::move(cleanup).Cancel();
    _success_packet = true;
  } else if (protocol_ver == CANCEL_REQUEST_CODE) {
    uint64_t key;
    if (packet.size() >= 8 + sizeof(key)) {
      memcpy(&key, packet.data() + 8, sizeof key);
      _feature.CancelTaskPacket(key);
    }
  } else if (protocol_ver == PG_PROTOCOL_LATEST) {
    ParseClientParameters(packet.substr(8));
    if (UserName().empty()) {
      // user name is mandatory and should always be present
      SDB_WARN(HTTP, "User name not set. Terminating connection ",
               std::bit_cast<size_t>(this));
      return;
    }

    // Pin the catalog snapshot at connection time -- all operations
    // on this connection use the same snapshot until statement/transaction end.
    auto snapshot =
      catalog::CatalogFeature::instance().Global().GetCatalogSnapshot();
    auto database = snapshot->GetDatabase(DatabaseName());
    if (!database) {
      return SendError(
        absl::StrCat("Database ", DatabaseName(), " is not accessible"),
        ERRCODE_INVALID_SCHEMA_NAME);
    }

    _duckdb_conn = query::DuckDBEngine::Instance().CreateConnection();

    _connection_ctx = std::make_shared<ConnectionContext>(
      *_duckdb_conn->context, UserName(), DatabaseName(), database->GetId(),
      std::move(database), &_send, &_copy_queue);

    const auto& ci = GetConnectionInfo();
    [[maybe_unused]] hba::Client client{
      .user_name = UserName(),
      .database_name = DatabaseName(),
      .host_name = ci.client_address,
      .raddr =
        ci.sas_len ? reinterpret_cast<const sockaddr*>(&ci.sas) : nullptr,
      .ssl_in_use = _ssl_handshake_passed,
    };

    // TODO: auth check
    hba::AuthInfo auth;
    auth.auth.auth_method = hba::UserAuth::Trust;
    // auto authRes = _feature.server()
    //                  .getFeature<schema::SchemaFeature>()
    //                  .storage()
    //                  .getAuthInfo(client);
    // if (authRes.fail()) {
    //   return SendError(authRes.errorMessage(), "28000");
    // }

    // TODO: check user name and decide do we need authorizing and how.
    // use task as context carrier

    // TODO Remove Test
    // authRes->auth.authMethod = hba::UserAuth::Password;

    if (auth.auth.auth_method == hba::UserAuth::Trust) {
      // _session_ctx.auth_method = auth.auth.auth_method;
      // _session_ctx.session_user = auth.user;
      // _session_ctx.user = _session_ctx.user;
      // _session_ctx.database = auth.database;
      // _session_ctx.system_user = UserName();
      _state = State::Idle;
      _send.Write(ToBuffer(kAuthOk), false);
      _key = _feature.RegisterTask(*this);

      connector::SereneDBClientState::Register(*_duckdb_conn->context,
                                               _connection_ctx);
      // PG: the session user is used to resolve "$user" in catalog_search_path.
      _duckdb_conn->context->session_user = UserName();
      // PG default search_path: "$user", public. The "$user" entry is resolved
      // on each lookup to the current session user; if no schema with that name
      // exists it's silently skipped during resolution.
      std::vector<duckdb::CatalogSearchEntry> default_paths{
        duckdb::CatalogSearchEntry{std::string{DatabaseName()}, "$user"},
        duckdb::CatalogSearchEntry{std::string{DatabaseName()}, "public"},
      };
      _duckdb_conn->context->client_data->catalog_search_path->SetDefaultPaths(
        std::vector{default_paths});
      _duckdb_conn->context->client_data->catalog_search_path->Set(
        std::move(default_paths), duckdb::CatalogSetPathType::SET_DIRECTLY);

      _connection_ctx->SetSetting("session_authorization",
                                  std::string{UserName()}, false);
      // serened has no RBAC yet -- every catalog role has rolsuper = true
      _connection_ctx->SetSetting("is_superuser", "on", false);

      // Apply all user settings from startup packet
      for (const auto& user_setting : _client_parameters) {
        if (user_setting.first == kDatabaseParameter ||
            user_setting.first == kUserParameter) {
          // skip non-setting part of startup packet.
          continue;
        }
        auto res = basics::SafeCall([&] -> Result {
          _connection_ctx->SetSettingChecked(user_setting.first,
                                             user_setting.second, false);
          return {};
        });
        if (res.fail()) {
          // As per protocol description  - encountered during client hello
          // error aborts connection.
          return SendError(res.errorMessage(), res.errorNumber());
        }
      }
      // TODO:
      // ParameterStatus messages will be generated when vars from the list:
      // https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-ASYNC
      // are changed (for instance by SET command).
      static constexpr auto kParameterStatusVariables =
        std::to_array<std::string_view>({
          "application_name",
          "client_encoding",
          "DateStyle",
          "default_transaction_read_only",
          "in_hot_standby",
          "integer_datetimes",
          "IntervalStyle",
          "is_superuser",
          "scram_iterations",
          "search_path",
          "server_encoding",
          "server_version",
          "session_authorization",
          "standard_conforming_strings",
          "TimeZone",
        });
      for (const auto param : kParameterStatusVariables) {
        // TODO(codeworse): Avoid copy string in GetSetting
        SendParameterStatus(param, *_connection_ctx->Get(param));
      }

      // clang-format off
      std::array<char, 13> backend_key_data {
        PQ_MSG_BACKEND_KEY_DATA, 0x00, 0x00, 0x00, 0x0c, 0x01, 0x02, 0x03, 0x04,
        0x01, 0x01, 0x01, 0x01};
      // clang-format on
      memcpy(backend_key_data.data() + 5, &_key, sizeof(_key));
      _send.Write(ToBuffer(backend_key_data), false);

      _send.Write(ToBuffer(kReadyForQuery), true);
      std::move(cleanup).Cancel();
      _success_packet = true;
    } else if (auth.auth.auth_method == hba::UserAuth::Reject ||
               auth.auth.auth_method == hba::UserAuth::ImplicitReject) {
      SendError("CHECK ERROR MESSAGE",
                ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION);
    } else {
      SendError("NO IMPLEMENTED", ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION);
    }
  } else {
    SDB_WARN(HTTP, "Unknown packet:", protocol_ver, " connection aborted");
  }
}

void PgSQLCommTaskBase::HandleClientPacket(std::string_view packet) {
  // Crash injection for recovery testing
  SDB_IF_FAILURE("crash_on_packet") { SDB_IMMEDIATE_ABORT(); }

  // 1 byte type + 4 byte length
  SDB_ASSERT(packet.size() >= 5);
  SDB_ASSERT(_state == State::Idle);
  _state = State::Processing;
  // first byte is always packet type
  const auto msg = packet.front();
  SDB_ASSERT(absl::big_endian::Load32(packet.data() + 1) == packet.size() - 1);
  packet.remove_prefix(5);
  switch (msg) {
    case PQ_MSG_QUERY:
      return RunSimpleQuery(packet);
    case PQ_MSG_PARSE:
      return ParseQuery(packet);
    case PQ_MSG_BIND:
      return BindQuery(packet);
    case PQ_MSG_EXECUTE:
      return ExecuteQuery(packet);
    case PQ_MSG_CLOSE:
      return ExecuteClose(packet);
    case PQ_MSG_DESCRIBE:
      return DescribeQuery(packet);
    case PQ_MSG_SYNC:
      // TODO(Dronplane) implement Sync
      return;
    case PQ_MSG_FLUSH:
      // TODO: think about Flush implementation
      SDB_ASSERT(_send.GetUncommittedSize() == 0);
      _send.Commit(true);
      _success_packet = true;
      return;
    default:
      SDB_LOG_PGSQL("Unknown packet type: ", std::string_view{&msg, 1});
      return SendError("Malformed packet.", ERRCODE_PROTOCOL_VIOLATION);
  }
}

// Send RowDescription for a DuckDB prepared statement.
// Skips for DML/DDL (returns NoData instead).
void PgSQLCommTaskBase::DescribeAnalyzedQuery(
  duckdb::StatementReturnType return_type,
  const std::vector<duckdb::LogicalType>& types,
  const std::vector<std::string>& full_names,
  const std::vector<VarFormat>& formats, bool extended) {
  // DML/DDL don't return data rows
  if (return_type != duckdb::StatementReturnType::QUERY_RESULT) {
    if (extended) {
      _send.Write(ToBuffer(kNoData), extended);
    }
    return;
  }

  auto names = query::ToAliases(full_names);
  const uint16_t num_fields = types.size();

  if (num_fields == 0) {
    if (extended) {
      _send.Write(ToBuffer(kNoData), extended);
    }
    return;
  }

  const auto uncommitted_size = _send.GetUncommittedSize();
  auto* prefix_data = _send.GetContiguousData(7);
  const auto default_format = formats.empty() ? VarFormat::Text : formats[0];
  for (uint16_t i = 0; i < num_fields; ++i) {
    _send.WriteUncommitted({names[i].data(), names[i].size()});
    _send.WriteUncommitted({"\0", 1});
    int32_t table_oid = 0;
    int16_t attr_number = 0;
    int32_t type_oid = Type2Oid(types[i]);
    int16_t type_size = -1;
    int32_t type_modifier = -1;
    const auto format_code = i < formats.size() ? formats[i] : default_format;

    absl::big_endian::Store32(_send.GetContiguousData(4), table_oid);
    absl::big_endian::Store16(_send.GetContiguousData(2), attr_number);
    absl::big_endian::Store32(_send.GetContiguousData(4), type_oid);
    absl::big_endian::Store16(_send.GetContiguousData(2), type_size);
    absl::big_endian::Store32(_send.GetContiguousData(4), type_modifier);
    absl::big_endian::Store16(_send.GetContiguousData(2),
                              std::to_underlying(format_code));
  }
  prefix_data[0] = PQ_MSG_ROW_DESCRIPTION;
  absl::big_endian::Store32(prefix_data + 1,
                            _send.GetUncommittedSize() - uncommitted_size - 1);
  absl::big_endian::Store16(prefix_data + 5, num_fields);
  _send.Commit(extended);
}

void DescribeParameters(const DuckDBStatement& stmt, message::Buffer& buffer) {
  // Emit (count, [oid]*num). Client OIDs from Parse are passed to DuckDB's
  // binder as type hints, so expected_types reflects them for non-zero slots
  // (and the binder's inference for OID-0 slots). Fall back to text when the
  // binder couldn't infer.
  const auto& prepared = *stmt.prepared;
  const uint16_t num_fields =
    static_cast<uint16_t>(prepared.named_param_map.size());

  const auto uncommitted_size = buffer.GetUncommittedSize();
  auto* prefix_data = buffer.GetContiguousData(7);
  prefix_data[0] = PQ_MSG_PARAMETER_DESCRIPTION;
  absl::big_endian::Store16(prefix_data + 5, num_fields);

  for (uint16_t i = 0; i < num_fields; ++i) {
    int32_t oid = Type2Oid(ResolveExpectedType(prepared.data->value_map, i));
    absl::big_endian::Store32(buffer.GetContiguousData(4), oid);
  }

  absl::big_endian::Store32(prefix_data + 1,
                            buffer.GetUncommittedSize() - uncommitted_size - 1);
  buffer.Commit(false);
}

void PgSQLCommTaskBase::DescribePortal(const DuckDBPortal& portal) {
  SDB_ASSERT(portal.stmt);
  SDB_ASSERT(portal.stmt->prepared);
  SDB_ASSERT(portal.pending);
  const auto return_type =
    portal.stmt->prepared->GetStatementProperties().return_type;
  DescribeAnalyzedQuery(return_type, portal.pending->types,
                        portal.pending->names, portal.bind_info.output_formats);
}

void PgSQLCommTaskBase::DescribeStatement(DuckDBStatement& statement) {
  SDB_ASSERT(statement.prepared);
  DescribeParameters(statement, _send);
  auto& prepared = *statement.prepared;
  const auto return_type = prepared.GetStatementProperties().return_type;
  const auto* types = &prepared.GetTypes();
  const auto* names = &prepared.GetNames();

  // For QUERY_RESULT statements with parameters, column types may still be
  // UNKNOWN/INVALID if the binder couldn't infer them at Parse. Drive a
  // dummy bind with hint-derived values to coerce concrete types -- this
  // path only exists for Describe-before-Bind; once Bind happens the portal
  // carries fully resolved types.
  duckdb::unique_ptr<duckdb::PendingQueryResult> pending;
  if (return_type == duckdb::StatementReturnType::QUERY_RESULT &&
      !prepared.named_param_map.empty() &&
      absl::c_any_of(*types, [](const duckdb::LogicalType& t) {
        return t.id() == duckdb::LogicalTypeId::UNKNOWN ||
               t.id() == duckdb::LogicalTypeId::INVALID;
      })) {
    duckdb::vector<duckdb::Value> dummy;
    dummy.reserve(prepared.named_param_map.size());
    for (size_t i = 0; i < prepared.named_param_map.size(); ++i) {
      auto type = ResolveExpectedType(prepared.data->value_map, i);
      duckdb::Value v{"1"};
      if (!v.DefaultTryCastAs(type)) {
        v = duckdb::Value{type};
      }
      dummy.emplace_back(std::move(v));
    }
    pending = PendingQueryEnsured(prepared, dummy, true);
    if (!pending->HasError()) {
      types = &pending->types;
      names = &pending->names;
    }
  }

  DescribeAnalyzedQuery(return_type, *types, *names, {});
}

void PgSQLCommTaskBase::DescribeQuery(std::string_view packet) {
  if (packet.size() < 2 || packet.back() != '\0') {
    return SendError("Malformed Describe packet.", ERRCODE_PROTOCOL_VIOLATION);
  }
  std::string_view name{packet.data() + 1, packet.size() - 2};
  // TODO: not found error when empty
  if (packet[0] == 'P') {
    if (name.empty()) {
      DescribePortal(_anonymous_portal);
    } else {
      auto it = _portals.find(name);
      if (it == _portals.end()) {
        return SendError("Invalid portal name", ERRCODE_INVALID_CURSOR_NAME);
      }
      DescribePortal(it->second);
    }
  } else if (packet[0] == 'S') {
    if (name.empty()) {
      DescribeStatement(_anonymous_statement);
    } else {
      auto it = _statements.find(name);
      if (it == _statements.end()) {
        return SendError("Invalid statement name", ERRCODE_INVALID_CURSOR_NAME);
      }
      DescribeStatement(it->second);
    }
  } else {
    return SendError("Wrong Describe packet.", ERRCODE_PROTOCOL_VIOLATION);
  }
  _success_packet = true;
}

void PgSQLCommTaskBase::ExecuteClose(std::string_view packet) {
  if (packet.size() < 2 || packet.back() != '\0') {
    return SendError("Malformed Close packet.", ERRCODE_PROTOCOL_VIOLATION);
  }
  std::string_view name{packet.data() + 1, packet.size() - 2};
  // TODO: error if not found
  if (packet[0] == 'P') {
    if (name.empty()) {
      _anonymous_portal.Reset(*this);
    } else {
      if (auto it = _portals.find(name); it != _portals.end()) {
        it->second.Reset(*this);
        _portals.erase(it);
      }
    }
  } else if (packet[0] == 'S') {
    auto close_dependent_portals = [&](DuckDBStatement& stmt) {
      if (&stmt == _anonymous_portal.stmt) {
        _anonymous_portal.Reset(*this);
      }
      erase_if(_portals, [&](auto& e) {
        if (&stmt != e.second.stmt) {
          return false;
        }
        e.second.Reset(*this);
        return true;
      });
    };
    if (name.empty()) {
      close_dependent_portals(_anonymous_statement);
      _anonymous_statement.Reset();
    } else {
      if (auto it = _statements.find(name); it != _statements.end()) {
        close_dependent_portals(it->second);
        it->second.Reset();
        _statements.erase(it);
      }
    }
  } else {
    return SendError("Wrong Close packet.", ERRCODE_PROTOCOL_VIOLATION);
  }
  _send.Write(ToBuffer(kCloseComplete), true);
  _success_packet = true;
}

void PgSQLCommTaskBase::RunSimpleQuery(std::string_view query_string) {
  if (IsCancelled()) {
    return;
  }

  // Strip trailing null bytes from PG wire protocol
  while (!query_string.empty() && query_string.back() == '\0') {
    query_string.remove_suffix(1);
  }
  if (query_string.empty()) {
    _success_packet = true;
    return;
  }

  // Extract individual statements (simple protocol allows multi-statement)
  _anonymous_statement.Reset();
  _anonymous_statement.extracted =
    _duckdb_conn->ExtractStatements(std::string{query_string});
  if (_anonymous_statement.extracted.empty()) {
    _success_packet = true;
    return;
  }
  _anonymous_statement.current_stmt_idx = 0;

  // Execute first statement -- ProcessNextRoot handles remaining if async
  ExecuteNextSimpleStatement();
}

void PgSQLCommTaskBase::ExecuteNextSimpleStatement() {
  auto& stmt = _anonymous_statement;
  SDB_ASSERT(stmt.current_stmt_idx < stmt.extracted.size());

  auto& sql_stmt = stmt.extracted[stmt.current_stmt_idx];
  stmt.prepared = _duckdb_conn->Prepare(std::move(sql_stmt));
  if (stmt.prepared->HasError()) {
    SendError(stmt.prepared->GetErrorObject());
    return;
  }
  _current_query = stmt.prepared->query;

  // Bind (no params for simple protocol)
  DuckDBBindInfo bind_info;
  _anonymous_portal = BindStatement(stmt, std::move(bind_info));
  if (!_anonymous_portal.pending) {
    return;  // error was sent
  }

  // Describe (send RowDescription for simple protocol)
  DescribeAnalyzedQuery(stmt.prepared->GetStatementProperties().return_type,
                        _anonymous_portal.pending->types,
                        _anonymous_portal.pending->names,
                        _anonymous_portal.bind_info.output_formats, false);

  // Execute
  _pop_packet = true;
  ExecutePortal(_anonymous_portal);
  if (!_pop_packet) {
    return;  // went async -- ProcessWakeup will resume, then ProcessNextRoot
  }
  if (!_success_packet) {
    return;  // error
  }

  // Advance to next statement if any
  ++stmt.current_stmt_idx;
  while (stmt.current_stmt_idx < stmt.extracted.size()) {
    if (IsCancelled()) {
      return;
    }
    auto& next = stmt.extracted[stmt.current_stmt_idx];
    stmt.prepared = _duckdb_conn->Prepare(std::move(next));
    if (stmt.prepared->HasError()) {
      SendError(stmt.prepared->GetErrorObject());
      return;
    }
    _current_query = stmt.prepared->query;

    DuckDBBindInfo next_bind;
    _anonymous_portal = BindStatement(stmt, std::move(next_bind));
    if (!_anonymous_portal.pending) {
      return;  // error was sent
    }
    DescribeAnalyzedQuery(stmt.prepared->GetStatementProperties().return_type,
                          _anonymous_portal.pending->types,
                          _anonymous_portal.pending->names,
                          _anonymous_portal.bind_info.output_formats, false);

    _anonymous_portal.rows = 0;
    _pop_packet = true;
    ExecutePortal(_anonymous_portal);
    if (!_pop_packet) {
      return;  // went async
    }
    if (!_success_packet) {
      return;  // error
    }
    ++stmt.current_stmt_idx;
  }
}

void PgSQLCommTaskBase::ExecuteQuery(std::string_view packet) {
  if (IsCancelled()) {
    return;
  }

  const auto name_end = packet.find('\0');
  if (name_end == std::string_view::npos ||
      name_end + sizeof(int32_t) >= packet.size()) {
    return SendError("Malformed Execute packet.", ERRCODE_PROTOCOL_VIOLATION);
  }

  // TODO: handle limit, think about multiple queries
  // auto limit = std::bit_cast<int32_t>(
  //   absl::big_endian::Load32(packet.data() + name_end + 1));
  // if (limit < 0) {
  //   limit = 0;
  // }

  auto* portal = name_end == 0 ? &_anonymous_portal : nullptr;
  if (!portal) {
    auto it = _portals.find(std::string_view{packet.data(), name_end});
    if (it != _portals.end()) {
      portal = &it->second;
    }
  }
  // TODO: portal->stmt should be assert,
  // but we don't handle not having empty name portal
  if (!portal || !portal->stmt) {
    return SendError("Invalid portal name", ERRCODE_INVALID_CURSOR_NAME);
  }

  _current_query = portal->stmt->prepared->query;
  ExecutePortal(*portal);
}

std::optional<std::vector<VarFormat>> PgSQLCommTaskBase::ParseBindFormats(
  std::string_view& packet) {
  const uint16_t format_codes_size = absl::big_endian::Load16(packet.data());

  std::vector<VarFormat> formats;
  formats.reserve(format_codes_size);

  for (uint16_t i = 0; i < format_codes_size; ++i) {
    uint16_t format =
      absl::big_endian::Load16(packet.data() + (i + 1) * sizeof(uint16_t));
    switch (format) {
      case 0:
        formats.emplace_back(VarFormat::Text);
        break;
      case 1:
        formats.emplace_back(VarFormat::Binary);
        break;
      default:
        SendError(absl::StrCat("invalid format code: ", format),
                  ERRCODE_PROTOCOL_VIOLATION);
        return std::nullopt;
    }
  }
  packet.remove_prefix((format_codes_size + 1) * sizeof(uint16_t));
  return formats;
}

std::optional<DuckDBBindInfo> PgSQLCommTaskBase::ParseBindVars(
  std::string_view packet, std::string_view statement_name,
  const DuckDBStatement& stmt) {
  auto maybe_input_formats = ParseBindFormats(packet);
  if (!maybe_input_formats) {
    return std::nullopt;
  }
  auto input_formats = std::move(*maybe_input_formats);
  uint16_t params = absl::big_endian::Load16(packet.data());
  packet.remove_prefix(sizeof(int16_t));

  if (input_formats.size() > 1 && input_formats.size() != params) {
    SendError(absl::StrCat("bind message has ", input_formats.size(),
                           " parameter formats but ", params, " parameters"),
              ERRCODE_PROTOCOL_VIOLATION);
    return std::nullopt;
  }

  duckdb::vector<duckdb::Value> param_values;
  if (params > 0) {
    param_values.reserve(params);

    const auto default_format =
      input_formats.empty() ? VarFormat::Text : input_formats[0];

    for (uint16_t i = 0; i < params; ++i) {
      int32_t length = absl::big_endian::Load32(packet.data());
      packet.remove_prefix(sizeof(int32_t));

      if (length == -1) {  // NULL
        param_values.emplace_back(duckdb::Value());
        continue;
      }

      if (length < 0) {
        SendError(absl::StrCat("invalid parameter length: ", length),
                  ERRCODE_PROTOCOL_VIOLATION);
        return std::nullopt;
      }

      auto format = default_format;
      if (input_formats.size() > 1) {
        format = input_formats[i];
      }

      // Parameter types are pinned at Parse via the type-hints API.
      // VARCHAR remains the fallback for
      // the OID-0 case where the binder genuinely couldn't infer a type.
      duckdb::LogicalType param_type =
        ResolveExpectedType(stmt.prepared->data->value_map, i);

      std::string_view param{packet.data(), static_cast<size_t>(length)};
      auto param_value = DeserializeParameter(
        param_type, format, param, *_connection_ctx->EnsureCatalogSnapshot());
      if (!param_value) {
        switch (param_value.error()) {
          case DeserializeError::InvalidRepresentation:
            if (format == VarFormat::Binary) {
              SendError(
                absl::StrCat("incorrect binary data format in bind parameter ",
                             i + 1),
                ERRCODE_INVALID_BINARY_REPRESENTATION);
            } else {
              SDB_ASSERT(format == VarFormat::Text);
              SendError(
                absl::StrCat("invalid input syntax for bind parameter ", i + 1),
                ERRCODE_INVALID_TEXT_REPRESENTATION);
            }
            return std::nullopt;
        }
      }

      param_values.emplace_back(std::move(*param_value));
      packet.remove_prefix(length);
    }
  }

  auto maybe_output_formats = ParseBindFormats(packet);
  if (!maybe_output_formats) {
    return std::nullopt;
  }
  SDB_ASSERT(packet.empty());
  return DuckDBBindInfo{
    std::move(*maybe_output_formats),
    std::move(param_values),
  };
}

void PgSQLCommTaskBase::BindQuery(std::string_view packet) {
  if (IsCancelled()) {
    return;
  }

  const auto portal_end = packet.find('\0');
  if (portal_end == std::string_view::npos) {
    return SendError("Malformed Bind packet.", ERRCODE_PROTOCOL_VIOLATION);
  }
  const std::string_view portal_name{packet.data(), portal_end};
  auto [portal_it, emplaced] = portal_name.empty()
                                 ? std::pair{_portals.end(), true}
                                 : _portals.try_emplace(portal_name);
  if (!emplaced) {
    return SendError("Duplicate portal name", ERRCODE_DUPLICATE_CURSOR);
  }
  irs::Finally cleanup = [&]() noexcept {
    if (portal_it != _portals.end()) {
      _portals.erase(portal_it);
    }
  };
  packet.remove_prefix(portal_end + 1);

  const auto statement_end = packet.find('\0');
  if (statement_end == std::string_view::npos) {
    return SendError("Malformed Bind packet.", ERRCODE_PROTOCOL_VIOLATION);
  }
  const std::string_view statement_name{packet.data(), statement_end};
  packet.remove_prefix(statement_end + 1);
  auto* statement = statement_name.empty() ? &_anonymous_statement : nullptr;
  if (!statement) {
    auto statement_it = _statements.find(statement_name);
    if (statement_it == _statements.end()) {
      return SendError(absl::StrCat("prepared statement \"", statement_name,
                                    "\" does not exist"),
                       ERRCODE_INVALID_SQL_STATEMENT_NAME);
    }
    statement = &statement_it->second;
  }
  if (!statement->prepared) {
    return SendError("Statement not prepared",
                     ERRCODE_SQL_STATEMENT_NOT_YET_COMPLETE);
  }

  _current_query = statement->prepared->query;

  auto bind_info = ParseBindVars(packet, statement_name, *statement);
  if (!bind_info) {
    return;
  }
  auto portal = BindStatement(*statement, std::move(*bind_info));
  if (!portal.pending) {
    return;  // error was sent
  }
  if (portal_it == _portals.end()) {
    _anonymous_portal = std::move(portal);
  } else {
    portal_it->second = std::move(portal);
  }
  portal_it = _portals.end();
  _send.Write(ToBuffer(kBindComplete), true);
  _success_packet = true;
}

void PgSQLCommTaskBase::ParseQuery(std::string_view packet) {
  if (IsCancelled()) {
    return;
  }

  const auto statement_end = packet.find('\0');
  if (statement_end == std::string_view::npos) {
    return SendError("Malformed Parse packet.", ERRCODE_PROTOCOL_VIOLATION);
  }
  std::string_view statement_name{packet.data(), statement_end};
  auto [it, emplaced] = statement_name.empty()
                          ? std::pair{_statements.end(), true}
                          : _statements.try_emplace(statement_name);
  if (!emplaced) {
    return SendError("Duplicate statement name", ERRCODE_DUPLICATE_PSTATEMENT);
  }
  irs::Finally cleanup = [&]() noexcept {
    if (it != _statements.end()) {
      _statements.erase(it);
    }
  };
  packet.remove_prefix(statement_end + 1);

  const auto query_end = packet.find('\0');
  if (query_end == std::string_view::npos) {
    return SendError("Malformed Parse packet.", ERRCODE_PROTOCOL_VIOLATION);
  }
  _current_query = {packet.data(), query_end};
  packet.remove_prefix(query_end + 1);

  if (packet.size() < sizeof(int16_t)) {
    return SendError("Malformed Parse packet.", ERRCODE_PROTOCOL_VIOLATION);
  }
  ParamIndex num_params = absl::big_endian::Load16(packet.data());
  packet.remove_prefix(sizeof(int16_t));

  duckdb::case_insensitive_map_t<duckdb::LogicalType> type_hints;
  if (num_params > 0) {
    if (packet.size() < num_params * sizeof(int32_t)) {
      return SendError("Malformed Parse packet.", ERRCODE_PROTOCOL_VIOLATION);
    }
    auto snapshot = _connection_ctx->EnsureCatalogSnapshot();
    for (ParamIndex i = 0; i < num_params; ++i) {
      auto oid = static_cast<int32_t>(absl::big_endian::Load32(packet.data()));
      packet.remove_prefix(sizeof(int32_t));
      // Client OID 0 means "infer" -- skip so the binder is free to pick a
      // type from surrounding expression context.
      if (oid != 0) {
        type_hints.emplace(absl::StrCat(i + 1), Oid2Type(oid, *snapshot));
      }
    }
  }

  // Prepare via DuckDB instead of libpg_query + Velox. Pass client-supplied
  // OIDs as type hints so DuckDB's binder pins parameter slot types instead
  // of unifying them with surrounding column types (which would propagate
  // aliases like "oid" to a parameter the client declared as int8). After
  // Prepare, prepared->GetExpectedParameterTypes() is the single source of
  // truth for parameter types -- we don't need to retain the raw OIDs.
  auto& stmt = (it == _statements.end()) ? _anonymous_statement : it->second;
  stmt.Reset();
  stmt.prepared = _duckdb_conn->Prepare(
    std::string{_current_query}, type_hints.empty() ? nullptr : &type_hints);
  if (stmt.prepared->HasError()) {
    SendError(stmt.prepared->GetErrorObject());
    stmt.prepared.reset();
    return;
  }
  _current_query = stmt.prepared->query;

  it = _statements.end();
  _send.Write(ToBuffer(kParseComplete), true);
  _success_packet = true;
}

void PgSQLCommTaskBase::ExecutePortal(DuckDBPortal& portal) {
  SDB_ASSERT(_pop_packet);
  SDB_ASSERT(portal.stmt);
  SDB_ASSERT(portal.stmt->prepared);
  SDB_ASSERT(portal.pending);
  if (IsCancelled()) {
    return;
  }
  {
    std::lock_guard lock{_queue_mutex};
    if (IsCancelled()) {
      portal.pending.reset();
      return;
    }
    _current_portal = &portal;
  }
  auto state = ProcessState::DonePacket;
  do {
    state = ProcessQueryResult();
  } while (state == ProcessState::More);
  _pop_packet = state == ProcessState::DonePacket;
}

duckdb::unique_ptr<duckdb::PendingQueryResult>
PgSQLCommTaskBase::PendingQueryEnsured(duckdb::PreparedStatement& prepared,
                                       duckdb::vector<duckdb::Value>& values,
                                       bool allow_stream_result) {
  const auto& props = prepared.GetStatementProperties();
  const auto db_name = DatabaseName();
  _connection_ctx->EnsureCatalogSnapshot();
  if (props.modified_databases.contains(db_name)) {
    _connection_ctx->EnsureRocksDBTransaction();
    _connection_ctx->EnsureRocksDBSnapshot();
  } else if (props.read_databases.contains(db_name)) {
    if (_connection_ctx->IsExplicitTransaction()) {
      _connection_ctx->EnsureRocksDBTransaction();
    }
    _connection_ctx->EnsureRocksDBSnapshot();
  }
  return prepared.PendingQuery(values, allow_stream_result);
}

auto PgSQLCommTaskBase::BindStatement(DuckDBStatement& stmt,
                                      DuckDBBindInfo bind_info)
  -> DuckDBPortal {
  DuckDBPortal portal{.serialization_context{.buffer = &_send}};
  FillContext(*_connection_ctx, portal.serialization_context);

  portal.bind_info = std::move(bind_info);

  auto& prepared = *stmt.prepared;
  portal.pending =
    PendingQueryEnsured(prepared, portal.bind_info.param_values, true);
  if (portal.pending->HasError()) {
    SendError(portal.pending->GetErrorObject());
    portal.pending.reset();
    return portal;
  }

  portal.stmt = &stmt;
  BuildColumnSerializers(portal);

  return portal;
}

void PgSQLCommTaskBase::BuildColumnSerializers(DuckDBPortal& portal) {
  SDB_ASSERT(portal.stmt);
  SDB_ASSERT(portal.stmt->prepared);
  SDB_ASSERT(portal.pending);
  portal.columns_serializers.clear();

  auto& prepared = *portal.stmt->prepared;
  auto return_type = prepared.GetStatementProperties().return_type;

  // DML/DDL don't return data rows
  if (return_type != duckdb::StatementReturnType::QUERY_RESULT) {
    return;
  }

  auto& types = portal.pending->types;
  const auto columns_count = types.size();
  if (columns_count == 0) {
    return;
  }
  portal.columns_serializers.reserve(columns_count);

  const auto& formats = portal.bind_info.output_formats;
  const auto default_format = formats.empty() ? VarFormat::Text : formats[0];

  for (uint16_t i = 0; i < columns_count; ++i) {
    const auto format = i < formats.size() ? formats[i] : default_format;
    portal.columns_serializers.push_back(
      GetSerialization(types[i], format, portal.serialization_context));
  }
}

void PgSQLCommTaskBase::SendBatch(const duckdb::DataChunk& chunk) {
  SDB_ASSERT(_current_portal);
  auto& portal = *_current_portal;

  const auto batch_rows = chunk.size();
  if (batch_rows == 0) {
    return;
  }

  const uint16_t batch_columns = chunk.ColumnCount();
  if (batch_columns == 0) {
    return;
  }

  // DML: extract affected row count (single row with count)
  auto return_type =
    portal.stmt->prepared->GetStatementProperties().return_type;
  if (return_type == duckdb::StatementReturnType::CHANGED_ROWS) {
    SDB_ASSERT(batch_rows == 1);
    portal.rows += chunk.GetValue(0, 0).GetValue<int64_t>();
    return;
  } else if (return_type == duckdb::StatementReturnType::NOTHING) {
    return;
  }

  SDB_ASSERT(batch_columns == portal.columns_serializers.size());

  // Convert columns to RecursiveUnifiedVectorFormat (like Velox DecodedVector)
  std::vector<duckdb::RecursiveUnifiedVectorFormat> decoded_columns(
    batch_columns);
  for (uint16_t i = 0; i < batch_columns; ++i) {
    duckdb::Vector::RecursiveToUnifiedFormat(chunk.data[i], batch_rows,
                                             decoded_columns[i]);
  }

  for (duckdb::idx_t row = 0; row < batch_rows; ++row) {
    ++portal.rows;
    const auto uncommitted_size = _send.GetUncommittedSize();
    auto* prefix_data = _send.GetContiguousData(7);
    for (uint16_t column = 0; column < batch_columns; ++column) {
      portal.columns_serializers[column](portal.serialization_context,
                                         decoded_columns[column], row);
    }
    prefix_data[0] = PQ_MSG_DATA_ROW;
    absl::big_endian::Store32(
      prefix_data + 1, _send.GetUncommittedSize() - uncommitted_size - 1);
    absl::big_endian::Store16(prefix_data + 5, batch_columns);
    _send.Commit(false);
  }
}

auto PgSQLCommTaskBase::ProcessQueryResult() -> ProcessState {
  SDB_ASSERT(_current_portal);
  auto& portal = *_current_portal;
  SDB_ASSERT(portal.stmt);

  if (portal.pending) {
    auto status = portal.pending->ExecuteTask(
      [weak = weak_from_this(), feature = &_feature] {
        feature->ScheduleProcessWakeup(weak);
      });
    switch (status) {
      case duckdb::PendingExecutionResult::RESULT_READY: {
        // Execution complete -- get the streaming result
        portal.result = portal.pending->Execute();
        portal.pending.reset();
        if (portal.result->HasError()) {
          SendError(portal.result->GetErrorObject());
          ReleaseResult(portal);
          _success_packet = false;
          return ProcessState::DonePacket;
        }
        // Fall through to fetch first chunk
        break;
      }
      case duckdb::PendingExecutionResult::RESULT_NOT_READY:
      case duckdb::PendingExecutionResult::NO_TASKS_AVAILABLE:
        // More work needed -- continue polling
        return ProcessState::More;
      case duckdb::PendingExecutionResult::BLOCKED:
        // Blocked -- callback was registered by ExecuteTask.
        return ProcessState::Wait;
      case duckdb::PendingExecutionResult::EXECUTION_FINISHED:
        // Same as RESULT_READY -- execution complete
        portal.result = portal.pending->Execute();
        portal.pending.reset();
        if (portal.result->HasError()) {
          SendError(portal.result->GetErrorObject());
          ReleaseResult(portal);
          _success_packet = false;
          return ProcessState::DonePacket;
        }
        break;
      case duckdb::PendingExecutionResult::EXECUTION_ERROR:
        SendError(portal.pending->GetErrorObject());
        ReleaseResult(portal);
        _success_packet = false;
        return ProcessState::DonePacket;
    }
  }

  SDB_ASSERT(portal.result);

  auto chunk = portal.result->FetchRaw();
  if (!chunk || chunk->size() == 0) {
    // Done -- send command complete
    auto stmt_type = portal.result->statement_type;
    SendCommandComplete(stmt_type, portal.rows);

    ReleaseResult(portal);
    _success_packet = true;
    return ProcessState::DonePacket;
  }

  SendBatch(*chunk);
  return ProcessState::More;
}

namespace {

std::string_view CatalogObjectTag(duckdb::CatalogType t) {
  using duckdb::CatalogType;
  switch (t) {
    case CatalogType::TABLE_ENTRY:
      return "TABLE";
    case CatalogType::VIEW_ENTRY:
      return "VIEW";
    case CatalogType::INDEX_ENTRY:
      return "INDEX";
    case CatalogType::SCHEMA_ENTRY:
      return "SCHEMA";
    case CatalogType::SEQUENCE_ENTRY:
      return "SEQUENCE";
    case CatalogType::TYPE_ENTRY:
      return "TYPE";
    case CatalogType::MACRO_ENTRY:
    case CatalogType::TABLE_MACRO_ENTRY:
      return "FUNCTION";
    case CatalogType::DATABASE_ENTRY:
      return "DATABASE";
    default:
      return {};
  }
}

struct CommandTag {
  std::string tag;
  // The "effective" statement type -- same as `prepared.data->statement_type`
  // except for EXECUTE, where it's the underlying prepared's type. Drives the
  // `INSERT 0 N` / `UPDATE N` / etc. row-count formatting.
  duckdb::StatementType effective_type;
};

CommandTag BuildCommandTag(const duckdb::PreparedStatement& prepared);

// `EXECUTE name` reports the underlying statement's tag in PG (e.g. a
// SELECT-backed prepared statement yields "SELECT N"). Look the referenced
// statement up in DuckDB's client-local prepared-statement catalog.
CommandTag ExecuteTagForPrepared(const duckdb::PreparedStatement& prepared) {
  auto* unbound = prepared.data->unbound_statement.get();
  if (!unbound) {
    return {"EXECUTE", duckdb::StatementType::EXECUTE_STATEMENT};
  }
  auto& exec_stmt = unbound->Cast<duckdb::ExecuteStatement>();
  auto& client_data = duckdb::ClientData::Get(*prepared.context);
  auto it = client_data.prepared_statements.find(exec_stmt.name);
  if (it == client_data.prepared_statements.end() || !it->second) {
    return {"EXECUTE", duckdb::StatementType::EXECUTE_STATEMENT};
  }
  using duckdb::StatementType;
  const auto inner = it->second->statement_type;
  switch (inner) {
    case StatementType::SELECT_STATEMENT:
      return {"SELECT", inner};
    case StatementType::INSERT_STATEMENT:
      return {"INSERT", inner};
    case StatementType::UPDATE_STATEMENT:
      return {"UPDATE", inner};
    case StatementType::DELETE_STATEMENT:
      return {"DELETE", inner};
    default:
      return {duckdb::StatementTypeToString(inner), inner};
  }
}

// PG-compatible CommandComplete tag. Not all DuckDB statement types exist
// in PG (e.g. PRAGMA); those fall back to DuckDB's string representation,
// which is better than nothing.
CommandTag BuildCommandTag(const duckdb::PreparedStatement& prepared) {
  const auto stmt_type = prepared.data->statement_type;
  const auto* unbound = prepared.data->unbound_statement.get();
  auto make = [&](std::string s) -> CommandTag {
    return {std::move(s), stmt_type};
  };
  using duckdb::StatementType;
  switch (stmt_type) {
    case StatementType::SELECT_STATEMENT:
      return make("SELECT");
    case StatementType::INSERT_STATEMENT:
      return make("INSERT");
    case StatementType::UPDATE_STATEMENT:
      return make("UPDATE");
    case StatementType::DELETE_STATEMENT:
      if (unbound->Cast<duckdb::DeleteStatement>().node->is_truncate) {
        return make("TRUNCATE TABLE");
      }
      return make("DELETE");
    case StatementType::COPY_STATEMENT:
      return make("COPY");
    case StatementType::MERGE_INTO_STATEMENT:
      return make("MERGE");
    case StatementType::PREPARE_STATEMENT:
      return make("PREPARE");
    case StatementType::EXECUTE_STATEMENT:
      return ExecuteTagForPrepared(prepared);
    case StatementType::EXPLAIN_STATEMENT:
      return make("EXPLAIN");
    case StatementType::VACUUM_STATEMENT:
      return make("VACUUM");
    case StatementType::ANALYZE_STATEMENT:
      return make("ANALYZE");
    case StatementType::ATTACH_STATEMENT:
      return make("ATTACH");
    case StatementType::DETACH_STATEMENT:
      return make("DETACH");
    case StatementType::SET_STATEMENT:
    case StatementType::VARIABLE_SET_STATEMENT:
      return make("SET");
    case StatementType::LOAD_STATEMENT:
      return make("LOAD");
    case StatementType::CALL_STATEMENT:
      return make("CALL");
    case StatementType::CREATE_STATEMENT:
    case StatementType::CREATE_FUNC_STATEMENT: {
      if (unbound) {
        const auto& create_stmt = unbound->Cast<duckdb::CreateStatement>();
        if (create_stmt.info) {
          auto obj = CatalogObjectTag(create_stmt.info->type);
          if (!obj.empty()) {
            return make(absl::StrCat("CREATE ", obj));
          }
        }
      }
      return make("CREATE");
    }
    case StatementType::DROP_STATEMENT: {
      if (unbound) {
        const auto& drop_stmt = unbound->Cast<duckdb::DropStatement>();
        if (drop_stmt.info) {
          if (drop_stmt.info->type == duckdb::CatalogType::PREPARED_STATEMENT) {
            return make(drop_stmt.info->name.empty() ? "DEALLOCATE ALL"
                                                     : "DEALLOCATE");
          }
          auto obj = CatalogObjectTag(drop_stmt.info->type);
          if (!obj.empty()) {
            return make(absl::StrCat("DROP ", obj));
          }
        }
      }
      return make("DROP");
    }
    case StatementType::ALTER_STATEMENT: {
      if (unbound) {
        const auto& alter_stmt = unbound->Cast<duckdb::AlterStatement>();
        if (alter_stmt.info) {
          switch (alter_stmt.info->type) {
            case duckdb::AlterType::ALTER_TABLE:
              return make("ALTER TABLE");
            case duckdb::AlterType::ALTER_VIEW:
              return make("ALTER VIEW");
            case duckdb::AlterType::ALTER_SEQUENCE:
              return make("ALTER SEQUENCE");
            case duckdb::AlterType::ALTER_DATABASE:
              return make("ALTER DATABASE");
            default:
              break;
          }
        }
      }
      return make("ALTER");
    }
    case StatementType::TRANSACTION_STATEMENT: {
      if (unbound) {
        const auto& tx = unbound->Cast<duckdb::TransactionStatement>();
        if (tx.info) {
          switch (tx.info->type) {
            case duckdb::TransactionType::BEGIN_TRANSACTION:
              return make("BEGIN");
            case duckdb::TransactionType::COMMIT:
              return make("COMMIT");
            case duckdb::TransactionType::ROLLBACK:
              return make("ROLLBACK");
            default:
              break;
          }
        }
      }
      return make("TRANSACTION");
    }
    default:
      return make(duckdb::StatementTypeToString(stmt_type));
  }
}

}  // namespace

void PgSQLCommTaskBase::DeallocateNamedStatement(std::string_view name) {
  if (name.empty()) {
    // DEALLOCATE ALL: clear every named statement and any portal built on
    // one. Anonymous state stays -- PG's DEALLOCATE only touches named
    // prepared statements on the session.
    erase_if(_portals, [&](auto& e) {
      if (e.second.stmt == nullptr || e.second.stmt == &_anonymous_statement) {
        return false;
      }
      e.second.Reset(*this);
      return true;
    });
    for (auto& [_, stmt] : _statements) {
      stmt.Reset();
    }
    _statements.clear();
    return;
  }
  auto it = _statements.find(name);
  if (it == _statements.end()) {
    return;
  }
  // Close any portal built on this statement first.
  if (it->second.prepared && &it->second == _anonymous_portal.stmt) {
    _anonymous_portal.Reset(*this);
  }
  erase_if(_portals, [&](auto& e) {
    if (&it->second != e.second.stmt) {
      return false;
    }
    e.second.Reset(*this);
    return true;
  });
  it->second.Reset();
  _statements.erase(it);
}

void PgSQLCommTaskBase::SendCommandComplete(duckdb::StatementType stmt_type,
                                            uint64_t rows) {
  auto& prepared = *_current_portal->stmt->prepared;
  auto tag = BuildCommandTag(prepared);
  auto return_type = prepared.GetStatementProperties().return_type;

  // DEALLOCATE target is tracked separately from DuckDB's catalog: our
  // extended-protocol named statements live in `_statements`. Clean them up
  // here so the name is no longer reachable.
  if (stmt_type == duckdb::StatementType::DROP_STATEMENT && prepared.data &&
      prepared.data->unbound_statement) {
    auto& drop_stmt =
      prepared.data->unbound_statement->Cast<duckdb::DropStatement>();
    if (drop_stmt.info &&
        drop_stmt.info->type == duckdb::CatalogType::PREPARED_STATEMENT) {
      DeallocateNamedStatement(drop_stmt.info->name);
    }
  }

  const auto uncommitted_size = _send.GetUncommittedSize();
  auto* prefix_data = _send.GetContiguousData(5);
  _send.WriteUncommitted({tag.tag.data(), tag.tag.size()});

  if (return_type == duckdb::StatementReturnType::CHANGED_ROWS) {
    if (tag.effective_type == duckdb::StatementType::INSERT_STATEMENT) {
      _send.WriteUncommitted({" 0 ", 3});
    } else {
      _send.WriteUncommitted({" ", 1});
    }
    _send.WriteContiguousData(basics::kIntStrMaxLen, [&](auto* data) {
      char* buf = reinterpret_cast<char*>(data);
      char* ptr = absl::numbers_internal::FastIntToBuffer(rows, buf);
      return static_cast<size_t>(ptr - buf);
    });
  } else if (return_type == duckdb::StatementReturnType::QUERY_RESULT) {
    _send.WriteUncommitted({" ", 1});
    _send.WriteContiguousData(basics::kIntStrMaxLen, [&](auto* data) {
      char* buf = reinterpret_cast<char*>(data);
      char* ptr = absl::numbers_internal::FastIntToBuffer(rows, buf);
      return static_cast<size_t>(ptr - buf);
    });
  }
  // NOTHING: no count appended

  _send.WriteUncommitted({"\0", 1});
  prefix_data[0] = PQ_MSG_COMMAND_COMPLETE;
  absl::big_endian::Store32(prefix_data + 1,
                            _send.GetUncommittedSize() - uncommitted_size - 1);
  _send.Commit(false);
}

void PgSQLCommTaskBase::ParseClientParameters(std::string_view data) {
  SDB_ASSERT(_client_parameters.empty());
  while (true) {
    const auto name_end = data.find('\0');
    if (name_end == std::string_view::npos) {
      return;
    }
    const std::string_view name{data.data(), name_end};
    if (name.empty()) {
      // psql at least adds \0 as termintaion flag and we get empty name.
      return;
    }
    data.remove_prefix(name_end + 1);

    const auto value_end = data.find('\0');
    const bool last = value_end == std::string_view::npos;
    const std::string_view value{data.data(), last ? data.size() : value_end};
    _client_parameters.try_emplace(name, value);
    if (last) {
      return;
    }
    data.remove_prefix(value_end + 1);
  }
}

void PgSQLCommTaskBase::CancelPacket() {
  std::unique_lock lock{_queue_mutex};
  _cancel_packet.store(true, std::memory_order_relaxed);
  if (_duckdb_conn) {
    _duckdb_conn->Interrupt();
  }
  _copy_queue.Abort(lock);
}

void PgSQLCommTaskBase::ReleaseResult(DuckDBPortal& portal) {
  std::lock_guard lock{_queue_mutex};
  portal.pending.reset();
  portal.result.reset();
}

std::string_view PgSQLCommTaskBase::DatabaseName() const noexcept {
  auto db_name = _client_parameters.find(kDatabaseParameter);
  if (db_name != _client_parameters.end()) {
    return db_name->second;
  }
  return StaticStrings::kDefaultDatabase;
}

std::string_view PgSQLCommTaskBase::UserName() const noexcept {
  auto user_name = _client_parameters.find(kUserParameter);
  if (user_name != _client_parameters.end()) {
    return user_name->second;
  }
  return StaticStrings::kDefaultUser;
}

void PgSQLCommTaskBase::SendNotices() {
  if (!_connection_ctx) {
    return;
  }
  for (const auto& notice : _connection_ctx->StealNotices()) {
    SendNotice(PQ_MSG_NOTICE_RESPONSE, notice);
  }
}

void PgSQLCommTaskBase::SendParameterStatus(std::string_view name,
                                            std::string_view value) {
  const auto uncommitted_size = _send.GetUncommittedSize();
  auto* prefix_data = _send.GetContiguousData(5);
  _send.WriteUncommitted(name);
  _send.WriteUncommitted({"\0", 1});
  _send.WriteUncommitted(value);
  _send.WriteUncommitted({"\0", 1});
  prefix_data[0] = PQ_MSG_PARAMETER_STATUS;
  absl::big_endian::Store32(prefix_data + 1,
                            _send.GetUncommittedSize() - uncommitted_size - 1);
  _send.Commit(false);
}

std::string_view PgSQLCommTaskBase::StartPacket() noexcept {
  std::lock_guard lock{_queue_mutex};
  SDB_ASSERT(!_queue.empty());
  _cancel_packet.store(false, std::memory_order_relaxed);
  _pop_packet = true;
  _success_packet = false;
  _current_packet_type = _queue.front()[0];
  return _queue.front();
}

void PgSQLCommTaskBase::FinishPacket() noexcept try {
  SendNotices();
  if (!_success_packet && IsCancelled()) {
    // TODO(mbkkt) flush is probably unnecessary here
    _send.Write(ToBuffer(kPacketCancelled), true);
  }
  if (_state != State::ClientHello) {
    // Failure brings us in the recovery state.
    // But simple query never brings us to ErrorRecovery as there is nothing
    // to skip. And if we are poping Sync packet - our recovery is done.
    if (_success_packet || _current_packet_type == PQ_MSG_QUERY ||
        _current_packet_type == PQ_MSG_SYNC) {
      _state = State::Idle;
    } else if (!_success_packet) {
      _state = State::ErrorRecovery;
    }
  }

  if (_current_packet_type == PQ_MSG_QUERY ||
      _current_packet_type == PQ_MSG_SYNC) {
    _send.Write(ToBuffer(kReadyForQuery), true);
  }
  std::lock_guard lock{_queue_mutex};
  if (_current_packet_type == PQ_MSG_QUERY ||
      _current_packet_type == PQ_MSG_EXECUTE) {
    if (_current_portal) {
      _current_portal->pending.reset();
      _current_portal->result.reset();
      _current_portal = nullptr;
    }
  }
  _current_packet_type = 0;
  _pop_packet = false;
  SDB_ASSERT(!_queue.empty());
  _queue.pop();
  if (Stopped()) {
    return;
  }

  if (!_queue.empty()) {
    _feature.ScheduleProcessFirst(weak_from_this());
  } else {
    SetIOTimeoutImpl();
  }
} catch (...) {
  SDB_ERROR(HTTP, "<pgsql> connection closed due to exception in finalizing ",
            std::bit_cast<size_t>(this));
  Stop();
}

void PgSQLCommTaskBase::SendNotice(char type, const pg::SqlErrorData& what) {
  char sql_state[pg::kSqlStateSize];
  pg::UnpackSqlState(sql_state, what.errcode);
  SendNotice(type, what.errmsg, {sql_state, pg::kSqlStateSize}, what.errdetail,
             what.errhint, what.context, _current_query, what.cursorpos);
}

void PgSQLCommTaskBase::SendError(const duckdb::ErrorData& error) {
  SafeCall([&] { error.Throw(); });
}

void PgSQLCommTaskBase::SendError(std::string_view message, int errcode) {
  char sql_state[pg::kSqlStateSize];
  pg::UnpackSqlState(sql_state, errcode);
  SendNotice(PQ_MSG_ERROR_RESPONSE, message, {sql_state, pg::kSqlStateSize});
}

void PgSQLCommTaskBase::SendNotice(char type, std::string_view message,
                                   std::string_view sqlstate,
                                   std::string_view error_detail /*= {} */,
                                   std::string_view error_hint /*= {} */,
                                   std::string_view context /*= {} */,
                                   std::string_view query /* = {} */,
                                   int cursor_pos /* = -1 */) {
  SDB_ASSERT(type == PQ_MSG_ERROR_RESPONSE || type == PQ_MSG_NOTICE_RESPONSE);
  const auto uncommitted_size = _send.GetUncommittedSize();
  auto* prefix_data = _send.GetContiguousData(5);
  _send.WriteUncommitted(type == PQ_MSG_ERROR_RESPONSE
                           ? std::string_view{"SERROR\0VERROR\0C", 15}
                           : std::string_view{"SWARNING\0VWARNING\0C", 19});
  _send.WriteUncommitted({"\0M", 2});
  _send.WriteUncommitted(message);
  _send.WriteUncommitted({"\0C", 2});
  _send.WriteUncommitted(sqlstate);
  // we have to iff as "empty" field is not permitted
  if (error_detail.size()) {
    _send.WriteUncommitted({"\0D", 2});
    _send.WriteUncommitted(error_detail);
  }
  if (error_hint.size()) {
    _send.WriteUncommitted({"\0H", 2});
    _send.WriteUncommitted(error_hint);
  }
  if (context.size()) {
    _send.WriteUncommitted({"\0W", 2});
    _send.WriteUncommitted(context);
  }

  // TODO: 'q' (internal query) and 'p' (internal position) fields should only
  // be sent for errors originating from internally-generated queries
  // (e.g. PL/pgSQL functions, triggers).
  // if (query.size() > 1) {
  //   _send.WriteUncommitted({"\0q", 2});
  //   _send.WriteUncommitted({query.data(), query.size() - 1});
  // }

  if (cursor_pos > 0) {
    _send.WriteUncommitted({"\0P", 2});
    // TODO: zero copy serialization here
    _send.WriteUncommitted(absl::StrCat(cursor_pos));
  }
  _send.WriteUncommitted({"\0", 2});
  prefix_data[0] = type;
  absl::big_endian::Store32(prefix_data + 1,
                            _send.GetUncommittedSize() - uncommitted_size - 1);
  _send.Commit(true);
}

template<rest::SocketType T>
PgSQLCommTask<T>::PgSQLCommTask(rest::GeneralServer& server,
                                ConnectionInfo info,
                                std::shared_ptr<rest::AsioSocket<T>> so)
  : GenericCommTask<T, PgSQLCommTaskBase>{server, std::move(info),
                                          std::move(so)} {
  this->_protocol->socket.lowest_layer().set_option(
    asio_ns::ip::tcp::no_delay{true});
}

template<rest::SocketType T>
void PgSQLCommTask<T>::Start() {
  SDB_DEBUG(HTTP, "<pgsql> opened connection ", std::bit_cast<size_t>(this));
  asio_ns::post(this->_protocol->context.io_context,
                [self = this->shared_from_this()] {
                  if constexpr (T == rest::SocketType::Ssl) {
                    // SSL starts as plain text and waits for actual SSL request
                    // before doing handshake to allow client change connection
                    // type over same port. We do not support such choices but
                    // still have to follow the protocol.
                    basics::downCast<PgSQLCommTask<T>>(*self)
                      .template AsyncReadSome<true>();
                  } else {
                    basics::downCast<PgSQLCommTask<T>>(*self)
                      .template AsyncReadSome<false>();
                  }
                });
}

template<rest::SocketType T>
void PgSQLCommTask<T>::SendAsync(message::SequenceView data) noexcept {
  if (_send_should_close.load(std::memory_order_acquire)) {
    asio_ns::dispatch(
      this->_protocol->context.io_context,
      [self = this->shared_from_this(), ec = this->_close_error] {
        basics::downCast<PgSQLCommTask<T>>(*self).Base::Close(ec);
      });
    return;
  }
  if (data.Empty()) {
    this->_send.FlushDone();
    return;
  }
  SDB_LOG_PGSQL("Sending Packet:", data.Print());
  asio_ns::async_write(
    this->_protocol->socket, data,
    [self = this->shared_from_this()](asio_ns::error_code ec, size_t nwrite) {
      auto& task = basics::downCast<PgSQLCommTask<T>>(*self);
      // TODO: Make PGSQL connection statistics
      // if (!ec) {
      //   task.statistics(1UL).ADD_SENT_BYTES(nwrite);
      // }
      task._send.FlushDone();
    });
}

template<rest::SocketType T>
bool PgSQLCommTask<T>::ReadCallback(asio_ns::error_code ec) {
  if (!ec) {
    // Inspect the received data
    size_t nparsed = 0;
    const auto buffers = this->_protocol->buffer.data();
    auto it = asio_ns::buffer_sequence_begin(buffers);
    const auto end = asio_ns::buffer_sequence_end(buffers);
    for (; it != end; ++it) {
      const char* data = static_cast<const char*>(it->data());
      const char* end = data + it->size();
      do {
        size_t datasize = end - data;
        const auto old_size = _packet.size();
        _packet.resize(_packet.size() + datasize);
        memcpy(_packet.data() + old_size, data, datasize);
        nparsed += datasize;
        data += datasize;
        if (this->_state.load() == State::ClientHello &&
            _packet.size() > MAX_STARTUP_PACKET_LENGTH) {
          SDB_DEBUG(HTTP, "Too long startup packet. Dropping connection ptr",
                    std::bit_cast<size_t>(this));
          this->Close();
          return false;
        }
      } while (SDB_UNLIKELY(data < end));
      while (!_pending_len ||
             (_pending_len && _packet.size() >= _pending_len)) {
        if (!_pending_len) {
          // For historical reasons, the very first message sent
          // by the client (the startup message) has no initial
          // message-type byte.
          const size_t type_offset =
            static_cast<size_t>(this->_state.load() != State::ClientHello);
          if (_packet.size() >= (4 + type_offset)) {
            _pending_len =
              absl::big_endian::Load32(_packet.data() + type_offset) +
              type_offset;
          } else {
            // received too small number of bytes, we need more to get at least
            // packet length
            break;
          }
        }
        if (_pending_len && _packet.size() >= _pending_len) {
          SDB_LOG_PGSQL("Received: ", _pending_len, " bytes Packet ");
          SDB_LOG_PGSQL("Packet:", [&] {
            std::string ss_str;
            absl::strings_internal::OStringStream ss{&ss_str};
            for (size_t i = 0; i < _pending_len; ++i) {
              ss << std::hex << (int)_packet[i] << " ";
            }
            return ss_str;
          }());
          const auto hello_passed = this->_state.load() != State::ClientHello;
          if (hello_passed && _packet[0] == PQ_MSG_TERMINATE) {
            SDB_DEBUG(HTTP, "Termination request for ",
                      std::bit_cast<size_t>(this));
            this->Stop();
            _packet.clear();
            _pending_len = 0;
            return false;
          }
          {
            if constexpr (T == rest::SocketType::Ssl) {
              if (!this->_ssl_handshake_passed) {
                // Wait for SSL request
                ProtocolVersion protocol_ver =
                  absl::big_endian::Load32(_packet.data() + 4);
                if (protocol_ver == NEGOTIATE_SSL_CODE) {
                  asio_ns::write(this->_protocol->socket.next_layer(),
                                 asio_ns::buffer(kYes));
                  auto cb = [this](const asio_ns::error_code& ec) mutable {
                    if (ec) {
                      SDB_DEBUG(HTTP, "error during TLS handshake: '",
                                ec.message(), "'");
                      this->Stop();
                      return;
                    }
                    // Would be fair to restart keep-alive timeout
                    // to not include what have passed during handshake
                    this->SetIOTimeout();
                    this->_ssl_handshake_passed = true;
                    _packet.clear();
                    this->_protocol->buffer.consume(_pending_len);
                    _pending_len = 0;
                    // Now switch to reads from SSL stream
                    asio_ns::post(this->_protocol->context.io_context,
                                  [self = this->shared_from_this()] {
                                    basics::downCast<PgSQLCommTask<T>>(*self)
                                      .template AsyncReadSome<false>();
                                  });
                  };
                  this->SetIOTimeout();
                  this->_protocol->handshake(std::move(cb));
                  // stop reading with this mode.
                  return false;
                } else if (protocol_ver != CANCEL_REQUEST_CODE) {
                  this->Stop();
                  _packet.clear();
                  _pending_len = 0;
                  return false;
                }
              }
            }
            std::string packet{_packet.data(), _pending_len};
            // TODO: error if there's no copy in progress
            if (hello_passed && packet.starts_with(PQ_MSG_COPY_DATA)) {
              this->_copy_queue.AppendCopyDataMsg(std::move(packet));
            } else if (hello_passed && packet.starts_with(PQ_MSG_COPY_DONE)) {
              this->_copy_queue.AppendCopyDoneMsg();
            } else {
              std::lock_guard lock{this->_queue_mutex};
              this->_queue.emplace(std::move(packet));
              if (this->_queue.size() == 1 && !this->Stopped()) {
                this->_feature.ScheduleProcessFirst(this->weak_from_this());
              }
            }
          }
          _packet.erase(0, _pending_len);
          _pending_len = 0;
        }
      }
    }
    SDB_ASSERT(nparsed < std::numeric_limits<size_t>::max());
    // Remove consumed data from receive buffer.
    this->_protocol->buffer.consume(nparsed);
    // And count it in the statistics:
    // TODO Make PGSQL statistics
    // this->statistics(1UL).ADD_RECEIVED_BYTES(nparsed);
  } else {
    // got a connection error
    if (ec == asio_ns::error::misc_errors::eof) {
      SDB_TRACE(HTTP, "Eof with ptr ", std::bit_cast<size_t>(this));
    } else {
      SDB_DEBUG(HTTP, "Error while reading from socket: '", ec.message(), "'");
      this->Close(ec);
      return false;
    }
  }
  return true;
}

template<rest::SocketType T>
void PgSQLCommTask<T>::SetIOTimeoutImpl() {
  if (!this->_queue.empty()) {
    return;
  }
  double secs = this->_general_server_feature.keepAliveTimeout();
  if (secs <= 0) {
    return;
  }
  auto millis = std::chrono::milliseconds(static_cast<int64_t>(secs * 1000));
  this->_protocol->timer.expires_after(millis);
  this->_protocol->timer.async_wait(
    [weak = this->weak_from_this()](const asio_ns::error_code& ec) {
      std::shared_ptr<rest::CommTask> self;
      if (ec || !(self = weak.lock())) {  // was canceled / deallocated
        return;
      }

      auto& task = basics::downCast<PgSQLCommTask<T>>(*self);
      task.TimeoutStop();
    });
}

template<rest::SocketType T>
void PgSQLCommTask<T>::TimeoutStop() {
  std::lock_guard lock{this->_queue_mutex};
  if (!this->_queue.empty() || this->Stopped()) {
    return;
  }
  SDB_INFO(HTTP, "keep alive timeout, closing stream!");
  SDB_ASSERT(!this->_current_portal);
  this->_send.Write(ToBuffer(kTimeoutTermination), true);
  CloseImpl({});
}

template<rest::SocketType T>
void PgSQLCommTask<T>::CloseImpl(asio_ns::error_code close_error) {
  this->_close_error = close_error;
  this->_send_should_close.store(true, std::memory_order_release);
  if (this->_queue.empty()) {
    this->_send.Commit(true);
  }
}

template class PgSQLCommTask<rest::SocketType::Tcp>;
template class PgSQLCommTask<rest::SocketType::Ssl>;
template class PgSQLCommTask<rest::SocketType::Unix>;

}  // namespace sdb::pg
