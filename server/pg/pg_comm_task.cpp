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
#include <absl/strings/internal/ostringstream.h>
#include <absl/strings/str_format.h>
#include <axiom/optimizer/Plan.h>
#include <axiom/optimizer/QueryGraphContext.h>
#include <axiom/optimizer/VeloxHistory.h>
#include <velox/common/memory/HashStringAllocator.h>
#include <velox/core/PlanFragment.h>
#include <velox/exec/Task.h>
#include <velox/expression/Expr.h>
#include <velox/type/Type.h>

#include <cfloat>
#include <string_view>

#include "app/app_server.h"
#include "auth/role_utils.h"
#include "basics/application-exit.h"
#include "basics/assert.h"
#include "basics/endian.h"
#include "basics/global_resource_monitor.h"
#include "basics/logger/logger.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "general_server/general_server_feature.h"
#include "pg/connection_context.h"
#include "pg/hba.h"
#include "pg/pg_feature.h"
#include "pg/pg_types.h"
#include "pg/protocol.h"
#include "pg/serialize.h"
#include "pg/sql_collector.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_resolver.h"
#include "query/config.h"
#include "query/cursor.h"
#include "query/utils.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "libpq/pqcomm.h"
#include "nodes/pg_list.h"
#include "protocol.h"
#include "tcop/cmdtag.h"
#include "tcop/utility.h"
#include "utils/elog.h"
LIBPG_QUERY_INCLUDES_END

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

constexpr std::array<char, 5> kEmptyResult{
  PQ_MSG_EMPTY_QUERY_RESPONSE, 0x00, 0x00, 0x00, 0x04,
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

template<size_t N>
BufferView ToBuffer(const std::array<char, N>& data) {
  return BufferView{data.data(), data.size()};
}

}  // namespace

PgSQLCommTaskBase::PgSQLCommTaskBase(rest::GeneralServer& server,
                                     ConnectionInfo info)
  : rest::CommTask(server, std::move(info)),
    _feature{server.server().getFeature<PostgresFeature>()},
    _send{64, 4096, 4096,
          [this](message::SequenceView data) { this->SendAsync(data); }} {}

PgSQLCommTaskBase::~PgSQLCommTaskBase() {
  if (_key != 0) {
    _feature.UnregisterTask(_key);
  }
}

void PgSQLCommTaskBase::Process() {
  std::lock_guard lock{_execution_mutex};
  try {
    SDB_ASSERT(_state != State::Processing);
    const auto packet = StartPacket();
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
  } catch (const SqlException& e) {
    SendNotice(PQ_MSG_ERROR_RESPONSE, e.error(), _current_query);
  } catch (const std::exception& e) {
    SendError(e.what(), "42601");
  } catch (...) {
    SendError("Unhandled exception", "XX000");
  }
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
  if (protocol_ver == NEGOTIATE_SSL_CODE ||
      protocol_ver == NEGOTIATE_GSS_CODE) {
    // TODO: HANDLE SSL / GSS here
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
      SDB_WARN("xxxxx", Logger::REQUESTS,
               "User name not set. Terminating connection ",
               std::bit_cast<size_t>(this));
      return;
    }

    auto database = _feature.server()
                      .getFeature<catalog::CatalogFeature>()
                      .Global()
                      .GetSnapshot()
                      ->GetDatabase(DatabaseName());
    if (!database) {
      // sending invalid schema name as SQLSTATE
      return SendError(
        absl::StrCat("Database ", DatabaseName(), " is not accessible"),
        "3F000");
    }

    _connection_ctx = std::make_shared<ConnectionContext>(
      UserName(), DatabaseName(), database->GetId());

    const auto& ci = GetConnectionInfo();
    [[maybe_unused]] hba::Client client{
      .user_name = UserName(),
      .database_name = DatabaseName(),
      .host_name = ci.client_address,
      .raddr =
        ci.sas_len ? reinterpret_cast<const sockaddr*>(&ci.sas) : nullptr,
      .ssl_in_use = false,
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
      _database = std::move(database);
      // _session_ctx.auth_method = auth.auth.auth_method;
      // _session_ctx.session_user = auth.user;
      // _session_ctx.user = _session_ctx.user;
      // _session_ctx.database = auth.database;
      // _session_ctx.system_user = UserName();
      _state = State::Idle;
      _send.Write(ToBuffer(kAuthOk), false);
      _key = _feature.RegisterTask(*this);
      // clang-format off
      std::array<char, 13> backend_key_data {
        PQ_MSG_BACKEND_KEY_DATA, 0x00, 0x00, 0x00, 0x0c, 0x01, 0x02, 0x03, 0x04,
        0x01, 0x01, 0x01, 0x01};
      // clang-format on
      memcpy(backend_key_data.data() + 5, &_key, sizeof(_key));
      _send.Write(ToBuffer(backend_key_data), false);

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
          "scram_iterations",
          "search_path",
          "server_encoding",
          "server_version",
          "standard_conforming_strings",
          "TimeZone",
        });
      for (const auto param : kParameterStatusVariables) {
        SendParameterStatus(param, GetDefaultVariable(param));
      }

      _send.Write(ToBuffer(kReadyForQuery), true);
      std::move(cleanup).Cancel();
      _success_packet = true;
    } else if (auth.auth.auth_method == hba::UserAuth::Reject ||
               auth.auth.auth_method == hba::UserAuth::ImplicitReject) {
      SendError("CHECK ERROR MESSAGE", "28000");
    } else {
      SendError("NO IMPLEMENTED", "28000");
    }
  } else {
    SDB_WARN("xxxxx", Logger::REQUESTS, "Unknown packet:", protocol_ver,
             " connection aborted");
  }
}

void PgSQLCommTaskBase::HandleClientPacket(std::string_view packet) {
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
      return SendError("Malformed packet.", "08P01");
  }
}

void PgSQLCommTaskBase::DescribeAnalyzedQuery(
  const SqlStatement& statement, const std::vector<VarFormat>& formats,
  bool extended) {
  const auto& query = *statement.query;
  const auto& output_type = query.GetOutputType();
  const uint16_t num_fields = output_type ? output_type->size() : 0;
  const auto* pg_node = castNode(RawStmt, statement.tree.GetRoot());
  SDB_ASSERT(pg_node);

  // We want **don't** want to describe columns in the following cases:
  // 1. Query is CALL some_procedure()
  // 2. Query is without logical plan and doesn't have columns at all,
  //    for example CREATE and DROP). But query may be without logical plan, but
  //    with columns: SHOW and SHOW ALL. For these cases we **want** to describe
  //    columns
  // 3. Query is INSERT, DELETE, UPDATE or MERGE and **without** EXPLAIN
  if ((pg_node->stmt->type == T_CallStmt) ||
      (!query.IsDataQuery() && num_fields == 0) ||
      (query.IsDML() && output_type->nameOf(0) == "rows")) {
    if (extended) {
      _send.Write(ToBuffer(kNoData), extended);
    }
    return;
  }
  const auto uncommitted_size = _send.GetUncommittedSize();
  auto* prefix_data = _send.GetContiguousData(7);
  SDB_ASSERT(formats.size() <= 1 || num_fields == formats.size());
  const auto default_format = formats.empty() ? VarFormat::Text : formats[0];
  for (uint16_t i = 0; i < num_fields; ++i) {
    const auto& name = sdb::query::CleanColumnNames(output_type->nameOf(i));
    _send.WriteUncommitted(name);
    _send.WriteUncommitted({"\0", 1});
    int32_t table_oid = 0;
    int16_t attr_number = 0;
    int32_t type_oid = GetTypeOID(output_type->childAt(i));
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

void DescribeParameters(const std::vector<velox::TypePtr>& param_types,
                        message::Buffer& buffer) {
  const auto uncommitted_size = buffer.GetUncommittedSize();
  auto* prefix_data = buffer.GetContiguousData(7);
  const uint16_t num_fields = param_types.size();
  for (const auto& type : param_types) {
    SDB_ASSERT(type);
    const auto oid = GetTypeOID(type);
    SDB_ASSERT(oid > 0);
    absl::big_endian::Store32(buffer.GetContiguousData(4), oid);
  }
  prefix_data[0] = PQ_MSG_PARAMETER_DESCRIPTION;
  absl::big_endian::Store32(prefix_data + 1,
                            buffer.GetUncommittedSize() - uncommitted_size - 1);
  absl::big_endian::Store16(prefix_data + 5, num_fields);
  buffer.Commit(false);
}

void PgSQLCommTaskBase::DescribePortal(const SqlPortal& portal) {
  SDB_ASSERT(portal.stmt);
  if (!portal.stmt->query && portal.stmt->NextRoot(_connection_ctx)) {
    SendWarnings();
  }
  SDB_ASSERT(portal.stmt->query);
  DescribeAnalyzedQuery(*portal.stmt, portal.bind_info.output_formats);
}

void PgSQLCommTaskBase::DescribeStatement(SqlStatement& statement) {
  if (!statement.query && statement.NextRoot(_connection_ctx)) {
    SendWarnings();
  }
  DescribeParameters(statement.params.types, _send);
  SDB_ASSERT(statement.query);
  DescribeAnalyzedQuery(statement, {});
}

void PgSQLCommTaskBase::DescribeQuery(std::string_view packet) {
  if (packet.size() < 2 || packet.back() != '\0') {
    return SendError("Malformed Describe packet.", "08P01");
  }
  std::string_view name{packet.data() + 1, packet.size() - 2};
  // TODO: not found error when empty
  if (packet[0] == 'P') {
    if (name.empty()) {
      DescribePortal(_anonymous_portal);
    } else {
      auto it = _portals.find(name);
      if (it == _portals.end()) {
        return SendError("Invalid portal name", "34000");
      }
      DescribePortal(it->second);
    }
  } else if (packet[0] == 'S') {
    if (name.empty()) {
      DescribeStatement(_anonymous_statement);
    } else {
      auto it = _statements.find(name);
      if (it == _statements.end()) {
        return SendError("Invalid statement name", "26000");
      }
      DescribeStatement(it->second);
    }
  } else {
    return SendError("Wrong Describe packet.", "08P01");
  }
  _success_packet = true;
}

void PgSQLCommTaskBase::ExecuteClose(std::string_view packet) {
  if (packet.size() < 2 || packet.back() != '\0') {
    return SendError("Malformed Close packet.", "08P01");
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
    auto close_dependent_portals = [&](SqlStatement& stmt) {
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
    return SendError("Wrong Close packet.", "08P01");
  }
  _send.Write(ToBuffer(kCloseComplete), true);
  _success_packet = true;
}

void PgSQLCommTaskBase::RunSimpleQuery(std::string_view query_string) {
  if (IsCancelled()) {
    return;
  }

  // Note that a simple Query message also destroys the unnamed portal.
  _anonymous_portal.Reset(*this);
  try {
    _current_query = query_string;
    // Note that a simple Query message also destroys the unnamed statement.
    _anonymous_statement.Reset();
    _anonymous_statement = MakeStatement(query_string);
    _current_query = _anonymous_statement.query_string->view();
    if (!_anonymous_statement.tree.list) {
      // no query
      _send.Write(ToBuffer(kEmptyResult), false);
      _success_packet = true;
      return;
    }
    if (!_anonymous_statement.query &&
        _anonymous_statement.NextRoot(_connection_ctx)) {
      SendWarnings();
    }

    if (_anonymous_statement.query) {
      _anonymous_portal = BindStatement(_anonymous_statement, {});
      DescribeAnalyzedQuery(_anonymous_statement,
                            _anonymous_portal.bind_info.output_formats, false);
      ExecutePortal(_anonymous_portal, 0);
    } else if (!IsCancelled()) {
      _send.Write(ToBuffer(kEmptyResult), false);
      _success_packet = true;
    }
  } catch (const SqlException& e) {
    SendNotice(PQ_MSG_ERROR_RESPONSE, e.error(), _current_query);
  } catch (const std::exception& e) {
    SendError(e.what(), "42601");
  }
}

SqlStatement PgSQLCommTaskBase::MakeStatement(std::string_view query_string) {
  SDB_LOG_PGSQL("Parsing: <", query_string, ">");
  SqlStatement res;
  res.query_string = std::make_shared<QueryString>(query_string);
  // Note use resetMemoryContext for re-binding!
  res.memory_context = pg::CreateMemoryContext();
  res.tree = {
    .list = pg::Parse(*res.memory_context, *res.query_string),
    .root_idx = 0,
  };
  SendWarnings();
  return res;
}

void PgSQLCommTaskBase::ExecuteQuery(std::string_view packet) {
  if (IsCancelled()) {
    return;
  }

  const auto name_end = packet.find('\0');
  if (name_end == std::string_view::npos ||
      name_end + sizeof(int32_t) >= packet.size()) {
    return SendError("Malformed Execute packet.", "08P01");
  }

  auto limit = std::bit_cast<int32_t>(
    absl::big_endian::Load32(packet.data() + name_end + 1));
  if (limit < 0) {
    limit = 0;
  }

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
    return SendError("Invalid portal name", "34000");
  }

  try {
    _current_query = portal->stmt->query_string->view();

    if (!portal->stmt->query && portal->stmt->NextRoot(_connection_ctx)) {
      SendWarnings();
    }

    ExecutePortal(*portal, static_cast<size_t>(limit));
  } catch (const SqlException& e) {
    SendNotice(PQ_MSG_ERROR_RESPONSE, e.error(), _current_query);
  } catch (const std::exception& e) {
    SendError(e.what(), "42601");
  }
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

std::optional<BindInfo> PgSQLCommTaskBase::ParseBindVars(
  std::string_view packet, const std::string_view statement_name,
  const std::vector<velox::TypePtr>& param_types) {
  // TODO: reuse vectors for BindInfo
  auto maybe_input_formats = ParseBindFormats(packet);
  if (!maybe_input_formats) {
    return std::nullopt;
  }
  auto input_formats = std::move(*maybe_input_formats);
  ParamIndex params = absl::big_endian::Load16(packet.data());
  packet.remove_prefix(sizeof(int16_t));

  if (params != param_types.size()) {
    SendError(absl::StrCat("bind message supplies ", params,
                           " parameters, but prepared "
                           "statement \"",
                           statement_name, "\" requires ", param_types.size()),
              ERRCODE_PROTOCOL_VIOLATION);
    return std::nullopt;
  }

  if (input_formats.size() > 1 && input_formats.size() != params) {
    SendError(absl::StrCat("bind message has ", input_formats.size(),
                           " parameter formats but ", params, " parameters"),
              ERRCODE_PROTOCOL_VIOLATION);
    return std::nullopt;
  }

  std::vector<std::shared_ptr<velox::Variant>> param_values;
  if (params > 0) {
    param_values.reserve(params);

    // If format_codes_size = 0, then all the vars have default format(text)
    // If format_codes_size = 1, then all the vars have one format code
    // Otherwise each var has its own format code
    const auto default_format =
      input_formats.empty() ? VarFormat::Text : input_formats[0];

    for (ParamIndex i = 0; i < params; ++i) {
      int32_t length = absl::big_endian::Load32(packet.data());
      packet.remove_prefix(sizeof(int32_t));

      if (length == -1) {  // NULL
        auto val = std::make_shared<velox::Variant>(velox::TypeKind::UNKNOWN);
        param_values.emplace_back(std::move(val));
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

      SDB_ASSERT(param_types[i]);
      std::string_view param{packet.data(), static_cast<size_t>(length)};
      auto param_value = DeserializeParameter(*param_types[i], format, param);
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
              SendError(absl::StrCat("invalid input syntax for type ",
                                     param_types[i]->toString()),
                        ERRCODE_INVALID_TEXT_REPRESENTATION);
            }
            return std::nullopt;
        }
      }

      auto val = std::make_shared<velox::Variant>(std::move(*param_value));
      param_values.emplace_back(std::move(val));
      packet.remove_prefix(length);
    }
  }

  auto maybe_output_formats = ParseBindFormats(packet);
  if (!maybe_output_formats) {
    return std::nullopt;
  }
  SDB_ASSERT(packet.empty());
  return BindInfo{
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
    return SendError("Malformed Bind packet.", "08P01");
  }
  const std::string_view portal_name{packet.data(), portal_end};
  auto [portal_it, emplaced] = portal_name.empty()
                                 ? std::pair{_portals.end(), true}
                                 : _portals.try_emplace(portal_name);
  if (!emplaced) {
    return SendError("Duplicate portal name", "42P03");
  }
  irs::Finally cleanup = [&]() noexcept {
    if (portal_it != _portals.end()) {
      _portals.erase(portal_it);
    }
  };
  packet.remove_prefix(portal_end + 1);

  const auto statement_end = packet.find('\0');
  if (statement_end == std::string_view::npos) {
    return SendError("Malformed Bind packet.", "08P01");
  }
  const std::string_view statement_name{packet.data(), statement_end};
  packet.remove_prefix(statement_end + 1);
  auto* statement = statement_name.empty() ? &_anonymous_statement : nullptr;
  if (!statement) {
    auto statement_it = _statements.find(statement_name);
    if (statement_it == _statements.end()) {
      return SendError("Invalid statement name", "26000");
    }
    statement = &statement_it->second;
  }
  if (!statement->query_string) {
    return SendError("Statement not completed", "03000");
  }

  if (statement->RootCount() > 1) {
    return SendError(
      "cannot insert multiple commands into a prepared statement", "42601");
  }

  if (!statement->query && statement->NextRoot(_connection_ctx)) {
    SendWarnings();
  }

  _current_query = statement->query_string->view();
  auto bind_info =
    ParseBindVars(packet, statement_name, statement->params.types);
  if (!bind_info) {
    return;
  }
  try {
    if (portal_it == _portals.end()) {
      _anonymous_portal = BindStatement(*statement, std::move(*bind_info));
    } else {
      portal_it->second = BindStatement(*statement, std::move(*bind_info));
    }
    portal_it = _portals.end();
    SendWarnings();
    _send.Write(ToBuffer(kBindComplete), true);
    _success_packet = true;
  } catch (const SqlException& e) {
    SendNotice(PQ_MSG_ERROR_RESPONSE, e.error(), _current_query);
  } catch (const std::exception& e) {
    SendError(e.what(), "42601");
  }
}

void PgSQLCommTaskBase::ParseQuery(std::string_view packet) {
  if (IsCancelled()) {
    return;
  }

  const auto statement_end = packet.find('\0');
  if (statement_end == std::string_view::npos) {
    return SendError("Malformed Parse packet.", "08P01");
  }
  std::string_view statement_name{packet.data(), statement_end};
  auto [it, emplaced] = statement_name.empty()
                          ? std::pair{_statements.end(), true}
                          : _statements.try_emplace(statement_name);
  if (!emplaced) {
    return SendError("Duplicate statement name", "42P05");
  }
  irs::Finally cleanup = [&]() noexcept {
    if (it != _statements.end()) {
      _statements.erase(it);
    }
  };
  packet.remove_prefix(statement_end + 1);

  const auto query_end = packet.find('\0');
  if (query_end == std::string_view::npos) {
    return SendError("Malformed Parse packet.", "08P01");
  }
  _current_query = {packet.data(), query_end};
  packet.remove_prefix(query_end + 1);

  if (packet.size() < sizeof(int16_t)) {
    return SendError("Malformed Parse packet.", "08P01");
  }
  ParamIndex num_params = absl::big_endian::Load16(packet.data());
  packet.remove_prefix(sizeof(int16_t));

  if (num_params > 0) {
    if (packet.size() < num_params * sizeof(int32_t)) {
      return SendError("Malformed Parse packet.", "08P01");
    }
    for (ParamIndex i = 0; i < num_params; ++i) {
      // TODO: use type OID from client if it's specified
      [[maybe_unused]] int32_t param_oid =
        absl::big_endian::Load32(packet.data());
      packet.remove_prefix(sizeof(int32_t));
    }
  }

  try {
    size_t root_count = 0;
    if (it == _statements.end()) {
      _anonymous_statement = MakeStatement(_current_query);
      root_count = _anonymous_statement.RootCount();
      _current_query = _anonymous_statement.query_string->view();
    } else {
      it->second = MakeStatement(_current_query);
      root_count = it->second.RootCount();
      _current_query = it->second.query_string->view();
    }

    if (root_count > 1) {
      return SendError(
        "cannot insert multiple commands into a prepared statement", "42601");
    }

    it = _statements.end();
    _send.Write(ToBuffer(kParseComplete), true);
    _success_packet = true;
  } catch (const SqlException& e) {
    SendNotice(PQ_MSG_ERROR_RESPONSE, e.error(), _current_query);
  } catch (const std::exception& e) {
    SendError(e.what(), "42601");
  }
}

void PgSQLCommTaskBase::ExecutePortal(SqlPortal& portal, size_t limit) {
  SDB_ASSERT(portal.stmt);
  if (IsCancelled()) {
    return;
  }
  SDB_ASSERT(portal.stmt->query);
  auto cursor = portal.stmt->query->MakeCursor(
    [user_task = basics::downCast<PgSQLCommTaskBase>(shared_from_this())] {
      user_task->WakeupHandler();
    });
  if (RegisterCursor(std::move(cursor), portal)) {
    SDB_ASSERT(&portal == _current_portal);
    // Throws if soft shutdown is ongoing!
    _pop_packet = true;
    auto state = ProcessState::DonePacket;
    do {
      state = ProcessQueryResult();
    } while (state == ProcessState::More);
    _pop_packet = state == ProcessState::DonePacket;
  }
}

void PgSQLCommTaskBase::NextRootPortal() {
  std::lock_guard lock{_execution_mutex};
  try {
    // We don't need to hold cancellation mutex here
    // as active portal is only changed during packet execution.
    // But here we are just re-startin next root in simple query.
    SDB_ASSERT(_current_portal);
    ExecutePortal(*_current_portal, 0);
  } catch (const SqlException& e) {
    SendNotice(PQ_MSG_ERROR_RESPONSE, e.error(), _current_query);
  } catch (const std::exception& e) {
    SendError(e.what(), "42601");
  }
}

auto PgSQLCommTaskBase::BindStatement(SqlStatement& stmt, BindInfo bind_info)
  -> SqlPortal {
  SqlPortal portal{.serialization_context{.buffer = &_send}};
  FillContext(*_connection_ctx, portal.serialization_context);

  portal.bind_info = std::move(bind_info);
  auto& param_values = portal.bind_info.param_values;
  // TODO: Check if bind was already and reparse AST
  if (!param_values.empty()) {
    auto types = std::move(stmt.params.types);
    stmt = MakeStatement(stmt.query_string->view());
    stmt.params.values = std::move(param_values);
    stmt.params.types = std::move(types);
  }
  portal.stmt = &stmt;

  if (!portal.stmt->query && portal.stmt->NextRoot(_connection_ctx)) {
    SendWarnings();
  }
  SDB_ASSERT(stmt.query);
  const auto& output_type = stmt.query->GetOutputType();

  // DDL does not have output type
  if (!output_type) {
    return portal;
  }
  const auto columns_count = output_type->size();
  if (columns_count == 0) {
    return portal;
  }
  SDB_ASSERT(columns_count > 0);
  portal.columns_serializers.reserve(columns_count);

  const auto& formats = portal.bind_info.output_formats;
  const auto default_format = formats.empty() ? VarFormat::Text : formats[0];

  for (uint16_t i = 0; i < columns_count; ++i) {
    const auto& column_type = output_type->childAt(i);
    const auto format = i < formats.size() ? formats[i] : default_format;
    portal.columns_serializers.push_back(
      GetSerialization(column_type, format, portal.serialization_context));
  }

  return portal;
}

void PgSQLCommTaskBase::WakeupHandler() noexcept {
  std::lock_guard lock{_execution_mutex};
  auto state = ProcessState::Wait;
  irs::Finally cleanup = [&] noexcept {
    if (state == ProcessState::DonePacket) {
      FinishPacket();
    }
  };
  try {
    do {
      state = ProcessState::DonePacket;
      state = ProcessQueryResult();
    } while (state == ProcessState::More);
  } catch (const SqlException& e) {
    SendNotice(PQ_MSG_ERROR_RESPONSE, e.error(), _current_query);
  } catch (const std::exception& e) {
    SendError(e.what(), "42601");
  }
}

void PgSQLCommTaskBase::SendBatch(const velox::RowVectorPtr& batch) {
  SDB_ASSERT(_current_portal);
  auto& portal = *_current_portal;
  const velox::vector_size_t batch_rows = batch ? batch->size() : 0;
  if (batch_rows == 0) {
    return;
  }

  const auto& output_type = portal.stmt->query->GetOutputType();
  const uint16_t batch_columns = batch->childrenSize();
  SDB_ASSERT(batch_columns == output_type->size());
  if (batch_columns == 0) {
    return;
  }

  // If it's DML but name of column isn't "rows" it means it's explain for DML.
  // In such case we should send rows as usual.
  if (portal.stmt->query->IsDML() && output_type->nameOf(0) == "rows") {
    const auto& column = batch->childAt(0);
    const auto& cell = column->variantAt(0);
    const auto& value = cell.value<int64_t>();
    SDB_ENSURE(value >= 0, ERROR_INTERNAL);
    portal.rows += value;
    return;
  }

  const auto& formats = portal.bind_info.output_formats;
  SDB_ASSERT(formats.size() <= 1 || batch_columns == formats.size());

  std::vector<velox::DecodedVector> decoded_columns;
  decoded_columns.reserve(batch_columns);
  for (uint16_t i = 0; i < batch_columns; ++i) {
    const auto& column = *batch->childAt(i);
    decoded_columns.emplace_back().decode(column, false);
  }

  for (velox::vector_size_t row = 0; row < batch_rows; ++row) {
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
  SDB_ASSERT(portal.stmt->query);
  SDB_ASSERT(portal.cursor);

  velox::RowVectorPtr batch;
  const auto [state, r] = portal.cursor->Next(batch);

  if (state == query::Cursor::Process::Wait) {
    return ProcessState::Wait;
  }
  SendWarnings();
  if (r.ok()) {
    SendBatch(batch);
  } else {
    // TODO: Make SDB ERROR->SQLSTATE translation.
    // System error here is just a placeholder
    if (r.errorNumber() == ERROR_QUERY_USER_WARN) {
      SendNotice(PQ_MSG_NOTICE_RESPONSE,
                 pg::SqlErrorData{
                   .errcode = ERRCODE_WARNING,
                   .errmsg = std::string{r.errorMessage()},
                 },
                 {});
    } else {
      SendError(r.errorMessage(), "58000");
    }
  }
  if (state == query::Cursor::Process::More) {
    return ProcessState::More;
  }
  SDB_ASSERT(state == query::Cursor::Process::Done);
  SendCommandComplete(portal.stmt->tree, portal.rows);

  // In case of EXPLAIN statement
  // batch should be cleaned before cursor,
  // because cursor stores batch's memory pool
  batch.reset();
  ReleaseCursor(portal);
  if (_current_packet_type == PQ_MSG_QUERY &&
      portal.stmt->NextRoot(_connection_ctx)) {
    SendWarnings();
    _feature.ScheduleContinueProcessing(weak_from_this());
    return ProcessState::DoneQuery;
  }
  _success_packet = !r.fail();
  return ProcessState::DonePacket;
}

void PgSQLCommTaskBase::SendCommandComplete(const SqlTree& tree,
                                            uint64_t rows) {
  SDB_ASSERT(tree.root_idx);
  auto* root = castNode(Node, tree.GetRoot());

  const auto command_tag = CreateCommandTag(root);
  const auto uncommitted_size = _send.GetUncommittedSize();
  auto* prefix_data = _send.GetContiguousData(5);
  {
    Size taglen = 0;
    const char* tagname = GetCommandTagNameAndLen(command_tag, &taglen);
    _send.WriteUncommitted({tagname, taglen});
  }
  if (command_tag_display_rowcount(command_tag)) {
    _send.WriteContiguousData(3 + basics::kIntStrMaxLen, [&](auto* data) {
      char* const buf = reinterpret_cast<char*>(data);
      char* ptr = buf;
      if (command_tag == CMDTAG_INSERT) {
        *ptr++ = ' ';
        *ptr++ = '0';
      }
      *ptr++ = ' ';
      ptr = absl::numbers_internal::FastIntToBuffer(rows, ptr);
      return static_cast<size_t>(ptr - buf);
    });
  }
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
  std::lock_guard lock{_queue_mutex};
  _cancel_packet.store(true, std::memory_order_relaxed);
  if (_current_portal && _current_portal->cursor) {
    _current_portal->cursor->RequestCancel();
  }
}

void PgSQLCommTaskBase::ReleaseCursor(SqlPortal& portal) {
  std::lock_guard lock{_queue_mutex};
  portal.cursor.reset();
}

bool PgSQLCommTaskBase::RegisterCursor(std::unique_ptr<query::Cursor> cursor,
                                       SqlPortal& portal) {
  SDB_ASSERT(cursor);
  std::lock_guard lock{_queue_mutex};
  if (IsCancelled()) {
    // No need to remove wakeup handler.
    // Should be none set yet.
    return false;
  }
  SDB_ASSERT(!portal.cursor);
  portal.cursor = std::move(cursor);
  _current_portal = &portal;
  return true;
}

std::string_view PgSQLCommTaskBase::DatabaseName() const noexcept {
  auto db_name = _client_parameters.find(kDatabaseParameter);

  if (db_name != _client_parameters.end()) {
    if (db_name->second == StaticStrings::kPgDefaultDatabase) {
      return StaticStrings::kSystemDatabase;
    }
    return db_name->second;
  }
  return StaticStrings::kSystemDatabase;
}

std::string_view PgSQLCommTaskBase::UserName() const noexcept {
  auto user_name = _client_parameters.find(kUserParameter);
  if (user_name != _client_parameters.end()) {
    if (user_name->second == StaticStrings::kPgDefaultUser) {
      return auth::kRootUserName;
    }
    return user_name->second;
  }
  return {};
}

void PgSQLCommTaskBase::SendWarnings() {
  // TODO(#334) We should handle warnings
  // for (const auto& warn : _warnings.stealAll()) {
  //   SendNotice(PqMsg_NoticeResponse, warn, _current_query);
  // }
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

std::string_view PgSQLCommTaskBase::StartPacket() {
  std::scoped_lock lock(_queue_mutex);
  SDB_ASSERT(!_queue.empty());
  _cancel_packet.store(false, std::memory_order_relaxed);
  _pop_packet = true;
  _success_packet = false;
  _current_packet_type = _queue.front()[0];
  return _queue.front();
}

void PgSQLCommTaskBase::FinishPacket() noexcept try {
  SendWarnings();
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
    _current_portal = nullptr;
  }
  _current_packet_type = 0;
  SDB_ASSERT(!_queue.empty());
  _queue.pop();
  if (Stopped()) {
    return;
  }

  if (!_queue.empty()) {
    Reschedule();
  } else {
    SetIOTimeoutImpl();
  }
} catch (...) {
  SDB_FATAL("xxxxx", Logger::REQUESTS,
            "<pgsql> connection closed due to exception in finalizing ",
            std::bit_cast<size_t>(this));
  Stop();
}

void PgSQLCommTaskBase::Reschedule() {
  _feature.ScheduleProcessing(weak_from_this());
}

void PgSQLCommTaskBase::SendNotice(char type, const pg::SqlErrorData& what,
                                   std::string_view query) {
  std::string sql_state;
  sql_state.resize(pg::kSqlStateSize, '0');
  pg::UnpackSqlState(sql_state, what.errcode);
  SendNotice(type, what.errmsg, sql_state, what.errdetail, what.errhint, query,
             what.cursorpos);
}

void PgSQLCommTaskBase::SendError(std::string_view message,
                                  std::string_view sqlstate) {
  SendNotice(PQ_MSG_ERROR_RESPONSE, message, sqlstate);
}

void PgSQLCommTaskBase::SendError(std::string_view message, int errcode) {
  std::string sql_state;
  sql_state.resize(pg::kSqlStateSize, '0');
  pg::UnpackSqlState(sql_state, errcode);
  SendError(message, sql_state);
}

void PgSQLCommTaskBase::SendNotice(char type, std::string_view message,
                                   std::string_view sqlstate,
                                   std::string_view error_detail /*= {} */,
                                   std::string_view error_hint /*= {} */,
                                   std::string_view query /* = {} */,
                                   int cursor_pos /* = 0 */) {
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
  if (query.size() > 1) {
    _send.WriteUncommitted({"\0q", 2});
    _send.WriteUncommitted({query.data(), query.size() - 1});
  }
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
  SDB_DEBUG("xxxxx", Logger::REQUESTS, "<pgsql> opened connection ",
            std::bit_cast<size_t>(this));
  asio_ns::post(this->_protocol->context.io_context,
                [self = this->shared_from_this()] {
                  basics::downCast<PgSQLCommTask<T>>(*self).AsyncReadSome();
                });
}

template<rest::SocketType T>
void PgSQLCommTask<T>::SendAsync(message::SequenceView data) {
  SDB_ASSERT(!data.Empty());
  SDB_LOG_PGSQL("Sending Packet:", data.Print());
  std::lock_guard lock{this->_queue_mutex};
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
          SDB_DEBUG("xxxxx", Logger::REQUESTS,
                    "Too long startup packet. Dropping connection ptr",
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
          if (this->_state.load() != State::ClientHello &&
              _packet[0] == PQ_MSG_TERMINATE) {
            SDB_DEBUG("xxxxx", Logger::REQUESTS, "Termination request for ",
                      std::bit_cast<size_t>(this));
            this->Stop();
            _packet.clear();
            _pending_len = 0;
            return false;
          }
          {
            std::string packet{_packet.data(), _pending_len};
            std::scoped_lock lock{this->_queue_mutex};
            this->_queue.emplace(std::move(packet));
            if (this->_queue.size() == 1 && !this->Stopped()) {
              this->Reschedule();
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
      SDB_TRACE("xxxxx", Logger::REQUESTS, "Eof with ptr ",
                std::bit_cast<size_t>(this));
    } else {
      SDB_DEBUG("xxxxx", Logger::REQUESTS, "Error while reading from socket: '",
                ec.message(), "'");
      this->Close(ec);
      return false;
    }
  }
  return true;
}

template<rest::SocketType T>
void PgSQLCommTask<T>::SetIOTimeout() {
  std::scoped_lock lock(this->_queue_mutex);
  SetIOTimeoutImpl();
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
  SDB_INFO("xxxxx", Logger::REQUESTS, "keep alive timeout, closing stream!");
  SDB_ASSERT(!this->_current_portal);
  this->_send.Write(ToBuffer(kTimeoutTermination), true);
  Base::Close();
}

template class PgSQLCommTask<rest::SocketType::Tcp>;
template class PgSQLCommTask<rest::SocketType::Ssl>;
template class PgSQLCommTask<rest::SocketType::Unix>;

}  // namespace sdb::pg
