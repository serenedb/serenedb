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

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <yaclib/async/future.hpp>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/coro.hpp>
#include <yaclib/coro/future.hpp>
#include <yaclib/coro/on.hpp>
#include <yaclib/util/helper.hpp>

#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/pending_query_result.hpp>
#include <duckdb/main/prepared_statement.hpp>
#include <duckdb/main/prepared_statement_data.hpp>
#include <duckdb/main/query_result.hpp>
#include <duckdb/main/valid_checker.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/parser/parsed_data/copy_info.hpp>
#include <duckdb/parser/statement/copy_statement.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <duckdb/transaction/transaction_context.hpp>

#include "basics/asio_ns.h"
#include "basics/duckdb_engine.h"
#include "basics/message_buffer.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "connector/duckdb_client_state.h"
#include "network/connection.h"
#include "network/io_executor.h"
#include "network/pg/duck_executor.h"
#include "network/pg/pg_frame_codec.h"
#include "network/pg/query_pump.h"
#include "network/pg/wire_frames.h"
#include "network/socket.h"
#include "pg/command_tag.h"
#include "pg/connection_context.h"
#include "pg/copy_in_bridge.h"
#include "pg/copy_messages_queue.h"
#include "pg/duckdb_sql_statement.h"
#include "pg/errcodes.h"
#include "pg/pg_types.h"
#include "pg/protocol.h"
#include "pg/serialize.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/node_hash_map.h"

#include <absl/base/internal/endian.h>
#include <absl/strings/str_cat.h>

#include <source_location>

#include <duckdb/common/case_insensitive_map.hpp>

namespace sdb::network::pg {

// `ssl` is non-null when TLS is configured; then the pg endpoint runs as
// SocketKind::MaybeTls and answers SSLRequest with 'S' + an in-band upgrade.
struct PgServerContext {
  asio_ns::ssl::context* ssl = nullptr;
};

// A bound portal: a prepared statement plus its pending/streaming execution and
// the result column formats chosen at Bind.
struct Portal {
  sdb::pg::DuckDBStatement* stmt = nullptr;
  duckdb::unique_ptr<duckdb::PendingQueryResult> pending;
  duckdb::unique_ptr<duckdb::QueryResult> result;
  sdb::pg::DuckDBBindInfo bind_info;
};

inline duckdb::LogicalType ResolveExpectedType(const auto& value_map,
                                               uint16_t id) {
  const auto it = value_map.find(absl::StrCat(id + 1));
  if (it != value_map.end()) {
    const auto type = it->second->GetValue().type();
    if (type.id() != duckdb::LogicalTypeId::UNKNOWN &&
        type.id() != duckdb::LogicalTypeId::INVALID) {
      return type;
    }
  }
  return duckdb::LogicalTypeId::VARCHAR;
}

// DuckDB's ErrorData preserves the original exception_ptr; Throw() rethrows it,
// so a serenedb SqlException (with its sqlstate/detail/hint) survives execution
// intact and is caught typed by the command loop -- instead of being flattened
// through DuckErrorToSqlData, which only sees the bare message.
[[noreturn]] inline void ThrowDuck(const duckdb::ErrorData& error) {
  error.Throw();
}

// Parse/Bind/Describe/Execute/Close/Flush. An error in one of these arms
// ignore-till-Sync; an error in simple Query (self-syncing) does not.
inline bool IsExtended(char type) {
  switch (type) {
    case PQ_MSG_PARSE:
    case PQ_MSG_BIND:
    case PQ_MSG_DESCRIBE:
    case PQ_MSG_EXECUTE:
    case PQ_MSG_CLOSE:
    case PQ_MSG_FLUSH:
      return true;
    default:
      return false;
  }
}

// COPY ... FROM STDIN: the patched parser rewrites STDIN to "/dev/stdin", so it
// needs the io-side CopyData feeder + bridge (vs file COPY / COPY TO STDOUT,
// which DuckDB drives entirely on the worker).
// Detected from the parsed statement BEFORE Prepare, because the CSV sniff
// opens /dev/stdin during Prepare -- the bridge must already be live by then.
inline bool IsCopyFromStdin(const duckdb::SQLStatement& statement) {
  if (statement.type != duckdb::StatementType::COPY_STATEMENT) {
    return false;
  }
  const auto& copy = statement.Cast<duckdb::CopyStatement>();
  return copy.info && copy.info->is_from && copy.info->file_path == "/dev/stdin";
}

inline sdb::pg::VarFormat FormatFor(std::span<const sdb::pg::VarFormat> formats,
                                    size_t column) {
  if (formats.empty()) {
    return sdb::pg::VarFormat::Text;
  }
  return formats.size() == 1 ? formats.front() : formats[column];
}

template<SocketKind Kind>
class PgWireSession : public std::enable_shared_from_this<PgWireSession<Kind>> {
 public:
  using Deps = PgServerContext;

  PgWireSession(PgServerContext&, asio_ns::io_context& io)
    requires(Kind == SocketKind::Tcp)
    : _socket{io}, _io{io} {}

  PgWireSession(PgServerContext& ctx, asio_ns::io_context& io)
    requires(Kind == SocketKind::MaybeTls)
    : _socket{io, *ctx.ssl}, _io{io} {}

  void Start() { Run().Detach(); }

  void Close() noexcept { _socket.Close(); }

  asio_ns::ip::tcp::socket& Lowest() noexcept { return _socket.Lowest(); }

 private:
  // A decoded wire frame. `payload` is a borrowed view: into _recv when the
  // frame arrived contiguous (recv_consume = frame length, the caller consumes
  // it after processing), or into _scratch when the frame was split across recv
  // chunks and had to be linearized (recv_consume = 0, _recv already consumed).
  struct Frame {
    char type = 0;
    std::string_view payload;
    FrameStatus status = FrameStatus::Malformed;
    size_t recv_consume = 0;
  };

  yaclib::Future<> Run();
  yaclib::Future<> Flush();
  yaclib::Future<Frame> NextFrame(bool typed, uint32_t max_len);
  void CopyRecvInto(uint8_t* dst, size_t n) const;
  std::optional<size_t> PeekTotalLength(bool typed) const;
  char TxnStatusByte() const;
  void DrainNotices();

  yaclib::Future<> RunSimpleQuery(std::string_view query);
  yaclib::Future<> RunCopyFromStdin(
    duckdb::unique_ptr<duckdb::SQLStatement> statement);
  yaclib::Future<> RunCopyInFeeder(sdb::pg::CopyInBridge& bridge);
  void HandleParse(std::string_view payload);
  void HandleBind(std::string_view payload);
  void HandleDescribe(std::string_view payload);
  yaclib::Future<> HandleExecute(std::string_view payload);
  void HandleClose(std::string_view payload);
  void DescribeStatement(sdb::pg::DuckDBStatement& stmt);
  void DescribePortal(Portal& portal);
  std::vector<sdb::pg::VarFormat> ParseBindFormats(std::string_view& cursor);
  sdb::pg::DuckDBBindInfo ParseBindVars(std::string_view cursor,
                                        const sdb::pg::DuckDBStatement& stmt);

  void ParseStartupParams(std::string_view payload);
  std::string_view DatabaseName() const;
  std::string_view UserName() const;
  bool SetupConnection();
  void SendStartupBurst();
  duckdb::unique_ptr<duckdb::PendingQueryResult> PendingQueryEnsured(
    duckdb::PreparedStatement& prepared, duckdb::vector<duckdb::Value>& values);
  void SerializeResult(const duckdb::PreparedStatement& prepared,
                       duckdb::QueryResult& result,
                       duckdb::StatementReturnType return_type,
                       bool with_row_description,
                       std::span<const sdb::pg::VarFormat> formats);

  Socket<Kind> _socket;
  asio_ns::io_context& _io;
  message::Buffer _recv{kReadBlock, kBufferMaxGrowth};
  message::Buffer _send{kReadBlock, kBufferMaxGrowth};
  std::string _scratch;
  // The current command frame's pending _recv consume. Normally the command
  // loop applies it after the handler; COPY FROM STDIN consumes it early (so
  // the feeder reads past it) and zeroes this.
  size_t _dispatch_consume = 0;
  containers::FlatHashMap<std::string, std::string> _params;
  duckdb::unique_ptr<duckdb::Connection> _conn;
  std::shared_ptr<ConnectionContext> _connection_ctx;
  yaclib::IExecutorPtr _duck;
  yaclib::IExecutorPtr _ioexec;
  containers::NodeHashMap<std::string, sdb::pg::DuckDBStatement> _statements;
  containers::NodeHashMap<std::string, Portal> _portals;
  sdb::pg::DuckDBStatement _anon_statement;
  Portal _anon_portal;
};

template<SocketKind Kind>
yaclib::Future<> PgWireSession<Kind>::Flush() {
  if (!_send.Written().Empty()) {
    co_await _socket.Write(_send.Written());
    _send.Clear();
  }
  co_return {};
}

template<SocketKind Kind>
void PgWireSession<Kind>::CopyRecvInto(uint8_t* dst, size_t n) const {
  size_t offset = 0;
  for (const auto buffer : _recv.ReadableView(n)) {
    std::memcpy(dst + offset, buffer.data(), buffer.size());
    offset += buffer.size();
  }
}

template<SocketKind Kind>
std::optional<size_t> PgWireSession<Kind>::PeekTotalLength(bool typed) const {
  const size_t offset = typed ? 1 : 0;
  const size_t header = offset + sizeof(uint32_t);
  if (_recv.ReadableSize() < header) {
    return std::nullopt;
  }
  std::array<uint8_t, 1 + sizeof(uint32_t)> bytes;
  CopyRecvInto(bytes.data(), header);
  return offset + absl::big_endian::Load32(bytes.data() + offset);
}

template<SocketKind Kind>
yaclib::Future<typename PgWireSession<Kind>::Frame>
PgWireSession<Kind>::NextFrame(bool typed, uint32_t max_len) {
  const size_t offset = typed ? 1 : 0;
  for (;;) {
    if (_recv.Readable()) {
      const auto parsed = PgFrameCodec::Parse(_recv.Front(), typed, max_len);
      if (parsed.status == FrameStatus::Ok) {
        co_return Frame{parsed.frame.type, parsed.frame.payload,
                        FrameStatus::Ok, parsed.consumed};
      }
      if (parsed.status != FrameStatus::NeedMore) {
        co_return Frame{0, {}, parsed.status, 0};
      }
      // Front() is head-chunk-only; a frame that spans recv chunks parses as
      // NeedMore forever. Once the whole frame has arrived, linearize it into
      // _scratch (one copy) so the codec sees it contiguous.
      if (const auto total = PeekTotalLength(typed)) {
        const auto length = *total - offset;
        if (length < sizeof(uint32_t)) {
          co_return Frame{0, {}, FrameStatus::Malformed, 0};
        }
        if (length > max_len) {
          co_return Frame{0, {}, FrameStatus::TooLarge, 0};
        }
        if (_recv.ReadableSize() >= *total) {
          _scratch.resize(*total);
          CopyRecvInto(reinterpret_cast<uint8_t*>(_scratch.data()), *total);
          _recv.Consume(*total);
          const std::string_view flat{_scratch.data(), *total};
          const auto split = PgFrameCodec::Parse(flat, typed, max_len);
          co_return Frame{split.frame.type, split.frame.payload, split.status,
                          0};
        }
      }
    }
    const size_t n = co_await _socket.ReadSome(_recv.Reserve(kReadBlock));
    if (n == 0) {
      co_return Frame{0, {}, FrameStatus::Malformed, 0};
    }
    _recv.CommitWrite(n);
  }
}

template<SocketKind Kind>
void PgWireSession<Kind>::ParseStartupParams(std::string_view payload) {
  if (payload.size() < 4) {
    return;
  }
  payload.remove_prefix(4);  // protocol version
  for (;;) {
    const auto name_end = payload.find('\0');
    if (name_end == std::string_view::npos) {
      return;
    }
    const std::string_view name{payload.data(), name_end};
    if (name.empty()) {
      return;
    }
    payload.remove_prefix(name_end + 1);
    const auto value_end = payload.find('\0');
    const bool last = value_end == std::string_view::npos;
    const std::string_view value{payload.data(),
                                 last ? payload.size() : value_end};
    _params.try_emplace(name, value);
    if (last) {
      return;
    }
    payload.remove_prefix(value_end + 1);
  }
}

template<SocketKind Kind>
std::string_view PgWireSession<Kind>::DatabaseName() const {
  const auto it = _params.find("database");
  return it != _params.end() ? std::string_view{it->second}
                             : StaticStrings::kDefaultDatabase;
}

template<SocketKind Kind>
std::string_view PgWireSession<Kind>::UserName() const {
  const auto it = _params.find("user");
  return it != _params.end() ? std::string_view{it->second}
                             : StaticStrings::kDefaultUser;
}

template<SocketKind Kind>
bool PgWireSession<Kind>::SetupConnection() {
  const auto snapshot =
    catalog::CatalogFeature::instance().Global().GetCatalogSnapshot();
  auto database = snapshot->GetDatabase(DatabaseName());
  if (!database) {
    WriteErrorResponse(_send,
                       absl::StrCat("database \"", DatabaseName(),
                                    "\" is not accessible"),
                       "3D000");
    return false;
  }
  const auto database_id = database->GetId();

  _conn = DuckDBEngine::Instance().CreateConnection();
  _connection_ctx = std::make_shared<ConnectionContext>(
    *_conn->context, UserName(), DatabaseName(), database_id,
    std::move(database), &_send, nullptr);
  connector::SereneDBClientState::Register(*_conn->context, _connection_ctx);

  _conn->context->session_user = std::string{UserName()};
  std::vector<duckdb::CatalogSearchEntry> default_paths{
    duckdb::CatalogSearchEntry{std::string{DatabaseName()}, "$user"},
    duckdb::CatalogSearchEntry{std::string{DatabaseName()}, "public"},
  };
  _conn->context->client_data->catalog_search_path->SetDefaultPaths(
    std::vector{default_paths});
  _conn->context->client_data->catalog_search_path->Set(
    std::move(default_paths), duckdb::CatalogSetPathType::SET_DIRECTLY);

  _connection_ctx->SetSetting("session_authorization", std::string{UserName()},
                              false);
  _connection_ctx->SetSetting("is_superuser", "on", false);

  for (const auto& [name, value] : _params) {
    if (name == "user" || name == "database") {
      continue;
    }
    try {
      _connection_ctx->SetSettingChecked(name, value, false);
    } catch (const std::exception& exception) {
      WriteErrorResponse(_send, exception.what(), "22023");
      return false;
    }
  }
  return true;
}

template<SocketKind Kind>
void PgWireSession<Kind>::SendStartupBurst() {
  static constexpr std::array<char, 9> kAuthOk{
    PQ_MSG_AUTHENTICATION_REQUEST, 0, 0, 0, 8, 0, 0, 0, 0};
  _send.WriteUncommitted({kAuthOk.data(), kAuthOk.size()});

  static constexpr auto kParameterStatusVariables =
    std::to_array<std::string_view>({
      "client_encoding", "DateStyle", "integer_datetimes", "IntervalStyle",
      "is_superuser", "search_path", "server_encoding", "server_version",
      "session_authorization", "standard_conforming_strings", "TimeZone",
    });
  for (const auto name : kParameterStatusVariables) {
    if (const auto value = _connection_ctx->Get(name)) {
      WriteParameterStatus(_send, name, *value);
    }
  }

  static constexpr std::array<char, 13> kBackendKeyData{
    PQ_MSG_BACKEND_KEY_DATA, 0, 0, 0, 12, 0, 0, 0, 1, 0, 0, 0, 1};
  _send.WriteUncommitted({kBackendKeyData.data(), kBackendKeyData.size()});
  DrainNotices();
  WriteReadyForQuery(_send, 'I');
}

template<SocketKind Kind>
duckdb::unique_ptr<duckdb::PendingQueryResult>
PgWireSession<Kind>::PendingQueryEnsured(duckdb::PreparedStatement& prepared,
                                         duckdb::vector<duckdb::Value>& values) {
  const auto& props = prepared.GetStatementProperties();
  const auto db_name = DatabaseName();
  _connection_ctx->EnsureCatalogSnapshot();
  if (props.modified_databases.contains(std::string{db_name})) {
    _connection_ctx->EnsureRocksDBTransaction();
    _connection_ctx->EnsureRocksDBSnapshot();
  } else if (props.read_databases.contains(std::string{db_name})) {
    if (_connection_ctx->IsExplicitTransaction()) {
      _connection_ctx->EnsureRocksDBTransaction();
    }
    _connection_ctx->EnsureRocksDBSnapshot();
  }
  return prepared.PendingQuery(values, /*allow_stream_result=*/true);
}

template<SocketKind Kind>
void PgWireSession<Kind>::DrainNotices() {
  for (const auto& notice : _connection_ctx->StealNotices()) {
    WriteNoticeResponse(_send, notice);
  }
}

template<SocketKind Kind>
char PgWireSession<Kind>::TxnStatusByte() const {
  auto& transaction = _conn->context->transaction;
  if (transaction.IsAutoCommit()) {
    return 'I';
  }
  if (transaction.HasActiveTransaction() &&
      duckdb::ValidChecker::IsInvalidated(transaction.ActiveTransaction())) {
    return 'E';
  }
  return 'T';
}

template<SocketKind Kind>
void PgWireSession<Kind>::SerializeResult(
  const duckdb::PreparedStatement& prepared, duckdb::QueryResult& result,
  duckdb::StatementReturnType return_type, bool with_row_description,
  std::span<const sdb::pg::VarFormat> formats) {
  uint64_t rows = 0;
  if (return_type == duckdb::StatementReturnType::QUERY_RESULT) {
    if (with_row_description) {
      WriteRowDescription(_send, result.types, result.names, formats);
    }
    sdb::pg::SerializationContext context;
    context.buffer = &_send;
    sdb::pg::FillContext(*_connection_ctx, context);
    std::vector<sdb::pg::SerializationFunction> serializers;
    serializers.reserve(result.types.size());
    for (size_t column = 0; column < result.types.size(); ++column) {
      serializers.push_back(sdb::pg::GetSerialization(
        result.types[column], FormatFor(formats, column), context));
    }
    for (;;) {
      auto chunk = result.Fetch();
      if (!chunk || chunk->size() == 0) {
        break;
      }
      rows += chunk->size();
      WriteDataChunk(_send, *chunk, serializers, context);
    }
  } else if (return_type == duckdb::StatementReturnType::CHANGED_ROWS) {
    auto chunk = result.Fetch();
    if (chunk && chunk->size() > 0) {
      rows = static_cast<uint64_t>(chunk->GetValue(0, 0).GetValue<int64_t>());
    }
  }
  WriteCommandComplete(_send,
                       sdb::pg::FormatCommandTag(prepared, return_type, rows));
}

template<SocketKind Kind>
yaclib::Future<> PgWireSession<Kind>::RunSimpleQuery(std::string_view query) {
  while (!query.empty() && query.back() == '\0') {
    query.remove_suffix(1);
  }
  if (query.empty()) {
    WriteEmptyFrame(_send, PQ_MSG_EMPTY_QUERY_RESPONSE);
    co_return {};
  }
  auto extracted = _conn->ExtractStatements(std::string{query});
  for (auto& statement : extracted) {
    if (IsCopyFromStdin(*statement)) {
      co_await RunCopyFromStdin(std::move(statement));
      continue;
    }
    auto prepared = _conn->Prepare(std::move(statement));
    if (prepared->HasError()) {
      ThrowDuck(prepared->GetErrorObject());
    }
    const auto return_type = prepared->GetStatementProperties().return_type;
    duckdb::vector<duckdb::Value> params;
    auto pending = PendingQueryEnsured(*prepared, params);
    if (pending->HasError()) {
      ThrowDuck(pending->GetErrorObject());
    }
    const auto status = co_await DrivePending(*_duck, *pending);
    if (status == duckdb::PendingExecutionResult::EXECUTION_ERROR ||
        pending->HasError()) {
      ThrowDuck(pending->GetErrorObject());
    }
    auto result = pending->Execute();
    if (result->HasError()) {
      ThrowDuck(result->GetErrorObject());
    }
    SerializeResult(*prepared, *result, return_type,
                    /*with_row_description=*/true, {});
  }
  co_return {};
}

// COPY FROM STDIN binds (sniffs the CSV dialect by reading /dev/stdin) at
// PendingQuery time, so the bridge + feeder must be live BEFORE binding, and
// CopyInResponse must already be flushed (the client streams CopyData only
// after seeing it). The worker-side COPY blocks pulling CopyData via the bridge
// while the io feeder reads it from the socket -- two threads, since DuckDB's
// FileSystem::Read can't yield.
template<SocketKind Kind>
yaclib::Future<> PgWireSession<Kind>::RunCopyFromStdin(
  duckdb::unique_ptr<duckdb::SQLStatement> statement) {
  // Handshake first (no bridge needed yet); a Flush failure here is clean. The
  // client streams CopyData only after CopyInResponse, so it must go out before
  // Prepare (which sniffs /dev/stdin).
  co_await yaclib::On(*_ioexec);
  if (_dispatch_consume) {
    _recv.Consume(_dispatch_consume);
    _dispatch_consume = 0;
  }
  WriteCopyInResponse(_send);
  co_await Flush();

  auto& client_state =
    *_conn->context->registered_state
       ->template Get<connector::SereneDBClientState>(
         connector::kSereneDBClientStateKey);
  client_state.copy_stdin_open_count = 0;
  client_state.copy_stdin_done = false;
  client_state.copy_stdin_buffer.reset();
  sdb::pg::CopyInBridge bridge;
  _connection_ctx->SetCopyInBridge(&bridge);
  auto feeder = RunCopyInFeeder(bridge);

  co_await yaclib::On(*_duck);
  std::exception_ptr error;
  duckdb::unique_ptr<duckdb::PreparedStatement> prepared;
  duckdb::unique_ptr<duckdb::QueryResult> result;
  try {
    prepared = _conn->Prepare(std::move(statement));  // sniffs /dev/stdin
    if (prepared->HasError()) {
      ThrowDuck(prepared->GetErrorObject());
    }
    duckdb::vector<duckdb::Value> params;
    auto pending = PendingQueryEnsured(*prepared, params);
    if (pending->HasError()) {
      ThrowDuck(pending->GetErrorObject());
    }
    const auto status = co_await DrivePending(*_duck, *pending);
    if (status == duckdb::PendingExecutionResult::EXECUTION_ERROR ||
        pending->HasError()) {
      ThrowDuck(pending->GetErrorObject());
    }
    result = pending->Execute();
    if (result->HasError()) {
      ThrowDuck(result->GetErrorObject());
    }
  } catch (...) {
    error = std::current_exception();
  }
  // Always join the feeder before the bridge (a local) is destroyed; Abort
  // wakes it if the worker errored without draining the client's CopyData.
  bridge.Abort();
  co_await yaclib::On(*_ioexec);
  co_await std::move(feeder);
  _connection_ctx->SetCopyInBridge(nullptr);
  if (error) {
    std::rethrow_exception(error);
  }
  SerializeResult(*prepared, *result,
                  prepared->GetStatementProperties().return_type,
                  /*with_row_description=*/true, {});
  co_return {};
}

template<SocketKind Kind>
yaclib::Future<> PgWireSession<Kind>::RunCopyInFeeder(
  sdb::pg::CopyInBridge& bridge) {
  co_await yaclib::On(*_ioexec);
  for (;;) {
    auto frame = co_await NextFrame(true, kBufferMaxGrowth);
    if (frame.status != FrameStatus::Ok) {
      bridge.Fail(std::make_exception_ptr(sdb::SqlException{
        sdb::pg::SqlErrorData{.errcode = ERRCODE_CONNECTION_EXCEPTION,
                              .errmsg = "unexpected EOF during COPY from stdin"},
        std::source_location::current()}));
      co_return {};
    }
    const char type = frame.type;
    if (type == PQ_MSG_COPY_DATA) {
      if (!bridge.Aborted()) {
        auto payload = frame.payload;
        if (payload.ends_with(std::string_view{"\\.\n"})) {
          payload.remove_suffix(3);
        } else if (payload.ends_with(std::string_view{"\\.\r\n"})) {
          payload.remove_suffix(4);
        }
        bridge.Publish(payload.data(), payload.size());
        co_await bridge.Drained(*_ioexec);
        bridge.ResetDrained();
      }
      if (frame.recv_consume) {
        _recv.Consume(frame.recv_consume);
      }
    } else if (type == PQ_MSG_COPY_DONE) {
      bridge.Finish();
      if (frame.recv_consume) {
        _recv.Consume(frame.recv_consume);
      }
      co_return {};
    } else if (type == PQ_MSG_COPY_FAIL) {
      bridge.Fail(std::make_exception_ptr(sdb::SqlException{
        sdb::pg::SqlErrorData{.errcode = ERRCODE_QUERY_CANCELED,
                              .errmsg = "COPY from stdin failed"},
        std::source_location::current()}));
      if (frame.recv_consume) {
        _recv.Consume(frame.recv_consume);
      }
      co_return {};
    } else if (type == PQ_MSG_FLUSH || type == PQ_MSG_SYNC) {
      if (frame.recv_consume) {
        _recv.Consume(frame.recv_consume);
      }
    } else {
      bridge.Fail(std::make_exception_ptr(sdb::SqlException{
        sdb::pg::SqlErrorData{
          .errcode = ERRCODE_PROTOCOL_VIOLATION,
          .errmsg = "unexpected message during COPY from stdin"},
        std::source_location::current()}));
      if (frame.recv_consume) {
        _recv.Consume(frame.recv_consume);
      }
      co_return {};
    }
  }
}

template<SocketKind Kind>
void PgWireSession<Kind>::HandleParse(std::string_view payload) {
  const auto name_end = payload.find('\0');
  const auto query_end = name_end == std::string_view::npos
                           ? std::string_view::npos
                           : payload.find('\0', name_end + 1);
  if (query_end == std::string_view::npos ||
      payload.size() - query_end - 1 < sizeof(uint16_t)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("malformed Parse message"));
  }
  const std::string_view statement_name = payload.substr(0, name_end);
  const std::string_view query =
    payload.substr(name_end + 1, query_end - name_end - 1);
  payload.remove_prefix(query_end + 1);
  const auto num_params = absl::big_endian::Load16(payload.data());
  payload.remove_prefix(sizeof(uint16_t));
  if (payload.size() < num_params * sizeof(int32_t)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("malformed Parse message"));
  }

  duckdb::case_insensitive_map_t<duckdb::LogicalType> type_hints;
  if (num_params > 0) {
    const auto snapshot = _connection_ctx->EnsureCatalogSnapshot();
    for (uint16_t i = 0; i < num_params; ++i) {
      const auto oid =
        static_cast<int32_t>(absl::big_endian::Load32(payload.data()));
      payload.remove_prefix(sizeof(int32_t));
      if (oid != 0) {
        type_hints.emplace(absl::StrCat(i + 1),
                           sdb::pg::Oid2Type(oid, *snapshot));
      }
    }
  }

  sdb::pg::DuckDBStatement* statement = &_anon_statement;
  if (!statement_name.empty()) {
    auto [it, emplaced] = _statements.try_emplace(std::string{statement_name});
    if (!emplaced) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_PSTATEMENT),
                      ERR_MSG("prepared statement \"", statement_name,
                              "\" already exists"));
    }
    statement = &it->second;
  }
  statement->Reset();
  statement->prepared = _conn->Prepare(
    std::string{query}, type_hints.empty() ? nullptr : &type_hints);
  if (statement->prepared->HasError()) {
    const auto error = statement->prepared->GetErrorObject();
    statement->prepared.reset();
    if (!statement_name.empty()) {
      _statements.erase(_statements.find(statement_name));
    }
    ThrowDuck(error);
  }
  WriteEmptyFrame(_send, PQ_MSG_PARSE_COMPLETE);
}

template<SocketKind Kind>
std::vector<sdb::pg::VarFormat> PgWireSession<Kind>::ParseBindFormats(
  std::string_view& cursor) {
  if (cursor.size() < sizeof(uint16_t)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("malformed Bind message"));
  }
  const auto count = absl::big_endian::Load16(cursor.data());
  cursor.remove_prefix(sizeof(uint16_t));
  if (cursor.size() < count * sizeof(uint16_t)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("malformed Bind message"));
  }
  std::vector<sdb::pg::VarFormat> formats;
  formats.reserve(count);
  for (uint16_t i = 0; i < count; ++i) {
    const auto code = absl::big_endian::Load16(cursor.data());
    cursor.remove_prefix(sizeof(uint16_t));
    if (code != 0 && code != 1) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                      ERR_MSG("invalid format code: ", code));
    }
    formats.push_back(code == 0 ? sdb::pg::VarFormat::Text
                                : sdb::pg::VarFormat::Binary);
  }
  return formats;
}

template<SocketKind Kind>
sdb::pg::DuckDBBindInfo PgWireSession<Kind>::ParseBindVars(
  std::string_view cursor, const sdb::pg::DuckDBStatement& stmt) {
  const auto input_formats = ParseBindFormats(cursor);
  if (cursor.size() < sizeof(uint16_t)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("malformed Bind message"));
  }
  const auto params = absl::big_endian::Load16(cursor.data());
  cursor.remove_prefix(sizeof(uint16_t));
  if (input_formats.size() > 1 && input_formats.size() != params) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("bind message has ", input_formats.size(),
                            " parameter formats but ", params, " parameters"));
  }

  duckdb::vector<duckdb::Value> values;
  values.reserve(params);
  const auto snapshot = _connection_ctx->EnsureCatalogSnapshot();
  for (uint16_t i = 0; i < params; ++i) {
    if (cursor.size() < sizeof(int32_t)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                      ERR_MSG("malformed Bind message"));
    }
    const auto length =
      static_cast<int32_t>(absl::big_endian::Load32(cursor.data()));
    cursor.remove_prefix(sizeof(int32_t));
    if (length == -1) {
      values.emplace_back();
      continue;
    }
    if (length < 0 || cursor.size() < static_cast<size_t>(length)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                      ERR_MSG("invalid parameter length: ", length));
    }
    const auto format = FormatFor(input_formats, i);
    const auto type = ResolveExpectedType(stmt.prepared->data->value_map, i);
    auto value = sdb::pg::DeserializeParameter(
      type, format, cursor.substr(0, length), *snapshot);
    if (!value) {
      if (format == sdb::pg::VarFormat::Binary) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_BINARY_REPRESENTATION),
          ERR_MSG("incorrect binary data format in bind parameter ", i + 1));
      }
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
        ERR_MSG("invalid input syntax for bind parameter ", i + 1));
    }
    values.emplace_back(std::move(*value));
    cursor.remove_prefix(length);
  }
  return sdb::pg::DuckDBBindInfo{ParseBindFormats(cursor), std::move(values)};
}

template<SocketKind Kind>
void PgWireSession<Kind>::HandleBind(std::string_view payload) {
  const auto portal_end = payload.find('\0');
  const auto statement_end = portal_end == std::string_view::npos
                               ? std::string_view::npos
                               : payload.find('\0', portal_end + 1);
  if (statement_end == std::string_view::npos) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("malformed Bind message"));
  }
  const std::string_view portal_name = payload.substr(0, portal_end);
  const std::string_view statement_name =
    payload.substr(portal_end + 1, statement_end - portal_end - 1);
  payload.remove_prefix(statement_end + 1);

  sdb::pg::DuckDBStatement* statement = &_anon_statement;
  if (!statement_name.empty()) {
    const auto it = _statements.find(statement_name);
    if (it == _statements.end()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_SQL_STATEMENT_NAME),
                      ERR_MSG("prepared statement \"", statement_name,
                              "\" does not exist"));
    }
    statement = &it->second;
  }
  if (!statement->prepared) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_SQL_STATEMENT_NAME),
                    ERR_MSG("prepared statement is not ready"));
  }

  Portal portal;
  portal.stmt = statement;
  portal.bind_info = ParseBindVars(payload, *statement);
  portal.pending =
    PendingQueryEnsured(*statement->prepared, portal.bind_info.param_values);
  if (portal.pending->HasError()) {
    ThrowDuck(portal.pending->GetErrorObject());
  }

  if (portal_name.empty()) {
    _anon_portal = std::move(portal);
  } else {
    auto [it, emplaced] = _portals.try_emplace(std::string{portal_name});
    if (!emplaced) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_CURSOR),
                      ERR_MSG("portal \"", portal_name, "\" already exists"));
    }
    it->second = std::move(portal);
  }
  WriteEmptyFrame(_send, PQ_MSG_BIND_COMPLETE);
}

template<SocketKind Kind>
void PgWireSession<Kind>::DescribeStatement(sdb::pg::DuckDBStatement& stmt) {
  if (!stmt.prepared) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_SQL_STATEMENT_NAME),
                    ERR_MSG("prepared statement is not ready"));
  }
  auto& prepared = *stmt.prepared;
  const auto param_count = prepared.named_param_map.size();
  std::vector<int32_t> oids;
  oids.reserve(param_count);
  for (uint16_t i = 0; i < param_count; ++i) {
    oids.push_back(
      sdb::pg::Type2Oid(ResolveExpectedType(prepared.data->value_map, i)));
  }
  WriteParameterDescription(_send, oids);

  const auto return_type = prepared.GetStatementProperties().return_type;
  if (return_type != duckdb::StatementReturnType::QUERY_RESULT) {
    WriteEmptyFrame(_send, PQ_MSG_NO_DATA);
    return;
  }
  const auto* types = &prepared.GetTypes();
  const auto* names = &prepared.GetNames();
  duckdb::unique_ptr<duckdb::PendingQueryResult> pending;
  if (!prepared.named_param_map.empty() &&
      std::ranges::any_of(*types, [](const duckdb::LogicalType& type) {
        return type.id() == duckdb::LogicalTypeId::UNKNOWN ||
               type.id() == duckdb::LogicalTypeId::INVALID;
      })) {
    duckdb::vector<duckdb::Value> dummy;
    dummy.reserve(param_count);
    for (size_t i = 0; i < param_count; ++i) {
      auto type = ResolveExpectedType(prepared.data->value_map, i);
      duckdb::Value value{"1"};
      if (!value.DefaultTryCastAs(type)) {
        value = duckdb::Value{type};
      }
      dummy.emplace_back(std::move(value));
    }
    pending = PendingQueryEnsured(prepared, dummy);
    if (!pending->HasError()) {
      types = &pending->types;
      names = &pending->names;
    }
  }
  WriteRowDescription(_send, *types, *names, {});
}

template<SocketKind Kind>
void PgWireSession<Kind>::DescribePortal(Portal& portal) {
  const auto return_type =
    portal.stmt->prepared->GetStatementProperties().return_type;
  if (return_type != duckdb::StatementReturnType::QUERY_RESULT) {
    WriteEmptyFrame(_send, PQ_MSG_NO_DATA);
    return;
  }
  WriteRowDescription(_send, portal.pending->types, portal.pending->names,
                      portal.bind_info.output_formats);
}

template<SocketKind Kind>
void PgWireSession<Kind>::HandleDescribe(std::string_view payload) {
  if (payload.size() < 2 || payload.back() != '\0') {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("malformed Describe message"));
  }
  const char what = payload.front();
  const std::string_view name = payload.substr(1, payload.size() - 2);
  if (what == 'S') {
    if (name.empty()) {
      DescribeStatement(_anon_statement);
    } else {
      const auto it = _statements.find(name);
      if (it == _statements.end()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_SQL_STATEMENT_NAME),
                        ERR_MSG("prepared statement \"", name,
                                "\" does not exist"));
      }
      DescribeStatement(it->second);
    }
  } else if (what == 'P') {
    if (name.empty()) {
      DescribePortal(_anon_portal);
    } else {
      const auto it = _portals.find(name);
      if (it == _portals.end()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_CURSOR_NAME),
                        ERR_MSG("portal \"", name, "\" does not exist"));
      }
      DescribePortal(it->second);
    }
  } else {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("invalid Describe target"));
  }
}

template<SocketKind Kind>
yaclib::Future<> PgWireSession<Kind>::HandleExecute(std::string_view payload) {
  const auto name_end = payload.find('\0');
  if (name_end == std::string_view::npos) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("malformed Execute message"));
  }
  const std::string_view portal_name = payload.substr(0, name_end);
  Portal* portal = &_anon_portal;
  if (!portal_name.empty()) {
    const auto it = _portals.find(portal_name);
    if (it == _portals.end()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_CURSOR_NAME),
                      ERR_MSG("portal \"", portal_name, "\" does not exist"));
    }
    portal = &it->second;
  }
  if (!portal->stmt || !portal->pending) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_CURSOR_NAME),
                    ERR_MSG("portal is not bound"));
  }

  const auto return_type =
    portal->stmt->prepared->GetStatementProperties().return_type;
  const auto status = co_await DrivePending(*_duck, *portal->pending);
  if (status == duckdb::PendingExecutionResult::EXECUTION_ERROR ||
      portal->pending->HasError()) {
    ThrowDuck(portal->pending->GetErrorObject());
  }
  portal->result = portal->pending->Execute();
  if (portal->result->HasError()) {
    ThrowDuck(portal->result->GetErrorObject());
  }
  SerializeResult(*portal->stmt->prepared, *portal->result, return_type,
                  /*with_row_description=*/false,
                  portal->bind_info.output_formats);
  co_return {};
}

template<SocketKind Kind>
void PgWireSession<Kind>::HandleClose(std::string_view payload) {
  if (payload.size() < 2 || payload.back() != '\0') {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("malformed Close message"));
  }
  const char what = payload.front();
  const std::string_view name = payload.substr(1, payload.size() - 2);
  if (what == 'P') {
    if (name.empty()) {
      _anon_portal = Portal{};
    } else if (const auto it = _portals.find(name); it != _portals.end()) {
      _portals.erase(it);
    }
  } else if (what == 'S') {
    sdb::pg::DuckDBStatement* statement = nullptr;
    if (name.empty()) {
      statement = &_anon_statement;
    } else if (const auto it = _statements.find(name);
               it != _statements.end()) {
      statement = &it->second;
    }
    if (statement) {
      if (_anon_portal.stmt == statement) {
        _anon_portal = Portal{};
      }
      erase_if(_portals, [statement](const auto& entry) {
        return entry.second.stmt == statement;
      });
      if (name.empty()) {
        statement->Reset();
      } else {
        _statements.erase(_statements.find(name));
      }
    }
  } else {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("invalid Close target"));
  }
  WriteEmptyFrame(_send, PQ_MSG_CLOSE_COMPLETE);
}

template<SocketKind Kind>
yaclib::Future<> PgWireSession<Kind>::Run() {
  auto self = this->shared_from_this();
  if constexpr (Kind == SocketKind::Ssl) {
    try {
      co_await _socket.Handshake();
    } catch (const std::exception&) {
      _socket.Close();
      co_return {};
    }
  }
  try {
    for (;;) {
      const auto frame = co_await NextFrame(false, MAX_STARTUP_PACKET_LENGTH);
      if (frame.status != FrameStatus::Ok) {
        _socket.Close();
        co_return {};
      }
      const uint32_t code = PgFrameCodec::StartupCode(frame.payload);
      if (code == NEGOTIATE_SSL_CODE) {
        if (frame.recv_consume) {
          _recv.Consume(frame.recv_consume);
        }
        if constexpr (Kind == SocketKind::MaybeTls) {
          if (!_socket.IsTls()) {
            // CVE-2021-23214: the client must send nothing between SSLRequest
            // and the TLS ClientHello -- any buffered plaintext here is a bug
            // or a MITM injection, so refuse rather than process it post-TLS.
            if (_recv.Readable()) {
              _socket.Close();
              co_return {};
            }
            _send.WriteUncommitted("S");
            co_await Flush();
            try {
              co_await _socket.Handshake();
            } catch (const std::exception&) {
              _socket.Close();
              co_return {};
            }
            _socket.MarkTls();
            continue;
          }
        }
        _send.WriteUncommitted("N");
        co_await Flush();
        continue;
      }
      if (code == NEGOTIATE_GSS_CODE) {
        if (frame.recv_consume) {
          _recv.Consume(frame.recv_consume);
        }
        _send.WriteUncommitted("N");
        co_await Flush();
        continue;
      }
      if (code == static_cast<uint32_t>(PG_PROTOCOL_LATEST)) {
        ParseStartupParams(frame.payload);
        if (frame.recv_consume) {
          _recv.Consume(frame.recv_consume);
        }
        break;
      }
      _socket.Close();
      co_return {};
    }

    if (!SetupConnection()) {
      co_await Flush();
      _socket.Close();
      co_return {};
    }
    _duck = yaclib::MakeShared<DuckExecutor>(
      1, duckdb::TaskScheduler::GetScheduler(*_conn->context));
    _ioexec = yaclib::MakeShared<IoExecutor>(1, _io);

    SendStartupBurst();
    co_await Flush();

    // After an error in an extended-protocol message the backend silently
    // discards every following message until the next Sync (postgres.c
    // ignore_till_sync). A coroutine makes this a plain loop local.
    bool ignore_till_sync = false;
    for (;;) {
      auto frame = co_await NextFrame(true, kBufferMaxGrowth);
      if (frame.status != FrameStatus::Ok || frame.type == PQ_MSG_TERMINATE) {
        break;
      }
      const char type = frame.type;

      if (type == PQ_MSG_FLUSH) {
        if (frame.recv_consume) {
          _recv.Consume(frame.recv_consume);
        }
        co_await Flush();
        continue;
      }
      if (type == PQ_MSG_SYNC) {
        ignore_till_sync = false;
        if (frame.recv_consume) {
          _recv.Consume(frame.recv_consume);
        }
        WriteReadyForQuery(_send, TxnStatusByte());
        co_await Flush();
        continue;
      }
      if (ignore_till_sync) {
        if (frame.recv_consume) {
          _recv.Consume(frame.recv_consume);
        }
        continue;
      }

      _dispatch_consume = frame.recv_consume;
      co_await yaclib::On(*_duck);
      try {
        switch (type) {
          case PQ_MSG_QUERY:
            co_await RunSimpleQuery(frame.payload);
            break;
          case PQ_MSG_PARSE:
            HandleParse(frame.payload);
            break;
          case PQ_MSG_BIND:
            HandleBind(frame.payload);
            break;
          case PQ_MSG_DESCRIBE:
            HandleDescribe(frame.payload);
            break;
          case PQ_MSG_EXECUTE:
            co_await HandleExecute(frame.payload);
            break;
          case PQ_MSG_CLOSE:
            HandleClose(frame.payload);
            break;
          default:
            THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                            ERR_MSG("unsupported message type"));
        }
      } catch (const SqlException& exception) {
        WriteErrorResponse(_send, exception.error());
        ignore_till_sync = IsExtended(type);
      } catch (const duckdb::Exception& exception) {
        WriteErrorResponse(_send, DuckErrorToSqlData(duckdb::ErrorData{exception}));
        ignore_till_sync = IsExtended(type);
      } catch (const std::exception& exception) {
        WriteErrorResponse(_send, sdb::pg::SqlErrorData{
                                    .errcode = ERRCODE_INTERNAL_ERROR,
                                    .errmsg = exception.what()});
        ignore_till_sync = IsExtended(type);
      }
      co_await yaclib::On(*_ioexec);
      DrainNotices();
      if (_dispatch_consume) {
        _recv.Consume(_dispatch_consume);
        _dispatch_consume = 0;
      }

      // Simple Query is self-syncing: emit ReadyForQuery and flush now.
      // Extended replies accumulate and flush at the client's Sync.
      if (type == PQ_MSG_QUERY) {
        WriteReadyForQuery(_send, TxnStatusByte());
        co_await Flush();
      }
    }
  } catch (const std::exception&) {
  }
  // Destroy portal/statement-bound DuckDB results on the worker (they were
  // created there) before the connection/context tear down, and discard any
  // notices their cleanup produced so ~ConnectionContext sees an empty queue.
  if (_duck) {
    co_await yaclib::On(*_duck);
    _portals.clear();
    _anon_portal = Portal{};
    _statements.clear();
    _anon_statement.Reset();
    co_await yaclib::On(*_ioexec);
  }
  if (_connection_ctx) {
    _connection_ctx->StealNotices();
  }
  _socket.Close();
  co_return {};
}

}  // namespace sdb::network::pg
