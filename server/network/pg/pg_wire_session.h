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

#include <absl/base/internal/endian.h>
#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/common/case_insensitive_map.hpp>
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
#include <memory>
#include <optional>
#include <source_location>
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

#include "basics/asio_ns.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/node_hash_map.h"
#include "basics/duckdb_engine.h"
#include "basics/message_buffer.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "connector/duckdb_client_state.h"
#include "network/connection.h"
#include "network/io_executor.h"
#include "network/pg/auth.h"
#include "network/pg/cancel_registry.h"
#include "network/pg/duck_executor.h"
#include "network/pg/pg_frame_codec.h"
#include "network/pg/query_pump.h"
#include "network/pg/wire_frames.h"
#include "network/socket.h"
#include "pg/command_tag.h"
#include "pg/connection_context.h"
#include "pg/copy_in_bridge.h"
#include "pg/duckdb_sql_statement.h"
#include "pg/errcodes.h"
#include "pg/pg_types.h"
#include "pg/protocol.h"
#include "pg/serialize.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"

namespace sdb::network::pg {

// `ssl` is non-null when TLS is configured; then the pg endpoint runs as
// SocketKind::MaybeTls and answers SSLRequest with 'S' + an in-band upgrade.
// `credentials` is the role-decoupled auth seam (null => trust everyone, as
// today); a future RBAC layer supplies a real provider.
struct PgServerContext {
  asio_ns::ssl::context* ssl = nullptr;
  const CredentialProvider* credentials = nullptr;
  bool allow_cleartext_without_tls = false;
  CancelRegistry* cancel = nullptr;
  uint32_t max_message_bytes = kDefaultMaxMessageBytes;
};

// A bound portal: a prepared statement plus its pending/streaming execution and
// the result column formats chosen at Bind.
struct Portal {
  sdb::pg::DuckDBStatement* stmt = nullptr;
  duckdb::unique_ptr<duckdb::PendingQueryResult> pending;
  duckdb::unique_ptr<duckdb::QueryResult> result;
  sdb::pg::DuckDBBindInfo bind_info;
  // Cursor paging for Execute max_rows: the result is executed once (started),
  // then fetched in batches. carry holds the unsent tail of the last fetched
  // chunk when the row limit fell mid-chunk; exhausted means the stream is
  // fully drained (and, being a StreamQueryResult, already closed -- must not
  // be fetched again).
  bool started = false;
  bool exhausted = false;
  duckdb::unique_ptr<duckdb::DataChunk> carry;
  duckdb::idx_t carry_offset = 0;
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
  return copy.info && copy.info->is_from &&
         copy.info->file_path == "/dev/stdin";
}

// COPY ... (FORMAT binary): info->format is the lowercased copy-function name;
// "binary" selects the PGCOPY wire format (CopyInResponse format byte 1, and no
// text "\." end-marker to strip).
inline bool IsBinaryCopy(const duckdb::SQLStatement& statement) {
  if (statement.type != duckdb::StatementType::COPY_STATEMENT) {
    return false;
  }
  const auto& copy = statement.Cast<duckdb::CopyStatement>();
  return copy.info && copy.info->format == "binary";
}

// Cheap gate for the COPY-FROM-STDIN parse-probe in HandleParse: does the text
// begin with the COPY keyword (case-insensitive, after leading whitespace)?
// Only COPY-prefixed statements get a parse-only ExtractStatements; everything
// else takes the normal Prepare path unchanged. A false negative (e.g. COPY
// behind a comment) just falls through to Prepare, which still errors cleanly.
inline bool StartsWithCopy(std::string_view query) {
  const auto start = query.find_first_not_of(" \t\r\n\f\v");
  if (start == std::string_view::npos) {
    return false;
  }
  query.remove_prefix(start);
  static constexpr std::string_view kCopy = "copy";
  if (query.size() < kCopy.size()) {
    return false;
  }
  for (size_t i = 0; i < kCopy.size(); ++i) {
    if ((query[i] | 0x20) != kCopy[i]) {
      return false;
    }
  }
  if (query.size() == kCopy.size()) {
    return true;
  }
  const char c = query[kCopy.size()];
  return !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
           (c >= '0' && c <= '9') || c == '_');
}

inline sdb::pg::VarFormat FormatFor(std::span<const sdb::pg::VarFormat> formats,
                                    size_t column) {
  if (formats.empty()) {
    return sdb::pg::VarFormat::Text;
  }
  return formats.size() == 1 ? formats.front() : formats[column];
}

// The GUCs reported to the client via ParameterStatus: once at startup
// (SendStartupBurst) and again whenever a command changes one
// (ReportChangedParameters), matching postgres's GUC_REPORT set.
inline constexpr auto kReportParams = std::to_array<std::string_view>({
  "client_encoding",
  "DateStyle",
  "integer_datetimes",
  "IntervalStyle",
  "is_superuser",
  "search_path",
  "server_encoding",
  "server_version",
  "session_authorization",
  "standard_conforming_strings",
  "TimeZone",
});

template<SocketKind Kind>
class PgWireSession : public std::enable_shared_from_this<PgWireSession<Kind>> {
 public:
  using Deps = PgServerContext;

  PgWireSession(PgServerContext& ctx, asio_ns::io_context& io)
    requires(Kind == SocketKind::Tcp)
    : _socket{io},
      _io{io},
      _credentials{ctx.credentials},
      _allow_cleartext{ctx.allow_cleartext_without_tls},
      _cancel{ctx.cancel},
      _max_message{ctx.max_message_bytes} {}

  PgWireSession(PgServerContext& ctx, asio_ns::io_context& io)
    requires(Kind == SocketKind::MaybeTls)
    : _socket{io, *ctx.ssl},
      _io{io},
      _credentials{ctx.credentials},
      _allow_cleartext{ctx.allow_cleartext_without_tls},
      _cancel{ctx.cancel},
      _max_message{ctx.max_message_bytes} {}

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
  void ReportChangedParameters();

  yaclib::Future<bool> Authenticate();
  yaclib::Future<bool> AuthenticateCleartext(const std::string& expected);
  yaclib::Future<bool> AuthenticateScram(const ScramVerifier& verifier);

  yaclib::Future<> RunSimpleQuery(std::string_view query);
  yaclib::Future<> RunCopyFromStdin(
    duckdb::unique_ptr<duckdb::SQLStatement> statement);
  yaclib::Future<> RunCopyInFeeder(sdb::pg::CopyInBridge& bridge,
                                   bool is_binary);
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
  // Emit up to max_rows (>0) DataRows from the portal's retained result,
  // resuming from a carried chunk. Returns true when the row limit was hit and
  // more rows remain (caller sends PortalSuspended); false when the portal
  // drained (CommandComplete already written, with this Execute's row count).
  bool SerializePage(Portal& portal, duckdb::StatementReturnType return_type,
                     uint64_t max_rows);

  Socket<Kind> _socket;
  asio_ns::io_context& _io;
  const CredentialProvider* _credentials = nullptr;
  bool _allow_cleartext = false;
  CancelRegistry* _cancel = nullptr;
  std::shared_ptr<CancelToken> _cancel_token;
  uint64_t _cancel_key = 0;
  uint32_t _max_message = kDefaultMaxMessageBytes;
  message::Buffer _recv{kReadBlock, kBufferMaxGrowth};
  message::Buffer _send{kReadBlock, kBufferMaxGrowth};
  std::string _scratch;
  // The current command frame's pending _recv consume. Normally the command
  // loop applies it after the handler; COPY FROM STDIN consumes it early (so
  // the feeder reads past it) and zeroes this.
  size_t _dispatch_consume = 0;
  containers::FlatHashMap<std::string, std::string> _params;
  containers::FlatHashMap<std::string, std::string> _reported_params;
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
    WriteErrorResponse(
      _send,
      absl::StrCat("database \"", DatabaseName(), "\" is not accessible"),
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

  for (const auto name : kReportParams) {
    if (const auto value = _connection_ctx->Get(name)) {
      _reported_params.insert_or_assign(std::string{name}, *value);
      WriteParameterStatus(_send, name, *value);
    }
  }

  // BackendKeyData carries this session's cancel key (pid = high 32 bits,
  // secret = low 32; both 4 bytes for the 3.0 protocol). A CancelRequest echoes
  // them back so CancelRegistry can find and interrupt this connection.
  auto* backend_key = _send.GetContiguousData(13);
  backend_key[0] = PQ_MSG_BACKEND_KEY_DATA;
  absl::big_endian::Store32(backend_key + 1, 12);
  absl::big_endian::Store32(backend_key + 5,
                            static_cast<uint32_t>(_cancel_key >> 32));
  absl::big_endian::Store32(backend_key + 9,
                            static_cast<uint32_t>(_cancel_key & 0xffffffffu));
  _send.Commit(false);
  DrainNotices();
  WriteReadyForQuery(_send, 'I');
}

template<SocketKind Kind>
duckdb::unique_ptr<duckdb::PendingQueryResult>
PgWireSession<Kind>::PendingQueryEnsured(
  duckdb::PreparedStatement& prepared, duckdb::vector<duckdb::Value>& values) {
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

// A command may change a reportable GUC (e.g. SET TimeZone, SET search_path,
// or a transaction rollback reverting one). Postgres echoes such changes with
// ParameterStatus before the next ReadyForQuery; emit one for each value that
// differs from what the client was last told.
template<SocketKind Kind>
void PgWireSession<Kind>::ReportChangedParameters() {
  for (const auto name : kReportParams) {
    const auto value = _connection_ctx->Get(name);
    if (!value) {
      continue;
    }
    const auto it = _reported_params.find(name);
    if (it != _reported_params.end() && it->second == *value) {
      continue;
    }
    _reported_params.insert_or_assign(std::string{name}, *value);
    WriteParameterStatus(_send, name, *value);
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
bool PgWireSession<Kind>::SerializePage(Portal& portal,
                                        duckdb::StatementReturnType return_type,
                                        uint64_t max_rows) {
  auto& result = *portal.result;
  sdb::pg::SerializationContext context;
  context.buffer = &_send;
  sdb::pg::FillContext(*_connection_ctx, context);
  std::vector<sdb::pg::SerializationFunction> serializers;
  serializers.reserve(result.types.size());
  for (size_t column = 0; column < result.types.size(); ++column) {
    serializers.push_back(sdb::pg::GetSerialization(
      result.types[column], FormatFor(portal.bind_info.output_formats, column),
      context));
  }

  uint64_t sent = 0;
  while (sent < max_rows) {
    if (!portal.carry || portal.carry_offset >= portal.carry->size()) {
      portal.carry = result.Fetch();
      portal.carry_offset = 0;
      if (!portal.carry || portal.carry->size() == 0) {
        portal.carry.reset();
        portal.exhausted = true;
        break;
      }
    }
    const auto take = std::min<uint64_t>(
      portal.carry->size() - portal.carry_offset, max_rows - sent);
    WriteDataChunkRange(_send, *portal.carry, serializers, context,
                        portal.carry_offset, portal.carry_offset + take);
    portal.carry_offset += take;
    sent += take;
  }

  if (portal.exhausted) {
    // Postgres reports this Execute's row count (not the portal-wide total).
    WriteCommandComplete(_send, sdb::pg::FormatCommandTag(
                                  *portal.stmt->prepared, return_type, sent));
    return false;
  }
  return true;
}

template<SocketKind Kind>
yaclib::Future<bool> PgWireSession<Kind>::Authenticate() {
  if (!_credentials) {
    co_return true;  // no provider configured -> trust (current behavior)
  }
  const auto credential = _credentials->LookupCredential(UserName());
  if (!credential) {
    co_return true;  // no credential for this user -> trust
  }
  if (credential->scram) {
    co_return co_await AuthenticateScram(*credential->scram);
  }
  if (credential->cleartext) {
    co_return co_await AuthenticateCleartext(*credential->cleartext);
  }
  WriteErrorResponse(
    _send, sdb::pg::SqlErrorData{
             .errcode = ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION,
             .errmsg = "no usable authentication credential for user"});
  co_return false;
}

template<SocketKind Kind>
yaclib::Future<bool> PgWireSession<Kind>::AuthenticateCleartext(
  const std::string& expected) {
  if (!_socket.IsTls() && !_allow_cleartext) {
    WriteErrorResponse(
      _send, sdb::pg::SqlErrorData{
               .errcode = ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION,
               .errmsg = "cleartext password authentication requires TLS"});
    co_return false;
  }
  WriteAuthRequest(_send, 3, {});  // AuthenticationCleartextPassword
  co_await Flush();
  auto frame = co_await NextFrame(true, _max_message);
  if (frame.status != FrameStatus::Ok ||
      frame.type != PQ_MSG_PASSWORD_MESSAGE) {
    if (frame.recv_consume) {
      _recv.Consume(frame.recv_consume);
    }
    co_return false;
  }
  auto payload = frame.payload;
  while (!payload.empty() && payload.back() == '\0') {
    payload.remove_suffix(1);
  }
  const std::string given = SaslPrep(payload);
  if (frame.recv_consume) {
    _recv.Consume(frame.recv_consume);
  }
  const std::string want = SaslPrep(expected);
  const bool ok = ConstantTimeEqual(
    {reinterpret_cast<const uint8_t*>(given.data()), given.size()},
    {reinterpret_cast<const uint8_t*>(want.data()), want.size()});
  if (!ok) {
    WriteErrorResponse(_send, sdb::pg::SqlErrorData{
                                .errcode = ERRCODE_INVALID_PASSWORD,
                                .errmsg = absl::StrCat(
                                  "password authentication failed for user \"",
                                  UserName(), "\"")});
    co_return false;
  }
  co_return true;
}

template<SocketKind Kind>
yaclib::Future<bool> PgWireSession<Kind>::AuthenticateScram(
  const ScramVerifier& verifier) {
  const auto fail = [&](int errcode, std::string message) {
    WriteErrorResponse(
      _send,
      sdb::pg::SqlErrorData{.errcode = errcode, .errmsg = std::move(message)});
  };

  std::string mechanisms = "SCRAM-SHA-256";
  mechanisms.push_back('\0');
  mechanisms.push_back('\0');
  WriteAuthRequest(_send, 10, mechanisms);  // AuthenticationSASL
  co_await Flush();

  // --- client-first (SASLInitialResponse) ---
  auto first = co_await NextFrame(true, _max_message);
  if (first.status != FrameStatus::Ok ||
      first.type != PQ_MSG_PASSWORD_MESSAGE) {
    if (first.recv_consume) {
      _recv.Consume(first.recv_consume);
    }
    co_return false;
  }
  std::string_view head = first.payload;
  const auto mech_end = head.find('\0');
  if (mech_end == std::string_view::npos ||
      head.substr(0, mech_end) != "SCRAM-SHA-256" ||
      head.size() < mech_end + 1 + sizeof(int32_t)) {
    if (first.recv_consume) {
      _recv.Consume(first.recv_consume);
    }
    fail(ERRCODE_PROTOCOL_VIOLATION, "malformed SASL initial response");
    co_return false;
  }
  head.remove_prefix(mech_end + 1);
  const auto initial_len =
    static_cast<int32_t>(absl::big_endian::Load32(head.data()));
  head.remove_prefix(sizeof(int32_t));
  if (initial_len < 0 || static_cast<size_t>(initial_len) > head.size()) {
    if (first.recv_consume) {
      _recv.Consume(first.recv_consume);
    }
    fail(ERRCODE_PROTOCOL_VIOLATION, "malformed SASL initial response");
    co_return false;
  }
  const std::string client_first{head.substr(0, initial_len)};
  if (first.recv_consume) {
    _recv.Consume(first.recv_consume);
  }

  // parse gs2 header + client-first-bare ("n,," + "n=,r=<cnonce>")
  const auto comma1 = client_first.find(',');
  const auto comma2 = comma1 == std::string::npos
                        ? std::string::npos
                        : client_first.find(',', comma1 + 1);
  if (comma2 == std::string::npos) {
    fail(ERRCODE_PROTOCOL_VIOLATION, "malformed SCRAM message");
    co_return false;
  }
  const std::string_view cbind_flag{client_first.data(), comma1};
  if (cbind_flag != "n" && cbind_flag != "y") {
    // 'p' would require SCRAM-SHA-256-PLUS, which we don't advertise yet.
    fail(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION,
         "channel binding not supported");
    co_return false;
  }
  if (comma2 != comma1 + 1) {
    fail(ERRCODE_FEATURE_NOT_SUPPORTED, "authorization identity not supported");
    co_return false;
  }
  const std::string gs2_header = client_first.substr(0, comma2 + 1);
  const std::string client_first_bare = client_first.substr(comma2 + 1);
  const auto r_pos = client_first_bare.find("r=");
  if (r_pos == std::string::npos) {
    fail(ERRCODE_PROTOCOL_VIOLATION, "malformed SCRAM message");
    co_return false;
  }
  std::string_view client_nonce{client_first_bare};
  client_nonce.remove_prefix(r_pos + 2);
  if (const auto end = client_nonce.find(','); end != std::string_view::npos) {
    client_nonce = client_nonce.substr(0, end);
  }

  // --- server-first (SASLContinue) ---
  uint8_t raw_nonce[18];
  if (!RandomBytes(raw_nonce)) {
    fail(ERRCODE_INTERNAL_ERROR, "could not generate nonce");
    co_return false;
  }
  const std::string combined_nonce =
    absl::StrCat(client_nonce, Base64Encode(raw_nonce));
  const std::string server_first =
    absl::StrCat("r=", combined_nonce, ",s=", Base64Encode(verifier.salt),
                 ",i=", verifier.iterations);
  WriteAuthRequest(_send, 11, server_first);
  co_await Flush();

  // --- client-final (SASLResponse) ---
  auto final_frame = co_await NextFrame(true, _max_message);
  if (final_frame.status != FrameStatus::Ok ||
      final_frame.type != PQ_MSG_PASSWORD_MESSAGE) {
    if (final_frame.recv_consume) {
      _recv.Consume(final_frame.recv_consume);
    }
    co_return false;
  }
  const std::string client_final{final_frame.payload};
  if (final_frame.recv_consume) {
    _recv.Consume(final_frame.recv_consume);
  }
  const auto proof_pos = client_final.rfind(",p=");
  if (proof_pos == std::string::npos || !client_final.starts_with("c=")) {
    fail(ERRCODE_PROTOCOL_VIOLATION, "malformed SCRAM message");
    co_return false;
  }
  const std::string without_proof = client_final.substr(0, proof_pos);
  const std::string_view proof_b64{client_final.data() + proof_pos + 3,
                                   client_final.size() - proof_pos - 3};
  const auto r_idx = without_proof.find(",r=");
  if (r_idx == std::string::npos) {
    fail(ERRCODE_PROTOCOL_VIOLATION, "malformed SCRAM message");
    co_return false;
  }
  const std::string_view c_value{without_proof.data() + 2, r_idx - 2};
  const std::string_view r_value{without_proof.data() + r_idx + 3,
                                 without_proof.size() - r_idx - 3};
  if (r_value != combined_nonce) {
    fail(ERRCODE_PROTOCOL_VIOLATION, "SCRAM nonce mismatch");
    co_return false;
  }
  const auto cbind = Base64Decode(c_value);
  if (!cbind || std::string_view{reinterpret_cast<const char*>(cbind->data()),
                                 cbind->size()} != gs2_header) {
    fail(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION,
         "SCRAM channel binding check failed");
    co_return false;
  }
  const auto proof = Base64Decode(proof_b64);
  if (!proof || proof->size() != static_cast<size_t>(kScramKeyLen)) {
    fail(ERRCODE_PROTOCOL_VIOLATION, "malformed SCRAM proof");
    co_return false;
  }
  const std::string auth_message =
    absl::StrCat(client_first_bare, ",", server_first, ",", without_proof);
  if (!VerifyClientProof(verifier, auth_message, *proof)) {
    fail(ERRCODE_INVALID_PASSWORD,
         absl::StrCat("password authentication failed for user \"", UserName(),
                      "\""));
    co_return false;
  }
  const auto signature = ScramServerSignature(verifier, auth_message);
  WriteAuthRequest(_send, 12, absl::StrCat("v=", Base64Encode(signature)));
  co_return true;
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
  if (extracted.empty()) {
    // A non-empty but statement-less query (";", a bare comment): postgres
    // replies EmptyQueryResponse, not just a bare ReadyForQuery.
    WriteEmptyFrame(_send, PQ_MSG_EMPTY_QUERY_RESPONSE);
    co_return {};
  }
  for (auto& statement : extracted) {
    if (IsCopyFromStdin(*statement)) {
      co_await RunCopyFromStdin(std::move(statement));
      // RunCopyFromStdin returns pinned to _ioexec; the remaining statements'
      // Prepare/Execute must run on the DuckDB worker, not the io thread.
      co_await yaclib::On(*_duck);
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
  const bool is_binary = statement && IsBinaryCopy(*statement);
  // Handshake first (no bridge needed yet); a Flush failure here is clean. The
  // client streams CopyData only after CopyInResponse, so it must go out before
  // Prepare (which sniffs /dev/stdin).
  co_await yaclib::On(*_ioexec);
  if (_dispatch_consume) {
    _recv.Consume(_dispatch_consume);
    _dispatch_consume = 0;
  }
  WriteCopyInResponse(_send, is_binary);
  co_await Flush();

  auto& client_state = *_conn->context->registered_state
                          ->template Get<connector::SereneDBClientState>(
                            connector::kSereneDBClientStateKey);
  client_state.copy_stdin_open_count = 0;
  client_state.copy_stdin_done = false;
  client_state.copy_stdin_buffer.reset();
  client_state.copy_stdin_no_replay = is_binary;
  sdb::pg::CopyInBridge bridge;
  _connection_ctx->SetCopyInBridge(&bridge);
  auto feeder = RunCopyInFeeder(bridge, is_binary);

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
  sdb::pg::CopyInBridge& bridge, bool is_binary) {
  co_await yaclib::On(*_ioexec);
  for (;;) {
    auto frame = co_await NextFrame(true, _max_message);
    if (frame.status != FrameStatus::Ok) {
      bridge.Fail(std::make_exception_ptr(
        sdb::SqlException{sdb::pg::SqlErrorData{
                            .errcode = ERRCODE_CONNECTION_EXCEPTION,
                            .errmsg = "unexpected EOF during COPY from stdin"},
                          std::source_location::current()}));
      co_return {};
    }
    const char type = frame.type;
    if (type == PQ_MSG_COPY_DATA) {
      if (!bridge.Aborted()) {
        auto payload = frame.payload;
        // The text "\." end-marker is a text-COPY convention; binary COPY ends
        // with the in-stream PGCOPY -1 trailer, so its bytes pass through raw.
        if (!is_binary) {
          if (payload.ends_with(std::string_view{"\\.\n"})) {
            payload.remove_suffix(3);
          } else if (payload.ends_with(std::string_view{"\\.\r\n"})) {
            payload.remove_suffix(4);
          }
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
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DUPLICATE_PSTATEMENT),
        ERR_MSG("prepared statement \"", statement_name, "\" already exists"));
    }
    statement = &it->second;
  }
  statement->Reset();
  // COPY ... FROM STDIN can't be prepared here (the CSV sniff would open
  // /dev/stdin before the CopyData feeder is live). Parse-probe COPY-prefixed
  // statements only, and stash the unbound statement; the bind happens at
  // Execute via RunCopyFromStdin.
  if (StartsWithCopy(query)) {
    auto extracted = _conn->ExtractStatements(std::string{query});
    if (extracted.size() == 1 && IsCopyFromStdin(*extracted[0])) {
      statement->deferred_copy = std::move(extracted[0]);
      WriteEmptyFrame(_send, PQ_MSG_PARSE_COMPLETE);
      return;
    }
  }
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
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_SQL_STATEMENT_NAME),
        ERR_MSG("prepared statement \"", statement_name, "\" does not exist"));
    }
    statement = &it->second;
  }
  if (!statement->prepared && !statement->deferred_copy) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_SQL_STATEMENT_NAME),
                    ERR_MSG("prepared statement is not ready"));
  }

  Portal portal;
  portal.stmt = statement;
  // A deferred COPY FROM STDIN has no prepared statement, no parameters, and no
  // result columns; just record the (empty) bind. ParseBindVars would otherwise
  // dereference the null prepared.
  if (!statement->deferred_copy) {
    portal.bind_info = ParseBindVars(payload, *statement);
    portal.pending =
      PendingQueryEnsured(*statement->prepared, portal.bind_info.param_values);
    if (portal.pending->HasError()) {
      ThrowDuck(portal.pending->GetErrorObject());
    }
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
  if (stmt.deferred_copy) {
    // COPY FROM STDIN takes no parameters and returns no rows.
    WriteParameterDescription(_send, {});
    WriteEmptyFrame(_send, PQ_MSG_NO_DATA);
    return;
  }
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
  if (portal.stmt && portal.stmt->deferred_copy) {
    WriteEmptyFrame(_send, PQ_MSG_NO_DATA);
    return;
  }
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
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_SQL_STATEMENT_NAME),
          ERR_MSG("prepared statement \"", name, "\" does not exist"));
      }
      DescribeStatement(it->second);
    }
  } else if (what == 'P') {
    if (name.empty()) {
      if (!_anon_portal.stmt) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_CURSOR_NAME),
                        ERR_MSG("portal \"\" does not exist"));
      }
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
  if (name_end == std::string_view::npos ||
      payload.size() < name_end + 1 + sizeof(int32_t)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("malformed Execute message"));
  }
  const std::string_view portal_name = payload.substr(0, name_end);
  const auto requested_rows = static_cast<int32_t>(
    absl::big_endian::Load32(payload.data() + name_end + 1));
  // Postgres treats max_rows <= 0 as "fetch all" -> the unpaged fast path.
  const uint64_t max_rows =
    requested_rows > 0 ? static_cast<uint64_t>(requested_rows) : 0;

  Portal* portal = &_anon_portal;
  if (!portal_name.empty()) {
    const auto it = _portals.find(portal_name);
    if (it == _portals.end()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_CURSOR_NAME),
                      ERR_MSG("portal \"", portal_name, "\" does not exist"));
    }
    portal = &it->second;
  }
  if (portal->stmt && portal->stmt->deferred_copy) {
    // Deferred COPY FROM STDIN: now safe to bind -- RunCopyFromStdin brings the
    // feeder live before the sniff. Consume the stashed statement so a stray
    // re-Execute (or a second portal bound to it) gets a clean "not bound"
    // error rather than re-running the COPY.
    co_await RunCopyFromStdin(std::move(portal->stmt->deferred_copy));
    co_return {};
  }
  if (!portal->stmt || !portal->pending) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_CURSOR_NAME),
                    ERR_MSG("portal is not bound"));
  }

  const auto return_type =
    portal->stmt->prepared->GetStatementProperties().return_type;

  // Drive + Execute exactly once per portal; a re-Execute (cursor paging)
  // resumes the retained result. PendingQueryResult::Execute() closes the
  // pending, so calling it twice would throw.
  if (!portal->started) {
    const auto status = co_await DrivePending(*_duck, *portal->pending);
    if (status == duckdb::PendingExecutionResult::EXECUTION_ERROR ||
        portal->pending->HasError()) {
      ThrowDuck(portal->pending->GetErrorObject());
    }
    portal->result = portal->pending->Execute();
    if (portal->result->HasError()) {
      ThrowDuck(portal->result->GetErrorObject());
    }
    portal->started = true;
  }

  // Re-Execute of a portal already drained by a previous Execute: its stream
  // is closed, so don't fetch -- just complete with 0 rows (postgres's
  // atEnd -> CommandComplete path).
  if (portal->exhausted) {
    WriteCommandComplete(_send, sdb::pg::FormatCommandTag(
                                  *portal->stmt->prepared, return_type, 0));
    co_return {};
  }

  // max_rows paginates only row-returning portals; DDL/DML are materialized,
  // run to completion, never suspend. Both keep the existing full-drain path,
  // which stays byte-for-byte unchanged (zero added per-row overhead).
  if (max_rows == 0 ||
      return_type != duckdb::StatementReturnType::QUERY_RESULT) {
    SerializeResult(*portal->stmt->prepared, *portal->result, return_type,
                    /*with_row_description=*/false,
                    portal->bind_info.output_formats);
    portal->exhausted = true;
    co_return {};
  }

  if (SerializePage(*portal, return_type, max_rows)) {
    WriteEmptyFrame(_send, PQ_MSG_PORTAL_SUSPENDED);
  }
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
      if (code == static_cast<uint32_t>(CANCEL_REQUEST_CODE)) {
        // CancelRequest payload: code(4) + backend pid(4) + secret(4). Look the
        // (pid,secret) up in the shared registry and interrupt the target's
        // running query cross-thread. No reply -- just close (per protocol).
        if (_cancel && frame.payload.size() >= 12) {
          const uint32_t pid =
            absl::big_endian::Load32(frame.payload.data() + 4);
          const uint32_t secret =
            absl::big_endian::Load32(frame.payload.data() + 8);
          _cancel->Cancel((uint64_t{pid} << 32) | secret);
        }
        _socket.Close();
        co_return {};
      }
      const auto requested_major = PG_PROTOCOL_MAJOR(code);
      const auto requested_minor = PG_PROTOCOL_MINOR(code);
      constexpr auto kLatestMajor =
        static_cast<uint32_t>(PG_PROTOCOL_MAJOR(PG_PROTOCOL_LATEST));
      constexpr auto kLatestMinor =
        static_cast<uint32_t>(PG_PROTOCOL_MINOR(PG_PROTOCOL_LATEST));
      if (requested_major != kLatestMajor) {
        WriteErrorResponse(
          _send,
          absl::StrCat("unsupported frontend protocol ", requested_major, ".",
                       requested_minor, ": server supports ", kLatestMajor, ".",
                       kLatestMinor),
          "0A000");
        co_await Flush();
        _socket.Close();
        co_return {};
      }
      ParseStartupParams(frame.payload);
      if (frame.recv_consume) {
        _recv.Consume(frame.recv_consume);
      }
      // A client asking for a newer minor version or sending unrecognized _pq_
      // protocol options is negotiated down to what we support rather than
      // dropped; the _pq_ options must not reach the GUC layer.
      {
        std::vector<std::string_view> unrecognized;
        for (const auto& entry : _params) {
          if (entry.first.starts_with("_pq_.")) {
            unrecognized.push_back(entry.first);
          }
        }
        if (requested_minor > kLatestMinor || !unrecognized.empty()) {
          WriteNegotiateProtocolVersion(
            _send, static_cast<int32_t>(kLatestMinor), unrecognized);
        }
      }
      erase_if(_params, [](const auto& entry) {
        return entry.first.starts_with("_pq_.");
      });
      break;
    }

    // SereneDB has no WAL/walsender, so refuse replication connections with a
    // clear error instead of mishandling the replication sub-protocol.
    if (const auto it = _params.find("replication"); it != _params.end()) {
      const auto& mode = it->second;
      if (mode == "true" || mode == "on" || mode == "yes" || mode == "1" ||
          mode == "database") {
        WriteErrorResponse(
          _send, "replication connections are not supported by SereneDB",
          "0A000");
        co_await Flush();
        _socket.Close();
        co_return {};
      }
    }

    // Authenticate before establishing the connection / sending
    // AuthenticationOk (which SendStartupBurst does). TLS is already settled
    // (the SSLRequest upgrade ran earlier in the loop), so IsTls() is final
    // here.
    if (!co_await Authenticate()) {
      co_await Flush();
      _socket.Close();
      co_return {};
    }

    if (!SetupConnection()) {
      co_await Flush();
      _socket.Close();
      co_return {};
    }

    // Register for query cancellation. The token holds this connection's
    // ClientContext; a CancelRequest from another connection interrupts it
    // cross-thread. Detach-then-Unregister on every exit path (the guard fires
    // as the coroutine frame unwinds, while _conn is still alive) so a racing
    // Cancel can never touch a torn-down context.
    _cancel_token = std::make_shared<CancelToken>();
    _cancel_token->ctx = _conn->context.get();
    if (_cancel) {
      _cancel_key = _cancel->Register(_cancel_token);
    }
    absl::Cleanup cancel_guard{[this] {
      _cancel_token->Detach();
      if (_cancel && _cancel_key) {
        _cancel->Unregister(_cancel_key);
      }
    }};

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
      auto frame = co_await NextFrame(true, _max_message);
      if (frame.status == FrameStatus::TooLarge) {
        // PG sends an error then closes rather than silently dropping the
        // connection on an over-cap message.
        WriteErrorResponse(_send, "incoming message exceeds maximum size",
                           "54000");
        co_await Flush();
        _socket.Close();
        co_return {};
      }
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
        WriteErrorResponse(_send,
                           DuckErrorToSqlData(duckdb::ErrorData{exception}));
        ignore_till_sync = IsExtended(type);
      } catch (const std::exception& exception) {
        WriteErrorResponse(
          _send, sdb::pg::SqlErrorData{.errcode = ERRCODE_INTERNAL_ERROR,
                                       .errmsg = exception.what()});
        ignore_till_sync = IsExtended(type);
      }
      co_await yaclib::On(*_ioexec);
      DrainNotices();
      ReportChangedParameters();
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
