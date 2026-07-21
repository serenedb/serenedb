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

#include <atomic>
#include <chrono>
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
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/star_expression.hpp>
#include <duckdb/parser/parsed_data/copy_info.hpp>
#include <duckdb/parser/query_node/select_node.hpp>
#include <duckdb/parser/statement/copy_statement.hpp>
#include <duckdb/parser/statement/select_statement.hpp>
#include <duckdb/parser/tableref/basetableref.hpp>
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
#include <yaclib/coro/task.hpp>
#include <yaclib/util/helper.hpp>

#include "basics/asio_ns.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/node_hash_map.h"
#include "basics/duckdb_engine.h"
#include "basics/message_buffer.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_pg_text_copy.h"
#include "network/connection.h"
#include "network/cpu_resumer.h"
#include "network/credentials.h"
#include "network/gate.h"
#include "network/io_executor.h"
#include "network/pg/cancel_registry.h"
#include "network/pg/frame_reader.h"
#include "network/pg/pg_frame_codec.h"
#include "network/pg/protocol_state.h"
#include "network/pg/startup_request.h"
#include "network/pg/wire_collector.h"
#include "network/pg/wire_frames.h"
#include "network/socket.h"
#include "pg/command_tag.h"
#include "pg/connection_context.h"
#include "pg/copy_in_bridge.h"
#include "pg/deserialize.h"
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
enum class AuthMethod : uint8_t { Scram, Md5, Cleartext };

struct PgServerContext {
  asio_ns::ssl::context* ssl = nullptr;
  const CredentialProvider* credentials = nullptr;
  bool allow_cleartext_without_tls = false;
  // sslmode=require/verify-ca/verify-full: reject a plaintext startup.
  bool require_tls = false;
  AuthMethod auth_method = AuthMethod::Scram;
  CancelRegistry* cancel = nullptr;
  uint32_t max_message_bytes = kDefaultMaxMessageBytes;
  // Shared live-connection counter + the cap this listener enforces (0 =
  // unlimited). Over-cap connections get 53300 then close.
  std::atomic<uint32_t>* active = nullptr;
  uint32_t max_connections = 0;
  // Deadline for TLS handshake + startup + auth (0 = off); slowloris guard.
  std::chrono::milliseconds auth_timeout{0};
  // HAProxy PROXY-protocol preface policy (off / optional / require).
  ProxyMode proxy = ProxyMode::Off;
};

// COPY wire format, from info->format (the lowercased copy-function name).
// Binary and text are serenedb's own (de)serialized PG COPY formats; parquet
// needs a seekable file; Other (csv / no FORMAT) goes through DuckDB's writer.
enum class CopyFormat : uint8_t { Other, Text, Binary, Parquet };

// Portal / PageCursor / PortalState live in protocol_state.h (with the stores
// that own them).

template<SocketKind Kind>
class PgWireSession
  : public Transport<Kind, PgWireSession<Kind>>,
    public duckdb::enable_shared_from_this<PgWireSession<Kind>> {
 public:
  using Deps = PgServerContext;
  static constexpr SocketKind kSocketKind = Kind;

  PgWireSession(PgServerContext& ctx, IoExecutor& exec)
    requires(Kind == SocketKind::Tcp)
    : Transport<Kind, PgWireSession<Kind>>{exec},
      _io{exec.Context()},
      _deadline{exec.Context()},
      _credentials{ctx.credentials},
      _allow_cleartext{ctx.allow_cleartext_without_tls},
      _require_tls{ctx.require_tls},
      _auth_method{ctx.auth_method},
      _cancel{ctx.cancel},
      _max_message{ctx.max_message_bytes},
      _active{ctx.active},
      _max_conn{ctx.max_connections},
      _auth_timeout{ctx.auth_timeout},
      _proxy{ctx.proxy},
      _frames{this->_recv} {}

  PgWireSession(PgServerContext& ctx, IoExecutor& exec)
    requires(Kind == SocketKind::MaybeTls)
    : Transport<Kind, PgWireSession<Kind>>{exec, *ctx.ssl},
      _io{exec.Context()},
      _deadline{exec.Context()},
      _credentials{ctx.credentials},
      _allow_cleartext{ctx.allow_cleartext_without_tls},
      _require_tls{ctx.require_tls},
      _auth_method{ctx.auth_method},
      _cancel{ctx.cancel},
      _max_message{ctx.max_message_bytes},
      _active{ctx.active},
      _max_conn{ctx.max_connections},
      _auth_timeout{ctx.auth_timeout},
      _proxy{ctx.proxy},
      _frames{this->_recv} {}

  PgWireSession(PgServerContext& ctx, IoExecutor& exec)
    requires(Kind == SocketKind::Unix)
    : Transport<Kind, PgWireSession<Kind>>{exec},
      _io{exec.Context()},
      _deadline{exec.Context()},
      _credentials{ctx.credentials},
      _allow_cleartext{true},
      _require_tls{false},
      _auth_method{ctx.auth_method},
      _cancel{ctx.cancel},
      _max_message{ctx.max_message_bytes},
      _active{ctx.active},
      _max_conn{ctx.max_connections},
      _auth_timeout{ctx.auth_timeout},
      _proxy{ctx.proxy},
      _frames{this->_recv} {}

  // Run is the session's sole owner: it grabs the one shared_from_this, starts
  // the writer + cpu futures on a raw `this`, and joins both before it returns.
  void Start() { Run().Detach(); }

  // Stop hook: wake the COPY feeder and interrupt any running query.
  void OnStop() {
    _copy_gate.Kick();
    if (_cancel_token) {
      _cancel_token->Cancel();
    }
  }

 private:
  // The session owner. Starts the writer, negotiates startup+auth, hands off to
  // the cpu task, pumps recv, then stops everything and joins the cpu + writer
  // futures before its frame (and the session) is destroyed.
  yaclib::Task<> Run();
  // Proxy preface + TLS handshake + startup negotiation + auth, lifted out of
  // Run so it has a single bool exit (proceed / terminal). Sends any fatal
  // reply itself; the caller tears down regardless.
  yaclib::Task<bool> Negotiate();
  // Whether the startup negotiation produced a session to proceed with, or a
  // terminal outcome (frame error / SSL or GSS reply / CancelRequest / bad
  // version / replication) where Run just closes the socket.
  enum class StartupOutcome : bool { Proceed, Close };
  // The SSL/GSS/Cancel/version/_pq_ negotiation loop, lifted out of Run so the
  // pure decode (ParseStartup) and the co_await IO read as one sequence. Fills
  // `startup` and returns Proceed on a real StartupMessage.
  yaclib::Task<StartupOutcome> NegotiateStartup(StartupRequest& startup);
  // Register the cancel token, emplace the task, and start the cpu coroutine on
  // a duck worker. Returns its future for Run to join.
  yaclib::Future<> SpawnSession();
  // The steady recv pump: socket read -> _recv -> wake the worker (or the
  // copy-gate while a COPY FROM STDIN feeder owns the consumer role).
  yaclib::Task<> PumpRecv();
  // Each wraps the shared frame assembler (_frames) with its own wait:
  // NextFrame (RecvLoop startup phase: socket reads), AwaitFrame (SessionTask:
  // park), FeedFrame (COPY feeder: gate wait).
  yaclib::Task<Frame> NextFrame(FrameKind kind, uint32_t max_len);
  yaclib::Task<Frame> AwaitFrame(FrameKind kind, uint32_t max_len);
  yaclib::Task<Frame> FeedFrame(FrameKind kind, uint32_t max_len);
  void DrainNotices();
  void ReportChangedParameters();
  // At a sync point (client Sync, or the self-sync after a simple query):
  // commit our implicit block if we own it, then emit ReadyForQuery + kick the
  // writer. Shared so the two sync points cannot drift apart.
  void CommitAndReportReady();

  // The transaction-boundary owners -- the only places the implicit block is
  // resolved and txn-scoped portals are dropped, so the logic cannot smear.
  // Commit the implicit block if we own it (drops its portals); returns the
  // commit error, if any, for the caller to order against the CommandComplete.
  std::optional<sdb::pg::SqlErrorData> CommitImplicitBlock();
  // Roll the implicit block back if we own it (drops its portals); returns
  // whether it rolled back (so the caller can fall through to the explicit
  // case).
  bool RollbackImplicitBlock();
  // After an explicit BEGIN/COMMIT/ROLLBACK ran: track ownership, and if it
  // closed the transaction drop the portals it owned.
  void AfterTxnStatement();

  yaclib::Task<bool> Authenticate();
  void CapturePeerAddress();
  yaclib::Task<bool> AuthenticateCleartext(const Credential& credential);
  yaclib::Task<bool> AuthenticateMd5(std::string stored_md5);
  yaclib::Task<bool> AuthenticateScram(const ScramVerifier& verifier);

  // The duck-side half of the session: an endless coroutine hosted by _task
  // on the DuckDB scheduler. Prologue (SetupConnection + startup burst) ->
  // command loop -> teardown. Never runs on an io thread.
  yaclib::Future<> SessionMain();
  // The post-bring-up command loop: assemble one frame, dispatch it, resolve
  // sync points. Runs until Terminate / EOF / a fatal frame.
  yaclib::Task<> RunCommandLoop();
  // Drives one query as inline ExecuteTask slices on the SessionTask's
  // worker: yields between productive slices, parks on NO_TASKS/BLOCKED until
  // the executor's on_reschedule wake. With a wire context, every wake also
  // drains the collector (splice chains, reschedule blocked sinks).
  yaclib::Task<duckdb::PendingExecutionResult> DriveQuery(
    duckdb::PendingQueryResult& pending, WireSinkContext* wire = nullptr,
    bool own_waiter = true);
  // The drive sequence the simple, extended and COPY flows share: plan, drive
  // to a ready status, then materialize. The error boundaries fire in postgres
  // order -- pending plan error, then mid-stream drive error, then result
  // error. The caller owns `pending` storage (a portal retains it to keep a
  // paged StreamQueryResult's executor state alive). A wire context routes rows
  // to the socket during the drive and is fully drained before Execute.
  yaclib::Task<duckdb::unique_ptr<duckdb::QueryResult>> DriveToResult(
    duckdb::PreparedStatement& prepared, duckdb::vector<duckdb::Value>& values,
    ClosingPending& pending, std::shared_ptr<WireSinkContext> wire,
    std::shared_ptr<const catalog::Snapshot> bound_snapshot = nullptr);
  yaclib::Task<> RunSimpleQuery(std::string_view query);
  yaclib::Task<> RunCopyFromStdin(
    duckdb::unique_ptr<duckdb::SQLStatement> statement);
  yaclib::Task<> RunCopyToStdout(
    duckdb::unique_ptr<duckdb::SQLStatement> statement, CopyFormat format);
  // COPY ... TO STDOUT for DuckDB-native formats (csv/json/parquet/...): DuckDB
  // writes the bytes, the session frames them.
  yaclib::Task<> RunCopyToStdoutViaFormat(
    duckdb::unique_ptr<duckdb::SQLStatement> statement, CopyFormat format);
  yaclib::Task<> RunCopyInFeeder(sdb::pg::CopyInBridge& bridge,
                                 CopyFormat format);
  void HandleParse(std::string_view payload);
  void HandleBind(std::string_view payload);
  void HandleDescribe(std::string_view payload);
  yaclib::Task<> HandleExecute(std::string_view payload);
  // Run a bound Prepared portal: arm the block, then plan/drive once and emit
  // rows (full-drain wire path or cursor paging), or resume a paged portal.
  yaclib::Task<> ExecutePrepared(Portal& portal, uint64_t max_rows);
  // Run a compound portal: one user command the parser expanded into a body
  // (ALTER ... DEFAULT <volatile>, PIVOT). Runs the whole body in the session's
  // implicit block and reports the primary sub-statement's one CommandComplete.
  yaclib::Task<> ExecuteCompound(Portal& portal);
  // Run (and consume) a bound deferred-COPY portal at Execute.
  yaclib::Task<> RunDeferredCopy(Portal& portal);
  void HandleClose(std::string_view payload);
  void DescribeStatement(Statement& stmt);
  void DescribePortal(Portal& portal);
  BindInfo ParseBindVars(std::string_view cursor, const Statement& stmt,
                         std::string_view statement_name);

  std::string_view DatabaseName() const;
  std::string_view UserName() const;
  bool SetupConnection();
  void SendStartupBurst();
  duckdb::unique_ptr<duckdb::PendingQueryResult> PendingQueryEnsured(
    duckdb::PreparedStatement& prepared, duckdb::vector<duckdb::Value>& values,
    std::shared_ptr<WireSinkContext> wire);
  // Single-lifecycle variant for the simple protocol: bind and execution share
  // one duckdb query (and one transaction), so plan-held catalog references
  // stay valid end-to-end. RowDescription is written by the collector hook
  // (wire->announce_rowdesc), the only post-bind point that precedes task
  // start.
  duckdb::unique_ptr<duckdb::PendingQueryResult> PendingStatementEnsured(
    duckdb::unique_ptr<duckdb::SQLStatement> statement,
    const std::shared_ptr<WireSinkContext>& wire);
  yaclib::Task<duckdb::unique_ptr<duckdb::QueryResult>> DriveStatementToResult(
    duckdb::unique_ptr<duckdb::SQLStatement> statement, ClosingPending& pending,
    std::shared_ptr<WireSinkContext> wire);
  // Rejects (sdb_strict_ddl) or notices catalog DDL inside an explicit
  // transaction block. Statement type is a parse-time property.
  void NoticeDdlInTransaction(duckdb::StatementType type);
  // Builds an armed per-execution wire-sink contract (see wire_collector.h).
  std::shared_ptr<WireSinkContext> MakeWireContext(
    std::span<const sdb::pg::VarFormat> formats);
  // Splices queued chains onto _send (parallel mode), respecting the send
  // high-water, and reschedules blocked sinks. Returns true while the wire
  // path is healthy (false = client gone).
  bool DrainWire(WireSinkContext& wire);
  // After the drive: splice everything left, parking for the writer when the
  // send buffer is over the cap.
  yaclib::Task<> FinishWireDrain(WireSinkContext& wire);
  // Post-error discard mode (postgres.c ignore_till_sync): after an error in an
  // extended-protocol message the backend drops every following frame until the
  // next Sync. Simple Query is self-syncing, so it never enters
  // DiscardUntilSync.
  enum class PostError : bool { Resync = false, DiscardUntilSync = true };
  // The one per-command error funnel: convert any thrown exception to the wire
  // (one ErrorResponse), then apply the transaction transition (implicit ->
  // roll back inline; explicit -> invalidate so the next statement surfaces
  // 25P02), returning the discard mode the command loop must enter.
  PostError FunnelError(const std::exception& exception, char type);
  void WriteCommandTag(const duckdb::PreparedStatement& prepared,
                       duckdb::QueryResult& result,
                       duckdb::StatementReturnType return_type);
  void WriteCommandTag(const sdb::pg::CommandTag& tag,
                       duckdb::QueryResult& result,
                       duckdb::StatementReturnType return_type);

  asio_ns::io_context& _io;
  asio_ns::steady_timer _deadline;
  const CredentialProvider* _credentials = nullptr;
  bool _allow_cleartext = false;
  bool _require_tls = false;
  AuthMethod _auth_method = AuthMethod::Scram;
  CancelRegistry* _cancel = nullptr;
  std::shared_ptr<CancelToken> _cancel_token;
  uint64_t _cancel_key = 0;
  uint32_t _max_message = kDefaultMaxMessageBytes;
  std::atomic<uint32_t>* _active = nullptr;
  uint32_t _max_conn = 0;
  std::chrono::milliseconds _auth_timeout{0};
  ProxyMode _proxy = ProxyMode::Off;
  // Client peer IP, captured once from the socket before auth. Unspecified
  // (the default) for a unix socket or a capture failure -- both no-IP, so the
  // gate falls back to is_local / fails closed. Normalized so a v4-mapped-v6
  // loopback reads as IPv4. Feeds HBA CIDR matching and the passwordless-trust
  // gate (via asio's address::is_loopback()).
  asio_ns::ip::address _peer_addr;
  FrameReader _frames;
  // The current command frame's pending _recv consume. Normally the command
  // loop applies it after the handler; COPY FROM STDIN consumes it early (so
  // the feeder reads past it) and zeroes this.
  size_t _dispatch_consume = 0;
  containers::FlatHashMap<std::string, std::string> _params;
  containers::FlatHashMap<std::string, std::string> _reported_params;
  // Connection settings version last reflected into _reported_params; gates the
  // per-command ParameterStatus poll in ReportChangedParameters.
  uint64_t _reported_settings_version = 0;
  duckdb::unique_ptr<duckdb::Connection> _conn;
  std::shared_ptr<ConnectionContext> _connection_ctx;
  // Owned by _conn's registered_state; cached here to skip the keyed registry
  // lookup on every query. Valid for the connection's lifetime.
  connector::SereneDBClientState* _client_state = nullptr;
  // Reused across queries (Reset per query); see MakeWireContext.
  std::shared_ptr<WireSinkContext> _wire_ctx;
  // True while a COPY FROM STDIN feeder owns the recv consumer role: RecvLoop
  // then kicks _copy_gate instead of RequestRun.
  std::atomic_bool _copy_route = false;
  Gate _copy_gate;
  std::atomic_bool _feeder_done = false;
  // Prepared-statement + portal stores (named + anon slots) and the cross-store
  // close cascade; see ProtocolState.
  ProtocolState _proto;
  // Implicit-transaction state (see ImplicitTxnState): one deferred transaction
  // per Sync-delimited batch, committed at Sync, rolled back inline on error.
  // Optional because it borrows the connection's TransactionContext, which only
  // exists after SetupConnection creates _conn; emplaced there.
  std::optional<ImplicitTxnState> _txn_state;
};

}  // namespace sdb::network::pg
