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

#include <absl/cleanup/cleanup.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_split.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/materialized_query_result.hpp>
#include <duckdb/main/pending_query_result.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <yaclib/async/future.hpp>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/coro.hpp>
#include <yaclib/coro/future.hpp>
#include <yaclib/coro/task.hpp>

#include "basics/asio_ns.h"
#include "basics/duckdb_engine.h"
#include "basics/exceptions.h"
#include "basics/message_buffer.h"
#include "basics/metrics.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "connector/duckdb_client_state.h"
#include "network/connection.h"
#include "network/cpu_resumer.h"
#include "network/gate.h"
#include "network/http/auth.h"
#include "network/http/h1_codec.h"
#include "network/http/response_writer.h"
#include "network/http/router.h"
#include "network/io_executor.h"
#include "network/socket.h"
#include "pg/connection_context.h"

namespace sdb::network {

struct HttpServerContext {
  HttpRouter& router;
  asio_ns::ssl::context* ssl = nullptr;
  // Same credential store as the pg endpoint (one user database for both
  // protocols); null => trust, matching the pg session.
  const CredentialProvider* credentials = nullptr;
  const http::ApiKeyValidator* api_keys = nullptr;
  const http::BearerValidator* bearer = nullptr;
  // Shared live-connection counter + the cap this listener enforces (0 =
  // unlimited). Over-cap connections get a 503 then close.
  std::atomic<uint32_t>* active = nullptr;
  uint32_t max_connections = 0;
  // CORS allow-origin list (comma-separated, or "*"); empty disables CORS.
  std::string_view cors_origins;
  // HAProxy PROXY-protocol preface policy (off / optional / require); never
  // set on an https listener (the header precedes the TLS handshake).
  ProxyMode proxy = ProxyMode::Off;
};

// The v2 session model (the pg-wire shape): RecvLoop and SendWriter are
// io-pinned byte pumps; SessionMain -- parsing, routing, HANDLERS -- runs as
// a duckdb task (TaskRunner), so handlers drive queries inline and block
// on backpressure without ever occupying an io thread. _recv is the io->duck
// byte channel (watermark-published), _send runs write-behind (committed
// bytes auto-flush at kSendFlushSize; the callback arms the writer).
template<SocketKind Kind>
class HttpSession final
  : public Transport<Kind, HttpSession<Kind>>,
    public duckdb::enable_shared_from_this<HttpSession<Kind>>,
    public http::ResponseSink,
    public RequestContext {
 public:
  using Deps = HttpServerContext;
  static constexpr SocketKind kSocketKind = Kind;

  HttpSession(HttpServerContext& ctx, IoExecutor& exec)
    requires(Kind != SocketKind::Ssl)
    : Transport<Kind, HttpSession<Kind>>{exec},
      _io{exec.Context()},
      _deadline{exec.Context()},
      _router{ctx.router},
      _auth{ctx.credentials, ctx.api_keys, ctx.bearer},
      _active{ctx.active},
      _max_conn{ctx.max_connections},
      _cors_origins{ctx.cors_origins},
      _proxy{ctx.proxy} {}

  HttpSession(HttpServerContext& ctx, IoExecutor& exec)
    requires(Kind == SocketKind::Ssl)
    : Transport<Kind, HttpSession<Kind>>{exec, *ctx.ssl},
      _io{exec.Context()},
      _deadline{exec.Context()},
      _router{ctx.router},
      _auth{ctx.credentials, ctx.api_keys, ctx.bearer},
      _active{ctx.active},
      _max_conn{ctx.max_connections},
      _cors_origins{ctx.cors_origins},
      _proxy{ctx.proxy} {}

  // Run is the session's sole owner: it grabs the one shared_from_this, starts
  // the writer + cpu futures on a raw `this`, and joins both before it returns.
  void Start() { Run().Detach(); }

  void OnStop() {}

  // --- http::ResponseSink (handler-side backpressure) ----------------------
  yaclib::Task<> Drain() override { return AwaitSendBelowHighWater(); }
  bool Broken() const noexcept override { return SendBroken(); }

  // --- RequestContext -------------------------------------------------------
  // First use sets up the full SereneDB client state (like pg-wire's
  // SetupConnection, minus the wire collector): server-side functions reach
  // ConnectionContext through GetSereneDBContext. The user is whoever
  // authenticated the request that first touched the connection.
  duckdb::Connection& Connection() override {
    if (!_conn) {
      const auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
      const auto dbname = StaticStrings::kDefaultDatabase;
      auto database = snapshot->GetDatabase(dbname);
      SDB_ENSURE(database, ERROR_INTERNAL);
      const auto database_id = database->GetId();
      std::string_view user =
        _user.empty() ? StaticStrings::kDefaultUser : _user;
      auto role = snapshot->GetRole(user);
      if (!role) {
        user = StaticStrings::kDefaultUser;
        role = snapshot->GetRole(user);
      }
      SDB_ENSURE(role, ERROR_INTERNAL);

      _conn = DuckDBEngine::Instance().CreateConnection();
      _connection_ctx = std::make_shared<ConnectionContext>(
        *_conn->context, user, role->GetId(), dbname, database_id,
        std::move(database), nullptr, nullptr, /*backend_pid=*/0,
        /*cancel_registry=*/nullptr);
      connector::SereneDBClientState::Register(*_conn->context,
                                               _connection_ctx);
      _conn->context->session_user = std::string{user};
      std::vector<duckdb::CatalogSearchEntry> default_paths{
        duckdb::CatalogSearchEntry{std::string{dbname}, "$user"},
        duckdb::CatalogSearchEntry{std::string{dbname}, "public"},
      };
      _conn->context->client_data->catalog_search_path->SetDefaultPaths(
        std::vector{default_paths});
      _conn->context->client_data->catalog_search_path->Set(
        std::move(default_paths), duckdb::CatalogSetPathType::SET_DIRECTLY);
    }
    return *_conn;
  }

  // Cooperative query drive -- the pg-wire DriveQuery pattern (see
  // PgWireSession::DriveQuery): run executor slices inline up to a budget,
  // then Yield the worker back to the scheduler; Park on NO_TASKS/BLOCKED
  // until the executor's on_reschedule wake re-runs us. A blocking
  // Connection().Query() would instead pin this scheduler worker for the
  // whole query and starve the pool under concurrent requests.
  yaclib::Task<duckdb::unique_ptr<duckdb::MaterializedQueryResult>> RunQuery(
    std::string sql, bool /*writes*/) override {
    auto& conn = Connection();
    auto& sdb_ctx = connector::GetSereneDBContext(*conn.context);
    sdb_ctx.EnsureCatalogSnapshot();

    // Connection::Query() captures execution exceptions into the result's
    // ErrorData; the manual drive must do the same (table functions
    // THROW_SQL_ERROR), preserving the typed exception so the handlers'
    // sqlstate->ES-error mapping still works.
    try {
      auto pending = conn.PendingQuery(sql, /*allow_stream_result=*/false);
      if (!pending->HasError()) {
        // In debug interleave queries more often to see more bugs.
#ifdef SDB_DEV
        static constexpr int kInlineSliceBudget = 0;
#else
        static constexpr int kInlineSliceBudget = 8;
#endif
        int inline_slices = 0;
        for (;;) {
          const auto status =
            pending->ExecuteTask([this] { _task->RequestRun(); });
          if (duckdb::PendingQueryResult::IsResultReady(status)) {
            break;
          }
          if (status == duckdb::PendingExecutionResult::RESULT_NOT_READY) {
            if (++inline_slices < kInlineSliceBudget) {
              continue;
            }
            inline_slices = 0;
            co_await _task->Yield();
          } else {
            inline_slices = 0;
            co_await _task->Park();
          }
        }
      }
      // An execution error leaves the pending result un-executable; pull the
      // error directly (it preserves the typed exception) instead of calling
      // Execute(), which would throw "unsuccessful pending result".
      if (pending->HasError()) {
        co_return duckdb::make_uniq<duckdb::MaterializedQueryResult>(
          pending->GetErrorObject());
      }
      auto result = pending->Execute();
      co_return duckdb::unique_ptr_cast<duckdb::QueryResult,
                                        duckdb::MaterializedQueryResult>(
        std::move(result));
    } catch (const std::exception& ex) {
      co_return duckdb::make_uniq<duckdb::MaterializedQueryResult>(
        duckdb::ErrorData{ex});
    }
  }

  std::string_view User() const override { return _user; }

 private:
  using Transport<Kind, HttpSession<Kind>>::_socket;
  using Transport<Kind, HttpSession<Kind>>::_ioexec;
  using Transport<Kind, HttpSession<Kind>>::_recv;
  using Transport<Kind, HttpSession<Kind>>::_send;
  using Transport<Kind, HttpSession<Kind>>::_write_gate;
  using Transport<Kind, HttpSession<Kind>>::_write_view;
  using Transport<Kind, HttpSession<Kind>>::_write_armed;
  using Transport<Kind, HttpSession<Kind>>::_send_written;
  using Transport<Kind, HttpSession<Kind>>::_send_waiter;
  using Transport<Kind, HttpSession<Kind>>::_stopping;
  using Transport<Kind, HttpSession<Kind>>::_producer_gate;
  using Transport<Kind, HttpSession<Kind>>::_task;
  using Transport<Kind, HttpSession<Kind>>::Stop;
  using Transport<Kind, HttpSession<Kind>>::KickSend;
  using Transport<Kind, HttpSession<Kind>>::HasUnsentBytes;
  using Transport<Kind, HttpSession<Kind>>::SendBroken;
  using Transport<Kind, HttpSession<Kind>>::OnSendViewReady;
  using Transport<Kind, HttpSession<Kind>>::ArmSendWaiter;
  using Transport<Kind, HttpSession<Kind>>::DisarmSendWaiter;
  using Transport<Kind, HttpSession<Kind>>::AwaitSendBelowHighWater;
  using Transport<Kind, HttpSession<Kind>>::DrainSendOnTask;
  using Transport<Kind, HttpSession<Kind>>::AwaitMoreBytes;
  using Transport<Kind, HttpSession<Kind>>::SendWriter;

  // The session owner. Starts the writer, negotiates the connection, hands off
  // to the cpu task, pumps recv, then stops everything and joins the cpu +
  // writer futures before its frame (and the session) is destroyed.
  yaclib::Task<> Run();
  // Proxy preface + TLS handshake, lifted out of Run for a single bool exit.
  yaclib::Task<bool> Negotiate();
  // duck-side: the request loop. Parse -> body -> auth -> route -> handler.
  yaclib::Future<> SessionMain();

  // Incremental llhttp feed over the recv channel. Returns the head event or
  // nullopt when the connection died mid-parse.
  yaclib::Task<std::optional<H1Event>> ReadHead();
  // Assembles the request body (chunked into _dechunk, otherwise a pinned
  // view over _recv). Returns false when the connection died / parse failed.
  yaclib::Task<bool> ReadBody(HttpRequest& request, size_t& pinned_body);

  asio_ns::io_context& _io;
  asio_ns::steady_timer _deadline;
  HttpRouter& _router;
  http::HttpAuthenticator _auth;
  std::atomic<uint32_t>* _active = nullptr;
  uint32_t _max_conn = 0;
  std::string_view _cors_origins;
  ProxyMode _proxy = ProxyMode::Off;
  H1Codec _codec;

  message::Buffer _dechunk{kReadBlock, kWireChunkMax};

  // Read-deadline phase for RecvLoop: idle (between requests, generous
  // keep-alive timeout) vs mid-request (strict header/body timeout).
  std::atomic<bool> _idle{true};
  // Lazily created on first handler use; lives (and dies) on the session
  // task like the pg session's connection.
  duckdb::unique_ptr<duckdb::Connection> _conn;
  std::shared_ptr<ConnectionContext> _connection_ctx;
  std::string _user;
};

template<SocketKind Kind>
yaclib::Task<bool> HttpSession<Kind>::Negotiate() {
  if (!co_await this->ReadProxyPreface(_proxy)) {
    co_return false;
  }
  if constexpr (Kind == SocketKind::Ssl) {
    // Header timeout for the handshake; cancelled on exit, the handler holds a
    // self so a fire racing teardown is harmless.
    absl::Cleanup deadline_guard = [this] { _deadline.cancel(); };
    _deadline.expires_after(gHttpHeaderTimeout);
    _deadline.async_wait(
      [self = this->shared_from_this()](const asio_ns::error_code& ec) {
        if (!ec) {
          self->_socket.Close();
        }
      });
    if (co_await _socket.Handshake().NoThrow()) {
      co_return false;
    }
  }
  co_return true;
}

template<SocketKind Kind>
yaclib::Task<> HttpSession<Kind>::Run() {
  auto self = this->shared_from_this();
  if (_active != nullptr) {
    _active->fetch_add(1, std::memory_order_relaxed);
  }
  metrics::Add(metrics::Gauge::HttpConnections);
  absl::Cleanup conn_guard = [this] {
    if (_active != nullptr) {
      _active->fetch_sub(1, std::memory_order_relaxed);
    }
    metrics::Sub(metrics::Gauge::HttpConnections);
  };
  // The writer drains committed bytes on a raw `this`, concurrently with the
  // recv loop below; joined before this frame ends.
  auto writer = this->SendWriter();
  yaclib::Future<> cpu;
  if (co_await Negotiate()) {
    _task = duckdb::make_shared_ptr<CpuResumer>(
      duckdb::TaskScheduler::GetScheduler(DuckDBEngine::Instance().instance()),
      *_ioexec);
    // SessionMain (eager) runs to its first Park; the bootstrap kick schedules
    // it onto a duck worker.
    cpu = SessionMain();
    _task->RequestRun();

    for (;;) {
      _deadline.expires_after(_idle.load(std::memory_order_acquire)
                                ? gHttpKeepAliveTimeout
                                : gHttpBodyTimeout);
      _deadline.async_wait([self](const asio_ns::error_code& ec) {
        if (!ec) {
          self->_socket.Close();
        }
      });
      auto [ec, n] =
        co_await _socket.ReadSome(_recv.Reserve(kReadBlock)).NoThrow();
      _deadline.cancel();
      if (ec || n == 0) {
        break;
      }
      _recv.CommitWrite(n);
      _task->RequestRun();
    }
  }
  this->Stop();
  if (cpu.Valid()) {
    co_await std::move(cpu);
  }
  co_await std::move(writer);
  co_return {};
}

template<SocketKind Kind>
yaclib::Task<std::optional<H1Event>> HttpSession<Kind>::ReadHead() {
  for (;;) {
    // Observe the readable size ONCE and use it both to decide whether to
    // parse and as the AwaitMoreBytes threshold. Re-reading ReadableSize() for
    // the park threshold would race RecvLoop: bytes committed between the parse
    // decision and the park would inflate `seen`, so the park would wait for
    // *more* than what just arrived and never reparse the buffered request.
    const size_t avail = _recv.ReadableSize();
    if (avail != 0) {
      const auto fed = _codec.ParseHead(_recv.Front());
      _recv.Consume(fed.consumed);
      if (fed.event == H1Event::Error || fed.event == H1Event::Head ||
          fed.event == H1Event::Continue) {
        co_return fed.event;
      }
      if (fed.consumed != 0) {
        continue;
      }
    }
    _idle.store(avail == 0, std::memory_order_release);
    if (!co_await AwaitMoreBytes(avail)) {
      co_return std::nullopt;
    }
    _idle.store(false, std::memory_order_release);
  }
}

template<SocketKind Kind>
yaclib::Task<bool> HttpSession<Kind>::ReadBody(HttpRequest& request,
                                               size_t& pinned_body) {
  if (_codec.IsChunked()) {
    _dechunk.Clear();
    message::Writer writer{_dechunk};
    for (;;) {
      // Single ReadableSize() observation per iteration (see ReadHead): the
      // park threshold must match what was inspected, or a body chunk that
      // arrives between the decode and the park is skipped.
      const size_t avail = _recv.ReadableSize();
      if (avail != 0) {
        const auto body = _codec.DecodeBody(_recv.Front(), writer);
        _recv.Consume(body.consumed);
        if (body.error) {
          co_return false;
        }
        if (body.done) {
          break;
        }
        if (body.consumed != 0) {
          continue;
        }
      }
      if (!co_await AwaitMoreBytes(avail)) {
        co_return false;
      }
    }
    writer.Commit(false);
    request.body = _dechunk.Written();
    co_return true;
  }
  const auto length = static_cast<size_t>(_codec.ContentLength());
  for (;;) {
    const size_t avail = _recv.ReadableSize();
    if (avail >= length) {
      break;
    }
    if (!co_await AwaitMoreBytes(avail)) {
      co_return false;
    }
  }
  request.body = _recv.ReadableView(length);
  pinned_body = length;
  co_return true;
}

template<SocketKind Kind>
yaclib::Future<> HttpSession<Kind>::SessionMain() {
  co_await _task->Park();
  absl::Cleanup finish_guard = [this] { _task->Finish(); };
  try {
    for (;;) {
      const auto event = co_await ReadHead();
      if (!event) {
        break;
      }
      _idle.store(false, std::memory_order_release);
      if (*event == H1Event::Error) {
        http::HttpResponseWriter error_writer{_send, *this,
                                              /*keep_alive=*/false,
                                              /*head_only=*/false};
        error_writer.Error(_codec.ErrorStatus(), "bad_request");
        co_await DrainSendOnTask();
        break;
      }
      if (*event == H1Event::Continue) {
        message::Writer writer{_send};
        writer.Write("HTTP/1.1 100 Continue\r\n\r\n");
        writer.Commit(true);
      }

      HttpRequest request = _codec.TakeHead();
      size_t pinned_body = 0;
      if (!co_await ReadBody(request, pinned_body)) {
        if (!SendBroken()) {
          http::HttpResponseWriter error_writer{_send, *this, false, false};
          error_writer.Error(
            _codec.ErrorStatus() != 0 ? _codec.ErrorStatus() : 400,
            "bad_request");
          co_await DrainSendOnTask();
        }
        break;
      }

      const bool keep_alive = request.keep_alive;
      const bool head_only = request.method == HttpMethod::Head;
      http::HttpResponseWriter writer{_send, *this, keep_alive, head_only};

      if (_max_conn != 0 && _active != nullptr &&
          _active->load(std::memory_order_relaxed) > _max_conn) {
        writer.Fixed(503, "application/json",
                     R"({"error":"too_many_connections"})", "");
        co_await DrainSendOnTask();
        break;
      }

      // CORS: echo an allowed Origin (credentials-safe), answer preflight here.
      std::string cors_headers;
      if (const std::string_view origin = request.Header(HttpHeader::Origin);
          !origin.empty() && !_cors_origins.empty()) {
        bool allowed = _cors_origins == "*";
        if (!allowed) {
          for (std::string_view o :
               absl::StrSplit(_cors_origins, ',', absl::SkipEmpty())) {
            if (absl::StripAsciiWhitespace(o) == origin) {
              allowed = true;
              break;
            }
          }
        }
        if (allowed) {
          cors_headers = absl::StrCat(
            "Access-Control-Allow-Origin: ", origin,
            "\r\nAccess-Control-Allow-Credentials: true\r\nVary: Origin\r\n");
          if (request.method == HttpMethod::Options) {
            writer.Fixed(
              204, "text/plain", "",
              absl::StrCat(
                cors_headers,
                "Access-Control-Allow-Methods: GET, POST, PUT, DELETE, HEAD, "
                "OPTIONS\r\nAccess-Control-Allow-Headers: Content-Type, "
                "Authorization\r\nAccess-Control-Max-Age: 86400\r\n"));
            co_await DrainSendOnTask();
            continue;
          }
          writer.SetExtraHeaders(cors_headers);
        }
      }

      auto auth = _auth.Authenticate(request.Header(HttpHeader::Authorization));
      _user = std::move(auth.context.user);
      if (auth.status != 0) {
        writer.Fixed(auth.status, "application/json",
                     R"({"error":"unauthorized"})",
                     "WWW-Authenticate: Basic realm=\"serenedb\"\r\n");
      } else if (HttpHandler* handler = _router.Match(request)) {
        try {
          co_await handler->Handle(*this, request, writer);
        } catch (const std::exception&) {
          if (!writer.HeadWritten()) {
            writer.Error(500, "internal");
          }
        }
        if (writer.HeadWritten() && !writer.Finished()) {
          // The head promised a body that never fully materialized; a clean
          // HTTP error is no longer possible -- drop the connection so the
          // client sees truncation, not a corrupt next response.
          Stop();
          break;
        }
        if (!writer.HeadWritten()) {
          writer.Error(500, "internal");
        }
        if (_connection_ctx) {
          // No NoticeResponse equivalent on this protocol (and the
          // ConnectionContext dtor asserts the queue is empty).
          _connection_ctx->ConsumeNotices([](const sdb::pg::SqlErrorData&) {});
        }
      } else {
        writer.Error(404, "not_found");
      }
      KickSend();

      if (pinned_body != 0) {
        _recv.Consume(pinned_body);
      }
      if (!keep_alive || SendBroken()) {
        break;
      }
      _codec.Reset();
      _idle.store(true, std::memory_order_release);
    }
  } catch (const std::exception&) {
  }
  co_await DrainSendOnTask();
  Stop();
  co_return {};
}

}  // namespace sdb::network
