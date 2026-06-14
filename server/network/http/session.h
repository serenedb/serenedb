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

#include "basics/asio_ns.h"
#include "basics/duckdb_engine.h"
#include "basics/exceptions.h"
#include "basics/message_buffer.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "connector/duckdb_client_state.h"
#include "network/connection.h"
#include "network/gate.h"
#include "network/http/auth.h"
#include "network/http/h1_codec.h"
#include "network/http/response_writer.h"
#include "network/http/router.h"
#include "network/io_executor.h"
#include "network/pg/task_runner.h"
#include "network/socket.h"
#include "pg/connection_context.h"

namespace sdb::network {

struct HttpServerContext {
  HttpRouter& router;
  asio_ns::ssl::context* ssl = nullptr;
  // Same credential store as the pg endpoint (one user database for both
  // protocols); null => trust, matching the pg session.
  const pg::CredentialProvider* credentials = nullptr;
  const http::ApiKeyValidator* api_keys = nullptr;
  const http::BearerValidator* bearer = nullptr;
};

// The v2 session model (the pg-wire shape): RecvLoop and SendWriter are
// io-pinned byte pumps; SessionMain -- parsing, routing, HANDLERS -- runs as
// a duckdb task (pg::TaskRunner), so handlers drive queries inline and block
// on backpressure without ever occupying an io thread. _recv is the io->duck
// byte channel (watermark-published), _send runs write-behind (committed
// bytes auto-flush at kSendFlushSize; the callback arms the writer).
template<SocketKind Kind>
class HttpSession final
  : public Transport<Kind, HttpSession<Kind>>,
    public std::enable_shared_from_this<HttpSession<Kind>>,
    public http::ResponseSink,
    public RequestContext {
 public:
  using Deps = HttpServerContext;

  HttpSession(HttpServerContext& ctx, IoExecutor& exec)
    requires(Kind != SocketKind::Ssl)
    : Transport<Kind, HttpSession<Kind>>{exec},
      _io{exec.Context()},
      _deadline{exec.Context()},
      _router{ctx.router},
      _auth{ctx.credentials, ctx.api_keys, ctx.bearer} {}

  HttpSession(HttpServerContext& ctx, IoExecutor& exec)
    requires(Kind == SocketKind::Ssl)
    : Transport<Kind, HttpSession<Kind>>{exec, *ctx.ssl},
      _io{exec.Context()},
      _deadline{exec.Context()},
      _router{ctx.router},
      _auth{ctx.credentials, ctx.api_keys, ctx.bearer} {}

  void Start() {
    Run().Detach();
    this->SendWriter().Detach();
  }

  void OnSendPoison() {}

  // --- http::ResponseSink (handler-side backpressure) ----------------------
  yaclib::Future<> Drain() override { return AwaitSendBelowHighWater(); }
  bool Broken() const noexcept override { return SendBroken(); }

  // --- RequestContext -------------------------------------------------------
  // First use sets up the full SereneDB client state (like pg-wire's
  // SetupConnection, minus the wire collector): server-side functions reach
  // ConnectionContext through GetSereneDBContext. The user is whoever
  // authenticated the request that first touched the connection.
  duckdb::Connection& Connection() override {
    if (!_conn) {
      const auto snapshot =
        catalog::CatalogFeature::instance().Global().GetCatalogSnapshot();
      const auto dbname = StaticStrings::kDefaultDatabase;
      auto database = snapshot->GetDatabase(dbname);
      SDB_ENSURE(database, ERROR_INTERNAL);
      const auto database_id = database->GetId();
      const std::string_view user =
        _user.empty() ? StaticStrings::kDefaultUser : _user;

      _conn = DuckDBEngine::Instance().CreateConnection();
      _connection_ctx = std::make_shared<ConnectionContext>(
        *_conn->context, user, dbname, database_id, std::move(database),
        nullptr, nullptr);
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
  yaclib::Future<duckdb::unique_ptr<duckdb::MaterializedQueryResult>> RunQuery(
    std::string sql, bool writes) override {
    auto& conn = Connection();
    auto& sdb_ctx = connector::GetSereneDBContext(*conn.context);
    sdb_ctx.EnsureCatalogSnapshot();
    if (writes) {
      sdb_ctx.EnsureRocksDBTransaction();
    }
    sdb_ctx.EnsureRocksDBSnapshot();

    // Connection::Query() captures execution exceptions into the result's
    // ErrorData; the manual drive must do the same (table functions
    // THROW_SQL_ERROR), preserving the typed exception so the handlers'
    // sqlstate->ES-error mapping still works.
    try {
      auto pending = conn.PendingQuery(sql, /*allow_stream_result=*/false);
      if (!pending->HasError()) {
        static constexpr int kInlineSliceBudget = 8;
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
  using Transport<Kind, HttpSession<Kind>>::_io_broken;
  using Transport<Kind, HttpSession<Kind>>::_writer_stop;
  using Transport<Kind, HttpSession<Kind>>::_producer_gate;
  using Transport<Kind, HttpSession<Kind>>::_task;
  using Transport<Kind, HttpSession<Kind>>::_task_spawned;
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

  // io-pinned: TLS handshake, then the byte pump feeding _recv; arms the
  // per-read deadline (keep-alive idle between requests, body timeout
  // within one -- SessionMain publishes the phase).
  yaclib::Future<> Run();
  // duck-side: the request loop. Parse -> body -> auth -> route -> handler.
  yaclib::Future<> SessionMain();

  // Incremental llhttp feed over the recv channel. Returns the head event or
  // nullopt when the connection died mid-parse.
  yaclib::Future<std::optional<H1Event>> ReadHead();
  // Assembles the request body (chunked into _dechunk, otherwise a pinned
  // view over _recv). Returns false when the connection died / parse failed.
  yaclib::Future<bool> ReadBody(HttpRequest& request, size_t& pinned_body);

  asio_ns::io_context& _io;
  asio_ns::steady_timer _deadline;
  HttpRouter& _router;
  http::HttpAuthenticator _auth;
  H1Codec _codec;

  message::Buffer _dechunk{kReadBlock, kBufferMaxGrowth};

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
yaclib::Future<> HttpSession<Kind>::Run() {
  auto self = this->shared_from_this();
  absl::Cleanup writer_guard{[this] {
    _writer_stop = true;
    _write_gate.Kick();
  }};
  if constexpr (Kind == SocketKind::Ssl) {
    _deadline.expires_after(kHttpHeaderReadTimeout);
    _deadline.async_wait([self](const asio_ns::error_code& ec) {
      if (!ec) {
        self->_socket.Close();
      }
    });
    if (co_await _socket.Handshake().NoThrow()) {
      _socket.Close();
      co_return {};
    }
    _deadline.cancel();
  }

  _task.emplace(
    duckdb::TaskScheduler::GetScheduler(DuckDBEngine::Instance().instance()),
    *_ioexec);
  SessionMain().Detach();
  _task_spawned = true;

  for (;;) {
    _deadline.expires_after(_idle.load(std::memory_order_acquire)
                              ? kHttpKeepAliveIdleTimeout
                              : kHttpBodyReadTimeout);
    _deadline.async_wait([self](const asio_ns::error_code& ec) {
      if (!ec) {
        self->_socket.Close();
      }
    });
    auto [ec, n] = co_await _socket.ReadSome(_recv.Reserve(kReadBlock)).NoThrow();
    _deadline.cancel();
    if (ec || n == 0) {
      break;
    }
    _recv.CommitWrite(n);
    _task->RequestRun();
  }
  _io_broken.store(true, std::memory_order_release);
  if (_task_spawned) {
    _task->RequestRun();
  }
  _socket.Close();
  co_return {};
}

template<SocketKind Kind>
yaclib::Future<std::optional<H1Event>> HttpSession<Kind>::ReadHead() {
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
yaclib::Future<bool> HttpSession<Kind>::ReadBody(HttpRequest& request,
                                                 size_t& pinned_body) {
  if (_codec.IsChunked()) {
    _dechunk.Clear();
    for (;;) {
      // Single ReadableSize() observation per iteration (see ReadHead): the
      // park threshold must match what was inspected, or a body chunk that
      // arrives between the decode and the park is skipped.
      const size_t avail = _recv.ReadableSize();
      if (avail != 0) {
        const auto body = _codec.DecodeBody(_recv.Front(), _dechunk);
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
  auto self = this->shared_from_this();
  co_await _task->Begin();
  absl::Cleanup finish_guard{[this] { _task->Finish(); }};
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
        _send.Write("HTTP/1.1 100 Continue\r\n\r\n", true);
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
          _io_broken.store(true, std::memory_order_release);
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
  asio_ns::post(_io, [self] { self->_socket.Close(); });
  co_return {};
}

}  // namespace sdb::network
