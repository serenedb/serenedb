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

#include "network/http/quack/service.h"

#include <absl/cleanup/cleanup.h>
#include <absl/synchronization/mutex.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/serializer/read_stream.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/coro.hpp>
#include <yaclib/coro/future.hpp>

#include "network/cpu_resumer.h"
#include "pg/connection_context.h"
#include "quack_statement.hpp"
#include "scheduler/background_scheduler.h"
#include "server/utils/asio_ns.h"

namespace sdb::network::http::quack {
namespace {

class BodyReader final : public duckdb::ReadStream {
 public:
  explicit BodyReader(const message::SequenceView& body)
    : _next{body.begin()}, _end{body.end()} {}

  void ReadData(duckdb::data_ptr_t buffer, duckdb::idx_t read_size) final {
    while (read_size != 0) {
      if (_left.empty()) {
        if (_next == _end) {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                          ERR_MSG("the Quack request ends early"));
        }
        const auto chunk = *_next;
        ++_next;
        _left = {static_cast<const uint8_t*>(chunk.data()), chunk.size()};
        continue;
      }
      const auto n = std::min<size_t>(read_size, _left.size());
      std::memcpy(buffer, _left.data(), n);
      _left = _left.subspan(n);
      buffer += n;
      read_size -= n;
    }
  }

  void ReadData(duckdb::QueryContext, duckdb::data_ptr_t buffer,
                duckdb::idx_t read_size) final {
    ReadData(buffer, read_size);
  }

 private:
  message::SequenceView::ConstIterator _next;
  message::SequenceView::ConstIterator _end;
  std::span<const uint8_t> _left;
};

constexpr std::string_view kContentType = "application/vnd.duckdb";
constexpr size_t kWriteSlice = size_t{1} << 20;
constexpr auto kSweepInterval = std::chrono::seconds{1};
constexpr auto kPrepareWait = std::chrono::milliseconds{100};

class Session final : public TransportBase,
                      public duckdb::QuackHostSession,
                      public duckdb::enable_shared_from_this<Session> {
 public:
  explicit Session(Service& service, std::string connection_id,
                   std::string_view user, std::string_view database)
    : _service{service},
      _cancel{service.Cancels()},
      _connection_id{std::move(connection_id)},
      _task{CpuResumer::Make()},
      _cancel_token{std::make_shared<CancelToken>()},
      _cancel_key{_cancel.Register(_cancel_token)} {
    absl::Cleanup unregister = [this] { _cancel.Unregister(_cancel_key); };
    SetupConnection(user, database);
    std::move(unregister).Cancel();
  }

  ~Session() final {
    _cancel_token->Detach();
    _cancel.Unregister(_cancel_key);
    _ctx->DiscardNotices();
  }

  void Start() {
    {
      absl::MutexLock lock{&_cancel_token->mu};
      _cancel_token->session = shared_from_this();
    }
    _service.Tasks().Consume(Drive());
    _task->Start();
    if (_service.Stopping()) {
      Close();
    }
  }

  duckdb::ClientContext& Context() final { return *_ctx; }

  void Run(duckdb::unique_ptr<duckdb::QuackStatementRunner> runner) final {
    SDB_PARK_ONCE_ON_FAILURE("quack_statement_handed_over_late");
    {
      absl::MutexLock lock{&_mu};
      if (!_closed && !runner->Stopped()) {
        _next.swap(runner);
      }
    }
    _task->RequestRun();
  }

  void Interrupt() final {
    _ctx->Interrupt();
    _task->RequestRun();
  }

  void Close() final {
    duckdb::unique_ptr<duckdb::QuackStatementRunner> next;
    {
      absl::MutexLock lock{&_mu};
      _closed = true;
      next.swap(_next);
    }
    Interrupt();
  }

  void Stop() final {
    _service.Core().Disconnect(_connection_id);
    Close();
  }

  void Wake() final { _task->RequestRun(); }

 private:
  void SetupConnection(std::string_view user, std::string_view database) {
    _conn =
      OpenClientSession(user, database, static_cast<int32_t>(_cancel_key >> 32),
                        &_cancel, _task->Account());
    _ctx = &ConnectionContext::Get(*_conn->context);
    _ctx->lists_system_schemas = false;
    _progress = sdb::pg::ProgressRegistration{_ctx->ProgressSource()};
    _cancel_token->Attach(*_ctx, _ctx->GetLoginRoleId());
  }

  yaclib::Future<> Drive() {
    auto self = shared_from_this();
    co_await _task->Park();
    absl::Cleanup finish_guard = [this] { _task->Finish(); };
    const duckdb::QuackWaker wake = [task = _task] { task->RequestRun(); };
    while (!Closed()) {
      auto runner = Next();
      if (!runner) {
        co_await _task->Park();
        continue;
      }
      for (;;) {
        const auto state = runner->Step(wake);
        if (state == duckdb::QuackRunState::FINISHED) {
          break;
        }
        if (state == duckdb::QuackRunState::BLOCKED) {
          co_await _task->Park();
        } else if (duckdb::TaskScheduler::ShouldYield()) {
          co_await _task->Yield();
        }
      }
      runner.reset();
      _ctx->DiscardNotices();
    }
    co_return {};
  }

  bool Closed() {
    absl::MutexLock lock{&_mu};
    return _closed;
  }

  duckdb::unique_ptr<duckdb::QuackStatementRunner> Next() {
    absl::MutexLock lock{&_mu};
    return std::move(_next);
  }

  Service& _service;
  CancelRegistry& _cancel;
  const std::string _connection_id;
  const duckdb::shared_ptr<CpuResumer> _task;
  const std::shared_ptr<CancelToken> _cancel_token;
  const uint64_t _cancel_key;
  duckdb::unique_ptr<duckdb::Connection> _conn;
  ConnectionContext* _ctx = nullptr;
  sdb::pg::ProgressRegistration _progress;
  absl::Mutex _mu;
  duckdb::unique_ptr<duckdb::QuackStatementRunner> _next ABSL_GUARDED_BY(_mu);
  bool _closed ABSL_GUARDED_BY(_mu) = false;
};

class Host final : public duckdb::QuackServerHost {
 public:
  explicit Host(Service& service, RequestContext& context)
    : _service{service}, _context{context} {}

  duckdb::shared_ptr<duckdb::QuackHostSession> OpenSession(
    const std::string& connection_id,
    const duckdb::ConnectionRequestMessage& request) final {
    if (_service.Stopping()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_ADMIN_SHUTDOWN),
                      ERR_MSG("the database system is shutting down"));
    }
    auto auth = _context.Authenticate(request.AuthString());
    if (auth.status != HttpStatus::None) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PASSWORD),
                      ERR_MSG("authentication failed"));
    }
    auto session = duckdb::make_shared_ptr<Session>(
      _service, connection_id, auth.context.user, _context.Database());
    session->Start();
    return session;
  }

 private:
  Service& _service;
  RequestContext& _context;
};

}  // namespace

Service::Service(CancelRegistry& cancel, yaclib::WaitGroup<>& tasks,
                 std::string server_id)
  : _cancel{cancel},
    _tasks{tasks},
    _core{irs::DuckDBEngine::Instance().instance(), std::move(server_id)} {
  _tasks.Consume(SweepLoop());
}

void Service::RequestStop() {
  _stopping.store(true, std::memory_order_release);
  _core.DisconnectAll();
}

yaclib::Future<> Service::SweepLoop() {
  auto& scheduler = BackgroundScheduler::instance();
  while (!Stopping() && !scheduler.IsStopping()) {
    co_await scheduler.Delay(kSweepInterval);
    if (Stopping() || scheduler.IsStopping()) {
      break;
    }
    SDB_IF_FAILURE("quack_sweep_paused") { continue; }
    _core.Sweep();
  }
  co_return {};
}

yaclib::Task<> Service::Handle(RequestContext& context,
                               const HttpRequest& request,
                               HttpResponseWriter& writer) {
  BodyReader body{request.body};
  duckdb::QuackRequest quack_request{
    body, std::string{request.Header(duckdb::QUACK_SERENEDB_HEADER)}};
  Host host{*this, context};
  const auto& task = context.Resumer();
  const duckdb::QuackWaker waker = [task] { task->RequestRun(); };
  std::shared_ptr<asio_ns::steady_timer> prepare_timer;
  const auto waited_enough = std::make_shared<std::atomic<bool>>(false);
  absl::Cleanup stop_timer = [&] {
    if (prepare_timer) {
      prepare_timer->cancel();
    }
  };
  bool prepare_timer_enabled = true;
  SDB_IF_FAILURE("quack_prepare_answers_late") {
    prepare_timer_enabled = false;
  }
  while (!_core.Handle(quack_request, host, waker)) {
    if (prepare_timer_enabled && !prepare_timer &&
        quack_request.Type() == duckdb::MessageType::PREPARE_REQUEST) {
      prepare_timer = std::make_shared<asio_ns::steady_timer>(
        context.Io().Context(), kPrepareWait);
      prepare_timer->async_wait([timer = prepare_timer, waited_enough,
                                 task](const asio_ns::error_code& ec) {
        if (ec) {
          return;
        }
        waited_enough->store(true, std::memory_order_release);
        task->RequestRun();
      });
    }
    co_await task->Park();
    if (writer.Broken()) {
      co_return {};
    }
    if (waited_enough->load(std::memory_order_acquire)) {
      quack_request.AnswerEarly();
    }
  }
  const auto* data =
    reinterpret_cast<const char*>(quack_request.ResponseData());
  const size_t size = quack_request.ResponseSize();
  writer.WriteHead(HttpStatus::Ok, kContentType, size);
  for (size_t offset = 0; offset < size; offset += kWriteSlice) {
    writer.Write({data + offset, std::min(kWriteSlice, size - offset)});
    co_await writer.Drain();
    if (writer.Broken()) {
      co_return {};
    }
  }
  writer.Finish();
  co_return {};
}

}  // namespace sdb::network::http::quack
