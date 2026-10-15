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
#include <deque>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <functional>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <memory>
#include <quack_statement.hpp>
#include <string>
#include <string_view>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/coro.hpp>
#include <yaclib/coro/future.hpp>

#include "catalog/cluster.h"
#include "connector/duckdb_client_state.h"
#include "network/cpu_resumer.h"
#include "pg/connection_context.h"
#include "scheduler/background_scheduler.h"
#include "server/utils/asio_ns.h"

namespace sdb::network::http::quack {
namespace {

constexpr std::string_view kContentType = "application/vnd.duckdb";
constexpr size_t kWriteSlice = size_t{1} << 20;
constexpr auto kReapInterval = std::chrono::seconds{1};
constexpr auto kPrepareWait = std::chrono::milliseconds{100};
#ifdef SDB_DEV
constexpr int kInlineSliceBudget = 0;
#else
constexpr int kInlineSliceBudget = 8;
#endif

class Session final : public TransportBase,
                      public duckdb::enable_shared_from_this<Session> {
 public:
  Session(Service& service, IoExecutor& io, std::string connection_id,
          std::string_view user, std::string_view database)
    : _service{service},
      _cancel{service.Cancel()},
      _connection_id{std::move(connection_id)},
      _task{duckdb::make_shared_ptr<CpuResumer>(
        duckdb::TaskScheduler::GetScheduler(
          irs::DuckDBEngine::Instance().instance()),
        io)} {
    const std::string_view dbname =
      database.empty() ? irs::StaticStrings::kDefaultDatabase : database;
    duckdb::idx_t database_id = 0;
    duckdb::Permissions permissions;
    if (!catalog::ReadDatabase(dbname,
                               [&](const catalog::DatabaseCatalogEntry& entry) {
                                 database_id = entry.oid;
                                 permissions = entry.permissions;
                               })) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_CATALOG_NAME),
                      ERR_MSG("database \"", dbname, "\" does not exist"));
    }
    const std::string_view login_user =
      user.empty() ? irs::StaticStrings::kDefaultUser : user;
    auto login = sdb::pg::RequireLoginRole(login_user, dbname, permissions);
    if (!login.role) {
      THROW_SQL_ERROR_FROM_DATA(std::move(login.error));
    }
    _conn = irs::DuckDBEngine::Instance().CreateConnection();
    _cancel_token = std::make_shared<CancelToken>();
    _cancel_key = _cancel.Register(_cancel_token);
    _ctx = std::make_shared<ConnectionContext>(
      *_conn->context, login_user, login.role, dbname, database_id, nullptr,
      static_cast<int32_t>(_cancel_key >> 32), &_cancel);
    connector::SereneDBClientState::Register(*_conn->context, _ctx);
    connector::SetDefaultSearchPath(*_conn->context, dbname);
  }

  ~Session() final {
    _cancel_token->Detach();
    _cancel.Unregister(_cancel_key);
    _ctx->ConsumeNotices([](const irs::pg::SqlErrorData&) {});
  }

  void Start() {
    {
      absl::MutexLock lock{&_cancel_token->mu};
      _cancel_token->ctx = _conn->context.get();
      _cancel_token->session = shared_from_this();
    }
    _service.Tasks().Consume(Drive());
    _task->Start();
    if (_service.Stopping()) {
      Close();
    }
  }

  duckdb::ClientContext& Context() { return *_conn->context; }

  void Run(duckdb::unique_ptr<duckdb::QuackStatementRunner> runner) {
    {
      absl::MutexLock lock{&_mu};
      if (!_closed) {
        _queue.push_back(std::move(runner));
      }
    }
    _task->RequestRun();
  }

  void Interrupt() {
    _conn->context->Interrupt();
    _task->RequestRun();
  }

  void Close() {
    {
      absl::MutexLock lock{&_mu};
      _closed = true;
    }
    Interrupt();
  }

  void Stop() final {
    _service.Core().Disconnect(_connection_id);
    Close();
  }

  void Wake() final { _task->RequestRun(); }

 private:
  yaclib::Future<> Drive() {
    auto self = shared_from_this();
    co_await _task->Park();
    absl::Cleanup finish_guard = [this] { _task->Finish(); };
    const std::function<void()> wake = [task = _task] { task->RequestRun(); };
    for (;;) {
      duckdb::unique_ptr<duckdb::QuackStatementRunner> runner;
      bool closed = false;
      {
        absl::MutexLock lock{&_mu};
        closed = _closed;
        if (!_queue.empty()) {
          runner = std::move(_queue.front());
          _queue.pop_front();
        }
      }
      if (!runner) {
        if (closed) {
          break;
        }
        co_await _task->Park();
        continue;
      }
      if (closed) {
        continue;
      }
      int inline_slices = 0;
      for (;;) {
        const auto state = runner->Step(wake);
        if (state == duckdb::QuackRunState::FINISHED) {
          break;
        }
        if (state == duckdb::QuackRunState::NOT_READY) {
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
      runner.reset();
      _ctx->ConsumeNotices([](const irs::pg::SqlErrorData&) {});
    }
    co_return {};
  }

  Service& _service;
  CancelRegistry& _cancel;
  const std::string _connection_id;
  const duckdb::shared_ptr<CpuResumer> _task;
  duckdb::unique_ptr<duckdb::Connection> _conn;
  std::shared_ptr<ConnectionContext> _ctx;
  std::shared_ptr<CancelToken> _cancel_token;
  uint64_t _cancel_key = 0;
  absl::Mutex _mu;
  std::deque<duckdb::unique_ptr<duckdb::QuackStatementRunner>> _queue
    ABSL_GUARDED_BY(_mu);
  bool _closed ABSL_GUARDED_BY(_mu) = false;
};

class SessionHandle final : public duckdb::QuackHostSession {
 public:
  explicit SessionHandle(duckdb::shared_ptr<Session> session)
    : _session{std::move(session)} {}

  ~SessionHandle() final { _session->Close(); }

  duckdb::ClientContext& Context() final { return _session->Context(); }

  void Run(duckdb::unique_ptr<duckdb::QuackStatementRunner> runner) final {
    _session->Run(std::move(runner));
  }

  void Interrupt() final { _session->Interrupt(); }

 private:
  duckdb::shared_ptr<Session> _session;
};

class Host final : public duckdb::QuackServerHost {
 public:
  Host(Service& service, RequestContext& context)
    : _service{service}, _context{context} {}

  duckdb::unique_ptr<duckdb::QuackHostSession> OpenSession(
    const std::string& connection_id,
    const duckdb::ConnectionRequestMessage& request) final {
    if (_service.Stopping()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_ADMIN_SHUTDOWN),
                      ERR_MSG("the server is shutting down"));
    }
    auto auth = _service.Auth().AuthenticateCredential(
      request.AuthString(), _context.PeerIsLoopback());
    if (auth.status != HttpStatus::None) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PASSWORD),
                      ERR_MSG("authentication failed"));
    }
    auto session =
      duckdb::make_shared_ptr<Session>(_service, _context.Io(), connection_id,
                                       auth.context.user, _context.Database());
    session->Start();
    return duckdb::make_uniq<SessionHandle>(std::move(session));
  }

 private:
  Service& _service;
  RequestContext& _context;
};

}  // namespace

Service::Service(const CredentialProvider* credentials,
                 const ApiKeyValidator* api_keys, const BearerValidator* bearer,
                 CancelRegistry& cancel, yaclib::WaitGroup<>& tasks,
                 std::string server_id)
  : _auth{credentials, api_keys, bearer},
    _cancel{cancel},
    _tasks{tasks},
    _core{irs::DuckDBEngine::Instance().instance(), std::move(server_id)} {}

void Service::Start() { _tasks.Consume(Reap()); }

void Service::RequestStop() noexcept {
  _stopping.store(true, std::memory_order_release);
  _core.DisconnectAll();
}

yaclib::Future<> Service::Reap() {
  auto& scheduler = BackgroundScheduler::instance();
  while (!Stopping() && !scheduler.IsStopping()) {
    co_await scheduler.Delay(kReapInterval);
    if (Stopping() || scheduler.IsStopping()) {
      break;
    }
    _core.Sweep();
  }
  co_return {};
}

yaclib::Task<> Service::Handle(RequestContext& context,
                               const HttpRequest& request,
                               HttpResponseWriter& writer) {
  duckdb::MemoryStream body;
  for (const auto buffer : request.body) {
    body.WriteData(static_cast<duckdb::const_data_ptr_t>(buffer.data()),
                   buffer.size());
  }
  duckdb::QuackRequest quack_request{body};
  Host host{*this, context};
  const auto& task = context.Resumer();
  const duckdb::QuackWaker waker = [task] { task->RequestRun(); };
  std::shared_ptr<std::atomic<bool>> waited_enough;
  while (!_core.Handle(quack_request, host, waker)) {
    if (!waited_enough) {
      waited_enough = std::make_shared<std::atomic<bool>>(false);
      auto timer = std::make_shared<asio_ns::steady_timer>(
        context.Io().Context(), kPrepareWait);
      timer->async_wait(
        [timer, waited_enough, task](const asio_ns::error_code&) {
          waited_enough->store(true, std::memory_order_release);
          task->RequestRun();
        });
    }
    co_await task->Park();
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
