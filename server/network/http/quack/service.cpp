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
#include <absl/strings/str_cat.h>
#include <absl/synchronization/mutex.h>
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netinet/in.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <duckdb/common/serializer/read_stream.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <functional>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <quack_statement.hpp>
#include <span>
#include <string>
#include <string_view>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/coro.hpp>
#include <yaclib/coro/future.hpp>

#include "network/cpu_resumer.h"
#include "network/http/sql_connection.h"
#include "network/pg/login.h"
#include "pg/connection_context.h"
#include "quack_storage.hpp"
#include "scheduler/background_scheduler.h"
#include "server/utils/asio_ns.h"

namespace sdb::network::http::quack {
namespace {

bool IsLocalAddress(const asio_ns::ip::address& address) {
  if (address.is_loopback()) {
    return true;
  }
  ifaddrs* interfaces = nullptr;
  if (getifaddrs(&interfaces) != 0) {
    return false;
  }
  absl::Cleanup free_interfaces = [interfaces] { freeifaddrs(interfaces); };
  for (auto* entry = interfaces; entry; entry = entry->ifa_next) {
    if (!entry->ifa_addr) {
      continue;
    }
    if (entry->ifa_addr->sa_family == AF_INET && address.is_v4()) {
      sockaddr_in in;
      std::memcpy(&in, entry->ifa_addr, sizeof(in));
      if (asio_ns::ip::address_v4{ntohl(in.sin_addr.s_addr)} ==
          address.to_v4()) {
        return true;
      }
    } else if (entry->ifa_addr->sa_family == AF_INET6 && address.is_v6()) {
      sockaddr_in6 in6;
      std::memcpy(&in6, entry->ifa_addr, sizeof(in6));
      asio_ns::ip::address_v6::bytes_type bytes;
      std::memcpy(bytes.data(), &in6.sin6_addr, bytes.size());
      if (asio_ns::ip::address_v6{bytes} == address.to_v6()) {
        return true;
      }
    }
  }
  return false;
}

bool Serves(std::span<const asio_ns::ip::tcp::endpoint> endpoints,
            std::string_view host, uint16_t port) {
  if (host.starts_with('[') && host.ends_with(']')) {
    host = host.substr(1, host.size() - 2);
  }
  std::vector<asio_ns::ip::address> targets;
  asio_ns::error_code ec;
  if (const auto address = asio_ns::ip::make_address(host, ec); !ec) {
    targets.push_back(address);
  } else {
    asio_ns::io_context io;
    asio_ns::ip::tcp::resolver resolver{io};
    for (const auto& entry : resolver.resolve(host, absl::StrCat(port), ec)) {
      targets.push_back(entry.endpoint().address());
    }
  }
  for (const auto& endpoint : endpoints) {
    if (endpoint.port() != port) {
      continue;
    }
    const auto listener = pg::NormalizePeerAddress(endpoint.address());
    for (const auto& target : targets) {
      const auto address = pg::NormalizePeerAddress(target);
      if (listener.is_unspecified() ? IsLocalAddress(address)
                                    : listener == address) {
        return true;
      }
    }
  }
  return false;
}

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
        io)},
      _cancel_token{std::make_shared<CancelToken>()},
      _cancel_key{_cancel.Register(_cancel_token)} {
    absl::Cleanup unregister = [this] { _cancel.Unregister(_cancel_key); };
    auto sql = OpenSqlConnection(user, database, _cancel_key, &_cancel);
    _conn = std::move(sql.conn);
    _ctx = std::move(sql.context);
    std::move(unregister).Cancel();
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
  const std::shared_ptr<CancelToken> _cancel_token;
  const uint64_t _cancel_key;
  duckdb::unique_ptr<duckdb::Connection> _conn;
  std::shared_ptr<ConnectionContext> _ctx;
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
      request.AuthString(), _context.Peer(), _context.Database());
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
                 std::string server_id,
                 std::vector<asio_ns::ip::tcp::endpoint> endpoints)
  : _auth{credentials, api_keys, bearer},
    _cancel{cancel},
    _tasks{tasks},
    _core{irs::DuckDBEngine::Instance().instance(), std::move(server_id)} {
  duckdb::QuackStorageExtensionInfo::GetState(
    irs::DuckDBEngine::Instance().instance())
    .serves = [endpoints = std::move(endpoints)](const std::string& host,
                                                 uint16_t port) {
    return Serves(endpoints, host, port);
  };
}

Service::~Service() {
  duckdb::QuackStorageExtensionInfo::GetState(
    irs::DuckDBEngine::Instance().instance())
    .serves = nullptr;
}

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
  BodyReader body{request.body};
  duckdb::QuackRequest quack_request{body};
  Host host{*this, context};
  const auto& task = context.Resumer();
  const duckdb::QuackWaker waker = [task] { task->RequestRun(); };
  std::shared_ptr<asio_ns::steady_timer> answer_early;
  const auto waited_enough = std::make_shared<std::atomic<bool>>(false);
  absl::Cleanup stop_timer = [&] {
    if (answer_early) {
      answer_early->cancel();
    }
  };
  bool answers_early = true;
  SDB_IF_FAILURE("quack_prepare_answers_late") { answers_early = false; }
  while (!_core.Handle(quack_request, host, waker)) {
    if (answers_early && !answer_early &&
        quack_request.Type() == duckdb::MessageType::PREPARE_REQUEST) {
      answer_early = std::make_shared<asio_ns::steady_timer>(
        context.Io().Context(), kPrepareWait);
      answer_early->async_wait([timer = answer_early, waited_enough,
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
