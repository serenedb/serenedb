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

#include "network/http/quack/session.h"

#include <absl/cleanup/cleanup.h>

#include <duckdb/common/fault_points.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/utils/duckdb_engine.hpp>

#include "network/http/quack/service.h"
#include "quack_session_state.hpp"

namespace sdb::network::http::quack {

Session::Session(Service& service, std::string connection_id,
                 std::string_view user, std::string_view database)
  : _service{service},
    _cancel{service.Cancels()},
    _connection_id{std::move(connection_id)},
    _task{CpuResumer::Make()},
    _cancel_token{std::make_shared<CancelToken>()},
    _cancel_key{_cancel.Register(_cancel_token)} {
  absl::Cleanup unregister = [this] { _cancel.Unregister(_cancel_key); };
  _conn =
    OpenClientSession(user, database, static_cast<int32_t>(_cancel_key >> 32),
                      &_cancel, _task->Account());
  _ctx = &ConnectionContext::Get(*_conn->context);
  _ctx->lists_system_schemas = false;
  _ctx->registered_state->Insert(
    duckdb::QuackSessionState::KEY,
    duckdb::make_shared_ptr<duckdb::QuackSessionState>(_connection_id));
  _progress = sdb::pg::ProgressRegistration{_ctx->ProgressSource()};
  _cancel_token->Attach(*_ctx, _ctx->GetLoginRoleId());
  std::move(unregister).Cancel();
}

Session::~Session() {
  _cancel_token->Detach();
  _cancel.Unregister(_cancel_key);
  _ctx->DiscardNotices();
}

void Session::Start() {
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

void Session::Run(std::unique_ptr<StatementRunner> runner) {
  SDB_PARK_ONCE_ON_FAILURE("quack_statement_handed_over_late");
  {
    absl::MutexLock lock{&_mu};
    if (!_closed && !runner->Stopped()) {
      _next.swap(runner);
    }
  }
  _task->RequestRun();
}

void Session::Interrupt() {
  _ctx->Interrupt();
  _task->RequestRun();
}

void Session::Close() {
  std::unique_ptr<StatementRunner> next;
  {
    absl::MutexLock lock{&_mu};
    _closed = true;
    next.swap(_next);
  }
  Interrupt();
}

void Session::Stop() {
  _service.Disconnect(_connection_id);
  Close();
}

yaclib::Future<> Session::Drive() {
  auto self = shared_from_this();
  co_await _task->Park();
  absl::Cleanup finish_guard = [this] { _task->Finish(); };
  const std::function<void()> wake = [task = _task] { task->RequestRun(); };
  while (!Closed()) {
    auto runner = Next();
    if (!runner) {
      co_await _task->Park();
      continue;
    }
    for (;;) {
      const auto state = runner->Step(wake);
      if (state == StepResult::Finished) {
        break;
      }
      if (state == StepResult::Blocked) {
        co_await _task->Park();
      } else if (duckdb::TaskScheduler::ShouldYield()) {
        co_await _task->Yield();
      }
    }
    runner.reset();
    _ctx->DiscardNotices();
    _ctx->ReportIdle();
  }
  co_return {};
}

bool Session::Closed() {
  absl::MutexLock lock{&_mu};
  return _closed;
}

std::unique_ptr<StatementRunner> Session::Next() {
  absl::MutexLock lock{&_mu};
  return std::move(_next);
}

}  // namespace sdb::network::http::quack
