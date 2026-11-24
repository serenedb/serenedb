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

#include <absl/synchronization/mutex.h>

#include <cstdint>
#include <duckdb/common/shared_ptr.hpp>
#include <duckdb/main/connection.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <yaclib/async/future.hpp>

#include "network/cancel_registry.h"
#include "network/cpu_resumer.h"
#include "network/http/quack/statement.h"
#include "pg/connection_context.h"
#include "pg/progress_registry.h"

namespace sdb::network::http::quack {

class Service;

// The SQL session behind a client's Quack session: a SereneDB session of the
// client's role, and the coroutine that runs its statements one at a time.
class Session final : public TransportBase,
                      public duckdb::enable_shared_from_this<Session> {
 public:
  Session(Service& service, std::string connection_id, std::string_view user,
          std::string_view database);
  ~Session() final;

  void Start();

  duckdb::ClientContext& Context() { return *_ctx; }

  // Runs `runner` after the statement before it, in place of any statement
  // that still waits to start. A runner that a later PREPARE, a cancel or the
  // end of the session stopped on its way is dropped.
  void Run(std::unique_ptr<StatementRunner> runner);
  void Interrupt();
  void Close();

  void Stop() final;
  void Wake() final { _task->RequestRun(); }

 private:
  yaclib::Future<> Drive();
  bool Closed();
  std::unique_ptr<StatementRunner> Next();

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
  std::unique_ptr<StatementRunner> _next ABSL_GUARDED_BY(_mu);
  bool _closed ABSL_GUARDED_BY(_mu) = false;
};

}  // namespace sdb::network::http::quack
