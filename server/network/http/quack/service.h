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

#include <atomic>
#include <quack_server.hpp>
#include <string>
#include <vector>
#include <yaclib/algo/wait_group.hpp>
#include <yaclib/async/future.hpp>
#include <yaclib/coro/task.hpp>

#include "network/cancel_registry.h"
#include "network/credentials.h"
#include "network/http/auth.h"
#include "network/http/handler.h"
#include "server/utils/asio_ns.h"

namespace sdb::network::http::quack {

class Service final {
 public:
  Service(const CredentialProvider* credentials,
          const ApiKeyValidator* api_keys, const BearerValidator* bearer,
          CancelRegistry& cancel, yaclib::WaitGroup<>& tasks,
          std::string server_id,
          std::vector<asio_ns::ip::tcp::endpoint> endpoints);
  ~Service();

  void Start();
  void RequestStop() noexcept;

  yaclib::Task<> Handle(RequestContext& context, const HttpRequest& request,
                        HttpResponseWriter& writer);

  const HttpAuthenticator& Auth() const noexcept { return _auth; }
  CancelRegistry& Cancel() noexcept { return _cancel; }
  yaclib::WaitGroup<>& Tasks() noexcept { return _tasks; }
  duckdb::QuackServer& Core() noexcept { return _core; }
  bool Stopping() const noexcept {
    return _stopping.load(std::memory_order_acquire);
  }

 private:
  yaclib::Future<> Reap();

  HttpAuthenticator _auth;
  CancelRegistry& _cancel;
  yaclib::WaitGroup<>& _tasks;
  duckdb::QuackServer _core;
  std::atomic<bool> _stopping{false};
};

}  // namespace sdb::network::http::quack
