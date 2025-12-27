////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "general_server.h"

#include <algorithm>
#include <chrono>
#include <thread>

#include "app/app_server.h"
#include "basics/application-exit.h"
#include "basics/exitcodes.h"
#include "basics/logger/logger.h"
#include "endpoint/endpoint.h"
#include "endpoint/endpoint_list.h"
#include "general_server/acceptor.h"
#include "general_server/comm_task.h"
#include "general_server/general_server_feature.h"
#include "general_server/scheduler.h"
#include "general_server/scheduler_feature.h"

using namespace sdb;
using namespace sdb::basics;
using namespace sdb::rest;

namespace {
int ClientHelloCallback(SSL* ssl, int* al, void* arg) { return 1; }
}  // namespace

GeneralServer::GeneralServer(GeneralServerFeature& feature,
                             uint64_t num_io_threads,
                             bool allow_early_connections)
  : _feature(feature), _allow_early_connections(allow_early_connections) {
  auto& server = feature.server();

  _contexts.reserve(num_io_threads);
  for (size_t i = 0; i < num_io_threads; ++i) {
    _contexts.emplace_back(server);
  }
}

GeneralServer::~GeneralServer() = default;

bool GeneralServer::allowEarlyConnections() const noexcept {
  return _allow_early_connections;
}

void GeneralServer::registerTask(std::shared_ptr<CommTask> task) {
  if (_feature.server().isStopping()) {
    SDB_THROW(ERROR_SHUTTING_DOWN);
  }
  auto* t = task.get();
  {
    std::lock_guard guard(_tasks_lock);
    _comm_tasks.try_emplace(t, std::move(task));
  }
  t->Start();
}

void GeneralServer::unregisterTask(CommTask* task) {
  std::shared_ptr<CommTask> old;
  {
    std::lock_guard guard(_tasks_lock);
    auto it = _comm_tasks.find(task);
    if (it != _comm_tasks.end()) {
      old = std::move(it->second);
      _comm_tasks.erase(it);
    }
  }
  old.reset();
}

void GeneralServer::startListening(EndpointList& list) {
  unsigned int i = 0;

  list.apply([&i, this](const std::string& specification, Endpoint& ep) {
    // distribute endpoints across all io contexts
    IoContext& io_context = _contexts[i++ % _contexts.size()];
    bool ok = openEndpoint(io_context, &ep);

    if (ok) {
      SDB_DEBUG("xxxxx", sdb::Logger::FIXME, "bound to endpoint '",
                specification, "'");
    } else {
      SDB_FATAL_EXIT_CODE(
        "xxxxx", sdb::Logger::FIXME, EXIT_COULD_NOT_BIND_PORT,
        "failed to bind to endpoint '", specification,
        "'. Please check whether another instance is already "
        "running using this endpoint and review your endpoints "
        "configuration.");
    }
  });

  // print out messages to which endpoints the server is bound to
  list.dump();
}

/// stop accepting new connections
void GeneralServer::stopListening() {
  for (std::unique_ptr<Acceptor>& acceptor : _acceptors) {
    acceptor->close();
  }
}

/// stop connections
void GeneralServer::stopConnections() {
  // close connections of all socket tasks so the tasks will
  // eventually shut themselves down
  std::unique_lock guard(_tasks_lock);
  auto comm_task = _comm_tasks;
  guard.unlock();
  for (const auto& pair : comm_task) {
    pair.second->Stop();
  }
}

void GeneralServer::stopWorking() {
  const auto started = std::chrono::steady_clock::now();
  constexpr auto kTimeout = std::chrono::seconds(5);
  do {
    {
      std::unique_lock guard(_tasks_lock);
      if (_comm_tasks.empty())
        break;
    }
    std::this_thread::yield();
  } while ((std::chrono::steady_clock::now() - started) < kTimeout);
  {
    std::lock_guard guard(_tasks_lock);
    _comm_tasks.clear();
  }
  // need to stop IoThreads before cleaning up the acceptors
  for (auto& ctx : _contexts) {
    ctx.stop();
  }
  _acceptors.clear();
  _contexts.clear();
}

bool GeneralServer::openEndpoint(IoContext& io_context, Endpoint* endpoint) {
  auto acceptor = rest::Acceptor::factory(*this, io_context, endpoint);
  try {
    acceptor->open();
  } catch (...) {
    return false;
  }
  _acceptors.emplace_back(std::move(acceptor));
  return true;
}

IoContext& GeneralServer::selectIoContext() {
  return *absl::c_min_element(_contexts, [](const auto& a, const auto& b) {
    return a.clients() < b.clients();
  });
}

SslServerFeature::SslContextList GeneralServer::sslContexts() {
  std::lock_guard guard(_ssl_context_mutex);
  if (!_ssl_contexts) {
    _ssl_contexts = server().getFeature<SslServerFeature>().createSslContexts();
    if (_ssl_contexts->size() > 0) {
      // Set a client hello callback such that we have a chance to change the
      // SSL context:
      SSL_CTX_set_client_hello_cb((*_ssl_contexts)[0].native_handle(),
                                  &ClientHelloCallback, this);
    }
  }
  return _ssl_contexts;
}

SSL_CTX* GeneralServer::getSSL_CTX(size_t index) {
  std::lock_guard guard(_ssl_context_mutex);
  return (*_ssl_contexts)[index].native_handle();
}

Result GeneralServer::reloadTLS() {
  try {
    {
      std::lock_guard guard(_ssl_context_mutex);
      _ssl_contexts =
        server().getFeature<SslServerFeature>().createSslContexts();
      if (_ssl_contexts->size() > 0) {
        // Set a client hello callback such that we have a chance to change the
        // SSL context:
        SSL_CTX_set_client_hello_cb((*_ssl_contexts)[0].native_handle(),
                                    &ClientHelloCallback, (void*)this);
      }
    }
    // Now cancel every acceptor once, such that a new AsioSocket is generated
    // which will use the new context. Otherwise, the first connection will
    // still use the old certs:
    for (auto& a : _acceptors) {
      a->cancel();
    }
    return {};
  } catch (std::exception& e) {
    SDB_ERROR(
      "xxxxx", Logger::SSL,
      "Could not reload TLS context from files, got exception with this "
      "error: ",
      e.what());
    return Result(ERROR_CANNOT_READ_FILE,
                  "Could not reload TLS context from files.");
  }
}

SerenedServer& GeneralServer::server() const { return _feature.server(); }
