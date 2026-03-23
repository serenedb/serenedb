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

#pragma once

#include <map>
#include <mutex>

#include "basics/result.h"
#include "basics/thread.h"
#include "general_server/io_context.h"
#include "general_server/ssl_server_feature.h"

namespace sdb {
namespace app {

class AppServer;
}
class Endpoint;
class EndpointList;
class GeneralServerFeature;

namespace rest {

class Acceptor;
class CommTask;

class GeneralServer {
  GeneralServer(const GeneralServer&) = delete;
  const GeneralServer& operator=(const GeneralServer&) = delete;

 public:
  explicit GeneralServer(GeneralServerFeature&, uint64_t num_io_threads,
                         bool allow_early_connections);
  ~GeneralServer();

  void registerTask(std::shared_ptr<rest::CommTask>);
  void unregisterTask(rest::CommTask*);
  void startListening(EndpointList& list);  /// start accepting connections
  void stopListening();                     /// stop accepting new connections
  void stopConnections();                   /// stop connections
  void stopWorking();

  bool allowEarlyConnections() const noexcept;
  IoContext& selectIoContext();
  SslServerFeature::SslContextList sslContexts();
  SSL_CTX* getSSL_CTX(size_t index);

  SerenedServer& server() const;

  Result reloadTLS();

 protected:
  bool openEndpoint(IoContext& io_context, Endpoint* endpoint);

 private:
  GeneralServerFeature& _feature;
  std::vector<IoContext> _contexts;
  const bool _allow_early_connections;

  std::recursive_mutex _tasks_lock;
  std::vector<std::unique_ptr<Acceptor>> _acceptors;
  std::map<void*, std::shared_ptr<rest::CommTask>> _comm_tasks;

  /// protect ssl context creation
  absl::Mutex _ssl_context_mutex;
  /// global SSL context to use here
  SslServerFeature::SslContextList _ssl_contexts;
};

}  // namespace rest
}  // namespace sdb
