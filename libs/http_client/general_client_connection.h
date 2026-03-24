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

#include <stddef.h>

#include <atomic>
#include <memory>
#include <string>

#include "basics/socket-utils.h"
#include "endpoint/endpoint.h"

namespace sdb {
namespace app {

class AppServer;
class CommunicationFeaturePhase;

}  // namespace app
namespace basics {

class StringBuffer;

}  // namespace basics
namespace httpclient {

class GeneralClientConnection {
 public:
  GeneralClientConnection(app::CommunicationFeaturePhase&,
                          std::unique_ptr<Endpoint> endpoint, double, double,
                          size_t);

  GeneralClientConnection(const GeneralClientConnection&) = delete;
  GeneralClientConnection& operator=(const GeneralClientConnection&) = delete;

  virtual ~GeneralClientConnection();

  static std::unique_ptr<GeneralClientConnection> factory(
    app::CommunicationFeaturePhase& comm, std::unique_ptr<Endpoint>,
    double request_timeout, double connect_timeout, size_t num_retries,
    uint64_t ssl_protocol);

  const Endpoint* GetEndpoint() const { return _endpoint.get(); }

  std::string getEndpointSpecification() const {
    return _endpoint->specification();
  }

  bool isConnected() const { return _is_connected; }

  void resetNumConnectRetries() { _num_connect_retries = 0; }

  size_t connectRetries() const { return _connect_retries; }

  void repurpose(double connect_timeout, double request_timeout,
                 size_t connect_retries);

  bool connect();

  void disconnect();

  bool handleWrite(double, const void*, size_t, size_t*);

  bool handleRead(double, basics::StringBuffer&, bool& connection_closed);

  const auto& getErrorDetails() const noexcept { return _error_details; }

  bool isInterrupted() const noexcept {
    return _is_interrupted.load(std::memory_order_acquire);
  }

  void setInterrupted(bool value) noexcept {
    _is_interrupted.store(value, std::memory_order_release);
  }

  app::AppServer& server() const;

  app::CommunicationFeaturePhase& comm() const noexcept { return _comm; }

  bool isIdleConnection() const noexcept;

 protected:
  static constexpr int kReadbufferSize = 8192;

  virtual bool connectSocket() = 0;

  virtual void disconnectSocket() = 0;

  /// prepare connection for read/write I/O
  bool prepare(SocketWrapper socket, double timeout, bool is_write);

  /// check whether the socket is still alive
  bool checkSocket();

  virtual bool writeClientConnection(const void*, size_t, size_t*) = 0;

  virtual bool readClientConnection(basics::StringBuffer&, bool& porgress) = 0;

  virtual bool readable() = 0;

  SocketWrapper _socket;

  mutable std::string _error_details;

  std::unique_ptr<Endpoint> _endpoint;

  /// reference to communication feature phase (populated only once for
  /// the entire lifetime of the HttpClient, as the repeated feature
  /// lookup may be expensive otherwise)
  app::CommunicationFeaturePhase& _comm;

  double _request_timeout;  // in seconds

  double _connect_timeout;  // in seconds

  /// allowed number of connect retries
  size_t _connect_retries;

  /// number of connection attempts made
  size_t _num_connect_retries = 0;

  // TODO(mbkkt) is it still needed?
  bool _is_socket_non_blocking = false;

  bool _is_connected = false;

#ifdef SDB_DEV
  uint64_t _read = 0;
  uint64_t _written = 0;
#endif

  std::atomic_bool _is_interrupted = false;
};

}  // namespace httpclient
}  // namespace sdb
