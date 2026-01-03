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

#include "general_server/acceptor_unix_domain.h"

#include "basics/exceptions.h"
#include "basics/file_utils.h"
#include "basics/logger/logger.h"
#include "endpoint/connection_info.h"
#include "endpoint/endpoint_unix_domain.h"
#include "general_server/general_server.h"
#include "general_server/http_comm_task.h"

using namespace sdb;
using namespace sdb::rest;

void AcceptorUnixDomain::open() {
  std::string path(((EndpointUnixDomain*)_endpoint)->path());
  if (basics::file_utils::Exists(path)) {
    // socket file already exists
    SDB_WARN("xxxxx", sdb::Logger::FIXME, "socket file '", path,
             "' already exists.");

    // delete previously existing socket file
    if (basics::file_utils::Remove(path) == ERROR_OK) {
      SDB_WARN("xxxxx", sdb::Logger::FIXME,
               "deleted previously existing socket file '", path, "'");
    } else {
      SDB_ERROR("xxxxx", sdb::Logger::FIXME,
                "unable to delete previously existing socket file '", path,
                "'");
    }
  }

  asio_ns::local::stream_protocol::stream_protocol::endpoint endpoint(path);
  _acceptor.open(endpoint.protocol());
  _acceptor.bind(endpoint);
  _acceptor.listen();

  _open = true;
  asyncAccept();
}

void AcceptorUnixDomain::asyncAccept() {
  IoContext& context = _server.selectIoContext();

  auto asio_socket = std::make_shared<AsioSocket<SocketType::Unix>>(context);
  auto& socket = asio_socket->socket;
  auto& peer = asio_socket->peer;
  auto handler = [this, asio_socket = std::move(asio_socket)](
                   const asio_ns::error_code& ec) mutable {
    if (ec) {
      handleError(ec);
      return;
    }

    // set the endpoint
    ConnectionInfo info;
    info.endpoint = _endpoint->specification();
    info.endpoint_type = _endpoint->domainType();
    info.encryption_type = _endpoint->encryption();
    info.server_address = _endpoint->host();
    info.server_port = _endpoint->port();
    info.client_address = "local";
    info.client_port = 0;
    if (asio_socket->peer.size() <= sizeof(info.sas)) {
      info.sas_len = asio_socket->peer.size();
      memcpy(&info.sas, asio_socket->peer.data(), info.sas_len);
    }

    auto comm_task = std::make_shared<HttpCommTask<SocketType::Unix>>(
      _server, std::move(info), std::move(asio_socket));
    _server.registerTask(std::move(comm_task));
    this->asyncAccept();
  };

  _acceptor.async_accept(socket, peer, std::move(handler));
}

void AcceptorUnixDomain::close() {
  if (_open) {
    _open = false;  // make sure this flag is reset to `false` before
                    // we cancel/close the acceptor, otherwise the
                    // handleError method would restart async_accept
                    // right away
    asio_ns::dispatch(_ctx.io_context, [this] {
      _acceptor.close();
      auto path = basics::downCast<EndpointUnixDomain>(_endpoint)->path();
      if (basics::file_utils::Remove(path) != ERROR_OK) {
        SDB_TRACE("xxxxx", sdb::Logger::FIXME, "unable to remove socket file '",
                  path, "'");
      }
    });
  }
}

void AcceptorUnixDomain::cancel() {
  asio_ns::dispatch(_ctx.io_context, [this] { _acceptor.cancel(); });
}
