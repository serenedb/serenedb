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

#include "acceptor_tcp.h"

#include "basics/common.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "endpoint/connection_info.h"
#include "endpoint/endpoint_ip.h"
#include "general_server/general_server.h"
#include "general_server/h2_comm_task.h"
#include "general_server/http_comm_task.h"
#include "pg/pg_comm_task.h"

namespace sdb::rest {
namespace {

template<typename SocketType>
ConnectionInfo CreateConnectionInfo(const SocketType& socket,
                                    const Endpoint& endpoint) {
  ConnectionInfo info;
  info.endpoint = endpoint.specification();
  info.endpoint_type = endpoint.domainType();
  info.encryption_type = endpoint.encryption();
  info.server_address = endpoint.host();
  info.server_port = endpoint.port();

  info.client_address = socket.peer.address().to_string();
  info.client_port = socket.peer.port();
  if (socket.peer.size() <= sizeof(info.sas)) {
    info.sas_len = socket.peer.size();
    memcpy(&info.sas, socket.peer.data(), info.sas_len);
  }
  return info;
}

bool TlsH2Negotiated(SSL* ssl) {
  const unsigned char* next_proto = nullptr;
  unsigned int next_proto_len = 0;

  SSL_get0_alpn_selected(ssl, &next_proto, &next_proto_len);

  // allowed value is "h2"
  // http://www.iana.org/assignments/tls-extensiontype-values/tls-extensiontype-values.xhtml
  if (next_proto != nullptr && next_proto_len == 2 &&
      memcmp(next_proto, "h2", 2) == 0) {
    return true;
  }
  return false;
}

}  // namespace

template<SocketType T>
void AcceptorTcp<T>::open() {
  asio_ns::ip::tcp::resolver resolver(_ctx.io_context);

  std::string hostname = _endpoint->host();
  int port_number = _endpoint->port();

  asio_ns::ip::tcp::endpoint asio_endpoint;
  asio_ns::error_code ec;
  auto address = asio_ns::ip::make_address(hostname, ec);
  if (!ec) {
    asio_endpoint = asio_ns::ip::tcp::endpoint(address, port_number);
  } else {  // we need to resolve the string containing the ip
    const auto results = [&] {
      if (_endpoint->domain() == AF_INET6) {
        return resolver.resolve(asio_ns::ip::tcp::v6(), hostname,
                                std::to_string(port_number), ec);
      } else if (_endpoint->domain() == AF_INET) {
        return resolver.resolve(asio_ns::ip::tcp::v4(), hostname,
                                std::to_string(port_number), ec);
      }
      SDB_THROW(ERROR_IP_ADDRESS_INVALID);
    }();

    if (ec) {
      SDB_ERROR("xxxxx", Logger::COMMUNICATION,
                "unable to to resolve endpoint ' ", _endpoint->specification(),
                "': ", ec.message());
      throw std::runtime_error(ec.message());
    }

    if (results.empty()) {
      SDB_ERROR(
        "xxxxx", Logger::COMMUNICATION,
        "unable to to resolve endpoint: endpoint is default constructed");
    } else {
      asio_endpoint = results.begin()->endpoint();
    }
  }
  _acceptor.open(asio_endpoint.protocol());

  _acceptor.set_option(asio_ns::ip::tcp::acceptor::reuse_address{
    basics::downCast<EndpointIp>(_endpoint)->reuseAddress()});

  _acceptor.bind(asio_endpoint, ec);
  if (ec) {
    SDB_ERROR("xxxxx", Logger::COMMUNICATION, "unable to bind to endpoint '",
              _endpoint->specification(), "': ", ec.message());
    throw std::runtime_error(ec.message());
  }

  SDB_ASSERT(_endpoint->listenBacklog() > 8);
  _acceptor.listen(_endpoint->listenBacklog(), ec);
  if (ec) {
    SDB_ERROR("xxxxx", Logger::COMMUNICATION, "unable to listen to endpoint '",
              _endpoint->specification(), ": ", ec.message());
    throw std::runtime_error(ec.message());
  }
  _open = true;

  SDB_DEBUG("xxxxx", sdb::Logger::COMMUNICATION,
            "successfully opened acceptor TCP");

  asyncAccept();
}

template<SocketType T>
void AcceptorTcp<T>::close() {
  if (_open) {
    _open = false;  // make sure the _open flag is `false` before we
                    // cancel/close the acceptor, since otherwise the
                    // handleError method will restart async_accept.
    asio_ns::dispatch(_ctx.io_context, [this] { _acceptor.close(); });
  }
}

template<SocketType T>
void AcceptorTcp<T>::cancel() {
  asio_ns::dispatch(_ctx.io_context, [this] { _acceptor.cancel(); });
}

template<>
void AcceptorTcp<SocketType::Tcp>::asyncAccept() {
  SDB_ASSERT(_endpoint->encryption() == Endpoint::EncryptionType::None);

  auto asio_socket =
    std::make_shared<AsioSocket<SocketType::Tcp>>(_server.selectIoContext());
  auto& socket = asio_socket->socket;
  auto& peer = asio_socket->peer;
  auto handler = [this, asio_socket = std::move(asio_socket)](
                   const asio_ns::error_code& ec) mutable {
    if (ec) {
      handleError(ec);
      return;
    }

    // set the endpoint
    ConnectionInfo info = CreateConnectionInfo(*asio_socket, *_endpoint);

    SDB_DEBUG("xxxxx", sdb::Logger::COMMUNICATION, "accepted connection from ",
              info.client_address, ":", info.client_port);

    if (_endpoint->transport() == Endpoint::TransportType::HTTP) {
      auto comm_task = std::make_shared<HttpCommTask<SocketType::Tcp>>(
        _server, std::move(info), std::move(asio_socket));
      _server.registerTask(std::move(comm_task));
    } else {
      SDB_ASSERT(_endpoint->transport() == Endpoint::TransportType::PGSQL);
      auto comm_task = std::make_shared<pg::PgSQLCommTask<SocketType::Tcp>>(
        _server, std::move(info), std::move(asio_socket));
      _server.registerTask(std::move(comm_task));
    }
    this->asyncAccept();
  };

  _acceptor.async_accept(socket, peer, std::move(handler));
}

template<>
void AcceptorTcp<SocketType::Ssl>::PerformHandshake(
  std::shared_ptr<AsioSocket<SocketType::Ssl>> proto) {
  // io_context is single-threaded, no sync needed
  auto* ptr = proto.get();
  proto->timer.expires_after(std::chrono::seconds{60});
  proto->timer.async_wait([ptr](const asio_ns::error_code& ec) {
    if (ec) {  // canceled
      return;
    }
    ptr->shutdown([](const asio_ns::error_code&) {});  // ignore error
  });

  auto cb = [this,
             as = std::move(proto)](const asio_ns::error_code& ec) mutable {
    as->timer.cancel();
    if (ec) {
      SDB_DEBUG("xxxxx", sdb::Logger::COMMUNICATION,
                "error during TLS handshake: '", ec.message(), "'");
      as.reset();  // ungraceful shutdown
      return;
    }

    // set the endpoint
    ConnectionInfo info = CreateConnectionInfo(*as, *_endpoint);

    std::shared_ptr<CommTask> task;

    if (TlsH2Negotiated(as->socket.native_handle())) {
      task = std::make_shared<H2CommTask<SocketType::Ssl>>(
        _server, std::move(info), std::move(as));
    } else {
      task = std::make_shared<HttpCommTask<SocketType::Ssl>>(
        _server, std::move(info), std::move(as));
    }

    _server.registerTask(std::move(task));
  };
  ptr->handshake(std::move(cb));
}

template<>
void AcceptorTcp<SocketType::Ssl>::asyncAccept() {
  SDB_ASSERT(_endpoint->encryption() == Endpoint::EncryptionType::SSL);

  // select the io context for this socket
  auto& ctx = _server.selectIoContext();

  auto asio_socket =
    std::make_shared<AsioSocket<SocketType::Ssl>>(ctx, _server.sslContexts());
  auto& socket = asio_socket->socket.lowest_layer();
  auto& peer = asio_socket->peer;
  auto handler = [this, asio_socket = std::move(asio_socket)](
                   const asio_ns::error_code& ec) mutable {
    if (ec) {
      handleError(ec);
      return;
    }
    if (_endpoint->transport() == Endpoint::TransportType::PGSQL) {
      ConnectionInfo info = CreateConnectionInfo(*asio_socket, *_endpoint);
      auto comm_task = std::make_shared<pg::PgSQLCommTask<SocketType::Ssl>>(
        _server, std::move(info), std::move(asio_socket));
      _server.registerTask(std::move(comm_task));
    } else {
      PerformHandshake(std::move(asio_socket));
    }
    this->asyncAccept();
  };

  _acceptor.async_accept(socket, peer, std::move(handler));
}

template class AcceptorTcp<SocketType::Tcp>;
template class AcceptorTcp<SocketType::Ssl>;

}  // namespace sdb::rest
