////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "general_server/generic_comm_task.h"

#include "app/app_feature.h"
#include "app/app_server.h"
#include "general_server/general_server_feature.h"
#include "pg/pg_comm_task.h"

namespace sdb {

template<rest::SocketType SocketType, typename Base>
GenericCommTask<SocketType, Base>::GenericCommTask(
  rest::GeneralServer& server, ConnectionInfo info,
  std::shared_ptr<rest::AsioSocket<SocketType>> socket)
  : Base{server, std::move(info)},
    _protocol{std::move(socket)},
    _general_server_feature{
      server.server().getFeature<GeneralServerFeature>()} {
  if (rest::AsioSocket<SocketType>::supportsMixedIO()) {
    _protocol->setNonBlocking(true);
  }
}

template<rest::SocketType SocketType, typename Base>
void GenericCommTask<SocketType, Base>::Stop() {
  _stopped.store(true, std::memory_order_release);
  if (_protocol) {
    asio_ns::dispatch(_protocol->context.io_context,
                      [self = this->shared_from_this()] { self->Close(); });
  } else {
    this->_server.unregisterTask(this);
  }
}

template<rest::SocketType SocketType, typename Base>
void GenericCommTask<SocketType, Base>::Close(asio_ns::error_code ec) {
  _stopped.store(true, std::memory_order_release);
  if (ec && (ec != asio_ns::error::misc_errors::eof &&
             ec != asio_ns::ssl::error::stream_truncated)) {
    // stream_truncated will occur when a peer closes an SSL/TLS connection
    // without performing a proper connection shutdown. unfortunately that
    // can happen at any time, and we have no control over it.
    SDB_WARN("xxxxx", Logger::REQUESTS, "asio IO error: '", ec.message(), "'");
  }

  if (_protocol) {
    _protocol->timer.cancel();
    _protocol->shutdown(
      [self = this->shared_from_this(), this](asio_ns::error_code ec) {
        if (ec) {
          SDB_INFO("xxxxx", Logger::REQUESTS,
                   "error shutting down asio socket: '", ec.message(), "'");
        }
        this->_server.unregisterTask(this);
      });
  } else {
    this->_server.unregisterTask(this);
  }
}

template<rest::SocketType SocketType, typename Base>
void GenericCommTask<SocketType, Base>::AsyncReadSome() try {
  asio_ns::error_code ec;
  // first try a sync read for performance
  if (rest::AsioSocket<SocketType>::supportsMixedIO()) {
    size_t available = _protocol->available(ec);

    while (!ec && available > 8) {
      auto mutable_buff = _protocol->buffer.prepare(available);
      size_t nread = _protocol->socket.read_some(mutable_buff, ec);
      _protocol->buffer.commit(nread);
      if (ec) {
        break;
      }

      if (!ReadCallback(ec)) {
        return;
      }
      available = _protocol->available(ec);
    }
    if (ec == asio_ns::error::would_block) {
      ec.clear();
    }
  }

  // read pipelined requests / remaining data
  if (_protocol->buffer.size() > 0 && !ReadCallback(ec)) {
    return;
  }

  auto mutable_buff = _protocol->buffer.prepare(kReadBlockSize);

  _reading = true;
  this->SetIOTimeout();
  _protocol->socket.async_read_some(
    mutable_buff, [self = this->shared_from_this()](
                    const asio_ns::error_code& ec, size_t nread) {
      auto& me = static_cast<GenericCommTask<SocketType, Base>&>(*self);
      me._reading = false;
      me._protocol->buffer.commit(nread);

      try {
        if (me.ReadCallback(ec)) {
          me.AsyncReadSome();
        }
      } catch (...) {
        SDB_ERROR("xxxxx", Logger::REQUESTS,
                  "unhandled protocol exception, closing connection");
        me.Close(ec);
      }
    });
} catch (...) {
  SDB_ERROR("xxxxx", Logger::REQUESTS,
            "unhandled protocol exception, closing connection");
  Close();
}

template class GenericCommTask<rest::SocketType::Tcp, rest::CommTask>;
template class GenericCommTask<rest::SocketType::Ssl, rest::CommTask>;
template class GenericCommTask<rest::SocketType::Unix, rest::CommTask>;

template class GenericCommTask<rest::SocketType::Tcp, pg::PgSQLCommTaskBase>;
template class GenericCommTask<rest::SocketType::Ssl, pg::PgSQLCommTaskBase>;
template class GenericCommTask<rest::SocketType::Unix, pg::PgSQLCommTaskBase>;

}  // namespace sdb
