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

#pragma once

#include "endpoint/connection_info.h"
#include "general_server/asio_socket.h"
#include "general_server/general_server.h"

namespace sdb {

// TODO move all rest independend part out of rest namespace.

template<rest::SocketType SocketType, typename Base>
class GenericCommTask : public Base {
  GenericCommTask(const GenericCommTask&) = delete;
  const GenericCommTask& operator=(const GenericCommTask&) = delete;

 public:
  GenericCommTask(rest::GeneralServer& server, ConnectionInfo,
                  std::shared_ptr<rest::AsioSocket<SocketType>>);

  void Stop() override;
  void Close(asio_ns::error_code err = {}) override;

 protected:
  /// default max chunksize is 30kb in serenedb (each read fits)
  static constexpr size_t kReadBlockSize = 1024 * 32;

  /// read from socket
  void AsyncReadSome();

  bool Stopped() const noexcept override {
    return _stopped.load(std::memory_order_acquire);
  }

  /// called to process data in _read_buffer, return false to stop
  virtual bool ReadCallback(asio_ns::error_code ec) = 0;

  std::shared_ptr<rest::AsioSocket<SocketType>> _protocol;
  GeneralServerFeature& _general_server_feature;
  std::atomic_bool _stopped = false;
  // Used only for keep-alive timeout in rest related tasks but
  // set in AsyncReadSome. Remove when getting rid of rest tasks.
  bool _reading = false;
};

}  // namespace sdb
