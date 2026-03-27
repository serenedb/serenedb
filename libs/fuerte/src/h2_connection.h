////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Simon Grätzer
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <nghttp2/nghttp2.h>

#include "general_connection.h"
#include "http.h"

// naming in this file will be closer to asio for internal functions and types
// functions that are exposed to other classes follow coding conventions

namespace sdb::fuerte::http {

// ongoing Http2 stream
struct Stream {
  Stream(std::unique_ptr<Request>&& req, RequestCallback&& cb)
    : callback(std::move(cb)),
      request(std::move(req)),
      callback_called(false) {}

  vpack::BufferUInt8 data;

  RequestCallback callback;
  std::unique_ptr<sdb::fuerte::Request> request;
  std::unique_ptr<sdb::fuerte::Response> response;
  size_t response_offset = 0;
  /// point in time when the message expires
  std::chrono::steady_clock::time_point expires;
  bool callback_called;

  inline void invokeOnError(Error e) {
    // In the HTTP/2 case a stream in which the callback has been called
    // is not deleted from _streams right away. Therefore it is possible
    // that two timeouts happen and the callback would be called twice.
    // This is prevented by this flag, which is only ever read and
    // written in the I/O thread.
    if (!callback_called) {
      callback_called = true;
      callback(e, std::move(request), nullptr);
    }
  }
};

// Connection object that handles sending and receiving of
//  Velocystream Messages.
template<SocketType T>
class H2Connection final : public fuerte::MultiConnection<T, Stream> {
 public:
  explicit H2Connection(EventLoopService& loop,
                        const detail::ConnectionConfiguration&);

  ~H2Connection() final;

 protected:
  virtual void finishConnect() override;

  /// perform writes
  virtual void DoWrite() override;

  // called by the async_read handler (called from IO thread)
  virtual void asyncReadCallback(const asio_ns::error_code&) override;

  /// abort ongoing / unfinished requests expiring before given timpoint
  virtual void abortRequests(
    fuerte::Error, std::chrono::steady_clock::time_point now) override;

 private:
  static int on_begin_headers(nghttp2_session* session,
                              const nghttp2_frame* frame, void* user_data);
  static int on_header(nghttp2_session* session, const nghttp2_frame* frame,
                       const uint8_t* name, size_t namelen,
                       const uint8_t* value, size_t valuelen, uint8_t flags,
                       void* user_data);
  static int on_frame_recv(nghttp2_session* session, const nghttp2_frame* frame,
                           void* user_data);
  static int on_data_chunk_recv(nghttp2_session* session, uint8_t flags,
                                int32_t stream_id, const uint8_t* data,
                                size_t len, void* user_data);
  static int on_stream_close(nghttp2_session* session, int32_t stream_id,
                             uint32_t error_code, void* user_data);
  static int on_frame_not_send(nghttp2_session* session,
                               const nghttp2_frame* frame, int lib_error_code,
                               void* user_data);

 private:
  void InitNgHttp2Session();

  void SendHttp1UpgradeRequest();
  void ReadSwitchingProtocolsResponse();

  // queue the response onto the session, call only on IO thread
  void QueueHttp2Requests();

  Stream* FindStream(int32_t sid) const;

  std::unique_ptr<Stream> EraseStream(int32_t sid);

  /// should close connection
  bool ShouldStop() const;

  // ping ensures server does not close the connection
  void StartPing();

 private:
  vpack::BufferUInt8 _outbuffer;
  asio_ns::steady_timer _ping;  // keep connection open
  const std::string _auth_header;
  nghttp2_session* _session = nullptr;
};

}  // namespace sdb::fuerte::http
