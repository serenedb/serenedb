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

#include "h2_connection.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>
#include <fuerte/helper.h>
#include <fuerte/loop.h>
#include <fuerte/message.h>
#include <fuerte/types.h>

#include <string_view>

namespace sdb::fuerte::http {

template<SocketType T>
int H2Connection<T>::on_begin_headers(nghttp2_session* session,
                                      const nghttp2_frame* frame,
                                      void* user_data) {
  SDB_TRACE("xxxxx", Logger::FUERTE, "on_begin_headers ", frame->hd.stream_id);

  // only care about (first) response headers
  if (frame->hd.type != NGHTTP2_HEADERS ||
      frame->headers.cat != NGHTTP2_HCAT_RESPONSE) {
    return 0;
  }

  H2Connection<T>* me = static_cast<H2Connection<T>*>(user_data);
  Stream* strm = me->FindStream(frame->hd.stream_id);
  if (strm) {
    strm->response = std::make_unique<fuerte::Response>();
    return 0;
  } else {  // reset the stream
    return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
  }
}

template<SocketType T>
/*static*/ int H2Connection<T>::on_header(nghttp2_session* session,
                                          const nghttp2_frame* frame,
                                          const uint8_t* name, size_t namelen,
                                          const uint8_t* value, size_t valuelen,
                                          uint8_t flags, void* user_data) {
  H2Connection<T>* me = static_cast<H2Connection<T>*>(user_data);
  int32_t stream_id = frame->hd.stream_id;
  SDB_TRACE("xxxxx", Logger::FUERTE, "on_header ", stream_id);

  if (frame->hd.type != NGHTTP2_HEADERS ||
      frame->headers.cat != NGHTTP2_HCAT_RESPONSE) {
    return 0;
  }

  SDB_TRACE("xxxxx", Logger::FUERTE, "got HEADER frame for stream ", stream_id);

  Stream* strm = me->FindStream(stream_id);
  if (!strm) {
    SDB_TRACE("xxxxx", Logger::FUERTE, "HEADER frame for unkown stream ",
              stream_id);
    return 0;
  }

  // handle pseudo headers
  // https://http2.github.io/http2-spec/#rfc.section.8.1.2.3
  std::string_view field(reinterpret_cast<const char*>(name), namelen);
  std::string_view val(reinterpret_cast<const char*>(value), valuelen);

  if (field == ":status") {
    std::string v(val);
    strm->response->header.response_code = (StatusCode)std::stoul(v);
  } else if (field == kFuContentLengthKey) {
    std::string v(val);
    size_t len = std::min<size_t>(std::stoul(v), 1024 * 1024 * 64);
    strm->data.reserve(len);
    strm->response->header.addMeta(std::string(field), std::move(val));
  } else {  // fall through
    strm->response->header.addMeta(std::string(field), std::string(val));
    // TODO limit max header size ??
  }

  return 0;
}

template<SocketType T>
int H2Connection<T>::on_frame_recv(nghttp2_session* session,
                                   const nghttp2_frame* frame,
                                   void* user_data) {
  H2Connection<T>* me = static_cast<H2Connection<T>*>(user_data);

  const int32_t sid = frame->hd.stream_id;
  SDB_TRACE("xxxxx", Logger::FUERTE, "on_frame_recv ", sid);

  switch (frame->hd.type) {
    case NGHTTP2_DATA:
    case NGHTTP2_HEADERS: {
      if (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) {
        auto strm = me->EraseStream(sid);
        if (strm) {
          SDB_TRACE("xxxxx", Logger::FUERTE, "got response on stream ", sid);
          strm->response->setPayload(std::move(strm->data), /*offset*/ 0);
          strm->callback(Error::NoError, std::move(strm->request),
                         std::move(strm->response));
        }
      }
      break;
    }
  }

  return 0;
}

template<SocketType T>
/*static*/ int H2Connection<T>::on_data_chunk_recv(
  nghttp2_session* session, uint8_t flags, int32_t stream_id,
  const uint8_t* data, size_t len, void* user_data) {
  SDB_TRACE("xxxxx", Logger::FUERTE, "DATA frame for stream ", stream_id);

  H2Connection<T>* me = static_cast<H2Connection<T>*>(user_data);
  Stream* strm = me->FindStream(stream_id);
  if (strm) {
    strm->data.append(data, len);
  }

  return 0;
}

template<SocketType T>
/*static*/ int H2Connection<T>::on_stream_close(nghttp2_session* session,
                                                int32_t stream_id,
                                                uint32_t error_code,
                                                void* user_data) {
  SDB_TRACE("xxxxx", Logger::FUERTE, "closing stream ", stream_id, " error '",
            nghttp2_http2_strerror(error_code), "' (", error_code, ")");
  H2Connection<T>* me = static_cast<H2Connection<T>*>(user_data);

  auto strm = me->EraseStream(stream_id);
  if (error_code != NGHTTP2_NO_ERROR && strm != nullptr) {
    if (strm->request) {  // abortRequest() may have closed the stream
      SDB_ERROR("xxxxx", Logger::FUERTE, "http2 closing stream '", stream_id,
                "' with error ", nghttp2_http2_strerror(error_code), " (",
                error_code, ")");
      strm->invokeOnError(fuerte::Error::ProtocolError);
    }
  }

  return 0;
}

template<SocketType T>
/*static*/ int H2Connection<T>::on_frame_not_send(nghttp2_session* session,
                                                  const nghttp2_frame* frame,
                                                  int lib_error_code,
                                                  void* user_data) {
  if (frame->hd.type != NGHTTP2_HEADERS) {
    return 0;
  }
  SDB_TRACE("xxxxx", Logger::FUERTE, "frame not send: '",
            nghttp2_strerror(lib_error_code), "' (", lib_error_code, ")");

  // Issue RST_STREAM so that stream does not hang around.
  nghttp2_submit_rst_stream(session, NGHTTP2_FLAG_NONE, frame->hd.stream_id,
                            NGHTTP2_INTERNAL_ERROR);

  return 0;
}

namespace {

int OnErrorCallback(nghttp2_session* session, int lib_error_code,
                    const char* msg, size_t len, void*) {
  SDB_ERROR("xxxxx", Logger::FUERTE, "http2 error: \"", std::string(msg, len),
            "\" (", lib_error_code, ")");
  return 0;
}

int OnInvalidFrameRecv(nghttp2_session* session, const nghttp2_frame* frame,
                       int lib_error_code, void* user_data) {
  SDB_DEBUG("xxxxx", Logger::FUERTE, "received illegal data frame on stream ",
            frame->hd.stream_id, ": '", nghttp2_strerror(lib_error_code), "' (",
            lib_error_code, ")");
  return 0;
}

constexpr uint32_t kWindowSize = 512 * 1024 * 1024;
void PopulateSettings(std::array<nghttp2_settings_entry, 4>& iv) {
  // 32 streams matches the queue capacity
  iv[0] = {NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 32};
  // typically client is just a *sink* and just process data as
  // much as possible.  Use large window size by default.
  iv[1] = {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, kWindowSize};
  iv[2] = {NGHTTP2_SETTINGS_MAX_FRAME_SIZE, (1 << 14)};  // 16k
  iv[3] = {NGHTTP2_SETTINGS_ENABLE_PUSH, 0};
}

void SubmitConnectionPreface(nghttp2_session* session) {
  std::array<nghttp2_settings_entry, 4> iv;
  PopulateSettings(iv);

  nghttp2_submit_settings(session, NGHTTP2_FLAG_NONE, iv.data(), iv.size());
  // increase connection window size up to window_size
  nghttp2_session_set_local_window_size(session, NGHTTP2_FLAG_NONE, 0,
                                        kWindowSize);
}

std::string MakeAuthHeader(const detail::ConnectionConfiguration& config) {
  std::string auth;
  // preemptively cache authentication
  if (config.authentication_type == AuthenticationType::Basic) {
    auth.append("Basic ");
    auth.append(
      absl::Base64Escape(absl::StrCat(config.user, ":", config.password)));
  } else if (config.authentication_type == AuthenticationType::Jwt) {
    if (config.jwt_token.empty()) {
      throw std::logic_error("JWT token is not set");
    }
    auth.append("bearer ");
    auth.append(config.jwt_token);
  }
  return auth;
}

}  // namespace

template<SocketType T>
H2Connection<T>::H2Connection(EventLoopService& loop,
                              const detail::ConnectionConfiguration& config)
  : fuerte::MultiConnection<T, Stream>(loop, config),
    _ping(*(this->_io_context)),
    _auth_header(MakeAuthHeader(config)) {
  // Set ALPN "h2" advertisement on connection
  if constexpr (T == SocketType::Ssl) {
    SSL_set_alpn_protos(this->_proto.socket.native_handle(),
                        (const unsigned char*)"\x02h2", 3);
  }
}

template<SocketType T>
H2Connection<T>::~H2Connection() try {
  abortRequests(Error::ConnectionCanceled, /*now*/ Clock::time_point::max());
  nghttp2_session_del(_session);
  _session = nullptr;
} catch (...) {
}

/// init h2 session
template<SocketType T>
void H2Connection<T>::InitNgHttp2Session() {
  nghttp2_session_callbacks* callbacks;
  int rv = nghttp2_session_callbacks_new(&callbacks);
  if (rv != 0) {
    throw std::runtime_error("out ouf memory");
  }

  nghttp2_session_callbacks_set_on_begin_headers_callback(
    callbacks, H2Connection<T>::on_begin_headers);
  nghttp2_session_callbacks_set_on_header_callback(callbacks,
                                                   H2Connection<T>::on_header);
  nghttp2_session_callbacks_set_on_frame_recv_callback(
    callbacks, H2Connection<T>::on_frame_recv);
  nghttp2_session_callbacks_set_on_data_chunk_recv_callback(
    callbacks, H2Connection<T>::on_data_chunk_recv);
  nghttp2_session_callbacks_set_on_stream_close_callback(
    callbacks, H2Connection<T>::on_stream_close);
  nghttp2_session_callbacks_set_on_frame_not_send_callback(
    callbacks, H2Connection<T>::on_frame_not_send);
  nghttp2_session_callbacks_set_on_invalid_frame_recv_callback(
    callbacks, OnInvalidFrameRecv);
  nghttp2_session_callbacks_set_error_callback2(callbacks, OnErrorCallback);

  if (_session) {  // this might be called again if we reconnect
    nghttp2_session_del(_session);
  }

  rv = nghttp2_session_client_new(&_session, callbacks, /*args*/ this);
  nghttp2_session_callbacks_del(callbacks);

  if (rv != 0) {
    throw std::runtime_error("out ouf memory");
  }
}

// socket connection is used without TLS
template<SocketType T>
void H2Connection<T>::finishConnect() {
  SDB_TRACE("xxxxx", Logger::FUERTE, "finishInitialization (h2)");
  if (this->state() != Connection::State::Connecting) {
    return;
  }

  // lets do the HTTP2 session upgrade right away
  InitNgHttp2Session();

  if constexpr (T == SocketType::Tcp) {
    if (this->_config.upgrade_h1_to_h2) {
      SendHttp1UpgradeRequest();
      return;
    }
  }

  this->_state.store(Connection::State::Connected);

  // send mandatory client connection preface
  SubmitConnectionPreface(_session);

  // submit a ping so the connection is not closed right away
  StartPing();

  this->AsyncReadSome();  // start reading
  DoWrite();              // start writing
}

template<>
void H2Connection<SocketType::Tcp>::ReadSwitchingProtocolsResponse() {
  SDB_TRACE("xxxxx", Logger::FUERTE, "readSwitchingProtocolsResponse)");

  auto self = Connection::shared_from_this();
  this->_proto.timer.expires_after(std::chrono::seconds(5));
  this->_proto.timer.async_wait([self](auto ec) {
    if (!ec) {
      self->cancel();
    }
  });
  // read until we find end of http header
  asio_ns::async_read_until(
    this->_proto.socket, this->_receive_buffer, "\r\n\r\n",
    [self](const asio_ns::error_code& ec, size_t nread) {
      auto& me = static_cast<H2Connection<SocketType::Tcp>&>(*self);
      me.cancelTimer();
      if (ec) {
        me.shutdownConnection(Error::ReadError,
                              "error reading upgrade response");
        return;
      }

      // server should respond with 101 and "Upgrade: h2c"
      auto it = asio_ns::buffers_begin(me._receive_buffer.data());
      std::string header(it, it + static_cast<ptrdiff_t>(nread));
      if (header.compare(0, 12, "HTTP/1.1 101") == 0 &&
          header.find("Upgrade: h2c\r\n") != std::string::npos) {
        SDB_ASSERT(nread == header.size());
        me._receive_buffer.consume(nread);
        me._state.store(Connection::State::Connected);

        // submit a ping so the connection is not closed right away
        me.StartPing();

        me.AsyncReadSome();
        me.DoWrite();
      } else {
        SDB_ASSERT(false);
        me.shutdownConnection(Error::ProtocolError, "illegal upgrade response");
      }
    });
}

template<>
void H2Connection<SocketType::Tcp>::SendHttp1UpgradeRequest() {
  // client connection preface via Upgrade header
  std::array<nghttp2_settings_entry, 4> iv;
  PopulateSettings(iv);

  std::string packed(iv.size() * 6, ' ');
  ssize_t nwrite = nghttp2_pack_settings_payload(
    (uint8_t*)packed.data(), packed.size(), iv.data(), iv.size());
  SDB_ASSERT(nwrite >= 0);
  packed.resize(static_cast<size_t>(nwrite));

  // this will submit the settings field for us
  ssize_t rv = nghttp2_session_upgrade2(_session, (const uint8_t*)packed.data(),
                                        packed.size(), /*head*/ 0, nullptr);
  if (rv < 0) {
    SDB_ASSERT(false);
    this->shutdownConnection(Error::ProtocolError, "error during upgrade");
    return;
  }

  // simon: important otherwise big responses fail
  // increase connection window size up to window_size
  rv = nghttp2_session_set_local_window_size(_session, NGHTTP2_FLAG_NONE, 0,
                                             kWindowSize);
  SDB_ASSERT(rv == 0);

  auto req = std::make_shared<std::string>();
  req->append("GET / HTTP/1.1\r\nConnection: Upgrade, HTTP2-Settings\r\n");
  req->append("Upgrade: h2c\r\nHTTP2-Settings: ");
  req->append(absl::Base64Escape(packed));
  req->append("\r\n\r\n");

  asio_ns::async_write(
    this->_proto.socket, asio_ns::buffer(req->data(), req->size()),
    [self(Connection::shared_from_this())](const auto& ec, size_t nsend) {
      auto& me = static_cast<H2Connection<SocketType::Tcp>&>(*self);
      if (ec) {
        me.shutdownConnection(Error::WriteError, ec.message());
      } else {
        me.ReadSwitchingProtocolsResponse();
      }
    });
}

// socket connection is up (with optional SSL), now initiate the H2 protocol.
template<>
void H2Connection<SocketType::Ssl>::finishConnect() {
  SDB_TRACE("xxxxx", Logger::FUERTE, "finishInitialization (h2)\n");
  if (this->state() != Connection::State::Connecting) {
    return;
  }

  const unsigned char* alpn = NULL;
  unsigned int alpnlen = 0;
  SSL_get0_alpn_selected(this->_proto.socket.native_handle(), &alpn, &alpnlen);

  if (alpn == NULL || alpnlen != 2 || memcmp("h2", alpn, 2) != 0) {
    SDB_ERROR("xxxxx", Logger::FUERTE, "h2 is not negotiated");
    shutdownConnection(Error::ProtocolError, "h2 is not negotiated");
    return;
  }

  this->_state.store(Connection::State::Connected);

  InitNgHttp2Session();

  // send mandatory client connection preface
  SubmitConnectionPreface(_session);

  // submit a ping so the connection is not closed right away
  StartPing();

  AsyncReadSome();  // start reading
  DoWrite();        // start writing
}

// ------------------------------------
// Writing data
// ------------------------------------

// queue the response onto the session, call only on IO thread
template<SocketType T>
void H2Connection<T>::QueueHttp2Requests() {
  int num_queued = 0;  // make sure we do not send too many request

  Stream* tmp = nullptr;
  while (num_queued++ < 4 && this->_queue.pop(tmp)) {
    std::unique_ptr<Stream> strm(tmp);
    uint32_t q = this->_num_queued.fetch_sub(1, std::memory_order_relaxed);
    SDB_ASSERT(q > 0);

    SDB_TRACE("xxxxx", Logger::FUERTE,
              "queued request this=", reinterpret_cast<uintptr_t>(this));

    fuerte::Request& req = *strm->request;
    // we need a continous block of memory for headers
    std::vector<nghttp2_nv> nva;
    nva.reserve(4 + req.header.meta().size());

    std::string verb = fuerte::ToString(req.header.rest_verb);
    nva.push_back({(uint8_t*)":method", (uint8_t*)verb.data(), 7, verb.size(),
                   NGHTTP2_NV_FLAG_NO_COPY_NAME});

    if constexpr (T == SocketType::Tcp) {
      nva.push_back(
        {(uint8_t*)":scheme", (uint8_t*)"http", 7, 4,
         NGHTTP2_NV_FLAG_NO_COPY_NAME | NGHTTP2_NV_FLAG_NO_COPY_VALUE});
    } else {
      nva.push_back(
        {(uint8_t*)":scheme", (uint8_t*)"https", 7, 5,
         NGHTTP2_NV_FLAG_NO_COPY_NAME | NGHTTP2_NV_FLAG_NO_COPY_VALUE});
    }

    std::string path;
    http::AppendPath(req, path);

    nva.push_back({(uint8_t*)":path", (uint8_t*)path.data(), 5, path.size(),
                   NGHTTP2_NV_FLAG_NO_COPY_NAME});

    nva.push_back(
      {(uint8_t*)":authority", (uint8_t*)this->_config.host.c_str(), 10,
       this->_config.host.size(),
       NGHTTP2_NV_FLAG_NO_COPY_NAME | NGHTTP2_NV_FLAG_NO_COPY_VALUE});

    std::string type;
    if (req.header.rest_verb != RestVerb::Get &&
        req.contentType() != ContentType::Custom) {
      type = ToString(req.contentType());
      nva.push_back({(uint8_t*)"content-type", (uint8_t*)type.c_str(), 12,
                     type.length(), NGHTTP2_NV_FLAG_NO_COPY_NAME});
    }
    std::string accept;
    if (req.acceptType() != ContentType::Custom) {
      accept = ToString(req.acceptType());
      nva.push_back({(uint8_t*)"accept", (uint8_t*)accept.c_str(), 6,
                     accept.length(), NGHTTP2_NV_FLAG_NO_COPY_NAME});
    }

    bool have_auth = false;
    for (const auto& pair : req.header.meta()) {
      if (pair.first == kFuContentLengthKey) {
        continue;  // skip content-length header
      }

      if (pair.first == kFuAuthorizationKey) {
        have_auth = true;
      }
      SDB_ASSERT(absl::c_none_of(pair.first, absl::ascii_isupper));

      nva.push_back(
        {(uint8_t*)pair.first.data(), (uint8_t*)pair.second.data(),
         pair.first.size(), pair.second.size(),
         NGHTTP2_NV_FLAG_NO_COPY_NAME | NGHTTP2_NV_FLAG_NO_COPY_VALUE});
    }

    if (!have_auth && !_auth_header.empty()) {
      nva.push_back(
        {(uint8_t*)"authorization", (uint8_t*)_auth_header.data(), 13,
         _auth_header.size(),
         NGHTTP2_NV_FLAG_NO_COPY_NAME | NGHTTP2_NV_FLAG_NO_COPY_VALUE});
    }

    nghttp2_data_provider *prd_ptr = nullptr, prd;

    std::string len;
    if (req.header.rest_verb != RestVerb::Get &&
        req.header.rest_verb != RestVerb::Head) {
      len = std::to_string(req.payloadSize());
      nva.push_back({(uint8_t*)"content-length", (uint8_t*)len.c_str(), 14,
                     len.size(), NGHTTP2_NV_FLAG_NO_COPY_NAME});

      if (req.payloadSize() > 0) {
        prd.source.ptr = strm.get();
        prd.read_callback =
          [](nghttp2_session* session, int32_t stream_id, uint8_t* buf,
             size_t length, uint32_t* data_flags, nghttp2_data_source* source,
             void* user_data) -> ssize_t {
          auto strm = static_cast<Stream*>(source->ptr);
          asio_ns::const_buffer payload = strm->request->payload();

          // TODO do not copy the body if it is > 16kb
          SDB_ASSERT(payload.size() > strm->response_offset);
          const uint8_t* src =
            reinterpret_cast<const uint8_t*>(payload.data()) +
            strm->response_offset;
          size_t len = std::min(length, payload.size() - strm->response_offset);
          SDB_ASSERT(len > 0);
          std::copy_n(src, len, buf);

          strm->response_offset += len;
          if (strm->response_offset == payload.size()) {
            *data_flags |= NGHTTP2_DATA_FLAG_EOF;
          }

          return static_cast<ssize_t>(len);
        };
        prd_ptr = &prd;
      }
    }

    int32_t sid = nghttp2_submit_request(_session, /*pri_spec*/ nullptr,
                                         nva.data(), nva.size(), prd_ptr,
                                         /*stream_user_data*/ nullptr);

    if (sid < 0) {
      SDB_ERROR("xxxxx", Logger::FUERTE, "illegal stream id");
      this->shutdownConnection(Error::ProtocolError, "illegal stream id");
      return;
    }
    SDB_TRACE("xxxxx", Logger::FUERTE, "enqueuing stream ", sid, " to ",
              req.header.path);
    this->_streams.emplace(sid, std::move(strm));
    this->_stream_count.fetch_add(1, std::memory_order_relaxed);
  }
}

// writes data from task queue to network using asio_ns::async_write
template<SocketType T>
void H2Connection<T>::DoWrite() {
  SDB_TRACE("xxxxx", Logger::FUERTE, "DoWrite");

  if (this->_writing) {
    return;
  }
  this->_writing = true;

  QueueHttp2Requests();

  static constexpr size_t kMaxOutBufferLen = 32 * 1024 * 1024;
  _outbuffer.resetTo(0);
  _outbuffer.reserve(16 * 1024);

  std::array<asio_ns::const_buffer, 2> out_buffers;
  while (true) {
    const uint8_t* data;
    ssize_t rv = nghttp2_session_mem_send(_session, &data);
    if (rv < 0) {  // error
      this->_writing = false;
      this->_active.store(false);
      SDB_ERROR("xxxxx", Logger::FUERTE, "http2 framing error");
      this->shutdownConnection(Error::ProtocolError, "http2 framing error");
      return;
    } else if (rv == 0) {  // done
      break;
    }

    const size_t nread = static_cast<size_t>(rv);
    // if the data is long we just pass it to async_write
    if (_outbuffer.size() + nread > kMaxOutBufferLen) {
      out_buffers[1] = asio_ns::buffer(data, nread);
      break;
    }

    _outbuffer.append(data, nread);
  }
  out_buffers[0] = asio_ns::buffer(_outbuffer.data(), _outbuffer.size());

  if (asio_ns::buffer_size(out_buffers) == 0) {
    this->_writing = false;
    this->_active.store(false);
    if (ShouldStop()) {
      this->shutdownConnection(Error::CloseRequested,
                               "nothing to write and connection should stop");
    } else if (!this->_queue.empty() && !this->_active.exchange(true)) {
      DoWrite();  // no idea if this can happen
    }
    return;
  }

  // Reset read timer here, because normally client is sending
  // something, it does not expect timeout while doing it.
  this->setIOTimeout();
  asio_ns::async_write(
    this->_proto.socket, out_buffers,
    [self = this->shared_from_this()](const asio_ns::error_code& ec, size_t) {
      auto& me = static_cast<H2Connection<T>&>(*self);
      me._writing = false;
      if (ec) {
        me.shutdownConnection(Error::WriteError);
      } else {
        me.DoWrite();
      }
    });

  SDB_TRACE("xxxxx", Logger::FUERTE, "DoWrite: done");
}

// ------------------------------------
// Reading data
// ------------------------------------

// asyncReadCallback is called when AsyncReadSome is resulting in some data.
template<SocketType T>
void H2Connection<T>::asyncReadCallback(const asio_ns::error_code& ec) {
  if (ec) {
    SDB_DEBUG(
      "xxxxx", Logger::FUERTE,
      "asyncReadCallback: Error while reading from socket: ", ec.message());
    this->shutdownConnection(this->translateError(ec, Error::ReadError));
    return;
  }

  // Inspect the data we've received so far.
  size_t parsed_bytes = 0;
  const auto buffers = this->_receive_buffer.data();
  auto it = asio_ns::buffer_sequence_begin(buffers);
  const auto end = asio_ns::buffer_sequence_end(buffers);
  for (; it != end; ++it) {
    const auto* data = static_cast<const uint8_t*>(it->data());

    ssize_t rv = nghttp2_session_mem_recv(_session, data, it->size());
    if (rv < 0) {
      SDB_ERROR("xxxxx", Logger::FUERTE, "http2 parsing error");
      this->shutdownConnection(Error::ProtocolError, "http2 parsing error");
      return;  // stop read loop
    }

    parsed_bytes += static_cast<size_t>(rv);
  }

  SDB_ASSERT(this->_receive_buffer.size() == parsed_bytes);

  // Remove consumed data from receive buffer.
  this->_receive_buffer.consume(parsed_bytes);
  SDB_TRACE("xxxxx", Logger::FUERTE, "parsed ", parsed_bytes, " bytes");

  DoWrite();

  if (!this->_writing && ShouldStop()) {
    this->shutdownConnection(Error::CloseRequested,
                             "nothing more to read or write on connection");
    return;  // stop read loop
  }

  this->AsyncReadSome();  // Continue read loop
}

/// abort ongoing / unfinished requests (locally)
template<SocketType T>
void H2Connection<T>::abortRequests(fuerte::Error err, Clock::time_point now) {
  auto it = this->_streams.begin();
  while (it != this->_streams.end()) {
    if (it->second->expires <= now) {
      it->second->invokeOnError(err);

      if (now == Clock::time_point::max()) {
        // connection shutdown case, only in this case may we remove the stream
        it = this->_streams.erase(it);
        uint32_t q =
          this->_stream_count.fetch_sub(1, std::memory_order_relaxed);
        SDB_ASSERT(q > 0);
        continue;
      } else {
        // if we expire a request locally we have to tell the framework,
        // otherwise the framework may try to use the stream
        nghttp2_submit_rst_stream(_session, NGHTTP2_FLAG_NONE,
                                  static_cast<int32_t>(it->first),
                                  NGHTTP2_CANCEL);
      }
    }
    it++;
  }

  SDB_ASSERT(now != Clock::time_point::max() || this->_streams.empty());
}

template<SocketType T>
Stream* H2Connection<T>::FindStream(int32_t sid) const {
  const auto& it = this->_streams.find(static_cast<uint64_t>(sid));
  if (it != this->_streams.end()) {
    return it->second.get();
  }
  return nullptr;
}

template<SocketType T>
std::unique_ptr<Stream> H2Connection<T>::EraseStream(int32_t sid) {
  std::unique_ptr<Stream> tmp;
  auto it = this->_streams.find(static_cast<uint64_t>(sid));
  if (it != this->_streams.end()) {
    tmp = std::move(it->second);
    this->_streams.erase(it);
    uint32_t cc = this->_stream_count.fetch_sub(1, std::memory_order_relaxed);
    SDB_ASSERT(cc > 0);
  }
  if (this->_streams.empty()) {
    StartPing();
  }
  return tmp;
}

/// should close connection
template<SocketType T>
bool H2Connection<T>::ShouldStop() const {
  return !nghttp2_session_want_read(_session) &&
         !nghttp2_session_want_write(_session);
}

// ping ensures server does not close the connection
template<SocketType T>
void H2Connection<T>::StartPing() {
  _ping.expires_after(std::chrono::seconds(30));

  _ping.async_wait([self(Connection::weak_from_this())](const auto& ec) {
    std::shared_ptr<Connection> s;
    if (ec || !(s = self.lock())) {
      return;
    }

    auto& me = static_cast<H2Connection<T>&>(*s);
    if (me._state != Connection::State::Connected || !me._streams.empty()) {
      return;
    }
    // queue the ping frame in nghttp2
    nghttp2_submit_ping(me._session, NGHTTP2_FLAG_NONE, nullptr);

    me.DoWrite();    // signal write
    me.StartPing();  // do again in 30s
  });
}

template class H2Connection<SocketType::Tcp>;
template class H2Connection<SocketType::Ssl>;
#ifdef ASIO_HAS_LOCAL_SOCKETS
template class H2Connection<SocketType::Unix>;
#endif

}  // namespace sdb::fuerte::http
