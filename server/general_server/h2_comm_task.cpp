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

#include "h2_comm_task.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/escaping.h>
#include <llhttp.h>
#include <nghttp2/nghttp2.h>

#include <cstring>
#include <thread>

#include "app/app_server.h"
#include "basics/asio_ns.h"
#include "basics/dtrace-wrapper.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "basics/string_buffer.h"
#include "basics/string_utils.h"
#include "general_server/authentication_feature.h"
#include "general_server/general_server.h"
#include "general_server/general_server_feature.h"
#include "general_server/state.h"
#include "rest/http_request.h"
#include "rest/http_response.h"
#include "statistics/connection_statistics.h"
#include "statistics/request_statistics.h"

using namespace sdb::basics;
using std::string_view;

namespace {

constexpr std::string_view kSwitchingProtocols(
  "HTTP/1.1 101 Switching Protocols\r\nConnection: "
  "Upgrade\r\nUpgrade: h2c\r\n\r\n");

bool ExpectResponseBody(int status_code) {
  return status_code == 101 ||
         (status_code / 100 != 1 && status_code != 304 && status_code != 204);
}

}  // namespace

#ifdef USE_DTRACE
// Moved out to avoid duplication by templates.
static void __attribute__((noinline)) DTraceH2CommTaskSendResponse(size_t th) {
  DTRACE_PROBE1(serened, H2CommTaskSendResponse, th);
}
#else
static void DTraceH2CommTaskSendResponse(size_t) {}
#endif

namespace sdb::rest {

struct H2Response : public HttpResponse {
  H2Response(ResponseCode code, uint64_t mid)
    : HttpResponse(code, mid, nullptr, rest::ResponseCompressionType::Unset) {}

  RequestStatistics::Item statistics;
};

template<SocketType T>
/*static*/ int H2CommTask<T>::on_begin_headers(nghttp2_session* session,
                                               const nghttp2_frame* frame,
                                               void* user_data) try {
  H2CommTask<T>* me = static_cast<H2CommTask<T>*>(user_data);

  if (frame->hd.type != NGHTTP2_HEADERS ||
      frame->headers.cat != NGHTTP2_HCAT_REQUEST) {
    return HPE_OK;
  }

  const int32_t sid = frame->hd.stream_id;
  me->AcquireRequestStatistics(sid).SET_READ_START(utilities::GetMicrotime());
  auto req =
    std::make_unique<HttpRequest>(me->_connection_info, /*messageId*/ sid);
  me->CreateStream(sid, std::move(req));

  SDB_TRACE("xxxxx", Logger::REQUESTS, "<http2> creating new stream ", sid);

  return HPE_OK;
} catch (...) {
  // the caller of this function is a C function, which doesn't know
  // exceptions. we must not let an exception escape from here.
  return HPE_INTERNAL;
}

template<SocketType T>
/*static*/ int H2CommTask<T>::on_header(nghttp2_session* session,
                                        const nghttp2_frame* frame,
                                        const uint8_t* name, size_t namelen,
                                        const uint8_t* value, size_t valuelen,
                                        uint8_t flags, void* user_data) try {
  H2CommTask<T>* me = static_cast<H2CommTask<T>*>(user_data);
  const int32_t sid = frame->hd.stream_id;

  if (frame->hd.type != NGHTTP2_HEADERS ||
      frame->headers.cat != NGHTTP2_HCAT_REQUEST) {
    return HPE_OK;
  }

  Stream* strm = me->FindStream(sid);
  if (!strm) {
    return HPE_OK;
  }

  // prevent stream headers from becoming too large
  strm->header_buff_size += namelen + valuelen;
  if (strm->header_buff_size > 64 * 1024 * 1024) {
    return nghttp2_submit_rst_stream(me->_session, NGHTTP2_FLAG_NONE, sid,
                                     NGHTTP2_INTERNAL_ERROR);
  }

  // handle pseudo headers
  // https://http2.github.io/http2-spec/#rfc.section.8.1.2.3
  std::string_view field(reinterpret_cast<const char*>(name), namelen);
  std::string_view val(reinterpret_cast<const char*>(value), valuelen);

  if (std::string_view(":method") == field) {
    strm->request->setRequestType(GeneralRequest::translateMethod(val));
  } else if (std::string_view(":scheme") == field) {
    // simon: ignore, should contain 'http' or 'https'
  } else if (std::string_view(":path") == field) {
    strm->request->parseUrl(reinterpret_cast<const char*>(value), valuelen);
  } else if (std::string_view(":authority") == field) {
    // simon: ignore, could treat like "Host" header
  } else {  // fall through
    strm->request->setHeader(std::string(field), std::string(val));
  }

  return HPE_OK;
} catch (...) {
  // the caller of this function is a C function, which doesn't know
  // exceptions. we must not let an exception escape from here.
  return HPE_INTERNAL;
}

template<SocketType T>
/*static*/ int H2CommTask<T>::on_frame_recv(nghttp2_session* session,
                                            const nghttp2_frame* frame,
                                            void* user_data) try {
  H2CommTask<T>* me = static_cast<H2CommTask<T>*>(user_data);

  switch (frame->hd.type) {
    case NGHTTP2_DATA:  // GET or HEAD do not use DATA frames
    case NGHTTP2_HEADERS: {
      if (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) {
        const int32_t sid = frame->hd.stream_id;

        Stream* strm = me->FindStream(sid);
        if (strm) {
          me->ProcessStream(*strm);
        }
      }
      break;
    }
  }

  return HPE_OK;
} catch (...) {
  // the caller of this function is a C function, which doesn't know
  // exceptions. we must not let an exception escape from here.
  return HPE_INTERNAL;
}

template<SocketType T>
/*static*/ int H2CommTask<T>::on_data_chunk_recv(
  nghttp2_session* session, uint8_t flags, int32_t stream_id,
  const uint8_t* data, size_t len, void* user_data) try {
  SDB_TRACE("xxxxx", Logger::REQUESTS, "<http2> received data for stream ",
            stream_id);
  H2CommTask<T>* me = static_cast<H2CommTask<T>*>(user_data);
  Stream* strm = me->FindStream(stream_id);
  if (strm) {
    strm->request->appendBody(reinterpret_cast<const char*>(data), len);
  }

  return HPE_OK;
} catch (...) {
  // the caller of this function is a C function, which doesn't know
  // exceptions. we must not let an exception escape from here.
  return HPE_INTERNAL;
}

template<SocketType T>
/*static*/ int H2CommTask<T>::on_stream_close(nghttp2_session* session,
                                              int32_t stream_id,
                                              uint32_t error_code,
                                              void* user_data) try {
  H2CommTask<T>* me = static_cast<H2CommTask<T>*>(user_data);
  auto it = me->_streams.find(stream_id);
  if (it != me->_streams.end()) {
    Stream& strm = it->second;
    if (strm.response) {
      auto* h2_response = dynamic_cast<H2Response*>(strm.response.get());
      if (h2_response != nullptr) {
        h2_response->statistics.SET_WRITE_END();
      }
    }
    me->_streams.erase(it);
  }

  if (error_code != NGHTTP2_NO_ERROR) {
    SDB_DEBUG("xxxxx", Logger::REQUESTS, "<http2> closing stream ", stream_id,
              " with error '", nghttp2_http2_strerror(error_code), "' (",
              error_code, ")");
  }

  return HPE_OK;
} catch (...) {
  // the caller of this function is a C function, which doesn't know
  // exceptions. we must not let an exception escape from here.
  return HPE_INTERNAL;
}

template<SocketType T>
/*static*/ int H2CommTask<T>::on_frame_send(nghttp2_session* session,
                                            const nghttp2_frame* frame,
                                            void* user_data) {
  // can be used for push promises
  return HPE_OK;
}

template<SocketType T>
/*static*/ int H2CommTask<T>::on_frame_not_send(nghttp2_session* session,
                                                const nghttp2_frame* frame,
                                                int lib_error_code,
                                                void* user_data) try {
  if (frame->hd.type != NGHTTP2_HEADERS) {
    return HPE_OK;
  }

  const int32_t sid = frame->hd.stream_id;
  SDB_DEBUG("xxxxx", Logger::REQUESTS, "sending RST on stream ", sid,
            " with error '", nghttp2_strerror(lib_error_code), "' (",
            lib_error_code, ")");

  // Issue RST_STREAM so that stream does not hang around.
  nghttp2_submit_rst_stream(session, NGHTTP2_FLAG_NONE, sid,
                            NGHTTP2_INTERNAL_ERROR);

  return HPE_OK;
} catch (...) {
  // the caller of this function is a C function, which doesn't know
  // exceptions. we must not let an exception escape from here.
  return HPE_INTERNAL;
}

template<SocketType T>
H2CommTask<T>::H2CommTask(GeneralServer& server, ConnectionInfo info,
                          std::shared_ptr<AsioSocket<T>> so)
  : GeneralCommTask<T>(server, std::move(info), std::move(so)) {
  this->_connection_statistics.SET_HTTP();
  this->_general_server_feature.countHttp2Connection();
  InitNgHttp2Session();
}

template<SocketType T>
H2CommTask<T>::~H2CommTask() {
  nghttp2_session_del(_session);
  _session = nullptr;
  if (!_streams.empty()) {
    SDB_DEBUG("xxxxx", Logger::REQUESTS, "<http2> got ", _streams.size(),
              " remaining streams");
  }
  HttpResponse* res = nullptr;
  while (_responses.pop(res)) {
    delete res;
  }

  SDB_DEBUG("xxxxx", Logger::REQUESTS, "<http2> closing connection \"",
            std::bit_cast<size_t>(this), "\"");
}

namespace {

int OnErrorCallback(nghttp2_session* session, int lib_error_code,
                    const char* msg, size_t len, void*) try {
  // use INFO log level, its still hidden by default
  SDB_INFO("xxxxx", Logger::REQUESTS, "http2 connection error: \"",
           std::string_view(msg, len), "\" (", lib_error_code, ")");
  return HPE_OK;
} catch (...) {
  // the caller of this function is a C function, which doesn't know
  // exceptions. we must not let an exception escape from here.
  return HPE_INTERNAL;
}

int OnInvalidFrameRecv(nghttp2_session* session, const nghttp2_frame* frame,
                       int lib_error_code, void* user_data) try {
  SDB_INFO("xxxxx", Logger::REQUESTS, "received illegal data frame on stream ",
           frame->hd.stream_id, ": '", nghttp2_strerror(lib_error_code), "' (",
           lib_error_code, ")");
  return HPE_OK;
} catch (...) {
  // the caller of this function is a C function, which doesn't know
  // exceptions. we must not let an exception escape from here.
  return HPE_INTERNAL;
}

constexpr uint32_t kWindowSize = (1 << 30) - 1;  // 1 GiB
void SubmitConnectionPreface(nghttp2_session* session) {
  std::array<nghttp2_settings_entry, 4> iv;
  // 32 streams matches the queue capacity
  iv[0] = {NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS,
           sdb::kH2MaxConcurrentStreams};
  // typically client is just a *sink* and just process data as
  // much as possible.  Use large window size by default.
  iv[1] = {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, kWindowSize};
  iv[2] = {NGHTTP2_SETTINGS_MAX_FRAME_SIZE, (1 << 14)};  // 16k
  iv[3] = {NGHTTP2_SETTINGS_ENABLE_PUSH, 0};

  nghttp2_submit_settings(session, NGHTTP2_FLAG_NONE, iv.data(), iv.size());
  // increase connection window size up to window_size
  //  nghttp2_session_set_local_window_size(session, NGHTTP2_FLAG_NONE, 0, 1 <<
  //  30);
}

ssize_t DataSourceReadLengthCallback(nghttp2_session* session,
                                     uint8_t frame_type, int32_t stream_id,
                                     int32_t session_remote_window_size,
                                     int32_t stream_remote_window_size,
                                     uint32_t remote_max_frame_size,
                                     void* user_data) {
  SDB_TRACE("xxxxx", Logger::REQUESTS,
            "session_remote_window_size: ", session_remote_window_size,
            ", stream_remote_window_size: ", stream_remote_window_size,
            ", remote_max_frame_size: ", remote_max_frame_size);
  return (1 << 16);  // 64kB
}

}  // namespace

/// init h2 session
template<SocketType T>
void H2CommTask<T>::InitNgHttp2Session() {
  nghttp2_session_callbacks* callbacks;
  int rv = nghttp2_session_callbacks_new(&callbacks);
  if (rv != 0) {
    SDB_THROW(ERROR_OUT_OF_MEMORY);
  }

  absl::Cleanup cb_scope = [&]() noexcept {
    nghttp2_session_callbacks_del(callbacks);
  };

  nghttp2_session_callbacks_set_on_begin_headers_callback(
    callbacks, H2CommTask<T>::on_begin_headers);
  nghttp2_session_callbacks_set_on_header_callback(callbacks,
                                                   H2CommTask<T>::on_header);
  nghttp2_session_callbacks_set_on_frame_recv_callback(
    callbacks, H2CommTask<T>::on_frame_recv);
  nghttp2_session_callbacks_set_on_data_chunk_recv_callback(
    callbacks, H2CommTask<T>::on_data_chunk_recv);
  nghttp2_session_callbacks_set_on_stream_close_callback(
    callbacks, H2CommTask<T>::on_stream_close);
  nghttp2_session_callbacks_set_on_frame_send_callback(
    callbacks, H2CommTask<T>::on_frame_send);
  nghttp2_session_callbacks_set_on_frame_not_send_callback(
    callbacks, H2CommTask<T>::on_frame_not_send);
  nghttp2_session_callbacks_set_on_invalid_frame_recv_callback(
    callbacks, OnInvalidFrameRecv);
  nghttp2_session_callbacks_set_error_callback2(callbacks, OnErrorCallback);
  nghttp2_session_callbacks_set_data_source_read_length_callback(
    callbacks, DataSourceReadLengthCallback);

  rv = nghttp2_session_server_new(&_session, callbacks, /*args*/ this);
  if (rv != 0) {
    SDB_THROW(ERROR_OUT_OF_MEMORY);
  }
}

template<SocketType T>
void H2CommTask<T>::UpgradeHttp1(std::unique_ptr<HttpRequest> req) {
  bool found;
  const std::string& settings = req->header("http2-settings", found);
  const bool was_head = req->requestType() == RequestType::Head;

  std::string decoded;
  absl::Base64Unescape(settings, &decoded);
  const uint8_t* src = reinterpret_cast<const uint8_t*>(decoded.data());
  int rv =
    nghttp2_session_upgrade2(_session, src, decoded.size(), was_head, nullptr);

  if (rv != 0) {
    // The settings_payload is badly formed.
    SDB_INFO("xxxxx", Logger::REQUESTS, "error during HTTP2 upgrade: \"",
             nghttp2_strerror((int)rv), "\" (", rv, ")");
    this->Close();
    return;
  }

  // https://http2.github.io/http2-spec/#discover-http
  auto buffer =
    asio_ns::buffer(::kSwitchingProtocols.data(), ::kSwitchingProtocols.size());
  asio_ns::async_write(
    this->_protocol->socket, buffer,
    [self(this->shared_from_this()), request(std::move(req))](
      const asio_ns::error_code& ec, size_t nwrite) mutable {
      auto& me = static_cast<H2CommTask<T>&>(*self);
      if (ec) {
        me.Close(ec);
        return;
      }

      SubmitConnectionPreface(me._session);

      // The HTTP/1.1 request that is sent prior to upgrade is assigned
      // a stream identifier of 1 (see Section 5.1.1).
      // Stream 1 is implicitly "half-closed" from the client toward the
      // server

      SDB_ASSERT(request->messageId() == 1);
      auto* strm = me.CreateStream(1, std::move(request));
      SDB_ASSERT(strm);

      // will start writing later
      me.ProcessStream(*strm);

      // start reading
      me.AsyncReadSome();
    });
}

template<SocketType T>
void H2CommTask<T>::Start() {
  SDB_DEBUG("xxxxx", Logger::REQUESTS, "<http2> opened connection \"",
            std::bit_cast<size_t>(this), "\"");

  asio_ns::post(this->_protocol->context.io_context,
                [self = this->shared_from_this()] {
                  auto& me = static_cast<H2CommTask<T>&>(*self);

                  // queueing the server connection preface,
                  // which always consists of a SETTINGS frame.
                  SubmitConnectionPreface(me._session);

                  me.DoWrite();        // write out preface
                  me.AsyncReadSome();  // start reading
                });
}

template<SocketType T>
bool H2CommTask<T>::ReadCallback(asio_ns::error_code ec) {
  if (ec) {
    this->Close(ec);
    return false;  // stop read loop
  }

  size_t parsed_bytes = 0;
  const auto buffers = this->_protocol->buffer.data();
  auto it = asio_ns::buffer_sequence_begin(buffers);
  const auto end = asio_ns::buffer_sequence_end(buffers);
  for (; it != end; ++it) {
    const auto* data = static_cast<const uint8_t*>(it->data());

    auto rv = nghttp2_session_mem_recv(_session, data, it->size());
    if (rv < 0 || static_cast<size_t>(rv) != it->size()) {
      SDB_INFO("xxxxx", Logger::REQUESTS, "HTTP2 parsing error: \"",
               nghttp2_strerror((int)rv), "\" (", rv, ")");
      this->Close(ec);
      return false;  // stop read loop
    }

    parsed_bytes += static_cast<size_t>(rv);
  }

  SDB_ASSERT(parsed_bytes < std::numeric_limits<size_t>::max());
  // Remove consumed data from receive buffer.
  this->_protocol->buffer.consume(parsed_bytes);

  DoWrite();

  if (!this->_writing && ShouldStop()) {
    this->Close();
    return false;  // stop read loop
  }

  return true;  //  continue read lopp
}

template<SocketType T>
void H2CommTask<T>::SetIOTimeout() {
  double secs = this->_general_server_feature.keepAliveTimeout();
  if (secs <= 0) {
    return;
  }

  const bool was_reading = this->_reading;
  const bool was_writing = this->_writing;
  SDB_ASSERT(was_reading || was_writing);
  if (was_writing) {
    secs = std::max(kWriteTimeout, secs);
  }

  auto millis = std::chrono::milliseconds(static_cast<int64_t>(secs * 1000));
  this->_protocol->timer.expires_after(millis);  // cancels old waiters
  this->_protocol->timer.async_wait(
    [=, this,
     self = CommTask::weak_from_this()](const asio_ns::error_code& ec) {
      std::shared_ptr<CommTask> s;
      if (ec || !(s = self.lock())) {  // was canceled / deallocated
        return;
      }

      auto& me = static_cast<H2CommTask<T>&>(*s);

      bool idle = was_reading && me._reading && !me._writing;
      bool write_timeout = was_writing && me._writing;
      if (idle || write_timeout) {
        // _num_processing == 0 also if responses wait for writing
        if (me._num_processing.load(std::memory_order_relaxed) == 0) {
          SDB_INFO("xxxxx", Logger::REQUESTS,
                   "keep alive timeout, closing stream!");
          static_cast<GeneralCommTask<T>&>(*s).Close(ec);
        } else {
          SetIOTimeout();
        }
      }
      // In all other cases we do nothing, since we have been posted to the
      // iocontext but the thing we should be timing out has already
      // completed.
    });
}

#ifdef USE_DTRACE
// Moved out to avoid duplication by templates.
static void __attribute__((noinline)) DTraceH2CommTaskProcessStream(size_t th) {
  DTRACE_PROBE1(serened, H2CommTaskProcessStream, th);
}
#else
static void DTraceH2CommTaskProcessStream(size_t) {}
#endif

template<SocketType T>
std::string H2CommTask<T>::url(const HttpRequest* req) const {
  if (req != nullptr) {
    return std::string(
             (req->databaseName().empty()
                ? ""
                : "/_db/" + string_utils::UrlEncode(req->databaseName()))) +
           (log::GetLogRequestParameters() ? req->fullUrl()
                                           : req->requestPath());
  }
  return "";
}

template<SocketType T>
void H2CommTask<T>::ProcessStream(Stream& stream) {
  DTraceH2CommTaskProcessStream((size_t)this);

  if (!stream.request->header("x-omit-www-authenticate").empty()) {
    stream.must_send_auth_header = false;
  } else {
    stream.must_send_auth_header = true;
  }
  std::unique_ptr<HttpRequest> req = std::move(stream.request);

  auto msg_id = req->messageId();
  auto resp_content_type = req->contentTypeResponse();
  try {
    ProcessRequest(stream, std::move(req));
  } catch (const sdb::basics::Exception& ex) {
    SDB_WARN("xxxxx", Logger::REQUESTS, "request failed with error ", ex.code(),
             " ", ex.message());
    this->SendErrorResponse(GeneralResponse::responseCode(ex.code()),
                            resp_content_type, msg_id, ex.code(), ex.message());
  } catch (const std::exception& ex) {
    SDB_WARN("xxxxx", Logger::REQUESTS, "request failed with error ",
             ex.what());
    this->SendErrorResponse(ResponseCode::ServerError, resp_content_type,
                            msg_id, ErrorCode(ERROR_FAILED), ex.what());
  }
}

template<SocketType T>
void H2CommTask<T>::ProcessRequest(Stream& stream,
                                   std::unique_ptr<HttpRequest> req) {
  // ensure there is a null byte termination. RestHandlers use
  // C functions like strchr that except a C string as input
  req->appendNullTerminator();

  if (this->Stopped()) {
    return;  // we have to ignore this request because the connection has
             // already been closed
  }

  // from here on we will send a response, the connection is not IDLE
  _num_processing.fetch_add(1, std::memory_order_relaxed);
  {
    SDB_INFO("xxxxx", Logger::REQUESTS, "\"h2-request-begin\",\"",
             std::bit_cast<size_t>(this), "\",\"",
             this->_connection_info.client_address, "\",\"",
             HttpRequest::translateMethod(req->requestType()), "\",\"",
             url(req.get()), "\"");

    std::string_view body = req->rawPayload();
    this->_general_server_feature.countHttp2Request(body.size());
    if (log::IsEnabled(LogLevel::TRACE, Logger::REQUESTS) &&
        log::GetLogRequestParameters()) {
      // Log HTTP headers:
      this->LogRequestHeaders("h2", req->headers());

      if (!body.empty()) {
        this->LogRequestBody("h2", req->contentType(), body);
      }
    }
  }

  // store origin header for later use
  stream.origin = req->header(StaticStrings::kOrigin);
  auto message_id = req->messageId();
  const auto& stat = this->GetRequestStatistics(message_id);
  stat.SET_REQUEST_TYPE(req->requestType());
  stat.ADD_RECEIVED_BYTES(stream.header_buff_size + req->body().size());
  stat.SET_READ_END();
  stat.SET_WRITE_START();

  // OPTIONS requests currently go unauthenticated
  if (req->requestType() == rest::RequestType::Options) {
    this->ProcessCorsOptions(std::move(req), stream.origin);
    return;
  }

  ServerState::Mode mode = ServerState::instance()->GetMode();

  // scrape the auth headers to determine and authenticate the user
  auto auth_token = this->CheckAuthHeader(*req, mode);

  // We want to separate superuser token traffic:
  if (req->authenticated() && req->user().empty()) {
    stat.SET_SUPERUSER();
  }

  // first check whether we allow the request to continue
  Flow cont = this->PrepareExecution(auth_token, *req, mode);
  if (cont != Flow::Continue) {
    return;  // PrepareExecution sends the error message
  }

  // gzip-uncompress / zlib-deflate / lz4-uncompress
  if (Result res = this->HandleContentEncoding(*req); res.fail()) {
    this->SendErrorResponse(rest::ResponseCode::Bad, req->contentTypeResponse(),
                            1, ERROR_BAD_PARAMETER, res.errorMessage());
    return;
  }

  // create a handler and execute
  auto resp =
    std::make_unique<H2Response>(rest::ResponseCode::ServerError, message_id);
  resp->setContentType(req->contentTypeResponse());
  this->ExecuteRequest(std::move(req), std::move(resp), mode);
}

template<SocketType T>
void H2CommTask<T>::SendResponse(std::unique_ptr<GeneralResponse> res,
                                 RequestStatistics::Item stat) {
  DTraceH2CommTaskSendResponse((size_t)this);

  unsigned n = _num_processing.fetch_sub(1, std::memory_order_relaxed);
  SDB_ASSERT(n > 0);

  if (this->Stopped()) {
    return;
  }

  // note: the response we get here can either be an H2Response (HTTP/2) or an
  // HTTP/1 response!
  auto* tmp = static_cast<HttpResponse*>(res.get());

  // handle response code 204 No Content
  if (tmp->responseCode() == rest::ResponseCode::NoContent) {
    tmp->clearBody();
  }

  if (log::IsEnabled(LogLevel::TRACE, Logger::REQUESTS) &&
      log::GetLogRequestParameters()) {
    const auto& body_buf = tmp->body();
    std::string_view body{body_buf.data(), body_buf.size()};
    if (!body.empty()) {
      this->LogRequestBody("h2", res->contentType(), body,
                           true /* isResponse */);
    }
  }

  // and give some request information
  SDB_DEBUG("xxxxx", Logger::REQUESTS, "\"h2-request-end\",\"",
            std::bit_cast<size_t>(this), "\",\"",
            this->_connection_info.client_address, "\",\"", url(nullptr),
            "\",\"", static_cast<int>(res->responseCode()), "\",",
            absl::StrFormat("%.6f", stat.ELAPSED_SINCE_READ_START()), ",",
            absl::StrFormat("%.6f", stat.ELAPSED_WHILE_QUEUED()));

  auto* h2_response = dynamic_cast<H2Response*>(tmp);
  if (h2_response != nullptr) {
    h2_response->statistics = std::move(stat);
  }

  // this uses a fixed capacity queue, push might fail (unlikely, we limit max
  // streams)
  unsigned retries = 512;
  try {
    while (SDB_UNLIKELY(!_responses.push(tmp) && --retries > 0)) {
      std::this_thread::yield();
    }
  } catch (...) {
    retries = 0;
  }
  if (retries == 0) {
    SDB_WARN("xxxxx", Logger::REQUESTS, "was not able to queue response this=",
             std::bit_cast<size_t>(this));
    // we are overloaded close stream
    asio_ns::post(this->_protocol->context.io_context,
                  [self(this->shared_from_this()), mid(res->messageId())] {
                    auto& me = static_cast<H2CommTask<T>&>(*self);
                    nghttp2_submit_rst_stream(me._session, NGHTTP2_FLAG_NONE,
                                              static_cast<int32_t>(mid),
                                              NGHTTP2_ENHANCE_YOUR_CALM);
                  });
    return;
  }
  res.release();

  // avoid using asio_ns::post if possible
  bool signaled = _signaled_write.load();
  if (!signaled && !_signaled_write.exchange(true)) {
    asio_ns::post(this->_protocol->context.io_context,
                  [self = this->shared_from_this()] {
                    auto& me = static_cast<H2CommTask<T>&>(*self);
                    me._signaled_write.store(false);
                    me.DoWrite();
                  });
  }
}

// queue the response onto the session, call only on IO thread
template<SocketType T>
void H2CommTask<T>::QueueHttp2Responses() {
  HttpResponse* response = nullptr;
  while (_responses.pop(response)) {
    std::unique_ptr<HttpResponse> guard(response);

    int32_t stream_id = static_cast<int32_t>(response->messageId());
    Stream* strm = FindStream(stream_id);
    if (strm == nullptr) {  // stream was already closed for some reason
      SDB_DEBUG("xxxxx", Logger::REQUESTS, "response with message id '",
                stream_id, "' has no H2 stream on server");
      return;
    }
    strm->response = std::move(guard);
    auto& res = *response;

    // will add CORS headers if necessary
    this->FinishExecution(res, strm->origin);

    if (log::IsEnabled(LogLevel::TRACE, Logger::REQUESTS) &&
        log::GetLogRequestParameters()) {
      this->LogResponseHeaders("h2", res.headers());
    }

    // we need a continuous block of memory for headers
    std::vector<nghttp2_nv> nva;
    nva.reserve(4 + res.headers().size());

    std::string status = std::to_string(static_cast<int>(res.responseCode()));
    nva.push_back({(uint8_t*)":status", (uint8_t*)status.data(), 7,
                   status.size(), NGHTTP2_NV_FLAG_NO_COPY_NAME});

    // if we return HTTP 401, we need to send a www-authenticate header back
    // with the response. in this case we need to check if the header was
    // already set or if we need to set it ourselves. note that clients can
    // suppress sending the www-authenticate header by sending us an
    // x-omit-www-authenticate header.
    bool need_www_authenticate =
      (this->_auth->isActive() &&
       res.responseCode() == rest::ResponseCode::Unauthorized &&
       strm->must_send_auth_header);

    bool seen_server_header = false;
    for (const auto& it : res.headers()) {
      const std::string& key = it.first;
      const std::string& val = it.second;

      // ignore content-length
      if (key == StaticStrings::kContentLength ||
          key == StaticStrings::kConnection ||
          key == StaticStrings::kTransferEncoding) {
        continue;
      }

      if (key == StaticStrings::kServer) {
        seen_server_header = true;
      } else if (need_www_authenticate &&
                 key == StaticStrings::kWwwAuthenticate) {
        need_www_authenticate = false;
      }

      nva.push_back(
        {(uint8_t*)key.data(), (uint8_t*)val.data(), key.size(), val.size(),
         NGHTTP2_NV_FLAG_NO_COPY_NAME | NGHTTP2_NV_FLAG_NO_COPY_VALUE});
    }

    // add "Server" response header
    if (!seen_server_header && this->_is_user_request) {
      nva.push_back(
        {(uint8_t*)"server", (uint8_t*)"SereneDB", 6, 8,
         NGHTTP2_NV_FLAG_NO_COPY_NAME | NGHTTP2_NV_FLAG_NO_COPY_VALUE});
    }

    if (need_www_authenticate) {
      SDB_ASSERT(res.responseCode() == rest::ResponseCode::Unauthorized);
      nva.push_back(
        {(uint8_t*)"www-authenticate", (uint8_t*)"Basic, realm=\"SereneDB\"",
         16, 23, NGHTTP2_NV_FLAG_NO_COPY_NAME | NGHTTP2_NV_FLAG_NO_COPY_VALUE});

      nva.push_back(
        {(uint8_t*)"www-authenticate",
         (uint8_t*)"Bearer, token_type=\"JWT\", realm=\"SereneDB\"", 16, 42,
         NGHTTP2_NV_FLAG_NO_COPY_NAME | NGHTTP2_NV_FLAG_NO_COPY_VALUE});
    }

    for (const std::string& cookie : res.cookies()) {
      nva.push_back(
        {(uint8_t*)"set-cookie", (uint8_t*)cookie.data(), 10, cookie.size(),
         NGHTTP2_NV_FLAG_NO_COPY_NAME | NGHTTP2_NV_FLAG_NO_COPY_VALUE});
    }

    std::string type;
    if (res.contentType() != ContentType::Custom) {
      type = rest::ContentTypeToString(res.contentType());
      nva.push_back({(uint8_t*)"content-type", (uint8_t*)type.c_str(), 12,
                     type.length(), NGHTTP2_NV_FLAG_NO_COPY_NAME});
    }

    std::string len;
    nghttp2_data_provider *prd_ptr = nullptr, prd;
    if (!res.generateBody() ||
        ExpectResponseBody(static_cast<int>(res.responseCode()))) {
      len = std::to_string(res.bodySize());
      nva.push_back({(uint8_t*)"content-length", (uint8_t*)len.c_str(), 14,
                     len.size(), NGHTTP2_NV_FLAG_NO_COPY_NAME});
    }

    if ((res.bodySize() > 0) && res.generateBody() &&
        ExpectResponseBody(static_cast<int>(res.responseCode()))) {
      prd.source.ptr = strm;
      prd.read_callback = [](nghttp2_session* session, int32_t stream_id,
                             uint8_t* buf, size_t length, uint32_t* data_flags,
                             nghttp2_data_source* source,
                             void* user_data) -> ssize_t {
        auto strm = static_cast<H2CommTask<T>::Stream*>(source->ptr);

        basics::StringBuffer& body = strm->response->body();

        // TODO do not copy the body if it is > 16kb
        SDB_ASSERT(body.size() > strm->response_offset);
        auto nread = std::min(length, body.size() - strm->response_offset);

        const char* src = body.data() + strm->response_offset;
        std::copy_n(src, nread, buf);
        strm->response_offset += nread;

        if (strm->response_offset == body.size()) {
          *data_flags |= NGHTTP2_DATA_FLAG_EOF;
        }

        // simon: might be needed if NGHTTP2_DATA_FLAG_NO_COPY is used
        //      if (nghttp2_session_get_stream_remote_close(session, stream_id)
        //      == 0) {
        //          nghttp2_submit_rst_stream(session, NGHTTP2_FLAG_NONE,
        //          stream_id, NGHTTP2_NO_ERROR);
        //      }
        return static_cast<ssize_t>(nread);
      };
      prd_ptr = &prd;
    }

    // we may have an HTTP/1 response here or an HTTP/2 response.
    // try if upcasting works, and only if so, treat it as HTTP/2.
    auto* h2_response = dynamic_cast<H2Response*>(&res);
    if (h2_response != nullptr) {
      h2_response->statistics.ADD_SENT_BYTES(res.bodySize());
    }

    int rv = nghttp2_submit_response(this->_session, stream_id, nva.data(),
                                     nva.size(), prd_ptr);
    if (rv != 0) {
      SDB_INFO("xxxxx", sdb::Logger::REQUESTS,
               "HTTP2 submit_response error: \"", nghttp2_strerror((int)rv),
               "\" (", rv, ")");
      this->Close();
      return;
    }
  }
}

#ifdef USE_DTRACE
// Moved out to avoid duplication by templates.
static void __attribute__((noinline)) DTraceH2CommTaskBeforeAsyncWrite(
  size_t th) {
  DTRACE_PROBE1(serened, H2CommTaskBeforeAsyncWrite, th);
}
static void __attribute__((noinline)) DTraceH2CommTaskAfterAsyncWrite(
  size_t th) {
  DTRACE_PROBE1(serened, H2CommTaskAfterAsyncWrite, th);
}
#else
static void DTraceH2CommTaskBeforeAsyncWrite(size_t) {}
static void DTraceH2CommTaskAfterAsyncWrite(size_t) {}
#endif

// called on IO context thread
template<SocketType T>
void H2CommTask<T>::DoWrite() {
  if (this->_writing) {
    return;
  }
  this->_writing = true;

  QueueHttp2Responses();

  static constexpr size_t kMaxOutBufferLen = 128 * 1024;
  _outbuffer.resetTo(0);
  _outbuffer.reserve(16 * 1024);
  SDB_ASSERT(_outbuffer.size() == 0);

  std::array<asio_ns::const_buffer, 2> out_buffers;
  while (true) {
    const uint8_t* data;
    auto rv = nghttp2_session_mem_send(_session, &data);
    if (rv < 0) {  // error
      this->_writing = false;
      SDB_INFO("xxxxx", sdb::Logger::REQUESTS, "HTTP2 framing error: \"",
               nghttp2_strerror((int)rv), "\" (", rv, ")");
      this->Close();
      return;
    }

    if (rv == 0) {  // done
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
    if (ShouldStop()) {
      this->Close();
    }
    return;
  }

  // Reset read timer here, because normally client is sending
  // something, it does not expect timeout while doing it.
  SetIOTimeout();

  DTraceH2CommTaskBeforeAsyncWrite((size_t)this);
  asio_ns::async_write(this->_protocol->socket, out_buffers,
                       [self = this->shared_from_this()](
                         const asio_ns::error_code& ec, size_t nwrite) {
                         auto& me = static_cast<H2CommTask<T>&>(*self);
                         me._writing = false;
                         if (ec) {
                           me.Close(ec);
                           return;
                         }

                         DTraceH2CommTaskAfterAsyncWrite((size_t)self.get());

                         me.DoWrite();
                       });
}

template<SocketType T>
std::unique_ptr<GeneralResponse> H2CommTask<T>::CreateResponse(
  rest::ResponseCode response_code, uint64_t mid) {
  return std::make_unique<H2Response>(response_code, mid);
}

template<SocketType T>
typename H2CommTask<T>::Stream* H2CommTask<T>::CreateStream(
  int32_t sid, std::unique_ptr<HttpRequest> req) {
  SDB_ASSERT(static_cast<uint64_t>(sid) == req->messageId());
  auto [it, inserted] = _streams.emplace(sid, Stream{std::move(req)});
  SDB_ASSERT(inserted == true);
  return &it->second;
}

template<SocketType T>
typename H2CommTask<T>::Stream* H2CommTask<T>::FindStream(int32_t sid) {
  const auto& it = _streams.find(sid);
  if (it != _streams.end()) {
    return &it->second;
  }
  return nullptr;
}

/// should close connection
template<SocketType T>
bool H2CommTask<T>::ShouldStop() const {
  return !nghttp2_session_want_read(_session) &&
         !nghttp2_session_want_write(_session);
}

template class sdb::rest::H2CommTask<SocketType::Tcp>;
template class sdb::rest::H2CommTask<SocketType::Ssl>;
template class sdb::rest::H2CommTask<SocketType::Unix>;

}  // namespace sdb::rest
