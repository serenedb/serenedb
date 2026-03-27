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

#include "http_comm_task.h"

#include <absl/strings/match.h>

#include <cstring>

#include "app/app_server.h"
#include "basics/asio_ns.h"
#include "basics/dtrace-wrapper.h"
#include "basics/logger/logger.h"
#include "basics/string_buffer.h"
#include "general_server/general_server.h"
#include "general_server/general_server_feature.h"
#include "general_server/h2_comm_task.h"
#include "general_server/state.h"
#include "rest/http_request.h"
#include "rest/http_response.h"
#include "statistics/connection_statistics.h"
#include "statistics/request_statistics.h"

using namespace sdb;
using namespace sdb::basics;
using namespace sdb::rest;

namespace {

using namespace sdb;
using namespace sdb::rest;

rest::RequestType LlhttpToRequestType(llhttp_t* p) {
  switch (p->method) {
    case HTTP_DELETE:
      return RequestType::DeleteReq;
    case HTTP_GET:
      return RequestType::Get;
    case HTTP_HEAD:
      return RequestType::Head;
    case HTTP_POST:
      return RequestType::Post;
    case HTTP_PUT:
      return RequestType::Put;
    case HTTP_OPTIONS:
      return RequestType::Options;
    case HTTP_PATCH:
      return RequestType::Patch;
    default:
      return RequestType::Illegal;
  }
}

static constexpr const char* kVst10 = "VST/1.0\r\n\r\n";
static constexpr const char* kVst11 = "VST/1.1\r\n\r\n";
static constexpr const char* kH2Preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
static constexpr size_t kVstLen = 11;        // length of vst connection preface
static constexpr size_t kH2PrefaceLen = 24;  // length of h2 connection preface
static constexpr size_t kMinHttpRequestLen =
  18;  // min length of http 1.0 request

}  // namespace

template<SocketType T>
bool HttpCommTask<T>::transferEncodingContainsChunked(
  HttpCommTask<T>& comm_task, std::string_view encoding) {
  if (absl::StrContainsIgnoreCase(encoding, "chunked")) {
    comm_task.SendErrorResponse(
      rest::ResponseCode::NotImplemented, rest::ContentType::Unset, 1,
      ERROR_NOT_IMPLEMENTED,
      "Support for Transfer-Encoding: chunked is not implemented.");
    return true;
  }
  return false;
}

template<SocketType T>
int HttpCommTask<T>::on_message_began(llhttp_t* p) try {
  HttpCommTask<T>* me = static_cast<HttpCommTask<T>*>(p->data);
  me->_last_header_field.clear();
  me->_last_header_value.clear();
  me->_origin.clear();
  me->_url.clear();
  me->_request =
    std::make_unique<HttpRequest>(me->_connection_info, /*messageId*/ 1);
  me->_response.reset();
  me->_last_header_was_value = false;
  me->_should_keep_alive = false;
  me->_message_done = false;

  // acquire a new statistics entry for the request
  me->AcquireRequestStatistics(1UL).SET_READ_START(utilities::GetMicrotime());
  return HPE_OK;
} catch (...) {
  // the caller of this function is a C function, which doesn't know
  // exceptions. we must not let an exception escape from here.
  return HPE_INTERNAL;
}

template<SocketType T>
int HttpCommTask<T>::on_url(llhttp_t* p, const char* at, size_t len) try {
  HttpCommTask<T>* me = static_cast<HttpCommTask<T>*>(p->data);
  me->_request->setRequestType(LlhttpToRequestType(p));
  if (me->_request->requestType() == RequestType::Illegal) {
    me->SendSimpleResponse(rest::ResponseCode::MethodNotAllowed,
                           rest::ContentType::Unset, 1, vpack::BufferUInt8());
    return HPE_USER;
  }
  me->GetRequestStatistics(1UL).SET_REQUEST_TYPE(me->_request->requestType());

  me->_url.append(at, len);
  return HPE_OK;
} catch (...) {
  // the caller of this function is a C function, which doesn't know
  // exceptions. we must not let an exception escape from here.
  return HPE_INTERNAL;
}

template<SocketType T>
int HttpCommTask<T>::on_status(llhttp_t* p, const char* at, size_t len) {
  // should not be used
  return HPE_OK;
}

template<SocketType T>
int HttpCommTask<T>::on_header_field(llhttp_t* p, const char* at,
                                     size_t len) try {
  HttpCommTask<T>* me = static_cast<HttpCommTask<T>*>(p->data);
  if (me->_last_header_was_value) {
    me->_request->setHeader(std::move(me->_last_header_field),
                            std::move(me->_last_header_value));
    me->_last_header_field.assign(at, len);
  } else {
    me->_last_header_field.append(at, len);
  }
  me->_last_header_was_value = false;
  return HPE_OK;
} catch (...) {
  // the caller of this function is a C function, which doesn't know
  // exceptions. we must not let an exception escape from here.
  return HPE_INTERNAL;
}

template<SocketType T>
int HttpCommTask<T>::on_header_value(llhttp_t* p, const char* at,
                                     size_t len) try {
  HttpCommTask<T>* me = static_cast<HttpCommTask<T>*>(p->data);
  if (me->_last_header_was_value) {
    me->_last_header_value.append(at, len);
  } else {
    me->_last_header_value.assign(at, len);
  }
  me->_last_header_was_value = true;
  return HPE_OK;
} catch (...) {
  // the caller of this function is a C function, which doesn't know
  // exceptions. we must not let an exception escape from here.
  return HPE_INTERNAL;
}

template<SocketType T>
int HttpCommTask<T>::on_header_complete(llhttp_t* p) try {
  HttpCommTask<T>* me = static_cast<HttpCommTask<T>*>(p->data);
  me->_response.reset();
  if (!me->_last_header_field.empty()) {
    me->_request->setHeader(std::move(me->_last_header_field),
                            std::move(me->_last_header_value));
  }

  bool found;
  const std::string& encoding =
    me->_request->header(StaticStrings::kTransferEncoding, found);

  if (found && transferEncodingContainsChunked(*me, encoding)) {
    return HPE_USER;
  }

  if ((p->http_major != 1 || p->http_minor != 0) &&
      (p->http_major != 1 || p->http_minor != 1)) {
    me->SendSimpleResponse(rest::ResponseCode::HttpVersionNotSupported,
                           rest::ContentType::Unset, 1, vpack::BufferUInt8());
    return HPE_USER;
  }
  if (p->content_length > kMaximalBodySize) {
    me->SendSimpleResponse(rest::ResponseCode::RequestEntityTooLarge,
                           rest::ContentType::Unset, 1, vpack::BufferUInt8());
    return HPE_USER;
  }
  me->_should_keep_alive = llhttp_should_keep_alive(p);

  const std::string& expect =
    me->_request->header(StaticStrings::kExpect, found);
  if (found && string_utils::Trim(expect) == "100-continue") {
    const char* response = "HTTP/1.1 100 Continue\r\n\r\n";
    auto buff = asio_ns::buffer(response, strlen(response));
    asio_ns::async_write(
      me->_protocol->socket, buff,
      [self = me->shared_from_this()](const asio_ns::error_code& ec, size_t) {
        if (ec) {
          static_cast<HttpCommTask<T>*>(self.get())->Close(ec);
        }
      });
    return HPE_OK;
  }

  if (me->_request->requestType() == RequestType::Head) {
    // Assume that request/response has no body, proceed parsing next message
    return 1;  // 1 is defined by parser
  }
  return HPE_OK;
} catch (...) {
  // the caller of this function is a C function, which doesn't know
  // exceptions. we must not let an exception escape from here.
  return HPE_INTERNAL;
}

template<SocketType T>
int HttpCommTask<T>::on_body(llhttp_t* p, const char* at, size_t len) try {
  HttpCommTask<T>* me = static_cast<HttpCommTask<T>*>(p->data);
  me->_request->appendBody(at, len);
  return HPE_OK;
} catch (...) {
  // the caller of this function is a C function, which doesn't know
  // exceptions. we must not let an exception escape from here.
  return HPE_INTERNAL;
}

template<SocketType T>
int HttpCommTask<T>::on_message_complete(llhttp_t* p) try {
  HttpCommTask<T>* me = static_cast<HttpCommTask<T>*>(p->data);
  me->_request->parseUrl(me->_url.data(), me->_url.size());

  me->GetRequestStatistics(1UL).SET_READ_END();
  me->_message_done = true;

  return HPE_PAUSED;
} catch (...) {
  // the caller of this function is a C function, which doesn't know
  // exceptions. we must not let an exception escape from here.
  return HPE_INTERNAL;
}

template<SocketType T>
HttpCommTask<T>::HttpCommTask(GeneralServer& server, ConnectionInfo info,
                              std::shared_ptr<AsioSocket<T>> so)
  : GeneralCommTask<T>(server, std::move(info), std::move(so)),
    _last_header_was_value(false),
    _should_keep_alive(false),
    _message_done(false) {
  this->_connection_statistics.SET_HTTP();

  // initialize http parsing code
  llhttp_settings_init(&_parser_settings);
  _parser_settings.on_message_begin = HttpCommTask<T>::on_message_began;
  _parser_settings.on_url = HttpCommTask<T>::on_url;
  _parser_settings.on_status = HttpCommTask<T>::on_status;
  _parser_settings.on_header_field = HttpCommTask<T>::on_header_field;
  _parser_settings.on_header_value = HttpCommTask<T>::on_header_value;
  _parser_settings.on_headers_complete = HttpCommTask<T>::on_header_complete;
  _parser_settings.on_body = HttpCommTask<T>::on_body;
  _parser_settings.on_message_complete = HttpCommTask<T>::on_message_complete;
  llhttp_init(&_parser, HTTP_REQUEST, &_parser_settings);
  _parser.data = this;

  this->_general_server_feature.countHttp1Connection();
}

template<SocketType T>
HttpCommTask<T>::~HttpCommTask() = default;

template<SocketType T>
void HttpCommTask<T>::Start() {
  SDB_DEBUG("xxxxx", Logger::REQUESTS, "<http> opened connection \"",
            std::bit_cast<size_t>(this), "\"");

  asio_ns::post(
    this->_protocol->context.io_context, [self = this->shared_from_this()] {
      static_cast<HttpCommTask<T>&>(*self.get()).CheckProtocolUpgrade();
    });
}

template<SocketType T>
bool HttpCommTask<T>::ReadCallback(asio_ns::error_code ec) {
  llhttp_errno_t err = HPE_OK;
  if (!ec) {
    // Inspect the received data
    size_t nparsed = 0;
    const auto buffers = this->_protocol->buffer.data();
    auto it = asio_ns::buffer_sequence_begin(buffers);
    const auto end = asio_ns::buffer_sequence_end(buffers);
    for (; it != end; ++it) {
      const char* data = static_cast<const char*>(it->data());
      const char* end = data + it->size();
      do {
        size_t datasize = end - data;

        SDB_IF_FAILURE("HttpCommTask<T>::readCallback_in_small_chunks") {
          // we had an issue that URLs were cut off because the url data was
          // handed in in multiple buffers. To cover this case, we simulate that
          // data fed to the parser in small chunks.
          constexpr size_t kChunksize = 5;
          datasize = std::min<size_t>(datasize, kChunksize);
        }

        err = llhttp_execute(&_parser, data, datasize);
        if (err != HPE_OK) {
          ptrdiff_t diff = llhttp_get_error_pos(&_parser) - data;
          SDB_ASSERT(diff >= 0);
          nparsed += static_cast<size_t>(diff);
          break;
        }
        nparsed += datasize;
        data += datasize;
      } while (SDB_UNLIKELY(data < end));
    }

    SDB_ASSERT(nparsed < std::numeric_limits<size_t>::max());
    // Remove consumed data from receive buffer.
    this->_protocol->buffer.consume(nparsed);
    // And count it in the statistics:
    this->GetRequestStatistics(1UL).ADD_RECEIVED_BYTES(nparsed);

    if (_message_done) {
      SDB_ASSERT(err == HPE_PAUSED);
      _message_done = false;
      ProcessRequest();
      return false;  // stop read loop
    }

  } else {
    // got a connection error
    if (ec == asio_ns::error::misc_errors::eof) {
      err = llhttp_finish(&_parser);
    } else {
      SDB_DEBUG("xxxxx", Logger::REQUESTS, "Error while reading from socket: '",
                ec.message(), "'");
      err = HPE_INVALID_EOF_STATE;
    }
  }

  if (err != HPE_OK && err != HPE_USER && err != HPE_CB_HEADERS_COMPLETE) {
    if (err == HPE_INVALID_EOF_STATE) {
      SDB_TRACE("xxxxx", Logger::REQUESTS,
                "Connection closed by peer, with ptr ",
                std::bit_cast<size_t>(this));
    } else {
      SDB_TRACE("xxxxx", Logger::REQUESTS, "HTTP parse failure: '",
                llhttp_get_error_reason(&_parser), "'");
    }
    this->Close(ec);
  }

  return err == HPE_OK && !ec;
}

template<SocketType T>
void HttpCommTask<T>::SetIOTimeout() {
  double secs = this->_general_server_feature.keepAliveTimeout();
  if (secs <= 0) {
    return;
  }

  const bool was_reading = this->_reading;
  const bool was_writing = this->_writing;
  SDB_ASSERT((was_reading && !was_writing) || (!was_reading && was_writing));

  auto millis = std::chrono::milliseconds(static_cast<int64_t>(secs * 1000));
  this->_protocol->timer.expires_after(millis);
  this->_protocol->timer.async_wait(
    [=, self = CommTask::weak_from_this()](const asio_ns::error_code& ec) {
      std::shared_ptr<CommTask> s;
      if (ec || !(s = self.lock())) {  // was canceled / deallocated
        return;
      }

      auto& me = static_cast<HttpCommTask<T>&>(*s);
      if ((was_reading && me._reading) || (was_writing && me._writing)) {
        SDB_INFO("xxxxx", Logger::REQUESTS,
                 "keep alive timeout, closing stream!");
        static_cast<GeneralCommTask<T>&>(*s).Close(ec);
      }
    });
}

template<SocketType T>
void HttpCommTask<T>::CheckProtocolUpgrade() {
  auto cb = [self = this->shared_from_this()](const asio_ns::error_code& ec,
                                              size_t nread) {
    auto& me = static_cast<HttpCommTask<T>&>(*self);
    if (ec) {
      me.Close(ec);
      return;
    }
    me._protocol->buffer.commit(nread);

    auto bg = asio_ns::buffers_begin(me._protocol->buffer.data());
    if (nread >= kH2PrefaceLen &&
        std::equal(::kH2Preface, ::kH2Preface + kH2PrefaceLen, bg,
                   bg + ptrdiff_t(kH2PrefaceLen))) {
      // http2 upgrade
      // do not remove preface here, H2CommTask will read it from buffer
      auto comm_task = std::make_unique<H2CommTask<T>>(
        me._server, me._connection_info, std::move(me._protocol));
      comm_task->SetRequestStatistics(1UL, me.StealRequestStatistics(1UL));
      me._server.registerTask(std::move(comm_task));
      me.Close(ec);
      return;
    }
    if (nread >= kVstLen && (std::equal(::kVst10, ::kVst10 + kVstLen, bg,
                                        bg + ptrdiff_t(kVstLen)) ||
                             std::equal(::kVst11, ::kVst11 + kVstLen, bg,
                                        bg + ptrdiff_t(kVstLen)))) {
      // attempt to use VST 1.0 or VST 1.1
      me.Close(ec);
      return;
    }

    me.AsyncReadSome();  // continue reading
  };
  auto buffs =
    this->_protocol->buffer.prepare(GeneralCommTask<T>::kReadBlockSize);
  asio_ns::async_read(this->_protocol->socket, buffs,
                      asio_ns::transfer_at_least(kMinHttpRequestLen),
                      std::move(cb));
}

#ifdef USE_DTRACE
// Moved here to prevent multiplicity by template
static void __attribute__((noinline)) DTraceHttpCommTaskProcessRequest(
  size_t th) {
  DTRACE_PROBE1(serened, HttpCommTaskProcessRequest, th);
}
#else
static void DTraceHttpCommTaskProcessRequest(size_t) {}
#endif

template<SocketType T>
std::string HttpCommTask<T>::url() const {
  if (_request != nullptr) {
    return std::string((
             _request->databaseName().empty()
               ? ""
               : "/_db/" + string_utils::UrlEncode(_request->databaseName()))) +
           (log::GetLogRequestParameters() ? _request->fullUrl()
                                           : _request->requestPath());
  }
  return "";
}

template<SocketType T>
void HttpCommTask<T>::ProcessRequest() {
  DTraceHttpCommTaskProcessRequest((size_t)this);

  SDB_ASSERT(_request);
  auto msg_id = _request->messageId();
  auto resp_content_type = _request->contentTypeResponse();
  try {
    DoProcessRequest();
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
void HttpCommTask<T>::DoProcessRequest() {
  this->_protocol->timer.cancel();
  if (this->Stopped()) {
    return;  // we have to ignore this request because the connection has
             // already been closed
  }

  // we may have gotten an H2 Upgrade request
  if (_parser.upgrade) [[unlikely]] {
    SDB_INFO("xxxxx", Logger::REQUESTS, "detected an 'Upgrade' header");
    bool found;
    const std::string& h2 = _request->header("upgrade");
    const std::string& settings = _request->header("http2-settings", found);
    if (h2 == "h2c" && found && !settings.empty()) {
      auto task = std::make_shared<H2CommTask<T>>(
        this->_server, this->_connection_info, std::move(this->_protocol));
      task->SetRequestStatistics(1UL, this->StealRequestStatistics(1UL));
      task->UpgradeHttp1(std::move(_request));
      this->Close();
      return;
    }
  }

  // ensure there is a null byte termination. Some RestHandlers use
  // C functions like strchr that except a C string as input
  _request->appendNullTerminator();
  // no need to increase memory usage here!
  {
    SDB_INFO("xxxxx", Logger::REQUESTS, "\"http-request-begin\",\"",
             std::bit_cast<size_t>(this), "\",\"",
             this->_connection_info.client_address, "\",\"",
             HttpRequest::translateMethod(_request->requestType()), "\",\"",
             url(), "\"");

    std::string_view body = _request->rawPayload();
    this->_general_server_feature.countHttp1Request(body.size());

    if (log::IsEnabled(LogLevel::TRACE, Logger::REQUESTS) &&
        log::GetLogRequestParameters()) {
      // Log HTTP headers:
      this->LogRequestHeaders("http", _request->headers());

      if (!body.empty()) {
        this->LogRequestBody("http", _request->contentType(), body);
      }
    }
  }

  // store origin header for later use
  _origin = _request->header(StaticStrings::kOrigin);

  // OPTIONS requests currently go unauthenticated
  if (_request->requestType() == rest::RequestType::Options) {
    this->ProcessCorsOptions(std::move(_request), _origin);
    return;
  }

  ServerState::Mode mode = ServerState::instance()->GetMode();

  // scrape the auth headers to determine and authenticate the user
  auto auth_token = this->CheckAuthHeader(*_request, mode);

  // We want to separate superuser token traffic:
  if (_request->authenticated() && _request->user().empty()) {
    this->GetRequestStatistics(1UL).SET_SUPERUSER();
  }

  // first check whether we allow the request to continue
  Flow cont = this->PrepareExecution(auth_token, *_request, mode);
  if (cont != Flow::Continue) {
    return;  // PrepareExecution sends the error message
  }

  // gzip-uncompress / zlib-deflate / lz4-uncompress
  if (Result res = this->HandleContentEncoding(*_request); res.fail()) {
    this->SendErrorResponse(rest::ResponseCode::Bad,
                            _request->contentTypeResponse(), 1,
                            ERROR_BAD_PARAMETER, res.errorMessage());
    return;
  }

  // create a handler and execute
  auto resp =
    std::make_unique<HttpResponse>(rest::ResponseCode::ServerError, 1, nullptr,
                                   rest::ResponseCompressionType::Unset);
  resp->setContentType(_request->contentTypeResponse());
  this->ExecuteRequest(std::move(_request), std::move(resp), mode);
}

#ifdef USE_DTRACE
// Moved here to prevent multiplicity by template
static void __attribute__((noinline)) DTraceHttpCommTaskSendResponse(
  size_t th) {
  DTRACE_PROBE1(serened, HttpCommTaskSendResponse, th);
}
#else
static void DTraceHttpCommTaskSendResponse(size_t) {}
#endif

template<SocketType T>
void HttpCommTask<T>::SendResponse(std::unique_ptr<GeneralResponse> base_res,
                                   RequestStatistics::Item stat) {
  if (this->Stopped()) {
    return;
  }

  DTraceHttpCommTaskSendResponse((size_t)this);

#ifdef SDB_DEV
  HttpResponse& response = dynamic_cast<HttpResponse&>(*base_res);
#else
  HttpResponse& response = static_cast<HttpResponse&>(*base_res);
#endif

  // will add CORS headers if necessary
  this->FinishExecution(*base_res, _origin);

  // handle response code 204 No Content
  if (response.responseCode() == rest::ResponseCode::NoContent) {
    response.clearBody();
  }

  _header.clear();
  _header.reserve(220);

  _header.append(std::string_view("HTTP/1.1 "));
  _header.append(GeneralResponse::responseString(response.responseCode()));
  _header.append("\r\n", 2);

  // if we return HTTP 401, we need to send a www-authenticate header back with
  // the response. in this case we need to check if the header was already set
  // or if we need to set it ourselves.
  // note that clients can suppress sending the www-authenticate header by
  // sending us an x-omit-www-authenticate header.
  bool need_www_authenticate =
    (response.responseCode() == rest::ResponseCode::Unauthorized &&
     (!_request || _request->header("x-omit-www-authenticate").empty()));

  bool seen_server_header = false;
  // bool seenConnectionHeader = false;
  for (const auto& it : response.headers()) {
    const std::string& key = it.first;
    const size_t key_length = key.size();
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

    // reserve enough space for header name + ": " + value + "\r\n"
    _header.reserve(key.size() + 2 + it.second.size() + 2);

    const char* p = key.data();
    const char* end = p + key_length;
    int cap_state = 1;
    while (p < end) {
      if (cap_state == 1) {
        // upper case
        _header.push_back(absl::ascii_toupper(*p));
        cap_state = 0;
      } else if (cap_state == 0) {
        // normal case
        _header.push_back(absl::ascii_tolower(*p));
        if (*p == '-') {
          cap_state = 1;
        } else if (*p == ':') {
          cap_state = 2;
        }
      } else {
        // output as is
        _header.push_back(*p);
      }
      ++p;
    }

    _header.append(": ", 2);
    _header.append(it.second);
    _header.append("\r\n", 2);
  }

  // add "Server" response header
  if (!seen_server_header && this->_is_user_request) {
    _header.append(std::string_view("Server: SereneDB\r\n"));
  }

  if (need_www_authenticate) {
    SDB_ASSERT(response.responseCode() == rest::ResponseCode::Unauthorized);
    _header.append(
      std::string_view("Www-Authenticate: Basic, realm=\"SereneDB\"\r\n"));
    _header.append(
      std::string_view("Www-Authenticate: Bearer, token_type=\"JWT\", "
                       "realm=\"SereneDB\"\r\n"));
  }

  // turn on the keepAlive timer
  double secs = this->_general_server_feature.keepAliveTimeout();
  if (_should_keep_alive && secs > 0) {
    _header.append(std::string_view("Connection: Keep-Alive\r\n"));
  } else {
    _header.append(std::string_view("Connection: Close\r\n"));
  }

  if (response.contentType() != ContentType::Custom) {
    _header.append(std::string_view("Content-Type: "));
    _header.append(rest::ContentTypeToString(response.contentType()));
    _header.append("\r\n", 2);
  }

  for (const auto& it : response.cookies()) {
    _header.append(std::string_view("Set-Cookie: "));
    _header.append(it);
    _header.append("\r\n", 2);
  }

  size_t len = response.bodySize();
  SDB_ASSERT(
    response.responseCode() != rest::ResponseCode::NoContent || len == 0,
    "response code 204 requires body length to be zero");
  _header.append(std::string_view("Content-Length: "));
  _header.append(std::to_string(len));
  _header.append("\r\n\r\n", 4);

  SDB_ASSERT(_response == nullptr);
  _response = response.stealBody();
  // append write buffer and statistics
  SDB_ASSERT(response.responseCode() != rest::ResponseCode::NoContent ||
               _response->empty(),
             "response code 204 requires body length to be zero");

  if (log::IsEnabled(LogLevel::TRACE, Logger::REQUESTS) &&
      log::GetLogRequestParameters()) {
    // Log HTTP headers:
    this->LogResponseHeaders("http", response.headers());

    if (!_response->empty()) {
      std::string_view body(_response->data(), _response->size());
      this->LogRequestBody("http", response.contentType(), body,
                           true /* isResponse */);
    }
  }

  // and give some request information
  SDB_DEBUG("xxxxx", Logger::REQUESTS, "\"http-request-end\",\"",
            std::bit_cast<size_t>(this), "\",\"",
            this->_connection_info.client_address, "\",\"",
            GeneralRequest::translateMethod(::LlhttpToRequestType(&_parser)),
            "\",\"", url(), "\",\"", static_cast<int>(response.responseCode()),
            "\",", absl::StrFormat("%.6f", stat.ELAPSED_SINCE_READ_START()),
            ",", absl::StrFormat("%.6f", stat.ELAPSED_WHILE_QUEUED()));

  // sendResponse is always called from a scheduler thread
  boost::asio::post(
    this->_protocol->context.io_context,
    [self = this->shared_from_this(), stat = std::move(stat)]() mutable {
      static_cast<HttpCommTask<T>&>(*self).WriteResponse(std::move(stat));
    });
}

#ifdef USE_DTRACE
// Moved here to prevent multiplicity by template
static void __attribute__((noinline)) DTraceHttpCommTaskWriteResponse(
  size_t th) {
  DTRACE_PROBE1(serened, HttpCommTaskWriteResponse, th);
}
static void __attribute__((noinline)) DTraceHttpCommTaskResponseWritten(
  size_t th) {
  DTRACE_PROBE1(serened, HttpCommTaskResponseWritten, th);
}
#else
static void DTraceHttpCommTaskWriteResponse(size_t) {}
static void DTraceHttpCommTaskResponseWritten(size_t) {}
#endif

// called on IO context thread
template<SocketType T>
void HttpCommTask<T>::WriteResponse(RequestStatistics::Item stat) {
  DTraceHttpCommTaskWriteResponse((size_t)this);

  SDB_ASSERT(!_header.empty());

  stat.SET_WRITE_START();

  std::array<asio_ns::const_buffer, 2> buffers;
  buffers[0] = asio_ns::buffer(_header.data(), _header.size());
  if (HTTP_HEAD != _parser.method) {
    buffers[1] = asio_ns::buffer(_response->data(), _response->size());
  }

  this->_writing = true;
  asio_ns::async_write(
    this->_protocol->socket, buffers,
    [self = this->shared_from_this(), stat = std::move(stat)](
      asio_ns::error_code ec, size_t nwrite) {
      DTraceHttpCommTaskResponseWritten((size_t)self.get());

      auto& me = static_cast<HttpCommTask<T>&>(*self);
      me._writing = false;

      stat.SET_WRITE_END();
      stat.ADD_SENT_BYTES(nwrite);

      me._response.reset();

      llhttp_errno_t err = llhttp_get_errno(&me._parser);
      if (ec || !me._should_keep_alive || err != HPE_PAUSED) {
        me.Close(ec);
      } else {  // ec == HPE_PAUSED
        llhttp_resume(&me._parser);
        me.AsyncReadSome();
      }
    });
}

template<SocketType T>
std::unique_ptr<GeneralResponse> HttpCommTask<T>::CreateResponse(
  rest::ResponseCode response_code, uint64_t mid) {
  SDB_ASSERT(mid == 1);
  return std::make_unique<HttpResponse>(response_code, mid, nullptr,
                                        rest::ResponseCompressionType::Unset);
}

template class sdb::rest::HttpCommTask<SocketType::Tcp>;
template class sdb::rest::HttpCommTask<SocketType::Ssl>;
template class sdb::rest::HttpCommTask<SocketType::Unix>;
