////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2018-2020 ArangoDB GmbH, Cologne, Germany
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

#include "h1_connection.h"

#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>
#include <fuerte/helper.h>
#include <fuerte/loop.h>
#include <fuerte/types.h>
#include <vpack/parser.h>

#include <atomic>
#include <cassert>

namespace sdb::fuerte::http {

RequestItem::RequestItem(std::unique_ptr<Request>&& req, RequestCallback&& cb,
                         std::string&& header)
  : request_header(std::move(header)),
    callback(std::move(cb)),
    request(std::move(req)) {}

template<SocketType ST>
int H1Connection<ST>::on_message_begin(llhttp_t* p) try {
  H1Connection<ST>* self = static_cast<H1Connection<ST>*>(p->data);
  self->_last_header_field.clear();
  self->_last_header_value.clear();
  self->_last_header_was_value = false;
  self->_should_keep_alive = false;
  self->_message_complete = false;
  self->_response.reset(new Response());
  return HPE_OK;
} catch (...) {
  return HPE_INTERNAL;
}

template<SocketType ST>
int H1Connection<ST>::on_status(llhttp_t* parser, const char* at,
                                size_t len) try {
  H1Connection<ST>* self = static_cast<H1Connection<ST>*>(parser->data);
  // compat for serenesh
  self->_response->header.addMeta(
    std::string("http/") + std::to_string(parser->http_major) + '.' +
      std::to_string(parser->http_minor),
    std::to_string(parser->status_code) + ' ' + std::string(at, len));
  return HPE_OK;
} catch (...) {
  return HPE_INTERNAL;
}

template<SocketType ST>
int H1Connection<ST>::on_header_field(llhttp_t* parser, const char* at,
                                      size_t len) try {
  H1Connection<ST>* self = static_cast<H1Connection<ST>*>(parser->data);
  if (self->_last_header_was_value) {
    absl::AsciiStrToLower(&self->_last_header_field);
    self->_response->header.addMeta(std::move(self->_last_header_field),
                                    std::move(self->_last_header_value));
    self->_last_header_field.assign(at, len);
  } else {
    self->_last_header_field.append(at, len);
  }
  self->_last_header_was_value = false;
  return HPE_OK;
} catch (...) {
  return HPE_INTERNAL;
}

template<SocketType ST>
int H1Connection<ST>::on_header_value(llhttp_t* parser, const char* at,
                                      size_t len) try {
  H1Connection<ST>* self = static_cast<H1Connection<ST>*>(parser->data);
  if (self->_last_header_was_value) {
    self->_last_header_value.append(at, len);
  } else {
    self->_last_header_value.assign(at, len);
  }
  self->_last_header_was_value = true;
  return HPE_OK;
} catch (...) {
  return HPE_INTERNAL;
}

template<SocketType ST>
int H1Connection<ST>::on_headers_complete(llhttp_t* parser) try {
  H1Connection<ST>* self = static_cast<H1Connection<ST>*>(parser->data);
  SDB_TRACE(GENERAL,
            "on_headers_complete this=", reinterpret_cast<uintptr_t>(self));

  self->_response->header.response_code =
    static_cast<StatusCode>(parser->status_code);
  if (!self->_last_header_field.empty()) {
    absl::AsciiStrToLower(&self->_last_header_field);
    self->_response->header.addMeta(std::move(self->_last_header_field),
                                    std::move(self->_last_header_value));
  }
  // Adjust idle timeout if necessary
  self->_should_keep_alive = llhttp_should_keep_alive(parser);

  // head has no body, but may have a Content-Length
  if (self->_item->request->header.rest_verb == RestVerb::Head) {
    return 1;  // tells the parser it should not expect a body
  } else if (parser->content_length > 0 &&
             parser->content_length < ULLONG_MAX) {
    uint64_t max_reserve = std::min<uint64_t>(2 << 24, parser->content_length);
    self->_response_buffer.reserve(max_reserve);
  }

  return HPE_OK;
} catch (...) {
  return HPE_INTERNAL;
}

template<SocketType ST>
int H1Connection<ST>::on_body(llhttp_t* parser, const char* at,
                              size_t len) try {
  SDB_TRACE(GENERAL, "on_body len=", len,
            " this=", reinterpret_cast<uintptr_t>(parser->data));
  static_cast<H1Connection<ST>*>(parser->data)
    ->_response_buffer.append(at, len);
  return HPE_OK;
} catch (...) {
  return HPE_INTERNAL;
}

template<SocketType ST>
int H1Connection<ST>::on_message_complete(llhttp_t* parser) try {
  SDB_TRACE(GENERAL, "on_message_complete this=",
            reinterpret_cast<uintptr_t>(parser->data));
  static_cast<H1Connection<ST>*>(parser->data)->_message_complete = true;
  return HPE_OK;
} catch (...) {
  return HPE_INTERNAL;
}

template<SocketType ST>
H1Connection<ST>::H1Connection(EventLoopService& loop,
                               const detail::ConnectionConfiguration& config)
  : GeneralConnection<ST, RequestItem>(loop, config),
    _last_header_was_value(false),
    _should_keep_alive(false),
    _message_complete(false) {
  // initialize http parsing code
  llhttp_settings_init(&_parser_settings);
  _parser_settings.on_message_begin = &on_message_begin;
  _parser_settings.on_status = &on_status;
  _parser_settings.on_header_field = &on_header_field;
  _parser_settings.on_header_value = &on_header_value;
  _parser_settings.on_headers_complete = &on_headers_complete;
  _parser_settings.on_body = &on_body;
  _parser_settings.on_message_complete = &on_message_complete;
  llhttp_init(&_parser, HTTP_RESPONSE, &_parser_settings);
  _parser.data = static_cast<void*>(this);

  // preemptively cache
  if (this->_config.authentication_type == AuthenticationType::Basic) {
    _auth_header.append("Authorization: Basic ");
    _auth_header.append(absl::Base64Escape(
      absl::StrCat(this->_config.user, ":", this->_config.password)));
    _auth_header.append("\r\n");
  } else if (this->_config.authentication_type == AuthenticationType::Jwt) {
    if (this->_config.jwt_token.empty()) {
      throw std::logic_error("JWT token is not set");
    }
    _auth_header.append("Authorization: bearer ");
    _auth_header.append(this->_config.jwt_token);
    _auth_header.append("\r\n");
  }

  SDB_TRACE(GENERAL, "creating http connection: this=",
            reinterpret_cast<uintptr_t>(this));
}

template<SocketType ST>
H1Connection<ST>::~H1Connection() try {
  abortRequests(Error::ConnectionCanceled, Clock::time_point::max());
} catch (...) {
}

template<SocketType ST>
size_t H1Connection<ST>::requestsLeft() const {
  size_t q = this->_num_queued.load(std::memory_order_relaxed);
  if (this->_active.load(std::memory_order_relaxed)) {
    q++;
  }
  return q;
}

template<SocketType ST>
void H1Connection<ST>::finishConnect() {
  // Note that the connection timeout alarm has already been disarmed.
  // If it has already gone off, we might have a completion handler
  // already posted on the iocontext. However, this will not touch anything
  // if we have first set the state to `Connected`.
  auto exp = Connection::State::Connecting;
  if (this->_state.compare_exchange_strong(exp, Connection::State::Connected)) {
    SDB_ASSERT(this->_active.load());
    this->AsyncWriteNextRequest();  // starts writing if queue non-empty
  } else {
    SDB_ERROR(GENERAL,
              "finishConnect: found state other than 'Connecting': ",
              static_cast<int>(exp));
    SDB_ASSERT(exp == Connection::State::Closed);
    // If this happens, then the connection has been shut down before
    // it could be fully connected, but the completion handler of the
    // connect call was still scheduled. No more work to do.
  }
}

template<SocketType ST>
std::string H1Connection<ST>::BuildRequestHeader(const Request& req) {
  // build the request header
  SDB_ASSERT(req.header.rest_verb != RestVerb::Illegal);

  std::string header;
  header.reserve(256);  // TODO is there a meaningful size ?
  header.append(fuerte::ToString(req.header.rest_verb));
  header.push_back(' ');

  http::AppendPath(req, /*target*/ header);

  header.append(" HTTP/1.1\r\n")
    .append("Host: ")
    .append(this->_config.host)
    .append("\r\n");
  // technically not required for http 1.1
  header.append("Connection: Keep-Alive\r\n");

  if (req.header.rest_verb != RestVerb::Get &&
      req.contentType() != ContentType::Custom) {
    header.append("Content-Type: ")
      .append(ToString(req.contentType()))
      .append("\r\n");
  }
  if (req.acceptType() != ContentType::Custom) {
    header.append("Accept: ").append(ToString(req.acceptType())).append("\r\n");
  }

  bool have_auth = false;
  for (const auto& pair : req.header.meta()) {
    if (pair.first == kFuContentLengthKey) {
      continue;  // skip content-length header
    }

    if (pair.first == kFuAuthorizationKey) {
      have_auth = true;
    }

    header.append(pair.first);
    header.append(": ");
    header.append(pair.second);
    header.append("\r\n");
  }

  if (!have_auth && !_auth_header.empty()) {
    header.append(_auth_header);
  }

  if (req.header.rest_verb != RestVerb::Get &&
      req.header.rest_verb != RestVerb::Head) {
    header.append("Content-Length: ");
    header.append(std::to_string(req.payloadSize()));
    header.append("\r\n\r\n");
  } else {
    header.append("\r\n");
  }
  // body will be appended seperately
  return header;
}

// writes data from task queue to network using asio_ns::async_write
template<SocketType ST>
void H1Connection<ST>::AsyncWriteNextRequest() {
  SDB_TRACE(GENERAL,
            "asyncWriteNextRequest: this=", reinterpret_cast<uintptr_t>(this));
  SDB_ASSERT(this->_active.load());
  auto state = this->_state.load();
  SDB_ASSERT(state == Connection::State::Connected);
  SDB_ASSERT(_item == nullptr);

  RequestItem* ptr = nullptr;
  if (!this->_queue.pop(ptr)) {  // check
    this->_active.store(false);  // set
    if (this->_queue.empty()) {  // check again
      SDB_TRACE(GENERAL,
                "asyncWriteNextRequest: stopped writing, this=",
                reinterpret_cast<uintptr_t>(this));
      if (_should_keep_alive) {
        this->setIOTimeout();
      } else {
        SDB_TRACE(GENERAL, "no keep-alive set, this=",
                  reinterpret_cast<uintptr_t>(this));
        this->shutdownConnection(Error::CloseRequested);
      }
      return;
    }
    if (this->_active.exchange(true)) {
      return;  // someone else restarted
    }
    bool success = this->_queue.pop(ptr);
    SDB_ASSERT(success);
  }
  uint32_t q = this->_num_queued.fetch_sub(1, std::memory_order_relaxed);
  SDB_ASSERT(_item.get() == nullptr);
  SDB_ASSERT(ptr != nullptr);
  SDB_ASSERT(q > 0);

  _item.reset(ptr);

  std::array<asio_ns::const_buffer, 2> buffers;
  buffers[0] =
    asio_ns::buffer(_item->request_header.data(), _item->request_header.size());
  // GET and HEAD have no payload
  if (_item->request->header.rest_verb != RestVerb::Get &&
      _item->request->header.rest_verb != RestVerb::Head) {
    buffers[1] = _item->request->payload();
  }

  this->_writing = true;
  this->setIOTimeout();
  asio_ns::async_write(
    this->_proto.socket, std::move(buffers),
    [self(Connection::shared_from_this())](const asio_ns::error_code& ec,
                                           size_t nwrite) {
      static_cast<H1Connection<ST>&>(*self).AsyncWriteCallback(ec, nwrite);
    });
  this->_item->request->setTimeAsyncWrite();
  SDB_TRACE(GENERAL, "asyncWriteNextRequest: done, this=",
            reinterpret_cast<uintptr_t>(this));
}

// called by the async_write handler (called from IO thread)
template<SocketType ST>
void H1Connection<ST>::AsyncWriteCallback(const asio_ns::error_code& ec,
                                          size_t nwrite) {
  SDB_ASSERT(this->_writing);
  // A connection can go to Closed state essentially at any time
  // and in this case _active will already be false if we get back here.
  // Therefore we cannot assert on it being true, which would otherwise be
  // correct.
  SDB_ASSERT(this->_state == Connection::State::Connected ||
             this->_state == Connection::State::Closed);
  this->_writing = false;  // indicate that no async write is ongoing any more
  this->cancelTimer();     // cancel alarm for timeout

  const auto now = Clock::now();
  if (ec || _item == nullptr || _item->expires < now) {
    // Send failed
    SDB_DEBUG(GENERAL, "asyncWriteCallback (http): error '",
              ec.message(), "', this=", reinterpret_cast<uintptr_t>(this));
    if (_item != nullptr) {
      SDB_DEBUG(GENERAL,
                "asyncWriteCallback (http): timeoutLeft: ",
                std::chrono::duration_cast<std::chrono::milliseconds>(
                  _item->expires - now)
                  .count(),
                " milliseconds");
    }

    this->shutdownConnection(TranslateError(ec, Error::WriteError));
    return;
  }
  SDB_ASSERT(_item != nullptr);

  // Send succeeded
  SDB_TRACE(GENERAL, "asyncWriteCallback: send succeeded ",
            "this=", reinterpret_cast<uintptr_t>(this));
  this->_item->request->setTimeSent();

  // request is written we no longer need data for that
  _item->request_header.clear();

  this->AsyncReadSome();  // listen for the response
}

// ------------------------------------
// Reading data
// ------------------------------------

// called by the async_read handler (called from IO thread)
template<SocketType ST>
void H1Connection<ST>::asyncReadCallback(const asio_ns::error_code& ec) {
  // Do not cancel timeout now, because we might be going on to read!
  if (_item == nullptr) {  // could happen on aborts
    this->cancelTimer();
    this->shutdownConnection(Error::CloseRequested);
    return;
  }
  SDB_ASSERT(_item != nullptr);

  // Inspect the data we've received so far.
  size_t nparsed = 0;
  const auto buffers = this->_receive_buffer.data();
  auto it = asio_ns::buffer_sequence_begin(buffers);
  const auto end = asio_ns::buffer_sequence_end(buffers);
  for (; it != end; ++it) {
    const char* data = static_cast<const char*>(it->data());

    enum llhttp_errno err = llhttp_execute(&_parser, data, it->size());
    if (err != HPE_OK) {
      /* Handle error. Usually just close the connection. */
      auto msg = absl::StrCat("Invalid HTTP response in parser: '",
                              llhttp_errno_name(err), "' reason: '",
                              llhttp_get_error_reason(&_parser), "'");
      SDB_ERROR(GENERAL, msg,
                ", this=", reinterpret_cast<uintptr_t>(this));
      // will cleanup _item
      this->shutdownConnection(Error::ProtocolError, msg);
      return;
    }
    nparsed += it->size();
  }

  // Remove consumed data from receive buffer.
  this->_receive_buffer.consume(nparsed);

  if (ec == asio_ns::error::misc_errors::eof &&
      llhttp_message_needs_eof(&_parser)) {
    llhttp_finish(&_parser);
  }

  if (_message_complete) {
    SDB_ASSERT(_response != nullptr);
    _message_complete = false;  // prevent entering branch on EOF

    this->cancelTimer();  // got response in time

    if (!_response_buffer.empty()) {
      _response->setPayload(std::move(_response_buffer), 0);
    }

    if (_item->request->header.rest_verb == RestVerb::Head) {
      // for some reason the llhttp parser swallows the on_message_done
      // signal for the following request
      llhttp_reset(&_parser);
    }

    try {
      _item->callback(Error::NoError, std::move(_item->request),
                      std::move(_response));
    } catch (...) {
      SDB_ERROR(GENERAL,
                "unhandled exception in fuerte callback");
    }

    _item.reset();
    SDB_TRACE(GENERAL,
              "asyncReadCallback: completed parsing "
              "response this=",
              reinterpret_cast<uintptr_t>(this));

    if (!ec) {
      AsyncWriteNextRequest();  // send next request
      return;
    }
  }

  if (ec) {
    SDB_DEBUG(GENERAL,
              "asyncReadCallback: Error while reading from socket: '",
              ec.message(), "' , this=", reinterpret_cast<uintptr_t>(this));
    this->shutdownConnection(TranslateError(ec, Error::ReadError));
  } else {
    SDB_TRACE(GENERAL,
              "asyncReadCallback: response not complete yet this=",
              reinterpret_cast<uintptr_t>(this));

    this->AsyncReadSome();  // keep reading from socket
    // leave read timeout in place!
  }
}

/// abort ongoing / unfinished requests
template<SocketType ST>
void H1Connection<ST>::abortRequests(fuerte::Error err, Clock::time_point) {
  // simon: thread-safe, only called from IO-Thread
  // (which holds shared_ptr) and destructors
  if (_item) {
    // Item has failed, remove from message store
    _item->invokeOnError(err);
    _item.reset();
  }
}

/// Set timeout accordingly
template<SocketType ST>
void H1Connection<ST>::setIOTimeout() {
  const bool is_idle = _item == nullptr;
  if (is_idle && !this->_config.use_idle_timeout) {
    this->cancelTimer();
    return;
  }

  const bool was_reading = this->_reading;
  const bool was_writing = this->_writing;
  auto tp = _item ? _item->expires : Clock::now() + this->_config.idle_timeout;

  // expires_after cancels pending ops
  this->_proto.timer.expires_at(tp);
  this->_proto.timer.async_wait(
    [=, self = Connection::weak_from_this()](const auto& ec) {
      std::shared_ptr<Connection> s;
      if (ec || !(s = self.lock())) {  // was canceled / deallocated
        return;
      }

      auto& me = static_cast<H1Connection<ST>&>(*s);
      if ((was_writing && me._writing) || (was_reading && me._reading)) {
        SDB_DEBUG(GENERAL, "HTTP-Request timeout",
                  " this=", reinterpret_cast<uintptr_t>(&me));
        me._timeout_on_read_write = true;
        me._proto.cancel();
        // We simply cancel all ongoing asynchronous operations, the
        // completion handlers will do the rest.
        return;
      } else if (is_idle && !me._writing & !me._reading) {
        if (!me._active && me._state == Connection::State::Connected) {
          SDB_DEBUG(GENERAL, "HTTP-Request idle timeout",
                    " this=", reinterpret_cast<uintptr_t>(&me));
          me.shutdownConnection(Error::CloseRequested);
        }
      }
      // In all other cases we do nothing, since we have been posted to the
      // iocontext but the thing we should be timing out has already
      // completed.
    });
}

template class H1Connection<SocketType::Tcp>;
template class H1Connection<SocketType::Ssl>;
#ifdef ASIO_HAS_LOCAL_SOCKETS
template class H1Connection<SocketType::Unix>;
#endif

}  // namespace sdb::fuerte::http
