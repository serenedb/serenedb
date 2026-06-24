////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "network/http/h1_codec.h"

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>

#include <utility>

namespace sdb::network {
namespace {

HttpMethod MapMethod(uint8_t method) {
  switch (method) {
    case HTTP_GET:
      return HttpMethod::Get;
    case HTTP_POST:
      return HttpMethod::Post;
    case HTTP_PUT:
      return HttpMethod::Put;
    case HTTP_DELETE:
      return HttpMethod::Delete;
    case HTTP_HEAD:
      return HttpMethod::Head;
    case HTTP_OPTIONS:
      return HttpMethod::Options;
    default:
      return HttpMethod::Other;
  }
}

}  // namespace

H1Codec::H1Codec(H1Limits limits) : _limits{limits} {
  static const llhttp_settings_t settings = [] {
    llhttp_settings_t s;
    llhttp_settings_init(&s);
    s.on_url = &H1Codec::OnUrl;
    s.on_header_field = &H1Codec::OnHeaderField;
    s.on_header_value = &H1Codec::OnHeaderValue;
    s.on_headers_complete = &H1Codec::OnHeadersComplete;
    s.on_body = &H1Codec::OnBody;
    s.on_message_complete = &H1Codec::OnMessageComplete;
    return s;
  }();
  llhttp_init(&_parser, HTTP_REQUEST, &settings);
  _parser.data = this;
}

int H1Codec::Fail(int status) noexcept {
  _error_status = status;
  return HPE_USER;
}

int H1Codec::OnUrl(llhttp_t* parser, const char* at, size_t length) {
  auto* self = static_cast<H1Codec*>(parser->data);
  self->_head_bytes += length;
  if (self->_head_bytes > self->_limits.max_head_bytes) {
    return self->Fail(431);
  }
  self->_target.append(at, length);
  return HPE_OK;
}

int H1Codec::OnHeaderField(llhttp_t* parser, const char* at, size_t length) {
  auto* self = static_cast<H1Codec*>(parser->data);
  self->_head_bytes += length;
  if (self->_head_bytes > self->_limits.max_head_bytes) {
    return self->Fail(431);
  }
  if (self->_last_was_value || self->_fields.empty()) {
    self->_fields.emplace_back();
    self->_last_was_value = false;
  }
  self->_fields.back().raw.append(at, length);
  return HPE_OK;
}

int H1Codec::OnHeaderValue(llhttp_t* parser, const char* at, size_t length) {
  auto* self = static_cast<H1Codec*>(parser->data);
  self->_head_bytes += length;
  if (self->_head_bytes > self->_limits.max_head_bytes) {
    return self->Fail(431);
  }
  self->_fields.back().value.append(at, length);
  self->_last_was_value = true;
  return HPE_OK;
}

int H1Codec::OnHeadersComplete(llhttp_t* parser) {
  auto* self = static_cast<H1Codec*>(parser->data);
  self->_keep_alive = llhttp_should_keep_alive(parser) != 0;
  self->_chunked = (parser->flags & F_CHUNKED) != 0;
  self->_content_length = self->_chunked ? 0 : parser->content_length;
  if (self->_content_length > self->_limits.max_body_bytes) {
    return self->Fail(413);
  }
  std::string_view expect;
  for (auto& field : self->_fields) {
    field.name = InternHeader(field.raw);
    if (field.name != HttpHeader::Unknown) {
      if (field.name == HttpHeader::Expect) {
        expect = field.value;
      }
      field.raw.clear();
    }
  }
  if (!expect.empty()) {
    if (!absl::EqualsIgnoreCase(expect, "100-continue")) {
      return self->Fail(417);
    }
    if (self->_content_length > 0 || self->_chunked) {
      self->_event = H1Event::Continue;
      return HPE_PAUSED;
    }
  }
  self->_event = H1Event::Head;
  return HPE_PAUSED;
}

int H1Codec::OnBody(llhttp_t* parser, const char* at, size_t length) {
  auto* self = static_cast<H1Codec*>(parser->data);
  // Enforce the body cap per byte: a chunked body has content_length 0 at
  // headers-complete, so the OnHeadersComplete check can't bound it.
  self->_body_bytes += length;
  if (self->_body_bytes > self->_limits.max_body_bytes) {
    return self->Fail(413);
  }
  if (self->_body_out == nullptr) {
    return HPE_OK;
  }
  self->_body_out->Write({at, length});
  return HPE_OK;
}

int H1Codec::OnMessageComplete(llhttp_t* parser) {
  auto* self = static_cast<H1Codec*>(parser->data);
  self->_body_done = true;
  return HPE_PAUSED;
}

H1FeedResult H1Codec::ParseHead(std::string_view input) noexcept {
  if (_error_status != 0) {
    return {0, H1Event::Error};
  }
  _event = H1Event::NeedMore;
  const llhttp_errno_t err =
    llhttp_execute(&_parser, input.data(), input.size());
  if (err == HPE_PAUSED) {
    _paused = true;
    const auto consumed =
      static_cast<size_t>(llhttp_get_error_pos(&_parser) - input.data());
    return {consumed, _event};
  }
  if (err != HPE_OK) {
    if (_error_status == 0) {
      _error_status = 400;
    }
    const auto consumed =
      static_cast<size_t>(llhttp_get_error_pos(&_parser) - input.data());
    return {consumed, H1Event::Error};
  }
  return {input.size(), H1Event::NeedMore};
}

H1BodyResult H1Codec::DecodeBody(std::string_view input,
                                 message::Writer& out) noexcept {
  if (_error_status != 0) {
    return {0, false, true};
  }
  _body_out = &out;
  _body_done = false;
  if (_paused) {
    llhttp_resume(&_parser);
    _paused = false;
  }
  const llhttp_errno_t err =
    llhttp_execute(&_parser, input.data(), input.size());
  _body_out = nullptr;
  if (err == HPE_PAUSED) {
    _paused = true;
    const auto consumed =
      static_cast<size_t>(llhttp_get_error_pos(&_parser) - input.data());
    return {consumed, _body_done, false};
  }
  if (err != HPE_OK) {
    if (_error_status == 0) {
      _error_status = 400;
    }
    const auto consumed =
      static_cast<size_t>(llhttp_get_error_pos(&_parser) - input.data());
    return {consumed, false, true};
  }
  return {input.size(), false, false};
}

HttpRequest H1Codec::TakeHead() noexcept {
  HttpRequest request;
  request.method = MapMethod(llhttp_get_method(&_parser));
  request.target = std::move(_target);
  request.headers = std::move(_fields);
  request.keep_alive = _keep_alive;
  return request;
}

void H1Codec::Reset() noexcept {
  llhttp_reset(&_parser);
  _target.clear();
  _fields.clear();
  _content_length = 0;
  _body_bytes = 0;
  _head_bytes = 0;
  _body_out = nullptr;
  _chunked = false;
  _last_was_value = false;
  _keep_alive = true;
  _paused = false;
  _body_done = false;
  _error_status = 0;
  _event = H1Event::NeedMore;
}

}  // namespace sdb::network
