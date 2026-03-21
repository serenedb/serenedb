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

#include "http_response.h"

#include <time.h>
#include <vpack/builder.h>
#include <vpack/dumper.h>
#include <vpack/options.h>
#include <vpack/vpack_helper.h>

#include <string_view>

#include "basics/encoding_utils.h"
#include "basics/error_code.h"
#include "basics/exceptions.h"
#include "basics/sink.h"
#include "basics/static_strings.h"
#include "basics/string_buffer.h"
#include "basics/string_utils.h"
#include "rest/general_request.h"

using namespace sdb;
using namespace sdb::basics;

namespace {
template<typename Func>
ErrorCode Deflate(Func&& func, basics::StringBuffer& body,
                  bool only_if_smaller) {
  basics::StringBuffer deflated;

  ErrorCode code = func(body, deflated);

  if (code == ERROR_OK) {
    if (only_if_smaller && deflated.size() >= body.size()) {
      code = ERROR_DISABLED;
    } else {
      std::swap(body.Impl(), deflated.Impl());
    }
  }
  return code;
}
}  // namespace

HttpResponse::HttpResponse(ResponseCode code, uint64_t mid,
                           std::unique_ptr<basics::StringBuffer> buffer,
                           rest::ResponseCompressionType rct)
  : GeneralResponse(code, mid),
    _body(std::move(buffer)),
    _body_size(0),
    _allow_compression(rct) {
  _content_type = ContentType::Text;

  if (!_body) {
    _body = std::make_unique<basics::StringBuffer>();
  }
}

void HttpResponse::reset(ResponseCode code) {
  _response_code = code;
  _headers.clear();
  _content_type = ContentType::Text;
  SDB_ASSERT(_body != nullptr);
  _body->clear();
  _body_size = 0;
}

void HttpResponse::setCookie(const std::string& name, const std::string& value,
                             int life_time_seconds, const std::string& path,
                             const std::string& domain, bool secure,
                             bool http_only) {
  StringBuffer buffer;

  buffer.PushStr(string_utils::Trim(name));
  buffer.PushChr('=');

  auto tmp = string_utils::UrlEncode(value);
  buffer.PushStr(tmp);

  if (life_time_seconds != 0) {
    time_t rawtime;

    time(&rawtime);
    if (life_time_seconds > 0) {
      rawtime += life_time_seconds;
    } else {
      rawtime = 1;
    }

    if (rawtime > 0) {
      struct tm* timeinfo;
      char buffer2[80];

      timeinfo = gmtime(&rawtime);
      strftime(buffer2, 80, "%a, %d-%b-%Y %H:%M:%S %Z", timeinfo);
      buffer.PushStr("; expires=");
      buffer.PushStr(buffer2);
    }
  }

  if (!path.empty()) {
    buffer.PushStr("; path=");
    buffer.PushStr(path);
  }

  if (!domain.empty()) {
    buffer.PushStr("; domain=");
    buffer.PushStr(domain);
  }

  if (secure) {
    buffer.PushStr("; secure");
  }

  if (http_only) {
    buffer.PushStr("; HttpOnly");
  }
  // copies buffer into a std::string
  _cookies.emplace_back(std::move(buffer.Impl()));
}

void HttpResponse::headResponse(size_t size) {
  SDB_ASSERT(_body != nullptr);
  _body->clear();
  _body_size = size;
  _generate_body = false;
}

size_t HttpResponse::bodySize() const {
  if (!_generate_body) {
    return _body_size;
  }
  SDB_ASSERT(_body != nullptr);
  return _body->size();
}

void HttpResponse::clearBody() noexcept {
  _body->clear();
  _body_size = 0;
}

void HttpResponse::setAllowCompression(
  rest::ResponseCompressionType rct) noexcept {
  if (_allow_compression == rest::ResponseCompressionType::Unset) {
    _allow_compression = rct;
  }
}

rest::ResponseCompressionType HttpResponse::compressionAllowed()
  const noexcept {
  return _allow_compression;
}

void HttpResponse::writeHeader(StringBuffer* output) {
  output->PushStr("HTTP/1.1 ");
  output->PushStr(responseString(_response_code));
  output->PushStr("\r\n");

  bool seen_server_header = false;
  bool seen_transfer_encoding_header = false;
  std::string transfer_encoding;

  for (const auto& it : _headers) {
    const std::string& key = it.first;
    const size_t key_length = key.size();

    // ignore content-length
    if (key == StaticStrings::kContentLength) {
      continue;
    } else if (key == StaticStrings::kConnection) {
      // this ensures we don't print two "Connection" headers
      continue;
    }

    // save transfer encoding
    if (key == StaticStrings::kTransferEncoding) {
      seen_transfer_encoding_header = true;
      transfer_encoding = it.second;
      continue;
    }

    if (key == StaticStrings::kServer) {
      // this ensures we don't print two "Server" headers
      seen_server_header = true;
      // go on and use the user-defined "Server" header value
    }

    // reserve enough space for header name + ": " + value + "\r\n"
    output->reserve(key_length + 2 + it.second.size() + 2);

    const char* p = key.data();
    const char* end = p + key_length;
    int cap_state = 1;

    while (p < end) {
      if (cap_state == 1) {
        // upper case
        output->PushChr(absl::ascii_toupper(*p));
        cap_state = 0;
      } else if (cap_state == 0) {
        // normal case
        output->PushChr(absl::ascii_tolower(*p));
        if (*p == '-') {
          cap_state = 1;
        } else if (*p == ':') {
          cap_state = 2;
        }
      } else {
        // output as is
        output->PushChr(*p);
      }
      ++p;
    }

    output->PushStr(": ");
    output->PushStr(it.second);
    output->PushStr("\r\n");
  }

  // add "Server" response header
  if (!seen_server_header) {
    output->PushStr("Server: SereneDB\r\n");
  }

  // this is just used by the batch handler, close connection
  output->PushStr("Connection: Close \r\n");

  // add "Content-Type" header
  switch (_content_type) {
    case ContentType::Unset:
    case ContentType::Json:
      output->PushStr("Content-Type: application/json; charset=utf-8\r\n");
      break;
    case ContentType::VPack:
      output->PushStr("Content-Type: application/x-vpack\r\n");
      break;
    case ContentType::Text:
      output->PushStr("Content-Type: text/plain; charset=utf-8\r\n");
      break;
    case ContentType::Html:
      output->PushStr("Content-Type: text/html; charset=utf-8\r\n");
      break;
    case ContentType::Dump:
      output->PushStr(
        "Content-Type: application/x-serene-dump; charset=utf-8\r\n");
      break;
    case ContentType::Custom: {
      // intentionally don't print anything here
      // the header should have been in _headers already, and should have been
      // handled above
    }
  }

  for (const auto& it : _cookies) {
    output->PushStr("Set-Cookie: ");
    output->PushStr(it);
    output->PushStr("\r\n");
  }

  if (seen_transfer_encoding_header && transfer_encoding == "chunked") {
    output->PushStr("Transfer-Encoding: chunked\r\n\r\n");
  } else {
    if (seen_transfer_encoding_header) {
      output->PushStr("Transfer-Encoding: ");
      output->PushStr(transfer_encoding);
      output->PushStr("\r\n");
    }

    // From http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.13
    //
    // 14.13 Content-Length
    //
    // "The Content-Length entity-header field indicates the size of the
    // entity-body, in decimal number of OCTETs, sent to the recipient or, in
    // the case of the HEAD method, the size of the entity-body that would have
    // been sent had the request been a GET."
    //
    // Note that a corner case exists where the HEAD method is sent with an
    // X-Serene-Async header. This causes the server to store the result, which
    // can be later retrieved via PUT. However, the PUT response cannot possibly
    // return the initial Content-Length header, but will return 0 instead.
    output->PushStr("Content-Length: ");
    output->PushU64(bodySize());
    output->PushStr("\r\n\r\n");
  }
  // end of header, body to follow
}

void HttpResponse::addPayload(vpack::Slice slice,
                              const vpack::Options* options) {
  if (_content_type == rest::ContentType::Json &&
      _content_type_requested == rest::ContentType::VPack) {
    // content type was set by a handler to Json but the client wants VPACK
    // as we have a slice at had we are able to reply with VPACK
    _content_type = rest::ContentType::VPack;
  }

  addPayloadInternal(slice.start(), slice.byteSize(), options);
}

void HttpResponse::addPayload(vpack::BufferUInt8&& buffer,
                              const vpack::Options* options) {
  if (_content_type == rest::ContentType::Json &&
      _content_type_requested == rest::ContentType::VPack) {
    // content type was set by a handler to Json but the client wants VPACK
    // as we have a slice at had we are able to reply with VPACK
    _content_type = rest::ContentType::VPack;
  }

  if (buffer.size() > 0) {
    addPayloadInternal(buffer.data(), buffer.size(), options);
  }
}

void HttpResponse::addRawPayload(std::string_view payload) {
  _body->PushStr(payload);
}

void HttpResponse::addPayloadInternal(const uint8_t* data, size_t length,
                                      const vpack::Options* options) {
  SDB_ASSERT(data != nullptr);

  if (!options) {
    options = &vpack::Options::gDefaults;
  }
  SDB_ASSERT(options != nullptr);

  if (_content_type == rest::ContentType::VPack) {
    // the input (data) may contain multiple vpack values, written
    // one after the other
    // here, we iterate over the slices in the input data, until we have
    // reached the specified total size (length)

    // total length of our generated response
    size_t result_length = 0;

    while (length > 0) {
      vpack::Slice current_data(data);
      const vpack::ValueLength input_length = current_data.byteSize();
      vpack::ValueLength output_length = input_length;

      SDB_ASSERT(length >= input_length);

      // will contain sanitized data
      if (_generate_body) {
        _body->PushStr({current_data.startAs<const char>(), output_length});
      }
      result_length += output_length;

      // advance to next slice (if any)
      if (length < input_length) {
        // oops, length specification may be wrong?!
        break;
      }

      data += input_length;
      length -= input_length;
    }

    if (!_generate_body) {
      headResponse(result_length);
    }
    return;
  }

  setContentType(rest::ContentType::Json);

  /// dump options contain have the escapeUnicode attribute set to true
  /// this allows dumping of string values as plain 7-bit ASCII values.
  /// for example, the string "möter" will be dumped as "m\u00F6ter".
  /// this allows faster JSON parsing in some client implementations,
  /// which speculate on ASCII strings first and only fall back to slower
  /// multibyte strings on first actual occurrence of a multibyte character.
  vpack::Options tmp_opts = *options;
  tmp_opts.escape_unicode = true;

  // here, the input (data) must **not** contain multiple vpack values,
  // written one after the other
  vpack::Slice current(data);
  SDB_ASSERT(current.byteSize() == length);

  if (_generate_body) {
    // convert object to JSON string
    vpack::Dumper dumper(&static_cast<basics::StrSink&>(*_body), &tmp_opts);
    dumper.Dump(current);
  } else {
    // determine the length of the to-be-generated JSON string,
    // without actually generating it
    basics::LenSink sink;

    // usual dumping -  but not to the response body
    vpack::Dumper dumper(&sink, &tmp_opts);
    dumper.Dump(current);

    headResponse(sink.Impl());
  }
}

ErrorCode HttpResponse::ZLibDeflate(bool only_if_smaller) {
  return Deflate(
    [](basics::StringBuffer& body, basics::StringBuffer& deflated) {
      return encoding::ZLibDeflate(
        reinterpret_cast<const uint8_t*>(body.data()), body.size(), deflated);
    },
    *_body, only_if_smaller);
}

ErrorCode HttpResponse::GZipCompress(bool only_if_smaller) {
  return Deflate(
    [](basics::StringBuffer& body, basics::StringBuffer& deflated) {
      return encoding::GZipCompress(
        reinterpret_cast<const uint8_t*>(body.data()), body.size(), deflated);
    },
    *_body, only_if_smaller);
}

ErrorCode HttpResponse::Lz4Compress(bool only_if_smaller) {
  return Deflate(
    [](basics::StringBuffer& body, basics::StringBuffer& deflated) {
      return encoding::Lz4Compress(
        reinterpret_cast<const uint8_t*>(body.data()), body.size(), deflated);
    },
    *_body, only_if_smaller);
}
