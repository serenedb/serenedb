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

#include "http_request.h"

#include <fuerte/types.h>
#include <vpack/builder.h>
#include <vpack/options.h>
#include <vpack/parser.h>
#include <vpack/validator.h>

#include "basics/debugging.h"
#include "basics/number_utils.h"
#include "basics/static_strings.h"
#include "basics/string_utils.h"
#include "basics/utf8_helper.h"
#include "vpack/vpack_helper.h"

using namespace sdb;
using namespace sdb::basics;

namespace {
std::string UrlDecode(const char* begin, const char* end) {
  std::string out;
  out.reserve(static_cast<size_t>(end - begin));
  for (const char* i = begin; i != end; ++i) {
    std::string::value_type c = (*i);
    if (c == '%') {
      if (i + 2 < end) {
        int h = string_utils::Hex2int(i[1], 256) << 4;
        h += string_utils::Hex2int(i[2], 256);
        if (h >= 256) {
          SDB_THROW(ERROR_BAD_PARAMETER,
                    "invalid encoding value in request URL");
        }
        out.push_back(static_cast<char>(h & 0xFF));
        i += 2;
      } else {
        SDB_THROW(ERROR_BAD_PARAMETER, "invalid encoding value in request URL");
      }
    } else if (c == '+') {
      out.push_back(' ');
    } else {
      out.push_back(c);
    }
  }

  return out;
}
}  // namespace

HttpRequest::HttpRequest(const ConnectionInfo& connection_info, uint64_t mid)
  : GeneralRequest(connection_info, mid), _validated_payload(false) {
  _content_type = ContentType::Unset;
  _content_type_response = ContentType::Json;
  SDB_ASSERT(_memory_usage == 0);
  _memory_usage += sizeof(HttpRequest);
}

HttpRequest::~HttpRequest() {
#ifdef SDB_DEV
  auto expected = sizeof(HttpRequest) + _full_url.size() +
                  _request_path.size() + _database_name.size() + _user.size() +
                  _prefix.size() + _content_type_response_plain.size() +
                  _payload.size();
  for (const auto& it : _suffixes) {
    expected += it.size();
  }
  for (const auto& it : _headers) {
    expected += it.first.size() + it.second.size();
  }
  for (const auto& it : _cookies) {
    expected += it.first.size() + it.second.size();
  }
  for (const auto& it : _values) {
    expected += it.first.size() + it.second.size();
  }
  for (const auto& it : _array_values) {
    expected += it.first.size();
    for (const auto& it2 : it.second) {
      expected += it2.size();
    }
  }
  if (_vpack_builder) {
    expected += _vpack_builder->bufferRef().size();
  }

  SDB_ASSERT(_memory_usage == expected, "expected memory usage: ", expected,
             ", actual: ", _memory_usage,
             ", diff: ", (_memory_usage - expected));
#endif
}

void HttpRequest::appendBody(const char* data, size_t size) {
  _payload.append(data, size);
  _memory_usage += size;
}

void HttpRequest::appendNullTerminator() {
  _payload.push_back('\0');
  _payload.resetTo(_payload.size() - 1);
  // DO NOT increase memory usage here
}

void HttpRequest::clearBody() noexcept {
  auto old = _payload.size();
  _payload.clear();
  SDB_ASSERT(_memory_usage >= old);
  _memory_usage -= old;
}

void HttpRequest::parseUrl(const char* path, size_t length) {
  std::string tmp;
  tmp.reserve(length);
  // get rid of '//'
  for (size_t i = 0; i < length; ++i) {
    tmp.push_back(path[i]);
    if (path[i] == '/') {
      while (i + 1 < length && path[i + 1] == '/') {
        ++i;
      }
    }
  }

  const char* start = tmp.data();
  const char* end = start + tmp.size();
  // look for database name in URL
  if (end - start >= 5) {
    const char* q = start;

    // check if the prefix is "_db"
    if (q[0] == '/' && q[1] == '_' && q[2] == 'd' && q[3] == 'b' &&
        q[4] == '/') {
      // request contains database name
      q += 5;
      start = q;

      // read until end of database name
      while (q < end) {
        if (*q == '/' || *q == '?' || *q == ' ' || *q == '\n' || *q == '\r') {
          break;
        }
        ++q;
      }

      SDB_ASSERT(q >= start);
      setDatabaseName(::UrlDecode(start, q));
      if (_database_name != NormalizeUtf8ToNFC(_database_name)) {
        SDB_THROW(ERROR_SERVER_ILLEGAL_NAME,
                  "database name is not properly UTF-8 NFC-normalized");
      }
      setFullUrl(std::string_view(q, end - q));
      start = q;
    } else {
      setFullUrl(std::string_view(start, end - start));
    }
  } else {
    setFullUrl(std::string_view(start, end - start));
  }

  SDB_ASSERT(!_full_url.empty());

  const char* q = start;
  while (q != end && *q != '?') {
    ++q;
  }

  if (q == end || *q == '?') {
    setRequestPath(std::string_view(start, q - start));
  }
  if (q == end) {
    return;
  }

  bool key_phase = true;
  const char* key_begin = ++q;
  const char* key_end = key_begin;
  const char* value_begin = nullptr;

  while (q != end) {
    if (key_phase) {
      key_end = q;
      if (*q == '=') {
        key_phase = false;
        value_begin = q + 1;
      }
      ++q;
      continue;
    }

    bool is_ampersand = (*q == '&');

    if (q + 1 == end || *(q + 1) == '&' || is_ampersand) {
      if (!is_ampersand) {
        ++q;  // skip ahead
      }

      std::string val = ::UrlDecode(value_begin, q);
      if (key_end - key_begin > 2 && *(key_end - 2) == '[' &&
          *(key_end - 1) == ']') {
        // found parameter xxx[]
        setArrayValue(::UrlDecode(key_begin, key_end - 2), std::move(val));
      } else {
        setValue(::UrlDecode(key_begin, key_end), std::move(val));
      }
      key_phase = true;
      key_begin = q + 1;

      if (!is_ampersand) {
        continue;
      }
    }
    ++q;
  }
}

void HttpRequest::setHeader(std::string key, std::string value) {
  absl::AsciiStrToLower(&key);

  if (key == StaticStrings::kContentLength) {
    size_t len = number_utils::AtoiZero<uint64_t>(value.c_str(),
                                                  value.c_str() + value.size());
    if (_payload.capacity() < len) {
      // lets not reserve more than 64MB at once
      uint64_t max_reserve = std::min<uint64_t>(2 << 26, len);
      _payload.reserve(max_reserve);
    }
    // do not store this header
    return;
  }

  if (key == StaticStrings::kAccept) {
    _content_type_response =
      rest::StringToContentType(value, /*default*/ ContentType::Json);
    if (value.find(',') != std::string::npos) {
      setStringValue(_content_type_response_plain, std::move(value));
    } else {
      setStringValue(_content_type_response_plain, std::string());
    }
    return;
  } else if (_content_type == ContentType::Unset &&
             key == StaticStrings::kContentTypeHeader) {
    auto res = rest::StringToContentType(value, /*default*/ ContentType::Unset);
    // simon: the "requests" module by default uses the "text/plain"
    // content-types for JSON in most tests. As soon as someone fixes all the
    // tests we can enable these again.
    if (res == ContentType::Json || res == ContentType::VPack ||
        res == ContentType::Dump) {
      _content_type = res;
      return;
    }
  } else if (key == StaticStrings::kAcceptEncoding) {
    _accept_encoding = parseAcceptEncoding(value);
  } else if (key == StaticStrings::kCookie) {
    parseCookies(value.c_str(), value.size());
    return;
  }

  auto memory_usage = key.size() + value.size();
  auto it = _headers.try_emplace(std::move(key), std::move(value));
  if (!it.second) {
    auto old = it.first->first.size() + it.first->second.size();
    _headers[std::move(key)] = std::move(value);
    _memory_usage -= old;
  }
  _memory_usage += memory_usage;
}

void HttpRequest::setValues(char* buffer, char* end) {
  char* key_begin = nullptr;
  char* key = nullptr;

  char* value_begin = nullptr;
  char* value = nullptr;

  enum { kKey, kValue } phase = kKey;
  enum { kNormal, kHeX1, kHeX2 } reader = kNormal;

  int hex = 0;

  const char amb = '&';
  const char equal = '=';
  const char percent = '%';
  const char plus = '+';

  for (key_begin = key = buffer; buffer < end; buffer++) {
    char next = *buffer;

    if (phase == kKey && next == equal) {
      phase = kValue;

      value_begin = value = buffer + 1;

      continue;
    } else if (next == amb) {
      phase = kKey;

      *key = '\0';

      // check for missing value phase
      if (value_begin == nullptr) {
        value_begin = value = key;
      } else {
        *value = '\0';
      }

      if (key - key_begin > 2 && (*(key - 2)) == '[' && (*(key - 1)) == ']') {
        // found parameter xxx[]
        *(key - 2) = '\0';
        setArrayValue(std::string_view(key_begin, key - key_begin - 2),
                      std::string_view(value_begin, value - value_begin));
      } else {
        setValue(std::string_view(key_begin, key - key_begin),
                 std::string_view(value_begin, value - value_begin));
      }

      key_begin = key = buffer + 1;
      value_begin = value = nullptr;

      continue;
    } else if (next == percent) {
      reader = kHeX1;
      continue;
    } else if (reader == kHeX1) {
      int h1 = string_utils::Hex2int(next, -1);

      if (h1 == -1) {
        reader = kNormal;
        --buffer;
        continue;
      }

      hex = h1 * 16;
      reader = kHeX2;
      continue;
    } else if (reader == kHeX2) {
      int h1 = string_utils::Hex2int(next, -1);

      if (h1 == -1) {
        --buffer;
      } else {
        hex += h1;
      }

      reader = kNormal;
      next = static_cast<char>(hex);
    } else if (next == plus) {
      next = ' ';
    }

    if (phase == kKey) {
      *key++ = next;
    } else {
      *value++ = next;
    }
  }

  if (key_begin != key) {
    *key = '\0';

    // check for missing value phase
    if (value_begin == nullptr) {
      value_begin = value = key;
    } else {
      *value = '\0';
    }

    if (key - key_begin > 2 && (*(key - 2)) == '[' && (*(key - 1)) == ']') {
      // found parameter xxx[]
      *(key - 2) = '\0';
      setArrayValue(std::string_view(key_begin, key - key_begin - 2),
                    std::string_view(value_begin, value - value_begin));
    } else {
      setValue(std::string_view(key_begin, key - key_begin),
               std::string_view(value_begin, value - value_begin));
    }
  }
}

void HttpRequest::setCookie(std::string key, std::string value) {
  auto memory_usage = key.size() + value.size();
  auto it = _cookies.try_emplace(std::move(key), std::move(value));
  if (!it.second) {
    auto old = it.first->first.size() + it.first->second.size();
    _cookies[std::move(key)] = std::move(value);
    _memory_usage -= old;
  }
  _memory_usage += memory_usage;
}

void HttpRequest::parseCookies(const char* buffer, size_t length) {
  char* key_begin = nullptr;
  char* key = nullptr;

  char* value_begin = nullptr;
  char* value = nullptr;

  enum { kKey, kValue } phase = kKey;
  enum { kNormal, kHeX1, kHeX2 } reader = kNormal;

  int hex = 0;

  const char amb = ';';
  const char equal = '=';
  const char percent = '%';
  const char space = ' ';

  char* buffer2 = (char*)buffer;
  char* end = buffer2 + length;

  for (key_begin = key = buffer2; buffer2 < end; buffer2++) {
    char next = *buffer2;

    if (phase == kKey && next == equal) {
      phase = kValue;

      value_begin = value = buffer2 + 1;

      continue;
    } else if (next == amb) {
      phase = kKey;

      *key = '\0';

      // check for missing value phase
      if (value_begin == nullptr) {
        value_begin = value = key;
      } else {
        *value = '\0';
      }

      setCookie(std::string(key_begin, key - key_begin),
                std::string(value_begin, value - value_begin));

      // keyBegin = key = buffer2 + 1;
      while (*(key_begin = key = buffer2 + 1) == space && buffer2 < end) {
        buffer2++;
      }
      value_begin = value = nullptr;

      continue;
    } else if (next == percent) {
      reader = kHeX1;
      continue;
    } else if (reader == kHeX1) {
      int h1 = string_utils::Hex2int(next, -1);

      if (h1 == -1) {
        reader = kNormal;
        --buffer2;
        continue;
      }

      hex = h1 * 16;
      reader = kHeX2;
      continue;
    } else if (reader == kHeX2) {
      int h1 = string_utils::Hex2int(next, -1);

      if (h1 == -1) {
        --buffer2;
      } else {
        hex += h1;
      }

      reader = kNormal;
      next = static_cast<char>(hex);
    }

    if (phase == kKey) {
      *key++ = next;
    } else {
      *value++ = next;
    }
  }

  if (key_begin != key) {
    *key = '\0';

    // check for missing value phase
    if (value_begin == nullptr) {
      value_begin = key;
    } else {
      *value = '\0';
    }

    setCookie(std::string(key_begin, key - key_begin),
              std::string(value_begin, value - value_begin));
  }
}

const std::string& HttpRequest::cookieValue(const std::string& key) const {
  auto it = _cookies.find(key);

  if (it == _cookies.end()) {
    return StaticStrings::kEmpty;
  }

  return it->second;
}

const std::string& HttpRequest::cookieValue(const std::string& key,
                                            bool& found) const {
  auto it = _cookies.find(key);

  if (it == _cookies.end()) {
    found = false;
    return StaticStrings::kEmpty;
  }

  found = true;
  return it->second;
}

std::string_view HttpRequest::rawPayload() const {
  return std::string_view(reinterpret_cast<const char*>(_payload.data()),
                          _payload.size());
}

vpack::Slice HttpRequest::payload(bool strict_validation) {
  if (_content_type == ContentType::Unset ||
      _content_type == ContentType::Json) {
    if (!_payload.empty()) {
      if (!_vpack_builder) {
        SDB_ASSERT(!_validated_payload);
        const vpack::Options* options = validationOptions(strict_validation);
        vpack::Parser parser(options);
        parser.parse(_payload.data(), _payload.size());
        _vpack_builder = parser.steal();
        _validated_payload = true;
        _memory_usage += _vpack_builder->bufferRef().size();
      }
      SDB_ASSERT(_validated_payload);
      return vpack::Slice(_vpack_builder->slice());
    }
    // no body
    // fallthrough intentional
  } else if (_content_type == ContentType::VPack) {
    if (!_payload.empty()) {
      if (!_validated_payload) {
        const vpack::Options* options = validationOptions(strict_validation);
        vpack::Validator validator(options);
        _validated_payload = validator.validate(
          _payload.data(), _payload.size());  // throws on error
      }
      SDB_ASSERT(_validated_payload);
      return vpack::Slice(reinterpret_cast<const uint8_t*>(_payload.data()));
    }
    // no body
    // fallthrough intentional
  }

  return vpack::Slice::noneSlice();
}

EncodingType HttpRequest::parseAcceptEncoding(std::string_view value) const {
  // let fuerte translate the content encoding for us
  switch (fuerte::ToContentEncoding(value)) {
    case fuerte::ContentEncoding::Deflate:
      return EncodingType::Deflate;
    case fuerte::ContentEncoding::Gzip:
      return EncodingType::GZip;
    case fuerte::ContentEncoding::Lz4:
      return EncodingType::Lz4;
    default:
      // everything else counts as unset
      return EncodingType::Unset;
  }
}
