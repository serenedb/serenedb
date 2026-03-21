////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016-2018 ArangoDB GmbH, Cologne, Germany
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
/// @author Jan Christoph Uhde
////////////////////////////////////////////////////////////////////////////////

#include "http.h"

namespace sdb::fuerte::http {

static inline int Hex2int(char ch, int error_value = 0) {
  if ('0' <= ch && ch <= '9') {
    return ch - '0';
  } else if ('A' <= ch && ch <= 'F') {
    return ch - 'A' + 10;
  } else if ('a' <= ch && ch <= 'f') {
    return ch - 'a' + 10;
  }

  return error_value;
}

void UrlEncode(std::string& out, std::string_view str) {
  static constexpr char kHexChars[16] = {'0', '1', '2', '3', '4', '5',
                                         '6', '7', '8', '9', 'A', 'B',
                                         'C', 'D', 'E', 'F'};

  const char* src = str.data();
  size_t len = str.size();

  if (len >= (SIZE_MAX - 1) / 3) {
    throw std::overflow_error("out of memory");
  }

  out.reserve(out.size() + 3 * len);

  const char* end = src + len;
  for (; src < end; ++src) {
    if ('0' <= *src && *src <= '9') {
      out.push_back(*src);
    } else if ('a' <= *src && *src <= 'z') {
      out.push_back(*src);
    } else if ('A' <= *src && *src <= 'Z') {
      out.push_back(*src);
    } else if (*src == '-' || *src == '_' || *src == '~') {
      out.push_back(*src);
    } else {
      uint8_t n = (uint8_t)(*src);
      uint8_t n1 = n >> 4;
      uint8_t n2 = n & 0x0F;

      out.push_back('%');
      out.push_back(kHexChars[n1]);
      out.push_back(kHexChars[n2]);
    }
  }
}

void UrlDecode(std::string& out, std::string_view str) {
  const char* src = str.data();
  size_t len = str.size();

  // reserve enough room so we do not need to re-alloc
  out.reserve(out.size() + len + 16);

  const char* end = src + len;

  for (; src < end && *src != '%'; ++src) {
    if (*src == '+') {
      out.push_back(' ');
    } else {
      out.push_back(*src);
    }
  }

  while (src < end) {
    if (src + 2 < end) {
      int h1 = Hex2int(src[1], -1);
      int h2 = Hex2int(src[2], -1);

      if (h1 == -1) {
        src += 1;
      } else {
        if (h2 == -1) {
          out.push_back(h1);
          src += 2;
        } else {
          out.push_back(h1 << 4 | h2);
          src += 3;
        }
      }
    } else if (src + 1 < end) {
      int h1 = Hex2int(src[1], -1);

      if (h1 == -1) {
        src += 1;
      } else {
        out.push_back(h1);
        src += 2;
      }
    } else {
      src += 1;
    }

    for (; src < end && *src != '%'; ++src) {
      if (*src == '+') {
        out.push_back(' ');
      } else {
        out.push_back(*src);
      }
    }
  }
}

void AppendPath(const Request& req, std::string& target) {
  // construct request path ("/_db/<name>/" prefix)
  if (!req.header.database.empty()) {
    target.append("/_db/", 5);
    http::UrlEncode(target, req.header.database);
  }
  // must start with /, also turns /_db/abc into /_db/abc/
  if (req.header.path.empty() || req.header.path[0] != '/') {
    target.push_back('/');
  }

  target.append(req.header.path);

  if (!req.header.parameters.empty()) {
    target.push_back('?');
    for (const auto& p : req.header.parameters) {
      if (target.back() != '?') {
        target.push_back('&');
      }
      http::UrlEncode(target, p.first);
      target.push_back('=');
      http::UrlEncode(target, p.second);
    }
  }
}

}  // namespace sdb::fuerte::http
