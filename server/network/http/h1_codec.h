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

#pragma once

#include <llhttp.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "basics/message_buffer.h"
#include "network/http/request.h"
#include "network/http/response.h"

namespace sdb::network {

enum class H1Event : uint8_t {
  NeedMore,
  Continue,
  Head,
  Error,
};

struct H1FeedResult {
  size_t consumed;
  H1Event event;
};

struct H1BodyResult {
  size_t consumed;
  bool done;
  bool error;
};

struct H1Limits {
  size_t max_head_bytes = 64u * 1024;
  uint64_t max_body_bytes = 64ull * 1024 * 1024;
};

class H1Codec final {
 public:
  explicit H1Codec(H1Limits limits = {});

  [[nodiscard]] H1FeedResult ParseHead(std::string_view input) noexcept;

  [[nodiscard]] HttpRequest TakeHead() noexcept;

  [[nodiscard]] H1BodyResult DecodeBody(std::string_view input,
                                        message::Buffer& out) noexcept;

  [[nodiscard]] uint64_t ContentLength() const noexcept {
    return _content_length;
  }
  [[nodiscard]] bool IsChunked() const noexcept { return _chunked; }
  [[nodiscard]] bool KeepAlive() const noexcept { return _keep_alive; }
  [[nodiscard]] int ErrorStatus() const noexcept { return _error_status; }

  void EncodeHead(const HttpResponse& response, bool keep_alive,
                  message::Buffer& out) const;

  void Reset() noexcept;

 private:
  static int OnUrl(llhttp_t* parser, const char* at, size_t length);
  static int OnHeaderField(llhttp_t* parser, const char* at, size_t length);
  static int OnHeaderValue(llhttp_t* parser, const char* at, size_t length);
  static int OnHeadersComplete(llhttp_t* parser);
  static int OnBody(llhttp_t* parser, const char* at, size_t length);
  static int OnMessageComplete(llhttp_t* parser);

  int Fail(int status) noexcept;

  H1Limits _limits;
  llhttp_t _parser;
  std::string _target;
  std::vector<HttpRequest::Field> _fields;
  uint64_t _content_length{0};
  size_t _head_bytes{0};
  message::Buffer* _body_out{nullptr};
  bool _chunked{false};
  bool _last_was_value{false};
  bool _keep_alive{true};
  bool _paused{false};
  bool _body_done{false};
  int _error_status{0};
  H1Event _event{H1Event::NeedMore};
};

}  // namespace sdb::network
