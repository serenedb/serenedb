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

#include "network/http/test_api_handlers.h"

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <yaclib/async/make.hpp>

#include "network/http/handler.h"

namespace sdb::network {
namespace {

size_t ParseUInt(std::string_view text) {
  size_t value = 0;
  std::from_chars(text.data(), text.data() + text.size(), value);
  return value;
}

// POST /_test/echo -> request body verbatim, reflecting the content-type.
class EchoHandler final : public HttpHandler {
 public:
  yaclib::Future<HttpResponse> Handle(const HttpRequest& request) override {
    HttpResponse response;
    response.reason = "OK";
    const std::string_view content_type =
      request.Header(HttpHeader::ContentType);
    response.content_type = content_type.empty() ? "application/octet-stream"
                                                 : std::string{content_type};
    for (const auto buffer : request.body) {
      response.body.append(static_cast<const char*>(buffer.data()),
                           buffer.size());
    }
    return yaclib::MakeFuture(std::move(response));
  }
};

// GET /_test/ping -> tiny fixed body (transport-latency baseline).
class PingHandler final : public HttpHandler {
 public:
  yaclib::Future<HttpResponse> Handle(const HttpRequest&) override {
    HttpResponse response = HttpResponse::Json(200, "OK", "pong");
    response.content_type = "text/plain; charset=UTF-8";
    return yaclib::MakeFuture(std::move(response));
  }
};

// GET /_test/bytes?n=N -> N deterministic bytes, streamed in bounded chunks so
// arbitrarily large N never materializes (exercises chunked transfer-encoding).
class BytesHandler final : public HttpHandler {
 public:
  yaclib::Future<HttpResponse> Handle(const HttpRequest& request) override {
    struct State {
      size_t remaining;
      size_t offset = 0;
    };
    auto state = std::make_shared<State>(State{ParseUInt(request.Query("n"))});
    return yaclib::MakeFuture(HttpResponse::Stream(
      200, "OK", "application/octet-stream",
      [state](ChunkSink& sink) -> yaclib::Future<bool> {
        constexpr size_t kChunk = 64 * 1024;
        const size_t take = std::min(state->remaining, kChunk);
        std::string chunk(take, '\0');
        for (size_t i = 0; i < take; ++i) {
          chunk[i] = static_cast<char>('A' + ((state->offset + i) % 26));
        }
        sink.Write(chunk);
        state->remaining -= take;
        state->offset += take;
        return yaclib::MakeFuture(state->remaining > 0);
      }));
  }
};

// POST /_test/fuzz -> tolerates arbitrary/malformed input (never throws);
// returns the received byte count. Oversized bodies are 413'd by the codec.
class FuzzHandler final : public HttpHandler {
 public:
  yaclib::Future<HttpResponse> Handle(const HttpRequest& request) override {
    size_t length = 0;
    for (const auto buffer : request.body) {
      length += buffer.size();
    }
    return yaclib::MakeFuture(HttpResponse::Json(
      200, "OK", absl::StrCat(R"({"received":)", length, "}")));
  }
};

// GET /_test/status?code=NNN -> respond with that status (clamped 100..599).
class StatusHandler final : public HttpHandler {
 public:
  yaclib::Future<HttpResponse> Handle(const HttpRequest& request) override {
    size_t raw = ParseUInt(request.Query("code"));
    if (raw == 0) {
      raw = 200;
    }
    const int code = static_cast<int>(std::clamp<size_t>(raw, 100, 599));
    return yaclib::MakeFuture(HttpResponse::Json(
      code, "Status", absl::StrCat(R"({"code":)", code, "}")));
  }
};

}  // namespace

void RegisterTestApi(HttpRouter& router) {
  router.Add(HttpMethod::Post, "/_test/echo", std::make_unique<EchoHandler>());
  router.Add(HttpMethod::Get, "/_test/ping", std::make_unique<PingHandler>());
  router.Add(HttpMethod::Get, "/_test/bytes", std::make_unique<BytesHandler>());
  router.Add(HttpMethod::Post, "/_test/fuzz", std::make_unique<FuzzHandler>());
  router.Add(HttpMethod::Get, "/_test/status",
             std::make_unique<StatusHandler>());
}

}  // namespace sdb::network
