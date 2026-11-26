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

#include "network/http/test/handlers.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <absl/synchronization/mutex.h>
#include <fast_float/fast_float.h>

#include <algorithm>
#include <atomic>
#include <charconv>
#include <cstddef>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/query_result.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <yaclib/async/make.hpp>
#include <yaclib/coro/coro.hpp>
#include <yaclib/coro/future.hpp>
#include <yaclib/coro/task.hpp>
#include <yaclib/lazy/make.hpp>

#include "connector/duckdb_copy_filesystem.h"
#include "network/http/common.h"
#include "network/http/handler.h"
#include "pg/connection_context.h"

namespace sdb::network::http::test {
namespace {

size_t ParseUInt(std::string_view text) {
  size_t value = 0;
  fast_float::from_chars(text.data(), text.data() + text.size(), value);
  return value;
}

// POST /_test/echo -> request body verbatim, reflecting the content-type.
class EchoHandler final : public HttpHandler {
 public:
  yaclib::Task<> Handle(RequestContext&, const HttpRequest& request,
                        http::HttpResponseWriter& writer) override {
    const std::string_view content_type =
      request.Header(HttpHeader::ContentType);
    size_t length = 0;
    for (const auto buffer : request.body) {
      length += buffer.size();
    }
    writer.WriteHead(
      HttpStatus::Ok,
      content_type.empty() ? "application/octet-stream" : content_type, length);
    for (const auto buffer : request.body) {
      writer.Write({static_cast<const char*>(buffer.data()), buffer.size()});
    }
    writer.Finish();
    return yaclib::MakeTask();
  }
};

// GET /_test/ping -> tiny fixed body (transport-latency baseline).
class PingHandler final : public HttpHandler {
 public:
  yaclib::Task<> Handle(RequestContext&, const HttpRequest&,
                        http::HttpResponseWriter& writer) override {
    writer.Text(HttpStatus::Ok, "pong");
    return yaclib::MakeTask();
  }
};

// GET /_test/bytes?n=N -> N deterministic bytes, streamed in bounded pieces
// with real backpressure so arbitrarily large N never materializes.
class BytesHandler final : public HttpHandler {
 public:
  yaclib::Task<> Handle(RequestContext&, const HttpRequest& request,
                        http::HttpResponseWriter& writer) override {
    size_t remaining = ParseUInt(request.Query("n"));
    writer.WriteHeadChunked(HttpStatus::Ok, "application/octet-stream");
    constexpr size_t kPiece = 64 * 1024;
    std::string piece;
    size_t offset = 0;
    while (remaining > 0 && !writer.Broken()) {
      const size_t take = std::min(remaining, kPiece);
      piece.resize(take);
      for (size_t i = 0; i < take; ++i) {
        piece[i] = static_cast<char>('A' + ((offset + i) % 26));
      }
      writer.Write(piece);
      remaining -= take;
      offset += take;
      co_await writer.Drain();
    }
    writer.Finish();
    co_return {};
  }
};

// POST /_test/fuzz -> tolerates arbitrary/malformed input (never throws);
// returns the received byte count. Oversized bodies are 413'd by the codec.
class FuzzHandler final : public HttpHandler {
 public:
  yaclib::Task<> Handle(RequestContext&, const HttpRequest& request,
                        http::HttpResponseWriter& writer) override {
    size_t length = 0;
    for (const auto buffer : request.body) {
      length += buffer.size();
    }
    writer.Json(HttpStatus::Ok, absl::StrCat(R"({"received":)", length, "}"));
    return yaclib::MakeTask();
  }
};

class SessionUserHandler final : public HttpHandler {
 public:
  yaclib::Task<> Handle(RequestContext& context, const HttpRequest&,
                        http::HttpResponseWriter& writer) override {
    auto result = co_await context.RunQuery("SELECT current_user", false);
    if (result->HasError()) {
      writer.Error(HttpStatus::InternalError, "query_failed");
    } else {
      writer.Text(HttpStatus::Ok,
                  result->Collection().GetValue(0, 0).ToString());
    }
    co_return {};
  }
};

class CopyStream final : public connector::CopyOutput {
 public:
  explicit CopyStream(http::HttpResponseWriter& writer) : _writer{writer} {}

  void Write(std::string_view bytes) final {
    absl::MutexLock lock{&_mutex};
    if (!_writer.HeadWritten()) {
      _writer.WriteHeadChunked(HttpStatus::Ok, "application/octet-stream");
    }
    if (!bytes.empty() && !_writer.Broken()) {
      _writer.Write(bytes);
    }
    if (bytes.size() > _largest.load(std::memory_order_relaxed)) {
      _largest.store(bytes.size(), std::memory_order_relaxed);
    }
  }

  bool Full(duckdb::optional_ptr<const duckdb::InterruptState> wake) final {
    return _writer.Full(_largest.load(std::memory_order_relaxed), wake);
  }

 private:
  absl::Mutex _mutex;
  http::HttpResponseWriter& _writer;
  std::atomic<size_t> _largest{0};
};

class CopyHandler final : public HttpHandler {
 public:
  yaclib::Task<> Handle(RequestContext& context, const HttpRequest& request,
                        http::HttpResponseWriter& writer) override {
    CopyStream output{writer};
    auto& connection = ConnectionContext::Get(*context.Connection().context);
    connection.SetSideChannel<connector::CopyOutput>(&output);
    absl::Cleanup detach = [&] {
      connection.SetSideChannel<connector::CopyOutput>(nullptr);
    };
    auto result = co_await context.RunQuery(FlattenBody(request.body), true);
    if (writer.HeadWritten()) {
      if (!result->HasError()) {
        writer.Finish();
      }
    } else if (result->HasError()) {
      writer.Text(HttpStatus::BadRequest, result->GetError());
    } else {
      writer.Fixed(HttpStatus::Ok, "application/octet-stream", "");
    }
    co_return {};
  }
};

// GET /_test/status?code=NNN -> respond with that status (clamped 100..599).
class StatusHandler final : public HttpHandler {
 public:
  yaclib::Task<> Handle(RequestContext&, const HttpRequest& request,
                        http::HttpResponseWriter& writer) override {
    size_t raw = ParseUInt(request.Query("code"));
    if (raw == 0) {
      raw = 200;
    }
    const int code = static_cast<int>(std::clamp<size_t>(raw, 100, 599));
    writer.Json(static_cast<HttpStatus>(code),
                absl::StrCat(R"({"code":)", code, "}"));
    return yaclib::MakeTask();
  }
};

}  // namespace

std::unique_ptr<HttpHandler> Make(Endpoint endpoint) {
  switch (endpoint) {
    case Endpoint::Echo:
      return std::make_unique<EchoHandler>();
    case Endpoint::Ping:
      return std::make_unique<PingHandler>();
    case Endpoint::Bytes:
      return std::make_unique<BytesHandler>();
    case Endpoint::Fuzz:
      return std::make_unique<FuzzHandler>();
    case Endpoint::Status:
      return std::make_unique<StatusHandler>();
    case Endpoint::SessionUser:
      return std::make_unique<SessionUserHandler>();
    case Endpoint::Copy:
      return std::make_unique<CopyHandler>();
  }
  SDB_UNREACHABLE();
}

}  // namespace sdb::network::http::test
