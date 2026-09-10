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

#include "network/http/mcp/handlers.h"

#include <absl/strings/str_cat.h>
#include <simdjson.h>

#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <yaclib/coro/task.hpp>
#include <yaclib/lazy/make.hpp>

#include "basics/build.h"
#include "basics/serializer.h"
#include "basics/simdjson_sink.h"
#include "network/http/common.h"
#include "network/http/handler.h"
#include "network/http/mcp/tools.h"
#include "network/http/mcp/wire.h"

namespace sdb::network::http::mcp {
namespace {

constexpr std::string_view kLatestProtocol = "2025-06-18";
constexpr std::string_view kKnownProtocols[] = {"2025-06-18", "2025-03-26",
                                                "2024-11-05"};

// JSON-RPC 2.0 error codes
enum class RpcCode : int {
  ParseError = -32700,
  InvalidRequest = -32600,
  MethodNotFound = -32601,
  InvalidParams = -32602,
  ServerError = -32000,
};

constexpr std::string_view kInstructions =
  "SereneDB documentation server. search_docs finds documentation sections "
  "by relevance; read_doc returns a whole page or one section as Markdown "
  "with its SQL examples; list_docs lists the available pages.";

// --- Wire shapes; field names are the JSON keys ------------------------------

struct RpcEnvelope {
  RpcId id;
  std::string method;
};

// One shape for every method: each reads the fields it needs, the rest stay at
// their defaults.
struct RpcParams {
  std::string protocolVersion;  // NOLINT(readability-identifier-naming)
  std::string name;
  ToolArgs arguments;
};

struct RpcParamsEnvelope {
  RpcParams params;
};

struct RpcError {
  int64_t code;
  std::string message;
};

struct ErrorResponse {
  std::string jsonrpc{"2.0"};
  RpcId id;
  RpcError error;
};

template<typename R>
struct ResultResponse {
  std::string jsonrpc{"2.0"};
  RpcId id;
  R result;
};

struct ToolsCapability {
  bool listChanged = false;  // NOLINT(readability-identifier-naming)
};

struct Capabilities {
  ToolsCapability tools;
};

struct ServerInfo {
  std::string name{"serenedb"};
  std::string version{SERENEDB_VERSION};
};

struct InitializeResult {
  std::string protocolVersion;  // NOLINT(readability-identifier-naming)
  Capabilities capabilities;
  ServerInfo serverInfo;  // NOLINT(readability-identifier-naming)
  std::string instructions{kInstructions};
};

struct TextContent {
  std::string type{"text"};
  std::string text;
};

struct ToolCallResult {
  std::vector<TextContent> content;
  bool isError = false;  // NOLINT(readability-identifier-naming)
};

struct RpcRequest {
  RpcId id;
  std::string method;
  RpcParams params;
};

// --- JSON-RPC envelopes ------------------------------------------------------

std::string Error(const RpcId& id, RpcCode code, std::string message) {
  return ToJson(ErrorResponse{
    .id = id, .error = {std::to_underlying(code), std::move(message)}});
}

template<typename R>
std::string Result(const RpcId& id, R result) {
  return ToJson(ResultResponse<R>{.id = id, .result = std::move(result)});
}

// --- initialize --------------------------------------------------------------

std::string Initialize(const RpcRequest& rpc) {
  InitializeResult result{.protocolVersion = std::string{kLatestProtocol}};
  for (const auto known : kKnownProtocols) {
    if (known == rpc.params.protocolVersion) {
      result.protocolVersion = rpc.params.protocolVersion;
    }
  }
  return Result(rpc.id, std::move(result));
}

// --- tools/call --------------------------------------------------------------

yaclib::Task<std::string> ToolsCall(RequestContext& ctx,
                                    const RpcRequest& rpc) {
  if (!KnownTool(rpc.params.name)) {
    co_return Error(rpc.id, RpcCode::InvalidParams,
                    absl::StrCat("Unknown tool: ", rpc.params.name));
  }
  auto tool = co_await CallTool(ctx, rpc.params.name, rpc.params.arguments);
  ToolCallResult result{.content = {{.text = std::move(tool.text)}},
                        .isError = tool.is_error};
  co_return Result(rpc.id, std::move(result));
}

// --- Dispatch ----------------------------------------------------------------

yaclib::Task<std::string> Dispatch(RequestContext& ctx, const RpcRequest& rpc) {
  if (rpc.method == "initialize") {
    co_return Initialize(rpc);
  }
  if (rpc.method == "ping") {
    co_return Result(rpc.id, EmptyObject{});
  }
  if (rpc.method == "tools/list") {
    co_return Result(rpc.id, Tools());
  }
  if (rpc.method == "tools/call") {
    co_return co_await ToolsCall(ctx, rpc);
  }
  co_return Error(rpc.id, RpcCode::MethodNotFound,
                  absl::StrCat("Method not found: ", rpc.method));
}

class McpHandler final : public HttpHandler {
 public:
  yaclib::Task<> Handle(RequestContext& ctx, const HttpRequest& request,
                        HttpResponseWriter& writer) final {
    const auto body = FlattenBody(request.body);
    simdjson::padded_string padded{body};
    simdjson::ondemand::parser parser;
    simdjson::ondemand::document doc;
    simdjson::ondemand::object object;
    if (parser.iterate(padded).get(doc) != simdjson::SUCCESS) {
      writer.Json(HttpStatus::BadRequest,
                  Error({}, RpcCode::ParseError, "Parse error"));
      co_return {};
    }
    if (const auto error = doc.get_object().get(object);
        error != simdjson::SUCCESS) {
      writer.Json(HttpStatus::BadRequest,
                  error == simdjson::INCORRECT_TYPE
                    ? Error({}, RpcCode::InvalidRequest,
                            "Invalid Request: expected a single JSON-RPC "
                            "object")
                    : Error({}, RpcCode::ParseError, "Parse error"));
      co_return {};
    }
    // The envelope first, so a bad id or method is an invalid request while a
    // bad params object is invalid params on a request that is otherwise fine.
    RpcRequest rpc;
    try {
      RpcEnvelope envelope;
      doc.rewind();
      basics::JsonSource source{doc};
      basics::ReadObject(source, envelope);
      rpc.id = std::move(envelope.id);
      rpc.method = std::move(envelope.method);
    } catch (const std::exception& e) {
      writer.Json(HttpStatus::BadRequest,
                  Error({}, RpcCode::InvalidRequest,
                        absl::StrCat("Invalid Request: ", e.what())));
      co_return {};
    }
    if (!rpc.id.present) {
      writer.Fixed(HttpStatus::Accepted, kJsonContentType, "");
      co_return {};
    }
    try {
      RpcParamsEnvelope params;
      doc.rewind();
      basics::JsonSource source{doc};
      basics::ReadObject(source, params);
      rpc.params = std::move(params.params);
    } catch (const std::exception& e) {
      writer.Json(HttpStatus::Ok,
                  Error(rpc.id, RpcCode::InvalidParams,
                        absl::StrCat("Invalid params: ", e.what())));
      co_return {};
    }
    writer.Json(HttpStatus::Ok, co_await Dispatch(ctx, rpc));
    co_return {};
  }
};

class MethodNotAllowedHandler final : public HttpHandler {
 public:
  yaclib::Task<> Handle(RequestContext&, const HttpRequest&,
                        HttpResponseWriter& writer) final {
    writer.Fixed(HttpStatus::MethodNotAllowed, kJsonContentType,
                 Error({}, RpcCode::ServerError, "Method not allowed"),
                 "Allow: POST\r\n");
    return yaclib::MakeTask();
  }
};

}  // namespace

// TODO: serve the conventional /mcp too; needs a way to keep an index named
// 'mcp' reachable under ES's GET /:index at the same time.
void Register(HttpRouter& router) {
  router.Add(HttpMethod::Post, "/_mcp", std::make_unique<McpHandler>());
  for (const auto method : {HttpMethod::Get, HttpMethod::Delete,
                            HttpMethod::Put, HttpMethod::Head}) {
    router.Add(method, "/_mcp", std::make_unique<MethodNotAllowedHandler>());
  }
}

}  // namespace sdb::network::http::mcp
