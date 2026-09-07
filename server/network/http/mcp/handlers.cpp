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

#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <simdjson.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <yaclib/coro/task.hpp>
#include <yaclib/lazy/make.hpp>

#include "basics/build.h"
#include "network/http/common.h"
#include "network/http/handler.h"
#include "network/http/mcp/tools.h"

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

struct RpcRequest {
  std::string id = "null";
  bool has_id = false;
  std::string method;
  std::string params;
};

struct RpcFailure {
  RpcCode code;
  std::string message;
};

struct ToolCall {
  std::string name;
  ToolArgs args;
};

using JsonValue = simdjson::simdjson_result<simdjson::ondemand::value>;

// --- JSON-RPC envelopes ------------------------------------------------------

std::string RpcError(std::string_view id, RpcCode code,
                     std::string_view message) {
  simdjson::builder::string_builder sb;
  sb.append_raw(R"({"jsonrpc":"2.0","id":)");
  sb.append_raw(id);
  sb.append_raw(R"(,"error":{"code":)");
  sb.append(static_cast<int64_t>(std::to_underlying(code)));
  sb.append_raw(R"(,"message":)");
  sb.escape_and_append_with_quotes(message);
  sb.append_raw("}}");
  return std::string{sb.view().value()};
}

std::string RpcResult(std::string_view id, std::string_view result_json) {
  return absl::StrCat(R"({"jsonrpc":"2.0","id":)", id, R"(,"result":)",
                      result_json, "}");
}

std::string ToolResultJson(const ToolResult& result) {
  simdjson::builder::string_builder sb;
  sb.append_raw(R"({"content":[{"type":"text","text":)");
  sb.escape_and_append_with_quotes(result.text);
  sb.append_raw("}]");
  if (result.is_error) {
    sb.append_raw(R"(,"isError":true)");
  }
  sb.append_raw("}");
  return std::string{sb.view().value()};
}

// --- Request parsing ---------------------------------------------------------

std::string Trimmed(std::string_view raw) {
  return std::string{absl::StripAsciiWhitespace(raw)};
}

// The id is kept as raw JSON so it is echoed back exactly as sent.
std::optional<RpcFailure> SetRequestField(RpcRequest& request,
                                          std::string_view key,
                                          std::string_view raw) {
  if (key == "id") {
    request.id = Trimmed(raw);
    request.has_id = true;
  } else if (key == "method") {
    const auto trimmed = Trimmed(raw);
    if (trimmed.size() < 2 || trimmed.front() != '"') {
      return RpcFailure{RpcCode::InvalidRequest,
                        "Invalid Request: method must be a string"};
    }
    request.method = trimmed.substr(1, trimmed.size() - 2);
  } else if (key == "params") {
    request.params = Trimmed(raw);
  }
  return std::nullopt;
}

std::optional<RpcFailure> ParseRequest(std::string_view body,
                                       RpcRequest& request) {
  const RpcFailure parse_error{RpcCode::ParseError, "Parse error"};
  simdjson::padded_string padded{body};
  simdjson::ondemand::parser parser;
  simdjson::ondemand::document doc;
  simdjson::ondemand::object object;
  if (parser.iterate(padded).get(doc) != simdjson::SUCCESS) {
    return parse_error;
  }
  if (const auto error = doc.get_object().get(object);
      error != simdjson::SUCCESS) {
    if (error == simdjson::INCORRECT_TYPE) {
      return RpcFailure{RpcCode::InvalidRequest,
                        "Invalid Request: expected a single JSON-RPC object"};
    }
    return parse_error;
  }
  for (auto field : object) {
    std::string_view key;
    std::string_view raw;
    if (field.unescaped_key().get(key) != simdjson::SUCCESS ||
        field.value().raw_json().get(raw) != simdjson::SUCCESS) {
      return parse_error;
    }
    if (auto failure = SetRequestField(request, key, raw)) {
      return failure;
    }
  }
  return std::nullopt;
}

// Visits the fields of the params object; false when params is not an object
// or the visitor stopped.
template<typename Fn>
bool ForEachParam(std::string_view params, Fn&& fn) {
  if (params.empty()) {
    return true;
  }
  simdjson::padded_string padded{params};
  simdjson::ondemand::parser parser;
  simdjson::ondemand::document doc;
  simdjson::ondemand::object object;
  if (parser.iterate(padded).get(doc) != simdjson::SUCCESS ||
      doc.get_object().get(object) != simdjson::SUCCESS) {
    return false;
  }
  for (auto field : object) {
    std::string_view key;
    if (field.unescaped_key().get(key) != simdjson::SUCCESS) {
      return false;
    }
    if (!fn(key, field.value())) {
      return false;
    }
  }
  return true;
}

// --- initialize --------------------------------------------------------------

std::string NegotiatedProtocol(std::string_view params) {
  std::string protocol{kLatestProtocol};
  ForEachParam(params, [&](std::string_view key, JsonValue value) {
    std::string_view requested;
    if (key == "protocolVersion" &&
        value.get_string().get(requested) == simdjson::SUCCESS) {
      for (const auto known : kKnownProtocols) {
        if (known == requested) {
          protocol = std::string{requested};
        }
      }
    }
    return true;
  });
  return protocol;
}

std::string Initialize(const RpcRequest& request) {
  simdjson::builder::string_builder sb;
  sb.append_raw(R"({"protocolVersion":)");
  sb.escape_and_append_with_quotes(NegotiatedProtocol(request.params));
  sb.append_raw(
    R"(,"capabilities":{"tools":{"listChanged":false}},"serverInfo":{"name":"serenedb","version":)");
  sb.escape_and_append_with_quotes(std::string_view{SERENEDB_VERSION});
  sb.append_raw(R"(},"instructions":)");
  sb.escape_and_append_with_quotes(kInstructions);
  sb.append_raw("}");
  return RpcResult(request.id, std::string_view{sb.view().value()});
}

// --- tools/call --------------------------------------------------------------

// One tool argument; names no tool declares are ignored.
std::optional<std::string> ParseArgument(std::string_view name,
                                         JsonValue argument, ToolArgs& args) {
  if (name == "limit") {
    int64_t limit = 0;
    double as_double = 0;
    if (argument.get_int64().get(limit) == simdjson::SUCCESS) {
      args.limit = limit;
      return std::nullopt;
    }
    if (argument.get_double().get(as_double) == simdjson::SUCCESS) {
      args.limit = static_cast<int64_t>(as_double);
      return std::nullopt;
    }
    return "Invalid params: limit must be an integer";
  }
  std::optional<std::string>* target = nullptr;
  if (name == "query") {
    target = &args.query;
  } else if (name == "path") {
    target = &args.path;
  } else if (name == "prefix") {
    target = &args.prefix;
  }
  if (target == nullptr) {
    return std::nullopt;
  }
  std::string_view text;
  if (argument.get_string().get(text) != simdjson::SUCCESS) {
    return absl::StrCat("Invalid params: ", name, " must be a string");
  }
  *target = std::string{text};
  return std::nullopt;
}

std::optional<std::string> ParseArguments(simdjson::ondemand::object arguments,
                                          ToolArgs& args) {
  for (auto field : arguments) {
    std::string_view name;
    if (field.unescaped_key().get(name) != simdjson::SUCCESS) {
      return "Invalid params: malformed arguments";
    }
    if (auto error = ParseArgument(name, field.value(), args)) {
      return error;
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseToolCall(std::string_view params,
                                         ToolCall& call) {
  std::optional<std::string> error;
  const bool is_object =
    ForEachParam(params, [&](std::string_view key, JsonValue value) {
      if (key == "name") {
        std::string_view text;
        if (value.get_string().get(text) != simdjson::SUCCESS) {
          error = "Invalid params: name must be a string";
          return false;
        }
        call.name = std::string{text};
      } else if (key == "arguments") {
        simdjson::ondemand::object arguments;
        if (value.get_object().get(arguments) != simdjson::SUCCESS) {
          error = "Invalid params: arguments must be an object";
          return false;
        }
        error = ParseArguments(arguments, call.args);
        return !error;
      }
      return true;
    });
  if (!error && !is_object) {
    error = "Invalid params: expected an object";
  }
  return error;
}

yaclib::Task<std::string> ToolsCall(RequestContext& ctx,
                                    const RpcRequest& rpc) {
  ToolCall call;
  if (const auto error = ParseToolCall(rpc.params, call)) {
    co_return RpcError(rpc.id, RpcCode::InvalidParams, *error);
  }
  if (!KnownTool(call.name)) {
    co_return RpcError(rpc.id, RpcCode::InvalidParams,
                       absl::StrCat("Unknown tool: ", call.name));
  }
  const auto result = co_await CallTool(ctx, call.name, call.args);
  co_return RpcResult(rpc.id, ToolResultJson(result));
}

// --- Dispatch ----------------------------------------------------------------

yaclib::Task<std::string> Dispatch(RequestContext& ctx, const RpcRequest& rpc) {
  if (rpc.method == "initialize") {
    co_return Initialize(rpc);
  }
  if (rpc.method == "ping") {
    co_return RpcResult(rpc.id, "{}");
  }
  if (rpc.method == "tools/list") {
    co_return RpcResult(rpc.id, ToolsListJson());
  }
  if (rpc.method == "tools/call") {
    co_return co_await ToolsCall(ctx, rpc);
  }
  co_return RpcError(rpc.id, RpcCode::MethodNotFound,
                     absl::StrCat("Method not found: ", rpc.method));
}

class McpHandler final : public HttpHandler {
 public:
  yaclib::Task<> Handle(RequestContext& ctx, const HttpRequest& request,
                        HttpResponseWriter& writer) final {
    RpcRequest rpc;
    if (const auto failure = ParseRequest(FlattenBody(request.body), rpc)) {
      writer.Json(HttpStatus::BadRequest,
                  RpcError("null", failure->code, failure->message));
      co_return {};
    }
    if (!rpc.has_id) {
      writer.Fixed(HttpStatus::Accepted, kJsonContentType, "");
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
                 RpcError("null", RpcCode::ServerError, "Method not allowed"),
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
