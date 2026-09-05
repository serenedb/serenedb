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
#include <yaclib/async/make.hpp>
#include <yaclib/coro/task.hpp>
#include <yaclib/lazy/make.hpp>

#include "basics/build.h"
#include "network/http/es/common.h"
#include "network/http/handler.h"
#include "network/http/mcp/tools.h"

namespace sdb::network::http::mcp {
namespace {

constexpr std::string_view kLatestProtocol = "2025-06-18";
constexpr std::string_view kKnownProtocols[] = {"2025-06-18", "2025-03-26",
                                                "2024-11-05"};

constexpr int kParseError = -32700;
constexpr int kInvalidRequest = -32600;
constexpr int kMethodNotFound = -32601;
constexpr int kInvalidParams = -32602;

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

std::string RpcError(std::string_view id, int code, std::string_view message) {
  simdjson::builder::string_builder sb;
  sb.append_raw(R"({"jsonrpc":"2.0","id":)");
  sb.append_raw(id);
  sb.append_raw(R"(,"error":{"code":)");
  sb.append(static_cast<int64_t>(code));
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

std::string Trimmed(std::string_view raw) {
  return std::string{absl::StripAsciiWhitespace(raw)};
}

std::optional<RpcRequest> ParseRequest(std::string_view body, int& code,
                                       std::string& message) {
  simdjson::padded_string padded{body};
  simdjson::ondemand::parser parser;
  simdjson::ondemand::document doc;
  if (parser.iterate(padded).get(doc) != simdjson::SUCCESS) {
    code = kParseError;
    message = "Parse error";
    return std::nullopt;
  }
  simdjson::ondemand::object object;
  if (const auto error = doc.get_object().get(object);
      error != simdjson::SUCCESS) {
    if (error == simdjson::INCORRECT_TYPE) {
      code = kInvalidRequest;
      message = "Invalid Request: expected a single JSON-RPC object";
    } else {
      code = kParseError;
      message = "Parse error";
    }
    return std::nullopt;
  }
  RpcRequest request;
  for (auto field : object) {
    std::string_view key;
    if (field.unescaped_key().get(key) != simdjson::SUCCESS) {
      code = kParseError;
      message = "Parse error";
      return std::nullopt;
    }
    auto value = field.value();
    std::string_view raw;
    if (value.raw_json().get(raw) != simdjson::SUCCESS) {
      code = kParseError;
      message = "Parse error";
      return std::nullopt;
    }
    if (key == "id") {
      request.id = Trimmed(raw);
      request.has_id = true;
    } else if (key == "method") {
      const auto trimmed = Trimmed(raw);
      if (trimmed.size() < 2 || trimmed.front() != '"') {
        code = kInvalidRequest;
        message = "Invalid Request: method must be a string";
        return std::nullopt;
      }
      request.method = trimmed.substr(1, trimmed.size() - 2);
    } else if (key == "params") {
      request.params = Trimmed(raw);
    }
  }
  return request;
}

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

std::string Initialize(const RpcRequest& request) {
  std::string protocol{kLatestProtocol};
  ForEachParam(request.params, [&](std::string_view key, auto value) {
    if (key == "protocolVersion") {
      std::string_view requested;
      if (value.get_string().get(requested) == simdjson::SUCCESS) {
        for (const auto known : kKnownProtocols) {
          if (known == requested) {
            protocol = std::string{requested};
          }
        }
      }
    }
    return true;
  });
  simdjson::builder::string_builder sb;
  sb.append_raw(R"({"protocolVersion":)");
  sb.escape_and_append_with_quotes(protocol);
  sb.append_raw(
    R"(,"capabilities":{"tools":{"listChanged":false}},"serverInfo":{"name":"serenedb","version":)");
  sb.escape_and_append_with_quotes(std::string_view{SERENEDB_VERSION});
  sb.append_raw(R"(},"instructions":)");
  sb.escape_and_append_with_quotes(kInstructions);
  sb.append_raw("}");
  return RpcResult(request.id, std::string_view{sb.view().value()});
}

bool ParseToolCall(std::string_view params, std::string& name, ToolArgs& args,
                   std::string& message) {
  bool ok = true;
  const bool parsed = ForEachParam(params, [&](std::string_view key,
                                               auto value) {
    if (key == "name") {
      std::string_view text;
      if (value.get_string().get(text) != simdjson::SUCCESS) {
        message = "Invalid params: name must be a string";
        return ok = false;
      }
      name = std::string{text};
      return true;
    }
    if (key != "arguments") {
      return true;
    }
    simdjson::ondemand::object arguments;
    if (value.get_object().get(arguments) != simdjson::SUCCESS) {
      message = "Invalid params: arguments must be an object";
      return ok = false;
    }
    for (auto field : arguments) {
      std::string_view arg;
      if (field.unescaped_key().get(arg) != simdjson::SUCCESS) {
        message = "Invalid params: malformed arguments";
        return ok = false;
      }
      auto argument = field.value();
      if (arg == "limit") {
        int64_t limit = 0;
        double as_double = 0;
        if (argument.get_int64().get(limit) == simdjson::SUCCESS) {
          args.limit = limit;
        } else if (argument.get_double().get(as_double) == simdjson::SUCCESS) {
          args.limit = static_cast<int64_t>(as_double);
        } else {
          message = "Invalid params: limit must be an integer";
          return ok = false;
        }
        continue;
      }
      std::optional<std::string>* target = nullptr;
      if (arg == "query") {
        target = &args.query;
      } else if (arg == "path") {
        target = &args.path;
      } else if (arg == "section") {
        target = &args.section;
      } else if (arg == "prefix") {
        target = &args.prefix;
      }
      if (target == nullptr) {
        continue;
      }
      std::string_view text;
      if (argument.get_string().get(text) != simdjson::SUCCESS) {
        message = absl::StrCat("Invalid params: ", arg, " must be a string");
        return ok = false;
      }
      *target = std::string{text};
    }
    return true;
  });
  if (!parsed && ok) {
    message = "Invalid params: expected an object";
    return false;
  }
  return ok;
}

class McpHandler final : public HttpHandler {
 public:
  yaclib::Task<> Handle(RequestContext& ctx, const HttpRequest& request,
                        HttpResponseWriter& writer) override {
    int code = 0;
    std::string message;
    auto rpc = ParseRequest(es::FlattenBody(request.body), code, message);
    if (!rpc) {
      es::WriteJson(writer, 400, RpcError("null", code, message));
      co_return {};
    }
    if (!rpc->has_id) {
      writer.Fixed(202, "application/json", "");
      co_return {};
    }
    if (rpc->method == "initialize") {
      es::WriteJson(writer, 200, Initialize(*rpc));
    } else if (rpc->method == "ping") {
      es::WriteJson(writer, 200, RpcResult(rpc->id, "{}"));
    } else if (rpc->method == "tools/list") {
      es::WriteJson(writer, 200, RpcResult(rpc->id, ToolsListJson()));
    } else if (rpc->method == "tools/call") {
      std::string name;
      ToolArgs args;
      if (!ParseToolCall(rpc->params, name, args, message)) {
        es::WriteJson(writer, 200, RpcError(rpc->id, kInvalidParams, message));
      } else if (!KnownTool(name)) {
        es::WriteJson(writer, 200,
                      RpcError(rpc->id, kInvalidParams,
                               absl::StrCat("Unknown tool: ", name)));
      } else {
        const auto result = co_await CallTool(ctx, name, args);
        es::WriteJson(writer, 200, RpcResult(rpc->id, ToolResultJson(result)));
      }
    } else {
      es::WriteJson(writer, 200,
                    RpcError(rpc->id, kMethodNotFound,
                             absl::StrCat("Method not found: ", rpc->method)));
    }
    co_return {};
  }
};

class MethodNotAllowedHandler final : public HttpHandler {
 public:
  yaclib::Task<> Handle(RequestContext&, const HttpRequest&,
                        HttpResponseWriter& writer) override {
    writer.Fixed(405, "application/json",
                 RpcError("null", -32000, "Method not allowed"),
                 "Allow: POST\r\n");
    return yaclib::MakeTask();
  }
};

}  // namespace

void Register(HttpRouter& router) {
  router.Add(HttpMethod::Post, "/mcp", std::make_unique<McpHandler>());
  for (const auto method : {HttpMethod::Get, HttpMethod::Delete,
                            HttpMethod::Put, HttpMethod::Head}) {
    router.Add(method, "/mcp", std::make_unique<MethodNotAllowedHandler>());
  }
}

}  // namespace sdb::network::http::mcp
