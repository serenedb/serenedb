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

#include "connector/functions/ai/chat.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <simdjson.h>

#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "connector/functions/ai/common.h"
#include "connector/functions/ai/provider_openai.h"

namespace sdb::connector::ai {
namespace {

constexpr std::string_view kChatPath = "/v1/chat/completions";
constexpr int32_t kDefaultMaxTokens = 1024;

bool IsControl(char c) {
  const auto u = static_cast<unsigned char>(c);
  return (u < 0x20 && c != '\t' && c != '\n' && c != '\r') || u == 0x7F;
}

std::string_view View(simdjson::builder::string_builder& builder,
                      std::string_view what) {
  std::string_view out;
  if (builder.view().get(out) != simdjson::SUCCESS) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("failed to build ", what));
  }
  return out;
}

}  // namespace

ChatConfig BindChat(duckdb::ClientContext& context, std::string_view fn,
                    const std::optional<std::string>& model,
                    const std::optional<std::string>& secret_name,
                    const std::optional<duckdb::Value>& temperature,
                    const std::optional<duckdb::Value>& max_tokens,
                    double default_temperature) {
  const auto secret = LoadSecret(context, fn, secret_name,
                                 kTextDefaultSecretSetting, kOpenAISecretType);
  ChatConfig chat{
    .url = JoinUrl(secret.base_url, kOpenAIDefaultBaseUrl,
                   secret.chat_path.empty() ? kChatPath : secret.chat_path),
    .api_key = secret.api_key,
    .model = model ? *model : secret.model,
    .temperature =
      temperature ? temperature->GetValue<double>() : default_temperature,
    .max_tokens =
      max_tokens ? max_tokens->GetValue<int32_t>() : kDefaultMaxTokens,
  };
  if (chat.model.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(fn, ": no model given"),
                    ERR_HINT("Pass model := '<name>' or set the secret's "
                             "model option."));
  }
  if (chat.max_tokens <= 0) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(fn, ": \"max_tokens\" must be a positive integer"));
  }
  return chat;
}

std::string StrictJsonSchema(std::string_view name, std::string_view properties,
                             std::span<const std::string> required) {
  simdjson::builder::string_builder builder(128 + name.size() +
                                            properties.size());
  builder.append_raw(R"({"type":"json_schema","json_schema":{"name":)");
  builder.escape_and_append_with_quotes(name);
  builder.append_raw(
    R"(,"strict":true,"schema":{"type":"object","properties":)");
  builder.append_raw(properties);
  builder.append_raw(R"(,"required":[)");
  for (size_t i = 0; i != required.size(); ++i) {
    if (i != 0) {
      builder.append_comma();
    }
    builder.escape_and_append_with_quotes(required[i]);
  }
  builder.append_raw(R"(],"additionalProperties":false}}})");
  return std::string{View(builder, "a JSON schema response format")};
}

ChatTemplate MakeChatTemplate(const ChatConfig& cfg, std::string_view system) {
  simdjson::builder::string_builder prefix(128 + cfg.model.size() +
                                           system.size());
  prefix.append_raw(R"({"model":)");
  prefix.escape_and_append_with_quotes(cfg.model);
  prefix.append_raw(R"(,"messages":[{"role":"system","content":)");
  prefix.escape_and_append_with_quotes(system);
  prefix.append_raw(R"(},{"role":"user","content":)");

  simdjson::builder::string_builder suffix(128 + cfg.response_format.size());
  suffix.append_raw(R"(}],"temperature":)");
  suffix.append(cfg.temperature);
  suffix.append_raw(R"(,"max_tokens":)");
  suffix.append(cfg.max_tokens);
  if (!cfg.response_format.empty()) {
    suffix.append_raw(R"(,"response_format":)");
    suffix.append_raw(cfg.response_format);
  }
  suffix.append_raw("}");
  return {
    .prefix = std::string{View(prefix, "a chat completion request")},
    .suffix = std::string{View(suffix, "a chat completion request")},
  };
}

std::string BuildChatBody(const ChatTemplate& chat, std::string_view user) {
  std::string sanitized;
  if (absl::c_any_of(user, IsControl)) {
    sanitized = user;
    absl::c_replace_if(sanitized, IsControl, ' ');
    user = sanitized;
  }
  simdjson::builder::string_builder builder(
    chat.prefix.size() + chat.suffix.size() + user.size() + 16);
  builder.append_raw(chat.prefix);
  builder.escape_and_append_with_quotes(user);
  builder.append_raw(chat.suffix);
  return std::string{View(builder, "a chat completion request")};
}

ChatReply ParseChatReply(std::string_view fn, std::string_view body) {
  simdjson::dom::parser parser;
  simdjson::dom::element doc;
  if (parser.parse(body.data(), body.size()).get(doc) != simdjson::SUCCESS) {
    ThrowRowError(
      absl::StrCat(fn, ": chat completion response is not valid JSON: ", body));
  }
  ChatReply reply;
  if (uint64_t tokens = 0;
      doc["usage"]["completion_tokens"].get(tokens) == simdjson::SUCCESS) {
    reply.output_tokens = tokens;
  }
  simdjson::dom::element choice;
  if (doc["choices"].at(0).get(choice) != simdjson::SUCCESS) {
    ThrowRowError(
      absl::StrCat(fn, ": chat completion response has no 'choices': ", body));
  }
  if (std::string_view content;
      choice["message"]["content"].get(content) == simdjson::SUCCESS) {
    reply.content = content;
  }
  if (std::string_view refusal;
      choice["message"]["refusal"].get(refusal) == simdjson::SUCCESS &&
      !refusal.empty()) {
    reply.content = refusal;
    reply.reason = "refusal";
    reply.finish = ChatFinish::Filtered;
    return reply;
  }
  if (std::string_view finish;
      choice["finish_reason"].get(finish) == simdjson::SUCCESS) {
    reply.reason = finish;
    if (finish == "length") {
      reply.finish = ChatFinish::Truncated;
    } else if (finish == "content_filter") {
      reply.finish = ChatFinish::Filtered;
    } else if (finish == "tool_calls" || finish == "function_call") {
      reply.finish = ChatFinish::Action;
    }
  }
  return reply;
}

void CheckFinish(std::string_view fn, const ChatReply& reply,
                 int32_t max_tokens) {
  switch (reply.finish) {
    case ChatFinish::Complete:
      return;
    case ChatFinish::Truncated:
      ThrowRowError(absl::StrCat(
        fn, ": model reply was cut off at max_tokens (", max_tokens, ")"));
    case ChatFinish::Filtered:
      ThrowRowError(absl::StrCat(
        fn, ": provider withheld or filtered the reply (finish_reason '",
        reply.reason, "'): ", reply.content));
    case ChatFinish::Action:
      ThrowRowError(absl::StrCat(fn, ": model stopped to call a tool (",
                                 "finish_reason '", reply.reason,
                                 "') instead of answering"));
  }
}

}  // namespace sdb::connector::ai
