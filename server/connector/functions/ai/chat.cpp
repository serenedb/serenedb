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

#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <simdjson.h>

#include <duckdb/common/types/value.hpp>
#include <duckdb/function/function.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <tuple>

#include "connector/functions/ai/common.h"
#include "connector/functions/ai/provider_openai.h"

namespace sdb::connector::ai {
namespace {

constexpr std::string_view kChatPath = "/v1/chat/completions";
constexpr int32_t kDefaultMaxTokens = 1024;

}  // namespace

void AddChatOptions(duckdb::FunctionSignature& signature) {
  AddOption(signature, "model", duckdb::LogicalType::VARCHAR);
  AddOption(signature, "secret_name", duckdb::LogicalType::VARCHAR);
  AddOption(signature, "temperature", duckdb::LogicalType::DOUBLE);
  AddOption(signature, "max_tokens", duckdb::LogicalType::INTEGER);
}

ChatConfig BindChat(duckdb::ClientContext& context, std::string_view fn,
                    std::span<duckdb::unique_ptr<duckdb::Expression>> options,
                    double default_temperature) {
  const auto model = FoldString(context, *options[0], fn, "model");
  const auto secret_name = FoldString(context, *options[1], fn, "secret_name");
  const auto temperature =
    FoldArgument(context, *options[2], fn, "temperature");
  const auto max_tokens = FoldArgument(context, *options[3], fn, "max_tokens");
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
  return std::string{builder.view().value()};
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
    .prefix = std::string{prefix.view().value()},
    .suffix = std::string{suffix.view().value()},
  };
}

std::string BuildChatBody(const ChatTemplate& chat, std::string_view user) {
  simdjson::builder::string_builder builder(
    chat.prefix.size() + chat.suffix.size() + user.size() + 16);
  builder.append_raw(chat.prefix);
  builder.escape_and_append_with_quotes(user);
  builder.append_raw(chat.suffix);
  return std::string{builder.view().value()};
}

uint64_t ChatOutputTokens(std::string_view body) {
  simdjson::dom::parser parser;
  simdjson::dom::element doc;
  uint64_t tokens = 0;
  if (parser.parse(body.data(), body.size()).get(doc) == simdjson::SUCCESS) {
    std::ignore = doc["usage"]["completion_tokens"].get(tokens);
  }
  return tokens;
}

std::optional<std::string> Chat(Requester& requester, std::string_view fn,
                                Response response, int32_t max_tokens) {
  const auto body = requester.Accept(std::move(response));
  if (!body) {
    return std::nullopt;
  }
  simdjson::dom::parser parser;
  simdjson::dom::element doc;
  if (parser.parse(*body).get(doc) != simdjson::SUCCESS) {
    ThrowRowError(absl::StrCat(
      fn, ": chat completion response is not valid JSON: ", *body));
  }
  simdjson::dom::element choice;
  if (doc["choices"].at(0).get(choice) != simdjson::SUCCESS) {
    ThrowRowError(
      absl::StrCat(fn, ": chat completion response has no 'choices': ", *body));
  }
  std::string_view content;
  std::ignore = choice["message"]["content"].get(content);
  std::string_view finish;
  if (std::string_view refusal;
      choice["message"]["refusal"].get(refusal) == simdjson::SUCCESS &&
      !refusal.empty()) {
    content = refusal;
    finish = "refusal";
  } else {
    std::ignore = choice["finish_reason"].get(finish);
  }
  if (finish == "length") {
    ThrowRowError(absl::StrCat(fn, ": model reply was cut off at max_tokens (",
                               max_tokens, ")"));
  }
  if (finish == "refusal" || finish == "content_filter") {
    ThrowRowError(absl::StrCat(
      fn, ": provider withheld or filtered the reply (finish_reason '", finish,
      "'): ", content));
  }
  if (finish == "tool_calls" || finish == "function_call") {
    ThrowRowError(absl::StrCat(fn, ": model stopped to call a tool (",
                               "finish_reason '", finish,
                               "') instead of answering"));
  }
  return std::string{absl::StripAsciiWhitespace(content)};
}

}  // namespace sdb::connector::ai
