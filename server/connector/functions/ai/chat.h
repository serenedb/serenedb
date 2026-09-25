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

#include <cstdint>
#include <duckdb/common/types/value.hpp>
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::connector::ai {

struct ChatConfig {
  std::string url;
  std::string api_key;
  std::string model;
  double temperature = 0;
  int32_t max_tokens = 0;
  std::string response_format;

  bool operator==(const ChatConfig&) const = default;
};

struct ChatTemplate {
  std::string prefix;
  std::string suffix;

  bool operator==(const ChatTemplate&) const = default;
};

enum class ChatFinish : uint8_t {
  Complete,
  Truncated,
  Filtered,
  Action,
};

struct ChatReply {
  std::string content;
  std::string reason;
  ChatFinish finish = ChatFinish::Complete;
  uint64_t output_tokens = 0;
};

ChatConfig BindChat(duckdb::ClientContext& context, std::string_view fn,
                    const std::optional<std::string>& model,
                    const std::optional<std::string>& secret_name,
                    const std::optional<duckdb::Value>& temperature,
                    const std::optional<duckdb::Value>& max_tokens,
                    double default_temperature);

std::string StrictJsonSchema(std::string_view name, std::string_view properties,
                             std::span<const std::string> required);

ChatTemplate MakeChatTemplate(const ChatConfig& cfg, std::string_view system);

std::string BuildChatBody(const ChatTemplate& chat, std::string_view user);

ChatReply ParseChatReply(std::string_view fn, std::string_view body);

void CheckFinish(std::string_view fn, const ChatReply& reply,
                 int32_t max_tokens);

}  // namespace sdb::connector::ai
