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

#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <cstdint>
#include <duckdb/common/http_util.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/function/function.hpp>
#include <duckdb/main/client_context_state.hpp>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace duckdb {

class ClientContext;
class Expression;
class ExpressionState;
class ExtensionLoader;

}  // namespace duckdb
namespace sdb::connector::ai {

inline constexpr std::string_view kOpenAISecretType = "openai";
inline constexpr std::string_view kTypeSafeSecretType = "typesafe";

inline constexpr std::string_view kTextDefaultSecretSetting =
  "sdb_ai_text_default_secret";
inline constexpr std::string_view kEmbeddingDefaultSecretSetting =
  "sdb_ai_embedding_default_secret";
inline constexpr std::string_view kJevDefaultSecretSetting =
  "sdb_ai_jev_default_secret";

struct SecretConfig {
  std::string api_key;
  std::string base_url;
  std::string model;
  std::string chat_path;
  std::string embeddings_path;
};

std::optional<duckdb::Value> FoldArgument(duckdb::ClientContext& context,
                                          duckdb::Expression& expr,
                                          std::string_view fn,
                                          std::string_view arg);

std::optional<std::string> FoldString(duckdb::ClientContext& context,
                                      duckdb::Expression& expr,
                                      std::string_view fn,
                                      std::string_view arg);

SecretConfig LoadSecret(duckdb::ClientContext& context, std::string_view fn,
                        const std::optional<std::string>& secret_name,
                        std::string_view default_setting,
                        std::string_view type);

std::string JoinUrl(std::string_view base_url, std::string_view default_base,
                    std::string_view path);

[[noreturn]] void ThrowRowError(std::string message);

std::string ToJson(std::string_view text);

struct Criterion {
  std::string label;
  std::optional<std::string> description;
};

std::vector<Criterion> ParseCriteria(const duckdb::Value& value,
                                     std::string_view fn,
                                     std::string_view param);

struct Inputs {
  static constexpr size_t kNone = std::numeric_limits<size_t>::max();

  std::vector<size_t> slots;
  std::vector<std::string_view> texts;
};

Inputs CollectInputs(duckdb::Vector& input, duckdb::idx_t count, bool dedup,
                     bool skip_empty);

void SetOutputs(duckdb::Vector& result, const Inputs& inputs,
                std::span<const duckdb::Value> outputs);

struct Response {
  uint16_t status = 0;
  std::string body;
  bool skipped = false;
};

class AIQueryUsage final : public duckdb::ClientContextState {
 public:
  void QueryBegin(duckdb::ClientContext&) final;

  void Acquire(duckdb::ClientContext& context, size_t limit);
  void Release();

  std::atomic_uint64_t calls = 0;
  std::atomic_uint64_t output_tokens = 0;

 private:
  absl::Mutex _mutex;
  size_t _in_flight ABSL_GUARDED_BY(_mutex) = 0;
};

class Requester {
 public:
  Requester(duckdb::ClientContext& context, std::string fn, std::string url,
            std::string_view api_key);

  Response Send(size_t worker, std::string_view body);

  std::optional<std::string> Accept(Response response) const;

  std::optional<std::string> Post(size_t worker, std::string_view body) {
    return Accept(Send(worker, body));
  }

  void AddOutputTokens(uint64_t tokens);

  void ForEach(size_t n, absl::FunctionRef<void(size_t, size_t)> fn);

  size_t MaxConcurrency() const noexcept { return _max_concurrency; }

  bool ThrowOnError() const noexcept { return _throw_on_error; }

 private:
  struct Slot {
    duckdb::unique_ptr<duckdb::HTTPParams> params;
    duckdb::unique_ptr<duckdb::HTTPClient> client;
  };

  bool ReserveCall();
  void Sleep(uint64_t ms) const;

  duckdb::ClientContext& _context;
  std::string _fn;
  std::string _url;
  duckdb::HTTPHeaders _headers;
  duckdb::shared_ptr<AIQueryUsage> _usage;
  std::vector<Slot> _slots;
  uint64_t _max_calls;
  uint64_t _max_output_tokens;
  uint32_t _max_retries;
  uint32_t _retry_delay_ms;
  uint32_t _timeout;
  size_t _max_concurrency;
  bool _throw_on_error;
  bool _throw_on_quota;
};

struct AILocalState final : public duckdb::FunctionLocalState {
  AILocalState(duckdb::ClientContext& context, std::string fn, std::string url,
               std::string_view api_key);

  Requester requester;
};

Requester& LocalRequester(duckdb::ExpressionState& state);

void RegisterTextFunctions(duckdb::ExtensionLoader& loader);

void RegisterJevFunction(duckdb::ExtensionLoader& loader);

void RegisterAggregateFunctions(duckdb::ExtensionLoader& loader);

}  // namespace sdb::connector::ai
