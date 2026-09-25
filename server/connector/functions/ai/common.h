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

#include <atomic>
#include <cstdint>
#include <duckdb/common/http_util.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/function/function.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context_state.hpp>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace duckdb {

class BoundAggregateExpression;
class ClientContext;
class DatabaseInstance;
class DataChunk;
class Expression;
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

  std::atomic_uint64_t calls = 0;
  std::atomic_uint64_t output_tokens = 0;
};

struct Endpoint {
  std::string_view fn;
  std::string_view url;
  std::string_view api_key;
};

class Requester {
 public:
  Requester(duckdb::ClientContext& context, const Endpoint& endpoint);

  Response Send(std::string_view body);

  std::optional<std::string> Accept(Response response) const;

  std::optional<std::string> Post(std::string_view body) {
    return Accept(Send(body));
  }

  void AddOutputTokens(uint64_t tokens);

  void ForEach(size_t n, absl::FunctionRef<void(size_t)> fn);

  bool ThrowOnError() const noexcept { return _throw_on_error; }

 private:
  bool ReserveCall();
  void Sleep(uint64_t ms) const;

  duckdb::ClientContext& _context;
  std::string _fn;
  std::string _url;
  duckdb::HTTPHeaders _headers;
  duckdb::shared_ptr<AIQueryUsage> _usage;
  duckdb::unique_ptr<duckdb::HTTPParams> _params;
  duckdb::unique_ptr<duckdb::HTTPClient> _client;
  uint64_t _max_calls;
  uint64_t _max_output_tokens;
  uint32_t _max_retries;
  uint32_t _retry_delay_ms;
  uint32_t _timeout;
  bool _throw_on_error;
  bool _throw_on_quota;
};

class AIFunctionData : public duckdb::FunctionData {
 public:
  virtual Endpoint GetEndpoint() const = 0;

  virtual size_t BatchSize() const { return 1; }

  virtual void Evaluate(Requester& requester, duckdb::DataChunk& args,
                        duckdb::Vector& result) const = 0;
};

struct AILocalState final : public duckdb::FunctionLocalState {
  AILocalState(duckdb::ClientContext& context, const AIFunctionData& bind);

  Requester requester;
};

duckdb::ScalarFunction MakeAIFunction(std::string_view name,
                                      duckdb::LogicalType type,
                                      duckdb::bind_scalar_function_t bind);

void AddOption(duckdb::FunctionSignature& signature, std::string_view name,
               const duckdb::LogicalType& type);

bool IsAIAggregate(const duckdb::BoundAggregateExpression& aggregate);

duckdb::unique_ptr<duckdb::Expression> MakeAggregateReducer(
  const duckdb::BoundAggregateExpression& aggregate,
  duckdb::unique_ptr<duckdb::Expression> list);

void RegisterTextFunctions(duckdb::ExtensionLoader& loader);

void RegisterJevFunction(duckdb::ExtensionLoader& loader);

void RegisterAggregateFunctions(duckdb::ExtensionLoader& loader);

void RegisterAIOptimizer(duckdb::DatabaseInstance& db);

}  // namespace sdb::connector::ai
