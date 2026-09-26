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

#include <absl/base/thread_annotations.h>
#include <absl/functional/function_ref.h>
#include <absl/strings/str_cat.h>
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <cstdint>
#include <duckdb/common/http_util.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/function/function.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context_state.hpp>
#include <exception>
#include <limits>
#include <memory>
#include <optional>
#include <semaphore>
#include <span>
#include <string>
#include <string_view>
#include <utility>
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

struct Api {
  std::string_view secret_type;
  std::string_view default_secret;
  std::string_view base_url;
  std::string_view path_key;
  std::string_view path;
  std::string_view default_model;
  std::string_view usage_key;
};

inline constexpr Api kChatApi{
  .secret_type = kOpenAISecretType,
  .default_secret = "sdb_ai_text_default_secret",
  .base_url = "https://api.openai.com",
  .path_key = "chat_path",
  .path = "/v1/chat/completions",
  .usage_key = "completion_tokens",
};

inline constexpr Api kEmbeddingApi{
  .secret_type = kOpenAISecretType,
  .default_secret = "sdb_ai_embedding_default_secret",
  .base_url = "https://api.openai.com",
  .path_key = "embeddings_path",
  .path = "/v1/embeddings",
};

inline constexpr Api kJevApi{
  .secret_type = kTypeSafeSecretType,
  .default_secret = "sdb_ai_system1_default_secret",
  .base_url = "https://api.typesafe.ai",
  .path_key = "path",
  .path = "/v1/systemone",
  .default_model = "jev-latest",
  .usage_key = "output_tokens",
};

struct Endpoint {
  std::string fn;
  std::string url;
  std::string api_key;
  std::string model;
  std::string_view usage_key;

  bool operator==(const Endpoint&) const = default;
};

std::optional<duckdb::Value> FoldArgument(duckdb::ClientContext& context,
                                          duckdb::Expression& expr,
                                          std::string_view fn,
                                          std::string_view arg);

std::optional<std::string> FoldString(duckdb::ClientContext& context,
                                      duckdb::Expression& expr,
                                      std::string_view fn,
                                      std::string_view arg);

Endpoint LoadEndpoint(duckdb::ClientContext& context, std::string_view fn,
                      const std::optional<std::string>& secret_name,
                      const Api& api);

void RebindEachExecution(duckdb::BindScalarFunctionInput& input);

[[noreturn]] void ThrowRowError(std::string message);

[[noreturn]] void ThrowBadReply(std::string_view fn, std::string_view problem,
                                std::string_view reply);

std::string ToJson(std::string_view text);

template<typename Range>
std::string JsonArray(const Range& values) {
  std::string out = "[";
  std::string_view comma;
  for (const auto& value : values) {
    absl::StrAppend(&out, std::exchange(comma, ","), ToJson(value));
  }
  absl::StrAppend(&out, "]");
  return out;
}

struct Criterion {
  std::string label;
  std::optional<std::string> description;
};

std::vector<Criterion> ParseCriteria(const duckdb::Value& value,
                                     std::string_view fn,
                                     std::string_view param);

std::string CriteriaObject(std::span<const Criterion> criteria);

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

class Requester {
 public:
  Requester(duckdb::ClientContext& context, const Endpoint& endpoint);

  ~Requester();

  Response Send(std::string_view body);

  std::optional<std::string> Accept(Response response) const;

  void ForEach(size_t n, absl::FunctionRef<void(size_t)> fn);

  bool ThrowOnError() const noexcept { return _throw_on_error; }

 private:
  bool ReserveCall();
  void Sleep(uint64_t ms) const;

  duckdb::ClientContext& _context;
  const Endpoint& _endpoint;
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

struct AIRequest {
  std::string body;
  Response response;
};

class AIWork {
 public:
  virtual ~AIWork() = default;

  virtual void Advance(Requester& requester) = 0;

  virtual void Finish(duckdb::Vector& result) = 0;

  std::vector<AIRequest> requests;
};

class BatchWork : public AIWork {
 public:
  void Advance(Requester& requester) final;

 protected:
  void QueueBatches(size_t n, size_t batch_size);

  virtual std::string Body(size_t begin, size_t size) const = 0;

  virtual void Parse(Requester& requester, Response response, size_t begin,
                     size_t size) = 0;

 private:
  void Queue(size_t begin, size_t size);

  std::vector<std::pair<size_t, size_t>> _batches;
};

class AIFunctionData : public duckdb::FunctionData {
 public:
  virtual std::unique_ptr<AIWork> Start(duckdb::DataChunk& args) const = 0;

  Endpoint endpoint;
};

class Fetch {
 public:
  Fetch(duckdb::ClientContext& context, std::vector<const Endpoint*> endpoints,
        std::counting_semaphore<>& permits)
    : _context{context}, _endpoints{std::move(endpoints)}, _permits{permits} {}

  void Add(AIRequest& request, size_t endpoint) {
    _requests.emplace_back(&request, endpoint);
  }

  size_t Size() const noexcept { return _requests.size(); }

  void Run();

  void Rethrow();

 private:
  duckdb::ClientContext& _context;
  std::vector<const Endpoint*> _endpoints;
  std::counting_semaphore<>& _permits;
  std::vector<std::pair<AIRequest*, size_t>> _requests;
  std::atomic_size_t _next = 0;
  std::atomic_bool _stop = false;
  absl::Mutex _mutex;
  std::exception_ptr _error ABSL_GUARDED_BY(_mutex);
};

size_t MaxConcurrentRequests(duckdb::ClientContext& context);

void RunWork(duckdb::ClientContext& context, Requester& requester,
             const Endpoint& endpoint, AIWork& work);

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

void RegisterEmbeddingFunctions(duckdb::ExtensionLoader& loader);

void RegisterTextFunctions(duckdb::ExtensionLoader& loader);

void RegisterJevFunction(duckdb::ExtensionLoader& loader);

void RegisterAggregateFunctions(duckdb::ExtensionLoader& loader);

void RegisterAIOptimizer(duckdb::DatabaseInstance& db);

}  // namespace sdb::connector::ai
