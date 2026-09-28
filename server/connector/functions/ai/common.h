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
#include <absl/time/time.h>

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
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace duckdb {

class ClientContext;
class DatabaseInstance;
class DataChunk;
class Expression;
class ExtensionLoader;

}  // namespace duckdb
namespace simdjson::dom {

class element;

}  // namespace simdjson::dom
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
};

struct EndpointRef {
  std::string fn;
  const Api* api = nullptr;
  std::optional<std::string> secret_name;
  std::optional<std::string> model;

  bool operator==(const EndpointRef&) const = default;
};

std::optional<duckdb::Value> FoldArgument(duckdb::ClientContext& context,
                                          duckdb::Expression& expr,
                                          std::string_view fn,
                                          std::string_view arg);

std::optional<std::string> FoldString(duckdb::ClientContext& context,
                                      duckdb::Expression& expr,
                                      std::string_view fn,
                                      std::string_view arg);

void LoadEndpoint(duckdb::ClientContext& context, const EndpointRef& ref);

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

struct Settings {
  uint64_t max_calls = 0;
  uint64_t max_output_tokens = 0;
  uint32_t max_retries = 0;
  uint32_t retry_delay_ms = 0;
  uint32_t timeout = 0;
  uint32_t concurrency = 1;
  uint32_t embedding_batch = 1;
  bool throw_on_error = true;
  bool throw_on_quota = true;
};

class Limiter {
 public:
  void Reset(size_t permits);

  bool TryAcquire();

  bool AcquireFor(absl::Duration timeout);

  void Release();

  size_t Available();

 private:
  absl::Mutex _mutex;
  size_t _available ABSL_GUARDED_BY(_mutex) = 0;
};

class AIQuery final : public duckdb::ClientContextState {
 public:
  struct Target {
    Endpoint endpoint;
    duckdb::unique_ptr<duckdb::HTTPParams> params;
    duckdb::HTTPHeaders headers;
  };

  explicit AIQuery(duckdb::ClientContext& context);

  static AIQuery& Get(duckdb::ClientContext& context);

  void QueryBegin(duckdb::ClientContext& context) final;

  using duckdb::ClientContextState::QueryEnd;

  void QueryEnd(duckdb::ClientContext& context) final;

  const Target& Resolve(duckdb::ClientContext& context, const EndpointRef& ref);

  bool ReserveCall(std::string_view fn);

  void AddOutputTokens(uint64_t tokens) {
    _output_tokens.fetch_add(tokens, std::memory_order_relaxed);
  }

  void ForEach(size_t n, absl::FunctionRef<void(size_t)> fn) const;

  Settings settings;
  Limiter limiter;

 private:
  void Begin(duckdb::ClientContext& context);

  std::atomic_uint64_t _calls = 0;
  std::atomic_uint64_t _output_tokens = 0;
  std::atomic_bool _active = false;
  absl::Mutex _mutex;
  std::vector<std::pair<EndpointRef, std::unique_ptr<Target>>> _targets
    ABSL_GUARDED_BY(_mutex);
};

struct AIExecution {
  duckdb::ClientContext& context;
  AIQuery& query;
  const AIQuery::Target& target;

  bool Stopped() const;
};

struct Response {
  uint16_t status = 0;
  bool skipped = false;
  std::string body;
  std::exception_ptr error;
};

class Replies {
 public:
  Replies(const AIExecution& exec, std::span<const Response> responses)
    : _exec{exec}, _responses{responses} {}

  uint16_t Status(size_t k) const { return _responses[k].status; }

  bool Ok(size_t k) const;

  void ForEach(absl::FunctionRef<void(size_t)> fn) const {
    _exec.query.ForEach(_responses.size(), fn);
  }

  bool ThrowOnError() const noexcept {
    return _exec.query.settings.throw_on_error;
  }

 private:
  const AIExecution& _exec;
  std::span<const Response> _responses;
};

class AIWork {
 public:
  virtual ~AIWork() = default;

  virtual size_t Size() const = 0;

  virtual std::string Body(size_t k) const = 0;

  virtual void Decode(size_t k, simdjson::dom::element reply,
                      std::string_view raw) = 0;

  virtual void Advance(const Replies& replies) = 0;
};

class ScalarWork : public AIWork {
 public:
  virtual void Finish(duckdb::Vector& result) = 0;
};

class BatchWork : public ScalarWork {
 public:
  size_t Size() const final { return _batches.size(); }

  std::string Body(size_t k) const final;

  void Decode(size_t k, simdjson::dom::element reply,
              std::string_view raw) final;

  void Advance(const Replies& replies) final;

 protected:
  void QueueBatches(size_t n, size_t batch_size);

  virtual std::string BatchBody(size_t begin, size_t size) const = 0;

  virtual std::string ProbeBody() const = 0;

  virtual void DecodeBatch(size_t begin, size_t size,
                           simdjson::dom::element reply,
                           std::string_view raw) = 0;

 private:
  enum class Verdict : uint8_t {
    Unknown,
    Probing,
    Content,
    Request,
  };

  struct Batch {
    size_t begin = 0;
    size_t size = 0;
    bool probe = false;
  };

  void Queue(size_t begin, size_t size);

  std::vector<Batch> _batches;
  Verdict _verdict = Verdict::Unknown;
  uint16_t _rejected = 0;
};

void RunWork(const AIExecution& exec, AIWork& work);

class AIFunctionData : public duckdb::FunctionData {
 public:
  virtual std::unique_ptr<ScalarWork> Start(const AIExecution& exec,
                                            duckdb::DataChunk& args) const = 0;

  EndpointRef endpoint;
};

struct AILocalState final : public duckdb::FunctionLocalState {
  AILocalState(duckdb::ClientContext& context, const EndpointRef& ref);

  AIExecution exec;
};

duckdb::ScalarFunction MakeAIFunction(std::string_view name,
                                      duckdb::LogicalType type,
                                      duckdb::bind_scalar_function_t bind);

void AddOption(duckdb::FunctionSignature& signature, std::string_view name,
               const duckdb::LogicalType& type);

void RegisterEmbeddingFunctions(duckdb::ExtensionLoader& loader);

void RegisterTextFunctions(duckdb::ExtensionLoader& loader);

void RegisterJevFunction(duckdb::ExtensionLoader& loader);

void RegisterAggregateFunctions(duckdb::ExtensionLoader& loader);

void RegisterAIOptimizer(duckdb::DatabaseInstance& db);

}  // namespace sdb::connector::ai
