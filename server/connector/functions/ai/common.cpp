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

#include "connector/functions/ai/common.h"

#include <absl/container/flat_hash_map.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <simdjson.h>

#include <algorithm>
#include <chrono>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/extension_helper.hpp>
#include <duckdb/main/secret/secret.hpp>
#include <duckdb/main/secret/secret_manager.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <thread>
#include <utility>

#include "query/config.h"

namespace sdb::connector::ai {
namespace {

constexpr uint64_t kMaxRetryDelayMs = 60'000;
constexpr size_t kMaxErrorBody = 256;

constinit SettingRef gAllowInsecure{"sdb_ai_allow_insecure_endpoint"};
constinit SettingRef gConcurrency{"sdb_ai_max_concurrent_requests"};
constinit SettingRef gMaxCalls{"sdb_ai_max_api_calls_per_query"};
constinit SettingRef gMaxOutputTokens{"sdb_ai_max_output_tokens_per_query"};
constinit SettingRef gMaxRetries{"sdb_ai_max_retries"};
constinit SettingRef gRetryDelay{"sdb_ai_retry_initial_delay_ms"};
constinit SettingRef gTimeout{"sdb_ai_request_timeout"};
constinit SettingRef gThrowOnError{"sdb_ai_throw_on_error"};
constinit SettingRef gThrowOnQuota{"sdb_ai_throw_on_quota_exceeded"};

std::string ReadStringSetting(duckdb::ClientContext& context,
                              std::string_view name) {
  duckdb::Value value;
  if (!context.TryGetCurrentSetting(std::string{name}, value) ||
      value.IsNull()) {
    return {};
  }
  return value.ToString();
}

bool IsSuccess(uint16_t status) { return status >= 200 && status < 300; }

bool IsRetryable(uint16_t status) {
  switch (status) {
    case 0:
    case 408:
    case 429:
    case 500:
    case 502:
    case 503:
    case 504:
    case 529:
      return true;
    default:
      return false;
  }
}

int FatalErrorCode(uint16_t status) {
  switch (status) {
    case 401:
    case 403:
      return ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION;
    case 404:
      return ERRCODE_UNDEFINED_OBJECT;
    case 422:
      return ERRCODE_INVALID_PARAMETER_VALUE;
    default:
      return 0;
  }
}

[[noreturn]] void ThrowInterrupted() {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_QUERY_CANCELED),
                  ERR_MSG("canceling statement due to user request"));
}

std::string_view UrlHost(std::string_view url) {
  if (const auto pos = url.find("://"); pos != std::string_view::npos) {
    url.remove_prefix(pos + 3);
  }
  url = url.substr(0, url.find_first_of("/?#"));
  if (const auto at = url.rfind('@'); at != std::string_view::npos) {
    url.remove_prefix(at + 1);
  }
  if (url.starts_with('[')) {
    return url.substr(0, url.find(']') + 1);
  }
  return url.substr(0, url.find(':'));
}

bool IsLoopbackHost(std::string_view host) {
  return absl::EqualsIgnoreCase(host, "localhost") || host == "::1" ||
         host == "[::1]" ||
         (host.starts_with("127.") &&
          host.find_first_not_of("0123456789.") == std::string_view::npos);
}

bool IsInsecureEndpoint(std::string_view base_url) {
  return !absl::StartsWithIgnoreCase(base_url, "https://") &&
         !IsLoopbackHost(UrlHost(base_url));
}

std::string ReadableText(std::string_view text) {
  std::string out{text.substr(0, kMaxErrorBody)};
  for (auto& c : out) {
    const auto u = static_cast<unsigned char>(c);
    if (u < 0x20 || u == 0x7F) {
      c = ' ';
    }
  }
  return out;
}

std::string ProviderError(std::string_view body) {
  simdjson::dom::parser parser;
  simdjson::dom::element error;
  std::string_view message;
  if (parser.parse(body.data(), body.size())["error"].get(error) ==
      simdjson::SUCCESS) {
    if (error.get_string().get(message) == simdjson::SUCCESS ||
        error["message"].get_string().get(message) == simdjson::SUCCESS) {
      std::string_view type;
      if (error["type"].get_string().get(type) == simdjson::SUCCESS &&
          !type.empty()) {
        return absl::StrCat("[", ReadableText(type), "] ",
                            ReadableText(message));
      }
      return ReadableText(message);
    }
  }
  return ReadableText(body);
}

uint64_t RetryDelayMs(uint32_t initial_ms, uint32_t attempt,
                      std::string_view retry_after) {
  if (uint64_t seconds = 0; absl::SimpleAtoi(retry_after, &seconds)) {
    return std::min(seconds, kMaxRetryDelayMs / 1000) * 1000;
  }
  return std::min(uint64_t{initial_ms} << std::min(attempt, 16U),
                  kMaxRetryDelayMs);
}

class FetchTask final : public duckdb::BaseExecutorTask {
 public:
  FetchTask(duckdb::TaskExecutor& executor, Fetch& fetch)
    : BaseExecutorTask{executor}, _fetch{fetch} {}

  void ExecuteTask() final { _fetch.Run(); }

 private:
  Fetch& _fetch;
};

void AIExecute(duckdb::DataChunk& args, duckdb::ExpressionState& state,
               duckdb::Vector& result) {
  const auto& bind = state.expr.Cast<duckdb::BoundFunctionExpression>()
                       .BindInfo()
                       ->Cast<AIFunctionData>();
  auto work = bind.Start(args);
  RunWork(state.GetContext(),
          duckdb::ExecuteFunctionState::GetFunctionState(state)
            ->Cast<AILocalState>()
            .requester,
          bind.GetEndpoint(), *work);
  work->Finish(result);
}

duckdb::unique_ptr<duckdb::FunctionLocalState> AIInitLocal(
  duckdb::ExpressionState& state, const duckdb::BoundFunctionExpression&,
  duckdb::FunctionData* bind_data) {
  return duckdb::make_uniq<AILocalState>(state.GetContext(),
                                         bind_data->Cast<AIFunctionData>());
}

}  // namespace

void ThrowRowError(std::string message) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION), ERR_MSG(message),
    ERR_HINT("SET sdb_ai_throw_on_error = false to return NULL for rows "
             "that fail."));
}

std::optional<duckdb::Value> FoldArgument(duckdb::ClientContext& context,
                                          duckdb::Expression& expr,
                                          std::string_view fn,
                                          std::string_view arg) {
  if (expr.HasParameter() || !expr.IsFoldable()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG(fn, ": \"", arg, "\" parameter must be a constant value"));
  }
  auto value = duckdb::ExpressionExecutor::EvaluateScalar(context, expr);
  if (value.IsNull()) {
    return std::nullopt;
  }
  return value;
}

std::optional<std::string> FoldString(duckdb::ClientContext& context,
                                      duckdb::Expression& expr,
                                      std::string_view fn,
                                      std::string_view arg) {
  auto value = FoldArgument(context, expr, fn, arg);
  if (!value) {
    return std::nullopt;
  }
  return value->ToString();
}

SecretConfig LoadSecret(duckdb::ClientContext& context, std::string_view fn,
                        const std::optional<std::string>& secret_name,
                        std::string_view default_setting,
                        std::string_view type) {
  const auto name =
    secret_name ? *secret_name : ReadStringSetting(context, default_setting);
  if (name.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(fn, ": no secret given"),
                    ERR_HINT("Pass secret_name := '<name>' or SET ",
                             default_setting, " = '<name>'."));
  }
  auto& secret_manager = duckdb::SecretManager::Get(context);
  auto txn = duckdb::CatalogTransaction::GetSystemCatalogTransaction(context);
  auto entry = secret_manager.GetSecretByName(txn, name);
  if (!entry || !entry->secret) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG(fn, ": secret '", name, "' not found"));
  }
  const auto actual = entry->secret->GetType().GetIdentifierName();
  if (actual != type) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                    ERR_MSG(fn, ": secret '", name, "' has type '", actual,
                            "', expected '", type, "'"));
  }
  const auto& kv =
    irs::utils::downCast<const duckdb::KeyValueSecret>(*entry->secret);
  auto get = [&](const char* key) {
    duckdb::Value v;
    if (kv.TryGetValue(duckdb::Identifier{key}, v) && !v.IsNull()) {
      return v.ToString();
    }
    return std::string{};
  };
  SecretConfig config{
    .api_key = get("api_key"),
    .base_url = get("base_url"),
    .model = get("model"),
    .chat_path = get("chat_path"),
    .embeddings_path = get("embeddings_path"),
    .path = get("path"),
  };
  if (!config.base_url.empty() && IsInsecureEndpoint(config.base_url) &&
      !gAllowInsecure.Bool(context)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG(fn, ": secret '", name, "' uses the insecure endpoint '",
              config.base_url, "'"),
      ERR_HINT("Use an https:// base_url, or SET "
               "sdb_ai_allow_insecure_endpoint = true to send requests over "
               "plain HTTP to hosts other than localhost."));
  }
  duckdb::ExtensionHelper::TryAutoLoadExtension(*context.db, "httpfs");
  return config;
}

std::string JoinUrl(std::string_view base_url, std::string_view default_base,
                    std::string_view path) {
  std::string url{base_url.empty() ? default_base : base_url};
  while (!url.empty() && url.back() == '/') {
    url.pop_back();
  }
  if (url.ends_with(absl::StrCat("/", path.substr(path.rfind('/') + 1)))) {
    return url;
  }
  if (!path.empty() && path.front() != '/') {
    url.push_back('/');
  }
  url.append(path);
  return url;
}

std::string ToJson(std::string_view text) {
  simdjson::builder::string_builder builder(text.size() + 8);
  builder.escape_and_append_with_quotes(text);
  return std::string{builder.view().value()};
}

std::vector<Criterion> ParseCriteria(const duckdb::Value& value,
                                     std::string_view fn,
                                     std::string_view param) {
  auto fail = [&] {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(fn, ": \"", param,
                            "\" must be VARCHAR[] or STRUCT(label VARCHAR, "
                            "description VARCHAR)[]"));
  };
  if (value.type().id() != duckdb::LogicalTypeId::LIST) {
    fail();
  }
  std::vector<Criterion> criteria;
  for (const auto& child : duckdb::ListValue::GetChildren(value)) {
    auto& criterion = criteria.emplace_back();
    if (child.IsNull()) {
      continue;
    }
    if (child.type().id() == duckdb::LogicalTypeId::VARCHAR) {
      criterion.label = child.ToString();
      continue;
    }
    if (child.type().id() != duckdb::LogicalTypeId::STRUCT) {
      fail();
    }
    const auto& fields = duckdb::StructValue::GetChildren(child);
    for (size_t i = 0; i != fields.size(); ++i) {
      const auto name =
        duckdb::StructType::GetChildName(child.type(), i).GetIdentifierName();
      if (absl::EqualsIgnoreCase(name, "label")) {
        criterion.label = fields[i].IsNull() ? "" : fields[i].ToString();
      } else if (absl::EqualsIgnoreCase(name, "description")) {
        if (!fields[i].IsNull()) {
          criterion.description = fields[i].ToString();
        }
      } else {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG(fn, ": unknown criteria field \"", name,
                                "\" (label or description)"));
      }
    }
  }
  return criteria;
}

Inputs CollectInputs(duckdb::Vector& input, duckdb::idx_t count, bool dedup,
                     bool skip_empty) {
  Inputs inputs;
  inputs.slots.assign(count, Inputs::kNone);
  absl::flat_hash_map<std::string_view, size_t> seen;
  auto values = input.Values<duckdb::string_t>();
  for (duckdb::idx_t i = 0; i < count; i++) {
    auto value = values[i];
    if (!value.IsValid()) {
      continue;
    }
    const auto& text = value.GetValue();
    const std::string_view view{text.GetData(), text.GetSize()};
    if (skip_empty && view.empty()) {
      continue;
    }
    if (dedup) {
      const auto [it, inserted] = seen.try_emplace(view, inputs.texts.size());
      if (!inserted) {
        inputs.slots[i] = it->second;
        continue;
      }
    }
    inputs.slots[i] = inputs.texts.size();
    inputs.texts.push_back(view);
  }
  return inputs;
}

void SetOutputs(duckdb::Vector& result, const Inputs& inputs,
                std::span<const duckdb::Value> outputs) {
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  const duckdb::Value null{result.GetType()};
  for (size_t i = 0; i != inputs.slots.size(); ++i) {
    const auto slot = inputs.slots[i];
    result.SetValue(i, slot == Inputs::kNone || outputs[slot].IsNull()
                         ? null
                         : outputs[slot]);
  }
}

void AIQueryUsage::QueryBegin(duckdb::ClientContext&) {
  calls.store(0, std::memory_order_relaxed);
  output_tokens.store(0, std::memory_order_relaxed);
}

Requester::Requester(duckdb::ClientContext& context, const Endpoint& endpoint)
  : _context{context},
    _fn{endpoint.fn},
    _url{endpoint.url},
    _output_tokens{endpoint.output_tokens},
    _usage{context.registered_state->GetOrCreate<AIQueryUsage>("sdb_ai_usage")},
    _max_calls{gMaxCalls.Int(context)},
    _max_output_tokens{gMaxOutputTokens.Int(context)},
    _max_retries{gMaxRetries.Int(context)},
    _retry_delay_ms{gRetryDelay.Int(context)},
    _timeout{gTimeout.Int(context)},
    _throw_on_error{gThrowOnError.Bool(context)},
    _throw_on_quota{gThrowOnQuota.Bool(context)} {
  _headers.Insert("Content-Type", "application/json");
  _headers.Insert("X-SereneDB-AI-Function", _fn);
  if (!endpoint.api_key.empty()) {
    _headers.Insert("Authorization", absl::StrCat("Bearer ", endpoint.api_key));
  }
}

bool Requester::ReserveCall() {
  std::string_view setting;
  uint64_t limit = 0;
  if (_max_output_tokens != 0 &&
      _usage->output_tokens.load(std::memory_order_relaxed) >=
        _max_output_tokens) {
    setting = "sdb_ai_max_output_tokens_per_query";
    limit = _max_output_tokens;
  } else if (const auto calls =
               _usage->calls.fetch_add(1, std::memory_order_relaxed) + 1;
             _max_calls != 0 && calls > _max_calls) {
    setting = "sdb_ai_max_api_calls_per_query";
    limit = _max_calls;
  } else {
    return true;
  }
  if (_throw_on_quota) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
      ERR_MSG(_fn, ": query exceeded ", setting, " (", limit, ")"),
      ERR_HINT("Raise ", setting,
               " or SET sdb_ai_throw_on_quota_exceeded = "
               "false to return NULL for the remaining "
               "rows."));
  }
  return false;
}

void Requester::Sleep(uint64_t ms) const {
  const auto deadline =
    std::chrono::steady_clock::now() + std::chrono::milliseconds{ms};
  for (auto now = std::chrono::steady_clock::now(); now < deadline;
       now = std::chrono::steady_clock::now()) {
    if (_context.IsInterrupted()) {
      ThrowInterrupted();
    }
    std::this_thread::sleep_for(std::min<std::chrono::steady_clock::duration>(
      deadline - now, std::chrono::milliseconds{50}));
  }
}

Response Requester::Send(std::string_view body) {
  auto& http = duckdb::HTTPUtil::Get(*_context.db);
  if (!_params) {
    _params = http.InitializeParameters(_context, _url);
    _params->retries = 0;
    _params->timeout = _timeout;
    _params->timeout_usec = 0;
  }
  Response response;
  for (uint32_t attempt = 0;; ++attempt) {
    if (_context.IsInterrupted()) {
      ThrowInterrupted();
    }
    if (!ReserveCall()) {
      return {.skipped = true};
    }
    duckdb::PostRequestInfo request{
      _url, _headers, *_params,
      reinterpret_cast<duckdb::const_data_ptr_t>(body.data()), body.size()};
    request.try_request = true;
    auto result = http.Request(request, _client);
    std::string retry_after;
    if (!result || result->HasRequestError()) {
      _client.reset();
      response.status = 0;
      response.body =
        result ? result->GetRequestError() : std::string{"no response"};
    } else {
      response.status = static_cast<uint16_t>(result->status);
      response.body = request.buffer_out.empty()
                        ? std::move(result->body)
                        : std::move(request.buffer_out);
      if (result->HasHeader("Retry-After")) {
        retry_after = result->GetHeaderValue("Retry-After");
      }
    }
    if (IsSuccess(response.status)) {
      if (_output_tokens) {
        _usage->output_tokens.fetch_add(_output_tokens(response.body),
                                        std::memory_order_relaxed);
      }
      return response;
    }
    if (!IsRetryable(response.status) || attempt >= _max_retries) {
      return response;
    }
    Sleep(RetryDelayMs(_retry_delay_ms, attempt, retry_after));
  }
}

std::optional<std::string> Requester::Accept(Response response) const {
  if (response.skipped) {
    return std::nullopt;
  }
  if (IsSuccess(response.status)) {
    return std::move(response.body);
  }
  if (response.status == 0) {
    ThrowRowError(absl::StrCat(_fn, ": request to '", _url,
                               "' failed: ", ReadableText(response.body)));
  }
  auto message =
    absl::StrCat(_fn, ": '", _url, "' returned HTTP ", response.status, ": ",
                 ProviderError(response.body));
  if (const auto code = FatalErrorCode(response.status); code != 0) {
    THROW_SQL_ERROR(ERR_CODE(code), ERR_MSG(message));
  }
  ThrowRowError(std::move(message));
}

void Requester::ForEach(size_t n, absl::FunctionRef<void(size_t)> fn) {
  for (size_t i = 0; i != n; ++i) {
    try {
      fn(i);
    } catch (const irs::SqlException& e) {
      if (_throw_on_error ||
          e.error().errcode != ERRCODE_EXTERNAL_ROUTINE_EXCEPTION) {
        throw;
      }
    }
  }
}

void Fetch::Run() {
  std::vector<std::optional<Requester>> requesters(_endpoints.size());
  while (!_stop.load(std::memory_order_relaxed)) {
    const auto k = _next.fetch_add(1, std::memory_order_relaxed);
    if (k >= _requests.size()) {
      return;
    }
    auto& [request, endpoint] = _requests[k];
    auto& requester = requesters[endpoint];
    try {
      if (!requester) {
        requester.emplace(_context, _endpoints[endpoint]);
      }
      request->response = requester->Send(request->body);
    } catch (...) {
      absl::MutexLock lock{&_mutex};
      if (!_error) {
        _error = std::current_exception();
      }
      _stop.store(true, std::memory_order_relaxed);
    }
  }
}

void Fetch::Rethrow() {
  absl::MutexLock lock{&_mutex};
  if (_error) {
    std::rethrow_exception(_error);
  }
}

size_t MaxConcurrentRequests(duckdb::ClientContext& context) {
  return std::max<size_t>(1, gConcurrency.Int(context));
}

void RunWork(duckdb::ClientContext& context, Requester& requester,
             const Endpoint& endpoint, AIWork& work) {
  const auto workers = MaxConcurrentRequests(context);
  while (!work.requests.empty()) {
    Fetch fetch{context, {endpoint}};
    for (auto& request : work.requests) {
      fetch.Add(request, 0);
    }
    duckdb::TaskExecutor executor{context, duckdb::TaskSchedulerType::ASYNC};
    for (size_t i = 0, n = std::min(workers, fetch.Size()); i != n; ++i) {
      executor.ScheduleTask(duckdb::make_uniq<FetchTask>(executor, fetch));
    }
    executor.WorkOnTasks();
    fetch.Rethrow();
    work.Advance(requester);
  }
}

AILocalState::AILocalState(duckdb::ClientContext& context,
                           const AIFunctionData& bind)
  : requester{context, bind.GetEndpoint()} {}

duckdb::ScalarFunction MakeAIFunction(std::string_view name,
                                      duckdb::LogicalType type,
                                      duckdb::bind_scalar_function_t bind) {
  duckdb::ScalarFunction fn{duckdb::Identifier{name},
                            {},
                            std::move(type),
                            AIExecute,
                            bind,
                            nullptr,
                            AIInitLocal};
  fn.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
  fn.SetFallible();
  return fn;
}

void AddOption(duckdb::FunctionSignature& signature, std::string_view name,
               const duckdb::LogicalType& type) {
  signature.AddParameter(duckdb::Identifier{name}, type, duckdb::Value{type});
}

}  // namespace sdb::connector::ai
