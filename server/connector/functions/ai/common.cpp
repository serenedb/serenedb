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

#include <absl/cleanup/cleanup.h>
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
#include <duckdb/parallel/task.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <thread>
#include <tuple>
#include <utility>

#include "query/config.h"

namespace sdb::connector::ai {
namespace {

constexpr uint64_t kMaxRetryDelayMs = 60'000;
constexpr size_t kMaxErrorBody = 256;

constinit SettingRef gAllowInsecure{"sdb_ai_allow_insecure_endpoint"};
constinit SettingRef gConcurrency{"sdb_ai_max_concurrent_requests"};
constinit SettingRef gEmbeddingBatch{"sdb_ai_embedding_max_batch_size"};
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

bool IsQuotaExhausted(std::string_view body) {
  const simdjson::padded_string padded{body};
  simdjson::ondemand::parser parser;
  auto doc = parser.iterate(padded);
  simdjson::ondemand::object error;
  if (doc["error"].get_object().get(error) != simdjson::SUCCESS) {
    return false;
  }
  auto is_quota = [&](std::string_view key) {
    std::string_view value;
    return error[key].get_string().get(value) == simdjson::SUCCESS &&
           value == "insufficient_quota";
  };
  return is_quota("code") || is_quota("type");
}

int FatalErrorCode(uint16_t status, std::string_view body) {
  switch (status) {
    case 401:
    case 403:
      return ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION;
    case 404:
      return ERRCODE_UNDEFINED_OBJECT;
    case 422:
      return ERRCODE_INVALID_PARAMETER_VALUE;
    case 429:
      return IsQuotaExhausted(body) ? ERRCODE_INSUFFICIENT_RESOURCES : 0;
    default:
      return 0;
  }
}

bool IsBatchRejection(uint16_t status) {
  return status == 400 || status == 413 || status == 422;
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
  return absl::EqualsIgnoreCase(host, "localhost") || host == "[::1]" ||
         (host.starts_with("127.") &&
          host.find_first_not_of("0123456789.") == std::string_view::npos);
}

bool IsInsecureEndpoint(std::string_view base_url) {
  return !absl::StartsWithIgnoreCase(base_url, "https://") &&
         !IsLoopbackHost(UrlHost(base_url));
}

std::string JoinUrl(std::string url, std::string_view path) {
  while (!url.empty() && url.back() == '/') {
    url.pop_back();
  }
  if (url.ends_with(absl::StrCat("/", path.substr(path.rfind('/') + 1)))) {
    return url;
  }
  absl::StrAppend(&url, path.empty() || path.starts_with('/') ? "" : "/", path);
  return url;
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
  const simdjson::padded_string padded{body};
  simdjson::ondemand::parser parser;
  auto doc = parser.iterate(padded);
  simdjson::ondemand::value error;
  if (doc["error"].get(error) != simdjson::SUCCESS) {
    return ReadableText(body);
  }
  std::string_view message;
  if (error.get_string().get(message) == simdjson::SUCCESS) {
    return ReadableText(message);
  }
  simdjson::ondemand::object object;
  if (error.get_object().get(object) != simdjson::SUCCESS ||
      object["message"].get_string().get(message) != simdjson::SUCCESS) {
    return ReadableText(body);
  }
  std::string_view type;
  if (object["type"].get_string().get(type) == simdjson::SUCCESS &&
      !type.empty()) {
    return absl::StrCat("[", ReadableText(type), "] ", ReadableText(message));
  }
  return ReadableText(message);
}

[[noreturn]] void ThrowReplyError(const Endpoint& endpoint,
                                  const Response& response) {
  if (response.status == 0) {
    ThrowRowError(absl::StrCat(endpoint.fn, ": request to '", endpoint.url,
                               "' failed: ", ReadableText(response.body)));
  }
  auto message =
    absl::StrCat(endpoint.fn, ": '", endpoint.url, "' returned HTTP ",
                 response.status, ": ", ProviderError(response.body));
  if (const auto code = FatalErrorCode(response.status, response.body);
      code != 0) {
    THROW_SQL_ERROR(ERR_CODE(code), ERR_MSG(message));
  }
  ThrowRowError(std::move(message));
}

uint64_t RetryDelayMs(uint32_t initial_ms, uint32_t attempt,
                      std::string_view retry_after) {
  if (uint64_t seconds = 0; absl::SimpleAtoi(retry_after, &seconds)) {
    return std::min(seconds, kMaxRetryDelayMs / 1000) * 1000;
  }
  return std::min(uint64_t{initial_ms} << std::min(attempt, 16U),
                  kMaxRetryDelayMs);
}

Endpoint ResolveEndpoint(duckdb::ClientContext& context,
                         const EndpointRef& ref) {
  const auto& api = *ref.api;
  const std::string_view fn = ref.fn;
  const auto name = ref.secret_name
                      ? *ref.secret_name
                      : ReadStringSetting(context, api.default_secret);
  if (name.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(fn, ": no secret given"),
                    ERR_HINT("Pass secret_name := '<name>' or SET ",
                             api.default_secret, " = '<name>'."));
  }
  auto& secret_manager = duckdb::SecretManager::Get(context);
  auto txn = duckdb::CatalogTransaction::GetSystemCatalogTransaction(context);
  auto entry = secret_manager.GetSecretByName(txn, name);
  if (!entry || !entry->secret) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG(fn, ": secret '", name, "' not found"));
  }
  const auto actual = entry->secret->GetType().GetIdentifierName();
  if (actual != api.secret_type) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                    ERR_MSG(fn, ": secret '", name, "' has type '", actual,
                            "', expected '", api.secret_type, "'"));
  }
  const auto& kv =
    irs::utils::downCast<const duckdb::KeyValueSecret>(*entry->secret);
  auto get = [&](std::string_view key, std::string_view fallback) {
    duckdb::Value v;
    if (kv.TryGetValue(duckdb::Identifier{key}, v) && !v.IsNull()) {
      if (auto value = v.ToString(); !value.empty()) {
        return value;
      }
    }
    return std::string{fallback};
  };
  auto base_url = get("base_url", api.base_url);
  if (IsInsecureEndpoint(base_url) && !gAllowInsecure.Bool(context)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG(fn, ": secret '", name, "' uses the insecure endpoint '",
              base_url, "'"),
      ERR_HINT("Use an https:// base_url, or SET "
               "sdb_ai_allow_insecure_endpoint = true to send requests over "
               "plain HTTP to hosts other than localhost."));
  }
  Endpoint endpoint{
    .fn = ref.fn,
    .url = JoinUrl(std::move(base_url), get(api.path_key, api.path)),
    .api_key = get("api_key", ""),
    .model = ref.model ? *ref.model : get("model", api.default_model),
    .usage_key = api.usage_key,
  };
  if (endpoint.model.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(fn, ": no model given"),
                    ERR_HINT("Pass model := '<name>' or set the secret's "
                             "model option."));
  }
  return endpoint;
}

class Sender {
 public:
  explicit Sender(const AIExecution& exec) : _exec{exec} {}

  ~Sender() {
    if (_client) {
      duckdb::HTTPUtil::Get(*_exec.context.db).CloseClient(std::move(_client));
    }
  }

  Response Send(
    std::string_view body,
    absl::FunctionRef<void(simdjson::ondemand::object&, std::string_view)>
      decode) {
    auto& http = duckdb::HTTPUtil::Get(*_exec.context.db);
    const auto& target = _exec.target;
    const auto& endpoint = target.endpoint;
    const auto& settings = _exec.query.settings;
    Response response;
    for (uint32_t attempt = 0;; ++attempt) {
      _exec.context.InterruptCheck();
      if (!_exec.query.ReserveCall(endpoint.fn)) {
        return {.skipped = true};
      }
      duckdb::PostRequestInfo request{
        endpoint.url, target.headers, *target.params,
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
        Decode(response, decode);
        return response;
      }
      if (!IsRetryable(response.status) ||
          FatalErrorCode(response.status, response.body) != 0 ||
          attempt >= settings.max_retries) {
        return response;
      }
      Sleep(RetryDelayMs(settings.retry_delay_ms, attempt, retry_after));
    }
  }

 private:
  void Decode(
    Response& response,
    absl::FunctionRef<void(simdjson::ondemand::object&, std::string_view)>
      decode) {
    const auto& endpoint = _exec.target.endpoint;
    try {
      const auto size = response.body.size();
      response.body.append(simdjson::SIMDJSON_PADDING, '\0');
      const std::string_view raw{response.body.data(), size};
      simdjson::ondemand::document doc;
      simdjson::ondemand::object reply;
      if (_parser
              .iterate(simdjson::padded_string_view{raw.data(), size,
                                                    response.body.size()})
              .get(doc) != simdjson::SUCCESS ||
          doc.get_object().get(reply) != simdjson::SUCCESS) {
        ThrowBadReply(endpoint.fn, "response is not valid JSON", raw);
      }
      if (!endpoint.usage_key.empty()) {
        uint64_t tokens = 0;
        std::ignore = reply["usage"][endpoint.usage_key].get(tokens);
        _exec.query.AddOutputTokens(tokens);
      }
      decode(reply, raw);
    } catch (...) {
      response.error = std::current_exception();
    }
    response.body = {};
  }

  void Sleep(uint64_t ms) const {
    const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds{ms};
    for (auto now = std::chrono::steady_clock::now(); now < deadline;
         now = std::chrono::steady_clock::now()) {
      _exec.context.InterruptCheck();
      std::this_thread::sleep_for(std::min<std::chrono::steady_clock::duration>(
        deadline - now, std::chrono::milliseconds{50}));
    }
  }

  const AIExecution& _exec;
  duckdb::unique_ptr<duckdb::HTTPClient> _client;
  simdjson::ondemand::parser _parser;
};

struct FetchState {
  FetchState(const AIExecution& exec, AIWork& work, std::span<Response> out)
    : exec{exec},
      work{work},
      out{out},
      scheduler{duckdb::TaskScheduler::GetScheduler(exec.context)},
      token{scheduler.CreateProducer()} {}

  bool Stop() const {
    return stop.load(std::memory_order_relaxed) || exec.context.IsInterrupted();
  }

  bool Pending() const {
    return next.load(std::memory_order_relaxed) < out.size();
  }

  void SendOne(Sender& sender) noexcept {
    const auto k = next.fetch_add(1, std::memory_order_relaxed);
    if (k >= out.size()) {
      return;
    }
    auto& response = out[k];
    const auto& settings = exec.query.settings;
    try {
      response =
        sender.Send(work.Body(k),
                    [&](simdjson::ondemand::object& reply,
                        std::string_view raw) { work.Decode(k, reply, raw); });
      if (response.error) {
        if (settings.throw_on_error) {
          std::rethrow_exception(response.error);
        }
      } else if (!response.skipped && !IsSuccess(response.status) &&
                 !IsBatchRejection(response.status) &&
                 (settings.throw_on_error ||
                  FatalErrorCode(response.status, response.body) != 0)) {
        ThrowReplyError(exec.target.endpoint, response);
      }
    } catch (...) {
      Fail(std::current_exception());
    }
  }

  void Fail(std::exception_ptr error) {
    absl::MutexLock lock{&mutex};
    if (!this->error) {
      this->error = std::move(error);
    }
    stop.store(true, std::memory_order_relaxed);
  }

  void Finish() {
    exec.query.helpers.fetch_sub(1, std::memory_order_relaxed);
    absl::MutexLock lock{&mutex};
    --helpers;
  }

  void Join() {
    for (;;) {
      duckdb::shared_ptr<duckdb::Task> task;
      if (scheduler.GetTaskFromProducer(*token, task)) {
        task->Execute(duckdb::TaskExecutionMode::PROCESS_ALL);
        continue;
      }
      absl::MutexLock lock{&mutex};
      if (helpers == 0) {
        return;
      }
      mutex.AwaitWithTimeout(absl::Condition(
                               +[](size_t* n) { return *n == 0; }, &helpers),
                             absl::Milliseconds(5));
    }
  }

  void Rethrow() {
    std::exception_ptr first;
    {
      absl::MutexLock lock{&mutex};
      first = error;
    }
    if (first) {
      std::rethrow_exception(first);
    }
  }

  const AIExecution& exec;
  AIWork& work;
  std::span<Response> out;
  duckdb::TaskScheduler& scheduler;
  duckdb::unique_ptr<duckdb::ProducerToken> token;
  std::atomic_size_t next = 0;
  std::atomic_bool stop = false;
  absl::Mutex mutex;
  size_t helpers ABSL_GUARDED_BY(mutex) = 0;
  std::exception_ptr error ABSL_GUARDED_BY(mutex);
};

class FetchTask final : public duckdb::Task {
 public:
  FetchTask(duckdb::shared_ptr<FetchState> state,
            std::unique_ptr<Sender> sender)
    : _state{std::move(state)}, _sender{std::move(sender)} {}

  static void Schedule(const duckdb::shared_ptr<FetchState>& state,
                       std::unique_ptr<Sender> sender) {
    state->scheduler.ScheduleTask(
      *state->token,
      duckdb::make_shared_ptr<FetchTask>(state, std::move(sender)),
      duckdb::TaskSchedulerType::ASYNC);
  }

  duckdb::TaskExecutionResult Execute(
    duckdb::TaskExecutionMode) noexcept final {
    auto& state = *_state;
    auto& limiter = state.exec.query.limiter;
    try {
      if (!state.Stop() && state.Pending() &&
          limiter.AcquireFor(absl::ZeroDuration())) {
        state.SendOne(*_sender);
        limiter.Release();
        if (!state.Stop() && state.Pending()) {
          Schedule(_state, std::move(_sender));
          return duckdb::TaskExecutionResult::TASK_FINISHED;
        }
      }
    } catch (...) {
      state.Fail(std::current_exception());
    }
    _sender.reset();
    state.Finish();
    return duckdb::TaskExecutionResult::TASK_FINISHED;
  }

  std::string TaskType() const final { return "AIFetchTask"; }

 private:
  duckdb::shared_ptr<FetchState> _state;
  std::unique_ptr<Sender> _sender;
};

void TopUp(const duckdb::shared_ptr<FetchState>& state) {
  auto& s = *state;
  auto& query = s.exec.query;
  const auto remaining =
    s.out.size() -
    std::min(s.next.load(std::memory_order_relaxed), s.out.size());
  const auto cap = std::min<size_t>(query.limiter.Available(),
                                    s.scheduler.NumberOfAsyncThreads());
  absl::MutexLock lock{&s.mutex};
  while (s.helpers + 1 < remaining) {
    absl::Cleanup release = [&] {
      query.helpers.fetch_sub(1, std::memory_order_relaxed);
    };
    if (query.helpers.fetch_add(1, std::memory_order_relaxed) >= cap) {
      break;
    }
    FetchTask::Schedule(state, std::make_unique<Sender>(s.exec));
    std::move(release).Cancel();
    ++s.helpers;
  }
}

void Fetch(const AIExecution& exec, AIWork& work, std::span<Response> out) {
  auto state = duckdb::make_shared_ptr<FetchState>(exec, work, out);
  auto& limiter = exec.query.limiter;
  {
    absl::Cleanup join = [&] {
      state->stop.store(true, std::memory_order_relaxed);
      state->Join();
    };
    Sender sender{exec};
    while (!state->Stop() && state->Pending()) {
      TopUp(state);
      if (limiter.AcquireFor(absl::Milliseconds(50))) {
        state->SendOne(sender);
        limiter.Release();
      }
    }
  }
  state->Rethrow();
  exec.context.InterruptCheck();
}

AIExecution Execution(duckdb::ClientContext& context, const EndpointRef& ref) {
  auto& query = AIQuery::Get(context);
  return {
    .context = context,
    .query = query,
    .target = query.Resolve(context, ref),
  };
}

void AIExecute(duckdb::DataChunk& args, duckdb::ExpressionState& state,
               duckdb::Vector& result) {
  const auto& bind = state.expr.Cast<duckdb::BoundFunctionExpression>()
                       .BindInfo()
                       ->Cast<AIFunctionData>();
  const auto& exec = duckdb::ExecuteFunctionState::GetFunctionState(state)
                       ->Cast<AILocalState>()
                       .exec;
  auto work = bind.Start(exec, args);
  RunWork(exec, *work);
  work->Finish(result);
}

duckdb::unique_ptr<duckdb::FunctionLocalState> AIInitLocal(
  duckdb::ExpressionState& state, const duckdb::BoundFunctionExpression&,
  duckdb::FunctionData* bind_data) {
  return duckdb::make_uniq<AILocalState>(
    state.GetContext(), bind_data->Cast<AIFunctionData>().endpoint);
}

}  // namespace

void ThrowRowError(std::string message) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION), ERR_MSG(message),
    ERR_HINT("SET sdb_ai_throw_on_error = false to return NULL for rows "
             "that fail."));
}

void ThrowBadReply(std::string_view fn, std::string_view problem,
                   std::string_view reply) {
  ThrowRowError(absl::StrCat(fn, ": ", problem, ": ", ReadableText(reply)));
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

void LoadEndpoint(duckdb::ClientContext& context, const EndpointRef& ref) {
  ResolveEndpoint(context, ref);
  duckdb::ExtensionHelper::TryAutoLoadExtension(*context.db, "httpfs");
}

std::string ToJson(std::string_view text) {
  simdjson::builder::string_builder builder(text.size() + 8);
  builder.escape_and_append_with_quotes(text);
  return std::string{builder.view().value()};
}

std::string MinifyJson(std::string_view json) {
  std::string out(json.size(), '\0');
  size_t size = 0;
  if (simdjson::minify(json.data(), json.size(), out.data(), size) !=
      simdjson::SUCCESS) {
    return std::string{json};
  }
  out.resize(size);
  return out;
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

std::string CriteriaObject(std::span<const Criterion> criteria) {
  std::string out = "{";
  std::string_view comma;
  for (const auto& criterion : criteria) {
    absl::StrAppend(
      &out, std::exchange(comma, ","), ToJson(criterion.label), ":",
      criterion.description ? ToJson(*criterion.description) : "null");
  }
  absl::StrAppend(&out, "}");
  return out;
}

Inputs CollectInputs(duckdb::Vector& input, duckdb::idx_t count, bool dedup,
                     bool skip_empty) {
  Inputs inputs;
  inputs.slots.assign(count, Inputs::kNone);
  irs::containers::FlatHashMap<std::string_view, size_t> seen;
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

void Limiter::Reset(size_t permits) {
  absl::MutexLock lock{&_mutex};
  _available = permits;
}

bool Limiter::AcquireFor(absl::Duration timeout) {
  absl::MutexLock lock{&_mutex};
  if (!_mutex.AwaitWithTimeout(
        absl::Condition(
          +[](size_t* n) { return *n != 0; }, &_available),
        timeout)) {
    return false;
  }
  --_available;
  return true;
}

void Limiter::Release() {
  absl::MutexLock lock{&_mutex};
  ++_available;
}

size_t Limiter::Available() {
  absl::MutexLock lock{&_mutex};
  return _available;
}

AIQuery& AIQuery::Get(duckdb::ClientContext& context) {
  auto& query = *context.registered_state->GetOrCreate<AIQuery>("sdb_ai_query");
  if (!query._active.load(std::memory_order_acquire)) {
    absl::MutexLock lock{&query._mutex};
    if (!query._active.load(std::memory_order_relaxed)) {
      query.Begin(context);
    }
  }
  return query;
}

void AIQuery::Begin(duckdb::ClientContext& context) {
  settings = {
    .max_calls = gMaxCalls.Int(context),
    .max_output_tokens = gMaxOutputTokens.Int(context),
    .max_retries = gMaxRetries.Int(context),
    .retry_delay_ms = gRetryDelay.Int(context),
    .timeout = gTimeout.Int(context),
    .concurrency = gConcurrency.Int(context),
    .embedding_batch = gEmbeddingBatch.Int(context),
    .throw_on_error = gThrowOnError.Bool(context),
    .throw_on_quota = gThrowOnQuota.Bool(context),
  };
  _calls.store(0, std::memory_order_relaxed);
  _output_tokens.store(0, std::memory_order_relaxed);
  limiter.Reset(settings.concurrency);
  _targets.clear();
  _active.store(true, std::memory_order_release);
}

void AIQuery::QueryBegin(duckdb::ClientContext& context) {
  absl::MutexLock lock{&_mutex};
  Begin(context);
}

void AIQuery::QueryEnd(duckdb::ClientContext&) {
  _active.store(false, std::memory_order_release);
}

const AIQuery::Target& AIQuery::Resolve(duckdb::ClientContext& context,
                                        const EndpointRef& ref) {
  absl::MutexLock lock{&_mutex};
  for (const auto& [key, target] : _targets) {
    if (key == ref) {
      return *target;
    }
  }
  auto target = std::make_unique<Target>();
  target->endpoint = ResolveEndpoint(context, ref);
  target->params = duckdb::HTTPUtil::Get(*context.db)
                     .InitializeParameters(context, target->endpoint.url);
  target->params->retries = 0;
  target->params->timeout = settings.timeout;
  target->headers.Insert("Content-Type", "application/json");
  target->headers.Insert("X-SereneDB-AI-Function", target->endpoint.fn);
  if (!target->endpoint.api_key.empty()) {
    target->headers.Insert("Authorization",
                           absl::StrCat("Bearer ", target->endpoint.api_key));
  }
  return *_targets.emplace_back(ref, std::move(target)).second;
}

bool AIQuery::ReserveCall(std::string_view fn) {
  std::string_view setting;
  uint64_t limit = 0;
  if (settings.max_output_tokens != 0 &&
      _output_tokens.load(std::memory_order_relaxed) >=
        settings.max_output_tokens) {
    setting = "sdb_ai_max_output_tokens_per_query";
    limit = settings.max_output_tokens;
  } else if (const auto calls =
               _calls.fetch_add(1, std::memory_order_relaxed) + 1;
             settings.max_calls != 0 && calls > settings.max_calls) {
    setting = "sdb_ai_max_api_calls_per_query";
    limit = settings.max_calls;
  } else {
    return true;
  }
  if (settings.throw_on_quota) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                    ERR_MSG(fn, ": query exceeded ", setting, " (", limit, ")"),
                    ERR_HINT("Raise ", setting,
                             " or SET sdb_ai_throw_on_quota_exceeded = "
                             "false to return NULL for the remaining "
                             "rows."));
  }
  return false;
}

void Replies::ForEach(absl::FunctionRef<void(size_t)> fn) const {
  for (size_t k = 0; k != _responses.size(); ++k) {
    try {
      fn(k);
    } catch (const irs::SqlException& e) {
      if (ThrowOnError() ||
          e.error().errcode != ERRCODE_EXTERNAL_ROUTINE_EXCEPTION) {
        throw;
      }
    }
  }
}

bool Replies::Ok(size_t k) const {
  const auto& response = _responses[k];
  if (response.skipped) {
    return false;
  }
  if (response.error) {
    std::rethrow_exception(response.error);
  }
  if (IsSuccess(response.status)) {
    return true;
  }
  ThrowReplyError(_exec.target.endpoint, response);
}

std::string BatchWork::Body(size_t k) const {
  const auto& batch = _batches[k];
  return batch.probe ? ProbeBody() : BatchBody(batch.begin, batch.size);
}

void BatchWork::Decode(size_t k, simdjson::ondemand::object& reply,
                       std::string_view raw) {
  if (const auto& batch = _batches[k]; !batch.probe) {
    DecodeBatch(batch.begin, batch.size, reply, raw);
  }
}

void BatchWork::QueueBatches(size_t n, size_t batch_size) {
  for (size_t begin = 0; begin < n; begin += batch_size) {
    _batches.push_back({
      .begin = begin,
      .size = std::min(batch_size, n - begin),
    });
  }
}

void BatchWork::Advance(const Replies& replies) {
  const auto batches = std::exchange(_batches, {});
  for (size_t k = 0; k != batches.size(); ++k) {
    if (batches[k].probe) {
      _verdict =
        replies.Status(k) == _rejected ? Verdict::Request : Verdict::Content;
    }
  }
  replies.ForEach([&](size_t k) {
    const auto& batch = batches[k];
    if (batch.probe) {
      return;
    }
    const auto status = replies.Status(k);
    const bool uniform = _verdict == Verdict::Request && status == _rejected;
    if (batch.size > 1 && IsBatchRejection(status) && !uniform) {
      if (_verdict == Verdict::Unknown && status != 413) {
        _verdict = Verdict::Probing;
        _rejected = status;
        _batches.push_back({.probe = true});
      }
      const auto half = batch.size / 2;
      _batches.push_back({
        .begin = batch.begin,
        .size = half,
      });
      _batches.push_back({
        .begin = batch.begin + half,
        .size = batch.size - half,
      });
      return;
    }
    replies.Ok(k);
  });
}

void RunWork(const AIExecution& exec, AIWork& work) {
  for (auto n = work.Size(); n != 0; n = work.Size()) {
    std::vector<Response> responses(n);
    Fetch(exec, work, responses);
    work.Advance(Replies{exec, responses});
  }
}

AILocalState::AILocalState(duckdb::ClientContext& context,
                           const EndpointRef& ref)
  : exec{Execution(context, ref)} {}

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
