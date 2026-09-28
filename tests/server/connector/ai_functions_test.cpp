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

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <simdjson.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <duckdb.hpp>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>
#include <yaclib/coro/task.hpp>
#include <yaclib/lazy/make.hpp>

#include "../http_server_harness.h"
#include "connector/functions/ai/ai.h"
#include "gtest/gtest.h"
#include "network/http/common.h"
#include "network/http/handler.h"
#include "query/config.h"

using namespace sdb;

namespace {

constexpr std::string_view kChat = "/v1/chat/completions";
constexpr std::string_view kEmbeddings = "/v1/embeddings";
constexpr std::string_view kSystemOne = "/v1/systemone";

struct Reply {
  int status = 200;
  std::string body;
  std::string headers;
};

class MockHandler final : public network::HttpHandler {
 public:
  using Script = std::function<Reply(std::string_view)>;

  explicit MockHandler(Script script) : _script{std::move(script)} {}

  yaclib::Task<> Handle(network::RequestContext&,
                        const network::HttpRequest& request,
                        network::http::HttpResponseWriter& writer) override {
    auto body = network::http::FlattenBody(request.body);
    {
      std::lock_guard lock{_mutex};
      _bodies.push_back(body);
    }
    const auto reply = _script(body);
    writer.Fixed(static_cast<network::http::HttpStatus>(reply.status),
                 network::http::kJsonContentType, reply.body, reply.headers);
    return yaclib::MakeTask();
  }

  std::vector<std::string> Bodies() {
    std::lock_guard lock{_mutex};
    return _bodies;
  }

 private:
  std::mutex _mutex;
  Script _script;
  std::vector<std::string> _bodies;
};

std::string ChatReply(std::string_view content, std::string_view finish,
                      int tokens) {
  return absl::StrCat(
    R"({"choices":[{"message":{"role":"assistant","content":")", content,
    R"("},"finish_reason":")", finish,
    R"("}],"usage":{"prompt_tokens":1,"completion_tokens":)", tokens, "}}");
}

simdjson::dom::element Parse(simdjson::dom::parser& parser,
                             std::string_view json) {
  simdjson::dom::element element;
  EXPECT_EQ(parser.parse(json.data(), json.size()).get(element),
            simdjson::SUCCESS)
    << json;
  return element;
}

std::string Content(std::string_view body, size_t message) {
  simdjson::dom::parser parser;
  std::string_view content;
  EXPECT_EQ(parser.parse(body.data(), body.size())["messages"]
              .at(message)["content"]
              .get(content),
            simdjson::SUCCESS)
    << body;
  return std::string{content};
}

bool IsPartial(std::string_view body) {
  return Content(body, 0).starts_with("You condense");
}

struct InFlight {
  int Enter() {
    const auto current = ++now;
    for (auto seen = peak.load(); current > seen;) {
      if (peak.compare_exchange_weak(seen, current)) {
        break;
      }
    }
    return current;
  }

  void Leave() { --now; }

  std::atomic_int now = 0;
  std::atomic_int peak = 0;
};

template<typename Fn>
std::chrono::steady_clock::duration Timed(Fn&& fn) {
  const auto start = std::chrono::steady_clock::now();
  fn();
  return std::chrono::steady_clock::now() - start;
}

class AIFunctionsTest : public ::testing::Test {
 protected:
  AIFunctionsTest() : _db{nullptr}, _conn{_db} {
    connector::RegisterConfigVariables(
      duckdb::DBConfig::GetConfig(*_db.instance));
    connector::RegisterAIFunctions(*_db.instance);
  }

  MockHandler& Mock(std::string_view path, MockHandler::Script script) {
    auto handler = std::make_unique<MockHandler>(std::move(script));
    auto& mock = *handler;
    _router.Add(network::HttpMethod::Post, path, std::move(handler));
    return mock;
  }

  void Start() {
    _harness = std::make_unique<test::HttpServerHarness>(_router);
    _url = absl::StrCat("http://127.0.0.1:", _harness->server.port());
    Run(absl::StrCat("CREATE SECRET chat (TYPE openai, base_url '", _url,
                     "', model 'm')"));
    Run(
      absl::StrCat("CREATE SECRET jev (TYPE typesafe, base_url '", _url, "')"));
    Run("SET sdb_ai_retry_initial_delay_ms = 1");
  }

  duckdb::unique_ptr<duckdb::MaterializedQueryResult> Run(
    const std::string& sql) {
    auto result = _conn.Query(sql);
    EXPECT_FALSE(result->HasError()) << sql << ": " << result->GetError();
    return result;
  }

  void ExpectError(const std::string& sql, std::string_view needle) {
    auto result = _conn.Query(sql);
    ASSERT_TRUE(result->HasError()) << sql;
    EXPECT_NE(result->GetError().find(needle), std::string::npos)
      << result->GetError();
  }

  network::HttpRouter _router;
  std::unique_ptr<test::HttpServerHarness> _harness;
  std::string _url;
  duckdb::DuckDB _db;
  duckdb::Connection _conn;
};

TEST_F(AIFunctionsTest, RetriesHonorRetryAfter) {
  std::atomic_int calls = 0;
  auto& mock = Mock(kChat, [&](std::string_view) -> Reply {
    switch (calls++) {
      case 0:
        return {429, R"({"error":"slow down"})", "Retry-After: 0\r\n"};
      case 1:
        return {529, R"({"error":"overloaded"})", "Retry-After: 0\r\n"};
      default:
        return {200, ChatReply("done", "stop", 1)};
    }
  });
  Start();
  Run("SET sdb_ai_retry_initial_delay_ms = 600000");
  const auto start = std::chrono::steady_clock::now();
  auto result = Run("SELECT ai_generate('hi', secret_name := 'chat')");
  EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds{60});
  EXPECT_EQ(result->GetValue(0, 0).ToString(), "done");
  EXPECT_EQ(mock.Bodies().size(), 3);
}

TEST_F(AIFunctionsTest, FatalStatusesIgnoreThrowOnError) {
  std::atomic_int status = 401;
  Mock(kChat, [&](std::string_view) {
    return Reply{status.load(), R"({"error":"nope"})"};
  });
  Start();
  Run("SET sdb_ai_throw_on_error = false");
  ExpectError("SELECT ai_generate('hi', secret_name := 'chat')",
              "returned HTTP 401");
  status = 422;
  ExpectError("SELECT ai_generate('hi', secret_name := 'chat')",
              "returned HTTP 422");
  status = 400;
  auto result = Run("SELECT ai_generate('hi', secret_name := 'chat')");
  EXPECT_TRUE(result->GetValue(0, 0).IsNull());
}

TEST_F(AIFunctionsTest, TruncatedReplies) {
  Mock(kChat, [](std::string_view) {
    return Reply{200, ChatReply("positi", "length", 1)};
  });
  Start();
  ExpectError(
    "SELECT ai_classify('x', ['positive', 'negative'], secret_name := 'chat')",
    "cut off at max_tokens");
  ExpectError("SELECT ai_generate('x', secret_name := 'chat')",
              "cut off at max_tokens (1024)");
  Run("SET sdb_ai_throw_on_error = false");
  auto result = Run("SELECT ai_generate('x', secret_name := 'chat')");
  EXPECT_TRUE(result->GetValue(0, 0).IsNull());
}

TEST_F(AIFunctionsTest, OutputTokenQuota) {
  auto& mock = Mock(kChat, [](std::string_view) {
    return Reply{200, ChatReply("ok", "stop", 10)};
  });
  Start();
  Run("SET sdb_ai_max_concurrent_requests = 1");
  Run("SET sdb_ai_max_output_tokens_per_query = 10");
  ExpectError(
    "SELECT ai_generate(range::VARCHAR, secret_name := 'chat') FROM range(3)",
    "sdb_ai_max_output_tokens_per_query");
  Run("SET sdb_ai_throw_on_quota_exceeded = false");
  auto result = Run(
    "SELECT count(g) FROM (SELECT ai_generate(range::VARCHAR, secret_name := "
    "'chat') AS g FROM range(3))");
  EXPECT_EQ(result->GetValue(0, 0).GetValue<int64_t>(), 1);
  EXPECT_EQ(mock.Bodies().size(), 2);
}

TEST_F(AIFunctionsTest, ChatRequestBody) {
  auto& mock = Mock(kChat, [](std::string_view) {
    return Reply{200,
                 ChatReply(R"({\"city\":\"Berlin\",\"extra\":1})", "stop", 1)};
  });
  Start();
  auto result = Run(
    R"(SELECT ai_extract('Lives in Berlin.', '{"city": "the city"}', secret_name := 'chat'))");
  EXPECT_EQ(result->GetValue(0, 0).ToString(), R"({"city":"Berlin"})");
  Run(
    "SELECT ai_generate('q', system_prompt := 'be brief', temperature := 0.2, "
    "max_tokens := 7, secret_name := 'chat')");

  const auto bodies = mock.Bodies();
  ASSERT_EQ(bodies.size(), 2);
  simdjson::dom::parser parser;
  auto extract = Parse(parser, bodies[0]);
  EXPECT_EQ(std::string_view{extract["model"]}, "m");
  EXPECT_EQ(double{extract["temperature"]}, 0.0);
  EXPECT_EQ(int64_t{extract["max_tokens"]}, 1024);
  EXPECT_EQ(std::string_view{extract["response_format"]["type"]},
            "json_schema");
  auto schema = extract["response_format"]["json_schema"];
  EXPECT_TRUE(bool{schema["strict"]});
  EXPECT_EQ(std::string_view{schema["schema"]["required"].at(0)}, "city");
  EXPECT_EQ(std::string_view{extract["messages"].at(0)["role"]}, "system");
  EXPECT_EQ(std::string_view{extract["messages"].at(1)["content"]},
            "Lives in Berlin.");

  simdjson::dom::parser generate_parser;
  auto generate = Parse(generate_parser, bodies[1]);
  EXPECT_EQ(double{generate["temperature"]}, 0.2);
  EXPECT_EQ(int64_t{generate["max_tokens"]}, 7);
  EXPECT_EQ(std::string_view{generate["messages"].at(0)["content"]},
            "be brief");
  EXPECT_TRUE(generate["response_format"].error());
}

TEST_F(AIFunctionsTest, EmbeddingBatchesAndDimensions) {
  auto& mock = Mock(kEmbeddings, [](std::string_view body) {
    simdjson::dom::parser parser;
    const auto n = Parse(parser, body)["input"].get_array().size();
    std::string data;
    for (size_t i = 0; i != n; ++i) {
      absl::StrAppend(&data, i == 0 ? "" : ",", R"({"embedding":[1,0]})");
    }
    return Reply{200, absl::StrCat(R"({"data":[)", data, "]}")};
  });
  Start();
  Run("SET sdb_ai_embedding_max_batch_size = 2");
  auto result = Run(
    "SELECT count(e) FROM (SELECT ai_embed(body, 'm', 'chat', dimensions := "
    "2) AS e FROM (VALUES ('a'), (NULL), ('b'), ('c')) v(body))");
  EXPECT_EQ(result->GetValue(0, 0).GetValue<int64_t>(), 3);

  auto bodies = mock.Bodies();
  ASSERT_EQ(bodies.size(), 2);
  std::vector<size_t> sizes;
  for (const auto& body : bodies) {
    simdjson::dom::parser parser;
    auto doc = Parse(parser, body);
    EXPECT_EQ(int64_t{doc["dimensions"]}, 2);
    sizes.push_back(doc["input"].get_array().size());
  }
  std::ranges::sort(sizes);
  EXPECT_EQ(sizes, (std::vector<size_t>{1, 2}));

  result = Run("SELECT ai_similarity('a', 'b', 'm', 'chat')");
  EXPECT_DOUBLE_EQ(result->GetValue(0, 0).GetValue<double>(), 1.0);
}

TEST_F(AIFunctionsTest, JevPackingSplitsOn422) {
  auto& mock = Mock(kSystemOne, [](std::string_view body) -> Reply {
    simdjson::dom::parser parser;
    auto doc = Parse(parser, body);
    simdjson::dom::object state;
    if (doc["state"].get_object().get(state) == simdjson::SUCCESS &&
        state.size() > 2) {
      return {422, R"({"detail":"state is too long"})"};
    }
    std::string answers;
    for (auto question : doc["questions"].get_object()) {
      absl::StrAppend(
        &answers, answers.empty() ? "" : ",", "\"", question.key,
        R"(":{"type":"choice","choice":"billing","confidence":0.9,"probabilities":{"billing":0.9,"sales":0.1}})");
    }
    return {200,
            absl::StrCat(R"({"model":"jev-latest","answers":{)", answers,
                         R"(},"usage":{"input_tokens":1,"output_tokens":2}})")};
  });
  Start();
  auto result = Run(
    "SELECT count(*) FILTER (WHERE r.choice = 'billing' AND "
    "r.probabilities[2].value = 'sales') FROM (SELECT ai_system1(body, "
    "'Which team?', choice := [{label: 'billing', description: 'Invoices'}, "
    "{label: 'sales', description: NULL}], batch_size := 3, secret_name := "
    "'jev') AS r FROM (VALUES ('a'), ('b'), ('c')) v(body)) sub");
  EXPECT_EQ(result->GetValue(0, 0).GetValue<int64_t>(), 3);

  const auto bodies = mock.Bodies();
  ASSERT_EQ(bodies.size(), 4);
  simdjson::dom::parser packed_parser;
  auto packed = Parse(packed_parser, bodies[0]);
  EXPECT_EQ(std::string_view{packed["model"]}, "jev-latest");
  EXPECT_EQ(packed["state"].get_object().size(), 3);
  EXPECT_EQ(std::string_view{packed["state"]["r2"]}, "c");
  auto question = packed["questions"]["r1"];
  EXPECT_EQ(std::string_view{question["type"]}, "choice");
  EXPECT_EQ(std::string_view{question["instructions"]["question"]},
            "Which team?");
  EXPECT_EQ(simdjson::minify(question["criteria"]),
            R"({"billing":"Invoices","sales":null})");

  std::vector<std::string> states;
  for (size_t i = 1; i != bodies.size(); ++i) {
    simdjson::dom::parser parser;
    auto doc = Parse(parser, bodies[i]);
    std::string_view state;
    if (doc["state"].get(state) == simdjson::SUCCESS) {
      EXPECT_EQ(std::string_view{doc["questions"]["answer"]["instructions"]},
                "Which team?");
      states.emplace_back(state);
    } else {
      states.push_back(absl::StrCat(doc["state"].get_object().size()));
    }
  }
  std::ranges::sort(states);
  EXPECT_EQ(states, (std::vector<std::string>{"2", "a", "x"}));
}

TEST_F(AIFunctionsTest, JevUnprocessableRowFailsQuery) {
  Mock(kSystemOne, [](std::string_view) {
    return Reply{422, R"({"detail":"bad question"})"};
  });
  Start();
  Run("SET sdb_ai_throw_on_error = false");
  ExpectError(
    "SELECT ai_system1('x', questions := {a: {type: 'noul', instructions: "
    "'q'}}, secret_name := 'jev')",
    "returned HTTP 422");
}

TEST_F(AIFunctionsTest, RequestsSpreadOverThreads) {
  InFlight flight;
  Mock(kChat, [&](std::string_view) {
    flight.Enter();
    std::this_thread::sleep_for(std::chrono::milliseconds{300});
    flight.Leave();
    return Reply{200, ChatReply("ok", "stop", 1)};
  });
  Start();
  Run("SET threads = 1");
  const std::string sql =
    "SELECT ai_generate(v, secret_name := 'chat') FROM (VALUES ('a'), ('b'), "
    "('c'), ('d')) t(v)";
  EXPECT_EQ(Run(sql)->RowCount(), 4);
  EXPECT_EQ(flight.peak.load(), 4);
  flight.peak = 0;
  EXPECT_EQ(Run("SELECT CASE WHEN v <> 'x' THEN ai_generate(v, secret_name := "
                "'chat') END FROM (VALUES ('a'), ('b'), ('c'), ('d')) t(v)")
              ->RowCount(),
            4);
  EXPECT_EQ(flight.peak.load(), 4);
  flight.peak = 0;
  Run("SET sdb_ai_max_concurrent_requests = 1");
  EXPECT_EQ(Run(sql)->RowCount(), 4);
  EXPECT_EQ(flight.peak.load(), 1);
}

TEST_F(AIFunctionsTest, ThreadsShareQueryCap) {
  InFlight flight;
  std::atomic_int busy = 0;
  std::atomic_int total = 0;
  Mock(kChat, [&](std::string_view) {
    ++total;
    if (flight.Enter() >= 2) {
      ++busy;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{2});
    flight.Leave();
    return Reply{200, ChatReply("ok", "stop", 1)};
  });
  Start();
  Run("SET threads = 4");
  Run("SET sdb_ai_max_concurrent_requests = 4");
  for (const auto* call :
       {"ai_generate(range::VARCHAR, secret_name := 'chat')",
        "CASE WHEN range >= 0 THEN ai_generate(range::VARCHAR, secret_name := "
        "'chat') END"}) {
    flight.peak = 0;
    busy = 0;
    total = 0;
    auto result =
      Run(absl::StrCat("SELECT count(", call, ") FROM range(4096)"));
    EXPECT_EQ(result->GetValue(0, 0).GetValue<int64_t>(), 4096) << call;
    EXPECT_LE(flight.peak.load(), 4) << call;
    EXPECT_GE(busy.load() * 4, total.load() * 3)
      << call << ": " << busy.load() << " of " << total.load();
  }
}

TEST_F(AIFunctionsTest, AsyncThreadsZeroKeepsConcurrency) {
  InFlight flight;
  Mock(kChat, [&](std::string_view) {
    flight.Enter();
    std::this_thread::sleep_for(std::chrono::milliseconds{2});
    flight.Leave();
    return Reply{200, ChatReply("ok", "stop", 1)};
  });
  Start();
  Run("SET threads = 2");
  Run("SET async_threads = 0");
  auto result = Run(
    "SELECT count(ai_generate(range::VARCHAR, secret_name := 'chat')) FROM "
    "range(4096)");
  EXPECT_EQ(result->GetValue(0, 0).GetValue<int64_t>(), 4096);
  EXPECT_GE(flight.peak.load(), 2);
}

TEST_F(AIFunctionsTest, TopNDefersAICalls) {
  auto& mock = Mock(kChat, [](std::string_view body) {
    if (Content(body, 0).starts_with("You rate how well")) {
      return Reply{200, ChatReply(R"({\"score\":0.5})", "stop", 1)};
    }
    return Reply{200, ChatReply(Content(body, 1), "stop", 1)};
  });
  Start();
  constexpr std::string_view kRows =
    "(SELECT range AS id, range::VARCHAR AS v FROM range(100)) t";
  auto sent = [&](const std::string& sql) {
    const auto before = mock.Bodies().size();
    auto result = Run(sql);
    return std::pair{mock.Bodies().size() - before, std::move(result)};
  };
  auto [by_id, result] =
    sent(absl::StrCat("SELECT id, ai_generate(v, secret_name := 'chat') FROM ",
                      kRows, " ORDER BY id LIMIT 3"));
  EXPECT_EQ(by_id, 3);
  EXPECT_EQ(result->GetValue(1, 2).ToString(), "2");
  EXPECT_EQ(sent(absl::StrCat("SELECT ai_generate(v, secret_name := 'chat') "
                              "FROM ",
                              kRows, " ORDER BY length(v) DESC, id LIMIT 3"))
              .first,
            3);
  EXPECT_EQ(sent(absl::StrCat("SELECT * FROM (SELECT id, ai_generate(v, "
                              "secret_name := 'chat') AS g FROM ",
                              kRows, ") s ORDER BY id LIMIT 3"))
              .first,
            3);
  EXPECT_EQ(sent(absl::StrCat("SELECT ai_generate(v, secret_name := 'chat') "
                              "FROM ",
                              kRows,
                              " ORDER BY ai_score(v, 'x', secret_name := "
                              "'chat'), id LIMIT 3"))
              .first,
            103);
  auto [filtered, rows] = sent(absl::StrCat(
    "SELECT id, g FROM (SELECT id, ai_generate(v, secret_name := 'chat') AS g "
    "FROM ",
    kRows, " ORDER BY id LIMIT 5) s WHERE id % 2 = 0 ORDER BY id"));
  EXPECT_EQ(filtered, 5);
  ASSERT_EQ(rows->RowCount(), 3);
  for (duckdb::idx_t i = 0; i != 3; ++i) {
    EXPECT_EQ(rows->GetValue(0, i).GetValue<int64_t>(), int64_t(2 * i));
    EXPECT_EQ(rows->GetValue(1, i).ToString(), absl::StrCat(2 * i));
  }
}

TEST_F(AIFunctionsTest, TopNUnderFilterAndJoinKeepsBindings) {
  Mock(kChat, [](std::string_view body) {
    return Reply{200, ChatReply(Content(body, 1), "stop", 1)};
  });
  Start();
  auto none = Run(
    "SELECT count(*), string_agg(c, ',') FROM (SELECT id, k, ai_generate(v, "
    "secret_name := 'chat') AS c FROM (SELECT 1 AS id, range AS k, "
    "range::VARCHAR AS v FROM range(3)) t ORDER BY k LIMIT 2) s WHERE s.id > "
    "1000");
  EXPECT_EQ(none->GetValue(0, 0).GetValue<int64_t>(), 0);
  EXPECT_TRUE(none->GetValue(1, 0).IsNull());
  constexpr std::string_view kTop =
    "(SELECT id, k, ai_generate(v, secret_name := 'chat') AS c FROM (SELECT "
    "range AS id, range AS k, range::VARCHAR AS v FROM range(3)) t ORDER BY k "
    "LIMIT 2) s";
  auto one = Run(absl::StrCat("SELECT count(*), string_agg(c, ',') FROM ", kTop,
                              " WHERE s.id >= 1"));
  EXPECT_EQ(one->GetValue(0, 0).GetValue<int64_t>(), 1);
  EXPECT_EQ(one->GetValue(1, 0).ToString(), "1");
  auto joined = Run(absl::StrCat(
    "SELECT count(*), string_agg(c, ',' ORDER BY c) FROM range(3) u JOIN ",
    kTop, " ON u.range = s.id"));
  EXPECT_EQ(joined->GetValue(0, 0).GetValue<int64_t>(), 2);
  EXPECT_EQ(joined->GetValue(1, 0).ToString(), "0,1");
}

TEST_F(AIFunctionsTest, AndConjunctsRunLazily) {
  auto& mock = Mock(kChat, [](std::string_view body) {
    const bool first = absl::StrContains(Content(body, 0), "first");
    const bool match =
      !first || Content(body, 1) == "0" || Content(body, 1) == "1";
    return Reply{
      200, ChatReply(match ? R"({\"match\":true})" : R"({\"match\":false})",
                     "stop", 1)};
  });
  Start();
  auto result = Run(
    "SELECT count(*) FROM range(4) WHERE ai_filter(range::VARCHAR, 'first', "
    "secret_name := 'chat') AND ai_filter(range::VARCHAR, 'second', "
    "secret_name := 'chat')");
  EXPECT_EQ(result->GetValue(0, 0).GetValue<int64_t>(), 2);
  EXPECT_EQ(absl::c_count_if(mock.Bodies(),
                             [](const std::string& body) {
                               return absl::StrContains(Content(body, 0),
                                                        "second");
                             }),
            2);
}

TEST_F(AIFunctionsTest, CheapPredicatesAndLimitCutRequests) {
  auto& mock = Mock(kChat, [](std::string_view) {
    return Reply{200, ChatReply(R"({\"match\":true})", "stop", 1)};
  });
  Start();
  auto result = Run(
    "SELECT count(*) FROM range(4) r WHERE range <> 2 AND ai_filter(range::"
    "VARCHAR, 'x', secret_name := 'chat')");
  EXPECT_EQ(result->GetValue(0, 0).GetValue<int64_t>(), 3);
  EXPECT_EQ(mock.Bodies().size(), 3);
  Run(
    "SELECT ai_generate(range::VARCHAR, secret_name := 'chat') FROM "
    "range(100) LIMIT 3");
  EXPECT_EQ(mock.Bodies().size(), 6);
}

TEST_F(AIFunctionsTest, EvaluateKeepsInputOrder) {
  Mock(kChat, [](std::string_view body) {
    return Reply{200, ChatReply(Content(body, 1), "stop", 1)};
  });
  Start();
  for (const auto* threads : {"4", "1"}) {
    Run(absl::StrCat("SET threads = ", threads));
    auto result = Run(
      "SELECT count(*) FILTER (WHERE r <> range::VARCHAR) FROM (SELECT range, "
      "ai_generate(range::VARCHAR, secret_name := 'chat') AS r FROM "
      "range(2500))");
    EXPECT_EQ(result->GetValue(0, 0).GetValue<int64_t>(), 0) << threads;
    result = Run(
      "SELECT list(r) = list(range::VARCHAR) FROM (SELECT range, "
      "ai_generate(range::VARCHAR, secret_name := 'chat') AS r FROM "
      "range(2500))");
    EXPECT_TRUE(result->GetValue(0, 0).GetValue<bool>()) << threads;
  }
}

TEST_F(AIFunctionsTest, AggregateOrderAndWindow) {
  auto& mock = Mock(kChat, [](std::string_view) {
    return Reply{200, ChatReply("summary", "stop", 1)};
  });
  Start();
  auto result = Run(
    "SELECT g, ai_agg(v, 'q', secret_name := 'chat' ORDER BY v DESC) FROM "
    "(VALUES (1, 'a'), (1, 'b'), (2, NULL)) t(g, v) GROUP BY g ORDER BY g");
  EXPECT_EQ(result->GetValue(1, 0).ToString(), "summary");
  EXPECT_TRUE(result->GetValue(1, 1).IsNull());
  auto bodies = mock.Bodies();
  ASSERT_EQ(bodies.size(), 1);
  EXPECT_EQ(Content(bodies[0], 1), R"({"group_size":2,"values":["b","a"]})");

  result = Run(
    "SELECT ai_agg(v, 'q', secret_name := 'chat') OVER () FROM (VALUES ('a'), "
    "('b')) t(v)");
  EXPECT_EQ(result->GetValue(0, 1).ToString(), "summary");
  bodies = mock.Bodies();
  ASSERT_GE(bodies.size(), 2);
  EXPECT_EQ(Content(bodies.back(), 1),
            R"({"group_size":2,"values":["a","b"]})");
}

TEST_F(AIFunctionsTest, DuplicateInputsShareOneRequest) {
  auto& mock = Mock(kChat, [](std::string_view) {
    return Reply{200, ChatReply(R"({\"category\":\"a\"})", "stop", 1)};
  });
  Start();
  Run("SET sdb_ai_max_concurrent_requests = 1");
  auto result = Run(
    "SELECT count(ai_classify(v, ['a', 'b'], secret_name := 'chat')) FROM "
    "(VALUES ('x'), ('x'), ('y')) t(v)");
  EXPECT_EQ(result->GetValue(0, 0).GetValue<int64_t>(), 3);
  EXPECT_EQ(mock.Bodies().size(), 2);
}

TEST_F(AIFunctionsTest, ProviderRefusalIsRowError) {
  Mock(kChat, [](std::string_view) {
    return Reply{
      200,
      R"({"choices":[{"message":{"content":null,"refusal":"no"},"finish_reason":"stop"}]})"};
  });
  Start();
  ExpectError("SELECT ai_generate('x', secret_name := 'chat')",
              "withheld or filtered");
  Run("SET sdb_ai_throw_on_error = false");
  auto result = Run("SELECT ai_generate('x', secret_name := 'chat')");
  EXPECT_TRUE(result->GetValue(0, 0).IsNull());
}

TEST_F(AIFunctionsTest, EmbeddingsFollowResponseIndex) {
  Mock(kEmbeddings, [](std::string_view) {
    return Reply{
      200,
      R"({"data":[{"index":1,"embedding":[0,1]},{"index":0,"embedding":[1,0]}]})"};
  });
  Start();
  auto result = Run(
    "SELECT v, ai_embed(v, 'm', 'chat')[1] FROM (VALUES ('a'), ('b')) t(v) "
    "ORDER BY v");
  EXPECT_EQ(result->GetValue(1, 0).GetValue<float>(), 1);
  EXPECT_EQ(result->GetValue(1, 1).GetValue<float>(), 0);
}

TEST_F(AIFunctionsTest, InsecureEndpointNeedsOptIn) {
  Start();
  Run(
    "CREATE SECRET far (TYPE openai, base_url 'http://example.invalid', "
    "model 'm')");
  ExpectError("SELECT ai_generate(NULL, secret_name := 'far')",
              "insecure endpoint");
  Run("SET sdb_ai_allow_insecure_endpoint = true");
  auto result = Run("SELECT ai_generate(NULL, secret_name := 'far')");
  EXPECT_TRUE(result->GetValue(0, 0).IsNull());
}

TEST_F(AIFunctionsTest, CancelWhileRequestsWait) {
  Mock(kChat, [](std::string_view) {
    std::this_thread::sleep_for(std::chrono::milliseconds{500});
    return Reply{200, ChatReply("ok", "stop", 1)};
  });
  Start();
  std::thread cancel{[&] {
    std::this_thread::sleep_for(std::chrono::milliseconds{200});
    _conn.Interrupt();
  }};
  auto result = _conn.Query(
    "SELECT ai_generate(range::VARCHAR, secret_name := 'chat') FROM "
    "range(64)");
  cancel.join();
  EXPECT_TRUE(result->HasError());
  auto after = Run("SELECT 1");
  EXPECT_EQ(after->GetValue(0, 0).GetValue<int32_t>(), 1);
}

TEST_F(AIFunctionsTest, RateLimitRetriesThenFailsRow) {
  auto& mock = Mock(kChat, [](std::string_view) {
    return Reply{
      429, R"({"error":{"message":"Rate limit reached","type":"requests"}})",
      "retry-after: 0\r\n"};
  });
  Start();
  Run("SET sdb_ai_max_retries = 2");
  const std::string sql = "SELECT ai_generate('x', secret_name := 'chat')";
  ExpectError(sql, "returned HTTP 429: [requests] Rate limit reached");
  EXPECT_EQ(mock.Bodies().size(), 3);
  Run("SET sdb_ai_throw_on_error = false");
  EXPECT_TRUE(Run(sql)->GetValue(0, 0).IsNull());
  EXPECT_EQ(mock.Bodies().size(), 6);
  Run("SET sdb_ai_max_retries = 0");
  EXPECT_TRUE(Run(sql)->GetValue(0, 0).IsNull());
  EXPECT_EQ(mock.Bodies().size(), 7);
}

TEST_F(AIFunctionsTest, RetryBackoffDoubles) {
  std::atomic_int calls = 0;
  Mock(kChat, [&](std::string_view) -> Reply {
    switch (calls++) {
      case 0:
      case 1:
        return {503, R"({"error":"busy"})"};
      case 3:
        return {429, R"({"error":"slow down"})",
                "Retry-After: Wed, 21 Oct 2015 07:28:00 GMT\r\n"};
      default:
        return {200, ChatReply("ok", "stop", 1)};
    }
  });
  Start();
  Run("SET sdb_ai_retry_initial_delay_ms = 100");
  auto query = [&] { Run("SELECT ai_generate('x', secret_name := 'chat')"); };
  EXPECT_GE(Timed(query), std::chrono::milliseconds{300});
  const auto date = Timed(query);
  EXPECT_GE(date, std::chrono::milliseconds{100});
  EXPECT_LT(date, std::chrono::seconds{10});
  EXPECT_EQ(calls.load(), 5);
}

TEST_F(AIFunctionsTest, CancelDuringRetryAfter) {
  Mock(kChat, [](std::string_view) {
    return Reply{429, R"({"error":"slow down"})", "Retry-After: 3600\r\n"};
  });
  Start();
  Run("SET sdb_ai_throw_on_error = false");
  std::thread cancel{[&] {
    std::this_thread::sleep_for(std::chrono::milliseconds{200});
    _conn.Interrupt();
  }};
  duckdb::unique_ptr<duckdb::MaterializedQueryResult> result;
  const auto elapsed = Timed([&] {
    result = _conn.Query("SELECT ai_generate('x', secret_name := 'chat')");
  });
  cancel.join();
  EXPECT_TRUE(result->HasError());
  EXPECT_LT(elapsed, std::chrono::seconds{5});
  EXPECT_EQ(Run("SELECT 1")->GetValue(0, 0).GetValue<int32_t>(), 1);
}

TEST_F(AIFunctionsTest, RetriesStayWithinConcurrency) {
  InFlight flight;
  std::mutex mutex;
  std::set<std::string> seen;
  auto& mock = Mock(kChat, [&](std::string_view body) -> Reply {
    flight.Enter();
    std::this_thread::sleep_for(std::chrono::milliseconds{20});
    flight.Leave();
    std::lock_guard lock{mutex};
    if (seen.emplace(body).second) {
      return {429, R"({"error":"slow down"})", "Retry-After: 0\r\n"};
    }
    return {200, ChatReply("ok", "stop", 1)};
  });
  Start();
  Run("SET sdb_ai_max_concurrent_requests = 2");
  auto result = Run(
    "SELECT count(ai_generate(range::VARCHAR, secret_name := 'chat')) FROM "
    "range(8)");
  EXPECT_EQ(result->GetValue(0, 0).GetValue<int64_t>(), 8);
  EXPECT_LE(flight.peak.load(), 2);
  EXPECT_EQ(mock.Bodies().size(), 16);
}

TEST_F(AIFunctionsTest, RetriesCountTowardCallQuota) {
  auto& mock = Mock(
    kChat, [](std::string_view) { return Reply{503, R"({"error":"busy"})"}; });
  Start();
  Run("SET sdb_ai_max_retries = 5");
  Run("SET sdb_ai_max_api_calls_per_query = 2");
  ExpectError("SELECT ai_generate('x', secret_name := 'chat')",
              "sdb_ai_max_api_calls_per_query (2)");
  EXPECT_EQ(mock.Bodies().size(), 2);
}

TEST_F(AIFunctionsTest, ExhaustedProviderQuotaFailsAtOnce) {
  auto& mock = Mock(kChat, [](std::string_view) {
    return Reply{
      429,
      R"({"error":{"message":"You exceeded your current quota","type":"insufficient_quota","code":"insufficient_quota"}})"};
  });
  Start();
  Run("SET sdb_ai_throw_on_error = false");
  ExpectError(
    "SELECT ai_generate('x', secret_name := 'chat')",
    "returned HTTP 429: [insufficient_quota] You exceeded your current quota");
  EXPECT_EQ(mock.Bodies().size(), 1);
}

TEST_F(AIFunctionsTest, OutputTokenQuotaBoundsInFlight) {
  auto& mock = Mock(kChat, [](std::string_view) {
    std::this_thread::sleep_for(std::chrono::milliseconds{50});
    return Reply{200, ChatReply("ok", "stop", 1)};
  });
  Start();
  Run("SET sdb_ai_max_concurrent_requests = 4");
  Run("SET sdb_ai_max_output_tokens_per_query = 1");
  Run("SET sdb_ai_throw_on_quota_exceeded = false");
  auto result = Run(
    "SELECT count(ai_generate(range::VARCHAR, secret_name := 'chat')) FROM "
    "range(64)");
  const auto sent = mock.Bodies().size();
  EXPECT_GE(sent, 1);
  EXPECT_LE(sent, 4);
  EXPECT_EQ(result->GetValue(0, 0).GetValue<int64_t>(), sent);
}

TEST_F(AIFunctionsTest, OutputTokenQuotaCountsSystem1) {
  auto& mock = Mock(kSystemOne, [](std::string_view) {
    return Reply{
      200,
      R"({"answers":{"answer":{"noul":0.9}},"usage":{"input_tokens":1,"output_tokens":5}})"};
  });
  Start();
  Run("SET sdb_ai_max_concurrent_requests = 1");
  Run("SET sdb_ai_max_output_tokens_per_query = 5");
  ExpectError(
    "SELECT ai_system1(v, 'q', batch_size := 1, secret_name := 'jev') FROM "
    "(VALUES ('a'), ('b'), ('c')) t(v)",
    "sdb_ai_max_output_tokens_per_query (5)");
  EXPECT_EQ(mock.Bodies().size(), 1);
}

TEST_F(AIFunctionsTest, UnreportedUsageNeverTrips) {
  auto& mock = Mock(kChat, [](std::string_view) {
    return Reply{
      200,
      R"({"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]})"};
  });
  Start();
  Run("SET sdb_ai_max_concurrent_requests = 1");
  Run("SET sdb_ai_max_output_tokens_per_query = 1");
  auto result = Run(
    "SELECT count(ai_generate(range::VARCHAR, secret_name := 'chat')) FROM "
    "range(3)");
  EXPECT_EQ(result->GetValue(0, 0).GetValue<int64_t>(), 3);
  EXPECT_EQ(mock.Bodies().size(), 3);
}

TEST_F(AIFunctionsTest, ContextLengthErrorFailsOnlyLongRows) {
  auto& mock = Mock(kChat, [](std::string_view body) {
    if (Content(body, 1).size() > 100) {
      return Reply{
        400,
        R"({"error":{"message":"This model's maximum context length is 16 tokens","type":"invalid_request_error","code":"context_length_exceeded"}})"};
    }
    return Reply{200, ChatReply("ok", "stop", 1)};
  });
  Start();
  auto long_rows = [&] {
    return absl::c_count_if(mock.Bodies(), [](const std::string& body) {
      return Content(body, 1).size() > 100;
    });
  };
  const std::string sql =
    "SELECT v, ai_generate(v, secret_name := 'chat') FROM (VALUES ('short'), "
    "(repeat('x', 1000))) t(v) ORDER BY length(v)";
  ExpectError(sql,
              "returned HTTP 400: [invalid_request_error] This model's "
              "maximum context length is 16 tokens");
  EXPECT_EQ(long_rows(), 1);
  Run("SET sdb_ai_throw_on_error = false");
  auto result = Run(sql);
  EXPECT_EQ(result->GetValue(1, 0).ToString(), "ok");
  EXPECT_TRUE(result->GetValue(1, 1).IsNull());
  EXPECT_EQ(long_rows(), 2);
}

TEST_F(AIFunctionsTest, EmbeddingBatchSplitsAroundOversizedText) {
  auto& mock = Mock(kEmbeddings, [](std::string_view body) -> Reply {
    simdjson::dom::parser parser;
    std::string data;
    for (auto input : Parse(parser, body)["input"].get_array()) {
      if (std::string_view{input}.size() > 100) {
        return {
          400,
          R"({"error":{"message":"input is too long","type":"invalid_request_error"}})"};
      }
      absl::StrAppend(&data, data.empty() ? "" : ",", R"({"embedding":[1,0]})");
    }
    return {200, absl::StrCat(R"({"data":[)", data, "]}")};
  });
  Start();
  Run("SET sdb_ai_embedding_max_batch_size = 4");
  const std::string sql =
    "SELECT ai_embed(v, 'm', 'chat') IS NULL FROM (VALUES ('a'), ('b'), "
    "(repeat('x', 1000)), ('c')) t(v) ORDER BY v";
  ExpectError(sql, "returned HTTP 400: [invalid_request_error] input is too");
  Run("SET sdb_ai_throw_on_error = false");
  const auto before = mock.Bodies().size();
  auto result = Run(sql);
  for (duckdb::idx_t i = 0; i != 4; ++i) {
    EXPECT_EQ(result->GetValue(0, i).GetValue<bool>(), i == 3) << i;
  }
  const auto bodies = mock.Bodies();
  std::vector<size_t> sizes;
  for (size_t i = before; i != bodies.size(); ++i) {
    simdjson::dom::parser parser;
    sizes.push_back(Parse(parser, bodies[i])["input"].get_array().size());
  }
  std::ranges::sort(sizes);
  EXPECT_EQ(sizes, (std::vector<size_t>{1, 1, 1, 2, 2, 4}));
}

TEST_F(AIFunctionsTest, UniformRejectionStopsSplitting) {
  auto& mock = Mock(kEmbeddings, [](std::string_view) {
    return Reply{
      400,
      R"({"error":{"message":"dimensions is not supported","type":"invalid_request_error"}})"};
  });
  Start();
  Run("SET sdb_ai_embedding_max_batch_size = 8");
  const std::string sql =
    "SELECT count(ai_embed(range::VARCHAR, 'm', 'chat', dimensions := 3)) "
    "FROM range(8)";
  ExpectError(sql, "dimensions is not supported");
  EXPECT_EQ(mock.Bodies().size(), 4);
  Run("SET sdb_ai_throw_on_error = false");
  EXPECT_EQ(Run(sql)->GetValue(0, 0).GetValue<int64_t>(), 0);
  EXPECT_EQ(mock.Bodies().size(), 8);
}

TEST_F(AIFunctionsTest, SizeRejectionStillSplits) {
  auto& mock = Mock(kSystemOne, [](std::string_view body) -> Reply {
    simdjson::dom::parser parser;
    auto doc = Parse(parser, body);
    simdjson::dom::object state;
    if (doc["state"].get_object().get(state) == simdjson::SUCCESS &&
        state.size() > 2) {
      return {422, R"({"detail":"state is too long"})"};
    }
    std::string answers;
    for (auto question : doc["questions"].get_object()) {
      absl::StrAppend(&answers, answers.empty() ? "" : ",", "\"", question.key,
                      R"(":{"noul":0.9})");
    }
    return {200, absl::StrCat(R"({"answers":{)", answers, "}}")};
  });
  Start();
  auto result = Run(
    "SELECT count(r) FROM (SELECT ai_system1(range::VARCHAR, 'q', batch_size "
    ":= 8, secret_name := 'jev') AS r FROM range(8))");
  EXPECT_EQ(result->GetValue(0, 0).GetValue<int64_t>(), 8);
  EXPECT_EQ(mock.Bodies().size(), 8);
}

TEST_F(AIFunctionsTest, EmbeddingReplyMustMatchDimensions) {
  std::atomic_bool empty = false;
  Mock(kEmbeddings, [&](std::string_view) {
    return Reply{200, empty ? R"({"data":[{"embedding":[]}]})"
                            : R"({"data":[{"embedding":[1,0]}]})"};
  });
  Start();
  const std::string sql =
    "SELECT ai_embed(v, 'm', 'chat', dimensions := 3) FROM (VALUES ('a')) t(v)";
  ExpectError(sql, "has 2 values, expected 3");
  empty = true;
  ExpectError("SELECT ai_embed(v, 'm', 'chat') FROM (VALUES ('a')) t(v)",
              "has 0 values, expected at least 1");
  Run("SET sdb_ai_throw_on_error = false");
  EXPECT_TRUE(Run(sql)->GetValue(0, 0).IsNull());
}

TEST_F(AIFunctionsTest, AggregateRespectsContextBudget) {
  auto& mock = Mock(kChat, [](std::string_view body) {
    return Reply{200, ChatReply(IsPartial(body) ? "n" : "done", "stop", 1)};
  });
  Start();
  auto result = Run(
    "SELECT ai_agg(v, 'q', max_context_chars := 25, secret_name := 'chat') "
    "FROM (SELECT repeat(range::VARCHAR, 10) AS v FROM range(6))");
  EXPECT_EQ(result->GetValue(0, 0).ToString(), "done");
  const auto bodies = mock.Bodies();
  ASSERT_EQ(bodies.size(), 4);
  size_t partials = 0;
  for (const auto& body : bodies) {
    if (!IsPartial(body)) {
      continue;
    }
    ++partials;
    simdjson::dom::parser parser;
    const auto message = Content(body, 1);
    size_t chars = 0;
    for (auto value : Parse(parser, message)["values"].get_array()) {
      chars += std::string_view{value}.size();
    }
    EXPECT_EQ(chars, 20) << message;
  }
  EXPECT_EQ(partials, 3);
}

TEST_F(AIFunctionsTest, AggregateCutsOnCharacterBoundaries) {
  constexpr std::string_view kEmoji = "\xF0\x9F\x98\x80";
  auto& mock = Mock(kChat, [](std::string_view body) {
    return Reply{200, ChatReply(IsPartial(body) ? "" : "done", "stop", 1)};
  });
  Start();
  auto result = Run(absl::StrCat(
    "SELECT ai_agg(v, 'q', max_context_chars := 2, secret_name := 'chat') "
    "FROM (VALUES ('",
    kEmoji, kEmoji, kEmoji, "')) t(v)"));
  EXPECT_EQ(result->GetValue(0, 0).ToString(), "done");
  size_t partials = 0;
  for (const auto& body : mock.Bodies()) {
    if (IsPartial(body)) {
      ++partials;
      EXPECT_EQ(Content(body, 1), absl::StrCat(R"({"group_size":1,"values":[")",
                                               kEmoji, R"("]})"));
    }
  }
  EXPECT_EQ(partials, 3);
}

TEST_F(AIFunctionsTest, AggregateStopsWithoutProgress) {
  Mock(kChat, [](std::string_view body) {
    return Reply{200, ChatReply(IsPartial(body) ? std::string(50, 'n') : "done",
                                "stop", 1)};
  });
  Start();
  const std::string sql =
    "SELECT ai_agg(v, 'q', max_context_chars := 15, secret_name := 'chat') "
    "FROM (SELECT repeat('a', 10) AS v FROM range(3))";
  ExpectError(sql, "condensing the group made no progress");
  Run("SET sdb_ai_throw_on_error = false");
  EXPECT_TRUE(Run(sql)->GetValue(0, 0).IsNull());
}

TEST_F(AIFunctionsTest, MaximalIntegerSettings) {
  Mock(kChat,
       [](std::string_view) { return Reply{200, ChatReply("ok", "stop", 1)}; });
  Mock(kEmbeddings, [](std::string_view) {
    return Reply{200, R"({"data":[{"embedding":[1,0]}]})"};
  });
  Start();
  for (const auto* name :
       {"sdb_ai_max_api_calls_per_query", "sdb_ai_max_output_tokens_per_query",
        "sdb_ai_max_retries", "sdb_ai_retry_initial_delay_ms",
        "sdb_ai_request_timeout", "sdb_ai_max_concurrent_requests",
        "sdb_ai_embedding_max_batch_size"}) {
    Run(absl::StrCat("SET ", name, " = 4294967295"));
  }
  EXPECT_EQ(Run("SELECT ai_generate('x', secret_name := 'chat')")
              ->GetValue(0, 0)
              .ToString(),
            "ok");
  EXPECT_EQ(Run("SELECT len(ai_embed(v, 'm', 'chat')) FROM (VALUES ('a')) t(v)")
              ->GetValue(0, 0)
              .GetValue<int64_t>(),
            2);
}

TEST_F(AIFunctionsTest, RequestTimeoutFailsRow) {
  Mock(kChat, [](std::string_view) {
    std::this_thread::sleep_for(std::chrono::seconds{3});
    return Reply{200, ChatReply("ok", "stop", 1)};
  });
  Start();
  Run("SET sdb_ai_request_timeout = 1");
  Run("SET sdb_ai_max_retries = 0");
  const std::string sql = "SELECT ai_generate('x', secret_name := 'chat')";
  EXPECT_LT(Timed([&] { ExpectError(sql, "/v1/chat/completions' failed: "); }),
            std::chrono::milliseconds{2500});
  Run("SET sdb_ai_throw_on_error = false");
  EXPECT_TRUE(Run(sql)->GetValue(0, 0).IsNull());
}

TEST_F(AIFunctionsTest, ThrowOnErrorCoversEveryFunction) {
  for (const auto path : {kChat, kEmbeddings, kSystemOne}) {
    Mock(path, [](std::string_view) { return Reply{200, "not json"}; });
  }
  Start();
  for (const auto* call : {
         "ai_generate(v, secret_name := 'chat')",
         "ai_classify(v, ['a', 'b'], secret_name := 'chat')",
         "ai_classify_labels(v, ['a', 'b'], secret_name := 'chat')",
         "ai_extract(v, 'the city', secret_name := 'chat')",
         "ai_filter(v, 'x', secret_name := 'chat')",
         "ai_translate(v, 'German', secret_name := 'chat')",
         "ai_redact(v, ['email'], secret_name := 'chat')",
         "ai_score(v, 'x', secret_name := 'chat')",
         "ai_rerank('q', v, secret_name := 'chat')",
         "ai_embed(v, 'm', 'chat')",
         "ai_similarity(v, v, 'm', 'chat')",
         "ai_system1(v, 'q', secret_name := 'jev')",
         "ai_agg(v, 'q', secret_name := 'chat')",
         "ai_summarize_agg(v, secret_name := 'chat')",
       }) {
    const auto sql =
      absl::StrCat("SELECT ", call, " IS NULL FROM (VALUES ('x')) t(v)");
    Run("SET sdb_ai_throw_on_error = true");
    ExpectError(sql, "not valid JSON");
    Run("SET sdb_ai_throw_on_error = false");
    EXPECT_TRUE(Run(sql)->GetValue(0, 0).GetValue<bool>()) << call;
  }
}

TEST_F(AIFunctionsTest, ThrowOnErrorKeepsQuotaErrors) {
  Mock(kChat,
       [](std::string_view) { return Reply{200, ChatReply("ok", "stop", 1)}; });
  Start();
  Run("SET sdb_ai_throw_on_error = false");
  Run("SET sdb_ai_max_concurrent_requests = 1");
  Run("SET sdb_ai_max_api_calls_per_query = 1");
  ExpectError(
    "SELECT ai_generate(v, secret_name := 'chat') FROM (VALUES ('a'), ('b')) "
    "t(v)",
    "sdb_ai_max_api_calls_per_query (1)");
}

TEST_F(AIFunctionsTest, AsyncThreadsZeroStillSends) {
  Mock(kChat,
       [](std::string_view) { return Reply{200, ChatReply("ok", "stop", 1)}; });
  Start();
  Run("SET threads = 1");
  Run("SET async_threads = 0");
  for (const auto* select :
       {"ai_generate(v, secret_name := 'chat')",
        "CASE WHEN v <> '' THEN ai_generate(v, secret_name := 'chat') END"}) {
    auto result = Run(absl::StrCat("SELECT count(", select,
                                   ") FROM (VALUES ('a'), ('b')) t(v)"));
    EXPECT_EQ(result->GetValue(0, 0).GetValue<int64_t>(), 2) << select;
  }
}

TEST_F(AIFunctionsTest, PreparedStatementsResolveAtExecution) {
  auto ok = [](std::string_view) {
    return Reply{200, ChatReply("ok", "stop", 1)};
  };
  auto jev_ok = [](std::string_view) {
    return Reply{200, R"({"answers":{"answer":{"noul":0.9}}})"};
  };
  auto& first = Mock(kChat, ok);
  auto& second = Mock("/v2/chat/completions", ok);
  auto& jev_first = Mock(kSystemOne, jev_ok);
  auto& jev_second = Mock("/v2/systemone", jev_ok);
  auto& embeddings = Mock(kEmbeddings, [](std::string_view body) {
    simdjson::dom::parser parser;
    std::string data;
    for (size_t i = 0, n = Parse(parser, body)["input"].get_array().size();
         i != n; ++i) {
      absl::StrAppend(&data, i == 0 ? "" : ",", R"({"embedding":[1,0]})");
    }
    return Reply{200, absl::StrCat(R"({"data":[)", data, "]}")};
  });
  Start();
  auto execute = [](duckdb::PreparedStatement& statement) {
    duckdb::vector<duckdb::Value> values;
    return statement.Execute(values, false);
  };
  auto succeed = [&](duckdb::PreparedStatement& statement) {
    auto result = execute(statement);
    EXPECT_FALSE(result->HasError()) << result->GetError();
  };

  auto chat_secret = [&](std::string_view name, std::string_view path) {
    Run(absl::StrCat("CREATE OR REPLACE SECRET ", name,
                     " (TYPE openai, base_url '", _url, "', chat_path '", path,
                     "', model 'm')"));
  };

  auto generate =
    _conn.Prepare("SELECT ai_generate('x', secret_name := 'chat')");
  ASSERT_FALSE(generate->HasError()) << generate->GetError();
  auto agg = _conn.Prepare(
    "SELECT ai_agg(v, 'q', secret_name := 'chat') FROM (VALUES ('a')) t(v)");
  ASSERT_FALSE(agg->HasError()) << agg->GetError();
  succeed(*generate);
  succeed(*agg);
  chat_secret("chat", "/v2/chat/completions");
  succeed(*generate);
  succeed(*agg);
  EXPECT_EQ(first.Bodies().size(), 2);
  EXPECT_EQ(second.Bodies().size(), 2);

  auto system1 =
    _conn.Prepare("SELECT ai_system1('x', 'q', secret_name := 'jev')");
  ASSERT_FALSE(system1->HasError()) << system1->GetError();
  succeed(*system1);
  Run(absl::StrCat("CREATE OR REPLACE SECRET jev (TYPE typesafe, base_url '",
                   _url, "', path '/v2/systemone')"));
  succeed(*system1);
  EXPECT_EQ(jev_first.Bodies().size(), 1);
  EXPECT_EQ(jev_second.Bodies().size(), 1);

  chat_secret("text_a", kChat);
  chat_secret("text_b", "/v2/chat/completions");
  Run("SET sdb_ai_text_default_secret = 'text_a'");
  auto by_default = _conn.Prepare("SELECT ai_generate('x')");
  ASSERT_FALSE(by_default->HasError()) << by_default->GetError();
  succeed(*by_default);
  Run("SET sdb_ai_text_default_secret = 'text_b'");
  succeed(*by_default);
  EXPECT_EQ(first.Bodies().size(), 3);
  EXPECT_EQ(second.Bodies().size(), 3);

  auto embed = _conn.Prepare(
    "SELECT count(ai_embed(v, 'm', 'chat')) FROM (VALUES ('a'), ('b'), ('c'), "
    "('d')) t(v)");
  ASSERT_FALSE(embed->HasError()) << embed->GetError();
  succeed(*embed);
  EXPECT_EQ(embeddings.Bodies().size(), 1);
  Run("SET sdb_ai_embedding_max_batch_size = 1");
  succeed(*embed);
  EXPECT_EQ(embeddings.Bodies().size(), 5);

  Run(
    "CREATE SECRET far (TYPE openai, base_url 'http://example.invalid', "
    "model 'm')");
  Run("SET sdb_ai_allow_insecure_endpoint = true");
  auto far = _conn.Prepare("SELECT ai_generate(NULL, secret_name := 'far')");
  ASSERT_FALSE(far->HasError()) << far->GetError();
  succeed(*far);
  Run("SET sdb_ai_allow_insecure_endpoint = false");
  auto result = execute(*far);
  ASSERT_TRUE(result->HasError());
  EXPECT_NE(result->GetError().find("insecure endpoint"), std::string::npos)
    << result->GetError();
}

TEST_F(AIFunctionsTest, ControlCharactersAreEscaped) {
  auto& mock = Mock(kChat, [](std::string_view) {
    return Reply{200, ChatReply("ok", "stop", 1)};
  });
  Start();
  Run("SELECT ai_generate('a' || chr(1) || 'b', secret_name := 'chat')");
  const auto bodies = mock.Bodies();
  ASSERT_EQ(bodies.size(), 1);
  EXPECT_NE(bodies[0].find(R"("a\u0001b")"), std::string::npos) << bodies[0];
  EXPECT_EQ(Content(bodies[0], 1), std::string_view("a\x01"
                                                    "b",
                                                    3));
}

TEST_F(AIFunctionsTest, CancelWithoutAsyncThreads) {
  Mock(kChat, [](std::string_view) {
    std::this_thread::sleep_for(std::chrono::milliseconds{100});
    return Reply{200, ChatReply("ok", "stop", 1)};
  });
  Start();
  Run("SET threads = 1");
  Run("SET async_threads = 0");
  std::thread cancel{[&] {
    std::this_thread::sleep_for(std::chrono::milliseconds{300});
    _conn.Interrupt();
  }};
  duckdb::unique_ptr<duckdb::MaterializedQueryResult> result;
  const auto elapsed = Timed([&] {
    result = _conn.Query(
      "SELECT ai_generate(range::VARCHAR, secret_name := 'chat') FROM "
      "range(64)");
  });
  cancel.join();
  EXPECT_TRUE(result->HasError());
  EXPECT_LT(elapsed, std::chrono::seconds{5});
  EXPECT_EQ(Run("SELECT 1")->GetValue(0, 0).GetValue<int32_t>(), 1);
}

TEST_F(AIFunctionsTest, PrepareAfterExhaustedQuota) {
  Mock(kEmbeddings, [](std::string_view) {
    return Reply{200, R"({"data":[{"embedding":[1,0]},{"embedding":[1,0]}]})"};
  });
  Start();
  Run("SET sdb_ai_max_api_calls_per_query = 1");
  Run("SELECT ai_similarity('a', 'b', 'm', 'chat')");
  auto prepared = _conn.Prepare("SELECT ai_similarity('c', 'd', 'm', 'chat')");
  ASSERT_FALSE(prepared->HasError()) << prepared->GetError();
  duckdb::vector<duckdb::Value> values;
  auto result = prepared->Execute(values, false);
  ASSERT_FALSE(result->HasError()) << result->GetError();
}

TEST_F(AIFunctionsTest, JevBatchWithBadAnswerFailsWhole) {
  Mock(kSystemOne, [](std::string_view) {
    return Reply{
      200,
      R"({"answers":{"r0":{"noul":0.1},"r1":{"noul":1.5},"r2":{"noul":0.3}}})"};
  });
  Start();
  Run("SET sdb_ai_throw_on_error = false");
  auto result = Run(
    "SELECT ai_system1(body, 'Q?', batch_size := 3, secret_name := 'jev') "
    "FROM (VALUES ('a'), ('b'), ('c')) v(body)");
  ASSERT_EQ(result->RowCount(), 3);
  for (duckdb::idx_t i = 0; i != 3; ++i) {
    EXPECT_TRUE(result->GetValue(0, i).IsNull()) << i;
  }
}

TEST_F(AIFunctionsTest, ClassifyMatchesPunctuatedLabels) {
  Mock(kChat, [](std::string_view body) {
    return Reply{
      200, ChatReply(Content(body, 1) == "x" ? R"({\"category\":\"U.S.\"})"
                                             : R"(\"EU\".)",
                     "stop", 1)};
  });
  Start();
  auto result = Run(
    "SELECT ai_classify('x', ['U.S.', 'EU'], secret_name := 'chat'), "
    "ai_classify('y', ['U.S.', 'EU'], secret_name := 'chat')");
  EXPECT_EQ(result->GetValue(0, 0).ToString(), "U.S.");
  EXPECT_EQ(result->GetValue(1, 0).ToString(), "EU");
}

TEST_F(AIFunctionsTest, ExtractReturnsNonStringResult) {
  Mock(kChat, [](std::string_view) {
    return Reply{200, ChatReply(R"({\"result\":42})", "stop", 1)};
  });
  Start();
  auto result = Run(
    "SELECT ai_extract('x', 'the number', secret_name := "
    "'chat')");
  EXPECT_EQ(result->GetValue(0, 0).ToString(), "42");
}

TEST_F(AIFunctionsTest, DuplicateKeysFail) {
  Start();
  ExpectError(
    R"(SELECT ai_system1('x', questions := '{"q": {"instructions": "a"}, "Q": {"instructions": "b"}}', secret_name := 'jev'))",
    "defined twice");
  ExpectError(
    R"(SELECT ai_extract('x', '{"city": "a", "city": "b"}', secret_name := 'chat'))",
    "defined twice");
}

TEST_F(AIFunctionsTest, FailuresStopTheRound) {
  std::atomic_int status = 401;
  auto& mock = Mock(kChat, [&](std::string_view) {
    return Reply{status.load(), R"({"error":"nope"})"};
  });
  Start();
  Run("SET sdb_ai_max_concurrent_requests = 4");
  constexpr std::string_view kQuery =
    "SELECT count(ai_generate(range::VARCHAR, secret_name := 'chat')) FROM "
    "range(2048)";
  ExpectError(std::string{kQuery}, "returned HTTP 401");
  EXPECT_LE(mock.Bodies().size(), 8);
  status = 500;
  Run("SET sdb_ai_max_retries = 1");
  const auto before = mock.Bodies().size();
  ExpectError(std::string{kQuery}, "returned HTTP 500");
  EXPECT_LE(mock.Bodies().size() - before, 16);
}

}  // namespace
