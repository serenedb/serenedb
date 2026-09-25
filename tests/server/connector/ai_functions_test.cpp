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

#include <absl/strings/str_cat.h>
#include <simdjson.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <duckdb.hpp>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
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
    Reply reply;
    {
      std::lock_guard lock{_mutex};
      reply = _script(body);
      _bodies.push_back(std::move(body));
    }
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
    const auto url = absl::StrCat("http://127.0.0.1:", _harness->server.port());
    Run(absl::StrCat("CREATE SECRET chat (TYPE openai, base_url '", url,
                     "', model 'm')"));
    Run(
      absl::StrCat("CREATE SECRET jev (TYPE typesafe, base_url '", url, "')"));
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
  auto result = Run("SELECT ai_generate('x', secret_name := 'chat')");
  EXPECT_EQ(result->GetValue(0, 0).ToString(), "positi");
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
            "json_object");
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
    "r.probabilities[2].value = 'sales') FROM (SELECT prompt_jev(body, "
    "'Which team?', choice := [{label: 'billing', description: 'Invoices'}, "
    "{label: 'sales', description: NULL}], batch_size := 3, secret_name := "
    "'jev') AS r FROM (VALUES ('a'), ('b'), ('c')) v(body)) sub");
  EXPECT_EQ(result->GetValue(0, 0).GetValue<int64_t>(), 3);

  const auto bodies = mock.Bodies();
  ASSERT_EQ(bodies.size(), 3);
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

  simdjson::dom::parser single_parser;
  auto single = Parse(single_parser, bodies[1]);
  EXPECT_EQ(std::string_view{single["state"]}, "a");
  EXPECT_EQ(std::string_view{single["questions"]["answer"]["instructions"]},
            "Which team?");

  simdjson::dom::parser pair_parser;
  EXPECT_EQ(Parse(pair_parser, bodies[2])["state"].get_object().size(), 2);
}

TEST_F(AIFunctionsTest, JevUnprocessableRowFailsQuery) {
  Mock(kSystemOne, [](std::string_view) {
    return Reply{422, R"({"detail":"bad question"})"};
  });
  Start();
  Run("SET sdb_ai_throw_on_error = false");
  ExpectError(
    "SELECT prompt_jev('x', questions := {a: {type: 'noul', instructions: "
    "'q'}}, secret_name := 'jev')",
    "returned HTTP 422");
}

}  // namespace
