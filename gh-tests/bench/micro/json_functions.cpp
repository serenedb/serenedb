////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <absl/random/random.h>
#include <benchmark/benchmark.h>

#include <cstdio>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "pg/functions/json.h"
#include "velox/type/StringView.h"

using sdb::pg::JsonParser;

namespace {

// Very simple JSON quoting(handles only double quotes) for test data
// generation.
static inline std::string JsonStringLiteral(std::string_view raw) {
  std::string out;
  out.reserve(raw.size() + 16);
  out.push_back('"');
  for (unsigned char c : raw) {
    switch (c) {
      case '"':
        out += "\\\"";
        break;
      default:
        out.push_back(static_cast<char>(c));
    }
  }
  out.push_back('"');
  return out;
}

constexpr std::string_view kSmallJson =
  R"({"k1": "v1", "k2": 42, "k3": [1, 2, 3]})";

std::string MakeArray(int size) {
  std::string json = "[";
  for (int i = 0; i < size; ++i) {
    if (i > 0) {
      json += ",";
    }
    json += kSmallJson;
  }
  json += "]";
  return json;
}

constexpr int kNumJsonSamples = 10'000;

struct JsonFixture {
  JsonFixture() = default;
  const std::string& GetJson() const { return json; }
  std::string json;
};

class JsonArrayFixture : public benchmark::Fixture, public JsonFixture {
 public:
  JsonArrayFixture() {
    absl::BitGen bitgen;
    json = JsonStringLiteral(MakeArray(kNumJsonSamples));
    _indexes.reserve(kNumJsonSamples);
    for (int i = 0; i < kNumJsonSamples; ++i) {
      _indexes.push_back(
        absl::Uniform(bitgen, -kNumJsonSamples, kNumJsonSamples));
    }
  }

  int GetNextIndex() {
    if (_current_index >= _indexes.size()) {
      _current_index = 0;
    }
    return _indexes[_current_index++];
  }

 private:
  std::vector<int> _indexes;
  size_t _current_index = 0;
};

BENCHMARK_DEFINE_F(JsonArrayFixture,
                   BmJsonArrayExtractIndexText)(benchmark::State& state) {
  const std::string& json = GetJson();

  for (auto _ : state) {
    JsonParser parser;
    parser.PrepareJson(json);
    const int64_t index = GetNextIndex();
    auto r = parser.ExtractByIndex<JsonParser::OutputType::TEXT>(index);
    benchmark::DoNotOptimize(r);
  }
}

BENCHMARK_DEFINE_F(JsonArrayFixture,
                   BmJsonArrayExtractIndex)(benchmark::State& state) {
  const std::string& json = GetJson();

  for (auto _ : state) {
    JsonParser parser;
    parser.PrepareJson(json);
    const int64_t index = GetNextIndex();
    auto r = parser.ExtractByIndex<JsonParser::OutputType::JSON>(index);
    benchmark::DoNotOptimize(r);
  }
}

class JsonObjectFixture : public benchmark::Fixture, public JsonFixture {
 public:
  JsonObjectFixture() {
    absl::BitGen bitgen;
    _fields.reserve(kNumJsonSamples);
    json = "{";
    for (int i = 0; i < kNumJsonSamples; ++i) {
      if (i > 0) {
        json += ",";
      }
      int index = absl::Uniform(bitgen, 1, kNumJsonSamples);
      std::string key = "key" + absl::StrCat(index);
      json += JsonStringLiteral(key) + ": " + kSmallJson;
      _fields.push_back(key);
    }
    json += "}";
    json = JsonStringLiteral(json);
    absl::c_shuffle(_fields, bitgen);
  }

  const std::string& GetNextField() {
    if (_current_field >= _fields.size()) {
      _current_field = 0;
    }
    return _fields[_current_field++];
  }

 private:
  std::vector<std::string> _fields;
  size_t _current_field = 0;
};

BENCHMARK_DEFINE_F(JsonObjectFixture,
                   BmJsonObjectExtractFieldText)(benchmark::State& state) {
  const std::string& json = GetJson();

  for (auto _ : state) {
    JsonParser parser;
    parser.PrepareJson(json);
    const std::string& field = GetNextField();
    auto r = parser.ExtractByField<JsonParser::OutputType::TEXT>(field);
    benchmark::DoNotOptimize(r);
  }
}

BENCHMARK_DEFINE_F(JsonObjectFixture,
                   BmJsonObjectExtractField)(benchmark::State& state) {
  const std::string& json = GetJson();

  for (auto _ : state) {
    JsonParser parser;
    parser.PrepareJson(json);
    const std::string& field = GetNextField();
    auto r = parser.ExtractByField<JsonParser::OutputType::JSON>(field);
    benchmark::DoNotOptimize(r);
  }
}

class JsonPathFixture : public benchmark::Fixture, public JsonFixture {
 public:
  JsonPathFixture() {
    absl::BitGen bitgen;
    json = JsonStringLiteral(MakeArray(kNumJsonSamples));
    _paths.reserve(kNumJsonSamples);
    for (int i = 0; i < kNumJsonSamples; ++i) {
      int index = absl::Uniform(bitgen, -kNumJsonSamples, kNumJsonSamples);
      _paths.emplace_back(std::vector<std::string>{
        absl::StrCat(index),
        absl::StrCat("k", i % 3 + 1),
      });
    }
  }

  const std::vector<std::string>& GetNextPath() {
    if (_current_path >= _paths.size()) {
      _current_path = 0;
    }
    return _paths[_current_path++];
  }

 private:
  std::vector<std::vector<std::string>> _paths;
  size_t _current_path = 0;
};

BENCHMARK_DEFINE_F(JsonPathFixture,
                   BmJsonPathExtractText)(benchmark::State& state) {
  const std::string& json = GetJson();

  for (auto _ : state) {
    JsonParser parser;
    parser.PrepareJson(json);
    const std::vector<std::string>& path = GetNextPath();
    std::vector<std::optional<velox::StringView>> path_views;
    path_views.reserve(path.size());
    for (const auto& p : path) {
      path_views.emplace_back(
        std::make_optional<velox::StringView>(p.data(), p.size()));
    }
    auto r = parser.Extract<JsonParser::OutputType::TEXT>(path_views);
    benchmark::DoNotOptimize(r);
  }
}

BENCHMARK_DEFINE_F(JsonPathFixture,
                   BmJsonPathExtract)(benchmark::State& state) {
  const std::string& json = GetJson();

  for (auto _ : state) {
    JsonParser parser;
    parser.PrepareJson(json);
    const std::vector<std::string>& path = GetNextPath();
    std::vector<std::optional<velox::StringView>> path_views;
    path_views.reserve(path.size());
    for (const auto& p : path) {
      path_views.emplace_back(
        std::make_optional<velox::StringView>(p.data(), p.size()));
    }
    auto r = parser.Extract<JsonParser::OutputType::JSON>(path_views);
    benchmark::DoNotOptimize(r);
  }
}

}  // namespace
BENCHMARK_REGISTER_F(JsonArrayFixture, BmJsonArrayExtractIndexText);
BENCHMARK_REGISTER_F(JsonObjectFixture, BmJsonObjectExtractFieldText);
BENCHMARK_REGISTER_F(JsonPathFixture, BmJsonPathExtractText);
BENCHMARK_REGISTER_F(JsonArrayFixture, BmJsonArrayExtractIndex);
BENCHMARK_REGISTER_F(JsonObjectFixture, BmJsonObjectExtractField);
BENCHMARK_REGISTER_F(JsonPathFixture, BmJsonPathExtract);
BENCHMARK_MAIN();
