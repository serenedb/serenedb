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
                   BmJsonArrayExtractIndex)(benchmark::State& state) {
  const std::string& json = GetJson();

  for (auto _ : state) {
    JsonParser parser;
    parser.PrepareJson(json);
    const int64_t index = GetNextIndex();
    simdjson::ondemand::value v;
    auto r = parser.ExtractByIndex(index).get(v);
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
                   BmJsonObjectExtractField)(benchmark::State& state) {
  const std::string& json = GetJson();

  for (auto _ : state) {
    JsonParser parser;
    parser.PrepareJson(json);
    const std::string& field = GetNextField();
    simdjson::ondemand::value v;
    auto r = parser.ExtractByField(field).get(v);
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
                   BmJsonPathExtract)(benchmark::State& state) {
  const std::string& json = GetJson();

  for (auto _ : state) {
    JsonParser parser;
    parser.PrepareJson(json);
    const std::vector<std::string>& path = GetNextPath();
    std::vector<velox::StringView> path_views;
    path_views.reserve(path.size());
    for (const auto& p : path) {
      path_views.emplace_back(p.data(), p.size());
    }
    simdjson::ondemand::value v;
    auto r = parser.Extract(path_views).get(v);
    benchmark::DoNotOptimize(r);
  }
}

}  // namespace
BENCHMARK_REGISTER_F(JsonArrayFixture, BmJsonArrayExtractIndex);
BENCHMARK_REGISTER_F(JsonObjectFixture, BmJsonObjectExtractField);
BENCHMARK_REGISTER_F(JsonPathFixture, BmJsonPathExtract);
BENCHMARK_MAIN();
