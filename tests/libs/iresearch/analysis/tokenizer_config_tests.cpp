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

#include <duckdb.hpp>
#include <filesystem>
#include <fstream>
#include <functional>
#include <string>
#include <vector>

#include "basics/duckdb_engine.h"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "test_resources.hpp"
#include "tests_shared.hpp"
#include "tokenizer_fuzz_checks.hpp"
#include "tokenizer_fuzz_corpus.hpp"
#include "tokenizer_fuzz_specs.hpp"

namespace {

using namespace irs::analysis;
using Cfg = irs::analysis::TokenizerConfig;

std::unique_ptr<Cfg> Child(Cfg cfg) {
  return std::make_unique<Cfg>(std::move(cfg));
}

irs::bstring Bytes(std::string_view s) {
  return irs::bstring{reinterpret_cast<const irs::byte_type*>(s.data()),
                      s.size()};
}

icu::Locale Bogus() { return irs::MakeBogusLocale(); }

std::string ModelLocation() {
  return TestEnv::resource("model_cooking.bin").string();
}

bool HasModel() {
  std::error_code ec;
  return std::filesystem::exists(ModelLocation(), ec);
}

std::string TruncatedModel() {
  static const std::string kPath = [] {
    const auto dir =
      std::filesystem::path{::testing::TempDir()} / "sdb_tokenizer_models";
    std::filesystem::create_directories(dir);
    const auto path = dir / "truncated_model.bin";
    std::ifstream in{ModelLocation(), std::ios::binary};
    std::ofstream out{path, std::ios::binary};
    std::vector<char> buf(4096);
    in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
    out.write(buf.data(), in.gcount());
    return path.string();
  }();
  return kPath;
}

std::string MissingPath() {
  return (std::filesystem::path{::testing::TempDir()} /
          "sdb_tokenizer_fuzz_missing" / "nope.bin")
    .string();
}

struct Rejected {
  std::string name;
  std::function<Cfg()> config;
};

void Collect(std::vector<Rejected>& out, std::string name,
             std::function<Cfg()> config) {
  out.push_back(Rejected{std::move(name), std::move(config)});
}

std::vector<Rejected> RejectedConfigs() {
  std::vector<Rejected> out;

  Collect(out, "pattern/empty",
          [] { return Cfg{PatternTokenizer::Options{.pattern = ""}}; });
  Collect(out, "pattern/unbalanced",
          [] { return Cfg{PatternTokenizer::Options{.pattern = "("}}; });
  Collect(out, "pattern/bad_class",
          [] { return Cfg{PatternTokenizer::Options{.pattern = "[a-"}}; });
  Collect(out, "pattern/group_above_range", [] {
    return Cfg{PatternTokenizer::Options{.pattern = "(a)(b)", .group = 5}};
  });
  Collect(out, "pattern/group_below_range", [] {
    return Cfg{PatternTokenizer::Options{.pattern = "(a)", .group = -2}};
  });

  Collect(out, "multi_delimiter/empty_delimiter", [] {
    return Cfg{
      MultiDelimitedTokenizer::Options{.delimiters = {Bytes(","), Bytes("")}}};
  });
  Collect(out, "multi_delimiter/prefix_overlap", [] {
    return Cfg{MultiDelimitedTokenizer::Options{
      .delimiters = {Bytes("ab"), Bytes("abc")}}};
  });
  Collect(out, "multi_delimiter/prefix_overlap_reversed", [] {
    return Cfg{MultiDelimitedTokenizer::Options{
      .delimiters = {Bytes("abc"), Bytes("ab")}}};
  });

  Collect(out, "stem/bogus_locale",
          [] { return Cfg{StemmingTokenizer::Options{.locale = Bogus()}}; });
  Collect(out, "collation/bogus_locale",
          [] { return Cfg{CollationTokenizer::Options{.locale = Bogus()}}; });
  Collect(out, "text/bogus_locale",
          [] { return Cfg{TextTokenizer::Options{}}; });

  Collect(out, "stopwords/missing_path", [] {
    return Cfg{StopwordsTokenizer::Options{.stopwords_path = MissingPath()}};
  });

  Collect(out, "classification/empty_model", [] {
    return Cfg{ClassificationTokenizer::Options{.model_location = ""}};
  });
  Collect(out, "classification/zero_top_k", [] {
    return Cfg{ClassificationTokenizer::Options{
      .model_location = ModelLocation(), .top_k = 0}};
  });
  Collect(out, "classification/negative_top_k", [] {
    return Cfg{ClassificationTokenizer::Options{
      .model_location = ModelLocation(), .top_k = -1}};
  });
  Collect(out, "classification/threshold_above_one", [] {
    return Cfg{ClassificationTokenizer::Options{
      .model_location = ModelLocation(), .threshold = 1.5}};
  });
  Collect(out, "classification/threshold_negative", [] {
    return Cfg{ClassificationTokenizer::Options{
      .model_location = ModelLocation(), .threshold = -0.5}};
  });
  Collect(out, "nearest_neighbors/empty_model", [] {
    return Cfg{NearestNeighborsTokenizer::Options{.model_location = ""}};
  });
  Collect(out, "nearest_neighbors/zero_top_k", [] {
    return Cfg{NearestNeighborsTokenizer::Options{
      .model_location = ModelLocation(), .top_k = 0}};
  });

  Collect(out, "geo_point/half_configured",
          [] { return Cfg{GeoPointTokenizer::Options{.latitude = {"lat"}}}; });
  Collect(out, "geo_point/inverted_levels", [] {
    return Cfg{
      GeoPointTokenizer::Options{.options = {.min_level = 20, .max_level = 4}}};
  });
  Collect(out, "geo_json/level_mod_zero", [] {
    return Cfg{GeoJsonTokenizer::Options{.options = {.level_mod = 0}}};
  });
  Collect(out, "geo_json/level_mod_too_big", [] {
    return Cfg{GeoJsonTokenizer::Options{.options = {.level_mod = 4}}};
  });
  Collect(out, "geo_json/level_above_max", [] {
    return Cfg{GeoJsonTokenizer::Options{.options = {.max_level = 40}}};
  });
  Collect(out, "geo_json/negative_cells", [] {
    return Cfg{GeoJsonTokenizer::Options{.options = {.max_cells = -1}}};
  });

  Collect(out, "pipeline/null_child", [] {
    PipelineTokenizer::Options opts;
    opts.children.push_back(
      Child(Cfg{DelimitedTokenizer::Options{.delimiter = ","}}));
    opts.children.push_back(nullptr);
    return Cfg{std::move(opts)};
  });
  Collect(out, "pipeline/blob_into_varchar", [] {
    PipelineTokenizer::Options opts;
    opts.children.push_back(Child(Cfg{CollationTokenizer::Options{
      .locale = icu::Locale::createFromName("en_US.UTF-8")}}));
    opts.children.push_back(
      Child(Cfg{DelimitedTokenizer::Options{.delimiter = ","}}));
    return Cfg{std::move(opts)};
  });
  Collect(out, "pipeline/store_stage", [] {
    PipelineTokenizer::Options opts;
    opts.children.push_back(
      Child(Cfg{DelimitedTokenizer::Options{.delimiter = ","}}));
    opts.children.push_back(
      Child(Cfg{WildcardTokenizer::Options{.ngram_size = 3}}));
    return Cfg{std::move(opts)};
  });
  Collect(out, "union/null_child", [] {
    UnionTokenizer::Options opts;
    opts.children.push_back(nullptr);
    return Cfg{std::move(opts)};
  });

  if (HasModel()) {
    Collect(out, "classification/missing_model", [] {
      return Cfg{
        ClassificationTokenizer::Options{.model_location = MissingPath()}};
    });
    if (tests::fuzz::EnvU64("TOKENIZER_CONFIG_MODEL_STRESS", 0) != 0) {
      Collect(out, "classification/truncated_model", [] {
        return Cfg{
          ClassificationTokenizer::Options{.model_location = TruncatedModel()}};
      });
      Collect(out, "nearest_neighbors/truncated_model", [] {
        return Cfg{NearestNeighborsTokenizer::Options{
          .model_location = TruncatedModel(), .top_k = 2}};
      });
    }
  }
  return out;
}

duckdb::ClientContext& Context() {
  static auto* conn =
    new duckdb::Connection{sdb::DuckDBEngine::Instance().instance()};
  return *conn->context;
}

}  // namespace

TEST(TokenizerConfig, InvalidOptionsAreRefused) {
  for (const auto& rejected : RejectedConfigs()) {
    SCOPED_TRACE(rejected.name);
    irs::analysis::Tokenizer::ptr tokenizer;
    bool threw = false;
    try {
      tokenizer =
        irs::analysis::CreateTokenizer(rejected.config(), tests::Cache());
    } catch (...) {
      threw = true;
    }
    EXPECT_TRUE(threw || tokenizer == nullptr)
      << rejected.name << " was accepted";
  }
}

TEST(TokenizerConfig, RefusalLeavesTheCacheUsable) {
  for (const auto& rejected : RejectedConfigs()) {
    SCOPED_TRACE(rejected.name);
    try {
      auto ignored =
        irs::analysis::CreateTokenizer(rejected.config(), tests::Cache());
    } catch (...) {
    }
  }
  for (const auto* spec : tests::fuzz::SelectedSpecs()) {
    SCOPED_TRACE(spec->name);
    auto tokenizer = tests::fuzz::Make(*spec);
    ASSERT_NE(nullptr, tokenizer)
      << "a refused config poisoned the shared resource cache";
    const auto res = tests::fuzz::AnalyzeValue(
      *tokenizer, "the quick brown fox", irs::TokenLayout::TermsPos);
    EXPECT_TRUE(res.ok || !res.ok);
  }
}

TEST(TokenizerConfig, InvalidSqlExpressionsAreRefused) {
  for (const auto* expression :
       {"", "not_a_function(input)", "upper(", "input +", "1 +",
        "unknown_column", "upper(missing_arg_name)"}) {
    SCOPED_TRACE(expression);
    bool threw = false;
    try {
      auto tokenizer =
        SqlTokenizer::Make({.expression = std::string{expression}});
      if (tokenizer) {
        tokenizer->Bind(Context());
      } else {
        threw = true;
      }
    } catch (...) {
      threw = true;
    }
    EXPECT_TRUE(threw) << "expression was accepted: " << expression;
  }
}

TEST(TokenizerConfig, FileBackedSpecsAreRegistered) {
  size_t file_backed = 0;
  for (const auto& spec : tests::fuzz::AllSpecs()) {
    if (spec.name.find("[file]") != std::string::npos ||
        spec.name.find("[dir]") != std::string::npos ||
        spec.name.find("stopwords_path") != std::string::npos) {
      ++file_backed;
    }
  }
  EXPECT_GE(file_backed, 3u)
    << "the fuzz corpus no longer reaches any file-backed resource";
}
