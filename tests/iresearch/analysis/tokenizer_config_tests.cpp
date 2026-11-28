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
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <string>
#include <vector>

#include "bound_config.hpp"
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

duckdb::text::Locale Bogus() { return duckdb::text::Locale{}; }

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

}  // namespace

TEST(TokenizerConfig, InvalidOptionsAreRefused) {
  for (const auto& rejected : RejectedConfigs()) {
    SCOPED_TRACE(rejected.name);
    irs::analysis::Tokenizer::ptr tokenizer;
    bool threw = false;
    try {
      tokenizer = tests::MakeBound(rejected.config());
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
      auto ignored = tests::MakeBound(rejected.config());
    } catch (...) {
    }
  }
  for (const auto* spec : tests::fuzz::SelectedSpecs()) {
    SCOPED_TRACE(spec->name);
    auto tokenizer = tests::fuzz::Make(*spec, tests::EngineContext());
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
    EXPECT_THROW(
      tests::Bound(Cfg{SqlTokenizer::Options{.expression = expression}}),
      irs::SqlException)
      << "expression was accepted: " << expression;
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

namespace {

std::string Pack(const Cfg& cfg) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer{stream};
  irs::utils::WriteTuple(serializer, cfg);
  return std::string{reinterpret_cast<const char*>(stream.GetData()),
                     stream.GetPosition()};
}

Cfg Unpack(const std::string& bytes) {
  duckdb::MemoryStream stream{
    const_cast<duckdb::data_ptr_t>(
      reinterpret_cast<duckdb::const_data_ptr_t>(bytes.data())),
    bytes.size()};
  duckdb::BinaryDeserializer deserializer{stream};
  Cfg cfg;
  irs::utils::ReadTuple(deserializer, cfg);
  EXPECT_EQ(stream.GetPosition(), bytes.size());
  return cfg;
}

}  // namespace

TEST(TokenizerConfig, EverySpecRoundTripsThroughTheBinaryFormat) {
  size_t round_tripped = 0;
  for (const auto& spec : tests::fuzz::AllSpecs()) {
    SCOPED_TRACE(spec.name);
    std::string bytes;
    try {
      bytes = Pack(spec.config());
    } catch (const std::exception& e) {
      EXPECT_NE(std::string_view{e.what()}.find("for Accept"),
                std::string_view::npos)
        << e.what();
      continue;
    }
    EXPECT_EQ(Pack(Unpack(bytes)), bytes);
    ++round_tripped;
  }
  EXPECT_GT(round_tripped, tests::fuzz::AllSpecs().size() / 2);
}

TEST(TokenizerConfig, EveryBoundSpecRoundTripsThroughTheBinaryFormat) {
  size_t round_tripped = 0;
  for (const auto& spec : tests::fuzz::AllSpecs()) {
    SCOPED_TRACE(spec.name);
    std::string bytes;
    try {
      bytes =
        Pack(tests::Bound(spec.config(), duckdb::LogicalType{spec.input}));
    } catch (const std::exception& e) {
      EXPECT_NE(std::string_view{e.what()}.find("for Accept"),
                std::string_view::npos)
        << e.what();
      continue;
    }
    EXPECT_EQ(Pack(Unpack(bytes)), bytes);
    ++round_tripped;
  }
  EXPECT_GT(round_tripped, tests::fuzz::AllSpecs().size() / 2);
}

TEST(TokenizerConfig, ATextTemplateKeepsTheTypeItIsGiven) {
  const Cfg keyword{irs::KeywordTokenizer::Options{}};
  EXPECT_EQ(duckdb::LogicalTypeId::VARCHAR, tests::Bound(keyword).terms);
  EXPECT_EQ(duckdb::LogicalTypeId::BLOB,
            tests::Bound(keyword, duckdb::LogicalType::BLOB).terms);
  EXPECT_THROW(tests::Bound(keyword, duckdb::LogicalType::INTEGER),
               irs::SqlException);

  const Cfg shingle{ShingleTokenizer::Options{}};
  EXPECT_EQ(duckdb::LogicalTypeId::VARCHAR, tests::Bound(shingle).terms);
  EXPECT_EQ(duckdb::LogicalTypeId::BLOB,
            tests::Bound(shingle, duckdb::LogicalType::BLOB).terms);
}

TEST(TokenizerConfig, APipelineThreadsTheTermTypeThroughItsStages) {
  const auto bytes = [] {
    return Cfg{NGramTokenizer::Options{
      .min_gram = 2,
      .max_gram = 2,
      .preserve_original = false,
      .stream_bytes_type = NGramTokenizer::InputType::Binary}};
  };
  const auto keyword = [] { return Cfg{irs::KeywordTokenizer::Options{}}; };
  const auto text = [] { return Cfg{TextTokenizer::Options{}}; };
  const auto pipeline = [](auto... stages) {
    PipelineTokenizer::Options opts;
    (opts.children.push_back(Child(stages())), ...);
    return Cfg{std::move(opts)};
  };
  for (const auto& cfg : {pipeline(bytes, keyword), pipeline(keyword, bytes),
                          pipeline(bytes, text)}) {
    const auto bound = tests::Bound(cfg);
    EXPECT_EQ(duckdb::LogicalTypeId::BLOB, bound.terms);
    const auto& stages = std::get<PipelineTokenizer::Options>(bound.config);
    for (size_t i = 1; i < stages.children.size(); ++i) {
      EXPECT_EQ(stages.children[i - 1]->terms, stages.children[i]->input);
    }
    EXPECT_NE(nullptr, tests::MakeBound(cfg));
  }
  EXPECT_EQ(duckdb::LogicalTypeId::VARCHAR,
            tests::Bound(pipeline(keyword, text)).terms);
}

TEST(TokenizerConfig, AUnionTakesTheTypeItsMembersShare) {
  const auto members = [](Cfg a, Cfg b) {
    UnionTokenizer::Options opts;
    opts.children.push_back(Child(std::move(a)));
    opts.children.push_back(Child(std::move(b)));
    return Cfg{std::move(opts)};
  };
  EXPECT_EQ(duckdb::LogicalTypeId::VARCHAR,
            tests::Bound(members(Cfg{irs::KeywordTokenizer::Options{}},
                                 Cfg{TextTokenizer::Options{}}))
              .terms);
  EXPECT_EQ(duckdb::LogicalTypeId::BLOB,
            tests::Bound(members(Cfg{irs::KeywordTokenizer::Options{}},
                                 Cfg{CollationTokenizer::Options{}}))
              .terms);
}

TEST(TokenizerConfig, AnSqlStageBindsItsExpressionToItsInput) {
  const Cfg identity{SqlTokenizer::Options{.expression = "input"}};
  EXPECT_EQ(duckdb::LogicalTypeId::VARCHAR, tests::Bound(identity).terms);
  EXPECT_EQ(duckdb::LogicalTypeId::BLOB,
            tests::Bound(identity, duckdb::LogicalType::BLOB).terms);
  const Cfg hex{SqlTokenizer::Options{.expression = "hex(input)"}};
  EXPECT_EQ(duckdb::LogicalTypeId::VARCHAR,
            tests::Bound(hex, duckdb::LogicalType::BLOB).terms);
  const Cfg upper{SqlTokenizer::Options{.expression = "upper(input)"}};
  EXPECT_THROW(tests::Bound(upper, duckdb::LogicalType::BLOB),
               irs::SqlException);
}

TEST(TokenizerConfig, GeoTakesJsonTextOrGeometry) {
  const auto wkb = [](const Cfg& cfg, const duckdb::LogicalType& input) {
    return std::get<GeoJsonTokenizer::Options>(tests::Bound(cfg, input).config)
      .wkb;
  };
  const Cfg geojson{GeoJsonTokenizer::Options{}};
  EXPECT_FALSE(wkb(geojson, duckdb::LogicalType::VARCHAR));
  EXPECT_TRUE(wkb(geojson, duckdb::LogicalType::GEOMETRY()));
  EXPECT_THROW(tests::Bound(geojson, duckdb::LogicalType::BLOB),
               irs::SqlException);
  const Cfg latlng{
    GeoJsonTokenizer::Options{.coding = GeoJsonTokenizer::Coding::S2LatLngF64}};
  EXPECT_THROW(tests::Bound(latlng, duckdb::LogicalType::GEOMETRY()),
               irs::SqlException);
}

TEST(TokenizerConfig, AWildcardCutsByteGramsForBytes) {
  const auto bytes = [](const duckdb::LogicalType& input) {
    return std::get<WildcardTokenizer::Options>(
             tests::Bound(Cfg{WildcardTokenizer::Options{.ngram_size = 3}},
                          input)
               .config)
      .bytes;
  };
  EXPECT_FALSE(bytes(duckdb::LogicalType::VARCHAR));
  EXPECT_TRUE(bytes(duckdb::LogicalType::BLOB));
}

TEST(TokenizerConfig, APatternMatchingSingleBytesHasByteTerms) {
  const auto terms = [](std::string pattern, int group) {
    return tests::Bound(Cfg{PatternTokenizer::Options{
                          .pattern = std::move(pattern), .group = group}})
      .terms;
  };
  EXPECT_EQ(duckdb::LogicalTypeId::VARCHAR, terms("\\s+", -1));
  EXPECT_EQ(duckdb::LogicalTypeId::VARCHAR, terms("\\\\C", -1));
  EXPECT_EQ(duckdb::LogicalTypeId::BLOB, terms("\\C", 0));
  EXPECT_EQ(duckdb::LogicalTypeId::BLOB, terms("a|(x\\C)+", 1));
  EXPECT_EQ(duckdb::LogicalTypeId::BLOB, terms("[ ,]\\C?", -1));
}

TEST(TokenizerConfig, OptionsAtTheirDefaultAreNotStored) {
  const auto plain =
    Pack(Cfg{NGramTokenizer::Options{.min_gram = 2, .max_gram = 3}});
  const auto prefix = Pack(Cfg{NGramTokenizer::Options{
    .min_gram = 2,
    .max_gram = 3,
    .ngram_mode = NGramTokenizer::NGramMode::Prefix,
  }});
  EXPECT_LT(plain.size(), prefix.size());

  const auto read = Unpack(plain);
  const auto& options = std::get<NGramTokenizer::Options>(read.config);
  EXPECT_EQ(options.min_gram, 2u);
  EXPECT_EQ(options.max_gram, 3u);
  EXPECT_TRUE(options.preserve_original);
  EXPECT_EQ(options.ngram_mode, NGramTokenizer::NGramMode::All);
  EXPECT_EQ(std::get<NGramTokenizer::Options>(Unpack(prefix).config).ngram_mode,
            NGramTokenizer::NGramMode::Prefix);
}
