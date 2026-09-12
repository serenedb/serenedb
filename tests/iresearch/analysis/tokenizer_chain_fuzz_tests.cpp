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

#include <algorithm>
#include <duckdb.hpp>
#include <iresearch/analysis/pipeline_tokenizer.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <iresearch/analysis/union_tokenizer.hpp>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <vector>

#include "test_resources.hpp"
#include "tests_shared.hpp"
#include "tokenizer_fuzz_checks.hpp"
#include "tokenizer_fuzz_corpus.hpp"
#include "tokenizer_fuzz_mutator.hpp"
#include "tokenizer_fuzz_specs.hpp"

namespace {

using namespace tests::fuzz;
using irs::analysis::PipelineTokenizer;
using irs::analysis::Tokenizer;
using irs::analysis::TokenizerConfig;
using irs::analysis::UnionTokenizer;

duckdb::ClientContext& Context() {
  static duckdb::Connection conn{tests::Database()};
  return *conn.context;
}

Tokenizer::ptr Build(TokenizerConfig config) {
  auto tokenizer =
    irs::analysis::CreateTokenizer(std::move(config), tests::Cache());
  if (tokenizer) {
    tokenizer->Bind(Context());
  }
  return tokenizer;
}

std::unique_ptr<TokenizerConfig> Child(const Spec& spec) {
  return std::make_unique<TokenizerConfig>(spec.config());
}

TokenizerConfig Chain(std::span<const Spec* const> stages) {
  PipelineTokenizer::Options opts;
  for (const auto* spec : stages) {
    opts.children.push_back(Child(*spec));
  }
  return TokenizerConfig{std::move(opts)};
}

TokenizerConfig Branches(std::span<const Spec* const> members) {
  UnionTokenizer::Options opts;
  for (const auto* spec : members) {
    opts.children.push_back(Child(*spec));
  }
  return TokenizerConfig{std::move(opts)};
}

std::optional<std::vector<std::string>> Terms(Tokenizer& tokenizer,
                                              std::string_view value) {
  auto res = AnalyzeValue(tokenizer, value, irs::TokenLayout::Terms);
  if (!res.ok) {
    return std::nullopt;
  }
  std::vector<std::string> out;
  out.reserve(res.tokens.size());
  for (auto& token : res.tokens) {
    out.push_back(std::move(token.term));
  }
  return out;
}

std::optional<std::vector<std::string>> Sequential(
  std::span<const Tokenizer::ptr> stages, std::string_view value) {
  std::vector<std::string> tokens{std::string{value}};
  for (const auto& stage : stages) {
    std::vector<std::string> next;
    for (const auto& token : tokens) {
      auto terms = Terms(*stage, token);
      if (!terms) {
        return std::nullopt;
      }
      next.insert(next.end(), std::make_move_iterator(terms->begin()),
                  std::make_move_iterator(terms->end()));
    }
    tokens = std::move(next);
  }
  return tokens;
}

bool Chainable(const Spec& spec) {
  if (spec.model != Model::None || spec.setup) {
    return false;
  }
  auto tokenizer = Make(spec);
  if (!tokenizer) {
    return false;
  }
  const auto traits = tokenizer->Traits();
  if (traits.input != duckdb::LogicalTypeId::VARCHAR ||
      traits.output != duckdb::LogicalTypeId::VARCHAR || traits.store) {
    return false;
  }
  auto probes = SpecCorpus(spec, Seed(), 4);
  probes.insert(probes.end(),
                {"ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz",
                 "The Quick, Brown Fox! Jumps; over/the/lazy/dog"});
  for (const auto& value : probes) {
    if (!IsValidUtf8(value)) {
      continue;
    }
    const auto terms = Terms(*tokenizer, value);
    if (!terms) {
      continue;
    }
    const bool ascii = IsAscii(value);
    for (const auto& term : *terms) {
      if (!IsValidUtf8(term) || (ascii && !IsAscii(term))) {
        return false;
      }
    }
  }
  return true;
}

const std::vector<const Spec*>& Stages() {
  static const std::vector<const Spec*> kStages = [] {
    std::vector<const Spec*> out;
    for (const auto* spec : SelectedSpecs()) {
      if (Chainable(*spec)) {
        out.push_back(spec);
      }
    }
    return out;
  }();
  return kStages;
}

std::string Describe(std::span<const Spec* const> stages, std::string_view op) {
  std::string out;
  for (const auto* spec : stages) {
    if (!out.empty()) {
      out += op;
    }
    out += spec->name;
  }
  return out;
}

std::vector<std::string> Values(std::span<const Spec* const> stages,
                                uint64_t seed, size_t random_count) {
  std::vector<std::string> values;
  for (const auto* spec : stages) {
    auto more = SpecCorpus(*spec, seed, random_count);
    values.insert(values.end(), std::make_move_iterator(more.begin()),
                  std::make_move_iterator(more.end()));
  }
  std::sort(values.begin(), values.end());
  values.erase(std::unique(values.begin(), values.end()), values.end());
  return values;
}

std::vector<const Spec*> Pick(std::mt19937_64& rng, size_t count) {
  const auto& pool = Stages();
  std::vector<const Spec*> out;
  std::uniform_int_distribution<size_t> dist{0, pool.size() - 1};
  for (size_t i = 0; i < count; ++i) {
    out.push_back(pool[dist(rng)]);
  }
  return out;
}

}  // namespace

TEST(TokenizerChainFuzz, StagePoolIsNotEmpty) {
  ASSERT_GE(Stages().size(), 8u);
}

TEST(TokenizerChainFuzz, PipelineEqualsSequentialApplication) {
  const auto iters = static_cast<size_t>(EnvU64("TOKENIZER_CHAIN_ITERS", 200));
  std::mt19937_64 rng{Seed()};
  std::uniform_int_distribution<size_t> length{1, 4};
  size_t compared = 0;
  for (size_t iter = 0; iter < iters; ++iter) {
    const auto stages = Pick(rng, length(rng));
    SCOPED_TRACE(Describe(stages, " | "));
    auto chain = Build(Chain(stages));
    ASSERT_NE(nullptr, chain);
    std::vector<Tokenizer::ptr> oracle;
    for (const auto* spec : stages) {
      oracle.push_back(Make(*spec));
      ASSERT_NE(nullptr, oracle.back());
    }
    for (const auto& value : Values(stages, Seed() + iter, 4)) {
      SCOPED_TRACE(value);
      const auto expected = Sequential(oracle, value);
      const auto actual = Terms(*chain, value);
      if (!expected) {
        continue;
      }
      ASSERT_TRUE(actual.has_value());
      EXPECT_EQ(*expected, *actual);
      ++compared;
    }
  }
  EXPECT_GT(compared, 0u);
}

TEST(TokenizerChainFuzz, NestedPipelineEqualsFlatPipeline) {
  const auto iters = static_cast<size_t>(EnvU64("TOKENIZER_CHAIN_ITERS", 200));
  std::mt19937_64 rng{Seed() ^ 0x9E3779B97F4A7C15ull};
  std::uniform_int_distribution<size_t> length{2, 4};
  for (size_t iter = 0; iter < iters; ++iter) {
    const auto stages = Pick(rng, length(rng));
    SCOPED_TRACE(Describe(stages, " | "));
    std::uniform_int_distribution<size_t> cut{1, stages.size() - 1};
    const auto split = cut(rng);
    PipelineTokenizer::Options nested;
    nested.children.push_back(
      std::make_unique<TokenizerConfig>(Chain(std::span{stages}.first(split))));
    for (const auto* spec : std::span{stages}.subspan(split)) {
      nested.children.push_back(Child(*spec));
    }
    auto flat = Build(Chain(stages));
    auto grouped = Build(TokenizerConfig{std::move(nested)});
    ASSERT_NE(nullptr, flat);
    ASSERT_NE(nullptr, grouped);
    for (const auto& value : Values(stages, Seed() + iter, 2)) {
      SCOPED_TRACE(value);
      EXPECT_EQ(Terms(*flat, value), Terms(*grouped, value));
    }
  }
}

TEST(TokenizerChainFuzz, SingleStagePipelineIsTheStage) {
  for (const auto* spec : Stages()) {
    SCOPED_TRACE(spec->name);
    const Spec* stages[] = {spec};
    auto chain = Build(Chain(stages));
    auto alone = Make(*spec);
    ASSERT_NE(nullptr, chain);
    ASSERT_NE(nullptr, alone);
    EXPECT_EQ(alone->type(), chain->type());
    for (const auto& value : SpecCorpus(*spec, Seed(), 2)) {
      SCOPED_TRACE(value);
      EXPECT_EQ(Terms(*alone, value), Terms(*chain, value));
    }
  }
}

TEST(TokenizerChainFuzz, UnionMergesEveryBranch) {
  const auto iters = static_cast<size_t>(EnvU64("TOKENIZER_CHAIN_ITERS", 200));
  std::mt19937_64 rng{Seed() ^ 0xD1B54A32D192ED03ull};
  std::uniform_int_distribution<size_t> width{2, 3};
  for (size_t iter = 0; iter < iters; ++iter) {
    const auto members = Pick(rng, width(rng));
    SCOPED_TRACE(Describe(members, " , "));
    auto merged = Build(Branches(members));
    ASSERT_NE(nullptr, merged);
    std::vector<Tokenizer::ptr> branches;
    for (const auto* spec : members) {
      branches.push_back(Make(*spec));
      ASSERT_NE(nullptr, branches.back());
    }
    for (const auto& value : Values(members, Seed() + iter, 2)) {
      SCOPED_TRACE(value);
      std::vector<std::string> expected;
      bool rejected = false;
      for (const auto& branch : branches) {
        auto terms = Terms(*branch, value);
        if (!terms) {
          rejected = true;
          break;
        }
        expected.insert(expected.end(), terms->begin(), terms->end());
      }
      auto actual = Terms(*merged, value);
      if (rejected) {
        continue;
      }
      ASSERT_TRUE(actual.has_value());
      std::sort(expected.begin(), expected.end());
      std::sort(actual->begin(), actual->end());
      EXPECT_EQ(expected, *actual);
    }
  }
}

TEST(TokenizerChainFuzz, ChainSurvivesPoison) {
  std::mt19937_64 rng{Seed() ^ 0xA0761D6478BD642Full};
  std::uniform_int_distribution<size_t> length{1, 4};
  std::vector<std::string> poison = {
    "\xFF\xFE\xFD",
    "\xC3",
    "\xED\xA0\x80",
    std::string(4096, '\xC3'),
    std::string(3000, ','),
    std::string(64, '\0'),
    "",
  };
  for (const auto& bad : Utf8Adversary()) {
    poison.push_back(bad);
  }
  for (size_t iter = 0; iter < 64; ++iter) {
    const auto stages = Pick(rng, length(rng));
    SCOPED_TRACE(Describe(stages, " | "));
    auto chain = Build(Chain(stages));
    ASSERT_NE(nullptr, chain);
    for (const auto& value : poison) {
      const auto terms = Terms(*chain, value);
      if (!terms) {
        continue;
      }
      for (const auto& term : *terms) {
        EXPECT_LE(term.size(), value.size() + 64);
      }
    }
  }
}
