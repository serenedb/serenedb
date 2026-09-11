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
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <variant>
#include <vector>

#include "iresearch/analysis/tokenizer_config.hpp"
#include "tests_shared.hpp"
#include "tokenizer_fuzz_checks.hpp"
#include "tokenizer_fuzz_corpus.hpp"
#include "tokenizer_fuzz_mutator.hpp"
#include "tokenizer_fuzz_specs.hpp"

namespace {

using namespace tests::fuzz;
using Clock = std::chrono::steady_clock;

constexpr size_t kMemoryCap = size_t{256} << 20;

uint64_t NameSeed(const std::string& name) {
  uint64_t h = 0xCBF29CE484222325ull;
  for (const char c : name) {
    h = (h ^ static_cast<unsigned char>(c)) * 0x100000001B3ull;
  }
  return h;
}

double Seconds(Clock::time_point from) {
  return std::chrono::duration<double>(Clock::now() - from).count();
}

bool OutOfTime(Clock::time_point start, double budget) {
  return budget > 0.0 && Seconds(start) >= budget;
}

const std::vector<std::string>& PoisonValues() {
  static const std::vector<std::string> kValues = [] {
    std::vector<std::string> v = {
      "\xFF\xFE\xFD",
      "\xC3",
      "\xED\xA0\x80",
      "{oops",
      "not-wkb",
      "\"open",
      std::string(4096, '\xC3'),
      std::string(3000, ','),
    };
    v.emplace_back("\0\0\0\0", 4);
    std::string binary;
    for (size_t i = 0; i < 512; ++i) {
      binary.push_back(static_cast<char>(i * 37 % 256));
    }
    v.push_back(std::move(binary));
    for (const auto& bad : Utf8Adversary()) {
      v.push_back(bad);
    }
    return v;
  }();
  return kValues;
}

}  // namespace

TEST(TokenizerFuzz, SpecsCoverEveryTokenizerKind) {
  using Config = decltype(irs::analysis::TokenizerConfig::config);
  constexpr auto kKinds = std::variant_size_v<Config>;
  std::vector<bool> seen(kKinds, false);
  for (const auto& spec : AllSpecs()) {
    SCOPED_TRACE(spec.name);
    const auto cfg = spec.config();
    ASSERT_LT(cfg.config.index(), kKinds);
    seen[cfg.config.index()] = true;
  }
  std::vector<size_t> optional;
  if (!ModelsAvailable()) {
    optional.push_back(
      Config{irs::analysis::ClassificationTokenizer::Options{}}.index());
    optional.push_back(
      Config{irs::analysis::NearestNeighborsTokenizer::Options{}}.index());
  }
  for (size_t i = 0; i < kKinds; ++i) {
    if (std::find(optional.begin(), optional.end(), i) != optional.end()) {
      continue;
    }
    EXPECT_TRUE(seen[i]) << "TokenizerConfig alternative " << i
                         << " has no fuzz spec";
  }
}

TEST(TokenizerFuzz, EverySpecConstructsAndDeclaresSaneTraits) {
  for (const auto* spec : SelectedSpecs()) {
    SCOPED_TRACE(spec->name);
    auto tokenizer = Make(*spec);
    ASSERT_NE(nullptr, tokenizer);

    const auto traits = tokenizer->Traits();
    const auto again = tokenizer->Traits();
    EXPECT_EQ(traits.input, again.input);
    EXPECT_EQ(traits.output, again.output);
    EXPECT_EQ(traits.unique, again.unique);
    EXPECT_EQ(traits.keyword, again.keyword);
    EXPECT_EQ(traits.explicit_pos, again.explicit_pos);
    EXPECT_EQ(traits.offsets, again.offsets);
    EXPECT_EQ(traits.store, again.store);
    EXPECT_EQ(traits.stable, again.stable);
    EXPECT_TRUE(!traits.keyword || traits.unique)
      << "keyword traits imply unique";
    EXPECT_TRUE(traits.output == duckdb::LogicalTypeId::VARCHAR ||
                traits.output == duckdb::LogicalTypeId::BLOB)
      << "unexpected output type";
    EXPECT_EQ(duckdb::LogicalTypeId::VARCHAR, traits.input);

    const auto wanted = tokenizer->WantedBlockTraits();
    EXPECT_EQ(wanted.ascii, tokenizer->WantedBlockTraits().ascii);
    EXPECT_GE(kMemoryCap, tokenizer->MemoryUsage());
  }
}

TEST(TokenizerFuzz, EdgeCases) {
  for (const auto* spec : SelectedSpecs()) {
    SCOPED_TRACE(spec->name);
    const auto values = SpecCorpus(*spec, Seed(), 0);
    ASSERT_NO_FATAL_FAILURE(CheckSpec(*spec, values));
  }
}

TEST(TokenizerFuzz, Mutation) {
  const auto iters = static_cast<size_t>(EnvU64("TOKENIZER_FUZZ_ITERS", 4096));
  const auto seconds = static_cast<double>(EnvU64("TOKENIZER_FUZZ_SECONDS", 0));
  const auto seed = Seed();
  const auto start = Clock::now();

  size_t total_inputs = 0;
  size_t total_classes = 0;
  size_t total_bytes = 0;

  for (const auto* spec : SelectedSpecs()) {
    SCOPED_TRACE(testing::Message() << spec->name << " seed=" << seed);
    Probe probe{*spec};
    ASSERT_TRUE(probe.valid()) << spec->name;

    Mutator mutator{seed ^ NameSeed(spec->name), spec->dict, SizeCap(*spec)};
    FeedbackCorpus corpus;
    for (const auto& v : SpecCorpus(*spec, seed, 0)) {
      corpus.Seed(v);
    }

    const auto budget = ValueBudget(*spec, iters);
    for (size_t i = 0; i < budget; ++i) {
      if (OutOfTime(start, seconds)) {
        break;
      }
      std::string input;
      const auto roll = i % 16;
      if (roll == 0) {
        input = mutator.Generate();
      } else if (roll % 5 == 0) {
        const auto a = std::string{corpus.Pick(mutator)};
        input = mutator.Splice(a, corpus.Pick(mutator));
      } else {
        input = mutator.Mutate(corpus.Pick(mutator));
      }
      const bool full = (i % 8) == 0;
      total_bytes += input.size();
      ++total_inputs;

      if (auto err = probe(input, full)) {
        Probe shrink_probe{*spec};
        ASSERT_TRUE(shrink_probe.valid());
        const auto still_fails = [&](std::string_view v) {
          return shrink_probe(v, full).has_value();
        };
        const auto minimal = Shrink(input, still_fails);
        const auto minimal_err = shrink_probe(minimal, full);
        FAIL() << spec->name << ": " << *err
               << "\n  operator: " << mutator.LastOperator()
               << "\n  input:    " << Describe(input)
               << "\n  minimal:  " << Describe(minimal) << "\n  minimal error: "
               << minimal_err.value_or(std::string{"<not reproducible>"})
               << "\n  reproduce with TOKENIZER_FUZZ_SEED=" << seed
               << " TOKENIZER_FUZZ_ONLY=" << spec->name;
      }
      corpus.Offer(std::move(input), probe.behaviour());
    }
    total_classes += corpus.classes();
  }

  std::printf(
    "[   FUZZ   ] %zu inputs, %zu MiB mutated, %zu behaviour classes, %.1fs\n",
    total_inputs, total_bytes >> 20, total_classes, Seconds(start));
}

TEST(TokenizerFuzz, BlockFillMatchesPerValue) {
  const auto count =
    static_cast<size_t>(EnvU64("TOKENIZER_FUZZ_BLOCK_VALUES", 128));
  for (const auto* spec : SelectedSpecs()) {
    SCOPED_TRACE(testing::Message() << spec->name << " seed=" << Seed());
    const auto values =
      SpecCorpus(*spec, Seed() ^ 0x9E3779B9ull, ValueBudget(*spec, count));
    ASSERT_NO_FATAL_FAILURE(CheckSpecBlocks(*spec, values));
  }
}

TEST(TokenizerFuzz, StableTermsSurviveFinish) {
  for (const auto* spec : SelectedSpecs()) {
    SCOPED_TRACE(spec->name);
    const auto values = SpecCorpus(*spec, Seed(), 64);
    ASSERT_NO_FATAL_FAILURE(CheckSpecStableTerms(*spec, values));
  }
}

TEST(TokenizerFuzz, RecoversAfterRejectedValue) {
  const auto& good = WordValues();
  for (const auto* spec : SelectedSpecs()) {
    SCOPED_TRACE(spec->name);
    auto clean = Make(*spec);
    ASSERT_NE(nullptr, clean);
    auto poisoned = Make(*spec);
    ASSERT_NE(nullptr, poisoned);

    for (const auto layout : DeclaredLayouts(clean->Traits())) {
      SCOPED_TRACE(testing::Message() << "layout=" << LayoutName(layout));
      for (size_t i = 0; i < good.size(); ++i) {
        const auto& poison = PoisonValues()[i % PoisonValues().size()];
        if (spec->utf8_only && !IsValidUtf8(poison)) {
          continue;
        }
        const auto baseline = AnalyzeValue(*clean, good[i], layout);
        AnalyzeValue(*poisoned, poison, layout);
        const auto after = AnalyzeValue(*poisoned, good[i], layout);
        SCOPED_TRACE(testing::Message()
                     << "value=" << i << " " << Describe(good[i]));
        ASSERT_EQ(baseline.ok, after.ok);
        ASSERT_EQ(baseline.tokens.size(), after.tokens.size());
        for (size_t k = 0; k < baseline.tokens.size(); ++k) {
          ASSERT_EQ(baseline.tokens[k], after.tokens[k]) << "token=" << k;
        }
      }
    }
  }
}

TEST(TokenizerFuzz, BatchBoundaryResumption) {
  std::vector<std::string> values;
  for (const size_t tokens :
       {1023u, 1024u, 1025u, 2047u, 2048u, 2049u, 4096u}) {
    std::string v;
    for (size_t i = 0; i < tokens; ++i) {
      if (i != 0) {
        v.push_back(',');
      }
      v.push_back(static_cast<char>('a' + (i % 26)));
      v += std::to_string(i % 97);
    }
    values.push_back(std::move(v));
  }
  values.emplace_back("tail,value");
  values.emplace_back();

  for (const auto* spec : SelectedSpecs()) {
    if (spec->cost > 2) {
      continue;
    }
    SCOPED_TRACE(spec->name);
    ASSERT_NO_FATAL_FAILURE(CheckSpecBlocks(*spec, values));
  }
}

TEST(TokenizerFuzz, HugeValues) {
  const auto mib =
    static_cast<size_t>(EnvU64("TOKENIZER_FUZZ_HUGE_VALUE_MIB", 4));
  std::vector<std::string> values;
  for (const size_t size : {size_t{1} << 16, size_t{1} << 20, mib << 20}) {
    std::string v;
    v.reserve(size);
    size_t i = 0;
    while (v.size() < size) {
      v.push_back(static_cast<char>('a' + (i % 26)));
      if (i % 9 == 8) {
        v.push_back(',');
      }
      if (i % 257 == 256) {
        v += "\xC3\xA9";
      }
      ++i;
    }
    v.resize(size);
    values.push_back(std::move(v));
  }
  values.emplace_back(size_t{1} << 20, 'x');
  values.emplace_back(size_t{1} << 20, ',');

  for (const auto* spec : SelectedFamilies()) {
    if (spec->cost > 1) {
      continue;
    }
    SCOPED_TRACE(spec->name);
    Probe probe{*spec};
    ASSERT_TRUE(probe.valid()) << spec->name;
    for (size_t i = 0; i < values.size(); ++i) {
      const auto err = probe(values[i], /*full=*/true);
      ASSERT_FALSE(err.has_value())
        << spec->name << ": " << *err << "\n  value " << i << " of "
        << values[i].size() << " bytes";
    }
  }
}

TEST(TokenizerFuzz, RebindCycles) {
  const auto values = WordValues();
  for (const auto* spec : SelectedSpecs()) {
    SCOPED_TRACE(spec->name);
    auto tokenizer = Make(*spec);
    ASSERT_NE(nullptr, tokenizer);
    const auto layout = DeclaredLayouts(tokenizer->Traits()).back();
    std::vector<Result> baseline;
    baseline.reserve(values.size());
    for (const auto& v : values) {
      baseline.push_back(AnalyzeValue(*tokenizer, v, layout));
    }

    for (size_t round = 0; round < 3; ++round) {
      tokenizer->Unbind();
      auto rebound = Make(*spec);
      ASSERT_NE(nullptr, rebound);
      std::swap(tokenizer, rebound);
      const auto traits = tokenizer->Traits();
      ASSERT_EQ(layout, DeclaredLayouts(traits).back())
        << "declared layouts changed across a rebind";
      for (size_t i = 0; i < values.size(); ++i) {
        const auto after = AnalyzeValue(*tokenizer, values[i], layout);
        ASSERT_EQ(baseline[i].ok, after.ok) << "value " << i;
        ASSERT_EQ(baseline[i].tokens.size(), after.tokens.size())
          << "value " << i;
        for (size_t k = 0; k < after.tokens.size(); ++k) {
          ASSERT_EQ(baseline[i].tokens[k], after.tokens[k])
            << "value " << i << " token " << k;
        }
      }
    }
  }
}

TEST(TokenizerFuzz, MemoryUsageStaysBounded) {
  const auto count =
    static_cast<size_t>(EnvU64("TOKENIZER_FUZZ_MEMORY_VALUES", 2048));
  for (const auto* spec : SelectedSpecs()) {
    SCOPED_TRACE(spec->name);
    auto tokenizer = Make(*spec);
    ASSERT_NE(nullptr, tokenizer);
    const auto values = SpecCorpus(*spec, Seed(), ValueBudget(*spec, count));
    const auto layout = DeclaredLayouts(tokenizer->Traits()).back();
    Drain(*tokenizer, values, layout, 1024);
    const auto after_warmup = tokenizer->MemoryUsage();
    for (size_t round = 0; round < 4; ++round) {
      Drain(*tokenizer, values, layout, 1024);
    }
    const auto after_load = tokenizer->MemoryUsage();
    EXPECT_LE(after_load, kMemoryCap);
    EXPECT_LE(after_load, std::max<size_t>(after_warmup * 4, 1u << 20))
      << "MemoryUsage grows with the number of processed values";
  }
}

TEST(TokenizerFuzzLoad, ManyValues) {
  const auto budget = EnvU64("TOKENIZER_LOAD_BYTES", 64ull << 20);
  const auto seconds = static_cast<double>(EnvU64("TOKENIZER_LOAD_SECONDS", 0));
  const auto seed = Seed() ^ 0x10AD10ADull;
  const auto start = Clock::now();

  for (const auto* spec : SelectedFamilies()) {
    SCOPED_TRACE(testing::Message() << spec->name << " seed=" << seed);
    auto tokenizer = Make(*spec);
    ASSERT_NE(nullptr, tokenizer);
    const auto traits = tokenizer->Traits();
    const auto layout = DeclaredLayouts(traits).back();

    Mutator mutator{seed ^ NameSeed(spec->name), spec->dict, SizeCap(*spec)};
    FeedbackCorpus corpus;
    for (const auto& v : SpecCorpus(*spec, seed, 0)) {
      corpus.Seed(v);
    }

    const auto spec_budget = budget / std::max<uint32_t>(1, spec->cost);
    size_t produced = 0;
    size_t rows = 0;
    size_t tokens = 0;
    const auto spec_start = Clock::now();
    while (produced < spec_budget && !OutOfTime(start, seconds)) {
      std::vector<std::string> chunk;
      chunk.reserve(1024);
      size_t chunk_bytes = 0;
      while (chunk.size() < 1024 && chunk_bytes < (4u << 20) &&
             produced + chunk_bytes < spec_budget) {
        auto v = (chunk.size() % 32 == 0)
                   ? mutator.Generate()
                   : mutator.Mutate(corpus.Pick(mutator));
        if (spec->utf8_only && !IsValidUtf8(v)) {
          continue;
        }
        chunk_bytes += v.size() + 1;
        chunk.push_back(std::move(v));
      }
      if (chunk.empty()) {
        break;
      }
      std::vector<Row> block(chunk.size());
      for (size_t i = 0; i < chunk.size(); ++i) {
        block[i].value = &chunk[i];
      }
      std::string error;
      const auto got =
        FillBlock(*tokenizer, block, layout, BlockMode::Flat, &error);
      ASSERT_TRUE(error.empty()) << spec->name << ": " << error;
      ASSERT_EQ(chunk.size(), got.size());
      for (size_t i = 0; i < got.size(); ++i) {
        tokens += got[i].tokens.size();
        if (i % 37 != 0) {
          continue;
        }
        const auto err = ValueInvariants(traits, chunk[i], layout, got[i]);
        ASSERT_FALSE(err.has_value())
          << spec->name << ": " << *err << "\n  value: " << Describe(chunk[i]);
      }
      for (size_t i = 0; i < chunk.size(); i += 7) {
        size_t term_bytes = 0;
        size_t max_term = 0;
        for (const auto& t : got[i].tokens) {
          term_bytes += t.term.size();
          max_term = std::max(max_term, t.term.size());
        }
        corpus.Offer(chunk[i], BehaviourClass(
                                 got[i].tokens.empty(), got[i].tokens.size(),
                                 term_bytes, max_term, got[i].store.size(), 0));
      }
      produced += chunk_bytes;
      rows += chunk.size();
    }
    EXPECT_LE(tokenizer->MemoryUsage(), kMemoryCap) << spec->name;
    std::printf(
      "[   LOAD   ] %-42s %6zu MiB  %8zu rows  %10zu tokens  %6.1fs\n",
      spec->name.c_str(), produced >> 20, rows, tokens, Seconds(spec_start));
  }
}
