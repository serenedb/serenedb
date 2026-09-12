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

#include <iresearch/utils/duckdb_engine.h>

#include <atomic>
#include <duckdb.hpp>
#include <iresearch/analysis/tokenizer_pool.hpp>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "tests_shared.hpp"
#include "tokenizer_fuzz_checks.hpp"
#include "tokenizer_fuzz_corpus.hpp"
#include "tokenizer_fuzz_specs.hpp"

namespace {

using namespace tests::fuzz;
using irs::analysis::TokenizerPool;

duckdb::ClientContext& Context() {
  static thread_local auto* conn =
    new duckdb::Connection{sdb::DuckDBEngine::Instance().instance()};
  return *conn->context;
}

std::vector<Result> Drive(irs::analysis::Tokenizer& tokenizer,
                          std::span<const std::string> values,
                          irs::TokenLayout layout) {
  std::vector<Result> out;
  out.reserve(values.size());
  for (const auto& v : values) {
    out.push_back(AnalyzeValue(tokenizer, v, layout));
  }
  return out;
}

void ExpectSame(std::span<const Result> want, std::span<const Result> got,
                std::string_view what) {
  ASSERT_EQ(want.size(), got.size()) << what;
  for (size_t i = 0; i < want.size(); ++i) {
    ASSERT_EQ(want[i].ok, got[i].ok) << what << " value " << i;
    ASSERT_EQ(want[i].tokens.size(), got[i].tokens.size())
      << what << " value " << i;
    for (size_t k = 0; k < want[i].tokens.size(); ++k) {
      ASSERT_EQ(want[i].tokens[k], got[i].tokens[k])
        << what << " value " << i << " token " << k;
    }
    ASSERT_EQ(want[i].store, got[i].store) << what << " value " << i;
  }
}

}  // namespace

TEST(TokenizerLifecycle, PooledInstancesStayEquivalent) {
  duckdb::DuckDB db{nullptr};
  size_t pool_id = 0;

  for (const auto* spec : SelectedSpecs()) {
    SCOPED_TRACE(spec->name);
    const auto values = SpecCorpus(*spec, Seed(), 48);

    auto pool = TokenizerPool::Get(*db.instance, std::to_string(pool_id++));
    ASSERT_TRUE(pool);
    ASSERT_EQ(nullptr, pool->Acquire());

    auto first = Make(*spec);
    ASSERT_NE(nullptr, first);
    const auto layout = DeclaredLayouts(first->Traits()).back();
    const auto baseline = Drive(*first, values, layout);

    auto* raw = first.get();
    pool->Release(std::move(first));
    ASSERT_EQ(1u, pool->IdleCount());

    for (size_t round = 0; round < 3; ++round) {
      auto borrowed = pool->Acquire();
      ASSERT_NE(nullptr, borrowed) << "round " << round;
      ASSERT_EQ(raw, borrowed.get())
        << "the pool handed back a different instance";
      borrowed->Bind(Context());
      if (spec->setup) {
        spec->setup(*borrowed);
      }
      const auto after = Drive(*borrowed, values, layout);
      ASSERT_NO_FATAL_FAILURE(
        ExpectSame(baseline, after, "recycled through the pool"));
      pool->Release(std::move(borrowed));
    }
    ASSERT_EQ(1u, pool->IdleCount());
  }
}

TEST(TokenizerLifecycle, PoolCapDropsExcessInstances) {
  duckdb::DuckDB db{nullptr};
  const auto* spec = SelectedSpecs().front();
  auto pool = duckdb::make_shared_ptr<TokenizerPool>(*db.instance, "cap", 2);
  for (size_t i = 0; i < 5; ++i) {
    auto tokenizer = Make(*spec);
    ASSERT_NE(nullptr, tokenizer);
    pool->Release(std::move(tokenizer));
  }
  EXPECT_EQ(2u, pool->IdleCount());
  EXPECT_NE(nullptr, pool->Acquire());
  EXPECT_NE(nullptr, pool->Acquire());
  EXPECT_EQ(nullptr, pool->Acquire());
}

TEST(TokenizerLifecycle, OnePoolPerDictionaryAcrossThreads) {
  duckdb::DuckDB db{nullptr};
  const auto* spec = SelectedSpecs().front();
  const auto values = SpecCorpus(*spec, Seed(), 32);

  auto reference = Make(*spec);
  ASSERT_NE(nullptr, reference);
  const auto layout = DeclaredLayouts(reference->Traits()).back();
  const auto baseline = Drive(*reference, values, layout);

  const auto threads =
    std::max<unsigned>(2, std::thread::hardware_concurrency());
  std::atomic<size_t> failures{0};
  std::vector<std::thread> pool_threads;
  for (unsigned t = 0; t < threads; ++t) {
    pool_threads.emplace_back([&] {
      auto pool = TokenizerPool::Get(*db.instance, "shared");
      if (!pool) {
        ++failures;
        return;
      }
      for (size_t round = 0; round < 8; ++round) {
        auto borrowed = pool->Acquire();
        if (!borrowed) {
          borrowed = Make(*spec);
        } else {
          borrowed->Bind(Context());
          if (spec->setup) {
            spec->setup(*borrowed);
          }
        }
        if (!borrowed) {
          ++failures;
          return;
        }
        const auto got = Drive(*borrowed, values, layout);
        if (got.size() != baseline.size()) {
          ++failures;
          return;
        }
        for (size_t i = 0; i < got.size(); ++i) {
          if (got[i].ok != baseline[i].ok ||
              got[i].tokens.size() != baseline[i].tokens.size()) {
            ++failures;
            return;
          }
          for (size_t k = 0; k < got[i].tokens.size(); ++k) {
            if (!(got[i].tokens[k] == baseline[i].tokens[k])) {
              ++failures;
              return;
            }
          }
        }
        pool->Release(std::move(borrowed));
      }
    });
  }
  for (auto& thread : pool_threads) {
    thread.join();
  }
  EXPECT_EQ(0u, failures.load());
}
