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
//
// Filter `prepare()` micro benchmark.
//
// Builds a small in-memory index with a configurable number of segments and
// measures Filter::prepare() for every supported filter kind, crossed with
// {nullptr, BM25, TFIDF} scorers and a boosted BM25 variant.
//
// state.range(0) controls the segment count of the index built in SetUp().
//
// Examples:
//   ./serenedb-bench-micro-filter_prepare --benchmark_min_time=0.3s
//   ./serenedb-bench-micro-filter_prepare \
//       --benchmark_filter='ByTerm_.*/4$' --benchmark_min_time=0.5s
//

#include <benchmark/benchmark.h>

#include <array>
#include <cstdio>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/analysis/analyzers.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/search/bm25.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/range_filter.hpp"
#include "iresearch/search/search_range.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/terms_filter.hpp"
#include "iresearch/search/tfidf.hpp"
#include "iresearch/search/wildcard_filter.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/utils/compression.hpp"
#include "iresearch/utils/string.hpp"

namespace {

constexpr size_t kDocsPerSegment = 256;
constexpr size_t kTermPoolSize = 512;
constexpr irs::score_t kBoostValue = 2.5f;

// Keyword field: one token per value, indexed with Freq|Norm so it is usable
// against BM25 / TFIDF scorers.
struct KeywordField {
  std::string_view Name() const noexcept { return name; }

  irs::Tokenizer& GetTokens() const {
    stream.reset(value);
    return stream;
  }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::Freq | irs::IndexFeatures::Norm;
  }

  bool Write(irs::DataOutput&) const { return true; }

  std::string_view name;
  std::string_view value;
  mutable irs::StringTokenizer stream;
};

// Text field: segmentation-tokenised. Used by phrase cases (requires Pos).
struct TextField {
  std::string_view Name() const noexcept { return name; }

  irs::Tokenizer& GetTokens() const {
    tokenizer->reset(value);
    return *tokenizer;
  }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
           irs::IndexFeatures::Norm;
  }

  bool Write(irs::DataOutput&) const { return true; }

  std::string_view name;
  std::string_view value;
  irs::analysis::Analyzer::ptr tokenizer = irs::analysis::analyzers::Get(
    "segmentation", irs::Type<irs::text_format::Json>::get(), "{}");
};

std::vector<std::string> MakeTermPool() {
  std::vector<std::string> v;
  v.reserve(kTermPoolSize);
  char buf[16];
  for (size_t i = 0; i < kTermPoolSize; ++i) {
    std::snprintf(buf, sizeof(buf), "term_%04zu", i);
    v.emplace_back(buf);
  }
  return v;
}

constexpr std::array<std::string_view, 8> kBodyPool{
  "quick brown fox jumps over the lazy dog",
  "alpha beta gamma delta epsilon",
  "the quick brown rabbit",
  "lorem ipsum dolor sit amet",
  "quick brown fox runs",
  "lazy dog sleeps",
  "alpha gamma delta",
  "lorem ipsum",
};

inline irs::bytes_view AsBytes(std::string_view s) noexcept {
  return irs::ViewCast<irs::byte_type>(s);
}

class FilterPrepareFixture : public benchmark::Fixture {
 public:
  void SetUp(const ::benchmark::State& state) override {
    // Lazy-initialise registry-backed handles. The fixture is constructed
    // at static-init time (BENCHMARK_REGISTER_F instantiates one per
    // benchmark), before main() has called formats::Init() et al — so
    // anything that calls `…::Get(name)` has to wait until SetUp.
    if (!codec_) {
      codec_ = irs::formats::Get("1_5simd");
      term_pool_ = MakeTermPool();
    }
    BuildIndex(static_cast<size_t>(state.range(0)));
  }

  void TearDown(const ::benchmark::State&) override {
    reader_ = irs::DirectoryReader{};
  }

 protected:
  void BuildIndex(size_t num_segments);

  // MemoryDirectory is Noncopyable + has no move; wrap in optional so we
  // can re-create it on every SetUp (the fixture is reused for multiple
  // state.range(0) values).
  std::optional<irs::MemoryDirectory> dir_;
  irs::Format::ptr codec_;
  irs::DirectoryReader reader_;

  irs::BM25 bm25_{};
  irs::TFIDF tfidf_{};

  std::vector<std::string> term_pool_;
};

void FilterPrepareFixture::BuildIndex(size_t num_segments) {
  reader_ = irs::DirectoryReader{};
  dir_.emplace();

  auto writer = irs::IndexWriter::Make(*dir_, codec_, irs::kOmCreate);

  KeywordField kw_field{.name = "kw"};
  TextField body_field{.name = "body"};

  for (size_t s = 0; s < num_segments; ++s) {
    {
      auto trx = writer->GetBatch();
      for (size_t d = 0; d < kDocsPerSegment; ++d) {
        const size_t i = s * kDocsPerSegment + d;
        kw_field.value = term_pool_[i % term_pool_.size()];
        body_field.value = kBodyPool[i % kBodyPool.size()];

        auto doc = trx.Insert();
        doc.Insert(kw_field);
        doc.Insert(body_field);
      }
    }
    writer->Commit();
  }

  reader_ = irs::DirectoryReader{*dir_, codec_};
}

// Common segment-count grid for every case.
void ApplyArgs(benchmark::internal::Benchmark* b) {
  b->Arg(1)->Arg(4)->Arg(16)->Arg(64)->Unit(benchmark::kMicrosecond);
}

void SetUpRange(irs::ByRange& f) {
  *f.mutable_field() = "kw";
  auto& range = f.mutable_options()->range;
  range.min = AsBytes("term_0000");
  range.min_type = irs::BoundType::Inclusive;
  range.max = AsBytes("term_0100");
  range.max_type = irs::BoundType::Inclusive;
}

void SetUpTerms(irs::ByTerms& f) {
  *f.mutable_field() = "kw";
  auto& opts = *f.mutable_options();
  for (size_t i = 0; i < 8; ++i) {
    char buf[16];
    std::snprintf(buf, sizeof(buf), "term_%04zu", i * 37);  // spread
    opts.terms.emplace(irs::bstring{AsBytes(std::string_view(buf))});
  }
}

void SetUpEdit(irs::ByEditDistance& f) {
  *f.mutable_field() = "kw";
  auto& opts = *f.mutable_options();
  opts.term = AsBytes("term_0042");
  opts.max_distance = 1;
  opts.with_transpositions = false;
  opts.max_terms = 64;
}

void SetUpPhrase(irs::ByPhrase& f) {
  *f.mutable_field() = "body";
  auto* opts = f.mutable_options();
  opts->push_back<irs::ByTermOptions>().term = AsBytes("quick");
  opts->push_back<irs::ByTermOptions>().term = AsBytes("brown");
}

void SetUpAnd(irs::And& a) {
  auto& f1 = a.add<irs::ByTerm>();
  *f1.mutable_field() = "kw";
  f1.mutable_options()->term = AsBytes("term_0042");

  auto& f2 = a.add<irs::ByPrefix>();
  *f2.mutable_field() = "kw";
  f2.mutable_options()->term = AsBytes("term_0");

  auto& f3 = a.add<irs::ByRange>();
  *f3.mutable_field() = "kw";
  auto& range = f3.mutable_options()->range;
  range.min = AsBytes("term_0000");
  range.min_type = irs::BoundType::Inclusive;
  range.max = AsBytes("term_0100");
  range.max_type = irs::BoundType::Inclusive;
}

void SetUpOr(irs::Or& o) {
  static constexpr std::array<std::string_view, 3> kTerms{
    "term_0042", "term_0100", "term_0250"};
  for (auto t : kTerms) {
    auto& f = o.add<irs::ByTerm>();
    *f.mutable_field() = "kw";
    f.mutable_options()->term = AsBytes(t);
  }
}

void SetUpNot(irs::Not& n) {
  auto& inner = n.filter<irs::ByTerm>();
  *inner.mutable_field() = "kw";
  inner.mutable_options()->term = AsBytes("term_0042");
}

}  // namespace

// ---------------------------------------------------------------------------
// ByTerm
// ---------------------------------------------------------------------------

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByTerm_NoScorer)(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByTerm f;
    *f.mutable_field() = "kw";
    f.mutable_options()->term = AsBytes("term_0042");
    auto q = f.prepare({.index = reader_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByTerm_NoScorer)->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByTerm_Bm25)(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByTerm f;
    *f.mutable_field() = "kw";
    f.mutable_options()->term = AsBytes("term_0042");
    auto q = f.prepare({.index = reader_, .scorer = &bm25_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByTerm_Bm25)->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByTerm_Tfidf)(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByTerm f;
    *f.mutable_field() = "kw";
    f.mutable_options()->term = AsBytes("term_0042");
    auto q = f.prepare({.index = reader_, .scorer = &tfidf_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByTerm_Tfidf)->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByTerm_Bm25_Boost)
(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByTerm f;
    *f.mutable_field() = "kw";
    f.mutable_options()->term = AsBytes("term_0042");
    f.boost(kBoostValue);
    auto q = f.prepare({.index = reader_, .scorer = &bm25_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByTerm_Bm25_Boost)
  ->Apply(ApplyArgs);

// ---------------------------------------------------------------------------
// ByPrefix
// ---------------------------------------------------------------------------

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByPrefix_NoScorer)
(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByPrefix f;
    *f.mutable_field() = "kw";
    f.mutable_options()->term = AsBytes("term_0");
    auto q = f.prepare({.index = reader_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByPrefix_NoScorer)
  ->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByPrefix_Bm25)(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByPrefix f;
    *f.mutable_field() = "kw";
    f.mutable_options()->term = AsBytes("term_0");
    auto q = f.prepare({.index = reader_, .scorer = &bm25_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByPrefix_Bm25)->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByPrefix_Tfidf)(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByPrefix f;
    *f.mutable_field() = "kw";
    f.mutable_options()->term = AsBytes("term_0");
    auto q = f.prepare({.index = reader_, .scorer = &tfidf_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByPrefix_Tfidf)->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByPrefix_Bm25_Boost)
(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByPrefix f;
    *f.mutable_field() = "kw";
    f.mutable_options()->term = AsBytes("term_0");
    f.boost(kBoostValue);
    auto q = f.prepare({.index = reader_, .scorer = &bm25_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByPrefix_Bm25_Boost)
  ->Apply(ApplyArgs);

// ---------------------------------------------------------------------------
// ByRange
// ---------------------------------------------------------------------------

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByRange_NoScorer)
(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByRange f;
    SetUpRange(f);
    auto q = f.prepare({.index = reader_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByRange_NoScorer)
  ->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByRange_Bm25)(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByRange f;
    SetUpRange(f);
    auto q = f.prepare({.index = reader_, .scorer = &bm25_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByRange_Bm25)->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByRange_Tfidf)(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByRange f;
    SetUpRange(f);
    auto q = f.prepare({.index = reader_, .scorer = &tfidf_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByRange_Tfidf)->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByRange_Bm25_Boost)
(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByRange f;
    SetUpRange(f);
    f.boost(kBoostValue);
    auto q = f.prepare({.index = reader_, .scorer = &bm25_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByRange_Bm25_Boost)
  ->Apply(ApplyArgs);

// ---------------------------------------------------------------------------
// ByTerms (8 terms)
// ---------------------------------------------------------------------------

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByTerms_NoScorer)
(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByTerms f;
    SetUpTerms(f);
    auto q = f.prepare({.index = reader_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByTerms_NoScorer)
  ->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByTerms_Bm25)(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByTerms f;
    SetUpTerms(f);
    auto q = f.prepare({.index = reader_, .scorer = &bm25_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByTerms_Bm25)->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByTerms_Tfidf)(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByTerms f;
    SetUpTerms(f);
    auto q = f.prepare({.index = reader_, .scorer = &tfidf_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByTerms_Tfidf)->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByTerms_Bm25_Boost)
(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByTerms f;
    SetUpTerms(f);
    f.boost(kBoostValue);
    auto q = f.prepare({.index = reader_, .scorer = &bm25_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByTerms_Bm25_Boost)
  ->Apply(ApplyArgs);

// ---------------------------------------------------------------------------
// ByWildcard
// ---------------------------------------------------------------------------

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByWildcard_NoScorer)
(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByWildcard f;
    *f.mutable_field() = "kw";
    f.mutable_options()->term = AsBytes("term_0??");
    auto q = f.prepare({.index = reader_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByWildcard_NoScorer)
  ->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByWildcard_Bm25)
(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByWildcard f;
    *f.mutable_field() = "kw";
    f.mutable_options()->term = AsBytes("term_0??");
    auto q = f.prepare({.index = reader_, .scorer = &bm25_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByWildcard_Bm25)->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByWildcard_Tfidf)
(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByWildcard f;
    *f.mutable_field() = "kw";
    f.mutable_options()->term = AsBytes("term_0??");
    auto q = f.prepare({.index = reader_, .scorer = &tfidf_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByWildcard_Tfidf)
  ->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByWildcard_Bm25_Boost)
(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByWildcard f;
    *f.mutable_field() = "kw";
    f.mutable_options()->term = AsBytes("term_0??");
    f.boost(kBoostValue);
    auto q = f.prepare({.index = reader_, .scorer = &bm25_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByWildcard_Bm25_Boost)
  ->Apply(ApplyArgs);

// ---------------------------------------------------------------------------
// ByEditDistance (Levenshtein) — max_distance=1, max_terms=64
// ---------------------------------------------------------------------------

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByEditDistance_NoScorer)
(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByEditDistance f;
    SetUpEdit(f);
    auto q = f.prepare({.index = reader_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByEditDistance_NoScorer)
  ->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByEditDistance_Bm25)
(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByEditDistance f;
    SetUpEdit(f);
    auto q = f.prepare({.index = reader_, .scorer = &bm25_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByEditDistance_Bm25)
  ->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByEditDistance_Tfidf)
(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByEditDistance f;
    SetUpEdit(f);
    auto q = f.prepare({.index = reader_, .scorer = &tfidf_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByEditDistance_Tfidf)
  ->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByEditDistance_Bm25_Boost)
(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByEditDistance f;
    SetUpEdit(f);
    f.boost(kBoostValue);
    auto q = f.prepare({.index = reader_, .scorer = &bm25_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByEditDistance_Bm25_Boost)
  ->Apply(ApplyArgs);

// ---------------------------------------------------------------------------
// ByPhrase ("quick brown")
// ---------------------------------------------------------------------------

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByPhrase_NoScorer)
(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByPhrase f;
    SetUpPhrase(f);
    auto q = f.prepare({.index = reader_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByPhrase_NoScorer)
  ->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByPhrase_Bm25)(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByPhrase f;
    SetUpPhrase(f);
    auto q = f.prepare({.index = reader_, .scorer = &bm25_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByPhrase_Bm25)->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByPhrase_Tfidf)(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByPhrase f;
    SetUpPhrase(f);
    auto q = f.prepare({.index = reader_, .scorer = &tfidf_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByPhrase_Tfidf)->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, ByPhrase_Bm25_Boost)
(benchmark::State& s) {
  for (auto _ : s) {
    irs::ByPhrase f;
    SetUpPhrase(f);
    f.boost(kBoostValue);
    auto q = f.prepare({.index = reader_, .scorer = &bm25_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, ByPhrase_Bm25_Boost)
  ->Apply(ApplyArgs);

// ---------------------------------------------------------------------------
// And { ByTerm, ByPrefix, ByRange }
// ---------------------------------------------------------------------------

BENCHMARK_DEFINE_F(FilterPrepareFixture, And_NoScorer)(benchmark::State& s) {
  for (auto _ : s) {
    irs::And a;
    SetUpAnd(a);
    auto q = a.prepare({.index = reader_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, And_NoScorer)->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, And_Bm25)(benchmark::State& s) {
  for (auto _ : s) {
    irs::And a;
    SetUpAnd(a);
    auto q = a.prepare({.index = reader_, .scorer = &bm25_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, And_Bm25)->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, And_Tfidf)(benchmark::State& s) {
  for (auto _ : s) {
    irs::And a;
    SetUpAnd(a);
    auto q = a.prepare({.index = reader_, .scorer = &tfidf_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, And_Tfidf)->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, And_Bm25_Boost)(benchmark::State& s) {
  for (auto _ : s) {
    irs::And a;
    SetUpAnd(a);
    a.boost(kBoostValue);
    auto q = a.prepare({.index = reader_, .scorer = &bm25_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, And_Bm25_Boost)->Apply(ApplyArgs);

// ---------------------------------------------------------------------------
// Or { ByTerm, ByTerm, ByTerm }
// ---------------------------------------------------------------------------

BENCHMARK_DEFINE_F(FilterPrepareFixture, Or_NoScorer)(benchmark::State& s) {
  for (auto _ : s) {
    irs::Or o;
    SetUpOr(o);
    auto q = o.prepare({.index = reader_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, Or_NoScorer)->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, Or_Bm25)(benchmark::State& s) {
  for (auto _ : s) {
    irs::Or o;
    SetUpOr(o);
    auto q = o.prepare({.index = reader_, .scorer = &bm25_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, Or_Bm25)->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, Or_Tfidf)(benchmark::State& s) {
  for (auto _ : s) {
    irs::Or o;
    SetUpOr(o);
    auto q = o.prepare({.index = reader_, .scorer = &tfidf_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, Or_Tfidf)->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, Or_Bm25_Boost)(benchmark::State& s) {
  for (auto _ : s) {
    irs::Or o;
    SetUpOr(o);
    o.boost(kBoostValue);
    auto q = o.prepare({.index = reader_, .scorer = &bm25_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, Or_Bm25_Boost)->Apply(ApplyArgs);

// ---------------------------------------------------------------------------
// Not(ByTerm)
// ---------------------------------------------------------------------------

BENCHMARK_DEFINE_F(FilterPrepareFixture, Not_NoScorer)(benchmark::State& s) {
  for (auto _ : s) {
    irs::Not n;
    SetUpNot(n);
    auto q = n.prepare({.index = reader_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, Not_NoScorer)->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, Not_Bm25)(benchmark::State& s) {
  for (auto _ : s) {
    irs::Not n;
    SetUpNot(n);
    auto q = n.prepare({.index = reader_, .scorer = &bm25_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, Not_Bm25)->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, Not_Tfidf)(benchmark::State& s) {
  for (auto _ : s) {
    irs::Not n;
    SetUpNot(n);
    auto q = n.prepare({.index = reader_, .scorer = &tfidf_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, Not_Tfidf)->Apply(ApplyArgs);

BENCHMARK_DEFINE_F(FilterPrepareFixture, Not_Bm25_Boost)(benchmark::State& s) {
  for (auto _ : s) {
    irs::Not n;
    SetUpNot(n);
    n.boost(kBoostValue);
    auto q = n.prepare({.index = reader_, .scorer = &bm25_});
    benchmark::DoNotOptimize(q);
  }
}
BENCHMARK_REGISTER_F(FilterPrepareFixture, Not_Bm25_Boost)->Apply(ApplyArgs);

int main(int argc, char** argv) {
  // Statically-linked plugin registries need explicit initialisation; without
  // these calls `formats::Get` / `scorers::Get` / `analyzers::Get` fall
  // through to a dynamic-load path that spams "load failed" errors and
  // returns null.
  irs::analysis::analyzers::Init();
  irs::formats::Init();
  irs::scorers::Init();
  irs::compression::Init();

  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
