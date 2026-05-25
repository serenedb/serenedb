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

  template <typename Filter, typename Setup>
  void RunBench(benchmark::State& s, Setup setup, const irs::Scorer* scorer,
                std::optional<irs::score_t> boost) {
    for (auto _ : s) {
      Filter f;
      setup(f);
      if (boost) {
        f.boost(*boost);
      }
      auto q = f.prepare({.index = reader_, .scorer = scorer});
      benchmark::DoNotOptimize(q);
    }
  }

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

void SetUpTerm(irs::ByTerm& f) {
  *f.mutable_field() = "kw";
  f.mutable_options()->term = AsBytes("term_0042");
}

void SetUpPrefix(irs::ByPrefix& f) {
  *f.mutable_field() = "kw";
  f.mutable_options()->term = AsBytes("term_0");
}

void SetUpWildcard(irs::ByWildcard& f) {
  *f.mutable_field() = "kw";
  f.mutable_options()->term = AsBytes("term_0??");
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

#define DEFINE_FILTER_VARIANTS(Tag, Filter, Setup)                              \
  BENCHMARK_DEFINE_F(FilterPrepareFixture, Tag##_NoScorer)                      \
  (benchmark::State & s) {                                                     \
    RunBench<Filter>(s, Setup, nullptr, std::nullopt);                         \
  }                                                                            \
  BENCHMARK_REGISTER_F(FilterPrepareFixture, Tag##_NoScorer)->Apply(ApplyArgs); \
  BENCHMARK_DEFINE_F(FilterPrepareFixture, Tag##_Bm25)                          \
  (benchmark::State & s) {                                                     \
    RunBench<Filter>(s, Setup, &bm25_, std::nullopt);                          \
  }                                                                            \
  BENCHMARK_REGISTER_F(FilterPrepareFixture, Tag##_Bm25)->Apply(ApplyArgs);     \
  BENCHMARK_DEFINE_F(FilterPrepareFixture, Tag##_Tfidf)                         \
  (benchmark::State & s) {                                                     \
    RunBench<Filter>(s, Setup, &tfidf_, std::nullopt);                         \
  }                                                                            \
  BENCHMARK_REGISTER_F(FilterPrepareFixture, Tag##_Tfidf)->Apply(ApplyArgs);    \
  BENCHMARK_DEFINE_F(FilterPrepareFixture, Tag##_Bm25_Boost)                    \
  (benchmark::State & s) {                                                     \
    RunBench<Filter>(s, Setup, &bm25_, kBoostValue);                           \
  }                                                                            \
  BENCHMARK_REGISTER_F(FilterPrepareFixture, Tag##_Bm25_Boost)                  \
    ->Apply(ApplyArgs)

DEFINE_FILTER_VARIANTS(ByTerm, irs::ByTerm, SetUpTerm);
DEFINE_FILTER_VARIANTS(ByPrefix, irs::ByPrefix, SetUpPrefix);
DEFINE_FILTER_VARIANTS(ByRange, irs::ByRange, SetUpRange);
DEFINE_FILTER_VARIANTS(ByTerms, irs::ByTerms, SetUpTerms);
DEFINE_FILTER_VARIANTS(ByWildcard, irs::ByWildcard, SetUpWildcard);
DEFINE_FILTER_VARIANTS(ByEditDistance, irs::ByEditDistance, SetUpEdit);
DEFINE_FILTER_VARIANTS(ByPhrase, irs::ByPhrase, SetUpPhrase);
DEFINE_FILTER_VARIANTS(And, irs::And, SetUpAnd);
DEFINE_FILTER_VARIANTS(Or, irs::Or, SetUpOr);
DEFINE_FILTER_VARIANTS(Not, irs::Not, SetUpNot);

#undef DEFINE_FILTER_VARIANTS

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
