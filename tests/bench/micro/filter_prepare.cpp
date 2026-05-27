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

#include <absl/strings/str_format.h>
#include <benchmark/benchmark.h>

#include <array>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
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
#include "iresearch/store/mmap_directory.hpp"
#include "iresearch/utils/compression.hpp"
#include "iresearch/utils/string.hpp"

namespace {

constexpr size_t kDocsPerSegment = 256;
constexpr size_t kTermPoolSize = 512;
constexpr irs::score_t kBoostValue = 2.5f;

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
  for (size_t i = 0; i < kTermPoolSize; ++i) {
    if (i % 2 == 0) {
      v.emplace_back(absl::StrFormat("term_%04d", i));
    } else {
      v.emplace_back(absl::StrFormat("kw%04d", i));
    }
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
    if (!_codec) {
      _codec = irs::formats::Get("1_5simd");
      _term_pool = MakeTermPool();
    }
    BuildIndex(static_cast<size_t>(state.range(0)));
  }

  void TearDown(const ::benchmark::State&) override {
    _reader = irs::DirectoryReader{};
    _dir.reset();
    if (!_dir_path.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(_dir_path, ec);
      _dir_path.clear();
    }
  }

 protected:
  void BuildIndex(size_t num_segments);

  std::unique_ptr<irs::MMapDirectory> _dir;
  std::filesystem::path _dir_path;
  irs::Format::ptr _codec;
  irs::DirectoryReader _reader;

  irs::BM25 _bm25;
  irs::TFIDF _tfidf;

  std::vector<std::string> _term_pool;
};

template<typename Filter, void (*Configure)(Filter&), bool Boosted = false>
class FilterPrepareFixtureT : public FilterPrepareFixture {
 public:
  void SetUp(const ::benchmark::State& state) override {
    FilterPrepareFixture::SetUp(state);
    _filter = Filter{};
    Configure(_filter);
    if constexpr (Boosted) {
      _filter.boost(kBoostValue);
    }
  }

 protected:
  Filter _filter;
};

void FilterPrepareFixture::BuildIndex(size_t num_segments) {
  _reader = irs::DirectoryReader{};
  _dir.reset();

  if (_dir_path.empty()) {
    _dir_path =
      std::filesystem::temp_directory_path() / "serenedb-bench-filter-prepare";
  }
  std::filesystem::remove_all(_dir_path);
  std::filesystem::create_directories(_dir_path);
  _dir = std::make_unique<irs::MMapDirectory>(_dir_path);

  auto writer = irs::IndexWriter::Make(*_dir, _codec, irs::kOmCreate);

  KeywordField kw_field{.name = "kw"};
  TextField body_field{.name = "body"};

  for (size_t s = 0; s < num_segments; ++s) {
    {
      auto trx = writer->GetBatch();
      for (size_t d = 0; d < kDocsPerSegment; ++d) {
        const size_t i = s * kDocsPerSegment + d;
        kw_field.value = _term_pool[i % _term_pool.size()];
        body_field.value = kBodyPool[i % kBodyPool.size()];

        auto doc = trx.Insert();
        doc.Insert(kw_field);
        doc.Insert(body_field);
      }
    }
    writer->RefreshCommit();
  }

  _reader = irs::DirectoryReader{*_dir, _codec};
}

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

void SetUpWildcardTerm(irs::ByWildcard& f) {
  *f.mutable_field() = "kw";
  f.mutable_options()->term = AsBytes("kw0001");
}

void SetUpWildcardTermEscaped(irs::ByWildcard& f) {
  *f.mutable_field() = "kw";
  f.mutable_options()->term = AsBytes("term\\_0042");
}

void SetUpWildcardPrefix(irs::ByWildcard& f) {
  *f.mutable_field() = "kw";
  f.mutable_options()->term = AsBytes("term%");
}

void SetUpWildcardPrefixEscaped(irs::ByWildcard& f) {
  *f.mutable_field() = "kw";
  f.mutable_options()->term = AsBytes("term\\_0%");
}

void SetUpWildcardGeneric(irs::ByWildcard& f) {
  *f.mutable_field() = "kw";
  f.mutable_options()->term = AsBytes("term_0%");
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
    const std::string term = absl::StrFormat("term_%04d", i * 37);
    opts.terms.emplace(irs::bstring{AsBytes(term)});
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

void SetUpPhraseVariadic(irs::ByPhrase& f) {
  *f.mutable_field() = "body";
  auto* opts = f.mutable_options();
  opts->push_back<irs::ByTermOptions>().term = AsBytes("quick");
  opts->push_back<irs::ByPrefixOptions>().term = AsBytes("bro");
}

void SetUpPhraseFuzzy(irs::ByPhrase& f) {
  *f.mutable_field() = "body";
  auto* opts = f.mutable_options();
  opts->push_back<irs::ByTermOptions>().term = AsBytes("quick");
  auto& fuzzy = opts->push_back<irs::ByEditDistanceOptions>();
  fuzzy.term = AsBytes("brown");
  fuzzy.max_distance = 1;
  fuzzy.with_transpositions = false;
  fuzzy.max_terms = 16;
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

#define DEFINE_FILTER_VARIANTS(Tag, Filter, Setup)                             \
  BENCHMARK_TEMPLATE_DEFINE_F(FilterPrepareFixtureT, Tag##_NoScorer, Filter,   \
                              Setup)                                           \
  (benchmark::State & s) {                                                     \
    for (auto _ : s) {                                                         \
      auto q = _filter.prepare({.index = _reader});                            \
      benchmark::DoNotOptimize(q);                                             \
    }                                                                          \
  }                                                                            \
  BENCHMARK_REGISTER_F(FilterPrepareFixtureT, Tag##_NoScorer)                  \
    ->Name("FilterPrepareFixture/" #Tag "_NoScorer")                           \
    ->Apply(ApplyArgs);                                                        \
  BENCHMARK_TEMPLATE_DEFINE_F(FilterPrepareFixtureT, Tag##_Bm25, Filter,       \
                              Setup)                                           \
  (benchmark::State & s) {                                                     \
    for (auto _ : s) {                                                         \
      auto q = _filter.prepare({.index = _reader, .scorer = &_bm25});          \
      benchmark::DoNotOptimize(q);                                             \
    }                                                                          \
  }                                                                            \
  BENCHMARK_REGISTER_F(FilterPrepareFixtureT, Tag##_Bm25)                      \
    ->Name("FilterPrepareFixture/" #Tag "_Bm25")                               \
    ->Apply(ApplyArgs);                                                        \
  BENCHMARK_TEMPLATE_DEFINE_F(FilterPrepareFixtureT, Tag##_Tfidf, Filter,      \
                              Setup)                                           \
  (benchmark::State & s) {                                                     \
    for (auto _ : s) {                                                         \
      auto q = _filter.prepare({.index = _reader, .scorer = &_tfidf});         \
      benchmark::DoNotOptimize(q);                                             \
    }                                                                          \
  }                                                                            \
  BENCHMARK_REGISTER_F(FilterPrepareFixtureT, Tag##_Tfidf)                     \
    ->Name("FilterPrepareFixture/" #Tag "_Tfidf")                              \
    ->Apply(ApplyArgs);                                                        \
  BENCHMARK_TEMPLATE_DEFINE_F(FilterPrepareFixtureT, Tag##_Bm25_Boost, Filter, \
                              Setup, true)                                     \
  (benchmark::State & s) {                                                     \
    for (auto _ : s) {                                                         \
      auto q = _filter.prepare({.index = _reader, .scorer = &_bm25});          \
      benchmark::DoNotOptimize(q);                                             \
    }                                                                          \
  }                                                                            \
  BENCHMARK_REGISTER_F(FilterPrepareFixtureT, Tag##_Bm25_Boost)                \
    ->Name("FilterPrepareFixture/" #Tag "_Bm25_Boost")                         \
    ->Apply(ApplyArgs)

DEFINE_FILTER_VARIANTS(ByTerm, irs::ByTerm, SetUpTerm);
DEFINE_FILTER_VARIANTS(ByPrefix, irs::ByPrefix, SetUpPrefix);
DEFINE_FILTER_VARIANTS(ByRange, irs::ByRange, SetUpRange);
DEFINE_FILTER_VARIANTS(ByTerms, irs::ByTerms, SetUpTerms);
DEFINE_FILTER_VARIANTS(ByWildcard_Term, irs::ByWildcard, SetUpWildcardTerm);
DEFINE_FILTER_VARIANTS(ByWildcard_TermEscaped, irs::ByWildcard,
                       SetUpWildcardTermEscaped);
DEFINE_FILTER_VARIANTS(ByWildcard_Prefix, irs::ByWildcard, SetUpWildcardPrefix);
DEFINE_FILTER_VARIANTS(ByWildcard_PrefixEscaped, irs::ByWildcard,
                       SetUpWildcardPrefixEscaped);
DEFINE_FILTER_VARIANTS(ByWildcard_Generic, irs::ByWildcard,
                       SetUpWildcardGeneric);
DEFINE_FILTER_VARIANTS(ByEditDistance, irs::ByEditDistance, SetUpEdit);
DEFINE_FILTER_VARIANTS(ByPhrase, irs::ByPhrase, SetUpPhrase);
DEFINE_FILTER_VARIANTS(ByPhraseVariadic, irs::ByPhrase, SetUpPhraseVariadic);
DEFINE_FILTER_VARIANTS(ByPhraseFuzzy, irs::ByPhrase, SetUpPhraseFuzzy);
DEFINE_FILTER_VARIANTS(And, irs::And, SetUpAnd);
DEFINE_FILTER_VARIANTS(Or, irs::Or, SetUpOr);
DEFINE_FILTER_VARIANTS(Not, irs::Not, SetUpNot);

#undef DEFINE_FILTER_VARIANTS

int main(int argc, char** argv) {
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
