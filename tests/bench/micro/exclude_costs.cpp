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

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <iresearch/analysis/delimited_tokenizer.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/detail/exclude_block.hpp>
#include <iresearch/search/detail/exclusion_of.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/string.hpp>
#include <map>
#include <memory>
#include <span>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "insert_field.hpp"

namespace {

using irs::detail::ExcludeForm;
using irs::detail::ExcludeUse;
using irs::detail::PostingClause;

constexpr irs::field_id kBodyId = 1;
constexpr uint32_t kDocs = 1u << 20;
constexpr uint32_t kBlock = irs::doc_limits::kBlockSize;
constexpr int64_t kExcluded[] = {16, 1024, 16384, 262144, 524288};

uint64_t Mix(uint64_t x) noexcept {
  x += 0x9E3779B97F4A7C15ULL;
  x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
  x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
  return x ^ (x >> 31);
}

bool Picked(uint64_t i, uint64_t salt, uint64_t count) noexcept {
  return Mix(i * 0x100000001B3ULL + salt) % kDocs < count;
}

std::string ExcludedTerm(int64_t count) { return "x" + std::to_string(count); }

struct BodyField {
  irs::field_id Id() const noexcept { return kBodyId; }

  irs::analysis::Tokenizer& GetTokens() const { return stream; }

  std::string_view Value() const noexcept { return value; }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::Freq;
  }

  bool Write(irs::DataOutput&) const { return true; }

  std::string value;
  mutable irs::analysis::DelimitedTokenizer stream{" "};
};

struct Index {
  std::filesystem::path path;
  std::unique_ptr<irs::MMapDirectory> dir;
  irs::DirectoryReader reader;
};

const Index& IndexOf() {
  static Index index;
  if (index.dir) {
    return index;
  }
  index.path =
    std::filesystem::temp_directory_path() / "serenedb-bench-exclude-costs";
  std::filesystem::remove_all(index.path);
  std::filesystem::create_directories(index.path);
  index.dir = std::make_unique<irs::MMapDirectory>(index.path);

  auto* db = &irs::DuckDBEngine::Instance().instance();
  irs::IndexWriterOptions opts;
  opts.db = db;
  opts.reader_options.db = db;
  opts.column_options = [](irs::field_id) -> irs::ColumnOptions { return {}; };
  auto writer =
    irs::IndexWriter::Make(*index.dir, irs::kOmCreate, std::move(opts));
  {
    auto trx = writer->GetBatch();
    BodyField field;
    for (uint64_t i = 0; i != kDocs; ++i) {
      field.value = "a";
      for (const auto count : kExcluded) {
        if (Picked(i, static_cast<uint64_t>(count),
                   static_cast<uint64_t>(count))) {
          field.value += ' ';
          field.value += ExcludedTerm(count);
        }
      }
      auto doc = trx.Insert();
      tests::InsertField(doc, field);
    }
    trx.Commit();
  }
  writer->RefreshCommit();
  index.reader =
    irs::DirectoryReader{*index.dir, irs::IndexReaderOptions{.db = db}};
  SDB_ASSERT(index.reader.size() == 1);
  return index;
}

const std::vector<irs::doc_id_t>& CandidatesOf(uint64_t lead, uint32_t ratio) {
  static std::map<std::pair<uint64_t, uint32_t>, std::vector<irs::doc_id_t>>
    cache;
  auto [it, added] = cache.try_emplace({lead, ratio});
  if (added) {
    uint64_t seen = 0;
    for (uint64_t i = 0; i != kDocs; ++i) {
      if (Picked(i, 0xC0FFEE, lead) && seen++ % ratio == 0) {
        it->second.push_back(
          static_cast<irs::doc_id_t>(irs::doc_limits::min() + i));
      }
    }
  }
  return it->second;
}

struct Runner {
  virtual ~Runner() = default;
  virtual uint64_t Run(std::span<const irs::doc_id_t> candidates,
                       ExcludeUse use, uint32_t chunk) = 0;
};

template<typename Exclude>
struct RunnerOf final : Runner {
  template<typename Args>
  explicit RunnerOf(Args&& args)
    : exclude{std::make_from_tuple<Exclude>(std::forward<Args>(args))} {}

  uint64_t Run(std::span<const irs::doc_id_t> candidates, ExcludeUse use,
               uint32_t chunk) final {
    uint64_t kept = 0;
    if (use == ExcludeUse::PerDoc) {
      for (const auto doc : candidates) {
        kept += static_cast<uint64_t>(!irs::detail::IsExcluded(exclude, doc));
      }
      return kept;
    }
    irs::doc_id_t docs[kBlock];
    irs::score_t scores[kBlock]{};
    for (size_t at = 0; at < candidates.size(); at += chunk) {
      const auto len =
        static_cast<uint32_t>(std::min<size_t>(chunk, candidates.size() - at));
      std::copy_n(candidates.data() + at, len, docs);
      kept += irs::detail::ExcludeBlock(exclude, docs, scores, len);
    }
    return kept;
  }

  Exclude exclude;
};

using Window = irs::probe::BooleanWindow<
  irs::detail::OrGroup<irs::fill::SetLeaves<irs::fill::Erased>>>;

std::unique_ptr<Runner> Build(ExcludeForm form,
                              std::span<const PostingClause> metas,
                              const irs::SubReader& segment,
                              uint64_t candidates) {
  using Result = std::unique_ptr<Runner>;
  const auto make = [&]<typename Exclude>(auto&& args) -> Result {
    return std::make_unique<RunnerOf<Exclude>>(
      std::forward<decltype(args)>(args));
  };
  switch (form) {
    case ExcludeForm::Probes:
      return irs::detail::BuildExcludeProbes<Result, void>(
        metas, {}, nullptr, segment, candidates, make);
    case ExcludeForm::Bitset:
      return irs::detail::BuildExcludeBitset<Result>(
        metas, {}, nullptr, segment, *irs::detail::SegmentDoc(segment),
        [&](auto&& set) -> Result {
          return make.template operator()<irs::probe::BitsetDocs>(
            std::forward_as_tuple(std::forward<decltype(set)>(set)));
        });
    case ExcludeForm::Window:
      break;
  }
  return irs::detail::BuildExcludeFills<Result>(
    metas, {}, nullptr, segment, [&](auto&& leaves) -> Result {
      return make.template operator()<Window>(std::forward_as_tuple(
        std::piecewise_construct, std::forward<decltype(leaves)>(leaves)));
    });
}

struct Point {
  ExcludeUse use;
  uint64_t lead;
  uint32_t ratio;
  int64_t excluded;
};

struct Setup {
  const irs::SubReader& segment;
  PostingClause clause;
  const std::vector<irs::doc_id_t>& candidates;
  uint64_t span;
};

Setup SetupOf(const Point& p) {
  const auto& segment = IndexOf().reader[0];
  const auto* field = segment.field(kBodyId);
  SDB_ASSERT(field != nullptr);
  const auto term = ExcludedTerm(p.excluded);
  const auto meta =
    field->Lookup(irs::ViewCast<irs::byte_type>(std::string_view{term}));
  const auto& candidates = CandidatesOf(p.lead, p.ratio);
  return {segment, PostingClause{irs::TermState{field, meta}}, candidates,
          p.use == ExcludeUse::PerBlock ? p.lead : candidates.size()};
}

ExcludeForm PickOf(const Point& p, const Setup& s) {
  const irs::detail::ExcludeCosts<PostingClause> costs{
    std::span{&s.clause, 1},
    {},
    s.candidates.size(),
    s.span,
    static_cast<irs::doc_id_t>(s.segment.docs_count()),
    p.use};
  return costs.Probed(true);
}

Point PointOf(const benchmark::State& state) {
  return {static_cast<ExcludeUse>(state.range(0)),
          uint64_t{1} << state.range(1), static_cast<uint32_t>(state.range(2)),
          state.range(3)};
}

void BmExclude(benchmark::State& state) {
  const auto p = PointOf(state);
  const auto form = static_cast<ExcludeForm>(state.range(4));
  const auto s = SetupOf(p);
  uint64_t kept = 0;
  for (auto _ : state) {
    auto runner =
      Build(form, std::span{&s.clause, 1}, s.segment, s.candidates.size());
    kept += runner->Run(s.candidates, p.use, kBlock / p.ratio);
  }
  benchmark::DoNotOptimize(kept);
  state.counters["pick"] = static_cast<double>(PickOf(p, s));
  state.counters["cand"] = static_cast<double>(s.candidates.size());
  state.counters["excl"] =
    static_cast<double>(s.clause.state.cookie.docs_count);
  state.SetItemsProcessed(state.iterations() *
                          static_cast<int64_t>(s.candidates.size()));
}

void BmCheck(benchmark::State& state) {
  for (const auto use : {ExcludeUse::PerDoc, ExcludeUse::PerBlock}) {
    for (const auto excluded : kExcluded) {
      for (const int64_t lead : {8, 16}) {
        const Point p{use, uint64_t{1} << lead, 1, excluded};
        const auto s = SetupOf(p);
        uint64_t expected = 0;
        for (const auto form :
             {ExcludeForm::Probes, ExcludeForm::Window, ExcludeForm::Bitset}) {
          const auto kept =
            Build(form, std::span{&s.clause, 1}, s.segment, s.candidates.size())
              ->Run(s.candidates, use, kBlock);
          if (form == ExcludeForm::Probes) {
            expected = kept;
          } else if (kept != expected) {
            state.SkipWithError("forms disagree");
            return;
          }
        }
      }
    }
  }
  for (auto _ : state) {
  }
}

void Args(benchmark::internal::Benchmark* b) {
  for (const int64_t use : {0, 1}) {
    for (const int64_t lead : {8, 12, 16, 19}) {
      for (const int64_t ratio : {1, 16}) {
        if (ratio != 1 && lead < 12) {
          continue;
        }
        for (const auto excluded : kExcluded) {
          for (const int64_t form : {0, 1, 2}) {
            b->Args({use, lead, ratio, excluded, form});
          }
        }
      }
    }
  }
  b->ArgNames({"use", "lead", "ratio", "excl", "form"});
}

BENCHMARK(BmCheck)->Iterations(1);
BENCHMARK(BmExclude)->Apply(Args)->Unit(benchmark::kMicrosecond);

}  // namespace

static int Main(int argc, char** argv) {
  irs::DuckDBEngine::Instance().Initialize();

  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();

  irs::DuckDBEngine::Instance().Shutdown();
  return 0;
}

[[maybe_unused]] static const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);
