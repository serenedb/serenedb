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

// Microbenchmark: dictionary enumeration strategies over the burst trie.
//
// Decides whether ts_dict / multiterm drivers should compile every claimable
// filter to a DFA and ride the lockstep automaton iterator, or keep the
// seek-based paths (RandomOnly exact seek, seek_ge + bounded scan,
// seek-per-candidate IN merge). OpenFST costs are reported split:
//
//   *_AutomatonBuild -- constructing the DFA (MakeTermAcceptor /
//                       MakePrefixAcceptor / FromRegexp / IntersectAcceptors)
//   *_MatcherBuild   -- constructing the table matcher (CompiledAcceptor)
//   *_Walk           -- pure enumeration with everything prebuilt
//
// against the seek paths that need no build step at all. FullWalk and
// FullWalkPredicate bound the fallback (plain walk + per-term Accept).
//
// Dictionary: N unique keyword terms "<a..z><%05x>" in one segment, one doc
// per term; the first letter gives 26 disjoint prefix regions (~N/26 terms
// per single-letter prefix).

#include <benchmark/benchmark.h>

#include <filesystem>
#include <map>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/strings/str_format.h"
#include "basics/containers/bitset.hpp"
#include "basics/duckdb_engine.h"
#include "fst/arcsort.h"
#include "fst/minimize.h"
#include "iresearch/analysis/tokenizers.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/search/bitset_doc_iterator.hpp"
#include "iresearch/search/conjunction.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/term_iterator.hpp"
#include "iresearch/store/mmap_directory.hpp"
#include "iresearch/utils/automaton_utils.hpp"
#include "iresearch/utils/regexp_utils.hpp"
#include "iresearch/utils/string.hpp"

namespace {

constexpr irs::field_id kKwFieldId = 1;

struct KeywordField {
  irs::field_id Id() const noexcept { return id; }

  irs::Tokenizer& GetTokens() const {
    stream.reset(value);
    return stream;
  }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::Freq;
  }

  bool Write(irs::DataOutput&) const { return true; }

  irs::field_id id{irs::field_limits::invalid()};
  std::string_view value;
  mutable irs::StringTokenizer stream;
};

inline irs::bytes_view AsBytes(std::string_view s) noexcept {
  return irs::ViewCast<irs::byte_type>(s);
}

std::string TermAt(size_t i) {
  return absl::StrFormat("%c%05x", 'a' + static_cast<char>(i % 26), i);
}

struct CachedIndex {
  std::filesystem::path path;
  std::unique_ptr<irs::MMapDirectory> dir;
  irs::DirectoryReader reader;
  std::vector<std::string> terms;
};

const CachedIndex& IndexOf(size_t num_terms) {
  static std::map<size_t, CachedIndex> gCache;
  auto [it, added] = gCache.try_emplace(num_terms);
  auto& cached = it->second;
  if (!added) {
    return cached;
  }

  cached.path = std::filesystem::temp_directory_path() /
                absl::StrFormat("serenedb-bench-term-enum-%d", num_terms);
  std::filesystem::remove_all(cached.path);
  std::filesystem::create_directories(cached.path);
  cached.dir = std::make_unique<irs::MMapDirectory>(cached.path);

  cached.terms.reserve(num_terms);
  for (size_t i = 0; i < num_terms; ++i) {
    cached.terms.push_back(TermAt(i));
  }

  auto* db = &sdb::DuckDBEngine::Instance().instance();
  auto codec = irs::formats::Get("1_5simd");
  irs::IndexWriterOptions writer_opts;
  writer_opts.db = db;
  writer_opts.reader_options.db = db;
  writer_opts.column_options = [](irs::field_id) -> irs::ColumnOptions {
    return {};
  };
  auto writer =
    irs::IndexWriter::Make(*cached.dir, codec, irs::kOmCreate, writer_opts);

  KeywordField field{.id = kKwFieldId};
  {
    auto trx = writer->GetBatch();
    for (const auto& term : cached.terms) {
      field.value = term;
      auto doc = trx.Insert();
      doc.Insert(field);
    }
    trx.Commit();
  }
  writer->RefreshCommit();

  cached.reader =
    irs::DirectoryReader{*cached.dir, codec, irs::IndexReaderOptions{.db = db}};
  return cached;
}

const irs::TermReader& FieldOf(const CachedIndex& index) {
  const auto* field = index.reader[0].field(kKwFieldId);
  SDB_ASSERT(field);
  return *field;
}

size_t DrainMatcher(const irs::TermReader& field,
                    const irs::automaton_table_matcher& matcher) {
  auto it = field.iterator(matcher);
  size_t n = 0;
  while (it->next()) {
    benchmark::DoNotOptimize(it->value().data());
    ++n;
  }
  return n;
}

std::vector<std::string> SampleTerms(const CachedIndex& index, size_t count) {
  std::vector<std::string> sampled;
  sampled.reserve(count);
  const size_t step = index.terms.size() / count;
  for (size_t i = 0; i < count; ++i) {
    sampled.push_back(index.terms[i * step]);
  }
  return sampled;
}

void ApplySizes(benchmark::internal::Benchmark* b) {
  b->Arg(10'000)->Arg(500'000)->Unit(benchmark::kMicrosecond);
}

// -- exact term ---------------------------------------------------------

void ExactSeekRandomOnly(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const auto targets = SampleTerms(index, 1000);
  size_t i = 0;
  for (auto _ : state) {
    auto it = field.iterator(irs::SeekMode::RandomOnly);
    const bool found = it->seek(AsBytes(targets[i++ % targets.size()]));
    benchmark::DoNotOptimize(found);
  }
  state.SetItemsProcessed(state.iterations());
}

void ExactSeekNormal(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const auto targets = SampleTerms(index, 1000);
  size_t i = 0;
  for (auto _ : state) {
    auto it = field.iterator(irs::SeekMode::NORMAL);
    const bool found = it->seek(AsBytes(targets[i++ % targets.size()]));
    benchmark::DoNotOptimize(found);
  }
  state.SetItemsProcessed(state.iterations());
}

void ExactAutomatonBuild(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto targets = SampleTerms(index, 1000);
  size_t i = 0;
  for (auto _ : state) {
    auto a = irs::MakeTermAcceptor(AsBytes(targets[i++ % targets.size()]));
    benchmark::DoNotOptimize(a.NumStates());
  }
  state.SetItemsProcessed(state.iterations());
}

void ExactMatcherBuild(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto a = irs::MakeTermAcceptor(AsBytes(index.terms.front()));
  for (auto _ : state) {
    irs::automaton_table_matcher matcher{a, irs::kTestAutomatonProps};
    benchmark::DoNotOptimize(&matcher);
  }
  state.SetItemsProcessed(state.iterations());
}

void ExactLockstepWalk(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const auto targets = SampleTerms(index, 1000);
  std::vector<irs::CompiledAcceptor> compiled;
  compiled.reserve(targets.size());
  for (const auto& t : targets) {
    compiled.emplace_back(irs::MakeTermAcceptor(AsBytes(t)));
  }
  size_t i = 0;
  size_t produced = 0;
  for (auto _ : state) {
    produced += DrainMatcher(field, compiled[i++ % compiled.size()].matcher);
  }
  benchmark::DoNotOptimize(produced);
  state.SetItemsProcessed(state.iterations());
}

// -- prefix --------------------------------------------------------------

std::string PrefixOf(size_t len) {
  const std::string full = TermAt(0);
  return full.substr(0, len);
}

void PrefixSeekScan(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const std::string prefix = PrefixOf(static_cast<size_t>(state.range(1)));
  const auto target = AsBytes(prefix);
  size_t produced = 0;
  for (auto _ : state) {
    auto it = field.iterator(irs::SeekMode::NORMAL);
    size_t n = 0;
    if (irs::SeekResult::End != it->seek_ge(target) &&
        it->value().starts_with(target)) {
      ++n;
      while (it->next() && it->value().starts_with(target)) {
        benchmark::DoNotOptimize(it->value().data());
        ++n;
      }
    }
    produced += n;
  }
  state.counters["terms"] = benchmark::Counter(
    static_cast<double>(produced) / static_cast<double>(state.iterations()));
  state.SetItemsProcessed(static_cast<int64_t>(produced));
}

void PrefixAutomatonBuild(benchmark::State& state) {
  const std::string prefix = PrefixOf(static_cast<size_t>(state.range(1)));
  for (auto _ : state) {
    auto a = irs::MakePrefixAcceptor(AsBytes(prefix));
    benchmark::DoNotOptimize(a.NumStates());
  }
  state.SetItemsProcessed(state.iterations());
}

void PrefixMatcherBuild(benchmark::State& state) {
  const std::string prefix = PrefixOf(static_cast<size_t>(state.range(1)));
  const auto a = irs::MakePrefixAcceptor(AsBytes(prefix));
  for (auto _ : state) {
    irs::automaton_table_matcher matcher{a, irs::kTestAutomatonProps};
    benchmark::DoNotOptimize(&matcher);
  }
  state.SetItemsProcessed(state.iterations());
}

void PrefixLockstepWalk(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const std::string prefix = PrefixOf(static_cast<size_t>(state.range(1)));
  const irs::CompiledAcceptor compiled{
    irs::MakePrefixAcceptor(AsBytes(prefix))};
  size_t produced = 0;
  for (auto _ : state) {
    produced += DrainMatcher(field, compiled.matcher);
  }
  state.counters["terms"] = benchmark::Counter(
    static_cast<double>(produced) / static_cast<double>(state.iterations()));
  state.SetItemsProcessed(static_cast<int64_t>(produced));
}

void PrefixLockstepWithBuild(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const std::string prefix = PrefixOf(static_cast<size_t>(state.range(1)));
  size_t produced = 0;
  for (auto _ : state) {
    const irs::CompiledAcceptor compiled{
      irs::MakePrefixAcceptor(AsBytes(prefix))};
    produced += DrainMatcher(field, compiled.matcher);
  }
  state.counters["terms"] = benchmark::Counter(
    static_cast<double>(produced) / static_cast<double>(state.iterations()));
  state.SetItemsProcessed(static_cast<int64_t>(produced));
}

// -- range ----------------------------------------------------------------

std::pair<std::string, std::string> RangeBoundsOf(int width) {
  return {std::string(1, 'b'), std::string(1, static_cast<char>('b' + width))};
}

void RangeSeekScan(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const auto [min, max] = RangeBoundsOf(static_cast<int>(state.range(1)));
  const auto min_bytes = AsBytes(min);
  const auto max_bytes = AsBytes(max);
  size_t produced = 0;
  for (auto _ : state) {
    auto it = field.iterator(irs::SeekMode::NORMAL);
    size_t n = 0;
    if (irs::SeekResult::End != it->seek_ge(min_bytes)) {
      do {
        if (!(it->value() < max_bytes)) {
          break;
        }
        benchmark::DoNotOptimize(it->value().data());
        ++n;
      } while (it->next());
    }
    produced += n;
  }
  state.counters["terms"] = benchmark::Counter(
    static_cast<double>(produced) / static_cast<double>(state.iterations()));
  state.SetItemsProcessed(static_cast<int64_t>(produced));
}

void RangeAutomatonBuild(benchmark::State& state) {
  const auto [min, max] = RangeBoundsOf(static_cast<int>(state.range(1)));
  for (auto _ : state) {
    auto a = irs::MakeRangeAcceptor(AsBytes(min), AsBytes(max), true, false);
    benchmark::DoNotOptimize(a.NumStates());
  }
  state.SetItemsProcessed(state.iterations());
}

void RangeLockstepWalk(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const auto [min, max] = RangeBoundsOf(static_cast<int>(state.range(1)));
  const irs::CompiledAcceptor compiled{
    irs::MakeRangeAcceptor(AsBytes(min), AsBytes(max), true, false)};
  size_t produced = 0;
  for (auto _ : state) {
    produced += DrainMatcher(field, compiled.matcher);
  }
  state.counters["terms"] = benchmark::Counter(
    static_cast<double>(produced) / static_cast<double>(state.iterations()));
  state.SetItemsProcessed(static_cast<int64_t>(produced));
}

// -- IN (seek per candidate) ---------------------------------------------

void InSeeks(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const auto candidates =
    SampleTerms(index, static_cast<size_t>(state.range(1)));
  size_t produced = 0;
  for (auto _ : state) {
    auto it = field.iterator(irs::SeekMode::NORMAL);
    size_t n = 0;
    for (const auto& candidate : candidates) {
      if (it->seek(AsBytes(candidate))) {
        benchmark::DoNotOptimize(it->value().data());
        ++n;
      }
    }
    produced += n;
  }
  state.counters["terms"] = benchmark::Counter(
    static_cast<double>(produced) / static_cast<double>(state.iterations()));
  state.SetItemsProcessed(static_cast<int64_t>(produced));
}

// -- fused AND vs driver + predicate --------------------------------------

const irs::automaton& RegexpAcceptorFor() {
  static const auto kAcceptor = irs::FromRegexp(
    AsBytes(std::string_view{"a.*[02468ace]"}), irs::kDefaultMaxDfaStates);
  return kAcceptor;
}

void FusedIntersectBuild(benchmark::State& state) {
  const std::string prefix = PrefixOf(1);
  const auto lhs = irs::MakePrefixAcceptor(AsBytes(prefix));
  const auto& rhs = RegexpAcceptorFor();
  for (auto _ : state) {
    auto product = irs::IntersectAcceptors(lhs, rhs, irs::kDefaultMaxDfaStates);
    benchmark::DoNotOptimize(product.has_value());
  }
  state.SetItemsProcessed(state.iterations());
}

void FusedLockstepWalk(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const auto product =
    irs::IntersectAcceptors(irs::MakePrefixAcceptor(AsBytes(PrefixOf(1))),
                            RegexpAcceptorFor(), irs::kDefaultMaxDfaStates);
  const irs::CompiledAcceptor compiled{*product};
  size_t produced = 0;
  for (auto _ : state) {
    produced += DrainMatcher(field, compiled.matcher);
  }
  state.counters["terms"] = benchmark::Counter(
    static_cast<double>(produced) / static_cast<double>(state.iterations()));
  state.SetItemsProcessed(static_cast<int64_t>(produced));
}

void FusedDriverPlusPredicate(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const std::string prefix = PrefixOf(1);
  const auto target = AsBytes(prefix);
  const auto& predicate = RegexpAcceptorFor();
  size_t produced = 0;
  for (auto _ : state) {
    auto it = field.iterator(irs::SeekMode::NORMAL);
    size_t n = 0;
    if (irs::SeekResult::End != it->seek_ge(target)) {
      do {
        if (!it->value().starts_with(target)) {
          break;
        }
        if (bool(irs::Accept(predicate, it->value()))) {
          benchmark::DoNotOptimize(it->value().data());
          ++n;
        }
      } while (it->next());
    }
    produced += n;
  }
  state.counters["terms"] = benchmark::Counter(
    static_cast<double>(produced) / static_cast<double>(state.iterations()));
  state.SetItemsProcessed(static_cast<int64_t>(produced));
}

// -- baselines -------------------------------------------------------------

void FullWalk(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  size_t produced = 0;
  for (auto _ : state) {
    auto it = field.iterator(irs::SeekMode::NORMAL);
    size_t n = 0;
    while (it->next()) {
      benchmark::DoNotOptimize(it->value().data());
      ++n;
    }
    produced += n;
  }
  state.SetItemsProcessed(static_cast<int64_t>(produced));
}

void FullWalkPredicate(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const auto& predicate = RegexpAcceptorFor();
  size_t produced = 0;
  for (auto _ : state) {
    auto it = field.iterator(irs::SeekMode::NORMAL);
    size_t n = 0;
    while (it->next()) {
      if (bool(irs::Accept(predicate, it->value()))) {
        ++n;
      }
    }
    produced += n;
  }
  state.SetItemsProcessed(static_cast<int64_t>(produced));
}

BENCHMARK(ExactSeekRandomOnly)->Apply(ApplySizes);
BENCHMARK(ExactSeekNormal)->Apply(ApplySizes);
BENCHMARK(ExactAutomatonBuild)->Apply(ApplySizes);
BENCHMARK(ExactMatcherBuild)->Apply(ApplySizes);
BENCHMARK(ExactLockstepWalk)->Apply(ApplySizes);

BENCHMARK(PrefixSeekScan)
  ->ArgsProduct({{10'000, 500'000}, {1, 3}})
  ->Unit(benchmark::kMicrosecond);
BENCHMARK(PrefixAutomatonBuild)
  ->ArgsProduct({{10'000}, {1, 3}})
  ->Unit(benchmark::kMicrosecond);
BENCHMARK(PrefixMatcherBuild)
  ->ArgsProduct({{10'000}, {1, 3}})
  ->Unit(benchmark::kMicrosecond);
BENCHMARK(PrefixLockstepWalk)
  ->ArgsProduct({{10'000, 500'000}, {1, 3}})
  ->Unit(benchmark::kMicrosecond);

BENCHMARK(PrefixLockstepWithBuild)
  ->ArgsProduct({{10'000, 500'000}, {1, 3}})
  ->Unit(benchmark::kMicrosecond);

BENCHMARK(RangeSeekScan)
  ->ArgsProduct({{10'000, 500'000}, {1, 4}})
  ->Unit(benchmark::kMicrosecond);
BENCHMARK(RangeAutomatonBuild)
  ->ArgsProduct({{10'000}, {1, 4}})
  ->Unit(benchmark::kMicrosecond);
BENCHMARK(RangeLockstepWalk)
  ->ArgsProduct({{10'000, 500'000}, {1, 4}})
  ->Unit(benchmark::kMicrosecond);

BENCHMARK(InSeeks)
  ->ArgsProduct({{10'000, 500'000}, {10, 1000}})
  ->Unit(benchmark::kMicrosecond);

BENCHMARK(FusedIntersectBuild)->Arg(10'000)->Unit(benchmark::kMicrosecond);
BENCHMARK(FusedLockstepWalk)->Apply(ApplySizes);
BENCHMARK(FusedDriverPlusPredicate)->Apply(ApplySizes);

void WhereProbeWalk(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const auto& seg = index.reader[0];
  const auto docs_count = seg.docs_count();
  const size_t words =
    irs::bitset::bits_to_words(docs_count + irs::doc_limits::min());
  std::vector<irs::bitset::word_t> set(words, ~irs::bitset::word_t{0});
  size_t produced = 0;
  for (auto _ : state) {
    auto it = field.iterator(irs::SeekMode::NORMAL);
    size_t n = 0;
    while (it->next()) {
      it->read();
      std::vector<irs::ScoreAdapter> itrs;
      itrs.emplace_back(it->postings(irs::IndexFeatures::None));
      itrs.emplace_back(irs::memory::make_managed<irs::BitsetDocIterator>(
        set.data(), set.data() + words));
      auto docs = irs::MakeConjunction(irs::ScoreMergeType::Noop, {},
                                       docs_count, std::move(itrs));
      n += !irs::doc_limits::eof(docs->advance());
    }
    produced += n;
  }
  state.SetItemsProcessed(static_cast<int64_t>(produced));
}

BENCHMARK(FullWalk)->Apply(ApplySizes);
BENCHMARK(FullWalkPredicate)->Apply(ApplySizes);
BENCHMARK(WhereProbeWalk)->Apply(ApplySizes);

// -- compiled TermIterator wrappers vs the raw loops above ------------------

void ExactIterator(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const auto targets = SampleTerms(index, 1000);
  irs::ByTerm filter;
  size_t i = 0;
  for (auto _ : state) {
    filter.mutable_options()->term = AsBytes(targets[i++ % targets.size()]);
    auto it = filter.CompileTermIterator(field);
    size_t n = 0;
    while (it->next()) {
      benchmark::DoNotOptimize(it->value().data());
      ++n;
    }
    benchmark::DoNotOptimize(n);
  }
  state.SetItemsProcessed(state.iterations());
}

void PrefixIteratorScan(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const std::string prefix = PrefixOf(static_cast<size_t>(state.range(1)));
  irs::ByPrefix prefix_filter;
  prefix_filter.mutable_options()->term = AsBytes(prefix);
  size_t produced = 0;
  for (auto _ : state) {
    auto it = prefix_filter.CompileTermIterator(field);
    size_t n = 0;
    while (it->next()) {
      benchmark::DoNotOptimize(it->value().data());
      ++n;
    }
    produced += n;
  }
  state.counters["terms"] = benchmark::Counter(
    static_cast<double>(produced) / static_cast<double>(state.iterations()));
  state.SetItemsProcessed(static_cast<int64_t>(produced));
}

void FullWalkFilteredIterator(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const auto& predicate = RegexpAcceptorFor();
  size_t produced = 0;
  for (auto _ : state) {
    irs::FilteredTermIterator it{
      field.iterator(irs::SeekMode::NORMAL),
      irs::MakeTermPredicate([&](irs::bytes_view term) {
        return bool(irs::Accept(predicate, term));
      })};
    size_t n = 0;
    while (it.next()) {
      benchmark::DoNotOptimize(it.value().data());
      ++n;
    }
    produced += n;
  }
  state.SetItemsProcessed(static_cast<int64_t>(produced));
}

// -- OR fusion: N separate child enumerations vs one union DFA --------------

std::vector<std::string> DisjointPrefixes(size_t n) {
  std::vector<std::string> prefixes;
  prefixes.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    prefixes.emplace_back(1, static_cast<char>('a' + 2 * i));
  }
  return prefixes;
}

std::vector<std::string> RegexpPatternsFor(size_t n) {
  std::vector<std::string> patterns;
  patterns.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    patterns.push_back(
      absl::StrFormat("%c.*[02468ace]", static_cast<char>('a' + 2 * i)));
  }
  return patterns;
}

std::vector<irs::automaton> RegexpAcceptorsFor(size_t n) {
  std::vector<irs::automaton> acceptors;
  acceptors.reserve(n);
  for (const auto& pattern : RegexpPatternsFor(n)) {
    acceptors.push_back(
      irs::FromRegexp(AsBytes(pattern), irs::kDefaultMaxDfaStates));
  }
  return acceptors;
}

irs::automaton RegexUnionOf(std::span<const std::string> patterns) {
  std::string rendered;
  for (const auto& pattern : patterns) {
    rendered += rendered.empty() ? "(?:" : "|(?:";
    rendered += pattern;
    rendered += ')';
  }
  auto dfa = irs::FromRegexp(AsBytes(rendered), irs::kDefaultMaxDfaStates);
  SDB_ASSERT(dfa.NumStates() != 0);
  return dfa;
}

void OrPrefixesSeparateSeekScans(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const auto prefixes = DisjointPrefixes(static_cast<size_t>(state.range(1)));
  size_t produced = 0;
  irs::ByPrefix prefix_filter;
  for (auto _ : state) {
    size_t n = 0;
    for (const auto& prefix : prefixes) {
      prefix_filter.mutable_options()->term = AsBytes(prefix);
      auto it = prefix_filter.CompileTermIterator(field);
      while (it->next()) {
        benchmark::DoNotOptimize(it->value().data());
        ++n;
      }
    }
    produced += n;
  }
  state.counters["terms"] = benchmark::Counter(
    static_cast<double>(produced) / static_cast<double>(state.iterations()));
  state.SetItemsProcessed(static_cast<int64_t>(produced));
}

std::vector<std::string> PrefixPatternsFor(size_t n) {
  std::vector<std::string> patterns;
  patterns.reserve(n);
  for (const auto& prefix : DisjointPrefixes(n)) {
    patterns.push_back(prefix + ".*");
  }
  return patterns;
}

void OrPrefixesUnionBuild(benchmark::State& state) {
  const auto patterns = PrefixPatternsFor(static_cast<size_t>(state.range(1)));
  for (auto _ : state) {
    const irs::CompiledAcceptor compiled{RegexUnionOf(patterns)};
    benchmark::DoNotOptimize(&compiled.matcher);
  }
  state.SetItemsProcessed(state.iterations());
}

void OrPrefixesFusedWalk(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const irs::CompiledAcceptor compiled{
    RegexUnionOf(PrefixPatternsFor(static_cast<size_t>(state.range(1))))};
  size_t produced = 0;
  for (auto _ : state) {
    produced += DrainMatcher(field, compiled.matcher);
  }
  state.counters["terms"] = benchmark::Counter(
    static_cast<double>(produced) / static_cast<double>(state.iterations()));
  state.SetItemsProcessed(static_cast<int64_t>(produced));
}

void OrRegexpsSeparateWalks(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  auto children = RegexpAcceptorsFor(static_cast<size_t>(state.range(1)));
  std::vector<irs::CompiledAcceptor> compiled;
  compiled.reserve(children.size());
  for (auto& a : children) {
    compiled.emplace_back(std::move(a));
  }
  size_t produced = 0;
  for (auto _ : state) {
    size_t n = 0;
    for (const auto& c : compiled) {
      n += DrainMatcher(field, c.matcher);
    }
    produced += n;
  }
  state.counters["terms"] = benchmark::Counter(
    static_cast<double>(produced) / static_cast<double>(state.iterations()));
  state.SetItemsProcessed(static_cast<int64_t>(produced));
}

void OrRegexpsUnionBuild(benchmark::State& state) {
  const auto patterns = RegexpPatternsFor(static_cast<size_t>(state.range(1)));
  for (auto _ : state) {
    const irs::CompiledAcceptor compiled{RegexUnionOf(patterns)};
    benchmark::DoNotOptimize(&compiled.matcher);
  }
  state.SetItemsProcessed(state.iterations());
}

std::vector<std::string> OverlappingRegexpPatterns() {
  return {"a.*[0-7]", "a.*[4-9abcdef]"};
}

std::vector<irs::automaton> OverlappingRegexpAcceptors() {
  std::vector<irs::automaton> acceptors;
  for (const auto& pattern : OverlappingRegexpPatterns()) {
    acceptors.push_back(
      irs::FromRegexp(AsBytes(pattern), irs::kDefaultMaxDfaStates));
  }
  return acceptors;
}

void OrRegexpsOverlapSeparateWalks(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  auto children = OverlappingRegexpAcceptors();
  std::vector<irs::CompiledAcceptor> compiled;
  compiled.reserve(children.size());
  for (auto& a : children) {
    compiled.emplace_back(std::move(a));
  }
  size_t produced = 0;
  for (auto _ : state) {
    size_t n = 0;
    for (const auto& c : compiled) {
      n += DrainMatcher(field, c.matcher);
    }
    produced += n;
  }
  state.counters["terms"] = benchmark::Counter(
    static_cast<double>(produced) / static_cast<double>(state.iterations()));
  state.SetItemsProcessed(static_cast<int64_t>(produced));
}

void OrRegexpsOverlapFusedWalk(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const irs::CompiledAcceptor compiled{
    RegexUnionOf(OverlappingRegexpPatterns())};
  size_t produced = 0;
  for (auto _ : state) {
    produced += DrainMatcher(field, compiled.matcher);
  }
  state.counters["terms"] = benchmark::Counter(
    static_cast<double>(produced) / static_cast<double>(state.iterations()));
  state.SetItemsProcessed(static_cast<int64_t>(produced));
}

void OrRegexpsFusedWalk(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const irs::CompiledAcceptor compiled{
    RegexUnionOf(RegexpPatternsFor(static_cast<size_t>(state.range(1))))};
  size_t produced = 0;
  for (auto _ : state) {
    produced += DrainMatcher(field, compiled.matcher);
  }
  state.counters["terms"] = benchmark::Counter(
    static_cast<double>(produced) / static_cast<double>(state.iterations()));
  state.SetItemsProcessed(static_cast<int64_t>(produced));
}

BENCHMARK(ExactIterator)->Apply(ApplySizes);
BENCHMARK(PrefixIteratorScan)
  ->ArgsProduct({{10'000, 500'000}, {1, 3}})
  ->Unit(benchmark::kMicrosecond);
BENCHMARK(FullWalkFilteredIterator)->Apply(ApplySizes);

BENCHMARK(OrPrefixesSeparateSeekScans)
  ->ArgsProduct({{10'000, 500'000}, {2, 4, 8}})
  ->Unit(benchmark::kMicrosecond);
BENCHMARK(OrPrefixesUnionBuild)
  ->ArgsProduct({{10'000}, {2, 4, 8}})
  ->Unit(benchmark::kMicrosecond);
BENCHMARK(OrPrefixesFusedWalk)
  ->ArgsProduct({{10'000, 500'000}, {2, 4, 8}})
  ->Unit(benchmark::kMicrosecond);
BENCHMARK(OrRegexpsSeparateWalks)
  ->ArgsProduct({{10'000, 500'000}, {2, 4, 8}})
  ->Unit(benchmark::kMicrosecond);
BENCHMARK(OrRegexpsUnionBuild)
  ->ArgsProduct({{10'000}, {2, 4, 8}})
  ->Unit(benchmark::kMicrosecond);
BENCHMARK(OrRegexpsOverlapSeparateWalks)->Apply(ApplySizes);
BENCHMARK(OrRegexpsOverlapFusedWalk)->Apply(ApplySizes);
BENCHMARK(OrRegexpsFusedWalk)
  ->ArgsProduct({{10'000, 500'000}, {2, 4, 8}})
  ->Unit(benchmark::kMicrosecond);

}  // namespace

int main(int argc, char** argv) {
  irs::formats::Init();
  sdb::DuckDBEngine::Instance().Initialize();

  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();

  sdb::DuckDBEngine::Instance().Shutdown();
  return 0;
}
