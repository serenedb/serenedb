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

#include <filesystem>
#include <iresearch/analysis/keyword_tokenizer.hpp>
#include <iresearch/formats/term_reader.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/detail/term_acceptor.hpp>
#include <iresearch/search/detail/term_iterator.hpp>
#include <iresearch/search/detail/term_predicate.hpp>
#include <iresearch/search/filters/prefix_filter.hpp>
#include <iresearch/search/filters/range_filter.hpp>
#include <iresearch/search/filters/regexp_filter.hpp>
#include <iresearch/search/filters/term_filter.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/regexp_acceptor.hpp>
#include <iresearch/utils/regexp_utils.hpp>
#include <iresearch/utils/string.hpp>
#include <map>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/strings/str_format.h"
#include "insert_field.hpp"

namespace {

constexpr irs::field_id kKwFieldId = 1;

struct KeywordField {
  irs::field_id Id() const noexcept { return id; }

  irs::analysis::Tokenizer& GetTokens() const { return stream; }

  std::string_view Value() const noexcept { return value; }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::Freq;
  }

  bool Write(irs::DataOutput&) const { return true; }

  irs::field_id id{irs::field_limits::invalid()};
  std::string_view value;
  mutable irs::KeywordTokenizer stream;
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

  auto* db = &irs::DuckDBEngine::Instance().instance();
  irs::IndexWriterOptions writer_opts;
  writer_opts.db = db;
  writer_opts.reader_options.db = db;
  writer_opts.column_options = [](irs::field_id) -> irs::ColumnOptions {
    return {};
  };
  auto writer =
    irs::IndexWriter::Make(*cached.dir, irs::kOmCreate, std::move(writer_opts));

  KeywordField field{.id = kKwFieldId};
  {
    auto trx = writer->GetBatch();
    for (const auto& term : cached.terms) {
      field.value = term;
      auto doc = trx.Insert();
      tests::InsertField(doc, field);
    }
    trx.Commit();
  }
  writer->RefreshCommit();

  cached.reader =
    irs::DirectoryReader{*cached.dir, irs::IndexReaderOptions{.db = db}};
  return cached;
}

const irs::TermReader& FieldOf(const CachedIndex& index) {
  const auto* field = index.reader[0].field(kKwFieldId);
  SDB_ASSERT(field);
  return *field;
}

template<typename Acceptor>
size_t DrainAcceptor(const irs::TermReader& field, const Acceptor& acceptor) {
  auto it = field.iterator(acceptor);
  size_t n = 0;
  while (it->next()) {
    benchmark::DoNotOptimize(it->value().data());
    ++n;
  }
  return n;
}

size_t DrainSource(const irs::TermReader& field,
                   const irs::TermAcceptorSource& source) {
  auto it = source.Iterator(field);
  size_t n = 0;
  while (it->next()) {
    benchmark::DoNotOptimize(it->value().data());
    ++n;
  }
  return n;
}

size_t DrainIterator(irs::TermIterator& it) {
  size_t n = 0;
  while (it.next()) {
    benchmark::DoNotOptimize(it.value().data());
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

void ReportTerms(benchmark::State& state, size_t produced) {
  state.counters["terms"] = benchmark::Counter(
    static_cast<double>(produced) / static_cast<double>(state.iterations()));
  state.SetItemsProcessed(static_cast<int64_t>(produced));
}

void ApplySizes(benchmark::internal::Benchmark* b) {
  b->Arg(10'000)->Arg(500'000)->Unit(benchmark::kMicrosecond);
}

irs::RegexpAcceptor WildcardAcceptorOf(std::string_view pattern) {
  return irs::RegexpAcceptor{irs::RegexpAcceptor::WildcardTag{},
                             AsBytes(pattern)};
}

void ExactSeekRandomOnly(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const auto targets = SampleTerms(index, 1000);
  size_t i = 0;
  for (auto _ : state) {
    auto it = field.iterator();
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
    auto it = field.iterator();
    const bool found = it->seek(AsBytes(targets[i++ % targets.size()]));
    benchmark::DoNotOptimize(found);
  }
  state.SetItemsProcessed(state.iterations());
}

void ExactAcceptorBuild(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto targets = SampleTerms(index, 1000);
  size_t i = 0;
  for (auto _ : state) {
    auto a = WildcardAcceptorOf(targets[i++ % targets.size()]);
    benchmark::DoNotOptimize(a.ok());
  }
  state.SetItemsProcessed(state.iterations());
}

void ExactSourceBuild(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto targets = SampleTerms(index, 1000);
  size_t i = 0;
  for (auto _ : state) {
    auto source = irs::MakePatternSource(AsBytes(targets[i++ % targets.size()]),
                                         irs::PatternKind::Wildcard);
    benchmark::DoNotOptimize(source->ok());
  }
  state.SetItemsProcessed(state.iterations());
}

void ExactAcceptorWalk(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const auto targets = SampleTerms(index, 1000);
  std::vector<std::unique_ptr<irs::RegexpAcceptor>> compiled;
  compiled.reserve(targets.size());
  for (const auto& t : targets) {
    compiled.emplace_back(std::make_unique<irs::RegexpAcceptor>(
      irs::RegexpAcceptor::WildcardTag{}, AsBytes(t)));
  }
  size_t i = 0;
  size_t produced = 0;
  for (auto _ : state) {
    produced += DrainAcceptor(field, *compiled[i++ % compiled.size()]);
  }
  benchmark::DoNotOptimize(produced);
  state.SetItemsProcessed(state.iterations());
}

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
    auto it = field.iterator();
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
  ReportTerms(state, produced);
}

void PrefixAcceptorBuild(benchmark::State& state) {
  const std::string pattern =
    PrefixOf(static_cast<size_t>(state.range(1))) + '%';
  for (auto _ : state) {
    auto a = WildcardAcceptorOf(pattern);
    benchmark::DoNotOptimize(a.ok());
  }
  state.SetItemsProcessed(state.iterations());
}

void PrefixSourceBuild(benchmark::State& state) {
  const std::string pattern =
    PrefixOf(static_cast<size_t>(state.range(1))) + '%';
  for (auto _ : state) {
    auto source =
      irs::MakePatternSource(AsBytes(pattern), irs::PatternKind::Wildcard);
    benchmark::DoNotOptimize(source->ok());
  }
  state.SetItemsProcessed(state.iterations());
}

void PrefixAcceptorWalk(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const std::string pattern =
    PrefixOf(static_cast<size_t>(state.range(1))) + '%';
  const auto compiled = WildcardAcceptorOf(pattern);
  size_t produced = 0;
  for (auto _ : state) {
    produced += DrainAcceptor(field, compiled);
  }
  ReportTerms(state, produced);
}

void PrefixAcceptorWithBuild(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const std::string pattern =
    PrefixOf(static_cast<size_t>(state.range(1))) + '%';
  size_t produced = 0;
  for (auto _ : state) {
    const auto compiled = WildcardAcceptorOf(pattern);
    produced += DrainAcceptor(field, compiled);
  }
  ReportTerms(state, produced);
}

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
    auto it = field.iterator();
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
  ReportTerms(state, produced);
}

void RangeBoundedWalk(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const auto [min, max] = RangeBoundsOf(static_cast<int>(state.range(1)));
  size_t produced = 0;
  for (auto _ : state) {
    const auto predicate = irs::MakeTermPredicate(irs::AcceptAllTerms{});
    irs::BoundedTermIterator it{field.iterator(), AsBytes(min), AsBytes(max),
                                predicate.get()};
    size_t n = 0;
    while (it.next()) {
      benchmark::DoNotOptimize(it.value().data());
      ++n;
    }
    produced += n;
  }
  ReportTerms(state, produced);
}

void RangeIteratorScan(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const auto [min, max] = RangeBoundsOf(static_cast<int>(state.range(1)));
  irs::ByRange filter;
  auto& range = filter.mutable_options()->range;
  range.min = AsBytes(min);
  range.min_type = irs::BoundType::Inclusive;
  range.max = AsBytes(max);
  range.max_type = irs::BoundType::Exclusive;
  size_t produced = 0;
  for (auto _ : state) {
    auto it = filter.CompileTermIterator(field);
    produced += DrainIterator(*it);
  }
  ReportTerms(state, produced);
}

void InSeeks(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const auto candidates =
    SampleTerms(index, static_cast<size_t>(state.range(1)));
  size_t produced = 0;
  for (auto _ : state) {
    auto it = field.iterator();
    size_t n = 0;
    for (const auto& candidate : candidates) {
      if (it->seek(AsBytes(candidate))) {
        benchmark::DoNotOptimize(it->value().data());
        ++n;
      }
    }
    produced += n;
  }
  ReportTerms(state, produced);
}

constexpr std::string_view kRegexpPattern = "a.*[02468ace]";

const irs::RegexpAcceptor& RegexpAcceptorFor() {
  static const irs::RegexpAcceptor kAcceptor{AsBytes(kRegexpPattern)};
  return kAcceptor;
}

void RegexpAcceptorBuild(benchmark::State& state) {
  for (auto _ : state) {
    const irs::RegexpAcceptor a{AsBytes(kRegexpPattern)};
    benchmark::DoNotOptimize(a.ok());
  }
  state.SetItemsProcessed(state.iterations());
}

void RegexpAcceptorBuildScaling(benchmark::State& state) {
  const auto words = static_cast<size_t>(state.range(0));
  const bool contains = state.range(1) != 0;
  std::string pattern = contains ? ".*(" : "(";
  for (size_t i = 0; i != words; ++i) {
    absl::StrAppendFormat(&pattern, "%sw%dx", i == 0 ? "" : "|",
                          (i * 7919) % 100003);
  }
  pattern += contains ? ").*" : ")";
  for (auto _ : state) {
    const irs::RegexpAcceptor a{AsBytes(pattern)};
    benchmark::DoNotOptimize(a.ok());
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(pattern.size()));
}

void RegexpSourceBuild(benchmark::State& state) {
  for (auto _ : state) {
    auto source = irs::MakePatternSource(AsBytes(kRegexpPattern),
                                         irs::PatternKind::RegexpPerl);
    benchmark::DoNotOptimize(source->ok());
  }
  state.SetItemsProcessed(state.iterations());
}

void RegexpAcceptorWalk(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  size_t produced = 0;
  for (auto _ : state) {
    produced += DrainAcceptor(field, RegexpAcceptorFor());
  }
  ReportTerms(state, produced);
}

irs::TermAcceptorSource::ptr PrefixDriverSource() {
  return irs::MakePatternSource(AsBytes(PrefixOf(1) + '%'),
                                irs::PatternKind::Wildcard);
}

irs::TermBounds PrefixDriverBounds() {
  const auto prefix = PrefixOf(1);
  return {.lower = irs::bstring{AsBytes(prefix)},
          .upper = irs::UpperBoundOf(AsBytes(prefix))};
}

void FusedSourceBuild(benchmark::State& state) {
  for (auto _ : state) {
    auto source = irs::MakeConjunctionSource(
      PrefixDriverSource(), PrefixDriverBounds(),
      irs::CreateByRegexp(kKwFieldId, AsBytes(kRegexpPattern)));
    benchmark::DoNotOptimize(source->ok());
  }
  state.SetItemsProcessed(state.iterations());
}

void FusedSourceWalk(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const auto source = irs::MakeConjunctionSource(
    PrefixDriverSource(), PrefixDriverBounds(),
    irs::CreateByRegexp(kKwFieldId, AsBytes(kRegexpPattern)));
  size_t produced = 0;
  for (auto _ : state) {
    produced += DrainSource(field, *source);
  }
  ReportTerms(state, produced);
}

void FusedBoundsOnlyWalk(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const auto source = irs::MakeConjunctionSource(
    nullptr, PrefixDriverBounds(),
    irs::CreateByRegexp(kKwFieldId, AsBytes(kRegexpPattern)));
  size_t produced = 0;
  for (auto _ : state) {
    produced += DrainSource(field, *source);
  }
  ReportTerms(state, produced);
}

void FusedDriverPlusPredicate(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const std::string prefix = PrefixOf(1);
  const auto target = AsBytes(prefix);
  const auto& predicate = RegexpAcceptorFor();
  size_t produced = 0;
  for (auto _ : state) {
    auto it = field.iterator();
    size_t n = 0;
    if (irs::SeekResult::End != it->seek_ge(target)) {
      do {
        if (!it->value().starts_with(target)) {
          break;
        }
        if (predicate.Matches(it->value())) {
          benchmark::DoNotOptimize(it->value().data());
          ++n;
        }
      } while (it->next());
    }
    produced += n;
  }
  ReportTerms(state, produced);
}

void FusedSeparateWalks(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const auto driver = WildcardAcceptorOf(PrefixOf(1) + '%');
  const auto& residual = RegexpAcceptorFor();
  size_t produced = 0;
  for (auto _ : state) {
    std::vector<irs::bstring> lhs;
    {
      auto it = field.iterator(driver);
      while (it->next()) {
        lhs.emplace_back(it->value());
      }
    }
    size_t n = 0;
    {
      auto it = field.iterator(residual);
      auto cursor = lhs.begin();
      while (it->next()) {
        const auto key = it->value();
        while (cursor != lhs.end() && irs::bytes_view{*cursor} < key) {
          ++cursor;
        }
        if (cursor != lhs.end() && irs::bytes_view{*cursor} == key) {
          benchmark::DoNotOptimize(key.data());
          ++n;
        }
      }
    }
    produced += n;
  }
  ReportTerms(state, produced);
}

void StepRunSelfLoop(benchmark::State& state) {
  static const irs::RegexpAcceptor kAny{AsBytes(std::string_view{".*"})};
  const std::string run(static_cast<size_t>(state.range(0)), 'a');
  const auto* p = reinterpret_cast<const irs::byte_type*>(run.data());
  for (auto _ : state) {
    irs::RegexpAcceptor::State out{};
    const size_t consumed = kAny.StepRun(kAny.Start(), p, run.size(), out);
    benchmark::DoNotOptimize(consumed);
    benchmark::DoNotOptimize(out);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(run.size()));
}

void StepPerByte(benchmark::State& state) {
  static const irs::RegexpAcceptor kAny{AsBytes(std::string_view{".*"})};
  const std::string run(static_cast<size_t>(state.range(0)), 'a');
  const auto* p = reinterpret_cast<const irs::byte_type*>(run.data());
  for (auto _ : state) {
    auto s = kAny.Start();
    for (size_t i = 0; i != run.size(); ++i) {
      s = kAny.Step(s, p[i]);
    }
    benchmark::DoNotOptimize(s);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(run.size()));
}

void FullWalk(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  size_t produced = 0;
  for (auto _ : state) {
    auto it = field.iterator();
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
    auto it = field.iterator();
    size_t n = 0;
    while (it->next()) {
      if (predicate.Matches(it->value())) {
        ++n;
      }
    }
    produced += n;
  }
  state.SetItemsProcessed(static_cast<int64_t>(produced));
}

void ExactIterator(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const auto targets = SampleTerms(index, 1000);
  irs::ByTerm filter;
  size_t i = 0;
  for (auto _ : state) {
    filter.mutable_options()->term = AsBytes(targets[i++ % targets.size()]);
    auto it = filter.CompileTermIterator(field);
    benchmark::DoNotOptimize(DrainIterator(*it));
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
    produced += DrainIterator(*it);
  }
  ReportTerms(state, produced);
}

void FullWalkFilteredIterator(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const auto& predicate = RegexpAcceptorFor();
  size_t produced = 0;
  for (auto _ : state) {
    irs::FilteredTermIterator it{
      field.iterator(), irs::MakeTermPredicate([&](irs::bytes_view term) {
        return predicate.Matches(term);
      })};
    produced += DrainIterator(it);
  }
  state.SetItemsProcessed(static_cast<int64_t>(produced));
}

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

std::vector<std::unique_ptr<irs::RegexpAcceptor>> RegexpAcceptorsFor(
  std::span<const std::string> patterns) {
  std::vector<std::unique_ptr<irs::RegexpAcceptor>> acceptors;
  acceptors.reserve(patterns.size());
  for (const auto& pattern : patterns) {
    acceptors.emplace_back(
      std::make_unique<irs::RegexpAcceptor>(AsBytes(pattern)));
  }
  return acceptors;
}

std::string RegexUnionPattern(std::span<const std::string> patterns) {
  std::string rendered;
  for (const auto& pattern : patterns) {
    rendered += rendered.empty() ? "(?:" : "|(?:";
    rendered += pattern;
    rendered += ')';
  }
  return rendered;
}

std::vector<std::string> PrefixPatternsFor(size_t n) {
  std::vector<std::string> patterns;
  patterns.reserve(n);
  for (const auto& prefix : DisjointPrefixes(n)) {
    patterns.push_back(prefix + ".*");
  }
  return patterns;
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
      n += DrainIterator(*it);
    }
    produced += n;
  }
  ReportTerms(state, produced);
}

void OrPrefixesUnionBuild(benchmark::State& state) {
  const auto pattern =
    RegexUnionPattern(PrefixPatternsFor(static_cast<size_t>(state.range(1))));
  for (auto _ : state) {
    const irs::RegexpAcceptor fused{AsBytes(pattern)};
    benchmark::DoNotOptimize(fused.ok());
  }
  state.SetItemsProcessed(state.iterations());
}

void OrPrefixesFusedWalk(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const irs::RegexpAcceptor fused{AsBytes(
    RegexUnionPattern(PrefixPatternsFor(static_cast<size_t>(state.range(1)))))};
  size_t produced = 0;
  for (auto _ : state) {
    produced += DrainAcceptor(field, fused);
  }
  ReportTerms(state, produced);
}

void OrRegexpsSeparateWalks(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const auto children =
    RegexpAcceptorsFor(RegexpPatternsFor(static_cast<size_t>(state.range(1))));
  size_t produced = 0;
  for (auto _ : state) {
    size_t n = 0;
    for (const auto& child : children) {
      n += DrainAcceptor(field, *child);
    }
    produced += n;
  }
  ReportTerms(state, produced);
}

void OrRegexpsUnionBuild(benchmark::State& state) {
  const auto pattern =
    RegexUnionPattern(RegexpPatternsFor(static_cast<size_t>(state.range(1))));
  for (auto _ : state) {
    const irs::RegexpAcceptor fused{AsBytes(pattern)};
    benchmark::DoNotOptimize(fused.ok());
  }
  state.SetItemsProcessed(state.iterations());
}

void OrRegexpsFusedWalk(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const irs::RegexpAcceptor fused{AsBytes(
    RegexUnionPattern(RegexpPatternsFor(static_cast<size_t>(state.range(1)))))};
  size_t produced = 0;
  for (auto _ : state) {
    produced += DrainAcceptor(field, fused);
  }
  ReportTerms(state, produced);
}

std::vector<std::string> OverlappingRegexpPatterns() {
  return {"a.*[0-7]", "a.*[4-9abcdef]"};
}

void OrRegexpsOverlapSeparateWalks(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const auto children = RegexpAcceptorsFor(OverlappingRegexpPatterns());
  size_t produced = 0;
  for (auto _ : state) {
    size_t n = 0;
    for (const auto& child : children) {
      n += DrainAcceptor(field, *child);
    }
    produced += n;
  }
  ReportTerms(state, produced);
}

void OrRegexpsOverlapFusedWalk(benchmark::State& state) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& field = FieldOf(index);
  const irs::RegexpAcceptor fused{
    AsBytes(RegexUnionPattern(OverlappingRegexpPatterns()))};
  size_t produced = 0;
  for (auto _ : state) {
    produced += DrainAcceptor(field, fused);
  }
  ReportTerms(state, produced);
}

BENCHMARK(ExactSeekRandomOnly)->Apply(ApplySizes);
BENCHMARK(ExactSeekNormal)->Apply(ApplySizes);
BENCHMARK(ExactAcceptorBuild)->Apply(ApplySizes);
BENCHMARK(ExactSourceBuild)->Apply(ApplySizes);
BENCHMARK(ExactAcceptorWalk)->Apply(ApplySizes);
BENCHMARK(ExactIterator)->Apply(ApplySizes);

BENCHMARK(PrefixSeekScan)
  ->ArgsProduct({{10'000, 500'000}, {1, 3}})
  ->Unit(benchmark::kMicrosecond);
BENCHMARK(PrefixAcceptorBuild)
  ->ArgsProduct({{10'000}, {1, 3}})
  ->Unit(benchmark::kMicrosecond);
BENCHMARK(PrefixSourceBuild)
  ->ArgsProduct({{10'000}, {1, 3}})
  ->Unit(benchmark::kMicrosecond);
BENCHMARK(PrefixAcceptorWalk)
  ->ArgsProduct({{10'000, 500'000}, {1, 3}})
  ->Unit(benchmark::kMicrosecond);
BENCHMARK(PrefixAcceptorWithBuild)
  ->ArgsProduct({{10'000, 500'000}, {1, 3}})
  ->Unit(benchmark::kMicrosecond);
BENCHMARK(PrefixIteratorScan)
  ->ArgsProduct({{10'000, 500'000}, {1, 3}})
  ->Unit(benchmark::kMicrosecond);

BENCHMARK(RangeSeekScan)
  ->ArgsProduct({{10'000, 500'000}, {1, 4}})
  ->Unit(benchmark::kMicrosecond);
BENCHMARK(RangeBoundedWalk)
  ->ArgsProduct({{10'000, 500'000}, {1, 4}})
  ->Unit(benchmark::kMicrosecond);
BENCHMARK(RangeIteratorScan)
  ->ArgsProduct({{10'000, 500'000}, {1, 4}})
  ->Unit(benchmark::kMicrosecond);

BENCHMARK(InSeeks)
  ->ArgsProduct({{10'000, 500'000}, {10, 1000}})
  ->Unit(benchmark::kMicrosecond);

BENCHMARK(RegexpAcceptorBuild)->Unit(benchmark::kMicrosecond);
BENCHMARK(RegexpAcceptorBuildScaling)
  ->ArgsProduct({{64, 128, 256, 512, 1024, 2048}, {0, 1}})
  ->Unit(benchmark::kMicrosecond);
BENCHMARK(RegexpSourceBuild)->Unit(benchmark::kMicrosecond);
BENCHMARK(RegexpAcceptorWalk)->Apply(ApplySizes);

BENCHMARK(FusedSourceBuild)->Unit(benchmark::kMicrosecond);
BENCHMARK(FusedSourceWalk)->Apply(ApplySizes);
BENCHMARK(FusedBoundsOnlyWalk)->Apply(ApplySizes);
BENCHMARK(FusedDriverPlusPredicate)->Apply(ApplySizes);
BENCHMARK(FusedSeparateWalks)->Apply(ApplySizes);

BENCHMARK(StepRunSelfLoop)->Arg(8)->Arg(64)->Arg(1024);
BENCHMARK(StepPerByte)->Arg(8)->Arg(64)->Arg(1024);

BENCHMARK(FullWalk)->Apply(ApplySizes);
BENCHMARK(FullWalkPredicate)->Apply(ApplySizes);
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
BENCHMARK(OrRegexpsFusedWalk)
  ->ArgsProduct({{10'000, 500'000}, {2, 4, 8}})
  ->Unit(benchmark::kMicrosecond);
BENCHMARK(OrRegexpsOverlapSeparateWalks)->Apply(ApplySizes);
BENCHMARK(OrRegexpsOverlapFusedWalk)->Apply(ApplySizes);

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
