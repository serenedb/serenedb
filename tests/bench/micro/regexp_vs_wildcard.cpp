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

// Microbenchmark: ByRegexp vs ByWildcard on europarl text.
//
// Dataset path comes from env SERENEDB_BENCH_EUROPARL, with a fallback
// path relative to the current working directory. Aborts if the file
// is missing - silent fallback would produce misleading numbers.

#include <benchmark/benchmark.h>
#include <unicode/locid.h>
#include <utf8.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/regexp_filter.hpp>
#include <iresearch/search/wildcard_filter.hpp>
#include <iresearch/store/data_output.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/string.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "insert_field.hpp"

namespace bench_regexp {

inline constexpr irs::field_id kFieldId = 1;

// Indexing helpers
//
// Trimmed copy of tests/libs/iresearch/index/doc_generator.{hpp,cpp}.
// Only the bits required to index europarl into a single analyzed
// body_anl field. Original supports many fields (title, date, id, body
// in several variants, with payloads); the bench needs none of that.

// Field interface, matches tests::Ifield contract that IndexWriter
// expects.
struct IField {
  using ptr = std::shared_ptr<IField>;
  virtual ~IField() = default;

  virtual irs::field_id Id() const = 0;
  virtual irs::IndexFeatures GetIndexFeatures() const = 0;
  virtual irs::analysis::Tokenizer& GetTokens() const = 0;
  virtual std::string_view Value() const = 0;
  virtual bool Write(irs::DataOutput& out) const = 0;
};

class FieldBase : public IField {
 public:
  FieldBase() = default;

  irs::field_id Id() const noexcept final { return _id; }
  irs::IndexFeatures GetIndexFeatures() const noexcept final {
    return _index_features;
  }

  void SetId(irs::field_id id) noexcept { _id = id; }
  void SetIndexFeatures(irs::IndexFeatures f) noexcept { _index_features = f; }

 private:
  irs::field_id _id{irs::field_limits::invalid()};
  irs::IndexFeatures _index_features{irs::IndexFeatures::None};
};

// Analyzed-text field. Tokenizer config matches the tests'
// text_ref_field: "text" analyzer, locale "C", no stopwords. body_anl
// is created with payload=false in EuroparlDocTemplate, so the payload
// variant is dropped here.
class TextField final : public FieldBase {
 public:
  TextField(irs::field_id id, irs::IndexFeatures extra_features)
    : _stream(irs::analysis::TextTokenizer::Make([] {
        irs::analysis::TextTokenizer::Options opts;
        opts.locale = icu::Locale::createFromName("C");
        opts.explicit_stopwords_set = true;
        return opts;
      }())) {
    SetId(id);
    SetIndexFeatures(irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
                     irs::IndexFeatures::Offs | extra_features);
  }

  void SetValue(std::string_view value) noexcept { _value = value; }

  irs::analysis::Tokenizer& GetTokens() const final { return *_stream; }

  std::string_view Value() const final { return _value; }

  bool Write(irs::DataOutput&) const final { return false; }

 private:
  irs::analysis::Tokenizer::ptr _stream;
  std::string_view _value;
};

// Container of indexed fields, equivalent to a trimmed tests::Particle.
class FieldList {
 public:
  class Iterator {
   public:
    using iterator_category = std::random_access_iterator_tag;
    using value_type = IField;
    using reference = IField&;
    using pointer = IField*;
    using difference_type = std::ptrdiff_t;

    Iterator() = default;
    explicit Iterator(std::vector<IField::ptr>::const_iterator it) : _it{it} {}

    reference operator*() const { return **_it; }
    pointer operator->() const { return _it->get(); }

    Iterator& operator++() {
      ++_it;
      return *this;
    }
    Iterator operator++(int) {
      Iterator tmp = *this;
      ++_it;
      return tmp;
    }

    bool operator==(const Iterator& rhs) const { return _it == rhs._it; }
    bool operator!=(const Iterator& rhs) const { return _it != rhs._it; }

    difference_type operator-(const Iterator& rhs) const {
      return _it - rhs._it;
    }

   private:
    std::vector<IField::ptr>::const_iterator _it;
  };

  void PushBack(IField::ptr f) { _fields.push_back(std::move(f)); }

  Iterator begin() const { return Iterator{_fields.begin()}; }
  Iterator end() const { return Iterator{_fields.end()}; }

 private:
  std::vector<IField::ptr> _fields;
};

struct Document {
  FieldList indexed;
  FieldList stored;
};

// Document template that indexes only the body column of an europarl
// line. Other columns (title, date) are read and discarded.
class EuroparlBodyTemplate {
 public:
  EuroparlBodyTemplate() {
    auto body_anl =
      std::make_shared<TextField>(kFieldId, irs::IndexFeatures::None);
    _body_anl = body_anl.get();
    _doc.indexed.PushBack(std::move(body_anl));
  }

  void SetColumn(size_t idx, const std::string& value) {
    if (idx == 2) {
      _body = value;
      _body_anl->SetValue(_body);
    }
  }

  const Document& Get() const { return _doc; }

 private:
  Document _doc;
  TextField* _body_anl;
  std::string _body;
};

// utf8::unchecked::BreakIterator, copied verbatim from
// tests/libs/iresearch/index/doc_generator.cpp. Splits a UTF-8 byte
// range on a delimiter rune, yielding string columns.
template<typename OctetIterator>
class BreakIterator {
 public:
  using Utf8Iterator = utf8::unchecked::iterator<OctetIterator>;

  BreakIterator(utf8::uint32_t delim, const OctetIterator& begin,
                const OctetIterator& end)
    : _delim{delim}, _wbegin{begin}, _wend{begin}, _end{end} {
    if (!Done()) {
      Next();
    }
  }

  explicit BreakIterator(const OctetIterator& end)
    : _wbegin{end}, _wend{end}, _end{end} {}

  const std::string& operator*() const { return _res; }

  bool operator==(const BreakIterator& rhs) const {
    return _wbegin == rhs._wbegin && _wend == rhs._wend;
  }
  bool operator!=(const BreakIterator& rhs) const { return !(*this == rhs); }

  bool Done() const { return _wbegin == _end; }

  BreakIterator& operator++() {
    Next();
    return *this;
  }

 private:
  void Next() {
    _wbegin = _wend;
    _wend = std::find(_wbegin, _end, _delim);
    if (_wend != _end) {
      _res.assign(_wbegin.base(), _wend.base());
      ++_wend;
    } else {
      _res.assign(_wbegin.base(), _end.base());
    }
  }

  utf8::uint32_t _delim;
  std::string _res;
  Utf8Iterator _wbegin;
  Utf8Iterator _wend;
  Utf8Iterator _end;
};

// Tab-separated line reader.
class EuroparlReader {
 public:
  EuroparlReader(const std::filesystem::path& file, EuroparlBodyTemplate& tpl,
                 uint32_t delim = 0x0009)
    : _ifs{file, std::ifstream::in | std::ifstream::binary},
      _tpl{&tpl},
      _delim{delim} {}

  const Document* Next() {
    if (!std::getline(_ifs, _line)) {
      return nullptr;
    }
    if (utf8::find_invalid(_line.begin(), _line.end()) != _line.end()) {
      return nullptr;
    }

    using Iter = BreakIterator<std::string::const_iterator>;
    Iter end{_line.end()};
    Iter it{_delim, _line.begin(), _line.end()};
    for (size_t i = 0; it != end; ++it, ++i) {
      _tpl->SetColumn(i, *it);
    }

    return &_tpl->Get();
  }

 private:
  std::ifstream _ifs;
  EuroparlBodyTemplate* _tpl;
  uint32_t _delim;
  std::string _line;
};

}  // namespace bench_regexp
namespace {

// Configuration

constexpr std::string_view kEuroparlFallbackPath =
  "resources/tests/iresearch/europarl.subset.big.txt";

using bench_regexp::kFieldId;

constexpr std::string_view kFormatName = "1_5simd";

// Number of repetitions per benchmark. google benchmark uses these to
// compute aggregate stats (mean, median, stddev, cv); ReportAggregates
// suppresses per-repetition lines in the output.
constexpr int kRepetitions = 5;

struct Corpus {
  std::filesystem::path dir_path;
  std::unique_ptr<irs::MMapDirectory> dir;
  irs::Format::ptr format;
  irs::DirectoryReader reader;
};

[[noreturn]] void Die(const char* msg) {
  std::fprintf(stderr, "regexp_vs_wildcard bench: %s\n", msg);
  std::abort();
}

std::filesystem::path ResolveDataPath() {
  if (const char* env = std::getenv("SERENEDB_BENCH_EUROPARL")) {
    return env;
  }
  return std::filesystem::path{kEuroparlFallbackPath};
}

Corpus BuildIndex() {
  auto data_path = ResolveDataPath();
  if (!std::filesystem::exists(data_path)) {
    std::fprintf(stderr,
                 "regexp_vs_wildcard bench: europarl dataset not found at "
                 "'%s'\nSet SERENEDB_BENCH_EUROPARL or run from the repo "
                 "root so the relative fallback resolves.\n",
                 data_path.string().c_str());
    std::abort();
  }

  auto tmp_root =
    std::filesystem::temp_directory_path() / "serenedb-bench-regexp-wildcard";
  std::filesystem::remove_all(tmp_root);
  std::filesystem::create_directories(tmp_root);

  irs::formats::Init();

  auto format = irs::formats::Get(std::string{kFormatName});
  if (!format) {
    Die("format 1_5simd not registered");
  }

  auto dir = std::make_unique<irs::MMapDirectory>(tmp_root);

  auto writer = irs::IndexWriter::Make(*dir, format, irs::kOmCreate,
                                       irs::IndexWriterOptions{});
  if (!writer) {
    Die("IndexWriter::Make returned null");
  }

  bench_regexp::EuroparlBodyTemplate tpl;
  bench_regexp::EuroparlReader reader{data_path, tpl};

  size_t inserted = 0;
  while (auto* doc = reader.Next()) {
    auto trx = writer->GetBatch();
    {
      auto inserter = trx.Insert();
      if (!tests::InsertFields(inserter, doc->indexed.begin(),
                               doc->indexed.end())) {
        Die("Insert returned false");
      }
    }
    trx.Commit();
    ++inserted;
  }
  writer->RefreshCommit();

  if (inserted == 0) {
    Die("inserted 0 documents - dataset file empty?");
  }

  std::fprintf(stderr,
               "regexp_vs_wildcard bench: indexed %zu documents from %s\n",
               inserted, data_path.string().c_str());

  auto rdr = irs::DirectoryReader{*dir, format};
  return Corpus{.dir_path = std::move(tmp_root),
                .dir = std::move(dir),
                .format = std::move(format),
                .reader = std::move(rdr)};
}

const Corpus& GetCorpus() {
  static const Corpus corpus = BuildIndex();
  return corpus;
}

// Fixture

class RegexpVsWildcardBench : public benchmark::Fixture {
 public:
  void SetUp(benchmark::State&) override { _corpus = &GetCorpus(); }

 protected:
  const Corpus* _corpus = nullptr;
};

// Filter constructors

irs::ByWildcard MakeWildcard(std::string_view pattern) {
  irs::ByWildcard q;
  *q.mutable_field_id() = kFieldId;
  *q.mutable_options() =
    irs::ByWildcardOptions{irs::ViewCast<irs::byte_type>(pattern)};
  return q;
}

irs::ByRegexp MakeRegexp(std::string_view pattern) {
  irs::ByRegexp q;
  *q.mutable_field_id() = kFieldId;
  *q.mutable_options() =
    irs::ByRegexpOptions{irs::ViewCast<irs::byte_type>(pattern)};
  return q;
}

// Benchmark helpers
//
// BenchPrepare - measures filter construction + prepare()
// BenchExecuteOnly - prepares once outside the timed loop, then
//                    measures only execute() + iteration over matched
//                    documents. Uses SetItemsProcessed to report
//                    docs/sec via the standard google benchmark
//                    items_per_second column.
//
// Both helpers pass only the required field of PrepareContext /
// ExecutionContext (index / segment) and rely on defaults for memory,
// scorer, ctx, boost. Default memory tracking is IResourceManager::
// gNoop

template<typename MakeFn>
void BenchPrepare(benchmark::State& state, const irs::DirectoryReader& rdr,
                  MakeFn make) {
  // Sanity check before entering the timed loop. A null prepared
  // filter means an invalid pattern slipped through. The timed loop
  // would still run but would measure an empty fast-fail path, not
  // the real prepare pipeline.
  {
    auto q = make();
    auto collector = q.MakeCollector(nullptr);
    irs::QueryBuilder::ptr check;
    for (const auto& sub : rdr) {
      check = q.PrepareSegment(sub, {.collector = collector.get()});
    }
    if (!check) {
      state.SkipWithError("prepare returned null");
      return;
    }
  }

  for (auto _ : state) {
    auto q = make();
    auto collector = q.MakeCollector(nullptr);
    irs::QueryBuilder::ptr prepared;
    for (const auto& sub : rdr) {
      prepared = q.PrepareSegment(sub, {.collector = collector.get()});
    }
    benchmark::DoNotOptimize(prepared);
  }
}

template<typename MakeFn>
void BenchExecuteOnly(benchmark::State& state, const irs::DirectoryReader& rdr,
                      MakeFn make) {
  auto q = make();
  auto collector = q.MakeCollector(nullptr);
  std::vector<irs::QueryBuilder::ptr> prepared;
  for (const auto& sub : rdr) {
    prepared.emplace_back(
      q.PrepareSegment(sub, {.collector = collector.get()}));
  }
  const auto stats = collector->Finish(irs::IResourceManager::gNoop);
  if (prepared.empty() || !prepared.front()) {
    state.SkipWithError("prepare returned null");
    return;
  }

  size_t per_iter = 0;
  for (auto _ : state) {
    per_iter = 0;
    size_t seg_idx = 0;
    for ([[maybe_unused]] const auto& sub : rdr) {
      auto& p = prepared[seg_idx++];
      if (!p) {
        continue;
      }
      auto docs = p->Execute({}, stats);
      while (!irs::doc_limits::eof(docs->advance())) {
        ++per_iter;
      }
    }
    benchmark::DoNotOptimize(per_iter);
  }

  // per_iter is the number of docs matched in a single execute pass.
  // For deterministic patterns on a fixed index this value is identical
  // across iterations, so reporting the last one is correct.
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(per_iter));
  state.counters["docs"] = static_cast<double>(per_iter);
}

// Benchmark cases
//
// Patterns split into three groups:
//
// 1. Complex on both sides (Infix, Suffix, Mixed, SingleChar). Both
//    filters classify as Complex and run the full automaton-build
//    pipeline.
//
// 2. Fast paths (PrefixFastPath, MatchAll). Both filters classify to
//    a non-automaton path (ByPrefix or All). Used as control points
//    to verify that classifiers route equivalent patterns the same
//    way.
//
// 3. Regexp-only (counted, alternation, perl-class). Features wildcard
//    cannot express. Reports absolute cost of those features only;
//    no comparison.
//
// Each Complex / fast-path case has 4 benches:
// <Group><Side>Prepare and <Group><Side>Exec for both Wildcard and
// Regexp. Regexp-only cases are prepare-only.

// Infix: %ment% / .*ment.*
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, InfixWildcardPrepare)
(benchmark::State& state) {
  BenchPrepare(state, _corpus->reader, [] { return MakeWildcard("%ment%"); });
}
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, InfixRegexpPrepare)
(benchmark::State& state) {
  BenchPrepare(state, _corpus->reader, [] { return MakeRegexp(".*ment.*"); });
}
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, InfixWildcardExec)
(benchmark::State& state) {
  BenchExecuteOnly(state, _corpus->reader,
                   [] { return MakeWildcard("%ment%"); });
}
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, InfixRegexpExec)
(benchmark::State& state) {
  BenchExecuteOnly(state, _corpus->reader,
                   [] { return MakeRegexp(".*ment.*"); });
}

// Suffix: %tion / .*tion
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, SuffixWildcardPrepare)
(benchmark::State& state) {
  BenchPrepare(state, _corpus->reader, [] { return MakeWildcard("%tion"); });
}
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, SuffixRegexpPrepare)
(benchmark::State& state) {
  BenchPrepare(state, _corpus->reader, [] { return MakeRegexp(".*tion"); });
}
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, SuffixWildcardExec)
(benchmark::State& state) {
  BenchExecuteOnly(state, _corpus->reader,
                   [] { return MakeWildcard("%tion"); });
}
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, SuffixRegexpExec)
(benchmark::State& state) {
  BenchExecuteOnly(state, _corpus->reader, [] { return MakeRegexp(".*tion"); });
}

// SingleChar: c_t / c.t
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, SingleCharWildcardPrepare)
(benchmark::State& state) {
  BenchPrepare(state, _corpus->reader, [] { return MakeWildcard("c_t"); });
}
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, SingleCharRegexpPrepare)
(benchmark::State& state) {
  BenchPrepare(state, _corpus->reader, [] { return MakeRegexp("c.t"); });
}
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, SingleCharWildcardExec)
(benchmark::State& state) {
  BenchExecuteOnly(state, _corpus->reader, [] { return MakeWildcard("c_t"); });
}
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, SingleCharRegexpExec)
(benchmark::State& state) {
  BenchExecuteOnly(state, _corpus->reader, [] { return MakeRegexp("c.t"); });
}

// Mixed: %un_t% / .*un.t.*
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, MixedWildcardPrepare)
(benchmark::State& state) {
  BenchPrepare(state, _corpus->reader, [] { return MakeWildcard("%un_t%"); });
}
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, MixedRegexpPrepare)
(benchmark::State& state) {
  BenchPrepare(state, _corpus->reader, [] { return MakeRegexp(".*un.t.*"); });
}
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, MixedWildcardExec)
(benchmark::State& state) {
  BenchExecuteOnly(state, _corpus->reader,
                   [] { return MakeWildcard("%un_t%"); });
}
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, MixedRegexpExec)
(benchmark::State& state) {
  BenchExecuteOnly(state, _corpus->reader,
                   [] { return MakeRegexp(".*un.t.*"); });
}

// PrefixFastPath: legislat% / legislat.*
// Both classify to ByPrefix; no automaton built.
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, PrefixFastPathWildcardPrepare)
(benchmark::State& state) {
  BenchPrepare(state, _corpus->reader,
               [] { return MakeWildcard("legislat%"); });
}
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, PrefixFastPathRegexpPrepare)
(benchmark::State& state) {
  BenchPrepare(state, _corpus->reader, [] { return MakeRegexp("legislat.*"); });
}
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, PrefixFastPathWildcardExec)
(benchmark::State& state) {
  BenchExecuteOnly(state, _corpus->reader,
                   [] { return MakeWildcard("legislat%"); });
}
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, PrefixFastPathRegexpExec)
(benchmark::State& state) {
  BenchExecuteOnly(state, _corpus->reader,
                   [] { return MakeRegexp("legislat.*"); });
}

// MatchAll: % / .*
// Both classify to ByPrefix with empty prefix, which iterates the
// entire term dictionary. Upper bound on execute cost; useful as a
// control point for the upper end of the cost range.
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, MatchAllWildcardPrepare)
(benchmark::State& state) {
  BenchPrepare(state, _corpus->reader, [] { return MakeWildcard("%"); });
}
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, MatchAllRegexpPrepare)
(benchmark::State& state) {
  BenchPrepare(state, _corpus->reader, [] { return MakeRegexp(".*"); });
}
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, MatchAllWildcardExec)
(benchmark::State& state) {
  BenchExecuteOnly(state, _corpus->reader, [] { return MakeWildcard("%"); });
}
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, MatchAllRegexpExec)
(benchmark::State& state) {
  BenchExecuteOnly(state, _corpus->reader, [] { return MakeRegexp(".*"); });
}

// Regexp-only patterns. No wildcard counterpart, so prepare-only:
// without a comparison there is nothing meaningful to learn from
// execute time alone.
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, RegexpOnlyCountedPrepare)
(benchmark::State& state) {
  BenchPrepare(state, _corpus->reader, [] { return MakeRegexp("a{3,6}"); });
}
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, RegexpOnlyAlternationPrepare)
(benchmark::State& state) {
  BenchPrepare(state, _corpus->reader,
               [] { return MakeRegexp("(foo|bar|baz).*"); });
}
BENCHMARK_DEFINE_F(RegexpVsWildcardBench, RegexpOnlyPerlClassPrepare)
(benchmark::State& state) {
  BenchPrepare(state, _corpus->reader, [] { return MakeRegexp("\\w+ing"); });
}

// Registration

#define REGISTER(name)                              \
  BENCHMARK_REGISTER_F(RegexpVsWildcardBench, name) \
    ->Repetitions(kRepetitions)                     \
    ->ReportAggregatesOnly(true)

REGISTER(InfixWildcardPrepare);
REGISTER(InfixRegexpPrepare);
REGISTER(InfixWildcardExec);
REGISTER(InfixRegexpExec);

REGISTER(SuffixWildcardPrepare);
REGISTER(SuffixRegexpPrepare);
REGISTER(SuffixWildcardExec);
REGISTER(SuffixRegexpExec);

REGISTER(SingleCharWildcardPrepare);
REGISTER(SingleCharRegexpPrepare);
REGISTER(SingleCharWildcardExec);
REGISTER(SingleCharRegexpExec);

REGISTER(MixedWildcardPrepare);
REGISTER(MixedRegexpPrepare);
REGISTER(MixedWildcardExec);
REGISTER(MixedRegexpExec);

REGISTER(PrefixFastPathWildcardPrepare);
REGISTER(PrefixFastPathRegexpPrepare);
REGISTER(PrefixFastPathWildcardExec);
REGISTER(PrefixFastPathRegexpExec);

REGISTER(MatchAllWildcardPrepare);
REGISTER(MatchAllRegexpPrepare);
REGISTER(MatchAllWildcardExec);
REGISTER(MatchAllRegexpExec);

REGISTER(RegexpOnlyCountedPrepare);
REGISTER(RegexpOnlyAlternationPrepare);
REGISTER(RegexpOnlyPerlClassPrepare);

#undef REGISTER

}  // namespace

BENCHMARK_MAIN();
