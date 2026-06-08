#include "iresearch/search/filter_rules.hpp"

#include <benchmark/benchmark.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/analysis/analyzers.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/regexp_filter.hpp"
#include "iresearch/search/wildcard_filter.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/fs_directory.hpp"
#include "iresearch/store/mmap_directory.hpp"
#include "iresearch/utils/string.hpp"

namespace {

auto MakeByTerm(std::string_view field,
                std::string_view term) -> std::unique_ptr<irs::ByTerm> {
  auto t = std::make_unique<irs::ByTerm>();
  *t->mutable_field() = field;
  auto bv = irs::ViewCast<irs::byte_type>(term);
  t->mutable_options()->term.assign(bv.data(), bv.size());
  return t;
}

auto BuildFlatAndOfByTerms(size_t n) -> irs::Filter::ptr {
  auto root = std::make_unique<irs::And>();
  for (size_t i = 0; i < n; ++i) {
    root->add(MakeByTerm("field", "term"));
  }
  return root;
}

auto BuildFlatAndOfEmpty(size_t n) -> irs::Filter::ptr {
  auto root = std::make_unique<irs::And>();
  for (size_t i = 0; i < n; ++i) {
    root->add<irs::Empty>();
  }
  return root;
}

auto BuildMixedFlatAnd(size_t n) -> irs::Filter::ptr {
  auto root = std::make_unique<irs::And>();
  for (size_t i = 0; i < n; ++i) {
    if ((i & 1u) == 0u) {
      root->add(MakeByTerm("field", "term"));
    } else {
      root->add<irs::Empty>();
    }
  }
  return root;
}

auto BuildNestedAnd(size_t depth) -> irs::Filter::ptr {
  auto root = std::make_unique<irs::And>();
  auto* current = root.get();
  for (size_t i = 0; i + 1 < depth; ++i) {
    current = &current->add<irs::And>();
    current->add<irs::Empty>();
  }
  return root;
}

auto BuildNotChain(size_t depth) -> irs::Filter::ptr {
  if (depth == 0) {
    return std::make_unique<irs::Empty>();
  }
  auto root = std::make_unique<irs::Not>();
  auto* current = root.get();
  for (size_t i = 1; i < depth; ++i) {
    current = &current->filter<irs::Not>();
  }
  current->filter<irs::Empty>();
  return root;
}

void BmBuildFlatAndOfByTerms(benchmark::State& state) {
  for (auto _ : state) {
    auto tree = BuildFlatAndOfByTerms(state.range(0));
    benchmark::DoNotOptimize(tree);
  }
}

void BmApplyEmptyPipeline(benchmark::State& state) {
  irs::FilterRulesConstructor constructor;
  for (auto _ : state) {
    auto tree = BuildFlatAndOfByTerms(state.range(0));
    tree = constructor.Apply(std::move(tree));
    benchmark::DoNotOptimize(tree);
  }
}

void BmApplyNonMatchingRule(benchmark::State& state) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::ByTermsFilterRule>();
  for (auto _ : state) {
    auto tree = BuildFlatAndOfEmpty(state.range(0));
    tree = constructor.Apply(std::move(tree));
    benchmark::DoNotOptimize(tree);
  }
}

void BmApplyByTermsFlat(benchmark::State& state) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::ByTermsFilterRule>();
  for (auto _ : state) {
    auto tree = BuildFlatAndOfByTerms(state.range(0));
    tree = constructor.Apply(std::move(tree));
    benchmark::DoNotOptimize(tree);
  }
}

void BmApplyByTermsMixed(benchmark::State& state) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::ByTermsFilterRule>();
  for (auto _ : state) {
    auto tree = BuildMixedFlatAnd(state.range(0));
    tree = constructor.Apply(std::move(tree));
    benchmark::DoNotOptimize(tree);
  }
}

void BmApplyAndFlatteningNested(benchmark::State& state) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::AndFlatteningFilterRule>();
  for (auto _ : state) {
    auto tree = BuildNestedAnd(state.range(0));
    tree = constructor.Apply(std::move(tree));
    benchmark::DoNotOptimize(tree);
  }
}

void BmApplyNotChain(benchmark::State& state) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::NotFilterRule>();
  for (auto _ : state) {
    auto tree = BuildNotChain(state.range(0));
    tree = constructor.Apply(std::move(tree));
    benchmark::DoNotOptimize(tree);
  }
}

auto MakeWildcard(std::string_view field, std::string_view pattern)
  -> std::unique_ptr<irs::ByWildcard> {
  auto f = std::make_unique<irs::ByWildcard>();
  *f->mutable_field() = field;
  f->mutable_options()->term = irs::ViewCast<irs::byte_type>(pattern);
  return f;
}

auto MakeRegexp(std::string_view field,
                std::string_view pattern) -> std::unique_ptr<irs::ByRegexp> {
  auto f = std::make_unique<irs::ByRegexp>();
  *f->mutable_field() = field;
  f->mutable_options()->pattern = irs::ViewCast<irs::byte_type>(pattern);
  return f;
}

auto BuildFlatAndOfWildcards(size_t n) -> irs::Filter::ptr {
  auto root = std::make_unique<irs::And>();
  for (size_t i = 0; i < n; ++i) {
    root->add((i & 1u) == 0u ? MakeWildcard("field", "pro%")
                             : MakeWildcard("field", "%ing"));
  }
  return root;
}

auto BuildFlatOrOfWildcards(size_t n) -> irs::Filter::ptr {
  auto root = std::make_unique<irs::Or>();
  for (size_t i = 0; i < n; ++i) {
    root->add((i & 1u) == 0u ? MakeWildcard("field", "cat%")
                             : MakeWildcard("field", "dog%"));
  }
  return root;
}

auto BuildAndOfOrFilters() -> irs::Filter::ptr {
  auto root = std::make_unique<irs::And>();
  auto& or1 = root->add<irs::Or>();
  or1.add(MakeWildcard("field", "cat%"));
  or1.add(MakeWildcard("field", "dog%"));
  auto& or2 = root->add<irs::Or>();
  or2.add(MakeRegexp("field", "^[a-z]+$"));
  or2.add(MakeRegexp("field", "^[0-9]+$"));
  return root;
}

void BmApplyChainedRules(benchmark::State& state) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::NotFilterRule>();
  constructor.Add<irs::AndFlatteningFilterRule>();
  constructor.Add<irs::OrFlatteningFilterRule>();
  constructor.Add<irs::ByTermsFilterRule>();
  for (auto _ : state) {
    auto tree = BuildMixedFlatAnd(state.range(0));
    tree = constructor.Apply(std::move(tree));
    benchmark::DoNotOptimize(tree);
  }
}

void BmAutomatonRuleIntersection(benchmark::State& state) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::AutomatonFilterRule>();
  for (auto _ : state) {
    auto tree = BuildFlatAndOfWildcards(state.range(0));
    tree = constructor.Apply(std::move(tree));
    benchmark::DoNotOptimize(tree);
  }
}

void BmAutomatonRuleUnion(benchmark::State& state) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::AutomatonFilterRule>();
  for (auto _ : state) {
    auto tree = BuildFlatOrOfWildcards(state.range(0));
    tree = constructor.Apply(std::move(tree));
    benchmark::DoNotOptimize(tree);
  }
}

void BmAutomatonRuleAndOfOr(benchmark::State& state) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::AutomatonFilterRule>();
  for (auto _ : state) {
    auto tree = BuildAndOfOrFilters();
    tree = constructor.Apply(std::move(tree));
    benchmark::DoNotOptimize(tree);
  }
}

struct IField {
  using ptr = std::shared_ptr<IField>;
  virtual ~IField() = default;

  virtual irs::IndexFeatures GetIndexFeatures() const = 0;
  virtual irs::Tokenizer& GetTokens() const = 0;
  virtual std::string_view Name() const = 0;
  virtual bool Write(irs::DataOutput& out) const = 0;
};

class TextField final : public IField {
 public:
  explicit TextField(std::string name)
    : _name(std::move(name)),
      _stream(irs::analysis::analyzers::Get(
        "text", irs::Type<irs::text_format::Json>::get(),
        "{\"locale\":\"C\", \"stopwords\":[]}")) {}

  irs::IndexFeatures GetIndexFeatures() const final {
    return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
           irs::IndexFeatures::Offs;
  }
  std::string_view Name() const final { return _name; }
  void SetValue(std::string_view value) noexcept { _value = value; }

  irs::Tokenizer& GetTokens() const final {
    _stream->reset(_value);
    return *_stream;
  }

  bool Write(irs::DataOutput&) const final { return false; }

 private:
  std::string _name;
  irs::analysis::Analyzer::ptr _stream;
  std::string_view _value;
};

class FieldList {
 public:
  class Iterator {
   public:
    using iterator_category = std::input_iterator_tag;
    using value_type = IField;
    using reference = IField&;
    using pointer = IField*;
    using difference_type = std::ptrdiff_t;

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

   private:
    std::vector<IField::ptr>::const_iterator _it;
  };

  void PushBack(IField::ptr f) { _fields.push_back(std::move(f)); }

  Iterator begin() const { return Iterator{_fields.begin()}; }
  Iterator end() const { return Iterator{_fields.end()}; }

 private:
  std::vector<IField::ptr> _fields;
};

class EuroparlReader {
 public:
  EuroparlReader(const std::filesystem::path& file, TextField& body)
    : _ifs{file, std::ifstream::in | std::ifstream::binary}, _body{&body} {}

  bool Next() {
    while (std::getline(_ifs, _line)) {
      const auto first = _line.find('\t');
      if (first == std::string::npos) {
        continue;
      }
      const auto second = _line.find('\t', first + 1);
      if (second == std::string::npos) {
        continue;
      }
      _value.assign(_line, second + 1, std::string::npos);
      _body->SetValue(_value);
      return true;
    }
    return false;
  }

 private:
  std::ifstream _ifs;
  TextField* _body;
  std::string _line;
  std::string _value;
};

class CountingInput final : public irs::IndexInput {
 public:
  CountingInput(irs::IndexInput::ptr impl, std::atomic<uint64_t>& reads,
                std::atomic<uint64_t>& bytes) noexcept
    : _impl{std::move(impl)}, _reads{&reads}, _bytes{&bytes} {}

  irs::byte_type ReadByte() final {
    Tally(1);
    return _impl->ReadByte();
  }
  uint64_t Position() const noexcept final { return _impl->Position(); }
  uint64_t Length() const noexcept final { return _impl->Length(); }

  int16_t ReadI16() final {
    Tally(sizeof(int16_t));
    return _impl->ReadI16();
  }
  int32_t ReadI32() final {
    Tally(sizeof(int32_t));
    return _impl->ReadI32();
  }
  int64_t ReadI64() final {
    Tally(sizeof(int64_t));
    return _impl->ReadI64();
  }
  uint32_t ReadV32() final {
    const auto before = _impl->Position();
    const auto value = _impl->ReadV32();
    Tally(_impl->Position() - before);
    return value;
  }
  uint64_t ReadV64() final {
    const auto before = _impl->Position();
    const auto value = _impl->ReadV64();
    Tally(_impl->Position() - before);
    return value;
  }

  void Skip(uint64_t count) final { _impl->Skip(count); }

  const irs::byte_type* ReadView(uint64_t count) final {
    Tally(count);
    return _impl->ReadView(count);
  }
  const irs::byte_type* ReadData(uint64_t count) final {
    Tally(count);
    return _impl->ReadData(count);
  }
  size_t ReadBytes(irs::byte_type* b, size_t count) final {
    const auto n = _impl->ReadBytes(b, count);
    Tally(n);
    return n;
  }

  bool IsEOF() const noexcept final { return _impl->IsEOF(); }
  Type GetType() const noexcept final { return _impl->GetType(); }

  void Seek(uint64_t pos) final { _impl->Seek(pos); }

  const irs::byte_type* ReadData(uint64_t offset, uint64_t count) final {
    Tally(count);
    return _impl->ReadData(offset, count);
  }
  const irs::byte_type* ReadView(uint64_t offset, uint64_t count) final {
    Tally(count);
    return _impl->ReadView(offset, count);
  }
  size_t ReadBytes(uint64_t offset, irs::byte_type* b, size_t count) final {
    const auto n = _impl->ReadBytes(offset, b, count);
    Tally(n);
    return n;
  }

  ptr Dup() const final {
    return std::make_unique<CountingInput>(_impl->Dup(), *_reads, *_bytes);
  }
  ptr Reopen() const final {
    return std::make_unique<CountingInput>(_impl->Reopen(), *_reads, *_bytes);
  }
  uint32_t Checksum(uint64_t offset) const final {
    return _impl->Checksum(offset);
  }
  uint64_t CountMappedMemory() const final {
    return _impl->CountMappedMemory();
  }

 private:
  void Tally(uint64_t bytes) noexcept {
    _reads->fetch_add(1, std::memory_order_relaxed);
    _bytes->fetch_add(bytes, std::memory_order_relaxed);
  }

  irs::IndexInput::ptr _impl;
  std::atomic<uint64_t>* _reads;
  std::atomic<uint64_t>* _bytes;
};

class CountingFSDirectory final : public irs::FSDirectory {
 public:
  using irs::FSDirectory::FSDirectory;

  irs::IndexInput::ptr open(std::string_view name,
                            irs::IOAdvice advice) const noexcept final {
    auto input = irs::FSDirectory::open(name, advice);
    if (!input) {
      return input;
    }
    return std::make_unique<CountingInput>(std::move(input), _reads, _bytes);
  }

  void ResetCounters() noexcept {
    _reads.store(0, std::memory_order_relaxed);
    _bytes.store(0, std::memory_order_relaxed);
  }
  uint64_t Reads() const noexcept {
    return _reads.load(std::memory_order_relaxed);
  }
  uint64_t Bytes() const noexcept {
    return _bytes.load(std::memory_order_relaxed);
  }

 private:
  mutable std::atomic<uint64_t> _reads{0};
  mutable std::atomic<uint64_t> _bytes{0};
};

constexpr std::string_view kMergeField = "body_anl";
constexpr std::string_view kMergeFormat = "1_5simd";
constexpr std::string_view kEuroparlFallbackPath =
  "resources/tests/iresearch/europarl.subset.big.txt";

[[noreturn]] void Die(const char* msg) {
  std::fprintf(stderr, "filter_rules merge bench: %s\n", msg);
  std::abort();
}

std::filesystem::path ResolveDataPath() {
  if (const char* env = std::getenv("SERENEDB_BENCH_EUROPARL")) {
    return env;
  }
  return std::filesystem::path{kEuroparlFallbackPath};
}

struct Corpus {
  std::filesystem::path dir_path;
  std::unique_ptr<CountingFSDirectory> dir;
  irs::Format::ptr format;
  irs::DirectoryReader reader;
};

Corpus BuildIndex() {
  auto data_path = ResolveDataPath();
  if (!std::filesystem::exists(data_path)) {
    std::fprintf(stderr,
                 "filter_rules merge bench: europarl dataset not found at "
                 "'%s'\nSet SERENEDB_BENCH_EUROPARL or run from the repo root "
                 "so the relative fallback resolves.\n",
                 data_path.string().c_str());
    std::abort();
  }

  auto tmp_root =
    std::filesystem::temp_directory_path() / "serenedb-bench-filter-rules";
  std::filesystem::remove_all(tmp_root);
  std::filesystem::create_directories(tmp_root);

  irs::analysis::analyzers::Init();
  irs::formats::Init();

  auto format = irs::formats::Get(std::string{kMergeFormat});
  if (!format) {
    Die("format not registered");
  }

  auto dir = std::make_unique<CountingFSDirectory>(tmp_root);

  auto writer = irs::IndexWriter::Make(*dir, format, irs::kOmCreate,
                                       irs::IndexWriterOptions{});
  if (!writer) {
    Die("IndexWriter::Make returned null");
  }

  auto body = std::make_shared<TextField>(std::string{kMergeField});
  FieldList fields;
  fields.PushBack(body);

  EuroparlReader reader{data_path, *body};
  size_t inserted = 0;
  while (reader.Next()) {
    auto trx = writer->GetBatch();
    auto inserter = trx.Insert();
    if (!inserter.Insert<irs::Action::INDEX>(fields.begin(), fields.end())) {
      Die("Insert returned false");
    }
    ++inserted;
  }
  writer->Commit();

  if (inserted == 0) {
    Die("inserted 0 documents - dataset file empty?");
  }

  std::fprintf(stderr,
               "filter_rules merge bench: indexed %zu documents from %s\n",
               inserted, data_path.string().c_str());

  auto rdr = irs::DirectoryReader{*dir, format};
  return Corpus{.dir_path = std::move(tmp_root),
                .dir = std::move(dir),
                .format = std::move(format),
                .reader = std::move(rdr)};
}

Corpus& GetCorpus() {
  static Corpus corpus = BuildIndex();
  return corpus;
}

struct PlainCorpus {
  std::unique_ptr<irs::MMapDirectory> dir;
  irs::DirectoryReader reader;
};

const irs::DirectoryReader& GetPlainReader() {
  static const PlainCorpus plain = [] {
    auto& corpus = GetCorpus();
    auto dir = std::make_unique<irs::MMapDirectory>(corpus.dir_path);
    auto reader = irs::DirectoryReader{*dir, corpus.format};
    return PlainCorpus{.dir = std::move(dir), .reader = std::move(reader)};
  }();
  return plain.reader;
}

enum class MergeOp { Union, Intersection };

struct Pattern {
  bool regexp;
  std::string_view value;
};

constexpr Pattern kUnionPatterns[] = {
  {false, "%ment"}, {false, "%tion"},  {true, ".*ing"}, {false, "%ed"},
  {true, ".*ous"},  {false, "%ation"}, {true, ".*ity"}, {false, "%ness"},
};

constexpr Pattern kIntersectionPatterns[] = {
  {false, "%a%"}, {true, ".*e.*"}, {false, "%i%"}, {true, ".*t.*"},
  {false, "%n%"}, {true, ".*o.*"}, {false, "%r%"}, {true, ".*s.*"},
};

constexpr size_t kMaxMergePatterns = std::size(kUnionPatterns);
static_assert(std::size(kIntersectionPatterns) == kMaxMergePatterns);

auto BuildMergeTree(size_t n, MergeOp op) -> irs::Filter::ptr {
  n = std::min(n, kMaxMergePatterns);
  const auto* patterns =
    op == MergeOp::Union ? kUnionPatterns : kIntersectionPatterns;

  irs::Filter::ptr root = op == MergeOp::Union
                            ? irs::Filter::ptr{std::make_unique<irs::Or>()}
                            : irs::Filter::ptr{std::make_unique<irs::And>()};
  auto& boolean = static_cast<irs::BooleanFilter&>(*root);
  for (size_t i = 0; i < n; ++i) {
    const auto& p = patterns[i];
    if (p.regexp) {
      boolean.add(MakeRegexp(kMergeField, p.value));
    } else {
      boolean.add(MakeWildcard(kMergeField, p.value));
    }
  }
  return root;
}

class MergeBench : public benchmark::Fixture {
 public:
  void SetUp(benchmark::State&) override { _corpus = &GetCorpus(); }

 protected:
  void RunCase(benchmark::State& state, bool apply_rules, MergeOp op) {
    auto& corpus = *_corpus;
    const auto n = static_cast<size_t>(state.range(0));

    irs::FilterRulesConstructor constructor;
    if (apply_rules) {
      constructor.Add<irs::AutomatonFilterRule>();
    }

    {
      auto tree = BuildMergeTree(n, op);
      if (apply_rules) {
        tree = constructor.Apply(std::move(tree));
      }
      auto prepared = tree->prepare({.index = corpus.reader});
      if (!prepared) {
        state.SkipWithError("prepare returned null");
        return;
      }
    }

    uint64_t reads = 0;
    uint64_t bytes = 0;
    for (auto _ : state) {
      auto tree = BuildMergeTree(n, op);
      if (apply_rules) {
        tree = constructor.Apply(std::move(tree));
      }
      corpus.dir->ResetCounters();
      auto prepared = tree->prepare({.index = corpus.reader});
      reads = corpus.dir->Reads();
      bytes = corpus.dir->Bytes();
      benchmark::DoNotOptimize(prepared);
    }

    state.counters["term_reads"] = static_cast<double>(reads);
    state.counters["term_bytes"] = static_cast<double>(bytes);
  }

  const Corpus* _corpus = nullptr;
};

BENCHMARK_DEFINE_F(MergeBench, UnionClassic)(benchmark::State& state) {
  RunCase(state, false, MergeOp::Union);
}
BENCHMARK_DEFINE_F(MergeBench, UnionRules)(benchmark::State& state) {
  RunCase(state, true, MergeOp::Union);
}
BENCHMARK_DEFINE_F(MergeBench, IntersectionClassic)(benchmark::State& state) {
  RunCase(state, false, MergeOp::Intersection);
}
BENCHMARK_DEFINE_F(MergeBench, IntersectionRules)(benchmark::State& state) {
  RunCase(state, true, MergeOp::Intersection);
}

void RunSpeed(benchmark::State& state, bool apply_rules, MergeOp op) {
  const auto& reader = GetPlainReader();
  const auto n = static_cast<size_t>(state.range(0));

  irs::FilterRulesConstructor constructor;
  if (apply_rules) {
    constructor.Add<irs::AutomatonFilterRule>();
  }

  size_t matched = 0;
  for (auto _ : state) {
    auto tree = BuildMergeTree(n, op);
    if (apply_rules) {
      tree = constructor.Apply(std::move(tree));
    }
    auto prepared = tree->prepare({.index = reader});
    matched = 0;
    for (const auto& segment : reader) {
      auto docs = prepared->execute({.segment = segment});
      while (docs->next()) {
        ++matched;
      }
    }
    benchmark::DoNotOptimize(matched);
  }

  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(matched));
  state.counters["docs"] = static_cast<double>(matched);
}

void BmQueryUnionClassic(benchmark::State& state) {
  RunSpeed(state, false, MergeOp::Union);
}
void BmQueryUnionRules(benchmark::State& state) {
  RunSpeed(state, true, MergeOp::Union);
}
void BmQueryIntersectionClassic(benchmark::State& state) {
  RunSpeed(state, false, MergeOp::Intersection);
}
void BmQueryIntersectionRules(benchmark::State& state) {
  RunSpeed(state, true, MergeOp::Intersection);
}

constexpr std::string_view kLevPrefix = "par";
constexpr std::string_view kLevTerm = "parliment";

auto MakePrefix(std::string_view field,
                std::string_view prefix) -> std::unique_ptr<irs::ByPrefix> {
  auto f = std::make_unique<irs::ByPrefix>();
  *f->mutable_field() = field;
  f->mutable_options()->term = irs::ViewCast<irs::byte_type>(prefix);
  return f;
}

auto MakeLevenshtein(std::string_view field, std::string_view term,
                     uint8_t max_distance)
  -> std::unique_ptr<irs::ByEditDistance> {
  auto f = std::make_unique<irs::ByEditDistance>();
  *f->mutable_field() = field;
  f->mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  f->mutable_options()->max_distance = max_distance;
  return f;
}

auto BuildLevenshteinPrefixTree(uint8_t max_distance) -> irs::Filter::ptr {
  auto root = std::make_unique<irs::And>();
  root->add(MakePrefix(kMergeField, kLevPrefix));
  root->add(MakeLevenshtein(kMergeField, kLevTerm, max_distance));
  return root;
}

#if defined(__clang__)
[[clang::optnone]]
#endif
void RunLevenshteinSpeed(benchmark::State& state, bool apply_rules) {
  const auto& reader = GetPlainReader();
  const auto max_distance = static_cast<uint8_t>(state.range(0));

  irs::FilterRulesConstructor constructor;
  if (apply_rules) {
    constructor.Add<irs::LevenshteinPrefixFilterRule>();
  }

  size_t matched = 0;
  for (auto _ : state) {
    auto tree = BuildLevenshteinPrefixTree(max_distance);
    if (apply_rules) {
      tree = constructor.Apply(std::move(tree));
    }
    auto prepared = tree->prepare({.index = reader});
    matched = 0;
    for (const auto& segment : reader) {
      auto docs = prepared->execute({.segment = segment});
      while (docs->next()) {
        ++matched;
      }
    }
    benchmark::DoNotOptimize(matched);
  }

  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(matched));
  state.counters["docs"] = static_cast<double>(matched);
}

void BmQueryLevenshteinPrefixClassic(benchmark::State& state) {
  RunLevenshteinSpeed(state, false);
}
void BmQueryLevenshteinPrefixRules(benchmark::State& state) {
  RunLevenshteinSpeed(state, true);
}

BENCHMARK(BmBuildFlatAndOfByTerms)->RangeMultiplier(4)->Range(1, 1024);
BENCHMARK(BmApplyEmptyPipeline)->RangeMultiplier(4)->Range(1, 1024);
BENCHMARK(BmApplyNonMatchingRule)->RangeMultiplier(4)->Range(1, 1024);
BENCHMARK(BmApplyByTermsFlat)->RangeMultiplier(4)->Range(1, 1024);
BENCHMARK(BmApplyByTermsMixed)->RangeMultiplier(4)->Range(1, 1024);
BENCHMARK(BmApplyAndFlatteningNested)->RangeMultiplier(4)->Range(1, 1024);
BENCHMARK(BmApplyNotChain)->RangeMultiplier(4)->Range(1, 1024);
BENCHMARK(BmApplyChainedRules)->RangeMultiplier(4)->Range(1, 1024);

BENCHMARK(BmAutomatonRuleIntersection)->RangeMultiplier(2)->Range(2, 16);
BENCHMARK(BmAutomatonRuleUnion)->RangeMultiplier(2)->Range(2, 16);
BENCHMARK(BmAutomatonRuleAndOfOr);

BENCHMARK_REGISTER_F(MergeBench, UnionClassic)->RangeMultiplier(2)->Range(2, 8);
BENCHMARK_REGISTER_F(MergeBench, UnionRules)->RangeMultiplier(2)->Range(2, 8);
BENCHMARK_REGISTER_F(MergeBench, IntersectionClassic)
  ->RangeMultiplier(2)
  ->Range(2, 8);
BENCHMARK_REGISTER_F(MergeBench, IntersectionRules)
  ->RangeMultiplier(2)
  ->Range(2, 8);

BENCHMARK(BmQueryUnionClassic)->RangeMultiplier(2)->Range(2, 8);
BENCHMARK(BmQueryUnionRules)->RangeMultiplier(2)->Range(2, 8);
BENCHMARK(BmQueryIntersectionClassic)->RangeMultiplier(2)->Range(2, 8);
BENCHMARK(BmQueryIntersectionRules)->RangeMultiplier(2)->Range(2, 8);

BENCHMARK(BmQueryLevenshteinPrefixClassic)->Arg(1)->Arg(2);
BENCHMARK(BmQueryLevenshteinPrefixRules)->Arg(1)->Arg(2);

}  // namespace

BENCHMARK_MAIN();
