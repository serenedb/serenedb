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

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <benchmark/benchmark.h>

#include <cstdlib>
#include <duckdb/common/file_system.hpp>
#include <filesystem>
#include <fstream>
#include <iresearch/formats/flush_state.hpp>
#include <iresearch/formats/index/field_reader.hpp>
#include <iresearch/formats/index/field_writer.hpp>
#include <iresearch/formats/index/idx_reader.hpp>
#include <iresearch/formats/index/idx_writer.hpp>
#include <iresearch/formats/index/term_dict_reader.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/utils/conjunction_acceptor.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/levenshtein_acceptor.hpp>
#include <iresearch/utils/levenshtein_default_pdp.hpp>
#include <iresearch/utils/regexp_acceptor.hpp>
#include <memory>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace {

using irs::bstring;
using irs::bytes_view;

constexpr size_t kBatchSizes[] = {16, 256, 4096};
constexpr std::string_view kBatchKinds[] = {"sorted", "unsorted", "miss",
                                            "dense"};

std::vector<bstring> ReadRecords(const std::filesystem::path& path) {
  std::ifstream in{path, std::ios::binary};
  std::vector<bstring> records;
  uint32_t len = 0;
  while (in.read(reinterpret_cast<char*>(&len), sizeof(len))) {
    bstring& record = records.emplace_back(len, irs::byte_type{0});
    in.read(reinterpret_cast<char*>(record.data()), len);
  }
  return records;
}

std::vector<bytes_view> Views(const std::vector<bstring>& records) {
  return {records.begin(), records.end()};
}

using irs::term_dict::Probe;

struct PaddedRecords {
  std::vector<bstring> keys;
  std::vector<Probe> probes;
};

PaddedRecords Pad(const std::vector<bstring>& records) {
  PaddedRecords padded;
  padded.keys.reserve(records.size());
  padded.probes.reserve(records.size());
  for (const auto& record : records) {
    auto& key = padded.keys.emplace_back(record);
    key.append(irs::term_dict::kKeyPad, irs::byte_type{0});
    padded.probes.push_back({key.data(), record.size()});
  }
  return padded;
}

class KeysIterator : public irs::TermOnlyIterator {
 public:
  explicit KeysIterator(std::span<const bstring> keys) noexcept : _keys{keys} {}

  bytes_view value() const noexcept final { return _keys[_next - 1]; }
  bool next() final { return _next++ < _keys.size(); }
  irs::TermPostings::ptr postings(irs::IndexFeatures) const final {
    return irs::TermPostings::empty();
  }
  irs::Attribute* GetMutable(irs::TypeInfo::type_id) noexcept final {
    return nullptr;
  }

  size_t NextTermsWithPostings(std::span<bytes_view> terms,
                               std::span<irs::PostingRows> postings,
                               irs::IndexFeatures) final {
    const size_t n = std::min(
      {terms.size(), postings.size(), _docs.size(), _keys.size() - _next});
    for (size_t i = 0; i != n; ++i, ++_next) {
      _docs[i] = static_cast<uint32_t>(_next + irs::doc_limits::min());
      terms[i] = _keys[_next];
      postings[i] = {.span = {.docs = &_docs[i], .count = 1}};
    }
    return n;
  }

 private:
  std::span<const bstring> _keys;
  std::array<uint32_t, 128> _docs{};
  size_t _next = 0;
};

class KeysReader final : public irs::BasicTermReader {
 public:
  explicit KeysReader(std::span<const bstring> keys) noexcept : _keys{keys} {
    const auto width = keys.front().size();
    key_width = absl::c_all_of(
                  keys, [&](const bstring& key) { return key.size() == width; })
                  ? static_cast<uint32_t>(width)
                  : 0;
  }

  irs::TermOnlyIterator::ptr iterator() const final {
    return irs::memory::make_managed<irs::TermOnlyIterator, KeysIterator>(
      _keys);
  }
  irs::field_id id() const final { return 1; }
  irs::FieldProperties properties() const final { return {}; }

  void SampleTerms(
    uint64_t bytes,
    absl::FunctionRef<void(std::span<const bytes_view>)> visit) const final {
    constexpr size_t kRun = 16;
    uint64_t total = 0;
    for (const auto& key : _keys) {
      total += key.size();
    }
    if (total == 0) {
      return;
    }
    const uint64_t runs =
      std::max<uint64_t>(1, bytes * _keys.size() / total / kRun);
    const size_t stride = std::max<size_t>(kRun, _keys.size() / runs);
    std::array<bytes_view, kRun> run;
    for (size_t i = 0; i < _keys.size(); i += stride) {
      const size_t n = std::min(kRun, _keys.size() - i);
      for (size_t j = 0; j != n; ++j) {
        run[j] = _keys[i + j];
      }
      visit({run.data(), n});
    }
  }

 private:
  std::span<const bstring> _keys;
};

struct Data {
  std::string base;
  std::vector<bstring> keys;
  std::vector<bstring> hit;
  std::vector<bstring> miss;
  std::vector<bstring> seek;
  std::vector<bstring> batches[std::size(kBatchKinds)][std::size(kBatchSizes)];
  std::string root;
  std::unique_ptr<irs::IdxReader> idx;
  std::unique_ptr<irs::term_dict::FieldReader> fields;
  const irs::TermReader* field = nullptr;
  std::string error;
};

std::filesystem::path KeysDir() {
  const char* dir = std::getenv("TERM_DICT_ROOT_KEYS");
  return dir ? std::filesystem::path{dir} : std::filesystem::path{};
}

void Write(Data& data) {
  const auto path =
    std::filesystem::temp_directory_path() / "serenedb-bench-term-dict-root";
  std::filesystem::remove_all(path);
  std::filesystem::create_directories(path);
  data.root = path.string();
  irs::LocalBudget budget{irs::gRootBudget};
  auto& db = irs::DuckDBEngine::Instance().instance();
  auto& fs = duckdb::FileSystem::GetFileSystem(db);
  irs::FileTracker tracker{fs, data.root, "segment"};
  const irs::FlushState state{
    .tracker = tracker,
    .doc_count = data.keys.size(),
    .budget = budget,
  };
  irs::IdxWriter idx{tracker, budget};
  irs::term_dict::FieldWriter writer{false, budget};
  writer.SetIdxWriter(idx);
  writer.prepare(state, 1, 0);
  writer.write(irs::StringLayout::kStrings, KeysReader{data.keys});
  writer.end();
  idx.Commit(nullptr);
}

void Open(Data& data) {
  static const irs::SegmentMeta kMeta{[] {
    irs::SegmentMeta meta;
    meta.name = "segment";
    return meta;
  }()};
  auto& db = irs::DuckDBEngine::Instance().instance();
  auto& fs = duckdb::FileSystem::GetFileSystem(db);
  data.idx =
    std::make_unique<irs::IdxReader>(fs, data.root, "segment", true, nullptr);
  data.fields = std::make_unique<irs::term_dict::FieldReader>();
  data.fields->prepare(irs::ReaderState{
    .db = db,
    .fs = fs,
    .root = data.root,
    .meta = kMeta,
    .idx = data.idx.get(),
  });
  data.field = data.fields->fields(1).front();
}

std::unique_ptr<Data> gData;

const Data& Load(const std::string& base) {
  if (gData && gData->base == base) {
    return *gData;
  }
  gData.reset();
  auto data = std::make_unique<Data>();
  data->base = base;
  const auto file = [&](std::string_view set) {
    return KeysDir() / absl::StrCat(base, ".", set);
  };
  data->keys = ReadRecords(file("keys"));
  data->hit = ReadRecords(file("hit"));
  data->miss = ReadRecords(file("miss"));
  data->seek = ReadRecords(file("seek"));
  for (size_t k = 0; k != std::size(kBatchKinds); ++k) {
    for (size_t s = 0; s != std::size(kBatchSizes); ++s) {
      data->batches[k][s] = ReadRecords(
        file(absl::StrCat("batch_", kBatchKinds[k], "_", kBatchSizes[s])));
    }
  }
  if (data->keys.empty()) {
    data->error = absl::StrCat("no keys for ", base);
  } else {
    Write(*data);
    Open(*data);
  }
  gData = std::move(data);
  return *gData;
}

void Locate(benchmark::State& state, const std::string& base, bool hit) {
  const auto& data = Load(base);
  if (!data.error.empty()) {
    state.SkipWithError(data.error);
    return;
  }
  const auto& field =
    static_cast<const irs::term_dict::TermReaderImpl&>(*data.field);
  std::vector<bstring> keys;
  for (const auto& record : hit ? data.hit : data.miss) {
    auto& key = keys.emplace_back(field.KeyPrefix());
    key.append(record);
  }
  const auto padded = Pad(keys);
  for (auto _ : state) {
    size_t sum = 0;
    for (const auto probe : padded.probes) {
      sum += field.Index().BlockOf(probe);
    }
    benchmark::DoNotOptimize(sum);
  }
  state.SetItemsProcessed(
    static_cast<int64_t>(state.iterations() * padded.probes.size()));
  state.counters["bytes"] = static_cast<double>(field.Index().MemoryBytes());
  state.counters["blocks"] = static_cast<double>(field.Blocks());
  state.counters["ns_per_probe"] = benchmark::Counter(
    static_cast<double>(state.iterations() * padded.probes.size()),
    benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

void Lookup(benchmark::State& state, const std::string& base, bool hit) {
  const auto& data = Load(base);
  if (!data.error.empty()) {
    state.SkipWithError(data.error);
    return;
  }
  const auto views = Views(hit ? data.hit : data.miss);
  for (auto _ : state) {
    uint64_t sum = 0;
    for (const auto key : views) {
      sum += data.field->Lookup(key).docs_count;
    }
    benchmark::DoNotOptimize(sum);
  }
  state.SetItemsProcessed(
    static_cast<int64_t>(state.iterations() * views.size()));
  state.counters["ns_per_probe"] = benchmark::Counter(
    static_cast<double>(state.iterations() * views.size()),
    benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

void LookupBatch(benchmark::State& state, const std::string& base, size_t kind,
                 size_t size) {
  const auto& data = Load(base);
  if (!data.error.empty()) {
    state.SkipWithError(data.error);
    return;
  }
  const auto views = Views(data.batches[kind][size]);
  for (auto _ : state) {
    uint64_t sum = 0;
    data.field->LookupBatch(
      views.size(), [&](size_t i) { return views[i]; },
      [&](size_t, const irs::PostingMeta& meta) { sum += meta.docs_count; });
    benchmark::DoNotOptimize(sum);
  }
  state.SetItemsProcessed(
    static_cast<int64_t>(state.iterations() * views.size()));
  state.counters["ns_per_probe"] = benchmark::Counter(
    static_cast<double>(state.iterations() * views.size()),
    benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

void Seek(benchmark::State& state, const std::string& base) {
  const auto& data = Load(base);
  if (!data.error.empty()) {
    state.SkipWithError(data.error);
    return;
  }
  const auto views = Views(data.seek);
  auto it = data.field->iterator();
  for (auto _ : state) {
    size_t sum = 0;
    for (const auto key : views) {
      sum += static_cast<size_t>(it->seek_ge(key));
    }
    benchmark::DoNotOptimize(sum);
  }
  state.SetItemsProcessed(
    static_cast<int64_t>(state.iterations() * views.size()));
  state.counters["ns_per_probe"] = benchmark::Counter(
    static_cast<double>(state.iterations() * views.size()),
    benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

bytes_view WordOf(bytes_view key) {
  constexpr size_t kMinWord = 5;
  constexpr size_t kMaxWord = 12;
  const auto lower = [&](size_t at) {
    return key[at] >= 'a' && key[at] <= 'z';
  };
  for (size_t begin = 0; begin != key.size();) {
    if (!lower(begin)) {
      ++begin;
      continue;
    }
    size_t end = begin;
    while (end != key.size() && lower(end)) {
      ++end;
    }
    if (end - begin >= kMinWord && end - begin <= kMaxWord) {
      return key.substr(begin, end - begin);
    }
    begin = end;
  }
  return {};
}

std::vector<bstring> TargetsOf(const Data& data) {
  constexpr size_t kTargets = 32;
  std::vector<bstring> targets;
  targets.emplace_back(
    irs::ViewCast<irs::byte_type>(std::string_view{"election"}));
  const size_t step = std::max<size_t>(1, data.keys.size() / (8 * kTargets));
  for (size_t i = 0; i < data.keys.size() && targets.size() != kTargets;
       i += step) {
    if (const auto word = WordOf(data.keys[i]);
        !word.empty() && absl::c_find(targets, word) == targets.end()) {
      targets.emplace_back(word);
    }
  }
  return targets;
}

void ReportWalks(benchmark::State& state, size_t terms, size_t per_iteration) {
  benchmark::DoNotOptimize(terms);
  const auto walks = static_cast<double>(state.iterations() * per_iteration);
  state.counters["terms_per_walk"] = static_cast<double>(terms) / walks;
  state.counters["us_per_walk"] = benchmark::Counter(
    walks / 1e6, benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

void Patterns(benchmark::State& state, const std::string& base) {
  const auto& data = Load(base);
  if (!data.error.empty()) {
    state.SkipWithError(data.error);
    return;
  }
  std::vector<std::unique_ptr<const irs::RegexpAcceptor>> acceptors;
  for (const auto& target : TargetsOf(data)) {
    const auto word = irs::ViewCast<char>(bytes_view{target});
    acceptors.push_back(std::make_unique<const irs::RegexpAcceptor>(
      irs::ViewCast<irs::byte_type>(std::string_view{
        absl::StrCat(word.substr(0, 1), ".", word.substr(2), ".*")})));
  }
  size_t terms = 0;
  for (auto _ : state) {
    for (const auto& acceptor : acceptors) {
      auto it = data.field->iterator(*acceptor);
      while (it->next()) {
        ++terms;
      }
    }
  }
  ReportWalks(state, terms, acceptors.size());
}

std::vector<bstring> TyposOf(const Data& data) {
  constexpr size_t kTargets = 32;
  std::vector<bstring> targets;
  std::mt19937 rng{11};
  while (targets.size() != kTargets) {
    bstring key = data.keys[rng() % data.keys.size()];
    if (key.size() < 4 || key.size() > 48) {
      continue;
    }
    const size_t at = rng() % key.size();
    if (key[at] >= 0x80) {
      continue;
    }
    key[at] = static_cast<irs::byte_type>('a' + rng() % 26);
    targets.push_back(std::move(key));
  }
  return targets;
}

void Fuzzy(benchmark::State& state, const std::string& base, uint8_t distance,
           bool typos) {
  const auto& data = Load(base);
  if (!data.error.empty()) {
    state.SkipWithError(data.error);
    return;
  }
  const auto targets = typos ? TyposOf(data) : TargetsOf(data);
  state.counters["targets"] = static_cast<double>(targets.size());
  const auto& description = irs::DefaultPDP(distance, false);
  std::vector<std::shared_ptr<const irs::LevenshteinAcceptor>> fuzzy;
  std::vector<std::shared_ptr<const irs::FuzzyConjunction>> dfas;
  for (const auto& target : targets) {
    fuzzy.push_back(std::make_shared<const irs::LevenshteinAcceptor>(
      description, bytes_view{}, target));
    dfas.push_back(
      irs::FuzzyConjunctionOf<irs::LevenshteinAcceptor>::Make(fuzzy.back()));
  }
  size_t terms = 0;
  for (auto _ : state) {
    for (size_t i = 0; i != targets.size(); ++i) {
      auto it = dfas[i] ? data.field->iterator(*dfas[i])
                        : data.field->iterator(*fuzzy[i]);
      while (it->next()) {
        ++terms;
      }
    }
  }
  ReportWalks(state, terms, targets.size());
}

constexpr std::string_view kInfixes[] = {"%ild%", "%onp%", "%ion%", "%elect%"};
constexpr std::string_view kSuffixes[] = {"%tion", "%nsis", "%ment", "%ly"};

void Wildcards(benchmark::State& state, const std::string& base,
               std::span<const std::string_view> patterns) {
  const auto& data = Load(base);
  if (!data.error.empty()) {
    state.SkipWithError(data.error);
    return;
  }
  std::vector<std::unique_ptr<const irs::RegexpAcceptor>> acceptors;
  for (const auto pattern : patterns) {
    acceptors.push_back(std::make_unique<const irs::RegexpAcceptor>(
      irs::RegexpAcceptor::WildcardTag{},
      irs::ViewCast<irs::byte_type>(pattern)));
  }
  size_t terms = 0;
  for (auto _ : state) {
    for (const auto& acceptor : acceptors) {
      auto it = data.field->iterator(*acceptor);
      while (it->next()) {
        ++terms;
      }
    }
  }
  ReportWalks(state, terms, acceptors.size());
}

std::vector<std::string> Bases() {
  std::vector<std::string> bases;
  const auto dir = KeysDir();
  if (dir.empty() || !std::filesystem::is_directory(dir)) {
    return bases;
  }
  for (const auto& entry : std::filesystem::directory_iterator{dir}) {
    const auto name = entry.path().filename().string();
    if (name.ends_with(".keys")) {
      bases.push_back(name.substr(0, name.size() - 5));
    }
  }
  absl::c_sort(bases);
  return bases;
}

int Main(int argc, char** argv) {
  auto& engine = irs::DuckDBEngine::Instance();
  engine.Initialize();
  absl::Cleanup shutdown = [&] {
    gData.reset();
    engine.Shutdown();
  };

  for (const auto& base : Bases()) {
    benchmark::RegisterBenchmark(
      absl::StrCat("lookup_hit/", base),
      [base](benchmark::State& state) { Lookup(state, base, true); });
    benchmark::RegisterBenchmark(
      absl::StrCat("lookup_miss/", base),
      [base](benchmark::State& state) { Lookup(state, base, false); });
    benchmark::RegisterBenchmark(
      absl::StrCat("seek/", base),
      [base](benchmark::State& state) { Seek(state, base); });
    for (size_t kind = 0; kind != std::size(kBatchKinds); ++kind) {
      if (kBatchKinds[kind] == "unsorted") {
        continue;
      }
      for (size_t size = 0; size != std::size(kBatchSizes); ++size) {
        benchmark::RegisterBenchmark(
          absl::StrCat("lookup_batch/", kBatchKinds[kind], "_",
                       kBatchSizes[size], "/", base),
          [base, kind, size](benchmark::State& state) {
            LookupBatch(state, base, kind, size);
          });
      }
    }
    for (const uint8_t distance : {1, 2}) {
      benchmark::RegisterBenchmark(absl::StrCat("fuzzy", distance, "/", base),
                                   [base, distance](benchmark::State& state) {
                                     Fuzzy(state, base, distance, false);
                                   });
    }
    benchmark::RegisterBenchmark(
      absl::StrCat("typo1/", base),
      [base](benchmark::State& state) { Fuzzy(state, base, 1, true); });
    benchmark::RegisterBenchmark(
      absl::StrCat("pattern/", base),
      [base](benchmark::State& state) { Patterns(state, base); });
    benchmark::RegisterBenchmark(
      absl::StrCat("infix/", base),
      [base](benchmark::State& state) { Wildcards(state, base, kInfixes); });
    benchmark::RegisterBenchmark(
      absl::StrCat("suffix/", base),
      [base](benchmark::State& state) { Wildcards(state, base, kSuffixes); });
    benchmark::RegisterBenchmark(
      absl::StrCat("locate_hit/", base),
      [base](benchmark::State& state) { Locate(state, base, true); });
    benchmark::RegisterBenchmark(
      absl::StrCat("locate_miss/", base),
      [base](benchmark::State& state) { Locate(state, base, false); });
  }
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}

[[maybe_unused]] const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);

}  // namespace
