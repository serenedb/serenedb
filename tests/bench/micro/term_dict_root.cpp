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
#include <absl/strings/str_cat.h>
#include <benchmark/benchmark.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iresearch/formats/index/idx_reader.hpp>
#include <iresearch/formats/index/idx_writer.hpp>
#include <iresearch/formats/index/term_dict.hpp>
#include <iresearch/formats/index/term_dict_root.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <memory>
#include <string>
#include <variant>
#include <vector>

namespace {

using irs::bstring;
using irs::bytes_view;

constexpr size_t kBatchSizes[] = {16, 256, 4096};
constexpr std::string_view kBatchKinds[] = {"sorted", "unsorted", "miss"};
constexpr size_t kColdBytes = size_t{512} << 20;

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
  explicit KeysReader(std::span<const bstring> keys) noexcept : _keys{keys} {}

  irs::TermOnlyIterator::ptr iterator() const final {
    return irs::memory::make_managed<irs::TermOnlyIterator, KeysIterator>(
      _keys);
  }
  irs::field_id id() const final { return 1; }
  irs::FieldProperties properties() const final { return {}; }
  bytes_view min() const final { return _keys.front(); }
  bytes_view max() const final { return _keys.back(); }
  uint32_t KeyWidth() const noexcept final {
    const auto width = _keys.front().size();
    return absl::c_all_of(
             _keys, [&](const bstring& key) { return key.size() == width; })
             ? static_cast<uint32_t>(width)
             : 0;
  }

 private:
  std::span<const bstring> _keys;
};

using Candidate =
  std::variant<irs::term_dict::SortedRoot, irs::term_dict::SortedHeadsRoot,
               irs::term_dict::EytzingerRoot, irs::term_dict::BTreeRoot,
               irs::term_dict::RadixRoot>;

constexpr std::string_view kCandidates[] = {
  "sorted", "sorted_heads", "eytzinger", "btree", "radix1", "radix2",
};

Candidate MakeCandidate(std::string_view name,
                        const irs::term_dict::Separators& separators) {
  if (name == "sorted") {
    return irs::term_dict::SortedRoot{separators};
  }
  if (name == "sorted_heads") {
    return irs::term_dict::SortedHeadsRoot{separators};
  }
  if (name == "eytzinger") {
    return irs::term_dict::EytzingerRoot{separators};
  }
  if (name == "btree") {
    return irs::term_dict::BTreeRoot{separators};
  }
  return irs::term_dict::RadixRoot{separators, name == "radix1" ? 1U : 2U};
}

struct Data {
  std::string base;
  std::vector<bstring> keys;
  std::vector<bstring> hit;
  std::vector<bstring> miss;
  std::vector<bstring> batches[std::size(kBatchKinds)][std::size(kBatchSizes)];
  std::vector<bytes_view> separators;
  irs::term_dict::Separators decoded;
  std::unique_ptr<irs::MMapDirectory> dir;
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
  data.dir = std::make_unique<irs::MMapDirectory>(path);
  const irs::FlushState state{
    .dir = data.dir.get(),
    .name = "segment",
    .doc_count = data.keys.size(),
  };
  irs::IdxWriter idx{*data.dir, "segment"};
  irs::term_dict::FieldWriter writer{false, irs::IResourceManager::gNoop};
  writer.SetIdxWriter(idx);
  writer.prepare(state);
  writer.write(KeysReader{data.keys});
  writer.end();
  idx.Commit();
}

void Open(Data& data) {
  static const irs::SegmentMeta kMeta{[] {
    irs::SegmentMeta meta;
    meta.name = "segment";
    return meta;
  }()};
  data.idx = std::make_unique<irs::IdxReader>(*data.dir, "segment", nullptr);
  data.fields =
    std::make_unique<irs::term_dict::FieldReader>(irs::IResourceManager::gNoop);
  data.fields->prepare(irs::ReaderState{
    .dir = data.dir.get(), .meta = &kMeta, .idx = data.idx.get()});
  data.field = data.fields->field(1);
  const auto& impl =
    static_cast<const irs::term_dict::TermReaderImpl&>(*data.field);
  std::vector<bytes_view> separators;
  if (!impl.Small()) {
    for (size_t block = 1; block < impl.GetRoot().Blocks(); ++block) {
      separators.push_back(impl.GetRoot().Separator(block));
    }
  }
  data.decoded = irs::term_dict::MakeSeparators(separators);
  data.separators.clear();
  for (size_t i = 0; i != irs::term_dict::SeparatorCount(data.decoded); ++i) {
    data.separators.push_back(irs::term_dict::SeparatorAt(data.decoded, i));
  }
}

const Data& Load(const std::string& base) {
  static std::unique_ptr<Data> gData;
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

size_t Reference(const Data& data, bytes_view key) {
  return std::upper_bound(data.separators.begin(), data.separators.end(), key) -
         data.separators.begin();
}

std::string Validate(const Data& data, const Candidate& candidate) {
  return std::visit(
    [&](const auto& root) -> std::string {
      std::vector<uint32_t> blocks;
      const auto check = [&](const std::vector<bstring>& probes,
                             size_t batch) -> std::string {
        const auto views = Views(probes);
        for (size_t i = 0; i < views.size(); i += batch) {
          const auto chunk =
            std::span{views}.subspan(i, std::min(batch, views.size() - i));
          blocks.resize(chunk.size());
          root.FindBatch(chunk, blocks);
          const bool sorted = absl::c_is_sorted(chunk);
          size_t from = 0;
          for (size_t j = 0; j != chunk.size(); ++j) {
            const auto expected = Reference(data, chunk[j]);
            const auto found = root.Find(chunk[j]);
            if (found != expected || blocks[j] != expected) {
              return absl::StrCat("probe ", i + j, " gives ", found, "/",
                                  blocks[j], ", expected ", expected);
            }
            if (sorted) {
              if (root.FindFrom(from, chunk[j]) != expected) {
                return absl::StrCat("probe ", i + j, " gallops wrong");
              }
              from = expected;
            }
          }
        }
        return {};
      };
      for (const auto* probes : {&data.hit, &data.miss}) {
        if (auto error = check(*probes, 16); !error.empty()) {
          return error;
        }
      }
      for (size_t k = 0; k != std::size(kBatchKinds); ++k) {
        for (size_t s = 0; s != std::size(kBatchSizes); ++s) {
          if (auto error = check(data.batches[k][s], kBatchSizes[s]);
              !error.empty()) {
            return error;
          }
        }
      }
      return {};
    },
    candidate);
}

struct Prepared {
  const Data* data;
  Candidate candidate;
  double build_ms;
  std::string error;
};

const Prepared& Prepare(const std::string& base, std::string_view name) {
  static std::unique_ptr<Prepared> gPrepared;
  static std::string gKey;
  const auto key = absl::StrCat(base, "/", name);
  if (gPrepared && gKey == key) {
    return *gPrepared;
  }
  gPrepared.reset();
  const auto& data = Load(base);
  const auto start = std::chrono::steady_clock::now();
  auto candidate = MakeCandidate(name, data.decoded);
  const auto elapsed = std::chrono::steady_clock::now() - start;
  gPrepared = std::make_unique<Prepared>(Prepared{
    .data = &data,
    .candidate = std::move(candidate),
    .build_ms = std::chrono::duration<double, std::milli>(elapsed).count(),
  });
  gPrepared->error =
    data.error.empty() ? Validate(data, gPrepared->candidate) : data.error;
  gKey = key;
  return *gPrepared;
}

bool Skip(benchmark::State& state, const Prepared& prepared) {
  if (prepared.error.empty()) {
    return false;
  }
  state.SkipWithError(prepared.error);
  return true;
}

void Report(benchmark::State& state, const Prepared& prepared, size_t probes) {
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * probes));
  std::visit(
    [&](const auto& root) {
      state.counters["bytes"] = static_cast<double>(root.MemoryBytes());
      state.counters["blocks"] = static_cast<double>(root.Blocks());
    },
    prepared.candidate);
  state.counters["build_ms"] = prepared.build_ms;
  state.counters["ns_per_probe"] = benchmark::Counter(
    static_cast<double>(state.iterations() * probes),
    benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

void FindPoint(benchmark::State& state, const std::string& base,
               std::string_view name, bool hit) {
  const auto& prepared = Prepare(base, name);
  if (Skip(state, prepared)) {
    return;
  }
  const auto views = Views(hit ? prepared.data->hit : prepared.data->miss);
  std::visit(
    [&](const auto& root) {
      for (auto _ : state) {
        size_t sum = 0;
        for (const auto key : views) {
          sum += root.Find(key);
        }
        benchmark::DoNotOptimize(sum);
      }
    },
    prepared.candidate);
  Report(state, prepared, views.size());
}

void FindCold(benchmark::State& state, const std::string& base,
              std::string_view name) {
  const auto& prepared = Prepare(base, name);
  if (Skip(state, prepared)) {
    return;
  }
  const auto views = Views(prepared.data->hit);
  const size_t bytes = std::visit(
    [](const auto& root) { return root.MemoryBytes(); }, prepared.candidate);
  const size_t copies = std::clamp<size_t>(kColdBytes / bytes, 1, 4096);
  std::vector<Candidate> roots;
  roots.reserve(copies);
  for (size_t i = 0; i != copies; ++i) {
    roots.push_back(MakeCandidate(name, prepared.data->decoded));
  }
  for (auto _ : state) {
    size_t sum = 0;
    for (size_t i = 0; i != views.size(); ++i) {
      sum += std::visit([&](const auto& root) { return root.Find(views[i]); },
                        roots[i % copies]);
    }
    benchmark::DoNotOptimize(sum);
  }
  Report(state, prepared, views.size());
  state.counters["copies"] = static_cast<double>(copies);
}

void FindBatch(benchmark::State& state, const std::string& base,
               std::string_view name, size_t kind, size_t size) {
  const auto& prepared = Prepare(base, name);
  if (Skip(state, prepared)) {
    return;
  }
  const auto views = Views(prepared.data->batches[kind][size]);
  const size_t batch = kBatchSizes[size];
  std::vector<uint32_t> blocks(batch);
  std::visit(
    [&](const auto& root) {
      for (auto _ : state) {
        for (size_t i = 0; i + batch <= views.size(); i += batch) {
          root.FindBatch(std::span{views}.subspan(i, batch), blocks);
          benchmark::DoNotOptimize(blocks.data());
        }
      }
    },
    prepared.candidate);
  Report(state, prepared, views.size() / batch * batch);
}

void FindFrom(benchmark::State& state, const std::string& base,
              std::string_view name) {
  const auto& prepared = Prepare(base, name);
  if (Skip(state, prepared)) {
    return;
  }
  const auto views = Views(prepared.data->batches[0][1]);
  const size_t batch = kBatchSizes[1];
  std::visit(
    [&](const auto& root) {
      for (auto _ : state) {
        size_t sum = 0;
        for (size_t i = 0; i < views.size(); i += batch) {
          size_t from = 0;
          for (size_t j = i; j != std::min(i + batch, views.size()); ++j) {
            from = root.FindFrom(from, views[j]);
            sum += from;
          }
        }
        benchmark::DoNotOptimize(sum);
      }
    },
    prepared.candidate);
  Report(state, prepared, views.size());
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
  for (const auto& base : Bases()) {
    benchmark::RegisterBenchmark(
      absl::StrCat("lookup_hit/", base),
      [base](benchmark::State& state) { Lookup(state, base, true); });
    benchmark::RegisterBenchmark(
      absl::StrCat("lookup_miss/", base),
      [base](benchmark::State& state) { Lookup(state, base, false); });
    for (const auto name : kCandidates) {
      const auto suffix = absl::StrCat(base, "/", name);
      benchmark::RegisterBenchmark(absl::StrCat("find_hit/", suffix),
                                   [base, name](benchmark::State& state) {
                                     FindPoint(state, base, name, true);
                                   });
      benchmark::RegisterBenchmark(absl::StrCat("find_miss/", suffix),
                                   [base, name](benchmark::State& state) {
                                     FindPoint(state, base, name, false);
                                   });
      benchmark::RegisterBenchmark(
        absl::StrCat("find_hit_cold/", suffix),
        [base, name](benchmark::State& state) { FindCold(state, base, name); });
      benchmark::RegisterBenchmark(
        absl::StrCat("find_from/", suffix),
        [base, name](benchmark::State& state) { FindFrom(state, base, name); });
      for (size_t k = 0; k != std::size(kBatchKinds); ++k) {
        for (size_t s = 0; s != std::size(kBatchSizes); ++s) {
          benchmark::RegisterBenchmark(
            absl::StrCat("find_batch_", kBatchKinds[k], "_", kBatchSizes[s],
                         "/", suffix),
            [base, name, k, s](benchmark::State& state) {
              FindBatch(state, base, name, k, s);
            });
        }
      }
    }
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
