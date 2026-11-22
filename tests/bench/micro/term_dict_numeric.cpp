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
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <unistd.h>

#include <array>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/page_residency.hpp>
#include <filesystem>
#include <iresearch/formats/flush_state.hpp>
#include <iresearch/formats/index/field_reader.hpp>
#include <iresearch/formats/index/field_writer.hpp>
#include <iresearch/formats/index/idx_reader.hpp>
#include <iresearch/formats/index/idx_writer.hpp>
#include <iresearch/formats/index/term_dict_cursor.hpp>
#include <iresearch/formats/index/term_dict_reader.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/numeric_utils.hpp>
#include <memory>
#include <numeric>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace {

using irs::bstring;
using irs::bytes_view;

constexpr size_t kTerms = 2'000'000;
constexpr size_t kProbes = 1 << 16;
constexpr size_t kBatch = 256;
constexpr size_t kBatches = 64;
constexpr size_t kCovers = 256;
constexpr size_t kColdCovers = 64;
constexpr size_t kColdLookups = 256;
constexpr int64_t kEpochMicros = 1'700'000'000'000'000;
constexpr double kMeanGapMicros = 315;
constexpr uint32_t kMaxListDocs = 16;
constexpr double kListExtraDocs = 2;
constexpr size_t kListWindow = 8;

enum class Shape : uint8_t {
  Random,
  Timestamps,
  TimestampsShuffled,
  Dense,
  Interval,
};

constexpr std::string_view kShapeNames[]{
  "random", "timestamps", "timestamps_shuffled", "dense", "interval",
};

enum class Lists : uint8_t {
  Singles,
  Clustered,
};

constexpr std::string_view kListsNames[]{"singles", "lists"};

struct Variant {
  Shape shape;
  Lists lists;

  bool operator==(const Variant&) const = default;
};

std::vector<int64_t> Values(Shape shape, std::mt19937_64& rng) {
  std::vector<int64_t> values;
  values.reserve(kTerms);
  switch (shape) {
    case Shape::Random:
      for (size_t i = 0; i != kTerms; ++i) {
        values.push_back(static_cast<int64_t>(rng()));
      }
      absl::c_sort(values);
      values.erase(std::unique(values.begin(), values.end()), values.end());
      break;
    case Shape::Timestamps:
    case Shape::TimestampsShuffled: {
      std::exponential_distribution<double> gap{1 / kMeanGapMicros};
      int64_t value = kEpochMicros;
      for (size_t i = 0; i != kTerms; ++i) {
        value += 1 + static_cast<int64_t>(gap(rng));
        values.push_back(value);
      }
      break;
    }
    case Shape::Dense:
      for (size_t i = 0; i != kTerms; ++i) {
        values.push_back(1'000'000 + static_cast<int64_t>(i));
      }
      break;
    case Shape::Interval:
      for (size_t i = 0; i != kTerms; ++i) {
        values.push_back(kEpochMicros + static_cast<int64_t>(i) * 1'000'000);
      }
      break;
  }
  return values;
}

bstring Encode(int64_t value) {
  irs::byte_type buf[irs::numeric_utils::kNumericTermMaxSize];
  return bstring{irs::numeric_utils::EncodeNumericTerm(buf, value)};
}

struct Postings {
  std::span<const irs::doc_id_t> docs;
  std::span<const uint32_t> starts;
};

class ValuesIterator : public irs::TermOnlyIterator {
 public:
  ValuesIterator(std::span<const bstring> keys, Postings postings) noexcept
    : _keys{keys}, _postings{postings} {}

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
    const size_t n =
      std::min({terms.size(), postings.size(), _keys.size() - _next});
    for (size_t i = 0; i != n; ++i, ++_next) {
      const uint32_t start = _postings.starts[_next];
      terms[i] = _keys[_next];
      postings[i] = {.span = {
                       .docs = &_postings.docs[start],
                       .count = _postings.starts[_next + 1] - start,
                     }};
    }
    return n;
  }

 private:
  std::span<const bstring> _keys;
  Postings _postings;
  size_t _next = 0;
};

class ValuesReader final : public irs::BasicTermReader {
 public:
  ValuesReader(std::span<const bstring> keys, Postings postings) noexcept
    : _keys{keys}, _postings{postings} {
    key_width = static_cast<uint32_t>(keys.front().size());
  }

  irs::TermOnlyIterator::ptr iterator() const final {
    return irs::memory::make_managed<irs::TermOnlyIterator, ValuesIterator>(
      _keys, _postings);
  }
  irs::field_id id() const final { return 1; }
  irs::FieldProperties properties() const final {
    return {.index_features = irs::IndexFeatures::Ranges};
  }

 private:
  std::span<const bstring> _keys;
  Postings _postings;
};

struct Data {
  Variant variant;
  std::vector<bstring> keys;
  std::vector<irs::doc_id_t> docs;
  std::vector<uint32_t> starts;
  irs::doc_id_t doc_count = 0;
  std::vector<bstring> hits;
  std::vector<bstring> misses;
  std::vector<std::vector<bstring>> batches;
  std::string root;
  std::unique_ptr<irs::IdxReader> idx;
  std::unique_ptr<irs::term_dict::FieldReader> fields;
  const irs::term_dict::TermReaderImpl* field = nullptr;
};

std::filesystem::path BenchPath() {
  return std::filesystem::temp_directory_path() /
         "serenedb-bench-term-dict-numeric";
}

void Write(Data& data) {
  const auto path = BenchPath();
  std::filesystem::remove_all(path);
  std::filesystem::create_directories(path);
  data.root = path.string();
  auto& db = irs::DuckDBEngine::Instance().instance();
  auto& fs = duckdb::FileSystem::GetFileSystem(db);
  irs::FileTracker tracker{fs, data.root, "segment"};
  const irs::FlushState state{
    .tracker = tracker,
    .doc_count = data.doc_count,
  };
  irs::IdxWriter idx{tracker};
  irs::term_dict::FieldWriter writer{false};
  writer.SetIdxWriter(idx);
  writer.prepare(state, 1);
  writer.write(
    irs::NumericLayout::kNumbers,
    ValuesReader{data.keys, {.docs = data.docs, .starts = data.starts}});
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
  data.field = &static_cast<const irs::term_dict::TermReaderImpl&>(
    *data.fields->fields(1).front());
}

void SingleDocs(Data& data, std::mt19937_64& rng) {
  const size_t terms = data.keys.size();
  data.docs.resize(terms);
  std::iota(data.docs.begin(), data.docs.end(), irs::doc_limits::min());
  const auto shape = data.variant.shape;
  if (shape == Shape::Random || shape == Shape::TimestampsShuffled) {
    absl::c_shuffle(data.docs, rng);
  }
  data.starts.resize(terms + 1);
  std::iota(data.starts.begin(), data.starts.end(), uint32_t{0});
  data.doc_count = static_cast<irs::doc_id_t>(terms);
}

void ListDocs(Data& data, std::mt19937_64& rng) {
  std::geometric_distribution<uint32_t> extra{1 / (1 + kListExtraDocs)};
  const auto terms = static_cast<uint32_t>(data.keys.size());
  data.starts.reserve(terms + 1);
  data.starts.push_back(0);
  for (uint32_t i = 0; i != terms; ++i) {
    data.starts.push_back(data.starts.back() +
                          std::min(2 + extra(rng), kMaxListDocs));
  }
  data.docs.resize(data.starts.back());
  std::vector<uint32_t> filled(terms);
  std::vector<uint32_t> active;
  uint32_t next = 0;
  while (next != terms && active.size() != kListWindow) {
    active.push_back(next++);
  }
  irs::doc_id_t doc = irs::doc_limits::min();
  for (; !active.empty(); ++doc) {
    const size_t slot = rng() % active.size();
    const uint32_t term = active[slot];
    data.docs[data.starts[term] + filled[term]++] = doc;
    if (filled[term] != data.starts[term + 1] - data.starts[term]) {
      continue;
    }
    if (next != terms) {
      active[slot] = next++;
    } else {
      active[slot] = active.back();
      active.pop_back();
    }
  }
  data.doc_count = doc - 1;
}

const Data& Load(Variant variant) {
  static std::unique_ptr<Data> gData;
  if (gData && gData->variant == variant) {
    return *gData;
  }
  gData.reset();
  auto data = std::make_unique<Data>();
  data->variant = variant;
  const auto shape = variant.shape;
  std::mt19937_64 rng{static_cast<uint64_t>(shape) + 7};
  const auto values = Values(shape, rng);
  data->keys.reserve(values.size());
  for (const auto value : values) {
    data->keys.push_back(Encode(value));
  }
  if (variant.lists == Lists::Singles) {
    SingleDocs(*data, rng);
  } else {
    ListDocs(*data, rng);
  }
  std::uniform_int_distribution<size_t> pick{0, values.size() - 1};
  for (size_t i = 0; i != kProbes; ++i) {
    const size_t at = pick(rng);
    data->hits.push_back(data->keys[at]);
    if (at + 1 != values.size() && values[at] + 1 != values[at + 1]) {
      data->misses.push_back(Encode(values[at] + 1));
    }
  }
  for (size_t b = 0; b != kBatches; ++b) {
    auto& batch = data->batches.emplace_back();
    for (size_t i = 0; i != kBatch; ++i) {
      batch.push_back(data->keys[pick(rng)]);
    }
    absl::c_sort(batch);
  }
  Write(*data);
  Open(*data);
  gData = std::move(data);
  return *gData;
}

void Layout(benchmark::State& state, const Data& data) {
  const auto& field = *data.field;
  const auto& store = field.Store();
  uint64_t key_bytes = 0;
  for (uint32_t block = 0; block != field.Blocks(); ++block) {
    const auto* begin = store.Base() + field.BlockOffset(block);
    const auto view = irs::term_dict::Parse(
      {begin, field.BlockOffset(block + 1) - field.BlockOffset(block)},
      field.Fsst());
    key_bytes += static_cast<uint64_t>(view.records - begin);
  }
  const auto terms = static_cast<double>(field.size());
  state.counters["blocks"] = static_cast<double>(field.Blocks());
  state.counters["terms_per_block"] = terms / field.Blocks();
  state.counters["page_bits_per_term"] =
    8.0 *
    static_cast<double>(field.BlockOffset(field.Blocks()) -
                        field.BlockOffset(0)) /
    terms;
  state.counters["idx_bits_per_term"] =
    8.0 * static_cast<double>(store.Input().GetSize()) / terms;
  state.counters["key_bits_per_term"] =
    8.0 * static_cast<double>(key_bytes) / terms;
  const auto index_bytes = [](const irs::term_dict::TermReaderImpl& dict) {
    return dict.Index().MemoryBytes();
  };
  const auto levels = field.RangeLevels();
  uint64_t level_index_bytes = 0;
  uint64_t level_blocks = 0;
  for (const auto* level : levels) {
    level_index_bytes += index_bytes(*level);
    level_blocks += level->Blocks();
  }
  state.counters["levels"] = static_cast<double>(levels.size());
  state.counters["level0_entries"] =
    static_cast<double>(levels.empty() ? 0 : levels.front()->size());
  state.counters["level_blocks"] = static_cast<double>(level_blocks);
  state.counters["index_bytes"] = static_cast<double>(index_bytes(field));
  state.counters["level_index_bytes"] = static_cast<double>(level_index_bytes);
}

template<typename Probe>
void Probes(benchmark::State& state, size_t count, Probe&& probe) {
  for (auto _ : state) {
    uint64_t sum = 0;
    for (size_t i = 0; i != count; ++i) {
      sum += probe(i);
    }
    benchmark::DoNotOptimize(sum);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * count));
  state.counters["ns_per_item"] = benchmark::Counter(
    static_cast<double>(state.iterations() * count),
    benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

uint64_t SumDocs(const Data& data, const irs::PostingMeta& meta) {
  uint64_t sum = 0;
  data.field->ReadDocs(meta, [&](irs::doc_id_t doc) {
    sum += doc;
    return true;
  });
  return sum;
}

void Locate(benchmark::State& state, Variant variant) {
  const auto& data = Load(variant);
  const auto& field = *data.field;
  std::vector<irs::term_dict::PaddedKey> keys(data.hits.size());
  std::vector<irs::term_dict::Probe> probes;
  for (size_t i = 0; i != data.hits.size(); ++i) {
    probes.push_back(keys[i].Assign(field.KeyPrefix(), data.hits[i]));
  }
  Probes(state, probes.size(),
         [&](size_t i) { return field.Index().BlockOf(probes[i]); });
  Layout(state, data);
}

void Cover(benchmark::State& state, Variant variant, size_t span, bool docs) {
  const auto& data = Load(variant);
  std::mt19937_64 rng{span};
  std::uniform_int_distribution<size_t> pick{0, data.keys.size() - span - 1};
  std::vector<std::pair<bytes_view, bytes_view>> ranges;
  for (size_t i = 0; i != kCovers; ++i) {
    const size_t at = pick(rng);
    ranges.emplace_back(data.keys[at], data.keys[at + span]);
  }
  uint64_t postings = 0;
  Probes(state, ranges.size(), [&](size_t i) {
    uint64_t sum = 0;
    data.field->CoverRange(ranges[i].first, ranges[i].second,
                           [&](bytes_view, const irs::PostingMeta& meta) {
                             sum +=
                               docs ? SumDocs(data, meta) : meta.docs_count;
                             ++postings;
                             return true;
                           });
    return sum;
  });
  state.counters["postings_per_range"] =
    static_cast<double>(postings) /
    static_cast<double>(state.iterations() * ranges.size());
}

void Lookup(benchmark::State& state, Variant variant, bool hit) {
  const auto& data = Load(variant);
  const auto& keys = hit ? data.hits : data.misses;
  if (keys.empty()) {
    state.SkipWithError("no probes");
    return;
  }
  Probes(state, keys.size(),
         [&](size_t i) { return data.field->Lookup(keys[i]).docs_count; });
}

void LookupDocs(benchmark::State& state, Variant variant) {
  const auto& data = Load(variant);
  Probes(state, data.hits.size(), [&](size_t i) {
    return SumDocs(data, data.field->Lookup(data.hits[i]));
  });
}

void LookupBatch(benchmark::State& state, Variant variant) {
  const auto& data = Load(variant);
  Probes(state, data.batches.size(), [&](size_t b) {
    const auto& batch = data.batches[b];
    uint64_t sum = 0;
    data.field->LookupBatch(
      batch.size(), [&](size_t i) { return bytes_view{batch[i]}; },
      [&](size_t, const irs::PostingMeta& meta) { sum += meta.docs_count; });
    return sum;
  });
  state.counters["ns_per_item"] = benchmark::Counter(
    static_cast<double>(state.iterations() * data.batches.size() * kBatch),
    benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

void Seek(benchmark::State& state, Variant variant) {
  const auto& data = Load(variant);
  auto it = data.field->iterator();
  Probes(state, data.hits.size(), [&](size_t i) {
    return static_cast<uint64_t>(it->seek_ge(data.hits[i]));
  });
}

void Scan(benchmark::State& state, Variant variant, bool meta) {
  const auto& data = Load(variant);
  Probes(state, 1, [&](size_t) {
    uint64_t sum = 0;
    for (auto it = data.field->iterator(); it->next();) {
      sum += meta ? it->cookie().docs_count : it->value().back();
    }
    return sum;
  });
  state.counters["ns_per_item"] = benchmark::Counter(
    static_cast<double>(state.iterations() * data.keys.size()),
    benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

class ColdCache {
 public:
  explicit ColdCache(const Data& data) {
    const auto& owner = data.field->Store();
    const auto page = static_cast<uintptr_t>(::sysconf(_SC_PAGESIZE));
    const auto base = reinterpret_cast<uintptr_t>(owner.Base());
    _begin = base & ~(page - 1);
    _length = base + owner.Input().GetSize() - _begin;
    _resident.resize((_length + page - 1) / page);
    for (const auto& entry : std::filesystem::directory_iterator(BenchPath())) {
      _files.push_back(::open(entry.path().c_str(), O_RDONLY));
    }
  }

  ~ColdCache() {
    for (const int fd : _files) {
      ::close(fd);
    }
  }

  void Evict() {
    ::madvise(reinterpret_cast<void*>(_begin), _length, MADV_DONTNEED);
    for (const int fd : _files) {
      ::posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
    }
    duckdb::PageResidency::Invalidate();
  }

  uint64_t Resident() {
    if (::mincore(reinterpret_cast<void*>(_begin), _length, _resident.data()) !=
        0) {
      return 0;
    }
    return absl::c_count_if(_resident,
                            [](unsigned char page) { return page & 1; });
  }

 private:
  uintptr_t _begin = 0;
  size_t _length = 0;
  std::vector<unsigned char> _resident;
  std::vector<int> _files;
};

uint64_t MajorFaults() {
  rusage usage;
  ::getrusage(RUSAGE_THREAD, &usage);
  return static_cast<uint64_t>(usage.ru_majflt);
}

template<typename Op>
void Cold(benchmark::State& state, const Data& data, size_t count,
          std::string_view unit, Op&& op) {
  ColdCache cache{data};
  uint64_t ops = 0;
  uint64_t faults = 0;
  uint64_t pages = 0;
  uint64_t left = 0;
  for (auto _ : state) {
    for (size_t i = 0; i != count; ++i) {
      state.PauseTiming();
      cache.Evict();
      const uint64_t resident = cache.Resident();
      left += resident;
      const uint64_t before = MajorFaults();
      state.ResumeTiming();
      benchmark::DoNotOptimize(op(i));
      state.PauseTiming();
      faults += MajorFaults() - before;
      pages += cache.Resident() - resident;
      ++ops;
      state.ResumeTiming();
    }
  }
  const auto per = [&](uint64_t value) {
    return static_cast<double>(value) / static_cast<double>(ops);
  };
  state.counters[absl::StrCat("us_per_", unit)] = benchmark::Counter(
    static_cast<double>(ops) * 1e-6,
    benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
  state.counters[absl::StrCat("faults_per_", unit)] = per(faults);
  state.counters[absl::StrCat("pages_per_", unit)] = per(pages);
  state.counters["pages_left_after_evict"] = per(left);
}

void ColdCover(benchmark::State& state, Variant variant, size_t span,
               bool docs) {
  const auto& data = Load(variant);
  std::mt19937_64 rng{span + 1};
  std::uniform_int_distribution<size_t> pick{0, data.keys.size() - span - 1};
  std::vector<std::pair<bytes_view, bytes_view>> ranges;
  for (size_t i = 0; i != kColdCovers; ++i) {
    const size_t at = pick(rng);
    ranges.emplace_back(data.keys[at], data.keys[at + span]);
  }
  Cold(state, data, ranges.size(), "cover", [&](size_t i) {
    uint64_t sum = 0;
    data.field->CoverRange(ranges[i].first, ranges[i].second,
                           [&](bytes_view, const irs::PostingMeta& meta) {
                             sum +=
                               docs ? SumDocs(data, meta) : meta.docs_count;
                             return true;
                           });
    return sum;
  });
}

void ColdLookup(benchmark::State& state, Variant variant, bool docs) {
  const auto& data = Load(variant);
  Cold(state, data, std::min(kColdLookups, data.hits.size()), "lookup",
       [&](size_t i) {
         const auto meta = data.field->Lookup(data.hits[i]);
         return docs ? SumDocs(data, meta) : meta.docs_count;
       });
}

void Register(Variant v) {
  const auto shape = kShapeNames[static_cast<size_t>(v.shape)];
  const auto name =
    v.lists == Lists::Singles
      ? std::string{shape}
      : absl::StrCat(kListsNames[static_cast<size_t>(v.lists)], "/", shape);
  const auto add = [&](std::string_view bench, auto run) {
    return benchmark::RegisterBenchmark(absl::StrCat(bench, "/", name),
                                        std::move(run));
  };
  const auto cold = [&](std::string_view bench, auto run) {
    add(bench, std::move(run))->UseRealTime()->Iterations(1);
  };
  add("locate", [v](benchmark::State& state) { Locate(state, v); });
  add("cover_1k",
      [v](benchmark::State& state) { Cover(state, v, 1000, false); });
  add("cover_1k_docs",
      [v](benchmark::State& state) { Cover(state, v, 1000, true); });
  add("cover_10pct",
      [v](benchmark::State& state) { Cover(state, v, kTerms / 10, false); });
  cold("cold_cover_1k",
       [v](benchmark::State& state) { ColdCover(state, v, 1000, false); });
  cold("cold_cover_1k_docs",
       [v](benchmark::State& state) { ColdCover(state, v, 1000, true); });
  cold("cold_cover_10pct", [v](benchmark::State& state) {
    ColdCover(state, v, kTerms / 10, false);
  });
  add("lookup_hit", [v](benchmark::State& state) { Lookup(state, v, true); });
  add("lookup_miss", [v](benchmark::State& state) { Lookup(state, v, false); });
  add("lookup_docs", [v](benchmark::State& state) { LookupDocs(state, v); });
  cold("cold_lookup",
       [v](benchmark::State& state) { ColdLookup(state, v, false); });
  cold("cold_lookup_docs",
       [v](benchmark::State& state) { ColdLookup(state, v, true); });
  add("lookup_batch", [v](benchmark::State& state) { LookupBatch(state, v); });
  add("seek", [v](benchmark::State& state) { Seek(state, v); });
  add("scan", [v](benchmark::State& state) { Scan(state, v, false); });
  add("scan_meta", [v](benchmark::State& state) { Scan(state, v, true); });
}

int Main(int argc, char** argv) {
  irs::DuckDBEngine::Instance().Initialize();

  for (const auto lists : {Lists::Singles, Lists::Clustered}) {
    for (size_t s = 0; s != std::size(kShapeNames); ++s) {
      Register({.shape = static_cast<Shape>(s), .lists = lists});
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
