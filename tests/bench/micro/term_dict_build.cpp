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
#include <fsst.h>

#include <array>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iresearch/formats/index/field_reader.hpp>
#include <iresearch/formats/index/field_writer.hpp>
#include <iresearch/formats/index/idx_reader.hpp>
#include <iresearch/formats/index/idx_writer.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <memory>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace {

using irs::bstring;
using irs::bytes_view;

constexpr size_t kSampleRun = 16;
constexpr size_t kCheckStep = 97;
constexpr std::string_view kSegment = "segment";

struct FileClose {
  void operator()(std::FILE* file) const noexcept { std::fclose(file); }
};

std::vector<bstring> ReadKeys(const std::filesystem::path& path) {
  std::vector<bstring> keys;
  std::unique_ptr<std::FILE, FileClose> file{std::fopen(path.c_str(), "rb")};
  if (!file) {
    return keys;
  }
  uint32_t len = 0;
  while (std::fread(&len, sizeof(len), 1, file.get()) == 1) {
    auto& key = keys.emplace_back(len, irs::byte_type{0});
    if (std::fread(key.data(), 1, len, file.get()) != len) {
      keys.pop_back();
      break;
    }
  }
  return keys;
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
  KeysReader(std::span<const bstring> keys, bool sample) noexcept
    : _keys{keys}, _sample{sample} {
    const auto width = keys.front().size();
    key_width = absl::c_all_of(
                  keys, [&](const bstring& key) { return key.size() == width; })
                  ? static_cast<uint32_t>(width)
                  : 0;
    for (const auto& key : keys) {
      _bytes += key.size();
    }
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
    const size_t n = _keys.size();
    if (!_sample || n == 0 || _bytes == 0) {
      return;
    }
    const uint64_t runs =
      std::max<uint64_t>(1, bytes * n / _bytes / kSampleRun);
    const size_t stride = std::max<size_t>(kSampleRun, n / runs);
    std::array<bytes_view, kSampleRun> run;
    for (size_t i = 0; i < n; i += stride) {
      const size_t count = std::min(kSampleRun, n - i);
      for (size_t j = 0; j != count; ++j) {
        run[j] = _keys[i + j];
      }
      visit({run.data(), count});
    }
  }

 private:
  std::span<const bstring> _keys;
  uint64_t _bytes = 0;
  bool _sample;
};

std::filesystem::path KeysDir() {
  const char* dir = std::getenv("TERM_DICT_BUILD_KEYS");
  return dir ? std::filesystem::path{dir} : std::filesystem::path{};
}

std::filesystem::path OutDir() {
  const char* dir = std::getenv("TERM_DICT_BUILD_DIR");
  return dir ? std::filesystem::path{dir}
             : std::filesystem::temp_directory_path() /
                 "serenedb-bench-term-dict-build";
}

const irs::SegmentMeta& Meta() {
  static const irs::SegmentMeta kMeta{[] {
    irs::SegmentMeta meta;
    meta.name = kSegment;
    return meta;
  }()};
  return kMeta;
}

void Write(irs::Directory& dir, irs::term_dict::FieldWriter& writer,
           std::span<const bstring> keys, bool sample) {
  const irs::FlushState state{
    .dir = &dir,
    .name = kSegment,
    .doc_count = keys.size(),
  };
  irs::IdxWriter idx{dir, kSegment};
  writer.SetIdxWriter(idx);
  writer.prepare(state, 1);
  writer.write(irs::StringLayout::kStrings, KeysReader{keys, sample});
  writer.end();
  idx.Commit();
}

struct Shape {
  uint64_t blocks = 0;
  uint64_t idx_bytes = 0;
  std::array<uint64_t, 3> kinds{};
  bool fsst = false;
  std::string error;
};

Shape Inspect(const irs::Directory& dir, std::span<const bstring> keys) {
  Shape shape;
  dir.length(shape.idx_bytes, absl::StrCat(kSegment, ".idx"));
  {
    irs::IdxReader idx{dir, kSegment, nullptr};
    const auto terms = idx.TakeTerms();
    shape.blocks = terms.blocks;
    shape.fsst = absl::c_any_of(
      terms.fields, [](const auto& head) { return !head.fsst_table.empty(); });
  }
  irs::IdxReader idx{dir, kSegment, nullptr};
  irs::term_dict::FieldReader fields{irs::IResourceManager::gNoop};
  fields.prepare(irs::ReaderState{.dir = &dir, .meta = &Meta(), .idx = &idx});
  const auto* base = fields.Base();
  if (base == nullptr) {
    shape.error = "segment is not memory-mapped";
    return shape;
  }
  for (uint32_t block = 0; block != fields.BlockCount(); ++block) {
    ++shape.kinds.at(base[fields.PageOffset(block)]);
  }
  const auto readers = fields.fields(1);
  if (readers.empty()) {
    shape.error = "no field";
    return shape;
  }
  for (size_t i = 0; i < keys.size(); i += kCheckStep) {
    if (readers.front()->Lookup(keys[i]).docs_count != 1) {
      shape.error = absl::StrCat("key ", i, " not found");
      return shape;
    }
  }
  return shape;
}

void Build(benchmark::State& state, const std::filesystem::path& path,
           bool sample) {
  static std::filesystem::path gPath;
  static std::vector<bstring> gKeys;
  if (gPath != path) {
    gKeys = ReadKeys(path);
    gPath = path;
  }
  if (gKeys.empty()) {
    state.SkipWithError("no keys");
    return;
  }
  const auto out = OutDir();
  std::filesystem::remove_all(out);
  std::filesystem::create_directories(out);
  irs::MMapDirectory dir{out};
  irs::term_dict::FieldWriter writer{false};
  for (auto _ : state) {
    Write(dir, writer, gKeys, sample);
  }
  const auto shape = Inspect(dir, gKeys);
  if (!shape.error.empty()) {
    state.SkipWithError(shape.error);
    return;
  }
  const auto keys = static_cast<double>(gKeys.size());
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(gKeys.size()));
  state.counters["ns_per_key"] = benchmark::Counter(
    static_cast<double>(state.iterations()) * keys,
    benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
  state.counters["keys"] = keys;
  state.counters["blocks"] = static_cast<double>(shape.blocks);
  state.counters["idx_bytes"] = static_cast<double>(shape.idx_bytes);
  state.counters["var"] = static_cast<double>(shape.kinds[0]);
  state.counters["fixed"] = static_cast<double>(shape.kinds[1]);
  state.counters["var_fsst"] = static_cast<double>(shape.kinds[2]);
  state.counters["fsst"] = shape.fsst ? 1 : 0;
}

// Looks up existing keys (or keys just after them, `miss`) in a one-field
// segment built from the keys with today's FSST rules, in random order.
void Seek(benchmark::State& state, const std::filesystem::path& path,
          bool miss) {
  const auto keys = ReadKeys(path);
  if (keys.empty()) {
    state.SkipWithError("no keys");
    return;
  }
  const auto out = OutDir() / "seek";
  std::filesystem::remove_all(out);
  std::filesystem::create_directories(out);
  irs::MMapDirectory dir{out};
  {
    irs::term_dict::FieldWriter writer{false};
    Write(dir, writer, keys, true);
  }
  irs::IdxReader idx{dir, kSegment, nullptr};
  irs::term_dict::FieldReader fields{irs::IResourceManager::gNoop};
  fields.prepare(irs::ReaderState{.dir = &dir, .meta = &Meta(), .idx = &idx});
  const auto readers = fields.fields(1);
  if (readers.empty()) {
    state.SkipWithError("no field");
    return;
  }
  const auto& reader = *readers.front();
  constexpr size_t kProbes = 100'000;
  const size_t step = std::max<size_t>(1, keys.size() / kProbes);
  std::vector<bstring> probes;
  for (size_t i = 0; i < keys.size(); i += step) {
    auto& probe = probes.emplace_back(keys[i]);
    if (miss) {
      probe.push_back(irs::byte_type{1});
    }
  }
  std::mt19937_64 rng{42};
  std::shuffle(probes.begin(), probes.end(), rng);
  uint64_t found = 0;
  for (auto _ : state) {
    found = 0;
    for (const auto& probe : probes) {
      const auto meta = reader.Lookup(probe);
      benchmark::DoNotOptimize(meta.docs_count);
      found += meta.docs_count != 0;
    }
  }
  state.counters["ns_per_lookup"] = benchmark::Counter(
    static_cast<double>(state.iterations()) *
      static_cast<double>(probes.size()),
    benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
  state.counters["found"] = static_cast<double>(found);
  state.counters["probes"] = static_cast<double>(probes.size());
  state.counters["blocks"] = static_cast<double>(fields.BlockCount());
}

struct FsstInput {
  std::vector<bytes_view> suffixes;
  std::vector<size_t> lens;
  std::vector<unsigned char*> ptrs;
  duckdb_fsst_encoder_t* encoder = nullptr;
  uint64_t bytes = 0;
};

// Front-coded suffixes of `keys` and a table trained like the writer's: runs
// of kSampleRun consecutive keys, about 32 KiB of suffixes.
FsstInput& Suffixes(const std::filesystem::path& path) {
  static std::filesystem::path gPath;
  static std::vector<bstring> gKeys;
  static FsstInput gInput;
  if (gPath == path) {
    return gInput;
  }
  if (gInput.encoder) {
    duckdb_fsst_destroy(gInput.encoder);
  }
  gInput = {};
  gKeys = ReadKeys(path);
  gPath = path;
  for (size_t i = 1; i < gKeys.size(); ++i) {
    const bytes_view prev = gKeys[i - 1];
    const bytes_view key = gKeys[i];
    if (i % kSampleRun == 0) {
      continue;
    }
    const auto suffix = key.substr(irs::CommonPrefixLength(prev, key));
    if (!suffix.empty()) {
      gInput.suffixes.push_back(suffix);
      gInput.bytes += suffix.size();
    }
  }
  for (const auto suffix : gInput.suffixes) {
    gInput.lens.push_back(suffix.size());
    gInput.ptrs.push_back(const_cast<unsigned char*>(suffix.data()));
  }
  const size_t n = gInput.suffixes.size();
  const uint64_t runs =
    std::max<uint64_t>(1, (uint64_t{32} << 10) * n /
                            std::max<uint64_t>(gInput.bytes, 1) / kSampleRun);
  const size_t stride = std::max<size_t>(kSampleRun, n / runs);
  std::vector<size_t> sample_lens;
  std::vector<unsigned char*> sample_ptrs;
  for (size_t i = 0; i < n; i += stride) {
    for (size_t j = i; j < std::min(n, i + kSampleRun); ++j) {
      sample_lens.push_back(gInput.lens[j]);
      sample_ptrs.push_back(gInput.ptrs[j]);
    }
  }
  gInput.encoder = duckdb_fsst_create(sample_lens.size(), sample_lens.data(),
                                      sample_ptrs.data(), 0);
  return gInput;
}

// Encodes every suffix in calls of `batch` suffixes, like the writer encoding
// one suffix per call (batch 1) or a block's pending suffixes at once.
void EncodeBatches(benchmark::State& state, const std::filesystem::path& path,
                   size_t batch) {
  auto& input = Suffixes(path);
  if (input.suffixes.empty() || input.encoder == nullptr) {
    state.SkipWithError("no suffixes");
    return;
  }
  const size_t n = input.suffixes.size();
  bstring out(7 + 2 * input.bytes + 16, 0);
  std::vector<size_t> out_lens(n);
  std::vector<unsigned char*> out_ptrs(n);
  uint64_t coded = 0;
  for (auto _ : state) {
    coded = 0;
    for (size_t at = 0; at < n; at += batch) {
      const size_t count = std::min(batch, n - at);
      const size_t done = duckdb_fsst_compress(
        input.encoder, count, input.lens.data() + at, input.ptrs.data() + at,
        out.size(), out.data(), out_lens.data() + at, out_ptrs.data() + at);
      benchmark::DoNotOptimize(done);
      for (size_t i = 0; i != count; ++i) {
        coded += out_lens[at + i];
      }
    }
  }
  state.counters["ns_per_suffix"] = benchmark::Counter(
    static_cast<double>(state.iterations()) * static_cast<double>(n),
    benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
  state.counters["suffixes"] = static_cast<double>(n);
  state.counters["mean_bytes"] =
    static_cast<double>(input.bytes) / static_cast<double>(n);
  state.counters["ratio"] = static_cast<double>(input.bytes) /
                            static_cast<double>(std::max<uint64_t>(coded, 1));
}

int Main(int argc, char** argv) {
  const auto dir = KeysDir();
  std::vector<std::filesystem::path> files;
  if (!dir.empty() && std::filesystem::is_directory(dir)) {
    for (const auto& entry : std::filesystem::directory_iterator{dir}) {
      if (entry.path().extension() == ".keys") {
        files.push_back(entry.path());
      }
    }
  }
  absl::c_sort(files);
  for (const auto& file : files) {
    const auto base = file.stem().string();
    for (const bool sample : {false, true}) {
      benchmark::RegisterBenchmark(
        absl::StrCat("build/", base, "/fsst:", sample ? 1 : 0),
        [file, sample](benchmark::State& state) { Build(state, file, sample); })
        ->Unit(benchmark::kMillisecond);
    }
    for (const bool miss : {false, true}) {
      benchmark::RegisterBenchmark(
        absl::StrCat("seek/", base, miss ? "/miss" : "/hit"),
        [file, miss](benchmark::State& state) { Seek(state, file, miss); })
        ->Unit(benchmark::kMillisecond);
    }
    for (const size_t batch : {1, 8, 32, 128, 1024}) {
      benchmark::RegisterBenchmark(
        absl::StrCat("fsst/", base, "/batch:", batch),
        [file, batch](benchmark::State& state) {
          EncodeBatches(state, file, batch);
        })
        ->Unit(benchmark::kMillisecond);
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
