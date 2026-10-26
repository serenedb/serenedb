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

#include <cstring>
#include <filesystem>
#include <iresearch/formats/index/idx_reader.hpp>
#include <iresearch/formats/index/idx_writer.hpp>
#include <iresearch/formats/index/term_dict.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/numeric_utils.hpp>
#include <memory>
#include <random>
#include <string>
#include <vector>

namespace {

using irs::bstring;
using irs::bytes_view;

constexpr uint64_t kRows = uint64_t{1} << 20;
constexpr uint64_t kSign = uint64_t{1} << 63;
constexpr uint32_t kStep = 16;
constexpr size_t kQueries = 64;

enum class Ladder : uint8_t {
  Static,
  Adaptive,
};

constexpr std::string_view kLadders[]{"static", "adaptive"};

struct Shape {
  std::string_view name;
  uint64_t (*value)(uint64_t row);
};

constexpr Shape kShapes[]{
  {"dense", [](uint64_t row) { return row; }},
  {"timestamps", [](uint64_t row) { return row * 1000000; }},
  {"values5", [](uint64_t row) { return row % 5; }},
  {"values100", [](uint64_t row) { return row % 100; }},
  {"values10k", [](uint64_t row) { return row % 10000; }},
  {"skewed", [](uint64_t row) { return row < kRows / 100 * 85 ? 0 : row; }},
};

struct Selectivity {
  std::string_view name;
  uint64_t rows;
};

constexpr Selectivity kSelectivities[]{
  {"rows100", 100},
  {"pct1", kRows / 100},
  {"pct3", kRows / 32},
};

bytes_view Key(irs::byte_type* buf, uint64_t sortable, uint32_t shift) {
  return {buf,
          irs::numeric_utils::Encode<uint64_t>(sortable ^ kSign, buf, shift)};
}

struct Terms {
  std::vector<bstring> keys;
  std::vector<uint32_t> ends;
  std::vector<irs::doc_id_t> docs;
};

Terms MakeTerms(const Shape& shape, Ladder ladder) {
  Terms terms;
  std::vector<std::pair<uint64_t, irs::doc_id_t>> pairs(kRows);
  const uint32_t end = ladder == Ladder::Static ? 64 : kStep;
  for (uint32_t shift = 0; shift != end; shift += kStep) {
    for (uint64_t row = 0; row != kRows; ++row) {
      pairs[row] = {(shape.value(row) ^ kSign) >> shift << shift,
                    static_cast<irs::doc_id_t>(row + irs::doc_limits::min())};
    }
    absl::c_sort(pairs);
    for (size_t i = 0; i != pairs.size(); ++i) {
      if (i == 0 || pairs[i].first != pairs[i - 1].first) {
        irs::byte_type buf[irs::numeric_utils::kNumericTermMaxSize];
        terms.keys.emplace_back(Key(buf, pairs[i].first, shift));
        terms.ends.push_back(0);
      }
      terms.docs.push_back(pairs[i].second);
      terms.ends.back() = static_cast<uint32_t>(terms.docs.size());
    }
  }
  return terms;
}

class TermsIterator : public irs::TermOnlyIterator {
 public:
  explicit TermsIterator(const Terms& terms) noexcept : _terms{&terms} {}

  bytes_view value() const noexcept final { return _terms->keys[_next - 1]; }
  bool next() final { return _next++ < _terms->keys.size(); }
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
      std::min({terms.size(), postings.size(), _terms->keys.size() - _next});
    for (size_t i = 0; i != n; ++i, ++_next) {
      const uint32_t begin = _next == 0 ? 0 : _terms->ends[_next - 1];
      terms[i] = _terms->keys[_next];
      postings[i] = {.span = {
                       .docs = _terms->docs.data() + begin,
                       .count = _terms->ends[_next] - begin,
                     }};
    }
    return n;
  }

 private:
  const Terms* _terms;
  size_t _next = 0;
};

class TermsReader final : public irs::BasicTermReader {
 public:
  TermsReader(const Terms& terms, Ladder ladder) noexcept
    : _terms{&terms}, _ladder{ladder} {}

  irs::TermOnlyIterator::ptr iterator() const final {
    return irs::memory::make_managed<irs::TermOnlyIterator, TermsIterator>(
      *_terms);
  }
  irs::field_id id() const final { return 1; }
  irs::FieldProperties properties() const final {
    return {.ranges = _ladder == Ladder::Adaptive};
  }
  bytes_view min() const final { return _terms->keys.front(); }
  bytes_view max() const final { return _terms->keys.back(); }
  uint32_t KeyWidth() const noexcept final {
    return _ladder == Ladder::Adaptive
             ? static_cast<uint32_t>(_terms->keys.front().size())
             : 0;
  }

 private:
  const Terms* _terms;
  Ladder _ladder;
};

void Write(irs::Directory& dir, const Terms& terms, Ladder ladder) {
  const irs::FlushState state{
    .dir = &dir,
    .name = "segment",
    .doc_count = kRows,
  };
  irs::IdxWriter idx{dir, "segment"};
  irs::term_dict::FieldWriter writer{false};
  writer.SetIdxWriter(idx);
  writer.prepare(state);
  writer.write(TermsReader{terms, ladder});
  writer.end();
  idx.Commit();
}

uint64_t DirectoryBytes(const irs::Directory& dir) {
  uint64_t total = 0;
  dir.visit([&](std::string_view name) {
    uint64_t size = 0;
    if (dir.length(size, name)) {
      total += size;
    }
    return true;
  });
  return total;
}

template<typename Visit>
void WalkTerms(irs::SeekTermIterator& it, bytes_view from, bytes_view to,
               Visit& visit) {
  if (it.seek_ge(from) == irs::SeekResult::End) {
    return;
  }
  do {
    if (it.value() > to) {
      return;
    }
    visit(it.cookie());
  } while (it.next());
}

template<typename Visit>
void VisitStatic(irs::SeekTermIterator& it, uint64_t lo, uint64_t hi,
                 Visit& visit) {
  const auto walk = [&](uint64_t from, uint64_t to, uint32_t shift) {
    irs::byte_type from_buf[irs::numeric_utils::kNumericTermMaxSize];
    irs::byte_type to_buf[irs::numeric_utils::kNumericTermMaxSize];
    WalkTerms(it, Key(from_buf, from, shift), Key(to_buf, to, shift), visit);
  };
  for (uint32_t shift = 0;; shift += kStep) {
    if (shift + kStep >= 64) {
      walk(lo, hi, shift);
      return;
    }
    const uint64_t mask = uint64_t{0xFFFF} << shift;
    const uint64_t diff = uint64_t{1} << (shift + kStep);
    const bool has_lower = (lo & mask) != 0;
    const bool has_upper = (hi & mask) != mask;
    const uint64_t next_lo = (has_lower ? lo + diff : lo) & ~mask;
    const uint64_t next_hi = (has_upper ? hi - diff : hi) & ~mask;
    if (next_lo > next_hi || next_lo < lo || next_hi > hi) {
      walk(lo, hi, shift);
      return;
    }
    if (has_lower) {
      walk(lo, lo | mask, shift);
    }
    if (has_upper) {
      walk(hi & ~mask, hi, shift);
    }
    lo = next_lo;
    hi = next_hi;
  }
}

template<typename Visit>
void VisitAdaptive(const irs::TermReader& field, uint64_t lo, uint64_t hi,
                   Visit& visit) {
  irs::byte_type from_buf[irs::numeric_utils::kNumericTermMaxSize];
  irs::byte_type to_buf[irs::numeric_utils::kNumericTermMaxSize + 1];
  const auto from = Key(from_buf, lo, 0);
  const auto to = Key(to_buf, hi, 0);
  to_buf[to.size()] = 0;
  field.VisitRange(
    from, {to_buf, to.size() + 1},
    [&](bytes_view, const irs::PostingMeta& meta) { visit(meta); });
}

struct Query {
  uint64_t lo;
  uint64_t hi;
  uint64_t rows;
};

struct Data {
  std::string key;
  Ladder ladder;
  Terms terms;
  std::unique_ptr<irs::MMapDirectory> dir;
  std::unique_ptr<irs::IdxReader> idx;
  std::unique_ptr<irs::term_dict::FieldReader> fields;
  const irs::TermReader* field = nullptr;
  std::vector<Query> queries[std::size(kSelectivities)];
  std::string error;
};

template<typename Visit>
void VisitQuery(const Data& data, irs::SeekTermIterator& it, const Query& query,
                Visit& visit) {
  if (data.ladder == Ladder::Static) {
    VisitStatic(it, query.lo, query.hi, visit);
  } else {
    VisitAdaptive(*data.field, query.lo, query.hi, visit);
  }
}

uint64_t ReadDocs(const Data& data, const irs::PostingMeta& meta,
                  irs::doc_id_t* buffer) {
  if (meta.docs_count == 1) {
    return 1;
  }
  auto it = data.fields->Postings().Postings(data.field->meta().index_features,
                                             irs::IndexFeatures::None, meta,
                                             data.field->HasScoreBounds());
  uint64_t count = 0;
  while (const auto n = it->NextDocs(buffer, nullptr)) {
    count += n;
  }
  return count;
}

std::vector<Query> MakeQueries(const std::vector<uint64_t>& sorted,
                               uint64_t rows) {
  std::mt19937_64 rng{42};
  std::vector<Query> queries;
  for (size_t i = 0; i != kQueries; ++i) {
    const uint64_t first = rng() % (sorted.size() - rows + 1);
    const uint64_t lo = sorted[first];
    const uint64_t hi = sorted[first + rows - 1];
    queries.push_back({
      .lo = lo,
      .hi = hi,
      .rows = static_cast<uint64_t>(absl::c_upper_bound(sorted, hi) -
                                    absl::c_lower_bound(sorted, lo)),
    });
  }
  return queries;
}

std::string Validate(const Data& data) {
  auto it = data.field->iterator();
  irs::doc_id_t buffer[irs::doc_limits::kBlockSize];
  for (size_t s = 0; s != std::size(kSelectivities); ++s) {
    for (const auto& query : data.queries[s]) {
      uint64_t rows = 0;
      auto visit = [&](const irs::PostingMeta& meta) {
        rows += ReadDocs(data, meta, buffer);
      };
      VisitQuery(data, *it, query, visit);
      if (rows != query.rows) {
        return absl::StrCat(kSelectivities[s].name, " query [",
                            query.lo ^ kSign, ", ", query.hi ^ kSign,
                            "] reads ", rows, " rows, expected ", query.rows);
      }
    }
  }
  return {};
}

const Data& Load(const Shape& shape, Ladder ladder) {
  static std::unique_ptr<Data> gData;
  const auto key =
    absl::StrCat(shape.name, "/", kLadders[static_cast<size_t>(ladder)]);
  if (gData && gData->key == key) {
    return *gData;
  }
  gData.reset();
  auto data = std::make_unique<Data>();
  data->key = key;
  data->ladder = ladder;
  data->terms = MakeTerms(shape, ladder);
  std::vector<uint64_t> sorted(kRows);
  for (uint64_t row = 0; row != kRows; ++row) {
    sorted[row] = shape.value(row) ^ kSign;
  }
  absl::c_sort(sorted);
  for (size_t s = 0; s != std::size(kSelectivities); ++s) {
    data->queries[s] = MakeQueries(sorted, kSelectivities[s].rows);
  }
  const auto path =
    std::filesystem::temp_directory_path() / "serenedb-bench-numeric-ranges";
  std::filesystem::remove_all(path);
  std::filesystem::create_directories(path);
  data->dir = std::make_unique<irs::MMapDirectory>(path);
  Write(*data->dir, data->terms, ladder);
  static const irs::SegmentMeta kMeta{[] {
    irs::SegmentMeta meta;
    meta.name = "segment";
    return meta;
  }()};
  data->idx = std::make_unique<irs::IdxReader>(*data->dir, "segment", nullptr);
  data->fields =
    std::make_unique<irs::term_dict::FieldReader>(irs::IResourceManager::gNoop);
  data->fields->prepare(irs::ReaderState{
    .dir = data->dir.get(), .meta = &kMeta, .idx = data->idx.get()});
  data->field = data->fields->field(1);
  data->error = Validate(*data);
  gData = std::move(data);
  return *gData;
}

void Build(benchmark::State& state, const Shape& shape, Ladder ladder) {
  const auto& data = Load(shape, ladder);
  if (!data.error.empty()) {
    state.SkipWithError(data.error);
    return;
  }
  for (auto _ : state) {
    irs::MemoryDirectory dir;
    Write(dir, data.terms, ladder);
    benchmark::DoNotOptimize(dir);
  }
  irs::MemoryDirectory dir;
  Write(dir, data.terms, ladder);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * kRows));
  state.counters["terms"] = static_cast<double>(data.terms.keys.size());
  state.counters["postings"] = static_cast<double>(data.terms.docs.size());
  state.counters["bytes"] = static_cast<double>(DirectoryBytes(dir));
  state.counters["levels"] = static_cast<double>(
    static_cast<const irs::term_dict::TermReaderImpl&>(*data.field)
      .RangeLevels()
      .size());
}

void Search(benchmark::State& state, const Shape& shape, Ladder ladder,
            size_t selectivity, bool docs) {
  const auto& data = Load(shape, ladder);
  if (!data.error.empty()) {
    state.SkipWithError(data.error);
    return;
  }
  const auto& queries = data.queries[selectivity];
  auto it = data.field->iterator();
  irs::doc_id_t buffer[irs::doc_limits::kBlockSize];
  uint64_t lists = 0;
  uint64_t rows = 0;
  for (auto _ : state) {
    lists = 0;
    rows = 0;
    for (const auto& query : queries) {
      auto visit = [&](const irs::PostingMeta& meta) {
        ++lists;
        rows += docs ? ReadDocs(data, meta, buffer) : meta.docs_count;
      };
      VisitQuery(data, *it, query, visit);
    }
    benchmark::DoNotOptimize(rows);
  }
  state.SetItemsProcessed(
    static_cast<int64_t>(state.iterations() * queries.size()));
  state.counters["lists_per_query"] =
    static_cast<double>(lists) / static_cast<double>(queries.size());
  state.counters["rows_per_query"] =
    static_cast<double>(rows) / static_cast<double>(queries.size());
  state.counters["ns_per_query"] = benchmark::Counter(
    static_cast<double>(state.iterations() * queries.size()),
    benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

int Main(int argc, char** argv) {
  for (const auto& shape : kShapes) {
    for (size_t l = 0; l != std::size(kLadders); ++l) {
      const auto ladder = static_cast<Ladder>(l);
      const auto suffix = absl::StrCat(shape.name, "/", kLadders[l]);
      benchmark::RegisterBenchmark(absl::StrCat("build/", suffix),
                                   [&shape, ladder](benchmark::State& state) {
                                     Build(state, shape, ladder);
                                   })
        ->Unit(benchmark::kMillisecond);
      for (size_t s = 0; s != std::size(kSelectivities); ++s) {
        for (const bool docs : {false, true}) {
          benchmark::RegisterBenchmark(
            absl::StrCat(docs ? "docs/" : "collect/", suffix, "/",
                         kSelectivities[s].name),
            [&shape, ladder, s, docs](benchmark::State& state) {
              Search(state, shape, ladder, s, docs);
            })
            ->Unit(benchmark::kMicrosecond);
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
