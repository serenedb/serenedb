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
#include <absl/strings/str_join.h>
#include <benchmark/benchmark.h>
#include <fast_float/fast_float.h>

#include <algorithm>
#include <bit>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iresearch/formats/index/field_reader.hpp>
#include <iresearch/formats/index/field_writer.hpp>
#include <iresearch/formats/index/idx_reader.hpp>
#include <iresearch/formats/index/idx_writer.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/search/detail/bitset_of.hpp>
#include <iresearch/search/queries/multiterm_state.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/numeric_utils.hpp>
#include <memory>
#include <random>
#include <span>
#include <string>
#include <system_error>
#include <vector>

namespace {

using irs::bstring;
using irs::bytes_view;

constexpr uint64_t kRows = uint64_t{1} << 20;
constexpr uint64_t kSign = uint64_t{1} << 63;
constexpr uint32_t kStep = 16;
constexpr size_t kQueries = 64;
constexpr uint64_t kWideRows = uint64_t{1} << 24;
constexpr size_t kWideQueries = 4;
constexpr uint32_t kGistStep = 4;
constexpr size_t kGistFanout = 256;

enum class Ladder : uint8_t {
  Static,
  Adaptive,
  Gist,
};

constexpr std::string_view kLadders[]{"static", "adaptive", "gist"};

struct Shape {
  std::string_view name;
  uint64_t (*value)(uint64_t row, uint64_t rows);
};

constexpr Shape kShapes[]{
  {"dense", [](uint64_t row, uint64_t) { return row; }},
  {"timestamps", [](uint64_t row, uint64_t) { return row * 1000000; }},
  {"values5", [](uint64_t row, uint64_t) { return row % 5; }},
  {"values100", [](uint64_t row, uint64_t) { return row % 100; }},
  {"values10k", [](uint64_t row, uint64_t) { return row % 10000; }},
  {"skewed",
   [](uint64_t row, uint64_t rows) { return row < rows / 100 * 85 ? 0 : row; }},
  {"random", [](uint64_t row, uint64_t) { return row * 0x9E3779B97F4A7C15; }},
  {"random10k",
   [](uint64_t row, uint64_t) {
     return (row * 0x9E3779B97F4A7C15 >> 32) % 10000;
   }},
};

constexpr std::string_view kWideShapes[]{"dense", "random", "values10k",
                                         "skewed", "random10k"};

const Shape& ShapeNamed(std::string_view name) {
  return *absl::c_find_if(
    kShapes, [&](const Shape& shape) { return shape.name == name; });
}

struct Selectivity {
  std::string_view name;
  uint64_t rows;
  bool tail = false;
};

constexpr std::string_view kWideKinds[]{
  "point", "rows100", "pct0.1", "pct1", "pct10", "pct50", "pct90", "tail50",
};

std::vector<Selectivity> WideKinds(uint64_t rows) {
  return {
    {.name = kWideKinds[0], .rows = 1},
    {.name = kWideKinds[1], .rows = 100},
    {.name = kWideKinds[2], .rows = rows / 1000},
    {.name = kWideKinds[3], .rows = rows / 100},
    {.name = kWideKinds[4], .rows = rows / 10},
    {.name = kWideKinds[5], .rows = rows / 2},
    {.name = kWideKinds[6], .rows = rows / 10 * 9},
    {.name = kWideKinds[7], .rows = rows / 2, .tail = true},
  };
}

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

size_t MaxChildren(std::span<const uint64_t> level, uint32_t shift) {
  size_t max = 0;
  size_t run = 0;
  for (size_t i = 0; i != level.size(); ++i) {
    run = i != 0 && level[i] >> shift == level[i - 1] >> shift ? run + 1 : 1;
    max = std::max(max, run);
  }
  return max;
}

std::vector<uint32_t> GistShifts(std::vector<uint64_t> level) {
  level.erase(std::ranges::unique(level).begin(), level.end());
  std::vector<uint32_t> shifts{0};
  while (level.size() > kGistFanout) {
    uint32_t next = shifts.back();
    while (next + kGistStep < 64 &&
           MaxChildren(level, next + kGistStep) <= kGistFanout) {
      next += kGistStep;
    }
    if (next == shifts.back()) {
      break;
    }
    shifts.push_back(next);
    for (auto& value : level) {
      value = value >> next << next;
    }
    level.erase(std::ranges::unique(level).begin(), level.end());
  }
  return shifts;
}

std::vector<uint32_t> ShiftsOf(Ladder ladder,
                               const std::vector<uint64_t>& sorted) {
  switch (ladder) {
    case Ladder::Static:
      return {0, kStep, 2 * kStep, 3 * kStep};
    case Ladder::Adaptive:
      return {0};
    case Ladder::Gist:
      return GistShifts(sorted);
  }
  return {};
}

Terms MakeTerms(const Shape& shape, std::span<const uint32_t> shifts,
                uint64_t rows) {
  Terms terms;
  std::vector<std::pair<uint64_t, irs::doc_id_t>> pairs(rows);
  for (const uint32_t shift : shifts) {
    for (uint64_t row = 0; row != rows; ++row) {
      pairs[row] = {(shape.value(row, rows) ^ kSign) >> shift << shift,
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
  TermsReader(const Terms& terms, Ladder ladder) noexcept : _terms{&terms} {
    key_width = ladder == Ladder::Adaptive
                  ? static_cast<uint32_t>(terms.keys.front().size())
                  : 0;
  }

  irs::TermOnlyIterator::ptr iterator() const final {
    return irs::memory::make_managed<irs::TermOnlyIterator, TermsIterator>(
      *_terms);
  }
  irs::field_id id() const final { return 1; }
  irs::FieldProperties properties() const final {
    return {.index_features = irs::IndexFeatures::Ranges};
  }

 private:
  const Terms* _terms;
};

void Write(irs::Directory& dir, const Terms& terms, Ladder ladder,
           uint64_t rows) {
  const irs::FlushState state{
    .dir = &dir,
    .name = "segment",
    .doc_count = rows,
  };
  irs::IdxWriter idx{dir, "segment"};
  irs::term_dict::FieldWriter writer{false};
  writer.SetIdxWriter(idx);
  writer.prepare(state, 1);
  writer.write(irs::NumericLayout::kNumbers, TermsReader{terms, ladder});
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
void VisitShifts(irs::SeekTermIterator& it, std::span<const uint32_t> shifts,
                 uint64_t lo, uint64_t hi, Visit& visit) {
  const auto walk = [&](uint64_t from, uint64_t to, uint32_t shift) {
    irs::byte_type from_buf[irs::numeric_utils::kNumericTermMaxSize];
    irs::byte_type to_buf[irs::numeric_utils::kNumericTermMaxSize];
    WalkTerms(it, Key(from_buf, from, shift), Key(to_buf, to, shift), visit);
  };
  for (size_t level = 0;; ++level) {
    const uint32_t shift = shifts[level];
    if (level + 1 == shifts.size()) {
      walk(lo, hi, shift);
      return;
    }
    const uint32_t next = shifts[level + 1];
    const uint64_t mask = ((uint64_t{1} << (next - shift)) - 1) << shift;
    const uint64_t diff = uint64_t{1} << next;
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

struct QuerySet {
  std::string_view name;
  std::vector<Query> queries;
};

struct Data {
  std::string key;
  Ladder ladder;
  uint64_t rows = 0;
  std::vector<uint32_t> shifts;
  Terms terms;
  std::unique_ptr<irs::MMapDirectory> dir;
  std::unique_ptr<irs::IdxReader> idx;
  std::unique_ptr<irs::term_dict::FieldReader> fields;
  const irs::TermReader* field = nullptr;
  std::vector<QuerySet> sets;
  std::string error;
};

template<typename Visit>
void VisitQuery(const Data& data, const Query& query, Visit& visit) {
  if (data.ladder == Ladder::Adaptive) {
    VisitAdaptive(*data.field, query.lo, query.hi, visit);
  } else {
    auto it = data.field->iterator();
    VisitShifts(*it, data.shifts, query.lo, query.hi, visit);
  }
}

double Levels(const Data& data) {
  if (data.ladder != Ladder::Adaptive) {
    return static_cast<double>(data.shifts.size());
  }
  return static_cast<double>(
    static_cast<const irs::term_dict::TermReaderImpl&>(*data.field)
      .RangeLevels()
      .size());
}

irs::TermPostings::ptr OpenDocs(const Data& data,
                                const irs::PostingMeta& meta) {
  return data.fields->Postings().Postings(data.field->meta().index_features,
                                          irs::IndexFeatures::None, meta,
                                          data.field->HasScoreBounds());
}

uint64_t ReadDocs(const Data& data, const irs::PostingMeta& meta,
                  irs::doc_id_t* buffer) {
  if (meta.docs_count == 1) {
    return 1;
  }
  auto it = OpenDocs(data, meta);
  uint64_t count = 0;
  while (const auto n = it->NextDocs(buffer, nullptr)) {
    count += n;
  }
  return count;
}

std::vector<Query> MakeQueries(const std::vector<uint64_t>& sorted,
                               const Selectivity& selectivity, size_t count) {
  std::mt19937_64 rng{42};
  std::vector<Query> queries;
  const uint64_t size = sorted.size();
  for (size_t i = 0; i != count; ++i) {
    const uint64_t first = selectivity.tail
                             ? size / 4 + rng() % (size / 2)
                             : rng() % (size - selectivity.rows + 1);
    const uint64_t lo = sorted[first];
    const uint64_t hi =
      selectivity.tail ? sorted.back() : sorted[first + selectivity.rows - 1];
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
  irs::doc_id_t buffer[irs::doc_limits::kBlockSize];
  for (const auto& set : data.sets) {
    for (const auto& query : set.queries) {
      uint64_t rows = 0;
      auto visit = [&](const irs::PostingMeta& meta) {
        rows += ReadDocs(data, meta, buffer);
      };
      VisitQuery(data, query, visit);
      if (rows != query.rows) {
        return absl::StrCat(set.name, " query [", query.lo ^ kSign, ", ",
                            query.hi ^ kSign, "] reads ", rows,
                            " rows, expected ", query.rows);
      }
    }
  }
  return {};
}

void Prepare(Data& data, const Shape& shape,
             std::span<const Selectivity> selectivities, size_t queries,
             std::string_view dir_name) {
  std::vector<uint64_t> sorted(data.rows);
  for (uint64_t row = 0; row != data.rows; ++row) {
    sorted[row] = shape.value(row, data.rows) ^ kSign;
  }
  absl::c_sort(sorted);
  data.shifts = ShiftsOf(data.ladder, sorted);
  data.terms = MakeTerms(shape, data.shifts, data.rows);
  for (const auto& selectivity : selectivities) {
    data.sets.push_back({
      .name = selectivity.name,
      .queries = MakeQueries(sorted, selectivity, queries),
    });
  }
  const auto path = std::filesystem::temp_directory_path() / dir_name;
  std::filesystem::remove_all(path);
  std::filesystem::create_directories(path);
  data.dir = std::make_unique<irs::MMapDirectory>(path);
  Write(*data.dir, data.terms, data.ladder, data.rows);
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
  data.field = data.fields->fields(1).front();
  data.error = Validate(data);
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
  data->rows = kRows;
  Prepare(*data, shape, kSelectivities, kQueries,
          "serenedb-bench-numeric-ranges");
  gData = std::move(data);
  return *gData;
}

uint64_t WideRows() {
  const char* env = std::getenv("NUMERIC_RANGES_WIDE_ROWS");
  if (!env) {
    return kWideRows;
  }
  const char* const end = env + std::strlen(env);
  uint64_t rows = 0;
  const auto [ptr, ec] = fast_float::from_chars(env, end, rows);
  return ec == std::errc{} && ptr == end ? rows : 0;
}

irs::detail::BitsetStorage Fold(
  const Data& data, std::span<const irs::MultiTermState::Entry> terms) {
  auto buckets = irs::detail::DisjunctionBuckets(terms, data.field);
  return irs::detail::BuildBitset(buckets, *data.field->Handles().doc,
                                  static_cast<irs::doc_id_t>(data.rows));
}

std::string ValidateFold(const Data& data) {
  std::vector<irs::MultiTermState::Entry> terms;
  for (const auto& set : data.sets) {
    for (const auto& query : set.queries) {
      terms.clear();
      auto visit = [&](const irs::PostingMeta& meta) {
        terms.push_back({.cookie = meta});
      };
      VisitQuery(data, query, visit);
      const auto bits = Fold(data, terms);
      uint64_t rows = 0;
      for (uint32_t i = 0; i != bits.WordCount(); ++i) {
        rows += static_cast<uint64_t>(std::popcount(bits.Words()[i]));
      }
      if (rows != query.rows) {
        return absl::StrCat(set.name, " fold reads ", rows, " rows, expected ",
                            query.rows);
      }
    }
  }
  return {};
}

const Data& LoadWide(uint64_t rows, const Shape& shape, Ladder ladder) {
  static std::unique_ptr<Data> gWide;
  const auto key = absl::StrCat("wide/", rows, "/", shape.name, "/",
                                kLadders[static_cast<size_t>(ladder)]);
  if (gWide && gWide->key == key) {
    return *gWide;
  }
  gWide.reset();
  auto data = std::make_unique<Data>();
  data->key = key;
  data->ladder = ladder;
  data->rows = rows;
  Prepare(*data, shape, WideKinds(rows), kWideQueries,
          "serenedb-bench-numeric-ranges-wide");
  data->terms = {};
  if (data->error.empty()) {
    data->error = ValidateFold(*data);
  }
  gWide = std::move(data);
  return *gWide;
}

void Build(benchmark::State& state, const Shape& shape, Ladder ladder) {
  const auto& data = Load(shape, ladder);
  if (!data.error.empty()) {
    state.SkipWithError(data.error);
    return;
  }
  for (auto _ : state) {
    irs::MemoryDirectory dir;
    Write(dir, data.terms, ladder, data.rows);
    benchmark::DoNotOptimize(dir);
  }
  irs::MemoryDirectory dir;
  Write(dir, data.terms, ladder, data.rows);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * data.rows));
  state.counters["terms"] = static_cast<double>(data.terms.keys.size());
  state.counters["postings"] = static_cast<double>(data.terms.docs.size());
  state.counters["bytes"] = static_cast<double>(DirectoryBytes(dir));
  state.counters["levels"] = Levels(data);
  state.SetLabel(absl::StrJoin(data.shifts, ","));
}

void Search(benchmark::State& state, const Shape& shape, Ladder ladder,
            size_t selectivity, bool docs) {
  const auto& data = Load(shape, ladder);
  if (!data.error.empty()) {
    state.SkipWithError(data.error);
    return;
  }
  const auto& queries = data.sets[selectivity].queries;
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
      VisitQuery(data, query, visit);
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

enum class Phase : uint8_t {
  Collect,
  Open,
  Docs,
  Fold,
};

constexpr std::string_view kPhases[]{"collect", "open", "docs", "fold"};

void Wide(benchmark::State& state, const Shape& shape, Ladder ladder,
          size_t kind, Phase phase) {
  const auto rows = WideRows();
  if (rows < 10 || rows >= irs::doc_limits::eof()) {
    state.SkipWithError(
      "NUMERIC_RANGES_WIDE_ROWS must be a row count from 10 up to 2^32");
    return;
  }
  const auto& data = LoadWide(rows, shape, ladder);
  if (!data.error.empty()) {
    state.SkipWithError(data.error);
    return;
  }
  const auto& queries = data.sets[kind].queries;
  std::vector<irs::MultiTermState::Entry> terms;
  irs::doc_id_t buffer[irs::doc_limits::kBlockSize];
  uint64_t lists = 0;
  uint64_t read = 0;
  for (auto _ : state) {
    lists = 0;
    read = 0;
    for (const auto& query : queries) {
      terms.clear();
      auto visit = [&](const irs::PostingMeta& meta) {
        ++lists;
        switch (phase) {
          case Phase::Collect:
            read += meta.docs_count;
            break;
          case Phase::Open:
            if (meta.docs_count > 1) {
              auto docs = OpenDocs(data, meta);
              benchmark::DoNotOptimize(docs.get());
            }
            read += meta.docs_count;
            break;
          case Phase::Docs:
            read += ReadDocs(data, meta, buffer);
            break;
          case Phase::Fold:
            terms.push_back({.cookie = meta});
            read += meta.docs_count;
            break;
        }
      };
      VisitQuery(data, query, visit);
      if (phase == Phase::Fold) {
        auto bits = Fold(data, terms);
        benchmark::DoNotOptimize(bits.Words());
      }
    }
  }
  const auto per_query = static_cast<double>(queries.size());
  state.counters["lists_per_query"] = static_cast<double>(lists) / per_query;
  state.counters["rows_per_query"] = static_cast<double>(read) / per_query;
  state.counters["per_query"] = benchmark::Counter(
    static_cast<double>(state.iterations()) * per_query,
    benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
  state.counters["levels"] = Levels(data);
  state.counters["bytes"] = static_cast<double>(DirectoryBytes(*data.dir));
  state.SetLabel(absl::StrJoin(data.shifts, ","));
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
  for (const auto name : kWideShapes) {
    const auto& shape = ShapeNamed(name);
    for (size_t l = 0; l != std::size(kLadders); ++l) {
      const auto ladder = static_cast<Ladder>(l);
      for (size_t k = 0; k != std::size(kWideKinds); ++k) {
        for (size_t p = 0; p != std::size(kPhases); ++p) {
          const auto phase = static_cast<Phase>(p);
          benchmark::RegisterBenchmark(
            absl::StrCat("wide/", shape.name, "/", kLadders[l], "/",
                         kWideKinds[k], "/", kPhases[p]),
            [&shape, ladder, k, phase](benchmark::State& state) {
              Wide(state, shape, ladder, k, phase);
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
