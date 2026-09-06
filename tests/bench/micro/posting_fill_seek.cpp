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

// Microbenchmark: the posting decode paths, one block encoding per shape.
//
// Built through `irs::IndexWriter` and driven through filters, so nothing here
// touches the codec's internals -- but the corpus is chosen so that a given
// term's blocks all take one encoding, and the index is committed exactly once
// with no compaction, so a run measures one path and not a mixture. What each
// shape gets out of `WriteTailDelta`, which picks the smallest encoding:
//
//   run    -- every document but one   -> deltas all 1, nothing on disk
//                                         (`de_delta_all_equal_to_1`)
//   almost -- all but 1 of every 1024  -> `de_for_bitset` whose words are
//                                         almost all saturated, the shape the
//                                         encoding is really for
//   dense  -- 15 of every 16           -> deltas 1 with every 16th a 2: three
//                                         64-bit words beat 32 bytes of bitpack
//                                         (`de_for_bitset`)
//   period -- 1 of every 8             -> deltas all 8, one byte on disk
//                                         (`de_delta_all_same_08`)
//   gen    -- 1 of 8 at irregular gaps -> neither, so the general path: 6-bit
//                                         bitpacked deltas
//                                         (`de_delta_bitpack_06`)
//
// The four drivers are the four ways the engine consumes a posting list:
//
//   Advance/<shape>  `lead::Node`, one document at a time, the fallback for
//                    everything.
//   Emit/<shape>     `docs::Root`, the unscored single-term scan.
//   Window/<shape>   `fill::Node`, the bitset window a conjunction intersects
//                    in; the only driver where a whole leaf can go into the
//                    mask without materializing its documents.
//   Seek/<shape>     a ladder of seeks, `range(1)` blocks apart.
//   Conj/<a>_<b>     two lists intersected. With a dense lead that is `Window`
//                    plus the AND; with the `rare` lead it is instead the
//                    converge loop, which is the only driver that reaches
//                    `LazySeek`.
//
// Reported as items/s = documents/s, so the shapes are comparable to each other
// directly.

#include <benchmark/benchmark.h>

#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <duckdb/main/database.hpp>
#include <filesystem>
#include <iresearch/analysis/delimited_tokenizer.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/common/window.hpp>
#include <iresearch/search/count/make.hpp>
#include <iresearch/search/docs/make.hpp>
#include <iresearch/search/fill/node.hpp>
#include <iresearch/search/lead/node.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/string.hpp>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "basics/assert.h"
#include "basics/duckdb_engine.h"
#include "insert_field.hpp"

namespace {

constexpr irs::field_id kBodyId = 1;

// `w<i>` is unique per document and only exists to keep the tokenizer honest;
// it is never queried.
constexpr std::string_view kRun = "r";
constexpr std::string_view kRun2 = "r2";
constexpr std::string_view kAlmost = "a";
constexpr std::string_view kDense = "d";
constexpr std::string_view kDense2 = "d2";
constexpr std::string_view kPeriod = "p";
constexpr std::string_view kGen = "g";
// Selective enough that a conjunction leading with it takes the converge path
// (`lead_cost < docs_count / 32`) instead of the windowed block intersection,
// which is the only way to reach `LazySeek`.
constexpr std::string_view kRare = "q";
constexpr std::string_view kNeedle = "z";

// 1 of 8 on average, but with gaps that differ, so the deltas neither share a
// value nor fit a bitset.
bool IsGen(size_t i) noexcept {
  return (static_cast<uint32_t>(i) * 2654435761u) >> 29 == 0;
}

// The lead of a real intersection -- "+griffith +observatory" leads with
// "griffith". A conjunction orders by cost, so the rarest term drives, and each
// of its documents makes the others seek thousands of documents ahead: past the
// current leaf, through the skip list, into a leaf that yields one document.
// `rare` above is too dense to leave the current leaf, and periodic besides.
bool IsNeedle(size_t i) noexcept {  // ~1/1024
  return (static_cast<uint32_t>(i) * 2654435761u) >> 22 == 0;
}

std::string MakeBody(size_t i, size_t docs) {
  std::string body;
  // A term in *every* document is answered by `AllIterator`, which never opens
  // the posting list, so both runs hold out one document -- a different one, so
  // their conjunction is a run as well.
  if (i + 1 != docs) {
    body += kRun;
  }
  if (i != 0) {
    body += ' ';
    body += kRun2;
  }
  if (i % 1024 != 0) {
    body += ' ';
    body += kAlmost;
  }
  if (i % 16 != 0) {
    body += ' ';
    body += kDense;
  }
  if (i % 16 != 7) {
    body += ' ';
    body += kDense2;
  }
  if (i % 8 == 0) {
    body += ' ';
    body += kPeriod;
  }
  if (IsGen(i)) {
    body += ' ';
    body += kGen;
  }
  if (i % 64 == 0) {
    body += ' ';
    body += kRare;
  }
  if (IsNeedle(i)) {
    body += ' ';
    body += kNeedle;
  }
  body += " w";
  body += std::to_string(i);
  return body;
}

// The shapes above are the codec's corner cases, which is what they are for:
// they attribute a change to an encoding. They are not text. No English term
// sits in 15 of every 16 documents, so `de_for_bitset` and
// `de_delta_all_equal_to_1` are nearly absent from a real index, and a query in
// the benchmark game lands on words whose posting lists are all general
// bitpacked deltas of wildly different lengths.
//
// This corpus is that instead: document lengths vary, and a term's rank follows
// 1/r, so `t1` behaves like "the" and `t3000` like "observatory". Nothing about
// it is periodic.
constexpr uint32_t kVocab = 1u << 16;

uint64_t Mix(uint64_t x) noexcept {
  x += 0x9E3779B97F4A7C15ULL;
  x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
  x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
  return x ^ (x >> 31);
}

std::string ZipfTerm(uint32_t rank) { return "t" + std::to_string(rank); }

std::string MakeZipfBody(size_t i) {
  auto state = Mix(i);
  const auto terms = 16 + (state >> 58);  // 16..79, so lengths vary like text
  std::string body;
  for (uint64_t t = 0; t != terms; ++t) {
    state = Mix(state);
    // rank = kVocab^u for uniform u gives P(rank) ~ 1/rank, which is Zipf.
    const auto u =
      static_cast<double>(state >> 11) / static_cast<double>(uint64_t{1} << 53);
    body += ZipfTerm(static_cast<uint32_t>(std::pow(kVocab, u)));
    body += ' ';
  }
  body.pop_back();
  return body;
}

struct BodyField {
  irs::field_id Id() const noexcept { return kBodyId; }

  irs::analysis::Tokenizer& GetTokens() const { return stream; }

  std::string_view Value() const noexcept { return value; }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::Freq;
  }

  bool Write(irs::DataOutput&) const { return true; }

  std::string value;
  mutable irs::analysis::DelimitedTokenizer stream{" "};
};

struct Index {
  std::filesystem::path path;
  std::unique_ptr<irs::MMapDirectory> dir;
  irs::Format::ptr codec;
  irs::DirectoryReader reader;
};

// One segment, one commit, no compaction: the block encodings a query walks are
// then a property of the corpus alone.
const Index& IndexOf(size_t docs, bool zipf = false) {
  static std::map<std::pair<size_t, bool>, Index> gCache;
  auto [it, added] = gCache.try_emplace(std::pair{docs, zipf});
  auto& index = it->second;
  if (!added) {
    return index;
  }

  index.path = std::filesystem::temp_directory_path() /
               ((zipf ? "serenedb-bench-zipf-" : "serenedb-bench-posting-") +
                std::to_string(docs));
  std::filesystem::remove_all(index.path);
  std::filesystem::create_directories(index.path);
  index.dir = std::make_unique<irs::MMapDirectory>(index.path);
  index.codec = irs::formats::Get("1_5simd");

  auto* db = &sdb::DuckDBEngine::Instance().instance();
  irs::IndexWriterOptions opts;
  opts.db = db;
  opts.reader_options.db = db;
  opts.column_options = [](irs::field_id) -> irs::ColumnOptions { return {}; };
  auto writer =
    irs::IndexWriter::Make(*index.dir, index.codec, irs::kOmCreate, opts);

  {
    auto trx = writer->GetBatch();
    BodyField field;
    for (size_t i = 0; i != docs; ++i) {
      field.value = zipf ? MakeZipfBody(i) : MakeBody(i, docs);
      auto doc = trx.Insert();
      tests::InsertField(doc, field);
    }
    trx.Commit();
  }
  writer->RefreshCommit();

  index.reader = irs::DirectoryReader{*index.dir, index.codec,
                                      irs::IndexReaderOptions{.db = db}};
  SDB_ASSERT(index.reader.size() == 1,
             "corpus must be one segment or the encodings get mixed");
  return index;
}

irs::Filter::ptr Term(std::string_view term) {
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field_id() = kBodyId;
  filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  return filter;
}

irs::Filter::ptr Both(std::string_view lhs, std::string_view rhs) {
  auto filter = std::make_unique<irs::BooleanFilter>();
  filter->Add(Term(lhs), irs::Occur::Must);
  filter->Add(Term(rhs), irs::Occur::Must);
  return filter;
}

// Prepared once per benchmark, outside the timed loop: what is being measured
// is posting iteration, not filter preparation.
struct Prepared {
  irs::QueryBuilder::ptr query;

  Prepared(const irs::SubReader& segment, const irs::Filter& filter)
    : query{filter.PrepareSegment(segment, {})} {}

  irs::lead::Node::ptr Lead() const { return query->PlanLead({}); }

  irs::docs::Root::ptr Docs() const { return irs::docs::MakeRoot(*query); }

  irs::fill::Node::ptr Fill() const {
    return query->PlanFill({}, irs::ScoreMergeType::Noop);
  }
};

void Report(benchmark::State& state, size_t docs) {
  state.SetItemsProcessed(static_cast<int64_t>(docs));
  state.counters["docs"] = benchmark::Counter(
    static_cast<double>(docs) / static_cast<double>(state.iterations()));
}

// How much a single `docs::Root::Run` is asked for, the batch a consumer of
// the emit API hands it.
constexpr uint32_t kCapacity = 2048;

// -- drivers ------------------------------------------------------------

size_t Advance(const irs::lead::Node::ptr& docs) {
  size_t n = 0;
  while (!irs::doc_limits::eof(docs->Advance())) {
    ++n;
  }
  return n;
}

size_t Emit(const irs::docs::Root::ptr& docs) {
  irs::SlackBuf<irs::doc_id_t, kCapacity, irs::doc_limits::kDocsSlack> out;
  size_t n = 0;
  for (;;) {
    const auto count = docs->Run(out, kCapacity);
    benchmark::DoNotOptimize(out);
    if (count == 0) {
      return n;
    }
    n += count;
  }
}

size_t Window(const irs::fill::Node::ptr& docs) {
  irs::search::Scratch mask;
  size_t n = 0;
  irs::doc_id_t min = 0;
  for (;;) {
    irs::search::Clear(mask.data(), irs::search::kWindowWords);
    const auto next =
      docs->FillOr(min, min + irs::search::kWindowDocs, mask.data());
    for (size_t i = 0; i != irs::search::kWindowWords; ++i) {
      n += static_cast<size_t>(std::popcount(mask[i]));
    }
    if (irs::doc_limits::eof(next)) {
      return n;
    }
    min = next;
  }
}

void BmAdvance(benchmark::State& state, std::string_view term) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto filter = Term(term);
  const Prepared prepared{index.reader[0], *filter};

  size_t docs = 0;
  for (auto _ : state) {
    docs += Advance(prepared.Lead());
  }
  Report(state, docs);
}

void BmEmit(benchmark::State& state, std::string_view term) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto filter = Term(term);
  const Prepared prepared{index.reader[0], *filter};

  size_t docs = 0;
  for (auto _ : state) {
    docs += Emit(prepared.Docs());
  }
  Report(state, docs);
}

void BmWindow(benchmark::State& state, std::string_view term) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto filter = Term(term);
  const Prepared prepared{index.reader[0], *filter};

  size_t docs = 0;
  for (auto _ : state) {
    docs += Window(prepared.Fill());
  }
  Report(state, docs);
}

#define BM_SHAPES(name)                                                \
  void Bm##name##Run(benchmark::State& s) { Bm##name(s, kRun); }       \
  void Bm##name##Almost(benchmark::State& s) { Bm##name(s, kAlmost); } \
  void Bm##name##Dense(benchmark::State& s) { Bm##name(s, kDense); }   \
  void Bm##name##Period(benchmark::State& s) { Bm##name(s, kPeriod); } \
  void Bm##name##Gen(benchmark::State& s) { Bm##name(s, kGen); }

BM_SHAPES(Advance)
BM_SHAPES(Emit)
BM_SHAPES(Window)

#undef BM_SHAPES

// -- seek ---------------------------------------------------------------

// A ladder with a stride wide enough that consecutive seeks land in different
// blocks, so this measures the seek path rather than an in-block advance.
// `range(1)` is how many BLOCKS a seek should skip, not how many doc ids. The
// shapes have very different list lengths -- every doc, 15 of 16, 1 of 8 -- so
// a fixed doc-id stride makes a dense seek cross ~30 blocks while a sparse one
// crosses ~4, and then the comparison is of list length rather than encoding.
// Scaling the stride by density equalises blocks crossed per seek.
void BmSeek(benchmark::State& state, std::string_view term, double density) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& segment = index.reader[0];
  const auto filter = Term(term);
  const Prepared prepared{segment, *filter};
  const auto n = static_cast<irs::doc_id_t>(state.range(0));
  const auto blocks = static_cast<double>(state.range(1));
  const auto kStride = static_cast<irs::doc_id_t>(
    blocks * static_cast<double>(irs::doc_limits::kBlockSize) / density);

  size_t hits = 0;
  for (auto _ : state) {
    auto docs = prepared.Lead();
    for (irs::doc_id_t target = irs::doc_limits::min(); target < n;
         target += kStride) {
      if (irs::doc_limits::eof(docs->Seek(target))) {
        break;
      }
      ++hits;
    }
  }
  Report(state, hits);
}

void BmSeekRun(benchmark::State& state) { BmSeek(state, kRun, 1.0); }
void BmSeekAlmost(benchmark::State& state) {
  BmSeek(state, kAlmost, 1023.0 / 1024);
}
void BmSeekDense(benchmark::State& state) { BmSeek(state, kDense, 15.0 / 16); }
void BmSeekPeriod(benchmark::State& state) { BmSeek(state, kPeriod, 1.0 / 8); }
void BmSeekGen(benchmark::State& state) { BmSeek(state, kGen, 1.0 / 8); }

// -- conjunction --------------------------------------------------------

void BmConj(benchmark::State& state, std::string_view lhs,
            std::string_view rhs) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& segment = index.reader[0];
  const auto filter = Both(lhs, rhs);
  const Prepared prepared{segment, *filter};

  size_t docs = 0;
  for (auto _ : state) {
    docs += Emit(prepared.Docs());
  }
  Report(state, docs);
}

void BmConjRunRun(benchmark::State& state) { BmConj(state, kRun, kRun2); }
void BmConjDenseDense(benchmark::State& state) {
  BmConj(state, kDense, kDense2);
}
void BmConjRunDense(benchmark::State& state) { BmConj(state, kRun, kDense); }
// A needle against a common term: the shape of "+griffith +observatory".
void BmConjNeedleRun(benchmark::State& state) { BmConj(state, kNeedle, kRun); }
void BmConjNeedleAlmost(benchmark::State& state) {
  BmConj(state, kNeedle, kAlmost);
}
void BmConjNeedleDense(benchmark::State& state) {
  BmConj(state, kNeedle, kDense);
}
void BmConjNeedleGen(benchmark::State& state) { BmConj(state, kNeedle, kGen); }

void BmConjRareRun(benchmark::State& state) { BmConj(state, kRare, kRun); }
void BmConjRareAlmost(benchmark::State& state) {
  BmConj(state, kRare, kAlmost);
}
void BmConjRareDense(benchmark::State& state) { BmConj(state, kRare, kDense); }
void BmConjRareGen(benchmark::State& state) { BmConj(state, kRare, kGen); }
void BmConjGenRun(benchmark::State& state) { BmConj(state, kGen, kRun); }
void BmConjGenAlmost(benchmark::State& state) { BmConj(state, kGen, kAlmost); }
void BmConjGenDense(benchmark::State& state) { BmConj(state, kGen, kDense); }

// -- text ---------------------------------------------------------------

// `Text/<rank>_<rank>` is an intersection of two English-like terms, counted
// the way `bench::Executor::ExecuteCount` counts: preparation included,
// because the harness pays it per query. Ranks are chosen to span what the
// benchmark game asks for -- `+the +english +restoration` leads with a term of
// rank ~3000 and intersects one of rank 1, `+griffith +observatory` is two
// terms in the thousands.
void BmText(benchmark::State& state, uint32_t lhs, uint32_t rhs) {
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)), true);
  const auto& segment = index.reader[0];
  const auto lhs_term = ZipfTerm(lhs);
  const auto rhs_term = ZipfTerm(rhs);
  const auto filter = Both(lhs_term, rhs_term);

  size_t docs = 0;
  for (auto _ : state) {
    auto query = filter->PrepareSegment(segment, {});
    auto plan = query ? irs::count::MakeRoot(*query) : irs::count::Root::ptr{};
    docs += plan ? plan->Run() : 0;
  }
  Report(state, docs);
}

void BmText1x3000(benchmark::State& s) { BmText(s, 1, 3000); }
void BmText8x900(benchmark::State& s) { BmText(s, 8, 900); }
void BmText40x400(benchmark::State& s) { BmText(s, 40, 400); }
void BmText200x2000(benchmark::State& s) { BmText(s, 200, 2000); }
void BmText1500x4000(benchmark::State& s) { BmText(s, 1500, 4000); }

void Sizes(benchmark::internal::Benchmark* b) {
  b->Arg(1'000'000)->Unit(benchmark::kMicrosecond);
}

void SeekSizes(benchmark::internal::Benchmark* b) {
  for (int blocks : {1, 4, 32}) {
    b->Args({1'000'000, blocks});
  }
  b->Unit(benchmark::kMicrosecond);
}

BENCHMARK(BmText1x3000)->Apply(Sizes);
BENCHMARK(BmText8x900)->Apply(Sizes);
BENCHMARK(BmText40x400)->Apply(Sizes);
BENCHMARK(BmText200x2000)->Apply(Sizes);
BENCHMARK(BmText1500x4000)->Apply(Sizes);

BENCHMARK(BmAdvanceRun)->Apply(Sizes);
BENCHMARK(BmAdvanceAlmost)->Apply(Sizes);
BENCHMARK(BmAdvanceDense)->Apply(Sizes);
BENCHMARK(BmAdvancePeriod)->Apply(Sizes);
BENCHMARK(BmAdvanceGen)->Apply(Sizes);

BENCHMARK(BmEmitRun)->Apply(Sizes);
BENCHMARK(BmEmitAlmost)->Apply(Sizes);
BENCHMARK(BmEmitDense)->Apply(Sizes);
BENCHMARK(BmEmitPeriod)->Apply(Sizes);
BENCHMARK(BmEmitGen)->Apply(Sizes);

BENCHMARK(BmWindowRun)->Apply(Sizes);
BENCHMARK(BmWindowAlmost)->Apply(Sizes);
BENCHMARK(BmWindowDense)->Apply(Sizes);
BENCHMARK(BmWindowPeriod)->Apply(Sizes);
BENCHMARK(BmWindowGen)->Apply(Sizes);

BENCHMARK(BmSeekRun)->Apply(SeekSizes);
BENCHMARK(BmSeekAlmost)->Apply(SeekSizes);
BENCHMARK(BmSeekDense)->Apply(SeekSizes);
BENCHMARK(BmSeekPeriod)->Apply(SeekSizes);
BENCHMARK(BmSeekGen)->Apply(SeekSizes);

BENCHMARK(BmConjRunRun)->Apply(Sizes);
BENCHMARK(BmConjDenseDense)->Apply(Sizes);
BENCHMARK(BmConjRunDense)->Apply(Sizes);
BENCHMARK(BmConjNeedleRun)->Apply(Sizes);
BENCHMARK(BmConjNeedleAlmost)->Apply(Sizes);
BENCHMARK(BmConjNeedleDense)->Apply(Sizes);
BENCHMARK(BmConjNeedleGen)->Apply(Sizes);
BENCHMARK(BmConjRareRun)->Apply(Sizes);
BENCHMARK(BmConjRareAlmost)->Apply(Sizes);
BENCHMARK(BmConjRareDense)->Apply(Sizes);
BENCHMARK(BmConjRareGen)->Apply(Sizes);
BENCHMARK(BmConjGenRun)->Apply(Sizes);
BENCHMARK(BmConjGenAlmost)->Apply(Sizes);
BENCHMARK(BmConjGenDense)->Apply(Sizes);

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
