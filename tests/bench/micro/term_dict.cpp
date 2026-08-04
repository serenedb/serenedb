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

// Perf gate for the v2 term dictionary: every case must be at least as good as
// the burst trie it replaces. Reported per corpus shape (arg `shape`) and
// document population (arg `docs`, see DocShape):
//
//   Build        -- dictionary write time for the whole field
//   PointSeek    -- RandomOnly exact seek, probes in random order
//   BatchSeek    -- the whole sorted probe set through `BatchIterator`
//   BatchSeekMisses -- the same with a miss between every pair of terms
//   Iterate      -- seek_ge(min) then next() to exhaustion (facet/merge shape)
//   SeekGe       -- seek_ge to a random miss, then one next()
//   Automaton    -- 4-byte prefix wildcard walk, metacharacters unescaped
//   AutomatonPrefix -- the same, escaped, i.e. a true `literal%` acceptor
//   Levenshtein  -- parametric fuzzy acceptor at distance 1 and 2
//   Regexp       -- regexp walk over patterns built from real key fragments
//   RegexpBuild  -- what a regexp query pays before it reads a term
//
// Variants, all term_dict -- attribution only, since there is one dictionary:
// v2 = the catalog row_group_size (what production writes), v2whole = the row
// group spanning the segment (isolates the dictionary from the partitioning),
// v2raw = v2 with FSST off, v2small = v2 at restart interval 8. Comparisons
// against the pre-rewrite dictionary are made against an old binary, not here.
//
// `IdxBytes` / `DocBytes` are emitted on Build, `ResidentOpenBytes` and
// `ResidentBytes` on PointSeek.

#include "iresearch/formats/index/term_dict.hpp"

#include <benchmark/benchmark.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "absl/strings/str_cat.h"
#include "basics/duckdb_engine.h"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/search/term_acceptor.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/utils/levenshtein_acceptor.hpp"
#include "iresearch/utils/levenshtein_utils.hpp"
#include "iresearch/utils/regexp_acceptor.hpp"
#include "iresearch/utils/regexp_utils.hpp"
#include "iresearch/utils/wildcard_utils.hpp"

namespace {

constexpr std::string_view kCodec = "1_5simd";
constexpr irs::field_id kFieldId = 1;
constexpr std::string_view kSegment = "bench";
constexpr size_t kTerms = 200000;
constexpr size_t kDocsPerTerm = 8;

enum class Shape {
  Word,
  Uuid,
  Numeric,
  Ngram,
};

std::string_view ShapeName(Shape shape) {
  switch (shape) {
    case Shape::Word:
      return "word";
    case Shape::Uuid:
      return "uuid";
    case Shape::Numeric:
      return "numeric";
    case Shape::Ngram:
      return "ngram";
  }
  return "?";
}

uint64_t Mix(uint64_t x) {
  x = x * 6364136223846793005ULL + 1442695040888963407ULL;
  x ^= x >> 31;
  return x;
}

std::vector<irs::bstring> MakeTerms(Shape shape, size_t count) {
  static constexpr std::string_view kSyllables[] = {
    "ka",  "re",  "shi", "to",  "mu",  "na",  "ri",  "ze",  "bo",  "lu",
    "tan", "ver", "sil", "mor", "dan", "kul", "pes", "rin", "vol", "hem",
  };
  static constexpr char kHex[] = "0123456789abcdef";

  std::vector<irs::bstring> terms;
  terms.reserve(count);
  for (size_t i = 0; i != count; ++i) {
    std::string term;
    uint64_t x = Mix(i + 1);
    switch (shape) {
      case Shape::Word:
        term = "the_quick_brown_";
        for (size_t j = 0; j != 8; ++j) {
          x = Mix(x);
          term += kSyllables[(x >> 29) % std::size(kSyllables)];
        }
        break;
      case Shape::Uuid:
        for (size_t j = 0; j != 32; ++j) {
          if (j == 8 || j == 12 || j == 16 || j == 20) {
            term += '-';
          }
          x = Mix(x);
          term += kHex[(x >> 29) & 0xF];
        }
        break;
      case Shape::Numeric:
        term =
          absl::StrCat(absl::Dec(Mix(i) % 1000000000ULL, absl::kZeroPad10));
        break;
      case Shape::Ngram:
        for (size_t j = 0; j != 4; ++j) {
          x = Mix(x);
          term += static_cast<char>('a' + (x >> 29) % 26);
        }
        break;
    }
    terms.emplace_back(irs::ViewCast<irs::byte_type>(std::string_view{term}));
  }
  std::ranges::sort(terms);
  terms.erase(std::unique(terms.begin(), terms.end()), terms.end());
  return terms;
}

// A term's postings, generated rather than stored: an arithmetic progression
// of doc ids so a corpus of any document count costs no memory. `first` is
// below `stride` and `count * stride <= segment_docs`, so the last doc always
// lands inside the segment.
struct DocRun {
  irs::doc_id_t first;
  irs::doc_id_t stride;
  uint32_t count;
};

class Postings : public irs::DocIterator {
 public:
  explicit Postings(DocRun run) : _run{run} {
    _attrs[irs::Type<irs::AttrProviderChangeAttr>::id()] = &_callback;
    _attrs[irs::Type<irs::FreqBlockAttr>::id()] = &_freq_block;
  }

  irs::doc_id_t advance() final {
    if (!irs::doc_limits::valid(_doc)) {
      _callback(*this);
    }
    if (_next == _run.count) {
      return _doc = irs::doc_limits::eof();
    }
    _doc = _run.first + _next * _run.stride;
    _freq = 1 + _next % 5;
    ++_next;
    return _doc;
  }

  irs::doc_id_t seek(irs::doc_id_t target) final {
    irs::seek(*this, target);
    return value();
  }

  uint32_t GetFreq() const final { return _freq; }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
    const auto it = _attrs.find(type);
    return it == _attrs.end() ? nullptr : it->second;
  }

  IRS_DOC_ITERATOR_DEFAULTS

 private:
  std::map<irs::TypeInfo::type_id, irs::Attribute*> _attrs;
  DocRun _run;
  uint32_t _next{0};
  uint32_t _freq{0};
  irs::FreqBlockAttr _freq_block{.value = &_freq};
  irs::AttrProviderChangeAttr _callback;
};

// Document population of the segment the dictionary is written for.
//   Flat -- 8 documents shared by every term. One row group at any realistic
//           size, so the rg model is degenerate; the reference shape §3 used.
//   Wide -- kWideDocs documents with a three-class df distribution, so the
//           catalog row_group_size actually cuts a term's postings.
enum class DocShape {
  Flat,
  Wide,
};

constexpr size_t kWideDocs = 300000;

std::string_view DocShapeName(DocShape shape) {
  return shape == DocShape::Flat ? "flat" : "wide";
}

DocRun RunFor(DocShape shape, size_t term) {
  if (shape == DocShape::Flat) {
    return {.first = irs::doc_limits::min(),
            .stride = 1,
            .count = static_cast<uint32_t>(kDocsPerTerm)};
  }
  // 0.5% dense, 5% medium, the rest sparse -- a term's whole run always fits
  // the segment because count * stride == kWideDocs.
  uint32_t count = 8;
  uint32_t stride = kWideDocs / 8;
  if (term % 200 == 0) {
    count = 9375;
    stride = kWideDocs / 9375;
  } else if (term % 20 == 0) {
    count = 625;
    stride = kWideDocs / 625;
  }
  return {.first = irs::doc_limits::min() +
                   static_cast<irs::doc_id_t>(Mix(term) % stride),
          .stride = stride,
          .count = count};
}

class Terms : public irs::SourceTermIterator {
 public:
  Terms(const std::vector<irs::bstring>& terms, DocShape docs)
    : _terms{&terms}, _docs{docs} {}

  bool next() final {
    if (_next == _terms->size()) {
      return false;
    }
    _value = (*_terms)[_next++];
    return true;
  }

  irs::bytes_view value() const noexcept final { return _value; }

  irs::DocIterator::ptr postings(irs::IndexFeatures) const final {
    return irs::memory::make_managed<Postings>(RunFor(_docs, _next - 1));
  }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id) noexcept final {
    return nullptr;
  }

 private:
  const std::vector<irs::bstring>* _terms;
  DocShape _docs;
  irs::bytes_view _value;
  size_t _next{0};
};

class Field : public irs::BasicTermReader {
 public:
  Field(irs::SourceTermIterator& it, const std::vector<irs::bstring>& terms,
        irs::IndexFeatures features)
    : _it{&it}, _terms{&terms} {
    _meta.id = kFieldId;
    _meta.index_features = features;
  }

 private:
  irs::Attribute* GetMutable(irs::TypeInfo::type_id) noexcept final {
    return nullptr;
  }
  irs::SourceTermIterator::ptr iterator() const final {
    return irs::memory::to_managed<irs::SourceTermIterator>(*_it);
  }
  irs::field_id id() const final { return _meta.id; }
  irs::FieldProperties properties() const final { return _meta; }
  irs::bytes_view min() const final { return _terms->front(); }
  irs::bytes_view max() const final { return _terms->back(); }

  irs::SourceTermIterator* _it;
  const std::vector<irs::bstring>* _terms;
  irs::FieldMeta _meta;
};

struct Corpus {
  std::vector<irs::bstring> terms;
  std::vector<uint32_t> probe_order;
};

const Corpus& GetCorpus(Shape shape) {
  static std::map<Shape, Corpus> cache;
  auto it = cache.find(shape);
  if (it != cache.end()) {
    return it->second;
  }
  Corpus corpus;
  corpus.terms = MakeTerms(shape, kTerms);
  corpus.probe_order.resize(corpus.terms.size());
  std::iota(corpus.probe_order.begin(), corpus.probe_order.end(), 0);
  std::shuffle(corpus.probe_order.begin(), corpus.probe_order.end(),
               std::mt19937{42});
  return cache.emplace(shape, std::move(corpus)).first->second;
}

constexpr auto kFeatures = irs::IndexFeatures::Freq;

irs::doc_id_t SegmentDocs(DocShape docs) {
  return docs == DocShape::Flat
           ? irs::doc_limits::min() + static_cast<irs::doc_id_t>(kDocsPerTerm)
           : irs::doc_limits::min() + static_cast<irs::doc_id_t>(kWideDocs);
}

irs::FlushState MakeFlushState(irs::Directory& dir, DocShape docs) {
  return irs::FlushState{
    .dir = &dir,
    .name = kSegment,
    .doc_count = SegmentDocs(docs),
    .index_features = kFeatures,
  };
}

template<typename W>
void Write(irs::Directory& dir, const Corpus& corpus, DocShape docs,
           W&& make_writer) {
  auto codec = irs::formats::Get(kCodec);
  Terms terms{corpus.terms, docs};
  Field field{terms, corpus.terms, kFeatures};
  const auto state = MakeFlushState(dir, docs);
  irs::IdxWriter idx{dir, kSegment, ::sdb::DuckDBEngine::Instance().instance()};
  auto writer = make_writer(*codec);
  writer->SetIdxWriter(idx);
  writer->prepare(state);
  writer->write(field);
  writer->end();
  idx.Commit();
}

// The wired variant: the catalog forwards its one `row_group_size`, so this is
// what production writes.
auto MakeV2(const irs::Format& codec) {
  auto writer = std::make_unique<irs::term_dict::FieldWriter>(
    codec.get_postings_writer(false, irs::IResourceManager::gNoop), false,
    irs::IResourceManager::gNoop);
  irs::term_dict::WriterOptions options;
  options.row_group_size = DEFAULT_ROW_GROUP_SIZE;
  writer->SetOptions(options);
  return writer;
}

// v2 with the row group spanning the segment: isolates the dictionary rewrite
// from the row-group partitioning.
auto MakeV2Whole(const irs::Format& codec) {
  auto writer = std::make_unique<irs::term_dict::FieldWriter>(
    codec.get_postings_writer(false, irs::IResourceManager::gNoop), false,
    irs::IResourceManager::gNoop);
  irs::term_dict::WriterOptions options;
  options.row_group_size = irs::term_dict::kRowGroupSizeUnbounded;
  writer->SetOptions(options);
  return writer;
}

auto MakeV2Small(const irs::Format& codec) {
  auto writer = std::make_unique<irs::term_dict::FieldWriter>(
    codec.get_postings_writer(false, irs::IResourceManager::gNoop), false,
    irs::IResourceManager::gNoop);
  irs::term_dict::WriterOptions options;
  options.row_group_size = DEFAULT_ROW_GROUP_SIZE;
  options.restart_interval = 8;
  writer->SetOptions(options);
  return writer;
}

auto MakeV2Raw(const irs::Format& codec) {
  auto writer = std::make_unique<irs::term_dict::FieldWriter>(
    codec.get_postings_writer(false, irs::IResourceManager::gNoop), false,
    irs::IResourceManager::gNoop);
  irs::term_dict::WriterOptions options;
  options.row_group_size = DEFAULT_ROW_GROUP_SIZE;
  options.fsst_enabled = false;
  writer->SetOptions(options);
  return writer;
}

uint64_t FileSize(const irs::Directory& dir, std::string_view ext) {
  uint64_t size = 0;
  dir.length(size, absl::StrCat(kSegment, ".", ext));
  return size;
}

template<typename Reader>
struct Opened {
  std::unique_ptr<irs::MemoryDirectory> dir;
  irs::SegmentMeta meta;
  std::unique_ptr<irs::IdxReader> idx;
  std::unique_ptr<Reader> fields;
  const irs::TermReader* field{};
};

template<typename Reader, typename W>
Opened<Reader> Build(const Corpus& corpus, DocShape docs, W&& make_writer) {
  Opened<Reader> out;
  out.dir = std::make_unique<irs::MemoryDirectory>();
  Write(*out.dir, corpus, docs, make_writer);
  auto codec = irs::formats::Get(kCodec);
  out.meta.name = kSegment;
  out.idx = std::make_unique<irs::IdxReader>(*out.dir, kSegment);
  out.fields = std::make_unique<Reader>(codec->get_postings_reader(),
                                        irs::IResourceManager::gNoop);
  out.fields->prepare(irs::ReaderState{
    .dir = out.dir.get(), .meta = &out.meta, .idx = out.idx.get()});
  out.field = out.fields->field(kFieldId);
  return out;
}

template<typename Reader, typename W>
void BenchBuild(benchmark::State& state, Shape shape, DocShape docs,
                W make_writer) {
  const auto& corpus = GetCorpus(shape);
  uint64_t idx_bytes = 0;
  for (auto _ : state) {
    irs::MemoryDirectory dir;
    Write(dir, corpus, docs, make_writer);
    idx_bytes = FileSize(dir, "idx");
    benchmark::DoNotOptimize(idx_bytes);
  }
  state.counters["terms"] = static_cast<double>(corpus.terms.size());
  state.counters["IdxBytes"] = static_cast<double>(idx_bytes);
  state.counters["DocBytes"] = static_cast<double>([&] {
    irs::MemoryDirectory dir;
    Write(dir, corpus, docs, make_writer);
    return FileSize(dir, "doc");
  }());
}

template<typename Reader, typename W>
void BenchPointSeek(benchmark::State& state, Shape shape, DocShape docs,
                    W make_writer) {
  const auto& corpus = GetCorpus(shape);
  auto opened = Build<Reader>(corpus, docs, make_writer);
  const auto resident_open = opened.fields->CountMappedMemory();
  size_t i = 0;
  size_t hits = 0;
  for (auto _ : state) {
    auto it = opened.field->iterator(irs::SeekMode::RandomOnly);
    const auto& term = corpus.terms[corpus.probe_order[i]];
    hits += it->seek(term) ? 1 : 0;
    if (++i == corpus.probe_order.size()) {
      i = 0;
    }
  }
  state.counters["hits"] = static_cast<double>(hits);
  state.counters["ResidentOpenBytes"] = static_cast<double>(resident_open);
  state.counters["ResidentBytes"] =
    static_cast<double>(opened.fields->CountMappedMemory());
}

// The wired batch path: the whole sorted probe set through `BatchIterator`,
// which is a loop of seeks for a dictionary with no batch backend and one
// forward pass for term_dict. `probes` covers every term plus a miss between
// each pair, so both outcomes drive the pass.
template<typename Reader, typename W>
void BenchBatchSeek(benchmark::State& state, Shape shape, DocShape docs,
                    W make_writer) {
  const auto& corpus = GetCorpus(shape);
  auto opened = Build<Reader>(corpus, docs, make_writer);
  std::vector<irs::bytes_view> probes;
  probes.reserve(corpus.terms.size());
  for (const auto& term : corpus.terms) {
    probes.emplace_back(term);
  }
  size_t hits = 0;
  for (auto _ : state) {
    auto it = opened.field->BatchIterator(probes);
    size_t count = 0;
    while (it->next()) {
      ++count;
    }
    hits = count;
    benchmark::DoNotOptimize(count);
  }
  state.counters["hits"] = static_cast<double>(hits);
  state.SetItemsProcessed(state.iterations() *
                          static_cast<int64_t>(probes.size()));
}

// The same probe set with a miss between every pair of terms: half the probes
// resolve to nothing, which is the shape an IN-list over a large dictionary
// actually has.
template<typename Reader, typename W>
void BenchBatchSeekMisses(benchmark::State& state, Shape shape, DocShape docs,
                          W make_writer) {
  const auto& corpus = GetCorpus(shape);
  auto opened = Build<Reader>(corpus, docs, make_writer);
  std::vector<irs::bstring> owned;
  owned.reserve(2 * corpus.terms.size());
  for (const auto& term : corpus.terms) {
    owned.emplace_back(term);
    irs::bstring miss{term};
    miss.push_back(0);
    owned.emplace_back(std::move(miss));
  }
  std::ranges::sort(owned);
  std::vector<irs::bytes_view> probes;
  probes.reserve(owned.size());
  for (const auto& term : owned) {
    probes.emplace_back(term);
  }
  size_t hits = 0;
  for (auto _ : state) {
    auto it = opened.field->BatchIterator(probes);
    size_t count = 0;
    while (it->next()) {
      ++count;
    }
    hits = count;
    benchmark::DoNotOptimize(count);
  }
  state.counters["hits"] = static_cast<double>(hits);
  state.SetItemsProcessed(state.iterations() *
                          static_cast<int64_t>(probes.size()));
}

template<typename Reader, typename W>
void BenchIterate(benchmark::State& state, Shape shape, DocShape docs,
                  W make_writer) {
  const auto& corpus = GetCorpus(shape);
  auto opened = Build<Reader>(corpus, docs, make_writer);
  for (auto _ : state) {
    auto it = opened.field->iterator(irs::SeekMode::NORMAL);
    size_t count = 0;
    while (it->next()) {
      count += it->value().size();
    }
    benchmark::DoNotOptimize(count);
  }
  state.SetItemsProcessed(state.iterations() *
                          static_cast<int64_t>(corpus.terms.size()));
}

template<typename Reader, typename W>
void BenchSeekGe(benchmark::State& state, Shape shape, DocShape docs,
                 W make_writer) {
  const auto& corpus = GetCorpus(shape);
  auto opened = Build<Reader>(corpus, docs, make_writer);
  std::vector<irs::bstring> probes;
  probes.reserve(corpus.probe_order.size());
  for (const auto i : corpus.probe_order) {
    irs::bstring probe{corpus.terms[i]};
    probe.push_back(0);
    probes.emplace_back(std::move(probe));
  }
  size_t i = 0;
  for (auto _ : state) {
    auto it = opened.field->iterator(irs::SeekMode::NORMAL);
    const auto res = it->seek_ge(probes[i]);
    benchmark::DoNotOptimize(res);
    if (++i == probes.size()) {
      i = 0;
    }
  }
}

template<typename Reader, typename W>
void BenchAutomaton(benchmark::State& state, Shape shape, DocShape docs,
                    W make_writer) {
  const auto& corpus = GetCorpus(shape);
  auto opened = Build<Reader>(corpus, docs, make_writer);
  const auto prefix = corpus.terms[corpus.terms.size() / 2].substr(0, 4);
  irs::bstring pattern{prefix};
  pattern.push_back('%');
  const auto source = irs::MakePatternSource(
    pattern, irs::PatternKind::Wildcard, irs::RegexpSyntax::Perl);
  size_t matched = 0;
  for (auto _ : state) {
    auto it = source->Iterator(*opened.field);
    size_t count = 0;
    while (it->next()) {
      ++count;
    }
    matched = count;
    benchmark::DoNotOptimize(count);
  }
  state.counters["matched"] = static_cast<double>(matched);
}

// A genuine literal-prefix wildcard: the same 4-byte corpus prefix with the
// wildcard metacharacters escaped, so the pattern is `literal%` on every shape.
// `Automaton` above deliberately keeps its unescaped pattern for continuity
// with the earlier gate runs, and on `word` that pattern is `the_%`, whose `_`
// is an any-character wildcard rather than part of a prefix.
template<typename Reader, typename W>
void BenchAutomatonPrefix(benchmark::State& state, Shape shape, DocShape docs,
                          W make_writer) {
  const auto& corpus = GetCorpus(shape);
  auto opened = Build<Reader>(corpus, docs, make_writer);
  irs::bstring pattern;
  for (const auto byte : corpus.terms[corpus.terms.size() / 2].substr(0, 4)) {
    if (byte == '%' || byte == '_' || byte == '\\') {
      pattern.push_back('\\');
    }
    pattern.push_back(byte);
  }
  pattern.push_back('%');
  const auto source = irs::MakePatternSource(
    pattern, irs::PatternKind::Wildcard, irs::RegexpSyntax::Perl);
  size_t matched = 0;
  for (auto _ : state) {
    auto it = source->Iterator(*opened.field);
    size_t count = 0;
    while (it->next()) {
      ++count;
    }
    matched = count;
    benchmark::DoNotOptimize(count);
  }
  state.counters["matched"] = static_cast<double>(matched);
}

// The head-and-tail wildcard, `literal%literal`: the shape a range bounds at
// one end and a byte comparison settles at the other, so nothing about it
// needs an automaton. The head is the same 4-byte corpus prefix the row above
// uses; the tail is the last two bytes of the same key.
template<typename Reader, typename W>
void BenchAutomatonHeadTail(benchmark::State& state, Shape shape, DocShape docs,
                            W make_writer) {
  const auto& corpus = GetCorpus(shape);
  auto opened = Build<Reader>(corpus, docs, make_writer);
  const auto& mid = corpus.terms[corpus.terms.size() / 2];
  const auto escape = [](irs::bytes_view part, irs::bstring& out) {
    for (const auto byte : part) {
      if (byte == '%' || byte == '_' || byte == '\\') {
        out.push_back('\\');
      }
      out.push_back(byte);
    }
  };
  irs::bstring pattern;
  escape(mid.substr(0, 4), pattern);
  pattern.push_back('%');
  escape(mid.substr(mid.size() - 2), pattern);
  const auto source = irs::MakePatternSource(
    pattern, irs::PatternKind::Wildcard, irs::RegexpSyntax::Perl);
  size_t matched = 0;
  for (auto _ : state) {
    auto it = source->Iterator(*opened.field);
    size_t count = 0;
    while (it->next()) {
      ++count;
    }
    matched = count;
    benchmark::DoNotOptimize(count);
  }
  state.counters["matched"] = static_cast<double>(matched);
}

// The regexp suite: patterns built from real key fragments, compiled once
// outside the timed loop, so the row is the per-walk cost rather than the
// compile.
//
// arg2 picks the pattern shape, all three of which a term regexp really has:
//   0 -- a literal prefix of a corpus term plus `.*`
//   1 -- the same prefix with a bounded any-char run and a literal tail
//   2 -- an alternation of two corpus prefixes, each with a character class
//   3 -- an infix literal: nothing bounds it and nothing seeks to it
//   4 -- an alternation whose second branch the corpus holds nowhere, so the
//        walk has to find that range empty rather than assume it
std::string RegexpFor(const Corpus& corpus, Shape corpus_shape, int shape) {
  const auto& mid = corpus.terms[corpus.terms.size() / 2];
  const auto& other = corpus.terms[corpus.terms.size() / 3];
  auto sub = [](irs::bytes_view term, size_t from, size_t len) {
    std::string out;
    for (size_t i = from; i != std::min(from + len, term.size()); ++i) {
      const auto byte = static_cast<char>(term[i]);
      if (std::strchr(".*+?|()[]^$\\{}", byte)) {
        out.push_back('\\');
      }
      out.push_back(byte);
    }
    return out;
  };
  switch (shape) {
    case 0:
      return sub(mid, 0, 4) + ".*";
    case 1:
      // An interior any-char, i.e. the shape no prefix range can answer.
      if (mid.size() >= 8) {
        return sub(mid, 0, 3) + "." + sub(mid, 4, 3) + ".*";
      }
      return sub(mid, 0, 1) + "." + sub(mid, 2, mid.size() - 2);
    case 2:
      return "(" + sub(mid, 0, 3) + "|" + sub(other, 0, 3) + ")[a-z0-9_].*";
    case 3:
      // One whole syllable of the word alphabet, which lands on a third of
      // that corpus; elsewhere a fragment cut from the middle of a real key,
      // which is the same shape at a different selectivity.
      return ".*" +
             (corpus_shape == Shape::Word ? std::string{"sil"}
                                          : sub(mid, mid.size() / 2, 3)) +
             ".*";
    default: {
      auto live = sub(mid, 0, 3);
      auto dead = live;
      if (dead.size() == 3) {
        std::swap(dead[0], dead[1]);
      }
      return "(" + live + "|" + dead + ")[a-z0-9_].*";
    }
  }
}

template<typename Reader, typename W>
void BenchRegexp(benchmark::State& state, Shape shape, DocShape docs,
                 W make_writer) {
  const auto& corpus = GetCorpus(shape);
  auto opened = Build<Reader>(corpus, docs, make_writer);
  const auto pattern =
    RegexpFor(corpus, shape, static_cast<int>(state.range(2)));
  const auto bytes = irs::ViewCast<irs::byte_type>(std::string_view{pattern});
  // The acceptor is built once, outside the timed loop: this row is the
  // per-walk cost. What it costs to build is `RegexpBuild` below, and it is a
  // per-(query, segment) cost in production -- an acceptor owns a live DFA walk
  // and drives one intersection at a time.
  const irs::RegexpAcceptor re2{bytes};
  size_t matched = 0;
  for (auto _ : state) {
    auto it = opened.field->iterator(re2);
    size_t count = 0;
    while (it->next()) {
      ++count;
    }
    matched = count;
    benchmark::DoNotOptimize(count);
  }
  state.counters["matched"] = static_cast<double>(matched);
}

// What a regexp query pays before it reads a term: RE2's parse + rewrite +
// compile, which leaves the DFA unbuilt.
void BenchRegexpBuild(benchmark::State& state, Shape shape) {
  const auto& corpus = GetCorpus(shape);
  const auto pattern =
    RegexpFor(corpus, shape, static_cast<int>(state.range(1)));
  const auto bytes = irs::ViewCast<irs::byte_type>(std::string_view{pattern});
  for (auto _ : state) {
    const irs::RegexpAcceptor acceptor{bytes};
    benchmark::DoNotOptimize(acceptor.Start());
  }
}

// The fuzzy suite: a parametric Levenshtein acceptor at distance 1 and 2 over
// a term drawn from the corpus. A different arc shape than the wildcard walk
// (dense ranges, many states), and the one the plan gates separately.
template<typename Reader, typename W>
void BenchLevenshtein(benchmark::State& state, Shape shape, DocShape docs,
                      W make_writer) {
  const auto& corpus = GetCorpus(shape);
  auto opened = Build<Reader>(corpus, docs, make_writer);
  const auto distance = static_cast<irs::byte_type>(state.range(2));
  const auto& target = corpus.terms[corpus.terms.size() / 2];
  const auto& description = irs::MakeParametricDescription(distance, false);
  const irs::LevenshteinAcceptor parametric{description, {}, target};
  size_t matched = 0;
  for (auto _ : state) {
    auto it = opened.field->iterator(parametric);
    size_t count = 0;
    while (it->next()) {
      ++count;
    }
    matched = count;
    benchmark::DoNotOptimize(count);
  }
  state.counters["matched"] = static_cast<double>(matched);
}

// What a fuzzy query pays before it reads a single term: the parametric
// acceptor's characteristic vectors. Excluded from the walk cases above, which
// build their acceptor once outside the timed loop, and paid per query in
// production -- `LevenshteinAutomatonOptions` builds it at filter lowering.
void BenchLevAcceptor(benchmark::State& state, Shape shape) {
  const auto& corpus = GetCorpus(shape);
  const auto distance = static_cast<irs::byte_type>(state.range(1));
  const auto& target = corpus.terms[corpus.terms.size() / 2];
  const auto& description = irs::MakeParametricDescription(distance, false);
  for (auto _ : state) {
    const irs::LevenshteinAcceptor acceptor{description, {}, target};
    benchmark::DoNotOptimize(acceptor.Start().pstate);
  }
}

}  // namespace

// arg0 = Shape, arg1 = Levenshtein distance.
#define TERM_DICT_LEV_ACCEPTOR(SUFFIX)                           \
  static void LevAcceptor_##SUFFIX(benchmark::State& state) {    \
    BenchLevAcceptor(state, static_cast<Shape>(state.range(0))); \
  }                                                              \
  BENCHMARK(LevAcceptor_##SUFFIX)                                \
    ->ArgNames({"shape", "dist"})                                \
    ->ArgsProduct({benchmark::CreateDenseRange(0, 3, 1),         \
                   benchmark::CreateDenseRange(1, 2, 1)})        \
    ->Unit(benchmark::kMicrosecond)

TERM_DICT_LEV_ACCEPTOR(parametric);

// arg0 = Shape, arg1 = DocShape.
#define TERM_DICT_VARIANT(NAME, FN, SUFFIX, READER, WRITER)    \
  static void NAME##_##SUFFIX(benchmark::State& state) {       \
    FN<READER>(state, static_cast<Shape>(state.range(0)),      \
               static_cast<DocShape>(state.range(1)), WRITER); \
  }                                                            \
  BENCHMARK(NAME##_##SUFFIX)                                   \
    ->ArgNames({"shape", "docs"})                              \
    ->ArgsProduct({benchmark::CreateDenseRange(0, 3, 1),       \
                   benchmark::CreateDenseRange(0, 1, 1)})      \
    ->Unit(benchmark::kMicrosecond)

#define TERM_DICT_BENCH(NAME, FN)                                             \
  TERM_DICT_VARIANT(NAME, FN, v2, irs::term_dict::FieldReader, MakeV2);       \
  TERM_DICT_VARIANT(NAME, FN, v2whole, irs::term_dict::FieldReader,           \
                    MakeV2Whole);                                             \
  TERM_DICT_VARIANT(NAME, FN, v2raw, irs::term_dict::FieldReader, MakeV2Raw); \
  TERM_DICT_VARIANT(NAME, FN, v2small, irs::term_dict::FieldReader, MakeV2Small)

// arg2 = Levenshtein distance.
#define TERM_DICT_LEV(SUFFIX, READER, WRITER)                                \
  static void Levenshtein_##SUFFIX(benchmark::State& state) {                \
    BenchLevenshtein<READER>(state, static_cast<Shape>(state.range(0)),      \
                             static_cast<DocShape>(state.range(1)), WRITER); \
  }                                                                          \
  BENCHMARK(Levenshtein_##SUFFIX)                                            \
    ->ArgNames({"shape", "docs", "dist"})                                    \
    ->ArgsProduct({benchmark::CreateDenseRange(0, 3, 1),                     \
                   benchmark::CreateDenseRange(0, 1, 1),                     \
                   benchmark::CreateDenseRange(1, 2, 1)})                    \
    ->Unit(benchmark::kMicrosecond)

TERM_DICT_BENCH(Build, BenchBuild);
TERM_DICT_BENCH(PointSeek, BenchPointSeek);
TERM_DICT_BENCH(BatchSeek, BenchBatchSeek);
TERM_DICT_BENCH(BatchSeekMisses, BenchBatchSeekMisses);
TERM_DICT_BENCH(Iterate, BenchIterate);
TERM_DICT_BENCH(SeekGe, BenchSeekGe);
TERM_DICT_BENCH(Automaton, BenchAutomaton);
TERM_DICT_BENCH(AutomatonPrefix, BenchAutomatonPrefix);
TERM_DICT_BENCH(AutomatonHeadTail, BenchAutomatonHeadTail);

// arg2 = regexp shape.
#define TERM_DICT_REGEXP(SUFFIX, READER, WRITER)                        \
  static void Regexp_##SUFFIX(benchmark::State& state) {                \
    BenchRegexp<READER>(state, static_cast<Shape>(state.range(0)),      \
                        static_cast<DocShape>(state.range(1)), WRITER); \
  }                                                                     \
  BENCHMARK(Regexp_##SUFFIX)                                            \
    ->ArgNames({"shape", "docs", "re"})                                 \
    ->ArgsProduct({benchmark::CreateDenseRange(0, 3, 1),                \
                   benchmark::CreateDenseRange(0, 1, 1),                \
                   benchmark::CreateDenseRange(0, 4, 1)})               \
    ->Unit(benchmark::kMicrosecond)

// Nothing is determinized: the leapfrog steps RE2's lazy DFA.
TERM_DICT_REGEXP(v2r, irs::term_dict::FieldReader, MakeV2);
TERM_DICT_REGEXP(v2wholer, irs::term_dict::FieldReader, MakeV2Whole);

// arg0 = Shape, arg1 = regexp shape.
#define TERM_DICT_REGEXP_BUILD(SUFFIX)                           \
  static void RegexpBuild_##SUFFIX(benchmark::State& state) {    \
    BenchRegexpBuild(state, static_cast<Shape>(state.range(0))); \
  }                                                              \
  BENCHMARK(RegexpBuild_##SUFFIX)                                \
    ->ArgNames({"shape", "re"})                                  \
    ->ArgsProduct({benchmark::CreateDenseRange(0, 3, 1),         \
                   benchmark::CreateDenseRange(0, 2, 1)})        \
    ->Unit(benchmark::kMicrosecond)

TERM_DICT_REGEXP_BUILD(re2);

// No automaton is materialized: the leapfrog steps `utils/levenshtein_utils`'s
// tables directly.
TERM_DICT_LEV(v2p, irs::term_dict::FieldReader, MakeV2);
TERM_DICT_LEV(v2wholep, irs::term_dict::FieldReader, MakeV2Whole);

int main(int argc, char** argv) {
  ::sdb::DuckDBEngine::Instance().Initialize();
  irs::formats::Init();
  for (int i = 0; i != 4; ++i) {
    const auto shape = static_cast<Shape>(i);
    const auto& corpus = GetCorpus(shape);
    size_t bytes = 0;
    for (const auto& term : corpus.terms) {
      bytes += term.size();
    }
    benchmark::AddCustomContext(
      absl::StrCat("corpus_", ShapeName(shape)),
      absl::StrCat(corpus.terms.size(), " terms, ", bytes, " key bytes"));
  }
  for (const auto docs : {DocShape::Flat, DocShape::Wide}) {
    uint64_t postings = 0;
    for (size_t t = 0; t != kTerms; ++t) {
      postings += RunFor(docs, t).count;
    }
    benchmark::AddCustomContext(
      absl::StrCat("docs_", DocShapeName(docs)),
      absl::StrCat(SegmentDocs(docs) - irs::doc_limits::min(), " documents, ",
                   postings, " postings, ",
                   (SegmentDocs(docs) - irs::doc_limits::min() +
                    DEFAULT_ROW_GROUP_SIZE - 1) /
                     DEFAULT_ROW_GROUP_SIZE,
                   " row groups at row_group_size=", DEFAULT_ROW_GROUP_SIZE));
  }
  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();
  ::sdb::DuckDBEngine::Instance().Shutdown();
  return 0;
}
