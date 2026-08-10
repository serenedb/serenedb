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

// Microbenchmark: SlopPhrase vs a functionally-equivalent disjunction of
// fixed phrases, on europarl text plus synthetic corpora.
//
// For two terms with slop N and expected step 1, the equivalent disjunction
// is built by MakeDisjunctionEquivalent.
//
// Two-term phrases run the fused merge-join (JoinPair); n >= 3 gathers
// every slot in full (bulk ReadAll) and runs the DFS matcher.
// DisjunctionExec is the baseline.
//
// Corpora: europarl (real text - balanced pairs, skewed "the commission",
// dense-dense "of the", reversal-heavy "union european", explicit-gap
// "the __ union", a slop=50 wide-window n=2 stress, and n=3/n=4 dense-slot0
// phrases); synthetic 60:1 (n=2 skew ceiling); dense3 ("aaa bbb ccc" repeated,
// every term dense so the n=3 phrase has many valid tuples - DFS stress across
// slop {1,2,5,10}); allsame ("aaa" repeated - n=2 repeated-term pair stresses
// the join's uniqueness check, n=3/n=4 stress the DFS).
//
// Europarl path comes from env SERENEDB_BENCH_EUROPARL, with a fallback
// relative to the working directory. Aborts if the file is missing.

#include <absl/algorithm/container.h>
#include <benchmark/benchmark.h>
#include <simdutf.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/phrase_filter.hpp>
#include <iresearch/search/phrase_query.hpp>
#include <iresearch/search/slop_phrase.hpp>
#include <iresearch/store/data_output.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/string.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "basics/duckdb_engine.h"
#include "utf8proc_wrapper.hpp"

#ifdef SLOP_PROFILE
#include <valgrind/callgrind.h>
#endif

namespace bench_sloppy {

// Indexes the body column of an europarl line into body_anl with
// Freq | Pos | Offs.

inline constexpr irs::field_id kFieldId = 1;

struct IField {
  using ptr = std::shared_ptr<IField>;
  virtual ~IField() = default;

  virtual irs::field_id Id() const = 0;
  virtual irs::IndexFeatures GetIndexFeatures() const = 0;
  virtual irs::Tokenizer& GetTokens() const = 0;
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

  irs::Tokenizer& GetTokens() const final {
    _stream->reset(_value);
    return *_stream;
  }

  bool Write(irs::DataOutput&) const final { return false; }

 private:
  irs::analysis::Analyzer::ptr _stream;
  std::string_view _value;
};

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

template<typename OctetIterator>
class BreakIterator {
 public:
  BreakIterator(uint32_t delim, const OctetIterator& begin,
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
    OctetIterator it = _wbegin;
    while (it != _end) {
      const OctetIterator prev = it;
      int sz = 0;
      const auto cp = duckdb::Utf8Proc::UTF8ToCodepoint(&*it, sz);
      it += sz > 0 ? sz : 1;
      if (static_cast<uint32_t>(cp) == _delim) {
        _res.assign(_wbegin, prev);
        _wend = it;
        return;
      }
    }
    _wend = _end;
    _res.assign(_wbegin, _end);
  }

  uint32_t _delim;
  std::string _res;
  OctetIterator _wbegin;
  OctetIterator _wend;
  OctetIterator _end;
};

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
    if (!simdutf::validate_utf8(_line.data(), _line.size())) {
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

}  // namespace bench_sloppy
namespace {

namespace spm = irs::detail::slop;

constexpr std::string_view kEuroparlFallbackPath =
  "resources/tests/iresearch/europarl.subset.big.txt";

using bench_sloppy::kFieldId;

constexpr std::string_view kFormatName = "1_5simd";

constexpr int kRepetitions = 5;

// Number of documents in the synthetic skewed corpus.
constexpr size_t kSyntheticDocs = 400;

// Dense 3-term corpus: docs of "aaa bbb ccc" repeated kDense3Reps times.
constexpr size_t kDense3Docs = 200;
constexpr int kDense3Reps = 40;

// Adversarial single-term corpus: docs of "aaa" repeated, regularly spaced.
// A repeated-term phrase ("aaa" in every slot) draws every slot from the same
// dense position set, stressing the matcher's uniqueness check. Kept small:
// all-same freq is combinatorial at high slop.
constexpr size_t kAllSameDocs = 64;
constexpr int kAllSameReps = 24;
constexpr size_t kFarApartDocs = 64;
constexpr int kFarApartReps = 24;
constexpr int kFarApartGap = 50;

struct TermPair {
  std::string_view label;
  std::string_view term0;
  std::string_view term1;
};

constexpr TermPair kTermPairs[] = {
  {"european_union", "european", "union"},
  {"human_rights", "human", "rights"},
  {"climate_change", "climate", "change"},
  // Skewed: slot0 "the" is dense, "commission" is sparse, so the gate
  // engages and the matcher would otherwise lead from the dense slot.
  {"the_commission", "the", "commission"},
  // Dense-dense: both terms occur in nearly every document with high
  // per-doc frequency. Worst case for the join's sliding partner
  // buffer (windows overlap heavily, the buffer never drains).
  {"of_the", "of", "the"},
  // Reversed real bigram: forward occurrences are rare, so matches at
  // higher slop come mostly through the reversal branch of StepCost
  // (adjacent reversal costs 2).
  {"union_european", "union", "european"},
};

constexpr irs::PosAttr::value_t kSlopValues[] = {1, 2, 5};

struct Corpus {
  std::filesystem::path dir_path;
  std::unique_ptr<irs::MMapDirectory> dir;
  irs::Format::ptr format;
  irs::DirectoryReader reader;
};

[[noreturn]] void Die(const char* msg) {
  std::fprintf(stderr, "sloppy_phrase_vs_disjunction bench: %s\n", msg);
  std::abort();
}

// Registers formats exactly once across all corpora.
void EnsureRegistered() {
  static const bool once = [] {
    irs::formats::Init();
    return true;
  }();
  (void)once;
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
                 "sloppy_phrase_vs_disjunction bench: europarl dataset not "
                 "found at '%s'\nSet SERENEDB_BENCH_EUROPARL or run from the "
                 "repo root so the relative fallback resolves.\n",
                 data_path.string().c_str());
    std::abort();
  }

  auto tmp_root = std::filesystem::temp_directory_path() /
                  "serenedb-bench-sloppy-phrase-vs-disjunction";
  std::filesystem::remove_all(tmp_root);
  std::filesystem::create_directories(tmp_root);

  EnsureRegistered();

  auto format = irs::formats::Get(std::string{kFormatName});
  if (!format) {
    Die("format 1_5simd not registered");
  }

  auto dir = std::make_unique<irs::MMapDirectory>(tmp_root);

  irs::IndexWriterOptions writer_opts;
  auto* db = &::sdb::DuckDBEngine::Instance().instance();
  writer_opts.db = db;
  writer_opts.reader_options.db = db;

  auto writer =
    irs::IndexWriter::Make(*dir, format, irs::kOmCreate, writer_opts);
  if (!writer) {
    Die("IndexWriter::Make returned null");
  }

  bench_sloppy::EuroparlBodyTemplate tpl;
  bench_sloppy::EuroparlReader reader{data_path, tpl};

  size_t inserted = 0;
  while (auto* doc = reader.Next()) {
    auto trx = writer->GetBatch();
    auto inserter = trx.Insert();
    if (!inserter.Insert(doc->indexed.begin(), doc->indexed.end())) {
      Die("Insert returned false");
    }
    trx.Commit();
    ++inserted;
  }
  writer->RefreshCommit();

  if (inserted == 0) {
    Die("inserted 0 documents - dataset file empty?");
  }

  std::fprintf(
    stderr,
    "sloppy_phrase_vs_disjunction bench: indexed %zu documents from %s\n",
    inserted, data_path.string().c_str());

  irs::IndexReaderOptions reader_opts;
  reader_opts.db = db;
  auto rdr = irs::DirectoryReader{*dir, format, reader_opts};
  return Corpus{.dir_path = std::move(tmp_root),
                .dir = std::move(dir),
                .format = std::move(format),
                .reader = std::move(rdr)};
}

// Builds a synthetic corpus where every document is 30x "zzcmn", one
// "zzrre", 30x "zzcmn" - a 60:1 in-document frequency skew with the
// dense term as phrase slot0 - the skew ceiling for the join's
// rare-anchor pick.
Corpus BuildSyntheticIndex() {
  auto tmp_root =
    std::filesystem::temp_directory_path() / "serenedb-bench-sloppy-synthetic";
  std::filesystem::remove_all(tmp_root);
  std::filesystem::create_directories(tmp_root);

  EnsureRegistered();

  auto format = irs::formats::Get(std::string{kFormatName});
  if (!format) {
    Die("format 1_5simd not registered");
  }

  auto dir = std::make_unique<irs::MMapDirectory>(tmp_root);

  irs::IndexWriterOptions writer_opts;
  auto* db = &::sdb::DuckDBEngine::Instance().instance();
  writer_opts.db = db;
  writer_opts.reader_options.db = db;

  auto writer =
    irs::IndexWriter::Make(*dir, format, irs::kOmCreate, writer_opts);
  if (!writer) {
    Die("IndexWriter::Make returned null");
  }

  std::string body;
  body.reserve(512);
  for (int i = 0; i < 30; ++i) {
    body += "zzcmn ";
  }
  body += "zzrre ";
  for (int i = 0; i < 30; ++i) {
    body += "zzcmn ";
  }

  bench_sloppy::EuroparlBodyTemplate tpl;
  for (size_t d = 0; d < kSyntheticDocs; ++d) {
    tpl.SetColumn(2, body);
    auto trx = writer->GetBatch();
    auto inserter = trx.Insert();
    const auto& doc = tpl.Get();
    if (!inserter.Insert(doc.indexed.begin(), doc.indexed.end())) {
      Die("synthetic Insert returned false");
    }
    trx.Commit();
  }
  writer->RefreshCommit();

  std::fprintf(stderr,
               "sloppy_phrase_vs_disjunction bench: indexed %zu synthetic "
               "documents (60:1 skew)\n",
               kSyntheticDocs);

  irs::IndexReaderOptions reader_opts;
  reader_opts.db = db;
  auto rdr = irs::DirectoryReader{*dir, format, reader_opts};
  return Corpus{.dir_path = std::move(tmp_root),
                .dir = std::move(dir),
                .format = std::move(format),
                .reader = std::move(rdr)};
}

// Builds a dense 3-term corpus: each doc is "aaa bbb ccc" repeated
// kDense3Reps times, so every term occurs kDense3Reps times per doc.
// The n=3 phrase "aaa bbb ccc" then has many valid tuples per doc,
// growing with slop - the stress case for the DFS matching path.
Corpus BuildDense3Index() {
  auto tmp_root =
    std::filesystem::temp_directory_path() / "serenedb-bench-sloppy-dense3";
  std::filesystem::remove_all(tmp_root);
  std::filesystem::create_directories(tmp_root);

  EnsureRegistered();

  auto format = irs::formats::Get(std::string{kFormatName});
  if (!format) {
    Die("format 1_5simd not registered");
  }

  auto dir = std::make_unique<irs::MMapDirectory>(tmp_root);

  irs::IndexWriterOptions writer_opts;
  auto* db = &::sdb::DuckDBEngine::Instance().instance();
  writer_opts.db = db;
  writer_opts.reader_options.db = db;

  auto writer =
    irs::IndexWriter::Make(*dir, format, irs::kOmCreate, writer_opts);
  if (!writer) {
    Die("IndexWriter::Make returned null");
  }

  std::string body;
  body.reserve(static_cast<size_t>(kDense3Reps) * 12);
  for (int i = 0; i < kDense3Reps; ++i) {
    body += "aaa bbb ccc ";
  }

  bench_sloppy::EuroparlBodyTemplate tpl;
  for (size_t d = 0; d < kDense3Docs; ++d) {
    tpl.SetColumn(2, body);
    auto trx = writer->GetBatch();
    auto inserter = trx.Insert();
    const auto& doc = tpl.Get();
    if (!inserter.Insert(doc.indexed.begin(), doc.indexed.end())) {
      Die("dense3 Insert returned false");
    }
    trx.Commit();
  }
  writer->RefreshCommit();

  std::fprintf(stderr,
               "sloppy_phrase_vs_disjunction bench: indexed %zu dense3 "
               "documents (%d reps of 'aaa bbb ccc')\n",
               kDense3Docs, kDense3Reps);

  irs::IndexReaderOptions reader_opts;
  reader_opts.db = db;
  auto rdr = irs::DirectoryReader{*dir, format, reader_opts};
  return Corpus{.dir_path = std::move(tmp_root),
                .dir = std::move(dir),
                .format = std::move(format),
                .reader = std::move(rdr)};
}

// Single-term corpus: each doc is "aaa" repeated kAllSameReps times.
Corpus BuildAllSameIndex() {
  auto tmp_root =
    std::filesystem::temp_directory_path() / "serenedb-bench-sloppy-allsame";
  std::filesystem::remove_all(tmp_root);
  std::filesystem::create_directories(tmp_root);

  EnsureRegistered();

  auto format = irs::formats::Get(std::string{kFormatName});
  if (!format) {
    Die("format 1_5simd not registered");
  }

  auto dir = std::make_unique<irs::MMapDirectory>(tmp_root);

  irs::IndexWriterOptions writer_opts;
  auto* db = &::sdb::DuckDBEngine::Instance().instance();
  writer_opts.db = db;
  writer_opts.reader_options.db = db;

  auto writer =
    irs::IndexWriter::Make(*dir, format, irs::kOmCreate, writer_opts);
  if (!writer) {
    Die("IndexWriter::Make returned null");
  }

  std::string body;
  body.reserve(static_cast<size_t>(kAllSameReps) * 4);
  for (int i = 0; i < kAllSameReps; ++i) {
    body += "aaa ";
  }

  bench_sloppy::EuroparlBodyTemplate tpl;
  for (size_t d = 0; d < kAllSameDocs; ++d) {
    tpl.SetColumn(2, body);
    auto trx = writer->GetBatch();
    auto inserter = trx.Insert();
    const auto& doc = tpl.Get();
    if (!inserter.Insert(doc.indexed.begin(), doc.indexed.end())) {
      Die("allsame Insert returned false");
    }
    trx.Commit();
  }
  writer->RefreshCommit();

  std::fprintf(stderr,
               "sloppy_phrase_vs_disjunction bench: indexed %zu allsame "
               "documents (%d reps of 'aaa')\n",
               kAllSameDocs, kAllSameReps);

  irs::IndexReaderOptions reader_opts;
  reader_opts.db = db;
  auto rdr = irs::DirectoryReader{*dir, format, reader_opts};
  return Corpus{.dir_path = std::move(tmp_root),
                .dir = std::move(dir),
                .format = std::move(format),
                .reader = std::move(rdr)};
}

// Never-matching dense pair corpus: each doc is kFarApartReps of "aaa",
// then kFarApartGap of "ccc" filler, then kFarApartReps of "bbb". Both
// query terms are dense in every document, but the blocks sit further
// apart than any benchmarked slop window can bridge: the conjunction
// admits every document while the position machinery finds nothing.
Corpus BuildFarApartIndex() {
  auto tmp_root =
    std::filesystem::temp_directory_path() / "serenedb-bench-sloppy-farapart";
  std::filesystem::remove_all(tmp_root);
  std::filesystem::create_directories(tmp_root);

  EnsureRegistered();

  auto format = irs::formats::Get(std::string{kFormatName});
  if (!format) {
    Die("format 1_5simd not registered");
  }

  auto dir = std::make_unique<irs::MMapDirectory>(tmp_root);

  irs::IndexWriterOptions writer_opts;
  auto* db = &::sdb::DuckDBEngine::Instance().instance();
  writer_opts.db = db;
  writer_opts.reader_options.db = db;

  auto writer =
    irs::IndexWriter::Make(*dir, format, irs::kOmCreate, writer_opts);
  if (!writer) {
    Die("IndexWriter::Make returned null");
  }

  std::string body;
  body.reserve(static_cast<size_t>(kFarApartReps * 2 + kFarApartGap) * 4);
  for (int i = 0; i < kFarApartReps; ++i) {
    body += "aaa ";
  }
  for (int i = 0; i < kFarApartGap; ++i) {
    body += "ccc ";
  }
  for (int i = 0; i < kFarApartReps; ++i) {
    body += "bbb ";
  }

  bench_sloppy::EuroparlBodyTemplate tpl;
  for (size_t d = 0; d < kFarApartDocs; ++d) {
    tpl.SetColumn(2, body);
    auto trx = writer->GetBatch();
    auto inserter = trx.Insert();
    const auto& doc = tpl.Get();
    if (!inserter.Insert(doc.indexed.begin(), doc.indexed.end())) {
      Die("farapart Insert returned false");
    }
    trx.Commit();
  }
  writer->RefreshCommit();

  std::fprintf(stderr,
               "sloppy_phrase_vs_disjunction bench: indexed %zu farapart "
               "documents (%d aaa, %d ccc, %d bbb)\n",
               kFarApartDocs, kFarApartReps, kFarApartGap, kFarApartReps);

  irs::IndexReaderOptions reader_opts2;
  reader_opts2.db = db;
  auto rdr2 = irs::DirectoryReader{*dir, format, reader_opts2};
  return Corpus{.dir_path = std::move(tmp_root),
                .dir = std::move(dir),
                .format = std::move(format),
                .reader = std::move(rdr2)};
}

const Corpus& GetCorpus() {
  static const Corpus corpus = BuildIndex();
  return corpus;
}

const Corpus& GetSyntheticCorpus() {
  static const Corpus corpus = BuildSyntheticIndex();
  return corpus;
}

const Corpus& GetDense3Corpus() {
  static const Corpus corpus = BuildDense3Index();
  return corpus;
}

const Corpus& GetAllSameCorpus() {
  static const Corpus corpus = BuildAllSameIndex();
  return corpus;
}

const Corpus& GetFarApartCorpus() {
  static const Corpus corpus = BuildFarApartIndex();
  return corpus;
}

irs::ByPhrase MakeSlopPhrase(std::string_view t0, std::string_view t1,
                             irs::PosAttr::value_t slop) {
  irs::ByPhrase q;
  *q.mutable_field_id() = kFieldId;
  q.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(t0);
  q.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(t1);
  q.mutable_options()->set_slop(slop);
  return q;
}

irs::ByPhrase MakeSlopPhrase3(std::string_view t0, std::string_view t1,
                              std::string_view t2, irs::PosAttr::value_t slop) {
  irs::ByPhrase q;
  *q.mutable_field_id() = kFieldId;
  q.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(t0);
  q.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(t1);
  q.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(t2);
  q.mutable_options()->set_slop(slop);
  return q;
}

irs::ByPhrase MakeSlopPhrase4(std::string_view t0, std::string_view t1,
                              std::string_view t2, std::string_view t3,
                              irs::PosAttr::value_t slop) {
  irs::ByPhrase q;
  *q.mutable_field_id() = kFieldId;
  q.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(t0);
  q.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(t1);
  q.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(t2);
  q.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(t3);
  q.mutable_options()->set_slop(slop);
  return q;
}

irs::ByPhrase MakeTheEuropeanUnion(irs::PosAttr::value_t slop) {
  return MakeSlopPhrase3("the", "european", "union", slop);
}

// Two-term phrase with an explicit expected position delta 'gap' between
// the terms (gap == 1 is a plain adjacent pair). push_back(offs) stores
// offs+1, so pass gap-1, mirroring AppendFixedPhrase.
irs::ByPhrase MakeSlopPhraseGap(std::string_view t0, std::string_view t1,
                                irs::PosAttr::value_t gap,
                                irs::PosAttr::value_t slop) {
  SDB_ASSERT(gap >= 1);
  irs::ByPhrase q;
  *q.mutable_field_id() = kFieldId;
  q.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(t0);
  q.mutable_options()->push_back<irs::ByTermOptions>(/*offs=*/gap - 1).term =
    irs::ViewCast<irs::byte_type>(t1);
  q.mutable_options()->set_slop(slop);
  return q;
}

// push_back<ByTermOptions>(offs) sets offs_min=offs_max=offs+1, so to
// request position delta g pass offs=g-1.
void AppendFixedPhrase(irs::Or& or_filter, std::string_view first,
                       std::string_view second, irs::PosAttr::value_t gap) {
  SDB_ASSERT(gap >= 1);
  auto& phrase = or_filter.add<irs::ByPhrase>();
  *phrase.mutable_field_id() = kFieldId;
  phrase.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(first);
  phrase.mutable_options()
    ->push_back<irs::ByTermOptions>(/*offs=*/gap - 1)
    .term = irs::ViewCast<irs::byte_type>(second);
}

// Builds the disjunction matching the same chains as MakeSlopPhrase for two
// terms with expected step 1: forward phrases for gaps in [1, slop+1] and
// reversed phrases for gaps in [1, slop-1] (reversed cost = gap + 1, so the
// bound is slop-1).
irs::Or MakeDisjunctionEquivalent(std::string_view t0, std::string_view t1,
                                  irs::PosAttr::value_t slop) {
  irs::Or q;
  for (irs::PosAttr::value_t g = 1; g <= slop + 1; ++g) {
    AppendFixedPhrase(q, t0, t1, g);
  }
  for (irs::PosAttr::value_t g = 1; g + 1 <= slop; ++g) {
    AppendFixedPhrase(q, t1, t0, g);
  }
  return q;
}

// General slop-phrase disjunction equivalent for n >= 2 terms: enumerates
// every distinct-position layout whose slot-order StepCost (expected step 1)
// is <= slop and ORs one exact phrase per layout. Matches exactly the docs
// the sloppy phrase matches over distinct-position layouts; same-position
// (synonym) layouts aren't represented, but real text has none, so its docs=
// must equal the slop benchmark's docs= - the built-in correctness check.
// Enumeration is bounded: for expected==1, span = sum(gaps) <= slop + n - 1.
// Reproduces the n==2 helper exactly. Untimed (runs in make()).
void AppendSlopPhraseVariants(irs::Or& or_filter,
                              const std::vector<std::string_view>& terms,
                              irs::PosAttr::value_t slop) {
  const size_t n = terms.size();
  SDB_ASSERT(n >= 2);
  const auto span_budget = static_cast<irs::PosAttr::value_t>(slop + (n - 1));

  std::vector<size_t> perm(n);
  for (size_t i = 0; i < n; ++i) {
    perm[i] = i;
  }
  std::vector<irs::PosAttr::value_t> gaps(n - 1, 0);
  std::vector<irs::PosAttr::value_t> pos(n, 0);  // slot index -> rel. position

  auto try_emit = [&] {
    irs::PosAttr::value_t tp = 0;
    pos[perm[0]] = 0;
    for (size_t j = 1; j < n; ++j) {
      tp = static_cast<irs::PosAttr::value_t>(tp + gaps[j - 1]);
      pos[perm[j]] = tp;
    }
    irs::PosAttr::value_t cost = 0;
    for (size_t i = 0; i + 1 < n; ++i) {
      const int64_t delta =
        static_cast<int64_t>(pos[i + 1]) - static_cast<int64_t>(pos[i]);
      cost = static_cast<irs::PosAttr::value_t>(cost + spm::StepCost(delta, 1));
      if (cost > slop) {
        return;
      }
    }
    auto& phrase = or_filter.add<irs::ByPhrase>();
    *phrase.mutable_field_id() = kFieldId;
    phrase.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(terms[perm[0]]);
    for (size_t j = 1; j < n; ++j) {
      phrase.mutable_options()
        ->push_back<irs::ByTermOptions>(/*offs=*/gaps[j - 1] - 1)
        .term = irs::ViewCast<irs::byte_type>(terms[perm[j]]);
    }
  };

  // Enumerate gap vectors (each >= 1, sum <= span_budget); cost is checked
  // exactly in try_emit.
  auto enumerate_gaps = [&](auto&& self, size_t idx,
                            irs::PosAttr::value_t span_used) -> void {
    if (idx == n - 1) {
      try_emit();
      return;
    }
    for (irs::PosAttr::value_t g = 1; span_used + g <= span_budget; ++g) {
      gaps[idx] = g;
      self(self, idx + 1, static_cast<irs::PosAttr::value_t>(span_used + g));
    }
  };

  do {
    enumerate_gaps(enumerate_gaps, 0, 0);
  } while (std::next_permutation(perm.begin(), perm.end()));
}

irs::Or MakeDisjunctionEquivalentN(const std::vector<std::string_view>& terms,
                                   irs::PosAttr::value_t slop) {
  irs::Or q;
  AppendSlopPhraseVariants(q, terms, slop);
  return q;
}

// Tracks peak allocated bytes; reset between iterations.
struct MaxMemoryCounter final : irs::IResourceManager {
  void Reset() noexcept {
    current = 0;
    max = 0;
  }

  void Increase(size_t value) final {
    current += value;
    max = std::max(max, current);
  }

  void Decrease(size_t value) noexcept final { current -= value; }

  size_t current{0};
  size_t max{0};
};

// Per-segment prepared queries plus an empty StatsBuffer for unscored
// execution: the NoCollector shape of tests::PreparedFilter, inlined here
// because the bench target has no access to the test helpers.
struct BenchPrepared {
  std::vector<irs::QueryBuilder::ptr> queries;
  irs::StatsBuffer stats;

  bool AnyNull() const noexcept {
    return absl::c_any_of(queries, [](const auto& q) { return !q; });
  }
};

template<typename Filter>
BenchPrepared Prepare(
  const Filter& q, const irs::DirectoryReader& rdr,
  irs::IResourceManager& memory = irs::IResourceManager::gNoop) {
  BenchPrepared out;
  out.queries.reserve(rdr.size());
  for (const auto& sub : rdr) {
    out.queries.emplace_back(q.PrepareSegment(sub, {
                                                     .collector = nullptr,
                                                     .memory = memory,
                                                   }));
  }
  return out;
}

template<typename MakeFn>
void BenchPrepare(benchmark::State& state, const irs::DirectoryReader& rdr,
                  MakeFn make) {
  {
    auto q = make();
    auto check = Prepare(q, rdr);
    if (check.AnyNull()) {
      state.SkipWithError("prepare returned null");
      return;
    }
  }

  MaxMemoryCounter counter;
  for (auto _ : state) {
    counter.Reset();
    auto q = make();
    auto prepared = Prepare(q, rdr, counter);
    benchmark::DoNotOptimize(prepared);
  }

  state.counters["prepare_mem_bytes"] = static_cast<double>(counter.max);
}

template<typename MakeFn>
[[gnu::noinline]] void BenchExecuteOnly(benchmark::State& state,
                                        const irs::DirectoryReader& rdr,
                                        MakeFn make) {
  auto q = make();
  auto prepared = Prepare(q, rdr);
  if (prepared.AnyNull()) {
    state.SkipWithError("prepare returned null");
    return;
  }

  size_t per_iter = 0;
#ifdef SLOP_PROFILE
  // Collect callgrind counts only over the timed loop (index build and
  // make()/prepare() stay out); requires --collect-atstart=no.
  CALLGRIND_TOGGLE_COLLECT;
#endif
  for (auto _ : state) {
    per_iter = 0;
    for (const auto& query : prepared.queries) {
      auto docs = query->Execute({}, prepared.stats);
      while (!irs::doc_limits::eof(docs->advance())) {
        ++per_iter;
      }
    }
    benchmark::DoNotOptimize(per_iter);
  }
#ifdef SLOP_PROFILE
  CALLGRIND_TOGGLE_COLLECT;
#endif

  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(per_iter));
  state.counters["docs"] = static_cast<double>(per_iter);
}

// Variadic n == 2: dense "the" x a two-term synonym set. A non-simple slot
// routes prepare() to VariadicPhraseQuery, which runs the fused merge-join
// over per-slot merged position streams (same join as the fixed pair, fed
// by MergedPosStream). Variadic counterpart of the skewed the_commission
// shape.
irs::ByPhrase MakeSlopPhraseVariadic2(irs::PosAttr::value_t slop) {
  irs::ByPhrase q;
  *q.mutable_field_id() = kFieldId;
  q.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(std::string_view("the"));
  auto& st = q.mutable_options()->push_back<irs::ByTermsOptions>();
  st.terms.emplace(
    irs::ViewCast<irs::byte_type>(std::string_view("commission")));
  st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("council")));
  q.mutable_options()->set_slop(slop);
  return q;
}

// Baseline for the variadic pair: the union of the per-synonym phrase
// expansions ("the commission" variants OR "the council" variants).
irs::Or MakeDisjunctionEquivalentVariadic2(irs::PosAttr::value_t slop) {
  irs::Or q;
  AppendSlopPhraseVariants(q, {"the", "commission"}, slop);
  AppendSlopPhraseVariants(q, {"the", "council"}, slop);
  return q;
}

// Variadic n == 2 with a four-term synonym slot: the disjunction's
// expansion count K doubles relative to the two-term set while the
// bind/merge machinery must stay flat.
irs::ByPhrase MakeSlopPhraseVariadic4(irs::PosAttr::value_t slop) {
  irs::ByPhrase q;
  *q.mutable_field_id() = kFieldId;
  q.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(std::string_view("the"));
  auto& st = q.mutable_options()->push_back<irs::ByTermsOptions>();
  st.terms.emplace(
    irs::ViewCast<irs::byte_type>(std::string_view("commission")));
  st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("council")));
  st.terms.emplace(
    irs::ViewCast<irs::byte_type>(std::string_view("parliament")));
  st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("union")));
  q.mutable_options()->set_slop(slop);
  return q;
}

irs::Or MakeDisjunctionEquivalentVariadic4(irs::PosAttr::value_t slop) {
  irs::Or q;
  AppendSlopPhraseVariants(q, {"the", "commission"}, slop);
  AppendSlopPhraseVariants(q, {"the", "council"}, slop);
  AppendSlopPhraseVariants(q, {"the", "parliament"}, slop);
  AppendSlopPhraseVariants(q, {"the", "union"}, slop);
  return q;
}

// ExecuteWithOffsets exists on FixedPhraseQuery and VariadicPhraseQuery
// (PhraseQueryT selects which); slop only, no disjunction analogue. Drains
// pos->next() per matched doc.
template<typename PhraseQueryT = irs::FixedPhraseQuery, typename MakeFn>
[[gnu::noinline]] void BenchExecuteWithOffsets(benchmark::State& state,
                                               const irs::DirectoryReader& rdr,
                                               MakeFn make) {
  auto q = make();
  auto prepared = Prepare(q, rdr);
  if (prepared.AnyNull()) {
    state.SkipWithError("prepare returned null");
    return;
  }
  std::vector<const PhraseQueryT*> phrase_queries;
  phrase_queries.reserve(prepared.queries.size());
  for (const auto& query : prepared.queries) {
    const auto* phrase_query = dynamic_cast<const PhraseQueryT*>(query.get());
    if (!phrase_query) {
      state.SkipWithError("prepared query has unexpected type");
      return;
    }
    phrase_queries.push_back(phrase_query);
  }

  size_t docs_per_iter = 0;
  size_t matches_per_iter = 0;
#ifdef SLOP_PROFILE
  // Collect callgrind counts only over the timed loop (index build and
  // make()/prepare() stay out); requires --collect-atstart=no.
  CALLGRIND_TOGGLE_COLLECT;
#endif
  for (auto _ : state) {
    docs_per_iter = 0;
    matches_per_iter = 0;
    size_t seg = 0;
    for (const auto& sub : rdr) {
      auto docs = phrase_queries[seg++]->ExecuteWithOffsets(sub);
      if (!docs) {
        continue;
      }
      auto* pos = irs::GetMutable<irs::PosAttr>(docs.get());
      if (!pos) {
        continue;
      }
      while (!irs::doc_limits::eof(docs->advance())) {
        ++docs_per_iter;
        while (pos->next()) {
          ++matches_per_iter;
        }
      }
    }
    benchmark::DoNotOptimize(docs_per_iter);
    benchmark::DoNotOptimize(matches_per_iter);
  }
#ifdef SLOP_PROFILE
  CALLGRIND_TOGGLE_COLLECT;
#endif

  state.counters["docs"] = static_cast<double>(docs_per_iter);
  state.counters["matches"] = static_cast<double>(matches_per_iter);
}

// Registers the slop Execute benchmark plus the disjunction baseline
// for one n == 2 (corpus, term pair, slop).
void RegisterExecVariants(const std::string& suffix, std::string_view t0,
                          std::string_view t1, irs::PosAttr::value_t slop,
                          const Corpus& (*corpus)()) {
  benchmark::RegisterBenchmark(
    ("SlopPhraseExec" + suffix).c_str(),
    [t0, t1, slop, corpus](benchmark::State& state) {
      BenchExecuteOnly(state, corpus().reader,
                       [t0, t1, slop] { return MakeSlopPhrase(t0, t1, slop); });
    })
    ->Repetitions(kRepetitions)
    ->ReportAggregatesOnly(true);

  benchmark::RegisterBenchmark(
    ("DisjunctionExec" + suffix).c_str(),
    [t0, t1, slop, corpus](benchmark::State& state) {
      BenchExecuteOnly(state, corpus().reader, [t0, t1, slop] {
        return MakeDisjunctionEquivalent(t0, t1, slop);
      });
    })
    ->Repetitions(kRepetitions)
    ->ReportAggregatesOnly(true);
}

void RegisterAll() {
  for (const auto& pair : kTermPairs) {
    for (auto slop : kSlopValues) {
      const std::string suffix = std::string{"_"} + std::string{pair.label} +
                                 "_slop" +
                                 std::to_string(static_cast<unsigned>(slop));

      // Prepare does not depend on slop; register it once per pair.
      if (slop == kSlopValues[0]) {
        benchmark::RegisterBenchmark(
          ("SlopPhrasePrepare" + suffix).c_str(),
          [t0 = pair.term0, t1 = pair.term1, slop](benchmark::State& state) {
            BenchPrepare(state, GetCorpus().reader, [t0, t1, slop] {
              return MakeSlopPhrase(t0, t1, slop);
            });
          })
          ->Repetitions(kRepetitions)
          ->ReportAggregatesOnly(true);
      }

      RegisterExecVariants(suffix, pair.term0, pair.term1, slop, &GetCorpus);

      benchmark::RegisterBenchmark(
        ("SlopPhraseExecOffs" + suffix).c_str(),
        [t0 = pair.term0, t1 = pair.term1, slop](benchmark::State& state) {
          BenchExecuteWithOffsets(state, GetCorpus().reader, [t0, t1, slop] {
            return MakeSlopPhrase(t0, t1, slop);
          });
        })
        ->Repetitions(kRepetitions)
        ->ReportAggregatesOnly(true);

      benchmark::RegisterBenchmark(
        ("DisjunctionPrepare" + suffix).c_str(),
        [t0 = pair.term0, t1 = pair.term1, slop](benchmark::State& state) {
          BenchPrepare(state, GetCorpus().reader, [t0, t1, slop] {
            return MakeDisjunctionEquivalent(t0, t1, slop);
          });
        })
        ->Repetitions(kRepetitions)
        ->ReportAggregatesOnly(true);
    }
  }

  // Synthetic 60:1 skew corpus (n=2): skew ceiling for the join.
  for (auto slop : kSlopValues) {
    const std::string suffix =
      "_synthetic_slop" + std::to_string(static_cast<unsigned>(slop));
    RegisterExecVariants(suffix, "zzcmn", "zzrre", slop, &GetSyntheticCorpus);
  }

  // 3-term dense-slot0 europarl phrase "the european union".
  for (auto slop : kSlopValues) {
    const std::string suffix =
      "_the_european_union3_slop" + std::to_string(static_cast<unsigned>(slop));
    benchmark::RegisterBenchmark(
      ("SlopPhraseExec" + suffix).c_str(),
      [slop](benchmark::State& state) {
        BenchExecuteOnly(state, GetCorpus().reader,
                         [slop] { return MakeTheEuropeanUnion(slop); });
      })
      ->Repetitions(kRepetitions)
      ->ReportAggregatesOnly(true);
    // n >= 3 with offsets has no pair-join bypass: the bulk Offs gather
    // plus the enumerating matcher.
    benchmark::RegisterBenchmark(
      ("SlopPhraseExecOffs" + suffix).c_str(),
      [slop](benchmark::State& state) {
        BenchExecuteWithOffsets(state, GetCorpus().reader,
                                [slop] { return MakeTheEuropeanUnion(slop); });
      })
      ->Repetitions(kRepetitions)
      ->ReportAggregatesOnly(true);
    benchmark::RegisterBenchmark(
      ("DisjunctionExec" + suffix).c_str(),
      [slop](benchmark::State& state) {
        BenchExecuteOnly(state, GetCorpus().reader, [slop] {
          return MakeDisjunctionEquivalentN({"the", "european", "union"}, slop);
        });
      })
      ->Repetitions(kRepetitions)
      ->ReportAggregatesOnly(true);
  }

  // 4-term dense-slot0 europarl phrase "the european union and".
  for (auto slop : kSlopValues) {
    const std::string suffix = "_the_european_union_and4_slop" +
                               std::to_string(static_cast<unsigned>(slop));
    benchmark::RegisterBenchmark(
      ("SlopPhraseExec" + suffix).c_str(),
      [slop](benchmark::State& state) {
        BenchExecuteOnly(state, GetCorpus().reader, [slop] {
          return MakeSlopPhrase4("the", "european", "union", "and", slop);
        });
      })
      ->Repetitions(kRepetitions)
      ->ReportAggregatesOnly(true);
    benchmark::RegisterBenchmark(
      ("DisjunctionExec" + suffix).c_str(),
      [slop](benchmark::State& state) {
        BenchExecuteOnly(state, GetCorpus().reader, [slop] {
          return MakeDisjunctionEquivalentN({"the", "european", "union", "and"},
                                            slop);
        });
      })
      ->Repetitions(kRepetitions)
      ->ReportAggregatesOnly(true);
  }

  // n == 2 join stress: explicit gap "the __ union" (expected delta 2,
  // dense anchor slot). No disjunction baseline - the equivalent OR
  // needs a non-unit expected step the n==2 helper does not model.
  for (auto slop : kSlopValues) {
    const std::string suffix =
      "_the_union_gap2_slop" + std::to_string(static_cast<unsigned>(slop));
    benchmark::RegisterBenchmark(
      ("SlopPhraseExec" + suffix).c_str(),
      [slop](benchmark::State& state) {
        BenchExecuteOnly(state, GetCorpus().reader, [slop] {
          return MakeSlopPhraseGap("the", "union", 2, slop);
        });
      })
      ->Repetitions(kRepetitions)
      ->ReportAggregatesOnly(true);
    benchmark::RegisterBenchmark(
      ("SlopPhraseExecOffs" + suffix).c_str(),
      [slop](benchmark::State& state) {
        BenchExecuteWithOffsets(state, GetCorpus().reader, [slop] {
          return MakeSlopPhraseGap("the", "union", 2, slop);
        });
      })
      ->Repetitions(kRepetitions)
      ->ReportAggregatesOnly(true);
  }

  // n == 2 join stress: slop=50 on the skewed pair. Windows are ~100
  // positions wide, so the partner buffer stays full and front-trim /
  // compaction run constantly. No disjunction baseline - the equivalent
  // OR would need 100+ branches.
  benchmark::RegisterBenchmark(
    "SlopPhraseExec_the_commission_slop50",
    [](benchmark::State& state) {
      BenchExecuteOnly(state, GetCorpus().reader,
                       [] { return MakeSlopPhrase("the", "commission", 50); });
    })
    ->Repetitions(kRepetitions)
    ->ReportAggregatesOnly(true);
  benchmark::RegisterBenchmark(
    "SlopPhraseExecOffs_the_commission_slop50",
    [](benchmark::State& state) {
      BenchExecuteWithOffsets(state, GetCorpus().reader, [] {
        return MakeSlopPhrase("the", "commission", 50);
      });
    })
    ->Repetitions(kRepetitions)
    ->ReportAggregatesOnly(true);

  // n == 2 join stress: repeated-term pair on the all-"aaa" corpus.
  // Anchor and partner walk the same posting; every buffered position
  // collides with the anchor once, so the uniqueness check runs on the
  // whole window. Offs variant counts every remaining pair per doc.
  for (unsigned slop : {1u, 2u, 5u, 10u}) {
    const std::string suffix = "_allsame2_slop" + std::to_string(slop);
    auto make = [slop] {
      return MakeSlopPhrase("aaa", "aaa",
                            static_cast<irs::PosAttr::value_t>(slop));
    };
    benchmark::RegisterBenchmark(("SlopPhraseExec" + suffix).c_str(),
                                 [make](benchmark::State& state) {
                                   BenchExecuteOnly(
                                     state, GetAllSameCorpus().reader, make);
                                 })
      ->Repetitions(kRepetitions)
      ->ReportAggregatesOnly(true);
    benchmark::RegisterBenchmark(("SlopPhraseExecOffs" + suffix).c_str(),
                                 [make](benchmark::State& state) {
                                   BenchExecuteWithOffsets(
                                     state, GetAllSameCorpus().reader, make);
                                 })
      ->Repetitions(kRepetitions)
      ->ReportAggregatesOnly(true);
  }

  // Variadic n == 2, routed through the merged-stream fused join.
  // ExecOffs additionally covers the stream's refresh-on-move offset capture.
  for (auto slop : kSlopValues) {
    const std::string suffix = "_var2_the_commission_council_slop" +
                               std::to_string(static_cast<unsigned>(slop));
    benchmark::RegisterBenchmark(
      ("SlopPhraseExec" + suffix).c_str(),
      [slop](benchmark::State& state) {
        BenchExecuteOnly(state, GetCorpus().reader,
                         [slop] { return MakeSlopPhraseVariadic2(slop); });
      })
      ->Repetitions(kRepetitions)
      ->ReportAggregatesOnly(true);
    benchmark::RegisterBenchmark(
      ("DisjunctionExec" + suffix).c_str(),
      [slop](benchmark::State& state) {
        BenchExecuteOnly(state, GetCorpus().reader, [slop] {
          return MakeDisjunctionEquivalentVariadic2(slop);
        });
      })
      ->Repetitions(kRepetitions)
      ->ReportAggregatesOnly(true);
    benchmark::RegisterBenchmark(
      ("SlopPhraseExecOffs" + suffix).c_str(),
      [slop](benchmark::State& state) {
        BenchExecuteWithOffsets<irs::VariadicPhraseQuery>(
          state, GetCorpus().reader,
          [slop] { return MakeSlopPhraseVariadic2(slop); });
      })
      ->Repetitions(kRepetitions)
      ->ReportAggregatesOnly(true);
  }

  // Never-worse probes: corners where the disjunction's expansion count K
  // is minimal while our per-document position machinery is at its widest.
  //
  // Dense pair that never matches: the conjunction admits every document
  // and the join does full per-doc position work for zero matches, while
  // each fixed expansion can fail fast.
  for (auto slop : kSlopValues) {
    RegisterExecVariants(
      "_farapart2_slop" + std::to_string(static_cast<unsigned>(slop)), "aaa",
      "bbb", slop, &GetFarApartCorpus);
  }

  // All-dense real-text 3-term phrase: minimal K for the disjunction, three
  // dense postings for the gather.
  for (auto slop : kSlopValues) {
    const std::string suffix =
      "_one_of_the3_slop" + std::to_string(static_cast<unsigned>(slop));
    benchmark::RegisterBenchmark(
      ("SlopPhraseExec" + suffix).c_str(),
      [slop](benchmark::State& state) {
        BenchExecuteOnly(state, GetCorpus().reader, [slop] {
          return MakeSlopPhrase3("one", "of", "the", slop);
        });
      })
      ->Repetitions(kRepetitions)
      ->ReportAggregatesOnly(true);
    benchmark::RegisterBenchmark(
      ("DisjunctionExec" + suffix).c_str(),
      [slop](benchmark::State& state) {
        BenchExecuteOnly(state, GetCorpus().reader, [slop] {
          return MakeDisjunctionEquivalentN({"one", "of", "the"}, slop);
        });
      })
      ->Repetitions(kRepetitions)
      ->ReportAggregatesOnly(true);
  }

  // Variadic pair with a four-term synonym slot: K doubles versus var2
  // while the bind/merge machinery must stay flat.
  for (auto slop : kSlopValues) {
    const std::string suffix =
      "_var4_the_syn4_slop" + std::to_string(static_cast<unsigned>(slop));
    benchmark::RegisterBenchmark(
      ("SlopPhraseExec" + suffix).c_str(),
      [slop](benchmark::State& state) {
        BenchExecuteOnly(state, GetCorpus().reader,
                         [slop] { return MakeSlopPhraseVariadic4(slop); });
      })
      ->Repetitions(kRepetitions)
      ->ReportAggregatesOnly(true);
    benchmark::RegisterBenchmark(
      ("DisjunctionExec" + suffix).c_str(),
      [slop](benchmark::State& state) {
        BenchExecuteOnly(state, GetCorpus().reader, [slop] {
          return MakeDisjunctionEquivalentVariadic4(slop);
        });
      })
      ->Repetitions(kRepetitions)
      ->ReportAggregatesOnly(true);
  }

  // Dense 3-term phrase "aaa bbb ccc" (every term dense): stress for the
  // DFS matching path across slop {1,2,5,10}. No disjunction baseline (no
  // 3-term equivalent helper registered for this corpus).
  for (unsigned slop : {1u, 2u, 5u, 10u}) {
    const std::string suffix = "_dense3_slop" + std::to_string(slop);
    benchmark::RegisterBenchmark(
      ("SlopPhraseExec" + suffix).c_str(),
      [slop](benchmark::State& state) {
        BenchExecuteOnly(state, GetDense3Corpus().reader, [slop] {
          return MakeSlopPhrase3("aaa", "bbb", "ccc",
                                 static_cast<irs::PosAttr::value_t>(slop));
        });
      })
      ->Repetitions(kRepetitions)
      ->ReportAggregatesOnly(true);
    benchmark::RegisterBenchmark(
      ("SlopPhraseExecOffs" + suffix).c_str(),
      [slop](benchmark::State& state) {
        BenchExecuteWithOffsets(state, GetDense3Corpus().reader, [slop] {
          return MakeSlopPhrase3("aaa", "bbb", "ccc",
                                 static_cast<irs::PosAttr::value_t>(slop));
        });
      })
      ->Repetitions(kRepetitions)
      ->ReportAggregatesOnly(true);
  }

  // Adversarial repeated-term phrases on the single-term corpus: n=3 and n=4
  // "aaa" stress the DFS uniqueness handling (freq is combinatorial at high
  // slop). Gather held at auto.
  for (unsigned slop : {1u, 2u, 5u, 10u}) {
    for (size_t terms : {size_t{3}, size_t{4}}) {
      const std::string suffix =
        "_allsame" + std::to_string(terms) + "_slop" + std::to_string(slop);
      auto make = [terms, slop] {
        const auto s = static_cast<irs::PosAttr::value_t>(slop);
        return terms == 3 ? MakeSlopPhrase3("aaa", "aaa", "aaa", s)
                          : MakeSlopPhrase4("aaa", "aaa", "aaa", "aaa", s);
      };
      benchmark::RegisterBenchmark(("SlopPhraseExec" + suffix).c_str(),
                                   [make](benchmark::State& state) {
                                     BenchExecuteOnly(
                                       state, GetAllSameCorpus().reader, make);
                                   })
        ->Repetitions(kRepetitions)
        ->ReportAggregatesOnly(true);
    }
  }
}

}  // namespace

int main(int argc, char** argv) {
  // --disable-offs-bulk-gather: route the offset gather through the scalar
  // per-position loop (in-binary A/B against the bulk ReadAll path). Must be
  // stripped from argv before benchmark::Initialize, which rejects unknown
  // flags. The seam exists only in SDB_DEV builds; anywhere else the flag
  // fails loudly instead of silently measuring the bulk path twice.
  for (int i = 1; i < argc;) {
    if (std::string_view{argv[i]} == "--disable-offs-bulk-gather") {
#ifdef SDB_DEV
      spm::gOffsBulkGatherDisabled = true;
#else
      std::fprintf(stderr,
                   "--disable-offs-bulk-gather requires an SDB_DEV build\n");
      return 1;
#endif
      for (int j = i; j + 1 < argc; ++j) {
        argv[j] = argv[j + 1];
      }
      --argc;
    } else {
      ++i;
    }
  }
  benchmark::Initialize(&argc, argv);
  // iresearch indexes require a process-wide duckdb::DatabaseInstance,
  // wired into IndexWriterOptions::db / IndexReaderOptions::db. The
  // corpora are built lazily inside the benchmark lambdas, so the engine
  // must be up before RunSpecifiedBenchmarks. Not torn down here: the
  // cached Corpus statics (reader + directory) outlive main and touch the
  // db in their destructors, so the instance must survive into the
  // static-destruction phase.
  sdb::DuckDBEngine::Instance().Initialize();
  RegisterAll();
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
