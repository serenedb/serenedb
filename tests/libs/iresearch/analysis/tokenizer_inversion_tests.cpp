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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/index/inverter/fields_inverter.hpp"
#include "iresearch/search/filters/term_filter.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/utils/index_utils.hpp"
#include "tests_shared.hpp"
#include "tokenizer_fuzz_checks.hpp"
#include "tokenizer_fuzz_corpus.hpp"
#include "tokenizer_fuzz_mutator.hpp"
#include "tokenizer_fuzz_specs.hpp"

namespace {

using namespace irs;
using namespace ::tests::fuzz;
using Clock = std::chrono::steady_clock;

constexpr field_id kKey = 1;
constexpr uint32_t kInsertBlock = 512;

struct FieldPlan {
  field_id id;
  IndexFeatures features;
  const char* name;
};

std::vector<FieldPlan> PlanFor(const TokenTraits& traits) {
  std::vector<FieldPlan> plan = {
    {2, IndexFeatures::None, "none"},
    {3, IndexFeatures::Freq, "freq"},
    {4, IndexFeatures::Freq | IndexFeatures::Pos, "freq_pos"},
    {6, IndexFeatures::Freq | IndexFeatures::Pos | IndexFeatures::Norm,
     "freq_pos_norm"},
  };
  if (traits.offsets) {
    plan.push_back(
      {5, IndexFeatures::Freq | IndexFeatures::Pos | IndexFeatures::Offs,
       "freq_pos_offs"});
    plan.push_back({7,
                    IndexFeatures::Freq | IndexFeatures::Pos |
                      IndexFeatures::Offs | IndexFeatures::Norm,
                    "freq_pos_offs_norm"});
  }
  return plan;
}

bool Has(IndexFeatures features, IndexFeatures wanted) {
  return IndexFeatures::None != (features & wanted);
}

struct Occurrence {
  uint32_t pos;
  uint32_t start;
  uint32_t end;

  bool operator==(const Occurrence&) const = default;
};

struct DocPostings {
  size_t row;
  uint32_t freq;
  std::vector<Occurrence> occ;
};

using TermMap = std::map<std::string, std::vector<DocPostings>>;

TermMap ExpectedPostings(std::span<const Result> rows, bool with_freq,
                         bool with_pos, bool with_offs) {
  TermMap out;
  for (size_t row = 0; row < rows.size(); ++row) {
    if (!rows[row].ok) {
      continue;
    }
    std::map<std::string, DocPostings> per_doc;
    for (const auto& token : rows[row].tokens) {
      auto& entry = per_doc[token.term];
      entry.row = row;
      ++entry.freq;
      if (!with_pos) {
        continue;
      }
      entry.occ.push_back(Occurrence{token.pos,
                                     with_offs ? token.offs_start : 0,
                                     with_offs ? token.offs_end : 0});
    }
    for (auto& [term, entry] : per_doc) {
      if (!with_freq) {
        entry.freq = 1;
      }
      out[term].push_back(std::move(entry));
    }
  }
  for (auto& [term, docs] : out) {
    std::ranges::sort(
      docs, [](const auto& a, const auto& b) { return a.row < b.row; });
  }
  return out;
}

void CollectKeys(const SubReader& segment, std::map<doc_id_t, size_t>& out) {
  const auto* mask = segment.docs_mask();
  const auto* reader = segment.field(kKey);
  ASSERT_NE(nullptr, reader) << "the row-key field must exist in every segment";
  auto terms = reader->iterator();
  ASSERT_TRUE(terms);
  while (terms->next()) {
    const auto value = terms->value();
    const std::string key{reinterpret_cast<const char*>(value.data()),
                          value.size()};
    auto docs = terms->postings(IndexFeatures::Freq);
    ASSERT_TRUE(docs);
    size_t seen = 0;
    for (auto doc = docs->Next(); !doc_limits::eof(doc); doc = docs->Next()) {
      if (mask != nullptr && mask->contains(doc)) {
        continue;
      }
      ASSERT_TRUE(
        out.emplace(doc, std::strtoull(key.c_str(), nullptr, 10)).second)
        << "duplicate row key for doc " << doc;
      ++seen;
    }
    ASSERT_LE(seen, 1u) << "row key '" << key << "' names several live docs";
  }
}

void CollectField(const SubReader& segment, const FieldPlan& field,
                  const std::map<doc_id_t, size_t>& rows,
                  PostingsDigest& digest, TermMap* exact) {
  const auto* mask = segment.docs_mask();
  const auto* reader = segment.field(field.id);
  if (reader == nullptr) {
    return;
  }
  const bool want_freq = Has(field.features, IndexFeatures::Freq);
  const bool want_pos = Has(field.features, IndexFeatures::Pos);
  const bool want_offs = Has(field.features, IndexFeatures::Offs);

  auto terms = reader->iterator();
  ASSERT_TRUE(terms);
  std::string previous;
  bool has_previous = false;
  size_t nterms = 0;
  std::vector<uint32_t> pos;
  std::vector<uint32_t> starts;
  std::vector<uint32_t> ends;
  const auto to_bytes = [](std::string_view v) {
    return bytes_view{reinterpret_cast<const byte_type*>(v.data()), v.size()};
  };

  while (terms->next()) {
    const auto value = terms->value();
    std::string term{reinterpret_cast<const char*>(value.data()), value.size()};
    if (has_previous) {
      ASSERT_LT(previous, term) << "term dictionary must be sorted ascending";
    }
    previous = term;
    has_previous = true;
    ++nterms;

    auto docs = terms->postings(field.features);
    ASSERT_TRUE(docs);
    auto* positions = docs->Positions();
    ASSERT_EQ(want_pos, positions != nullptr) << "term=" << term;
    size_t docs_seen = 0;

    for (auto doc = docs->Next(); !doc_limits::eof(doc); doc = docs->Next()) {
      if (mask != nullptr && mask->contains(doc)) {
        if (positions != nullptr) {
          while (positions->next()) {
          }
        }
        continue;
      }
      const auto it = rows.find(doc);
      ASSERT_TRUE(it != rows.end()) << "doc " << doc << " has no row key";
      const auto freq = want_freq ? docs->GetFreq() : 1u;
      pos.clear();
      starts.clear();
      ends.clear();
      if (positions != nullptr) {
        const auto* offs = irs::GetMutable<OffsAttr>(positions);
        ASSERT_EQ(want_offs, offs != nullptr) << "term=" << term;
        while (positions->next()) {
          pos.push_back(positions->value());
          if (offs != nullptr) {
            starts.push_back(offs->start);
            ends.push_back(offs->end);
          }
        }
        ASSERT_EQ(freq, pos.size()) << "term=" << term;
      }
      digest.Add(term, it->second, freq, pos, starts, ends);
      ++docs_seen;
      if (exact != nullptr) {
        DocPostings entry;
        entry.row = it->second;
        entry.freq = freq;
        for (size_t i = 0; i < pos.size(); ++i) {
          entry.occ.push_back(Occurrence{pos[i], want_offs ? starts[i] : 0,
                                         want_offs ? ends[i] : 0});
        }
        (*exact)[term].push_back(std::move(entry));
      }
    }
    if ((nterms % 37) == 1 && (mask == nullptr || mask->empty())) {
      const auto meta = reader->Lookup(to_bytes(term));
      ASSERT_EQ(docs_seen, meta.docs_count)
        << "Lookup disagrees with the postings for " << Describe(term);
      auto probe = reader->iterator();
      ASSERT_TRUE(probe);
      ASSERT_TRUE(probe->seek(to_bytes(term)))
        << "an indexed term is not reachable by seek: " << Describe(term);
      size_t reached = 0;
      reader->ReadDocs(to_bytes(term), [&](doc_id_t) {
        ++reached;
        return true;
      });
      ASSERT_EQ(docs_seen, reached)
        << "ReadDocs disagrees with the postings for " << Describe(term);
    }
  }
  ASSERT_EQ(nterms, reader->size()) << "term count must match the field meta";
}

void FoldExpectedField(std::span<const Result> per_layout_rows,
                       const FieldPlan& field, PostingsDigest& digest) {
  const bool want_freq = Has(field.features, IndexFeatures::Freq);
  const bool want_pos = Has(field.features, IndexFeatures::Pos);
  const bool want_offs = Has(field.features, IndexFeatures::Offs);
  for (size_t row = 0; row < per_layout_rows.size(); ++row) {
    FoldValue(digest, row, per_layout_rows[row], want_freq, want_pos,
              want_offs);
  }
}

class StoreCollector final : public StoreSink {
 public:
  void OnStore(doc_id_t doc, bytes_view blob) final {
    _blobs[doc].assign(reinterpret_cast<const char*>(blob.data()), blob.size());
  }

  const std::map<doc_id_t, std::string>& blobs() const noexcept {
    return _blobs;
  }

 private:
  std::map<doc_id_t, std::string> _blobs;
};

class IndexUnderTest {
 public:
  IndexUnderTest() : _codec{formats::Get("1_5simd")} {}

  bool valid() const noexcept { return _codec != nullptr; }
  const Format::ptr& codec() const noexcept { return _codec; }
  MemoryDirectory& dir() noexcept { return _dir; }

  std::shared_ptr<IndexWriter> Open() {
    auto options = irs::tests::DefaultWriterOptions();
    options.norm_column_id = [](field_id id) -> field_id {
      return static_cast<field_id>(id + 1000);
    };
    return IndexWriter::Make(_dir, _codec, kOmCreate, std::move(options));
  }

  DirectoryReader Read() {
    return DirectoryReader(_dir, _codec, irs::tests::DefaultReaderOptions());
  }

 private:
  MemoryDirectory _dir;
  Format::ptr _codec;
};

void WriteBlock(IndexWriter::Transaction& trx,
                irs::analysis::Tokenizer& tokenizer, const TokenTraits& traits,
                std::span<const FieldPlan> plan,
                std::span<const std::string> keys,
                std::span<const std::string> values, std::string& error,
                StoreCollector* store = nullptr) {
  const auto n = static_cast<uint32_t>(values.size());
  duckdb::Vector key_vec{duckdb::LogicalType::VARCHAR};
  auto* key_slots =
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(key_vec);
  duckdb::Vector text_vec{duckdb::LogicalType::VARCHAR};
  auto* text_slots =
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(text_vec);
  for (uint32_t i = 0; i < n; ++i) {
    key_slots[i] =
      duckdb::string_t{keys[i].data(), static_cast<uint32_t>(keys[i].size())};
    text_slots[i] =
      values[i].empty()
        ? duckdb::string_t{}
        : duckdb::string_t{values[i].data(),
                           static_cast<uint32_t>(values[i].size())};
  }
  duckdb::UnifiedVectorFormat key_fmt;
  key_vec.ToUnifiedFormat(n, key_fmt);
  duckdb::UnifiedVectorFormat text_fmt;
  text_vec.ToUnifiedFormat(n, text_fmt);

  auto doc = trx.Insert(false, n);
  if (!doc) {
    error = "Insert refused a batch";
    return;
  }
  const auto first_doc = doc.DocId();
  if (!doc.WithField(kKey, IndexFeatures::Freq, [&](FieldInverter& fld) {
        return fld.InvertKeywordBlock(key_fmt, n, first_doc);
      })) {
    error = "the row-key field was rejected";
    return;
  }
  for (const auto& field : plan) {
    StoreSink* sink_for_field = (&field == plan.data()) ? store : nullptr;
    const bool ok = doc.WithTokens(field.id, field.features, sink_for_field,
                                   [&](FieldInverter& fld, TokenSink& sink) {
                                     fld.Configure(traits);
                                     tokenizer.Fill(text_fmt, n, first_doc,
                                                    sink, {fld.Layout()});
                                   });
    if (!ok) {
      error = std::string{"inversion rejected a block for field "} + field.name;
      return;
    }
  }
}

void WriteMultiValueDocs(IndexWriter::Transaction& trx,
                         irs::analysis::Tokenizer& tokenizer,
                         const TokenTraits& traits,
                         std::span<const FieldPlan> plan,
                         std::span<const std::string> keys,
                         std::span<const std::vector<std::string>> docs,
                         std::string& error, StoreCollector* store = nullptr) {
  for (size_t d = 0; d < docs.size(); ++d) {
    duckdb::Vector key_vec{duckdb::LogicalType::VARCHAR};
    auto* key_slots =
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(key_vec);
    key_slots[0] =
      duckdb::string_t{keys[d].data(), static_cast<uint32_t>(keys[d].size())};
    duckdb::UnifiedVectorFormat key_fmt;
    key_vec.ToUnifiedFormat(1, key_fmt);

    auto doc = trx.Insert(false, 1);
    if (!doc) {
      error = "Insert refused a document";
      return;
    }
    const auto id = doc.DocId();
    if (!doc.WithField(kKey, IndexFeatures::Freq, [&](FieldInverter& fld) {
          return fld.InvertKeywordBlock(key_fmt, 1, id);
        })) {
      error = "the row-key field was rejected";
      return;
    }
    for (const auto& field : plan) {
      StoreSink* sink_for_field = (&field == plan.data()) ? store : nullptr;
      const bool ok = doc.WithTokens(
        field.id, field.features, sink_for_field,
        [&](FieldInverter& fld, TokenSink& sink) {
          fld.Configure(traits);
          for (const auto& value : docs[d]) {
            const duckdb::string_t handle =
              value.empty()
                ? duckdb::string_t{}
                : duckdb::string_t{value.data(),
                                   static_cast<uint32_t>(value.size())};
            tokenizer.Fill(handle, id, sink, {fld.Layout()});
          }
        });
      if (!ok) {
        error = std::string{"inversion rejected a multi-value document for "} +
                field.name;
        return;
      }
    }
  }
}

struct Verification {
  std::map<std::string, PostingsDigest> by_field;
  size_t segments = 0;
  uint64_t docs = 0;
};

void ReadIndex(IndexUnderTest& index, std::span<const FieldPlan> plan,
               Verification& out,
               std::map<std::string, TermMap>* exact = nullptr) {
  auto reader = index.Read();
  ASSERT_TRUE(reader);
  out.segments = reader.size();
  out.docs = reader.docs_count();
  for (size_t s = 0; s < reader.size(); ++s) {
    const auto& segment = reader[s];
    std::map<doc_id_t, size_t> row_of;
    ASSERT_NO_FATAL_FAILURE(CollectKeys(segment, row_of));
    for (const auto& field : plan) {
      TermMap* target = nullptr;
      if (exact != nullptr) {
        target = &(*exact)[field.name];
      }
      ASSERT_NO_FATAL_FAILURE(
        CollectField(segment, field, row_of, out.by_field[field.name], target));
    }
  }
}

void ExpectSamePostings(const TermMap& want, const TermMap& got, bool with_pos,
                        bool with_offs) {
  std::vector<std::string> missing;
  std::vector<std::string> extra;
  for (const auto& [term, docs] : want) {
    if (!got.contains(term)) {
      missing.push_back(term);
    }
  }
  for (const auto& [term, docs] : got) {
    if (!want.contains(term)) {
      extra.push_back(term);
    }
  }
  ASSERT_TRUE(missing.empty())
    << missing.size() << " emitted terms are absent from the index, first: "
    << Describe(missing.front());
  ASSERT_TRUE(extra.empty())
    << extra.size() << " terms in the index were never emitted, first: "
    << Describe(extra.front());

  for (const auto& [term, want_docs] : want) {
    SCOPED_TRACE(testing::Message() << "term=" << Describe(term));
    const auto& got_docs = got.at(term);
    ASSERT_EQ(want_docs.size(), got_docs.size()) << "document frequency";
    for (size_t i = 0; i < want_docs.size(); ++i) {
      SCOPED_TRACE(testing::Message() << "row=" << want_docs[i].row);
      ASSERT_EQ(want_docs[i].row, got_docs[i].row);
      ASSERT_EQ(want_docs[i].freq, got_docs[i].freq) << "term frequency";
      if (!with_pos) {
        continue;
      }
      ASSERT_EQ(want_docs[i].occ.size(), got_docs[i].occ.size());
      for (size_t k = 0; k < want_docs[i].occ.size(); ++k) {
        ASSERT_EQ(want_docs[i].occ[k].pos, got_docs[i].occ[k].pos)
          << "position " << k;
        if (with_offs) {
          ASSERT_EQ(want_docs[i].occ[k].start, got_docs[i].occ[k].start)
            << "offset start " << k;
          ASSERT_EQ(want_docs[i].occ[k].end, got_docs[i].occ[k].end)
            << "offset end " << k;
        }
      }
    }
  }
}

std::vector<std::string> MakeKeys(size_t count) {
  std::vector<std::string> keys;
  keys.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    keys.push_back(std::to_string(i));
  }
  return keys;
}

std::map<TokenLayout, std::vector<Result>> AnalyseForPlan(
  const Spec& spec, std::span<const FieldPlan> plan,
  std::span<const std::string> values) {
  auto reference = Make(spec);
  std::map<TokenLayout, std::vector<Result>> out;
  for (const auto& field : plan) {
    const auto layout = LayoutFromFeatures(field.features);
    if (out.contains(layout)) {
      continue;
    }
    std::vector<Result> rows;
    rows.reserve(values.size());
    for (const auto& v : values) {
      rows.push_back(AnalyzeValue(*reference, v, layout));
    }
    out.emplace(layout, std::move(rows));
  }
  return out;
}

void BuildIndex(const Spec& spec, std::span<const FieldPlan> plan,
                std::span<const std::string> keys,
                std::span<const std::string> values, IndexUnderTest& index,
                bool consolidate) {
  auto writer = index.Open();
  ASSERT_NE(nullptr, writer);
  auto tokenizer = Make(spec);
  ASSERT_NE(nullptr, tokenizer);
  const auto traits = tokenizer->Traits();

  {
    auto trx = writer->GetBatch();
    for (size_t base = 0; base < values.size(); base += kInsertBlock) {
      const auto n = std::min<size_t>(kInsertBlock, values.size() - base);
      std::string error;
      WriteBlock(trx, *tokenizer, traits, plan, keys.subspan(base, n),
                 values.subspan(base, n), error);
      ASSERT_TRUE(error.empty()) << spec.name << ": " << error;
    }
    ASSERT_TRUE(trx.Commit());
  }
  ASSERT_TRUE(writer->RefreshCommit());

  if (!consolidate) {
    return;
  }
  const irs::index_utils::CompactionCount compact_all;
  ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(compact_all)));
  writer->RefreshCommit();
}

void CheckInversion(const Spec& spec, std::span<const std::string> values,
                    bool exact, bool consolidate) {
  IndexUnderTest index;
  ASSERT_TRUE(index.valid());
  auto reference = Make(spec);
  ASSERT_NE(nullptr, reference);
  const auto plan = PlanFor(reference->Traits());
  const auto keys = MakeKeys(values.size());

  ASSERT_NO_FATAL_FAILURE(
    BuildIndex(spec, plan, keys, values, index, consolidate));

  const auto analysed = AnalyseForPlan(spec, plan, values);
  std::map<std::string, PostingsDigest> want;
  for (const auto& field : plan) {
    FoldExpectedField(analysed.at(LayoutFromFeatures(field.features)), field,
                      want[field.name]);
  }

  Verification got;
  std::map<std::string, TermMap> got_exact;
  ASSERT_NO_FATAL_FAILURE(
    ReadIndex(index, plan, got, exact ? &got_exact : nullptr));
  ASSERT_EQ(values.size(), got.docs);

  for (const auto& field : plan) {
    SCOPED_TRACE(testing::Message() << "field=" << field.name);
    ASSERT_EQ(want[field.name], got.by_field[field.name])
      << "expected " << want[field.name].Describe() << "\n     got "
      << got.by_field[field.name].Describe();
    if (!exact) {
      continue;
    }
    const auto& rows = analysed.at(LayoutFromFeatures(field.features));
    const bool with_pos = Has(field.features, IndexFeatures::Pos);
    const bool with_offs = Has(field.features, IndexFeatures::Offs);
    const auto expected = ExpectedPostings(
      rows, Has(field.features, IndexFeatures::Freq), with_pos, with_offs);
    auto& actual = got_exact[field.name];
    for (auto& [term, docs] : actual) {
      std::ranges::sort(
        docs, [](const auto& a, const auto& b) { return a.row < b.row; });
    }
    ASSERT_NO_FATAL_FAILURE(
      ExpectSamePostings(expected, actual, with_pos, with_offs));
  }
}

Result CombineDocValues(std::span<const Result> values) {
  Result combined;
  combined.ok = true;
  uint32_t pos_base = 0;
  uint32_t offs_base = 0;
  for (const auto& value : values) {
    if (!value.ok || value.tokens.empty()) {
      continue;
    }
    for (auto token : value.tokens) {
      token.pos += pos_base;
      token.offs_start += offs_base;
      token.offs_end += offs_base;
      combined.tokens.push_back(std::move(token));
    }
    pos_base += value.tokens.back().pos;
    offs_base += value.tokens.back().offs_end;
  }
  return combined;
}

std::vector<std::vector<std::string>> GroupIntoDocs(
  std::span<const std::string> values, size_t per_doc) {
  std::vector<std::vector<std::string>> docs;
  for (size_t i = 0; i < values.size(); i += per_doc) {
    const auto n = std::min(per_doc, values.size() - i);
    docs.emplace_back(values.begin() + static_cast<long>(i),
                      values.begin() + static_cast<long>(i + n));
  }
  return docs;
}

bool FillDocument(const IndexWriter::Document& doc,
                  irs::analysis::Tokenizer& tokenizer,
                  const TokenTraits& traits, std::span<const FieldPlan> plan,
                  const std::string& key, const std::string& value,
                  std::string& error) {
  const auto id = doc.DocId();
  duckdb::Vector key_vec{duckdb::LogicalType::VARCHAR};
  auto* key_slots =
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(key_vec);
  key_slots[0] =
    duckdb::string_t{key.data(), static_cast<uint32_t>(key.size())};
  duckdb::UnifiedVectorFormat key_fmt;
  key_vec.ToUnifiedFormat(1, key_fmt);

  duckdb::Vector text_vec{duckdb::LogicalType::VARCHAR};
  auto* text_slots =
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(text_vec);
  text_slots[0] =
    value.empty()
      ? duckdb::string_t{}
      : duckdb::string_t{value.data(), static_cast<uint32_t>(value.size())};
  duckdb::UnifiedVectorFormat text_fmt;
  text_vec.ToUnifiedFormat(1, text_fmt);

  if (!doc.WithField(kKey, IndexFeatures::Freq, [&](FieldInverter& fld) {
        return fld.InvertKeywordBlock(key_fmt, 1, id);
      })) {
    error = "the row-key field was rejected";
    return false;
  }
  for (const auto& field : plan) {
    const bool ok =
      doc.WithTokens(field.id, field.features, nullptr,
                     [&](FieldInverter& fld, TokenSink& sink) {
                       fld.Configure(traits);
                       tokenizer.Fill(text_fmt, 1, id, sink, {fld.Layout()});
                     });
    if (!ok) {
      error = std::string{"inversion rejected a document for "} + field.name;
      return false;
    }
  }
  return true;
}

Filter::ptr KeyFilter(std::string_view key) {
  auto filter = std::make_unique<ByTerm>();
  *filter->mutable_field_id() = kKey;
  filter->mutable_options()->term = irs::ViewCast<byte_type>(key);
  return filter;
}

double Seconds(Clock::time_point from) {
  return std::chrono::duration<double>(Clock::now() - from).count();
}

uint64_t NameSeed(const std::string& name) {
  uint64_t h = 0xCBF29CE484222325ull;
  for (const char c : name) {
    h = (h ^ static_cast<unsigned char>(c)) * 0x100000001B3ull;
  }
  return h;
}

std::vector<std::string> GenerateCorpus(const Spec& spec, uint64_t seed,
                                        uint64_t byte_budget, size_t& bytes) {
  Mutator mutator{seed ^ NameSeed(spec.name), spec.dict, SizeCap(spec)};
  FeedbackCorpus corpus;
  for (const auto& v : SpecCorpus(spec, seed, 0)) {
    corpus.Seed(v);
  }
  std::vector<std::string> values;
  bytes = 0;
  while (bytes < byte_budget) {
    auto v = (values.size() % 32 == 0) ? mutator.Generate()
                                       : mutator.Mutate(corpus.Pick(mutator));
    if (spec.utf8_only && !IsValidUtf8(v)) {
      continue;
    }
    bytes += v.size() + 1;
    if (values.size() % 11 == 0) {
      corpus.Offer(v, values.size());
    }
    values.push_back(std::move(v));
  }
  return values;
}

}  // namespace

TEST(TokenizerInversion, PostingsMatchEmittedTokens) {
  const auto docs =
    static_cast<size_t>(EnvU64("TOKENIZER_INVERSION_DOCS", 192));
  for (const auto* spec : SelectedSpecs()) {
    SCOPED_TRACE(testing::Message() << spec->name << " seed=" << Seed());
    const auto values = SpecCorpus(*spec, Seed(), ValueBudget(*spec, docs));
    ASSERT_NO_FATAL_FAILURE(
      CheckInversion(*spec, values, /*exact=*/true, /*consolidate=*/false));
  }
}

TEST(TokenizerInversion, MultiValueDocuments) {
  const auto docs_wanted =
    static_cast<size_t>(EnvU64("TOKENIZER_INVERSION_DOCS", 192));
  for (const auto* spec : SelectedSpecs()) {
    SCOPED_TRACE(testing::Message() << spec->name << " seed=" << Seed());
    const auto values =
      SpecCorpus(*spec, Seed() ^ 0x3117ull, ValueBudget(*spec, docs_wanted));
    for (const size_t per_doc : {size_t{2}, size_t{3}, size_t{7}}) {
      SCOPED_TRACE(testing::Message() << "values_per_doc=" << per_doc);
      const auto docs = GroupIntoDocs(values, per_doc);
      const auto keys = MakeKeys(docs.size());

      auto reference = Make(*spec);
      ASSERT_NE(nullptr, reference);
      const auto traits = reference->Traits();
      const auto plan = PlanFor(traits);

      IndexUnderTest index;
      ASSERT_TRUE(index.valid());
      {
        auto writer = index.Open();
        ASSERT_NE(nullptr, writer);
        auto tokenizer = Make(*spec);
        ASSERT_NE(nullptr, tokenizer);
        auto trx = writer->GetBatch();
        std::string error;
        WriteMultiValueDocs(trx, *tokenizer, traits, plan, keys, docs, error);
        ASSERT_TRUE(error.empty()) << spec->name << ": " << error;
        ASSERT_TRUE(trx.Commit());
        ASSERT_TRUE(writer->RefreshCommit());
      }

      std::map<std::string, PostingsDigest> want;
      for (const auto& field : plan) {
        const auto layout = LayoutFromFeatures(field.features);
        const bool with_freq = Has(field.features, IndexFeatures::Freq);
        const bool with_pos = Has(field.features, IndexFeatures::Pos);
        const bool with_offs = Has(field.features, IndexFeatures::Offs);
        auto analyser = Make(*spec);
        ASSERT_NE(nullptr, analyser);
        for (size_t d = 0; d < docs.size(); ++d) {
          std::vector<Result> per_value;
          per_value.reserve(docs[d].size());
          for (const auto& v : docs[d]) {
            per_value.push_back(AnalyzeValue(*analyser, v, layout));
          }
          FoldValue(want[field.name], d, CombineDocValues(per_value), with_freq,
                    with_pos, with_offs);
        }
      }

      Verification got;
      ASSERT_NO_FATAL_FAILURE(ReadIndex(index, plan, got, nullptr));
      ASSERT_EQ(docs.size(), got.docs);
      for (const auto& field : plan) {
        SCOPED_TRACE(testing::Message() << "field=" << field.name);
        ASSERT_EQ(want[field.name], got.by_field[field.name])
          << "expected " << want[field.name].Describe() << "\n     got "
          << got.by_field[field.name].Describe();
      }
    }
  }
}

TEST(TokenizerInversion, StoredBlobsReachTheSink) {
  for (const auto* spec : SelectedSpecs()) {
    auto reference = Make(*spec);
    ASSERT_NE(nullptr, reference);
    if (!reference->Traits().store) {
      continue;
    }
    SCOPED_TRACE(testing::Message() << spec->name << " seed=" << Seed());
    auto values = SpecCorpus(*spec, Seed(), 64);
    if (values.size() > kInsertBlock) {
      values.resize(kInsertBlock);
    }
    const auto plan = PlanFor(reference->Traits());
    const auto keys = MakeKeys(values.size());

    IndexUnderTest index;
    ASSERT_TRUE(index.valid());
    StoreCollector store;
    doc_id_t first_doc = doc_limits::invalid();
    {
      auto writer = index.Open();
      ASSERT_NE(nullptr, writer);
      auto tokenizer = Make(*spec);
      ASSERT_NE(nullptr, tokenizer);
      auto trx = writer->GetBatch();
      std::string error;
      WriteBlock(trx, *tokenizer, reference->Traits(), plan, keys, values,
                 error, &store);
      ASSERT_TRUE(error.empty()) << spec->name << ": " << error;
      first_doc = doc_limits::min();
      ASSERT_TRUE(trx.Commit());
      ASSERT_TRUE(writer->RefreshCommit());
    }

    const auto layout = LayoutFromFeatures(plan.front().features);
    auto analyser = Make(*spec);
    ASSERT_NE(nullptr, analyser);
    size_t expected_blobs = 0;
    for (size_t i = 0; i < values.size(); ++i) {
      const auto res = AnalyzeValue(*analyser, values[i], layout);
      if (!res.ok || res.store.empty()) {
        continue;
      }
      ++expected_blobs;
      const auto doc = first_doc + static_cast<doc_id_t>(i);
      const auto it = store.blobs().find(doc);
      ASSERT_TRUE(it != store.blobs().end())
        << "no stored blob for row " << i << ": " << Describe(values[i]);
      ASSERT_EQ(res.store, it->second)
        << "stored blob differs for row " << i << ": " << Describe(values[i]);
    }
    EXPECT_GT(expected_blobs, 0u)
      << spec->name << " declares store but produced no blob";
  }
}

TEST(TokenizerInversion, SurvivesDeletesAndUpdates) {
  const auto docs_wanted =
    static_cast<size_t>(EnvU64("TOKENIZER_INVERSION_DOCS", 192));
  for (const auto* spec : SelectedFamilies()) {
    SCOPED_TRACE(testing::Message() << spec->name << " seed=" << Seed());
    const auto values =
      SpecCorpus(*spec, Seed() ^ 0xDE1E7Eull, ValueBudget(*spec, docs_wanted));
    ASSERT_FALSE(values.empty());
    const auto keys = MakeKeys(values.size());

    auto reference = Make(*spec);
    ASSERT_NE(nullptr, reference);
    const auto traits = reference->Traits();
    const auto plan = PlanFor(traits);

    enum Fate : uint8_t {
      kKeep,
      kRemoved,
      kReplaced,
    };
    std::vector<Fate> fate(values.size(), kKeep);
    auto live = values;
    for (size_t i = 0; i < values.size(); ++i) {
      if (i % 3 == 0) {
        fate[i] = kRemoved;
      } else if (i % 5 == 0) {
        fate[i] = kReplaced;
        live[i] = values[(i + 7) % values.size()] + "-updated";
      }
    }

    IndexUnderTest index;
    ASSERT_TRUE(index.valid());
    auto writer = index.Open();
    ASSERT_NE(nullptr, writer);
    auto tokenizer = Make(*spec);
    ASSERT_NE(nullptr, tokenizer);

    {
      auto trx = writer->GetBatch();
      for (size_t base = 0; base < values.size(); base += kInsertBlock) {
        const auto n = std::min<size_t>(kInsertBlock, values.size() - base);
        std::string error;
        WriteBlock(trx, *tokenizer, traits, plan,
                   std::span{keys}.subspan(base, n),
                   std::span{values}.subspan(base, n), error);
        ASSERT_TRUE(error.empty()) << spec->name << ": " << error;
      }
      ASSERT_TRUE(trx.Commit());
    }
    ASSERT_TRUE(writer->RefreshCommit());

    {
      auto trx = writer->GetBatch();
      for (size_t i = 0; i < values.size(); ++i) {
        if (fate[i] == kRemoved) {
          trx.Remove(KeyFilter(keys[i]));
          continue;
        }
        if (fate[i] != kReplaced) {
          continue;
        }
        auto doc = trx.Replace(KeyFilter(keys[i]));
        ASSERT_TRUE(doc);
        std::string error;
        ASSERT_TRUE(
          FillDocument(doc, *tokenizer, traits, plan, keys[i], live[i], error))
          << spec->name << ": " << error;
      }
      ASSERT_TRUE(trx.Commit());
    }
    ASSERT_TRUE(writer->RefreshCommit());

    std::map<std::string, PostingsDigest> want;
    for (const auto& field : plan) {
      const auto layout = LayoutFromFeatures(field.features);
      const bool with_freq = Has(field.features, IndexFeatures::Freq);
      const bool with_pos = Has(field.features, IndexFeatures::Pos);
      const bool with_offs = Has(field.features, IndexFeatures::Offs);
      auto analyser = Make(*spec);
      ASSERT_NE(nullptr, analyser);
      for (size_t i = 0; i < live.size(); ++i) {
        if (fate[i] == kRemoved) {
          continue;
        }
        FoldValue(want[field.name], i, AnalyzeValue(*analyser, live[i], layout),
                  with_freq, with_pos, with_offs);
      }
    }

    Verification got;
    ASSERT_NO_FATAL_FAILURE(ReadIndex(index, plan, got, nullptr));
    for (const auto& field : plan) {
      SCOPED_TRACE(testing::Message() << "field=" << field.name);
      ASSERT_EQ(want[field.name], got.by_field[field.name])
        << "expected " << want[field.name].Describe() << "\n     got "
        << got.by_field[field.name].Describe();
    }
  }
}

TEST(TokenizerInversion, SurvivesConsolidation) {
  const auto docs =
    static_cast<size_t>(EnvU64("TOKENIZER_INVERSION_DOCS", 192));
  for (const auto* spec : SelectedFamilies()) {
    SCOPED_TRACE(testing::Message() << spec->name << " seed=" << Seed());
    const auto values = SpecCorpus(*spec, Seed(), ValueBudget(*spec, docs));
    ASSERT_NO_FATAL_FAILURE(
      CheckInversion(*spec, values, /*exact=*/true, /*consolidate=*/true));
  }
}

TEST(TokenizerInversionLoad, ManyDocuments) {
  const auto budget = EnvU64("TOKENIZER_INVERSION_LOAD_BYTES", 32ull << 20);
  const auto seconds =
    static_cast<double>(EnvU64("TOKENIZER_INVERSION_LOAD_SECONDS", 0));
  const auto seed = Seed() ^ 0x1E7E12ull;
  const auto start = Clock::now();

  for (const auto* spec : SelectedFamilies()) {
    if (seconds > 0.0 && Seconds(start) >= seconds) {
      break;
    }
    SCOPED_TRACE(testing::Message() << spec->name << " seed=" << seed);
    const auto spec_budget = budget / std::max<uint32_t>(1, spec->cost);
    size_t bytes = 0;
    const auto values = GenerateCorpus(*spec, seed, spec_budget, bytes);
    const auto spec_start = Clock::now();
    ASSERT_NO_FATAL_FAILURE(
      CheckInversion(*spec, values, /*exact=*/false, /*consolidate=*/false));
    std::printf("[ INVERTED ] %-42s %6zu MiB  %8zu docs  %6.1fs\n",
                spec->name.c_str(), bytes >> 20, values.size(),
                Seconds(spec_start));
  }
}

TEST(TokenizerInversionLoad, ConcurrentWriters) {
  const auto threads = static_cast<size_t>(
    EnvU64("TOKENIZER_INVERSION_THREADS",
           std::max<unsigned>(2, std::thread::hardware_concurrency() / 2)));
  const auto docs =
    static_cast<size_t>(EnvU64("TOKENIZER_INVERSION_CONCURRENT_DOCS", 2048));
  const auto seed = Seed() ^ 0xC0C0ull;

  for (const auto* spec : SelectedFamilies()) {
    SCOPED_TRACE(testing::Message() << spec->name << " threads=" << threads);
    const auto per_thread = std::max<size_t>(
      32, ValueBudget(*spec, docs) / std::max<size_t>(1, threads));
    const auto total = per_thread * threads;
    const auto values = SpecCorpus(*spec, seed, total);
    const auto trimmed =
      std::span{values}.first(std::min(total, values.size()));
    const auto keys = MakeKeys(trimmed.size());

    auto reference = Make(*spec);
    ASSERT_NE(nullptr, reference);
    const auto plan = PlanFor(reference->Traits());

    IndexUnderTest index;
    ASSERT_TRUE(index.valid());
    auto writer = index.Open();
    ASSERT_NE(nullptr, writer);

    std::atomic<size_t> failures{0};
    std::mutex report_mutex;
    std::string first_error;
    std::vector<std::thread> pool;
    const auto stride = (trimmed.size() + threads - 1) / threads;
    for (size_t t = 0; t < threads; ++t) {
      pool.emplace_back([&, t] {
        auto tokenizer = Make(*spec);
        if (!tokenizer) {
          ++failures;
          return;
        }
        const auto traits = tokenizer->Traits();
        const auto begin = std::min(t * stride, trimmed.size());
        const auto end = std::min(begin + stride, trimmed.size());
        auto trx = writer->GetBatch();
        for (size_t base = begin; base < end; base += kInsertBlock) {
          const auto n = std::min<size_t>(kInsertBlock, end - base);
          std::string error;
          WriteBlock(trx, *tokenizer, traits, plan,
                     std::span{keys}.subspan(base, n), trimmed.subspan(base, n),
                     error);
          if (!error.empty()) {
            std::lock_guard lock{report_mutex};
            if (first_error.empty()) {
              first_error = error;
            }
            ++failures;
            return;
          }
        }
        if (!trx.Commit()) {
          std::lock_guard lock{report_mutex};
          if (first_error.empty()) {
            first_error = "concurrent commit failed";
          }
          ++failures;
        }
      });
    }
    for (auto& thread : pool) {
      thread.join();
    }
    ASSERT_EQ(0u, failures.load()) << spec->name << ": " << first_error;
    ASSERT_TRUE(writer->RefreshCommit());

    const auto analysed = AnalyseForPlan(*spec, plan, trimmed);
    std::map<std::string, PostingsDigest> want;
    for (const auto& field : plan) {
      FoldExpectedField(analysed.at(LayoutFromFeatures(field.features)), field,
                        want[field.name]);
    }
    Verification got;
    ASSERT_NO_FATAL_FAILURE(ReadIndex(index, plan, got, nullptr));
    ASSERT_EQ(trimmed.size(), got.docs);
    for (const auto& field : plan) {
      SCOPED_TRACE(testing::Message() << "field=" << field.name);
      ASSERT_EQ(want[field.name], got.by_field[field.name])
        << "expected " << want[field.name].Describe() << "\n     got "
        << got.by_field[field.name].Describe();
    }
  }
}
