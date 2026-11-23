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
#include <absl/base/internal/endian.h>
#include <absl/strings/str_cat.h>

#include <concepts>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/external_file_cache/external_file_cache.hpp>
#include <iresearch/formats/flush_state.hpp>
#include <iresearch/formats/format_utils.hpp>
#include <iresearch/formats/index/field_reader.hpp>
#include <iresearch/formats/index/field_writer.hpp>
#include <iresearch/formats/index/idx_reader.hpp>
#include <iresearch/formats/index/idx_writer.hpp>
#include <iresearch/formats/index/term_dict_block_filter.hpp>
#include <iresearch/formats/index/term_dict_cursor.hpp>
#include <iresearch/formats/index/term_dict_index_walk.hpp>
#include <iresearch/formats/reader_state.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/levenshtein_acceptor.hpp>
#include <iresearch/utils/levenshtein_utils.hpp>
#include <iresearch/utils/numeric_utils.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/regexp_acceptor.hpp>
#include <numeric>
#include <random>
#include <span>

#include "formats_test_case_base.hpp"
#include "index_dir.hpp"
#include "tests_param.hpp"
#include "tests_shared.hpp"

#ifdef SERENEDB_HAVE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

namespace {

using Docs = std::vector<std::pair<irs::doc_id_t, uint32_t>>;

struct Entry {
  irs::bstring key;
  Docs docs;
};

struct Field {
  irs::field_id id;
  std::vector<Entry> entries;
  bool sample = true;
  irs::DictRef dict = irs::StringLayout::kStrings;
};

bool IsNumbers(irs::DictRef dict) {
  return dict.member == irs::DictMember::Numbers;
}

class OracleTerms final : public irs::TermIterator {
 public:
  explicit OracleTerms(const std::vector<Entry>& entries)
    : _entries{&entries} {}

  bool next() final {
    if (_next == _entries->size()) {
      return false;
    }
    _value = (*_entries)[_next++].key;
    return true;
  }

  irs::bytes_view value() const noexcept final { return _value; }

  const irs::PostingMeta& cookie() const noexcept final { return _meta; }

  irs::TermPostings::ptr postings(irs::IndexFeatures) const final {
    return irs::memory::make_managed<tests::FormatTestCase::TestPostings>(
      (*_entries)[_next - 1].docs);
  }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id) noexcept final {
    return nullptr;
  }

 private:
  const std::vector<Entry>* _entries;
  irs::bytes_view _value;
  irs::PostingMeta _meta;
  size_t _next = 0;
};

uint64_t CacheMisses() {
  return duckdb::ExternalFileCache::Get(
           irs::DuckDBEngine::Instance().instance())
    .GetStatistics()
    .misses;
}

#ifdef SERENEDB_HAVE_JEMALLOC
uint64_t ThreadAllocatedBytes() {
  uint64_t epoch = 1;
  size_t epoch_size = sizeof(epoch);
  mallctl("epoch", &epoch, &epoch_size, &epoch, sizeof(epoch));
  uint64_t allocated = 0;
  size_t size = sizeof(allocated);
  if (mallctl("thread.allocated", &allocated, &size, nullptr, 0) != 0) {
    return 0;
  }
  return allocated;
}
constexpr bool kHaveCounter = true;
#else
uint64_t ThreadAllocatedBytes() { return 0; }
constexpr bool kHaveCounter = false;
#endif

constexpr std::string_view kSegment = "term_dict_segment";

const irs::term_dict::TermReaderImpl& Blocks(const irs::TermReader& reader) {
  EXPECT_LT(1, reader.size());
  return static_cast<const irs::term_dict::TermReaderImpl&>(reader);
}

size_t BlockOf(const irs::term_dict::TermReaderImpl& impl,
               irs::bytes_view term) {
  irs::term_dict::PaddedKey key;
  return impl.Index().BlockOf(key.Assign(impl.KeyPrefix(), term));
}

std::array<size_t, 3> BlockKinds(const irs::term_dict::TermReaderImpl& reader) {
  std::array<size_t, 3> kinds{};
  irs::term_dict::PinnedRange pinned;
  for (uint32_t block = 0; block != reader.Blocks(); ++block) {
    ++kinds.at(reader.Block(block, pinned).front());
  }
  return kinds;
}

Docs MakeDocs(size_t count, uint32_t seed) {
  Docs docs;
  docs.reserve(count);
  irs::doc_id_t doc = irs::doc_limits::min() + seed % 7;
  for (size_t i = 0; i != count; ++i) {
    docs.emplace_back(doc, 1);
    doc += 1 + (seed + i) % 3;
  }
  return docs;
}

std::vector<irs::PostingMeta> BatchLookup(
  const irs::TermReader& reader, std::span<const irs::bytes_view> terms) {
  std::vector<irs::PostingMeta> metas(terms.size());
  reader.LookupBatch(
    terms.size(), [&](size_t i) { return terms[i]; },
    [&](size_t i, const irs::PostingMeta& meta) { metas[i] = meta; });
  return metas;
}

std::vector<Entry> MakeEntries(std::vector<irs::bstring> keys) {
  absl::c_sort(keys);
  keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
  constexpr size_t kCounts[]{1, 1, 2, 3, 1, 5, 128, 1, 129, 2, 300, 1};
  std::vector<Entry> entries;
  entries.reserve(keys.size());
  for (size_t i = 0; i != keys.size(); ++i) {
    entries.emplace_back(
      std::move(keys[i]),
      MakeDocs(kCounts[i % std::size(kCounts)], static_cast<uint32_t>(i)));
  }
  return entries;
}

irs::bstring Bytes(std::string_view str) {
  return irs::bstring{irs::ViewCast<irs::byte_type>(str)};
}

void AssertSameMeta(const irs::PostingMeta& expected,
                    const irs::PostingMeta& actual) {
  ASSERT_EQ(expected.docs_count, actual.docs_count);
  ASSERT_EQ(expected.freq, actual.freq);
  ASSERT_EQ(expected.doc_start, actual.doc_start);
  ASSERT_EQ(expected.doc_delta, actual.doc_delta);
  ASSERT_EQ(expected.Inline(), actual.Inline());
}

uint32_t KeyWidth(const std::vector<Entry>& entries) {
  if (entries.empty()) {
    return 0;
  }
  const auto width = entries.front().key.size();
  return absl::c_all_of(
           entries,
           [&](const Entry& entry) { return entry.key.size() == width; })
           ? static_cast<uint32_t>(width)
           : 0;
}

irs::bstring BigEndian(uint8_t shift, uint64_t value) {
  irs::bstring key(1 + sizeof(uint64_t), irs::byte_type{0});
  key[0] = shift;
  absl::big_endian::Store64(key.data() + 1, value >> shift << shift);
  return key;
}

std::vector<Entry> NumericEntries(
  std::vector<std::pair<int64_t, irs::doc_id_t>> values) {
  absl::c_sort(values);
  values.erase(std::unique(values.begin(), values.end()), values.end());
  std::vector<Entry> entries;
  for (const auto& [value, doc] : values) {
    irs::byte_type buf[irs::numeric_utils::kNumericTermMaxSize];
    const auto key = irs::numeric_utils::EncodeNumericTerm(buf, value);
    if (entries.empty() || entries.back().key != key) {
      entries.push_back({.key = irs::bstring{key}, .docs = {}});
    }
    entries.back().docs.emplace_back(doc, 1);
  }
  return entries;
}

std::vector<irs::bstring> Words(size_t count, uint32_t seed) {
  std::mt19937 rng{seed};
  constexpr std::string_view kStems[]{
    "inter", "nation", "al",  "ism", "pre", "sent",   "ation", "un",
    "der",   "stand",  "ing", "re",  "con", "struct", "ion",   "s",
  };
  std::vector<irs::bstring> words;
  words.reserve(count);
  for (size_t i = 0; i != count; ++i) {
    std::string word;
    const size_t parts = 1 + rng() % 4;
    for (size_t p = 0; p != parts; ++p) {
      word += kStems[rng() % std::size(kStems)];
    }
    if (rng() % 3 == 0) {
      word += std::to_string(rng() % 1000);
    }
    words.emplace_back(Bytes(word));
  }
  return words;
}

std::vector<Entry> Ladder(std::string_view last) {
  std::vector<irs::bstring> keys;
  for (size_t i = 0; i != 3000; ++i) {
    keys.emplace_back(Bytes(absl::StrCat("ay", absl::Dec(i, absl::kZeroPad4))));
  }
  keys.emplace_back(Bytes(last));
  return MakeEntries(std::move(keys));
}

std::vector<Entry> Multiples(irs::doc_id_t count) {
  std::vector<std::pair<int64_t, irs::doc_id_t>> values;
  for (irs::doc_id_t doc = 1; doc <= count; ++doc) {
    values.emplace_back(int64_t{doc} * 3, doc);
  }
  return NumericEntries(std::move(values));
}

irs::bstring OrdinalPrefix(uint32_t ordinal, uint32_t width) {
  irs::byte_type bytes[sizeof(uint32_t)];
  absl::big_endian::Store32(bytes, ordinal);
  return irs::bstring{bytes + sizeof(bytes) - width, width};
}

class EntriesReader final : public irs::BasicTermReader {
 public:
  explicit EntriesReader(const Field& field)
    : _field{&field}, _terms{field.entries}, _it{_terms} {
    key_width = KeyWidth(field.entries);
  }

  irs::TermOnlyIterator::ptr iterator() const final {
    return irs::memory::to_managed<irs::TermOnlyIterator>(_it);
  }
  irs::field_id id() const final { return _field->id; }
  irs::FieldProperties properties() const final {
    return {.index_features = IsNumbers(_field->dict)
                                ? irs::IndexFeatures::Ranges
                                : irs::IndexFeatures::None};
  }

  void SampleTerms(uint64_t bytes,
                   absl::FunctionRef<void(std::span<const irs::bytes_view>)>
                     visit) const final {
    constexpr size_t kRun = 16;
    const auto& entries = _field->entries;
    if (!_field->sample || entries.empty()) {
      return;
    }
    const size_t stride = std::max<size_t>(
      kRun, entries.size() / std::max<uint64_t>(1, bytes / 256));
    std::array<irs::bytes_view, kRun> run;
    for (size_t i = 0; i < entries.size(); i += stride) {
      const size_t n = std::min(kRun, entries.size() - i);
      for (size_t j = 0; j != n; ++j) {
        run[j] = entries[i + j].key;
      }
      visit({run.data(), n});
    }
  }

 private:
  const Field* _field;
  mutable OracleTerms _terms;
  mutable tests::BatchingTermIterator _it;
};

class TermDictTest : public tests::DirectoryTestCaseBase {
 protected:
  void TearDown() final {
    _reader.reset();
    _idx.reset();
    DirectoryTestCaseBase::TearDown();
  }

  void Write(std::span<const Field> fields, bool compaction = false,
             uint64_t keyed_bound = 0) {
    irs::doc_id_t docs = 0;
    for (const auto& field : fields) {
      for (const auto& entry : field.entries) {
        for (const auto& [doc, freq] : entry.docs) {
          docs = std::max(docs, doc);
        }
      }
    }
    _docs = docs;
    irs::FlushState state{
      .tracker = dir().Tracker(kSegment),
      .doc_count = docs,
      .index_features = irs::IndexFeatures::None,
    };
    irs::IdxWriter idx{dir().Tracker(kSegment)};
    irs::term_dict::FieldWriter writer{compaction};
    writer.SetIdxWriter(idx);
    const uint64_t keyed = absl::c_count_if(
      fields, [](const Field& field) { return field.entries.size() > 1; });
    writer.prepare(state, std::max(keyed, keyed_bound));
    std::vector<const Field*> ordered;
    for (const auto& field : fields) {
      ordered.push_back(&field);
    }
    absl::c_stable_sort(ordered, [](const Field* lhs, const Field* rhs) {
      return std::tie(lhs->id, lhs->dict.member) <
             std::tie(rhs->id, rhs->dict.member);
    });
    for (const auto* field : ordered) {
      if (!irs::IsTermMember(field->dict.member)) {
        ASSERT_EQ(1, field->entries.size());
        std::vector<irs::doc_id_t> docs;
        for (const auto& [doc, freq] : field->entries.front().docs) {
          docs.push_back(doc);
        }
        writer.WriteSlot(field->id, field->dict, docs);
        continue;
      }
      EntriesReader reader{*field};
      writer.write(field->dict, reader);
    }
    writer.end();
    idx.Commit(nullptr);
    Open(dir());
  }

  void Open(const tests::IndexDir& from) {
    _reader.reset();
    _meta.name = kSegment;
    _idx = std::make_unique<irs::IdxReader>(from.Fs(), from.Root(), kSegment,
                                            true, nullptr);
    _reader = std::make_unique<irs::term_dict::FieldReader>();
    _reader->prepare(irs::ReaderState{.db = from.Db(),
                                      .fs = from.Fs(),
                                      .root = from.Root(),
                                      .meta = _meta,
                                      .idx = _idx.get()});
  }

  const irs::TermReader* Get(
    irs::field_id id, irs::DictRef dict = irs::StringLayout::kStrings) const {
    for (const auto* reader : _reader->fields(id)) {
      if (reader->meta().dict == dict) {
        return reader;
      }
    }
    return nullptr;
  }

  irs::DictLayout LayoutOf(irs::field_id id) const {
    return _reader->fields(id).front()->meta().dict.layout;
  }

  const irs::TermReader* Get(const Field& field) const {
    return Get(field.id, field.dict);
  }

  irs::term_dict::TermsMeta TakeTerms() {
    return irs::IdxReader{dir().Fs(), dir().Root(), kSegment, true, nullptr}
      .TakeTerms();
  }

  void RewriteTerms(const irs::term_dict::TermsMeta& terms) {
    RewriteTermsWith([&](duckdb::BinarySerializer& object) {
      irs::term_dict::WriteTerms(object, terms);
    });
  }

  void RewriteTermsWith(
    absl::FunctionRef<void(duckdb::BinarySerializer&)> write) {
    _reader.reset();
    _idx.reset();
    const auto name = absl::StrCat(kSegment, ".", irs::kIdxFormatExt);
    irs::bstring data;
    {
      const auto file = dir().Open(name, duckdb::MappedFileAccess::RANDOM);
      ASSERT_TRUE(file);
      duckdb::InputFileReader in{*file};
      const uint64_t length = file->GetSize();
      in.Seek(length - sizeof(uint32_t));
      const uint64_t footer = in.Read<uint32_t>();
      data.resize(length - 2 * sizeof(uint32_t) - footer);
      in.Seek(0);
      in.ReadData(data.data(), data.size());
    }
    ASSERT_TRUE(dir().Remove(name));
    auto out = dir().Create(name);
    ASSERT_NE(nullptr, out);
    out->WriteData(data.data(), data.size());
    irs::format_utils::WriteFooter(*out, [&](duckdb::BinarySerializer& meta) {
      meta.WriteObject(irs::kIdxFieldTerms, "terms", write);
    });
    out->Close();
  }

  static std::vector<irs::bstring> Probes(const std::vector<Entry>& entries) {
    std::vector<irs::bstring> probes{
      irs::bstring{},
      Bytes("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"),
      Bytes("m"),
    };
    const size_t step = std::max<size_t>(1, entries.size() / 2000);
    for (size_t i = 0; i < entries.size(); i += step) {
      const auto& key = entries[i].key;
      probes.emplace_back(key);
      probes.emplace_back(key + irs::byte_type{0});
      probes.emplace_back(key + irs::byte_type{0xFF});
      if (!key.empty()) {
        probes.emplace_back(key.substr(0, key.size() - 1));
        if (key.back() != 0xFF) {
          auto next = key;
          ++next.back();
          probes.emplace_back(std::move(next));
        }
        if (key.back() != 0) {
          auto prev = key;
          --prev.back();
          probes.emplace_back(std::move(prev));
        }
      }
    }
    return probes;
  }

  static void AssertDocs(const irs::TermReader& reader, const Entry& entry) {
    std::vector<irs::doc_id_t> actual;
    reader.ReadDocs(entry.key, [&](irs::doc_id_t doc) {
      actual.push_back(doc);
      return true;
    });
    ASSERT_EQ(entry.docs.size(), actual.size());
    for (size_t i = 0; i != actual.size(); ++i) {
      ASSERT_EQ(entry.docs[i].first, actual[i]);
    }
  }

  static void AssertField(const irs::TermReader& reader,
                          const std::vector<Entry>& entries) {
    ASSERT_EQ(entries.size(), reader.size());
    ASSERT_EQ(irs::bytes_view{entries.front().key}, reader.min());
    ASSERT_EQ(irs::bytes_view{entries.back().key}, reader.max());

    {
      auto it = reader.iterator();
      size_t i = 0;
      for (; it->next(); ++i) {
        ASSERT_LT(i, entries.size());
        ASSERT_EQ(irs::bytes_view{entries[i].key}, it->value());
        ASSERT_EQ(entries[i].docs.size(), it->cookie().docs_count);
      }
      ASSERT_EQ(entries.size(), i);
      ASSERT_FALSE(it->next());
    }

    for (const auto& entry : entries) {
      ASSERT_EQ(entry.docs.size(), reader.Lookup(entry.key).docs_count);
      AssertDocs(reader, entry);
    }

    AssertProbes(reader, entries, Probes(entries));
  }

  static void AssertProbes(const irs::TermReader& reader,
                           const std::vector<Entry>& entries,
                           const std::vector<irs::bstring>& probes) {
    const auto less = [](const Entry& entry, irs::bytes_view key) {
      return irs::bytes_view{entry.key} < key;
    };
    std::vector<irs::bytes_view> terms{probes.begin(), probes.end()};
    absl::c_sort(terms);
    const auto metas = BatchLookup(reader, terms);
    for (size_t i = 0; i != terms.size(); ++i) {
      SCOPED_TRACE(testing::Message("batch probe ") << i);
      AssertSameMeta(reader.Lookup(terms[i]), metas[i]);
    }
    auto shared = reader.iterator();
    for (const auto& probe : probes) {
      SCOPED_TRACE(testing::Message("probe size ") << probe.size());
      const auto lb =
        absl::c_lower_bound(entries, irs::bytes_view{probe}, less);
      const bool present = lb != entries.end() && lb->key == probe;
      ASSERT_EQ(present ? lb->docs.size() : 0, reader.Lookup(probe).docs_count);

      auto fresh = reader.iterator();
      for (auto* it : {fresh.get(), shared.get()}) {
        auto& iter = *it;
        const auto result = iter.seek_ge(probe);
        if (lb == entries.end()) {
          ASSERT_EQ(irs::SeekResult::End, result);
          continue;
        }
        ASSERT_EQ(present ? irs::SeekResult::Found : irs::SeekResult::NotFound,
                  result);
        ASSERT_EQ(irs::bytes_view{lb->key}, iter.value());
        ASSERT_EQ(lb->docs.size(), iter.cookie().docs_count);
        auto expected = lb;
        for (size_t k = 0; k != 3 && ++expected != entries.end(); ++k) {
          ASSERT_TRUE(iter.next());
          ASSERT_EQ(irs::bytes_view{expected->key}, iter.value());
          ASSERT_EQ(expected->docs.size(), iter.cookie().docs_count);
        }
      }
    }
  }

  static void AssertWalk(const irs::TermReader& reader,
                         const std::vector<Entry>& entries,
                         const irs::RegexpAcceptor& acceptor) {
    ASSERT_TRUE(acceptor.ok());
    std::vector<irs::bytes_view> expected;
    for (const auto& entry : entries) {
      if (acceptor.Matches(entry.key)) {
        expected.emplace_back(entry.key);
      }
    }
    {
      auto it = reader.iterator(acceptor);
      size_t i = 0;
      for (; it->next(); ++i) {
        ASSERT_LT(i, expected.size());
        ASSERT_EQ(expected[i], it->value());
      }
      ASSERT_EQ(expected.size(), i);
    }
    auto it = reader.iterator(acceptor);
    for (const auto& probe : Probes(entries)) {
      const auto lb = absl::c_lower_bound(expected, irs::bytes_view{probe});
      const auto result = it->seek_ge(probe);
      if (lb == expected.end()) {
        ASSERT_EQ(irs::SeekResult::End, result);
        continue;
      }
      ASSERT_EQ(
        *lb == probe ? irs::SeekResult::Found : irs::SeekResult::NotFound,
        result);
      ASSERT_EQ(*lb, it->value());
      if (lb + 1 != expected.end()) {
        ASSERT_TRUE(it->next());
        ASSERT_EQ(*(lb + 1), it->value());
      }
    }
  }

  std::vector<irs::doc_id_t> DocsOf(const irs::TermReader& reader,
                                    const irs::PostingMeta& meta) const {
    std::vector<irs::doc_id_t> docs;
    if (meta.docs_count == 1) {
      docs.push_back(irs::doc_limits::min() + meta.doc_delta);
      return docs;
    }
    auto it = _reader->Postings().Postings(reader.meta().index_features,
                                           irs::IndexFeatures::None, meta,
                                           reader.HasScoreBounds());
    irs::doc_id_t buffer[irs::doc_limits::kBlockSize];
    while (const auto n = it->NextDocs(buffer, nullptr)) {
      docs.insert(docs.end(), buffer, buffer + n);
    }
    return docs;
  }

  static std::vector<irs::doc_id_t> DocsIn(const std::vector<Entry>& entries,
                                           irs::bytes_view from,
                                           irs::bytes_view to) {
    const auto less = [](const Entry& entry, irs::bytes_view key) {
      return irs::bytes_view{entry.key} < key;
    };
    const auto begin = absl::c_lower_bound(entries, from, less);
    const auto end =
      to.empty() ? entries.end() : absl::c_lower_bound(entries, to, less);
    std::vector<irs::doc_id_t> docs;
    for (auto it = begin; it < end; ++it) {
      for (const auto& [doc, freq] : it->docs) {
        docs.push_back(doc);
      }
    }
    absl::c_sort(docs);
    docs.erase(std::unique(docs.begin(), docs.end()), docs.end());
    return docs;
  }

  static std::vector<irs::bstring> MaximalVisits(
    const std::vector<Entry>& entries,
    const std::vector<std::vector<irs::bstring>>& starts, irs::bytes_view from,
    irs::bytes_view to) {
    const auto end_of = [&](size_t level, size_t i) -> irs::bytes_view {
      return i + 1 == starts[level].size() ? irs::bytes_view{}
                                           : starts[level][i + 1];
    };
    const auto fits = [&](size_t level, size_t i) {
      if (irs::bytes_view{starts[level][i]} < from) {
        return false;
      }
      const irs::bytes_view end = end_of(level, i);
      if (to.empty()) {
        return true;
      }
      return end.empty() ? irs::bytes_view{entries.back().key} < to : end <= to;
    };
    std::vector<irs::bstring> visits;
    irs::bytes_view lower;
    irs::bytes_view upper;
    bool covered = false;
    for (size_t level = 0; level != starts.size(); ++level) {
      for (size_t i = 0; i != starts[level].size(); ++i) {
        if (!fits(level, i)) {
          continue;
        }
        if (level + 1 != starts.size()) {
          const auto& above = starts[level + 1];
          const auto parent = static_cast<size_t>(
            absl::c_upper_bound(above, starts[level][i]) - above.begin() - 1);
          if (fits(level + 1, parent)) {
            continue;
          }
        }
        const irs::bytes_view start = starts[level][i];
        if (!covered || start < lower) {
          lower = start;
        }
        const irs::bytes_view end = end_of(level, i);
        if (!covered || (!upper.empty() && (end.empty() || upper < end))) {
          upper = end;
        }
        covered = true;
        visits.emplace_back(start);
      }
    }
    for (const auto& entry : entries) {
      const irs::bytes_view key = entry.key;
      if (key < from || (!to.empty() && key >= to)) {
        continue;
      }
      if (!covered || key < lower || (!upper.empty() && key >= upper)) {
        visits.emplace_back(key);
      }
    }
    absl::c_sort(visits);
    return visits;
  }

  void AssertLevels(const irs::TermReader& reader,
                    const std::vector<Entry>& entries) const {
    constexpr size_t kHeavyDocs = size_t{1} << 14;
    constexpr size_t kTopRangePostings = 16;
    const size_t heavy =
      std::max<size_t>(kHeavyDocs, _docs / kTopRangePostings);
    const auto& impl = Blocks(reader);
    const auto levels = impl.RangeLevels();
    std::vector<irs::bstring> below_keys;
    std::vector<std::vector<irs::doc_id_t>> below;
    for (const auto& entry : entries) {
      below_keys.push_back(entry.key);
      auto& docs = below.emplace_back();
      for (const auto& [doc, freq] : entry.docs) {
        docs.push_back(doc);
      }
    }
    for (size_t k = 0; k != levels.size(); ++k) {
      SCOPED_TRACE(testing::Message("level ") << k);
      const auto& level = *levels[k];
      ASSERT_EQ(reader.key_width, level.key_width);
      std::vector<irs::bstring> keys;
      std::vector<irs::PostingMeta> metas;
      for (auto it = level.iterator(); it->next();) {
        keys.emplace_back(it->value());
        metas.push_back(it->cookie());
      }
      const auto count = static_cast<uint32_t>(keys.size());
      ASSERT_EQ(level.size(), count);
      ASSERT_GE(count, 2);
      ASSERT_LT(count, below.size());
      const auto first_child = [&](uint32_t i) -> size_t {
        if (i == count) {
          return below.size();
        }
        return absl::c_lower_bound(below_keys, keys[i]) - below_keys.begin();
      };
      std::vector<std::vector<irs::doc_id_t>> docs(count);
      ASSERT_EQ(0, first_child(0));
      for (uint32_t i = 0; i != count; ++i) {
        const size_t first = first_child(i);
        const size_t end = first_child(i + 1);
        ASSERT_LT(first, end);
        ASSERT_EQ(below_keys[first], keys[i]);
        for (size_t j = first; j != end; ++j) {
          if (below[j].size() >= heavy) {
            ASSERT_EQ(1, end - first);
          }
          docs[i].insert(docs[i].end(), below[j].begin(), below[j].end());
        }
        absl::c_sort(docs[i]);
        docs[i].erase(std::unique(docs[i].begin(), docs[i].end()),
                      docs[i].end());
        ASSERT_EQ(docs[i], DocsOf(reader, metas[i]));
      }
      below_keys = std::move(keys);
      below = std::move(docs);
    }
  }

  void AssertVisits(const irs::TermReader& reader,
                    const std::vector<Entry>& entries, size_t rounds) const {
    constexpr size_t kLeafFanout = 64;
    constexpr size_t kFanout = 16;
    std::span<const irs::term_dict::TermReaderImpl* const> levels;
    if (reader.size() > 1) {
      levels = Blocks(reader).RangeLevels();
    }
    const size_t bound = levels.empty() ? entries.size()
                                        : levels.back()->size() +
                                            2 * kFanout * (levels.size() - 1) +
                                            2 * kLeafFanout;
    auto bounds = Probes(entries);
    std::vector<std::vector<irs::bstring>> starts;
    for (const auto* level : levels) {
      auto& level_starts = starts.emplace_back();
      for (auto it = level->iterator(); it->next();) {
        irs::bstring start{it->value()};
        level_starts.push_back(start);
        bounds.push_back(start + irs::byte_type{0});
        if (start.back() != 0) {
          auto prev = start;
          --prev.back();
          bounds.push_back(std::move(prev));
        }
        if (start.back() != 0xFF) {
          auto next = start;
          ++next.back();
          bounds.push_back(std::move(next));
        }
        bounds.push_back(std::move(start));
      }
    }
    std::mt19937 rng{42};
    for (size_t round = 0; round != rounds; ++round) {
      const irs::bytes_view from = bounds[rng() % bounds.size()];
      const irs::bytes_view to = bounds[rng() % bounds.size()];
      SCOPED_TRACE(testing::Message("round ") << round);
      std::vector<irs::bstring> keys;
      std::vector<irs::doc_id_t> actual;
      reader.CoverRange(from, to,
                        [&](irs::bytes_view key, const irs::PostingMeta& meta) {
                          keys.emplace_back(key);
                          const auto docs = DocsOf(reader, meta);
                          actual.insert(actual.end(), docs.begin(), docs.end());
                          return true;
                        });
      ASSERT_LE(keys.size(), bound);
      absl::c_sort(keys);
      for (size_t i = 0; i != keys.size(); ++i) {
        ASSERT_LE(from, irs::bytes_view{keys[i]});
        ASSERT_TRUE(to.empty() || irs::bytes_view{keys[i]} < to);
        ASSERT_TRUE(i == 0 || keys[i - 1] < keys[i]);
      }
      ASSERT_EQ(MaximalVisits(entries, starts, from, to), keys);
      absl::c_sort(actual);
      actual.erase(std::unique(actual.begin(), actual.end()), actual.end());
      ASSERT_EQ(DocsIn(entries, from, to), actual);
    }
  }

  void RangeLevelsOracle(bool compaction) {
    std::mt19937 rng{17};
    std::vector<std::pair<int64_t, irs::doc_id_t>> sequential;
    std::vector<std::pair<int64_t, irs::doc_id_t>> scattered;
    std::vector<std::pair<int64_t, irs::doc_id_t>> clustered;
    std::vector<std::pair<int64_t, irs::doc_id_t>> heavy;
    std::vector<std::pair<int64_t, irs::doc_id_t>> mixed;
    std::vector<std::pair<int64_t, irs::doc_id_t>> multi;
    for (irs::doc_id_t doc = 1; doc <= 50000; ++doc) {
      sequential.emplace_back(int64_t{doc} * 3, doc);
      scattered.emplace_back(
        static_cast<int64_t>(uint64_t{rng()} << 32 | rng()) / 2, doc);
      clustered.emplace_back(int64_t{doc / 200} * 1000000 + rng() % 50, doc);
    }
    for (irs::doc_id_t doc = 1; doc <= 340000; ++doc) {
      heavy.emplace_back(-int64_t{doc % 20}, doc);
    }
    for (irs::doc_id_t doc = 1; doc <= 60000; ++doc) {
      mixed.emplace_back(
        doc % 3 == 0 ? 7 : (doc % 5 == 1 ? 1000000 : int64_t{doc}), doc);
    }
    for (irs::doc_id_t doc = 1; doc <= 20000; ++doc) {
      for (uint32_t n = 1 + rng() % 3; n != 0; --n) {
        multi.emplace_back(rng() % 3000, doc);
      }
    }
    const Field fields[]{
      {.id = 1,
       .entries = NumericEntries(sequential),
       .dict = irs::NumericLayout::kNumbers},
      {.id = 2,
       .entries = NumericEntries(scattered),
       .dict = irs::NumericLayout::kNumbers},
      {.id = 3,
       .entries = NumericEntries(clustered),
       .dict = irs::NumericLayout::kNumbers},
      {.id = 4,
       .entries = NumericEntries(heavy),
       .dict = irs::NumericLayout::kNumbers},
      {.id = 5,
       .entries = NumericEntries(mixed),
       .dict = irs::NumericLayout::kNumbers},
      {.id = 6,
       .entries = NumericEntries(multi),
       .dict = irs::NumericLayout::kNumbers},
    };
    Write(fields, compaction);
    for (const auto& field : fields) {
      SCOPED_TRACE(testing::Message("field ") << field.id);
      const auto* reader = Get(field);
      ASSERT_NE(nullptr, reader);
      AssertField(*reader, field.entries);
      AssertLevels(*reader, field.entries);
      AssertVisits(*reader, field.entries, field.id == 4 ? 100 : 10000);
    }
    const auto height = [&](irs::field_id id) {
      return Blocks(*Get(id, irs::NumericLayout::kNumbers))
        .RangeLevels()
        .size();
    };
    ASSERT_EQ(4, height(1));
    ASSERT_EQ(1, height(4));
  }

  void AssertOrdinal(std::span<const Field> fields, size_t index,
                     uint32_t width) const {
    const auto& field = fields[index];
    SCOPED_TRACE(testing::Message("field ") << index);
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    const auto& impl = Blocks(*reader);
    auto ordinal = static_cast<uint32_t>(index);
    for (const auto& earlier : fields.first(index)) {
      if (IsNumbers(earlier.dict)) {
        ordinal += Blocks(*Get(earlier)).RangeLevels().size();
      }
    }
    ASSERT_EQ(irs::bytes_view{OrdinalPrefix(ordinal, width)}, impl.KeyPrefix());
    for (const auto* level : impl.RangeLevels()) {
      ASSERT_EQ(irs::bytes_view{OrdinalPrefix(++ordinal, width)},
                level->KeyPrefix());
    }
    AssertField(*reader, field.entries);
    if (IsNumbers(field.dict)) {
      ASSERT_FALSE(impl.RangeLevels().empty());
      AssertLevels(*reader, field.entries);
      AssertVisits(*reader, field.entries, 1000);
      return;
    }
    for (const std::string_view pattern :
         {".b.*", "a[a-x].*|b.*", "x.*", ".*ing"}) {
      SCOPED_TRACE(testing::Message("pattern ") << pattern);
      AssertWalk(*reader, field.entries,
                 irs::RegexpAcceptor{irs::ViewCast<irs::byte_type>(pattern)});
    }
    std::vector<irs::bytes_view> targets{field.entries.back().key};
    if (index + 1 != fields.size()) {
      targets.emplace_back(fields[index + 1].entries.front().key);
    }
    const auto description = irs::MakeParametricDescription(1, false);
    for (const auto target : targets) {
      SCOPED_TRACE(testing::Message("target ") << irs::ViewCast<char>(target));
      const irs::LevenshteinAcceptor acceptor{description, {}, target};
      std::vector<irs::bytes_view> expected;
      for (const auto& entry : field.entries) {
        if (acceptor.Matches(entry.key)) {
          expected.emplace_back(entry.key);
        }
      }
      auto it = reader->iterator(acceptor);
      size_t i = 0;
      for (; it->next(); ++i) {
        ASSERT_LT(i, expected.size());
        ASSERT_EQ(expected[i], it->value());
      }
      ASSERT_EQ(expected.size(), i);
    }
  }

  irs::SegmentMeta _meta;
  std::unique_ptr<irs::IdxReader> _idx;
  std::unique_ptr<irs::term_dict::FieldReader> _reader;
  irs::doc_id_t _docs = 0;
};

TEST_P(TermDictTest, edge_keys) {
  std::vector<irs::bstring> keys{
    Bytes(""),
    irs::bstring(1, irs::byte_type{0}),
    irs::bstring(2, irs::byte_type{0}),
    Bytes("a"),
    Bytes("ab"),
    Bytes("abc"),
    irs::bstring(3, irs::byte_type{0xFF}),
    irs::bstring(5000, irs::byte_type{'q'}),
    irs::bstring(70000, irs::byte_type{'r'}),
    irs::bstring(70000, irs::byte_type{'r'}) + irs::byte_type{'s'},
  };
  const std::string prefix(300, 'p');
  for (size_t i = 0; i != 400; ++i) {
    keys.emplace_back(Bytes(prefix + std::to_string(i)));
  }
  for (size_t i = 0; i != 40; ++i) {
    keys.emplace_back(Bytes(std::string(31 + i, 'h')));
  }
  const Field fields[]{{.id = 1, .entries = MakeEntries(std::move(keys))}};
  Write(fields);
  const auto* reader = Get(1);
  ASSERT_NE(nullptr, reader);
  AssertField(*reader, fields[0].entries);
}

TEST_P(TermDictTest, fixed_width_ladders) {
  std::vector<irs::bstring> keys;
  for (const uint8_t shift : {0, 8, 16, 32}) {
    for (uint64_t value = 0; value != 20000; ++value) {
      keys.emplace_back(BigEndian(shift, value * 977));
    }
  }
  for (size_t i = 0; i != 300; ++i) {
    keys.emplace_back(Bytes("\x08" + std::to_string(i * 31)));
  }
  const Field fields[]{{.id = 3, .entries = MakeEntries(std::move(keys))}};
  Write(fields);
  const auto* reader = Get(3);
  ASSERT_NE(nullptr, reader);
  AssertField(*reader, fields[0].entries);
}

TEST_P(TermDictTest, fixed_layout_by_key_width) {
  std::mt19937_64 rng{17};
  std::vector<Field> fields;
  for (const uint32_t width : {4, 5, 8, 9, 16, 32, 33}) {
    std::vector<irs::bstring> keys;
    for (size_t i = 0; i != 20000; ++i) {
      irs::bstring key(width, irs::byte_type{0});
      for (size_t j = 0; j != width; ++j) {
        key[j] = static_cast<irs::byte_type>(j < width / 2 ? i >> 8 : rng());
      }
      keys.push_back(std::move(key));
    }
    fields.push_back({.id = width, .entries = MakeEntries(std::move(keys))});
  }
  Write(fields);
  Open(dir());
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("width ") << field.id);
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    const auto width = field.entries.front().key.size();
    ASSERT_EQ(width <= irs::term_dict::kMaxStride ? width : 0,
              reader->key_width);
    if (reader->key_width != 0) {
      const size_t before = CacheMisses();
      reader->Lookup(field.entries[100].key + irs::byte_type{0});
      ASSERT_EQ(before, CacheMisses());
    }
    AssertField(*reader, field.entries);
  }
}

TEST_P(TermDictTest, fixed_keys_pack_by_value_range) {
  const auto keys = [](size_t width, size_t count, auto&& value) {
    std::vector<irs::bstring> keys;
    for (size_t i = 0; i != count; ++i) {
      irs::bstring key(sizeof(uint64_t), irs::byte_type{0});
      absl::big_endian::Store64(key.data(), value(i));
      keys.push_back(key.substr(sizeof(uint64_t) - width));
    }
    return MakeEntries(std::move(keys));
  };
  const Field fields[]{
    {.id = 1, .entries = keys(2, 20000, [](size_t i) { return 3 * i; })},
    {.id = 2, .entries = keys(4, 20000, [](size_t i) { return 0xFFF000 + i; })},
    {.id = 3, .entries = keys(8, 1024, [](size_t i) { return i << 54; })},
    {.id = 4,
     .entries = keys(8, 1001,
                     [](size_t i) {
                       return i == 1000 ? std::numeric_limits<uint64_t>::max()
                                        : i;
                     })},
  };
  Write(fields);
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    const auto kinds = BlockKinds(Blocks(*reader));
    ASSERT_NE(
      0, kinds[static_cast<size_t>(irs::term_dict::BlockKind::FixedStride)]);
    AssertField(*reader, field.entries);
  }
}

TEST_P(TermDictTest, fixed_keys_split_low_and_high_bits) {
  const auto keys = [](size_t width, size_t count, auto&& value) {
    std::vector<irs::bstring> keys;
    for (size_t i = 0; i != count; ++i) {
      irs::bstring key(sizeof(uint64_t), irs::byte_type{0});
      absl::big_endian::Store64(key.data(), value(i));
      keys.push_back(key.substr(sizeof(uint64_t) - width));
    }
    return MakeEntries(std::move(keys));
  };
  std::mt19937_64 rng{53};
  const Field fields[]{
    {.id = 1,
     .entries = keys(8, 20,
                     [](size_t i) {
                       return i * (std::numeric_limits<uint64_t>::max() / 19);
                     })},
    {.id = 2,
     .entries =
       keys(8, 6000, [](size_t i) { return (i / 100) << 40 | (i % 100); })},
    {.id = 3, .entries = keys(8, 6000, [](size_t i) { return i * i * i; })},
    {.id = 4,
     .entries = keys(3, 4000, [&](size_t) { return rng() & 0xFFFFFF; })},
    {.id = 5, .entries = keys(1, 200, [](size_t i) { return i + 50; })},
    {.id = 6,
     .entries = keys(6, 9000, [](size_t i) { return i * 3 + (i % 64 == 0); })},
  };
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    Write({&field, 1});
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    const auto& impl =
      static_cast<const irs::term_dict::TermReaderImpl&>(*reader);
    irs::term_dict::PinnedRange pinned;
    for (uint32_t block = 0; block != impl.Blocks(); ++block) {
      ASSERT_EQ(irs::term_dict::BlockKind::FixedStride,
                static_cast<irs::term_dict::BlockKind>(
                  impl.Block(block, pinned).front()));
    }
    AssertField(*reader, field.entries);
  }
}

TEST_P(TermDictTest, fixed_keys_fill_multi_page_blocks) {
  constexpr uint64_t kInlined = 40000;
  constexpr uint64_t kSingles = 100000;
  constexpr irs::doc_id_t kSpread = 300;
  constexpr irs::doc_id_t kListDocs = 16;
  const auto key = [](uint64_t value) {
    irs::bstring key(sizeof(uint64_t), irs::byte_type{0});
    absl::big_endian::Store64(key.data(), value);
    return key;
  };
  std::vector<Entry> entries;
  entries.reserve(kInlined + kSingles);
  for (uint64_t i = 0; i != kInlined; ++i) {
    Docs docs;
    for (irs::doc_id_t j = 0; j != kListDocs; ++j) {
      docs.emplace_back(irs::doc_limits::min() + j * kSpread +
                          static_cast<irs::doc_id_t>(i % kSpread),
                        1);
    }
    entries.push_back({.key = key(i), .docs = std::move(docs)});
  }
  for (uint64_t i = 0; i != kSingles; ++i) {
    const auto doc = irs::doc_limits::min() +
                     static_cast<irs::doc_id_t>(i % (kListDocs * kSpread));
    entries.push_back({.key = key(kInlined + i), .docs = {{doc, 1}}});
  }
  const Field field{.id = 1, .entries = std::move(entries)};
  Write({&field, 1});
  const auto& impl = Blocks(*Get(field));
  uint64_t widest = 0;
  for (uint32_t block = 0; block != impl.Blocks(); ++block) {
    widest =
      std::max(widest, impl.BlockOffset(block + 1) - impl.BlockOffset(block));
  }
  ASSERT_GT(widest, 8 * uint64_t{irs::term_dict::kPage});
  AssertField(impl, field.entries);
}

TEST_P(TermDictTest, fixed_keys_stay_within_their_samples_reach) {
  constexpr uint64_t kInlined = 40000;
  constexpr uint64_t kShared = 200000;
  constexpr irs::doc_id_t kSpread = 300;
  constexpr irs::doc_id_t kListDocs = 16;
  const auto key = [](uint64_t value) {
    irs::bstring key(sizeof(uint64_t), irs::byte_type{0});
    absl::big_endian::Store64(key.data(), value);
    return key;
  };
  std::vector<Entry> entries;
  entries.reserve(kInlined + kShared);
  for (uint64_t i = 0; i != kInlined; ++i) {
    Docs docs;
    for (irs::doc_id_t j = 0; j != kListDocs; ++j) {
      docs.emplace_back(irs::doc_limits::min() + j * kSpread +
                          static_cast<irs::doc_id_t>(i % kSpread),
                        1);
    }
    entries.push_back({.key = key(i), .docs = std::move(docs)});
  }
  for (uint64_t i = 0; i != kShared; ++i) {
    entries.push_back(
      {.key = key(kInlined + i), .docs = {{irs::doc_limits::min(), 1}}});
  }
  const Field field{.id = 1, .entries = std::move(entries)};
  Write({&field, 1});
  AssertField(Blocks(*Get(field)), field.entries);
}

TEST_P(TermDictTest, one_term_dictionaries_live_in_the_footer) {
  const irs::bstring slot_term{irs::kSlotTerm};
  const Field fields[]{
    {.id = 1,
     .entries = MakeEntries({slot_term}),
     .dict = irs::BoolLayout::kNulls},
    {.id = 1,
     .entries = MakeEntries({slot_term}),
     .dict = irs::BoolLayout::kFalse},
    {.id = 1,
     .entries = MakeEntries({slot_term}),
     .dict = irs::BoolLayout::kTrue},
    {.id = 2,
     .entries = MakeEntries({slot_term}),
     .dict = irs::JsonLayout::kJsonNulls},
    {.id = 2,
     .entries = MakeEntries({Bytes("a"), Bytes("b")}),
     .dict = irs::JsonLayout::kStrings},
    {.id = 3,
     .entries = MakeEntries({slot_term}),
     .dict = irs::StringLayout::kNulls},
    {.id = 4, .entries = MakeEntries({Bytes("solo")})},
  };
  Write(fields);
  Open(dir());
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ")
                 << field.id << " member "
                 << static_cast<int>(field.dict.member));
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    if (field.entries.size() == 1) {
      ASSERT_EQ(1, reader->size());
      ASSERT_EQ(field.entries.front().docs.size(), reader->docs_count());
    }
    const size_t before = CacheMisses();
    AssertField(*reader, field.entries);
    ASSERT_EQ(before, CacheMisses());
  }
  ASSERT_EQ(nullptr, Get(2, irs::JsonLayout::kNulls));
  ASSERT_EQ(nullptr, Get(3, irs::StringLayout::kStrings));
  ASSERT_EQ(0, Blocks(*Get(2, irs::JsonLayout::kStrings)).KeyPrefix().front());
  const auto terms = TakeTerms();
  ASSERT_EQ(1, absl::c_count_if(terms.fields, [](const auto& head) {
              return head.term_count > 1;
            }));
}

TEST_P(TermDictTest, long_shared_prefixes_round_trip) {
  const auto shared = Bytes(std::string(2048, 'p'));
  std::vector<irs::bstring> keys;
  for (size_t i = 0; i != 5000; ++i) {
    auto key = shared;
    key += Bytes(absl::StrCat("word", i * 7919 % 100003));
    keys.push_back(std::move(key));
  }
  const Field fields[]{
    {.id = 1, .entries = MakeEntries(keys), .sample = false},
    {.id = 2, .entries = MakeEntries(std::move(keys))},
  };
  Write(fields);
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    AssertField(*reader, field.entries);
  }
}

TEST_P(TermDictTest, fsst_on_and_off) {
  auto keys = Words(30000, 3);
  for (size_t i = 0; i < keys.size(); i += 97) {
    keys[i].push_back(static_cast<irs::byte_type>(0x80 + i * 7 % 128));
    keys[i].push_back(static_cast<irs::byte_type>(0x80 + i * 13 % 128));
  }
  auto mixed = keys;
  std::mt19937 rng{11};
  for (size_t i = 0; i != 2000; ++i) {
    irs::bstring key(1, irs::byte_type{'~'});
    for (size_t j = 0; j != 8; ++j) {
      key.push_back(static_cast<irs::byte_type>(rng()));
    }
    mixed.push_back(std::move(key));
  }
  const Field fields[]{
    {.id = 1, .entries = MakeEntries(keys), .sample = false},
    {.id = 2, .entries = MakeEntries(keys)},
    {.id = 3, .entries = MakeEntries(std::move(mixed))},
  };
  Write(fields);
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    const auto& impl = Blocks(*reader);
    ASSERT_EQ(field.sample, impl.Fsst() != nullptr);
    const auto kinds = BlockKinds(impl);
    ASSERT_EQ(
      0, kinds[static_cast<size_t>(irs::term_dict::BlockKind::FixedStride)]);
    ASSERT_EQ(
      field.sample,
      kinds[static_cast<size_t>(irs::term_dict::BlockKind::VarFsst)] > 1);
    ASSERT_EQ(field.id != 2,
              kinds[static_cast<size_t>(irs::term_dict::BlockKind::Var)] > 2);
    AssertField(*reader, field.entries);
    for (const std::string_view pattern :
         {"inter.*", ".*ation", ".*stand.*", "[a-c].*ing[0-9]+"}) {
      SCOPED_TRACE(testing::Message("pattern ") << pattern);
      AssertWalk(*reader, field.entries,
                 irs::RegexpAcceptor{irs::ViewCast<irs::byte_type>(pattern)});
    }
  }
}

TEST_P(TermDictTest, fsst_codes_only_overflowing_blocks_a_dictionary_owns) {
  const auto keys = Words(30000, 3);
  const Field fields[]{
    {.id = 1, .entries = MakeEntries({keys.begin(), keys.begin() + 60})},
    {.id = 2, .entries = MakeEntries(keys)},
    {.id = 3, .entries = MakeEntries({keys.begin(), keys.begin() + 60})},
  };
  Write(fields);
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    AssertField(*reader, field.entries);
  }
  const auto kind_of = [](const irs::term_dict::TermReaderImpl& reader,
                          uint32_t block) {
    irs::term_dict::PinnedRange pinned;
    return static_cast<irs::term_dict::BlockKind>(
      reader.Block(block, pinned).front());
  };
  for (const auto id : {1, 3}) {
    SCOPED_TRACE(testing::Message("field ") << id);
    const auto& small = Blocks(*Get(id));
    ASSERT_EQ(1, small.Blocks());
    ASSERT_EQ(nullptr, small.Fsst());
    ASSERT_EQ(irs::term_dict::BlockKind::Var, kind_of(small, 0));
  }
  const auto& words = Blocks(*Get(2));
  ASSERT_NE(nullptr, words.Fsst());
  ASSERT_LT(2, words.Blocks());
  ASSERT_NE(0, words.FirstEntry());
  ASSERT_EQ(irs::term_dict::BlockKind::Var, kind_of(words, 0));
  for (uint32_t block = 1; block + 1 < words.Blocks(); ++block) {
    SCOPED_TRACE(testing::Message("block ") << block);
    ASSERT_EQ(irs::term_dict::BlockKind::VarFsst, kind_of(words, block));
  }
  ASSERT_EQ(
    kind_of(words, words.Blocks() - 1) == irs::term_dict::BlockKind::VarFsst,
    Blocks(*Get(3)).FirstEntry() == 0);
}

TEST_P(TermDictTest, fsst_skips_incompressible_fields) {
  const auto words = Words(30000, 3);
  std::mt19937 rng{13};
  std::vector<irs::bstring> random;
  for (size_t i = 0; i != 20000; ++i) {
    irs::bstring key;
    const size_t size = 8 + rng() % 8;
    for (size_t j = 0; j != size; ++j) {
      key.push_back(static_cast<irs::byte_type>(rng()));
    }
    random.push_back(std::move(key));
  }
  const Field fields[]{
    {.id = 1, .entries = MakeEntries(words)},
    {.id = 2, .entries = MakeEntries(std::move(random))},
    {.id = 3, .entries = MakeEntries(words)},
  };
  Write(fields);
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    const auto& impl = Blocks(*reader);
    ASSERT_EQ(field.id != 2, impl.Fsst() != nullptr);
    const auto kinds = BlockKinds(impl);
    ASSERT_EQ(
      field.id != 2,
      kinds[static_cast<size_t>(irs::term_dict::BlockKind::VarFsst)] != 0);
    ASSERT_EQ(field.id == 2,
              kinds[static_cast<size_t>(irs::term_dict::BlockKind::Var)] > 2);
    AssertField(*reader, field.entries);
  }
}

TEST_P(TermDictTest, single_term_fields) {
  const Field fields[]{
    {.id = 1, .entries = MakeEntries({Bytes("hello")})},
    {.id = 2, .entries = MakeEntries({irs::bstring{}})},
    {.id = 3,
     .entries = MakeEntries({BigEndian(0, 42)}),
     .dict = irs::NumericLayout::kNumbers},
    {.id = 4, .entries = MakeEntries(Words(3000, 41))},
    {.id = 5, .entries = MakeEntries({Bytes("world")})},
  };
  Write(fields);
  Open(dir());
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    const bool single = field.entries.size() == 1;
    ASSERT_EQ(single, reader->size() == 1);
    ASSERT_EQ(KeyWidth(field.entries), reader->key_width);
    const size_t before = CacheMisses();
    AssertField(*reader, field.entries);
    for (const std::string_view pattern : {".*", "hel.*", "x.*", ".*o"}) {
      SCOPED_TRACE(testing::Message("pattern ") << pattern);
      AssertWalk(*reader, field.entries,
                 irs::RegexpAcceptor{irs::ViewCast<irs::byte_type>(pattern)});
    }
    AssertVisits(*reader, field.entries, 20);
    if (single) {
      ASSERT_EQ(before, CacheMisses());
    }
  }
}

TEST_P(TermDictTest, single_valued_when_no_document_holds_two_terms) {
  const Field fields[]{
    {.id = 1,
     .entries = NumericEntries({{10, 1}, {20, 2}, {30, 3}, {40, 4}}),
     .dict = irs::NumericLayout::kNumbers},
    {.id = 2,
     .entries = NumericEntries({{10, 1}, {20, 2}, {30, 2}}),
     .dict = irs::NumericLayout::kNumbers},
    {.id = 3,
     .entries = {{.key = Bytes("a"), .docs = {{1, 1}}},
                 {.key = Bytes("b"), .docs = {{2, 1}, {3, 1}}}}},
    {.id = 4, .entries = MakeEntries(Words(3000, 41))},
    {.id = 5, .entries = MakeEntries({Bytes("only")})},
  };
  constexpr bool kExpected[]{true, false, true, false, true};
  for (const bool compaction : {false, true}) {
    SCOPED_TRACE(testing::Message("compaction ") << compaction);
    Write(fields, compaction);
    for (size_t i = 0; i != std::size(fields); ++i) {
      SCOPED_TRACE(testing::Message("field ") << fields[i].id);
      const auto* reader = Get(fields[i]);
      ASSERT_NE(nullptr, reader);
      ASSERT_EQ(kExpected[i], reader->single_valued);
    }
    const auto terms = TakeTerms();
    for (const auto& head : terms.fields) {
      SCOPED_TRACE(testing::Message("head ") << head.id);
      ASSERT_EQ(kExpected[head.id - 1], head.single_valued);
    }
  }
}

TEST_P(TermDictTest, seeks_inside_fsst_symbols) {
  std::mt19937 rng{23};
  auto keys = Words(20000, 7);
  for (size_t i = 0; i < keys.size(); i += 5) {
    keys[i].insert(rng() % (keys[i].size() + 1), 1,
                   static_cast<irs::byte_type>(rng() % 2 == 0 ? 0x01 : 0xFE));
  }
  const Field fields[]{{.id = 1, .entries = MakeEntries(std::move(keys))}};
  Write(fields);
  const auto* reader = Get(1);
  ASSERT_NE(nullptr, reader);
  const auto& impl = Blocks(*reader);
  ASSERT_NE(nullptr, impl.Fsst());
  ASSERT_NE(
    0,
    BlockKinds(impl)[static_cast<size_t>(irs::term_dict::BlockKind::VarFsst)]);
  const auto& entries = fields[0].entries;
  AssertField(*reader, entries);
  std::vector<irs::bstring> probes;
  for (size_t i = 0; i < entries.size(); i += 37) {
    const auto& key = entries[i].key;
    for (size_t size = 0; size <= key.size(); ++size) {
      probes.emplace_back(key.substr(0, size));
      if (size == key.size()) {
        break;
      }
      for (const int delta : {-1, 1}) {
        auto changed = key;
        changed[size] = static_cast<irs::byte_type>(changed[size] + delta);
        probes.emplace_back(changed);
        probes.emplace_back(changed.substr(0, size + 1));
      }
    }
  }
  AssertProbes(*reader, entries, probes);
}

TEST_P(TermDictTest, restart_heads_ties) {
  std::vector<irs::bstring> keys;
  for (char first = 'a'; first <= 'z'; ++first) {
    for (size_t i = 0; i != 700; ++i) {
      std::string key{first};
      key += i % 7 == 0 ? std::string_view{"SAM\0E", 5} : "SAMEPART";
      key += std::to_string(i);
      keys.emplace_back(Bytes(key));
      if (i % 50 == 0) {
        keys.emplace_back(Bytes(std::string{first, 'S'}));
        keys.emplace_back(Bytes(std::string{first, 'S', '\0'}));
      }
    }
  }
  const Field fields[]{
    {.id = 1, .entries = MakeEntries(keys), .sample = false},
    {.id = 2, .entries = MakeEntries(std::move(keys))},
  };
  Write(fields);
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    AssertField(*reader, field.entries);
  }
}

TEST_P(TermDictTest, fixed_stride_searches) {
  std::mt19937_64 rng{29};
  std::vector<Field> fields;
  for (const uint32_t width : {1, 2, 3, 7, 8, 9, 15, 16, 17, 24, 31, 32}) {
    std::vector<irs::bstring> keys{
      irs::bstring(width, irs::byte_type{0x00}),
      irs::bstring(width, irs::byte_type{0xFF}),
    };
    for (size_t i = 0; i != 6000; ++i) {
      irs::bstring key(width, irs::byte_type{0});
      for (auto& byte : key) {
        byte = static_cast<irs::byte_type>(rng());
      }
      if (i % 3 == 0) {
        key[width - 1] = 0xFF;
      }
      keys.push_back(std::move(key));
    }
    fields.push_back({.id = width, .entries = MakeEntries(std::move(keys))});
  }
  Write(fields);
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("width ") << field.id);
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    ASSERT_EQ(field.id, reader->key_width);
    AssertField(*reader, field.entries);
  }
}

TEST_P(TermDictTest, fixed_stride_tied_heads) {
  std::mt19937_64 rng{41};
  std::vector<Field> fields;
  for (const uint32_t width : {5, 6, 9, 16}) {
    std::vector<irs::bstring> keys;
    for (size_t i = 0; i != 8000; ++i) {
      irs::bstring key(width, irs::byte_type{0xAB});
      key[0] = static_cast<irs::byte_type>(i / 300);
      for (size_t j = 4; j != width; ++j) {
        key[j] = static_cast<irs::byte_type>(rng() % 3);
      }
      keys.push_back(std::move(key));
    }
    fields.push_back({.id = width, .entries = MakeEntries(std::move(keys))});
  }
  Write(fields);
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("width ") << field.id);
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    ASSERT_EQ(field.id, reader->key_width);
    AssertField(*reader, field.entries);
  }
}

TEST_P(TermDictTest, infix_walks_across_front_coding) {
  std::mt19937 rng{5};
  std::vector<irs::bstring> keys;
  for (size_t i = 0; i != 12000; ++i) {
    irs::bstring key;
    const size_t size = 2 + rng() % 11;
    for (size_t j = 0; j != size; ++j) {
      key.push_back(static_cast<irs::byte_type>(
        rng() % 64 == 0 ? 'g' + rng() % 20 : 'a' + rng() % 3));
    }
    keys.push_back(std::move(key));
  }
  const Field fields[]{
    {.id = 1, .entries = MakeEntries(keys), .sample = false},
    {.id = 2, .entries = MakeEntries(keys)},
  };
  Write(fields);
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    ASSERT_EQ(
      field.sample,
      BlockKinds(Blocks(
        *reader))[static_cast<size_t>(irs::term_dict::BlockKind::VarFsst)] > 1);
    for (const std::string_view pattern :
         {".*abc.*", ".*aba.*", ".*cabca.*", ".*bhc.*", "b.*acb.*"}) {
      SCOPED_TRACE(testing::Message("pattern ") << pattern);
      AssertWalk(*reader, field.entries,
                 irs::RegexpAcceptor{irs::ViewCast<irs::byte_type>(pattern)});
    }
  }
}

TEST_P(TermDictTest, skips_report_the_prefix_a_restart_shares) {
  std::vector<irs::bstring> keys{Bytes("a"), Bytes("b")};
  for (size_t i = 0; i != 40; ++i) {
    keys.push_back(Bytes(absl::StrCat("c", std::string(20, 'x'), 100 + i)));
  }
  const Field fields[]{
    {.id = 1, .entries = MakeEntries(keys), .sample = false},
    {.id = 2, .entries = MakeEntries(keys)},
  };
  Write(fields);
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto& impl = Blocks(*Get(field));
    const size_t prefix = impl.KeyPrefix().size();
    irs::term_dict::Cursor cursor{impl};
    cursor.First();
    for (size_t i = 1; i != keys.size(); ++i) {
      SCOPED_TRACE(testing::Message("entry ") << i);
      ASSERT_FALSE(cursor.AtBlockEnd());
      ASSERT_EQ(prefix + irs::CommonPrefixLength(irs::bytes_view{keys[i - 1]},
                                                 irs::bytes_view{keys[i]}),
                cursor.Entries().Skip());
    }
  }
}

TEST_P(TermDictTest, keys_skipped_across_restarts_materialize) {
  const Field fields[]{
    {.id = 1, .entries = MakeEntries(Words(3000, 17)), .sample = false},
    {.id = 2, .entries = MakeEntries(Words(3000, 17))},
  };
  Write(fields);
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto& impl = Blocks(*Get(field));
    ASSERT_EQ(
      field.sample,
      BlockKinds(
        impl)[static_cast<size_t>(irs::term_dict::BlockKind::VarFsst)] > 1);
    const auto& entries = field.entries;
    for (const size_t stride : {size_t{1}, size_t{5}, size_t{23}}) {
      SCOPED_TRACE(testing::Message("stride ") << stride);
      irs::term_dict::Cursor cursor{impl};
      cursor.First();
      for (size_t i = 1; i != entries.size(); ++i) {
        SCOPED_TRACE(testing::Message("entry ") << i);
        if (cursor.AtBlockEnd()) {
          cursor.Load(cursor.BlockIndex() + 1);
          cursor.Entries().Skip();
        } else {
          ASSERT_EQ(
            impl.KeyPrefix().size() +
              irs::CommonPrefixLength(irs::bytes_view{entries[i - 1].key},
                                      irs::bytes_view{entries[i].key}),
            cursor.Entries().Skip());
        }
        if (i % stride == 0) {
          cursor.Entries().Materialize();
          ASSERT_EQ(irs::bytes_view{entries[i].key}, cursor.Term());
        }
      }
    }
  }
}

TEST_P(TermDictTest, many_blocks) {
  const Field fields[]{{.id = 2, .entries = MakeEntries(Words(60000, 7))}};
  Write(fields);
  const auto* reader = Get(2);
  ASSERT_NE(nullptr, reader);
  AssertField(*reader, fields[0].entries);
}

TEST_P(TermDictTest, seeks_find_every_term_in_any_order) {
  std::vector<irs::bstring> keys;
  for (size_t i = 0; i != 30000; ++i) {
    keys.emplace_back(Bytes(absl::StrCat("w", absl::Dec(i, absl::kZeroPad5))));
  }
  const Field fields[]{{.id = 2, .entries = MakeEntries(std::move(keys))}};
  Write(fields);
  const auto* reader = Get(2);
  ASSERT_NE(nullptr, reader);
  const auto& entries = fields[0].entries;
  std::vector<size_t> order(entries.size());
  std::iota(order.begin(), order.end(), size_t{0});
  std::shuffle(order.begin(), order.end(), std::mt19937{17});
  auto shared = reader->iterator();
  for (const size_t i : order) {
    SCOPED_TRACE(testing::Message("term ") << i);
    auto fresh = reader->iterator();
    for (auto* it : {fresh.get(), shared.get()}) {
      ASSERT_TRUE(it->seek(entries[i].key));
      ASSERT_EQ(irs::bytes_view{entries[i].key}, it->value());
      ASSERT_EQ(entries[i].docs.size(), it->cookie().docs_count);
    }
  }
}

TEST_P(TermDictTest, fields_small_empty_and_large) {
  const Field fields[]{
    {.id = 1, .entries = MakeEntries({Bytes("x"), Bytes("y"), Bytes("z")})},
    {.id = 4, .entries = {}},
    {.id = 5, .entries = MakeEntries(Words(20000, 11))},
    {.id = 9, .entries = MakeEntries({Bytes("only")})},
  };
  Write(fields);
  ASSERT_EQ(nullptr, Get(4));
  ASSERT_EQ(nullptr, Get(7));
  for (const auto& field : fields) {
    if (field.entries.empty()) {
      continue;
    }
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    AssertField(*reader, field.entries);
  }
}

TEST_P(TermDictTest, small_fields_are_copied_at_open) {
  const Field fields[]{
    {.id = 1, .entries = MakeEntries({Bytes("x"), Bytes("y"), Bytes("z")})},
    {.id = 2, .entries = MakeEntries(Words(20000, 5))},
    {.id = 3, .entries = MakeEntries({Bytes("false"), Bytes("true")})},
  };
  Write(fields);
  if (!_reader->Store().Base()) {
    GTEST_SKIP() << "only a mapped input reads blocks in place";
  }
  const auto* begin = _reader->Store().Base();
  const auto* end = begin + _reader->Store().Input().GetSize();
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto& impl = Blocks(*Get(field));
    ASSERT_EQ(field.id != 2, impl.Small());
    irs::term_dict::PinnedRange pinned;
    const auto* block = impl.Block(0, pinned).data();
    ASSERT_EQ(impl.Small(), block < begin || end <= block);
    AssertField(impl, field.entries);
  }
}

TEST_P(TermDictTest, open_takes_the_terms_from_the_footer) {
  std::vector<std::pair<int64_t, irs::doc_id_t>> values;
  for (irs::doc_id_t doc = 1; doc <= 50000; ++doc) {
    values.emplace_back(int64_t{doc} * 7, doc);
  }
  auto keys = Words(20000, 13);
  keys.emplace_back(irs::bstring(9000, irs::byte_type{'q'}));
  const Field fields[]{
    {.id = 1, .entries = MakeEntries(std::move(keys))},
    {.id = 2,
     .entries = NumericEntries(std::move(values)),
     .dict = irs::NumericLayout::kNumbers},
    {.id = 3, .entries = MakeEntries({Bytes("false"), Bytes("true")})},
    {.id = 4, .entries = MakeEntries({Bytes("only")})},
  };
  Write(fields);
  const uint64_t blocks_end = TakeTerms().blocks_end;
  Open(dir());
  ASSERT_TRUE(_idx->TakeTerms().fields.empty());
  const auto& ranged = Blocks(*Get(fields[1]));
  ASSERT_FALSE(ranged.RangeLevels().empty());
  const auto& owner = ranged.Store();
  ASSERT_LT(owner.BlockOffset(owner.BlockCount() - 1), blocks_end);
  ASSERT_EQ(blocks_end, owner.BlockOffset(owner.BlockCount()));
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    AssertField(*Get(field), field.entries);
  }
}

TEST_P(TermDictTest, a_cold_open_loads_the_whole_dictionary) {
  std::vector<irs::bstring> keys;
  for (size_t i = 0; i != 300000; ++i) {
    keys.push_back(Bytes(absl::StrCat("key-", i * 7919 % 1000003)));
  }
  const Field fields[]{
    {.id = 1, .entries = MakeEntries({Bytes("x"), Bytes("y"), Bytes("z")})},
    {.id = 2, .entries = MakeEntries(keys)},
  };
  Write(fields);
  if (_reader->Store().Mapped()) {
    GTEST_SKIP() << "a mapped input reads its dictionary in place";
  }
  auto& cache = duckdb::ExternalFileCache::Get(dir().Db());
  cache.SetEnabled(false);
  cache.SetEnabled(true);
  Open(dir());
  const auto& store = _reader->Store();
  const auto begin = store.BlockOffset(0);
  const auto end = store.BlockOffset(store.BlockCount());
  ASSERT_LT(begin, end);
  EXPECT_TRUE(store.Input().IsResident(begin, end - begin));
  AssertField(Blocks(*Get(2)), fields[1].entries);
}

TEST_P(TermDictTest, open_readers_count_against_the_memory_limit) {
  const auto& buffers = duckdb::BufferManager::GetBufferManager(dir().Db());
  const auto used = [&](duckdb::MemoryTag tag) {
    return buffers.GetMemoryUsageInfo()[static_cast<size_t>(tag)].size;
  };
  const auto copies = used(duckdb::MemoryTag::ALLOCATOR);
  const auto decoded = used(duckdb::MemoryTag::EXTENSION);
  const Field fields[]{
    {.id = 1, .entries = MakeEntries({Bytes("x"), Bytes("y"), Bytes("z")})},
    {.id = 2, .entries = MakeEntries(Words(20000, 5))},
  };
  Write(fields);
  EXPECT_LT(copies, used(duckdb::MemoryTag::ALLOCATOR));
  EXPECT_LT(decoded, used(duckdb::MemoryTag::EXTENSION));
  AssertField(Blocks(*Get(1)), fields[0].entries);
  _reader.reset();
  EXPECT_EQ(copies, used(duckdb::MemoryTag::ALLOCATOR));
  EXPECT_EQ(decoded, used(duckdb::MemoryTag::EXTENSION));
}

TEST_P(TermDictTest, inline_postings_live_in_their_block) {
  std::vector<std::pair<int64_t, irs::doc_id_t>> values;
  for (irs::doc_id_t doc = 1; doc <= 40000; ++doc) {
    values.emplace_back(int64_t{doc % 5000} * 7, doc);
  }
  const Field fields[]{
    {.id = 1, .entries = MakeEntries({Bytes("x"), Bytes("y"), Bytes("z")})},
    {.id = 2, .entries = MakeEntries(Words(20000, 5))},
    {.id = 3, .entries = NumericEntries(std::move(values))},
    {.id = 4, .entries = MakeEntries({Bytes("only")})},
    {.id = 5, .entries = MakeEntries({Bytes("p"), Bytes("q")})},
  };
  Write(fields);

  size_t inlined = 0;
  for (const auto& field : fields) {
    if (Get(field)->size() == 1) {
      continue;
    }
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto& impl = Blocks(*Get(field));
    irs::term_dict::Cursor cursor{impl};
    cursor.First();
    do {
      const auto& meta = cursor.Meta();
      if (meta.inline_size == 0) {
        continue;
      }
      ++inlined;
      const uint32_t block = cursor.BlockIndex();
      irs::term_dict::PinnedRange pinned;
      const auto bytes = impl.Block(block, pinned);
      const auto inline_bytes = meta.Inline();
      ASSERT_NE(bytes.end(), absl::c_search(bytes, inline_bytes));
    } while (cursor.Advance());
  }
  ASSERT_GT(inlined, 100);
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    AssertField(*Get(field), field.entries);
  }
}

TEST_P(TermDictTest, corrupt_block_prefixes_are_refused) {
  std::vector<std::pair<int64_t, irs::doc_id_t>> values;
  for (irs::doc_id_t doc = 1; doc <= 20000; ++doc) {
    values.emplace_back(int64_t{doc} * 3, doc);
  }
  const Field fields[]{
    {.id = 1,
     .entries = NumericEntries(std::move(values)),
     .dict = irs::NumericLayout::kNumbers},
    {.id = 2, .entries = MakeEntries(Words(20000, 5))},
  };
  Write(fields);
  irs::byte_type prefix_label = 0;
  {
    const auto& index = Blocks(*Get(fields[1])).Index();
    for (uint32_t block = 0; block != index.Blocks() && prefix_label == 0;
         ++block) {
      const auto prefix =
        index.BlockPrefix(block).substr(index.KeyPrefix().size());
      if (!prefix.empty()) {
        prefix_label = prefix.front();
      }
    }
  }
  ASSERT_NE(0, prefix_label);
  const auto terms = TakeTerms();
  ASSERT_EQ(2, terms.fields.size());
  for (const auto& head : terms.fields) {
    ASSERT_LT(head.first_block, head.last_block);
  }
  const auto refused = [&](auto&& corrupt) {
    auto copy = terms;
    corrupt(copy);
    RewriteTerms(copy);
    ASSERT_SQL_ERROR(Open(dir()), ERRCODE_INDEX_CORRUPTED);
  };
  using Terms = irs::term_dict::TermsMeta;
  ASSERT_TRUE(terms.fields[0].block_prefixes.empty());
  ASSERT_FALSE(terms.fields[1].block_prefixes.empty());
  refused([](Terms& t) { t.fields[1].block_prefixes.clear(); });
  refused([](Terms& t) { t.fields[1].block_prefixes.pop_back(); });
  refused([](Terms& t) { t.fields[1].block_prefixes.push_back('\0'); });
  refused(
    [](Terms& t) { t.fields[0].block_prefixes = t.fields[1].block_prefixes; });
  refused([](Terms& t) {
    std::swap(t.fields[0].block_prefixes, t.fields[1].block_prefixes);
  });
  refused([&](Terms& t) {
    auto& trie = t.fields[1].block_prefixes;
    size_t at = 0;
    const auto varint = [&] {
      uint32_t value = 0;
      for (uint32_t shift = 0;; shift += 7) {
        const auto byte = static_cast<uint8_t>(trie[at++]);
        value |= uint32_t{byte & 0x7FU} << shift;
        if (byte < 0x80) {
          return value;
        }
      }
    };
    ASSERT_EQ(0, varint());
    varint();
    ++at;
    const auto labels = varint();
    ASSERT_NE('\xFF', trie[at + labels - 1]);
    size_t shifted = 0;
    for (size_t i = at; i != at + labels; ++i) {
      auto& label = reinterpret_cast<irs::byte_type&>(trie[i]);
      if (label >= prefix_label) {
        ++label;
        ++shifted;
      }
    }
    ASSERT_NE(0, shifted);
  });

  RewriteTerms(terms);
  Open(dir());
  AssertField(*Get(fields[0]), fields[0].entries);
  AssertField(*Get(fields[1]), fields[1].entries);
}

std::vector<irs::bstring> Paths(size_t count, uint32_t seed) {
  std::mt19937 rng{seed};
  const auto words = Words(256, seed);
  std::vector<irs::bstring> paths;
  paths.reserve(count);
  for (size_t i = 0; i != count; ++i) {
    std::string path = absl::StrCat("https://", rng() % 64, ".example/");
    const size_t size = path.size() + 300 + rng() % 400;
    while (path.size() < size) {
      absl::StrAppend(
        &path,
        irs::ViewCast<char>(irs::bytes_view{words[rng() % words.size()]}), "/");
    }
    paths.emplace_back(Bytes(path));
  }
  return paths;
}

TEST_P(TermDictTest, headers_take_the_smaller_layout_per_block) {
  const auto words = Words(30000, 3);
  const auto paths = Paths(3000, 5);
  const Field fields[]{
    {.id = 1, .entries = MakeEntries(words), .sample = false},
    {.id = 2, .entries = MakeEntries(words)},
    {.id = 3, .entries = MakeEntries(paths), .sample = false},
    {.id = 4, .entries = MakeEntries(paths)},
  };
  Write(fields);
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto& impl = Blocks(*Get(field));
    ASSERT_EQ(
      field.sample,
      BlockKinds(
        impl)[static_cast<size_t>(irs::term_dict::BlockKind::VarFsst)] > 1);
    irs::term_dict::Cursor cursor{impl};
    cursor.First();
    uint64_t entries = 0;
    uint64_t escaped = 0;
    uint64_t packed = 0;
    uint64_t columns = 0;
    uint32_t block = irs::term_dict::kNoBlock;
    uint32_t last = irs::term_dict::kNoEntry;
    irs::bstring previous;
    do {
      const auto& view = cursor.Entries().View();
      ASSERT_FALSE(view.fixed);
      if (cursor.BlockIndex() != block) {
        block = cursor.BlockIndex();
        if (view.headers.codes) {
          ++packed;
          entries += view.count;
          escaped += view.headers.escaped;
        } else {
          ++columns;
        }
        last = irs::term_dict::kNoEntry;
      }
      const auto entry = cursor.Entries().Entry();
      const auto key = cursor.Entries().Key();
      const auto header = irs::term_dict::HeaderAt(view, entry);
      if (entry == 0) {
        ASSERT_EQ(view.headers.codes ? view.headers.lcp_base : 0, header.lcp);
      } else if (last + 1 == entry) {
        ASSERT_EQ(irs::CommonPrefixLength(irs::bytes_view{previous}, key),
                  header.lcp);
      }
      if (entry % irs::term_dict::kRestartInterval == 0) {
        ASSERT_EQ(key.size() - view.prefix, header.len);
      } else if (!view.coded) {
        ASSERT_EQ(key.size() - header.lcp, header.len);
      }
      previous.assign(key);
      last = entry;
    } while (cursor.Advance());
    if (field.id <= 2) {
      ASSERT_NE(0, packed);
      ASSERT_LE(escaped * 16, entries);
    } else {
      ASSERT_NE(0, columns);
    }
  }
}

TEST(TermDictBlockTest, block_headers_decode_and_refuse_corruption) {
  const std::vector<irs::byte_type> packed{
    0, 2, 1, 1, 2,    0,    0, 5, 3, 1,    0,    0,
    0, 0, 0, 0, 0xFF, 0x24, 0, 0, 0, 0x2C, 0x01,
  };
  const std::vector<irs::byte_type> columns{
    0, 2, 1, 1,    2,    0, 0, irs::term_dict::kColumnHeaders, 0, 0, 0, 0, 0,
    0, 0, 4, 0x2C, 0x01, 4, 0,
  };
  for (const auto* block : {&packed, &columns}) {
    auto padded = *block;
    padded.resize(block->size() + irs::term_dict::kSlack);
    const auto view =
      irs::term_dict::Parse({padded.data(), padded.size()}, nullptr);
    ASSERT_EQ(2, view.count);
    ASSERT_EQ(block == &packed, static_cast<bool>(view.headers.codes));
    ASSERT_EQ(block == &packed ? 1 : 0, view.headers.escaped);
    const auto first = irs::term_dict::HeaderAt(view, 0);
    ASSERT_EQ(0, first.lcp);
    ASSERT_EQ(300, first.len);
    const auto second = irs::term_dict::HeaderAt(view, 1);
    ASSERT_EQ(4, second.lcp);
    ASSERT_EQ(4, second.len);
    ASSERT_EQ(padded.data() + block->size(), view.suffixes);
    for (size_t size = 0; size != padded.size(); ++size) {
      SCOPED_TRACE(testing::Message("truncated to ") << size);
      ASSERT_SQL_ERROR(irs::term_dict::Parse({padded.data(), size}, nullptr),
                       ERRCODE_INDEX_CORRUPTED);
    }
  }

  const auto refused = [&](size_t at, irs::byte_type value) {
    auto corrupt = packed;
    corrupt[at] = value;
    corrupt.resize(corrupt.size() + irs::term_dict::kSlack);
    ASSERT_SQL_ERROR(
      irs::term_dict::Parse({corrupt.data(), corrupt.size()}, nullptr),
      ERRCODE_INDEX_CORRUPTED);
  };
  refused(0, 7);
  refused(0, static_cast<irs::byte_type>(irs::term_dict::BlockKind::VarFsst));
  refused(2, 2);
  refused(3, 3);
  refused(4, 3);
  refused(7, irs::term_dict::kColumnHeaders + 1);
  refused(9, 3);
}

TEST(TermDictBlockTest, fixed_keys_decode_and_refuse_corruption) {
  const std::vector<irs::byte_type> block{
    1, 2, 1, 0, 0, 3, 0x10, 0, 0, 0x11, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0,    0, 0, 0,    0, 0, 0, 0, 0, 0, 0,
  };
  const auto view =
    irs::term_dict::Parse({block.data(), block.size()}, nullptr);
  ASSERT_TRUE(view.fixed);
  ASSERT_EQ(2, view.count);
  ASSERT_EQ(1, view.stride);
  ASSERT_EQ(block.data() + 17, view.records);
  irs::term_dict::StrideBuffer buffer;
  ASSERT_EQ(irs::bytes_view{Bytes("\x10")},
            irs::term_dict::FixedKey(view, 0, 0, buffer));
  ASSERT_EQ(irs::bytes_view{Bytes("\x13")},
            irs::term_dict::FixedKey(view, 1, 4, buffer));
  struct Bound {
    irs::byte_type value;
    uint32_t entry;
    uint32_t at;
  };
  const Bound bounds[]{
    {0x00, 0, 0}, {0x10, 0, 0}, {0x11, 1, 4},
    {0x13, 1, 4}, {0x14, 2, 0}, {0xFF, 2, 0},
  };
  for (const auto [value, entry, at] : bounds) {
    SCOPED_TRACE(testing::Message("probe ") << uint32_t{value});
    irs::term_dict::PaddedKey key;
    const auto bound = irs::term_dict::FixedLowerBound(
      view, key.Assign(irs::bytes_view{&value, 1}));
    ASSERT_EQ(entry, bound.entry);
    ASSERT_EQ(at, bound.at);
  }

  std::vector<irs::byte_type> wide{1, 2, 8, 0, 0, 1};
  wide.resize(wide.size() + 8 + irs::term_dict::kEfSampleBytes);
  wide.push_back(0x05);
  wide.resize(wide.size() + 7 + irs::term_dict::kSlack);
  const auto wide_view =
    irs::term_dict::Parse({wide.data(), wide.size()}, nullptr);
  ASSERT_EQ(8, wide_view.stride);
  for (const auto& probe :
       {irs::bstring(8, irs::byte_type{0xFF}),
        irs::bstring(8, irs::byte_type{0xFF}) + irs::byte_type{0}}) {
    irs::term_dict::PaddedKey key;
    ASSERT_EQ(
      2, irs::term_dict::FixedLowerBound(wide_view, key.Assign(probe)).entry);
  }

  const size_t whole =
    static_cast<size_t>(view.records - block.data()) + irs::term_dict::kSlack;
  for (size_t size = 0; size != whole; ++size) {
    SCOPED_TRACE(testing::Message("truncated to ") << size);
    ASSERT_SQL_ERROR(irs::term_dict::Parse({block.data(), size}, nullptr),
                     ERRCODE_INDEX_CORRUPTED);
  }

  const auto refused = [&](size_t at, irs::byte_type value) {
    auto corrupt = block;
    corrupt[at] = value;
    ASSERT_SQL_ERROR(
      irs::term_dict::Parse({corrupt.data(), corrupt.size()}, nullptr),
      ERRCODE_INDEX_CORRUPTED);
  };
  refused(1, 0);
  refused(4, 8);
  refused(5, 0x80);
}

struct RecordShape {
  irs::IndexFeatures features;
  uint32_t count;
  uint32_t singles;
  uint32_t inlines;
  uint32_t big;
  uint32_t excess;
  uint32_t many_positions;
};

struct EncodedRecord {
  irs::PostingMeta meta;
  irs::bstring inline_bytes;
};

std::vector<EncodedRecord> MakeRecords(const RecordShape& shape,
                                       std::mt19937_64& rng) {
  const irs::PostingFeatures features{shape.features};
  const auto percent = [&](uint32_t p) { return rng() % 100 < p; };
  const auto below = [&](uint64_t bound) { return rng() % bound; };
  std::vector<EncodedRecord> records;
  uint64_t doc_start = below(uint64_t{1} << 40);
  uint64_t pos_start = below(uint64_t{1} << 40);
  uint64_t pay_start = below(uint64_t{1} << 40);
  uint64_t vec_start = below(uint64_t{1} << 40);
  uint64_t at = below(irs::PosGroup::kPositions);
  for (uint32_t i = 0; i != shape.count; ++i) {
    irs::PostingMeta meta;
    irs::bstring inline_bytes;
    if (percent(shape.singles)) {
      meta.docs_count = 1;
      meta.doc_delta = static_cast<uint32_t>(below(uint64_t{1} << 31));
    } else if (percent(shape.inlines)) {
      meta.docs_count = 2 + static_cast<uint32_t>(below(127));
      meta.inline_size =
        static_cast<uint8_t>(1 + below(irs::PostingMeta::kInlineBytes));
      for (uint32_t b = 0; b != meta.inline_size; ++b) {
        inline_bytes.push_back(static_cast<irs::byte_type>(below(256)));
      }
    } else {
      const bool big = percent(shape.big);
      meta.docs_count = big ? irs::doc_limits::kBlockSize + 1 +
                                static_cast<uint32_t>(below(1 << 20))
                            : 2 + static_cast<uint32_t>(below(127));
      doc_start += 1 + below(1 << 20);
      meta.doc_start = doc_start;
      if (big) {
        meta.doc_delta = 1 + static_cast<uint32_t>(below(uint64_t{1} << 31));
      }
    }
    if (features.freq) {
      meta.freq = meta.docs_count;
      if (percent(shape.excess)) {
        meta.freq += 1 + static_cast<uint32_t>(below(1000));
      }
      if (features.pos && percent(shape.many_positions)) {
        meta.freq +=
          irs::pos_limits::kBlockSize + 1 +
          static_cast<uint32_t>(below(3 * irs::PosGroup::kPositions));
      }
    }
    if (features.pos) {
      meta.pos_offset = static_cast<uint16_t>(at % irs::PosGroup::kPositions);
      if (i != 0 &&
          records.back().meta.pos_offset + uint64_t{records.back().meta.freq} >=
            irs::PosGroup::kPositions) {
        pos_start += 1 + below(1 << 16);
        pay_start += 1 + below(1 << 16);
      }
      meta.pos_start = pos_start;
      if (features.offs) {
        meta.pay_start = pay_start;
      }
      if (meta.freq > irs::pos_limits::kBlockSize) {
        meta.pos_extent = static_cast<uint32_t>(below(uint64_t{1} << 31));
        if (features.offs) {
          meta.pay_extent = static_cast<uint32_t>(below(uint64_t{1} << 31));
        }
      }
      at = at % irs::PosGroup::kPositions + meta.freq;
    } else if (features.vec) {
      vec_start += below(1 << 12);
      meta.pay_start = vec_start;
      meta.pos_offset = static_cast<uint16_t>(below(64));
    }
    records.push_back({.meta = meta, .inline_bytes = std::move(inline_bytes)});
  }
  for (auto& record : records) {
    record.meta.SetInline(record.inline_bytes);
  }
  return records;
}

void ExpectRecord(const EncodedRecord& expected,
                  const irs::term_dict::RecordDecoder& decoder,
                  const irs::PostingMeta& actual) {
  auto meta = expected.meta;
  ASSERT_EQ(meta.docs_count, actual.docs_count);
  ASSERT_EQ(meta.freq, actual.freq);
  ASSERT_EQ(meta.doc_delta, actual.doc_delta);
  ASSERT_EQ(meta.doc_start, actual.doc_start);
  ASSERT_EQ(meta.inline_size, actual.inline_size);
  ASSERT_EQ(meta.pos_start, actual.pos_start);
  ASSERT_EQ(meta.pay_start, actual.pay_start);
  ASSERT_EQ(meta.pos_offset, actual.pos_offset);
  ASSERT_EQ(meta.pos_extent, actual.pos_extent);
  ASSERT_EQ(meta.pay_extent, actual.pay_extent);
  if (meta.inline_size != 0) {
    ASSERT_EQ(irs::bytes_view{expected.inline_bytes},
              (irs::bytes_view{decoder.InlineData(), meta.inline_size}));
  }
}

TEST(TermDictRecordsTest, runs_decode_every_record_in_any_order) {
  constexpr auto kDocs = irs::IndexFeatures::None;
  constexpr auto kFreq = irs::IndexFeatures::Freq;
  constexpr auto kPos = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
  constexpr auto kOffs = kPos | irs::IndexFeatures::Offs;
  constexpr auto kVec = irs::IndexFeatures::Vec;
  const RecordShape shapes[]{
    {.features = kDocs, .count = 1, .singles = 100},
    {.features = kDocs, .count = 1, .singles = 0, .inlines = 100},
    {.features = kDocs, .count = 1, .singles = 0, .big = 100},
    {.features = kDocs, .count = 300, .singles = 100},
    {.features = kDocs, .count = 300, .singles = 0, .inlines = 100},
    {.features = kDocs, .count = 300, .singles = 0, .big = 100},
    {.features = kDocs, .count = 300, .singles = 0, .big = 0},
    {.features = kDocs, .count = 300, .singles = 30, .inlines = 50, .big = 30},
    {.features = kFreq,
     .count = 257,
     .singles = 60,
     .inlines = 50,
     .big = 10,
     .excess = 5},
    {.features = kFreq,
     .count = 64,
     .singles = 0,
     .inlines = 0,
     .big = 100,
     .excess = 100},
    {.features = kPos, .count = 1, .singles = 100},
    {.features = kPos,
     .count = 500,
     .singles = 60,
     .inlines = 60,
     .big = 5,
     .excess = 2,
     .many_positions = 1},
    {.features = kPos,
     .count = 200,
     .singles = 20,
     .inlines = 30,
     .big = 20,
     .excess = 60,
     .many_positions = 20},
    {.features = kPos, .count = 4000, .singles = 90},
    {.features = kOffs,
     .count = 333,
     .singles = 50,
     .inlines = 50,
     .big = 10,
     .excess = 10,
     .many_positions = 5},
    {.features = kVec, .count = 150, .singles = 40, .inlines = 40, .big = 10},
    {.features = kVec | kFreq,
     .count = 70,
     .singles = 40,
     .inlines = 40,
     .excess = 30},
  };
  std::mt19937_64 rng{42};
  for (const auto& shape : shapes) {
    SCOPED_TRACE(absl::StrCat("features ", static_cast<int>(shape.features),
                              " count ", shape.count));
    const auto records = MakeRecords(shape, rng);
    irs::term_dict::RecordRun run;
    run.Start(shape.features);
    for (const auto& record : records) {
      run.Commit(record.meta, run.Stage(record.meta));
    }
    duckdb::MemoryStream out;
    run.Write(out);
    ASSERT_EQ(run.Bytes(), out.GetPosition());
    irs::bstring bytes{out.GetData(), out.GetPosition()};
    bytes.resize(bytes.size() + irs::term_dict::kSlack);

    irs::term_dict::RecordDecoder decoder{shape.features};
    decoder.Reset(bytes.data());
    irs::PostingMeta meta;
    for (uint32_t i = 0; i != records.size(); ++i) {
      if (i == 0) {
        decoder.Seek(0, meta);
      } else {
        decoder.Next(meta);
      }
      ASSERT_NO_FATAL_FAILURE(ExpectRecord(records[i], decoder, meta));
    }
    std::vector<uint32_t> order(records.size());
    absl::c_iota(order, 0);
    absl::c_shuffle(order, rng);
    for (const auto i : order) {
      decoder.Seek(i, meta);
      ASSERT_NO_FATAL_FAILURE(ExpectRecord(records[i], decoder, meta));
      if (i + 1 != records.size()) {
        decoder.Next(meta);
        ASSERT_NO_FATAL_FAILURE(ExpectRecord(records[i + 1], decoder, meta));
      }
    }
    ASSERT_SQL_ERROR(decoder.Seek(static_cast<uint32_t>(records.size()), meta),
                     ERRCODE_INDEX_CORRUPTED);
    decoder.Seek(static_cast<uint32_t>(records.size() - 1), meta);
    ASSERT_SQL_ERROR(decoder.Next(meta), ERRCODE_INDEX_CORRUPTED);
  }
}

TEST(TermDictRecordsTest, runs_refuse_corruption) {
  const irs::PostingFeatures features{irs::IndexFeatures::Freq};
  irs::term_dict::RecordRun run;
  run.Start(irs::IndexFeatures::Freq);
  for (uint32_t i = 0; i != 3; ++i) {
    irs::PostingMeta meta;
    meta.docs_count = i == 1 ? 5 : 1;
    meta.freq = meta.docs_count;
    meta.doc_start = 100;
    meta.doc_delta = i;
    run.Commit(meta, run.Stage(meta));
  }
  duckdb::MemoryStream out;
  run.Write(out);
  irs::bstring bytes{out.GetData(), out.GetPosition()};
  bytes.resize(bytes.size() + irs::term_dict::kSlack);
  ASSERT_EQ(3, irs::term_dict::ParseRun(bytes.data(), features).count);
  const auto refused = [&](size_t at, irs::byte_type value) {
    auto corrupt = bytes;
    corrupt[at] = value;
    ASSERT_SQL_ERROR(irs::term_dict::ParseRun(corrupt.data(), features),
                     ERRCODE_INDEX_CORRUPTED);
  };
  refused(0, 0);
  refused(1, 0);
  refused(1, irs::term_dict::kRunInline);
  refused(1, irs::term_dict::kRunSingles | irs::term_dict::kRunMultis);
  refused(1, 0x10 | bytes[1]);
  refused(3, irs::term_dict::kMaxRecordBits + 1);
  const auto view = irs::term_dict::ParseRun(bytes.data(), features);
  ASSERT_TRUE(view.multi_bits);
  refused(static_cast<size_t>(view.multi_bits - bytes.data()), 0);
}

TEST(TermDictRecordsTest, runs_refuse_records_their_columns_lack) {
  const auto encode = [](irs::IndexFeatures index,
                         std::span<irs::PostingMeta> metas) {
    irs::term_dict::RecordRun run;
    run.Start(index);
    for (auto& meta : metas) {
      run.Commit(meta, run.Stage(meta));
    }
    duckdb::MemoryStream out;
    run.Write(out);
    irs::bstring bytes{out.GetData(), out.GetPosition()};
    bytes.resize(bytes.size() + irs::term_dict::kSlack);
    return bytes;
  };
  const auto offset = [](const irs::bstring& bytes, const irs::byte_type* at) {
    return static_cast<size_t>(at - bytes.data());
  };

  {
    const irs::PostingFeatures features{irs::IndexFeatures::None};
    const irs::bstring data(3, irs::byte_type{9});
    irs::PostingMeta metas[2];
    metas[0].docs_count = 3;
    metas[0].inline_size = static_cast<uint8_t>(data.size());
    metas[0].SetInline(data);
    metas[1].docs_count = 4;
    metas[1].doc_start = 100;
    auto bytes = encode(irs::IndexFeatures::None, metas);
    const auto view = irs::term_dict::ParseRun(bytes.data(), features);
    ASSERT_TRUE(view.inline_bits);
    bytes[offset(bytes, view.inline_bits)] = 0;
    ASSERT_SQL_ERROR(irs::term_dict::ParseRun(bytes.data(), features),
                     ERRCODE_INDEX_CORRUPTED);
  }

  {
    const irs::PostingFeatures features{irs::IndexFeatures::None};
    irs::PostingMeta metas[2];
    metas[0].docs_count = 5;
    metas[0].doc_start = 100;
    metas[1].docs_count = irs::doc_limits::kBlockSize + 100;
    metas[1].doc_start = 200;
    metas[1].doc_delta = 77;
    const auto bytes = encode(irs::IndexFeatures::None, metas);
    const auto view = irs::term_dict::ParseRun(bytes.data(), features);
    ASSERT_EQ(1, view.big.count);
    ASSERT_NE(0, view.big.keys.bits);
    const size_t count = offset(bytes, view.docs.data) - 3;
    ASSERT_EQ(1, bytes[count]);
    auto wide = bytes;
    wide[count] = 5;
    ASSERT_SQL_ERROR(irs::term_dict::ParseRun(wide.data(), features),
                     ERRCODE_INDEX_CORRUPTED);
    irs::term_dict::RecordDecoder decoder{irs::IndexFeatures::None};
    irs::PostingMeta meta;
    decoder.Reset(bytes.data());
    decoder.Seek(1, meta);
    ASSERT_EQ(77, meta.doc_delta);
    auto lost = bytes;
    lost[offset(bytes, view.big.keys.data)] = 0;
    decoder.Reset(lost.data());
    ASSERT_SQL_ERROR(decoder.Seek(1, meta), ERRCODE_INDEX_CORRUPTED);
  }

  {
    constexpr auto kIndex = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
    const irs::PostingFeatures features{kIndex};
    irs::PostingMeta metas[2];
    metas[0].docs_count = 2;
    metas[0].freq = 2;
    metas[0].doc_start = 100;
    metas[0].pos_offset = 200;
    metas[0].pos_start = 1000;
    metas[1].docs_count = 2;
    metas[1].freq = irs::pos_limits::kBlockSize + 100;
    metas[1].doc_start = 200;
    metas[1].pos_offset = 202;
    metas[1].pos_start = 1000;
    metas[1].pos_extent = 77;
    const auto bytes = encode(kIndex, metas);
    const auto view = irs::term_dict::ParseRun(bytes.data(), features);
    ASSERT_EQ(200, view.pos_offset);
    ASSERT_EQ(1, view.extents.count);
    ASSERT_NE(0, view.extents.keys.bits);
    const irs::byte_type pos_offset[]{0xC8, 0x01};
    const auto header = bytes.substr(0, offset(bytes, view.docs.data));
    const auto at = header.find(irs::bytes_view{pos_offset, 2});
    ASSERT_NE(irs::bstring::npos, at);
    auto far = bytes;
    far[at + 1] = 0x7F;
    ASSERT_SQL_ERROR(irs::term_dict::ParseRun(far.data(), features),
                     ERRCODE_INDEX_CORRUPTED);
    auto lost = bytes;
    irs::term_dict::RecordDecoder decoder{kIndex};
    irs::PostingMeta meta;
    decoder.Reset(bytes.data());
    decoder.Seek(1, meta);
    ASSERT_EQ(77, meta.pos_extent);
    lost[offset(bytes, view.extents.keys.data)] = 0;
    decoder.Reset(lost.data());
    ASSERT_SQL_ERROR(decoder.Seek(1, meta), ERRCODE_INDEX_CORRUPTED);
  }
}

TEST(TermDictRecordsTest, runs_refuse_inline_sizes_past_the_limit) {
  const irs::PostingFeatures features{irs::IndexFeatures::None};
  irs::term_dict::RecordRun run;
  run.Start(irs::IndexFeatures::None);
  const irs::bstring data(irs::PostingMeta::kInlineBytes, irs::byte_type{7});
  irs::PostingMeta meta;
  meta.docs_count = 2;
  meta.inline_size = static_cast<uint8_t>(data.size());
  meta.SetInline(data);
  run.Commit(meta, run.Stage(meta));
  duckdb::MemoryStream out;
  run.Write(out);
  irs::bstring bytes{out.GetData(), out.GetPosition()};
  bytes.resize(bytes.size() + irs::term_dict::kSlack);
  ASSERT_EQ(irs::term_dict::kRunMultis | irs::term_dict::kRunInline, bytes[1]);
  ASSERT_EQ(1, irs::term_dict::ParseRun(bytes.data(), features).count);
  constexpr auto kSizeBits = 3;
  ASSERT_EQ(std::bit_width(irs::PostingMeta::kInlineBytes - 1),
            bytes[kSizeBits]);
  bytes[kSizeBits] = static_cast<irs::byte_type>(
    std::bit_width(irs::PostingMeta::kInlineBytes - 1) + 1);
  ASSERT_SQL_ERROR(irs::term_dict::ParseRun(bytes.data(), features),
                   ERRCODE_INDEX_CORRUPTED);
}

TEST_P(TermDictTest, footer_refuses_corrupt_fields) {
  std::vector<std::pair<int64_t, irs::doc_id_t>> values;
  for (irs::doc_id_t doc = 1; doc <= 5000; ++doc) {
    values.emplace_back(int64_t{doc} * 3, doc);
  }
  const Field fields[]{
    {.id = 1,
     .entries = NumericEntries(std::move(values)),
     .dict = irs::NumericLayout::kNumbers},
    {.id = 2, .entries = MakeEntries(Words(3000, 5))},
    {.id = 3, .entries = MakeEntries({Bytes("single")})},
  };
  Write(fields);
  const auto terms = TakeTerms();
  ASSERT_EQ(3, terms.fields.size());
  ASSERT_EQ(irs::DictLayout::Numeric, terms.fields[0].layout);
  ASSERT_EQ(irs::NumericLayout::kNumbers.member, terms.fields[0].member);
  ASSERT_LT(1, terms.fields[1].term_count);
  ASSERT_EQ(1, terms.fields[2].term_count);

  const auto refused = [&](auto&& corrupt, int code) {
    auto copy = terms;
    corrupt(copy);
    RewriteTerms(copy);
    ASSERT_SQL_ERROR(Open(dir()), code);
  };
  using Terms = irs::term_dict::TermsMeta;
  refused([](Terms& t) { std::swap(t.fields[0].id, t.fields[1].id); },
          ERRCODE_INDEX_CORRUPTED);
  refused(
    [](Terms& t) {
      t.fields[1].features = static_cast<irs::IndexFeatures>(0x80);
    },
    ERRCODE_FEATURE_NOT_SUPPORTED);
  refused([](Terms& t) { t.fields[1].term_count = 0; },
          ERRCODE_INDEX_CORRUPTED);
  refused([](Terms& t) { t.fields[2].record.clear(); },
          ERRCODE_INDEX_CORRUPTED);
  refused([](Terms& t) { t.ordinal_bytes = 0; }, ERRCODE_FEATURE_NOT_SUPPORTED);
  refused(
    [](Terms& t) { t.ordinal_bytes = irs::term_dict::kMaxOrdinalBytes + 1; },
    ERRCODE_FEATURE_NOT_SUPPORTED);
  refused(
    [](Terms& t) {
      const auto keyed = t.fields[1];
      t.fields.assign(irs::term_dict::OrdinalLimit(1) + 1, keyed);
      for (size_t i = 0; i != t.fields.size(); ++i) {
        t.fields[i].id = i + 1;
      }
      t.ordinal_bytes = 1;
    },
    ERRCODE_INDEX_CORRUPTED);
  refused([](Terms& t) { t.blocks_end = std::numeric_limits<uint64_t>::max(); },
          ERRCODE_INDEX_CORRUPTED);
  ASSERT_FALSE(terms.separators.empty());
  refused([](Terms& t) { t.separators.pop_back(); }, ERRCODE_INDEX_CORRUPTED);
  refused([](Terms& t) { t.separators.push_back('\x01'); },
          ERRCODE_INDEX_CORRUPTED);
  ASSERT_LT(2, terms.separators.size());
  refused([](Terms& t) { t.separators[2] = '\xFF'; }, ERRCODE_INDEX_CORRUPTED);
  refused([](Terms& t) { t.fields[1].fsst_table = "garbage"; },
          ERRCODE_INDEX_CORRUPTED);
  ASSERT_FALSE(terms.fields[1].block_prefixes.empty());
  refused([](Terms& t) { t.fields[1].block_prefixes.pop_back(); },
          ERRCODE_INDEX_CORRUPTED);
  refused([](Terms& t) { t.fields[1].block_prefixes.push_back('\0'); },
          ERRCODE_INDEX_CORRUPTED);
  refused([](Terms& t) { t.fields[1].max = t.fields[1].min; },
          ERRCODE_INDEX_CORRUPTED);
  refused([](Terms& t) { t.fields[1].max = t.fields[1].min + '\x01'; },
          ERRCODE_INDEX_CORRUPTED);
  refused([](Terms& t) { t.fields[1].last_block = t.blocks; },
          ERRCODE_INDEX_CORRUPTED);
  refused(
    [](Terms& t) {
      t.blocks = 0;
      t.spans.clear();
    },
    ERRCODE_INDEX_CORRUPTED);
  using Span = irs::term_dict::SpanningBlock;
  refused(
    [](Terms& t) {
      t.spans.assign(t.blocks + 1, Span{.block = 0, .extra = 1});
    },
    ERRCODE_INDEX_CORRUPTED);
  refused([](Terms& t) { t.spans = {Span{.block = 0, .extra = 0}}; },
          ERRCODE_INDEX_CORRUPTED);
  refused(
    [](Terms& t) {
      t.spans = {
        Span{.block = 0, .extra = std::numeric_limits<uint32_t>::max()}};
    },
    ERRCODE_INDEX_CORRUPTED);
  refused(
    [](Terms& t) { t.first_page = t.blocks_end / irs::term_dict::kPage + 1; },
    ERRCODE_INDEX_CORRUPTED);
  ASSERT_FALSE(terms.fields[0].levels.empty());
  {
    auto copy = terms;
    auto& levels = copy.fields[0].levels;
    while (levels.size() <= irs::term_dict::kMaxRangeLevels) {
      levels.push_back(levels.back());
    }
    duckdb::MemoryStream stream;
    duckdb::BinarySerializer serializer{stream};
    serializer.Begin();
    irs::term_dict::WriteTerms(serializer, copy);
    serializer.End();
    stream.Rewind();
    duckdb::BinaryDeserializer deserializer{stream};
    deserializer.Begin();
    ASSERT_SQL_ERROR(irs::term_dict::ReadTerms(deserializer, copy.blocks_end),
                     ERRCODE_INDEX_CORRUPTED);
  }

  const auto refused_field = [&](auto&& write_field,
                                 int code = ERRCODE_INDEX_CORRUPTED) {
    RewriteTermsWith([&](duckdb::BinarySerializer& object) {
      object.WriteProperty<uint32_t>(0, "restart_interval",
                                     irs::term_dict::kRestartInterval);
      object.WriteList(5, "fields", 1,
                       [&](duckdb::BinarySerializer::List& list,
                           duckdb::idx_t) { list.WriteObject(write_field); });
    });
    ASSERT_SQL_ERROR(Open(dir()), code);
  };
  const auto single_record = [](duckdb::BinarySerializer& dict) {
    dict.WriteProperty(5, "record", std::string{"r"});
  };
  const auto levels = [](duckdb::BinarySerializer& dict) {
    dict.WriteList(16, "levels", 1,
                   [](duckdb::BinarySerializer::List& list, duckdb::idx_t) {
                     list.WriteObject([](duckdb::BinarySerializer& level) {
                       level.WriteProperty(5, "record", std::string{"r"});
                     });
                   });
  };
  const auto text = [](duckdb::BinarySerializer& dict) {
    dict.WriteObject(13, "text", [](duckdb::BinarySerializer& object) {
      object.WriteProperty<uint64_t>(2, "norm", 7);
    });
  };
  const auto member = [](duckdb::BinarySerializer& field, duckdb::field_id_t id,
                         const char* tag, auto&& write_dict) {
    field.WriteObject(2, "dicts", [&](duckdb::BinarySerializer& dicts) {
      dicts.WriteObject(id, tag, write_dict);
    });
  };
  const auto numeric = [&](duckdb::BinarySerializer& field, auto&& write_dict) {
    field.WriteProperty<uint64_t>(0, "id", 1);
    field.WriteProperty<uint8_t>(
      1, "layout", static_cast<uint8_t>(irs::DictLayout::Numeric));
    member(field, 1, "numeric", write_dict);
  };
  refused_field([](duckdb::BinarySerializer& field) {
    field.WriteProperty<uint64_t>(0, "id", 1);
    field.WriteObject(2, "dicts", [](duckdb::BinarySerializer&) {});
  });
  refused_field(
    [](duckdb::BinarySerializer& field) {
      field.WriteProperty<uint64_t>(0, "id", 1);
    },
    ERRCODE_FEATURE_NOT_SUPPORTED);
  refused_field([&](duckdb::BinarySerializer& field) {
    field.WriteProperty<uint64_t>(0, "id", 1);
    member(field, 4, "sql_null", [](duckdb::BinarySerializer& dict) {
      dict.WriteProperty(0, "record", std::string{});
    });
  });
  refused_field(
    [&](duckdb::BinarySerializer& field) {
      field.WriteProperty<uint64_t>(0, "id", 1);
      member(field, 4, "sql_null", [](duckdb::BinarySerializer& dict) {
        dict.WriteProperty(0, "record", std::string{"r"});
        dict.WriteProperty<uint32_t>(2, "features", 1);
      });
    },
    ERRCODE_FEATURE_NOT_SUPPORTED);
  refused_field([&](duckdb::BinarySerializer& field) {
    field.WriteProperty<uint64_t>(0, "id", 1);
    member(field, 0, "string", [](duckdb::BinarySerializer& dict) {
      dict.WriteProperty<uint64_t>(1, "term_count", 0);
    });
  });
  refused_field([&](duckdb::BinarySerializer& field) {
    numeric(field, [&](duckdb::BinarySerializer& dict) {
      dict.WriteProperty<uint32_t>(
        0, "features", static_cast<uint32_t>(irs::IndexFeatures::Ranges));
      dict.WriteProperty<uint32_t>(3, "width", 8);
      single_record(dict);
      levels(dict);
    });
  });
  refused_field(
    [&](duckdb::BinarySerializer& field) {
      numeric(field, [&](duckdb::BinarySerializer& dict) {
        single_record(dict);
        text(dict);
      });
    },
    ERRCODE_FEATURE_NOT_SUPPORTED);
  refused_field([&](duckdb::BinarySerializer& field) {
    field.WriteProperty<uint64_t>(0, "id", 1);
    member(field, 0, "string", [&](duckdb::BinarySerializer& dict) {
      single_record(dict);
      levels(dict);
    });
  });
  refused_field([&](duckdb::BinarySerializer& field) {
    field.WriteProperty<uint64_t>(0, "id", 1);
    member(field, 0, "string", [&](duckdb::BinarySerializer& dict) {
      single_record(dict);
      text(dict);
    });
  });
  refused_field([&](duckdb::BinarySerializer& field) {
    field.WriteProperty<uint64_t>(0, "id", 1);
    member(field, 0, "string", [&](duckdb::BinarySerializer& dict) {
      single_record(dict);
      dict.WriteProperty(15, "fsst_table", std::string{"table"});
    });
  });
  refused_field([&](duckdb::BinarySerializer& field) {
    field.WriteProperty<uint64_t>(0, "id", 1);
    member(field, 0, "string", [&](duckdb::BinarySerializer& dict) {
      single_record(dict);
      dict.WriteProperty(18, "block_prefixes",
                         std::string{"\x00\x00\x01\x00\x00\x00", 6});
    });
  });
  refused_field(
    [&](duckdb::BinarySerializer& field) {
      field.WriteProperty<uint64_t>(0, "id", 1);
      field.WriteProperty<uint8_t>(1, "layout", irs::kDictLayouts);
      member(field, 0, "string", single_record);
    },
    ERRCODE_FEATURE_NOT_SUPPORTED);
  for (const uint32_t size : {0U, irs::PostingMeta::kInlineBytes + 1}) {
    refused_field([&](duckdb::BinarySerializer& field) {
      field.WriteProperty<uint64_t>(0, "id", 1);
      member(field, 0, "string", [&](duckdb::BinarySerializer& dict) {
        dict.WriteProperty(5, "record",
                           std::string{'\x0A', static_cast<char>(size)});
        dict.WriteProperty(6, "record_inline", std::string(size, 'x'));
      });
    });
  }

  RewriteTerms(terms);
  Open(dir());
  for (const auto& field : fields) {
    AssertField(*Get(field), field.entries);
  }
}

TEST_P(TermDictTest, json_layout_holds_six_dictionaries) {
  std::vector<std::pair<int64_t, irs::doc_id_t>> values;
  for (irs::doc_id_t doc = 1; doc <= 5000; ++doc) {
    values.emplace_back(int64_t{doc} * 3, doc);
  }
  const irs::bstring slot_term{irs::kSlotTerm};
  const Field fields[]{
    {.id = 1,
     .entries = MakeEntries(Words(3000, 5)),
     .dict = irs::JsonLayout::kStrings},
    {.id = 1,
     .entries = MakeEntries({slot_term}),
     .dict = irs::JsonLayout::kFalse},
    {.id = 1,
     .entries = MakeEntries({slot_term}),
     .dict = irs::JsonLayout::kTrue},
    {.id = 1,
     .entries = NumericEntries(std::move(values)),
     .dict = irs::JsonLayout::kNumbers},
    {.id = 1,
     .entries = MakeEntries({slot_term}),
     .dict = irs::JsonLayout::kNulls},
    {.id = 1,
     .entries = MakeEntries({slot_term}),
     .dict = irs::JsonLayout::kJsonNulls},
    {.id = 2, .entries = MakeEntries(Words(3000, 6))},
    {.id = 2,
     .entries = MakeEntries({slot_term}),
     .dict = irs::StringLayout::kNulls},
  };
  Write(fields);
  ASSERT_EQ((std::vector<irs::field_id>{1, 2}),
            std::vector<irs::field_id>(_reader->field_ids().begin(),
                                       _reader->field_ids().end()));
  ASSERT_EQ(irs::DictLayout::Json, LayoutOf(1));
  ASSERT_EQ(6, _reader->fields(1).size());
  ASSERT_EQ(irs::DictLayout::String, LayoutOf(2));
  irs::byte_type ordinal = 0;
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ")
                 << field.id << " member "
                 << static_cast<int>(field.dict.member));
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    ASSERT_EQ(field.id, reader->meta().id);
    ASSERT_EQ(field.dict.member == irs::DictMember::Strings,
              irs::get<irs::FreqAttr>(*reader) != nullptr);
    if (field.entries.size() > 1) {
      const auto& impl = Blocks(*reader);
      ASSERT_EQ(irs::bytes_view(&ordinal, 1), impl.KeyPrefix());
      ASSERT_EQ(IsNumbers(field.dict), !impl.RangeLevels().empty());
      for (const auto* level : impl.RangeLevels()) {
        ++ordinal;
        ASSERT_EQ(irs::bytes_view(&ordinal, 1), level->KeyPrefix());
      }
      ++ordinal;
    } else {
      ASSERT_EQ(1, reader->size());
    }
    AssertField(*reader, field.entries);
  }
  ASSERT_EQ(nullptr, Get(3));
  const irs::RegexpAcceptor any{
    irs::ViewCast<irs::byte_type>(std::string_view{".*"})};
  AssertWalk(*Get(1, irs::JsonLayout::kStrings), fields[0].entries, any);
  AssertWalk(*Get(1, irs::JsonLayout::kNumbers), fields[3].entries, any);
  AssertWalk(*Get(2), fields[6].entries, any);
}

TEST_P(TermDictTest, layouts_resolve_members) {
  const irs::bstring slot_term{irs::kSlotTerm};
  const Field fields[]{
    {.id = 1,
     .entries = MakeEntries({slot_term}),
     .dict = irs::StringListLayout::kNulls},
    {.id = 1,
     .entries = MakeEntries({slot_term}),
     .dict = irs::StringListLayout::kNullElements},
    {.id = 1,
     .entries = MakeEntries(Words(100, 5)),
     .dict = irs::StringListLayout::kStrings},
    {.id = 2,
     .entries = MakeEntries({slot_term}),
     .dict = irs::JsonLayout::kNulls},
    {.id = 2,
     .entries = MakeEntries({slot_term}),
     .dict = irs::JsonLayout::kJsonNulls},
    {.id = 2,
     .entries = MakeEntries({slot_term}),
     .dict = irs::JsonLayout::kFalse},
    {.id = 2,
     .entries = MakeEntries({slot_term}),
     .dict = irs::JsonLayout::kTrue},
    {.id = 2,
     .entries = NumericEntries({{int64_t{7}, irs::doc_id_t{1}}}),
     .dict = irs::JsonLayout::kNumbers},
    {.id = 2,
     .entries = MakeEntries(Words(100, 6)),
     .dict = irs::JsonLayout::kStrings},
    {.id = 3, .entries = MakeEntries(Words(100, 7))},
    {.id = 4,
     .entries = MakeEntries({slot_term}),
     .dict = irs::JsonListLayout::kNulls},
    {.id = 4,
     .entries = MakeEntries({slot_term}),
     .dict = irs::JsonListLayout::kNullElements},
    {.id = 4,
     .entries = MakeEntries({slot_term}),
     .dict = irs::JsonListLayout::kJsonNulls},
    {.id = 4,
     .entries = MakeEntries({slot_term}),
     .dict = irs::JsonListLayout::kFalse},
    {.id = 4,
     .entries = MakeEntries({slot_term}),
     .dict = irs::JsonListLayout::kTrue},
    {.id = 4,
     .entries = NumericEntries({{int64_t{9}, irs::doc_id_t{2}}}),
     .dict = irs::JsonListLayout::kNumbers},
    {.id = 4,
     .entries = MakeEntries(Words(100, 8)),
     .dict = irs::JsonListLayout::kStrings},
  };
  Write(fields);
  ASSERT_EQ(irs::DictLayout::StringList, LayoutOf(1));
  ASSERT_EQ(irs::DictLayout::Json, LayoutOf(2));
  ASSERT_EQ(irs::DictLayout::String, LayoutOf(3));
  ASSERT_EQ(irs::DictLayout::JsonList, LayoutOf(4));
  ASSERT_EQ(irs::kDictMembers, _reader->fields(4).size());
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ")
                 << field.id << " member "
                 << static_cast<int>(field.dict.member));
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    ASSERT_EQ(field.dict, reader->meta().dict);
  }
  ASSERT_EQ(nullptr, Get(3, irs::StringLayout::kNulls));
  ASSERT_EQ(nullptr, Get(3, irs::StringLayout::Of(irs::DictMember::True)));
  ASSERT_TRUE(_reader->fields(5).empty());
  AssertField(*Get(1, irs::StringListLayout::kNullElements), fields[1].entries);
  AssertField(*Get(4, irs::JsonListLayout::kNumbers), fields[15].entries);
  AssertField(*Get(4, irs::JsonListLayout::kStrings), fields[16].entries);
}

TEST_P(TermDictTest, ordinal_width_follows_the_bound) {
  for (const uint64_t keyed : {uint64_t{1}, uint64_t{256}, uint64_t{257},
                               uint64_t{65536}, uint64_t{65537}}) {
    SCOPED_TRACE(testing::Message("keyed ") << keyed);
    const uint32_t width = keyed <= 256 ? 1 : keyed <= 65536 ? 2 : 3;
    ASSERT_EQ(width, irs::term_dict::OrdinalBytes(keyed));
  }
  std::vector<Field> fields;
  for (irs::field_id id = 1; id <= 300; ++id) {
    fields.push_back({.id = id,
                      .entries = MakeEntries({Bytes(absl::StrCat("a", id)),
                                              Bytes(absl::StrCat("b", id))})});
  }
  Write(fields);
  ASSERT_EQ(2, TakeTerms().ordinal_bytes);
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto& impl = Blocks(*Get(field));
    const auto ordinal = static_cast<uint16_t>(field.id - 1);
    const irs::byte_type prefix[]{static_cast<irs::byte_type>(ordinal >> 8),
                                  static_cast<irs::byte_type>(ordinal)};
    ASSERT_EQ(irs::bytes_view(prefix, 2), impl.KeyPrefix());
    AssertField(*Get(field), field.entries);
  }
}

TEST_P(TermDictTest, ordinal_limit_refuses_more_keyed_dictionaries) {
  constexpr auto kLimit =
    irs::term_dict::OrdinalLimit(irs::term_dict::kMaxOrdinalBytes);
  ASSERT_EQ(irs::term_dict::kMaxOrdinalBytes,
            irs::term_dict::OrdinalBytes(kLimit));
  const irs::FlushState state{
    .tracker = dir().Tracker(kSegment),
    .doc_count = 10,
    .index_features = irs::IndexFeatures::None,
  };
  irs::IdxWriter idx{dir().Tracker(kSegment)};
  irs::term_dict::FieldWriter writer{false};
  writer.SetIdxWriter(idx);
  ASSERT_SQL_ERROR(writer.prepare(state, kLimit + 1),
                   ERRCODE_PROGRAM_LIMIT_EXCEEDED);
  writer.prepare(state, kLimit);
}

TEST_P(TermDictTest, three_byte_ordinals_past_two_byte_dictionaries) {
  constexpr irs::field_id kFillers = 0xFFFE;
  std::vector<Field> fields;
  fields.reserve(kFillers + 6);
  for (irs::field_id id = 1; id <= kFillers; ++id) {
    fields.push_back({.id = id,
                      .entries = MakeEntries({Bytes(absl::StrCat("a", id)),
                                              Bytes(absl::StrCat("b", id))})});
  }
  fields.push_back({.id = kFillers + 1, .entries = Ladder("az")});
  fields.push_back({.id = kFillers + 2, .entries = Ladder("az0000")});
  fields.push_back(
    {.id = kFillers + 3, .entries = MakeEntries({Bytes("xb1"), Bytes("xb2")})});
  fields.push_back({.id = kFillers + 4,
                    .entries = MakeEntries(Words(3000, 7)),
                    .dict = irs::JsonLayout::kStrings});
  fields.push_back({.id = kFillers + 4,
                    .entries = Multiples(20000),
                    .dict = irs::JsonLayout::kNumbers});
  fields.push_back(
    {.id = kFillers + 5,
     .entries = MakeEntries({Bytes("xb1000"), Bytes("xb2000")})});
  Write(fields);
  ASSERT_EQ(3, TakeTerms().ordinal_bytes);
  const auto& last_two_byte = Blocks(*Get(kFillers + 2));
  const auto& first_three_byte = Blocks(*Get(kFillers + 3));
  ASSERT_LT(1, last_two_byte.Blocks());
  ASSERT_NE(0, first_three_byte.FirstEntry());
  for (const size_t ordinal :
       {size_t{0}, size_t{0xFF}, size_t{0x100}, size_t{kFillers - 1}}) {
    AssertOrdinal(fields, ordinal, 3);
  }
  for (size_t ordinal = kFillers; ordinal != fields.size(); ++ordinal) {
    AssertOrdinal(fields, ordinal, 3);
  }
}

TEST_P(TermDictTest, four_byte_ordinals_follow_the_bound) {
  const Field fields[]{
    {.id = 1, .entries = Ladder("az")},
    {.id = 2, .entries = MakeEntries({Bytes("xb1"), Bytes("xb2")})},
    {.id = 3,
     .entries = MakeEntries(Words(3000, 7)),
     .dict = irs::JsonLayout::kStrings},
    {.id = 3, .entries = Multiples(20000), .dict = irs::JsonLayout::kNumbers},
  };
  Write(fields, false, irs::term_dict::OrdinalLimit(3) + 1);
  ASSERT_EQ(4, TakeTerms().ordinal_bytes);
  for (size_t ordinal = 0; ordinal != std::size(fields); ++ordinal) {
    AssertOrdinal(fields, ordinal, 4);
  }
}

TEST_P(TermDictTest, infix_walk_seeks_back_from_skipped_tail) {
  std::vector<irs::bstring> keys;
  for (size_t i = 700; i != 1000; ++i) {
    keys.emplace_back(Bytes(std::to_string(i)));
  }
  keys.emplace_back(Bytes("950stand"));
  const Field fields[]{{.id = 1, .entries = MakeEntries(std::move(keys))}};
  Write(fields);
  const auto* reader = Get(1);
  ASSERT_NE(nullptr, reader);
  ASSERT_TRUE(Blocks(*reader).Small());
  const irs::RegexpAcceptor acceptor{
    irs::ViewCast<irs::byte_type>(std::string_view{".*stand.*"})};
  auto it = reader->iterator(acceptor);
  size_t found = 0;
  while (it->next()) {
    ++found;
  }
  ASSERT_EQ(1, found);
  ASSERT_EQ(irs::SeekResult::NotFound, it->seek_ge(Bytes("950stan")));
  ASSERT_EQ(irs::bytes_view{Bytes("950stand")}, it->value());
}

TEST_P(TermDictTest, acceptor_walks) {
  auto keys = Words(30000, 3);
  for (uint64_t value = 0; value != 5000; ++value) {
    keys.emplace_back(BigEndian(0, value * 13));
  }
  const Field fields[]{{.id = 6, .entries = MakeEntries(std::move(keys))}};
  Write(fields);
  const auto* reader = Get(6);
  ASSERT_NE(nullptr, reader);

  for (const std::string_view pattern :
       {"inter.*", ".*ation", ".*stand.*", "un(der|re).*s", "[a-c].*ing[0-9]+",
        "nation", ".*", "zzz.*", "re.*con.*", ".*al[0-9]*"}) {
    SCOPED_TRACE(testing::Message("pattern ") << pattern);
    AssertWalk(*reader, fields[0].entries,
               irs::RegexpAcceptor{irs::ViewCast<irs::byte_type>(pattern)});
  }

  using Kind = irs::RegexpAcceptor::PartKind;
  using Part = irs::RegexpAcceptor::Part;
  const auto part = [](Kind kind, std::string_view pattern) {
    return Part{kind, irs::ViewCast<irs::byte_type>(pattern)};
  };
  const std::vector<std::vector<Part>> unions{
    {part(Kind::Prefix, "pre"), part(Kind::Wildcard, "%ion")},
    {part(Kind::Term, "nation"), part(Kind::Perl, ".*(ism|al)"),
     part(Kind::Prefix, "con")},
    {part(Kind::Wildcard, "%sent%"), part(Kind::Term, "un")},
  };
  for (const auto& parts : unions) {
    AssertWalk(*reader, fields[0].entries,
               irs::RegexpAcceptor{std::span{parts}});
  }
}

TEST_P(TermDictTest, walks_stay_inside_their_field) {
  const auto ladder = [](size_t size, std::string_view last) {
    std::vector<irs::bstring> keys;
    for (size_t i = 0; i != size; ++i) {
      const auto digits = std::to_string(i);
      keys.emplace_back(
        Bytes("ay" + std::string(4 - digits.size(), '0') + digits));
    }
    keys.emplace_back(Bytes(last));
    return MakeEntries(std::move(keys));
  };
  size_t sizes[]{3000, 3000};
  std::vector<Field> fields;
  const auto write = [&] {
    fields.clear();
    fields.push_back({.id = 1, .entries = ladder(sizes[0], "az")});
    fields.push_back(
      {.id = 2, .entries = MakeEntries({Bytes("xb1"), Bytes("xb2")})});
    fields.push_back({.id = 3, .entries = ladder(sizes[1], "az0000")});
    fields.push_back(
      {.id = 4, .entries = MakeEntries({Bytes("xb1000"), Bytes("xb2000")})});
    Write(fields);
  };
  for (size_t i = 0; i != std::size(sizes); ++i) {
    write();
    const auto& entries = fields[2 * i].entries;
    const auto& impl = Blocks(*Get(fields[2 * i]));
    const size_t tail = BlockOf(impl, entries.back().key);
    size_t first = sizes[i];
    while (first != 0 && BlockOf(impl, entries[first - 1].key) == tail) {
      --first;
    }
    ASSERT_NE(0, first);
    sizes[i] = first + 2;
  }
  write();
  for (const irs::field_id id : {1, 3}) {
    SCOPED_TRACE(testing::Message("field ") << id);
    const auto& walked = Blocks(*Get(id));
    const auto& next = Blocks(*Get(id + 1));
    ASSERT_LT(1, walked.Blocks());
    ASSERT_NE(0, next.FirstEntry());
    const auto& entries = fields[id - 1].entries;
    for (const std::string_view pattern : {".b.*", "a[a-x].*|b.*"}) {
      SCOPED_TRACE(testing::Message("pattern ") << pattern);
      AssertWalk(walked, entries,
                 irs::RegexpAcceptor{irs::ViewCast<irs::byte_type>(pattern)});
    }
    const auto description = irs::MakeParametricDescription(1, false);
    const irs::LevenshteinAcceptor acceptor{description, {}, next.min()};
    auto it = walked.iterator(acceptor);
    ASSERT_FALSE(it->next());
  }
}

TEST_P(TermDictTest, infix_walks_match_coded_and_raw_blocks) {
  const auto keys = Words(20000, 17);
  const Field fields[]{
    {.id = 1, .entries = MakeEntries(keys), .sample = false},
    {.id = 2, .entries = MakeEntries(keys)},
  };
  Write(fields);
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    ASSERT_EQ(
      field.sample,
      BlockKinds(Blocks(
        *reader))[static_cast<size_t>(irs::term_dict::BlockKind::VarFsst)] > 1);
    for (const std::string_view pattern :
         {"%ion%", "%alism%", "%tionun%", "%ings%", "%ationalismpresent%",
          "%12%"}) {
      SCOPED_TRACE(testing::Message("pattern ") << pattern);
      AssertWalk(*reader, field.entries,
                 irs::RegexpAcceptor{irs::RegexpAcceptor::WildcardTag{},
                                     irs::ViewCast<irs::byte_type>(pattern)});
    }
    SCOPED_TRACE("pattern s.*tion.*");
    AssertWalk(*reader, field.entries,
               irs::RegexpAcceptor{
                 irs::ViewCast<irs::byte_type>(std::string_view{"s.*tion.*"})});
  }
}

TEST_P(TermDictTest, infix_walks_skip_empty_blocks_and_keep_split_matches) {
  std::mt19937 rng{29};
  constexpr std::string_view kLetters = "abcdhkmnprstuvwxyz";
  const auto word = [&](size_t length) {
    std::string out;
    for (size_t i = 0; i != length; ++i) {
      out += kLetters[rng() % kLetters.size()];
    }
    return out;
  };
  std::vector<std::string> hosts;
  for (size_t i = 0; i != 200; ++i) {
    hosts.push_back(word(6));
  }
  const auto path = [&] {
    return absl::StrCat("h/", hosts[rng() % hosts.size()], "/",
                        word(3 + rng() % 18));
  };
  std::vector<irs::bstring> keys;
  for (size_t i = 0; i != 20000; ++i) {
    keys.push_back(Bytes(path()));
  }
  for (size_t i = 0; i != 64; ++i) {
    const auto split = absl::StrCat(path(), "p", i, "/");
    keys.push_back(Bytes(split + "gooa"));
    keys.push_back(Bytes(absl::StrCat(split, "google", word(2))));
    const auto near = absl::StrCat(path(), "q", i, "/");
    keys.push_back(Bytes(near + "gooa"));
    keys.push_back(Bytes(near + "goog"));
    keys.push_back(Bytes(near + "googlx"));
    const auto dense = absl::StrCat(path(), "r", i, "/");
    keys.push_back(Bytes(dense + "abcda"));
    keys.push_back(Bytes(absl::StrCat(dense, "abcdhkm", word(2))));
    keys.push_back(Bytes(absl::StrCat(path(), "s", i, "/xgooglex")));
  }
  std::vector<irs::bstring> next;
  for (size_t i = 0; i != 100; ++i) {
    next.push_back(Bytes(absl::StrCat("google", i)));
  }
  const auto entries = MakeEntries(keys);
  const Field fields[]{
    {.id = 1, .entries = entries, .sample = false},
    {.id = 2, .entries = entries},
    {.id = 3, .entries = MakeEntries(next), .sample = false},
  };
  Write(fields);
  const auto split_by_prefix = [](const irs::term_dict::TermReaderImpl& impl,
                                  std::string_view literal) {
    irs::term_dict::Cursor cursor{impl};
    cursor.First();
    do {
      const auto key = irs::ViewCast<char>(cursor.Entries().Key());
      const auto at = key.find(literal);
      const auto lcp = irs::term_dict::HeaderAt(cursor.Entries().View(),
                                                cursor.Entries().Entry())
                         .lcp;
      if (cursor.Entries().Entry() % irs::term_dict::kRestartInterval != 0 &&
          at != std::string_view::npos && at < lcp &&
          lcp < at + literal.size()) {
        return true;
      }
    } while (cursor.Advance());
    return false;
  };
  for (const auto& field : std::span{fields}.first(2)) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    const auto& impl = Blocks(*reader);
    ASSERT_EQ(
      field.sample,
      BlockKinds(
        impl)[static_cast<size_t>(irs::term_dict::BlockKind::VarFsst)] > 1);
    ASSERT_TRUE(split_by_prefix(impl, "google"));
    ASSERT_TRUE(split_by_prefix(impl, "abcdhkm"));
    for (const std::string_view pattern :
         {"%google%", "%oogle%", "%gle%", "%abcdhkm%", "%gglle%"}) {
      SCOPED_TRACE(testing::Message("pattern ") << pattern);
      AssertWalk(*reader, field.entries,
                 irs::RegexpAcceptor{irs::RegexpAcceptor::WildcardTag{},
                                     irs::ViewCast<irs::byte_type>(pattern)});
    }
  }
}

TEST_P(TermDictTest, infix_walks_skip_groups_without_a_hit) {
  std::mt19937 rng{53};
  std::vector<irs::bstring> keys;
  for (uint32_t i = 0; i != 60000; ++i) {
    keys.push_back(Bytes(absl::StrCat(
      "n", i * 7919 % 1000003, "/",
      std::string(1 + rng() % 7, static_cast<char>('a' + rng() % 26)))));
  }
  const auto entries = MakeEntries(keys);
  const Field fields[]{
    {.id = 1, .entries = entries, .sample = false},
    {.id = 2, .entries = entries},
  };
  Write(fields);
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    for (const std::string_view pattern : {"%777%", "%0123%", "%9/qq%"}) {
      SCOPED_TRACE(testing::Message("pattern ") << pattern);
      AssertWalk(*reader, field.entries,
                 irs::RegexpAcceptor{irs::RegexpAcceptor::WildcardTag{},
                                     irs::ViewCast<irs::byte_type>(pattern)});
    }
  }
}

TEST_P(TermDictTest, key_filters_walk_fixed_width_and_empty_keys) {
  std::mt19937 rng{29};
  std::vector<irs::bstring> fixed;
  for (size_t i = 0; i != 20000; ++i) {
    irs::bstring key(8, irs::byte_type{0});
    for (auto& byte : key) {
      byte = static_cast<irs::byte_type>('a' + rng() % 8);
    }
    fixed.push_back(std::move(key));
  }
  auto words = Words(4000, 31);
  words.push_back(Bytes(""));
  const Field fields[]{
    {.id = 1, .entries = MakeEntries(std::move(fixed))},
    {.id = 2, .entries = MakeEntries(std::move(words))},
  };
  Write(fields);
  ASSERT_EQ(8, Get(fields[0])->key_width);
  ASSERT_TRUE(fields[1].entries.front().key.empty());
  const std::pair<const Field*, std::string_view> walks[]{
    {&fields[0], "%abc%"},
    {&fields[0], "%hgf%"},
    {&fields[1], "%s"},
    {&fields[1], "%ing"},
  };
  for (const auto& [field, pattern] : walks) {
    SCOPED_TRACE(testing::Message("field ")
                 << field->id << " pattern " << pattern);
    AssertWalk(*Get(*field), field->entries,
               irs::RegexpAcceptor{irs::RegexpAcceptor::WildcardTag{},
                                   irs::ViewCast<irs::byte_type>(pattern)});
  }
}

TEST_P(TermDictTest, walks_past_the_dfa_budget_match_by_simulation) {
  const Field fields[]{{.id = 1, .entries = MakeEntries(Words(20000, 41))}};
  Write(fields);
  const auto* reader = Get(fields[0]);
  ASSERT_NE(nullptr, reader);
  for (const std::string_view pattern : {"%ion%", "%ab%", "s%ing", "%e%e%e%"}) {
    SCOPED_TRACE(testing::Message("pattern ") << pattern);
    AssertWalk(*reader, fields[0].entries,
               irs::RegexpAcceptor{irs::RegexpAcceptor::WildcardTag{},
                                   irs::ViewCast<irs::byte_type>(pattern),
                                   irs::RegexpAcceptor::kDefaultMaxMem, 2048});
  }
}

TEST_P(TermDictTest, infix_hits_refuse_corrupt_fsst_suffixes) {
  const Field fields[]{{.id = 1, .entries = MakeEntries(Words(20000, 5))}};
  Write(fields);
  const auto& impl = Blocks(*Get(fields[0]));
  irs::term_dict::PinnedRange pinned;
  const auto infix = Bytes("qqq");
  size_t corrupted = 0;
  for (uint32_t block = 0; block != impl.Blocks(); ++block) {
    const auto bytes = impl.Block(block, pinned);
    const auto view = irs::term_dict::Parse(bytes, impl.Fsst());
    if (!view.coded || view.restarts < 2) {
      continue;
    }
    const auto* end = view.suffixes + irs::term_dict::SuffixOffset(view, 1);
    const auto* coded = view.suffixes + irs::term_dict::HeaderAt(view, 0).len;
    if (end - coded < 2 || end[-2] == FSST_ESC) {
      continue;
    }
    std::vector<irs::byte_type> copy(bytes.begin(), bytes.end());
    copy[end - 1 - bytes.data()] = FSST_ESC;
    irs::term_dict::BlockCursor cursor{impl.Fsst()};
    cursor.Open({copy.data(), copy.size()}, irs::term_dict::kNoEntry);
    irs::term_dict::InfixFilter filter{infix};
    ASSERT_SQL_ERROR(
      {
        cursor.Next();
        filter.Rejects(cursor);
        filter.Share(cursor.Skip());
        filter.Rejects(cursor);
      },
      ERRCODE_INDEX_CORRUPTED);
    ++corrupted;
  }
  ASSERT_NE(0, corrupted);
}

TEST_P(TermDictTest, walks_match_in_and_across_the_block_prefix) {
  std::mt19937 rng{41};
  constexpr std::string_view kLetters = "abcdeghklmnoprstuz";
  const auto word = [&](size_t length) {
    std::string out;
    for (size_t i = 0; i != length; ++i) {
      out += kLetters[rng() % kLetters.size()];
    }
    return out;
  };
  std::vector<irs::bstring> keys;
  for (size_t site = 0; site != 24; ++site) {
    const auto host = absl::StrCat("https://", word(5), ".", word(3), "/");
    for (size_t i = 0; i != 400; ++i) {
      keys.push_back(Bytes(
        absl::StrCat(host, word(1 + rng() % 3), "/", word(2 + rng() % 12))));
    }
  }
  const auto entries = MakeEntries(keys);
  const Field fields[]{
    {.id = 1, .entries = entries, .sample = false},
    {.id = 2, .entries = entries},
  };
  Write(fields);
  std::vector<std::string> patterns;
  for (size_t i = 0; i != 6; ++i) {
    const auto key =
      irs::ViewCast<char>(irs::bytes_view{entries[rng() % entries.size()].key});
    const size_t host_end = key.find('/', 8) + 1;
    const size_t size = 3 + rng() % 8;
    const size_t at = rng() % (key.size() - size + 1);
    patterns.push_back(absl::StrCat("%", key.substr(8, 4), "%"));
    patterns.push_back(absl::StrCat("%", key.substr(host_end - 3, 6), "%"));
    patterns.push_back(absl::StrCat("%", key.substr(at, size), "%"));
    patterns.push_back(absl::StrCat("%", key.substr(key.size() - size)));
  }
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    const auto& impl = Blocks(*reader);
    ASSERT_GT(impl.Blocks(), 8);
    irs::term_dict::Cursor cursor{impl};
    cursor.First();
    size_t prefixed = 0;
    do {
      prefixed +=
        cursor.Entries().Entry() == 0 && cursor.Entries().View().prefix > 8;
    } while (cursor.Advance());
    ASSERT_GT(prefixed, impl.Blocks() / 2);
    for (const auto& pattern : patterns) {
      SCOPED_TRACE(testing::Message("pattern ") << pattern);
      AssertWalk(*reader, field.entries,
                 irs::RegexpAcceptor{
                   irs::RegexpAcceptor::WildcardTag{},
                   irs::ViewCast<irs::byte_type>(std::string_view{pattern})});
    }
  }
}

TEST_P(TermDictTest, suffix_walks_match_coded_and_raw_blocks) {
  const auto keys = Words(4000, 17);
  auto escaped = keys;
  for (size_t i = 0; i < keys.size(); i += 199) {
    escaped.push_back(keys[i] + Bytes("#"));
  }
  std::vector<irs::bstring> wide;
  for (const auto& word : Words(100, 23)) {
    wide.push_back(Bytes("zz") + irs::bstring(600, 'q') + word);
    wide.push_back(word + irs::bstring(40, 'x') + word);
  }
  const Field fields[]{
    {.id = 1, .entries = MakeEntries(escaped), .sample = false},
    {.id = 2, .entries = MakeEntries(wide), .sample = false},
    {.id = 3, .entries = MakeEntries(keys)},
    {.id = 4, .entries = MakeEntries(escaped)},
  };
  Write(fields);
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    ASSERT_EQ(
      field.sample,
      BlockKinds(Blocks(
        *reader))[static_cast<size_t>(irs::term_dict::BlockKind::VarFsst)] > 1);
    for (const std::string_view pattern :
         {"%ion", "%s", "%tion", "%alism", "%ings", "%12", "%a", "%ationalism",
          "%ationalismpresent", "%nation", "%zz", "%#", "%n#", "%s#"}) {
      SCOPED_TRACE(testing::Message("pattern ") << pattern);
      AssertWalk(*reader, field.entries,
                 irs::RegexpAcceptor{irs::RegexpAcceptor::WildcardTag{},
                                     irs::ViewCast<irs::byte_type>(pattern)});
    }
    for (const std::string_view pattern : {".*(ing|ion)", ".*(s|al)", "s.*tion",
                                           ".*(ism|ation)s", ".*(#|ion)"}) {
      SCOPED_TRACE(testing::Message("pattern ") << pattern);
      AssertWalk(*reader, field.entries,
                 irs::RegexpAcceptor{irs::ViewCast<irs::byte_type>(pattern)});
    }
  }
}

TEST_P(TermDictTest, fuzzy_walks_match_coded_and_raw_blocks) {
  const auto keys = Words(20000, 17);
  const Field fields[]{
    {.id = 1, .entries = MakeEntries(keys), .sample = false},
    {.id = 2, .entries = MakeEntries(keys)},
  };
  Write(fields);
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    const auto& entries = field.entries;
    std::vector<irs::bstring> targets;
    for (size_t i = 0; i < entries.size(); i += entries.size() / 12) {
      irs::bstring target{entries[i].key};
      targets.push_back(target);
      if (target.size() > 2) {
        target[target.size() / 2] = 'q';
        targets.push_back(std::move(target));
      }
    }
    for (const uint8_t distance : {1, 2}) {
      for (const bool transpositions : {false, true}) {
        const auto description =
          irs::MakeParametricDescription(distance, transpositions);
        for (const auto& target : targets) {
          SCOPED_TRACE(testing::Message("target ")
                       << irs::ViewCast<char>(irs::bytes_view{target})
                       << " distance " << int{distance} << " transpositions "
                       << transpositions);
          const irs::LevenshteinAcceptor acceptor{description, {}, target};
          std::vector<irs::bytes_view> expected;
          for (const auto& entry : entries) {
            if (acceptor.Matches(entry.key)) {
              expected.emplace_back(entry.key);
            }
          }
          auto it = reader->iterator(acceptor);
          size_t i = 0;
          for (; it->next(); ++i) {
            ASSERT_LT(i, expected.size());
            ASSERT_EQ(expected[i], it->value());
          }
          ASSERT_EQ(expected.size(), i);
        }
      }
    }
  }
}

TEST_P(TermDictTest, walks_skip_blocks_missing_required_bytes) {
  std::vector<irs::bstring> keys;
  for (uint64_t i = 0; i != 6000; ++i) {
    keys.emplace_back(
      Bytes(absl::StrCat(absl::Dec(i * 7919 % 1000000007, absl::kZeroPad10))));
  }
  for (auto& word : Words(20000, 11)) {
    keys.push_back(std::move(word));
  }
  for (uint32_t i = 0; i != 40; ++i) {
    keys.emplace_back(Bytes(absl::StrCat("ingq", i)));
    keys.emplace_back(Bytes(absl::StrCat("zq", absl::Dec(i, absl::kZeroPad4))));
  }
  const Field fields[]{{.id = 1, .entries = MakeEntries(std::move(keys))}};
  Write(fields);
  const auto* reader = Get(fields[0]);
  ASSERT_NE(nullptr, reader);
  const auto& impl = Blocks(*reader);
  const auto& entries = fields[0].entries;
  for (const std::string_view pattern :
       {".*q.*", ".*tion", "[0-9]+7", "ing(q|x).*", "(?i)ZQ.*",
        ".*zq[0-9]{2}.*", "zq00(1|3)."}) {
    SCOPED_TRACE(testing::Message("pattern ") << pattern);
    AssertWalk(*reader, entries,
               irs::RegexpAcceptor{irs::ViewCast<irs::byte_type>(pattern)});
  }

  const irs::RegexpAcceptor rare{
    irs::ViewCast<irs::byte_type>(std::string_view{".*q.*"})};
  uint32_t holding = 0;
  const auto need = impl.Index().NeedOf(rare.RequiredBytes());
  for (uint32_t block = 0; block != impl.Blocks(); ++block) {
    holding += impl.Index().Holds(block, need);
  }
  ASSERT_LT(4 * holding, impl.Blocks());
  size_t found = 0;
  for (auto it = reader->iterator(rare); it->next();) {
    ++found;
  }
  ASSERT_EQ(80, found);

  const auto description = irs::MakeParametricDescription(1, false);
  const irs::LevenshteinAcceptor fuzzy{description, {}, Bytes("zq0017")};
  std::vector<irs::bytes_view> expected;
  for (const auto& entry : entries) {
    if (fuzzy.Matches(entry.key)) {
      expected.emplace_back(entry.key);
    }
  }
  ASSERT_FALSE(expected.empty());
  auto it = reader->iterator(fuzzy);
  size_t i = 0;
  for (; it->next(); ++i) {
    ASSERT_LT(i, expected.size());
    ASSERT_EQ(expected[i], it->value());
  }
  ASSERT_EQ(expected.size(), i);
}

TEST_P(TermDictTest,
       walks_read_block_prefixes_only_where_a_prefix_can_rule_out) {
  auto plain = Words(20000, 13);
  auto lines = plain;
  for (uint32_t i = 0; i != 300; ++i) {
    lines.emplace_back(Bytes(absl::StrCat("line", i, "\nstation")));
  }
  const Field fields[]{{.id = 1, .entries = MakeEntries(std::move(plain))},
                       {.id = 2, .entries = MakeEntries(std::move(lines))}};
  Write(fields);
  Open(dir());
  const auto reads_prefixes = [](const irs::TermReader& reader, const auto& a) {
    return irs::term_dict::UsesPrefixes(Blocks(reader).Index(), a);
  };
  struct Case {
    std::string_view pattern;
    bool plain;
    bool lines;
  };
  for (const auto& [pattern, plain_reads, lines_reads] : {
         Case{"(?s).*tion", false, false},
         Case{".*tion", false, true},
         Case{".*(stand|struct).*", false, true},
         Case{"inter.*", true, true},
         Case{"[a-z]+ing", true, true},
       }) {
    SCOPED_TRACE(testing::Message("pattern ") << pattern);
    const irs::RegexpAcceptor a{irs::ViewCast<irs::byte_type>(pattern)};
    for (const auto& [field, reads] : {std::pair{&fields[0], plain_reads},
                                       std::pair{&fields[1], lines_reads}}) {
      const auto* reader = Get(*field);
      ASSERT_NE(nullptr, reader);
      ASSERT_EQ(reads, reads_prefixes(*reader, a));
      AssertWalk(*reader, field->entries, a);
    }
  }
  const irs::RegexpAcceptor like{
    irs::RegexpAcceptor::WildcardTag{},
    irs::ViewCast<irs::byte_type>(std::string_view{"%sta_ion%"})};
  ASSERT_FALSE(reads_prefixes(*Get(fields[1]), like));
  AssertWalk(*Get(fields[1]), fields[1].entries, like);
  const auto description = irs::MakeParametricDescription(1, false);
  const irs::LevenshteinAcceptor fuzzy{description, {}, Bytes("station")};
  ASSERT_TRUE(reads_prefixes(*Get(fields[0]), fuzzy));
}

TEST_P(TermDictTest, fuzzy_walks_over_random_keys_read_near_their_matches) {
  std::mt19937 rng{17};
  constexpr std::string_view kAlphabet = "0123456789abcdefghijklmnopqrstuvwxyz";
  std::vector<irs::bstring> keys;
  for (size_t i = 0; i != 300000; ++i) {
    irs::bstring key(6 + rng() % 5, 0);
    for (auto& byte : key) {
      byte = static_cast<irs::byte_type>(kAlphabet[rng() % kAlphabet.size()]);
    }
    keys.push_back(std::move(key));
  }
  const Field fields[]{{.id = 2, .entries = MakeEntries(std::move(keys))}};
  Write(fields);
  const auto* reader = Get(2);
  ASSERT_NE(nullptr, reader);
  const auto& entries = fields[0].entries;
  const auto description = irs::MakeParametricDescription(1, true);
  for (size_t i = 0; i < entries.size(); i += entries.size() / 16) {
    auto target = entries[i].key;
    target[target.size() / 2] = 'X';
    for (const auto& probe : {entries[i].key, target}) {
      const irs::LevenshteinAcceptor acceptor{description, {}, probe};
      const auto expected = absl::c_count_if(entries, [&](const Entry& entry) {
        return acceptor.Matches(entry.key);
      });
      auto it = reader->iterator(acceptor);
      ptrdiff_t found = 0;
      for (; it->next(); ++found) {
        ASSERT_TRUE(acceptor.Matches(it->value()));
      }
      ASSERT_EQ(expected, found);
    }
  }
}

TEST_P(TermDictTest, walks_cross_long_shared_prefixes) {
  std::mt19937 rng{23};
  constexpr std::string_view kTails = "xyz019";
  std::vector<irs::bstring> keys{irs::bstring{}};
  for (size_t group = 0; group != 4; ++group) {
    const irs::bstring stem(40 + group * 5,
                            static_cast<irs::byte_type>('a' + group));
    for (size_t i = 0; i != 3000; ++i) {
      auto key = stem;
      for (size_t n = 1 + rng() % 6; n != 0; --n) {
        key.push_back(
          static_cast<irs::byte_type>(kTails[rng() % kTails.size()]));
      }
      keys.push_back(std::move(key));
    }
  }
  for (size_t chain = 0; chain != 2; ++chain) {
    irs::bstring path;
    for (size_t n = 0; n != 150; ++n) {
      path.push_back(static_cast<irs::byte_type>('k' + rng() % 4));
      keys.push_back(path);
    }
  }
  const Field fields[]{{.id = 3, .entries = MakeEntries(std::move(keys))}};
  Write(fields);
  const auto* reader = Get(3);
  ASSERT_NE(nullptr, reader);
  const auto& impl = Blocks(*reader);
  ASSERT_GT(impl.Blocks(), 1);
  const auto& entries = fields[0].entries;
  ASSERT_TRUE(entries.front().key.empty());

  for (const std::string_view pattern :
       {"a{20}b.*", "a{40}q", "b{45}[xyz]+", "c{50}0.*", "d{55}", "d{54}",
        "a{39}x.*", "[ab]+[0-9]", ".*", "", "k.*", "[kl]{30}.*", "m{2}",
        "(a{40}|b{45})z{2}"}) {
    SCOPED_TRACE(testing::Message("pattern ") << pattern);
    AssertWalk(*reader, entries,
               irs::RegexpAcceptor{irs::ViewCast<irs::byte_type>(pattern)});
  }

  for (const std::string_view pattern : {"a{20}b.*", "a{40}q", "c{50}q.*"}) {
    SCOPED_TRACE(testing::Message("dead pattern ") << pattern);
    auto it = reader->iterator(
      irs::RegexpAcceptor{irs::ViewCast<irs::byte_type>(pattern)});
    ASSERT_FALSE(it->next());
  }

  const auto description = irs::MakeParametricDescription(1, true);
  for (size_t i = 0; i < entries.size(); i += entries.size() / 16) {
    SCOPED_TRACE(testing::Message("target ") << i);
    const irs::LevenshteinAcceptor acceptor{description, {}, entries[i].key};
    std::vector<irs::bytes_view> expected;
    for (const auto& entry : entries) {
      if (acceptor.Matches(entry.key)) {
        expected.emplace_back(entry.key);
      }
    }
    auto it = reader->iterator(acceptor);
    size_t found = 0;
    for (; it->next(); ++found) {
      ASSERT_LT(found, expected.size());
      ASSERT_EQ(expected[found], it->value());
    }
    ASSERT_EQ(expected.size(), found);
  }
}

TEST_P(TermDictTest, walks_match_short_and_edge_byte_terms) {
  std::mt19937 rng{13};
  constexpr std::string_view kAlphabet[]{
    {"\x00", 1}, "\x01", "a", "b", "\x7f", "\xc3\xa9", "\xef\xbf\xbf"};
  std::vector<irs::bstring> keys;
  for (size_t i = 0; i != 60000; ++i) {
    irs::bstring key;
    for (size_t n = 1 + rng() % 6; n != 0; --n) {
      key.append(Bytes(kAlphabet[rng() % std::size(kAlphabet)]));
    }
    keys.push_back(std::move(key));
  }
  const Field fields[]{{.id = 2, .entries = MakeEntries(std::move(keys))}};
  Write(fields);
  const auto* reader = Get(2);
  ASSERT_NE(nullptr, reader);
  ASSERT_GT(Blocks(*reader).Blocks(), 10);
  const auto& entries = fields[0].entries;
  for (const uint8_t distance : {1, 2}) {
    const auto description = irs::MakeParametricDescription(distance, true);
    for (size_t i = 0; i < entries.size(); i += entries.size() / 16) {
      SCOPED_TRACE(testing::Message("target ")
                   << i << " distance " << int{distance});
      const irs::LevenshteinAcceptor acceptor{description, {}, entries[i].key};
      std::vector<irs::bytes_view> expected;
      for (const auto& entry : entries) {
        if (acceptor.Matches(entry.key)) {
          expected.emplace_back(entry.key);
        }
      }
      auto it = reader->iterator(acceptor);
      size_t found = 0;
      for (; it->next(); ++found) {
        ASSERT_LT(found, expected.size());
        ASSERT_EQ(expected[found], it->value());
      }
      ASSERT_EQ(expected.size(), found);
    }
  }
  for (const std::string_view pattern :
       {"\\x00.*", "a\\x00.*", "\\x01\\x00\\x00.*", "..b.*", ".\\x7f.*", "ab",
        "a", "\\x00"}) {
    SCOPED_TRACE(testing::Message("pattern ") << pattern);
    AssertWalk(*reader, entries,
               irs::RegexpAcceptor{irs::ViewCast<irs::byte_type>(pattern)});
  }
}

TEST_P(TermDictTest, inline_postings_follow_iterators) {
  const Field fields[]{{.id = 2, .entries = MakeEntries(Words(20000, 13))}};
  Write(fields);
  const auto* reader = Get(2);
  ASSERT_NE(nullptr, reader);
  const auto& entries = fields[0].entries;
  const auto docs = [](const Entry& entry) {
    std::vector<irs::doc_id_t> out;
    for (const auto& [doc, freq] : entry.docs) {
      out.push_back(doc);
    }
    return out;
  };
  size_t inlined = 0;
  auto it = reader->iterator();
  size_t i = 0;
  for (; it->next(); ++i) {
    ASSERT_LT(i, entries.size());
    inlined += it->cookie().inline_size != 0;
    ASSERT_EQ(docs(entries[i]), DocsOf(*reader, it->cookie()));
  }
  ASSERT_EQ(entries.size(), i);
  ASSERT_GT(inlined, 0);
  auto seeker = reader->iterator();
  for (size_t k = entries.size(); k-- != 0;) {
    if (k % 13 != 0) {
      continue;
    }
    ASSERT_EQ(irs::SeekResult::Found, seeker->seek_ge(entries[k].key));
    ASSERT_EQ(docs(entries[k]), DocsOf(*reader, seeker->cookie()));
  }
}

TEST_P(TermDictTest, cookies_outlive_their_iterators) {
  std::vector<std::pair<int64_t, irs::doc_id_t>> values;
  for (irs::doc_id_t doc = 1; doc <= 50000; ++doc) {
    values.emplace_back(int64_t{doc / 2} * 7, doc);
  }
  const Field fields[]{
    {.id = 1, .entries = MakeEntries({Bytes("x"), Bytes("y"), Bytes("z")})},
    {.id = 2, .entries = {{.key = Bytes("solo"), .docs = MakeDocs(5, 3)}}},
    {.id = 3, .entries = MakeEntries(Words(20000, 5))},
    {.id = 4,
     .entries = NumericEntries(std::move(values)),
     .dict = irs::NumericLayout::kNumbers},
  };
  Write(fields);
  const auto docs_of = [](const Entry& entry) {
    std::vector<irs::doc_id_t> docs;
    for (const auto& [doc, freq] : entry.docs) {
      docs.push_back(doc);
    }
    return docs;
  };
  struct Visit {
    irs::bstring from;
    irs::bstring to;
    std::vector<irs::PostingMeta> metas;
  };
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    ASSERT_EQ(field.id == 2, reader->size() == 1);
    if (field.id != 2) {
      const auto& impl = Blocks(*reader);
      ASSERT_EQ(field.id == 1, impl.Small());
      ASSERT_EQ(field.id == 4, !impl.RangeLevels().empty());
    }
    const auto& entries = field.entries;
    std::vector<std::pair<irs::PostingMeta, std::vector<irs::doc_id_t>>> saved;
    {
      auto it = reader->iterator();
      for (size_t i = 0; it->next(); ++i) {
        saved.emplace_back(it->cookie(), docs_of(entries[i]));
      }
    }
    {
      auto it = reader->iterator();
      for (size_t i = 0; i < entries.size(); i += 7) {
        ASSERT_TRUE(it->seek(entries[i].key));
        saved.emplace_back(it->cookie(), docs_of(entries[i]));
      }
    }
    std::vector<irs::bytes_view> terms;
    for (const auto& entry : entries) {
      saved.emplace_back(reader->Lookup(entry.key), docs_of(entry));
      terms.emplace_back(entry.key);
    }
    const auto metas = BatchLookup(*reader, terms);
    for (size_t i = 0; i != terms.size(); ++i) {
      saved.emplace_back(metas[i], docs_of(entries[i]));
    }
    std::vector<Visit> visits;
    const auto bounds = Probes(entries);
    std::mt19937 rng{3};
    for (size_t round = 0; round != 50; ++round) {
      auto& visit = visits.emplace_back(Visit{
        .from = round == 0 ? irs::bstring{} : bounds[rng() % bounds.size()],
        .to = round == 0 ? irs::bstring{} : bounds[rng() % bounds.size()],
        .metas = {},
      });
      reader->CoverRange(visit.from, visit.to,
                         [&](irs::bytes_view, const irs::PostingMeta& meta) {
                           visit.metas.push_back(meta);
                           return true;
                         });
    }
    {
      auto it = reader->iterator();
      while (it->next()) {
      }
    }

    size_t inlined = 0;
    for (const auto& [meta, docs] : saved) {
      inlined += meta.inline_size != 0;
      ASSERT_EQ(docs, DocsOf(*reader, meta));
    }
    ASSERT_GT(inlined, 0);
    for (const auto& visit : visits) {
      std::vector<irs::doc_id_t> actual;
      for (const auto& meta : visit.metas) {
        const auto docs = DocsOf(*reader, meta);
        actual.insert(actual.end(), docs.begin(), docs.end());
      }
      absl::c_sort(actual);
      actual.erase(std::unique(actual.begin(), actual.end()), actual.end());
      ASSERT_EQ(DocsIn(entries, visit.from, visit.to), actual);
    }
  }
}

TEST_P(TermDictTest, inline_postings_at_the_blocks_end) {
  auto words = MakeEntries(Words(20000, 5));
  words.back().docs = MakeDocs(3, 1);
  const Field fields[]{
    {.id = 1,
     .entries = {{.key = Bytes("a"), .docs = MakeDocs(1, 0)},
                 {.key = Bytes("b"), .docs = MakeDocs(3, 2)}}},
    {.id = 2, .entries = {{.key = Bytes("solo"), .docs = MakeDocs(4, 5)}}},
    {.id = 3, .entries = std::move(words)},
  };
  Write(fields);
  const auto terms = TakeTerms();
  const auto& small = Blocks(*Get(1));
  ASSERT_TRUE(small.Small());
  ASSERT_NE(0, small.Lookup(Bytes("b")).inline_size);
  const auto* single = Get(2);
  ASSERT_EQ(1, single->size());
  ASSERT_NE(0, single->Lookup(Bytes("solo")).inline_size);
  const auto& large = Blocks(*Get(3));
  {
    irs::term_dict::Cursor cursor{large};
    ASSERT_EQ(irs::SeekResult::Found,
              cursor.SeekGe(fields[2].entries.back().key));
    const auto& meta = cursor.Meta();
    ASSERT_NE(0, meta.inline_size);
    ASSERT_EQ(large.Blocks() - 1, cursor.BlockIndex());
  }
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    AssertField(*Get(field), field.entries);
  }
  ASSERT_EQ(terms.blocks_end, large.BlockOffset(large.Blocks()));
}

TEST_P(TermDictTest, first_terms_keep_their_inline_postings) {
  auto words = MakeEntries(Words(20000, 7));
  words[0].docs = MakeDocs(3, 1);
  words[1].docs = MakeDocs(5, 4);
  const Field fields[]{
    {.id = 1,
     .entries = {{.key = Bytes("a"), .docs = MakeDocs(3, 1)},
                 {.key = Bytes("b"), .docs = MakeDocs(5, 4)}}},
    {.id = 2, .entries = std::move(words)},
  };
  for (const bool compaction : {false, true}) {
    SCOPED_TRACE(testing::Message("compaction ") << compaction);
    Write(fields, compaction);
    for (const auto& field : fields) {
      SCOPED_TRACE(testing::Message("field ") << field.id);
      const auto* reader = Get(field);
      ASSERT_NE(nullptr, reader);
      ASSERT_NE(0, reader->Lookup(field.entries[0].key).inline_size);
      ASSERT_NE(0, reader->Lookup(field.entries[1].key).inline_size);
      AssertField(*reader, field.entries);
    }
  }
}

TEST_P(TermDictTest, held_range_postings_keep_their_inline_postings) {
  constexpr irs::doc_id_t kHeavy = 17000;
  std::vector<std::pair<int64_t, irs::doc_id_t>> values;
  int64_t value = 0;
  irs::doc_id_t doc = 1;
  for (size_t i = 0; i != 64 * 16 * 20 + 64; ++i) {
    values.emplace_back(value++, doc++);
  }
  for (irs::doc_id_t heavy = 1; heavy <= kHeavy; ++heavy) {
    values.emplace_back(value, heavy);
  }
  ++value;
  for (irs::doc_id_t light = 5; light <= 13; light += 4) {
    values.emplace_back(value, light);
  }
  ++value;
  for (irs::doc_id_t heavy = 2; heavy <= kHeavy + 1; ++heavy) {
    values.emplace_back(value, heavy);
  }
  ++value;
  for (size_t i = 0; i != 64; ++i) {
    values.emplace_back(value++, doc++);
  }
  const Field fields[]{
    {.id = 1,
     .entries = NumericEntries(std::move(values)),
     .dict = irs::NumericLayout::kNumbers},
  };
  for (const bool compaction : {false, true}) {
    SCOPED_TRACE(testing::Message("compaction ") << compaction);
    Write(fields, compaction);
    const auto* reader = Get(fields[0]);
    ASSERT_NE(nullptr, reader);
    const auto levels = Blocks(*reader).RangeLevels();
    ASSERT_GT(levels.size(), 1);
    size_t inlined = 0;
    for (auto it = levels[1]->iterator(); it->next();) {
      inlined += it->cookie().inline_size != 0;
    }
    ASSERT_GT(inlined, 0);
    AssertField(*reader, fields[0].entries);
    AssertLevels(*reader, fields[0].entries);
    AssertVisits(*reader, fields[0].entries, 200);
  }
}

TEST_P(TermDictTest, mapped_lookups_do_not_allocate) {
  if constexpr (!kHaveCounter) {
    GTEST_SKIP() << "needs a jemalloc build to count allocations";
  }
  const Field fields[]{
    {.id = 1, .entries = MakeEntries({Bytes("x"), Bytes("y"), Bytes("z")})},
    {.id = 2, .entries = MakeEntries(Words(20000, 9))},
    {.id = 3,
     .entries =
       MakeEntries({irs::bstring{}, irs::bstring(1, irs::byte_type{0xFF})})},
  };
  Write(fields);
  if (!_reader->Store().Base()) {
    GTEST_SKIP() << "only a mapped input reads blocks in place";
  }
  const auto& words = Blocks(*Get(2));
  ASSERT_NE(nullptr, words.Fsst());
  ASSERT_NE(
    0,
    BlockKinds(words)[static_cast<size_t>(irs::term_dict::BlockKind::VarFsst)]);
  for (const auto& field : fields) {
    const auto* reader = Get(field);
    ASSERT_NE(nullptr, reader);
    for (const auto& entry : field.entries) {
      const uint64_t before = ThreadAllocatedBytes();
      const auto meta = reader->Lookup(entry.key);
      if (meta.docs_count == 1) {
        reader->ReadDocs(entry.key, [](irs::doc_id_t) { return true; });
      }
      ASSERT_EQ(before, ThreadAllocatedBytes());
    }
  }
}

TEST_P(TermDictTest, mapped_batch_lookups_do_not_allocate) {
  if constexpr (!kHaveCounter) {
    GTEST_SKIP() << "needs a jemalloc build to count allocations";
  }
  const Field fields[]{{.id = 1, .entries = MakeEntries(Words(20000, 9))}};
  Write(fields);
  if (!_reader->Store().Base()) {
    GTEST_SKIP() << "only a mapped input reads blocks in place";
  }
  const auto& entries = fields[0].entries;
  std::vector<irs::bytes_view> terms;
  for (size_t i = 0; i < entries.size(); i += entries.size() / 60) {
    terms.emplace_back(entries[i].key);
  }
  const auto* reader = Get(fields[0]);
  ASSERT_NE(nullptr, reader);
  size_t found = 0;
  const uint64_t before = ThreadAllocatedBytes();
  reader->LookupBatch(
    terms.size(), [&](size_t i) { return terms[i]; },
    [&](size_t, const irs::PostingMeta&) { ++found; });
  ASSERT_EQ(before, ThreadAllocatedBytes());
  ASSERT_EQ(terms.size(), found);
}

TEST_P(TermDictTest, reads_go_on_after_pages_leave_memory) {
  const Field fields[]{{.id = 1, .entries = MakeEntries(Words(20000, 5))}};
  Write(fields);
  const auto* reader = Get(fields[0]);
  ASSERT_NE(nullptr, reader);
  const auto& impl = Blocks(*reader);
  ASSERT_FALSE(impl.Small());
  if (!impl.Store().Input().IsPaged()) {
    GTEST_SKIP() << "only a paged mapping tracks which pages are in memory";
  }
  const auto& entries = fields[0].entries;
  irs::term_dict::Cursor cursor{impl};
  cursor.First();
  uint32_t block = cursor.BlockIndex();
  size_t at = 0;
  do {
    ASSERT_LT(at, entries.size());
    ASSERT_EQ(irs::bytes_view{entries[at].key}, cursor.Term());
    ++at;
    if (cursor.BlockIndex() != block) {
      block = cursor.BlockIndex();
      duckdb::PageResidency::Invalidate();
    }
  } while (cursor.Advance());
  ASSERT_EQ(entries.size(), at);

  std::vector<irs::bytes_view> terms;
  for (size_t i = 0; i < entries.size(); i += entries.size() / 60) {
    terms.emplace_back(entries[i].key);
  }
  const auto expected = BatchLookup(*reader, terms);
  std::vector<irs::PostingMeta> metas(terms.size());
  size_t found = 0;
  reader->LookupBatch(
    terms.size(), [&](size_t i) { return terms[i]; },
    [&](size_t i, const irs::PostingMeta& meta) {
      metas[i] = meta;
      ++found;
      duckdb::PageResidency::Invalidate();
    });
  ASSERT_EQ(terms.size(), found);
  for (size_t i = 0; i != terms.size(); ++i) {
    AssertSameMeta(expected[i], metas[i]);
  }
}

TEST_P(TermDictTest, range_levels_oracle) { RangeLevelsOracle(false); }

TEST_P(TermDictTest, range_levels_oracle_compaction) {
  RangeLevelsOracle(true);
}

TEST_P(TermDictTest, range_top_level_is_capped_by_segment_docs) {
  std::vector<std::pair<int64_t, irs::doc_id_t>> values;
  for (irs::doc_id_t doc = 1; doc <= 400000; ++doc) {
    values.emplace_back(int64_t{doc} * 7, doc);
  }
  const Field fields[]{{.id = 1,
                        .entries = NumericEntries(std::move(values)),
                        .dict = irs::NumericLayout::kNumbers}};
  Write(fields);
  const auto* reader = Get(fields[0]);
  ASSERT_NE(nullptr, reader);
  const auto levels = Blocks(*reader).RangeLevels();
  ASSERT_EQ(4, levels.size());
  ASSERT_LE(levels.back()->size(), 16);
  AssertLevels(*reader, fields[0].entries);
  AssertVisits(*reader, fields[0].entries, 1000);
}

TEST_P(TermDictTest, ranges_stop_when_the_visitor_does) {
  std::vector<std::pair<int64_t, irs::doc_id_t>> values;
  for (irs::doc_id_t doc = 1; doc <= 50000; ++doc) {
    values.emplace_back(int64_t{doc} * 3, doc);
  }
  const Field fields[]{{.id = 1,
                        .entries = NumericEntries(std::move(values)),
                        .dict = irs::NumericLayout::kNumbers}};
  Write(fields);
  const auto* reader = Get(fields[0]);
  ASSERT_NE(nullptr, reader);
  const auto key = [](int64_t value) {
    irs::byte_type buf[irs::numeric_utils::kNumericTermMaxSize];
    return irs::bstring{irs::numeric_utils::EncodeNumericTerm(buf, value)};
  };
  const std::pair<irs::bstring, irs::bstring> ranges[]{
    {{}, {}},       {key(300), key(90000)}, {key(301), key(149990)},
    {{}, key(600)}, {key(149000), {}},      {key(200000), {}},
  };
  for (const auto& [from, to] : ranges) {
    SCOPED_TRACE(testing::Message("from ")
                 << from.size() << " to " << to.size());
    size_t total = 0;
    reader->CoverRange(from, to, [&](irs::bytes_view, const irs::PostingMeta&) {
      ++total;
      return true;
    });
    for (const size_t stop : {1, 2, 3, 7, 20, 100}) {
      SCOPED_TRACE(testing::Message("stop ") << stop);
      size_t visits = 0;
      reader->CoverRange(from, to,
                         [&](irs::bytes_view, const irs::PostingMeta&) {
                           return ++visits < stop;
                         });
      EXPECT_EQ(std::min(stop, total), visits);
    }
  }
}

TEST_P(TermDictTest, point_range_visits_only_its_term) {
  std::vector<std::pair<int64_t, irs::doc_id_t>> values;
  for (irs::doc_id_t doc = 1; doc <= 50000; ++doc) {
    values.emplace_back(int64_t{doc % 5000} * 3, doc);
  }
  const Field fields[]{{.id = 1,
                        .entries = NumericEntries(std::move(values)),
                        .dict = irs::NumericLayout::kNumbers}};
  Write(fields);
  const auto* reader = Get(fields[0]);
  ASSERT_NE(nullptr, reader);
  const auto& entries = fields[0].entries;
  const auto point = [&](irs::bytes_view key) {
    irs::bstring to{key};
    to.push_back(0);
    std::vector<irs::bstring> keys;
    std::vector<irs::doc_id_t> docs;
    const bool covered = reader->CoverRange(
      key, to, [&](irs::bytes_view visited, const irs::PostingMeta& meta) {
        keys.emplace_back(visited);
        docs = DocsOf(*reader, meta);
        return true;
      });
    EXPECT_FALSE(covered);
    return std::pair{std::move(keys), std::move(docs)};
  };
  for (const size_t i :
       {size_t{0}, size_t{1}, entries.size() / 2, entries.size() - 1}) {
    SCOPED_TRACE(testing::Message("entry ") << i);
    const auto& key = entries[i].key;
    irs::bstring to{key};
    to.push_back(0);
    const auto [keys, docs] = point(key);
    ASSERT_EQ(std::vector<irs::bstring>{key}, keys);
    ASSERT_EQ(DocsIn(entries, key, to), docs);
  }
  irs::byte_type buf[irs::numeric_utils::kNumericTermMaxSize];
  const auto [keys, docs] =
    point(irs::numeric_utils::EncodeNumericTerm(buf, int64_t{4}));
  ASSERT_TRUE(keys.empty());
}

TEST_P(TermDictTest, mapped_range_visits_do_not_allocate) {
  if constexpr (!kHaveCounter) {
    GTEST_SKIP() << "needs a jemalloc build to count allocations";
  }
  std::vector<std::pair<int64_t, irs::doc_id_t>> values;
  for (irs::doc_id_t doc = 1; doc <= 50000; ++doc) {
    values.emplace_back(int64_t{doc} * 3, doc);
  }
  const Field fields[]{
    {.id = 1,
     .entries = NumericEntries(std::move(values)),
     .dict = irs::NumericLayout::kNumbers},
  };
  Write(fields);
  if (!_reader->Store().Base()) {
    GTEST_SKIP() << "only a mapped input reads blocks in place";
  }
  const auto* reader = Get(fields[0]);
  ASSERT_NE(nullptr, reader);
  const auto bounds = Probes(fields[0].entries);
  std::mt19937 rng{5};
  for (size_t round = 0; round != 500; ++round) {
    const irs::bytes_view from = bounds[rng() % bounds.size()];
    const irs::bytes_view to = bounds[rng() % bounds.size()];
    const uint64_t before = ThreadAllocatedBytes();
    reader->CoverRange(
      from, to, [](irs::bytes_view, const irs::PostingMeta&) { return true; });
    ASSERT_EQ(before, ThreadAllocatedBytes());
  }
}

INSTANTIATE_TEST_SUITE_P(
  TermDictTest, TermDictTest,
  ::testing::Combine(::testing::ValuesIn(tests::kDirParams)),
  TermDictTest::to_string);

TEST(TermDictKeyTest, padded_mismatch_ignores_bytes_past_size) {
  for (size_t size = 0; size != 2 * sizeof(uint64_t); ++size) {
    const irs::bstring key(size, irs::byte_type{'k'});
    irs::term_dict::PaddedKey padded{key};
    auto unpadded = std::make_unique_for_overwrite<irs::byte_type[]>(
      size + irs::term_dict::kKeyPad);
    std::memcpy(unpadded.get(), key.data(), size);
    EXPECT_EQ(size, irs::term_dict::PaddedMismatch(unpadded.get(),
                                                   padded.Data(), size));
    if (size != 0) {
      unpadded[size - 1] = irs::byte_type{'z'};
      EXPECT_EQ(size - 1, irs::term_dict::PaddedMismatch(unpadded.get(),
                                                         padded.Data(), size));
    }
  }
}

irs::term_dict::PrefixTrie DecodeTrie(std::string_view encoded) {
  return irs::term_dict::PrefixTrie{irs::ViewCast<irs::byte_type>(encoded), 7};
}

struct Prefixed {
  std::vector<irs::bstring> terms;
  std::vector<uint32_t> blocks;
  irs::term_dict::Separators separators;
  irs::term_dict::FieldHead head;
  std::unique_ptr<irs::term_dict::DictIndex> index;
};

std::unique_ptr<Prefixed> MakePrefixed(std::vector<irs::bstring> terms,
                                       size_t block_size,
                                       irs::bytes_view key_prefix = {}) {
  absl::c_sort(terms);
  terms.erase(std::unique(terms.begin(), terms.end()), terms.end());
  auto out = std::make_unique<Prefixed>();
  out->terms = std::move(terms);
  irs::term_dict::PrefixTrieBuilder builder;
  for (size_t i = 0; i != out->terms.size(); ++i) {
    const auto block = static_cast<uint32_t>(i / block_size);
    out->blocks.push_back(block);
    builder.Add(out->terms[i]);
    if (i != 0 && block != out->blocks[i - 1]) {
      const irs::bytes_view prev = out->terms[i - 1];
      const irs::bytes_view first = out->terms[i];
      out->separators.arena.append(key_prefix);
      out->separators.arena.append(
        first.substr(0, irs::CommonPrefixLength(prev, first) + 1));
      out->separators.offsets.push_back(
        static_cast<uint32_t>(out->separators.arena.size()));
    }
  }
  auto& head = out->head;
  head.id = 7;
  head.term_count = out->terms.size();
  head.min = irs::ViewCast<char>(irs::bytes_view{out->terms.front()});
  head.max = irs::ViewCast<char>(irs::bytes_view{out->terms.back()});
  head.block_prefixes = builder.Finish();
  out->index = std::make_unique<irs::term_dict::DictIndex>(
    head, key_prefix, out->blocks.back() + 1, out->separators);
  return out;
}

void AssertRouting(std::vector<irs::bstring> terms, size_t block_size,
                   std::string_view key_prefix = {}) {
  const irs::bytes_view prefix = irs::ViewCast<irs::byte_type>(key_prefix);
  const auto dict = MakePrefixed(std::move(terms), block_size, prefix);
  const auto& index = *dict->index;
  const auto& owned = dict->separators;
  std::vector<irs::bytes_view> separators;
  for (size_t i = 0; i + 1 < owned.offsets.size(); ++i) {
    separators.push_back(
      irs::term_dict::SeparatorAt(irs::term_dict::Slice(owned), i));
  }
  ASSERT_EQ(separators.size() + 1, index.Blocks());
  std::vector<irs::bstring> probes{
    irs::bstring{prefix},
    irs::bstring{prefix} + irs::bstring(12, irs::byte_type{0xFF}),
  };
  const auto variants = [&](irs::bytes_view key) {
    probes.emplace_back(key);
    probes.emplace_back(irs::bstring{key} + irs::byte_type{0});
    probes.emplace_back(irs::bstring{key} + irs::byte_type{0xFF});
    for (size_t size = prefix.size(); size < key.size(); ++size) {
      probes.emplace_back(key.substr(0, size));
      auto changed = irs::bstring{key.substr(0, size + 1)};
      for (const int delta : {-1, 1}) {
        changed[size] = static_cast<irs::byte_type>(key[size] + delta);
        probes.emplace_back(changed);
      }
    }
  };
  for (const auto separator : separators) {
    variants(separator);
  }
  for (size_t i = 0; i < dict->terms.size();
       i += 1 + dict->terms.size() / 512) {
    variants(irs::bstring{prefix} + dict->terms[i]);
  }
  for (const auto& probe : probes) {
    SCOPED_TRACE(testing::Message("probe ") << probe.size());
    const irs::term_dict::PaddedKey padded{probe};
    const auto expected = static_cast<uint32_t>(
      absl::c_upper_bound(separators, irs::bytes_view{probe}) -
      separators.begin());
    ASSERT_EQ(expected, index.BlockOf(padded.View()));
    for (const uint32_t last : {uint32_t{0}, expected / 2, expected}) {
      ASSERT_EQ(expected, index.BlockFrom(last, padded.View()));
    }
  }
}

template<typename A>
std::vector<bool> LiveBlocksOf(const Prefixed& dict, const A& a) {
  const uint32_t blocks = dict.blocks.back() + 1;
  std::vector<bool> live(blocks);
  irs::term_dict::BlockFilter<A> filter{*dict.index, a};
  filter.Run(0, blocks, [&](uint32_t lo, uint32_t end) {
    for (; lo != end; ++lo) {
      live[lo] = true;
    }
  });
  return live;
}

template<typename A>
bool AcceptsTerm(const A& a, irs::bytes_view term) {
  auto state = a.Start();
  for (const auto c : term) {
    state = a.Step(state, c);
    if (!A::Alive(state)) {
      return false;
    }
  }
  typename A::PayloadType payload{};
  return a.Accept(state, payload);
}

size_t TermsWith(const std::vector<irs::bstring>& terms,
                 irs::bytes_view prefix) {
  const auto begin = absl::c_lower_bound(
    terms, prefix,
    [](const irs::bstring& term, irs::bytes_view key) { return term < key; });
  const auto end = std::partition_point(
    begin, terms.end(),
    [&](const irs::bstring& term) { return term.starts_with(prefix); });
  return static_cast<size_t>(end - begin);
}

size_t AssertTrieNode(const irs::term_dict::PrefixTrie& trie, uint32_t node,
                      irs::bstring& path,
                      const std::vector<irs::bstring>& terms,
                      size_t span = irs::term_dict::kTrieSpan) {
  using namespace irs::term_dict;
  SCOPED_TRACE(testing::Message("depth ") << path.size());
  const size_t count = TermsWith(terms, path);
  EXPECT_NE(0U, count);
  const bool terminal = absl::c_binary_search(terms, path);
  EXPECT_EQ(terminal, trie.Terminal(node));
  const size_t continuations = count - (terminal ? 1 : 0);
  const size_t depth = path.size() - std::min(path.size(), trie.Chain().size());
  if (trie.Leaf(node)) {
    EXPECT_EQ(continuations != 0, trie.Open(node));
    if (depth >= kTrieFull && depth < kTrieDepth) {
      EXPECT_LE(count, span);
    }
    EXPECT_TRUE(depth >= kTrieFull || continuations == 0);
    return count;
  }
  EXPECT_FALSE(trie.Open(node));
  EXPECT_LT(depth, kTrieDepth);
  if (depth >= kTrieFull) {
    EXPECT_LT(kTrieSpan, count);
  }
  size_t below = 0;
  for (auto child = trie.ChildrenOf(node, 0); child.Label() != kNoLabel;
       child.Next()) {
    const auto label = static_cast<irs::byte_type>(child.Label());
    EXPECT_EQ(child.Node(), trie.ChildOf(node, label));
    path.push_back(label);
    below += AssertTrieNode(trie, child.Node(), path, terms, span);
    path.pop_back();
  }
  EXPECT_EQ(continuations, below);
  return count;
}

void AssertLinks(const Prefixed& dict) {
  const auto& index = *dict.index;
  const auto& trie = index.Trie();
  const auto separators = irs::term_dict::Slice(dict.separators);
  const uint32_t blocks = index.Blocks();
  for (uint32_t block = 0; block != blocks; ++block) {
    SCOPED_TRACE(testing::Message("block ") << block);
    const irs::bytes_view lower =
      block == 0 ? irs::bytes_view{dict.terms.front()}
                 : irs::term_dict::SeparatorAt(separators, block - 1);
    const irs::bytes_view upper =
      block + 1 == blocks ? irs::bytes_view{dict.terms.back()}
                          : irs::term_dict::SeparatorAt(separators, block);
    const auto prefix = lower.substr(0, irs::CommonPrefixLength(lower, upper));
    uint32_t node = trie.Root();
    uint32_t depth = 0;
    while (depth != prefix.size() && !trie.Open(node) && !trie.Leaf(node)) {
      const auto child = trie.ChildOf(node, prefix[depth]);
      if (child == irs::term_dict::kNoNode) {
        break;
      }
      node = child;
      ++depth;
    }
    const auto link = index.Link(block);
    ASSERT_EQ(node, link.node);
    ASSERT_EQ(depth, link.depth);
  }
}

template<typename... Acceptors>
void AssertTrie(std::vector<irs::bstring> terms, size_t block_size,
                const Acceptors&... acceptors) {
  const auto dict = MakePrefixed(std::move(terms), block_size);
  const auto& trie = dict->index->Trie();
  if (dict->blocks.back() != 0) {
    ASSERT_FALSE(trie.Empty());
    const irs::bytes_view first = dict->terms.front();
    const irs::bytes_view last = dict->terms.back();
    ASSERT_EQ(first.substr(0, irs::CommonPrefixLength(first, last)),
              trie.Chain());
    irs::bstring path;
    ASSERT_EQ(dict->terms.size(),
              AssertTrieNode(trie, trie.Root(), path, dict->terms));
    AssertLinks(*dict);
  }
  const auto check = [&](const auto& a) {
    const auto live = LiveBlocksOf(*dict, a);
    for (size_t i = 0; i != dict->terms.size(); ++i) {
      if (AcceptsTerm(a, dict->terms[i])) {
        ASSERT_TRUE(live[dict->blocks[i]]);
      }
    }
  };
  (check(acceptors), ...);
}

std::vector<irs::bstring> RandomTerms(std::mt19937& rng,
                                      std::string_view alphabet, size_t count,
                                      size_t min_size, size_t max_size,
                                      std::string_view prefix = {}) {
  std::vector<irs::bstring> terms;
  terms.reserve(count);
  for (size_t i = 0; i != count; ++i) {
    auto term = Bytes(prefix);
    for (size_t n = min_size + rng() % (max_size - min_size + 1); n != 0; --n) {
      term.push_back(
        static_cast<irs::byte_type>(alphabet[rng() % alphabet.size()]));
    }
    terms.push_back(std::move(term));
  }
  return terms;
}

TEST(TermDictPrefixTrieTest, trie_matches_terms_and_filter_keeps_matches) {
  std::mt19937 rng{29};
  std::string bytes;
  for (size_t b = 0; b != 256; ++b) {
    bytes.push_back(static_cast<char>(b));
  }
  const std::string_view kEdgeBytes{
    "\x00\x01"
    "ab\xFF",
    5};
  const auto pattern = [](std::string_view p) {
    return irs::RegexpAcceptor{irs::ViewCast<irs::byte_type>(p)};
  };
  const auto any = pattern("(?s).*");
  const auto starts = pattern("a.*");
  const auto ends = pattern(".*b");
  const auto inner = pattern("[ab]{2}x.*");
  const auto dotted = pattern("a.c.*");
  const auto host = pattern("https://www\\.a[bx]z.*");
  const auto one = irs::MakeParametricDescription(1, false);
  const auto two = irs::MakeParametricDescription(2, false);
  const irs::LevenshteinAcceptor near{one, {}, Bytes("abcdefgh")};
  const irs::LevenshteinAcceptor url{one, {}, Bytes("https://www.abxy")};
  const irs::LevenshteinAcceptor far{two, {}, Bytes("abab")};
  const auto assert_all = [&](std::vector<irs::bstring> terms, size_t block) {
    AssertTrie(std::move(terms), block, any, starts, ends, inner, dotted, host,
               near, url, far);
  };
  assert_all(
    RandomTerms(rng, "0123456789abcdefghijklmnopqrstuvwxyz", 20000, 6, 10), 7);
  assert_all(RandomTerms(rng, bytes, 20000, 1, 8), 31);
  assert_all(RandomTerms(rng, kEdgeBytes, 5000, 0, 6), 5);
  assert_all(RandomTerms(rng, "abxyz019", 5000, 1, 8, "https://www."), 3);
  assert_all(RandomTerms(rng, "abxyz019", 5000, 1, 8, "https://www."), 400);
  auto narrowing = RandomTerms(rng, "abxyz019", 3000, 1, 8, "https://web.");
  for (auto& term : RandomTerms(rng, "abxyz019", 3000, 1, 8, "https://www.")) {
    narrowing.push_back(std::move(term));
  }
  narrowing.push_back(Bytes("z"));
  assert_all(narrowing, 3);
  narrowing.pop_back();
  assert_all(std::move(narrowing), 40);
  assert_all(RandomTerms(rng, "ab", 20000, 10, 20), 50);
  assert_all({Bytes("a"), Bytes("b")}, 1);
  assert_all({irs::bstring{}, Bytes("x")}, 1);
  assert_all({Bytes("abcdefgh"), Bytes("abcdefgz")}, 1);
  assert_all({Bytes("abc"), Bytes("abcdefgh"), Bytes("abcdefgz")}, 1);
  assert_all({Bytes("ab"), Bytes("abcd"), Bytes("abce")}, 2);
}

TEST(TermDictPrefixTrieTest, routing_matches_upper_bound) {
  std::mt19937 rng{37};
  std::string bytes;
  for (size_t b = 0; b != 256; ++b) {
    bytes.push_back(static_cast<char>(b));
  }
  const std::string_view kEdgeBytes{
    "\x00\x01"
    "ab\xFF",
    5};
  AssertRouting(
    RandomTerms(rng, "0123456789abcdefghijklmnopqrstuvwxyz", 20000, 6, 10), 7);
  AssertRouting(RandomTerms(rng, bytes, 20000, 1, 8), 5);
  AssertRouting(RandomTerms(rng, kEdgeBytes, 5000, 0, 6), 3);
  AssertRouting(RandomTerms(rng, "abxyz019", 5000, 1, 8, "https://www."), 3);
  AssertRouting(RandomTerms(rng, "ab", 20000, 10, 20), 50);
  AssertRouting(RandomTerms(rng, "ab", 20000, 10, 20), 2);
  AssertRouting(RandomTerms(rng, bytes, 3000, 1, 8), 4, "\x01k");
  AssertRouting({Bytes("a"), Bytes("b")}, 1);
}

template<typename Root>
void AssertRoot(std::vector<irs::bstring> separators) {
  absl::c_sort(separators);
  separators.erase(std::unique(separators.begin(), separators.end()),
                   separators.end());
  const std::vector<irs::bytes_view> views{separators.begin(),
                                           separators.end()};
  irs::term_dict::Separators owned;
  for (const auto separator : views) {
    owned.arena.append(separator);
    owned.offsets.push_back(static_cast<uint32_t>(owned.arena.size()));
  }
  const Root root{irs::term_dict::Slice(owned)};
  ASSERT_EQ(views.size() + 1, root.Blocks());
  std::vector<irs::bstring> probes{
    irs::bstring{},
    irs::bstring(1, irs::byte_type{0}),
    irs::bstring(12, irs::byte_type{0xFF}),
  };
  for (const auto& separator : separators) {
    probes.emplace_back(separator);
    probes.emplace_back(separator + irs::byte_type{0});
    probes.emplace_back(separator + irs::byte_type{0xFF});
    for (size_t size = 0; size < separator.size(); ++size) {
      probes.emplace_back(separator.substr(0, size));
      auto changed = separator.substr(0, size + 1);
      for (const int delta : {-1, 1}) {
        changed[size] = static_cast<irs::byte_type>(separator[size] + delta);
        probes.emplace_back(changed);
      }
    }
  }
  for (const auto& probe : probes) {
    const irs::bytes_view key{probe};
    const irs::term_dict::PaddedKey padded{key};
    const auto view = padded.View();
    const size_t expected = absl::c_upper_bound(views, key) - views.begin();
    ASSERT_EQ(expected, root.Find(view));
    if constexpr (std::derived_from<Root, irs::term_dict::Root>) {
      for (const size_t from : {size_t{0}, expected / 2, expected}) {
        ASSERT_EQ(expected, root.FindFrom(from, view));
      }
      ASSERT_EQ(views.size() + 1, root.FindFrom(views.size() + 1, view));
    }
  }
}

template<typename Root>
void AssertRoots() {
  AssertRoot<Root>({});
  AssertRoot<Root>({Bytes("m")});
  std::vector<irs::bstring> urls;
  for (size_t i = 0; i != 3000; ++i) {
    urls.emplace_back(Bytes(
      absl::StrCat("https://host", i / 10, ".example.com/seg", i % 10, "/p")));
  }
  AssertRoot<Root>(urls);
  std::vector<irs::bstring> ties;
  for (size_t i = 0; i != 64; ++i) {
    irs::bstring key = Bytes("prefix_");
    key.append(i % 9, irs::byte_type{0});
    key.push_back(static_cast<irs::byte_type>(i));
    ties.push_back(key);
    ties.push_back(key.substr(0, 7 + i % 12));
    key.append(9, irs::byte_type{0xFF});
    ties.push_back(std::move(key));
  }
  AssertRoot<Root>(ties);
  std::mt19937 rng{31};
  std::vector<irs::bstring> random;
  for (size_t i = 0; i != 2000; ++i) {
    irs::bstring key(1 + rng() % 12, irs::byte_type{0});
    for (auto& byte : key) {
      byte = static_cast<irs::byte_type>(rng() % 4 == 0 ? 0xFF : rng() % 3);
    }
    random.push_back(std::move(key));
  }
  AssertRoot<Root>(random);
  constexpr size_t kFanouts[] = {1,  2,  3,  4,  5,  6,  8,  9,  15, 16,  17,
                                 31, 32, 33, 47, 48, 49, 63, 64, 65, 100, 128};
  std::vector<irs::bstring> wide;
  size_t next = 0;
  for (size_t high = 2; high < 256; high += 1 + high % 3) {
    const size_t fanout = kFanouts[next++ % std::size(kFanouts)];
    for (size_t low = 0; low != fanout; ++low) {
      wide.push_back(irs::bstring{static_cast<irs::byte_type>(high),
                                  static_cast<irs::byte_type>(2 * low + 1)});
    }
  }
  AssertRoot<Root>(wide);
}

TEST(TermDictRootTest, tree_matches_upper_bound) {
  AssertRoots<irs::term_dict::TreeRoot>();
}

TEST(TermDictRootTest, eytzinger_matches_upper_bound) {
  AssertRoots<irs::term_dict::EytzingerRoot>();
}

TEST(TermDictRootTest, art_matches_upper_bound) {
  AssertRoots<irs::term_dict::ArtRoot>();
}

template<bool Fixed>
struct ChosenRoot : irs::term_dict::Root {
  explicit ChosenRoot(irs::term_dict::SeparatorsView separators)
    : Root{separators, Fixed} {}
};

TEST(TermDictRootTest, root_matches_upper_bound) {
  AssertRoots<ChosenRoot<false>>();
  AssertRoots<ChosenRoot<true>>();
}

TEST(TermDictPrefixTrieTest, refuses_corrupt_encodings) {
  using namespace std::string_view_literals;
  using irs::term_dict::kNoLabel;
  using irs::term_dict::kNoNode;
  constexpr auto kNodes =
    "\x04"
    "\x00\x02"
    "ab"
    "\x01\x01"
    "c"
    "\x02\x00"
    "\x01\x00"sv;
  const auto valid = DecodeTrie(absl::StrCat("\x00"sv, kNodes));
  ASSERT_EQ(4U, valid.Nodes());
  ASSERT_TRUE(valid.Chain().empty());
  ASSERT_EQ(0U, valid.Root());
  const auto a = valid.ChildOf(valid.Root(), 'a');
  const auto b = valid.ChildOf(valid.Root(), 'b');
  ASSERT_NE(kNoNode, a);
  ASSERT_NE(kNoNode, b);
  ASSERT_EQ(kNoNode, valid.ChildOf(valid.Root(), 'c'));
  ASSERT_TRUE(valid.Terminal(a));
  ASSERT_FALSE(valid.Open(a));
  ASSERT_TRUE(valid.Open(b));
  ASSERT_TRUE(valid.Leaf(b));
  const auto ac = valid.ChildOf(a, 'c');
  ASSERT_TRUE(valid.Terminal(ac));
  auto children = valid.ChildrenOf(valid.Root(), 0);
  ASSERT_EQ('a', children.Label());
  ASSERT_EQ(a, children.Node());
  children.Next();
  ASSERT_EQ('b', children.Label());
  ASSERT_EQ(b, children.Node());
  children.Next();
  ASSERT_EQ(kNoLabel, children.Label());
  ASSERT_EQ('b', valid.ChildrenOf(valid.Root(), 'b').Label());
  ASSERT_EQ(kNoLabel, valid.ChildrenOf(valid.Root(), 'c').Label());
  ASSERT_EQ(kNoLabel, valid.ChildrenOf(b, 0).Label());

  const auto chained = DecodeTrie(absl::StrCat("\x02xy"sv, kNodes));
  ASSERT_EQ(4U, chained.Nodes());
  ASSERT_EQ(irs::ViewCast<irs::byte_type>("xy"sv), chained.Chain());
  const auto x = chained.Root();
  ASSERT_FALSE(chained.Leaf(x));
  ASSERT_FALSE(chained.Open(x));
  ASSERT_FALSE(chained.Terminal(x));
  ASSERT_EQ(kNoNode, chained.ChildOf(x, 'y'));
  ASSERT_EQ(kNoLabel, chained.ChildrenOf(x, 'y').Label());
  const auto y = chained.ChildOf(x, 'x');
  ASSERT_NE(kNoNode, y);
  ASSERT_FALSE(chained.Leaf(y));
  children = chained.ChildrenOf(y, 0);
  ASSERT_EQ('y', children.Label());
  ASSERT_EQ(0U, children.Node());
  children.Next();
  ASSERT_EQ(kNoLabel, children.Label());
  ASSERT_EQ(a, chained.ChildOf(chained.ChildOf(y, 'y'), 'a'));

  for (const auto encoded : {
         ""sv,
         "\x00"sv,
         "\x00\x00"sv,
         "\x03xy"sv,
         "\x02xy"sv,
         "\x00\x02\x00\x01"sv,
         "\x00\x02\x00\x01"
         "a"sv,
         "\x00\x02\x00\x01"
         "a"
         "\x04\x00"sv,
         "\x00\x02\x02\x01"
         "a"
         "\x01\x00"sv,
         "\x00\x03\x00\x02"
         "ba"
         "\x01\x00"
         "\x01\x00"sv,
         "\x00\x03\x00\x02"
         "aa"
         "\x01\x00"
         "\x01\x00"sv,
         "\x00\x02\x00\x02"
         "ab"
         "\x01\x00"sv,
         "\x00\x02\x00\x01"
         "a"
         "\x01\x00"
         "\x00"sv,
         "\x00\x03\x00\x01"
         "a"
         "\x01\x00"
         "\x01\x00"sv,
       }) {
    ASSERT_SQL_ERROR(DecodeTrie(encoded), ERRCODE_INDEX_CORRUPTED);
  }
}

TEST(TermDictPrefixTrieTest, refuses_paths_deeper_than_the_trie) {
  const auto path = [](uint32_t depth) {
    std::string encoded{'\x00', static_cast<char>(depth + 1)};
    for (uint32_t node = 0; node != depth; ++node) {
      encoded.append({'\x00', '\x01', 'a'});
    }
    encoded.append({static_cast<char>(irs::term_dict::kTrieTerminal), '\x00'});
    return encoded;
  };
  ASSERT_EQ(irs::term_dict::kTrieDepth + 1,
            DecodeTrie(path(irs::term_dict::kTrieDepth)).Nodes());
  ASSERT_SQL_ERROR(DecodeTrie(path(irs::term_dict::kTrieDepth + 1)),
                   ERRCODE_INDEX_CORRUPTED);
}

void AssertSpans(std::vector<irs::bstring> terms, std::span<const size_t> runs,
                 std::span<const uint32_t> spans) {
  absl::c_sort(terms);
  terms.erase(std::unique(terms.begin(), terms.end()), terms.end());
  irs::term_dict::PrefixTrieBuilder builder;
  size_t at = 0;
  for (size_t run = 0; at != terms.size(); ++run) {
    builder.SetSpan(spans[run % spans.size()]);
    const size_t end = std::min(terms.size(), at + runs[run % runs.size()]);
    for (; at != end; ++at) {
      builder.Add(terms[at]);
    }
  }
  const auto trie = DecodeTrie(builder.Finish());
  irs::bstring path;
  ASSERT_EQ(terms.size(), AssertTrieNode(trie, trie.Root(), path, terms,
                                         *absl::c_max_element(spans)));
}

TEST(TermDictPrefixTrieTest, trie_follows_the_span_of_each_run) {
  std::mt19937 rng{5};
  constexpr size_t kRuns[] = {30, 900, 61, 400, 2000, 48, 1};
  constexpr uint32_t kSpans[] = {48, 900, 61, 400, 1024, 48, 48};
  AssertSpans(RandomTerms(rng, "abcdefghijklmnopqrstuvwxyz", 50000, 3, 10),
              kRuns, kSpans);
  AssertSpans(RandomTerms(rng, "ab", 20000, 8, 16), kRuns, kSpans);
}

TEST(TermDictPrefixTrieTest, split_children_keep_their_own_terms) {
  std::vector<irs::bstring> terms{Bytes("0")};
  for (const char child : {'b', 'c'}) {
    for (char x = 'a'; x != 'k'; ++x) {
      for (char y = 'a'; y != 'k'; ++y) {
        terms.push_back(Bytes(std::string{"aaaa"} + child + x + y));
      }
    }
  }
  terms.push_back(Bytes("aaaadaa"));
  constexpr size_t kRuns[] = {200, 1};
  constexpr uint32_t kSpans[] = {1024, 48};
  AssertSpans(std::move(terms), kRuns, kSpans);
}

struct MaskedIndex {
  irs::term_dict::FieldHead head;
  irs::term_dict::Separators separators;
  std::unique_ptr<irs::term_dict::DictIndex> index;
};

std::unique_ptr<MaskedIndex> IndexBlocks(
  const std::vector<std::vector<irs::bstring>>& blocks) {
  auto out = std::make_unique<MaskedIndex>();
  irs::term_dict::DictIndexBuilder builder;
  builder.Reset(true);
  size_t terms = 0;
  for (size_t block = 0; block != blocks.size(); ++block) {
    const auto& run = blocks[block];
    builder.Add(static_cast<uint32_t>(run.size()),
                [&](uint32_t i) { return irs::bytes_view{run[i]}; });
    if (block != 0) {
      out->separators.arena.append(run.front());
      out->separators.offsets.push_back(
        static_cast<uint32_t>(out->separators.arena.size()));
    }
    terms += run.size();
  }
  auto& head = out->head;
  head.id = 7;
  head.term_count = terms;
  head.min = irs::ViewCast<char>(irs::bytes_view{blocks.front().front()});
  head.max = irs::ViewCast<char>(irs::bytes_view{blocks.back().back()});
  builder.Finish(head);
  out->index = std::make_unique<irs::term_dict::DictIndex>(
    head, irs::bytes_view{}, static_cast<uint32_t>(blocks.size()),
    out->separators);
  return out;
}

std::vector<uint32_t> HoldingBlocks(const irs::term_dict::DictIndex& index,
                                    std::string_view bytes) {
  irs::ByteSet need;
  irs::AddBytes(need, irs::ViewCast<irs::byte_type>(bytes));
  const auto masks = index.NeedOf(need);
  std::vector<uint32_t> holding;
  for (uint32_t block = 0; block != index.Blocks(); ++block) {
    if (index.Holds(block, masks)) {
      holding.push_back(block);
    }
  }
  return holding;
}

TEST(TermDictIndexTest, masks_keep_the_bytes_that_reject_blocks) {
  using Blocks = std::vector<uint32_t>;
  const auto small = IndexBlocks({
    {Bytes("aa0"), Bytes("ab0")},
    {Bytes("ba1"), Bytes("bb1")},
    {Bytes("ca2"), Bytes("cc2")},
  });
  const auto& index = *small->index;
  EXPECT_EQ((Blocks{0, 1, 2}), HoldingBlocks(index, "a"));
  EXPECT_EQ((Blocks{0, 1}), HoldingBlocks(index, "b"));
  EXPECT_EQ((Blocks{2}), HoldingBlocks(index, "c"));
  EXPECT_EQ((Blocks{0}), HoldingBlocks(index, "b0"));
  EXPECT_EQ((Blocks{}), HoldingBlocks(index, "c0"));
  EXPECT_EQ((Blocks{}), HoldingBlocks(index, "z"));
  EXPECT_TRUE(index.MayHoldAny(irs::ByteSet{.words = {uint64_t{1} << '0'}}));
  EXPECT_FALSE(
    index.MayHoldAny(irs::ByteSet{.words = {0, uint64_t{1} << ('z' - 64)}}));

  const auto uniform = IndexBlocks({
    {Bytes("ab"), Bytes("ba")},
    {Bytes("bab"), Bytes("bb")},
  });
  EXPECT_EQ((Blocks{0, 1}), HoldingBlocks(*uniform->index, "ab"));
  EXPECT_EQ((Blocks{}), HoldingBlocks(*uniform->index, "az"));
  EXPECT_FALSE(
    uniform->index->MayHoldAny(irs::ByteSet{.words = {uint64_t{1} << '\n'}}));

  std::vector<std::vector<irs::bstring>> wide;
  for (uint32_t b = 128; b != 128 + 70; ++b) {
    wide.push_back(
      {irs::bstring{irs::byte_type{'m'}, static_cast<irs::byte_type>(b)}});
  }
  const auto many = IndexBlocks(wide);
  std::string selected{"\x82"};
  EXPECT_EQ((Blocks{2}), HoldingBlocks(*many->index, selected));
  Blocks all(wide.size());
  absl::c_iota(all, 0);
  std::string dropped{"\xC3"};
  EXPECT_EQ(all, HoldingBlocks(*many->index, dropped));
  EXPECT_EQ(all, HoldingBlocks(*many->index, "m"));
}

TEST(TermDictIndexTest, refuses_corrupt_masks) {
  const auto dict = IndexBlocks({
    {Bytes("aa0"), Bytes("ab0")},
    {Bytes("ba1"), Bytes("bb1")},
    {Bytes("ca2"), Bytes("cc2")},
  });
  constexpr size_t kUnion = irs::term_dict::kMaskUnionBytes;
  ASSERT_LE(kUnion + 2, dict->head.block_mask_bytes.size());
  ASSERT_GT(kUnion + 57, dict->head.block_mask_bytes.size());
  const auto refused = [&](auto&& corrupt) {
    auto head = dict->head;
    corrupt(head);
    ASSERT_SQL_ERROR((irs::term_dict::DictIndex{head, {}, 3, dict->separators}),
                     ERRCODE_INDEX_CORRUPTED);
  };
  using Head = irs::term_dict::FieldHead;
  refused([](Head& head) { head.block_mask_bytes.resize(kUnion - 1); });
  refused([](Head& head) { head.block_masks.pop_back(); });
  refused([](Head& head) {
    std::swap(head.block_mask_bytes[kUnion], head.block_mask_bytes[kUnion + 1]);
  });
  refused([](Head& head) { head.block_masks[7] = '\xFF'; });
}

TEST(TermDictPrefixTrieTest, refuses_chain_outside_block_bounds) {
  using namespace std::string_literals;
  const auto dict =
    MakePrefixed({Bytes("aa"), Bytes("ab"), Bytes("ba"), Bytes("bb")}, 2);
  const uint32_t blocks = dict->blocks.back() + 1;
  ASSERT_EQ(2U, blocks);
  auto head = dict->head;
  head.block_prefixes = "\x02zz\x01\x00\x00"s;
  ASSERT_SQL_ERROR(
    (irs::term_dict::DictIndex{head, {}, blocks, dict->separators}),
    ERRCODE_INDEX_CORRUPTED);
}

}  // namespace
