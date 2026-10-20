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

#include <iresearch/formats/flush_state.hpp>
#include <iresearch/formats/index/idx_reader.hpp>
#include <iresearch/formats/index/idx_writer.hpp>
#include <iresearch/formats/index/term_dict.hpp>
#include <iresearch/formats/reader_state.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/regexp_acceptor.hpp>
#include <random>

#include "formats_test_case_base.hpp"
#include "tests_param.hpp"

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
};

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

class CountingInput final : public irs::IndexInput {
 public:
  CountingInput(irs::IndexInput::ptr impl, std::shared_ptr<size_t> reads)
    : _impl{std::move(impl)}, _reads{std::move(reads)} {}

  const irs::byte_type* ReadStable(uint64_t) final { return nullptr; }
  const irs::byte_type* ReadStable(uint64_t, uint64_t) final { return nullptr; }
  const irs::byte_type* ReadVolatile(uint64_t offset, uint64_t count) final {
    return _impl->ReadVolatile(offset, count);
  }
  const irs::byte_type* ReadVolatile(uint64_t count) final {
    return _impl->ReadVolatile(count);
  }
  irs::byte_type ReadByte() final { return _impl->ReadByte(); }
  using DataInput::ReadData;
  void ReadData(irs::byte_type* b, uint64_t count) final {
    _impl->ReadData(b, count);
  }
  void ReadData(duckdb::QueryContext, irs::byte_type* b, uint64_t count) final {
    _impl->ReadData(b, count);
  }
  void ReadData(uint64_t offset, irs::byte_type* b, size_t count) final {
    ++*_reads;
    _impl->ReadData(offset, b, count);
  }
  int16_t ReadI16() final { return _impl->ReadI16(); }
  int32_t ReadI32() final { return _impl->ReadI32(); }
  int64_t ReadI64() final { return _impl->ReadI64(); }
  uint32_t ReadV32() final { return _impl->ReadV32(); }
  uint64_t ReadV64() final { return _impl->ReadV64(); }
  void SkipV32() final { _impl->SkipV32(); }
  void SkipV64() final { _impl->SkipV64(); }
  uint64_t Position() const noexcept final { return _impl->Position(); }
  uint64_t Length() const noexcept final { return _impl->Length(); }
  bool IsEOF() const noexcept final { return _impl->IsEOF(); }
  ptr Dup() const final {
    return std::make_unique<CountingInput>(_impl->Dup(), _reads);
  }
  ptr Reopen() const final {
    return std::make_unique<CountingInput>(_impl->Reopen(), _reads);
  }
  void Skip(uint64_t count) final { _impl->Skip(count); }
  void Seek(uint64_t pos) final { _impl->Seek(pos); }
  uint32_t Checksum(uint64_t offset) const final {
    return _impl->Checksum(offset);
  }

 private:
  irs::IndexInput::ptr _impl;
  std::shared_ptr<size_t> _reads;
};

class CountingDirectory final : public tests::DirectoryMock {
 public:
  explicit CountingDirectory(irs::Directory& impl) : DirectoryMock{impl} {}

  irs::IndexInput::ptr open(std::string_view name,
                            irs::IOAdvice advice) const final {
    auto in = DirectoryMock::open(name, advice);
    if (in && name.ends_with(".idx")) {
      return std::make_unique<CountingInput>(std::move(in), _reads);
    }
    return in;
  }

  size_t Reads() const noexcept { return *_reads; }

 private:
  std::shared_ptr<size_t> _reads = std::make_shared<size_t>(0);
};

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

irs::bstring BigEndian(uint8_t shift, uint64_t value) {
  irs::bstring key(1 + sizeof(uint64_t), irs::byte_type{0});
  key[0] = shift;
  absl::big_endian::Store64(key.data() + 1, value >> shift << shift);
  return key;
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

class TermDictTest : public tests::DirectoryTestCaseBase<> {
 protected:
  void Write(std::span<const Field> fields) {
    irs::doc_id_t docs = 0;
    for (const auto& field : fields) {
      for (const auto& entry : field.entries) {
        for (const auto& [doc, freq] : entry.docs) {
          docs = std::max(docs, doc);
        }
      }
    }
    irs::FlushState state{
      .dir = &dir(),
      .name = kSegment,
      .doc_count = docs,
      .index_features = irs::IndexFeatures::None,
    };
    irs::IdxWriter idx{dir(), kSegment};
    irs::term_dict::FieldWriter writer{/*compaction=*/false,
                                       irs::IResourceManager::gNoop};
    writer.SetIdxWriter(idx);
    writer.prepare(state);
    for (const auto& field : fields) {
      OracleTerms terms{field.entries};
      tests::MockTermReader reader{
        terms, irs::FieldMeta{field.id, irs::IndexFeatures::None},
        field.entries.empty() ? irs::bytes_view{}
                              : irs::bytes_view{field.entries.front().key},
        field.entries.empty() ? irs::bytes_view{}
                              : irs::bytes_view{field.entries.back().key}};
      writer.write(reader);
    }
    writer.end();
    idx.Commit();
    Open(dir());
  }

  void Open(const irs::Directory& from) {
    _reader.reset();
    _meta.name = kSegment;
    _idx = std::make_unique<irs::IdxReader>(from, kSegment, nullptr);
    _reader = std::make_unique<irs::term_dict::FieldReader>(
      irs::IResourceManager::gNoop);
    _reader->prepare(
      irs::ReaderState{.dir = &from, .meta = &_meta, .idx = _idx.get()});
  }

  const irs::TermReader* Get(irs::field_id id) const {
    return _reader->field(id);
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

    const auto less = [](const Entry& entry, irs::bytes_view key) {
      return irs::bytes_view{entry.key} < key;
    };
    auto shared = reader.iterator();
    for (const auto& probe : Probes(entries)) {
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

  irs::SegmentMeta _meta;
  std::unique_ptr<irs::IdxReader> _idx;
  std::unique_ptr<irs::term_dict::FieldReader> _reader;
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

TEST_P(TermDictTest, many_blocks) {
  const Field fields[]{{.id = 2, .entries = MakeEntries(Words(60000, 7))}};
  Write(fields);
  const auto* reader = Get(2);
  ASSERT_NE(nullptr, reader);
  AssertField(*reader, fields[0].entries);
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
    const auto* reader = Get(field.id);
    ASSERT_NE(nullptr, reader);
    AssertField(*reader, field.entries);
  }
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

TEST_P(TermDictTest, reads_per_operation) {
  const Field fields[]{
    {.id = 1, .entries = MakeEntries({Bytes("x"), Bytes("y"), Bytes("z")})},
    {.id = 2, .entries = MakeEntries(Words(20000, 5))},
  };
  Write(fields);
  CountingDirectory counting{dir()};
  Open(counting);

  const auto* small = Get(1);
  const auto* large = Get(2);
  ASSERT_NE(nullptr, small);
  ASSERT_NE(nullptr, large);
  const auto& impl = static_cast<const irs::term_dict::TermReaderImpl&>(*large);
  ASSERT_GT(impl.Blocks(), 10);

  const auto reads = [&](auto&& op) {
    const size_t before = counting.Reads();
    op();
    return counting.Reads() - before;
  };

  for (const auto& entry : fields[0].entries) {
    ASSERT_EQ(0, reads([&] { small->Lookup(entry.key); }));
  }
  const auto& entries = fields[1].entries;
  for (size_t i = 0; i < entries.size(); i += 97) {
    ASSERT_EQ(1, reads([&] { large->Lookup(entries[i].key); }));
  }
  ASSERT_EQ(0, reads([&] { large->Lookup(Bytes("0")); }));
  ASSERT_EQ(0, reads([&] { large->Lookup(Bytes("zzz")); }));

  size_t inlined = 0;
  for (size_t i = 0; i < entries.size(); i += 7) {
    const auto& entry = entries[i];
    if (large->Lookup(entry.key).inline_size == 0) {
      continue;
    }
    ++inlined;
    std::vector<irs::doc_id_t> docs;
    ASSERT_EQ(1, reads([&] {
                large->ReadDocs(entry.key, [&](irs::doc_id_t doc) {
                  docs.push_back(doc);
                  return true;
                });
              }));
    ASSERT_EQ(entry.docs.size(), docs.size());
    for (size_t j = 0; j != docs.size(); ++j) {
      ASSERT_EQ(entry.docs[j].first, docs[j]);
    }
  }
  ASSERT_GT(inlined, 0);

  for (const std::string_view pattern :
       {"inter.*", "b.*", "con.*ing", "zzz.*", "[d-f].*"}) {
    SCOPED_TRACE(testing::Message("pattern ") << pattern);
    const irs::RegexpAcceptor acceptor{irs::ViewCast<irs::byte_type>(pattern)};
    ASSERT_TRUE(acceptor.ok());
    irs::containers::FlatHashSet<size_t> blocks;
    for (const auto& entry : entries) {
      if (acceptor.Matches(entry.key)) {
        blocks.insert(impl.GetRoot().Find(entry.key));
      }
    }
    const size_t walked = reads([&] {
      auto it = large->iterator(acceptor);
      while (it->next()) {
      }
    });
    ASSERT_LE(walked, blocks.size() + 2);
  }
}

TEST_P(TermDictTest, mapped_lookups_do_not_allocate) {
  if constexpr (!kHaveCounter) {
    GTEST_SKIP() << "needs a jemalloc build to count allocations";
  }
  const Field fields[]{
    {.id = 1, .entries = MakeEntries({Bytes("x"), Bytes("y"), Bytes("z")})},
    {.id = 2, .entries = MakeEntries(Words(20000, 9))},
  };
  Write(fields);
  if (!_reader->Base()) {
    GTEST_SKIP() << "only a mapped input reads blocks in place";
  }
  for (const auto& field : fields) {
    const auto* reader = Get(field.id);
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

INSTANTIATE_TEST_SUITE_P(TermDictTest, TermDictTest,
                         ::testing::Combine(::testing::ValuesIn(
                           tests::GetDirectories<tests::kTypesDefault>())),
                         TermDictTest::to_string);

}  // namespace
