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
#include <iresearch/formats/index/term_dict_cursor.hpp>
#include <iresearch/formats/reader_state.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/levenshtein_acceptor.hpp>
#include <iresearch/utils/levenshtein_utils.hpp>
#include <iresearch/utils/numeric_utils.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/regexp_acceptor.hpp>
#include <random>

#include "formats_test_case_base.hpp"
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
  bool ranges = false;
  irs::TermDict dict = irs::TermDict::Main;
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

struct ReadState {
  size_t reads = 0;
  bool fail = false;
  std::vector<std::pair<uint64_t, uint64_t>> prefetched;
  std::vector<std::pair<uint64_t, uint64_t>> evicted;
  std::vector<uint64_t> offsets;
  std::vector<size_t> hinted;
  uint32_t warm_epoch = 0;
  uint64_t warm_limit = std::numeric_limits<uint64_t>::max();
  size_t resident_checks = 0;
  bool resident = false;
};

void MaybeFail(const ReadState& state) {
  if (state.fail) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR), ERR_MSG("injected failure"));
  }
}

class CountingInput final : public irs::IndexInput {
 public:
  CountingInput(irs::IndexInput::ptr impl, std::shared_ptr<ReadState> state)
    : _impl{std::move(impl)}, _state{std::move(state)} {}

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
    MaybeFail(*_state);
    ++_state->reads;
    _state->offsets.push_back(offset);
    _state->hinted.push_back(_state->prefetched.size());
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
    return std::make_unique<CountingInput>(_impl->Dup(), _state);
  }
  ptr Reopen() const final {
    MaybeFail(*_state);
    return std::make_unique<CountingInput>(_impl->Reopen(), _state);
  }
  void Skip(uint64_t count) final { _impl->Skip(count); }
  void Seek(uint64_t pos) final { _impl->Seek(pos); }
  uint32_t Checksum(uint64_t offset) const final {
    return _impl->Checksum(offset);
  }
  void Prefetch(uint64_t offset, uint64_t count) const noexcept final {
    _state->prefetched.emplace_back(offset, offset + count);
  }
  void Evict(uint64_t offset, uint64_t count) const noexcept final {
    _state->evicted.emplace_back(offset, offset + count);
  }
  bool Resident(uint64_t, uint64_t) const noexcept final {
    ++_state->resident_checks;
    return _state->resident;
  }
  bool Warm(uint64_t, uint64_t count) const noexcept final {
    return _state->warm_epoch == irs::file_utils::ResidencyEpoch() &&
           count <= _state->warm_limit;
  }

 private:
  irs::IndexInput::ptr _impl;
  std::shared_ptr<ReadState> _state;
};

class CountingDirectory final : public tests::DirectoryMock {
 public:
  explicit CountingDirectory(irs::Directory& impl) : DirectoryMock{impl} {}

  irs::IndexInput::ptr open(std::string_view name,
                            irs::IOAdvice advice) const final {
    auto in = DirectoryMock::open(name, advice);
    if (in && name.ends_with(".idx")) {
      return std::make_unique<CountingInput>(std::move(in), _state);
    }
    return in;
  }

  size_t Reads() const noexcept { return _state->reads; }
  std::vector<std::pair<uint64_t, uint64_t>>& Prefetched() const noexcept {
    return _state->prefetched;
  }
  std::vector<std::pair<uint64_t, uint64_t>>& Evicted() const noexcept {
    return _state->evicted;
  }
  std::vector<uint64_t>& Offsets() const noexcept { return _state->offsets; }
  std::vector<size_t>& Hinted() const noexcept { return _state->hinted; }
  void FailReads(bool fail) noexcept { _state->fail = fail; }
  void MarkWarm() noexcept {
    _state->warm_epoch = irs::file_utils::ResidencyEpoch();
  }
  void SetResident(bool resident) noexcept { _state->resident = resident; }
  void SetWarmLimit(uint64_t limit) noexcept { _state->warm_limit = limit; }
  size_t& ResidentChecks() const noexcept { return _state->resident_checks; }

 private:
  std::shared_ptr<ReadState> _state = std::make_shared<ReadState>();
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

size_t BlockOf(const irs::term_dict::TermReaderImpl& impl,
               irs::bytes_view term) {
  irs::bstring key{impl.KeyPrefix()};
  key.append(term);
  return impl.GetRoot().Find(key);
}

std::array<size_t, 3> BlockKinds(const irs::term_dict::TermReaderImpl& reader) {
  std::array<size_t, 3> kinds{};
  irs::term_dict::BlockBuffer buffer;
  for (uint32_t block = 0; block != reader.Blocks(); ++block) {
    ++kinds.at(*reader.Block(block, buffer));
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

class EntriesReader final : public irs::BasicTermReader {
 public:
  explicit EntriesReader(const Field& field)
    : _field{&field}, _terms{field.entries}, _it{_terms} {}

  irs::TermOnlyIterator::ptr iterator() const final {
    return irs::memory::to_managed<irs::TermOnlyIterator>(_it);
  }
  irs::FieldKey key() const final {
    return {.id = _field->id, .dict = _field->dict};
  }
  irs::FieldProperties properties() const final {
    return {.ranges = _field->ranges};
  }
  irs::bytes_view min() const final {
    return _field->entries.empty()
             ? irs::bytes_view{}
             : irs::bytes_view{_field->entries.front().key};
  }
  irs::bytes_view max() const final {
    return _field->entries.empty()
             ? irs::bytes_view{}
             : irs::bytes_view{_field->entries.back().key};
  }
  uint32_t KeyWidth() const noexcept final {
    return ::KeyWidth(_field->entries);
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

class TermDictTest : public tests::DirectoryTestCaseBase<> {
 protected:
  void Write(std::span<const Field> fields, bool compaction = false) {
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
    irs::term_dict::FieldWriter writer{compaction};
    writer.SetIdxWriter(idx);
    writer.prepare(state);
    for (const auto& field : fields) {
      EntriesReader reader{field};
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

  const irs::TermReader* Get(irs::field_id id,
                             irs::TermDict dict = irs::TermDict::Main) const {
    return _reader->field({.id = id, .dict = dict});
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
    std::vector<irs::PostingMeta> metas(terms.size());
    for (const bool sorted : {false, true}) {
      if (sorted) {
        absl::c_sort(terms);
      }
      reader.LookupBatch(terms, metas);
      for (size_t i = 0; i != terms.size(); ++i) {
        SCOPED_TRACE(testing::Message("batch probe ")
                     << i << " sorted " << sorted);
        AssertSameMeta(reader.Lookup(terms[i]), metas[i]);
      }
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

  void AssertLevels(const irs::TermReader& reader,
                    const std::vector<Entry>& entries) const {
    constexpr size_t kHeavyDocs = size_t{1} << 14;
    const auto& impl =
      static_cast<const irs::term_dict::TermReaderImpl&>(reader);
    const auto levels = impl.RangeLevels();
    const uint32_t prefix = impl.RangePrefix();
    const uint32_t stride = impl.KeyWidth() - prefix;
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
      const auto& level = levels[k];
      ASSERT_GE(level.count, 2);
      ASSERT_LT(level.count, below.size());
      std::vector<irs::bstring> keys;
      for (uint32_t i = 0; i != level.count; ++i) {
        auto& key = keys.emplace_back(reader.min().substr(0, prefix));
        key.append(level.starts + uint64_t{i} * stride, stride);
      }
      const auto first_child = [&](uint32_t i) -> size_t {
        if (i == level.count) {
          return below.size();
        }
        if (k != 0) {
          return absl::little_endian::Load32(level.firsts + 4 * i);
        }
        return absl::c_lower_bound(below_keys, keys[i]) - below_keys.begin();
      };
      std::vector<std::vector<irs::doc_id_t>> docs(level.count);
      ASSERT_EQ(0, first_child(0));
      for (uint32_t i = 0; i != level.count; ++i) {
        const size_t first = first_child(i);
        const size_t end = first_child(i + 1);
        ASSERT_LT(first, end);
        ASSERT_EQ(below_keys[first], keys[i]);
        for (size_t j = first; j != end; ++j) {
          if (below[j].size() >= kHeavyDocs) {
            ASSERT_EQ(1, end - first);
          }
          docs[i].insert(docs[i].end(), below[j].begin(), below[j].end());
        }
        absl::c_sort(docs[i]);
        docs[i].erase(std::unique(docs[i].begin(), docs[i].end()),
                      docs[i].end());
        irs::PostingMeta meta;
        irs::PostingsReader::decode(
          level.records + absl::little_endian::Load32(level.offsets + 4 * i),
          reader.meta().index_features, meta);
        ASSERT_EQ(docs[i], DocsOf(reader, meta));
      }
      below_keys = std::move(keys);
      below = std::move(docs);
    }
  }

  void AssertVisits(const irs::TermReader& reader,
                    const std::vector<Entry>& entries, size_t rounds) const {
    constexpr size_t kLeafFanout = 64;
    constexpr size_t kFanout = 16;
    const auto& impl =
      static_cast<const irs::term_dict::TermReaderImpl&>(reader);
    const auto levels = impl.RangeLevels();
    const size_t bound = levels.empty() ? entries.size()
                                        : levels.back().count +
                                            2 * kFanout * (levels.size() - 1) +
                                            2 * kLeafFanout;
    auto bounds = Probes(entries);
    const uint32_t prefix = impl.RangePrefix();
    const uint32_t stride = impl.KeyWidth() - prefix;
    for (const auto& level : levels) {
      for (uint32_t i = 0; i != level.count; ++i) {
        irs::bstring start{reader.min().substr(0, prefix)};
        start.append(level.starts + uint64_t{i} * stride, stride);
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
      reader.VisitRange(from, to,
                        [&](irs::bytes_view key, const irs::PostingMeta& meta) {
                          keys.emplace_back(key);
                          const auto docs = DocsOf(reader, meta);
                          actual.insert(actual.end(), docs.begin(), docs.end());
                        });
      ASSERT_LE(keys.size(), bound);
      for (size_t i = 0; i != keys.size(); ++i) {
        ASSERT_LE(from, irs::bytes_view{keys[i]});
        ASSERT_TRUE(to.empty() || irs::bytes_view{keys[i]} < to);
        ASSERT_TRUE(i == 0 || keys[i - 1] < keys[i]);
      }
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
      {.id = 1, .entries = NumericEntries(sequential), .ranges = true},
      {.id = 2, .entries = NumericEntries(scattered), .ranges = true},
      {.id = 3, .entries = NumericEntries(clustered), .ranges = true},
      {.id = 4, .entries = NumericEntries(heavy), .ranges = true},
      {.id = 5, .entries = NumericEntries(mixed), .ranges = true},
      {.id = 6, .entries = NumericEntries(multi), .ranges = true},
    };
    Write(fields, compaction);
    for (const auto& field : fields) {
      SCOPED_TRACE(testing::Message("field ") << field.id);
      const auto* reader = Get(field.id);
      ASSERT_NE(nullptr, reader);
      ASSERT_TRUE(reader->meta().ranges);
      AssertField(*reader, field.entries);
      AssertLevels(*reader, field.entries);
      AssertVisits(*reader, field.entries, field.id == 4 ? 100 : 10000);
    }
    const auto height = [&](irs::field_id id) {
      return static_cast<const irs::term_dict::TermReaderImpl&>(*Get(id))
        .RangeLevels()
        .size();
    };
    ASSERT_EQ(3, height(1));
    ASSERT_EQ(0, height(4));
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
  CountingDirectory counting{dir()};
  Open(counting);
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("width ") << field.id);
    const auto* reader = Get(field.id);
    ASSERT_NE(nullptr, reader);
    const auto width = field.entries.front().key.size();
    ASSERT_EQ(width <= irs::term_dict::kMaxStride ? width : 0,
              reader->KeyWidth());
    if (reader->KeyWidth() != 0) {
      const size_t before = counting.Reads();
      reader->Lookup(field.entries[100].key + irs::byte_type{0});
      ASSERT_EQ(before, counting.Reads());
    }
    AssertField(*reader, field.entries);
  }
}

TEST_P(TermDictTest, dictless_fields) {
  const irs::bstring null_key;
  const irs::bstring false_key(1, irs::byte_type{0x00});
  const irs::bstring true_key(1, irs::byte_type{0xFF});
  const Field fields[]{
    {.id = 1, .entries = MakeEntries({null_key})},
    {.id = 2, .entries = MakeEntries({false_key})},
    {.id = 3, .entries = MakeEntries({true_key})},
    {.id = 4, .entries = MakeEntries({false_key, true_key})},
    {.id = 5, .entries = MakeEntries({null_key, false_key, true_key})},
    {.id = 6, .entries = MakeEntries({false_key, Bytes("a")})},
  };
  Write(fields);
  CountingDirectory counting{dir()};
  Open(counting);
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto* reader = Get(field.id);
    ASSERT_NE(nullptr, reader);
    const auto& impl =
      static_cast<const irs::term_dict::TermReaderImpl&>(*reader);
    ASSERT_EQ(field.entries.size() == 1, impl.Single());
    ASSERT_LE(impl.Blocks(), 1);
    ASSERT_EQ(KeyWidth(field.entries), reader->KeyWidth());
    const size_t before = counting.Reads();
    AssertField(*reader, field.entries);
    AssertWalk(*reader, field.entries,
               irs::RegexpAcceptor{
                 irs::ViewCast<irs::byte_type>(std::string_view{".*"})});
    ASSERT_EQ(before, counting.Reads());
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
    const auto* reader = Get(field.id);
    ASSERT_NE(nullptr, reader);
    const auto& impl =
      static_cast<const irs::term_dict::TermReaderImpl&>(*reader);
    ASSERT_NE(nullptr, impl.Fsst());
    const auto kinds = BlockKinds(impl);
    ASSERT_EQ(
      0, kinds[static_cast<size_t>(irs::term_dict::BlockKind::FixedStride)]);
    ASSERT_EQ(
      field.sample,
      kinds[static_cast<size_t>(irs::term_dict::BlockKind::VarFsst)] > 1);
    ASSERT_EQ(field.id != 2,
              kinds[static_cast<size_t>(irs::term_dict::BlockKind::Var)] != 0);
    AssertField(*reader, field.entries);
    for (const std::string_view pattern :
         {"inter.*", ".*ation", ".*stand.*", "[a-c].*ing[0-9]+"}) {
      SCOPED_TRACE(testing::Message("pattern ") << pattern);
      AssertWalk(*reader, field.entries,
                 irs::RegexpAcceptor{irs::ViewCast<irs::byte_type>(pattern)});
    }
  }
}

TEST_P(TermDictTest, fsst_trained_inside_open_block) {
  const auto keys = Words(30000, 3);
  const Field fields[]{
    {.id = 1,
     .entries = MakeEntries({keys.begin(), keys.begin() + 60}),
     .sample = false},
    {.id = 2, .entries = MakeEntries(keys)},
  };
  Write(fields);
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto* reader = Get(field.id);
    ASSERT_NE(nullptr, reader);
    AssertField(*reader, field.entries);
  }
  const auto& first =
    static_cast<const irs::term_dict::TermReaderImpl&>(*Get(1));
  ASSERT_EQ(1, first.Blocks());
  ASSERT_EQ(
    1,
    BlockKinds(first)[static_cast<size_t>(irs::term_dict::BlockKind::VarFsst)]);
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
    const auto* reader = Get(field.id);
    ASSERT_NE(nullptr, reader);
    const auto kinds =
      BlockKinds(static_cast<const irs::term_dict::TermReaderImpl&>(*reader));
    ASSERT_EQ(
      field.id != 2,
      kinds[static_cast<size_t>(irs::term_dict::BlockKind::VarFsst)] != 0);
    ASSERT_EQ(field.id == 2,
              kinds[static_cast<size_t>(irs::term_dict::BlockKind::Var)] > 1);
    AssertField(*reader, field.entries);
  }
}

TEST_P(TermDictTest, single_term_fields) {
  const Field fields[]{
    {.id = 1, .entries = MakeEntries({Bytes("hello")})},
    {.id = 2, .entries = MakeEntries({irs::bstring{}})},
    {.id = 3, .entries = MakeEntries({BigEndian(0, 42)}), .ranges = true},
    {.id = 4, .entries = MakeEntries(Words(3000, 41))},
    {.id = 5, .entries = MakeEntries({Bytes("world")})},
  };
  Write(fields);
  CountingDirectory counting{dir()};
  Open(counting);
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto* reader = Get(field.id);
    ASSERT_NE(nullptr, reader);
    const auto& impl =
      static_cast<const irs::term_dict::TermReaderImpl&>(*reader);
    const bool single = field.entries.size() == 1;
    ASSERT_EQ(single, impl.Single());
    ASSERT_EQ(KeyWidth(field.entries), reader->KeyWidth());
    ASSERT_EQ(field.ranges, reader->meta().ranges);
    const size_t before = counting.Reads();
    AssertField(*reader, field.entries);
    for (const std::string_view pattern : {".*", "hel.*", "x.*", ".*o"}) {
      SCOPED_TRACE(testing::Message("pattern ") << pattern);
      AssertWalk(*reader, field.entries,
                 irs::RegexpAcceptor{irs::ViewCast<irs::byte_type>(pattern)});
    }
    AssertVisits(*reader, field.entries, 20);
    if (single) {
      ASSERT_EQ(before, counting.Reads());
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
  const auto& impl =
    static_cast<const irs::term_dict::TermReaderImpl&>(*reader);
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
    const auto* reader = Get(field.id);
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
    const auto* reader = Get(field.id);
    ASSERT_NE(nullptr, reader);
    ASSERT_EQ(field.id, reader->KeyWidth());
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
    const auto* reader = Get(field.id);
    ASSERT_NE(nullptr, reader);
    ASSERT_EQ(field.id, reader->KeyWidth());
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
    const auto* reader = Get(field.id);
    ASSERT_NE(nullptr, reader);
    ASSERT_EQ(
      field.sample,
      BlockKinds(static_cast<const irs::term_dict::TermReaderImpl&>(
        *reader))[static_cast<size_t>(irs::term_dict::BlockKind::VarFsst)] > 1);
    for (const std::string_view pattern :
         {".*abc.*", ".*aba.*", ".*cabca.*", ".*bhc.*", "b.*acb.*"}) {
      SCOPED_TRACE(testing::Message("pattern ") << pattern);
      AssertWalk(*reader, field.entries,
                 irs::RegexpAcceptor{irs::ViewCast<irs::byte_type>(pattern)});
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

TEST_P(TermDictTest, small_fields_are_copied_at_open) {
  const Field fields[]{
    {.id = 1, .entries = MakeEntries({Bytes("x"), Bytes("y"), Bytes("z")})},
    {.id = 2, .entries = MakeEntries(Words(20000, 5))},
    {.id = 3, .entries = MakeEntries({Bytes("false"), Bytes("true")})},
  };
  Write(fields);
  if (!_reader->Base()) {
    GTEST_SKIP() << "only a mapped input reads blocks in place";
  }
  const auto* begin = _reader->Base();
  const auto* end = begin + _reader->Input().Length();
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    const auto& impl =
      static_cast<const irs::term_dict::TermReaderImpl&>(*Get(field.id));
    ASSERT_EQ(field.id != 2, impl.Small());
    irs::term_dict::BlockBuffer buffer;
    const auto* block = impl.Block(0, buffer);
    ASSERT_EQ(impl.Small(), block < begin || end <= block);
    AssertField(impl, field.entries);
  }
}

TEST_P(TermDictTest, open_reads_the_head_once_and_evicts_it) {
  std::vector<std::pair<int64_t, irs::doc_id_t>> values;
  for (irs::doc_id_t doc = 1; doc <= 50000; ++doc) {
    values.emplace_back(int64_t{doc} * 7, doc);
  }
  auto keys = Words(20000, 13);
  keys.emplace_back(irs::bstring(9000, irs::byte_type{'q'}));
  const Field fields[]{
    {.id = 1, .entries = MakeEntries(std::move(keys))},
    {.id = 2, .entries = NumericEntries(std::move(values)), .ranges = true},
    {.id = 3, .entries = MakeEntries({Bytes("false"), Bytes("true")})},
    {.id = 4, .entries = MakeEntries({Bytes("only")})},
  };
  Write(fields);
  CountingDirectory counting{dir()};
  Open(counting);
  const auto& head = _idx->Terms();
  ASSERT_NE(0, head.size);
  const std::pair<uint64_t, uint64_t> extent{head.offset,
                                             head.offset + head.size};
  ASSERT_EQ(1, absl::c_count(counting.Prefetched(), extent));
  ASSERT_EQ(1, absl::c_count(counting.Evicted(), extent));
  const auto& ranged =
    static_cast<const irs::term_dict::TermReaderImpl&>(*Get(2));
  ASSERT_FALSE(ranged.RangeLevels().empty());
  ASSERT_LE(head.offset + head.size, ranged.Owner().RangesBegin());
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ") << field.id);
    AssertField(*Get(field.id), field.entries);
  }
}

TEST_P(TermDictTest, several_dicts_under_one_field) {
  std::vector<std::pair<int64_t, irs::doc_id_t>> values;
  for (irs::doc_id_t doc = 1; doc <= 5000; ++doc) {
    values.emplace_back(int64_t{doc} * 3, doc);
  }
  const Field fields[]{
    {.id = 1, .entries = MakeEntries(Words(3000, 5))},
    {.id = 1,
     .entries = NumericEntries(std::move(values)),
     .ranges = true,
     .dict = irs::TermDict::Numeric},
    {.id = 1,
     .entries = MakeEntries({Bytes("\x00"), Bytes("\xFF")}),
     .dict = irs::TermDict::Bool},
    {.id = 1,
     .entries = MakeEntries({irs::bstring{}}),
     .dict = irs::TermDict::Null},
    {.id = 2, .entries = MakeEntries(Words(3000, 6))},
    {.id = 2,
     .entries = MakeEntries({irs::bstring{}}),
     .dict = irs::TermDict::Null},
  };
  Write(fields);
  std::vector<irs::FieldKey> keys;
  for (const auto& field : fields) {
    keys.push_back({.id = field.id, .dict = field.dict});
  }
  ASSERT_EQ(keys, std::vector<irs::FieldKey>(_reader->field_keys().begin(),
                                             _reader->field_keys().end()));
  for (const auto& field : fields) {
    SCOPED_TRACE(testing::Message("field ")
                 << field.id << " dict " << static_cast<int>(field.dict));
    const auto* reader = Get(field.id, field.dict);
    ASSERT_NE(nullptr, reader);
    ASSERT_EQ(field.id, reader->meta().id);
    ASSERT_EQ(field.dict, reader->meta().dict);
    ASSERT_EQ(field.ranges, reader->meta().ranges);
    AssertField(*reader, field.entries);
  }
  ASSERT_EQ(nullptr, Get(2, irs::TermDict::Numeric));
  ASSERT_EQ(nullptr, Get(2, irs::TermDict::Bool));
  ASSERT_EQ(nullptr, Get(3));
  const irs::RegexpAcceptor any{
    irs::ViewCast<irs::byte_type>(std::string_view{".*"})};
  AssertWalk(*Get(1), fields[0].entries, any);
  AssertWalk(*Get(2), fields[4].entries, any);
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
  ASSERT_TRUE(
    static_cast<const irs::term_dict::TermReaderImpl&>(*reader).Small());
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
  const auto ladder = [](std::string_view last) {
    std::vector<irs::bstring> keys;
    for (size_t i = 0; i != 3000; ++i) {
      const auto digits = std::to_string(i);
      keys.emplace_back(
        Bytes("ay" + std::string(4 - digits.size(), '0') + digits));
    }
    keys.emplace_back(Bytes(last));
    return MakeEntries(std::move(keys));
  };
  const Field fields[]{
    {.id = 1, .entries = ladder("az")},
    {.id = 2, .entries = MakeEntries({Bytes("xb1"), Bytes("xb2")})},
    {.id = 3, .entries = ladder("az0000")},
    {.id = 4, .entries = MakeEntries({Bytes("xb1000"), Bytes("xb2000")})},
  };
  Write(fields);
  for (const irs::field_id id : {1, 3}) {
    SCOPED_TRACE(testing::Message("field ") << id);
    const auto& walked =
      static_cast<const irs::term_dict::TermReaderImpl&>(*Get(id));
    const auto& next =
      static_cast<const irs::term_dict::TermReaderImpl&>(*Get(id + 1));
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
    const auto* reader = Get(field.id);
    ASSERT_NE(nullptr, reader);
    ASSERT_EQ(
      field.sample,
      BlockKinds(static_cast<const irs::term_dict::TermReaderImpl&>(
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
    const auto* reader = Get(field.id);
    ASSERT_NE(nullptr, reader);
    ASSERT_EQ(
      field.sample,
      BlockKinds(static_cast<const irs::term_dict::TermReaderImpl&>(
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
    const auto* reader = Get(field.id);
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
    irs::PostingMeta meta;
    const size_t looked = reads([&] { meta = large->Lookup(entries[i].key); });
    ASSERT_EQ(meta.inline_size != 0 ? 2 : 1, looked);
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
    ASSERT_EQ(2, reads([&] {
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
        blocks.insert(BlockOf(impl, entry.key));
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

TEST_P(TermDictTest, walks_read_ahead) {
  const Field fields[]{{.id = 2, .entries = MakeEntries(Words(400000, 5))}};
  Write(fields);
  CountingDirectory counting{dir()};
  Open(counting);
  const auto* reader = Get(2);
  ASSERT_NE(nullptr, reader);
  const auto& impl =
    static_cast<const irs::term_dict::TermReaderImpl&>(*reader);
  ASSERT_GT(impl.Blocks(), 200);
  auto& prefetched = counting.Prefetched();
  prefetched.clear();

  const auto& entries = fields[0].entries;
  for (size_t i = 0; i < entries.size(); i += 37) {
    reader->Lookup(entries[i].key);
    auto it = reader->iterator();
    ASSERT_TRUE(it->seek(entries[i].key));
  }
  ASSERT_TRUE(prefetched.empty());

  size_t scanned = 0;
  {
    const auto& first = entries[entries.size() / 2].key;
    auto it = reader->iterator();
    ASSERT_TRUE(it->seek(first));
    for (size_t i = 0; i != 3000; ++i) {
      ASSERT_TRUE(it->next());
    }
    scanned = BlockOf(impl, it->value()) - BlockOf(impl, first);
  }
  ASSERT_GT(scanned, 1);
  ASSERT_FALSE(prefetched.empty());
  uint64_t short_scan = 0;
  for (const auto& [begin, end] : prefetched) {
    short_scan += end - begin;
  }
  ASSERT_LE(short_scan, (2 * scanned + 4) * irs::file_utils::kPage);
  prefetched.clear();

  const irs::RegexpAcceptor all{
    irs::ViewCast<irs::byte_type>(std::string_view{".*"})};
  auto it = reader->iterator(all);
  size_t terms = 0;
  while (it->next()) {
    ++terms;
  }
  ASSERT_EQ(entries.size(), terms);
  ASSERT_FALSE(prefetched.empty());
  ASSERT_LE(prefetched.size(), 16);
  absl::c_sort(prefetched);
  uint64_t covered = impl.BlockOffset(2);
  for (const auto& [begin, end] : prefetched) {
    ASSERT_LE(begin, covered);
    covered = std::max(covered, end);
  }
  ASSERT_EQ(impl.BlocksEnd(), covered);
}

TEST_P(TermDictTest, fuzzy_walk_prefetches_only_blocks_it_reads) {
  std::mt19937 rng{7};
  constexpr std::string_view kAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
  std::vector<irs::bstring> keys;
  for (size_t i = 0; i != 300000; ++i) {
    irs::bstring key(4 + rng() % 7, 0);
    for (auto& byte : key) {
      byte = static_cast<irs::byte_type>(kAlphabet[rng() % kAlphabet.size()]);
    }
    keys.push_back(std::move(key));
  }
  const Field fields[]{{.id = 2, .entries = MakeEntries(std::move(keys))}};
  Write(fields);
  CountingDirectory counting{dir()};
  Open(counting);
  const auto* reader = Get(2);
  ASSERT_NE(nullptr, reader);
  ASSERT_GT(
    static_cast<const irs::term_dict::TermReaderImpl&>(*reader).Blocks(), 500);
  auto& prefetched = counting.Prefetched();
  auto& offsets = counting.Offsets();
  const auto description = irs::MakeParametricDescription(1, false);
  for (const auto& target :
       {fields[0].entries[fields[0].entries.size() / 3].key,
        Bytes("connection")}) {
    SCOPED_TRACE(testing::Message("target ")
                 << irs::ViewCast<char>(irs::bytes_view{target}));
    const irs::LevenshteinAcceptor acceptor{description, {}, target};
    irs::file_utils::InvalidateResidency();
    prefetched.clear();
    offsets.clear();
    auto it = reader->iterator(acceptor);
    while (it->next()) {
    }
    ASSERT_FALSE(offsets.empty());
    irs::containers::FlatHashSet<uint64_t> read;
    for (const uint64_t at : offsets) {
      read.insert(at / irs::file_utils::kPage);
    }
    size_t unread = 0;
    for (const auto& [begin, end] : prefetched) {
      for (uint64_t page = begin / irs::file_utils::kPage;
           page * irs::file_utils::kPage < end; ++page) {
        unread += !read.contains(page);
      }
    }
    ASSERT_LE(unread, 4 + read.size() / 20);
  }
}

TEST_P(TermDictTest, fuzzy_walks_skip_blocks_without_their_prefixes) {
  std::mt19937 rng{11};
  constexpr std::string_view kAlphabet = "abdefghijklmpqrstuvwxyz0123456789";
  std::vector<irs::bstring> keys;
  for (size_t i = 0; i != 200000; ++i) {
    irs::bstring key(6 + rng() % 5, 0);
    for (auto& byte : key) {
      byte = static_cast<irs::byte_type>(kAlphabet[rng() % kAlphabet.size()]);
    }
    keys.push_back(std::move(key));
  }
  const Field fields[]{{.id = 2, .entries = MakeEntries(std::move(keys))}};
  Write(fields);
  CountingDirectory counting{dir()};
  Open(counting);
  const auto* reader = Get(2);
  ASSERT_NE(nullptr, reader);
  const auto& impl =
    static_cast<const irs::term_dict::TermReaderImpl&>(*reader);
  ASSERT_GT(impl.Blocks(), 300);
  const auto description = irs::MakeParametricDescription(1, false);
  const irs::LevenshteinAcceptor acceptor{description, {}, Bytes("connection")};
  auto& offsets = counting.Offsets();
  offsets.clear();
  auto it = reader->iterator(acceptor);
  ASSERT_FALSE(it->next());
  irs::containers::FlatHashSet<uint64_t> read;
  for (const uint64_t at : offsets) {
    if (at >= impl.BlockOffset(0) && at < impl.BlocksEnd()) {
      read.insert(at / irs::file_utils::kPage);
    }
  }
  ASSERT_LE(read.size(), 2);
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
  ASSERT_GT(
    static_cast<const irs::term_dict::TermReaderImpl&>(*reader).Blocks(), 10);
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

TEST_P(TermDictTest, batch_lookup_hints_before_reading) {
  const Field fields[]{{.id = 2, .entries = MakeEntries(Words(400000, 5))}};
  Write(fields);
  CountingDirectory counting{dir()};
  Open(counting);
  const auto* reader = Get(2);
  ASSERT_NE(nullptr, reader);
  const auto& entries = fields[0].entries;
  std::vector<irs::bytes_view> terms;
  for (size_t i = 0; i < entries.size(); i += 101) {
    terms.emplace_back(entries[i].key);
  }
  std::vector<irs::PostingMeta> metas(terms.size());
  auto& prefetched = counting.Prefetched();
  auto& offsets = counting.Offsets();
  auto& hinted = counting.Hinted();
  prefetched.clear();
  offsets.clear();
  hinted.clear();

  reader->LookupBatch(terms, metas);
  ASSERT_GT(absl::c_count_if(metas,
                             [](const irs::PostingMeta& meta) {
                               return meta.inline_size != 0;
                             }),
            1);
  ASSERT_FALSE(offsets.empty());
  for (size_t i = 0; i != offsets.size(); ++i) {
    const auto offset = offsets[i];
    ASSERT_TRUE(absl::c_any_of(std::span{prefetched}.first(hinted[i]),
                               [&](const auto& range) {
                                 return range.first <= offset &&
                                        offset < range.second;
                               }))
      << offset;
  }
  for (size_t i = 0; i != terms.size(); ++i) {
    AssertSameMeta(reader->Lookup(terms[i]), metas[i]);
  }
}

TEST_P(TermDictTest, evicted_warm_pages_are_prefetched_again) {
  const Field fields[]{{.id = 2, .entries = MakeEntries(Words(400000, 5))}};
  Write(fields);
  CountingDirectory counting{dir()};
  Open(counting);
  const auto* reader = Get(2);
  ASSERT_NE(nullptr, reader);
  const auto& impl =
    static_cast<const irs::term_dict::TermReaderImpl&>(*reader);
  const auto& entries = fields[0].entries;
  std::vector<irs::bytes_view> terms;
  for (size_t i = 0; i < entries.size(); i += 101) {
    terms.emplace_back(entries[i].key);
  }
  std::vector<irs::PostingMeta> metas(terms.size());
  auto& prefetched = counting.Prefetched();
  auto& offsets = counting.Offsets();
  auto& hinted = counting.Hinted();
  const irs::RegexpAcceptor all{
    irs::ViewCast<irs::byte_type>(std::string_view{".*"})};
  const auto walk = [&] {
    auto it = reader->iterator(all);
    size_t walked = 0;
    while (it->next()) {
      ++walked;
    }
    ASSERT_EQ(entries.size(), walked);
  };
  walk();
  reader->LookupBatch(terms, metas);
  irs::file_utils::SyncResidency();
  prefetched.clear();

  counting.MarkWarm();
  counting.SetResident(true);
  const auto epoch = irs::file_utils::ResidencyEpoch();
  walk();
  reader->LookupBatch(terms, metas);
  ASSERT_TRUE(prefetched.empty());
  ASSERT_EQ(epoch, irs::file_utils::ResidencyEpoch());

  counting.MarkWarm();
  counting.SetResident(false);
  walk();
  absl::c_sort(prefetched);
  uint64_t covered = impl.BlockOffset(2);
  for (const auto& [begin, end] : prefetched) {
    ASSERT_LE(begin, covered);
    covered = std::max(covered, end);
  }
  ASSERT_EQ(impl.BlocksEnd(), covered);

  std::vector<irs::bytes_view> sparse;
  for (size_t i = 0; i < entries.size(); i += entries.size() / 50) {
    sparse.emplace_back(entries[i].key);
  }
  metas.resize(sparse.size());
  counting.MarkWarm();
  prefetched.clear();
  offsets.clear();
  hinted.clear();
  reader->LookupBatch(sparse, metas);
  ASSERT_FALSE(offsets.empty());
  for (size_t i = 0; i != offsets.size(); ++i) {
    const auto offset = offsets[i];
    ASSERT_TRUE(absl::c_any_of(std::span{prefetched}.first(hinted[i]),
                               [&](const auto& range) {
                                 return range.first <= offset &&
                                        offset < range.second;
                               }))
      << offset;
  }

  counting.SetWarmLimit(irs::file_utils::kPrefetchChunk / 2);
  for (const std::string_view pattern : {"[a-z]nation.*", ".con"}) {
    SCOPED_TRACE(testing::Message("pattern ") << pattern);
    const irs::RegexpAcceptor acceptor{irs::ViewCast<irs::byte_type>(pattern)};
    ASSERT_TRUE(acceptor.ok());
    counting.MarkWarm();
    prefetched.clear();
    offsets.clear();
    auto it = reader->iterator(acceptor);
    while (it->next()) {
    }
    ASSERT_FALSE(offsets.empty());
    for (const uint64_t at : offsets) {
      ASSERT_TRUE(absl::c_any_of(prefetched,
                                 [&](const auto& range) {
                                   return range.first <= at &&
                                          at < range.second;
                                 }))
        << at;
    }
  }

  const auto scan = [&](std::vector<uint64_t>& missed) {
    prefetched.clear();
    offsets.clear();
    auto it = reader->iterator();
    ASSERT_TRUE(it->seek(entries[entries.size() / 2].key));
    for (size_t i = 0; i != 30000; ++i) {
      ASSERT_TRUE(it->next());
    }
    ASSERT_GT(offsets.size(), 32);
    for (const uint64_t at : offsets) {
      if (!absl::c_any_of(prefetched, [&](const auto& range) {
            return range.first <= at && at < range.second;
          })) {
        missed.push_back(at);
      }
    }
  };
  std::vector<uint64_t> fresh;
  irs::file_utils::InvalidateResidency();
  scan(fresh);
  std::vector<uint64_t> stale;
  counting.MarkWarm();
  scan(stale);
  ASSERT_EQ(fresh, stale);
}

TEST_P(TermDictTest, only_large_batches_check_the_page_cache) {
  const Field fields[]{{.id = 2, .entries = MakeEntries(Words(400000, 5))}};
  Write(fields);
  CountingDirectory counting{dir()};
  Open(counting);
  const auto* reader = Get(2);
  ASSERT_NE(nullptr, reader);
  const auto& entries = fields[0].entries;
  const auto lookup = [&](size_t count) {
    std::vector<irs::bytes_view> terms;
    for (size_t i = 0; i != count; ++i) {
      terms.emplace_back(entries[i * (entries.size() / count)].key);
    }
    std::vector<irs::PostingMeta> metas(terms.size());
    reader->LookupBatch(terms, metas);
  };
  lookup(50);
  irs::file_utils::SyncResidency();

  counting.MarkWarm();
  counting.SetResident(true);
  counting.ResidentChecks() = 0;
  lookup(2);
  ASSERT_EQ(0, counting.ResidentChecks());
  lookup(50);
  ASSERT_LT(0, counting.ResidentChecks());
}

TEST_P(TermDictTest, only_wide_walks_check_the_page_cache) {
  const Field fields[]{{.id = 2, .entries = MakeEntries(Words(400000, 5))}};
  Write(fields);
  CountingDirectory counting{dir()};
  Open(counting);
  const auto* reader = Get(2);
  ASSERT_NE(nullptr, reader);
  const auto walk = [&](std::string_view pattern) {
    const irs::RegexpAcceptor acceptor{irs::RegexpAcceptor::WildcardTag{},
                                       irs::ViewCast<irs::byte_type>(pattern)};
    auto it = reader->iterator(acceptor);
    while (it->next()) {
    }
  };
  walk("%");
  walk("conion%");
  irs::file_utils::SyncResidency();

  counting.MarkWarm();
  counting.SetResident(true);
  counting.ResidentChecks() = 0;
  walk("conion%");
  ASSERT_EQ(0, counting.ResidentChecks());
  walk("%");
  ASSERT_LT(0, counting.ResidentChecks());
}

TEST_P(TermDictTest, scans_read_ahead_to_their_bound) {
  const Field fields[]{{.id = 2, .entries = MakeEntries(Words(400000, 5))}};
  Write(fields);
  CountingDirectory counting{dir()};
  Open(counting);
  const auto* reader = Get(2);
  ASSERT_NE(nullptr, reader);
  const auto& impl =
    static_cast<const irs::term_dict::TermReaderImpl&>(*reader);
  auto& prefetched = counting.Prefetched();
  const auto inside = [&](irs::bytes_view lower, irs::bytes_view upper) {
    const uint64_t begin = impl.BlockOffset(BlockOf(impl, lower));
    const uint64_t end = impl.BlockOffset(BlockOf(impl, upper) + 1);
    for (const auto& [from, to] : prefetched) {
      ASSERT_LE(begin, from);
      ASSERT_LE(to, end);
    }
  };

  for (const std::string_view prefix : {"inter", "con", "s"}) {
    SCOPED_TRACE(testing::Message("prefix ") << prefix);
    prefetched.clear();
    const auto lower = Bytes(prefix);
    auto upper = lower;
    ++upper.back();
    auto it = reader->IteratorUntil(upper);
    ASSERT_NE(irs::SeekResult::End, it->seek_ge(lower));
    while (it->value() < irs::bytes_view{upper} && it->next()) {
    }
    inside(lower, upper);
    const auto first = BlockOf(impl, lower);
    const auto last = BlockOf(impl, upper);
    ASSERT_GT(last, first + 1);
    absl::c_sort(prefetched);
    uint64_t covered = impl.BlockOffset(first + 1);
    for (const auto& [from, to] : prefetched) {
      ASSERT_LE(from, covered);
      covered = std::max(covered, to);
    }
    ASSERT_EQ(impl.BlockOffset(last + 1), covered);
  }

  for (const std::string_view pattern : {"inter.*", "con.*ing", "s.*ion"}) {
    SCOPED_TRACE(testing::Message("pattern ") << pattern);
    prefetched.clear();
    const irs::RegexpAcceptor acceptor{irs::ViewCast<irs::byte_type>(pattern)};
    ASSERT_TRUE(acceptor.ok());
    auto it = reader->iterator(acceptor);
    while (it->next()) {
    }
    const auto lower = Bytes(pattern.substr(0, pattern.find('.')));
    auto upper = lower;
    ++upper.back();
    inside(lower, upper);
    ASSERT_EQ(1, prefetched.size());
    ASSERT_EQ(impl.BlockOffset(BlockOf(impl, lower)), prefetched[0].first);
    ASSERT_EQ(impl.BlockOffset(BlockOf(impl, upper) + 1), prefetched[0].second);
  }
}

TEST_P(TermDictTest, sparse_walks_prefetch_the_blocks_they_read) {
  const Field fields[]{{.id = 2, .entries = MakeEntries(Words(400000, 5))}};
  Write(fields);
  CountingDirectory counting{dir()};
  Open(counting);
  const auto* reader = Get(2);
  ASSERT_NE(nullptr, reader);
  const auto& impl =
    static_cast<const irs::term_dict::TermReaderImpl&>(*reader);
  auto& prefetched = counting.Prefetched();
  auto& offsets = counting.Offsets();
  const uint64_t field_bytes = impl.BlocksEnd() - impl.BlockOffset(0);
  for (const std::string_view pattern :
       {"[a-z]nation.*", ".con", "[0-9a-z]al[0-9]+"}) {
    SCOPED_TRACE(testing::Message("pattern ") << pattern);
    const irs::RegexpAcceptor acceptor{irs::ViewCast<irs::byte_type>(pattern)};
    ASSERT_TRUE(acceptor.ok());
    prefetched.clear();
    offsets.clear();
    auto it = reader->iterator(acceptor);
    while (it->next()) {
    }
    ASSERT_FALSE(offsets.empty());
    uint64_t bytes = 0;
    for (const auto& [from, to] : prefetched) {
      bytes += to - from;
    }
    ASSERT_LT(2 * bytes, field_bytes);
    for (const uint64_t at : offsets) {
      ASSERT_TRUE(absl::c_any_of(prefetched, [&](const auto& range) {
        return range.first <= at && at < range.second;
      }));
    }
  }
}

TEST_P(TermDictTest, walks_read_inline_postings_ahead) {
  const Field fields[]{{.id = 2, .entries = MakeEntries(Words(400000, 5))}};
  Write(fields);
  CountingDirectory counting{dir()};
  Open(counting);
  const auto* reader = Get(2);
  ASSERT_NE(nullptr, reader);
  const auto& impl =
    static_cast<const irs::term_dict::TermReaderImpl&>(*reader);
  ASSERT_LT(impl.InlineBegin(), impl.InlineEnd());
  auto& prefetched = counting.Prefetched();
  auto& offsets = counting.Offsets();
  const auto inlined = [&](uint64_t at) {
    return impl.InlineBegin() <= at && at < impl.InlineEnd();
  };
  const auto walk = [&](std::string_view pattern) {
    const irs::RegexpAcceptor acceptor{irs::ViewCast<irs::byte_type>(pattern)};
    EXPECT_TRUE(acceptor.ok());
    prefetched.clear();
    offsets.clear();
    auto it = reader->iterator(acceptor);
    size_t cookies = 0;
    while (it->next()) {
      cookies += it->cookie().inline_size != 0;
    }
    std::vector<uint64_t> reads;
    absl::c_copy_if(offsets, std::back_inserter(reads), inlined);
    EXPECT_EQ(cookies, reads.size());
    std::erase_if(prefetched,
                  [&](const auto& range) { return !inlined(range.first); });
    return reads;
  };

  for (const std::string_view pattern : {".*", ".*al.*", "s.*", "con.*"}) {
    SCOPED_TRACE(testing::Message("pattern ") << pattern);
    const auto reads = walk(pattern);
    ASSERT_GT(reads.size(), 100);
    ASSERT_TRUE(absl::c_is_sorted(reads));
    const uint64_t first = reads.front() / irs::file_utils::kPage;
    for (const uint64_t at : reads) {
      if (at / irs::file_utils::kPage == first) {
        continue;
      }
      ASSERT_TRUE(absl::c_any_of(prefetched, [&](const auto& range) {
        return range.first <= at && at < range.second;
      }));
    }
    uint64_t bytes = 0;
    for (const auto& [from, to] : prefetched) {
      ASSERT_LE(to, impl.InlineEnd());
      bytes += to - from;
    }
    ASSERT_LE(bytes,
              reads.back() - reads.front() + irs::file_utils::kMaxReadahead);
  }

  const auto& entries = fields[0].entries;
  for (size_t i = 0; i < entries.size(); i += entries.size() / 7) {
    const auto& key = entries[i].key;
    SCOPED_TRACE(testing::Message("term ")
                 << irs::ViewCast<char>(irs::bytes_view{key}));
    const auto reads = walk(irs::ViewCast<char>(irs::bytes_view{key}));
    ASSERT_LE(reads.size(), 1);
    ASSERT_TRUE(prefetched.empty());
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

TEST_P(TermDictTest, read_failures) {
  const Field fields[]{{.id = 2, .entries = MakeEntries(Words(20000, 5))}};
  Write(fields);
  CountingDirectory counting{dir()};
  Open(counting);
  const auto* reader = Get(2);
  ASSERT_NE(nullptr, reader);
  const auto& impl =
    static_cast<const irs::term_dict::TermReaderImpl&>(*reader);
  ASSERT_GT(impl.Blocks(), 2);
  const auto& entries = fields[0].entries;

  {
    counting.FailReads(true);
    auto it = reader->iterator();
    ASSERT_SQL_ERROR(it->next(), ERRCODE_IO_ERROR);
    counting.FailReads(false);
  }

  auto it = reader->iterator();
  ASSERT_TRUE(it->next());
  size_t last = 0;
  while (BlockOf(impl, entries[last + 1].key) == 0) {
    ASSERT_TRUE(it->next());
    ++last;
  }
  ASSERT_EQ(entries[last].key, it->value());
  const auto& first = entries[last + 1].key;

  counting.FailReads(true);
  ASSERT_SQL_ERROR(it->next(), ERRCODE_IO_ERROR);
  counting.FailReads(false);
  ASSERT_EQ(irs::SeekResult::Found, it->seek_ge(first));
  ASSERT_EQ(first, it->value());
  ASSERT_EQ(entries[last + 1].docs.size(), it->cookie().docs_count);
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
  if (!_reader->Base()) {
    GTEST_SKIP() << "only a mapped input reads blocks in place";
  }
  const auto& words =
    static_cast<const irs::term_dict::TermReaderImpl&>(*Get(2));
  ASSERT_NE(nullptr, words.Fsst());
  ASSERT_NE(
    0,
    BlockKinds(words)[static_cast<size_t>(irs::term_dict::BlockKind::VarFsst)]);
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

TEST_P(TermDictTest, range_levels_oracle) { RangeLevelsOracle(false); }

TEST_P(TermDictTest, range_levels_oracle_compaction) {
  RangeLevelsOracle(true);
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
    {.id = 1, .entries = NumericEntries(std::move(values)), .ranges = true},
  };
  Write(fields);
  if (!_reader->Base()) {
    GTEST_SKIP() << "only a mapped input reads blocks in place";
  }
  const auto* reader = Get(1);
  ASSERT_NE(nullptr, reader);
  const auto bounds = Probes(fields[0].entries);
  std::mt19937 rng{5};
  for (size_t round = 0; round != 500; ++round) {
    const irs::bytes_view from = bounds[rng() % bounds.size()];
    const irs::bytes_view to = bounds[rng() % bounds.size()];
    const uint64_t before = ThreadAllocatedBytes();
    reader->VisitRange(from, to,
                       [](irs::bytes_view, const irs::PostingMeta&) {});
    ASSERT_EQ(before, ThreadAllocatedBytes());
  }
}

INSTANTIATE_TEST_SUITE_P(TermDictTest, TermDictTest,
                         ::testing::Combine(::testing::ValuesIn(
                           tests::GetDirectories<tests::kTypesDefault>())),
                         TermDictTest::to_string);

template<typename Root>
void AssertRoot(std::vector<irs::bstring> separators) {
  absl::c_sort(separators);
  separators.erase(std::unique(separators.begin(), separators.end()),
                   separators.end());
  const std::vector<irs::bytes_view> views{separators.begin(),
                                           separators.end()};
  const Root root{irs::term_dict::MakeSeparators(views)};
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
    const size_t expected = absl::c_upper_bound(views, key) - views.begin();
    ASSERT_EQ(expected, root.Find(key));
    for (const size_t from : {size_t{0}, expected / 2, expected}) {
      ASSERT_EQ(expected, root.FindFrom(from, key));
    }
    ASSERT_EQ(views.size() + 1, root.FindFrom(views.size() + 1, key));
    for (size_t block = 1; block < root.Blocks(); block += 1 + block / 64) {
      ASSERT_EQ(views[block - 1] <= key, root.NotAfter(block, key));
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
  std::vector<irs::bstring> wide;
  for (size_t high = 2; high < 256; high += 1 + high % 3) {
    const size_t fanout = high % 5 == 0 ? 40 : high % 7 == 0 ? 100 : 3;
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
  explicit ChosenRoot(irs::term_dict::Separators separators)
    : Root{std::move(separators), Fixed} {}
};

TEST(TermDictRootTest, root_matches_upper_bound) {
  AssertRoots<ChosenRoot<false>>();
  AssertRoots<ChosenRoot<true>>();
}

}  // namespace
