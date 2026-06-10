////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2018 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <thread>

#include "formats/column/test_cs_helpers.hpp"
#include "index_tests.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/utils/index_utils.hpp"
#include "tests_shared.hpp"

namespace {

// Per-doc cs column id used by tests in this file. Holds the bytes of the
// indexed StringField named "name"; readback through `segment.Column(kNameId)`
// + `BlobPointReader` decodes back to the original string with `ReadStoredStr`.
// Also used as the field_id of the "name" StringField for term filters that
// delete by name (`MakeByTerm(kNameId, "B")`), so it must equal
// `FieldIdFor("name")`.
inline constexpr irs::field_id kNameId = tests::FieldIdFor("name");
// Stable per-name field ids used by `segment.field(...)` lookups in this
// file. Tests only use these to address the term reader for a known JSON
// field name; the on-disk index keys fields by id.
inline constexpr irs::field_id kSameId = tests::FieldIdFor("same");
inline constexpr irs::field_id kSameAnlPayId =
  tests::FieldIdFor("same_anl_pay");

auto MakeByTerm(irs::field_id field_id, std::string_view value) {
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field_id() = field_id;
  filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(value);
  return filter;
}

// Insert `doc->indexed` into `writer` and capture the indexed StringField
// named "name" into the cs column under `kNameId` at the same DocId. Tests
// then read it back with `segment.Column(kNameId)` + `BlobPointReader`.
bool InsertWithName(irs::IndexWriter& writer, const tests::Document& doc) {
  auto ctx = writer.GetBatch();
  {
    auto d = ctx.Insert();
    if (!d.Insert(doc.indexed.begin(), doc.indexed.end())) {
      return false;
    }
    const auto* name =
      dynamic_cast<const tests::StringField*>(doc.indexed.get_by_id(kNameId));
    if (name != nullptr) {
      if (auto* cs = d.GetColWriter(); cs != nullptr) {
        irs::tests::StoreFieldAt(*cs, kNameId, d.DocId(), *name);
      }
    }
  }
  ctx.Commit();
  return true;
}

class FailingDirectory : public tests::DirectoryMock {
 public:
  enum class Failure : size_t {
    CREATE = 0,
    EXISTS,
    LENGTH,
    MakeLock,
    MTIME,
    OPEN,
    RENAME,
    REMOVE,
    SYNC,
    REOPEN,
    ReopenNull,  // return nullptr from index_input::reopen
    DUP,
    DupNull  // return nullptr from index_input::dup
  };

 private:
  class FailingIndexInput : public irs::IndexInput {
   public:
    explicit FailingIndexInput(IndexInput::ptr&& impl, std::string_view name,
                               const FailingDirectory& dir)
      : _impl(std::move(impl)), _dir(&dir), _name(name) {}

    const irs::byte_type* ReadData(uint64_t count) final {
      return _impl->ReadData(count);
    }
    const irs::byte_type* ReadData(uint64_t offset, uint64_t count) final {
      return _impl->ReadData(offset, count);
    }

    const irs::byte_type* ReadView(uint64_t offset, uint64_t count) final {
      return _impl->ReadView(offset, count);
    }
    const irs::byte_type* ReadView(uint64_t count) final {
      return _impl->ReadView(count);
    }

    irs::byte_type ReadByte() final { return _impl->ReadByte(); }
    size_t ReadBytes(irs::byte_type* b, size_t count) final {
      return _impl->ReadBytes(b, count);
    }
    size_t ReadBytes(uint64_t offset, irs::byte_type* b, size_t count) final {
      return _impl->ReadBytes(offset, b, count);
    }

    int16_t ReadI16() final { return _impl->ReadI16(); }
    int32_t ReadI32() final { return _impl->ReadI32(); }
    int64_t ReadI64() final { return _impl->ReadI64(); }
    uint32_t ReadV32() final { return _impl->ReadV32(); }
    uint64_t ReadV64() final { return _impl->ReadV64(); }

    uint64_t Position() const noexcept final { return _impl->Position(); }
    uint64_t Length() const noexcept final { return _impl->Length(); }
    bool IsEOF() const noexcept final { return _impl->IsEOF(); }

    ptr Dup() const final {
      if (_dir->ShouldFail(Failure::DUP, _name)) {
        throw irs::IoError();
      }

      if (_dir->ShouldFail(Failure::DupNull, _name)) {
        return nullptr;
      }

      return std::make_unique<FailingIndexInput>(_impl->Dup(), this->_name,
                                                 *this->_dir);
    }
    ptr Reopen() const final {
      if (_dir->ShouldFail(Failure::REOPEN, _name)) {
        throw irs::IoError();
      }

      if (_dir->ShouldFail(Failure::ReopenNull, _name)) {
        return nullptr;
      }

      return std::make_unique<FailingIndexInput>(_impl->Reopen(), this->_name,
                                                 *this->_dir);
    }
    void Skip(uint64_t count) final { _impl->Skip(count); }
    void Seek(uint64_t pos) final { _impl->Seek(pos); }

    uint32_t Checksum(uint64_t offset) const final {
      return _impl->Checksum(offset);
    }

   private:
    IndexInput::ptr _impl;
    const FailingDirectory* _dir;
    std::string _name;
  };

 public:
  explicit FailingDirectory(irs::Directory& impl) noexcept
    : tests::DirectoryMock(impl) {}

  bool RegisterFailure(Failure type, const std::string& name) {
    return _failures.emplace(name, type).second;
  }

  void ClearFailures() noexcept { _failures.clear(); }

  size_t NumFailures() const noexcept { return _failures.size(); }

  bool NoFailures() const noexcept { return _failures.empty(); }

  irs::IndexOutput::ptr create(std::string_view name) noexcept final {
    if (ShouldFail(Failure::CREATE, name)) {
      return nullptr;
    }

    return tests::DirectoryMock::create(name);
  }
  bool exists(bool& result, std::string_view name) const noexcept final {
    if (ShouldFail(Failure::EXISTS, name)) {
      return false;
    }

    return tests::DirectoryMock::exists(result, name);
  }
  bool length(uint64_t& result, std::string_view name) const noexcept final {
    if (ShouldFail(Failure::LENGTH, name)) {
      return false;
    }

    return tests::DirectoryMock::length(result, name);
  }
  irs::IndexLock::ptr make_lock(std::string_view name) noexcept final {
    if (ShouldFail(Failure::MakeLock, name)) {
      return nullptr;
    }

    return tests::DirectoryMock::make_lock(name);
  }
  bool mtime(std::time_t& result, std::string_view name) const noexcept final {
    if (ShouldFail(Failure::MTIME, name)) {
      return false;
    }

    return tests::DirectoryMock::mtime(result, name);
  }
  irs::IndexInput::ptr open(std::string_view name,
                            irs::IOAdvice advice) const noexcept final {
    if (ShouldFail(Failure::OPEN, name)) {
      return nullptr;
    }

    return std::make_unique<FailingIndexInput>(
      tests::DirectoryMock::open(name, advice), name, *this);
  }
  bool remove(std::string_view name) noexcept final {
    if (ShouldFail(Failure::REMOVE, name)) {
      return false;
    }

    return tests::DirectoryMock::remove(name);
  }
  bool rename(std::string_view src, std::string_view dst) noexcept final {
    if (ShouldFail(Failure::RENAME, src)) {
      return false;
    }

    return tests::DirectoryMock::rename(src, dst);
  }
  bool sync(std::span<const std::string_view> files) noexcept final {
    return std::all_of(std::begin(files), std::end(files),
                       [this](std::string_view name) mutable noexcept {
                         if (ShouldFail(Failure::SYNC, name)) {
                           return false;
                         }

                         return tests::DirectoryMock::sync({&name, 1});
                       });
  }

 private:
  bool ShouldFail(Failure type, std::string_view name) const {
    auto it = _failures.find(std::make_pair(std::string{name}, type));

    if (_failures.end() != it) {
      _failures.erase(it);
      return true;
    }

    return false;
  }

  typedef std::pair<std::string, Failure> FailT;

  struct FailLess {
    bool operator()(const FailT& lhs, const FailT& rhs) const noexcept {
      if (lhs.second == rhs.second) {
        return lhs.first < rhs.first;
      }

      return lhs.second < rhs.second;
    }
  };

  mutable std::set<FailT, FailLess> _failures;
};

void OpenReader(std::string_view format,
                std::function<void(FailingDirectory& dir)> failure_registerer) {
  constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                              irs::IndexFeatures::Pos |
                                              irs::IndexFeatures::Offs;

  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();
  auto query_doc2 = MakeByTerm(kNameId, "B");

  auto codec = irs::formats::Get(format);
  ASSERT_NE(nullptr, codec);

  // create source segment
  irs::MemoryDirectory impl;
  FailingDirectory dir(impl);

  // write index
  {
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));

    tests::Remove(*writer, *query_doc2);

    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));
  }

  failure_registerer(dir);

  while (!dir.NoFailures()) {
    ASSERT_THROW(
      (irs::DirectoryReader{dir, codec, irs::tests::DefaultReaderOptions()}),
      irs::IoError);
  }

  // check data
  auto reader =
    irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
  ASSERT_TRUE(reader);
  ASSERT_EQ(1, reader->size());
  ASSERT_EQ(2, reader->docs_count());
  ASSERT_EQ(1, reader->live_docs_count());

  // validate index
  tests::index_t expected_index;
  expected_index.emplace_back();
  expected_index.back().insert(*doc1);
  expected_index.back().insert(*doc2);
  tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

  // validate columnstore
  auto& segment = reader[0];
  const auto* column = segment.Column(kNameId);
  ASSERT_NE(nullptr, column);
  irs::tests::BlobPointReader values{segment, *column};
  ASSERT_EQ(2, segment.docs_count());
  ASSERT_EQ(1, segment.live_docs_count());
  auto terms = segment.field(kSameId);
  ASSERT_NE(nullptr, terms);
  auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
  ASSERT_TRUE(term_itr->next());
  auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
  ASSERT_TRUE(docs_itr->next());
  ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                   values, docs_itr->value()));
  ASSERT_TRUE(docs_itr->next());
  ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                   values, docs_itr->value()));
  ASSERT_FALSE(docs_itr->next());

  // validate live docs
  auto live_docs = segment.docs_iterator();
  ASSERT_TRUE(live_docs->next());
  ASSERT_EQ(1, live_docs->value());
  ASSERT_FALSE(live_docs->next());
  ASSERT_EQ(irs::doc_limits::eof(), live_docs->value());
}

}  // namespace

TEST(index_death_test_formats_15, index_meta_write_fail_1st_phase) {
  tests::JsonDocGenerator gen(
    TestBase::resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::StringField>(name, data.str);
        field->id = tests::FieldIdFor(name);
        doc.insert(std::move(field));
      }
    });
  const auto* doc1 = gen.next();

  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);

  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(
      FailingDirectory::Failure::CREATE,
      "pending_segments_1");  // fail first phase of transaction
    dir.RegisterFailure(
      FailingDirectory::Failure::SYNC,
      "pending_segments_1");  // fail first phase of transaction

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    ASSERT_THROW(writer->RefreshBegin(), irs::IoError);  // creation failure
    ASSERT_THROW(writer->RefreshBegin(),
                 irs::IoError);  // synchronization failure

    // successful attempt
    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    // ensure no data
    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(0, reader->size());
    ASSERT_EQ(0, reader->docs_count());
    ASSERT_EQ(0, reader->live_docs_count());
  }

  {
    constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                                irs::IndexFeatures::Pos |
                                                irs::IndexFeatures::Offs;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(
      FailingDirectory::Failure::CREATE,
      "pending_segments_1");  // fail first phase of transaction
    dir.RegisterFailure(
      FailingDirectory::Failure::SYNC,
      "pending_segments_1");  // fail first phase of transaction

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    ASSERT_THROW(writer->RefreshBegin(), irs::IoError);  // creation failure
    ASSERT_THROW(writer->RefreshBegin(),
                 irs::IoError);  // synchronization failure

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    // successful attempt
    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());

    // check data
    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    tests::AssertSnapshotEquality(writer->GetSnapshot(), reader);
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(1, reader->docs_count());
    ASSERT_EQ(1, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    ASSERT_EQ(1, segment.docs_count());
    ASSERT_EQ(1, segment.live_docs_count());
    auto terms = segment.field(kSameId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));
    ASSERT_FALSE(docs_itr->next());
  }
}

TEST(index_death_test_formats_15, index_commit_fail_sync_1st_phase) {
  tests::JsonDocGenerator gen(
    TestBase::resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::StringField>(name, data.str);
        field->id = tests::FieldIdFor(name);
        doc.insert(std::move(field));
      }
    });
  const auto* doc1 = gen.next();

  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);

  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_1.0.sm");  // unable to sync segment meta
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_2.doc");  // unable to sync postings
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_3.idx");  // unable to sync term index + data

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    ASSERT_THROW(writer->RefreshBegin(),
                 irs::IoError);  // synchronization failure

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    ASSERT_THROW(writer->RefreshBegin(),
                 irs::IoError);  // synchronization failure

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    ASSERT_THROW(writer->RefreshBegin(),
                 irs::IoError);  // synchronization failure

    // successful attempt
    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());

    // ensure no data
    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    tests::AssertSnapshotEquality(writer->GetSnapshot(), reader);
    ASSERT_TRUE(reader);
    ASSERT_EQ(0, reader->size());
    ASSERT_EQ(0, reader->docs_count());
    ASSERT_EQ(0, reader->live_docs_count());
  }

  {
    constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                                irs::IndexFeatures::Pos |
                                                irs::IndexFeatures::Offs;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_1.0.sm");  // unable to sync segment meta
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_2.doc");  // unable to sync postings
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_3.idx");  // unable to sync term index + data

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // initial commit
    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    ASSERT_THROW(writer->RefreshBegin(),
                 irs::IoError);            // synchronization failure
    ASSERT_FALSE(writer->RefreshBegin());  // nothing to flush

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    ASSERT_THROW(writer->RefreshBegin(),
                 irs::IoError);            // synchronization failure
    ASSERT_FALSE(writer->RefreshBegin());  // nothing to flush

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    ASSERT_THROW(writer->RefreshBegin(),
                 irs::IoError);            // synchronization failure
    ASSERT_FALSE(writer->RefreshBegin());  // nothing to flush

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    // successful attempt
    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());

    // check data
    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    tests::AssertSnapshotEquality(writer->GetSnapshot(), reader);
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(1, reader->docs_count());
    ASSERT_EQ(1, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    ASSERT_EQ(1, segment.docs_count());
    ASSERT_EQ(1, segment.live_docs_count());
    auto terms = segment.field(kSameId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));
    ASSERT_FALSE(docs_itr->next());
  }
}

TEST(index_death_test_formats_15, index_meta_write_failure_2nd_phase) {
  tests::JsonDocGenerator gen(
    TestBase::resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::StringField>(name, data.str);
        field->id = tests::FieldIdFor(name);
        doc.insert(std::move(field));
      }
    });
  const auto* doc1 = gen.next();

  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);

  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    // fail second phase of transaction
    dir.RegisterFailure(FailingDirectory::Failure::RENAME,
                        "pending_segments_1");

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_THROW(writer->RefreshCommit(), irs::IoError);
    ASSERT_THROW(
      (irs::DirectoryReader{dir, codec, irs::tests::DefaultReaderOptions()}),
      irs::IndexNotFound);

    // second attempt
    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    // ensure no data
    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(0, reader->size());
    ASSERT_EQ(0, reader->docs_count());
    ASSERT_EQ(0, reader->live_docs_count());
  }

  {
    constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                                irs::IndexFeatures::Pos |
                                                irs::IndexFeatures::Offs;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(
      FailingDirectory::Failure::RENAME,
      "pending_segments_1");  // fail second phase of transaction

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_THROW(writer->RefreshCommit(), irs::IoError);
    ASSERT_THROW(
      (irs::DirectoryReader{dir, codec, irs::tests::DefaultReaderOptions()}),
      irs::IndexNotFound);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    // second attempt
    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    // check data
    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(1, reader->docs_count());
    ASSERT_EQ(1, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    ASSERT_EQ(1, segment.docs_count());
    ASSERT_EQ(1, segment.live_docs_count());
    auto terms = segment.field(kSameId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));
    ASSERT_FALSE(docs_itr->next());
  }
}

TEST(index_death_test_formats_15,
     segment_meta_creation_failure_1st_phase_flush) {
  tests::JsonDocGenerator gen(
    TestBase::resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::StringField>(name, data.str);
        field->id = tests::FieldIdFor(name);
        doc.insert(std::move(field));
      }
    });
  const auto* doc1 = gen.next();

  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);

  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_1.0.sm");  // fail at segment meta creation
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_2.0.sm");  // fail at segment meta synchronization

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // creation issue
    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    ASSERT_THROW(writer->RefreshBegin(), irs::IoError);

    // synchornization issue
    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    ASSERT_THROW(writer->RefreshBegin(), irs::IoError);

    // second attempt
    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    // ensure no data
    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(0, reader->size());
    ASSERT_EQ(0, reader->docs_count());
    ASSERT_EQ(0, reader->live_docs_count());
  }

  {
    constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                                irs::IndexFeatures::Pos |
                                                irs::IndexFeatures::Offs;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_1.0.sm");  // fail at segment meta creation
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_2.0.sm");  // fail at segment meta synchronization

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // creation issue
    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    ASSERT_THROW(writer->RefreshBegin(), irs::IoError);

    // synchornization issue
    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    ASSERT_THROW(writer->RefreshBegin(), irs::IoError);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    // second attempt
    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    // check data
    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(1, reader->docs_count());
    ASSERT_EQ(1, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    ASSERT_EQ(1, segment.docs_count());
    ASSERT_EQ(1, segment.live_docs_count());
    auto terms = segment.field(kSameId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));
    ASSERT_FALSE(docs_itr->next());
  }
}

TEST(index_death_test_formats_15,
     segment_meta_write_fail_immediate_compaction) {
  tests::JsonDocGenerator gen(
    TestBase::resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::StringField>(name, data.str);
        field->id = tests::FieldIdFor(name);
        doc.insert(std::move(field));
      }
    });
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();

  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);

  {
    constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                                irs::IndexFeatures::Pos |
                                                irs::IndexFeatures::Offs;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 0
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    // register failures
    dir.RegisterFailure(
      FailingDirectory::Failure::CREATE,
      "_3.0.sm");  // fail at segment meta creation on compaction
    dir.RegisterFailure(
      FailingDirectory::Failure::SYNC,
      "_4.0.sm");  // fail at segment meta synchronization on compaction

    const irs::index_utils::CompactionCount compact_all;

    // segment meta creation failure
    ASSERT_THROW(writer->Compact(irs::index_utils::MakePolicy(compact_all)),
                 irs::IoError);
    ASSERT_FALSE(writer->RefreshBegin());  // nothing to flush

    // segment meta synchronization failure
    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(compact_all)));
    ASSERT_THROW(writer->RefreshBegin(), irs::IoError);
    ASSERT_FALSE(writer->RefreshBegin());  // nothing to flush

    // check data
    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader->size());
    ASSERT_EQ(2, reader->docs_count());
    ASSERT_EQ(2, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    expected_index.emplace_back();
    expected_index.back().insert(*doc2);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore (segment 0)
    {
      auto& segment = reader[0];  // assume 0 is id of first/only segment
      const auto* column = segment.Column(kNameId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(1, segment.docs_count());
      ASSERT_EQ(1, segment.live_docs_count());
      auto terms = segment.field(kSameId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));
      ASSERT_FALSE(docs_itr->next());
    }

    // validate columnstore (segment 1)
    {
      auto& segment = reader[1];  // assume 0 is id of first/only segment
      const auto* column = segment.Column(kNameId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(1, segment.docs_count());
      ASSERT_EQ(1, segment.live_docs_count());
      auto terms = segment.field(kSameId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));
      ASSERT_FALSE(docs_itr->next());
    }
  }
}

TEST(index_death_test_formats_15, segment_meta_write_fail_deffered_compaction) {
  tests::JsonDocGenerator gen(
    TestBase::resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::StringField>(name, data.str);
        field->id = tests::FieldIdFor(name);
        doc.insert(std::move(field));
      }
    });
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();
  const auto* doc3 = gen.next();
  const auto* doc4 = gen.next();

  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);

  {
    constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                                irs::IndexFeatures::Pos |
                                                irs::IndexFeatures::Offs;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 0
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    // register failures
    dir.RegisterFailure(
      FailingDirectory::Failure::CREATE,
      "_4.0.sm");  // fail at segment meta creation on compaction
    dir.RegisterFailure(
      FailingDirectory::Failure::SYNC,
      "_6.0.sm");  // fail at segment meta synchronization on compaction

    const irs::index_utils::CompactionCount compact_all;

    // segment meta creation failure
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(writer->RefreshBegin());  // start transaction
    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(
      compact_all)));                       // register pending compaction
    ASSERT_FALSE(writer->RefreshCommit());  // commit started transaction
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));
    ASSERT_THROW(
      writer->RefreshBegin(),
      irs::IoError);  // start transaction to commit pending compaction

    // segment meta synchronization failure
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    ASSERT_TRUE(writer->RefreshBegin());  // start transaction
    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(
      compact_all)));                       // register pending compaction
    ASSERT_FALSE(writer->RefreshCommit());  // commit started transaction
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));
    ASSERT_THROW(
      writer->RefreshBegin(),
      irs::IoError);  // start transaction to commit pending compaction

    // check data
    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(4, reader->size());
    ASSERT_EQ(4, reader->docs_count());
    ASSERT_EQ(4, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    expected_index.emplace_back();
    expected_index.back().insert(*doc2);
    expected_index.emplace_back();
    expected_index.back().insert(*doc3);
    expected_index.emplace_back();
    expected_index.back().insert(*doc4);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore (segment 0)
    {
      auto& segment = reader[0];  // assume 0 is id of first/only segment
      const auto* column = segment.Column(kNameId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(1, segment.docs_count());
      ASSERT_EQ(1, segment.live_docs_count());
      auto terms = segment.field(kSameId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));
      ASSERT_FALSE(docs_itr->next());
    }

    // validate columnstore (segment 1)
    {
      auto& segment = reader[1];  // assume 0 is id of first/only segment
      const auto* column = segment.Column(kNameId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(1, segment.docs_count());
      ASSERT_EQ(1, segment.live_docs_count());
      auto terms = segment.field(kSameId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));
      ASSERT_FALSE(docs_itr->next());
    }

    // validate columnstore (segment 2)
    {
      auto& segment = reader[2];  // assume 0 is id of first/only segment
      const auto* column = segment.Column(kNameId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(1, segment.docs_count());
      ASSERT_EQ(1, segment.live_docs_count());
      auto terms = segment.field(kSameId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));
      ASSERT_FALSE(docs_itr->next());
    }

    // validate columnstore (segment 3)
    {
      auto& segment = reader[3];  // assume 0 is id of first/only segment
      const auto* column = segment.Column(kNameId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(1, segment.docs_count());
      ASSERT_EQ(1, segment.live_docs_count());
      auto terms = segment.field(kSameId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));
      ASSERT_FALSE(docs_itr->next());
    }
  }
}

TEST(index_death_test_formats_15, open_reader) {
  ::OpenReader("1_5simd", [](FailingDirectory& dir) {
    // postings list (documents)
    dir.RegisterFailure(FailingDirectory::Failure::OPEN, "_1.doc");
    // columnstore
    dir.RegisterFailure(FailingDirectory::Failure::OPEN, "_1.col");
    // term dictionary (.idx: term index + term data)
    dir.RegisterFailure(FailingDirectory::Failure::OPEN, "_1.idx");
    // postings list (positions)
    dir.RegisterFailure(FailingDirectory::Failure::OPEN, "_1.pos");
    // postings list (offset + payload)
    dir.RegisterFailure(FailingDirectory::Failure::OPEN, "_1.pay");
  });
}

TEST(index_death_test_formats_15, postings_reopen_fail) {
  constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                              irs::IndexFeatures::Pos |
                                              irs::IndexFeatures::Offs;

  constexpr irs::IndexFeatures kPositions =
    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;

  constexpr irs::IndexFeatures kPositionsOffsets = irs::IndexFeatures::Freq |
                                                   irs::IndexFeatures::Pos |
                                                   irs::IndexFeatures::Offs;

  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();
  auto query_doc2 = MakeByTerm(kNameId, "B");

  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);

  // create source segment
  irs::MemoryDirectory impl;
  FailingDirectory dir(impl);

  // write index
  {
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    ASSERT_TRUE(InsertWithName(*writer, *doc2));

    tests::Remove(*writer, *query_doc2);

    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));
  }

  // check data
  auto reader =
    irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
  ASSERT_TRUE(reader);
  ASSERT_EQ(1, reader->size());
  ASSERT_EQ(2, reader->docs_count());
  ASSERT_EQ(1, reader->live_docs_count());

  // validate index
  tests::index_t expected_index;
  expected_index.emplace_back();
  expected_index.back().insert(*doc1);
  expected_index.back().insert(*doc2);
  tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

  // validate columnstore
  auto& segment = reader[0];
  const auto* column = segment.Column(kNameId);
  ASSERT_NE(nullptr, column);
  irs::tests::BlobPointReader values{segment, *column};
  ASSERT_EQ(2, segment.docs_count());
  ASSERT_EQ(1, segment.live_docs_count());
  auto terms = segment.field(kSameAnlPayId);
  ASSERT_NE(nullptr, terms);

  // regiseter reopen failure in term dictionary
  {
    dir.RegisterFailure(FailingDirectory::Failure::REOPEN, "_1.idx");
    auto term_itr =
      terms->iterator(irs::SeekMode::NORMAL);  // successful attempt
    ASSERT_NE(nullptr, term_itr);
    ASSERT_THROW(term_itr->next(), irs::IoError);
  }

  // regiseter reopen failure in term dictionary (nullptr)
  {
    dir.RegisterFailure(FailingDirectory::Failure::ReopenNull, "_1.idx");
    auto term_itr =
      terms->iterator(irs::SeekMode::NORMAL);  // successful attempt
    ASSERT_NE(nullptr, term_itr);
    ASSERT_THROW(term_itr->next(), irs::IoError);
  }

  auto term_itr = terms->iterator(irs::SeekMode::NORMAL);  // successful attempt
  ASSERT_NE(nullptr, term_itr);
  ASSERT_TRUE(term_itr->next());

  // regiseter reopen failure in postings
  dir.RegisterFailure(FailingDirectory::Failure::REOPEN, "_1.doc");
  // can't reopen document input
  ASSERT_THROW((void)term_itr->postings(irs::IndexFeatures::None),
               irs::IoError);
  // regiseter reopen failure in postings (nullptr)
  dir.RegisterFailure(FailingDirectory::Failure::ReopenNull, "_1.doc");
  // can't reopen document input (nullptr)
  ASSERT_THROW((void)term_itr->postings(irs::IndexFeatures::None),
               irs::IoError);
  // regiseter reopen failure in positions
  dir.RegisterFailure(FailingDirectory::Failure::REOPEN, "_1.pos");
  // can't reopen position input
  ASSERT_THROW((void)term_itr->postings(kPositions), irs::IoError);
  // regiseter reopen failure in positions (nullptr)
  dir.RegisterFailure(FailingDirectory::Failure::ReopenNull, "_1.pos");
  // can't reopen position (nullptr)
  ASSERT_THROW((void)term_itr->postings(kPositions), irs::IoError);
  // regiseter reopen failure in payload
  dir.RegisterFailure(FailingDirectory::Failure::REOPEN, "_1.pay");
  // can't reopen offset input
  ASSERT_THROW((void)term_itr->postings(kPositionsOffsets), irs::IoError);

  // regiseter reopen failure in payload (nullptr)
  dir.RegisterFailure(FailingDirectory::Failure::ReopenNull, "_1.pay");

  // can't reopen position (nullptr)
  ASSERT_THROW((void)term_itr->postings(kPositionsOffsets), irs::IoError);
  // regiseter reopen failure in payload
  // can't reopen offset input
  dir.RegisterFailure(FailingDirectory::Failure::REOPEN, "_1.pay");
  ASSERT_THROW((void)term_itr->postings(kPositionsOffsets), irs::IoError);
  // regiseter reopen failure in payload (nullptr)
  dir.RegisterFailure(FailingDirectory::Failure::ReopenNull, "_1.pay");
  // can't reopen position (nullptr)
  ASSERT_THROW((void)term_itr->postings(kPositionsOffsets), irs::IoError);

  // regiseter reopen failure in postings
  dir.RegisterFailure(FailingDirectory::Failure::REOPEN, "_1.doc");
  // regiseter reopen failure in postings
  dir.RegisterFailure(FailingDirectory::Failure::ReopenNull, "_1.doc");
  // regiseter reopen failure in positions
  dir.RegisterFailure(FailingDirectory::Failure::REOPEN, "_1.pos");
  // regiseter reopen failure in positions
  dir.RegisterFailure(FailingDirectory::Failure::ReopenNull, "_1.pos");
  // regiseter reopen failure in payload
  dir.RegisterFailure(FailingDirectory::Failure::REOPEN, "_1.pay");
  // regiseter reopen failure in payload
  dir.RegisterFailure(FailingDirectory::Failure::ReopenNull, "_1.pay");
  ASSERT_THROW((void)term_itr->postings(kAllFeatures), irs::IoError);
  ASSERT_THROW((void)term_itr->postings(kAllFeatures), irs::IoError);
  ASSERT_THROW((void)term_itr->postings(kAllFeatures), irs::IoError);
  ASSERT_THROW((void)term_itr->postings(kAllFeatures), irs::IoError);
  ASSERT_THROW((void)term_itr->postings(kAllFeatures), irs::IoError);
  ASSERT_THROW((void)term_itr->postings(kAllFeatures), irs::IoError);

  ASSERT_TRUE(dir.NoFailures());
  // successful attempt
  auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
  ASSERT_TRUE(docs_itr->next());
  ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                   values, docs_itr->value()));
  ASSERT_TRUE(docs_itr->next());
  ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                   values, docs_itr->value()));
  ASSERT_FALSE(docs_itr->next());

  // validate live docs
  auto live_docs = segment.docs_iterator();
  ASSERT_TRUE(live_docs->next());
  ASSERT_EQ(1, live_docs->value());
  ASSERT_FALSE(live_docs->next());
  ASSERT_EQ(irs::doc_limits::eof(), live_docs->value());
}

// =======================================================================
// Failure-injection coverage for the `.col` columnstore. The
// `irs::ColWriter` emits one `<segment>.col` file per segment
// (see `kColFormatExt` in
// libs/iresearch/include/iresearch/formats/column/col_reader.hpp), so
// per-segment columnstore failures register against `_N.col`.
// =======================================================================

TEST(index_death_test_formats_15,
     segment_columnstore_creation_failure_1st_phase_flush) {
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();

  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);

  // Phase 1: columnstore creation fails on the very first segment.
  // SegmentWriter::reset(meta) wires the columnstore Writer via
  // `dir.create("_1.col")` and throws on failure.
  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(FailingDirectory::Failure::CREATE, "_1.col");

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // First insert triggers segment_writer::reset(meta) -> cs Writer
    // constructor -> dir.create("_1.col") -> throw.
    ASSERT_THROW(InsertWithName(*writer, *doc1), irs::IoError);

    // Successful follow-up attempt: failure already consumed.
    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(0, reader->size());
    ASSERT_EQ(0, reader->docs_count());
    ASSERT_EQ(0, reader->live_docs_count());
  }

  // Phase 2: first segment's cs fails, retry with another doc succeeds.
  {
    constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                                irs::IndexFeatures::Pos |
                                                irs::IndexFeatures::Offs;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(FailingDirectory::Failure::CREATE, "_1.col");

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    ASSERT_THROW(InsertWithName(*writer, *doc2), irs::IoError);
    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(1, reader->docs_count());
    ASSERT_EQ(1, reader->live_docs_count());

    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    auto& segment = reader[0];
    const auto* column = segment.Column(kNameId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    ASSERT_EQ(1, segment.docs_count());
    ASSERT_EQ(1, segment.live_docs_count());
    auto terms = segment.field(kSameId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));
    ASSERT_FALSE(docs_itr->next());
  }

  // Phase 3: second segment's cs fails, first segment is preserved.
  {
    constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                                irs::IndexFeatures::Pos |
                                                irs::IndexFeatures::Offs;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(FailingDirectory::Failure::CREATE, "_2.col");

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    ASSERT_THROW(InsertWithName(*writer, *doc2), irs::IoError);

    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader->size());
    ASSERT_EQ(2, reader->docs_count());
    ASSERT_EQ(2, reader->live_docs_count());

    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    expected_index.emplace_back();
    expected_index.back().insert(*doc2);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    {
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(1, segment.docs_count());
      ASSERT_EQ(1, segment.live_docs_count());
      auto terms = segment.field(kSameId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));
      ASSERT_FALSE(docs_itr->next());
    }
    {
      auto& segment = reader[1];
      const auto* column = segment.Column(kNameId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(1, segment.docs_count());
      ASSERT_EQ(1, segment.live_docs_count());
      auto terms = segment.field(kSameId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));
      ASSERT_FALSE(docs_itr->next());
    }
  }
}

TEST(index_death_test_formats_15,
     segment_components_creation_failure_1st_phase_flush) {
  // Per-segment file creation order (verified empirically): `_N.col` is
  // created at *insert* time (SegmentWriter::reset(meta) -> cs Writer
  // ctor -> dir.create), then at Begin() the segment flush creates
  // `_N.idx` -> `_N.doc` -> `_N.pos` -> `_N.pay` -> `_N.0.sm` ->
  // `pending_segments_M`. We exercise CREATE failures one per attempt
  // across consecutive segment ids; each retry sees the failure either
  // as Insert throwing (cs CREATE) or Begin throwing (idx/postings file
  // CREATE).
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();
  auto query_doc2 = MakeByTerm(kNameId, "B");

  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);

  // Helper that drives one round: register one failure, try
  // insert+insert+remove+Begin, expect *some* throw to consume the
  // failure. Returns when the failure budget is empty.
  auto DriveCreateFailures = [&](FailingDirectory& dir,
                                 irs::IndexWriter& writer) {
    while (!dir.NoFailures()) {
      const auto failures_before = dir.NumFailures();
      bool inserts_ok = true;
      try {
        if (!InsertWithName(writer, *doc1)) {
          inserts_ok = false;
        } else if (!InsertWithName(writer, *doc2)) {
          inserts_ok = false;
        }
      } catch (const irs::IoError&) {
        // cs CREATE on `_N.col` threw at insert-time.
        ASSERT_LT(dir.NumFailures(), failures_before);
        continue;
      }
      if (inserts_ok) {
        tests::Remove(writer, *query_doc2);
        ASSERT_THROW(writer.RefreshBegin(), irs::IoError);
        ASSERT_LT(dir.NumFailures(), failures_before);
      }
    }
  };

  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    // One failure per per-segment file. Each retry's segment id
    // advances, so the failing file is on a different segment. Five
    // syncable files per segment: cs -> idx -> doc -> pos -> pay.
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_1.col");  // cs (insert-time)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_2.idx");  // term index + data (merged)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_3.doc");  // postings list (documents)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_4.pos");  // postings list (positions)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_5.pay");  // postings list (offset + payload)

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // initial commit
    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    DriveCreateFailures(dir, *writer);

    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(0, reader->size());
    ASSERT_EQ(0, reader->docs_count());
    ASSERT_EQ(0, reader->live_docs_count());
  }

  // Same failures, then a successful insert + commit afterwards.
  {
    constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                                irs::IndexFeatures::Pos |
                                                irs::IndexFeatures::Offs;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(FailingDirectory::Failure::CREATE, "_1.col");
    dir.RegisterFailure(FailingDirectory::Failure::CREATE, "_2.idx");
    dir.RegisterFailure(FailingDirectory::Failure::CREATE, "_3.doc");
    dir.RegisterFailure(FailingDirectory::Failure::CREATE, "_4.pos");
    dir.RegisterFailure(FailingDirectory::Failure::CREATE, "_5.pay");

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    DriveCreateFailures(dir, *writer);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(1, reader->docs_count());
    ASSERT_EQ(1, reader->live_docs_count());

    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    auto& segment = reader[0];
    const auto* column = segment.Column(kNameId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    ASSERT_EQ(1, segment.docs_count());
    ASSERT_EQ(1, segment.live_docs_count());
    auto terms = segment.field(kSameId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));
    ASSERT_FALSE(docs_itr->next());
  }
}

TEST(index_death_test_formats_15,
     segment_components_sync_failure_1st_phase_flush) {
  // SYNC failures on each per-segment file. Sync order (verified
  // empirically): `_N.0.sm`, `_N.col`, `_N.doc`, `_N.pos`, `_N.idx`,
  // `_N.pay`, then `pending_segments_M`. Each retry advances the
  // segment id.
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();
  auto query_doc2 = MakeByTerm(kNameId, "B");

  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);

  // Helper: drive one round of inserts + Begin(), expecting a sync
  // failure to throw on Begin. Each retry advances the segment id.
  // Do not use Remove() here: the doc_mask sync surface would obscure
  // which per-file sync failure was actually triggered.
  auto DriveSyncFailures = [&](FailingDirectory& dir,
                               irs::IndexWriter& writer) {
    while (!dir.NoFailures()) {
      const auto failures_before = dir.NumFailures();
      ASSERT_TRUE(InsertWithName(writer, *doc1));
      ASSERT_THROW(writer.RefreshBegin(), irs::IoError);
      ASSERT_LT(dir.NumFailures(), failures_before);
    }
  };
  (void)doc2;
  (void)query_doc2;

  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    // Sync order per segment: sm, cs, doc, pos, idx, pay -- 6 syncable
    // files per segment.
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_1.0.sm");  // segment meta
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_2.col");  // columnstore
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_3.doc");  // postings list (documents)
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_4.pos");  // postings list (positions)
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_5.idx");  // term index + data (merged)
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_6.pay");  // postings list (offset + payload)

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    DriveSyncFailures(dir, *writer);

    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(0, reader->size());
    ASSERT_EQ(0, reader->docs_count());
    ASSERT_EQ(0, reader->live_docs_count());
  }

  // Same failures, then a successful insert + commit afterwards.
  {
    constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                                irs::IndexFeatures::Pos |
                                                irs::IndexFeatures::Offs;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(FailingDirectory::Failure::SYNC, "_1.0.sm");
    dir.RegisterFailure(FailingDirectory::Failure::SYNC, "_2.col");
    dir.RegisterFailure(FailingDirectory::Failure::SYNC, "_3.doc");
    dir.RegisterFailure(FailingDirectory::Failure::SYNC, "_4.pos");
    dir.RegisterFailure(FailingDirectory::Failure::SYNC, "_5.idx");
    dir.RegisterFailure(FailingDirectory::Failure::SYNC, "_6.pay");

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    DriveSyncFailures(dir, *writer);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(1, reader->docs_count());
    ASSERT_EQ(1, reader->live_docs_count());

    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    auto& segment = reader[0];
    const auto* column = segment.Column(kNameId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    ASSERT_EQ(1, segment.docs_count());
    ASSERT_EQ(1, segment.live_docs_count());
    auto terms = segment.field(kSameId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));
    ASSERT_FALSE(docs_itr->next());
  }
}

TEST(index_death_test_formats_15,
     segment_meta_write_fail_long_running_compaction) {
  // BlockingDirectory blocks on `_3.col` to keep compaction in
  // flight while the test runs a second commit; once the lock is
  // released the compaction thread observes the registered failure
  // and the test asserts the index is still healthy.
  tests::JsonDocGenerator gen(
    TestBase::resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::StringField>(name, data.str);
        field->id = tests::FieldIdFor(name);
        doc.insert(std::move(field));
      }
    });
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();
  const auto* doc3 = gen.next();

  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);

  // segment meta creation failure during compaction
  {
    constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                                irs::IndexFeatures::Pos |
                                                irs::IndexFeatures::Offs;

    irs::MemoryDirectory impl;
    FailingDirectory failing_dir(impl);
    tests::BlockingDirectory dir(failing_dir, "_3.col");

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 0
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    // fail segment meta creation on the compacted segment
    failing_dir.RegisterFailure(FailingDirectory::Failure::CREATE, "_3.0.sm");

    dir.intermediate_commits_lock.lock();

    std::thread compaction_thread([&writer]() {
      const irs::index_utils::CompactionCount compact_all;
      ASSERT_THROW(writer->Compact(irs::index_utils::MakePolicy(compact_all)),
                   irs::IoError);
    });

    dir.wait_for_blocker();

    // commit an intermediate segment while compaction is blocked
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    dir.intermediate_commits_lock.unlock();
    compaction_thread.join();

    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(3, reader->size());
    ASSERT_EQ(3, reader->docs_count());
    ASSERT_EQ(3, reader->live_docs_count());

    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    expected_index.emplace_back();
    expected_index.back().insert(*doc2);
    expected_index.emplace_back();
    expected_index.back().insert(*doc3);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);
  }

  // segment meta sync failure during compaction
  {
    constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                                irs::IndexFeatures::Pos |
                                                irs::IndexFeatures::Offs;

    irs::MemoryDirectory impl;
    FailingDirectory failing_dir(impl);
    tests::BlockingDirectory dir(failing_dir, "_3.col");

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    failing_dir.RegisterFailure(FailingDirectory::Failure::SYNC, "_3.0.sm");

    dir.intermediate_commits_lock.lock();

    std::thread compaction_thread([&writer]() {
      const irs::index_utils::CompactionCount compact_all;
      ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(compact_all)));
    });

    dir.wait_for_blocker();

    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    dir.intermediate_commits_lock.unlock();
    compaction_thread.join();

    // pending compaction commit fails on segment-meta sync.
    ASSERT_THROW(writer->RefreshBegin(), irs::IoError);

    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(3, reader->size());
    ASSERT_EQ(3, reader->docs_count());
    ASSERT_EQ(3, reader->live_docs_count());

    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    expected_index.emplace_back();
    expected_index.back().insert(*doc2);
    expected_index.emplace_back();
    expected_index.back().insert(*doc3);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);
  }
}

TEST(index_death_test_formats_15, segment_components_write_fail_compaction) {
  // Register CREATE failures on every per-segment component
  // (`_N.col`, `_N.idx`, `_N.doc`, `_N.pos`, `_N.pay`).
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();

  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);

  {
    constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                                irs::IndexFeatures::Pos |
                                                irs::IndexFeatures::Offs;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 0
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    // Register CREATE failures across the compacted-segment
    // components. Each compaction attempt allocates a fresh
    // segment id (NextSegmentId()), so after `_3.col` fails the next
    // attempt's segment is `_4`, the one after is `_5`, etc. Order
    // matches the order files are created during MergeWriter::Flush:
    // cs writer is opened first (in OpenColumnstoreContexts), then
    // postings, then term index/data, etc.
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_3.col");  // columnstore for compacted seg
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_4.idx");  // term index + data (merged)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_5.doc");  // postings list (documents)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_6.pos");  // postings list (positions)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_7.pay");  // postings list (offset + payload)

    const irs::index_utils::CompactionCount compact_all;

    while (!dir.NoFailures()) {
      ASSERT_THROW(writer->Compact(irs::index_utils::MakePolicy(compact_all)),
                   irs::IoError);
      ASSERT_FALSE(writer->RefreshBegin());  // nothing to flush
    }

    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader->size());
    ASSERT_EQ(2, reader->docs_count());
    ASSERT_EQ(2, reader->live_docs_count());

    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    expected_index.emplace_back();
    expected_index.back().insert(*doc2);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    {
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));
      ASSERT_FALSE(docs_itr->next());
    }
    {
      auto& segment = reader[1];
      const auto* column = segment.Column(kNameId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));
      ASSERT_FALSE(docs_itr->next());
    }
  }
}

TEST(index_death_test_formats_15, segment_components_sync_fail_compaction) {
  // Like the *_write_fail_compaction* test but with SYNC failures
  // (compaction creates the files, then sync fails at Begin()).
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();

  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);

  {
    constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                                irs::IndexFeatures::Pos |
                                                irs::IndexFeatures::Offs;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    // Sync failures fire at Begin() (after Compact succeeds in
    // creating the files). Each iteration consumes one failure;
    // segment id advances each retry.
    dir.RegisterFailure(FailingDirectory::Failure::SYNC, "_3.col");
    dir.RegisterFailure(FailingDirectory::Failure::SYNC, "_4.doc");
    dir.RegisterFailure(FailingDirectory::Failure::SYNC, "_5.idx");
    dir.RegisterFailure(FailingDirectory::Failure::SYNC, "_6.idx");
    dir.RegisterFailure(FailingDirectory::Failure::SYNC, "_7.pos");
    dir.RegisterFailure(FailingDirectory::Failure::SYNC, "_8.pay");

    const irs::index_utils::CompactionCount compact_all;

    while (!dir.NoFailures()) {
      ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(compact_all)));
      ASSERT_THROW(writer->RefreshBegin(), irs::IoError);
      ASSERT_FALSE(writer->RefreshBegin());
    }

    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader->size());
    ASSERT_EQ(2, reader->docs_count());
    ASSERT_EQ(2, reader->live_docs_count());

    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    expected_index.emplace_back();
    expected_index.back().insert(*doc2);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    {
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));
      ASSERT_FALSE(docs_itr->next());
    }
    {
      auto& segment = reader[1];
      const auto* column = segment.Column(kNameId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));
      ASSERT_FALSE(docs_itr->next());
    }
  }
}

TEST(index_death_test_formats_15, segment_components_fail_import) {
  // Import path: read from `src_index`, write a fresh segment in `dir`.
  constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                              irs::IndexFeatures::Pos |
                                              irs::IndexFeatures::Offs;

  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();

  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);

  // create source segment
  irs::MemoryDirectory src_dir;
  {
    auto writer = irs::IndexWriter::Make(src_dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(src_dir, codec, irs::tests::DefaultReaderOptions()));
  }

  auto src_index =
    irs::DirectoryReader(src_dir, codec, irs::tests::DefaultReaderOptions());
  ASSERT_TRUE(src_index);

  // file creation failures (no recovery)
  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    // 6 distinct per-segment files: cs, idx, doc, pos, pay, 0.sm.
    // Each Import retry advances the segment id.
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_1.col");  // columnstore
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_2.idx");  // term index + data (merged)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_3.doc");  // postings list (documents)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_4.pos");  // postings list (positions)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_5.pay");  // postings list (offset + payload)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_6.0.sm");  // segment meta

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // initial commit
    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    while (!dir.NoFailures()) {
      ASSERT_THROW(writer->Import(*src_index), irs::IoError);
      ASSERT_FALSE(writer->RefreshBegin());  // nothing to commit
    }

    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(0, reader->size());
    ASSERT_EQ(0, reader->docs_count());
    ASSERT_EQ(0, reader->live_docs_count());
  }

  // file creation failures, then successful import + commit
  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    dir.RegisterFailure(FailingDirectory::Failure::CREATE, "_1.col");
    dir.RegisterFailure(FailingDirectory::Failure::CREATE, "_2.idx");
    dir.RegisterFailure(FailingDirectory::Failure::CREATE, "_3.doc");
    dir.RegisterFailure(FailingDirectory::Failure::CREATE, "_4.pos");
    dir.RegisterFailure(FailingDirectory::Failure::CREATE, "_5.pay");
    dir.RegisterFailure(FailingDirectory::Failure::CREATE, "_6.0.sm");

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    while (!dir.NoFailures()) {
      ASSERT_THROW(writer->Import(*src_index), irs::IoError);
      ASSERT_FALSE(writer->RefreshBegin());  // nothing to commit
    }

    ASSERT_TRUE(writer->Import(*src_index));
    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(1, reader->docs_count());
    ASSERT_EQ(1, reader->live_docs_count());

    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    auto& segment = reader[0];
    const auto* column = segment.Column(kNameId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));
    ASSERT_FALSE(docs_itr->next());
  }
}

TEST(index_death_test_formats_15,
     segment_components_creation_fail_implicit_segment_flush) {
  // Implicit segment flush: `segment_docs_max = 1` makes every other
  // insert spill into a new segment. We exercise CREATE failures one
  // file at a time across consecutive segment ids, so the assertion
  // doesn't depend on file-creation order.
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();

  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);

  // file creation failures, individually verified
  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    auto opts = irs::tests::DefaultWriterOptions();
    opts.segment_docs_max = 1;  // flush every 2nd document

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate, opts);
    ASSERT_NE(nullptr, writer);

    // initial commit
    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    // Failure 1: `_1.col` CREATE fires at insert time (cs writer
    // construction).
    dir.RegisterFailure(FailingDirectory::Failure::CREATE, "_1.col");
    ASSERT_THROW(InsertWithName(*writer, *doc1), irs::IoError);
    ASSERT_TRUE(dir.NoFailures());

    // Failure 2: `_2.0.sm` CREATE fires during Begin's segment flush
    // (sm = segment meta) -- segment_docs_max=1 keeps each segment to
    // a single doc but the SM file is still produced at Begin time,
    // not at insert time.
    dir.RegisterFailure(FailingDirectory::Failure::CREATE, "_2.0.sm");
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_THROW(writer->RefreshBegin(), irs::IoError);
    ASSERT_TRUE(dir.NoFailures());

    (void)doc2;
  }
}

TEST(index_death_test_formats_15,
     columnstore_creation_fail_implicit_segment_flush) {
  // With `segment_docs_max=1`, every Insert flushes the previous
  // segment and opens a new one -- so CREATE failures on consecutive
  // `_N.col` files fire at the Insert that allocates the new segment.
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();

  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);

  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    auto opts = irs::tests::DefaultWriterOptions();
    opts.segment_docs_max = 1;

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate, opts);
    ASSERT_NE(nullptr, writer);

    // CREATE failure on the very first segment's cs file: insert
    // throws because cs Writer construction calls
    // `dir.create("_1.col")` which returns nullptr.
    dir.RegisterFailure(FailingDirectory::Failure::CREATE, "_1.col");
    ASSERT_THROW(InsertWithName(*writer, *doc1), irs::IoError);
    ASSERT_TRUE(dir.NoFailures());

    // After the failure clears, Insert + Commit proceed normally.
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1u, reader->size());
    ASSERT_EQ(1u, reader->live_docs_count());

    auto& segment = reader[0];
    const auto* column = segment.Column(kNameId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));
    ASSERT_FALSE(docs_itr->next());

    (void)doc2;  // unused; kept so the call sites read symmetrically.
  }
}

TEST(index_death_test_formats_15,
     columnstore_creation_sync_fail_implicit_segment_flush) {
  // Register both `CREATE` and `SYNC` failures on `_N.col` across
  // multiple per-segment columnstore files.
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();

  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);

  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    auto opts = irs::tests::DefaultWriterOptions();
    opts.segment_docs_max = 1;

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate, opts);
    ASSERT_NE(nullptr, writer);

    // initial commit so a DirectoryReader can be opened at the end
    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());

    // 1) CREATE failure on `_1.col` -> first insert throws.
    dir.RegisterFailure(FailingDirectory::Failure::CREATE, "_1.col");
    ASSERT_THROW(InsertWithName(*writer, *doc1), irs::IoError);
    ASSERT_TRUE(dir.NoFailures());

    // 2) Insert succeeds (allocates `_2.col`), but SYNC fails on it
    // during Begin(). Begin() throws irs::IoError; the failed flush
    // leaves the index empty.
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    dir.RegisterFailure(FailingDirectory::Failure::SYNC, "_2.col");
    ASSERT_THROW(writer->RefreshBegin(), irs::IoError);
    ASSERT_TRUE(dir.NoFailures());

    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(0, reader->size());
    ASSERT_EQ(0, reader->docs_count());
    ASSERT_EQ(0, reader->live_docs_count());

    (void)doc2;
  }
}

TEST(index_death_test_formats_15, fails_in_compact_with_removals) {
  // Mixed failures around CREATE/SYNC/REMOVE on `.col` files driven by
  // a sequence of inserts, commits, and a compact. Each failing
  // step is followed by a successful retry; final index has the full
  // dataset.
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();

  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);

  constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                              irs::IndexFeatures::Pos |
                                              irs::IndexFeatures::Offs;

  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "pending_segments_1");
    ASSERT_THROW(writer->RefreshBegin(), irs::IoError);

    dir.RegisterFailure(FailingDirectory::Failure::RENAME,
                        "pending_segments_1");
    ASSERT_THROW(writer->RefreshCommit(), irs::IoError);
    ASSERT_THROW(
      (irs::DirectoryReader{dir, codec, irs::tests::DefaultReaderOptions()}),
      irs::IndexNotFound);

    // Now empty commit succeeds.
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    // Insert fails because `.col` creation fails for the new segment.
    dir.RegisterFailure(FailingDirectory::Failure::CREATE, "_1.col");
    ASSERT_THROW(InsertWithName(*writer, *doc1), irs::IoError);
    ASSERT_FALSE(writer->RefreshCommit());  // nothing to commit
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));

    // SYNC fails on segment 0's `.col` -> Commit throws.
    dir.RegisterFailure(FailingDirectory::Failure::SYNC, "_2.col");
    ASSERT_THROW(writer->RefreshCommit(), irs::IoError);
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    // Nothing to commit after a failed first phase.
    ASSERT_FALSE(writer->RefreshCommit());

    // NOW IT IS OK -- insert + commit succeeds.
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    const irs::index_utils::CompactionCount compact_all;

    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(compact_all)));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    // Register REMOVE failures on superseded segment `.col` files; the
    // DirectoryCleaner should tolerate the failures (leave stale
    // files) and let the next commit observe the empty mask.
    dir.RegisterFailure(FailingDirectory::Failure::REMOVE, "_3.col");
    dir.RegisterFailure(FailingDirectory::Failure::REMOVE, "_5.col");
    irs::DirectoryCleaner::clean(dir);
    ASSERT_FALSE(writer->RefreshCommit());  // nothing changed
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    auto reader =
      irs::DirectoryReader{dir, codec, irs::tests::DefaultReaderOptions()};
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(2, reader->docs_count());
    ASSERT_EQ(2, reader->live_docs_count());

    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    expected_index.back().insert(*doc2);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    auto& segment = reader[0];
    const auto* column = segment.Column(kNameId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));
    ASSERT_FALSE(docs_itr->next());
  }
}

TEST(index_death_test_formats_15, fails_in_exists) {
  // Reader::Reader probes `dir.exists(filename)` before opening the
  // `.col`. Force the EXISTS failure on consecutive `.col` files to
  // exercise that path. After failures clear, commits and a
  // compaction must still succeed.
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);

  constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                              irs::IndexFeatures::Pos |
                                              irs::IndexFeatures::Offs;

  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();
  const auto* doc3 = gen.next();
  const auto* doc4 = gen.next();

  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);

  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // Will force errors during commit / reader open because of the
    // segment reader's columnstore probe.
    dir.RegisterFailure(FailingDirectory::Failure::EXISTS, "_1.col");
    dir.RegisterFailure(FailingDirectory::Failure::EXISTS, "_2.col");

    while (!dir.NoFailures()) {
      ASSERT_TRUE(InsertWithName(*writer, *doc1));
      ASSERT_THROW(writer->RefreshCommit(), irs::IoError);
      ASSERT_THROW(
        (irs::DirectoryReader{dir, codec, irs::tests::DefaultReaderOptions()}),
        irs::IndexNotFound);
    }

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    const irs::index_utils::CompactionCount compact_all;

    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(compact_all)));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    ASSERT_TRUE(dir.NoFailures());

    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(4, reader->docs_count());
    ASSERT_EQ(4, reader->live_docs_count());

    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    expected_index.back().insert(*doc2);
    expected_index.back().insert(*doc3);
    expected_index.back().insert(*doc4);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    auto& segment = reader[0];
    const auto* column = segment.Column(kNameId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));
    ASSERT_FALSE(docs_itr->next());
  }
}

TEST(index_death_test_formats_15, fails_in_length) {
  // `dir.length(name)` failures on per-segment files. Commit without
  // removals doesn't call `length`, so the failure budget must stay
  // unchanged. EXISTS failures on `.col` files then exercise the
  // compaction cleanup path.
  tests::JsonDocGenerator gen(
    TestBase::resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::StringField>(name, data.str);
        field->id = tests::FieldIdFor(name);
        doc.insert(std::move(field));
      }
    });
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();
  const auto* doc3 = gen.next();
  const auto* doc4 = gen.next();

  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);

  {
    constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                                irs::IndexFeatures::Pos |
                                                irs::IndexFeatures::Offs;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    // Register LENGTH failures on each of the 5 per-segment component
    // files: col, idx, doc, pos, pay.
    for (const auto& seg : {"_1", "_2", "_3", "_4"}) {
      dir.RegisterFailure(FailingDirectory::Failure::LENGTH,
                          std::string{seg} + ".col");
      dir.RegisterFailure(FailingDirectory::Failure::LENGTH,
                          std::string{seg} + ".idx");
      dir.RegisterFailure(FailingDirectory::Failure::LENGTH,
                          std::string{seg} + ".pos");
      dir.RegisterFailure(FailingDirectory::Failure::LENGTH,
                          std::string{seg} + ".pay");
      dir.RegisterFailure(FailingDirectory::Failure::LENGTH,
                          std::string{seg} + ".doc");
    }

    const size_t num_failures = dir.NumFailures();

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 0
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    // segment 2
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    // segment 3
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    {
      auto reader =
        irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
      ASSERT_TRUE(reader);
      ASSERT_EQ(4, reader->size());
      ASSERT_EQ(4, reader->docs_count());
      ASSERT_EQ(4, reader->live_docs_count());
    }

    // Commit without removals doesn't call `length`.
    ASSERT_EQ(num_failures, dir.NumFailures());
    dir.ClearFailures();

    // Now register EXISTS failures on the compacted path's `.col`
    // probes -- compaction should still succeed (the cleaner
    // tolerates the missed exists check; the post-compaction read
    // works because the compacted `.col` exists).
    for (const auto& seg : {"_1", "_2", "_3", "_4"}) {
      dir.RegisterFailure(FailingDirectory::Failure::EXISTS,
                          std::string{seg} + ".col");
    }

    const irs::index_utils::CompactionCount compact_all;

    const auto num_failures_before = dir.NumFailures();
    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(compact_all)));
    // Same number of failures: the compaction code path doesn't
    // probe exists on the input segment `.col` files.
    ASSERT_EQ(num_failures_before, dir.NumFailures());

    irs::DirectoryCleaner::clean(dir);
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));

    ASSERT_EQ(num_failures_before, dir.NumFailures());

    auto reader =
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(4, reader->docs_count());
    ASSERT_EQ(4, reader->live_docs_count());

    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    expected_index.back().insert(*doc2);
    expected_index.back().insert(*doc3);
    expected_index.back().insert(*doc4);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    auto& segment = reader[0];
    const auto* column = segment.Column(kNameId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));
    ASSERT_FALSE(docs_itr->next());
  }
}

TEST(index_death_test_formats_15, columnstore_reopen_fail) {
  // Reader-side failures on the columnstore file.
  //
  // OPEN failure on `_1.col` -- the segment reader's columnstore probe
  // (`ColReader::Reader` -> `OpenAndCheckHeader`) calls
  // `dir.open("_1.col")` and turns nullptr into `irs::IoError`.
  constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                              irs::IndexFeatures::Pos |
                                              irs::IndexFeatures::Offs;

  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();
  auto query_doc2 = MakeByTerm(kNameId, "B");

  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);

  irs::MemoryDirectory impl;
  FailingDirectory dir(impl);

  {
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    tests::Remove(*writer, *query_doc2);

    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions()));
  }

  // OPEN failure on `_1.col` -- DirectoryReader throws.
  dir.RegisterFailure(FailingDirectory::Failure::OPEN, "_1.col");
  ASSERT_THROW(
    (irs::DirectoryReader{dir, codec, irs::tests::DefaultReaderOptions()}),
    irs::IoError);

  // Read succeeds once the failure clears.
  auto reader =
    irs::DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
  ASSERT_TRUE(reader);
  ASSERT_EQ(1, reader->size());
  ASSERT_EQ(2, reader->docs_count());
  ASSERT_EQ(1, reader->live_docs_count());

  tests::index_t expected_index;
  expected_index.emplace_back();
  expected_index.back().insert(*doc1);
  expected_index.back().insert(*doc2);
  tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

  auto& segment = reader[0];
  const auto* column = segment.Column(kNameId);
  ASSERT_NE(nullptr, column);

  // Normal data readback.
  irs::tests::BlobPointReader values{segment, *column};
  ASSERT_EQ(2, segment.docs_count());
  ASSERT_EQ(1, segment.live_docs_count());
  auto terms = segment.field(kSameId);
  ASSERT_NE(nullptr, terms);
  auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
  ASSERT_TRUE(term_itr->next());
  auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
  ASSERT_TRUE(docs_itr->next());
  ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                   values, docs_itr->value()));
  ASSERT_TRUE(docs_itr->next());
  ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                   values, docs_itr->value()));
  ASSERT_FALSE(docs_itr->next());

  // live docs
  auto live_docs = segment.docs_iterator();
  ASSERT_TRUE(live_docs->next());
  ASSERT_EQ(1, live_docs->value());
  ASSERT_FALSE(live_docs->next());
  ASSERT_EQ(irs::doc_limits::eof(), live_docs->value());
}

TEST(index_death_test_formats_15, fails_in_dup) {
  // The cs Reader doesn't go through `IndexInput::Dup()`. It keeps a
  // single IndexInput and per-read `ReadContext` calls
  // `Reader::ReopenIn()` -> `IndexInput::Reopen()`. DUP-fail injection
  // on `_1.col` is therefore a no-op; skip until a Dup path appears
  // (e.g. parallel column readers).
  GTEST_SKIP() << "cs Reader uses Reopen(), not Dup(); DUP-fail injection "
                  "is a no-op on `_N.col`.";
}
