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

#include <iresearch/formats/formats.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/index_utils.hpp>

#include "index_tests.hpp"
#include "tests_shared.hpp"

namespace {

auto MakeByTerm(std::string_view name, std::string_view value) {
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field() = name;
  filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(value);
  return filter;
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
    const irs::byte_type* ReadBuffer(size_t offset, size_t size,
                                     irs::BufferHint hint) final {
      return _impl->ReadBuffer(offset, size, hint);
    }
    const irs::byte_type* ReadBuffer(size_t size, irs::BufferHint hint) final {
      return _impl->ReadBuffer(size, hint);
    }
    irs::byte_type ReadByte() final { return _impl->ReadByte(); }
    size_t ReadBytes(irs::byte_type* b, size_t count) final {
      return _impl->ReadBytes(b, count);
    }
    size_t ReadBytes(size_t offset, irs::byte_type* b, size_t count) final {
      return _impl->ReadBytes(offset, b, count);
    }
    uint64_t Position() const final { return _impl->Position(); }
    uint64_t Length() const final { return _impl->Length(); }
    bool IsEOF() const final { return _impl->IsEOF(); }
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
    void Seek(size_t pos) final { _impl->Seek(pos); }
    uint32_t Checksum(size_t offset) const final {
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

irs::FeatureInfoProvider DefaultFeatureInfo() {
  return [](irs::IndexFeatures) {
    return std::make_pair(
      irs::ColumnInfo{.compression = irs::Type<irs::compression::None>::get(),
                      .options = {},
                      .encryption = true,
                      .track_prev_doc = false},
      irs::FeatureWriterFactory{});
  };
}

void OpenReader(std::string_view format,
                std::function<void(FailingDirectory& dir)> failure_registerer) {
  constexpr irs::IndexFeatures kAllFeatures =
    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
    irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();
  auto query_doc2 = MakeByTerm("name", "B");

  auto codec = irs::formats::Get(format);
  ASSERT_NE(nullptr, codec);

  // create source segment
  irs::MemoryDirectory impl;
  FailingDirectory dir(impl);

  // write index
  {
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));

    writer->GetBatch().Remove(*query_doc2);

    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));
  }

  failure_registerer(dir);

  while (!dir.NoFailures()) {
    ASSERT_THROW(irs::DirectoryReader{dir}, irs::IoError);
  }

  // check data
  auto reader = irs::DirectoryReader(dir);
  ASSERT_TRUE(reader);
  ASSERT_EQ(1, reader->size());
  ASSERT_EQ(2, reader->docs_count());
  ASSERT_EQ(1, reader->live_docs_count());

  // validate index
  tests::index_t expected_index;
  expected_index.emplace_back(::DefaultFeatureInfo());
  expected_index.back().insert(*doc1);
  expected_index.back().insert(*doc2);
  tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

  // validate columnstore
  auto& segment = reader[0];  // assume 0 is id of first/only segment
  const auto* column = segment.column("name");
  ASSERT_NE(nullptr, column);
  auto values = column->iterator(irs::ColumnHint::Normal);
  ASSERT_NE(nullptr, values);
  auto* actual_value = irs::get<irs::PayAttr>(*values);
  ASSERT_NE(nullptr, actual_value);
  ASSERT_EQ(2, segment.docs_count());       // total count of documents
  ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
  auto terms = segment.field("same");
  ASSERT_NE(nullptr, terms);
  auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
  ASSERT_TRUE(term_itr->next());
  auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
  ASSERT_TRUE(docs_itr->next());
  ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
  ASSERT_EQ("A", irs::ToString<std::string_view>(
                   actual_value->value.data()));  // 'name' value in doc3
  ASSERT_TRUE(docs_itr->next());
  ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
  ASSERT_EQ("B", irs::ToString<std::string_view>(
                   actual_value->value.data()));  // 'name' value in doc3
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
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });
  const auto* doc1 = gen.next();

  auto codec = irs::formats::Get("1_5avx");
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
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_THROW(writer->Begin(), irs::IoError);  // creation failure
    ASSERT_THROW(writer->Begin(), irs::IoError);  // synchronization failure

    // successful attempt
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // ensure no data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(0, reader->size());
    ASSERT_EQ(0, reader->docs_count());
    ASSERT_EQ(0, reader->live_docs_count());
  }

  {
    constexpr irs::IndexFeatures kAllFeatures =
      irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
      irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(
      FailingDirectory::Failure::CREATE,
      "pending_segments_1");  // fail first phase of transaction
    dir.RegisterFailure(
      FailingDirectory::Failure::SYNC,
      "pending_segments_1");  // fail first phase of transaction

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_THROW(writer->Begin(), irs::IoError);  // creation failure
    ASSERT_THROW(writer->Begin(), irs::IoError);  // synchronization failure

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    // successful attempt
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());

    // check data
    auto reader = irs::DirectoryReader(dir);
    tests::AssertSnapshotEquality(writer->GetSnapshot(), reader);
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(1, reader->docs_count());
    ASSERT_EQ(1, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc1);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    ASSERT_EQ(1, segment.docs_count());       // total count of documents
    ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc3
    ASSERT_FALSE(docs_itr->next());
  }
}

TEST(index_death_test_formats_15, index_commit_fail_sync_1st_phase) {
  tests::JsonDocGenerator gen(
    TestBase::resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });
  const auto* doc1 = gen.next();

  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);

  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_1.0.sm");  // unable to sync segment meta
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_2.doc");  // unable to sync postings
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_3.ti");  // unable to sync term index

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_THROW(writer->Begin(), irs::IoError);  // synchronization failure

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_THROW(writer->Begin(), irs::IoError);  // synchronization failure

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_THROW(writer->Begin(), irs::IoError);  // synchronization failure

    // successful attempt
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());

    // ensure no data
    auto reader = irs::DirectoryReader(dir);
    tests::AssertSnapshotEquality(writer->GetSnapshot(), reader);
    ASSERT_TRUE(reader);
    ASSERT_EQ(0, reader->size());
    ASSERT_EQ(0, reader->docs_count());
    ASSERT_EQ(0, reader->live_docs_count());
  }

  {
    constexpr irs::IndexFeatures kAllFeatures =
      irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
      irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_1.0.sm");  // unable to sync segment meta
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_2.doc");  // unable to sync postings
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_3.tm");  // unable to sync term index

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    // initial commit
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_THROW(writer->Begin(), irs::IoError);  // synchronization failure
    ASSERT_FALSE(writer->Begin());                // nothing to flush

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_THROW(writer->Begin(), irs::IoError);  // synchronization failure
    ASSERT_FALSE(writer->Begin());                // nothing to flush

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_THROW(writer->Begin(), irs::IoError);  // synchronization failure
    ASSERT_FALSE(writer->Begin());                // nothing to flush

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    // successful attempt
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());

    // check data
    auto reader = irs::DirectoryReader(dir);
    tests::AssertSnapshotEquality(writer->GetSnapshot(), reader);
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(1, reader->docs_count());
    ASSERT_EQ(1, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc1);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    ASSERT_EQ(1, segment.docs_count());       // total count of documents
    ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc3
    ASSERT_FALSE(docs_itr->next());
  }
}

TEST(index_death_test_formats_15, index_meta_write_failure_2nd_phase) {
  tests::JsonDocGenerator gen(
    TestBase::resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });
  const auto* doc1 = gen.next();

  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);

  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    // fail second phase of transaction
    dir.RegisterFailure(FailingDirectory::Failure::RENAME,
                        "pending_segments_1");

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_TRUE(writer->Begin());
    ASSERT_THROW(writer->Commit(), irs::IoError);
    ASSERT_THROW((irs::DirectoryReader{dir}), irs::IndexNotFound);

    // second attempt
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // ensure no data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(0, reader->size());
    ASSERT_EQ(0, reader->docs_count());
    ASSERT_EQ(0, reader->live_docs_count());
  }

  {
    constexpr irs::IndexFeatures kAllFeatures =
      irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
      irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(
      FailingDirectory::Failure::RENAME,
      "pending_segments_1");  // fail second phase of transaction

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_TRUE(writer->Begin());
    ASSERT_THROW(writer->Commit(), irs::IoError);
    ASSERT_THROW((irs::DirectoryReader{dir}), irs::IndexNotFound);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    // second attempt
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // check data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(1, reader->docs_count());
    ASSERT_EQ(1, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc1);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    ASSERT_EQ(1, segment.docs_count());       // total count of documents
    ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc3
    ASSERT_FALSE(docs_itr->next());
  }
}

TEST(index_death_test_formats_15,
     segment_columnstore_creation_failure_1st_phase_flush) {
  GTEST_SKIP() << "TODO(mbkkt) Invesigate it";
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();

  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);

  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_1.cs");  // columnstore

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    // segment meta
    ASSERT_THROW(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                        doc1->stored.begin(), doc1->stored.end()),
                 irs::IoError);

    // successul attempt
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // ensure no data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(0, reader->size());
    ASSERT_EQ(0, reader->docs_count());
    ASSERT_EQ(0, reader->live_docs_count());
  }

  {
    constexpr irs::IndexFeatures kAllFeatures =
      irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
      irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_1.cs");  // columnstore

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    ASSERT_THROW(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                        doc2->stored.begin(), doc2->stored.end()),
                 irs::IoError);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    // successul attempt
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // check data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(1, reader->docs_count());
    ASSERT_EQ(1, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc1);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    ASSERT_EQ(1, segment.docs_count());       // total count of documents
    ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc3
    ASSERT_FALSE(docs_itr->next());
  }

  {
    constexpr irs::IndexFeatures kAllFeatures =
      irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
      irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_2.cs");  // columnstore

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    // successul attempt
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    ASSERT_THROW(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                        doc2->stored.begin(), doc2->stored.end()),
                 irs::IoError);

    // nothing to flush
    ASSERT_FALSE(writer->Begin());

    // check data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(1, reader->docs_count());
    ASSERT_EQ(1, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc1);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    ASSERT_EQ(1, segment.docs_count());       // total count of documents
    ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc3
    ASSERT_FALSE(docs_itr->next());
  }

  {
    constexpr irs::IndexFeatures kAllFeatures =
      irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
      irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_2.cs");  // columnstore

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    // successul attempt
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    ASSERT_THROW(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                        doc2->stored.begin(), doc2->stored.end()),
                 irs::IoError);

    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));

    // nothing to flush
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // check data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader->size());
    ASSERT_EQ(2, reader->docs_count());
    ASSERT_EQ(2, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc1);
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc2);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore (segment 0)
    {
      auto& segment = reader[0];  // assume 0 is id of first/only segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }

    // validate columnstore (segment 1)
    {
      auto& segment = reader[1];  // assume 0 is id of first/only segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }
  }
}

TEST(index_death_test_formats_15,
     segment_components_creation_failure_1st_phase_flush) {
  GTEST_SKIP() << "TODO(mbkkt) Invesigate it";
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();
  auto query_doc2 = MakeByTerm("name", "B");

  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);

  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_1.doc");  // postings list (documents)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_2.2.doc_mask");  // deleted docs
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_3.cm");  // column meta
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_4.ti");  // term index
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_5.tm");  // term data
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_6.pos");  // postings list (positions)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_7.pay");  // postings list (offset + payload)

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    // initial commit
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // segment meta
    while (!dir.NoFailures()) {
      ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                         doc1->stored.begin(), doc1->stored.end()));
      ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                         doc2->stored.begin(), doc2->stored.end()));

      writer->GetBatch().Remove(*query_doc2);

      ASSERT_THROW(writer->Begin(), irs::IoError);
      ASSERT_FALSE(writer->Begin());  // nothing to flush
    }

    // ensure no data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(0, reader->size());
    ASSERT_EQ(0, reader->docs_count());
    ASSERT_EQ(0, reader->live_docs_count());
  }

  {
    constexpr irs::IndexFeatures kAllFeatures =
      irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
      irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_1.doc");  // postings list (documents)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_2.2.doc_mask");  // deleted docs
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_3.cm");  // column meta
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_4.ti");  // term index
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_5.tm");  // term data
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_6.pos");  // postings list (positions)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_7.pay");  // postings list (offset + payload)

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    // segment meta
    while (!dir.NoFailures()) {
      ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                         doc1->stored.begin(), doc1->stored.end()));
      ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                         doc2->stored.begin(), doc2->stored.end()));

      writer->GetBatch().Remove(*query_doc2);

      ASSERT_THROW(writer->Begin(), irs::IoError);
    }

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    // successul attempt
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // check data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(1, reader->docs_count());
    ASSERT_EQ(1, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc1);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    ASSERT_EQ(1, segment.docs_count());       // total count of documents
    ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc3
    ASSERT_FALSE(docs_itr->next());
  }
}

TEST(index_death_test_formats_15,
     segment_components_sync_failure_1st_phase_flush) {
  GTEST_SKIP() << "TODO(mbkkt) Invesigate it";
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();
  auto query_doc2 = MakeByTerm("name", "B");

  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);

  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_1.2.sm");  // segment meta
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_2.doc");  // postings list (documents)
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_3.2.doc_mask");  // deleted docs
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_4.cm");  // column meta
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_5.cs");  // columnstore
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_6.ti");  // term index
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_7.tm");  // term data
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_8.pos");  // postings list (positions)
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_9.pay");  // postings list (offset + payload)

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    // segment meta
    while (!dir.NoFailures()) {
      ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                         doc1->stored.begin(), doc1->stored.end()));
      ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                         doc2->stored.begin(), doc2->stored.end()));

      writer->GetBatch().Remove(*query_doc2);

      ASSERT_THROW(writer->Begin(), irs::IoError);
    }

    // successul attempt
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // ensure no data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(0, reader->size());
    ASSERT_EQ(0, reader->docs_count());
    ASSERT_EQ(0, reader->live_docs_count());
  }

  {
    constexpr irs::IndexFeatures kAllFeatures =
      irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
      irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_1.2.sm");  // segment meta
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_2.doc");  // postings list (documents)
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_3.2.doc_mask");  // deleted docs
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_4.cm");  // column meta
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_5.cs");  // columnstore
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_6.ti");  // term index
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_7.tm");  // term data
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_8.pos");  // postings list (positions)
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_9.pay");  // postings list (offset + payload)

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    // segment meta
    while (!dir.NoFailures()) {
      ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                         doc1->stored.begin(), doc1->stored.end()));
      ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                         doc2->stored.begin(), doc2->stored.end()));

      writer->GetBatch().Remove(*query_doc2);

      ASSERT_THROW(writer->Begin(), irs::IoError);
    }

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    // successul attempt
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // check data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(1, reader->docs_count());
    ASSERT_EQ(1, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc1);

    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    ASSERT_EQ(1, segment.docs_count());       // total count of documents
    ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc3
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
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });
  const auto* doc1 = gen.next();

  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);

  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_1.0.sm");  // fail at segment meta creation
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_2.0.sm");  // fail at segment meta synchronization

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    // creation issue
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_THROW(writer->Begin(), irs::IoError);

    // synchornization issue
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_THROW(writer->Begin(), irs::IoError);

    // second attempt
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // ensure no data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(0, reader->size());
    ASSERT_EQ(0, reader->docs_count());
    ASSERT_EQ(0, reader->live_docs_count());
  }

  {
    constexpr irs::IndexFeatures kAllFeatures =
      irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
      irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_1.0.sm");  // fail at segment meta creation
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_2.0.sm");  // fail at segment meta synchronization

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    // creation issue
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_THROW(writer->Begin(), irs::IoError);

    // synchornization issue
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_THROW(writer->Begin(), irs::IoError);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    // second attempt
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // check data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(1, reader->docs_count());
    ASSERT_EQ(1, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc1);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    ASSERT_EQ(1, segment.docs_count());       // total count of documents
    ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc3
    ASSERT_FALSE(docs_itr->next());
  }
}

TEST(index_death_test_formats_15,
     segment_meta_write_fail_immediate_consolidation) {
  tests::JsonDocGenerator gen(
    TestBase::resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();

  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);

  {
    constexpr irs::IndexFeatures kAllFeatures =
      irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
      irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    // segment 0
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // register failures
    dir.RegisterFailure(
      FailingDirectory::Failure::CREATE,
      "_3.0.sm");  // fail at segment meta creation on consolidation
    dir.RegisterFailure(
      FailingDirectory::Failure::SYNC,
      "_4.0.sm");  // fail at segment meta synchronization on consolidation

    const irs::index_utils::ConsolidateCount consolidate_all;

    // segment meta creation failure
    ASSERT_THROW(
      writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)),
      irs::IoError);
    ASSERT_FALSE(writer->Begin());  // nothing to flush

    // segment meta synchronization failure
    ASSERT_TRUE(
      writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)));
    ASSERT_THROW(writer->Begin(), irs::IoError);
    ASSERT_FALSE(writer->Begin());  // nothing to flush

    // check data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader->size());
    ASSERT_EQ(2, reader->docs_count());
    ASSERT_EQ(2, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc1);
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc2);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore (segment 0)
    {
      auto& segment = reader[0];  // assume 0 is id of first/only segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }

    // validate columnstore (segment 1)
    {
      auto& segment = reader[1];  // assume 0 is id of first/only segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }
  }
}

TEST(index_death_test_formats_15,
     segment_meta_write_fail_deffered_consolidation) {
  tests::JsonDocGenerator gen(
    TestBase::resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();
  const auto* doc3 = gen.next();
  const auto* doc4 = gen.next();

  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);

  {
    constexpr irs::IndexFeatures kAllFeatures =
      irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
      irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    // segment 0
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // register failures
    dir.RegisterFailure(
      FailingDirectory::Failure::CREATE,
      "_4.0.sm");  // fail at segment meta creation on consolidation
    dir.RegisterFailure(
      FailingDirectory::Failure::SYNC,
      "_6.0.sm");  // fail at segment meta synchronization on consolidation

    const irs::index_utils::ConsolidateCount consolidate_all;

    // segment meta creation failure
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(writer->Begin());  // start transaction
    ASSERT_TRUE(writer->Consolidate(irs::index_utils::MakePolicy(
      consolidate_all)));            // register pending consolidation
    ASSERT_FALSE(writer->Commit());  // commit started transaction
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));
    ASSERT_THROW(
      writer->Begin(),
      irs::IoError);  // start transaction to commit pending consolidation

    // segment meta synchronization failure
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    ASSERT_TRUE(writer->Begin());  // start transaction
    ASSERT_TRUE(writer->Consolidate(irs::index_utils::MakePolicy(
      consolidate_all)));            // register pending consolidation
    ASSERT_FALSE(writer->Commit());  // commit started transaction
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));
    ASSERT_THROW(
      writer->Begin(),
      irs::IoError);  // start transaction to commit pending consolidation

    // check data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(4, reader->size());
    ASSERT_EQ(4, reader->docs_count());
    ASSERT_EQ(4, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc1);
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc2);
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc3);
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc4);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore (segment 0)
    {
      auto& segment = reader[0];  // assume 0 is id of first/only segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }

    // validate columnstore (segment 1)
    {
      auto& segment = reader[1];  // assume 0 is id of first/only segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }

    // validate columnstore (segment 2)
    {
      auto& segment = reader[2];  // assume 0 is id of first/only segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("C", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }

    // validate columnstore (segment 3)
    {
      auto& segment = reader[3];  // assume 0 is id of first/only segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("D", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }
  }
}

TEST(index_death_test_formats_15,
     segment_meta_write_fail_long_running_consolidation) {
  GTEST_SKIP() << "TODO(mbkkt) Invesigate it";
  tests::JsonDocGenerator gen(
    TestBase::resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();
  const auto* doc3 = gen.next();

  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);

  // segment meta creation failure
  {
    constexpr irs::IndexFeatures kAllFeatures =
      irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
      irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

    irs::MemoryDirectory impl;
    FailingDirectory failing_dir(impl);
    tests::BlockingDirectory dir(failing_dir, "_3.cs");

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    // segment 0
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // register failures
    failing_dir.RegisterFailure(
      FailingDirectory::Failure::CREATE,
      "_3.0.sm");  // fail at segment meta creation on consolidation

    dir.intermediate_commits_lock
      .lock();  // acquire directory lock, and block consolidation

    std::thread consolidation_thread([&writer]() {
      const irs::index_utils::ConsolidateCount consolidate_all;
      ASSERT_THROW(
        writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)),
        irs::IoError);  // consolidate
    });

    dir.wait_for_blocker();

    // add another segment
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    dir.intermediate_commits_lock.unlock();  // finish consolidation
    consolidation_thread.join();  // wait for the consolidation to complete

    // check data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(3, reader->size());
    ASSERT_EQ(3, reader->docs_count());
    ASSERT_EQ(3, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc1);
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc2);
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc3);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore (segment 0)
    {
      auto& segment = reader[0];  // assume 0 is id of first/only segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }

    // validate columnstore (segment 1)
    {
      auto& segment = reader[1];  // assume 0 is id of first/only segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }

    // validate columnstore (segment 2)
    {
      auto& segment = reader[2];  // assume 0 is id of first/only segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("C", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // segment meta synchonization failure
  {
    constexpr irs::IndexFeatures kAllFeatures =
      irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
      irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

    irs::MemoryDirectory impl;
    FailingDirectory failing_dir(impl);
    tests::BlockingDirectory dir(failing_dir, "_3.cs");

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    // segment 0
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // register failures
    failing_dir.RegisterFailure(
      FailingDirectory::Failure::SYNC,
      "_3.0.sm");  // fail at segment meta synchronization on consolidation

    dir.intermediate_commits_lock
      .lock();  // acquire directory lock, and block consolidation

    std::thread consolidation_thread([&writer]() {
      const irs::index_utils::ConsolidateCount consolidate_all;
      ASSERT_TRUE(writer->Consolidate(
        irs::index_utils::MakePolicy(consolidate_all)));  // consolidate
    });

    dir.wait_for_blocker();

    // add another segment
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    dir.intermediate_commits_lock.unlock();  // finish consolidation
    consolidation_thread.join();  // wait for the consolidation to complete

    // commit consolidation
    ASSERT_THROW(writer->Begin(), irs::IoError);

    // check data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(3, reader->size());
    ASSERT_EQ(3, reader->docs_count());
    ASSERT_EQ(3, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc1);
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc2);
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc3);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore (segment 0)
    {
      auto& segment = reader[0];  // assume 0 is id of first/only segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }

    // validate columnstore (segment 1)
    {
      auto& segment = reader[1];  // assume 0 is id of first/only segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }

    // validate columnstore (segment 2)
    {
      auto& segment = reader[2];  // assume 0 is id of first/only segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("C", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }
  }
}

TEST(index_death_test_formats_15, segment_components_write_fail_consolidation) {
  GTEST_SKIP() << "TODO(mbkkt) Invesigate it";
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();

  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);

  {
    constexpr irs::IndexFeatures kAllFeatures =
      irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
      irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    // segment 0
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // register failures
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_3.doc");  // postings list (documents)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_4.cm");  // column meta
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_5.ti");  // term index
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_6.tm");  // term data
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_7.pos");  // postings list (positions)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_8.pay");  // postings list (offset + payload)

    const irs::index_utils::ConsolidateCount consolidate_all;

    while (!dir.NoFailures()) {
      ASSERT_THROW(
        writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)),
        irs::IoError);
      ASSERT_FALSE(writer->Begin());  // nothing to flush
    }

    // check data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader->size());
    ASSERT_EQ(2, reader->docs_count());
    ASSERT_EQ(2, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc1);
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc2);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore (segment 0)
    {
      auto& segment = reader[0];  // assume 0 is id of first/only segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }

    // validate columnstore (segment 1)
    {
      auto& segment = reader[1];  // assume 0 is id of first/only segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }
  }
}

TEST(index_death_test_formats_15, segment_components_sync_fail_consolidation) {
  GTEST_SKIP() << "TODO(mbkkt) Invesigate it";
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();

  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);

  {
    constexpr irs::IndexFeatures kAllFeatures =
      irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
      irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    // segment 0
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // register failures
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_3.doc");  // postings list (documents)
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_4.cm");  // column meta
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_5.ti");  // term index
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_6.tm");  // term data
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_7.pos");  // postings list (positions)
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_8.pay");  // postings list (offset + payload)

    const irs::index_utils::ConsolidateCount consolidate_all;

    while (!dir.NoFailures()) {
      ASSERT_TRUE(
        writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)));
      ASSERT_THROW(writer->Begin(), irs::IoError);  // nothing to flush
      ASSERT_FALSE(writer->Begin());
    }

    // check data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader->size());
    ASSERT_EQ(2, reader->docs_count());
    ASSERT_EQ(2, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc1);
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc2);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore (segment 0)
    {
      auto& segment = reader[0];  // assume 0 is id of first/only segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }

    // validate columnstore (segment 1)
    {
      auto& segment = reader[1];  // assume 0 is id of first/only segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }
  }
}

TEST(index_death_test_formats_15, segment_components_fail_import) {
  GTEST_SKIP() << "TODO(mbkkt) Invesigate it";
  constexpr irs::IndexFeatures kAllFeatures =
    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
    irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();

  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);

  // create source segment
  irs::MemoryDirectory src_dir;

  {
    // write index
    auto writer = irs::IndexWriter::Make(src_dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(src_dir));
  }

  auto src_index = irs::DirectoryReader(src_dir);
  ASSERT_TRUE(src_index);

  // file creation failures
  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    // register failures
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_1.doc");  // postings list (documents)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_2.doc");  // postings list (documents)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_3.cm");  // column meta
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_4.ti");  // term index
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_5.tm");  // term data
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_6.pos");  // postings list (positions)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_7.pay");  // postings list (offset + payload)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_8.cs");  // columnstore
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_9.0.sm");  // segment meta

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    // initial commit
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    while (!dir.NoFailures()) {
      ASSERT_THROW(writer->Import(*src_index), irs::IoError);
      ASSERT_FALSE(writer->Begin());  // nothing to commit
    }

    // check data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(0, reader->size());
    ASSERT_EQ(0, reader->docs_count());
    ASSERT_EQ(0, reader->live_docs_count());
  }

  // file creation failures
  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    // register failures
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_1.doc");  // postings list (documents)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_2.doc");  // postings list (documents)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_3.cm");  // column meta
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_4.ti");  // term index
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_5.tm");  // term data
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_6.pos");  // postings list (positions)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_7.pay");  // postings list (offset + payload)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_8.cs");  // columnstore
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_9.0.sm");  // segment meta

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    // initial commit
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    while (!dir.NoFailures()) {
      ASSERT_THROW(writer->Import(*src_index), irs::IoError);
      ASSERT_FALSE(writer->Begin());  // nothing to commit
    }

    // successful commit
    ASSERT_TRUE(writer->Import(*src_index));
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // check data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(1, reader->docs_count());
    ASSERT_EQ(1, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc1);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore (segment 0)
    {
      auto& segment = reader[0];  // assume 0 is id of first/only segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // file synchronization failures
  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    // register failures
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_1.doc");  // postings list (documents)
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_2.doc");  // postings list (documents)
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_3.cm");  // column meta
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_4.ti");  // term index
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_5.tm");  // term data
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_6.pos");  // postings list (positions)
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_7.pay");  // postings list (offset + payload)
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_8.cs");  // columnstore
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_9.0.sm");  // segment meta

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    // initial commit
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    while (!dir.NoFailures()) {
      ASSERT_TRUE(writer->Import(*src_index));
      ASSERT_THROW(writer->Begin(), irs::IoError);  // nothing to commit
    }

    // check data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(0, reader->size());
    ASSERT_EQ(0, reader->docs_count());
    ASSERT_EQ(0, reader->live_docs_count());
  }

  // file synchronization failures
  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    // register failures
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_1.doc");  // postings list (documents)
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_2.doc");  // postings list (documents)
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_3.cm");  // column meta
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_4.ti");  // term index
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_5.tm");  // term data
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_6.pos");  // postings list (positions)
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_7.pay");  // postings list (offset + payload)
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_8.cs");  // columnstore
    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_9.0.sm");  // segment meta

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    // initial commit
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    while (!dir.NoFailures()) {
      ASSERT_TRUE(writer->Import(*src_index));
      ASSERT_THROW(writer->Begin(), irs::IoError);  // nothing to commit
    }

    // successful commit
    ASSERT_TRUE(writer->Import(*src_index));
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // check data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(1, reader->docs_count());
    ASSERT_EQ(1, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc1);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore (segment 0)
    {
      auto& segment = reader[0];  // assume 0 is id of first/only segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }
  }
}

TEST(index_death_test_formats_15,
     segment_components_creation_fail_implicit_segment_flush) {
  GTEST_SKIP() << "TODO(mbkkt) Invesigate it";
  constexpr irs::IndexFeatures kAllFeatures =
    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
    irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();
  const auto* doc3 = gen.next();

  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);

  size_t i{};
  // file creation failures
  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    // register failures
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_1.doc");  // postings list (documents)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_2.doc");  // postings list (documents)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_3.cm");  // column meta
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_4.ti");  // term index
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_5.tm");  // term data
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_6.pos");  // postings list (positions)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_7.pay");  // postings list (offset + payload)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_8.0.sm");  // segment meta

    // write index
    irs::IndexWriterOptions opts;
    opts.segment_docs_max = 1;  // flush every 2nd document

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate, opts);
    ASSERT_NE(nullptr, writer);

    // initial commit
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));
    i = 1;
    while (!dir.NoFailures()) {
      ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                         doc1->stored.begin(), doc1->stored.end()));
      if (i++ == 8) {
        ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                           doc2->stored.begin(), doc2->stored.end()));
        ASSERT_THROW(writer->Begin(), irs::IoError);
      } else {
        ASSERT_THROW(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                            doc2->stored.begin(), doc2->stored.end()),
                     irs::IoError);
      }
      ASSERT_FALSE(writer->Begin());  // nothing to commit
    }

    // check data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(0, reader->size());
    ASSERT_EQ(0, reader->docs_count());
    ASSERT_EQ(0, reader->live_docs_count());
  }

  // file creation failures
  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    // register failures
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_1.doc");  // postings list (documents)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_2.doc");  // postings list (documents)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_3.cm");  // column meta
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_4.ti");  // term index
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_5.tm");  // term data
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_6.pos");  // postings list (positions)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_7.pay");  // postings list (offset + payload)
    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_8.0.sm");  // segment meta

    // write index
    irs::IndexWriterOptions opts;
    opts.segment_docs_max = 1;  // flush every 2nd document

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate, opts);
    ASSERT_NE(nullptr, writer);

    // initial commit
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    i = 1;
    while (!dir.NoFailures()) {
      ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                         doc1->stored.begin(), doc1->stored.end()));
      if (i++ == 8) {
        ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                           doc2->stored.begin(), doc2->stored.end()));
        ASSERT_THROW(writer->Begin(), irs::IoError);
      } else {
        ASSERT_THROW(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                            doc2->stored.begin(), doc2->stored.end()),
                     irs::IoError);
      }

      ASSERT_FALSE(writer->Begin());  // nothing to commit
    }

    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));

    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // check data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(1, reader->docs_count());
    ASSERT_EQ(1, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc3);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    ASSERT_EQ(1, segment.docs_count());       // total count of documents
    ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("C", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc3
    ASSERT_FALSE(docs_itr->next());
  }
}

TEST(index_death_test_formats_15,
     columnstore_creation_fail_implicit_segment_flush) {
  constexpr irs::IndexFeatures kAllFeatures =
    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
    irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();

  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);

  // columnstore creation failure
  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    // write index
    irs::IndexWriterOptions opts;
    opts.segment_docs_max = 1;  // flush every 2nd document

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate, opts);
    ASSERT_NE(nullptr, writer);

    uint64_t length;

    // initial commit
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    // basic length check
    dir.length(length, "_1.csd");
    ASSERT_EQ(length, 0);

    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_2.csd");  // columnstore data creation failure

    ASSERT_THROW(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                        doc2->stored.begin(), doc2->stored.end()),
                 irs::IoError);

    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_3.csi");  // columnstore index creation failure

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    dir.length(length, "_3.csd");
    ASSERT_EQ(length, 0);

    ASSERT_THROW(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                        doc2->stored.begin(), doc2->stored.end()),
                 irs::IoError);

    dir.length(length, "_3.csd");
    ASSERT_GT(length, 0);

    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // check data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);

    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(1, reader->docs_count());
    ASSERT_EQ(1, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc1);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    ASSERT_EQ(1, segment.docs_count());       // total count of documents
    ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc3
    ASSERT_FALSE(docs_itr->next());
  }
}

TEST(index_death_test_formats_15,
     columnstore_creation_sync_fail_implicit_segment_flush) {
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();

  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);

  // columnstore creation + sync failures
  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    // write index
    irs::IndexWriterOptions opts;
    opts.segment_docs_max = 1;  // flush every 2nd document

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate, opts);
    ASSERT_NE(nullptr, writer);

    // initial commit
    ASSERT_TRUE(writer->Begin());
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_2.csd");  // columnstore data creation failure
    ASSERT_THROW(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                        doc2->stored.begin(), doc2->stored.end()),
                 irs::IoError);

    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_1.csd");  // columnstore data sync failure
    ASSERT_THROW(writer->Begin(), irs::IoError);

    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));

    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_3.csi");  // columnstore index creation failure
    ASSERT_THROW(writer->Begin(), irs::IoError);

    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));

    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_4.csi");  // columnstore index sync failure
    ASSERT_THROW(writer->Begin(), irs::IoError);

    // check data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(0, reader->size());
    ASSERT_EQ(0, reader->docs_count());
    ASSERT_EQ(0, reader->live_docs_count());
  }
}

TEST(index_death_test_formats_15, fails_in_consolidate_with_removals) {
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();

  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);

  constexpr irs::IndexFeatures kAllFeatures =
    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
    irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    // write index
    irs::IndexWriterOptions opts;

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate, opts);
    ASSERT_NE(nullptr, writer);

    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "pending_segments_1");  //
    // initial commit fails
    ASSERT_THROW(writer->Begin(), irs::IoError);

    dir.RegisterFailure(FailingDirectory::Failure::RENAME,
                        "pending_segments_1");  //
    ASSERT_THROW(writer->Commit(), irs::IoError);
    ASSERT_THROW((irs::DirectoryReader{dir}), irs::IndexNotFound);

    // now it is OK
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_1.csd");  // columnstore data creation failure
    ASSERT_THROW(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                        doc1->stored.begin(), doc1->stored.end()),
                 irs::IoError);

    // Nothing to commit
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));

    dir.RegisterFailure(FailingDirectory::Failure::CREATE,
                        "_2.csi");  // columnstore index creation failure
    ASSERT_THROW(writer->Commit(), irs::IoError);
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // Nothing to commit
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_3.csd");  // columnstore index creation failure
    ASSERT_THROW(writer->Commit(), irs::IoError);
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));

    dir.RegisterFailure(FailingDirectory::Failure::SYNC,
                        "_4.csi");  // columnstore index creation failure
    ASSERT_THROW(writer->Commit(), irs::IoError);
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // NOW IT IS OK

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    const irs::index_utils::ConsolidateCount consolidate_all;

    ASSERT_TRUE(
      writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)));
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    dir.RegisterFailure(FailingDirectory::Failure::REMOVE,
                        "_2.csd");  // columnstore data deletion failure
    dir.RegisterFailure(FailingDirectory::Failure::REMOVE,
                        "_3.csi");  // columnstore index deletion failure
    dir.RegisterFailure(FailingDirectory::Failure::REMOVE,
                        "_4.csd");  // columnstore data deletion failure
    dir.RegisterFailure(FailingDirectory::Failure::REMOVE,
                        "_4.csi");  // columnstore index deletion failure
    dir.RegisterFailure(FailingDirectory::Failure::REMOVE,
                        "_5.csi");  // columnstore index deletion failure
    dir.RegisterFailure(FailingDirectory::Failure::REMOVE,
                        "_6.csd");  // columnstore data deletion failure
    irs::DirectoryCleaner::clean(dir);
    ASSERT_FALSE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    ASSERT_TRUE(dir.NoFailures());

    // check data
    auto reader = irs::DirectoryReader{dir};
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(2, reader->docs_count());
    ASSERT_EQ(2, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc1);
    expected_index.back().insert(*doc2);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    ASSERT_EQ(2, segment.docs_count());       // total count of documents
    ASSERT_EQ(2, segment.live_docs_count());  // total count of documents
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc1
    ASSERT_TRUE(docs_itr->next());
    ASSERT_TRUE(values->next());
    actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }
}

TEST(index_death_test_formats_15, fails_in_exists) {
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);

  constexpr irs::IndexFeatures kAllFeatures =
    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
    irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();
  const auto* doc3 = gen.next();
  const auto* doc4 = gen.next();

  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);

  {
    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    // write index
    irs::IndexWriterOptions opts;

    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate, opts);
    ASSERT_NE(nullptr, writer);

    // Will force error during commit becuase of segment reader reopen.
    dir.RegisterFailure(FailingDirectory::Failure::EXISTS, "_1.csi");
    dir.RegisterFailure(FailingDirectory::Failure::EXISTS, "_2.csd");
    dir.RegisterFailure(FailingDirectory::Failure::EXISTS, "_3.csi");
    dir.RegisterFailure(FailingDirectory::Failure::EXISTS, "_4.csd");

    while (!dir.NoFailures()) {
      ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                         doc1->stored.begin(), doc1->stored.end()));
      ASSERT_THROW(writer->Commit(), irs::IoError);
      ASSERT_THROW((irs::DirectoryReader{dir}), irs::IndexNotFound);
    }

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    const irs::index_utils::ConsolidateCount consolidate_all;

    ASSERT_TRUE(
      writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)));
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    ASSERT_TRUE(dir.NoFailures());

    // check data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(4, reader->docs_count());
    ASSERT_EQ(4, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc1);
    expected_index.back().insert(*doc2);
    expected_index.back().insert(*doc3);
    expected_index.back().insert(*doc4);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    ASSERT_EQ(4, segment.docs_count());       // total count of documents
    ASSERT_EQ(4, segment.live_docs_count());  // total count of documents
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc1
    ASSERT_TRUE(docs_itr->next());
    ASSERT_TRUE(values->next());
    actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_TRUE(values->next());
    actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_EQ("C", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc3
    ASSERT_TRUE(docs_itr->next());
    ASSERT_TRUE(values->next());
    actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_EQ("D", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc4
    ASSERT_FALSE(docs_itr->next());
  }
}

TEST(index_death_test_formats_15, fails_in_length) {
  tests::JsonDocGenerator gen(
    TestBase::resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();
  const auto* doc3 = gen.next();
  const auto* doc4 = gen.next();

  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);

  {
    constexpr irs::IndexFeatures kAllFeatures =
      irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
      irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

    irs::MemoryDirectory impl;
    FailingDirectory dir(impl);

    dir.RegisterFailure(FailingDirectory::Failure::LENGTH, "_1.csd");
    dir.RegisterFailure(FailingDirectory::Failure::LENGTH, "_1.csi");
    dir.RegisterFailure(FailingDirectory::Failure::LENGTH,
                        "_1.ti");  // term index
    dir.RegisterFailure(FailingDirectory::Failure::LENGTH,
                        "_1.tm");  // term data
    dir.RegisterFailure(FailingDirectory::Failure::LENGTH,
                        "_1.pos");  // postings list (positions)
    dir.RegisterFailure(FailingDirectory::Failure::LENGTH, "_1.doc");

    dir.RegisterFailure(FailingDirectory::Failure::LENGTH, "_2.csd");
    dir.RegisterFailure(FailingDirectory::Failure::LENGTH, "_2.csi");
    dir.RegisterFailure(FailingDirectory::Failure::LENGTH,
                        "_2.ti");  // term index
    dir.RegisterFailure(FailingDirectory::Failure::LENGTH,
                        "_2.tm");  // term data
    dir.RegisterFailure(FailingDirectory::Failure::LENGTH,
                        "_2.pos");  // postings list (positions)
    dir.RegisterFailure(FailingDirectory::Failure::LENGTH, "_2.doc");

    dir.RegisterFailure(FailingDirectory::Failure::LENGTH, "_3.csd");
    dir.RegisterFailure(FailingDirectory::Failure::LENGTH, "_3.csi");
    dir.RegisterFailure(FailingDirectory::Failure::LENGTH,
                        "_3.ti");  // term index
    dir.RegisterFailure(FailingDirectory::Failure::LENGTH,
                        "_3.tm");  // term data
    dir.RegisterFailure(FailingDirectory::Failure::LENGTH,
                        "_3.pos");  // postings list (positions)
    dir.RegisterFailure(FailingDirectory::Failure::LENGTH, "_3.doc");

    dir.RegisterFailure(FailingDirectory::Failure::LENGTH, "_4.csd");
    dir.RegisterFailure(FailingDirectory::Failure::LENGTH, "_4.csi");
    dir.RegisterFailure(FailingDirectory::Failure::LENGTH,
                        "_4.ti");  // term index
    dir.RegisterFailure(FailingDirectory::Failure::LENGTH,
                        "_4.tm");  // term data
    dir.RegisterFailure(FailingDirectory::Failure::LENGTH,
                        "_4.pos");  // postings list (positions)
    dir.RegisterFailure(FailingDirectory::Failure::LENGTH, "_4.doc");

    const size_t num_failures = dir.NumFailures();

    // write index
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    // segment 0
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // segment 2
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    // segment 3
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    {
      // check data
      auto reader = irs::DirectoryReader(dir);
      ASSERT_TRUE(reader);
      ASSERT_EQ(4, reader->size());
      ASSERT_EQ(4, reader->docs_count());
      ASSERT_EQ(4, reader->live_docs_count());
    }

    // Commit without removals doesn't call `length`
    ASSERT_EQ(num_failures, dir.NumFailures());
    dir.ClearFailures();

    dir.RegisterFailure(FailingDirectory::Failure::EXISTS, "_1.csd");
    dir.RegisterFailure(FailingDirectory::Failure::EXISTS, "_1.csi");

    dir.RegisterFailure(FailingDirectory::Failure::EXISTS, "_2.csd");
    dir.RegisterFailure(FailingDirectory::Failure::EXISTS, "_2.csi");

    dir.RegisterFailure(FailingDirectory::Failure::EXISTS, "_3.csd");
    dir.RegisterFailure(FailingDirectory::Failure::EXISTS, "_3.csi");

    dir.RegisterFailure(FailingDirectory::Failure::EXISTS, "_4.csd");
    dir.RegisterFailure(FailingDirectory::Failure::EXISTS, "_4.csi");

    const irs::index_utils::ConsolidateCount consolidate_all;

    const auto num_failures_before = dir.NumFailures();
    ASSERT_TRUE(
      writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)));
    ASSERT_EQ(num_failures_before, dir.NumFailures());

    irs::DirectoryCleaner::clean(dir);
    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));

    ASSERT_EQ(num_failures_before, dir.NumFailures());

    // check data
    auto reader = irs::DirectoryReader(dir);
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader->size());
    ASSERT_EQ(4, reader->docs_count());
    ASSERT_EQ(4, reader->live_docs_count());

    // validate index
    tests::index_t expected_index;
    expected_index.emplace_back(writer->FeatureInfo());
    expected_index.back().insert(*doc1);
    expected_index.back().insert(*doc2);
    expected_index.back().insert(*doc3);
    expected_index.back().insert(*doc4);
    tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

    // validate columnstore (segment 0)
    {
      auto& segment = reader[0];  // assume 0 is id of first/only segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(4, segment.docs_count());       // total count of documents
      ASSERT_EQ(4, segment.live_docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc1
      ASSERT_TRUE(docs_itr->next());
      ASSERT_TRUE(values->next());
      actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_EQ("B", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc2
      ASSERT_TRUE(docs_itr->next());
      ASSERT_TRUE(values->next());
      actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_EQ("C", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc3
      ASSERT_TRUE(docs_itr->next());
      ASSERT_TRUE(values->next());
      actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_EQ("D", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc4
      ASSERT_FALSE(docs_itr->next());
    }
  }
}

TEST(index_death_test_formats_15, open_reader) {
  ::OpenReader("1_5avx", [](FailingDirectory& dir) {
    // postings list (documents)
    dir.RegisterFailure(FailingDirectory::Failure::OPEN, "_1.doc");
    // columnstore index
    dir.RegisterFailure(FailingDirectory::Failure::EXISTS, "_1.csi");
    // columnstore index
    dir.RegisterFailure(FailingDirectory::Failure::OPEN, "_1.csi");
    // columnstore data
    dir.RegisterFailure(FailingDirectory::Failure::EXISTS, "_1.csd");
    // columnstore data
    dir.RegisterFailure(FailingDirectory::Failure::OPEN, "_1.csd");
    // term index
    dir.RegisterFailure(FailingDirectory::Failure::OPEN, "_1.ti");
    // term data
    dir.RegisterFailure(FailingDirectory::Failure::OPEN, "_1.tm");
    // postings list (positions)
    dir.RegisterFailure(FailingDirectory::Failure::OPEN, "_1.pos");
    // postings list (offset + payload)
    dir.RegisterFailure(FailingDirectory::Failure::OPEN, "_1.pay");
  });
}

TEST(index_death_test_formats_15, columnstore_reopen_fail) {
  constexpr irs::IndexFeatures kAllFeatures =
    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
    irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();
  auto query_doc2 = MakeByTerm("name", "B");

  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);

  // create source segment
  irs::MemoryDirectory impl;
  FailingDirectory dir(impl);

  // write index
  {
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));

    writer->GetBatch().Remove(*query_doc2);

    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));
  }

  dir.RegisterFailure(FailingDirectory::Failure::OPEN,
                      "_1.csi");  // regiseter open failure in columnstore
  dir.RegisterFailure(FailingDirectory::Failure::OPEN,
                      "_1.csd");  // regiseter open failure in columnstore
  ASSERT_THROW(irs::DirectoryReader{dir}, irs::IoError);
  ASSERT_THROW(irs::DirectoryReader{dir}, irs::IoError);

  // check data
  auto reader = irs::DirectoryReader(dir);
  ASSERT_TRUE(reader);
  ASSERT_EQ(1, reader->size());
  ASSERT_EQ(2, reader->docs_count());
  ASSERT_EQ(1, reader->live_docs_count());

  // validate index
  tests::index_t expected_index;
  expected_index.emplace_back(::DefaultFeatureInfo());
  expected_index.back().insert(*doc1);
  expected_index.back().insert(*doc2);
  tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

  dir.RegisterFailure(FailingDirectory::Failure::REOPEN,
                      "_1.csd");  // regiseter reopen failure in columnstore
  dir.RegisterFailure(FailingDirectory::Failure::REOPEN,
                      "_1.csi");  // regiseter reopen failure in columnstore
  dir.RegisterFailure(FailingDirectory::Failure::ReopenNull,
                      "_1.csd");  // regiseter reopen failure in columnstore
  dir.RegisterFailure(FailingDirectory::Failure::ReopenNull,
                      "_1.csi");  // regiseter reopen failure in columnstore

  // validate columnstore
  auto& segment = reader[0];  // assume 0 is id of first/only segment
  const auto* column = segment.column("name");
  ASSERT_NE(nullptr, column);
  ASSERT_THROW(column->iterator(irs::ColumnHint::Normal),
               irs::IoError);  // failed to reopen csd
  ASSERT_THROW(column->iterator(irs::ColumnHint::Normal),
               irs::IoError);  // failed to reopen csd (nullptr)
  auto values = column->iterator(irs::ColumnHint::Normal);
  ASSERT_NE(nullptr, values);
  auto* actual_value = irs::get<irs::PayAttr>(*values);
  ASSERT_NE(nullptr, actual_value);
  ASSERT_EQ(2, segment.docs_count());       // total count of documents
  ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
  auto terms = segment.field("same");
  ASSERT_NE(nullptr, terms);
  auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
  ASSERT_TRUE(term_itr->next());
  auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
  ASSERT_TRUE(docs_itr->next());
  ASSERT_EQ(docs_itr->value(),
            values->seek(docs_itr->value()));  // successful attempt
  ASSERT_EQ("A", irs::ToString<std::string_view>(
                   actual_value->value.data()));  // 'name' value in doc3
  ASSERT_TRUE(docs_itr->next());
  ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
  ASSERT_EQ("B", irs::ToString<std::string_view>(
                   actual_value->value.data()));  // 'name' value in doc3
  ASSERT_FALSE(docs_itr->next());

  // validate live docs
  auto live_docs = segment.docs_iterator();
  ASSERT_TRUE(live_docs->next());
  ASSERT_EQ(1, live_docs->value());
  ASSERT_FALSE(live_docs->next());
  ASSERT_EQ(irs::doc_limits::eof(), live_docs->value());
}

TEST(index_death_test_formats_15, fails_in_dup) {
  constexpr irs::IndexFeatures kAllFeatures =
    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
    irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();
  const auto* doc3 = gen.next();
  const auto* doc4 = gen.next();

  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);

  // create source segment
  irs::MemoryDirectory impl;
  FailingDirectory dir(impl);

  // write index
  {
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end()));

    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));

    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end()));

    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));
  }
  // regiseter open failure in columnstore
  dir.RegisterFailure(FailingDirectory::Failure::OPEN, "_1.csi");
  // regiseter open failure in columnstore
  dir.RegisterFailure(FailingDirectory::Failure::OPEN, "_1.csd");
  ASSERT_THROW(irs::DirectoryReader{dir}, irs::IoError);
  ASSERT_THROW(irs::DirectoryReader{dir}, irs::IoError);

  // check data
  auto reader = irs::DirectoryReader(dir);
  ASSERT_TRUE(reader);
  ASSERT_EQ(1, reader->size());
  ASSERT_EQ(4, reader->docs_count());
  ASSERT_EQ(4, reader->live_docs_count());

  // validate index
  tests::index_t expected_index;
  expected_index.emplace_back(::DefaultFeatureInfo());
  expected_index.back().insert(*doc1);
  expected_index.back().insert(*doc2);
  expected_index.back().insert(*doc3);
  expected_index.back().insert(*doc4);
  tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);
  // regiseter dup failure in columnstore
  dir.RegisterFailure(FailingDirectory::Failure::DUP, "_1.csd");
  // regiseter dup failure in columnstore
  dir.RegisterFailure(FailingDirectory::Failure::DupNull, "_1.csd");

  auto& segment = reader[0];  // assume 0 is id of first/only segment
  const auto* column = segment.column("name");
  ASSERT_NE(nullptr, column);
  // failed to reopen csd
  ASSERT_THROW(column->iterator(irs::ColumnHint::Normal), irs::IoError);
  // failed to reopen csd (nullptr)
  ASSERT_THROW(column->iterator(irs::ColumnHint::Normal), irs::IoError);
  ASSERT_TRUE(dir.NoFailures());

  auto values = column->iterator(irs::ColumnHint::Normal);
  ASSERT_NE(nullptr, values);
  auto* actual_value = irs::get<irs::PayAttr>(*values);
  ASSERT_NE(nullptr, actual_value);
  ASSERT_EQ(4, segment.docs_count());       // total count of documents
  ASSERT_EQ(4, segment.live_docs_count());  // total count of documents
  auto terms = segment.field("same");
  ASSERT_NE(nullptr, terms);
  auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
  ASSERT_TRUE(term_itr->next());
  auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
  ASSERT_TRUE(docs_itr->next());

  ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));  // '1' and '1'
  // 'name' value in doc1
  ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
  ASSERT_TRUE(docs_itr->next());

  ASSERT_EQ(docs_itr->value(), 2);
  // because 2nd document is not stored
  ASSERT_EQ(values->seek(docs_itr->value()), 3);

  // 'name' value in doc3. because
  // 2nd document is not stored
  ASSERT_EQ("C", irs::ToString<std::string_view>(actual_value->value.data()));
  ASSERT_TRUE(docs_itr->next());

  ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));  // '3' and '3'
  ASSERT_EQ("C", irs::ToString<std::string_view>(actual_value->value.data()));
  ASSERT_TRUE(docs_itr->next());

  ASSERT_EQ(docs_itr->value(), 4);
  // because 4th document is not stored
  ASSERT_NE(values->seek(docs_itr->value()), 4);
  ASSERT_FALSE(docs_itr->next());
}

TEST(index_death_test_formats_15, postings_reopen_fail) {
  constexpr irs::IndexFeatures kAllFeatures =
    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
    irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

  constexpr irs::IndexFeatures kPositions =
    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;

  constexpr irs::IndexFeatures kPositionsOffsets = irs::IndexFeatures::Freq |
                                                   irs::IndexFeatures::Pos |
                                                   irs::IndexFeatures::Offs;

  constexpr irs::IndexFeatures kPositionsPayload = irs::IndexFeatures::Freq |
                                                   irs::IndexFeatures::Pos |
                                                   irs::IndexFeatures::Pay;

  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();
  auto query_doc2 = MakeByTerm("name", "B");

  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);

  // create source segment
  irs::MemoryDirectory impl;
  FailingDirectory dir(impl);

  // write index
  {
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));

    writer->GetBatch().Remove(*query_doc2);

    ASSERT_TRUE(writer->Commit());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir));
  }

  // check data
  auto reader = irs::DirectoryReader(dir);
  ASSERT_TRUE(reader);
  ASSERT_EQ(1, reader->size());
  ASSERT_EQ(2, reader->docs_count());
  ASSERT_EQ(1, reader->live_docs_count());

  // validate index
  tests::index_t expected_index;
  expected_index.emplace_back(::DefaultFeatureInfo());
  expected_index.back().insert(*doc1);
  expected_index.back().insert(*doc2);
  tests::AssertIndex(reader.GetImpl(), expected_index, kAllFeatures);

  // validate columnstore
  auto& segment = reader[0];  // assume 0 is id of first/only segment
  const auto* column = segment.column("name");
  ASSERT_NE(nullptr, column);
  auto values = column->iterator(irs::ColumnHint::Normal);
  ASSERT_NE(nullptr, values);
  auto* actual_value = irs::get<irs::PayAttr>(*values);
  ASSERT_NE(nullptr, actual_value);
  ASSERT_EQ(2, segment.docs_count());       // total count of documents
  ASSERT_EQ(1, segment.live_docs_count());  // total count of documents
  auto terms = segment.field("same_anl_pay");
  ASSERT_NE(nullptr, terms);

  // regiseter reopen failure in term dictionary
  {
    dir.RegisterFailure(FailingDirectory::Failure::REOPEN, "_1.tm");
    auto term_itr =
      terms->iterator(irs::SeekMode::NORMAL);  // successful attempt
    ASSERT_NE(nullptr, term_itr);
    ASSERT_THROW(term_itr->next(), irs::IoError);
  }

  // regiseter reopen failure in term dictionary (nullptr)
  {
    dir.RegisterFailure(FailingDirectory::Failure::ReopenNull, "_1.tm");
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
  ASSERT_THROW((void)term_itr->postings(kPositionsPayload), irs::IoError);
  // regiseter reopen failure in payload (nullptr)
  dir.RegisterFailure(FailingDirectory::Failure::ReopenNull, "_1.pay");
  // can't reopen position (nullptr)
  ASSERT_THROW((void)term_itr->postings(kPositionsPayload), irs::IoError);

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
  // successful attempt
  ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
  // 'name' value in doc3
  ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
  ASSERT_TRUE(docs_itr->next());
  ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
  // 'name' value in doc3
  ASSERT_EQ("B", irs::ToString<std::string_view>(actual_value->value.data()));
  ASSERT_FALSE(docs_itr->next());

  // validate live docs
  auto live_docs = segment.docs_iterator();
  ASSERT_TRUE(live_docs->next());
  ASSERT_EQ(1, live_docs->value());
  ASSERT_FALSE(live_docs->next());
  ASSERT_EQ(irs::doc_limits::eof(), live_docs->value());
}
