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

#include <absl/strings/str_cat.h>

#include <filesystem>
#include <iresearch/index/index_features.hpp>
#include <iresearch/search/filters/term_filter.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <magic_enum/magic_enum.hpp>
#include <thread>

#include "formats/column/test_cs_helpers.hpp"
#include "hook_file_system.hpp"
#include "index_dir.hpp"
#include "index_tests.hpp"
#include "insert_field.hpp"
#include "tests_shared.hpp"

namespace {

// Per-doc cs column id used by tests in this file. Holds the bytes of the
// indexed StringField named "name"; readback through `segment.Column(kNameId)`
// + `BlobPointReader` decodes back to the original string with `ReadStoredStr`.
// Also used as the field_id of the "name" StringField for removals that
// delete by name (`MakeByTerm(kNameId, "B")`), so it must equal
// `FieldIdFor("name")`.
inline constexpr irs::field_id kNameId = tests::FieldIdFor("name");
// Stable per-name field ids used by `segment.field(...)` lookups in this
// file. Tests only use these to address the term reader for a known JSON
// field name; the on-disk index keys fields by id.
inline constexpr irs::field_id kSameId = tests::FieldIdFor("same");

tests::Terms MakeByTerm(irs::field_id field_id, std::string_view value) {
  return {field_id, {std::string{value}}};
}

// Insert `doc->indexed` into `writer` and capture the indexed StringField
// named "name" into the cs column under `kNameId` at the same DocId. Tests
// then read it back with `segment.Column(kNameId)` + `BlobPointReader`.
bool InsertWithName(tests::IndexWriter& writer, const tests::Document& doc) {
  auto ctx = writer.GetBatch();
  {
    auto d = ctx.Insert();
    if (!tests::InsertFields(d, doc.indexed.begin(), doc.indexed.end())) {
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

class FailingFileSystem final : public tests::HookFileSystem {
 public:
  enum class Failure : size_t {
    CREATE = 0,
    OPEN,
    REMOVE,
    SYNC,
    CLOSE,
  };

  FailingFileSystem() : FailingFileSystem{tests::DatabaseFileSystem()} {}

  explicit FailingFileSystem(duckdb::FileSystem& inner) noexcept
    : tests::HookFileSystem{inner} {}

  bool RegisterFailure(Failure type, const std::string& name) {
    return _failures.emplace(name, type).second;
  }

  size_t NumFailures() const noexcept { return _failures.size(); }

  bool NoFailures() const noexcept { return _failures.empty(); }

  void OnCreate(std::string_view name) final { Inject(Failure::CREATE, name); }
  void OnOpen(std::string_view name) final { Inject(Failure::OPEN, name); }
  void OnRemove(std::string_view name) final { Inject(Failure::REMOVE, name); }
  void OnSync(std::string_view name) final { Inject(Failure::SYNC, name); }
  void OnClose(std::string_view name) final { Inject(Failure::CLOSE, name); }

 private:
  void Inject(Failure type, std::string_view name) {
    auto it = _failures.find(std::make_pair(std::string{name}, type));

    if (_failures.end() == it) {
      return;
    }

    _failures.erase(it);
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_IO_ERROR),
      ERR_MSG("injected ", magic_enum::enum_name(type), " failure: ", name));
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

  std::set<FailT, FailLess> _failures;
};

class IndexDeathTest : public TestBase {
 protected:
  tests::IndexDir LocalFiles(duckdb::FileSystem& fs) {
    const auto path = test_dir() / absl::StrCat("index", ++_roots);
    std::filesystem::create_directories(path);
    return tests::IndexDir{irs::DuckDBEngine::Instance().instance(), fs,
                           path.string()};
  }

  void OpenReader(
    std::function<void(FailingFileSystem& fs)> failure_registerer);

 private:
  size_t _roots = 0;
};

void IndexDeathTest::OpenReader(
  std::function<void(FailingFileSystem& fs)> failure_registerer) {
  constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                              irs::IndexFeatures::Pos |
                                              irs::IndexFeatures::Offs;

  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();
  auto query_doc2 = MakeByTerm(kNameId, "B");

  // create source segment
  FailingFileSystem fs;
  const auto dir = LocalFiles(fs);

  // write index
  {
    auto writer = tests::IndexWriter::Make(dir, tests::kOmCreate,
                                           irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(writer->RefreshCommit());

    tests::Remove(*writer, query_doc2);

    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      tests::OpenReader(dir, irs::tests::DefaultReaderOptions()));
  }

  failure_registerer(fs);

  while (!fs.NoFailures()) {
    ASSERT_SQL_ERROR(
      (tests::OpenReader(dir, irs::tests::DefaultReaderOptions())),
      ERRCODE_IO_ERROR);
  }

  // check data
  auto reader = tests::OpenReader(dir, irs::tests::DefaultReaderOptions());
  ASSERT_EQ(1, reader.size());
  ASSERT_EQ(2, reader.docs_count());
  ASSERT_EQ(1, reader.live_docs_count());

  // validate index
  tests::index_t expected_index;
  expected_index.emplace_back();
  expected_index.back().insert(*doc1);
  expected_index.back().insert(*doc2);
  tests::AssertIndex(reader, expected_index, kAllFeatures);

  // validate columnstore
  auto& segment = reader[0];
  const auto* column = segment.Column(kNameId);
  ASSERT_NE(nullptr, column);
  irs::tests::BlobPointReader values{segment, *column};
  ASSERT_EQ(2, segment.docs_count());
  ASSERT_EQ(1, segment.live_docs_count());
  auto terms = segment.field(kSameId);
  ASSERT_NE(nullptr, terms);
  auto term_itr = terms->iterator();
  ASSERT_TRUE(term_itr->next());
  auto docs_itr = tests::Docs(term_itr->postings(irs::IndexFeatures::None));
  ASSERT_TRUE(!irs::doc_limits::eof(docs_itr->Next()));
  ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                   values, docs_itr->Value()));
  ASSERT_TRUE(!irs::doc_limits::eof(docs_itr->Next()));
  ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                   values, docs_itr->Value()));
  ASSERT_FALSE(!irs::doc_limits::eof(docs_itr->Next()));

  // validate live docs
  auto live_docs = segment.docs_iterator();
  ASSERT_EQ(1, live_docs->Next());
  ASSERT_TRUE(irs::doc_limits::eof(live_docs->Next()));
}

using index_death_test_formats_15 = IndexDeathTest;

}  // namespace

TEST_F(index_death_test_formats_15, open_reader) {
  OpenReader([](FailingFileSystem& fs) {
    // postings list (documents)
    fs.RegisterFailure(FailingFileSystem::Failure::OPEN,
                       "0000000000000001.doc");
    // columnstore
    fs.RegisterFailure(FailingFileSystem::Failure::OPEN,
                       "0000000000000001.col");
    // term dictionary (.idx: term index + term data)
    fs.RegisterFailure(FailingFileSystem::Failure::OPEN,
                       "0000000000000001.idx");
    // postings list (positions)
    fs.RegisterFailure(FailingFileSystem::Failure::OPEN,
                       "0000000000000001.pos");
    // postings list (offset + payload)
    fs.RegisterFailure(FailingFileSystem::Failure::OPEN,
                       "0000000000000001.pay");
  });
}

// =======================================================================
// Failure-injection coverage for the `.col` columnstore. The
// `irs::ColWriter` emits one `<segment>.col` file per segment
// (see `kColFormatExt` in
// iresearch/formats/column/col_reader.hpp), so
// per-segment columnstore failures register against `_N.col`.
// =======================================================================

TEST_F(index_death_test_formats_15,
       segment_columnstore_creation_failure_1st_phase_flush) {
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();

  // Phase 1: columnstore creation fails on the very first segment.
  // SegmentWriter::reset(meta) wires the columnstore Writer, which creates
  // `0000000000000001.col` and throws on failure.
  {
    FailingFileSystem fs;
    const auto dir = LocalFiles(fs);
    fs.RegisterFailure(FailingFileSystem::Failure::CREATE,
                       "0000000000000001.col");

    auto writer = tests::IndexWriter::Make(dir, tests::kOmCreate,
                                           irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // First insert triggers segment_writer::reset(meta) -> cs Writer
    // constructor -> creating `0000000000000001.col` -> throw.
    ASSERT_SQL_ERROR(InsertWithName(*writer, *doc1), ERRCODE_IO_ERROR);

    ASSERT_FALSE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      tests::OpenReader(dir, irs::tests::DefaultReaderOptions()));

    auto reader = tests::OpenReader(dir, irs::tests::DefaultReaderOptions());
    ASSERT_EQ(0, reader.size());
    ASSERT_EQ(0, reader.docs_count());
    ASSERT_EQ(0, reader.live_docs_count());
  }

  // Phase 2: first segment's cs fails, retry with another doc succeeds.
  {
    constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                                irs::IndexFeatures::Pos |
                                                irs::IndexFeatures::Offs;

    FailingFileSystem fs;
    const auto dir = LocalFiles(fs);
    fs.RegisterFailure(FailingFileSystem::Failure::CREATE,
                       "0000000000000001.col");

    auto writer = tests::IndexWriter::Make(dir, tests::kOmCreate,
                                           irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    ASSERT_SQL_ERROR(InsertWithName(*writer, *doc2), ERRCODE_IO_ERROR);
    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      tests::OpenReader(dir, irs::tests::DefaultReaderOptions()));

    auto reader = tests::OpenReader(dir, irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(1, reader.docs_count());
    ASSERT_EQ(1, reader.live_docs_count());

    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    tests::AssertIndex(reader, expected_index, kAllFeatures);

    auto& segment = reader[0];
    const auto* column = segment.Column(kNameId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    ASSERT_EQ(1, segment.docs_count());
    ASSERT_EQ(1, segment.live_docs_count());
    auto terms = segment.field(kSameId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator();
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = tests::Docs(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(!irs::doc_limits::eof(docs_itr->Next()));
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->Value()));
    ASSERT_FALSE(!irs::doc_limits::eof(docs_itr->Next()));
  }

  // Phase 3: second segment's cs fails, first segment is preserved.
  {
    constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                                irs::IndexFeatures::Pos |
                                                irs::IndexFeatures::Offs;

    FailingFileSystem fs;
    const auto dir = LocalFiles(fs);
    fs.RegisterFailure(FailingFileSystem::Failure::CREATE,
                       "0000000000000002.col");

    auto writer = tests::IndexWriter::Make(dir, tests::kOmCreate,
                                           irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      tests::OpenReader(dir, irs::tests::DefaultReaderOptions()));

    ASSERT_SQL_ERROR(InsertWithName(*writer, *doc2), ERRCODE_IO_ERROR);

    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      tests::OpenReader(dir, irs::tests::DefaultReaderOptions()));

    auto reader = tests::OpenReader(dir, irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(2, reader.docs_count());
    ASSERT_EQ(2, reader.live_docs_count());

    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    expected_index.emplace_back();
    expected_index.back().insert(*doc2);
    tests::AssertIndex(reader, expected_index, kAllFeatures);

    {
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(1, segment.docs_count());
      ASSERT_EQ(1, segment.live_docs_count());
      auto terms = segment.field(kSameId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator();
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = tests::Docs(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(!irs::doc_limits::eof(docs_itr->Next()));
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->Value()));
      ASSERT_FALSE(!irs::doc_limits::eof(docs_itr->Next()));
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
      auto term_itr = terms->iterator();
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = tests::Docs(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(!irs::doc_limits::eof(docs_itr->Next()));
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->Value()));
      ASSERT_FALSE(!irs::doc_limits::eof(docs_itr->Next()));
    }
  }
}

TEST_F(index_death_test_formats_15,
       segment_components_creation_failure_1st_phase_flush) {
  // Per-segment file creation order (verified empirically): `_N.col` is
  // created at *insert* time (SegmentWriter::reset(meta) -> cs Writer
  // ctor), then at Begin() the segment flush creates
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

  // Helper that drives one round: register one failure, try
  // insert+insert+remove+Begin, expect *some* throw to consume the
  // failure. Returns when the failure budget is empty.
  auto DriveCreateFailures = [&](FailingFileSystem& fs,
                                 tests::IndexWriter& writer) {
    while (!fs.NoFailures()) {
      const auto failures_before = fs.NumFailures();
      bool inserts_ok = true;
      try {
        if (!InsertWithName(writer, *doc1)) {
          inserts_ok = false;
        } else if (!InsertWithName(writer, *doc2)) {
          inserts_ok = false;
        }
      } catch (const irs::SqlException& e) {
        ASSERT_EQ(e.error().errcode, ERRCODE_IO_ERROR) << e.message();
        // cs CREATE on `_N.col` threw at insert-time.
        ASSERT_LT(fs.NumFailures(), failures_before);
        continue;
      }
      if (inserts_ok) {
        tests::Remove(writer, query_doc2);
        ASSERT_SQL_ERROR(writer.RefreshBegin(), ERRCODE_IO_ERROR);
        ASSERT_LT(fs.NumFailures(), failures_before);
      }
    }
  };

  {
    FailingFileSystem fs;
    const auto dir = LocalFiles(fs);
    // One failure per per-segment file. Each retry's segment id
    // advances, so the failing file is on a different segment. Five
    // syncable files per segment: cs -> idx -> doc -> pos -> pay.
    fs.RegisterFailure(FailingFileSystem::Failure::CREATE,
                       "0000000000000001.col");  // cs (insert-time)
    fs.RegisterFailure(FailingFileSystem::Failure::CREATE,
                       "0000000000000002.idx");  // term index + data (merged)
    fs.RegisterFailure(FailingFileSystem::Failure::CREATE,
                       "0000000000000003.doc");  // postings list (documents)
    fs.RegisterFailure(FailingFileSystem::Failure::CREATE,
                       "0000000000000004.pos");  // postings list (positions)
    fs.RegisterFailure(
      FailingFileSystem::Failure::CREATE,
      "0000000000000005.pay");  // postings list (offset + payload)

    auto writer = tests::IndexWriter::Make(dir, tests::kOmCreate,
                                           irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    DriveCreateFailures(fs, *writer);

    auto reader = tests::OpenReader(dir, irs::tests::DefaultReaderOptions());
    ASSERT_EQ(0, reader.size());
    ASSERT_EQ(0, reader.docs_count());
    ASSERT_EQ(0, reader.live_docs_count());
  }

  // Same failures, then a successful insert + commit afterwards.
  {
    constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                                irs::IndexFeatures::Pos |
                                                irs::IndexFeatures::Offs;

    FailingFileSystem fs;
    const auto dir = LocalFiles(fs);
    fs.RegisterFailure(FailingFileSystem::Failure::CREATE,
                       "0000000000000001.col");
    fs.RegisterFailure(FailingFileSystem::Failure::CREATE,
                       "0000000000000002.idx");
    fs.RegisterFailure(FailingFileSystem::Failure::CREATE,
                       "0000000000000003.doc");
    fs.RegisterFailure(FailingFileSystem::Failure::CREATE,
                       "0000000000000004.pos");
    fs.RegisterFailure(FailingFileSystem::Failure::CREATE,
                       "0000000000000005.pay");

    auto writer = tests::IndexWriter::Make(dir, tests::kOmCreate,
                                           irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    DriveCreateFailures(fs, *writer);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      tests::OpenReader(dir, irs::tests::DefaultReaderOptions()));

    auto reader = tests::OpenReader(dir, irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(1, reader.docs_count());
    ASSERT_EQ(1, reader.live_docs_count());

    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    tests::AssertIndex(reader, expected_index, kAllFeatures);

    auto& segment = reader[0];
    const auto* column = segment.Column(kNameId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    ASSERT_EQ(1, segment.docs_count());
    ASSERT_EQ(1, segment.live_docs_count());
    auto terms = segment.field(kSameId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator();
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = tests::Docs(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(!irs::doc_limits::eof(docs_itr->Next()));
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->Value()));
    ASSERT_FALSE(!irs::doc_limits::eof(docs_itr->Next()));
  }
}

TEST_F(index_death_test_formats_15,
       segment_components_close_failure_1st_phase_flush) {
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc = gen.next();

  {
    FailingFileSystem fs;
    const auto dir = LocalFiles(fs);
    for (const auto* name : {"0000000000000001.col", "0000000000000002.doc",
                             "0000000000000003.pos", "0000000000000004.idx",
                             "0000000000000005.pay"}) {
      fs.RegisterFailure(FailingFileSystem::Failure::CLOSE, name);
    }
    auto writer = tests::IndexWriter::Make(dir, tests::kOmCreate,
                                           irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);
    while (!fs.NoFailures()) {
      const auto failures_before = fs.NumFailures();
      ASSERT_TRUE(InsertWithName(*writer, *doc));
      ASSERT_SQL_ERROR(writer->RefreshBegin(), ERRCODE_IO_ERROR);
      ASSERT_LT(fs.NumFailures(), failures_before);
    }
    ASSERT_TRUE(InsertWithName(*writer, *doc));
    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());

    auto reader = tests::OpenReader(dir, irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(1, reader.docs_count());
    ASSERT_EQ(1, reader.live_docs_count());
  }
}

TEST_F(index_death_test_formats_15,
       segment_components_sync_failure_1st_phase_flush) {
  // SYNC failures on each per-segment file. Each retry advances the
  // segment id.
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();
  auto query_doc2 = MakeByTerm(kNameId, "B");

  // Helper: drive one round of inserts + Begin(), expecting a sync
  // failure to throw on Begin. Each retry advances the segment id.
  // Do not use Remove() here: the doc_mask sync surface would obscure
  // which per-file sync failure was actually triggered.
  auto DriveSyncFailures = [&](FailingFileSystem& fs,
                               tests::IndexWriter& writer) {
    while (!fs.NoFailures()) {
      const auto failures_before = fs.NumFailures();
      ASSERT_TRUE(InsertWithName(writer, *doc1));
      ASSERT_SQL_ERROR(writer.RefreshBegin(), ERRCODE_IO_ERROR);
      ASSERT_LT(fs.NumFailures(), failures_before);
    }
  };
  (void)doc2;
  (void)query_doc2;

  {
    FailingFileSystem fs;
    const auto dir = LocalFiles(fs);
    fs.RegisterFailure(FailingFileSystem::Failure::SYNC,
                       "0000000000000001.col");  // columnstore
    fs.RegisterFailure(FailingFileSystem::Failure::SYNC,
                       "0000000000000002.doc");  // postings list (documents)
    fs.RegisterFailure(FailingFileSystem::Failure::SYNC,
                       "0000000000000003.pos");  // postings list (positions)
    fs.RegisterFailure(FailingFileSystem::Failure::SYNC,
                       "0000000000000004.idx");  // term index + data (merged)
    fs.RegisterFailure(
      FailingFileSystem::Failure::SYNC,
      "0000000000000005.pay");  // postings list (offset + payload)

    auto writer = tests::IndexWriter::Make(dir, tests::kOmCreate,
                                           irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    DriveSyncFailures(fs, *writer);

    ASSERT_FALSE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      tests::OpenReader(dir, irs::tests::DefaultReaderOptions()));

    auto reader = tests::OpenReader(dir, irs::tests::DefaultReaderOptions());
    ASSERT_EQ(0, reader.size());
    ASSERT_EQ(0, reader.docs_count());
    ASSERT_EQ(0, reader.live_docs_count());
  }

  // Same failures, then a successful insert + commit afterwards.
  {
    constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                                irs::IndexFeatures::Pos |
                                                irs::IndexFeatures::Offs;

    FailingFileSystem fs;
    const auto dir = LocalFiles(fs);
    fs.RegisterFailure(FailingFileSystem::Failure::SYNC,
                       "0000000000000001.col");
    fs.RegisterFailure(FailingFileSystem::Failure::SYNC,
                       "0000000000000002.doc");
    fs.RegisterFailure(FailingFileSystem::Failure::SYNC,
                       "0000000000000003.pos");
    fs.RegisterFailure(FailingFileSystem::Failure::SYNC,
                       "0000000000000004.idx");
    fs.RegisterFailure(FailingFileSystem::Failure::SYNC,
                       "0000000000000005.pay");

    auto writer = tests::IndexWriter::Make(dir, tests::kOmCreate,
                                           irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    DriveSyncFailures(fs, *writer);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_FALSE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      tests::OpenReader(dir, irs::tests::DefaultReaderOptions()));

    auto reader = tests::OpenReader(dir, irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(1, reader.docs_count());
    ASSERT_EQ(1, reader.live_docs_count());

    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    tests::AssertIndex(reader, expected_index, kAllFeatures);

    auto& segment = reader[0];
    const auto* column = segment.Column(kNameId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    ASSERT_EQ(1, segment.docs_count());
    ASSERT_EQ(1, segment.live_docs_count());
    auto terms = segment.field(kSameId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator();
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = tests::Docs(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(!irs::doc_limits::eof(docs_itr->Next()));
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->Value()));
    ASSERT_FALSE(!irs::doc_limits::eof(docs_itr->Next()));
  }
}

TEST_F(index_death_test_formats_15, segment_components_write_fail_compaction) {
  // Register CREATE failures on every per-segment component
  // (`_N.col`, `_N.idx`, `_N.doc`, `_N.pos`, `_N.pay`).
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();

  {
    constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                                irs::IndexFeatures::Pos |
                                                irs::IndexFeatures::Offs;

    FailingFileSystem fs;
    const auto dir = LocalFiles(fs);

    auto writer = tests::IndexWriter::Make(dir, tests::kOmCreate,
                                           irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 0
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      tests::OpenReader(dir, irs::tests::DefaultReaderOptions()));

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      tests::OpenReader(dir, irs::tests::DefaultReaderOptions()));

    // Register CREATE failures across the compacted-segment
    // components. Each compaction attempt allocates a fresh
    // segment id (NextSegmentId()), so after `_3.col` fails the next
    // attempt's segment is `_4`, the one after is `_5`, etc. Order
    // matches the order files are created during MergeWriter::Flush:
    // cs writer is opened first (in OpenColumnstoreContexts), then
    // postings, then term index/data, etc.
    fs.RegisterFailure(
      FailingFileSystem::Failure::CREATE,
      "0000000000000003.col");  // columnstore for compacted seg
    fs.RegisterFailure(FailingFileSystem::Failure::CREATE,
                       "0000000000000004.idx");  // term index + data (merged)
    fs.RegisterFailure(FailingFileSystem::Failure::CREATE,
                       "0000000000000005.doc");  // postings list (documents)
    fs.RegisterFailure(FailingFileSystem::Failure::CREATE,
                       "0000000000000006.pos");  // postings list (positions)
    fs.RegisterFailure(
      FailingFileSystem::Failure::CREATE,
      "0000000000000007.pay");  // postings list (offset + payload)

    const irs::index_utils::CompactionCount compact_all;

    while (!fs.NoFailures()) {
      ASSERT_SQL_ERROR(
        writer->Compact(irs::index_utils::MakePolicy(compact_all)),
        ERRCODE_IO_ERROR);
      ASSERT_FALSE(writer->RefreshBegin());  // nothing to flush
    }

    auto reader = tests::OpenReader(dir, irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(2, reader.docs_count());
    ASSERT_EQ(2, reader.live_docs_count());

    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    expected_index.emplace_back();
    expected_index.back().insert(*doc2);
    tests::AssertIndex(reader, expected_index, kAllFeatures);

    {
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator();
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = tests::Docs(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(!irs::doc_limits::eof(docs_itr->Next()));
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->Value()));
      ASSERT_FALSE(!irs::doc_limits::eof(docs_itr->Next()));
    }
    {
      auto& segment = reader[1];
      const auto* column = segment.Column(kNameId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator();
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = tests::Docs(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(!irs::doc_limits::eof(docs_itr->Next()));
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->Value()));
      ASSERT_FALSE(!irs::doc_limits::eof(docs_itr->Next()));
    }
  }
}

TEST_F(index_death_test_formats_15, segment_components_sync_fail_compaction) {
  // Like the *_write_fail_compaction* test but with SYNC failures
  // (compaction creates the files, then sync fails at Begin()).
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();

  {
    constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                                irs::IndexFeatures::Pos |
                                                irs::IndexFeatures::Offs;

    FailingFileSystem fs;
    const auto dir = LocalFiles(fs);

    auto writer = tests::IndexWriter::Make(dir, tests::kOmCreate,
                                           irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      tests::OpenReader(dir, irs::tests::DefaultReaderOptions()));

    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      tests::OpenReader(dir, irs::tests::DefaultReaderOptions()));

    // Each iteration consumes one failure; segment id advances each retry.
    fs.RegisterFailure(FailingFileSystem::Failure::SYNC,
                       "0000000000000003.col");
    fs.RegisterFailure(FailingFileSystem::Failure::SYNC,
                       "0000000000000004.doc");
    fs.RegisterFailure(FailingFileSystem::Failure::SYNC,
                       "0000000000000005.idx");
    fs.RegisterFailure(FailingFileSystem::Failure::SYNC,
                       "0000000000000006.idx");
    fs.RegisterFailure(FailingFileSystem::Failure::SYNC,
                       "0000000000000007.pos");
    fs.RegisterFailure(FailingFileSystem::Failure::SYNC,
                       "0000000000000008.pay");

    const irs::index_utils::CompactionCount compact_all;

    while (!fs.NoFailures()) {
      ASSERT_SQL_ERROR(
        writer->Compact(irs::index_utils::MakePolicy(compact_all)),
        ERRCODE_IO_ERROR);
      ASSERT_FALSE(writer->RefreshBegin());
    }

    auto reader = tests::OpenReader(dir, irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(2, reader.docs_count());
    ASSERT_EQ(2, reader.live_docs_count());

    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    expected_index.emplace_back();
    expected_index.back().insert(*doc2);
    tests::AssertIndex(reader, expected_index, kAllFeatures);

    {
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator();
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = tests::Docs(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(!irs::doc_limits::eof(docs_itr->Next()));
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->Value()));
      ASSERT_FALSE(!irs::doc_limits::eof(docs_itr->Next()));
    }
    {
      auto& segment = reader[1];
      const auto* column = segment.Column(kNameId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator();
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = tests::Docs(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(!irs::doc_limits::eof(docs_itr->Next()));
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->Value()));
      ASSERT_FALSE(!irs::doc_limits::eof(docs_itr->Next()));
    }
  }
}

TEST_F(index_death_test_formats_15,
       columnstore_creation_fail_implicit_segment_flush) {
  // With `segment_docs_max=1`, every Insert flushes the previous
  // segment and opens a new one -- so CREATE failures on consecutive
  // `_N.col` files fire at the Insert that allocates the new segment.
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();

  {
    FailingFileSystem fs;
    const auto dir = LocalFiles(fs);

    auto opts = irs::tests::DefaultWriterOptions();
    opts.segment_docs_max = 1;

    auto writer =
      tests::IndexWriter::Make(dir, tests::kOmCreate, std::move(opts));
    ASSERT_NE(nullptr, writer);

    // CREATE failure on the very first segment's cs file: insert
    // throws because cs Writer construction fails to create
    // `0000000000000001.col`.
    fs.RegisterFailure(FailingFileSystem::Failure::CREATE,
                       "0000000000000001.col");
    ASSERT_SQL_ERROR(InsertWithName(*writer, *doc1), ERRCODE_IO_ERROR);
    ASSERT_TRUE(fs.NoFailures());

    // After the failure clears, Insert + Commit proceed normally.
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      tests::OpenReader(dir, irs::tests::DefaultReaderOptions()));

    auto reader = tests::OpenReader(dir, irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1u, reader.size());
    ASSERT_EQ(1u, reader.live_docs_count());

    auto& segment = reader[0];
    const auto* column = segment.Column(kNameId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator();
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = tests::Docs(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(!irs::doc_limits::eof(docs_itr->Next()));
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->Value()));
    ASSERT_FALSE(!irs::doc_limits::eof(docs_itr->Next()));

    (void)doc2;  // unused; kept so the call sites read symmetrically.
  }
}

TEST_F(index_death_test_formats_15,
       columnstore_creation_sync_fail_implicit_segment_flush) {
  // Register both `CREATE` and `SYNC` failures on `_N.col` across
  // multiple per-segment columnstore files.
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();

  {
    FailingFileSystem fs;
    const auto dir = LocalFiles(fs);

    auto opts = irs::tests::DefaultWriterOptions();
    opts.segment_docs_max = 1;

    auto writer =
      tests::IndexWriter::Make(dir, tests::kOmCreate, std::move(opts));
    ASSERT_NE(nullptr, writer);

    // 1) CREATE failure on `_1.col` -> first insert throws.
    fs.RegisterFailure(FailingFileSystem::Failure::CREATE,
                       "0000000000000001.col");
    ASSERT_SQL_ERROR(InsertWithName(*writer, *doc1), ERRCODE_IO_ERROR);
    ASSERT_TRUE(fs.NoFailures());

    // 2) Insert succeeds (allocates `_2.col`), but SYNC fails on it
    // during Begin(). Begin() throws; the failed flush leaves the index
    // empty.
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    fs.RegisterFailure(FailingFileSystem::Failure::SYNC,
                       "0000000000000002.col");
    ASSERT_SQL_ERROR(writer->RefreshBegin(), ERRCODE_IO_ERROR);
    ASSERT_TRUE(fs.NoFailures());

    auto reader = tests::OpenReader(dir, irs::tests::DefaultReaderOptions());
    ASSERT_EQ(0, reader.size());
    ASSERT_EQ(0, reader.docs_count());
    ASSERT_EQ(0, reader.live_docs_count());

    (void)doc2;
  }
}

TEST_F(index_death_test_formats_15, fails_in_compact_with_removals) {
  // Mixed failures around CREATE/SYNC/REMOVE on `.col` files driven by
  // a sequence of inserts, commits, and a compact. Each failing
  // step is followed by a successful retry; final index has the full
  // dataset.
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::PayloadedJsonFieldFactory);
  const auto* doc1 = gen.next();
  const auto* doc2 = gen.next();

  constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                              irs::IndexFeatures::Pos |
                                              irs::IndexFeatures::Offs;

  {
    FailingFileSystem fs;
    const auto dir = LocalFiles(fs);

    auto writer = tests::IndexWriter::Make(dir, tests::kOmCreate,
                                           irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // Insert fails because `.col` creation fails for the new segment.
    fs.RegisterFailure(FailingFileSystem::Failure::CREATE,
                       "0000000000000001.col");
    ASSERT_SQL_ERROR(InsertWithName(*writer, *doc1), ERRCODE_IO_ERROR);
    ASSERT_FALSE(writer->RefreshCommit());  // nothing to commit
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      tests::OpenReader(dir, irs::tests::DefaultReaderOptions()));

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));

    // SYNC fails on segment 0's `.col` -> Commit throws.
    fs.RegisterFailure(FailingFileSystem::Failure::SYNC,
                       "0000000000000002.col");
    ASSERT_SQL_ERROR(writer->RefreshCommit(), ERRCODE_IO_ERROR);
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      tests::OpenReader(dir, irs::tests::DefaultReaderOptions()));

    // Nothing to commit after a failed first phase.
    ASSERT_FALSE(writer->RefreshCommit());

    // NOW IT IS OK -- insert + commit succeeds.
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      tests::OpenReader(dir, irs::tests::DefaultReaderOptions()));

    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      tests::OpenReader(dir, irs::tests::DefaultReaderOptions()));

    const irs::index_utils::CompactionCount compact_all;

    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(compact_all)));
    ASSERT_TRUE(writer->RefreshCommit());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      tests::OpenReader(dir, irs::tests::DefaultReaderOptions()));

    // Register REMOVE failures on superseded segment `.col` files; removing
    // the released segments should tolerate the failures (leave stale
    // files) and let the next commit observe the empty mask.
    fs.RegisterFailure(FailingFileSystem::Failure::REMOVE,
                       "0000000000000003.col");
    fs.RegisterFailure(FailingFileSystem::Failure::REMOVE,
                       "0000000000000005.col");
    dir.RemoveReleased();
    dir.RemoveReleased();
    ASSERT_FALSE(writer->RefreshCommit());  // nothing changed
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      tests::OpenReader(dir, irs::tests::DefaultReaderOptions()));

    auto reader = tests::OpenReader(dir, irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(2, reader.docs_count());
    ASSERT_EQ(2, reader.live_docs_count());

    tests::index_t expected_index;
    expected_index.emplace_back();
    expected_index.back().insert(*doc1);
    expected_index.back().insert(*doc2);
    tests::AssertIndex(reader, expected_index, kAllFeatures);

    auto& segment = reader[0];
    const auto* column = segment.Column(kNameId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator();
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = tests::Docs(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(!irs::doc_limits::eof(docs_itr->Next()));
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->Value()));
    ASSERT_TRUE(!irs::doc_limits::eof(docs_itr->Next()));
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->Value()));
    ASSERT_FALSE(!irs::doc_limits::eof(docs_itr->Next()));
  }
}
