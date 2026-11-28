////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <initializer_list>
#include <iresearch/index/file_names.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <iterator>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>

#include "formats/column/test_cs_helpers.hpp"
#include "hook_file_system.hpp"
#include "index/doc_generator.hpp"
#include "index/index_tests.hpp"
#include "index_dir.hpp"
#include "insert_field.hpp"
#include "tests_shared.hpp"

namespace {

// Stable per-name field ids, sourced from `tests::FieldIdFor` so the
// canonical JSON factories and these tests agree on the id-per-name.
[[maybe_unused]] inline constexpr irs::field_id kName =
  tests::FieldIdFor("name");
[[maybe_unused]] inline constexpr irs::field_id kSeq = tests::FieldIdFor("seq");
[[maybe_unused]] inline constexpr irs::field_id kSame =
  tests::FieldIdFor("same");
[[maybe_unused]] inline constexpr irs::field_id kDuplicated =
  tests::FieldIdFor("duplicated");
[[maybe_unused]] inline constexpr irs::field_id kPrefix =
  tests::FieldIdFor("prefix");
[[maybe_unused]] inline constexpr irs::field_id kValue =
  tests::FieldIdFor("value");

auto StoreName() {
  return [](irs::IndexWriter::Document& doc, const tests::Document& src) {
    const auto* name =
      dynamic_cast<const tests::StringField*>(src.stored.get_by_id(kName));
    if (name) {
      irs::tests::StoreFieldAt(*doc.GetColWriter(), kName, doc.DocId(), *name);
    }
  };
}

class OpenedFiles final : public tests::HookFileSystem {
 public:
  using tests::HookFileSystem::HookFileSystem;

  void OnOpen(std::string_view name) final { _opened.emplace(name); }

  std::set<std::string> Take() { return std::exchange(_opened, {}); }

 private:
  std::set<std::string> _opened;
};

std::set<std::string> FilesOf(const irs::SegmentMeta& meta, uint8_t kinds) {
  auto files = irs::SegmentFiles(meta.name, kinds);
  return {std::make_move_iterator(files.begin()),
          std::make_move_iterator(files.end())};
}

void CommitNames(tests::IndexWriter& writer,
                 std::initializer_list<std::string_view> names,
                 std::optional<irs::IndexFeatures> indexed) {
  auto batch = writer.GetBatch();
  for (const auto name : names) {
    tests::StringField field{"name", name};
    field.id = kName;
    auto doc = batch.Insert();
    if (indexed) {
      field.index_features = *indexed;
      ASSERT_TRUE(tests::InsertField(doc, field));
    }
    irs::tests::StoreFieldAt(*doc.GetColWriter(), kName, doc.DocId(), field);
  }
  ASSERT_TRUE(batch.Commit());
  ASSERT_TRUE(writer.RefreshCommit());
}

std::string CorruptionOf(const tests::IndexDir& dir,
                         const irs::SegmentMeta& meta) {
  const auto reader =
    irs::SegmentReaderImpl::Open(dir.Db(), dir.Fs(), dir.Root(), meta,
                                 irs::tests::DefaultReaderOptions(), nullptr);
  try {
    reader->Column(kName);
  } catch (const irs::SqlException& e) {
    EXPECT_EQ(e.error().errcode, ERRCODE_INDEX_CORRUPTED) << e.what();
    return std::string{e.message()};
  }
  return {};
}

}  // namespace

TEST(composite_reader_test, term_ranges_span_segments) {
  constexpr irs::field_id kBoth = 7;
  constexpr irs::field_id kFirst = 8;
  constexpr irs::field_id kAbsent = 9;
  tests::IndexDir dir{irs::DuckDBEngine::Instance().instance(),
                      TestEnv::MemoryRoot()};
  auto writer = tests::IndexWriter::Make(dir, tests::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
  const auto add = [&](std::string_view both, std::string_view first) {
    tests::Document doc;
    auto both_field = std::make_shared<tests::StringField>("both", both);
    both_field->id = kBoth;
    doc.insert(both_field);
    if (!first.empty()) {
      auto first_field = std::make_shared<tests::StringField>("first", first);
      first_field->id = kFirst;
      doc.insert(first_field);
    }
    auto ctx = writer->GetBatch();
    {
      auto d = ctx.Insert();
      ASSERT_TRUE(
        tests::InsertFields(d, doc.indexed.begin(), doc.indexed.end()));
    }
    ctx.Commit();
  };
  add("m", "x");
  add("b", "");
  ASSERT_TRUE(writer->RefreshCommit());
  add("a", "");
  add("c", "");
  ASSERT_TRUE(writer->RefreshCommit());

  const auto reader =
    tests::OpenReader(dir, irs::tests::DefaultReaderOptions());
  ASSERT_EQ(2, reader.size());
  const auto* both = reader.GetTermRange(kBoth, irs::StringLayout::kStrings);
  ASSERT_TRUE(both);
  EXPECT_EQ(irs::ViewCast<char>(both->min), "a");
  EXPECT_EQ(irs::ViewCast<char>(both->max), "m");
  EXPECT_EQ(both->docs, 4);
  const auto* first = reader.GetTermRange(kFirst, irs::StringLayout::kStrings);
  ASSERT_TRUE(first);
  EXPECT_EQ(irs::ViewCast<char>(first->min), "x");
  EXPECT_EQ(irs::ViewCast<char>(first->max), "x");
  EXPECT_EQ(first->docs, 1);
  EXPECT_FALSE(reader.GetTermRange(kAbsent, irs::StringLayout::kStrings));
  EXPECT_FALSE(reader.GetTermRange(kBoth, irs::StringLayout::kNulls));
}

TEST(composite_reader_test, loads_stats_on_first_use) {
  tests::IndexDir dir{irs::DuckDBEngine::Instance().instance(),
                      TestEnv::MemoryRoot()};
  auto writer = tests::IndexWriter::Make(dir, tests::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
  CommitNames(*writer, {"B", "D"}, irs::IndexFeatures::None);
  CommitNames(*writer, {"A", "A"}, irs::IndexFeatures::None);
  const auto committed = dir.Committed();
  ASSERT_EQ(2, committed.size());

  OpenedFiles fs{dir.Fs()};
  std::vector<irs::SegmentReader> segments;
  std::set<std::string> columns;
  std::set<std::string> indexes;
  for (const auto& meta : committed) {
    segments.emplace_back(irs::SegmentReaderImpl::Open(
      dir.Db(), fs, dir.Root(), meta, irs::tests::DefaultReaderOptions(),
      nullptr));
    columns.merge(FilesOf(meta, irs::kColFile));
    indexes.merge(
      FilesOf(meta, static_cast<uint8_t>(meta.kinds & ~irs::kColFile)));
  }
  const irs::CompositeReader reader{std::move(segments), /*stats=*/true};
  EXPECT_EQ(4, reader.docs_count());
  EXPECT_EQ(std::set<std::string>{}, fs.Take());

  ASSERT_NE(nullptr, reader.GetColumnStats(kName));
  EXPECT_EQ(columns, fs.Take());

  const auto* terms = reader.GetTermRange(kName, irs::StringLayout::kStrings);
  ASSERT_NE(nullptr, terms);
  EXPECT_EQ(irs::ViewCast<char>(terms->min), "A");
  EXPECT_EQ(irs::ViewCast<char>(terms->max), "D");
  EXPECT_EQ(4, terms->docs);
  EXPECT_EQ(indexes, fs.Take());
}

TEST(composite_reader_test, builds_no_stats_from_unopened_segments) {
  tests::IndexDir dir{irs::DuckDBEngine::Instance().instance(),
                      TestEnv::MemoryRoot()};
  auto writer = tests::IndexWriter::Make(dir, tests::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
  CommitNames(*writer, {"B", "D"}, irs::IndexFeatures::None);
  CommitNames(*writer, {"A", "A"}, irs::IndexFeatures::None);
  const auto committed = dir.Committed();
  ASSERT_EQ(2, committed.size());

  OpenedFiles fs{dir.Fs()};
  const auto open = [&](const irs::SegmentMeta& meta) {
    return irs::SegmentReader{irs::SegmentReaderImpl::Open(
      dir.Db(), fs, dir.Root(), meta, irs::tests::DefaultReaderOptions(),
      nullptr)};
  };
  const auto first = open(committed[0]);
  const auto second = open(committed[1]);
  const irs::SegmentReader added[]{second};
  const irs::CompositeReader base{{first}, /*stats=*/true};
  ASSERT_NE(nullptr, base.GetColumnStats(kName));
  ASSERT_NE(nullptr, base.GetTermRange(kName, irs::StringLayout::kStrings));
  fs.Take();

  const irs::CompositeReader next{base, {first, second}, added, {}, {}};
  EXPECT_EQ(std::set<std::string>{}, fs.Take());
  const auto* terms = next.GetTermRange(kName, irs::StringLayout::kStrings);
  ASSERT_NE(nullptr, terms);
  EXPECT_EQ(irs::ViewCast<char>(terms->min), "A");
  EXPECT_EQ(4, terms->docs);

  second.GetImpl()->OpenFiles();
  fs.Take();
  const irs::CompositeReader opened{base, {first, second}, added, {}, {}};
  EXPECT_EQ(std::set<std::string>{}, fs.Take());
  const auto* updated = opened.GetTermRange(kName, irs::StringLayout::kStrings);
  ASSERT_NE(nullptr, updated);
  EXPECT_EQ(irs::ViewCast<char>(updated->min), "A");
  EXPECT_EQ(irs::ViewCast<char>(updated->max), "D");
  EXPECT_EQ(4, updated->docs);
  EXPECT_EQ(std::set<std::string>{}, fs.Take());
}

TEST(composite_reader_test, open) {
  tests::JsonDocGenerator gen(
    TestBase::resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (tests::JsonDocGenerator::ValueType::STRING == data.vt) {
        auto field = std::make_shared<tests::StringField>(name, data.str);
        field->id = tests::FieldIdForRuntime(name);
        doc.insert(std::move(field));
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();
  const tests::Document* doc4 = gen.next();
  const tests::Document* doc5 = gen.next();
  const tests::Document* doc6 = gen.next();
  const tests::Document* doc7 = gen.next();
  const tests::Document* doc8 = gen.next();
  const tests::Document* doc9 = gen.next();

  tests::IndexDir dir{irs::DuckDBEngine::Instance().instance(),
                      TestEnv::MemoryRoot()};

  // create index
  {
    // open writer
    auto writer = tests::IndexWriter::Make(dir, tests::kOmCreate,
                                           irs::tests::DefaultWriterOptions());

    // add first segment
    {
      auto ctx = writer->GetBatch();
      {
        auto d = ctx.Insert();
        ASSERT_TRUE(
          tests::InsertFields(d, doc1->indexed.begin(), doc1->indexed.end()));
        StoreName()(d, *doc1);
      }
      ctx.Commit();
    }
    {
      auto ctx = writer->GetBatch();
      {
        auto d = ctx.Insert();
        ASSERT_TRUE(
          tests::InsertFields(d, doc2->indexed.begin(), doc2->indexed.end()));
        StoreName()(d, *doc2);
      }
      ctx.Commit();
    }
    {
      auto ctx = writer->GetBatch();
      {
        auto d = ctx.Insert();
        ASSERT_TRUE(
          tests::InsertFields(d, doc3->indexed.begin(), doc3->indexed.end()));
        StoreName()(d, *doc3);
      }
      ctx.Commit();
    }
    writer->RefreshCommit();
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      tests::OpenReader(dir, irs::tests::DefaultReaderOptions()));

    // add second segment
    {
      auto ctx = writer->GetBatch();
      {
        auto d = ctx.Insert();
        ASSERT_TRUE(
          tests::InsertFields(d, doc4->indexed.begin(), doc4->indexed.end()));
        StoreName()(d, *doc4);
      }
      ctx.Commit();
    }
    {
      auto ctx = writer->GetBatch();
      {
        auto d = ctx.Insert();
        ASSERT_TRUE(
          tests::InsertFields(d, doc5->indexed.begin(), doc5->indexed.end()));
        StoreName()(d, *doc5);
      }
      ctx.Commit();
    }
    {
      auto ctx = writer->GetBatch();
      {
        auto d = ctx.Insert();
        ASSERT_TRUE(
          tests::InsertFields(d, doc6->indexed.begin(), doc6->indexed.end()));
        StoreName()(d, *doc6);
      }
      ctx.Commit();
    }
    {
      auto ctx = writer->GetBatch();
      {
        auto d = ctx.Insert();
        ASSERT_TRUE(
          tests::InsertFields(d, doc7->indexed.begin(), doc7->indexed.end()));
        StoreName()(d, *doc7);
      }
      ctx.Commit();
    }
    writer->RefreshCommit();
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      tests::OpenReader(dir, irs::tests::DefaultReaderOptions()));

    // add third segment
    {
      auto ctx = writer->GetBatch();
      {
        auto d = ctx.Insert();
        ASSERT_TRUE(
          tests::InsertFields(d, doc8->indexed.begin(), doc8->indexed.end()));
        StoreName()(d, *doc8);
      }
      ctx.Commit();
    }
    {
      auto ctx = writer->GetBatch();
      {
        auto d = ctx.Insert();
        ASSERT_TRUE(
          tests::InsertFields(d, doc9->indexed.begin(), doc9->indexed.end()));
        StoreName()(d, *doc9);
      }
      ctx.Commit();
    }
    writer->RefreshCommit();
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      tests::OpenReader(dir, irs::tests::DefaultReaderOptions()));
  }

  // open reader
  auto rdr = tests::OpenReader(dir, irs::tests::DefaultReaderOptions());
  ASSERT_EQ(9, rdr.docs_count());
  ASSERT_EQ(9, rdr.live_docs_count());
  ASSERT_EQ(3, rdr.size());

  // check subreaders
  auto sub = rdr.begin();

  // first segment
  {
    ASSERT_NE(rdr.end(), sub);
    ASSERT_EQ(1, sub->size());
    ASSERT_EQ(3, sub->docs_count());
    ASSERT_EQ(3, sub->live_docs_count());

    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    // read documents
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(values, 1));
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(values, 2));
    ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(values, 3));

    // read invalid document
    ASSERT_TRUE(values.IsNull(4));
  }

  // second segment
  {
    ++sub;
    ASSERT_NE(rdr.end(), sub);
    ASSERT_EQ(1, sub->size());
    ASSERT_EQ(4, sub->docs_count());
    ASSERT_EQ(4, sub->live_docs_count());

    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    // read documents
    ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(values, 1));
    ASSERT_EQ("E", irs::tests::ReadStoredStr<std::string_view>(values, 2));
    ASSERT_EQ("F", irs::tests::ReadStoredStr<std::string_view>(values, 3));
    ASSERT_EQ("G", irs::tests::ReadStoredStr<std::string_view>(values, 4));

    // read invalid document
    ASSERT_TRUE(values.IsNull(5));
  }

  // third segment
  {
    ++sub;
    ASSERT_NE(rdr.end(), sub);
    ASSERT_EQ(1, sub->size());
    ASSERT_EQ(2, sub->docs_count());
    ASSERT_EQ(2, sub->live_docs_count());

    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    // read documents
    ASSERT_EQ("H", irs::tests::ReadStoredStr<std::string_view>(values, 1));
    ASSERT_EQ("I", irs::tests::ReadStoredStr<std::string_view>(values, 2));

    // read invalid document
    ASSERT_TRUE(values.IsNull(3));
  }

  ++sub;
  ASSERT_EQ(rdr.end(), sub);
}

TEST(segment_reader_test, open_invalid_segment) {
  tests::IndexDir dir{irs::DuckDBEngine::Instance().instance(),
                      TestEnv::MemoryRoot()};

  /* open invalid segment */
  {
    irs::SegmentMeta meta;
    meta.name = "invalid_segment_name";

    auto rdr = irs::SegmentReaderImpl::Open(
      dir.Db(), dir.Fs(), dir.Root(), meta, irs::IndexReaderOptions{}, nullptr);
    ASSERT_NE(nullptr, rdr);
    ASSERT_EQ(0, rdr->docs_count());
  }
}

TEST(segment_reader_test, writes_only_files_with_data) {
  tests::IndexDir dir{irs::DuckDBEngine::Instance().instance(),
                      TestEnv::MemoryRoot()};
  auto writer = tests::IndexWriter::Make(dir, tests::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
  constexpr auto kPositions =
    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
  CommitNames(*writer, {"A", "B"}, std::nullopt);
  CommitNames(*writer, {"A", "B"}, irs::IndexFeatures::None);
  CommitNames(*writer, {"A", "A"}, irs::IndexFeatures::None);
  CommitNames(*writer, {"A", "B"}, kPositions);

  const auto committed = dir.Committed();
  ASSERT_EQ(4, committed.size());
  EXPECT_EQ(irs::kColFile, committed[0].kinds);
  EXPECT_EQ(irs::kColFile | irs::kIdxFile, committed[1].kinds);
  EXPECT_EQ(irs::kColFile | irs::kIdxFile | irs::kDocFile, committed[2].kinds);
  EXPECT_EQ(irs::kColFile | irs::kIdxFile | irs::kPosFile, committed[3].kinds);
  size_t written = 0;
  for (const auto& meta : committed) {
    for (const auto& name : FilesOf(meta, meta.kinds)) {
      EXPECT_TRUE(dir.Has(name)) << name;
      ++written;
    }
  }
  EXPECT_EQ(written, dir.Files().size());
}

TEST(segment_reader_test, opens_files_on_first_use) {
  tests::IndexDir dir{irs::DuckDBEngine::Instance().instance(),
                      TestEnv::MemoryRoot()};
  CommitNames(*tests::IndexWriter::Make(dir, tests::kOmCreate,
                                        irs::tests::DefaultWriterOptions()),
              {"A", "B", "A"}, irs::IndexFeatures::None);
  const auto committed = dir.Committed();
  ASSERT_EQ(1, committed.size());
  const auto& meta = committed.front();
  ASSERT_EQ(irs::kColFile | irs::kIdxFile | irs::kDocFile, meta.kinds);

  OpenedFiles fs{dir.Fs()};
  const auto reader =
    irs::SegmentReaderImpl::Open(dir.Db(), fs, dir.Root(), meta,
                                 irs::tests::DefaultReaderOptions(), nullptr);
  EXPECT_EQ(3, reader->docs_count());
  EXPECT_EQ(std::set<std::string>{}, fs.Take());

  ASSERT_NE(nullptr, reader->Column(kName));
  EXPECT_EQ(FilesOf(meta, irs::kColFile), fs.Take());

  const auto* field = reader->field(kName);
  ASSERT_NE(nullptr, field);
  EXPECT_EQ(2, field->size());
  EXPECT_EQ(FilesOf(meta, irs::kIdxFile | irs::kDocFile), fs.Take());

  EXPECT_NE(nullptr, reader->Column(kName));
  EXPECT_NE(nullptr, reader->field(kName));
  EXPECT_EQ(std::set<std::string>{}, fs.Take());
}

TEST(segment_reader_test, reads_footers_at_recorded_sizes) {
  tests::IndexDir dir{irs::DuckDBEngine::Instance().instance(),
                      TestEnv::MemoryRoot()};
  CommitNames(*tests::IndexWriter::Make(dir, tests::kOmCreate,
                                        irs::tests::DefaultWriterOptions()),
              {"A", "B", "A"}, irs::IndexFeatures::None);
  const auto committed = dir.Committed();
  ASSERT_EQ(1, committed.size());
  const auto& meta = committed.front();
  ASSERT_EQ(irs::kColFile | irs::kIdxFile | irs::kDocFile, meta.kinds);
  for (const auto kind : {irs::kColFile, irs::kIdxFile, irs::kDocFile}) {
    const auto name = irs::SegmentFiles(meta.name, kind).front();
    const auto sizes = meta.files[irs::KindIndex(kind)];
    EXPECT_EQ(dir.Open(name, duckdb::MappedFileAccess::NORMAL)->GetSize(),
              sizes.file)
      << name;
    EXPECT_LT(0U, sizes.footer) << name;
    EXPECT_LT(sizes.footer, sizes.file) << name;
  }
  EXPECT_EQ("", CorruptionOf(dir, meta));

  auto longer = meta;
  ++longer.files[irs::KindIndex(irs::kColFile)].file;
  const auto size_error = CorruptionOf(dir, longer);
  EXPECT_NE(std::string::npos, size_error.find("but the log recorded it as"))
    << size_error;

  auto shorter_footer = meta;
  --shorter_footer.files[irs::KindIndex(irs::kColFile)].footer;
  const auto footer_error = CorruptionOf(dir, shorter_footer);
  EXPECT_NE(std::string::npos, footer_error.find("does not match its checksum"))
    << footer_error;
}

TEST(segment_reader_test, open) {
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);
  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();
  const tests::Document* doc4 = gen.next();
  const tests::Document* doc5 = gen.next();

  tests::IndexDir dir{irs::DuckDBEngine::Instance().instance(),
                      TestEnv::MemoryRoot()};
  irs::CompositeReader writer_snapshot;
  {
    // open writer
    auto writer = tests::IndexWriter::Make(dir, tests::kOmCreate,
                                           irs::tests::DefaultWriterOptions());

    // add first segment
    {
      auto ctx = writer->GetBatch();
      {
        auto d = ctx.Insert();
        ASSERT_TRUE(
          tests::InsertFields(d, doc1->indexed.begin(), doc1->indexed.end()));
        StoreName()(d, *doc1);
      }
      ctx.Commit();
    }
    {
      auto ctx = writer->GetBatch();
      {
        auto d = ctx.Insert();
        ASSERT_TRUE(
          tests::InsertFields(d, doc2->indexed.begin(), doc2->indexed.end()));
        StoreName()(d, *doc2);
      }
      ctx.Commit();
    }
    {
      auto ctx = writer->GetBatch();
      {
        auto d = ctx.Insert();
        ASSERT_TRUE(
          tests::InsertFields(d, doc3->indexed.begin(), doc3->indexed.end()));
        StoreName()(d, *doc3);
      }
      ctx.Commit();
    }
    {
      auto ctx = writer->GetBatch();
      {
        auto d = ctx.Insert();
        ASSERT_TRUE(
          tests::InsertFields(d, doc4->indexed.begin(), doc4->indexed.end()));
        StoreName()(d, *doc4);
      }
      ctx.Commit();
    }
    {
      auto ctx = writer->GetBatch();
      {
        auto d = ctx.Insert();
        ASSERT_TRUE(
          tests::InsertFields(d, doc5->indexed.begin(), doc5->indexed.end()));
        StoreName()(d, *doc5);
      }
      ctx.Commit();
    }
    writer->RefreshCommit();
    writer_snapshot = writer->GetSnapshot();
  }

  // check segment
  {
    irs::SegmentMeta meta;
    meta.docs_count = 5;
    meta.live_docs_count = 5;
    meta.name = irs::FileName(1);
    meta.kinds = dir.Committed().front().kinds;
    meta.files = dir.Committed().front().files;

    auto rdr =
      irs::SegmentReaderImpl::Open(dir.Db(), dir.Fs(), dir.Root(), meta,
                                   irs::tests::DefaultReaderOptions(), nullptr);
    ASSERT_FALSE(!rdr);
    ASSERT_EQ(1, rdr->size());
    ASSERT_EQ(meta.docs_count, rdr->docs_count());
    ASSERT_EQ(meta.live_docs_count, rdr->live_docs_count());

    auto& segment = *rdr->begin();
    const auto* column = segment.Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};

    // read documents
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(values, 1));
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(values, 2));
    ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(values, 3));
    ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(values, 4));
    ASSERT_EQ("E", irs::tests::ReadStoredStr<std::string_view>(values, 5));

    ASSERT_TRUE(values.IsNull(6));  // read invalid document

    // check iterators
    {
      auto it = rdr->begin();
      ASSERT_EQ(rdr.get(), &*it); /* should return self */
      ASSERT_NE(rdr->end(), it);
      ++it;
      ASSERT_EQ(rdr->end(), it);
    }

    // check field ids
    {
      auto ids = rdr->field_ids();
      ASSERT_EQ(6, ids.size());
      // Ids are sorted ascending; doc-generator assigns ids via the fixture,
      // not by name. Without a real catalog we only check the count here.
    }

    // check live docs
    {
      auto it = rdr->docs_iterator();
      ASSERT_EQ(1, it->Next());
      ASSERT_EQ(2, it->Next());
      ASSERT_EQ(3, it->Next());
      ASSERT_EQ(4, it->Next());
      ASSERT_EQ(5, it->Next());
      ASSERT_TRUE(irs::doc_limits::eof(it->Next()));
      ASSERT_TRUE(irs::doc_limits::eof(it->Next()));
    }

    // check field metadata
    {
      {
        ASSERT_EQ(6, rdr->field_ids().size());
      }

      // check field
      {
        constexpr irs::field_id id = kName;
        auto field = rdr->field(id);
        ASSERT_EQ(id, field->meta().id);

        // check terms
        auto terms = rdr->field(id);
        ASSERT_NE(nullptr, terms);

        ASSERT_EQ(5, terms->size());
        ASSERT_EQ(5, terms->docs_count());
        ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("A")),
                  (terms->min)());
        ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("E")),
                  (terms->max)());

        auto term = terms->iterator();

        // check term: A
        {
          ASSERT_TRUE(term->next());
          ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("A")),
                    term->value());

          // check docs
          {
            auto docs = tests::Docs(term->postings(irs::IndexFeatures::None));
            ASSERT_TRUE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_EQ(1, docs->Value());
            ASSERT_FALSE(!irs::doc_limits::eof(docs->Next()));
          }
        }

        // check term: B
        {
          ASSERT_TRUE(term->next());
          ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("B")),
                    term->value());

          // check docs
          {
            auto docs = tests::Docs(term->postings(irs::IndexFeatures::None));
            ASSERT_TRUE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_EQ(2, docs->Value());
            ASSERT_FALSE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_FALSE(!irs::doc_limits::eof(docs->Next()));
          }
        }

        // check term: C
        {
          ASSERT_TRUE(term->next());
          ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("C")),
                    term->value());

          // check docs
          {
            auto docs = tests::Docs(term->postings(irs::IndexFeatures::None));
            ASSERT_TRUE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_EQ(3, docs->Value());
            ASSERT_FALSE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_FALSE(!irs::doc_limits::eof(docs->Next()));
          }
        }

        // check term: D
        {
          ASSERT_TRUE(term->next());
          ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("D")),
                    term->value());

          // check docs
          {
            auto docs = tests::Docs(term->postings(irs::IndexFeatures::None));
            ASSERT_TRUE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_EQ(4, docs->Value());
            ASSERT_FALSE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_FALSE(!irs::doc_limits::eof(docs->Next()));
          }
        }

        // check term: E
        {
          ASSERT_TRUE(term->next());
          ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("E")),
                    term->value());

          // check docs
          {
            auto docs = tests::Docs(term->postings(irs::IndexFeatures::None));
            ASSERT_TRUE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_EQ(5, docs->Value());
            ASSERT_FALSE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_FALSE(!irs::doc_limits::eof(docs->Next()));
          }
        }

        ASSERT_FALSE(term->next());
      }

      // check field
      {
        constexpr irs::field_id id = kSeq;  // "seq"
        constexpr auto kNumeric = irs::NumericLayout::kNumbers;
        auto field = rdr->field(id, kNumeric);
        ASSERT_NE(nullptr, field);
        ASSERT_EQ(id, field->meta().id);

        // check terms
        auto terms = rdr->field(id, kNumeric);
        ASSERT_NE(nullptr, terms);
      }

      // check field
      {
        constexpr irs::field_id id = kSame;  // "same"
        auto field = rdr->field(id);
        ASSERT_EQ(id, field->meta().id);

        // check terms
        auto terms = rdr->field(id);
        ASSERT_NE(nullptr, terms);
        ASSERT_EQ(1, terms->size());
        ASSERT_EQ(5, terms->docs_count());
        ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("xyz")),
                  (terms->min)());
        ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("xyz")),
                  (terms->max)());

        auto term = terms->iterator();

        // check term: xyz
        {
          ASSERT_TRUE(term->next());
          ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("xyz")),
                    term->value());

          /* check docs */
          {
            auto docs = tests::Docs(term->postings(irs::IndexFeatures::None));
            ASSERT_TRUE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_EQ(1, docs->Value());
            ASSERT_TRUE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_EQ(2, docs->Value());
            ASSERT_TRUE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_EQ(3, docs->Value());
            ASSERT_TRUE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_EQ(4, docs->Value());
            ASSERT_TRUE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_EQ(5, docs->Value());
            ASSERT_FALSE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_FALSE(!irs::doc_limits::eof(docs->Next()));
          }
        }

        ASSERT_FALSE(term->next());
      }

      // check field
      {
        constexpr irs::field_id id = kDuplicated;  // "duplicated"
        auto field = rdr->field(id);
        ASSERT_EQ(id, field->meta().id);

        // check terms
        auto terms = rdr->field(id);
        ASSERT_NE(nullptr, terms);
        ASSERT_EQ(2, terms->size());
        ASSERT_EQ(4, terms->docs_count());
        ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("abcd")),
                  (terms->min)());
        ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("vczc")),
                  (terms->max)());

        auto term = terms->iterator();

        // check term: abcd
        {
          ASSERT_TRUE(term->next());
          ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("abcd")),
                    term->value());

          // check docs
          {
            auto docs = tests::Docs(term->postings(irs::IndexFeatures::None));
            ASSERT_TRUE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_EQ(1, docs->Value());
            ASSERT_TRUE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_EQ(5, docs->Value());
            ASSERT_FALSE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_FALSE(!irs::doc_limits::eof(docs->Next()));
          }
        }

        // check term: vczc
        {
          ASSERT_TRUE(term->next());
          ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("vczc")),
                    term->value());

          // check docs
          {
            auto docs = tests::Docs(term->postings(irs::IndexFeatures::None));
            ASSERT_TRUE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_EQ(2, docs->Value());
            ASSERT_TRUE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_EQ(3, docs->Value());
            ASSERT_FALSE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_FALSE(!irs::doc_limits::eof(docs->Next()));
          }
        }

        ASSERT_FALSE(term->next());
      }

      // check field
      {
        constexpr irs::field_id id = kPrefix;  // "prefix"
        auto field = rdr->field(id);
        ASSERT_EQ(id, field->meta().id);

        // check terms
        auto terms = rdr->field(id);
        ASSERT_NE(nullptr, terms);
        ASSERT_EQ(2, terms->size());
        ASSERT_EQ(2, terms->docs_count());
        ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("abcd")),
                  (terms->min)());
        ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("abcde")),
                  (terms->max)());

        auto term = terms->iterator();

        // check term: abcd
        {
          ASSERT_TRUE(term->next());
          ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("abcd")),
                    term->value());

          // check docs
          {
            auto docs = tests::Docs(term->postings(irs::IndexFeatures::None));
            ASSERT_TRUE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_EQ(1, docs->Value());
            ASSERT_FALSE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_FALSE(!irs::doc_limits::eof(docs->Next()));
          }
        }

        // check term: abcde
        {
          ASSERT_TRUE(term->next());
          ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("abcde")),
                    term->value());

          // check docs
          {
            auto docs = tests::Docs(term->postings(irs::IndexFeatures::None));
            ASSERT_TRUE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_EQ(4, docs->Value());
            ASSERT_FALSE(!irs::doc_limits::eof(docs->Next()));
            ASSERT_FALSE(!irs::doc_limits::eof(docs->Next()));
          }
        }

        ASSERT_FALSE(term->next());
      }

      // invalid field
      {
        ASSERT_EQ(nullptr, rdr->field(static_cast<irs::field_id>(999)));
      }
    }
  }
}
