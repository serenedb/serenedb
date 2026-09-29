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

#include <iresearch/formats/segment_meta_reader.hpp>
#include <iresearch/formats/segment_meta_writer.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>

#include "formats/column/test_cs_helpers.hpp"
#include "index/doc_generator.hpp"
#include "index/index_tests.hpp"
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

}  // namespace

TEST(directory_reader_test, open_empty_directory) {
  irs::MemoryDirectory dir;

  // No index
  ASSERT_THROW((irs::DirectoryReader{dir}), irs::IndexNotFound);
}

TEST(directory_reader_test, open_empty_index) {
  irs::MemoryDirectory dir;

  // Create empty index
  {
    auto writer = irs::IndexWriter::Make(dir, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_TRUE(writer->RefreshCommit());
  }

  auto rdr = irs::DirectoryReader(dir, irs::tests::DefaultReaderOptions());
  ASSERT_FALSE(!rdr);
  ASSERT_EQ(0, rdr.docs_count());
  ASSERT_EQ(0, rdr.live_docs_count());
  ASSERT_EQ(0, rdr.size());
  ASSERT_EQ(rdr.end(), rdr.begin());
}

TEST(directory_reader_test, open) {
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

  irs::MemoryDirectory dir;

  // create index
  {
    // open writer
    auto writer = irs::IndexWriter::Make(dir, irs::kOmCreate,
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
      irs::DirectoryReader(dir, irs::tests::DefaultReaderOptions()));

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
      irs::DirectoryReader(dir, irs::tests::DefaultReaderOptions()));

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
      irs::DirectoryReader(dir, irs::tests::DefaultReaderOptions()));
  }

  // open reader
  auto rdr = irs::DirectoryReader(dir, irs::tests::DefaultReaderOptions());
  ASSERT_FALSE(!rdr);
  ASSERT_EQ(9, rdr.docs_count());
  ASSERT_EQ(9, rdr.live_docs_count());
  ASSERT_EQ(3, rdr.size());
  ASSERT_EQ("segments_3", rdr.Meta().filename);
  ASSERT_EQ(rdr.size(), rdr.Meta().index_meta.segments.size());

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

TEST(segment_reader_test, segment_reader_has) {
  std::string filename;

  // has none (default)
  {
    irs::MemoryDirectory dir;
    irs::SegmentMeta expected;
    expected.name = "_1";

    irs::segment_meta::Write(dir, filename, expected);

    irs::SegmentMeta meta;

    irs::segment_meta::Read(dir, meta, filename);

    ASSERT_EQ(expected, meta);
    ASSERT_FALSE(irs::HasRemovals(meta));
  }

  // has column store
  {
    irs::MemoryDirectory dir;
    irs::SegmentMeta expected;
    expected.name = "_1";

    irs::segment_meta::Write(dir, filename, expected);

    irs::SegmentMeta meta;

    irs::segment_meta::Read(dir, meta, filename);

    ASSERT_EQ(expected, meta);
    ASSERT_FALSE(irs::HasRemovals(meta));
  }

  // has document mask
  {
    irs::MemoryDirectory dir;
    irs::SegmentMeta expected;
    expected.name = "_1";

    expected.docs_count = 43;
    expected.live_docs_count = 42;
    expected.version = 0;
    expected.docs_mask = [&] {
      irs::DocumentMask docs_mask;
      docs_mask.Add(4);
      docs_mask.Trim();
      return std::make_shared<irs::DocumentMask>(std::move(docs_mask));
    }();
    irs::segment_meta::Write(dir, filename, expected);

    irs::SegmentMeta meta;

    irs::segment_meta::Read(dir, meta, filename);

    ASSERT_EQ(expected, meta);
    ASSERT_TRUE(irs::HasRemovals(meta));
  }

  // has all
  {
    irs::MemoryDirectory dir;
    irs::SegmentMeta expected;
    expected.name = "_1";

    expected.docs_count = 43;
    expected.live_docs_count = 42;
    expected.version = 1;
    expected.docs_mask = [&] {
      irs::DocumentMask docs_mask;
      docs_mask.Add(4);
      docs_mask.Trim();
      return std::make_shared<irs::DocumentMask>(std::move(docs_mask));
    }();
    irs::segment_meta::Write(dir, filename, expected);

    irs::SegmentMeta meta;
    irs::segment_meta::Read(dir, meta, filename);

    ASSERT_EQ(expected, meta);
    ASSERT_TRUE(irs::HasRemovals(meta));
  }
}

TEST(segment_reader_test, open_invalid_segment) {
  irs::MemoryDirectory dir;

  /* open invalid segment */
  {
    irs::SegmentMeta meta;
    meta.name = "invalid_segment_name";

    auto rdr = irs::SegmentReaderImpl::Open(
      dir, meta,
      irs::IndexReaderOptions{.db =
                                &::irs::DuckDBEngine::Instance().instance()});
    ASSERT_NE(nullptr, rdr);
    ASSERT_EQ(0, rdr->docs_count());
  }
}

TEST(segment_reader_test, open) {
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);
  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();
  const tests::Document* doc4 = gen.next();
  const tests::Document* doc5 = gen.next();

  irs::MemoryDirectory dir;
  irs::DirectoryReader writer_snapshot;
  {
    // open writer
    auto writer = irs::IndexWriter::Make(dir, irs::kOmCreate,
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
    meta.name = "_1";
    meta.version = 42;

    auto rdr = irs::SegmentReaderImpl::Open(dir, meta,
                                            irs::tests::DefaultReaderOptions());
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
            auto docs = term->postings(irs::IndexFeatures::None);
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
            auto docs = term->postings(irs::IndexFeatures::None);
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
            auto docs = term->postings(irs::IndexFeatures::None);
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
            auto docs = term->postings(irs::IndexFeatures::None);
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
            auto docs = term->postings(irs::IndexFeatures::None);
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
        auto field = rdr->field(id);
        ASSERT_EQ(id, field->meta().id);

        // check terms
        auto terms = rdr->field(id);
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
            auto docs = term->postings(irs::IndexFeatures::None);
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
            auto docs = term->postings(irs::IndexFeatures::None);
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
            auto docs = term->postings(irs::IndexFeatures::None);
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
            auto docs = term->postings(irs::IndexFeatures::None);
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
            auto docs = term->postings(irs::IndexFeatures::None);
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
