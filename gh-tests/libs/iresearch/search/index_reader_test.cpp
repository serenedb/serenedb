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

#include <iresearch/formats/formats.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/store/memory_directory.hpp>

#include "index/doc_generator.hpp"
#include "index/index_tests.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/segment_reader_impl.hpp"
#include "tests_shared.hpp"

using namespace std::chrono_literals;

namespace {

irs::Format* gCodec0;
irs::Format* gCodec1;

irs::Format::ptr GetCodec0() {
  return irs::Format::ptr(gCodec0, [](irs::Format*) -> void {});
}
irs::Format::ptr GetCodec1() {
  return irs::Format::ptr(gCodec1, [](irs::Format*) -> void {});
}

}  // namespace

TEST(directory_reader_test, open_empty_directory) {
  irs::MemoryDirectory dir;
  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);

  // No index
  ASSERT_THROW((irs::DirectoryReader{dir, codec}), irs::IndexNotFound);
}

TEST(directory_reader_test, open_empty_index) {
  irs::MemoryDirectory dir;
  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);

  // Create empty index
  {
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_TRUE(writer->Commit());
  }

  auto rdr = irs::DirectoryReader(dir, codec);
  ASSERT_FALSE(!rdr);
  ASSERT_EQ(0, rdr.docs_count());
  ASSERT_EQ(0, rdr.live_docs_count());
  ASSERT_EQ(0, rdr.size());
  ASSERT_EQ(rdr.end(), rdr.begin());
}

TEST(directory_reader_test, open_newest_index) {
  struct TestIndexMetaReader : public irs::IndexMetaReader {
    bool last_segments_file(const irs::Directory&,
                            std::string& out) const final {
      out = segments_file;
      return true;
    }
    void read(const irs::Directory& /*dir*/, irs::IndexMeta& /*meta*/,
              std::string_view filename = std::string_view{}) final {
      read_file.assign(filename.data(), filename.size());
    }
    std::string segments_file;
    std::string read_file;
  };
  class TestFormat final : public irs::Format {
   public:
    explicit TestFormat(irs::TypeInfo::type_id type) : _type{type} {}

    irs::IndexMetaWriter::ptr get_index_meta_writer() const final {
      return nullptr;
    }
    irs::IndexMetaReader::ptr get_index_meta_reader() const final {
      return irs::memory::to_managed<irs::IndexMetaReader>(index_meta_reader);
    }
    irs::SegmentMetaWriter::ptr get_segment_meta_writer() const final {
      return nullptr;
    }
    irs::SegmentMetaReader::ptr get_segment_meta_reader() const final {
      return nullptr;
    }
    irs::FieldWriter::ptr get_field_writer(bool,
                                           irs::IResourceManager&) const final {
      return nullptr;
    }
    irs::FieldReader::ptr get_field_reader(irs::IResourceManager&) const final {
      return nullptr;
    }
    irs::ColumnstoreWriter::ptr get_columnstore_writer(
      bool, irs::IResourceManager&) const final {
      return nullptr;
    }
    irs::ColumnstoreReader::ptr get_columnstore_reader() const final {
      return nullptr;
    }
    irs::PostingsWriter::ptr get_postings_writer(
      bool consolidation, irs::IResourceManager&) const final {
      return nullptr;
    }
    irs::PostingsReader::ptr get_postings_reader() const final {
      return nullptr;
    }
    irs::TypeInfo::type_id type() const noexcept final { return _type; }

    mutable TestIndexMetaReader index_meta_reader;

   private:
    irs::TypeInfo::type_id _type;
  };

  struct TestFormat0 {};
  struct TestFormat1 {};

  TestFormat test_codec0(irs::Type<TestFormat0>::id());
  TestFormat test_codec1(irs::Type<TestFormat1>::id());
  irs::FormatRegistrar test_format0_registrar(irs::Type<TestFormat0>::get(),
                                              &GetCodec0);
  irs::FormatRegistrar test_format1_registrar(irs::Type<TestFormat1>::get(),
                                              &GetCodec1);
  TestIndexMetaReader& test_reader0 = test_codec0.index_meta_reader;
  TestIndexMetaReader& test_reader1 = test_codec1.index_meta_reader;
  gCodec0 = &test_codec0;
  gCodec1 = &test_codec1;

  irs::MemoryDirectory dir;
  std::string codec0_file0("0seg0");
  std::string codec0_file1("0seg1");
  std::string codec1_file0("1seg0");
  std::string codec1_file1("1seg1");

  ASSERT_FALSE(!dir.create(codec0_file0));
  ASSERT_FALSE(!dir.create(codec1_file0));
  std::this_thread::sleep_for(
    1s);  // wait 1 sec to ensure index file timestamps differ
  ASSERT_FALSE(!dir.create(codec0_file1));
  ASSERT_FALSE(!dir.create(codec1_file1));

  test_reader0.read_file.clear();
  test_reader1.read_file.clear();
  test_reader0.segments_file = codec0_file0;
  test_reader1.segments_file = codec1_file1;
  irs::DirectoryReader{dir};
  ASSERT_TRUE(test_reader0.read_file.empty());  // file not read from codec0
  ASSERT_EQ(codec1_file1,
            test_reader1.read_file);  // check file read from codec1

  test_reader0.read_file.clear();
  test_reader1.read_file.clear();
  test_reader0.segments_file = codec0_file1;
  test_reader1.segments_file = codec1_file0;
  irs::DirectoryReader{dir};
  ASSERT_EQ(codec0_file1,
            test_reader0.read_file);            // check file read from codec0
  ASSERT_TRUE(test_reader1.read_file.empty());  // file not read from codec1

  gCodec0 = nullptr;
  gCodec1 = nullptr;
}

TEST(directory_reader_test, open) {
  tests::JsonDocGenerator gen(
    TestBase::resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (tests::JsonDocGenerator::ValueType::STRING == data.vt) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
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
  auto codec_ptr = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec_ptr);

  // create index
  {
    // open writer
    auto writer = irs::IndexWriter::Make(dir, codec_ptr, irs::kOmCreate);

    // add first segment
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    writer->Commit();
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir, codec_ptr));

    // add second segment
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc5->indexed.begin(), doc5->indexed.end(),
                       doc5->stored.begin(), doc5->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc6->indexed.begin(), doc6->indexed.end(),
                       doc6->stored.begin(), doc6->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc7->indexed.begin(), doc7->indexed.end(),
                       doc7->stored.begin(), doc7->stored.end()));
    writer->Commit();
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir, codec_ptr));

    // add third segment
    ASSERT_TRUE(Insert(*writer, doc8->indexed.begin(), doc8->indexed.end(),
                       doc8->stored.begin(), doc8->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc9->indexed.begin(), doc9->indexed.end(),
                       doc9->stored.begin(), doc9->stored.end()));
    writer->Commit();
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir, codec_ptr));
  }

  // open reader
  auto rdr = irs::DirectoryReader(dir, codec_ptr);
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

    const auto* column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    // read documents
    ASSERT_EQ(1, values->seek(1));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc1
    ASSERT_EQ(2, values->seek(2));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_EQ(3, values->seek(3));
    ASSERT_EQ("C", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc3

    // read invalid document
    ASSERT_TRUE(irs::doc_limits::eof(values->seek(4)));
  }

  // second segment
  {
    ++sub;
    ASSERT_NE(rdr.end(), sub);
    ASSERT_EQ(1, sub->size());
    ASSERT_EQ(4, sub->docs_count());
    ASSERT_EQ(4, sub->live_docs_count());

    const auto* column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    // read documents
    ASSERT_EQ(1, values->seek(1));
    ASSERT_EQ("D", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc4
    ASSERT_EQ(2, values->seek(2));
    ASSERT_EQ("E", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc5
    ASSERT_EQ(3, values->seek(3));
    ASSERT_EQ("F", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc6
    ASSERT_EQ(4, values->seek(4));
    ASSERT_EQ("G", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc7

    // read invalid document
    ASSERT_TRUE(irs::doc_limits::eof(values->seek(5)));
  }

  // third segment
  {
    ++sub;
    ASSERT_NE(rdr.end(), sub);
    ASSERT_EQ(1, sub->size());
    ASSERT_EQ(2, sub->docs_count());
    ASSERT_EQ(2, sub->live_docs_count());

    const auto* column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    // read documents
    ASSERT_EQ(1, values->seek(1));
    ASSERT_EQ("H", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc8
    ASSERT_EQ(2, values->seek(2));
    ASSERT_EQ("I", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc9

    // read invalid document
    ASSERT_TRUE(irs::doc_limits::eof(values->seek(3)));
  }

  ++sub;
  ASSERT_EQ(rdr.end(), sub);
}

TEST(segment_reader_test, segment_reader_has) {
  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);

  std::string filename;

  // has none (default)
  {
    irs::MemoryDirectory dir;
    auto writer = codec->get_segment_meta_writer();
    auto reader = codec->get_segment_meta_reader();
    irs::SegmentMeta expected;

    writer->write(dir, filename, expected);

    irs::SegmentMeta meta;

    reader->read(dir, meta);

    ASSERT_EQ(expected, meta);
    ASSERT_FALSE(meta.column_store);
    ASSERT_FALSE(irs::HasRemovals(meta));
  }

  // has column store
  {
    irs::MemoryDirectory dir;
    auto writer = codec->get_segment_meta_writer();
    auto reader = codec->get_segment_meta_reader();
    irs::SegmentMeta expected;

    expected.column_store = true;
    writer->write(dir, filename, expected);

    irs::SegmentMeta meta;

    reader->read(dir, meta, filename);

    ASSERT_EQ(expected, meta);
    ASSERT_TRUE(meta.column_store);
    ASSERT_FALSE(irs::HasRemovals(meta));
  }

  // has document mask
  {
    irs::MemoryDirectory dir;
    auto writer = codec->get_segment_meta_writer();
    auto reader = codec->get_segment_meta_reader();
    irs::SegmentMeta expected;

    expected.docs_count = 43;
    expected.live_docs_count = 42;
    expected.version = 0;
    expected.docs_mask = [&] {
      auto docs_mask =
        std::make_shared<irs::DocumentMask>(irs::IResourceManager::gNoop);
      docs_mask->insert(4);
      return docs_mask;
    }();
    writer->write(dir, filename, expected);

    irs::SegmentMeta meta;

    reader->read(dir, meta, filename);

    ASSERT_EQ(expected, meta);
    ASSERT_FALSE(meta.column_store);
    ASSERT_TRUE(irs::HasRemovals(meta));
  }

  // has all
  {
    irs::MemoryDirectory dir;
    auto writer = codec->get_segment_meta_writer();
    auto reader = codec->get_segment_meta_reader();
    irs::SegmentMeta expected;

    expected.docs_count = 43;
    expected.live_docs_count = 42;
    expected.column_store = true;
    expected.version = 1;
    expected.docs_mask = [&] {
      auto docs_mask =
        std::make_shared<irs::DocumentMask>(irs::IResourceManager::gNoop);
      docs_mask->insert(4);
      return docs_mask;
    }();
    writer->write(dir, filename, expected);

    irs::SegmentMeta meta;
    reader->read(dir, meta, filename);

    ASSERT_EQ(expected, meta);
    ASSERT_TRUE(meta.column_store);
    ASSERT_TRUE(irs::HasRemovals(meta));
  }
}

TEST(segment_reader_test, open_invalid_segment) {
  irs::MemoryDirectory dir;
  auto codec_ptr = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec_ptr);

  /* open invalid segment */
  {
    irs::SegmentMeta meta;
    meta.codec = codec_ptr;
    meta.name = "invalid_segment_name";

    ASSERT_THROW(
      irs::SegmentReaderImpl::Open(dir, meta, irs::IndexReaderOptions{}),
      irs::IoError);
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
  auto codec_ptr = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec_ptr);
  irs::DirectoryReader writer_snapshot;
  {
    // open writer
    auto writer = irs::IndexWriter::Make(dir, codec_ptr, irs::kOmCreate);

    // add first segment
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc5->indexed.begin(), doc5->indexed.end(),
                       doc5->stored.begin(), doc5->stored.end()));
    writer->Commit();
    writer_snapshot = writer->GetSnapshot();
  }

  // check segment
  {
    irs::SegmentMeta meta;
    meta.codec = codec_ptr;
    meta.column_store = true;
    meta.docs_count = 5;
    meta.live_docs_count = 5;
    meta.name = "_1";
    meta.version = 42;

    auto rdr =
      irs::SegmentReaderImpl::Open(dir, meta, irs::IndexReaderOptions{});
    ASSERT_FALSE(!rdr);
    ASSERT_EQ(1, rdr->size());
    ASSERT_EQ(meta.docs_count, rdr->docs_count());
    ASSERT_EQ(meta.live_docs_count, rdr->live_docs_count());

    auto& segment = *rdr->begin();
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    // read documents
    ASSERT_EQ(1, values->seek(1));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc1
    ASSERT_EQ(2, values->seek(2));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_EQ(3, values->seek(3));
    ASSERT_EQ("C", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc3
    ASSERT_EQ(4, values->seek(4));
    ASSERT_EQ("D", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc4
    ASSERT_EQ(5, values->seek(5));
    ASSERT_EQ("E", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc5

    ASSERT_TRUE(
      irs::doc_limits::eof(values->seek(6)));  // read invalid document

    // check iterators
    {
      auto it = rdr->begin();
      ASSERT_EQ(rdr.get(), &*it); /* should return self */
      ASSERT_NE(rdr->end(), it);
      ++it;
      ASSERT_EQ(rdr->end(), it);
    }

    // check field names
    {
      auto it = rdr->fields();
      ASSERT_TRUE(it->next());
      ASSERT_EQ("duplicated", it->value().meta().name);
      ASSERT_TRUE(it->next());
      ASSERT_EQ("name", it->value().meta().name);
      ASSERT_TRUE(it->next());
      ASSERT_EQ("prefix", it->value().meta().name);
      ASSERT_TRUE(it->next());
      ASSERT_EQ("same", it->value().meta().name);
      ASSERT_TRUE(it->next());
      ASSERT_EQ("seq", it->value().meta().name);
      ASSERT_TRUE(it->next());
      ASSERT_EQ("value", it->value().meta().name);
      ASSERT_FALSE(it->next());
    }

    // check live docs
    {
      auto it = rdr->docs_iterator();
      ASSERT_TRUE(it->next());
      ASSERT_EQ(1, it->value());
      ASSERT_TRUE(it->next());
      ASSERT_EQ(2, it->value());
      ASSERT_TRUE(it->next());
      ASSERT_EQ(3, it->value());
      ASSERT_TRUE(it->next());
      ASSERT_EQ(4, it->value());
      ASSERT_TRUE(it->next());
      ASSERT_EQ(5, it->value());
      ASSERT_FALSE(it->next());
      ASSERT_FALSE(it->next());
    }

    // check field metadata
    {
      {
        auto it = rdr->fields();
        size_t size = 0;
        while (it->next()) {
          ++size;
        }
        ASSERT_EQ(6, size);
      }

      // check field
      {
        const std::string_view name = "name";
        auto field = rdr->field(name);
        ASSERT_EQ(name, field->meta().name);

        // check terms
        auto terms = rdr->field(name);
        ASSERT_NE(nullptr, terms);

        ASSERT_EQ(5, terms->size());
        ASSERT_EQ(5, terms->docs_count());
        ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("A")),
                  (terms->min)());
        ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("E")),
                  (terms->max)());

        auto term = terms->iterator(irs::SeekMode::NORMAL);

        // check term: A
        {
          ASSERT_TRUE(term->next());
          ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("A")),
                    term->value());

          // check docs
          {
            auto docs = term->postings(irs::IndexFeatures::None);
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(1, docs->value());
            ASSERT_FALSE(docs->next());
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
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(2, docs->value());
            ASSERT_FALSE(docs->next());
            ASSERT_FALSE(docs->next());
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
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(3, docs->value());
            ASSERT_FALSE(docs->next());
            ASSERT_FALSE(docs->next());
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
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(4, docs->value());
            ASSERT_FALSE(docs->next());
            ASSERT_FALSE(docs->next());
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
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(5, docs->value());
            ASSERT_FALSE(docs->next());
            ASSERT_FALSE(docs->next());
          }
        }

        ASSERT_FALSE(term->next());
      }

      // check field
      {
        const std::string_view name = "seq";
        auto field = rdr->field(name);
        ASSERT_EQ(name, field->meta().name);

        // check terms
        auto terms = rdr->field(name);
        ASSERT_NE(nullptr, terms);
      }

      // check field
      {
        const std::string_view name = "same";
        auto field = rdr->field(name);
        ASSERT_EQ(name, field->meta().name);

        // check terms
        auto terms = rdr->field(name);
        ASSERT_NE(nullptr, terms);
        ASSERT_EQ(1, terms->size());
        ASSERT_EQ(5, terms->docs_count());
        ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("xyz")),
                  (terms->min)());
        ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("xyz")),
                  (terms->max)());

        auto term = terms->iterator(irs::SeekMode::NORMAL);

        // check term: xyz
        {
          ASSERT_TRUE(term->next());
          ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("xyz")),
                    term->value());

          /* check docs */
          {
            auto docs = term->postings(irs::IndexFeatures::None);
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(1, docs->value());
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(2, docs->value());
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(3, docs->value());
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(4, docs->value());
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(5, docs->value());
            ASSERT_FALSE(docs->next());
            ASSERT_FALSE(docs->next());
          }
        }

        ASSERT_FALSE(term->next());
      }

      // check field
      {
        const std::string_view name = "duplicated";
        auto field = rdr->field(name);
        ASSERT_EQ(name, field->meta().name);

        // check terms
        auto terms = rdr->field(name);
        ASSERT_NE(nullptr, terms);
        ASSERT_EQ(2, terms->size());
        ASSERT_EQ(4, terms->docs_count());
        ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("abcd")),
                  (terms->min)());
        ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("vczc")),
                  (terms->max)());

        auto term = terms->iterator(irs::SeekMode::NORMAL);

        // check term: abcd
        {
          ASSERT_TRUE(term->next());
          ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("abcd")),
                    term->value());

          // check docs
          {
            auto docs = term->postings(irs::IndexFeatures::None);
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(1, docs->value());
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(5, docs->value());
            ASSERT_FALSE(docs->next());
            ASSERT_FALSE(docs->next());
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
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(2, docs->value());
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(3, docs->value());
            ASSERT_FALSE(docs->next());
            ASSERT_FALSE(docs->next());
          }
        }

        ASSERT_FALSE(term->next());
      }

      // check field
      {
        const std::string_view name = "prefix";
        auto field = rdr->field(name);
        ASSERT_EQ(name, field->meta().name);

        // check terms
        auto terms = rdr->field(name);
        ASSERT_NE(nullptr, terms);
        ASSERT_EQ(2, terms->size());
        ASSERT_EQ(2, terms->docs_count());
        ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("abcd")),
                  (terms->min)());
        ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("abcde")),
                  (terms->max)());

        auto term = terms->iterator(irs::SeekMode::NORMAL);

        // check term: abcd
        {
          ASSERT_TRUE(term->next());
          ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("abcd")),
                    term->value());

          // check docs
          {
            auto docs = term->postings(irs::IndexFeatures::None);
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(1, docs->value());
            ASSERT_FALSE(docs->next());
            ASSERT_FALSE(docs->next());
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
            ASSERT_TRUE(docs->next());
            ASSERT_EQ(4, docs->value());
            ASSERT_FALSE(docs->next());
            ASSERT_FALSE(docs->next());
          }
        }

        ASSERT_FALSE(term->next());
      }

      // invalid field
      {
        const std::string_view name = "invalid_field";
        ASSERT_EQ(nullptr, rdr->field(name));
      }
    }
  }
}
