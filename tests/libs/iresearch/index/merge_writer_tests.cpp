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

#include <unordered_map>
#include <unordered_set>

#include "formats/column/test_cs_helpers.hpp"
#include "index_tests.hpp"
#include "iresearch/columnstore/norm_reader.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/comparer.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/merge_writer.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/index/segment_reader_impl.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/utils/index_utils.hpp"
#include "iresearch/utils/lz4compression.hpp"
#include "iresearch/utils/type_limits.hpp"
#include "utils/write_helpers.hpp"

namespace {

inline constexpr irs::field_id kSeqId = 1;

irs::Filter::ptr MakeByTerm(std::string_view name, std::string_view value) {
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field() = name;
  filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(value);
  return filter;
}

}  // namespace
namespace tests {

class BinaryComparer final : public irs::Comparer {
  int CompareImpl(irs::bytes_view lhs, irs::bytes_view rhs) const final {
    EXPECT_FALSE(irs::IsNull(lhs));
    EXPECT_FALSE(irs::IsNull(rhs));

    return lhs.compare(rhs);
  }
};

template<typename T, typename H>
void ValidateTerms(
  const irs::SubReader& segment, const irs::TermReader& terms,
  uint64_t doc_count, const irs::bytes_view& min, const irs::bytes_view& max,
  size_t term_size, irs::IndexFeatures index_features,
  std::unordered_map<T, std::unordered_set<irs::doc_id_t>, H>& expected_terms,
  size_t* frequency = nullptr, std::vector<uint32_t>* position = nullptr) {
  ASSERT_EQ(doc_count, terms.docs_count());
  ASSERT_EQ((max), (terms.max)());
  ASSERT_EQ((min), (terms.min)());
  ASSERT_EQ(term_size, terms.size());
  ASSERT_EQ(index_features, terms.meta().index_features);

  for (auto term_itr = terms.iterator(irs::SeekMode::NORMAL);
       term_itr->next();) {
    auto itr = expected_terms.find(static_cast<T>(term_itr->value()));

    ASSERT_NE(expected_terms.end(), itr);

    for (auto docs_itr = segment.mask(term_itr->postings(index_features));
         docs_itr->next();) {  // FIXME
      ASSERT_EQ(1, itr->second.erase(docs_itr->value()));

      if (frequency) {
        const auto* freq_block = irs::get<irs::FreqBlockAttr>(*docs_itr);
        ASSERT_TRUE(freq_block);
        docs_itr->FetchScoreArgs(0);
        ASSERT_EQ(*frequency, freq_block->value[0]);
      }

      if (position) {
        auto* docs_itr_pos = irs::GetMutable<irs::PosAttr>(docs_itr.get());
        ASSERT_TRUE(docs_itr_pos);

        for (auto pos : *position) {
          ASSERT_TRUE(docs_itr_pos->next());
          ASSERT_EQ(pos, docs_itr_pos->value());
        }

        ASSERT_FALSE(docs_itr_pos->next());
      }
    }

    ASSERT_TRUE(itr->second.empty());
    expected_terms.erase(itr);
  }

  ASSERT_TRUE(expected_terms.empty());
}

}  // namespace tests

using namespace tests;

struct MergeWriterTestCase : public tests::DirectoryTestCaseBase<std::string> {
  std::shared_ptr<const irs::Format> Codec() const {
    const auto& p = tests::DirectoryTestCaseBase<std::string>::GetParam();
    const auto& codec_name = std::get<std::string>(p);
    const auto codec = irs::formats::Get(codec_name);
    EXPECT_NE(nullptr, codec);

    return codec;
  }

  bool SupportsSort() const noexcept { return true; }

  static std::string to_string(
    const testing::TestParamInfo<std::tuple<tests::dir_param_f, std::string>>&
      info) {
    auto [factory, codec] = info.param;

    return (*factory)(nullptr).second + "___" + codec;
  }

  void EnsureDocBlocksNotMixed(bool primary_sort);
};

void MergeWriterTestCase::EnsureDocBlocksNotMixed(bool primary_sort) {
  auto insert_documents = [](irs::IndexWriter::Transaction& ctx,
                             irs::doc_id_t seed, irs::doc_id_t count) {
    for (; seed < count; ++seed) {
      auto doc = ctx.Insert();
      const tests::StringField foo{"foo", "bar"};
      doc.Insert(foo);
      const tests::StringField seq{"seq", std::to_string(seed)};
      irs::tests::StoreFieldAt(*doc.Columnstore(), kSeqId, doc.DocId(), seq);
    }
  };

  auto codec_ptr = Codec();
  ASSERT_NE(nullptr, codec_ptr);
  irs::MemoryDirectory dir;
  BinaryComparer test_comparer;

  auto opts = irs::tests::DefaultWriterOptions();
  if (primary_sort) {
    opts.comparator = &test_comparer;
  }

  auto writer = irs::IndexWriter::Make(dir, codec_ptr, irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);

  {
    auto segment0 = writer->GetBatch();
    insert_documents(segment0, 0, 10);
    auto segment1 = writer->GetBatch();
    insert_documents(segment1, 10, 20);
    auto segment2 = writer->GetBatch();
    insert_documents(segment2, 20, 30);
  }

  ASSERT_TRUE(writer->RefreshCommit());
  AssertSnapshotEquality(
    writer->GetSnapshot(),
    irs::DirectoryReader(dir, codec_ptr, irs::tests::DefaultReaderOptions()));

  auto reader =
    irs::DirectoryReader(dir, codec_ptr, irs::tests::DefaultReaderOptions());
  ASSERT_NE(nullptr, reader);

  ASSERT_EQ(3, reader.size());
  for (auto& segment : reader) {
    ASSERT_EQ(10, segment.docs_count());
    ASSERT_EQ(10, segment.live_docs_count());
  }

  // Segments:
  // 0: 1..10
  // 1: 11..20
  // 2: 21..30
  const irs::index_utils::CompactionCount compact_all;
  ASSERT_EQ(!primary_sort || SupportsSort(),
            writer->Compact(irs::index_utils::MakePolicy(compact_all)));
  ASSERT_EQ(!primary_sort || SupportsSort(), writer->RefreshCommit());
  AssertSnapshotEquality(
    writer->GetSnapshot(),
    irs::DirectoryReader(dir, codec_ptr, irs::tests::DefaultReaderOptions()));

  if (!primary_sort || SupportsSort()) {
    reader = reader.Reopen();
    ASSERT_NE(nullptr, reader);

    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];
    ASSERT_EQ(30, segment.docs_count());
    ASSERT_EQ(30, segment.live_docs_count());

    const auto docs_count = segment.docs_count();
    const auto* col = segment.Column(kSeqId);
    ASSERT_NE(nullptr, col);
    irs::tests::BlobPointReader values{segment, *col};

    ptrdiff_t prev = -1;
    for (irs::doc_id_t i = 0; i < docs_count; ++i) {
      SCOPED_TRACE(testing::Message("Doc id ") << i);
      const auto doc = i + irs::doc_limits::min();

      const auto bytes = values.Get(doc);
      auto* p = bytes.data();
      const auto len = irs::vread<uint32_t>(p);
      const auto str_seq =
        static_cast<std::string>(irs::ViewCast<char>(irs::bytes_view{p, len}));
      const auto seq = atoi(str_seq.data());

      if (0 == (i % 10)) {
        ASSERT_EQ(0, seq % 10);
      } else {
        ASSERT_LT(prev, seq);
        ASSERT_NE(0, seq % 10);
      }

      prev = seq;
    }
  } else {
    AssertSnapshotEquality(
      reader,
      irs::DirectoryReader(dir, codec_ptr, irs::tests::DefaultReaderOptions()));
  }
}

TEST_P(MergeWriterTestCase, test_merge_writer_add_segments) {
  auto codec_ptr = Codec();
  ASSERT_NE(nullptr, codec_ptr);
  irs::MemoryDirectory data_dir;

  // populate directory
  {
    tests::JsonDocGenerator gen(TestBase::resource("simple_sequential_33.json"),
                                &tests::GenericJsonFieldFactory);
    std::vector<const tests::Document*> docs;
    docs.reserve(33);

    for (size_t i = 0; i < 33; ++i) {
      docs.emplace_back(gen.next());
    }

    auto writer = irs::IndexWriter::Make(data_dir, codec_ptr, irs::kOmCreate);

    for (auto* doc : docs) {
      ASSERT_NE(nullptr, doc);
      ASSERT_TRUE(Insert(*writer, doc->indexed.begin(), doc->indexed.end()));
      writer->RefreshCommit();  // create segmentN
      AssertSnapshotEquality(writer->GetSnapshot(),
                             irs::DirectoryReader(data_dir, codec_ptr));
    }
  }

  auto reader = irs::DirectoryReader(data_dir, codec_ptr);

  ASSERT_EQ(33, reader.size());

  // merge 33 segments to writer (segments > 32 to trigger GCC 8.2.0 optimizer
  // bug)
  {
    irs::MemoryDirectory dir;
    irs::SegmentMeta index_segment;
    const irs::SegmentWriterOptions options{.scorers_features = {}};
    irs::MergeWriter writer(dir, options);
    writer.Reset(reader.begin(), reader.end());

    index_segment.codec = codec_ptr;
    ASSERT_TRUE(writer.Flush(index_segment));

    auto segment = irs::SegmentReaderImpl::Open(dir, index_segment,
                                                irs::IndexReaderOptions{});
    ASSERT_EQ(33, segment->docs_count());
    ASSERT_EQ(33, segment->field("name")->docs_count());
    ASSERT_EQ(33, segment->field("seq")->docs_count());
    ASSERT_EQ(33, segment->field("same")->docs_count());
    ASSERT_EQ(13, segment->field("duplicated")->docs_count());
  }
}

TEST_P(MergeWriterTestCase, test_merge_writer_flush_progress) {
  auto codec_ptr = Codec();
  ASSERT_NE(nullptr, codec_ptr);
  irs::MemoryDirectory data_dir;

  // populate directory
  {
    tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    auto* doc1 = gen.next();
    auto* doc2 = gen.next();
    auto writer = irs::IndexWriter::Make(data_dir, codec_ptr, irs::kOmCreate);
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end()));
    writer->RefreshCommit();  // create segment0
    AssertSnapshotEquality(writer->GetSnapshot(),
                           irs::DirectoryReader(data_dir, codec_ptr));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end()));
    writer->RefreshCommit();  // create segment1
    AssertSnapshotEquality(writer->GetSnapshot(),
                           irs::DirectoryReader(data_dir, codec_ptr));
  }

  auto reader = irs::DirectoryReader(data_dir, codec_ptr);

  ASSERT_EQ(2, reader.size());
  ASSERT_EQ(1, reader[0].docs_count());
  ASSERT_EQ(1, reader[1].docs_count());

  // test default progress (false)
  {
    irs::MemoryDirectory dir;
    irs::SegmentMeta index_segment;
    irs::MergeWriter::FlushProgress progress;
    const irs::SegmentWriterOptions options{.scorers_features = {}};
    irs::MergeWriter writer(dir, options);

    index_segment.codec = codec_ptr;
    writer.Reset(reader.begin(), reader.end());
    ASSERT_TRUE(writer.Flush(index_segment, progress));

    ASSERT_FALSE(index_segment.files.empty());
    ASSERT_EQ(2, index_segment.docs_count);
    ASSERT_EQ(2, index_segment.live_docs_count);
    ASSERT_EQ(0, index_segment.version);

    auto segment = irs::SegmentReaderImpl::Open(dir, index_segment,
                                                irs::IndexReaderOptions{});
    ASSERT_EQ(2, segment->docs_count());
  }

  // test always-false progress
  {
    irs::MemoryDirectory dir;
    irs::SegmentMeta index_segment;
    irs::MergeWriter::FlushProgress progress = []() -> bool { return false; };
    const irs::SegmentWriterOptions options{.scorers_features = {}};
    irs::MergeWriter writer(dir, options);

    index_segment.codec = codec_ptr;
    writer.Reset(reader.begin(), reader.end());
    ASSERT_FALSE(writer.Flush(index_segment, progress));

    ASSERT_TRUE(index_segment.name.empty());
    ASSERT_TRUE(index_segment.files.empty());
    ASSERT_EQ(0, index_segment.version);
    ASSERT_EQ(0, index_segment.docs_count);
    ASSERT_EQ(0, index_segment.live_docs_count);
    ASSERT_EQ(0, index_segment.byte_size);

    ASSERT_ANY_THROW(irs::SegmentReaderImpl::Open(dir, index_segment,
                                                  irs::IndexReaderOptions{}));
  }

  size_t progress_call_count = 0;

  // test always-true progress
  {
    irs::MemoryDirectory dir;
    irs::SegmentMeta index_segment;
    irs::MergeWriter::FlushProgress progress =
      [&progress_call_count]() -> bool {
      ++progress_call_count;
      return true;
    };
    const irs::SegmentWriterOptions options{.scorers_features = {}};
    irs::MergeWriter writer(dir, options);

    index_segment.codec = codec_ptr;
    writer.Reset(reader.begin(), reader.end());
    ASSERT_TRUE(writer.Flush(index_segment, progress));

    ASSERT_FALSE(index_segment.files.empty());
    ASSERT_EQ(2, index_segment.docs_count);
    ASSERT_EQ(2, index_segment.live_docs_count);
    ASSERT_EQ(0, index_segment.version);

    auto segment = irs::SegmentReaderImpl::Open(dir, index_segment,
                                                irs::IndexReaderOptions{});
    ASSERT_EQ(2, segment->docs_count());
  }

  ASSERT_TRUE(
    progress_call_count);  // there should have been at least some calls

  // test limited-true progress
  for (size_t i = 1; i < progress_call_count;
       ++i) {  // +1 for pre-decrement in 'progress'
    size_t call_count = i;
    irs::MemoryDirectory dir;
    irs::SegmentMeta index_segment;
    irs::MergeWriter::FlushProgress progress = [&call_count]() -> bool {
      return --call_count;
    };
    const irs::SegmentWriterOptions options{.scorers_features = {}};
    irs::MergeWriter writer(dir, options);

    index_segment.codec = codec_ptr;
    index_segment.name = "merged";
    writer.Reset(reader.begin(), reader.end());
    ASSERT_FALSE(writer.Flush(index_segment, progress));
    ASSERT_EQ(0, call_count);

    ASSERT_TRUE(index_segment.name.empty());
    ASSERT_TRUE(index_segment.files.empty());
    ASSERT_EQ(0, index_segment.version);
    ASSERT_EQ(0, index_segment.docs_count);
    ASSERT_EQ(0, index_segment.live_docs_count);
    ASSERT_EQ(0, index_segment.byte_size);

    ASSERT_ANY_THROW(irs::SegmentReaderImpl::Open(dir, index_segment,
                                                  irs::IndexReaderOptions{}));
  }
}

TEST_P(MergeWriterTestCase, test_merge_writer_field_features) {
  std::string field("doc_string");
  std::string data("string_data");
  tests::Document doc1;  // string
  tests::Document doc2;  // text

  doc1.insert(std::make_shared<tests::StringField>(field, data));
  doc2.indexed.push_back(
    std::make_shared<tests::TextField<std::string_view>>(field, data, true));

  // FIXME
  // ASSERT_TRUE(irs::IsSubsetOf(doc1.indexed.get(field)->GetIndexFeatures(),
  //                             doc2.indexed.get(field)->GetIndexFeatures()));
  // ASSERT_FALSE(irs::IsSubsetOf(doc2.indexed.get(field)->GetIndexFeatures(),
  //                              doc1.indexed.get(field)->GetIndexFeatures()));

  auto codec_ptr = Codec();
  ASSERT_NE(nullptr, codec_ptr);
  irs::MemoryDirectory dir;

  // populate directory
  {
    auto writer = irs::IndexWriter::Make(dir, codec_ptr, irs::kOmCreate);
    ASSERT_TRUE(Insert(*writer, doc1.indexed.begin(), doc1.indexed.end()));
    writer->RefreshCommit();
    AssertSnapshotEquality(writer->GetSnapshot(),
                           irs::DirectoryReader(dir, codec_ptr));
    ASSERT_TRUE(Insert(*writer, doc2.indexed.begin(), doc2.indexed.end()));
    writer->RefreshCommit();
    AssertSnapshotEquality(writer->GetSnapshot(),
                           irs::DirectoryReader(dir, codec_ptr));
  }

  auto reader = irs::DirectoryReader(dir, codec_ptr);

  ASSERT_EQ(2, reader.size());
  ASSERT_EQ(1, reader[0].docs_count());
  ASSERT_EQ(1, reader[1].docs_count());

  // test merge existing with feature subset (success)
  {
    std::array<const irs::SubReader*, 2> segments{
      &reader[1],  // assume 1 is segment with text field
      &reader[0]   // assume 0 is segment with string field
    };

    const irs::SegmentWriterOptions options{.scorers_features = {}};
    irs::MergeWriter writer(dir, options);
    writer.Reset(segments.begin(), segments.end());

    irs::SegmentMeta index_segment;
    index_segment.codec = codec_ptr;
    ASSERT_TRUE(writer.Flush(index_segment));
  }

  // test merge existing with feature superset (fail)
  {
    std::array<const irs::SubReader*, 2> segments{
      &reader[0],  // assume 0 is segment with text field
      &reader[1]   // assume 1 is segment with string field
    };

    const irs::SegmentWriterOptions options{.scorers_features = {}};
    irs::MergeWriter writer(dir, options);
    writer.Reset(segments.begin(), segments.end());

    irs::SegmentMeta index_segment;
    index_segment.codec = codec_ptr;
    ASSERT_FALSE(writer.Flush(index_segment));
  }
}

TEST_P(MergeWriterTestCase, EnsureDocBlocksNotMixedPrimarySort) {
  EnsureDocBlocksNotMixed(true);
}

TEST_P(MergeWriterTestCase, EnsureDocBlocksNotMixed) {
  EnsureDocBlocksNotMixed(false);
}

TEST_P(MergeWriterTestCase, test_merge_writer) {
  auto codec_ptr = Codec();
  ASSERT_NE(nullptr, codec_ptr);
  irs::MemoryDirectory dir;

  irs::bstring bytes1;
  irs::bstring bytes2;
  irs::bstring bytes3;

  bytes1.append(irs::ViewCast<irs::byte_type>(std::string_view("bytes1_data")));
  bytes2.append(irs::ViewCast<irs::byte_type>(std::string_view("bytes2_data")));
  bytes3.append(irs::ViewCast<irs::byte_type>(std::string_view("bytes3_data")));

  constexpr irs::IndexFeatures kStringFieldFeatures =
    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
  constexpr irs::IndexFeatures kTextFieldFeatures = irs::IndexFeatures::Freq |
                                                    irs::IndexFeatures::Pos |
                                                    irs::IndexFeatures::Offs;

  std::string string1;
  std::string string2;
  std::string string3;
  std::string string4;

  string1.append("string1_data");
  string2.append("string2_data");
  string3.append("string3_data");
  string4.append("string4_data");

  std::string text1;
  std::string text2;
  std::string text3;

  text1.append("text1_data");
  text2.append("text2_data");
  text3.append("text3_data");

  tests::Document doc1;
  tests::Document doc2;
  tests::Document doc3;
  tests::Document doc4;

  // norm for 'doc_bytes' in 'doc1': 4
  doc1.insert(std::make_shared<tests::BinaryField>());
  {
    auto& field = doc1.indexed.back<tests::BinaryField>();
    field.Name("doc_bytes");
    field.value(bytes1);
    field.index_features |= irs::IndexFeatures::Norm;
  }
  doc1.insert(std::make_shared<tests::BinaryField>());
  {
    auto& field = doc1.indexed.back<tests::BinaryField>();
    field.Name("doc_bytes");
    field.value(bytes1);
    field.index_features |= irs::IndexFeatures::Norm;
  }
  doc1.insert(std::make_shared<tests::BinaryField>());
  {
    auto& field = doc1.indexed.back<tests::BinaryField>();
    field.Name("doc_bytes");
    field.value(bytes1);
    field.index_features |= irs::IndexFeatures::Norm;
  }
  doc1.insert(std::make_shared<tests::BinaryField>());
  {
    auto& field = doc1.indexed.back<tests::BinaryField>();
    field.Name("doc_bytes");
    field.value(bytes1);
    field.index_features |= irs::IndexFeatures::Norm;
  }

  // do not track norms for 'doc_bytes' in 'doc2' explicitly,
  // but norms are already tracked for 'doc_bytes' field
  doc2.insert(std::make_shared<tests::BinaryField>());
  {
    auto& field = doc2.indexed.back<tests::BinaryField>();
    field.Name("doc_bytes");
    field.value(bytes2);
  }
  doc2.insert(std::make_shared<tests::BinaryField>());
  {
    auto& field = doc2.indexed.back<tests::BinaryField>();
    field.Name("doc_bytes");
    field.value(bytes2);
  }

  // norm for 'doc_bytes' in 'doc3' : 3
  doc3.insert(std::make_shared<tests::BinaryField>());
  {
    auto& field = doc3.indexed.back<tests::BinaryField>();
    field.Name("doc_bytes");
    field.value(bytes3);
    field.index_features |= irs::IndexFeatures::Norm;
  }
  doc3.insert(std::make_shared<tests::BinaryField>());
  {
    auto& field = doc3.indexed.back<tests::BinaryField>();
    field.Name("doc_bytes");
    field.value(bytes3);
    field.index_features |= irs::IndexFeatures::Norm;
  }
  doc3.insert(std::make_shared<tests::BinaryField>());
  {
    auto& field = doc3.indexed.back<tests::BinaryField>();
    field.Name("doc_bytes");
    field.value(bytes3);
    field.index_features |= irs::IndexFeatures::Norm;
  }

  doc1.insert(std::make_shared<tests::DoubleField>());
  {
    auto& field = doc1.indexed.back<tests::DoubleField>();
    field.Name("doc_double");
    field.value(2.718281828 * 1);
  }
  doc2.insert(std::make_shared<tests::DoubleField>());
  {
    auto& field = doc2.indexed.back<tests::DoubleField>();
    field.Name("doc_double");
    field.value(2.718281828 * 2);
  }
  doc3.insert(std::make_shared<tests::DoubleField>());
  {
    auto& field = doc3.indexed.back<tests::DoubleField>();
    field.Name("doc_double");
    field.value(2.718281828 * 3);
  }
  doc1.insert(std::make_shared<tests::FloatField>());
  {
    auto& field = doc1.indexed.back<tests::FloatField>();
    field.Name("doc_float");
    field.value(3.1415926535f * 1);
  }
  doc2.insert(std::make_shared<tests::FloatField>());
  {
    auto& field = doc2.indexed.back<tests::FloatField>();
    field.Name("doc_float");
    field.value(3.1415926535f * 2);
  }
  doc3.insert(std::make_shared<tests::FloatField>());
  {
    auto& field = doc3.indexed.back<tests::FloatField>();
    field.Name("doc_float");
    field.value(3.1415926535f * 3);
  }
  doc1.insert(std::make_shared<tests::IntField>());
  {
    auto& field = doc1.indexed.back<tests::IntField>();
    field.Name("doc_int");
    field.value(42 * 1);
  }
  doc2.insert(std::make_shared<tests::IntField>());
  {
    auto& field = doc2.indexed.back<tests::IntField>();
    field.Name("doc_int");
    field.value(42 * 2);
  }
  doc3.insert(std::make_shared<tests::IntField>());
  {
    auto& field = doc3.indexed.back<tests::IntField>();
    field.Name("doc_int");
    field.value(42 * 3);
  }
  doc1.insert(std::make_shared<tests::LongField>());
  {
    auto& field = doc1.indexed.back<tests::LongField>();
    field.Name("doc_long");
    field.value(12345 * 1);
  }
  doc2.insert(std::make_shared<tests::LongField>());
  {
    auto& field = doc2.indexed.back<tests::LongField>();
    field.Name("doc_long");
    field.value(12345 * 2);
  }
  doc3.insert(std::make_shared<tests::LongField>());
  {
    auto& field = doc3.indexed.back<tests::LongField>();
    field.Name("doc_long");
    field.value(12345 * 3);
  }
  doc1.insert(std::make_shared<tests::StringField>("doc_string", string1));
  doc2.insert(std::make_shared<tests::StringField>("doc_string", string2));
  doc3.insert(std::make_shared<tests::StringField>("doc_string", string3));
  doc4.insert(std::make_shared<tests::StringField>("doc_string", string4));
  doc1.indexed.push_back(std::make_shared<tests::TextField<std::string_view>>(
    "doc_text", text1, true));
  doc2.indexed.push_back(std::make_shared<tests::TextField<std::string_view>>(
    "doc_text", text2, true));
  doc3.indexed.push_back(std::make_shared<tests::TextField<std::string_view>>(
    "doc_text", text3, true));

  auto opts = irs::tests::DefaultWriterOptions();

  // populate directory
  {
    auto query_doc4 = MakeByTerm("doc_string", "string4_data");
    auto writer = irs::IndexWriter::Make(dir, codec_ptr, irs::kOmCreate, opts);

    ASSERT_TRUE(Insert(*writer, doc1.indexed.begin(), doc1.indexed.end()));
    ASSERT_TRUE(Insert(*writer, doc2.indexed.begin(), doc2.indexed.end()));
    writer->RefreshCommit();
    AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec_ptr, irs::tests::DefaultReaderOptions()));
    ASSERT_TRUE(Insert(*writer, doc3.indexed.begin(), doc3.indexed.end()));
    ASSERT_TRUE(Insert(*writer, doc4.indexed.begin(), doc4.indexed.end()));
    writer->RefreshCommit();
    AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec_ptr, irs::tests::DefaultReaderOptions()));
    writer->GetBatch().Remove(std::move(query_doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, codec_ptr, irs::tests::DefaultReaderOptions()));
  }

  auto docs_count = [](const irs::SubReader& segment,
                       const std::string_view& field) {
    auto* reader = segment.field(field);
    return reader ? reader->docs_count() : 0;
  };

  auto reader =
    irs::DirectoryReader(dir, codec_ptr, irs::tests::DefaultReaderOptions());

  ASSERT_EQ(2, reader.size());
  ASSERT_EQ(2, reader[0].docs_count());
  ASSERT_EQ(2, reader[1].docs_count());

  // validate initial data (segment 0)
  {
    auto& segment = reader[0];
    ASSERT_EQ(2, segment.docs_count());

    {
      auto fields = segment.fields();
      size_t size = 0;
      while (fields->next()) {
        ++size;
      }
      ASSERT_EQ(7, size);
    }

    // validate bytes field
    {
      auto terms = segment.field("doc_bytes");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features =
        tests::BinaryField().GetIndexFeatures() | irs::IndexFeatures::Norm;
      std::unordered_map<irs::bytes_view, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bytes_view>>
        expected_terms;

      expected_terms[irs::ViewCast<irs::byte_type>(
                       std::string_view("bytes1_data"))]
        .emplace(1);
      expected_terms[irs::ViewCast<irs::byte_type>(
                       std::string_view("bytes2_data"))]
        .emplace(2);

      ASSERT_EQ(2, docs_count(segment, "doc_bytes"));

      ASSERT_EQ(features, field.index_features);
      ValidateTerms(segment, *terms, 2, bytes1, bytes2, 2, features,
                    expected_terms);

      // norm
      {
        ASSERT_TRUE(irs::field_limits::valid(field.norm));

        const auto* cs = segment.CsReader();
        ASSERT_NE(nullptr, cs);
        const auto* column = cs->NormColumn(field.norm);
        ASSERT_NE(nullptr, column);

        std::unordered_map<uint32_t, irs::doc_id_t> expected_values{{4, 1},
                                                                    {2, 2}};
        for (irs::doc_id_t doc = irs::doc_limits::min();
             doc < irs::doc_limits::min() + 2; ++doc) {
          const auto row = static_cast<uint64_t>(doc) - irs::doc_limits::min();
          const auto actual_value = column->Get(row);
          auto it = expected_values.find(actual_value);
          ASSERT_NE(expected_values.end(), it);
          ASSERT_EQ(doc, it->second);
          expected_values.erase(it);
        }
        ASSERT_TRUE(expected_values.empty());
      }
    }

    // validate double field
    {
      auto terms = segment.field("doc_double");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features = tests::DoubleField().GetIndexFeatures();
      irs::NumericTokenizer max;
      max.reset((double_t)(2.718281828 * 2));
      irs::NumericTokenizer min;
      min.reset((double_t)(2.718281828 * 1));
      std::unordered_map<irs::bstring, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bstring>>
        expected_terms;

      {
        irs::NumericTokenizer itr;
        itr.reset((double_t)(2.718281828 * 1));
        for (; itr.next();
             expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
               .emplace(1))
          ;
      }

      {
        irs::NumericTokenizer itr;
        itr.reset((double_t)(2.718281828 * 2));
        for (; itr.next();
             expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
               .emplace(2))
          ;
      }

      ASSERT_EQ(2, docs_count(segment, "doc_double"));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ASSERT_TRUE(max.next() && max.next() && max.next() &&
                  max.next());  // skip to last value
      ASSERT_TRUE(min.next());  // skip to first value
      ValidateTerms(segment, *terms, 2, irs::get<irs::TermAttr>(min)->value,
                    irs::get<irs::TermAttr>(max)->value, 8, features,
                    expected_terms);
    }

    // validate float field
    {
      auto terms = segment.field("doc_float");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features = tests::FloatField().GetIndexFeatures();
      irs::NumericTokenizer max;
      max.reset((float_t)(3.1415926535 * 2));
      irs::NumericTokenizer min;
      min.reset((float_t)(3.1415926535 * 1));
      std::unordered_map<irs::bstring, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bstring>>
        expected_terms;

      {
        irs::NumericTokenizer itr;
        itr.reset((float_t)(3.1415926535 * 1));
        for (; itr.next();
             expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
               .emplace(1))
          ;
      }

      {
        irs::NumericTokenizer itr;
        itr.reset((float_t)(3.1415926535 * 2));
        for (; itr.next();
             expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
               .emplace(2))
          ;
      }

      ASSERT_EQ(2, docs_count(segment, "doc_float"));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ASSERT_TRUE(max.next() && max.next());  // skip to last value
      ASSERT_TRUE(min.next());                // skip to first value
      ValidateTerms(segment, *terms, 2, irs::get<irs::TermAttr>(min)->value,
                    irs::get<irs::TermAttr>(max)->value, 4, features,
                    expected_terms);
    }

    // validate int field
    {
      auto terms = segment.field("doc_int");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features = tests::IntField().GetIndexFeatures();
      irs::NumericTokenizer max;
      max.reset(42 * 2);
      irs::NumericTokenizer min;
      min.reset(42 * 1);
      std::unordered_map<irs::bstring, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bstring>>
        expected_terms;

      {
        irs::NumericTokenizer itr;
        itr.reset(42 * 1);
        for (; itr.next();
             expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
               .emplace(1))
          ;
      }

      {
        irs::NumericTokenizer itr;
        itr.reset(42 * 2);
        for (; itr.next();
             expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
               .emplace(2))
          ;
      }

      ASSERT_EQ(2, docs_count(segment, "doc_int"));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ASSERT_TRUE(max.next() && max.next());  // skip to last value
      ASSERT_TRUE(min.next());                // skip to first value
      ValidateTerms(segment, *terms, 2, irs::get<irs::TermAttr>(min)->value,
                    irs::get<irs::TermAttr>(max)->value, 3, features,
                    expected_terms);
    }

    // validate long field
    {
      auto terms = segment.field("doc_long");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features = tests::LongField().GetIndexFeatures();
      irs::NumericTokenizer max;
      max.reset((int64_t)12345 * 2);
      irs::NumericTokenizer min;
      min.reset((int64_t)12345 * 1);
      std::unordered_map<irs::bstring, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bstring>>
        expected_terms;

      {
        irs::NumericTokenizer itr;
        itr.reset((int64_t)12345 * 1);
        for (; itr.next();
             expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
               .emplace(1))
          ;
      }

      {
        irs::NumericTokenizer itr;
        itr.reset((int64_t)12345 * 2);
        for (; itr.next();
             expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
               .emplace(2))
          ;
      }

      ASSERT_EQ(2, docs_count(segment, "doc_long"));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ASSERT_TRUE(max.next() && max.next() && max.next() &&
                  max.next());  // skip to last value
      ASSERT_TRUE(min.next());  // skip to first value
      ValidateTerms(segment, *terms, 2, irs::get<irs::TermAttr>(min)->value,
                    irs::get<irs::TermAttr>(max)->value, 5, features,
                    expected_terms);
    }

    // validate string field
    {
      auto terms = segment.field("doc_string");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features = kStringFieldFeatures;
      size_t frequency = 1;
      std::vector<uint32_t> position = {irs::pos_limits::min()};
      std::unordered_map<irs::bytes_view, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bytes_view>>
        expected_terms;

      expected_terms[irs::ViewCast<irs::byte_type>(
                       std::string_view("string1_data"))]
        .emplace(1);
      expected_terms[irs::ViewCast<irs::byte_type>(
                       std::string_view("string2_data"))]
        .emplace(2);

      ASSERT_EQ(2, docs_count(segment, "doc_string"));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ValidateTerms(segment, *terms, 2,
                    irs::ViewCast<irs::byte_type>(std::string_view(string1)),
                    irs::ViewCast<irs::byte_type>(std::string_view(string2)), 2,
                    features, expected_terms, &frequency, &position);
    }

    // validate text field
    {
      auto terms = segment.field("doc_text");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features = kTextFieldFeatures;
      size_t frequency = 1;
      std::vector<uint32_t> position = {irs::pos_limits::min()};
      std::unordered_map<irs::bytes_view, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bytes_view>>
        expected_terms;

      expected_terms[irs::ViewCast<irs::byte_type>(
                       std::string_view("text1_data"))]
        .emplace(1);
      expected_terms[irs::ViewCast<irs::byte_type>(
                       std::string_view("text2_data"))]
        .emplace(2);

      ASSERT_EQ(2, docs_count(segment, "doc_text"));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ValidateTerms(segment, *terms, 2,
                    irs::ViewCast<irs::byte_type>(std::string_view(text1)),
                    irs::ViewCast<irs::byte_type>(std::string_view(text2)), 2,
                    features, expected_terms, &frequency, &position);
    }
  }

  // validate initial data (segment 1)
  {
    auto& segment = reader[1];
    ASSERT_EQ(2, segment.docs_count());

    {
      auto fields = segment.fields();
      size_t size = 0;
      while (fields->next()) {
        ++size;
      }
      ASSERT_EQ(7, size);
    }

    // validate bytes field
    {
      auto terms = segment.field("doc_bytes");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features =
        tests::BinaryField().GetIndexFeatures() | irs::IndexFeatures::Norm;
      std::unordered_map<irs::bytes_view, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bytes_view>>
        expected_terms;
      expected_terms[irs::ViewCast<irs::byte_type>(
                       std::string_view("bytes3_data"))]
        .emplace(1);

      ASSERT_EQ(1, docs_count(segment, "doc_bytes"));
      ASSERT_TRUE(irs::field_limits::valid(field.norm));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ValidateTerms(segment, *terms, 1, bytes3, bytes3, 1, features,
                    expected_terms);

      {
        ASSERT_TRUE(irs::field_limits::valid(field.norm));

        const auto* cs = segment.CsReader();
        ASSERT_NE(nullptr, cs);
        const auto* column = cs->NormColumn(field.norm);
        ASSERT_NE(nullptr, column);

        const auto row = static_cast<uint64_t>(irs::doc_limits::min()) -
                         irs::doc_limits::min();
        ASSERT_EQ(3u, column->Get(row));
      }
    }

    // validate double field
    {
      auto terms = segment.field("doc_double");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features = tests::DoubleField().GetIndexFeatures();
      irs::NumericTokenizer max;
      max.reset((double_t)(2.718281828 * 3));
      irs::NumericTokenizer min;
      min.reset((double_t)(2.718281828 * 3));
      std::unordered_map<irs::bstring, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bstring>>
        expected_terms;

      {
        irs::NumericTokenizer itr;
        itr.reset((double_t)(2.718281828 * 3));
        for (; itr.next();
             expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
               .emplace(1))
          ;
      }

      ASSERT_EQ(1, docs_count(segment, "doc_double"));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ASSERT_TRUE(max.next() && max.next() && max.next() &&
                  max.next());  // skip to last value
      ASSERT_TRUE(min.next());  // skip to first value
      ValidateTerms(segment, *terms, 1, irs::get<irs::TermAttr>(min)->value,
                    irs::get<irs::TermAttr>(max)->value, 4, features,
                    expected_terms);
    }

    // validate float field
    {
      auto terms = segment.field("doc_float");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features = tests::FloatField().GetIndexFeatures();
      irs::NumericTokenizer max;
      max.reset((float_t)(3.1415926535 * 3));
      irs::NumericTokenizer min;
      min.reset((float_t)(3.1415926535 * 3));
      std::unordered_map<irs::bstring, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bstring>>
        expected_terms;

      {
        irs::NumericTokenizer itr;
        itr.reset((float_t)(3.1415926535 * 3));
        for (; itr.next();
             expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
               .emplace(1))
          ;
      }

      ASSERT_EQ(1, docs_count(segment, "doc_float"));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ASSERT_TRUE(max.next() && max.next());  // skip to last value
      ASSERT_TRUE(min.next());                // skip to first value
      ValidateTerms(segment, *terms, 1, irs::get<irs::TermAttr>(min)->value,
                    irs::get<irs::TermAttr>(max)->value, 2, features,
                    expected_terms);
    }

    // validate int field
    {
      auto terms = segment.field("doc_int");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features = tests::IntField().GetIndexFeatures();
      irs::NumericTokenizer max;
      max.reset(42 * 3);
      irs::NumericTokenizer min;
      min.reset(42 * 3);
      std::unordered_map<irs::bstring, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bstring>>
        expected_terms;

      {
        irs::NumericTokenizer itr;
        itr.reset(42 * 3);
        for (; itr.next();
             expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
               .emplace(1))
          ;
      }

      ASSERT_EQ(1, docs_count(segment, "doc_int"));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ASSERT_TRUE(max.next() && max.next());  // skip to last value
      ASSERT_TRUE(min.next());                // skip to first value
      ValidateTerms(segment, *terms, 1, irs::get<irs::TermAttr>(min)->value,
                    irs::get<irs::TermAttr>(max)->value, 2, features,
                    expected_terms);
    }

    // validate long field
    {
      auto terms = segment.field("doc_long");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features = tests::LongField().GetIndexFeatures();
      irs::NumericTokenizer max;
      max.reset((int64_t)12345 * 3);
      irs::NumericTokenizer min;
      min.reset((int64_t)12345 * 3);
      std::unordered_map<irs::bstring, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bstring>>
        expected_terms;

      {
        irs::NumericTokenizer itr;
        itr.reset((int64_t)12345 * 3);
        for (; itr.next();
             expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
               .emplace(1))
          ;
      }

      ASSERT_EQ(1, docs_count(segment, "doc_long"));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ASSERT_TRUE(max.next() && max.next() && max.next() &&
                  max.next());  // skip to last value
      ASSERT_TRUE(min.next());  // skip to first value
      ValidateTerms(segment, *terms, 1, irs::get<irs::TermAttr>(min)->value,
                    irs::get<irs::TermAttr>(max)->value, 4, features,
                    expected_terms);
    }

    // validate string field
    {
      auto terms = segment.field("doc_string");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features = kStringFieldFeatures;
      size_t frequency = 1;
      std::vector<uint32_t> position = {irs::pos_limits::min()};
      std::unordered_map<irs::bytes_view, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bytes_view>>
        expected_terms;

      expected_terms[irs::ViewCast<irs::byte_type>(
                       std::string_view("string3_data"))]
        .emplace(1);
      expected_terms[irs::ViewCast<irs::byte_type>(
        std::string_view("string4_data"))];

      ASSERT_EQ(2, docs_count(segment, "doc_string"));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ValidateTerms(segment, *terms, 2,
                    irs::ViewCast<irs::byte_type>(std::string_view(string3)),
                    irs::ViewCast<irs::byte_type>(std::string_view(string4)), 2,
                    features, expected_terms, &frequency, &position);
    }

    // validate text field
    {
      auto terms = segment.field("doc_text");
      ASSERT_NE(nullptr, terms);
      auto& field = terms->meta();
      auto features = kTextFieldFeatures;
      size_t frequency = 1;
      std::vector<uint32_t> position = {irs::pos_limits::min()};
      std::unordered_map<irs::bytes_view, std::unordered_set<irs::doc_id_t>,
                         absl::Hash<irs::bytes_view>>
        expected_terms;

      expected_terms[irs::ViewCast<irs::byte_type>(
                       std::string_view("text3_data"))]
        .emplace(1);

      ASSERT_EQ(1, docs_count(segment, "doc_text"));
      ASSERT_EQ(features, field.index_features);
      ASSERT_NE(nullptr, terms);
      ValidateTerms(segment, *terms, 1,
                    irs::ViewCast<irs::byte_type>(std::string_view(text3)),
                    irs::ViewCast<irs::byte_type>(std::string_view(text3)), 1,
                    features, expected_terms, &frequency, &position);
    }
  }

  irs::SegmentMeta index_segment;
  index_segment.codec = codec_ptr;

  const auto norm_column_options = irs::tests::MakeNormColumnOptionsProvider();
  const irs::SegmentWriterOptions options{
    .scorers_features = {},
    .db = &::sdb::DuckDBEngine::Instance().instance(),
    .norm_column_options = &norm_column_options,
  };
  irs::MergeWriter writer(dir, options);
  writer.Reset(reader.begin(), reader.end());
  ASSERT_TRUE(writer.Flush(index_segment));

  auto segment = irs::SegmentReaderImpl::Open(
    dir, index_segment, irs::tests::DefaultReaderOptions());

  ASSERT_EQ(3, segment->docs_count());  // doc4 removed during merge

  {
    auto fields = segment->fields();
    size_t size = 0;
    while (fields->next()) {
      ++size;
    }
    ASSERT_EQ(7, size);
  }

  // validate bytes field
  {
    auto terms = segment->field("doc_bytes");
    ASSERT_NE(nullptr, terms);
    auto& field = terms->meta();
    auto features =
      tests::BinaryField().GetIndexFeatures() | irs::IndexFeatures::Norm;
    std::unordered_map<irs::bytes_view, std::unordered_set<irs::doc_id_t>,
                       absl::Hash<irs::bytes_view>>
      expected_terms;

    expected_terms[irs::ViewCast<irs::byte_type>(
                     std::string_view("bytes1_data"))]
      .emplace(1);
    expected_terms[irs::ViewCast<irs::byte_type>(
                     std::string_view("bytes2_data"))]
      .emplace(2);
    expected_terms[irs::ViewCast<irs::byte_type>(
                     std::string_view("bytes3_data"))]
      .emplace(3);

    ASSERT_EQ(3, docs_count(*segment, "doc_bytes"));
    ASSERT_EQ(features, field.index_features);
    ASSERT_NE(nullptr, terms);
    ValidateTerms(*segment, *terms, 3, bytes1, bytes3, 3, features,
                  expected_terms);

    {
      ASSERT_TRUE(irs::field_limits::valid(field.norm));

      const auto* cs = segment->CsReader();
      ASSERT_NE(nullptr, cs);
      const auto* column = cs->NormColumn(field.norm);
      ASSERT_NE(nullptr, column);

      std::unordered_map<uint32_t, irs::doc_id_t> expected_values{
        {4, 1},  // norm value for 'doc_bytes' in 'doc1'
        {2, 2},  // norm value for 'doc_bytes' in 'doc2'
        {3, 3},  // norm value for 'doc_bytes' in 'doc3'
      };
      for (irs::doc_id_t doc = irs::doc_limits::min();
           doc < irs::doc_limits::min() + 3; ++doc) {
        const auto row = static_cast<uint64_t>(doc) - irs::doc_limits::min();
        const auto actual_value = column->Get(row);
        auto it = expected_values.find(actual_value);
        ASSERT_NE(expected_values.end(), it);
        ASSERT_EQ(doc, it->second);
        expected_values.erase(it);
      }
      ASSERT_TRUE(expected_values.empty());
    }
  }

  // validate double field
  {
    auto terms = segment->field("doc_double");
    ASSERT_NE(nullptr, terms);
    auto& field = terms->meta();
    auto features = tests::DoubleField().GetIndexFeatures();
    irs::NumericTokenizer max;
    max.reset((double_t)(2.718281828 * 3));
    irs::NumericTokenizer min;
    min.reset((double_t)(2.718281828 * 1));
    std::unordered_map<irs::bstring, std::unordered_set<irs::doc_id_t>,
                       absl::Hash<irs::bstring>>
      expected_terms;

    {
      irs::NumericTokenizer itr;
      itr.reset((double_t)(2.718281828 * 1));
      for (; itr.next();
           expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
             .emplace(1))
        ;
    }

    {
      irs::NumericTokenizer itr;
      itr.reset((double_t)(2.718281828 * 2));
      for (; itr.next();
           expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
             .emplace(2))
        ;
    }

    {
      irs::NumericTokenizer itr;
      itr.reset((double_t)(2.718281828 * 3));
      for (; itr.next();
           expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
             .emplace(3))
        ;
    }

    ASSERT_EQ(3, docs_count(*segment, "doc_double"));
    ASSERT_EQ(features, field.index_features);
    ASSERT_NE(nullptr, terms);
    ASSERT_TRUE(max.next() && max.next() && max.next() &&
                max.next());  // skip to last value
    ASSERT_TRUE(min.next());  // skip to first value
    ValidateTerms(*segment, *terms, 3, irs::get<irs::TermAttr>(min)->value,
                  irs::get<irs::TermAttr>(max)->value, 12, features,
                  expected_terms);
  }

  // validate float field
  {
    auto terms = segment->field("doc_float");
    ASSERT_NE(nullptr, terms);
    auto& field = terms->meta();
    auto features = tests::FloatField().GetIndexFeatures();
    irs::NumericTokenizer max;
    max.reset((float_t)(3.1415926535 * 3));
    irs::NumericTokenizer min;
    min.reset((float_t)(3.1415926535 * 1));
    std::unordered_map<irs::bstring, std::unordered_set<irs::doc_id_t>,
                       absl::Hash<irs::bstring>>
      expected_terms;

    {
      irs::NumericTokenizer itr;
      itr.reset((float_t)(3.1415926535 * 1));
      for (; itr.next();
           expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
             .emplace(1))
        ;
    }

    {
      irs::NumericTokenizer itr;
      itr.reset((float_t)(3.1415926535 * 2));
      for (; itr.next();
           expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
             .emplace(2))
        ;
    }

    {
      irs::NumericTokenizer itr;
      itr.reset((float_t)(3.1415926535 * 3));
      for (; itr.next();
           expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
             .emplace(3))
        ;
    }

    ASSERT_EQ(3, docs_count(*segment, "doc_float"));
    ASSERT_EQ(features, field.index_features);
    ASSERT_NE(nullptr, terms);
    ASSERT_TRUE(max.next() && max.next());  // skip to last value
    ASSERT_TRUE(min.next());                // skip to first value
    ValidateTerms(*segment, *terms, 3, irs::get<irs::TermAttr>(min)->value,
                  irs::get<irs::TermAttr>(max)->value, 6, features,
                  expected_terms);
  }

  // validate int field
  {
    auto terms = segment->field("doc_int");
    ASSERT_NE(nullptr, terms);
    auto& field = terms->meta();
    auto features = tests::IntField().GetIndexFeatures();
    irs::NumericTokenizer max;
    max.reset(42 * 3);
    irs::NumericTokenizer min;
    min.reset(42 * 1);
    std::unordered_map<irs::bstring, std::unordered_set<irs::doc_id_t>,
                       absl::Hash<irs::bstring>>
      expected_terms;

    {
      irs::NumericTokenizer itr;
      itr.reset(42 * 1);
      for (; itr.next();
           expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
             .emplace(1))
        ;
    }

    {
      irs::NumericTokenizer itr;
      itr.reset(42 * 2);
      for (; itr.next();
           expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
             .emplace(2))
        ;
    }

    {
      irs::NumericTokenizer itr;
      itr.reset(42 * 3);
      for (; itr.next();
           expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
             .emplace(3))
        ;
    }

    ASSERT_EQ(3, docs_count(*segment, "doc_int"));
    ASSERT_EQ(features, field.index_features);
    ASSERT_NE(nullptr, terms);
    ASSERT_TRUE(max.next() && max.next());  // skip to last value
    ASSERT_TRUE(min.next());                // skip to first value
    ValidateTerms(*segment, *terms, 3, irs::get<irs::TermAttr>(min)->value,
                  irs::get<irs::TermAttr>(max)->value, 4, features,
                  expected_terms);
  }

  // validate long field
  {
    auto terms = segment->field("doc_long");
    ASSERT_NE(nullptr, terms);
    auto& field = terms->meta();
    auto features = tests::LongField().GetIndexFeatures();
    irs::NumericTokenizer max;
    max.reset((int64_t)12345 * 3);
    irs::NumericTokenizer min;
    min.reset((int64_t)12345 * 1);
    std::unordered_map<irs::bstring, std::unordered_set<irs::doc_id_t>,
                       absl::Hash<irs::bstring>>
      expected_terms;

    {
      irs::NumericTokenizer itr;
      itr.reset((int64_t)12345 * 1);
      for (; itr.next();
           expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
             .emplace(1))
        ;
    }

    {
      irs::NumericTokenizer itr;
      itr.reset((int64_t)12345 * 2);
      for (; itr.next();
           expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
             .emplace(2))
        ;
    }

    {
      irs::NumericTokenizer itr;
      itr.reset((int64_t)12345 * 3);
      for (; itr.next();
           expected_terms[irs::bstring(irs::get<irs::TermAttr>(itr)->value)]
             .emplace(3))
        ;
    }

    ASSERT_EQ(3, docs_count(*segment, "doc_long"));
    ASSERT_EQ(features, field.index_features);
    ASSERT_NE(nullptr, terms);
    ASSERT_TRUE(max.next() && max.next() && max.next() &&
                max.next());  // skip to last value
    ASSERT_TRUE(min.next());  // skip to first value
    ValidateTerms(*segment, *terms, 3, irs::get<irs::TermAttr>(min)->value,
                  irs::get<irs::TermAttr>(max)->value, 6, features,
                  expected_terms);
  }

  // validate string field
  {
    auto terms = segment->field("doc_string");
    ASSERT_NE(nullptr, terms);
    auto& field = terms->meta();
    auto features = kStringFieldFeatures;
    size_t frequency = 1;
    std::vector<uint32_t> position = {irs::pos_limits::min()};
    std::unordered_map<irs::bytes_view, std::unordered_set<irs::doc_id_t>,
                       absl::Hash<irs::bytes_view>>
      expected_terms;

    expected_terms[irs::ViewCast<irs::byte_type>(
                     std::string_view("string1_data"))]
      .emplace(1);
    expected_terms[irs::ViewCast<irs::byte_type>(
                     std::string_view("string2_data"))]
      .emplace(2);
    expected_terms[irs::ViewCast<irs::byte_type>(
                     std::string_view("string3_data"))]
      .emplace(3);

    ASSERT_EQ(3, docs_count(*segment, "doc_string"));
    ASSERT_EQ(features, field.index_features);
    ASSERT_NE(nullptr, terms);
    ValidateTerms(*segment, *terms, 3,
                  irs::ViewCast<irs::byte_type>(std::string_view(string1)),
                  irs::ViewCast<irs::byte_type>(std::string_view(string3)), 3,
                  features, expected_terms, &frequency, &position);
  }

  // validate text field
  {
    auto terms = segment->field("doc_text");
    ASSERT_NE(nullptr, terms);
    auto& field = terms->meta();
    auto features = kTextFieldFeatures;
    size_t frequency = 1;
    std::vector<uint32_t> position = {irs::pos_limits::min()};
    std::unordered_map<irs::bytes_view, std::unordered_set<irs::doc_id_t>,
                       absl::Hash<irs::bytes_view>>
      expected_terms;

    expected_terms[irs::ViewCast<irs::byte_type>(
                     std::string_view("text1_data"))]
      .emplace(1);
    expected_terms[irs::ViewCast<irs::byte_type>(
                     std::string_view("text2_data"))]
      .emplace(2);
    expected_terms[irs::ViewCast<irs::byte_type>(
                     std::string_view("text3_data"))]
      .emplace(3);

    ASSERT_EQ(3, docs_count(*segment, "doc_text"));
    ASSERT_EQ(features, field.index_features);
    ASSERT_NE(nullptr, terms);
    ValidateTerms(*segment, *terms, 3,
                  irs::ViewCast<irs::byte_type>(std::string_view(text1)),
                  irs::ViewCast<irs::byte_type>(std::string_view(text3)), 3,
                  features, expected_terms, &frequency, &position);
  }
}

// Two segments carrying three cs columns (`doc_string`, `doc_int`,
// `doc_text`) over 4 docs total; the merge must keep all 4 docs and
// re-expose each column under a stable id. Per-doc payloads are
// distinct across the entire merged segment so we can cross-check
// rows by exact value, not just non-empty payload bytes. Mirrors the
// legacy multi-column scenario: pre-merge per-segment iteration of
// every column, post-merge iteration of the merged column once via
// VisitBlobColumn (the "cached" walk) and again via per-doc
// BlobPointReader (the "uncached"/random-access walk).
TEST_P(MergeWriterTestCase, test_merge_writer_columns) {
  auto codec_ptr = Codec();
  ASSERT_NE(nullptr, codec_ptr);
  constexpr irs::field_id kStringId = 1;   // doc_string
  constexpr irs::field_id kIntId = 2;      // doc_int
  constexpr irs::field_id kTextId = 3;     // doc_text -- per-doc unique
  constexpr irs::field_id kUnusedId = 99;  // never registered
  irs::MemoryDirectory dir;

  constexpr std::string_view kStrings[4] = {"string1_data", "string2_data",
                                            "string3_data", "string4_data"};
  constexpr int32_t kInts[4] = {42, 84, 126, 168};
  // Longer per-doc-distinct payloads to exercise variable-length blobs
  // across the merge -- not just short fixed-size rows. Each string is
  // unique across the whole merged segment so we can confirm row
  // ordering didn't get scrambled across sources.
  const std::array<std::string, 4> kTexts = {
    std::string{"text_payload_doc1_"} + std::string(64, 'a'),
    std::string{"text_payload_doc2_"} + std::string(96, 'b'),
    std::string{"text_payload_doc3_"} + std::string(128, 'c'),
    std::string{"text_payload_doc4_"} + std::string(160, 'd'),
  };

  // Insert 4 docs across 2 segments. Each doc writes the same 3 columns
  // under the same field_ids -- crucial: this is the cross-segment
  // id-stability invariant the merge depends on.
  {
    auto writer = irs::IndexWriter::Make(dir, codec_ptr, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    {
      auto seg = writer->GetBatch();
      for (size_t i = 0; i < 2; ++i) {
        auto doc = seg.Insert();
        const tests::StringField s{"doc_string", std::string{kStrings[i]}};
        doc.Insert(s);
        irs::tests::StoreFieldAt(*doc.Columnstore(), kStringId, doc.DocId(), s);
        const tests::IntField n{"doc_int", kInts[i]};
        doc.Insert(n);
        irs::tests::StoreFieldAt(*doc.Columnstore(), kIntId, doc.DocId(), n);
        const tests::StringField t{"doc_text", kTexts[i]};
        doc.Insert(t);
        irs::tests::StoreFieldAt(*doc.Columnstore(), kTextId, doc.DocId(), t);
      }
    }
    writer->RefreshCommit();
    {
      auto seg = writer->GetBatch();
      for (size_t i = 2; i < 4; ++i) {
        auto doc = seg.Insert();
        const tests::StringField s{"doc_string", std::string{kStrings[i]}};
        doc.Insert(s);
        irs::tests::StoreFieldAt(*doc.Columnstore(), kStringId, doc.DocId(), s);
        const tests::IntField n{"doc_int", kInts[i]};
        doc.Insert(n);
        irs::tests::StoreFieldAt(*doc.Columnstore(), kIntId, doc.DocId(), n);
        const tests::StringField t{"doc_text", kTexts[i]};
        doc.Insert(t);
        irs::tests::StoreFieldAt(*doc.Columnstore(), kTextId, doc.DocId(), t);
      }
    }
    writer->RefreshCommit();
  }

  // Pre-merge source-segment verification: each of the two segments
  // carries exactly 2 docs and all three columns. Read every cell
  // (per-segment local doc ids start at doc_limits::min()=1) and
  // cross-check the payload against what was written.
  {
    auto reader =
      irs::DirectoryReader(dir, codec_ptr, irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());
    for (size_t s = 0; s < reader.size(); ++s) {
      SCOPED_TRACE(testing::Message("source segment ") << s);
      auto& segment = reader[s];
      ASSERT_EQ(2, segment.docs_count());
      ASSERT_EQ(2, segment.live_docs_count());

      // Each segment must expose all three known column ids.
      const auto* str_col = segment.Column(kStringId);
      const auto* int_col = segment.Column(kIntId);
      const auto* text_col = segment.Column(kTextId);
      ASSERT_NE(nullptr, str_col);
      ASSERT_NE(nullptr, int_col);
      ASSERT_NE(nullptr, text_col);
      // Repeated lookups for the same id return the same column instance.
      ASSERT_EQ(str_col, segment.Column(kStringId));
      ASSERT_EQ(int_col, segment.Column(kIntId));
      ASSERT_EQ(text_col, segment.Column(kTextId));
      // No accidental extra column at an unrelated id.
      ASSERT_EQ(nullptr, segment.Column(kUnusedId));

      // Walk every doc in this segment with all three columns at once
      // -- mirrors the legacy "iterate over column" sub-scenarios.
      irs::tests::BlobPointReader str_reader{segment, *str_col};
      irs::tests::BlobPointReader int_reader{segment, *int_col};
      irs::tests::BlobPointReader text_reader{segment, *text_col};

      for (size_t local = 0; local < 2; ++local) {
        const auto doc =
          static_cast<irs::doc_id_t>(local + irs::doc_limits::min());
        const size_t global = s * 2 + local;
        SCOPED_TRACE(testing::Message("doc ") << doc << " global=" << global);

        // doc_string blob: WriteStr layout (vread<uint32_t> length, then
        // raw bytes).
        const auto sb = str_reader.Get(doc);
        ASSERT_FALSE(sb.empty());
        {
          const auto* p = sb.data();
          const auto len = irs::vread<uint32_t>(p);
          EXPECT_EQ(kStrings[global],
                    (std::string_view{reinterpret_cast<const char*>(p), len}));
        }

        // doc_int blob: WriteZV32 layout. Decode via BytesViewInput.
        const auto ib = int_reader.Get(doc);
        ASSERT_FALSE(ib.empty());
        {
          irs::BytesViewInput in{ib};
          EXPECT_EQ(kInts[global], irs::ReadZV32(in));
        }

        // doc_text blob: WriteStr layout.
        const auto tb = text_reader.Get(doc);
        ASSERT_FALSE(tb.empty());
        {
          const auto* p = tb.data();
          const auto len = irs::vread<uint32_t>(p);
          EXPECT_EQ(kTexts[global],
                    (std::string_view{reinterpret_cast<const char*>(p), len}));
        }
      }
    }
  }

  // Merge both segments into a single one.
  {
    auto writer = irs::IndexWriter::Make(dir, codec_ptr, irs::kOmAppend,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_TRUE(writer->Compact(
      irs::index_utils::MakePolicy(irs::index_utils::CompactionCount{})));
    ASSERT_TRUE(writer->RefreshCommit());
  }

  auto merged_reader =
    irs::DirectoryReader(dir, codec_ptr, irs::tests::DefaultReaderOptions());
  ASSERT_EQ(1, merged_reader.size());
  auto& merged = merged_reader[0];
  ASSERT_EQ(4, merged.docs_count());
  ASSERT_EQ(4, merged.live_docs_count());

  // All three columns must survive the merge under the same ids. Cache
  // the pointers up front so we can also verify cross-segment id
  // stability: each lookup is idempotent and returns the SAME instance.
  const auto* str_col = merged.Column(kStringId);
  const auto* int_col = merged.Column(kIntId);
  const auto* text_col = merged.Column(kTextId);
  ASSERT_NE(nullptr, str_col);
  ASSERT_NE(nullptr, int_col);
  ASSERT_NE(nullptr, text_col);
  ASSERT_EQ(str_col, merged.Column(kStringId));
  ASSERT_EQ(int_col, merged.Column(kIntId));
  ASSERT_EQ(text_col, merged.Column(kTextId));

  // No accidental extra column slots after the merge.
  ASSERT_EQ(nullptr, merged.Column(kUnusedId));
  ASSERT_EQ(nullptr, merged.Column(/*another-unused=*/77));

  // Pass 1 -- iterate over the merged column using VisitBlobColumn
  // (the "cached" / streaming walk in legacy terms). Collect rows into
  // an unordered_map keyed by exact payload so we can confirm every
  // doc shows up exactly once and the ordering is well-defined.
  {
    std::unordered_map<std::string, irs::doc_id_t> seen_strings;
    const bool ok = irs::tests::VisitBlobColumn(
      *merged.CsReader(), *str_col,
      [&](irs::doc_id_t doc, irs::bytes_view payload) {
        const auto* p = payload.data();
        const auto len = irs::vread<uint32_t>(p);
        std::string v(reinterpret_cast<const char*>(p), len);
        return seen_strings.emplace(std::move(v), doc).second;
      });
    EXPECT_TRUE(ok);
    EXPECT_EQ(4u, seen_strings.size());
    for (const auto& s : kStrings) {
      auto it = seen_strings.find(std::string{s});
      EXPECT_NE(seen_strings.end(), it) << s;
    }

    std::unordered_map<int32_t, irs::doc_id_t> seen_ints;
    const bool ok_int = irs::tests::VisitBlobColumn(
      *merged.CsReader(), *int_col,
      [&](irs::doc_id_t doc, irs::bytes_view payload) {
        irs::BytesViewInput in{payload};
        return seen_ints.emplace(irs::ReadZV32(in), doc).second;
      });
    EXPECT_TRUE(ok_int);
    EXPECT_EQ(4u, seen_ints.size());
    for (const auto v : kInts) {
      auto it = seen_ints.find(v);
      EXPECT_NE(seen_ints.end(), it) << v;
    }

    std::unordered_map<std::string, irs::doc_id_t> seen_texts;
    const bool ok_text = irs::tests::VisitBlobColumn(
      *merged.CsReader(), *text_col,
      [&](irs::doc_id_t doc, irs::bytes_view payload) {
        const auto* p = payload.data();
        const auto len = irs::vread<uint32_t>(p);
        std::string v(reinterpret_cast<const char*>(p), len);
        return seen_texts.emplace(std::move(v), doc).second;
      });
    EXPECT_TRUE(ok_text);
    EXPECT_EQ(4u, seen_texts.size());
    for (const auto& s : kTexts) {
      auto it = seen_texts.find(s);
      EXPECT_NE(seen_texts.end(), it) << s;
    }

    // Cross-column doc consistency: each doc id must report the same
    // (segment-global) payload under all three columns. We don't
    // assume merge order, so resolve doc -> source index by string
    // first.
    for (const auto& [s, doc] : seen_strings) {
      const auto it = std::find(std::begin(kStrings), std::end(kStrings), s);
      ASSERT_NE(std::end(kStrings), it);
      const size_t src = std::distance(std::begin(kStrings), it);
      auto ii = std::find_if(seen_ints.begin(), seen_ints.end(),
                             [&](const auto& kv) { return kv.second == doc; });
      ASSERT_NE(seen_ints.end(), ii);
      EXPECT_EQ(kInts[src], ii->first);
      auto ti = std::find_if(seen_texts.begin(), seen_texts.end(),
                             [&](const auto& kv) { return kv.second == doc; });
      ASSERT_NE(seen_texts.end(), ti);
      EXPECT_EQ(kTexts[src], ti->first);
    }
  }

  // Pass 2 -- walk every doc with per-doc BlobPointReader. This is the
  // random-access ("not cached") read path; the column must return the
  // same payload for the same doc id as we just saw in Pass 1.
  {
    irs::tests::BlobPointReader str_reader{merged, *str_col};
    irs::tests::BlobPointReader int_reader{merged, *int_col};
    irs::tests::BlobPointReader text_reader{merged, *text_col};

    std::set<std::string> walked_strings;
    std::set<int32_t> walked_ints;
    std::set<std::string> walked_texts;
    for (size_t i = 0; i < 4; ++i) {
      const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
      SCOPED_TRACE(testing::Message("merged doc ") << doc);

      ASSERT_FALSE(str_reader.IsNull(doc));
      ASSERT_FALSE(int_reader.IsNull(doc));
      ASSERT_FALSE(text_reader.IsNull(doc));

      const auto sb = str_reader.Get(doc);
      ASSERT_FALSE(sb.empty());
      {
        const auto* p = sb.data();
        const auto len = irs::vread<uint32_t>(p);
        walked_strings.emplace(reinterpret_cast<const char*>(p), len);
      }

      const auto ib = int_reader.Get(doc);
      ASSERT_FALSE(ib.empty());
      {
        irs::BytesViewInput in{ib};
        walked_ints.emplace(irs::ReadZV32(in));
      }

      const auto tb = text_reader.Get(doc);
      ASSERT_FALSE(tb.empty());
      {
        const auto* p = tb.data();
        const auto len = irs::vread<uint32_t>(p);
        walked_texts.emplace(reinterpret_cast<const char*>(p), len);
      }
    }
    EXPECT_EQ(4u, walked_strings.size());
    EXPECT_EQ(4u, walked_ints.size());
    EXPECT_EQ(4u, walked_texts.size());
    for (const auto& s : kStrings) {
      EXPECT_TRUE(walked_strings.contains(std::string{s})) << s;
    }
    for (const auto v : kInts) {
      EXPECT_TRUE(walked_ints.contains(v)) << v;
    }
    for (const auto& s : kTexts) {
      EXPECT_TRUE(walked_texts.contains(s)) << s;
    }
  }

  // Post-merge wrong-column lookups: not just kUnusedId but a few
  // adjacent-ish unused ids -- the column directory must be exact.
  EXPECT_EQ(nullptr, merged.Column(kUnusedId));
  EXPECT_EQ(nullptr, merged.Column(/*just-past-text=*/4));
  EXPECT_EQ(nullptr, merged.Column(/*far-out=*/1024));
}

// Three docs share doc_int + doc_string columns; the fourth doc has an
// exclusive `another_column`. Removing doc4 before compaction must
// either drop `another_column` from the merged segment or keep the
// slot with all-null rows -- in any case every surviving live doc must
// continue to read its doc_string + doc_int payload intact. Mirrors
// the legacy "remove kills a column / surviving columns intact"
// scenario, with two follow-up sub-scenarios: a per-segment pre-merge
// walk of every (kStringId, kIntId) row, and a separate
// fresh-directory case where the removed doc does NOT drop any column
// (so 2 live docs survive and all 3 columns stay present).
TEST_P(MergeWriterTestCase, test_merge_writer_columns_remove) {
  auto codec_ptr = Codec();
  ASSERT_NE(nullptr, codec_ptr);
  constexpr irs::field_id kStringId = 1;
  constexpr irs::field_id kIntId = 2;
  constexpr irs::field_id kAnotherId = 3;
  constexpr irs::field_id kUnusedId = 99;
  irs::MemoryDirectory dir;

  constexpr std::string_view kStrings[4] = {"string1_data", "string2_data",
                                            "string3_data", "string4_data"};
  // Indexed by (doc - 1). The fourth doc (idx 3) has no int payload --
  // it carries another_column instead, which is what the remove drops.
  constexpr int32_t kInts[4] = {1 * 42, 2 * 42, 3 * 42, 0};

  {
    auto writer = irs::IndexWriter::Make(dir, codec_ptr, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    // Segment 0: doc1 + doc3 -- both have string + int (legacy layout).
    {
      auto seg = writer->GetBatch();
      for (const size_t i : {0u, 2u}) {
        auto doc = seg.Insert();
        const tests::StringField s{"doc_string", std::string{kStrings[i]}};
        doc.Insert(s);
        irs::tests::StoreFieldAt(*doc.Columnstore(), kStringId, doc.DocId(), s);
        const tests::IntField n{"doc_int", kInts[i]};
        doc.Insert(n);
        irs::tests::StoreFieldAt(*doc.Columnstore(), kIntId, doc.DocId(), n);
      }
    }
    writer->RefreshCommit();
    // Segment 1: doc2 (string + int) and doc4 (string + another_column).
    {
      auto seg = writer->GetBatch();
      {
        auto doc = seg.Insert();
        const tests::StringField s{"doc_string", std::string{kStrings[1]}};
        doc.Insert(s);
        irs::tests::StoreFieldAt(*doc.Columnstore(), kStringId, doc.DocId(), s);
        const tests::IntField n{"doc_int", kInts[1]};
        doc.Insert(n);
        irs::tests::StoreFieldAt(*doc.Columnstore(), kIntId, doc.DocId(), n);
      }
      {
        auto doc = seg.Insert();
        const tests::StringField s{"doc_string", std::string{kStrings[3]}};
        doc.Insert(s);
        irs::tests::StoreFieldAt(*doc.Columnstore(), kStringId, doc.DocId(), s);
        const tests::StringField a{"another_column", "another_value"};
        doc.Insert(a);
        irs::tests::StoreFieldAt(*doc.Columnstore(), kAnotherId, doc.DocId(),
                                 a);
      }
    }
    writer->RefreshCommit();
    writer->GetBatch().Remove(MakeByTerm("doc_string", "string4_data"));
    writer->RefreshCommit();
  }

  // Pre-merge inspection. Track which segment doc3 ended up in -- the
  // batching order above puts {doc1, doc3} into segment 0, and
  // {doc2, doc4} into segment 1, but pin those expectations by reading
  // doc_string from each segment.
  {
    auto reader =
      irs::DirectoryReader(dir, codec_ptr, irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(2, reader[0].docs_count());
    ASSERT_EQ(2, reader[0].live_docs_count());
    ASSERT_EQ(2, reader[1].docs_count());
    ASSERT_EQ(1, reader[1].live_docs_count());  // doc4 removed

    // Segment 0: string + int present, another_column absent.
    {
      auto& segment = reader[0];
      const auto* str_col = segment.Column(kStringId);
      const auto* int_col = segment.Column(kIntId);
      ASSERT_NE(nullptr, str_col);
      ASSERT_NE(nullptr, int_col);
      EXPECT_EQ(nullptr, segment.Column(kAnotherId));
      EXPECT_EQ(nullptr, segment.Column(kUnusedId));

      // Expected per local doc id: doc1 -> string1, doc2(local) -> doc3.
      const std::unordered_map<std::string, int32_t> expected{
        {std::string{kStrings[0]}, kInts[0]},
        {std::string{kStrings[2]}, kInts[2]},
      };

      irs::tests::BlobPointReader sr{segment, *str_col};
      irs::tests::BlobPointReader ir{segment, *int_col};
      std::unordered_map<std::string, int32_t> seen;
      for (size_t i = 0; i < 2; ++i) {
        const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
        const auto sb = sr.Get(doc);
        const auto* p = sb.data();
        const auto len = irs::vread<uint32_t>(p);
        std::string s(reinterpret_cast<const char*>(p), len);

        const auto ib = ir.Get(doc);
        ASSERT_FALSE(ib.empty());
        irs::BytesViewInput in{ib};
        const auto v = irs::ReadZV32(in);

        EXPECT_EQ(expected.count(s), 1u);
        EXPECT_EQ(expected.at(s), v);
        seen.emplace(std::move(s), v);
      }
      EXPECT_EQ(expected, seen);
    }

    // Segment 1: string + int present (live doc2), another_column also
    // present (but its only contributor, doc4, was removed). Probing
    // doc4 via the live-docs mask must skip it.
    {
      auto& segment = reader[1];
      const auto* str_col = segment.Column(kStringId);
      const auto* int_col = segment.Column(kIntId);
      const auto* another_col = segment.Column(kAnotherId);
      ASSERT_NE(nullptr, str_col);
      ASSERT_NE(nullptr, int_col);
      ASSERT_NE(nullptr, another_col);
      EXPECT_EQ(nullptr, segment.Column(kUnusedId));

      // doc4's columns are still on disk; the live-docs mask hides it.
      // Use docs_iterator() to confirm only one local doc id is live.
      std::vector<irs::doc_id_t> live;
      for (auto it = segment.docs_iterator(); it->next();) {
        live.push_back(it->value());
      }
      ASSERT_EQ(1u, live.size());
      const auto live_doc = live.front();

      irs::tests::BlobPointReader sr{segment, *str_col};
      irs::tests::BlobPointReader ir{segment, *int_col};
      const auto sb = sr.Get(live_doc);
      const auto* p = sb.data();
      const auto len = irs::vread<uint32_t>(p);
      const std::string_view got{reinterpret_cast<const char*>(p), len};
      EXPECT_EQ(kStrings[1], got);
      const auto ib = ir.Get(live_doc);
      ASSERT_FALSE(ib.empty());
      irs::BytesViewInput in{ib};
      EXPECT_EQ(kInts[1], irs::ReadZV32(in));
    }
  }

  // Compact to a single segment.
  {
    auto writer = irs::IndexWriter::Make(dir, codec_ptr, irs::kOmAppend,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_TRUE(writer->Compact(
      irs::index_utils::MakePolicy(irs::index_utils::CompactionCount{})));
    ASSERT_TRUE(writer->RefreshCommit());
  }

  auto merged_reader =
    irs::DirectoryReader(dir, codec_ptr, irs::tests::DefaultReaderOptions());
  ASSERT_EQ(1, merged_reader.size());
  auto& merged = merged_reader[0];
  ASSERT_EQ(3, merged.docs_count());
  ASSERT_EQ(3, merged.live_docs_count());

  // doc_string + doc_int survive across all 3 live docs with their
  // original payloads intact. Walk the merged segment doc-by-doc and
  // cross-check (string, int) pairs by exact value -- order is
  // implementation-defined, so we collect into an unordered_map and
  // compare to the live-docs-only expectation.
  const std::unordered_map<std::string, int32_t> expected_live{
    {std::string{kStrings[0]}, kInts[0]},
    {std::string{kStrings[1]}, kInts[1]},
    {std::string{kStrings[2]}, kInts[2]},
  };

  const auto* str_col = merged.Column(kStringId);
  const auto* int_col = merged.Column(kIntId);
  ASSERT_NE(nullptr, str_col);
  ASSERT_NE(nullptr, int_col);

  {
    irs::tests::BlobPointReader sr{merged, *str_col};
    irs::tests::BlobPointReader ir{merged, *int_col};
    std::unordered_map<std::string, int32_t> seen;
    for (size_t i = 0; i < 3; ++i) {
      const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
      SCOPED_TRACE(testing::Message("merged doc ") << doc);

      ASSERT_FALSE(sr.IsNull(doc));
      ASSERT_FALSE(ir.IsNull(doc));
      const auto sb = sr.Get(doc);
      const auto* p = sb.data();
      const auto len = irs::vread<uint32_t>(p);
      std::string s(reinterpret_cast<const char*>(p), len);

      const auto ib = ir.Get(doc);
      irs::BytesViewInput in{ib};
      const auto v = irs::ReadZV32(in);

      seen.emplace(std::move(s), v);
    }
    EXPECT_EQ(expected_live, seen);
    EXPECT_FALSE(seen.contains(std::string{kStrings[3]}));
  }

  // Also verify the same expectation via VisitBlobColumn -- the
  // legacy "iterate over column (cached)" path. The visitor must see
  // exactly three rows.
  {
    std::unordered_map<std::string, irs::doc_id_t> visited_strings;
    const bool ok = irs::tests::VisitBlobColumn(
      *merged.CsReader(), *str_col,
      [&](irs::doc_id_t doc, irs::bytes_view payload) {
        const auto* p = payload.data();
        const auto len = irs::vread<uint32_t>(p);
        std::string v(reinterpret_cast<const char*>(p), len);
        return visited_strings.emplace(std::move(v), doc).second;
      });
    EXPECT_TRUE(ok);
    EXPECT_EQ(3u, visited_strings.size());
    EXPECT_TRUE(visited_strings.contains(std::string{kStrings[0]}));
    EXPECT_TRUE(visited_strings.contains(std::string{kStrings[1]}));
    EXPECT_TRUE(visited_strings.contains(std::string{kStrings[2]}));
    EXPECT_FALSE(visited_strings.contains(std::string{kStrings[3]}));

    std::unordered_map<int32_t, irs::doc_id_t> visited_ints;
    const bool ok_int = irs::tests::VisitBlobColumn(
      *merged.CsReader(), *int_col,
      [&](irs::doc_id_t doc, irs::bytes_view payload) {
        irs::BytesViewInput in{payload};
        return visited_ints.emplace(irs::ReadZV32(in), doc).second;
      });
    EXPECT_TRUE(ok_int);
    EXPECT_EQ(3u, visited_ints.size());
    EXPECT_TRUE(visited_ints.contains(kInts[0]));
    EXPECT_TRUE(visited_ints.contains(kInts[1]));
    EXPECT_TRUE(visited_ints.contains(kInts[2]));
  }

  // `another_column` may survive as a column slot inherited from segment 1
  // (the new cs's merge does not GC columns whose only source doc was
  // removed) -- but every surviving doc must read null for it, and a
  // VisitBlobColumn walk must never invoke the visitor.
  if (const auto* another = merged.Column(kAnotherId); another != nullptr) {
    irs::tests::BlobPointReader vals{merged, *another};
    for (size_t i = 0; i < 3; ++i) {
      const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
      EXPECT_TRUE(vals.IsNull(doc)) << "doc=" << doc;
    }
    size_t calls = 0;
    const bool ok = irs::tests::VisitBlobColumn(
      *merged.CsReader(), *another, [&](irs::doc_id_t, irs::bytes_view) {
        ++calls;
        return true;
      });
    EXPECT_TRUE(ok);
    EXPECT_EQ(0u, calls);
  }

  // No accidental extra column at unrelated ids.
  EXPECT_EQ(nullptr, merged.Column(kUnusedId));
  EXPECT_EQ(nullptr, merged.Column(/*far-out=*/2048));

  // Repeated Column(id) returns the same instance for the surviving
  // columns -- cross-segment id-stability invariant the merge preserves.
  EXPECT_EQ(str_col, merged.Column(kStringId));
  EXPECT_EQ(int_col, merged.Column(kIntId));

  // Sub-scenario: remove a doc whose deletion does NOT drop any
  // column entirely. Use a fresh directory + 4 docs where every doc
  // carries all 3 columns; remove doc2; expect 3 live docs post-merge
  // and all three column slots still present + iterable.
  {
    irs::MemoryDirectory dir2;
    {
      auto writer = irs::IndexWriter::Make(dir2, codec_ptr, irs::kOmCreate,
                                           irs::tests::DefaultWriterOptions());
      // Segment 0: doc1, doc2 -- both carry all 3 columns.
      {
        auto seg = writer->GetBatch();
        for (size_t i = 0; i < 2; ++i) {
          auto d = seg.Insert();
          const tests::StringField s{"doc_string", std::string{kStrings[i]}};
          d.Insert(s);
          irs::tests::StoreFieldAt(*d.Columnstore(), kStringId, d.DocId(), s);
          const tests::IntField n{"doc_int", kInts[i]};
          d.Insert(n);
          irs::tests::StoreFieldAt(*d.Columnstore(), kIntId, d.DocId(), n);
          const tests::StringField a{"another_column",
                                     "shared_value_" + std::to_string(i)};
          d.Insert(a);
          irs::tests::StoreFieldAt(*d.Columnstore(), kAnotherId, d.DocId(), a);
        }
      }
      writer->RefreshCommit();
      // Segment 1: doc3, doc4 -- same shape.
      {
        auto seg = writer->GetBatch();
        for (size_t i = 2; i < 4; ++i) {
          auto d = seg.Insert();
          const tests::StringField s{"doc_string", std::string{kStrings[i]}};
          d.Insert(s);
          irs::tests::StoreFieldAt(*d.Columnstore(), kStringId, d.DocId(), s);
          const tests::IntField n{"doc_int", i == 3 ? 4 * 42 : kInts[i]};
          d.Insert(n);
          irs::tests::StoreFieldAt(*d.Columnstore(), kIntId, d.DocId(), n);
          const tests::StringField a{"another_column",
                                     "shared_value_" + std::to_string(i)};
          d.Insert(a);
          irs::tests::StoreFieldAt(*d.Columnstore(), kAnotherId, d.DocId(), a);
        }
      }
      writer->RefreshCommit();
      // Remove doc2 -- still leaves the other two columns (doc_int,
      // another_column) populated in every remaining live doc.
      writer->GetBatch().Remove(MakeByTerm("doc_string", "string2_data"));
      writer->RefreshCommit();
    }

    // Compact.
    {
      auto writer = irs::IndexWriter::Make(dir2, codec_ptr, irs::kOmAppend,
                                           irs::tests::DefaultWriterOptions());
      ASSERT_TRUE(writer->Compact(
        irs::index_utils::MakePolicy(irs::index_utils::CompactionCount{})));
      ASSERT_TRUE(writer->RefreshCommit());
    }

    auto reader2 =
      irs::DirectoryReader(dir2, codec_ptr, irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader2.size());
    auto& merged2 = reader2[0];
    ASSERT_EQ(3, merged2.live_docs_count());

    const auto* sc = merged2.Column(kStringId);
    const auto* ic = merged2.Column(kIntId);
    const auto* ac = merged2.Column(kAnotherId);
    ASSERT_NE(nullptr, sc);
    ASSERT_NE(nullptr, ic);
    ASSERT_NE(nullptr, ac);

    // doc2 dropped -- but doc1, doc3, doc4 must all be readable on
    // every column. Track per-string the (int, another) so we can
    // confirm both surviving columns still align row-for-row.
    irs::tests::BlobPointReader sr{merged2, *sc};
    irs::tests::BlobPointReader ir{merged2, *ic};
    irs::tests::BlobPointReader ar{merged2, *ac};
    std::set<std::string> strings;
    std::set<int32_t> ints;
    std::set<std::string> anothers;
    for (size_t i = 0; i < merged2.docs_count(); ++i) {
      const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
      if (sr.IsNull(doc)) {
        // The killed doc is still on disk; the live-docs filter is the
        // segment-level mask. Skip it for this aggregation.
        EXPECT_TRUE(ir.IsNull(doc));
        EXPECT_TRUE(ar.IsNull(doc));
        continue;
      }
      const auto sb = sr.Get(doc);
      const auto* p = sb.data();
      const auto len = irs::vread<uint32_t>(p);
      strings.emplace(reinterpret_cast<const char*>(p), len);

      const auto ib = ir.Get(doc);
      irs::BytesViewInput iin{ib};
      ints.emplace(irs::ReadZV32(iin));

      const auto ab = ar.Get(doc);
      const auto* ap = ab.data();
      const auto alen = irs::vread<uint32_t>(ap);
      anothers.emplace(reinterpret_cast<const char*>(ap), alen);
    }
    EXPECT_EQ(3u, strings.size());
    EXPECT_TRUE(strings.contains("string1_data"));
    EXPECT_FALSE(strings.contains("string2_data"));
    EXPECT_TRUE(strings.contains("string3_data"));
    EXPECT_TRUE(strings.contains("string4_data"));
    EXPECT_EQ(3u, ints.size());
    EXPECT_TRUE(ints.contains(1 * 42));
    EXPECT_FALSE(ints.contains(2 * 42));
    EXPECT_TRUE(ints.contains(3 * 42));
    EXPECT_TRUE(ints.contains(4 * 42));
    EXPECT_EQ(3u, anothers.size());
    EXPECT_TRUE(anothers.contains("shared_value_0"));
    EXPECT_FALSE(anothers.contains("shared_value_1"));
    EXPECT_TRUE(anothers.contains("shared_value_2"));
    EXPECT_TRUE(anothers.contains("shared_value_3"));
  }
}

TEST_P(MergeWriterTestCase, test_merge_writer_sorted) {
  GTEST_SKIP() << "sorted-index merge path not supported on new cs";
}

INSTANTIATE_TEST_SUITE_P(
  merge_writer_test, MergeWriterTestCase,
  ::testing::Combine(
    ::testing::Values(&tests::Directory<&tests::MemoryDirectory>),
    ::testing::Values("1_5simd")),
  &MergeWriterTestCase::to_string);
