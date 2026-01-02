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

#include "index_tests.hpp"

#include <absl/random/random.h>
#include <faiss/utils/distances.h>

#include <iresearch/index/field_meta.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/index/norm.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/store/fs_directory.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/delta_compression.hpp>
#include <iresearch/utils/fstext/fst_table_matcher.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <iresearch/utils/lz4compression.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <iresearch/utils/wildcard_utils.hpp>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "basics/file_utils_ext.hpp"
#include "tests_shared.hpp"

using namespace std::literals;

namespace {

bool Visit(const irs::ColumnReader& reader,
           const std::function<bool(irs::doc_id_t, irs::bytes_view)>& visitor) {
  auto it = reader.iterator(irs::ColumnHint::Consolidation);

  irs::PayAttr dummy;
  auto* doc = irs::get<irs::DocAttr>(*it);
  if (!doc) {
    return false;
  }
  auto* payload = irs::get<irs::PayAttr>(*it);
  if (!payload) {
    payload = &dummy;
  }

  while (it->next()) {
    if (!visitor(doc->value, payload->value)) {
      return false;
    }
  }

  return true;
}

irs::Filter::ptr MakeByTerm(std::string_view name, std::string_view value) {
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field() = name;
  filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(value);
  return filter;
}

irs::Filter::ptr MakeByTermOrByTerm(std::string_view name0,
                                    std::string_view value0,
                                    std::string_view name1,
                                    std::string_view value1) {
  auto filter = std::make_unique<irs::Or>();
  filter->add<irs::ByTerm>() =
    std::move(static_cast<irs::ByTerm&>(*MakeByTerm(name0, value0)));
  filter->add<irs::ByTerm>() =
    std::move(static_cast<irs::ByTerm&>(*MakeByTerm(name1, value1)));
  return filter;
}

irs::Filter::ptr MakeOr(
  const std::vector<std::pair<std::string_view, std::string_view>>& parts) {
  auto filter = std::make_unique<irs::Or>();
  for (const auto& [name, value] : parts) {
    filter->add<irs::ByTerm>() =
      std::move(static_cast<irs::ByTerm&>(*MakeByTerm(name, value)));
  }
  return filter;
}

class SubReaderMock final : public irs::SubReader {
 public:
  uint64_t CountMappedMemory() const final { return 0; }

  const irs::SegmentInfo& Meta() const final { return _meta; }

  // Live & deleted docs

  const irs::DocumentMask* docs_mask() const final { return nullptr; }

  // Returns an iterator over live documents in current segment.
  irs::DocIterator::ptr docs_iterator() const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

  irs::DocIterator::ptr mask(irs::DocIterator::ptr&& it) const final {
    EXPECT_FALSE(true);
    return std::move(it);
  }

  // Inverted index

  irs::FieldIterator::ptr fields() const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

  // Returns corresponding term_reader by the specified field name.
  const irs::TermReader* field(std::string_view) const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

  // Columnstore

  irs::ColumnIterator::ptr columns() const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

  const irs::ColumnReader* column(irs::field_id) const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

  const irs::ColumnReader* column(std::string_view) const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

  const irs::ColumnReader* sort() const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

 private:
  irs::SegmentInfo _meta;
};

}  // namespace

namespace tests {

void AssertSnapshotEquality(irs::DirectoryReader lhs,
                            irs::DirectoryReader rhs) {
  ASSERT_EQ(lhs.size(), rhs.size());
  ASSERT_EQ(lhs.docs_count(), rhs.docs_count());
  ASSERT_EQ(lhs.live_docs_count(), rhs.live_docs_count());
  ASSERT_EQ(lhs.Meta(), rhs.Meta());
  auto rhs_segment = rhs.begin();
  for (auto& lhs_segment : lhs.GetImpl()->GetReaders()) {
    ASSERT_EQ(lhs_segment.size(), rhs_segment->size());
    ASSERT_EQ(lhs_segment.docs_count(), rhs_segment->docs_count());
    ASSERT_EQ(lhs_segment.live_docs_count(), rhs_segment->live_docs_count());
    ASSERT_EQ(lhs_segment.Meta(), rhs_segment->Meta());
    ASSERT_TRUE(!lhs_segment.docs_mask() && !rhs_segment->docs_mask() ||
                (lhs_segment.docs_mask() && rhs_segment->docs_mask() &&
                 *lhs_segment->docs_mask() == *rhs_segment->docs_mask()));
    ++rhs_segment;
  }
}

struct IncompatibleAttribute : irs::Attribute {};

REGISTER_ATTRIBUTE(IncompatibleAttribute);

std::string IndexTestBase::to_string(
  const testing::TestParamInfo<index_test_context>& info) {
  auto [factory, codec] = info.param;

  std::string str = (*factory)(nullptr).second;
  if (codec.codec) {
    str += "___";
    str += codec.codec;
  }

  return str;
}

std::shared_ptr<irs::Directory> IndexTestBase::get_directory(
  const TestBase& ctx) const {
  dir_param_f factory;
  std::tie(factory, std::ignore) = GetParam();

  return (*factory)(&ctx).first;
}

irs::Format::ptr IndexTestBase::get_codec() const {
  tests::FormatInfo info;
  std::tie(std::ignore, info) = GetParam();

  return irs::formats::Get(info.codec);
}

irs::doc_id_t IndexTestBase::GetPostingsBlockSize() const {
  if (get_codec()->type()().name().contains("avx")) {
    return 256;
  }
  return 128;
}

void IndexTestBase::AssertSnapshotEquality(const irs::IndexWriter& writer) {
  tests::AssertSnapshotEquality(writer.GetSnapshot(), open_reader());
}

void IndexTestBase::write_segment(irs::IndexWriter& writer,
                                  tests::IndexSegment& segment,
                                  tests::DocGeneratorBase& gen) {
  // add segment
  const Document* src;

  while ((src = gen.next())) {
    segment.insert(*src);

    ASSERT_TRUE(Insert(writer, src->indexed.begin(), src->indexed.end(),
                       src->stored.begin(), src->stored.end(), src->sorted));
  }

  if (writer.Comparator()) {
    segment.sort(*writer.Comparator());
  }
}

void IndexTestBase::write_segment_batched(irs::IndexWriter& writer,
                                          tests::IndexSegment& segment,
                                          tests::DocGeneratorBase& gen,
                                          size_t batch_size) {
  ASSERT_TRUE(InsertBatch(writer, gen, segment, batch_size));

  if (writer.Comparator()) {
    segment.sort(*writer.Comparator());
  }
}

void IndexTestBase::add_segment(irs::IndexWriter& writer,
                                tests::DocGeneratorBase& gen) {
  _index.emplace_back(writer.FeatureInfo());
  write_segment(writer, _index.back(), gen);
  writer.Commit();
}

void IndexTestBase::add_segments(irs::IndexWriter& writer,
                                 std::vector<DocGeneratorBase::ptr>& gens) {
  for (auto& gen : gens) {
    _index.emplace_back(writer.FeatureInfo());
    write_segment(writer, _index.back(), *gen);
  }
  writer.Commit();
}

void IndexTestBase::add_segment(tests::DocGeneratorBase& gen,
                                irs::OpenMode mode /*= irs::kOmCreate*/,
                                const irs::IndexWriterOptions& opts /*= {}*/) {
  auto writer = open_writer(mode, opts);
  add_segment(*writer, gen);
}

void IndexTestBase::add_segment_batched(
  tests::DocGeneratorBase& gen, size_t batch_size,
  irs::OpenMode mode /*= irs::kOmCreate*/,
  const irs::IndexWriterOptions& opts /*= {}*/) {
  auto writer = open_writer(mode, opts);
  _index.emplace_back(writer->FeatureInfo());
  write_segment_batched(*writer, _index.back(), gen, batch_size);
  writer->Commit();
}

}  // namespace tests

class IndexTestCase : public tests::IndexTestBase {
 public:
  static irs::FeatureInfoProvider FeaturesWithNorms() {
    return [](irs::IndexFeatures id) {
      const irs::ColumnInfo info{irs::Type<irs::compression::Lz4>::get(),
                                 {},

                                 false};

      if (irs::IndexFeatures::Norm == id) {
        return std::make_pair(info, &irs::Norm::MakeWriter);
      }

      return std::make_pair(info, irs::FeatureWriterFactory{});
    };
  }

  void assert_index(size_t skip = 0,
                    irs::automaton_table_matcher* matcher = nullptr) const {
    // index_test_base::assert_index(irs::IndexFeatures::None, skip, matcher);
    IndexTestBase::assert_index(irs::IndexFeatures::Freq, skip, matcher);
    IndexTestBase::assert_index(
      irs::IndexFeatures::Freq | irs::IndexFeatures::Pos, skip, matcher);
    IndexTestBase::assert_index(irs::IndexFeatures::Freq |
                                  irs::IndexFeatures::Pos |
                                  irs::IndexFeatures::Offs,
                                skip, matcher);
    IndexTestBase::assert_index(irs::IndexFeatures::Freq |
                                  irs::IndexFeatures::Pos |
                                  irs::IndexFeatures::Pay,
                                skip, matcher);
    IndexTestBase::assert_index(
      irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
        irs::IndexFeatures::Offs | irs::IndexFeatures::Pay,
      skip, matcher);
  }

  void ClearWriter() {
    tests::JsonDocGenerator gen(
      resource("simple_sequential.json"),
      [](tests::Document& doc, const std::string& name,
         const tests::JsonDocGenerator::JsonValue& data) {
        if (data.is_string()) {
          doc.insert(std::make_shared<tests::StringField>(name, data.str));
        }
      });

    const tests::Document* doc1 = gen.next();
    const tests::Document* doc2 = gen.next();
    const tests::Document* doc3 = gen.next();
    const tests::Document* doc4 = gen.next();
    const tests::Document* doc5 = gen.next();
    const tests::Document* doc6 = gen.next();

    // test import/insert/deletes/existing all empty after clear
    {
      irs::MemoryDirectory data_dir;
      auto writer = open_writer();

      writer->Commit();
      AssertSnapshotEquality(*writer);  // create initial empty segment

      // populate 'import' dir
      {
        auto data_writer =
          irs::IndexWriter::Make(data_dir, codec(), irs::kOmCreate);
        ASSERT_TRUE(Insert(*data_writer, doc1->indexed.begin(),
                           doc1->indexed.end(), doc1->stored.begin(),
                           doc1->stored.end()));
        ASSERT_TRUE(Insert(*data_writer, doc2->indexed.begin(),
                           doc2->indexed.end(), doc2->stored.begin(),
                           doc2->stored.end()));
        ASSERT_TRUE(Insert(*data_writer, doc3->indexed.begin(),
                           doc3->indexed.end(), doc3->stored.begin(),
                           doc3->stored.end()));
        data_writer->Commit();

        auto reader = irs::DirectoryReader(data_dir);
        ASSERT_EQ(1, reader.size());
        ASSERT_EQ(3, reader.docs_count());
        ASSERT_EQ(3, reader.live_docs_count());
      }

      {
        auto reader = irs::DirectoryReader(dir(), codec());
        ASSERT_EQ(0, reader.size());
        ASSERT_EQ(0, reader.docs_count());
        ASSERT_EQ(0, reader.live_docs_count());
      }

      // add sealed segment
      {
        ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                           doc4->stored.begin(), doc4->stored.end()));
        ASSERT_TRUE(Insert(*writer, doc5->indexed.begin(), doc5->indexed.end(),
                           doc5->stored.begin(), doc5->stored.end()));
        writer->Commit();
        AssertSnapshotEquality(*writer);
      }

      {
        auto reader = irs::DirectoryReader(dir(), codec());
        ASSERT_EQ(1, reader.size());
        ASSERT_EQ(2, reader.docs_count());
        ASSERT_EQ(2, reader.live_docs_count());
      }

      // add insert/remove/import
      {
        auto query_doc4 = MakeByTerm("name", "D");
        auto reader = irs::DirectoryReader(data_dir);

        ASSERT_TRUE(Insert(*writer, doc6->indexed.begin(), doc6->indexed.end(),
                           doc6->stored.begin(), doc6->stored.end()));
        writer->GetBatch().Remove(std::move(query_doc4));
        ASSERT_TRUE(writer->Import(irs::DirectoryReader(data_dir)));
      }

      size_t file_count = 0;

      {
        dir().visit([&file_count](std::string_view) -> bool {
          ++file_count;
          return true;
        });
      }

      writer->Clear();

      // should be empty after clear
      {
        auto reader = irs::DirectoryReader(dir(), codec());
        ASSERT_EQ(0, reader.size());
        ASSERT_EQ(0, reader.docs_count());
        ASSERT_EQ(0, reader.live_docs_count());
        size_t file_count_post_clear = 0;
        dir().visit([&file_count_post_clear](std::string_view) -> bool {
          ++file_count_post_clear;
          return true;
        });
        ASSERT_EQ(
          file_count + 1,
          file_count_post_clear);  // +1 extra file for new empty index meta
      }

      writer->Commit();
      AssertSnapshotEquality(*writer);

      // should be empty after commit (no new files or uncomited changes)
      {
        auto reader = irs::DirectoryReader(dir(), codec());
        ASSERT_EQ(0, reader.size());
        ASSERT_EQ(0, reader.docs_count());
        ASSERT_EQ(0, reader.live_docs_count());
        size_t file_count_post_commit = 0;
        dir().visit([&file_count_post_commit](std::string_view) -> bool {
          ++file_count_post_commit;
          return true;
        });
        ASSERT_EQ(
          file_count + 1,
          file_count_post_commit);  // +1 extra file for new empty index meta
      }
    }

    // test creation of an empty writer
    {
      irs::MemoryDirectory dir;
      auto writer = irs::IndexWriter::Make(dir, codec(), irs::kOmCreate);
      ASSERT_THROW(irs::DirectoryReader{dir},
                   irs::IndexNotFound);  // throws due to missing index

      {
        size_t file_count = 0;

        dir.visit([&file_count](std::string_view) -> bool {
          ++file_count;
          return true;
        });
        ASSERT_EQ(0, file_count);  // empty dierctory
      }

      writer->Clear();

      {
        size_t file_count = 0;

        dir.visit([&file_count](std::string_view) -> bool {
          ++file_count;
          return true;
        });
        ASSERT_EQ(1, file_count);  // +1 file for new empty index meta
      }

      auto reader = irs::DirectoryReader(dir);
      ASSERT_EQ(0, reader.size());
      ASSERT_EQ(0, reader.docs_count());
      ASSERT_EQ(0, reader.live_docs_count());
    }

    // ensue double clear does not increment meta
    {
      auto writer = open_writer();

      ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                         doc1->stored.begin(), doc1->stored.end()));
      writer->Commit();
      AssertSnapshotEquality(*writer);

      size_t file_count0 = 0;
      dir().visit([&file_count0](std::string_view) -> bool {
        ++file_count0;
        return true;
      });

      writer->Clear();

      size_t file_count1 = 0;
      dir().visit([&file_count1](std::string_view) -> bool {
        ++file_count1;
        return true;
      });
      ASSERT_EQ(file_count0 + 1,
                file_count1);  // +1 extra file for new empty index meta

      writer->Clear();

      size_t file_count2 = 0;
      dir().visit([&file_count2](std::string_view) -> bool {
        ++file_count2;
        return true;
      });
      ASSERT_EQ(file_count1, file_count2);
    }
  }

  void ConcurrentReadIndex() {
    // write test docs
    {
      tests::JsonDocGenerator gen(
        resource("simple_single_column_multi_term.json"),
        &tests::PayloadedJsonFieldFactory);
      add_segment(gen);
    }

    auto& expected_index = index();
    auto actual_reader = irs::DirectoryReader(dir(), codec());
    ASSERT_FALSE(!actual_reader);
    ASSERT_EQ(1, actual_reader.size());
    ASSERT_EQ(expected_index.size(), actual_reader.size());

    size_t thread_count = 16;                         // arbitrary value > 1
    std::vector<const tests::Field*> expected_terms;  // used to validate terms
                                                      // used to validate docs
    std::vector<irs::SeekTermIterator::ptr> expected_term_itrs;

    auto& actual_segment = actual_reader[0];
    auto actual_terms = actual_segment.field("name_anl_pay");
    ASSERT_FALSE(!actual_terms);

    for (size_t i = 0; i < thread_count; ++i) {
      auto field = expected_index[0].fields().find("name_anl_pay");
      ASSERT_NE(expected_index[0].fields().end(), field);
      expected_terms.emplace_back(&field->second);
      ASSERT_TRUE(nullptr != expected_terms.back());
      expected_term_itrs.emplace_back(expected_terms.back()->iterator());
      ASSERT_FALSE(!expected_term_itrs.back());
    }

    std::mutex mutex;

    // validate terms async
    {
      irs::async_utils::ThreadPool<> pool(thread_count);

      {
        std::lock_guard lock(mutex);

        for (size_t i = 0; i < thread_count; ++i) {
          auto& act_terms = actual_terms;
          auto& exp_terms = expected_terms[i];

          pool.run([&mutex, &act_terms, &exp_terms]() -> void {
            {
              // wait for all threads to be registered
              std::lock_guard lock(mutex);
            }

            auto act_term_itr = act_terms->iterator(irs::SeekMode::NORMAL);
            auto exp_terms_itr = exp_terms->iterator();
            ASSERT_FALSE(!act_term_itr);
            ASSERT_FALSE(!exp_terms_itr);

            while (act_term_itr->next()) {
              ASSERT_TRUE(exp_terms_itr->next());
              ASSERT_EQ(exp_terms_itr->value(), act_term_itr->value());
            }

            ASSERT_FALSE(exp_terms_itr->next());
          });
        }
      }

      pool.stop();
    }

    // validate docs async
    {
      auto actual_term_itr = actual_terms->iterator(irs::SeekMode::NORMAL);

      while (actual_term_itr->next()) {
        for (size_t i = 0; i < thread_count; ++i) {
          ASSERT_TRUE(expected_term_itrs[i]->next());
          ASSERT_EQ(expected_term_itrs[i]->value(), actual_term_itr->value());
        }

        irs::async_utils::ThreadPool<> pool(thread_count);

        {
          std::lock_guard lock(mutex);

          for (size_t i = 0; i < thread_count; ++i) {
            auto& act_term_itr = actual_term_itr;
            auto& exp_term_itr = expected_term_itrs[i];

            pool.run([&mutex, &act_term_itr, &exp_term_itr]() -> void {
              constexpr irs::IndexFeatures kFeatures =
                irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
                irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;
              irs::DocIterator::ptr act_docs_itr;
              irs::DocIterator::ptr exp_docs_itr;

              {
                // wait for all threads to be registered
                std::lock_guard lock(mutex);

                // iterators are not thread-safe
                act_docs_itr = act_term_itr->postings(
                  kFeatures);  // this step creates 3 internal iterators
                exp_docs_itr = exp_term_itr->postings(
                  kFeatures);  // this step creates 3 internal iterators
              }

              // FIXME
              //               auto& actual_attrs = act_docs_itr->attributes();
              //               auto& expected_attrs =
              //               exp_docs_itr->attributes();
              //               ASSERT_EQ(expected_attrs.features(),
              //               actual_attrs.features());

              auto* actual_freq = irs::get<irs::FreqAttr>(*act_docs_itr);
              auto* expected_freq = irs::get<irs::FreqAttr>(*exp_docs_itr);
              ASSERT_FALSE(!actual_freq);
              ASSERT_FALSE(!expected_freq);

              // FIXME const_cast
              auto* actual_pos = const_cast<irs::PosAttr*>(
                irs::get<irs::PosAttr>(*act_docs_itr));
              auto* expected_pos = const_cast<irs::PosAttr*>(
                irs::get<irs::PosAttr>(*exp_docs_itr));
              ASSERT_FALSE(!actual_pos);
              ASSERT_FALSE(!expected_pos);

              while (act_docs_itr->next()) {
                ASSERT_TRUE(exp_docs_itr->next());
                ASSERT_EQ(exp_docs_itr->value(), act_docs_itr->value());
                ASSERT_EQ(expected_freq->value, actual_freq->value);

                auto* actual_offs = irs::get<irs::OffsAttr>(*actual_pos);
                auto* expected_offs = irs::get<irs::OffsAttr>(*expected_pos);
                ASSERT_FALSE(!actual_offs);
                ASSERT_FALSE(!expected_offs);

                auto* actual_pay = irs::get<irs::PayAttr>(*actual_pos);
                auto* expected_pay = irs::get<irs::PayAttr>(*expected_pos);
                ASSERT_FALSE(!actual_pay);
                ASSERT_FALSE(!expected_pay);

                while (actual_pos->next()) {
                  ASSERT_TRUE(expected_pos->next());
                  ASSERT_EQ(expected_pos->value(), actual_pos->value());
                  ASSERT_EQ(expected_offs->start, actual_offs->start);
                  ASSERT_EQ(expected_offs->end, actual_offs->end);
                  ASSERT_EQ(expected_pay->value, actual_pay->value);
                }

                ASSERT_FALSE(expected_pos->next());
              }

              ASSERT_FALSE(exp_docs_itr->next());
            });
          }
        }

        pool.stop();
      }

      for (size_t i = 0; i < thread_count; ++i) {
        ASSERT_FALSE(expected_term_itrs[i]->next());
      }
    }
  }

  void OpenWriterCheckLock() {
    {
      // open writer
      auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate);
      ASSERT_NE(nullptr, writer);
      // can't open another writer at the same time on the same directory
      ASSERT_THROW(irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate),
                   irs::LockObtainFailed);
      ASSERT_EQ(0, writer->BufferedDocs());
    }

    {
      // open writer
      auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate);
      ASSERT_NE(nullptr, writer);

      writer->Commit();
      AssertSnapshotEquality(*writer);
      irs::DirectoryCleaner::clean(dir());
      // can't open another writer at the same time on the same directory
      ASSERT_THROW(irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate),
                   irs::LockObtainFailed);
      ASSERT_EQ(0, writer->BufferedDocs());
    }

    {
      // open writer
      auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate);
      ASSERT_NE(nullptr, writer);

      ASSERT_EQ(0, writer->BufferedDocs());
    }

    {
      // open writer with NOLOCK hint
      irs::IndexWriterOptions options0;
      options0.lock_repository = false;
      auto writer0 =
        irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate, options0);
      ASSERT_NE(nullptr, writer0);

      // can open another writer at the same time on the same directory
      irs::IndexWriterOptions options1;
      options1.lock_repository = false;
      auto writer1 =
        irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate, options1);
      ASSERT_NE(nullptr, writer1);

      ASSERT_EQ(0, writer0->BufferedDocs());
      ASSERT_EQ(0, writer1->BufferedDocs());
    }

    {
      // open writer with NOLOCK hint
      irs::IndexWriterOptions options0;
      options0.lock_repository = false;
      auto writer0 =
        irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate, options0);
      ASSERT_NE(nullptr, writer0);

      // can open another writer at the same time on the same directory and
      // acquire lock
      auto writer1 =
        irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate | irs::kOmAppend);
      ASSERT_NE(nullptr, writer1);

      ASSERT_EQ(0, writer0->BufferedDocs());
      ASSERT_EQ(0, writer1->BufferedDocs());
    }

    {
      // open writer with NOLOCK hint
      irs::IndexWriterOptions options0;
      options0.lock_repository = false;
      auto writer0 =
        irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate, options0);
      ASSERT_NE(nullptr, writer0);
      writer0->Commit();

      // can open another writer at the same time on the same directory and
      // acquire lock
      auto writer1 = irs::IndexWriter::Make(dir(), codec(), irs::kOmAppend);
      ASSERT_NE(nullptr, writer1);

      ASSERT_EQ(0, writer0->BufferedDocs());
      ASSERT_EQ(0, writer1->BufferedDocs());
    }
  }

  void WriterCheckOpenModes() {
    // APPEND to nonexisting index, shoud fail
    ASSERT_THROW(irs::IndexWriter::Make(dir(), codec(), irs::kOmAppend),
                 irs::FileNotFound);
    // read index in empty directory, should fail
    ASSERT_THROW((irs::DirectoryReader{dir(), codec()}), irs::IndexNotFound);

    // create empty index
    {
      auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate);

      writer->Commit();
      AssertSnapshotEquality(*writer);
    }

    // read empty index, it should not fail
    {
      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(0, reader.live_docs_count());
      ASSERT_EQ(0, reader.docs_count());
      ASSERT_EQ(0, reader.size());
      ASSERT_EQ(reader.begin(), reader.end());
    }

    // append to index
    {
      auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmAppend);
      tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                  &tests::GenericJsonFieldFactory);
      const tests::Document* doc1 = gen.next();
      ASSERT_EQ(0, writer->BufferedDocs());
      ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                         doc1->stored.begin(), doc1->stored.end()));
      ASSERT_EQ(1, writer->BufferedDocs());
      writer->Commit();
      AssertSnapshotEquality(*writer);
      ASSERT_EQ(0, writer->BufferedDocs());
    }

    // read index, it should not fail
    {
      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(1, reader.live_docs_count());
      ASSERT_EQ(1, reader.docs_count());
      ASSERT_EQ(1, reader.size());
      ASSERT_NE(reader.begin(), reader.end());
    }

    // append to index
    {
      auto writer =
        irs::IndexWriter::Make(dir(), codec(), irs::kOmAppend | irs::kOmCreate);
      tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                  &tests::GenericJsonFieldFactory);
      const tests::Document* doc1 = gen.next();
      ASSERT_EQ(0, writer->BufferedDocs());
      ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                         doc1->stored.begin(), doc1->stored.end()));
      ASSERT_EQ(1, writer->BufferedDocs());
      writer->Commit();
      AssertSnapshotEquality(*writer);
      ASSERT_EQ(0, writer->BufferedDocs());
    }

    // read index, it should not fail
    {
      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(2, reader.live_docs_count());
      ASSERT_EQ(2, reader.docs_count());
      ASSERT_EQ(2, reader.size());
      ASSERT_NE(reader.begin(), reader.end());
    }
  }

  void WriterTransactionIsolation() {
    tests::JsonDocGenerator gen(
      resource("simple_sequential.json"),
      [](tests::Document& doc, const std::string& name,
         const tests::JsonDocGenerator::JsonValue& data) {
        if (tests::JsonDocGenerator::ValueType::STRING == data.vt) {
          doc.insert(std::make_shared<tests::StringField>(name, data.str));
        }
      });
    const tests::Document* doc1 = gen.next();
    const tests::Document* doc2 = gen.next();

    auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_EQ(1, writer->BufferedDocs());
    writer->Begin();  // start transaction #1
    ASSERT_EQ(0, writer->BufferedDocs());
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(),
                       doc2->stored.end()));  // add another document while
                                              // transaction in opened
    ASSERT_EQ(1, writer->BufferedDocs());
    writer->Commit();
    AssertSnapshotEquality(*writer);       // finish transaction #1
    ASSERT_EQ(1, writer->BufferedDocs());  // still have 1 buffered document
                                           // not included into transaction #1

    // check index, 1 document in 1 segment
    {
      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(1, reader.live_docs_count());
      ASSERT_EQ(1, reader.docs_count());
      ASSERT_EQ(1, reader.size());
      ASSERT_NE(reader.begin(), reader.end());
    }

    writer->Commit();
    AssertSnapshotEquality(*writer);  // transaction #2
    ASSERT_EQ(0, writer->BufferedDocs());
    // check index, 2 documents in 2 segments
    {
      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(2, reader.live_docs_count());
      ASSERT_EQ(2, reader.docs_count());
      ASSERT_EQ(2, reader.size());
      ASSERT_NE(reader.begin(), reader.end());
    }

    // check documents
    {
      auto reader = irs::DirectoryReader(dir(), codec());

      // segment #1
      {
        auto& segment = reader[0];
        const auto* column = segment.column("name");
        ASSERT_NE(nullptr, column);
        auto values = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, values);
        auto* actual_value = irs::get<irs::PayAttr>(*values);
        ASSERT_NE(nullptr, actual_value);
        auto terms = segment.field("same");
        ASSERT_NE(nullptr, terms);
        auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
        ASSERT_TRUE(term_itr->next());
        auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("A",
                  irs::ToString<std::string_view>(actual_value->value.data()));
        ASSERT_FALSE(docs_itr->next());
      }

      // segment #1
      {
        auto& segment = reader[1];
        auto* column = segment.column("name");
        ASSERT_NE(nullptr, column);
        auto values = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, values);
        auto* actual_value = irs::get<irs::PayAttr>(*values);
        ASSERT_NE(nullptr, actual_value);
        auto terms = segment.field("same");
        ASSERT_NE(nullptr, terms);
        auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
        ASSERT_TRUE(term_itr->next());
        auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("B",
                  irs::ToString<std::string_view>(actual_value->value.data()));
        ASSERT_FALSE(docs_itr->next());
      }
    }
  }

  void WriterBeginRollback() {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);

    const tests::Document* doc1 = gen.next();
    const tests::Document* doc2 = gen.next();
    const tests::Document* doc3 = gen.next();

    {
      auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate);

      ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                         doc1->stored.begin(), doc1->stored.end()));
      writer->Rollback();  // does nothing
      ASSERT_EQ(1, writer->BufferedDocs());
      ASSERT_TRUE(writer->Begin());
      ASSERT_FALSE(writer->Begin());  // try to begin already opened transaction

      // index still does not exist
      ASSERT_THROW((irs::DirectoryReader{dir(), codec()}), irs::IndexNotFound);

      writer->Rollback();  // rollback transaction
      writer->Rollback();  // does nothing
      ASSERT_EQ(0, writer->BufferedDocs());

      writer->Commit();
      AssertSnapshotEquality(*writer);  // commit

      // check index, it should be empty
      {
        auto reader = irs::DirectoryReader(dir(), codec());
        ASSERT_EQ(0, reader.live_docs_count());
        ASSERT_EQ(0, reader.docs_count());
        ASSERT_EQ(0, reader.size());
        ASSERT_EQ(reader.begin(), reader.end());
      }
    }

    // test rolled-back index can still be opened after directory cleaner run
    {
      auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate);
      ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                         doc2->stored.begin(), doc2->stored.end()));
      ASSERT_TRUE(writer->Begin());  // prepare for commit tx #1
      writer->Commit();
      AssertSnapshotEquality(*writer);  // commit tx #1
      auto file_count = 0;
      auto dir_visitor = [&file_count](std::string_view) -> bool {
        ++file_count;
        return true;
      };
      // clear any unused files before taking count
      irs::directory_utils::RemoveAllUnreferenced(dir());
      dir().visit(dir_visitor);
      auto file_count_before = file_count;
      ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                         doc3->stored.begin(), doc3->stored.end()));
      ASSERT_TRUE(writer->Begin());  // prepare for commit tx #2
      writer->Rollback();            // rollback tx #2
      irs::directory_utils::RemoveAllUnreferenced(dir());
      file_count = 0;
      dir().visit(dir_visitor);
      ASSERT_EQ(file_count_before,
                file_count);  // ensure rolled back file refs were released

      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(1, reader.size());
      auto& segment = reader[0];  // assume 0 is id of first/only segment
      auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }
  }

  void WriterBatchWithErrorRollback() {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);

    struct InvalidTokenizer final : public irs::Tokenizer {
      irs::Attribute* GetMutable(irs::TypeInfo::type_id) noexcept final {
        return nullptr;
      }
      bool next() final { return false; }
    };
    // Field can be stored but can not be indexed due to lack of attributes in
    // tokenizer
    struct FieldT {
      std::string_view Name() const { return "test_field"; }

      irs::IndexFeatures GetIndexFeatures() const {
        return irs::IndexFeatures::None;
      }

      irs::Tokenizer& GetTokens() { return token_stream; }

      bool Write(irs::DataOutput& out) const {
        irs::WriteStr(out, Name());
        return true;
      }

      InvalidTokenizer token_stream;
    } invalid_field;

    const tests::Document* doc1 = gen.next();
    const tests::Document* doc2 = gen.next();
    const tests::Document* doc3 = gen.next();
    auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate);
    {
      auto ctx = writer->GetBatch();
      auto doc = ctx.Insert(false, 3);

      ASSERT_TRUE(doc.template Insert<irs::Action::STORE>(doc1->stored.begin(),
                                                          doc1->stored.end()));
      doc.NextDocument();
      ASSERT_TRUE(doc.template Insert<irs::Action::STORE>(doc2->stored.begin(),
                                                          doc2->stored.end()));
      doc.NextDocument();

      ASSERT_TRUE(doc.template Insert<irs::Action::STORE>(invalid_field));
      doc.NextDocument();
      doc.NextFieldBatch();

      ASSERT_TRUE(doc.template Insert<irs::Action::INDEX>(doc1->stored.begin(),
                                                          doc1->stored.end()));
      doc.NextDocument();
      ASSERT_TRUE(doc.template Insert<irs::Action::INDEX>(doc2->stored.begin(),
                                                          doc2->stored.end()));
      doc.NextDocument();

      ASSERT_FALSE(doc.template Insert<irs::Action::INDEX>(invalid_field));
      ASSERT_FALSE(doc);
      // entire batch should be rollbacked
    }
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    writer->Commit();
    // should be only one live doc - doc3
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    ASSERT_EQ(1, segment.live_docs_count());
    auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    // skip docs deleted during batch rollback
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("C", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc3
    ASSERT_FALSE(docs_itr->next());
  }

  void ConcurrentReadSingleColumnSmoke() {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    std::vector<const tests::Document*> expected_docs;

    // write some data into columnstore
    auto writer = open_writer();
    for (auto* doc = gen.next(); doc; doc = gen.next()) {
      ASSERT_TRUE(Insert(*writer, doc->indexed.end(), doc->indexed.end(),
                         doc->stored.begin(), doc->stored.end()));
      expected_docs.push_back(doc);
    }
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = open_reader();

    // 1-st iteration: noncached
    // 2-nd iteration: cached
    for (size_t i = 0; i < 2; ++i) {
      auto read_columns = [&expected_docs, &reader]() {
        size_t i = 0;
        for (auto& segment : reader) {
          auto* column = segment.column("name");
          if (!column) {
            return false;
          }
          auto values = column->iterator(irs::ColumnHint::Normal);
          EXPECT_NE(nullptr, values);
          auto* actual_value = irs::get<irs::PayAttr>(*values);
          EXPECT_NE(nullptr, actual_value);
          for (irs::doc_id_t doc = (irs::doc_limits::min)(),
                             max = segment.docs_count();
               doc <= max; ++doc) {
            if (doc != values->seek(doc)) {
              return false;
            }

            auto* expected_doc = expected_docs[i];
            auto expected_name =
              expected_doc->stored.get<tests::StringField>("name")->value();
            if (expected_name !=
                irs::ToString<std::string_view>(actual_value->value.data())) {
              return false;
            }

            ++i;
          }
        }

        return true;
      };

      std::mutex mutex;
      bool ready = false;
      std::condition_variable ready_cv;

      auto wait_for_all = [&mutex, &ready, &ready_cv]() {
        // wait for all threads to be registered
        std::unique_lock lock(mutex);
        while (!ready) {
          ready_cv.wait(lock);
        }
      };

      const auto thread_count = 10;
      std::vector<int> results(thread_count, 0);
      std::vector<std::thread> pool;

      for (size_t i = 0; i < thread_count; ++i) {
        auto& result = results[i];
        pool.emplace_back(
          std::thread([&wait_for_all, &result, &read_columns]() {
            wait_for_all();
            result = static_cast<int>(read_columns());
          }));
      }

      // all threads registered... go, go, go...
      {
        std::lock_guard lock{mutex};
        ready = true;
        ready_cv.notify_all();
      }

      for (auto& thread : pool) {
        thread.join();
      }

      ASSERT_TRUE(std::all_of(results.begin(), results.end(),
                              [](int res) { return 1 == res; }));
    }
  }

  void ConcurrentReadMultipleColumns() {
    struct CsvDocTemplateT : public tests::CsvDocGenerator::DocTemplate {
      void init() final {
        clear();
        reserve(2);
        insert(std::make_shared<tests::StringField>("id"));
        insert(std::make_shared<tests::StringField>("label"));
      }

      void value(size_t idx, const std::string_view& value) final {
        switch (idx) {
          case 0:
            indexed.get<tests::StringField>("id")->value(value);
            break;
          case 1:
            indexed.get<tests::StringField>("label")->value(value);
        }
      }
    };

    // write columns
    {
      CsvDocTemplateT csv_doc_template;
      tests::CsvDocGenerator gen(resource("simple_two_column.csv"),
                                 csv_doc_template);
      auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate);

      const tests::Document* doc;
      while ((doc = gen.next())) {
        ASSERT_TRUE(Insert(*writer, doc->indexed.end(), doc->indexed.end(),
                           doc->stored.begin(), doc->stored.end()));
      }
      writer->Commit();
      AssertSnapshotEquality(*writer);
    }

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = *(reader.begin());

    // read columns
    {
      auto visit_column = [&segment](const std::string_view& column_name) {
        auto* meta = segment.column(column_name);
        if (!meta) {
          return false;
        }

        irs::doc_id_t expected_id = 0;
        CsvDocTemplateT csv_doc_template;
        tests::CsvDocGenerator gen(resource("simple_two_column.csv"),
                                   csv_doc_template);
        auto visitor = [&gen, &column_name, &expected_id](
                         irs::doc_id_t id,
                         const irs::bytes_view& actual_value) {
          if (id != ++expected_id) {
            return false;
          }

          auto* doc = gen.next();

          if (!doc) {
            return false;
          }

          auto* field = doc->stored.get<tests::StringField>(column_name);

          if (!field) {
            return false;
          }

          if (field->value() !=
              irs::ToString<std::string_view>(actual_value.data())) {
            return false;
          }

          return true;
        };

        auto* column = segment.column(meta->id());

        if (!column) {
          return false;
        }

        return ::Visit(*column, visitor);
      };

      auto read_column_offset = [&segment](const std::string_view& column_name,
                                           irs::doc_id_t offset) {
        auto* meta = segment.column(column_name);
        if (!meta) {
          return false;
        }

        CsvDocTemplateT csv_doc_template;
        tests::CsvDocGenerator gen(resource("simple_two_column.csv"),
                                   csv_doc_template);
        const tests::Document* doc = nullptr;

        auto column = segment.column(meta->id());
        if (!column) {
          return false;
        }
        auto reader = column->iterator(irs::ColumnHint::Normal);
        EXPECT_NE(nullptr, reader);
        auto* actual_value = irs::get<irs::PayAttr>(*reader);
        EXPECT_NE(nullptr, actual_value);

        // skip first 'offset' docs
        doc = gen.next();
        for (irs::doc_id_t id = 0; id < offset && doc; ++id) {
          doc = gen.next();
        }

        if (!doc) {
          // not enough documents to skip
          return false;
        }

        while (doc) {
          const auto target = offset + (irs::doc_limits::min)();
          if (target != reader->seek(target)) {
            return false;
          }

          auto* field = doc->stored.get<tests::StringField>(column_name);

          if (!field) {
            return false;
          }

          if (field->value() !=
              irs::ToString<std::string_view>(actual_value->value.data())) {
            return false;
          }

          ++offset;
          doc = gen.next();
        }

        return true;
      };

      auto iterate_column = [&segment](const std::string_view& column_name) {
        auto* meta = segment.column(column_name);
        if (!meta) {
          return false;
        }

        irs::doc_id_t expected_id = 0;
        CsvDocTemplateT csv_doc_template;
        tests::CsvDocGenerator gen(resource("simple_two_column.csv"),
                                   csv_doc_template);
        const tests::Document* doc = nullptr;

        auto column = segment.column(meta->id());

        if (!column) {
          return false;
        }

        auto it = column->iterator(irs::ColumnHint::Normal);

        if (!it) {
          return false;
        }

        auto* payload = irs::get<irs::PayAttr>(*it);

        if (!payload) {
          return false;
        }

        doc = gen.next();

        if (!doc) {
          return false;
        }

        while (doc) {
          if (!it->next()) {
            return false;
          }

          if (++expected_id != it->value()) {
            return false;
          }

          auto* field = doc->stored.get<tests::StringField>(column_name);

          if (!field) {
            return false;
          }

          if (field->value() !=
              irs::ToString<std::string_view>(payload->value.data())) {
            return false;
          }

          doc = gen.next();
        }

        return true;
      };

      const auto thread_count = 9;
      std::vector<int> results(thread_count, 0);
      std::vector<std::thread> pool;

      const std::string_view id_column = "id";
      const std::string_view label_column = "label";

      std::mutex mutex;
      bool ready = false;
      std::condition_variable ready_cv;

      auto wait_for_all = [&mutex, &ready, &ready_cv]() {
        // wait for all threads to be registered
        std::unique_lock lock(mutex);
        while (!ready) {
          ready_cv.wait(lock);
        }
      };

      // add visiting threads
      auto i = 0;
      for (auto max = thread_count / 3; i < max; ++i) {
        auto& result = results[i];
        auto& column_name = i % 2 ? id_column : label_column;
        pool.emplace_back(
          std::thread([&wait_for_all, &result, &visit_column, column_name]() {
            wait_for_all();
            result = static_cast<int>(visit_column(column_name));
          }));
      }

      // add reading threads
      irs::doc_id_t skip = 0;
      for (; i < 2 * (thread_count / 3); ++i) {
        auto& result = results[i];
        auto& column_name = i % 2 ? id_column : label_column;
        pool.emplace_back(std::thread(
          [&wait_for_all, &result, &read_column_offset, column_name, skip]() {
            wait_for_all();
            result = static_cast<int>(read_column_offset(column_name, skip));
          }));
        skip += 10000;
      }

      // add iterating threads
      for (; i < thread_count; ++i) {
        auto& result = results[i];
        auto& column_name = i % 2 ? id_column : label_column;
        pool.emplace_back(
          std::thread([&wait_for_all, &result, &iterate_column, column_name]() {
            wait_for_all();
            result = static_cast<int>(iterate_column(column_name));
          }));
      }

      // all threads registered... go, go, go...
      {
        std::lock_guard lock{mutex};
        ready = true;
        ready_cv.notify_all();
      }

      for (auto& thread : pool) {
        thread.join();
      }

      ASSERT_TRUE(std::all_of(results.begin(), results.end(),
                              [](int res) { return 1 == res; }));
    }
  }

  void IterateFields() {
    std::vector<std::string_view> names{
      "06D36", "0OY4F", "1DTSP", "1KCSY", "2NGZD", "3ME9S", "4UIR7", "68QRT",
      "6XTTH", "7NDWJ", "9QXBA", "A8MSE", "CNH1B", "I4EWS", "JXQKH", "KPQ7R",
      "LK1MG", "M47KP", "NWCBQ", "OEKKW", "RI1QG", "TD7H7", "U56E5", "UKETS",
      "UZWN7", "V4DLA", "W54FF", "Z4K42", "ZKQCU", "ZPNXJ"};

    ASSERT_TRUE(std::is_sorted(names.begin(), names.end()));

    struct {
      std::string_view Name() const { return name; }

      irs::IndexFeatures GetIndexFeatures() const {
        return irs::IndexFeatures::None;
      }

      irs::Tokenizer& GetTokens() const {
        stream.reset(name);
        return stream;
      }

      std::string_view name;
      mutable irs::StringTokenizer stream;
    } field;

    // insert attributes
    {
      auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate);
      ASSERT_NE(nullptr, writer);

      {
        auto ctx = writer->GetBatch();
        auto doc = ctx.Insert();

        for (auto& name : names) {
          field.name = name;
          doc.Insert<irs::Action::INDEX>(field);
        }

        ASSERT_TRUE(doc);
      }

      writer->Commit();
      AssertSnapshotEquality(*writer);
    }

    // iterate over fields
    {
      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(1, reader.size());
      auto& segment = *(reader.begin());

      auto actual = segment.fields();

      for (auto expected = names.begin(); expected != names.end();) {
        ASSERT_TRUE(actual->next());
        ASSERT_EQ(*expected, actual->value().meta().name);
        ++expected;
      }
      ASSERT_FALSE(actual->next());
      ASSERT_FALSE(actual->next());
    }

    // seek over fields
    {
      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(1, reader.size());
      auto& segment = *(reader.begin());

      auto actual = segment.fields();

      for (auto expected = names.begin(), prev = expected;
           expected != names.end();) {
        ASSERT_TRUE(actual->seek(*expected));
        ASSERT_EQ(*expected, actual->value().meta().name);

        if (prev != expected) {
          ASSERT_TRUE(actual->seek(*prev));  // can't seek backwards
          ASSERT_EQ(*expected, actual->value().meta().name);
        }

        // seek to the same value
        ASSERT_TRUE(actual->seek(*expected));
        ASSERT_EQ(*expected, actual->value().meta().name);

        prev = expected;
        ++expected;
      }
      ASSERT_FALSE(actual->next());               // reached the end
      ASSERT_FALSE(actual->seek(names.front()));  // can't seek backwards
      ASSERT_FALSE(actual->next());
    }

    // seek before the first element
    {
      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(1, reader.size());
      auto& segment = *(reader.begin());

      auto actual = segment.fields();
      auto expected = names.begin();

      const auto key = std::string_view("0");
      ASSERT_TRUE(key < names.front());
      ASSERT_TRUE(actual->seek(key));
      ASSERT_EQ(*expected, actual->value().meta().name);

      ++expected;
      for (auto prev = names.begin(); expected != names.end();) {
        ASSERT_TRUE(actual->next());
        ASSERT_EQ(*expected, actual->value().meta().name);

        if (prev != expected) {
          ASSERT_TRUE(actual->seek(*prev));  // can't seek backwards
          ASSERT_EQ(*expected, actual->value().meta().name);
        }

        // seek to the same value
        ASSERT_TRUE(actual->seek(*expected));
        ASSERT_EQ(*expected, actual->value().meta().name);

        prev = expected;
        ++expected;
      }
      ASSERT_FALSE(actual->next());               // reached the end
      ASSERT_FALSE(actual->seek(names.front()));  // can't seek backwards
      ASSERT_FALSE(actual->next());
    }

    // seek after the last element
    {
      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(1, reader.size());
      auto& segment = *(reader.begin());

      auto actual = segment.fields();

      const auto key = std::string_view("~");
      ASSERT_TRUE(key > names.back());
      ASSERT_FALSE(actual->seek(key));
      ASSERT_FALSE(actual->next());               // reached the end
      ASSERT_FALSE(actual->seek(names.front()));  // can't seek backwards
    }

    // seek in between
    {
      std::vector<std::pair<std::string_view, std::string_view>> seeks{
        {"0B", names[1]}, {names[1], names[1]},   {"0", names[1]},
        {"D", names[13]}, {names[13], names[13]}, {names[12], names[13]},
        {"P", names[20]}, {"Z", names[27]}};

      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(1, reader.size());
      auto& segment = *(reader.begin());

      auto actual = segment.fields();

      for (auto& seek : seeks) {
        auto& key = seek.first;
        auto& expected = seek.second;

        ASSERT_TRUE(actual->seek(key));
        ASSERT_EQ(expected, actual->value().meta().name);
      }

      const auto key = std::string_view("~");
      ASSERT_TRUE(key > names.back());
      ASSERT_FALSE(actual->seek(key));
      ASSERT_FALSE(actual->next());               // reached the end
      ASSERT_FALSE(actual->seek(names.front()));  // can't seek backwards
    }

    // seek in between + next
    {
      std::vector<std::pair<std::string_view, size_t>> seeks{
        {"0B", 1}, {"D", 13}, {"O", 19}, {"P", 20}, {"Z", 27}};

      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(1, reader.size());
      auto& segment = *(reader.begin());

      for (auto& seek : seeks) {
        auto& key = seek.first;
        auto expected = names.begin() + seek.second;

        auto actual = segment.fields();

        ASSERT_TRUE(actual->seek(key));
        ASSERT_EQ(*expected, actual->value().meta().name);

        for (++expected; expected != names.end(); ++expected) {
          ASSERT_TRUE(actual->next());
          ASSERT_EQ(*expected, actual->value().meta().name);
        }

        ASSERT_FALSE(actual->next());               // reached the end
        ASSERT_FALSE(actual->seek(names.front()));  // can't seek backwards
      }
    }
  }

  void IterateAttributes() {
    std::vector<std::string_view> names{
      "06D36", "0OY4F", "1DTSP", "1KCSY", "2NGZD", "3ME9S", "4UIR7", "68QRT",
      "6XTTH", "7NDWJ", "9QXBA", "A8MSE", "CNH1B", "I4EWS", "JXQKH", "KPQ7R",
      "LK1MG", "M47KP", "NWCBQ", "OEKKW", "RI1QG", "TD7H7", "U56E5", "UKETS",
      "UZWN7", "V4DLA", "W54FF", "Z4K42", "ZKQCU", "ZPNXJ"};

    ASSERT_TRUE(absl::c_is_sorted(names));

    struct {
      std::string_view Name() const { return name; }

      bool Write(irs::DataOutput&) const noexcept { return true; }

      std::string_view name;
    } field;

    // insert attributes
    {
      auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate);
      ASSERT_NE(nullptr, writer);

      {
        auto ctx = writer->GetBatch();
        auto doc = ctx.Insert();

        for (auto& name : names) {
          field.name = name;
          doc.Insert<irs::Action::STORE>(field);
        }

        ASSERT_TRUE(doc);
      }

      writer->Commit();
      AssertSnapshotEquality(*writer);
    }

    // iterate over attributes
    {
      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(1, reader.size());
      auto& segment = *(reader.begin());

      auto actual = segment.columns();

      for (auto expected = names.begin(); expected != names.end();) {
        ASSERT_TRUE(actual->next());
        ASSERT_EQ(*expected, actual->value().name());
        ++expected;
      }
      ASSERT_FALSE(actual->next());
      ASSERT_FALSE(actual->next());
    }

    // seek over attributes
    {
      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(1, reader.size());
      auto& segment = *(reader.begin());

      auto actual = segment.columns();

      for (auto expected = names.begin(), prev = expected;
           expected != names.end();) {
        ASSERT_TRUE(actual->seek(*expected));
        ASSERT_EQ(*expected, actual->value().name());

        if (prev != expected) {
          ASSERT_TRUE(actual->seek(*prev));  // can't seek backwards
          ASSERT_EQ(*expected, actual->value().name());
        }

        // seek to the same value
        ASSERT_TRUE(actual->seek(*expected));
        ASSERT_EQ(*expected, actual->value().name());

        prev = expected;
        ++expected;
      }
      ASSERT_FALSE(actual->next());               // reached the end
      ASSERT_FALSE(actual->seek(names.front()));  // can't seek backwards
      ASSERT_FALSE(actual->next());
    }

    // seek before the first element
    {
      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(1, reader.size());
      auto& segment = *(reader.begin());

      auto actual = segment.columns();
      auto expected = names.begin();

      const auto key = std::string_view("0");
      ASSERT_TRUE(key < names.front());
      ASSERT_TRUE(actual->seek(key));
      ASSERT_EQ(*expected, actual->value().name());

      ++expected;
      for (auto prev = names.begin(); expected != names.end();) {
        ASSERT_TRUE(actual->next());
        ASSERT_EQ(*expected, actual->value().name());

        if (prev != expected) {
          ASSERT_TRUE(actual->seek(*prev));  // can't seek backwards
          ASSERT_EQ(*expected, actual->value().name());
        }

        // seek to the same value
        ASSERT_TRUE(actual->seek(*expected));
        ASSERT_EQ(*expected, actual->value().name());

        prev = expected;
        ++expected;
      }
      ASSERT_FALSE(actual->next());               // reached the end
      ASSERT_FALSE(actual->seek(names.front()));  // can't seek backwards
      ASSERT_FALSE(actual->next());
    }

    // seek after the last element
    {
      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(1, reader.size());
      auto& segment = *(reader.begin());

      auto actual = segment.columns();

      const auto key = std::string_view("~");
      ASSERT_TRUE(key > names.back());
      ASSERT_FALSE(actual->seek(key));
      ASSERT_FALSE(actual->next());               // reached the end
      ASSERT_FALSE(actual->seek(names.front()));  // can't seek backwards
    }

    // seek in between
    {
      std::vector<std::pair<std::string_view, std::string_view>> seeks{
        {"0B", names[1]}, {names[1], names[1]},   {"0", names[1]},
        {"D", names[13]}, {names[13], names[13]}, {names[12], names[13]},
        {"P", names[20]}, {"Z", names[27]}};

      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(1, reader.size());
      auto& segment = *(reader.begin());

      auto actual = segment.columns();

      for (auto& seek : seeks) {
        auto& key = seek.first;
        auto& expected = seek.second;

        ASSERT_TRUE(actual->seek(key));
        ASSERT_EQ(expected, actual->value().name());
      }

      const auto key = std::string_view("~");
      ASSERT_TRUE(key > names.back());
      ASSERT_FALSE(actual->seek(key));
      ASSERT_FALSE(actual->next());               // reached the end
      ASSERT_FALSE(actual->seek(names.front()));  // can't seek backwards
    }

    // seek in between + next
    {
      std::vector<std::pair<std::string_view, size_t>> seeks{
        {"0B", 1}, {"D", 13}, {"O", 19}, {"P", 20}, {"Z", 27}};

      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(1, reader.size());
      auto& segment = *(reader.begin());

      for (auto& seek : seeks) {
        auto& key = seek.first;
        auto expected = names.begin() + seek.second;

        auto actual = segment.columns();

        ASSERT_TRUE(actual->seek(key));
        ASSERT_EQ(*expected, actual->value().name());

        for (++expected; expected != names.end(); ++expected) {
          ASSERT_TRUE(actual->next());
          ASSERT_EQ(*expected, actual->value().name());
        }

        ASSERT_FALSE(actual->next());               // reached the end
        ASSERT_FALSE(actual->seek(names.front()));  // can't seek backwards
      }
    }
  }

  void InsertDocWithNullEmptyTerm() {
    class Field {
     public:
      Field(std::string&& name, const std::string_view& value)
        : _stream(std::make_unique<irs::StringTokenizer>()),
          _name(std::move(name)),
          _value(value) {}
      Field(Field&& other) noexcept
        : _stream(std::move(other._stream)),
          _name(std::move(other._name)),
          _value(std::move(other._value)) {}
      std::string_view Name() const { return _name; }
      irs::Tokenizer& GetTokens() const {
        _stream->reset(_value);
        return *_stream;
      }
      irs::IndexFeatures GetIndexFeatures() const {
        return irs::IndexFeatures::None;
      }

     private:
      mutable std::unique_ptr<irs::StringTokenizer> _stream;
      std::string _name;
      std::string_view _value;
    };

    // write docs with empty terms
    {
      auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate);
      // doc0: empty, nullptr
      {
        std::vector<Field> doc;
        doc.emplace_back(std::string("name"), std::string_view("", 0));
        doc.emplace_back(std::string("name"), std::string_view{});
        ASSERT_TRUE(tests::Insert(*writer, doc.begin(), doc.end()));
      }
      // doc1: nullptr, empty, nullptr
      {
        std::vector<Field> doc;
        doc.emplace_back(std::string("name1"), std::string_view{});
        doc.emplace_back(std::string("name1"), std::string_view("", 0));
        doc.emplace_back(std::string("name"), std::string_view{});
        ASSERT_TRUE(tests::Insert(*writer, doc.begin(), doc.end()));
      }
      writer->Commit();
      AssertSnapshotEquality(*writer);
    }

    // check fields with empty terms
    {
      auto reader = irs::DirectoryReader(dir());
      ASSERT_EQ(1, reader.size());
      auto& segment = reader[0];

      {
        size_t count = 0;
        auto fields = segment.fields();
        while (fields->next()) {
          ++count;
        }
        ASSERT_EQ(2, count);
      }

      {
        auto* field = segment.field("name");
        ASSERT_NE(nullptr, field);
        ASSERT_EQ(1, field->size());
        ASSERT_EQ(2, field->docs_count());
        auto term = field->iterator(irs::SeekMode::NORMAL);
        ASSERT_TRUE(term->next());
        ASSERT_EQ(0, term->value().size());
        ASSERT_FALSE(term->next());
      }

      {
        auto* field = segment.field("name1");
        ASSERT_NE(nullptr, field);
        ASSERT_EQ(1, field->size());
        ASSERT_EQ(1, field->docs_count());
        auto term = field->iterator(irs::SeekMode::NORMAL);
        ASSERT_TRUE(term->next());
        ASSERT_EQ(0, term->value().size());
        ASSERT_FALSE(term->next());
      }
    }
  }

  void WriterBulkInsert() {
    class IndexedAndStoredField {
     public:
      IndexedAndStoredField(std::string&& name, const std::string_view& value,
                            bool stored_valid = true, bool indexed_valid = true)
        : _stream(std::make_unique<irs::StringTokenizer>()),
          _name(std::move(name)),
          _value(value),
          _stored_valid(stored_valid) {
        if (!indexed_valid) {
          _index_features |= irs::IndexFeatures::Freq;
        }
      }
      IndexedAndStoredField(IndexedAndStoredField&& other) noexcept
        : _stream(std::move(other._stream)),
          _name(std::move(other._name)),
          _value(std::move(other._value)),
          _stored_valid(other._stored_valid) {}
      std::string_view Name() const { return _name; }
      irs::Tokenizer& GetTokens() const {
        _stream->reset(_value);
        return *_stream;
      }
      irs::IndexFeatures GetIndexFeatures() const { return _index_features; }
      bool Write(irs::DataOutput& out) const noexcept {
        irs::WriteStr(out, _value);
        return _stored_valid;
      }

     private:
      mutable std::unique_ptr<irs::StringTokenizer> _stream;
      std::string _name;
      std::string_view _value;
      irs::IndexFeatures _index_features{irs::IndexFeatures::None};
      bool _stored_valid;
    };

    class IndexedField {
     public:
      IndexedField(std::string&& name, const std::string_view& value,
                   bool valid = true)
        : _stream(std::make_unique<irs::StringTokenizer>()),
          _name(std::move(name)),
          _value(value) {
        if (!valid) {
          _index_features |= irs::IndexFeatures::Freq;
        }
      }
      IndexedField(IndexedField&& other) noexcept = default;

      std::string_view Name() const { return _name; }
      irs::Tokenizer& GetTokens() const {
        _stream->reset(_value);
        return *_stream;
      }
      irs::IndexFeatures GetIndexFeatures() const { return _index_features; }

     private:
      mutable std::unique_ptr<irs::StringTokenizer> _stream;
      std::string _name;
      std::string_view _value;
      irs::IndexFeatures _index_features{irs::IndexFeatures::None};
    };

    struct StoredField {
      StoredField(const std::string_view& name, const std::string_view& value,
                  bool valid = true)
        : _name(name), _value(value), _valid(valid) {}

      const std::string_view& Name() const { return _name; }

      bool Write(irs::DataOutput& out) const {
        WriteStr(out, _value);
        return _valid;
      }

     private:
      std::string_view _name;
      std::string_view _value;
      bool _valid;
    };

    // insert documents
    auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate);

    size_t i = 0;
    const size_t max = 8;
    bool states[max];
    std::fill(std::begin(states), std::end(states), true);

    auto ctx = writer->GetBatch();

    do {
      auto doc = ctx.Insert();
      auto& state = states[i];

      switch (i) {
        case 0: {  // doc0
          IndexedField indexed("indexed", "doc0");
          state &= doc.Insert<irs::Action::INDEX>(indexed);
          StoredField stored("stored", "doc0");
          state &= doc.Insert<irs::Action::STORE>(stored);
          IndexedAndStoredField indexed_and_stored("indexed_and_stored",
                                                   "doc0");
          state &= doc.Insert<(irs::Action::INDEX | irs::Action::STORE)>(
            indexed_and_stored);
          ASSERT_TRUE(doc);
        } break;
        case 1: {  // doc1
          // indexed and stored fields can be indexed/stored only
          IndexedAndStoredField indexed("indexed", "doc1");
          state &= doc.Insert<irs::Action::INDEX>(indexed);
          IndexedAndStoredField stored("stored", "doc1");
          state &= doc.Insert<irs::Action::STORE>(stored);
          ASSERT_TRUE(doc);
        } break;
        case 2: {  // doc2 (will be dropped since it contains invalid stored
                   // field)
          IndexedAndStoredField indexed("indexed", "doc2");
          state &= doc.Insert<irs::Action::INDEX>(indexed);
          StoredField stored("stored", "doc2", false);
          state &= doc.Insert<irs::Action::STORE>(stored);
          ASSERT_FALSE(doc);
        } break;
        case 3: {  // doc3 (will be dropped since it contains invalid indexed
                   // field)
          IndexedField indexed("indexed", "doc3", false);
          state &= doc.Insert<irs::Action::INDEX>(indexed);
          StoredField stored("stored", "doc3");
          state &= doc.Insert<irs::Action::STORE>(stored);
          ASSERT_FALSE(doc);
        } break;
        case 4: {  // doc4 (will be dropped since it contains invalid indexed
                   // and stored field)
          IndexedAndStoredField indexed_and_stored("indexed", "doc4", false,
                                                   false);
          state &= doc.Insert<(irs::Action::INDEX | irs::Action::STORE)>(
            indexed_and_stored);
          StoredField stored("stored", "doc4");
          state &= doc.Insert<irs::Action::STORE>(stored);
          ASSERT_FALSE(doc);
        } break;
        case 5: {  // doc5 (will be dropped since it contains failed stored
                   // field)
          IndexedAndStoredField indexed_and_stored(
            "indexed_and_stored", "doc5",
            false);  // will fail on store, but will pass on index
          state &= doc.Insert<(irs::Action::INDEX | irs::Action::STORE)>(
            indexed_and_stored);
          StoredField stored("stored", "doc5");
          state &= doc.Insert<irs::Action::STORE>(stored);
          ASSERT_FALSE(doc);
        } break;
        case 6: {  // doc6 (will be dropped since it contains failed indexed
                   // field)
          IndexedAndStoredField indexed_and_stored(
            "indexed_and_stored", "doc6", true,
            false);  // will fail on index, but will pass on store
          state &= doc.Insert<(irs::Action::INDEX | irs::Action::STORE)>(
            indexed_and_stored);
          StoredField stored("stored", "doc6");
          state &= doc.Insert<irs::Action::STORE>(stored);
          ASSERT_FALSE(doc);
        } break;
        case 7: {  // valid insertion of last doc will mark bulk insert result
                   // as valid
          IndexedAndStoredField indexed_and_stored(
            "indexed_and_stored", "doc7", true,
            true);  // will be indexed, and will be stored
          state &= doc.Insert<(irs::Action::INDEX | irs::Action::STORE)>(
            indexed_and_stored);
          StoredField stored("stored", "doc7");
          state &= doc.Insert<irs::Action::STORE>(stored);
          ASSERT_TRUE(doc);
        } break;
      }
    } while (++i != max);

    ASSERT_TRUE(states[0]);   // successfully inserted
    ASSERT_TRUE(states[1]);   // successfully inserted
    ASSERT_FALSE(states[2]);  // skipped
    ASSERT_FALSE(states[3]);  // skipped
    ASSERT_FALSE(states[4]);  // skipped
    ASSERT_FALSE(states[5]);  // skipped
    ASSERT_FALSE(states[6]);  // skipped
    ASSERT_TRUE(states[7]);   // successfully inserted

    {
      irs::IndexWriter::Transaction(std::move(ctx));
    }  // force flush of GetBatch()
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // check index
    {
      auto reader = irs::DirectoryReader(dir());
      ASSERT_EQ(1, reader.size());
      auto& segment = reader[0];
      ASSERT_EQ(8, reader->docs_count());       // we have 8 documents in total
      ASSERT_EQ(3, reader->live_docs_count());  // 5 of which marked as deleted

      std::unordered_set<std::string> expected_values{"doc0", "doc1", "doc7"};
      std::unordered_set<std::string> actual_values;

      const auto* column_reader = segment.column("stored");
      ASSERT_NE(nullptr, column_reader);
      auto column = column_reader->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, column);
      auto* actual_value = irs::get<irs::PayAttr>(*column);
      ASSERT_NE(nullptr, actual_value);

      auto it = segment.docs_iterator();
      while (it->next()) {
        ASSERT_EQ(it->value(), column->seek(it->value()));
        actual_values.emplace(
          irs::ToString<std::string>(actual_value->value.data()));
      }
      ASSERT_EQ(expected_values, actual_values);
    }
  }

  void WriterAtomicityCheck() {
    struct OverrideSyncDirectory : tests::DirectoryMock {
      typedef std::function<bool(std::string_view)> SyncF;

      OverrideSyncDirectory(irs::Directory& impl, SyncF&& sync)
        : DirectoryMock(impl), _sync(std::move(sync)) {}

      bool sync(std::span<const std::string_view> files) noexcept final {
        return absl::c_all_of(files, [this](std::string_view name) noexcept {
          try {
            if (_sync(name)) {
              return true;
            }
          } catch (...) {
            return false;
          }

          return DirectoryMock::sync({&name, 1});
        });
      }

     private:
      SyncF _sync;
    };

    // create empty index
    {
      auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate);

      writer->Commit();
      AssertSnapshotEquality(*writer);
    }

    // error while commiting index (during sync in index_meta_writer)
    {
      OverrideSyncDirectory override_dir(
        dir(), [](const std::string_view&) -> bool { throw irs::IoError(); });

      tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                  &tests::GenericJsonFieldFactory);
      const tests::Document* doc1 = gen.next();
      const tests::Document* doc2 = gen.next();
      const tests::Document* doc3 = gen.next();
      const tests::Document* doc4 = gen.next();

      auto writer =
        irs::IndexWriter::Make(override_dir, codec(), irs::kOmAppend);

      ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                         doc1->stored.begin(), doc1->stored.end()));
      ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                         doc2->stored.begin(), doc2->stored.end()));
      ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                         doc3->stored.begin(), doc3->stored.end()));
      ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                         doc4->stored.begin(), doc4->stored.end()));
      ASSERT_THROW(writer->Commit(), irs::IoError);
    }

    // error while commiting index (during sync in index_writer)
    {
      OverrideSyncDirectory override_dir(dir(),
                                         [](const std::string_view& name) {
                                           if (name.starts_with("_")) {
                                             throw irs::IoError();
                                           }
                                           return false;
                                         });

      tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                  &tests::GenericJsonFieldFactory);
      const tests::Document* doc1 = gen.next();
      const tests::Document* doc2 = gen.next();
      const tests::Document* doc3 = gen.next();
      const tests::Document* doc4 = gen.next();

      auto writer =
        irs::IndexWriter::Make(override_dir, codec(), irs::kOmAppend);

      ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                         doc1->stored.begin(), doc1->stored.end()));
      ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                         doc2->stored.begin(), doc2->stored.end()));
      ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                         doc3->stored.begin(), doc3->stored.end()));
      ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                         doc4->stored.begin(), doc4->stored.end()));
      ASSERT_THROW(writer->Commit(), irs::IoError);
    }

    // check index, it should be empty
    {
      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(0, reader.live_docs_count());
      ASSERT_EQ(0, reader.docs_count());
      ASSERT_EQ(0, reader.size());
      ASSERT_EQ(reader.begin(), reader.end());
    }
  }

  void DocsBitUnion(irs::IndexFeatures features);
};

void IndexTestCase::DocsBitUnion(irs::IndexFeatures features) {
  tests::StringViewField field("0", features);
  const size_t n = irs::BitsRequired<uint64_t>(2) + 7;

  {
    auto writer = open_writer();

    {
      auto docs = writer->GetBatch();
      for (size_t i = 1; i <= n; ++i) {
        const std::string_view value = i % 2 ? "A" : "B";
        field.value(value);
        ASSERT_TRUE(docs.Insert().Insert<irs::Action::INDEX>(field));
      }

      field.value("C");
      ASSERT_TRUE(docs.Insert().Insert<irs::Action::INDEX>(field));
    }

    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);
  ASSERT_EQ(1, reader->size());
  auto& segment = (*reader)[0];
  ASSERT_EQ(n + 1, segment.docs_count());
  ASSERT_EQ(n + 1, segment.live_docs_count());

  const auto* term_reader = segment.field(field.Name());
  ASSERT_NE(nullptr, term_reader);
  ASSERT_EQ(n + 1, term_reader->docs_count());
  ASSERT_EQ(3, term_reader->size());
  ASSERT_EQ("A", irs::ViewCast<char>(term_reader->min()));
  ASSERT_EQ("C", irs::ViewCast<char>(term_reader->max()));
  ASSERT_EQ(field.Name(), term_reader->meta().name);
  ASSERT_EQ(field.GetIndexFeatures(), term_reader->meta().index_features);

  constexpr size_t kExpectedDocsB[]{
    0b0101010101010101010101010101010101010101010101010101010101010100,
    0b0101010101010101010101010101010101010101010101010101010101010101,
    0b0000000000000000000000000000000000000000000000000000000001010101};

  constexpr size_t kExpectedDocsA[]{
    0b1010101010101010101010101010101010101010101010101010101010101010,
    0b1010101010101010101010101010101010101010101010101010101010101010,
    0b0000000000000000000000000000000000000000000000000000000010101010};

  irs::SeekCookie::ptr cookies[2];

  auto term = term_reader->iterator(irs::SeekMode::NORMAL);
  ASSERT_TRUE(term->next());
  ASSERT_EQ("A", irs::ViewCast<char>(term->value()));
  term->read();
  cookies[0] = term->cookie();
  ASSERT_TRUE(term->next());
  term->read();
  cookies[1] = term->cookie();
  ASSERT_EQ("B", irs::ViewCast<char>(term->value()));

  auto cookie_provider =
    [begin = std::begin(cookies),
     end = std::end(cookies)]() mutable -> const irs::SeekCookie* {
    if (begin != end) {
      auto cookie = begin->get();
      ++begin;
      return cookie;
    }
    return nullptr;
  };

  size_t actual_docs_ab[3]{};
  ASSERT_EQ(n, term_reader->BitUnion(cookie_provider, actual_docs_ab));
  ASSERT_EQ(kExpectedDocsA[0] | kExpectedDocsB[0], actual_docs_ab[0]);
  ASSERT_EQ(kExpectedDocsA[1] | kExpectedDocsB[1], actual_docs_ab[1]);
  ASSERT_EQ(kExpectedDocsA[2] | kExpectedDocsB[2], actual_docs_ab[2]);
}

TEST_P(IndexTestCase, s2sequence) {
  std::vector<std::string> sequence;
  absl::flat_hash_set<std::string> indexed;

  {
    auto writer = open_writer();
    ASSERT_NE(nullptr, writer);

    std::ifstream stream{resource("s2sequence"), std::ifstream::in};
    ASSERT_TRUE(static_cast<bool>(stream));

    std::string str;
    auto field = std::make_shared<tests::StringField>("value");
    tests::Document doc;
    doc.indexed.push_back(field);

    auto& expected_segment = this->index().emplace_back(writer->FeatureInfo());
    while (std::getline(stream, str)) {
      if (str.starts_with("#")) {
        break;
      }

      field->value(str);
      indexed.emplace(std::move(str));
      ASSERT_TRUE(tests::Insert(*writer, doc));
      expected_segment.insert(doc);
    }

    while (std::getline(stream, str)) {
      sequence.emplace_back(std::move(str));
    }

    ASSERT_TRUE(writer->Commit());
    AssertSnapshotEquality(*writer);
  }

  assert_index();
  assert_columnstore();

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);
  ASSERT_EQ(1, reader->size());
  auto& segment = (*reader)[0];
  auto* field = segment.field("value");
  ASSERT_NE(nullptr, field);
  auto& expected_field = index().front().fields().at("value");

  {
    auto terms = field->iterator(irs::SeekMode::RandomOnly);
    ASSERT_NE(nullptr, terms);
    auto* meta = irs::get<irs::TermMeta>(*terms);
    ASSERT_NE(nullptr, meta);

    auto expected_term = expected_field.iterator();
    ASSERT_NE(nullptr, expected_term);

    for (std::string_view term : sequence) {
      SCOPED_TRACE(testing::Message("Term: ") << term);

      const auto res = terms->seek(irs::ViewCast<irs::byte_type>(term));
      const auto exp_res =
        expected_term->seek(irs::ViewCast<irs::byte_type>(term));
      ASSERT_EQ(exp_res, res);

      if (res) {
        ASSERT_EQ(expected_term->value(), terms->value());
        terms->read();
        ASSERT_EQ(meta->docs_count, 1);
      }
    }
  }

  {
    auto terms = field->iterator(irs::SeekMode::NORMAL);
    ASSERT_NE(nullptr, terms);
    auto* meta = irs::get<irs::TermMeta>(*terms);
    ASSERT_NE(nullptr, meta);

    auto expected_term = expected_field.iterator();
    ASSERT_NE(nullptr, expected_term);

    for (std::string_view term : sequence) {
      SCOPED_TRACE(testing::Message("Term: ") << term);

      const auto res = terms->seek(irs::ViewCast<irs::byte_type>(term));
      const auto exp_res =
        expected_term->seek(irs::ViewCast<irs::byte_type>(term));
      ASSERT_EQ(exp_res, res);

      if (res) {
        ASSERT_EQ(expected_term->value(), terms->value());
        terms->read();
        ASSERT_EQ(meta->docs_count, 1);
      }
    }
  }

  {
    auto terms = field->iterator(irs::SeekMode::NORMAL);
    ASSERT_NE(nullptr, terms);
    auto* meta = irs::get<irs::TermMeta>(*terms);
    ASSERT_NE(nullptr, meta);

    auto expected_term = expected_field.iterator();
    ASSERT_NE(nullptr, expected_term);

    for (std::string_view term : sequence) {
      SCOPED_TRACE(testing::Message("Term: ") << term);

      const auto res = terms->seek_ge(irs::ViewCast<irs::byte_type>(term));
      const auto exp_res =
        expected_term->seek_ge(irs::ViewCast<irs::byte_type>(term));
      ASSERT_EQ(exp_res, res);

      if (res != irs::SeekResult::End) {
        ASSERT_EQ(expected_term->value(), terms->value());
        terms->read();
        ASSERT_EQ(meta->docs_count, 1);
      }
    }
  }
}

TEST_P(IndexTestCase, reopen_reader_after_writer_is_closed) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);
  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();

  irs::DirectoryReader reader0;
  irs::DirectoryReader reader1;
  irs::DirectoryReader reader2;
  {
    auto writer = open_writer();
    ASSERT_NE(nullptr, writer);
    reader0 = writer->GetSnapshot();
    ASSERT_EQ(0, reader0.size());
    ASSERT_EQ(irs::DirectoryMeta{}, reader0.Meta());
    ASSERT_EQ(0, reader0.docs_count());
    ASSERT_EQ(0, reader0.live_docs_count());

    ASSERT_TRUE(tests::Insert(*writer, *doc1));
    ASSERT_TRUE(writer->Commit());
    reader1 = writer->GetSnapshot();
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader{dir()});
    tests::AssertSnapshotEquality(writer->GetSnapshot(), reader0->Reopen());
    tests::AssertSnapshotEquality(writer->GetSnapshot(), reader1->Reopen());
    ASSERT_EQ(1, reader1.size());
    ASSERT_FALSE(reader1.Meta().filename.empty());
    ASSERT_EQ(1, reader1.docs_count());
    ASSERT_EQ(1, reader1.live_docs_count());

    ASSERT_TRUE(tests::Insert(*writer, *doc2));
    ASSERT_TRUE(writer->Commit());
    reader2 = writer->GetSnapshot();
    ASSERT_EQ(2, reader2.size());
    ASSERT_FALSE(reader2.Meta().filename.empty());
    ASSERT_EQ(2, reader2.docs_count());
    ASSERT_EQ(2, reader2.live_docs_count());
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader{dir()});
    tests::AssertSnapshotEquality(writer->GetSnapshot(), reader0->Reopen());
    tests::AssertSnapshotEquality(writer->GetSnapshot(), reader1->Reopen());
    tests::AssertSnapshotEquality(writer->GetSnapshot(), reader2->Reopen());
  }

  // Check reader after the writer is closed
  auto check_reader = [&](irs::DirectoryReader reader) {
    reader = reader0->Reopen();
    tests::AssertSnapshotEquality(reader, irs::DirectoryReader{dir()});
    EXPECT_EQ(2, reader.size());
    EXPECT_FALSE(reader.Meta().filename.empty());
    EXPECT_EQ(2, reader.docs_count());
    EXPECT_EQ(2, reader.live_docs_count());
  };

  check_reader(reader0);
  check_reader(reader1);
  check_reader(reader2);
}

TEST_P(IndexTestCase, serene_demo_docs) {
  {
    tests::JsonDocGenerator gen(resource("serene_demo.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  assert_index();
}

TEST_P(IndexTestCase, check_fields_order) { IterateFields(); }

TEST_P(IndexTestCase, check_attributes_order) { IterateAttributes(); }

TEST_P(IndexTestCase, clear_writer) { ClearWriter(); }

TEST_P(IndexTestCase, open_writer) { OpenWriterCheckLock(); }

TEST_P(IndexTestCase, check_writer_open_modes) { WriterCheckOpenModes(); }

TEST_P(IndexTestCase, writer_transaction_isolation) {
  WriterTransactionIsolation();
}

TEST_P(IndexTestCase, writer_atomicity_check) { WriterAtomicityCheck(); }

TEST_P(IndexTestCase, writer_bulk_insert) { WriterBulkInsert(); }

TEST_P(IndexTestCase, writer_begin_rollback) { WriterBeginRollback(); }

TEST_P(IndexTestCase, writer_batch_rollback) { WriterBatchWithErrorRollback(); }

TEST_P(IndexTestCase, writer_begin_clear_empty_index) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  const tests::Document* doc1 = gen.next();

  {
    auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_EQ(1, writer->BufferedDocs());
    writer->Clear();  // rollback started transaction and clear index
    ASSERT_EQ(0, writer->BufferedDocs());
    ASSERT_FALSE(writer->Begin());  // nothing to commit

    // check index, it should be empty
    {
      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(0, reader.live_docs_count());
      ASSERT_EQ(0, reader.docs_count());
      ASSERT_EQ(0, reader.size());
      ASSERT_EQ(reader.begin(), reader.end());
    }
  }
}

TEST_P(IndexTestCase, writer_begin_clear) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  const tests::Document* doc1 = gen.next();

  {
    auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_EQ(1, writer->BufferedDocs());
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, writer->BufferedDocs());

    // check index, it should not be empty
    {
      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(1, reader.live_docs_count());
      ASSERT_EQ(1, reader.docs_count());
      ASSERT_EQ(1, reader.size());
      ASSERT_NE(reader.begin(), reader.end());
    }

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_EQ(1, writer->BufferedDocs());
    ASSERT_TRUE(writer->Begin());  // start transaction
    ASSERT_EQ(0, writer->BufferedDocs());

    writer->Clear();  // rollback and clear index contents
    ASSERT_EQ(0, writer->BufferedDocs());
    ASSERT_FALSE(writer->Begin());  // nothing to commit

    // check index, it should be empty
    {
      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(0, reader.live_docs_count());
      ASSERT_EQ(0, reader.docs_count());
      ASSERT_EQ(0, reader.size());
      ASSERT_EQ(reader.begin(), reader.end());
    }
  }
}

TEST_P(IndexTestCase, writer_commit_cleanup_interleaved) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  auto clean = [this]() { irs::directory_utils::RemoveAllUnreferenced(dir()); };

  {
    tests::CallbackDirectory synced_dir(dir(), clean);
    auto writer = irs::IndexWriter::Make(synced_dir, codec(), irs::kOmCreate);
    const auto* doc1 = gen.next();
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // check index, it should contain expected number of docs
    auto reader = irs::DirectoryReader(synced_dir, codec());
    ASSERT_EQ(1, reader.live_docs_count());
    ASSERT_EQ(1, reader.docs_count());
    ASSERT_NE(reader.begin(), reader.end());
  }
}

TEST_P(IndexTestCase, writer_commit_clear) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  const tests::Document* doc1 = gen.next();

  {
    auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_EQ(1, writer->BufferedDocs());
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, writer->BufferedDocs());

    // check index, it should not be empty
    {
      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(1, reader.live_docs_count());
      ASSERT_EQ(1, reader.docs_count());
      ASSERT_EQ(1, reader.size());
      ASSERT_NE(reader.begin(), reader.end());
    }

    writer->Clear();  // clear index contents
    ASSERT_EQ(0, writer->BufferedDocs());
    ASSERT_FALSE(writer->Begin());  // nothing to commit

    // check index, it should be empty
    {
      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(0, reader.live_docs_count());
      ASSERT_EQ(0, reader.docs_count());
      ASSERT_EQ(0, reader.size());
      ASSERT_EQ(reader.begin(), reader.end());
    }
  }
}

TEST_P(IndexTestCase, insert_null_empty_term) { InsertDocWithNullEmptyTerm(); }

TEST_P(IndexTestCase, europarl_docs) {
  {
    tests::EuroparlDocTemplate doc;
    tests::DelimDocGenerator gen(resource("europarl.subset.txt"), doc);
    add_segment(gen);
  }
  assert_index();
}

TEST_P(IndexTestCase, europarl_docs_batched) {
  {
    tests::EuroparlDocTemplate doc;
    tests::DelimDocGenerator gen(resource("europarl.subset.txt"), doc);
    add_segment_batched(gen, 100);
  }
  assert_index();
}

TEST_P(IndexTestCase, europarl_docs_automaton) {
  {
    tests::EuroparlDocTemplate doc;
    tests::DelimDocGenerator gen(resource("europarl.subset.txt"), doc);
    add_segment(gen);
  }

  // prefix
  {
    auto acceptor = irs::FromWildcard("forb%");
    irs::automaton_table_matcher matcher(acceptor, true);
    assert_index(0, &matcher);
  }

  // part
  {
    auto acceptor = irs::FromWildcard("%ende%");
    irs::automaton_table_matcher matcher(acceptor, true);
    assert_index(0, &matcher);
  }

  // suffix
  {
    auto acceptor = irs::FromWildcard("%ione");
    irs::automaton_table_matcher matcher(acceptor, true);
    assert_index(0, &matcher);
  }
}

TEST_P(IndexTestCase, europarl_docs_big) {
  if (dynamic_cast<irs::FSDirectory*>(&dir()) != nullptr) {
    GTEST_SKIP() << "too long for our CI";
  }
  {
    tests::EuroparlDocTemplate doc;
    tests::DelimDocGenerator gen(resource("europarl.subset.big.txt"), doc);
    add_segment(gen);
  }
  assert_index();
}

TEST_P(IndexTestCase, europarl_docs_big_automaton) {
#ifdef SDB_DEV
  GTEST_SKIP() << "too long for our CI";
#endif

  {
    tests::EuroparlDocTemplate doc;
    tests::DelimDocGenerator gen(resource("europarl.subset.txt"), doc);
    add_segment(gen);
  }

  // prefix
  {
    auto acceptor = irs::FromWildcard("forb%");
    irs::automaton_table_matcher matcher(acceptor, true);
    assert_index(0, &matcher);
  }

  // part
  {
    auto acceptor = irs::FromWildcard("%ende%");
    irs::automaton_table_matcher matcher(acceptor, true);
    assert_index(0, &matcher);
  }

  // suffix
  {
    auto acceptor = irs::FromWildcard("%ione");
    irs::automaton_table_matcher matcher(acceptor, true);
    assert_index(0, &matcher);
  }
}

TEST_P(IndexTestCase, docs_bit_union) {
  DocsBitUnion(irs::IndexFeatures::None);
  DocsBitUnion(irs::IndexFeatures::Freq);
}

TEST_P(IndexTestCase, monarch_eco_onthology) {
  {
    tests::JsonDocGenerator gen(resource("ECO_Monarch.json"),
                                &tests::PayloadedJsonFieldFactory);
    add_segment(gen);
  }
  assert_index();
}

TEST_P(IndexTestCase, concurrent_read_column_mt) {
  ConcurrentReadSingleColumnSmoke();
  ConcurrentReadMultipleColumns();
}

TEST_P(IndexTestCase, concurrent_read_index_mt) { ConcurrentReadIndex(); }

TEST_P(IndexTestCase, concurrent_add_mt) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);
  std::vector<const tests::Document*> docs;

  for (const tests::Document* doc; (doc = gen.next()) != nullptr;
       docs.emplace_back(doc)) {
  }

  {
    auto writer = open_writer();

    std::thread thread0([&writer, docs]() {
      for (size_t i = 0, count = docs.size(); i < count; i += 2) {
        auto& doc = docs[i];
        ASSERT_TRUE(Insert(*writer, doc->indexed.begin(), doc->indexed.end(),
                           doc->stored.begin(), doc->stored.end()));
      }
    });
    std::thread thread1([&writer, docs]() {
      for (size_t i = 1, count = docs.size(); i < count; i += 2) {
        auto& doc = docs[i];
        ASSERT_TRUE(Insert(*writer, doc->indexed.begin(), doc->indexed.end(),
                           doc->stored.begin(), doc->stored.end()));
      }
    });

    thread0.join();
    thread1.join();
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader.size() == 1 ||
                reader.size() ==
                  2);  // can be 1 if thread0 finishes before thread1 starts
    ASSERT_EQ(docs.size(), reader.docs_count());
  }
}

TEST_P(IndexTestCase, concurrent_add_remove_mt) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });
  std::vector<const tests::Document*> docs;
  std::atomic<bool> first_doc(false);

  for (const tests::Document* doc; (doc = gen.next()) != nullptr;
       docs.emplace_back(doc)) {
  }

  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto writer = open_writer();

    std::thread thread0([&writer, docs, &first_doc]() {
      auto& doc = docs[0];
      Insert(*writer, doc->indexed.begin(), doc->indexed.end(),
             doc->stored.begin(), doc->stored.end());
      first_doc = true;

      for (size_t i = 2, count = docs.size(); i < count;
           i += 2) {  // skip first doc
        auto& doc = docs[i];
        Insert(*writer, doc->indexed.begin(), doc->indexed.end(),
               doc->stored.begin(), doc->stored.end());
      }
    });
    std::thread thread1([&writer, docs]() {
      for (size_t i = 1, count = docs.size(); i < count; i += 2) {
        auto& doc = docs[i];
        Insert(*writer, doc->indexed.begin(), doc->indexed.end(),
               doc->stored.begin(), doc->stored.end());
      }
    });
    std::thread thread2([&writer, &query_doc1, &first_doc]() {
      while (!first_doc)
        ;  // busy-wait until first document loaded
      writer->GetBatch().Remove(std::move(query_doc1));
    });

    thread0.join();
    thread1.join();
    thread2.join();
    writer->Commit();
    AssertSnapshotEquality(*writer);

    std::unordered_set<std::string_view> expected = {
      "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L",
      "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W",
      "X", "Y", "Z", "~", "!", "@", "#", "$", "%"};
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(
      reader.size() == 1 || reader.size() == 2 ||
      reader.size() ==
        3);  // can be 1 if thread0 finishes before thread1 starts, can be 2
             // if thread0 and thread1 finish before thread2 starts
    ASSERT_TRUE(reader.docs_count() == docs.size() ||
                reader.docs_count() ==
                  docs.size() -
                    1);  // removed doc might have been on its own segment

    for (size_t i = 0, count = reader.size(); i < count; ++i) {
      auto& segment = reader[i];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      while (docs_itr->next()) {
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ(1, expected.erase(irs::ToString<std::string_view>(
                       actual_value->value.data())));
      }
    }

    ASSERT_TRUE(expected.empty());
  }
}

TEST_P(IndexTestCase, concurrent_add_remove_overlap_commit_mt) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();

  // remove added docs, add same docs again commit separate thread before end of
  // add
  {
    std::condition_variable cond;
    std::mutex mutex;
    auto query_doc1_doc2 = MakeByTermOrByTerm("name", "A", "name", "B");
    auto writer = open_writer();
    std::unique_lock lock{mutex};
    std::atomic<bool> stop(false);
    std::thread thread([&]() -> void {
      std::lock_guard lock{mutex};
      writer->Commit();
      AssertSnapshotEquality(*writer);
      stop = true;
      cond.notify_all();
    });

    // initial add docs
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // remove docs
    writer->GetBatch().Remove(*(query_doc1_doc2.get()));

    // re-add docs into a single segment
    {
      auto ctx = writer->GetBatch();

      {
        auto doc = ctx.Insert();
        doc.Insert<irs::Action::INDEX>(doc1->indexed.begin(),
                                       doc1->indexed.end());
        doc.Insert<irs::Action::STORE>(doc1->indexed.begin(),
                                       doc1->indexed.end());
      }
      {
        auto doc = ctx.Insert();
        doc.Insert<irs::Action::INDEX>(doc2->indexed.begin(),
                                       doc2->indexed.end());
        doc.Insert<irs::Action::STORE>(doc2->indexed.begin(),
                                       doc2->indexed.end());
      }

      // commit from a separate thread before end of add
      lock.unlock();
      std::mutex cond_mutex;
      std::unique_lock cond_lock{cond_mutex};
      auto result = cond.wait_for(
        cond_lock, 100ms);  // assume thread commits within 100 msec

      // As declaration for wait_for contains "It may also be unblocked
      // spuriously." for all platforms
      while (!stop && result == std::cv_status::no_timeout)
        result = cond.wait_for(cond_lock, 100ms);

      // FIXME TODO add once segment_context will not block flush_all()
      // ASSERT_TRUE(stop);
    }

    thread.join();

    auto reader = irs::DirectoryReader(dir(), codec());
    /* FIXME TODO add once segment_context will not block flush_all()
    ASSERT_EQ(0, reader.docs_count());
    ASSERT_EQ(0, reader.live_docs_count());
    writer->Commit(); AssertSnapshotEquality(*writer); // commit after releasing
    documents_context reader = irs::directory_reader::open(dir(), codec());
    */
    ASSERT_EQ(2, reader.docs_count());
    ASSERT_EQ(2, reader.live_docs_count());
  }

  // remove added docs, add same docs again commit separate thread after end of
  // add
  {
    auto query_doc1_doc2 = MakeByTermOrByTerm("name", "A", "name", "B");
    auto writer = open_writer();

    // initial add docs
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // remove docs
    writer->GetBatch().Remove(*(query_doc1_doc2.get()));

    // re-add docs into a single segment
    {
      auto ctx = writer->GetBatch();

      {
        auto doc = ctx.Insert();
        doc.Insert<irs::Action::INDEX>(doc1->indexed.begin(),
                                       doc1->indexed.end());
        doc.Insert<irs::Action::STORE>(doc1->indexed.begin(),
                                       doc1->indexed.end());
      }
      {
        auto doc = ctx.Insert();
        doc.Insert<irs::Action::INDEX>(doc2->indexed.begin(),
                                       doc2->indexed.end());
        doc.Insert<irs::Action::STORE>(doc2->indexed.begin(),
                                       doc2->indexed.end());
      }
    }

    std::thread thread([&]() -> void {
      writer->Commit();
      AssertSnapshotEquality(*writer);
    });
    thread.join();

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(2, reader.docs_count());
    ASSERT_EQ(2, reader.live_docs_count());
  }
}

TEST_P(IndexTestCase, document_context) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();
  const tests::Document* doc4 = gen.next();

  struct {
    std::condition_variable cond;
    std::mutex cond_mutex;
    std::mutex mutex;
    std::condition_variable wait_cond;
    std::atomic<bool> wait;

    std::string_view Name() { return ""; }

    bool Write(irs::DataOutput&) {
      {
        std::lock_guard cond_lock{cond_mutex};
      }

      cond.notify_all();

      while (wait) {
        std::unique_lock lock{mutex};
        wait_cond.wait_for(lock, 100ms);
      }

      return true;
    }
  } field;

  // during insert across commit blocks
  {
    auto writer = open_writer();
    // wait for insertion to start
    auto field_cond_lock = std::unique_lock{field.cond_mutex};
    field.wait = true;  // prevent field from finishing

    // ensure segment is prsent in the active flush_context
    writer->GetBatch().Insert().Insert<irs::Action::STORE>(doc1->stored.begin(),
                                                           doc1->stored.end());

    std::thread thread0([&writer, &field]() -> void {
      writer->GetBatch().Insert().Insert<irs::Action::STORE>(field);
    });

    ASSERT_EQ(std::cv_status::no_timeout,
              field.cond.wait_for(field_cond_lock,
                                  1000ms));  // wait for insertion to start

    std::atomic<bool> stop(false);
    std::thread thread1([&]() -> void {
      writer->Commit();
      AssertSnapshotEquality(*writer);
      stop = true;
      {
        auto lock = std::lock_guard{field.cond_mutex};
      }
      field.cond.notify_all();
    });

    auto result = field.cond.wait_for(field_cond_lock, 100ms);

    // As declaration for wait_for contains "It may also be unblocked
    // spuriously." for all platforms
    while (!stop && result == std::cv_status::no_timeout) {
      result = field.cond.wait_for(field_cond_lock, 100ms);
    }

    ASSERT_EQ(std::cv_status::timeout, result);  // verify commit() blocks
    field.wait = false;
    field.wait_cond.notify_all();
    ASSERT_EQ(std::cv_status::no_timeout,
              field.cond.wait_for(field_cond_lock,
                                  10000ms));  // verify commit() finishes

    // FIXME TODO add once segment_context will not block flush_all()
    // ASSERT_TRUE(stop);
    thread0.join();
    thread1.join();
  }

  // during replace across commit blocks (single doc)
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    // wait for insertion to start
    auto field_cond_lock = std::unique_lock{field.cond_mutex};
    field.wait = true;  // prevent field from finishing

    std::thread thread0([&writer, &query_doc1, &field]() -> void {
      writer->GetBatch().Replace(*query_doc1).Insert<irs::Action::STORE>(field);
    });

    // wait for insertion to start
    ASSERT_EQ(std::cv_status::no_timeout,
              field.cond.wait_for(field_cond_lock, 1000ms));

    std::atomic<bool> commit(false);
    std::thread thread1([&]() -> void {
      writer->Commit();
      AssertSnapshotEquality(*writer);
      commit = true;
      auto lock = std::lock_guard{field.cond_mutex};
      field.cond.notify_all();
    });

    // verify commit() blocks
    auto result = field.cond.wait_for(field_cond_lock, 100ms);

    // As declaration for wait_for contains "It may also be unblocked
    // spuriously." for all platforms
    while (!commit && result == std::cv_status::no_timeout)
      result = field.cond.wait_for(field_cond_lock, 100ms);

    ASSERT_EQ(std::cv_status::timeout, result);
    field.wait = false;
    field.wait_cond.notify_all();

    // verify commit() finishes
    ASSERT_EQ(std::cv_status::no_timeout,
              field.cond.wait_for(field_cond_lock, 10000ms));

    // FIXME TODO add once segment_context will not block flush_all()
    // ASSERT_TRUE(commit);
    thread0.join();
    thread1.join();
  }

  // remove without tick
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto query_doc3 = MakeByTerm("name", "C");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->GetBatch().Remove<false>(*query_doc1);
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->GetBatch().Remove(*query_doc3);
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    EXPECT_EQ(3, reader.docs_count());
    EXPECT_EQ(2, reader.live_docs_count());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_FALSE(docs_itr->next());
  }

  // holding document_context after insert across commit does not block
  {
    auto writer = open_writer();
    auto ctx = writer->GetBatch();
    // wait for insertion to start
    auto field_cond_lock = std::unique_lock{field.cond_mutex};

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    std::thread thread1([&]() -> void {
      writer->Commit();
      AssertSnapshotEquality(*writer);
      auto lock = std::lock_guard{field.cond_mutex};
      field.cond.notify_all();
    });

    ASSERT_EQ(std::cv_status::no_timeout,
              field.cond.wait_for(
                field_cond_lock,
                std::chrono::milliseconds(10000)));  // verify commit() finishes
    {
      irs::IndexWriter::Transaction(std::move(ctx));
    }  // release ctx before join() in case of test failure
    thread1.join();

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A",
              irs::ToString<std::string_view>(
                actual_value->value.data()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // holding document_context after remove across commit does not block
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));

    auto ctx = writer->GetBatch();
    // wait for insertion to start
    auto field_cond_lock = std::unique_lock{field.cond_mutex};
    ctx.Remove(*(query_doc1));
    std::atomic<bool> commit(false);  // FIXME TODO remove once segment_context
                                      // will not block flush_all()
    std::thread thread1([&]() -> void {
      writer->Commit();
      AssertSnapshotEquality(*writer);
      commit = true;
      auto lock = std::lock_guard{field.cond_mutex};
      field.cond.notify_all();
    });

    auto result = field.cond.wait_for(
      field_cond_lock,
      std::chrono::milliseconds(
        10000));  // verify commit() finishes FIXME TODO remove once
                  // segment_context will not block flush_all()

    // As declaration for wait_for contains "It may also be unblocked
    // spuriously." for all platforms
    while (!commit && result == std::cv_status::no_timeout)
      result = field.cond.wait_for(field_cond_lock, 100ms);

    ASSERT_EQ(std::cv_status::timeout, result);
    field_cond_lock
      .unlock();  // verify commit() finishes FIXME TODO use below
                  // once segment_context will not block flush_all()
    // ASSERT_EQ(std::cv_status::no_timeout, result); // verify
    // commit() finishes
    //  FIXME TODO add once segment_context will not block flush_all()
    // ASSERT_TRUE(commit);
    {
      irs::IndexWriter::Transaction(std::move(ctx));
    }  // release ctx before join() in case of test failure
    thread1.join();
    // FIXME TODO add once segment_context will not block flush_all()
    // writer->Commit(); AssertSnapshotEquality(*writer); // commit doc removal

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // holding document_context after replace across commit does not block (single
  // doc)
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    auto ctx = writer->GetBatch();
    // wait for insertion to start
    auto field_cond_lock = std::unique_lock{field.cond_mutex};

    {
      auto doc = ctx.Replace(*(query_doc1));
      doc.Insert<irs::Action::INDEX>(doc2->indexed.begin(),
                                     doc2->indexed.end());
      doc.Insert<irs::Action::STORE>(doc2->stored.begin(), doc2->stored.end());
    }
    std::atomic<bool> commit(false);  // FIXME TODO remove once segment_context
                                      // will not block flush_all()
    std::thread thread1([&]() -> void {
      writer->Commit();
      AssertSnapshotEquality(*writer);
      commit = true;
      auto lock = std::lock_guard{field.cond_mutex};
      field.cond.notify_all();
    });

    auto result = field.cond.wait_for(
      field_cond_lock,
      std::chrono::milliseconds(
        10000));  // verify commit() finishes FIXME TODO remove once
                  // segment_context will not block flush_all()

    // override spurious wakeup
    while (!commit && result == std::cv_status::no_timeout)
      result = field.cond.wait_for(field_cond_lock, 100ms);

    ASSERT_EQ(std::cv_status::timeout, result);
    field_cond_lock
      .unlock();  // verify commit() finishes FIXME TODO use below
                  // once segment_context will not block flush_all()
    // ASSERT_EQ(std::cv_status::no_timeout, result); // verify
    // commit() finishes
    //  FIXME TODO add once segment_context will not block
    //  flush_all()
    // ASSERT_TRUE(commit);
    {
      irs::IndexWriter::Transaction(std::move(ctx));
    }  // release ctx before join() in case of test failure
    thread1.join();
    // FIXME TODO add once segment_context will not block
    // flush_all() writer->Commit(); AssertSnapshotEquality(*writer); // commit
    // doc replace

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // rollback empty
  {
    auto writer = open_writer();

    {
      auto ctx = writer->GetBatch();

      ctx.Reset();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc1->indexed.begin(),
                                                   doc1->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc1->stored.begin(),
                                                   doc1->stored.end()));
      }
    }

    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // rollback inserts
  {
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    {
      auto ctx = writer->GetBatch();

      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc2->indexed.begin(),
                                                   doc2->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc2->stored.begin(),
                                                   doc2->stored.end()));
      }
      ctx.Reset();
    }

    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // rollback inserts + some more
  {
    auto writer = open_writer();

    {
      auto ctx = writer->GetBatch();

      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc1->indexed.begin(),
                                                   doc1->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc1->stored.begin(),
                                                   doc1->stored.end()));
      }
      ctx.Reset();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc2->indexed.begin(),
                                                   doc2->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc2->stored.begin(),
                                                   doc2->stored.end()));
      }
    }

    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // rollback multiple inserts + some more
  {
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    {
      auto ctx = writer->GetBatch();

      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc2->indexed.begin(),
                                                   doc2->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc2->stored.begin(),
                                                   doc2->stored.end()));
      }
      ctx.Reset();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc3->indexed.begin(),
                                                   doc3->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc3->stored.begin(),
                                                   doc3->stored.end()));
      }
      ctx.Reset();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc4->indexed.begin(),
                                                   doc4->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc4->stored.begin(),
                                                   doc4->stored.end()));
      }
    }

    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc1
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("D", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc4
    ASSERT_FALSE(docs_itr->next());
  }

  // rollback inserts split over multiple segment_writers
  {
    irs::IndexWriterOptions options;
    options.segment_docs_max = 1;  // each doc will have its own segment
    auto writer = open_writer(irs::kOmCreate, options);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    {
      auto ctx = writer->GetBatch();

      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc2->indexed.begin(),
                                                   doc2->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc2->stored.begin(),
                                                   doc2->stored.end()));
      }
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc3->indexed.begin(),
                                                   doc3->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc3->stored.begin(),
                                                   doc3->stored.end()));
      }
      ctx.Reset();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc4->indexed.begin(),
                                                   doc4->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc4->stored.begin(),
                                                   doc4->stored.end()));
      }
    }

    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of first segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of second segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("D",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc4
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // rollback removals
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    {
      auto ctx = writer->GetBatch();

      ctx.Remove(*(query_doc1));
      ctx.Reset();
    }

    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // rollback removals + some more
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    {
      auto ctx = writer->GetBatch();

      ctx.Remove(*(query_doc1));
      ctx.Reset();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc2->indexed.begin(),
                                                   doc2->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc2->stored.begin(),
                                                   doc2->stored.end()));
      }
    }

    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc1
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // rollback removals split over multiple segment_writers
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto query_doc2 = MakeByTerm("name", "B");
    irs::IndexWriterOptions options;
    options.segment_docs_max = 1;  // each doc will have its own segment
    auto writer = open_writer(irs::kOmCreate, options);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    {
      auto ctx = writer->GetBatch();

      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc2->indexed.begin(),
                                                   doc2->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc2->stored.begin(),
                                                   doc2->stored.end()));
      }
      ctx.Remove(*(query_doc1));
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc3->indexed.begin(),
                                                   doc3->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc3->stored.begin(),
                                                   doc3->stored.end()));
      }
      ctx.Remove(*(query_doc2));
      ctx.Reset();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc4->indexed.begin(),
                                                   doc4->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc4->stored.begin(),
                                                   doc4->stored.end()));
      }
    }

    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of first segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of second segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("D",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc4
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // rollback replace (single doc)
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    {
      auto ctx = writer->GetBatch();

      {
        auto doc = ctx.Replace(*(query_doc1));
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc2->indexed.begin(),
                                                   doc2->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc2->stored.begin(),
                                                   doc2->stored.end()));
      }
      ctx.Reset();
    }

    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // rollback replace (single doc) + some more
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    {
      auto ctx = writer->GetBatch();

      {
        auto doc = ctx.Replace(*(query_doc1));
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc2->indexed.begin(),
                                                   doc2->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc2->stored.begin(),
                                                   doc2->stored.end()));
      }
      ctx.Reset();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc3->indexed.begin(),
                                                   doc3->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc3->stored.begin(),
                                                   doc3->stored.end()));
      }
      ASSERT_TRUE(ctx.Commit());
    }

    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc1
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("C", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc3
    ASSERT_FALSE(docs_itr->next());
  }

  // rollback flushed but not committed doc
  {
    irs::IndexWriterOptions options;
    options.segment_docs_max = 2;
    auto writer = open_writer(irs::kOmCreate, options);
    {
      auto ctx = writer->GetBatch();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc1->indexed.begin(),
                                                   doc1->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc1->stored.begin(),
                                                   doc1->stored.end()));
      }
      ctx.Commit();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc2->indexed.begin(),
                                                   doc2->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc2->stored.begin(),
                                                   doc2->stored.end()));
      }
      // implicit flush
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc3->indexed.begin(),
                                                   doc3->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc3->stored.begin(),
                                                   doc3->stored.end()));
      }
      ctx.Reset();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc4->indexed.begin(),
                                                   doc4->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc4->stored.begin(),
                                                   doc4->stored.end()));
      }
      // implicit commit and flush
    }

    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(2, reader.size());
    EXPECT_EQ(3, reader.docs_count());
    EXPECT_EQ(2, reader.live_docs_count());

    {
      auto& segment = reader[0];  // assume 0 is id of first segment
      EXPECT_EQ(2, segment.docs_count());
      EXPECT_EQ(1, segment.live_docs_count());
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of second segment
      EXPECT_EQ(1, segment.docs_count());
      EXPECT_EQ(1, segment.live_docs_count());
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("D",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc4
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // rollback flushed but not committed doc
  {
    constexpr size_t kWordSize = 256;
    irs::IndexWriterOptions options;
    options.segment_docs_max = kWordSize + 2;
    auto writer = open_writer(irs::kOmCreate, options);
    {
      auto ctx = writer->GetBatch();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc1->indexed.begin(),
                                                   doc1->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc1->stored.begin(),
                                                   doc1->stored.end()));
      }
      ctx.Commit();
      for (size_t i = 0; i != kWordSize; ++i) {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc2->indexed.begin(),
                                                   doc2->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc2->stored.begin(),
                                                   doc2->stored.end()));
      }
      ctx.Reset();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc1->indexed.begin(),
                                                   doc1->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc1->stored.begin(),
                                                   doc1->stored.end()));
      }
      ctx.Commit();
    }

    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    EXPECT_EQ(2 + kWordSize, reader.docs_count());
    EXPECT_EQ(2, reader.live_docs_count());

    {
      auto& segment = reader[0];  // assume 0 is id of first segment
      EXPECT_EQ(2 + kWordSize, segment.docs_count());
      EXPECT_EQ(2, segment.live_docs_count());
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      for (size_t i = 0; i != 2; ++i) {
        ASSERT_TRUE(docs_itr->next()) << i;
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value())) << i;
        // 'name' value in doc1
        ASSERT_EQ("A",
                  irs::ToString<std::string_view>(actual_value->value.data()))
          << i;
      }
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // rollback replacements (single doc) split over multiple segment_writers
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto query_doc2 = MakeByTerm("name", "B");
    irs::IndexWriterOptions options;
    options.segment_docs_max = 1;  // each doc will have its own segment
    auto writer = open_writer(irs::kOmCreate, options);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    {
      auto ctx = writer->GetBatch();

      {
        auto doc = ctx.Replace(*(query_doc1));
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc2->indexed.begin(),
                                                   doc2->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc2->stored.begin(),
                                                   doc2->stored.end()));
      }
      {
        auto doc = ctx.Replace(*(query_doc2));
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc3->indexed.begin(),
                                                   doc3->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc3->stored.begin(),
                                                   doc3->stored.end()));
      }
      ctx.Reset();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc4->indexed.begin(),
                                                   doc4->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc4->stored.begin(),
                                                   doc4->stored.end()));
      }
    }

    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of first segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of second segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("D",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc4
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // segment flush due to memory bytes limit (same flush_context)
  {
    irs::IndexWriterOptions options;
    options.segment_memory_max = 1;  // arbitaty size < 1 document (first doc
                                     // will always aquire a new SegmentWriter)
    auto writer = open_writer(irs::kOmCreate, options);

    {
      auto ctx = writer->GetBatch();

      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc1->indexed.begin(),
                                                   doc1->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc1->stored.begin(),
                                                   doc1->stored.end()));
      }
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc2->indexed.begin(),
                                                   doc2->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc2->stored.begin(),
                                                   doc2->stored.end()));
      }
    }

    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of first segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of second segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // segment flush due to memory bytes limit (split over different
  // flush_contexts)
  {
    irs::IndexWriterOptions options;
    options.segment_memory_max = 1;  // arbitaty size < 1 document (first doc
                                     // will always aquire a new SegmentWriter)
    auto writer = open_writer(irs::kOmCreate, options);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    /* FIXME TODO use below once segment_context will not block
       flush_all()
        {
          auto ctx = writer->GetBatch(); // will reuse
       segment_context from above

          {
            auto doc = ctx.Insert();
            ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc2->indexed.begin(),
       doc2->indexed.end()));
            ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc2->stored.begin(),
       doc2->stored.end()));
          }
          writer->Commit(); AssertSnapshotEquality(*writer);
          {
            auto doc = ctx.Insert();
            ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc3->indexed.begin(),
       doc3->indexed.end()));
            ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc3->stored.begin(),
       doc3->stored.end()));
          }
        }

        writer->Commit(); AssertSnapshotEquality(*writer);

        auto reader = irs::directory_reader::open(dir(), codec());
        ASSERT_EQ(3, reader.size());

        {
          auto& segment = reader[0]; // assume 0 is id of first
       segment const auto* column = segment.column("name");
          ASSERT_NE(nullptr, column);
          auto values = column->iterator(irs::ColumnHint::kNormal);
          ASSERT_NE(nullptr, values);
          auto* actual_value = irs::get<irs::PayAttr>(*values);
          ASSERT_NE(nullptr, actual_value);
          auto terms = segment.field("same");
          ASSERT_NE(nullptr, terms);
          auto termItr = terms->iterator(irs::SeekMode::NORMAL);
          ASSERT_TRUE(termItr->next());
          auto docsItr =
       segment.mask(termItr->postings(irs::IndexFeatures::DOCS));
          ASSERT_TRUE(docsItr->next());
          ASSERT_EQ(docsItr->value(),
       values->seek(docsItr->value())); ASSERT_EQ("A",
       irs::ToString<std::string_view>(actual_value->value.data()));
       // 'name' value in doc1 ASSERT_FALSE(docsItr->next());
        }

        {
          auto& segment = reader[1]; // assume 1 is id of second
       segment const auto* column = segment.column("name");
          ASSERT_NE(nullptr, column);
          auto values = column->iterator(irs::ColumnHint::kNormal);
          ASSERT_NE(nullptr, values);
          auto* actual_value = irs::get<irs::PayAttr>(*values);
          ASSERT_NE(nullptr, actual_value);
          auto terms = segment.field("same");
          ASSERT_NE(nullptr, terms);
          auto termItr = terms->iterator(irs::SeekMode::NORMAL);
          ASSERT_TRUE(termItr->next());
          auto docsItr =
       termItr->postings(irs::IndexFeatures::DOCS);
          ASSERT_TRUE(docsItr->next());
          ASSERT_EQ(docsItr->value(),
       values->seek(docsItr->value())); ASSERT_EQ("B",
       irs::ToString<std::string_view>(actual_value->value.data()));
       // 'name' value in doc2 ASSERT_FALSE(docsItr->next());
        }

        {
          auto& segment = reader[2]; // assume 2 is id of third
       segment const auto* column = segment.column("name");
          ASSERT_NE(nullptr, column);
          auto values = column->iterator(irs::ColumnHint::kNormal);
          ASSERT_NE(nullptr, values);
          auto* actual_value = irs::get<irs::PayAttr>(*values);
          ASSERT_NE(nullptr, actual_value);
          auto terms = segment.field("same");
          ASSERT_NE(nullptr, terms);
          auto termItr = terms->iterator(irs::SeekMode::NORMAL);
          ASSERT_TRUE(termItr->next());
          auto docsItr =
       termItr->postings(irs::IndexFeatures::DOCS);
          ASSERT_TRUE(docsItr->next());
          ASSERT_EQ(docsItr->value(),
       values->seek(docsItr->value())); ASSERT_EQ("C",
       irs::ToString<std::string_view>(actual_value->value.data()));
       // 'name' value in doc3 ASSERT_FALSE(docsItr->next());
        }
    */
  }

  // segment flush due to document count limit (same flush_context)
  {
    irs::IndexWriterOptions options;
    options.segment_docs_max = 1;  // each doc will have its own segment
    auto writer = open_writer(irs::kOmCreate, options);

    {
      auto ctx = writer->GetBatch();

      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc1->indexed.begin(),
                                                   doc1->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc1->stored.begin(),
                                                   doc1->stored.end()));
      }
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc2->indexed.begin(),
                                                   doc2->indexed.end()));
        ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc2->stored.begin(),
                                                   doc2->stored.end()));
      }
    }

    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of first segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of second segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // segment flush due to document count limit (split over different
  // flush_contexts)
  {
    irs::IndexWriterOptions options;
    options.segment_docs_max = 1;  // each doc will have its own segment
    auto writer = open_writer(irs::kOmCreate, options);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    /* FIXME TODO use below once segment_context will not block
       flush_all()
        {
          auto ctx = writer->GetBatch(); // will reuse
       segment_context from above

          {
            auto doc = ctx.Insert();
            ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc2->indexed.begin(),
       doc2->indexed.end()));
            ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc2->stored.begin(),
       doc2->stored.end()));
          }
          writer->Commit(); AssertSnapshotEquality(*writer);
          {
            auto doc = ctx.Insert();
            ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc3->indexed.begin(),
       doc3->indexed.end()));
            ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc3->stored.begin(),
       doc3->stored.end()));
          }
        }

        writer->Commit(); AssertSnapshotEquality(*writer);

        auto reader = irs::directory_reader::open(dir(), codec());
        ASSERT_EQ(3, reader.size());

        {
          auto& segment = reader[0]; // assume 0 is id of first
       segment const auto* column = segment.column("name");
          ASSERT_NE(nullptr, column);
          auto values = column->iterator(irs::ColumnHint::kNormal);
          ASSERT_NE(nullptr, values);
          auto* actual_value = irs::get<irs::PayAttr>(*values);
          ASSERT_NE(nullptr, actual_value);
          auto terms = segment.field("same");
          ASSERT_NE(nullptr, terms);
          auto termItr = terms->iterator(irs::SeekMode::NORMAL);
          ASSERT_TRUE(termItr->next());
          auto docsItr =
       segment.mask(termItr->postings(irs::IndexFeatures::DOCS));
          ASSERT_TRUE(docsItr->next());
          ASSERT_EQ(docsItr->value(),
       values->seek(docsItr->value())); ASSERT_EQ("A",
       irs::ToString<std::string_view>(actual_value->value.data()));
       // 'name' value in doc1 ASSERT_FALSE(docsItr->next());
        }

        {
          auto& segment = reader[1]; // assume 1 is id of second
       segment const auto* column = segment.column("name");
          ASSERT_NE(nullptr, column);
          auto values = column->iterator(irs::ColumnHint::kNormal);
          ASSERT_NE(nullptr, values);
          auto* actual_value = irs::get<irs::PayAttr>(*values);
          ASSERT_NE(nullptr, actual_value);
          auto terms = segment.field("same");
          ASSERT_NE(nullptr, terms);
          auto termItr = terms->iterator(irs::SeekMode::NORMAL);
          ASSERT_TRUE(termItr->next());
          auto docsItr =
       termItr->postings(irs::IndexFeatures::DOCS);
          ASSERT_TRUE(docsItr->next());
          ASSERT_EQ(docsItr->value(),
       values->seek(docsItr->value())); ASSERT_EQ("B",
       irs::ToString<std::string_view>(actual_value->value.data()));
       // 'name' value in doc2 ASSERT_FALSE(docsItr->next());
        }

        {
          auto& segment = reader[2]; // assume 2 is id of third
       segment const auto* column = segment.column("name");
          ASSERT_NE(nullptr, column);
          auto values = column->iterator(irs::ColumnHint::kNormal);
          ASSERT_NE(nullptr, values);
          auto* actual_value = irs::get<irs::PayAttr>(*values);
          ASSERT_NE(nullptr, actual_value);
          auto terms = segment.field("same");
          ASSERT_NE(nullptr, terms);
          auto termItr = terms->iterator(irs::SeekMode::NORMAL);
          ASSERT_TRUE(termItr->next());
          auto docsItr =
       termItr->postings(irs::IndexFeatures::DOCS);
          ASSERT_TRUE(docsItr->next());
          ASSERT_EQ(docsItr->value(),
       values->seek(docsItr->value())); ASSERT_EQ("C",
       irs::ToString<std::string_view>(actual_value->value.data()));
       // 'name' value in doc3 ASSERT_FALSE(docsItr->next());
        }
    */
  }

  // reuse of same segment initially with indexed fields then with only stored
  // fileds
  {
    auto writer = open_writer();
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);  // ensure flush() is called
    writer->GetBatch().Insert().Insert<irs::Action::STORE>(
      doc2->stored.begin(),
      doc2->stored.end());  // document without any indexed
                            // attributes (reuse segment writer)
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of first/old segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 0 is id of first/new segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      std::unordered_set<std::string_view> expected = {"B"};
      ASSERT_EQ(1, column->size());
      ASSERT_TRUE(::Visit(
        *column,
        [&expected](irs::doc_id_t, const irs::bytes_view& data) -> bool {
          auto* value = data.data();
          auto actual_value = irs::VReadString<std::string_view>(value);
          return 1 == expected.erase(actual_value);
        }));
      ASSERT_TRUE(expected.empty());
    }
  }
}

TEST_P(IndexTestCase, get_term) {
  {
    tests::JsonDocGenerator gen(
      resource("simple_sequential.json"),
      [](tests::Document& doc, const std::string& name,
         const tests::JsonDocGenerator::JsonValue& data) {
        if (data.is_string()) {
          doc.insert(std::make_shared<tests::StringField>(name, data.str));
        }
      });

    add_segment(gen);
  }

  auto reader = open_reader();
  ASSERT_EQ(1, reader.size());
  auto& segment = (*reader)[0];
  auto* field = segment.field("name");
  ASSERT_NE(nullptr, field);

  {
    const auto meta = field->term(irs::ViewCast<irs::byte_type>("invalid"sv));
    ASSERT_EQ(0, meta.docs_count);
    ASSERT_EQ(0, meta.freq);
  }

  {
    const auto meta = field->term(irs::ViewCast<irs::byte_type>("A"sv));
    ASSERT_EQ(1, meta.docs_count);
    ASSERT_EQ(1, meta.freq);
  }
}

TEST_P(IndexTestCase, read_documents) {
  {
    tests::JsonDocGenerator gen(
      resource("simple_sequential.json"),
      [](tests::Document& doc, const std::string& name,
         const tests::JsonDocGenerator::JsonValue& data) {
        if (data.is_string()) {
          doc.insert(std::make_shared<tests::StringField>(name, data.str));
        }
      });

    add_segment(gen);
  }

  auto reader = open_reader();
  ASSERT_EQ(1, reader.size());
  auto& segment = (*reader)[0];

  // no term
  {
    std::array<irs::doc_id_t, 10> docs{};
    auto* field = segment.field("name");
    ASSERT_NE(nullptr, field);
    const auto term = irs::ViewCast<irs::byte_type>("invalid"sv);
    const auto size = field->read_documents(term, docs);
    ASSERT_EQ(0, size);
    ASSERT_TRUE(
      std::all_of(docs.begin(), docs.end(), [](auto v) { return v == 0; }));
  }

  // singleton term
  {
    std::array<irs::doc_id_t, 10> docs{};
    auto* field = segment.field("name");
    ASSERT_NE(nullptr, field);
    const auto term = irs::ViewCast<irs::byte_type>("A"sv);
    const auto size = field->read_documents(term, docs);
    ASSERT_EQ(1, size);
    ASSERT_EQ(1, docs.front());
    ASSERT_TRUE(std::all_of(std::next(docs.begin(), size), docs.end(),
                            [](auto v) { return v == 0; }));
  }

  // singleton term
  {
    std::array<irs::doc_id_t, 10> docs{};
    auto* field = segment.field("name");
    ASSERT_NE(nullptr, field);
    const auto term = irs::ViewCast<irs::byte_type>("C"sv);
    const auto size = field->read_documents(term, docs);
    ASSERT_EQ(1, size);
    ASSERT_EQ(3, docs.front());
    ASSERT_TRUE(std::all_of(std::next(docs.begin(), size), docs.end(),
                            [](auto v) { return v == 0; }));
  }

  // regular term
  {
    std::array<irs::doc_id_t, 10> docs{};
    auto* field = segment.field("duplicated");
    ASSERT_NE(nullptr, field);
    const auto term = irs::ViewCast<irs::byte_type>("abcd"sv);
    const auto size = field->read_documents(term, docs);
    ASSERT_EQ(6, size);
    ASSERT_EQ(1, docs[0]);
    ASSERT_EQ(5, docs[1]);
    ASSERT_EQ(11, docs[2]);
    ASSERT_EQ(21, docs[3]);
    ASSERT_EQ(27, docs[4]);
    ASSERT_EQ(31, docs[5]);
    ASSERT_TRUE(std::all_of(std::next(docs.begin(), size), docs.end(),
                            [](auto v) { return v == 0; }));
  }

  // regular term, less requested
  {
    std::array<irs::doc_id_t, 3> docs{};
    auto* field = segment.field("duplicated");
    ASSERT_NE(nullptr, field);
    const auto term = irs::ViewCast<irs::byte_type>("abcd"sv);
    const auto size = field->read_documents(term, docs);
    ASSERT_EQ(3, size);
    ASSERT_EQ(1, docs[0]);
    ASSERT_EQ(5, docs[1]);
    ASSERT_EQ(11, docs[2]);
  }

  // regular term, nothing requested
  {
    std::array<irs::doc_id_t, 10> docs{};
    auto* field = segment.field("duplicated");
    ASSERT_NE(nullptr, field);
    const auto term = irs::ViewCast<irs::byte_type>("abcd"sv);
    const auto size =
      field->read_documents(term, std::span{docs.data(), size_t{0}});
    ASSERT_EQ(0, size);
    ASSERT_TRUE(
      std::all_of(docs.begin(), docs.end(), [](auto v) { return v == 0; }));
  }

  {
    auto* field = segment.field("duplicated");
    ASSERT_NE(nullptr, field);
    const auto term = irs::ViewCast<irs::byte_type>("abcd"sv);
    const auto size = field->read_documents(term, {});
    ASSERT_EQ(0, size);
  }
}

TEST_P(IndexTestCase, doc_removal) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
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

  // new segment: add
  {
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // new segment: add + remove 1st (as reference)
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->GetBatch().Remove(*(query_doc1.get()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // new segment: add + remove 1st (as unique_ptr)
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->GetBatch().Remove(std::move(query_doc1));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // new segment: add + remove 1st (as shared_ptr)
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->GetBatch().Remove(
      std::shared_ptr<irs::Filter>(std::move(query_doc1)));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // new segment: remove + add
  {
    auto query_doc2 = MakeByTerm("name", "B");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->GetBatch().Remove(std::move(query_doc2));  // not present yet
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
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
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // new segment: add + remove + readd
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->GetBatch().Remove(std::move(query_doc1));
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // new segment: add + remove, old segment: remove
  {
    auto query_doc2 = MakeByTerm("name", "B");
    auto query_doc3 = MakeByTerm("name", "C");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    writer->GetBatch().Remove(std::move(query_doc3));
    writer->Commit();
    AssertSnapshotEquality(*writer);  // document mask with 'doc3' created
    writer->GetBatch().Remove(std::move(query_doc2));
    writer->Commit();
    AssertSnapshotEquality(
      *writer);  // new document mask with 'doc2','doc3' created

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // new segment: add + add, old segment: remove + remove + add
  {
    auto query_doc1_doc2 = MakeByTermOrByTerm("name", "A", "name", "B");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    writer->GetBatch().Remove(std::move(query_doc1_doc2));
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
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

  // new segment: add, old segment: remove
  {
    auto query_doc2 = MakeByTerm("name", "B");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    writer->GetBatch().Remove(std::move(query_doc2));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of old segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of new segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("C",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // new segment: add + remove, old segment: remove
  {
    auto query_doc1_doc3 = MakeByTermOrByTerm("name", "A", "name", "C");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->GetBatch().Remove(std::move(query_doc1_doc3));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of old segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of new segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("D",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc4
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // new segment: add + remove, old segment: add + remove old-old
  // segment: remove
  {
    auto query_doc2_doc6_doc9 =
      MakeOr({{"name", "B"}, {"name", "F"}, {"name", "I"}});
    auto query_doc3_doc7 = MakeByTermOrByTerm("name", "C", "name", "G");
    auto query_doc4 = MakeByTerm("name", "D");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(),
                       doc1->stored.end()));  // A
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(),
                       doc2->stored.end()));  // B
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(),
                       doc3->stored.end()));  // C
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(),
                       doc4->stored.end()));  // D
    writer->GetBatch().Remove(std::move(query_doc4));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc5->indexed.begin(), doc5->indexed.end(),
                       doc5->stored.begin(),
                       doc5->stored.end()));  // E
    ASSERT_TRUE(Insert(*writer, doc6->indexed.begin(), doc6->indexed.end(),
                       doc6->stored.begin(),
                       doc6->stored.end()));  // F
    ASSERT_TRUE(Insert(*writer, doc7->indexed.begin(), doc7->indexed.end(),
                       doc7->stored.begin(),
                       doc7->stored.end()));  // G
    writer->GetBatch().Remove(std::move(query_doc3_doc7));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc8->indexed.begin(), doc8->indexed.end(),
                       doc8->stored.begin(),
                       doc8->stored.end()));  // H
    ASSERT_TRUE(Insert(*writer, doc9->indexed.begin(), doc9->indexed.end(),
                       doc9->stored.begin(),
                       doc9->stored.end()));  // I
    writer->GetBatch().Remove(std::move(query_doc2_doc6_doc9));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(3, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of old-old segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of old segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("E",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc5
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[2];  // assume 2 is id of new segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("H",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc8
      ASSERT_FALSE(docs_itr->next());
    }
  }
}

TEST_P(IndexTestCase, doc_update) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();
  const tests::Document* doc4 = gen.next();

  // another shitty case for update
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto query_doc2 = MakeByTerm("name", "B");
    auto query_doc3 = MakeByTerm("name", "C");
    auto query_doc4 = MakeByTerm("name", "D");
    auto query_doc5 = MakeByTerm("name", "E");
    auto writer = open_writer();

    auto trx1 = writer->GetBatch();
    auto trx2 = writer->GetBatch();
    auto trx3 = writer->GetBatch();
    auto trx4 = writer->GetBatch();

    {
      auto doc = trx1.Insert();
      doc.Insert<irs::Action::INDEX>(doc1->indexed.begin(),
                                     doc1->indexed.end());
      doc.Insert<irs::Action::STORE>(doc1->stored.begin(), doc1->stored.end());
    }
    {
      auto doc = trx1.Insert();
      doc.Insert<irs::Action::INDEX>(doc2->indexed.begin(),
                                     doc2->indexed.end());
      doc.Insert<irs::Action::STORE>(doc2->stored.begin(), doc2->stored.end());
      trx1.Remove(*query_doc2);
    }
    {
      auto doc = trx3.Insert();
      doc.Insert<irs::Action::INDEX>(doc2->indexed.begin(),
                                     doc2->indexed.end());
      doc.Insert<irs::Action::STORE>(doc2->stored.begin(), doc2->stored.end());
    }
    {
      auto doc = trx4.Replace(*query_doc2);
      doc.Insert<irs::Action::INDEX>(doc3->indexed.begin(),
                                     doc3->indexed.end());
      doc.Insert<irs::Action::STORE>(doc3->stored.begin(), doc3->stored.end());
    }
    {
      auto doc = trx2.Replace(*query_doc3);
      doc.Insert<irs::Action::INDEX>(doc4->indexed.begin(),
                                     doc4->indexed.end());
      doc.Insert<irs::Action::STORE>(doc4->stored.begin(), doc4->stored.end());
    }

    // only to deterministic emulate such situation,
    // it's possible without RegisterFlush
    trx1.RegisterFlush();
    trx4.RegisterFlush();
    trx2.RegisterFlush();
    trx3.RegisterFlush();
    trx1.Commit();
    trx3.Commit();
    trx4.Commit();
    trx2.Commit();

    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    EXPECT_EQ(reader.size(), 2);
    EXPECT_EQ(reader.live_docs_count(), 2);
  }

  // new segment update (as reference)
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Update(*writer, *(query_doc1.get()), doc2->indexed.begin(),
                       doc2->indexed.end(), doc2->stored.begin(),
                       doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // new segment update (as unique_ptr)
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Update(*writer, std::move(query_doc1), doc2->indexed.begin(),
                       doc2->indexed.end(), doc2->stored.begin(),
                       doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // new segment update (as shared_ptr)
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Update(*writer,
                       std::shared_ptr<irs::Filter>(std::move(query_doc1)),
                       doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // old segment update
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Update(*writer, std::move(query_doc1), doc3->indexed.begin(),
                       doc3->indexed.end(), doc3->stored.begin(),
                       doc3->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of old segment
      auto terms = segment.field("same");
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of new segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("C",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // 3x updates (same segment)
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto query_doc2 = MakeByTerm("name", "B");
    auto query_doc3 = MakeByTerm("name", "C");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Update(*writer, std::move(query_doc1), doc2->indexed.begin(),
                       doc2->indexed.end(), doc2->stored.begin(),
                       doc2->stored.end()));
    ASSERT_TRUE(Update(*writer, std::move(query_doc2), doc3->indexed.begin(),
                       doc3->indexed.end(), doc3->stored.begin(),
                       doc3->stored.end()));
    ASSERT_TRUE(Update(*writer, std::move(query_doc3), doc4->indexed.begin(),
                       doc4->indexed.end(), doc4->stored.begin(),
                       doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("D", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc4
    ASSERT_FALSE(docs_itr->next());
  }

  // 3x updates (different segments)
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto query_doc2 = MakeByTerm("name", "B");
    auto query_doc3 = MakeByTerm("name", "C");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Update(*writer, std::move(query_doc1), doc2->indexed.begin(),
                       doc2->indexed.end(), doc2->stored.begin(),
                       doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Update(*writer, std::move(query_doc2), doc3->indexed.begin(),
                       doc3->indexed.end(), doc3->stored.begin(),
                       doc3->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Update(*writer, std::move(query_doc3), doc4->indexed.begin(),
                       doc4->indexed.end(), doc4->stored.begin(),
                       doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("D", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc4
    ASSERT_FALSE(docs_itr->next());
  }

  // no matching documnts
  {
    auto query_doc2 = MakeByTerm("name", "B");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Update(*writer, std::move(query_doc2), doc2->indexed.begin(),
                       doc2->indexed.end(), doc2->stored.begin(),
                       doc2->stored.end()));  // non-existent document
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size()) << reader.live_docs_count();
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // update + delete (same segment)
  {
    auto query_doc2 = MakeByTerm("name", "B");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    ASSERT_TRUE(Update(*writer, *(query_doc2), doc3->indexed.begin(),
                       doc3->indexed.end(), doc3->stored.begin(),
                       doc3->stored.end()));
    writer->GetBatch().Remove(*(query_doc2));  // remove no longer existent
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc1
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("C", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc3
    ASSERT_FALSE(docs_itr->next());
  }

  // update + delete (different segments)
  {
    auto query_doc2 = MakeByTerm("name", "B");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Update(*writer, *(query_doc2), doc3->indexed.begin(),
                       doc3->indexed.end(), doc3->stored.begin(),
                       doc3->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    writer->GetBatch().Remove(*(query_doc2));  // remove no longer existent
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of old segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of new segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("C",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // delete + update (same segment)
  {
    auto query_doc2 = MakeByTerm("name", "B");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->GetBatch().Remove(*(query_doc2));
    ASSERT_TRUE(Update(*writer, *(query_doc2), doc3->indexed.begin(),
                       doc3->indexed.end(), doc3->stored.begin(),
                       doc3->stored.end()));  // update no longer existent
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // delete + update (different segments)
  {
    auto query_doc2 = MakeByTerm("name", "B");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    writer->GetBatch().Remove(*(query_doc2));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Update(*writer, *(query_doc2), doc3->indexed.begin(),
                       doc3->indexed.end(), doc3->stored.begin(),
                       doc3->stored.end()));  // update no longer existent
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // delete + update then update (2nd - update of modified doc)
  // (same segment)
  {
    auto query_doc2 = MakeByTerm("name", "B");
    auto query_doc3 = MakeByTerm("name", "C");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->GetBatch().Remove(*(query_doc2));
    ASSERT_TRUE(Update(*writer, *(query_doc2), doc3->indexed.begin(),
                       doc3->indexed.end(), doc3->stored.begin(),
                       doc3->stored.end()));
    ASSERT_TRUE(Update(*writer, *(query_doc3), doc4->indexed.begin(),
                       doc4->indexed.end(), doc4->stored.begin(),
                       doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // delete + update then update (2nd - update of modified doc)
  // (different segments)
  {
    auto query_doc2 = MakeByTerm("name", "B");
    auto query_doc3 = MakeByTerm("name", "C");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    writer->GetBatch().Remove(*(query_doc2));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Update(*writer, *(query_doc2), doc3->indexed.begin(),
                       doc3->indexed.end(), doc3->stored.begin(),
                       doc3->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Update(*writer, *(query_doc3), doc4->indexed.begin(),
                       doc4->indexed.end(), doc4->stored.begin(),
                       doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // new segment failed update (due to field features mismatch or
  // failed serializer)
  {
    class TestField : public tests::FieldBase {
     public:
      irs::StringTokenizer tokens;
      bool write_result;
      bool Write(irs::DataOutput& out) const final {
        out.WriteByte(1);
        return write_result;
      }
      irs::Tokenizer& GetTokens() const final {
        return const_cast<TestField*>(this)->tokens;
      }
    };

    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    auto doc1 = gen.next();
    auto doc2 = gen.next();
    auto doc3 = gen.next();
    auto doc4 = gen.next();
    auto query_doc1 = MakeByTerm("name", "A");

    irs::IndexWriterOptions opts;

    auto writer = open_writer(irs::kOmCreate, opts);
    auto test_field0 = std::make_shared<TestField>();
    auto test_field1 = std::make_shared<TestField>();
    auto test_field2 = std::make_shared<TestField>();
    auto test_field3 = std::make_shared<TestField>();
    std::string test_field_name("test_field");

    test_field0->index_features =
      irs::IndexFeatures::Freq | irs::IndexFeatures::Offs;  // feature superset
    test_field1->index_features =
      irs::IndexFeatures::Freq;  // feature subset of 'test_field0'
    test_field2->index_features =
      irs::IndexFeatures::Freq | irs::IndexFeatures::Offs;
    test_field3->index_features =
      irs::IndexFeatures::Freq | irs::IndexFeatures::Pay;
    test_field0->Name(test_field_name);
    test_field1->Name(test_field_name);
    test_field2->Name(test_field_name);
    test_field3->Name(test_field_name);
    test_field0->tokens.reset("data");
    test_field1->tokens.reset("data");
    test_field2->tokens.reset("data");
    test_field3->tokens.reset("data");
    test_field0->write_result = true;
    test_field1->write_result = true;
    test_field2->write_result = false;
    test_field3->write_result = true;

    const_cast<tests::Document*>(doc1)->insert(test_field0, true,
                                               true);  // inject field
    const_cast<tests::Document*>(doc2)->insert(test_field1, true,
                                               true);  // inject field
    const_cast<tests::Document*>(doc3)->insert(test_field2, true,
                                               true);  // inject field
    const_cast<tests::Document*>(doc4)->insert(test_field3, true,
                                               true);  // inject field

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(),
                       doc2->stored.end()));  // index features subset
    ASSERT_FALSE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                        doc3->stored.begin(),
                        doc3->stored.end()));  // serializer returs false
    ASSERT_FALSE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                        doc4->stored.begin(),
                        doc4->stored.end()));  // index features differ
    ASSERT_FALSE(Update(*writer, *(query_doc1.get()), doc3->indexed.begin(),
                        doc3->indexed.end(), doc3->stored.begin(),
                        doc3->stored.end()));
    ASSERT_TRUE(writer->Commit());
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    ASSERT_EQ(5, segment.docs_count());
    ASSERT_EQ(2, segment.live_docs_count());
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc1
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
    ASSERT_FALSE(docs_itr->next());
  }
}

TEST_P(IndexTestCase, import_reader) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();
  const tests::Document* doc4 = gen.next();

  // add a reader with 1 segment no docs
  {
    irs::MemoryDirectory data_dir;
    auto data_writer =
      irs::IndexWriter::Make(data_dir, codec(), irs::kOmCreate);
    auto writer = open_writer();

    writer->Commit();
    AssertSnapshotEquality(*writer);  // ensure the writer has an initial
                                      // completed state

    // check meta counter
    {
      irs::IndexMeta meta;
      std::string filename;
      auto meta_reader = codec()->get_index_meta_reader();
      ASSERT_NE(nullptr, meta_reader);
      ASSERT_TRUE(meta_reader->last_segments_file(dir(), filename));
      meta_reader->read(dir(), meta, filename);
      ASSERT_EQ(0, meta.seg_counter);
    }

    data_writer->Commit();
    ASSERT_TRUE(writer->Import(irs::DirectoryReader(data_dir, codec())));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(0, reader.size());
    ASSERT_EQ(0, reader.docs_count());

    // insert a document and check the meta counter again
    {
      ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                         doc1->stored.begin(), doc1->stored.end()));
      writer->Commit();
      AssertSnapshotEquality(*writer);

      irs::IndexMeta meta;
      std::string filename;
      auto meta_reader = codec()->get_index_meta_reader();
      ASSERT_NE(nullptr, meta_reader);
      ASSERT_TRUE(meta_reader->last_segments_file(dir(), filename));
      meta_reader->read(dir(), meta, filename);
      ASSERT_EQ(1, meta.seg_counter);
    }
  }

  // add a reader with 1 segment no live-docs
  {
    auto query_doc1 = MakeByTerm("name", "A");
    irs::MemoryDirectory data_dir;
    auto data_writer =
      irs::IndexWriter::Make(data_dir, codec(), irs::kOmCreate);
    auto writer = open_writer();

    writer->Commit();
    AssertSnapshotEquality(*writer);  // ensure the writer has an initial
                                      // completed state

    // check meta counter
    {
      irs::IndexMeta meta;
      std::string filename;
      auto meta_reader = codec()->get_index_meta_reader();
      ASSERT_NE(nullptr, meta_reader);
      ASSERT_TRUE(meta_reader->last_segments_file(dir(), filename));
      meta_reader->read(dir(), meta, filename);
      ASSERT_EQ(1, meta.seg_counter);
    }

    ASSERT_TRUE(Insert(*data_writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    data_writer->Commit();
    data_writer->GetBatch().Remove(std::move(query_doc1));
    data_writer->Commit();
    writer->Commit();
    AssertSnapshotEquality(*writer);  // ensure the writer has an initial
                                      // completed state
    ASSERT_TRUE(writer->Import(irs::DirectoryReader(data_dir, codec())));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(0, reader.size());
    ASSERT_EQ(0, reader.docs_count());

    // insert a document and check the meta counter again
    {
      ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                         doc1->stored.begin(), doc1->stored.end()));
      writer->Commit();
      AssertSnapshotEquality(*writer);

      irs::IndexMeta meta;
      std::string filename;
      auto meta_reader = codec()->get_index_meta_reader();
      ASSERT_NE(nullptr, meta_reader);
      ASSERT_TRUE(meta_reader->last_segments_file(dir(), filename));
      meta_reader->read(dir(), meta, filename);
      ASSERT_EQ(2, meta.seg_counter);
    }
  }

  // add a reader with 1 full segment
  {
    irs::MemoryDirectory data_dir;
    auto data_writer =
      irs::IndexWriter::Make(data_dir, codec(), irs::kOmCreate);
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*data_writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*data_writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    data_writer->Commit();
    ASSERT_TRUE(writer->Import(irs::DirectoryReader(data_dir, codec())));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    ASSERT_EQ(2, segment.docs_count());
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
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
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // add a reader with 1 sparse segment
  {
    auto query_doc1 = MakeByTerm("name", "A");
    irs::MemoryDirectory data_dir;
    auto data_writer =
      irs::IndexWriter::Make(data_dir, codec(), irs::kOmCreate);
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*data_writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*data_writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    data_writer->GetBatch().Remove(std::move(query_doc1));
    data_writer->Commit();
    ASSERT_TRUE(writer->Import(irs::DirectoryReader(data_dir, codec())));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    ASSERT_EQ(1, segment.docs_count());
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // add a reader with 2 full segments
  {
    irs::MemoryDirectory data_dir;
    auto data_writer =
      irs::IndexWriter::Make(data_dir, codec(), irs::kOmCreate);
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*data_writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*data_writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    data_writer->Commit();
    ASSERT_TRUE(Insert(*data_writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*data_writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    data_writer->Commit();
    ASSERT_TRUE(writer->Import(irs::DirectoryReader(data_dir, codec())));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    ASSERT_EQ(4, segment.docs_count());
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
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
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("C", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc3
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("D", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc4
    ASSERT_FALSE(docs_itr->next());
  }

  // add a reader with 2 sparse segments
  {
    auto query_doc2_doc3 = MakeOr({{"name", "B"}, {"name", "C"}});
    irs::MemoryDirectory data_dir;
    auto data_writer =
      irs::IndexWriter::Make(data_dir, codec(), irs::kOmCreate);
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*data_writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*data_writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    data_writer->Commit();
    ASSERT_TRUE(Insert(*data_writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*data_writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    data_writer->GetBatch().Remove(std::move(query_doc2_doc3));
    data_writer->Commit();
    ASSERT_TRUE(writer->Import(irs::DirectoryReader(data_dir, codec())));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    ASSERT_EQ(2, segment.docs_count());
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
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
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("D", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc4
    ASSERT_FALSE(docs_itr->next());
  }

  // add a reader with 2 mixed segments
  {
    auto query_doc4 = MakeByTerm("name", "D");
    irs::MemoryDirectory data_dir;
    auto data_writer =
      irs::IndexWriter::Make(data_dir, codec(), irs::kOmCreate);
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*data_writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*data_writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    data_writer->Commit();
    ASSERT_TRUE(Insert(*data_writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*data_writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    data_writer->GetBatch().Remove(std::move(query_doc4));
    data_writer->Commit();
    ASSERT_TRUE(writer->Import(irs::DirectoryReader(data_dir, codec())));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    ASSERT_EQ(3, segment.docs_count());
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
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
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("C", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc3
    ASSERT_FALSE(docs_itr->next());
  }

  // new: add + add + delete, old: import
  {
    auto query_doc2 = MakeByTerm("name", "B");
    irs::MemoryDirectory data_dir;
    auto data_writer =
      irs::IndexWriter::Make(data_dir, codec(), irs::kOmCreate);
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*data_writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*data_writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    data_writer->Commit();
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    writer->GetBatch().Remove(
      std::move(query_doc2));  // should not match any documents
    ASSERT_TRUE(writer->Import(irs::DirectoryReader(data_dir, codec())));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of imported segment
      ASSERT_EQ(2, segment.docs_count());
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of original segment
      ASSERT_EQ(1, segment.docs_count());
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("C",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }
  }
}

TEST_P(IndexTestCase, refresh_reader) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();
  const tests::Document* doc4 = gen.next();

  // initial state (1st segment 2 docs)
  {
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // refreshable reader
  auto reader = irs::DirectoryReader(dir(), codec());

  // validate state
  {
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
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
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // modify state (delete doc2)
  {
    auto writer = open_writer(irs::kOmAppend);
    auto query_doc2 = MakeByTerm("name", "B");

    writer->GetBatch().Remove(std::move(query_doc2));
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }
  // validate state pre/post refresh (existing segment changed)
  {
    ((void)1);  // for clang-format
    {
      ASSERT_EQ(1, reader.size());
      auto& segment = reader[0];  // assume 0 is id of first/only segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
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
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }

    {
      reader = reader.Reopen();
      ASSERT_EQ(1, reader.size());
      auto& segment = reader[0];  // assume 0 is id of first/only segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // modify state (2nd segment 2 docs)
  {
    auto writer = open_writer(irs::kOmAppend);

    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // validate state pre/post refresh (new segment added)
  {
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("A",
              irs::ToString<std::string_view>(
                actual_value->value.data()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());

    reader = reader.Reopen();
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of first segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of second segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("C", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc3
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("D", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc4
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // modify state (delete doc1)
  {
    auto writer = open_writer(irs::kOmAppend);
    auto query_doc1 = MakeByTerm("name", "A");

    writer->GetBatch().Remove(std::move(query_doc1));
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // validate state pre/post refresh (old segment removed)
  {
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of first segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of second segment
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("C", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc3
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("D", irs::ToString<std::string_view>(
                       actual_value->value.data()));  // 'name' value in doc4
      ASSERT_FALSE(docs_itr->next());
    }

    reader = reader.Reopen();
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of second segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("C",
              irs::ToString<std::string_view>(
                actual_value->value.data()));  // 'name' value in doc3
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("D",
              irs::ToString<std::string_view>(
                actual_value->value.data()));  // 'name' value in doc4
    ASSERT_FALSE(docs_itr->next());
  }
}

TEST_P(IndexTestCase, reuse_segment_writer) {
  tests::JsonDocGenerator gen0(resource("serene_demo.json"),
                               &tests::GenericJsonFieldFactory);
  tests::JsonDocGenerator gen1(resource("simple_sequential.json"),
                               &tests::GenericJsonFieldFactory);
  auto writer = open_writer();

  // populate initial 2 very small segments
  {
    auto& index_ref = const_cast<tests::index_t&>(index());
    index_ref.emplace_back(writer->FeatureInfo());
    gen0.reset();
    write_segment(*writer, index_ref.back(), gen0);
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  {
    auto& index_ref = const_cast<tests::index_t&>(index());
    index_ref.emplace_back(writer->FeatureInfo());
    gen1.reset();
    write_segment(*writer, index_ref.back(), gen1);
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // populate initial small segment
  {
    auto& index_ref = const_cast<tests::index_t&>(index());
    index_ref.emplace_back(writer->FeatureInfo());
    gen0.reset();
    write_segment(*writer, index_ref.back(), gen0);
    gen1.reset();
    write_segment(*writer, index_ref.back(), gen1);
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // populate initial large segment
  {
    auto& index_ref = const_cast<tests::index_t&>(index());
    index_ref.emplace_back(writer->FeatureInfo());

    for (size_t i = 100; i > 0; --i) {
      gen0.reset();
      write_segment(*writer, index_ref.back(), gen0);
      gen1.reset();
      write_segment(*writer, index_ref.back(), gen1);
    }

    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // populate and validate small segments in hopes of triggering SegmentWriter
  // reuse 10 iterations, although 2 should be enough since
  // index_wirter::flush_context_pool_.size() == 2
  for (size_t i = 10; i > 0; --i) {
    auto& index_ref = const_cast<tests::index_t&>(index());
    index_ref.emplace_back(writer->FeatureInfo());

    // add varying sized segments
    for (size_t j = 0; j < i; ++j) {
      // add test documents
      if (i % 3 == 0 || i % 3 == 1) {
        gen0.reset();
        write_segment(*writer, index_ref.back(), gen0);
      }

      // add different test docs (overlap to make every 3rd segment contain
      // docs from both sources)
      if (i % 3 == 1 || i % 3 == 2) {
        gen1.reset();
        write_segment(*writer, index_ref.back(), gen1);
      }
    }

    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  assert_index();

  // merge all segments
  {
    ASSERT_TRUE(writer->Consolidate(
      irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }
}

TEST_P(IndexTestCase, segment_column_user_system) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      // add 2 identical fields (without storing) to trigger non-default
      // norm value
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });

  // document to add a system column not present in subsequent documents
  tests::Document doc0;

  // add 2 identical fields (without storing) to trigger non-default norm
  // value
  for (size_t i = 2; i; --i) {
    doc0.insert(std::make_shared<tests::StringField>("test-field", "test-value",
                                                     irs::IndexFeatures::Norm),
                true, false);
  }

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();

  irs::IndexWriterOptions opts;
  opts.features = FeaturesWithNorms();

  auto writer = open_writer(irs::kOmCreate, opts);

  ASSERT_TRUE(Insert(*writer, doc0.indexed.begin(), doc0.indexed.end(),
                     doc0.stored.begin(), doc0.stored.end()));
  ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                     doc1->stored.begin(), doc1->stored.end()));
  ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                     doc2->stored.begin(), doc2->stored.end()));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  std::unordered_set<std::string_view> expected_name = {"A", "B"};

  // validate segment
  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(1, reader.size());
  auto& segment = reader[0];           // assume 0 is id of first/only segment
  ASSERT_EQ(3, segment.docs_count());  // total count of documents

  auto* field =
    segment.field("test-field");  // 'norm' column added by doc0 above
  ASSERT_NE(nullptr, field);

  ASSERT_TRUE(
    irs::IsSubsetOf(irs::IndexFeatures::Norm, field->meta().index_features));
  ASSERT_TRUE(irs::field_limits::valid(field->meta().norm));

  auto* column = segment.column(field->meta().norm);  // system column
  ASSERT_NE(nullptr, column);

  column = segment.column("name");
  ASSERT_NE(nullptr, column);
  auto values = column->iterator(irs::ColumnHint::Normal);
  ASSERT_NE(nullptr, values);
  auto* actual_value = irs::get<irs::PayAttr>(*values);
  ASSERT_NE(nullptr, actual_value);
  ASSERT_EQ(expected_name.size() + 1,
            segment.docs_count());  // total count of documents (+1 for doc0)
  auto terms = segment.field("same");
  ASSERT_NE(nullptr, terms);
  auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
  ASSERT_TRUE(term_itr->next());

  for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
       docs_itr->next();) {
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ(1, expected_name.erase(irs::ToString<std::string_view>(
                   actual_value->value.data())));
  }

  ASSERT_TRUE(expected_name.empty());
}

TEST_P(IndexTestCase, import_concurrent) {
  struct Store {
    Store(const irs::Format::ptr& codec)
      : dir(std::make_unique<irs::MemoryDirectory>()) {
      writer = irs::IndexWriter::Make(*dir, codec, irs::kOmCreate);
      writer->Commit();
      reader = irs::DirectoryReader(*dir);
    }

    Store(Store&& rhs) noexcept
      : dir(std::move(rhs.dir)),
        writer(std::move(rhs.writer)),
        reader(rhs.reader) {}

    Store(const Store&) = delete;
    Store& operator=(const Store&) = delete;

    std::unique_ptr<irs::MemoryDirectory> dir;
    irs::IndexWriter::ptr writer;
    irs::DirectoryReader reader;
  };

  std::vector<Store> stores;
  stores.reserve(4);
  for (size_t i = 0; i < stores.capacity(); ++i) {
    stores.emplace_back(codec());
  }
  std::vector<std::thread> workers;

  std::set<std::string> names;
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [&names](tests::Document& doc, const std::string& name,
             const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));

        if (name == "name") {
          names.emplace(data.str.data, data.str.size);
        }
      }
    });

  const auto count = 10;
  for (auto& store : stores) {
    for (auto i = 0; i < count; ++i) {
      auto* doc = gen.next();

      if (!doc) {
        break;
      }

      ASSERT_TRUE(Insert(*store.writer, doc->indexed.begin(),
                         doc->indexed.end(), doc->stored.begin(),
                         doc->stored.end()));
    }
    store.writer->Commit();
    store.reader = irs::DirectoryReader(*store.dir);
    tests::AssertSnapshotEquality(store.writer->GetSnapshot(), store.reader);
  }

  std::mutex mutex;
  std::condition_variable ready_cv;
  bool ready = false;

  auto wait_for_all = [&mutex, &ready, &ready_cv]() {
    // wait for all threads to be registered
    std::unique_lock<std::remove_reference<decltype(mutex)>::type> lock(mutex);
    while (!ready) {
      ready_cv.wait(lock);
    }
  };

  irs::MemoryDirectory dir;
  irs::IndexWriter::ptr writer =
    irs::IndexWriter::Make(dir, codec(), irs::kOmCreate);

  for (auto& store : stores) {
    workers.emplace_back([&wait_for_all, &writer, &store]() {
      wait_for_all();
      writer->Import(store.reader);
    });
  }

  // all threads are registered... go, go, go...
  {
    std::lock_guard lock{mutex};
    ready = true;
    ready_cv.notify_all();
  }

  // wait for workers to finish
  for (auto& worker : workers) {
    worker.join();
  }

  writer->Commit();  // commit changes

  auto reader = irs::DirectoryReader(dir);
  tests::AssertSnapshotEquality(writer->GetSnapshot(), reader);
  ASSERT_EQ(workers.size(), reader.size());
  ASSERT_EQ(names.size(), reader.docs_count());
  ASSERT_EQ(names.size(), reader.live_docs_count());

  size_t removed = 0;
  for (auto& segment : reader) {
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    while (docs_itr->next()) {
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ(
        1, names.erase(irs::ToString<std::string>(actual_value->value.data())));
      ++removed;
    }
    ASSERT_FALSE(docs_itr->next());
  }
  ASSERT_EQ(removed, reader.docs_count());
  ASSERT_TRUE(names.empty());
}

static void ConsolidateRange(irs::Consolidation& candidates,
                             const irs::ConsolidatingSegments& segments,
                             const irs::IndexReader& reader, size_t begin,
                             size_t end) {
  if (begin > reader.size() || end > reader.size()) {
    return;
  }

  for (; begin < end; ++begin) {
    auto& r = reader[begin];
    if (!segments.contains(r.Meta().name)) {
      candidates.emplace_back(&r);
    }
  }
}

TEST_P(IndexTestCase, concurrent_consolidation) {
  auto writer = open_writer(dir());
  ASSERT_NE(nullptr, writer);

  std::set<std::string> names;
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [&names](tests::Document& doc, const std::string& name,
             const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));

        if (name == "name") {
          names.emplace(data.str.data, data.str.size);
        }
      }
    });

  // insert multiple small segments
  size_t size = 0;
  while (const auto* doc = gen.next()) {
    ASSERT_TRUE(Insert(*writer, doc->indexed.begin(), doc->indexed.end(),
                       doc->stored.begin(), doc->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ++size;
  }
  ASSERT_EQ(size - 1, irs::DirectoryCleaner::clean(dir()));

  std::mutex mutex;
  bool ready = false;
  std::condition_variable ready_cv;

  auto wait_for_all = [&mutex, &ready, &ready_cv]() {
    // wait for all threads to be registered
    std::unique_lock lock(mutex);
    while (!ready) {
      ready_cv.wait(lock);
    }
  };

  const auto thread_count = 10;
  std::vector<std::thread> pool;

  for (size_t i = 0; i < thread_count; ++i) {
    pool.emplace_back(std::thread([&, i]() mutable {
      wait_for_all();

      size_t num_segments = std::numeric_limits<size_t>::max();

      while (num_segments > 1) {
        auto policy = [&i, &num_segments](
                        irs::Consolidation& candidates,
                        const irs::IndexReader& reader,
                        const irs::ConsolidatingSegments& segments) mutable {
          num_segments = reader.size();
          ConsolidateRange(candidates, segments, reader, i, i + 2);
        };

        if (writer->Consolidate(policy)) {
          writer->Commit();
        }

        i = (i + 1) % num_segments;
      }
    }));
  }

  // all threads registered... go, go, go...
  {
    std::lock_guard lock{mutex};
    ready = true;
    ready_cv.notify_all();
  }

  for (auto& thread : pool) {
    thread.join();
  }

  writer->Commit();
  AssertSnapshotEquality(*writer);

  auto reader = irs::DirectoryReader(this->dir(), codec());
  ASSERT_EQ(1, reader.size());

  ASSERT_EQ(names.size(), reader.docs_count());
  ASSERT_EQ(names.size(), reader.live_docs_count());

  size_t removed = 0;
  auto& segment = reader[0];
  const auto* column = segment.column("name");
  ASSERT_NE(nullptr, column);
  auto values = column->iterator(irs::ColumnHint::Normal);
  ASSERT_NE(nullptr, values);
  auto* actual_value = irs::get<irs::PayAttr>(*values);
  ASSERT_NE(nullptr, actual_value);
  auto terms = segment.field("same");
  ASSERT_NE(nullptr, terms);
  auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
  ASSERT_TRUE(term_itr->next());
  auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
  while (docs_itr->next()) {
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ(
      1, names.erase(irs::ToString<std::string>(actual_value->value.data())));
    ++removed;
  }
  ASSERT_FALSE(docs_itr->next());

  ASSERT_EQ(removed, reader.docs_count());
  ASSERT_TRUE(names.empty());
}

TEST_P(IndexTestCase, concurrent_consolidation_dedicated_commit) {
  auto writer = open_writer(dir());
  ASSERT_NE(nullptr, writer);

  std::set<std::string> names;
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [&names](tests::Document& doc, const std::string& name,
             const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));

        if (name == "name") {
          names.emplace(data.str.data, data.str.size);
        }
      }
    });

  // insert multiple small segments
  size_t size = 0;
  while (const auto* doc = gen.next()) {
    ASSERT_TRUE(Insert(*writer, doc->indexed.begin(), doc->indexed.end(),
                       doc->stored.begin(), doc->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ++size;
  }
  ASSERT_EQ(size - 1, irs::DirectoryCleaner::clean(dir()));

  std::mutex mutex;
  bool ready = false;
  std::condition_variable ready_cv;

  auto wait_for_all = [&mutex, &ready, &ready_cv]() {
    // wait for all threads to be registered
    std::unique_lock lock(mutex);
    while (!ready) {
      ready_cv.wait(lock);
    }
  };

  const auto thread_count = 10;
  std::vector<std::thread> pool;

  for (size_t i = 0; i < thread_count; ++i) {
    pool.emplace_back(std::thread([&wait_for_all, &writer, i]() mutable {
      wait_for_all();

      size_t num_segments = std::numeric_limits<size_t>::max();

      while (num_segments > 1) {
        auto policy = [&i, &num_segments](
                        irs::Consolidation& candidates,
                        const irs::IndexReader& reader,
                        const irs::ConsolidatingSegments& segments) mutable {
          num_segments = reader.size();
          ConsolidateRange(candidates, segments, reader, i, i + 2);
        };

        writer->Consolidate(policy);

        i = (i + 1) % num_segments;
      }
    }));
  }

  // add dedicated commit thread
  std::atomic<bool> shutdown(false);
  std::thread commit_thread([&]() {
    wait_for_all();

    while (!shutdown.load()) {
      writer->Commit();
      AssertSnapshotEquality(*writer);
      std::this_thread::sleep_for(100ms);
    }
  });

  // all threads registered... go, go, go...
  {
    std::lock_guard lock{mutex};
    ready = true;
    ready_cv.notify_all();
  }

  for (auto& thread : pool) {
    thread.join();
  }

  // wait for commit thread to finish
  shutdown = true;
  commit_thread.join();

  writer->Commit();
  AssertSnapshotEquality(*writer);

  auto reader = irs::DirectoryReader(this->dir(), codec());
  ASSERT_EQ(1, reader.size());

  ASSERT_EQ(names.size(), reader.docs_count());
  ASSERT_EQ(names.size(), reader.live_docs_count());

  size_t removed = 0;
  auto& segment = reader[0];
  const auto* column = segment.column("name");
  ASSERT_NE(nullptr, column);
  auto values = column->iterator(irs::ColumnHint::Normal);
  ASSERT_NE(nullptr, values);
  auto* actual_value = irs::get<irs::PayAttr>(*values);
  ASSERT_NE(nullptr, actual_value);
  auto terms = segment.field("same");
  ASSERT_NE(nullptr, terms);
  auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
  ASSERT_TRUE(term_itr->next());
  auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
  while (docs_itr->next()) {
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ(
      1, names.erase(irs::ToString<std::string>(actual_value->value.data())));
    ++removed;
  }
  ASSERT_FALSE(docs_itr->next());

  ASSERT_EQ(removed, reader.docs_count());
  ASSERT_TRUE(names.empty());
}

TEST_P(IndexTestCase, concurrent_consolidation_two_phase_dedicated_commit) {
  auto writer = open_writer(dir());
  ASSERT_NE(nullptr, writer);

  std::set<std::string> names;
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [&names](tests::Document& doc, const std::string& name,
             const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));

        if (name == "name") {
          names.emplace(data.str.data, data.str.size);
        }
      }
    });

  // insert multiple small segments
  size_t size = 0;
  while (const auto* doc = gen.next()) {
    ASSERT_TRUE(Insert(*writer, doc->indexed.begin(), doc->indexed.end(),
                       doc->stored.begin(), doc->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ++size;
  }
  ASSERT_EQ(size - 1, irs::DirectoryCleaner::clean(dir()));

  std::mutex mutex;
  bool ready = false;
  std::condition_variable ready_cv;

  auto wait_for_all = [&mutex, &ready, &ready_cv]() {
    // wait for all threads to be registered
    std::unique_lock lock(mutex);
    while (!ready) {
      ready_cv.wait(lock);
    }
  };

  const auto thread_count = 10;
  std::vector<std::thread> pool;

  for (size_t i = 0; i < thread_count; ++i) {
    pool.emplace_back(std::thread([&wait_for_all, &writer, i]() mutable {
      wait_for_all();

      size_t num_segments = std::numeric_limits<size_t>::max();

      while (num_segments > 1) {
        auto policy = [&i, &num_segments](
                        irs::Consolidation& candidates,
                        const irs::IndexReader& meta,
                        const irs::ConsolidatingSegments& segments) mutable {
          num_segments = meta.size();
          ConsolidateRange(candidates, segments, meta, i, i + 2);
        };

        writer->Consolidate(policy);

        i = (i + 1) % num_segments;
      }
    }));
  }

  // add dedicated commit thread
  std::atomic<bool> shutdown(false);
  std::thread commit_thread([&]() {
    wait_for_all();

    while (!shutdown.load()) {
      writer->Begin();
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
      writer->Commit();
      AssertSnapshotEquality(*writer);
      std::this_thread::sleep_for(100ms);
    }
  });

  // all threads registered... go, go, go...
  {
    std::lock_guard lock{mutex};
    ready = true;
    ready_cv.notify_all();
  }

  for (auto& thread : pool) {
    thread.join();
  }

  // wait for commit thread to finish
  shutdown = true;
  commit_thread.join();

  writer->Commit();
  AssertSnapshotEquality(*writer);

  auto reader = irs::DirectoryReader(this->dir(), codec());
  ASSERT_EQ(1, reader.size());

  ASSERT_EQ(names.size(), reader.docs_count());
  ASSERT_EQ(names.size(), reader.live_docs_count());

  size_t removed = 0;
  auto& segment = reader[0];
  const auto* column = segment.column("name");
  ASSERT_NE(nullptr, column);
  auto values = column->iterator(irs::ColumnHint::Normal);
  ASSERT_NE(nullptr, values);
  auto* actual_value = irs::get<irs::PayAttr>(*values);
  ASSERT_NE(nullptr, actual_value);
  auto terms = segment.field("same");
  ASSERT_NE(nullptr, terms);
  auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
  ASSERT_TRUE(term_itr->next());
  auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
  while (docs_itr->next()) {
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ(
      1, names.erase(irs::ToString<std::string>(actual_value->value.data())));
    ++removed;
  }
  ASSERT_FALSE(docs_itr->next());

  ASSERT_EQ(removed, reader.docs_count());
  ASSERT_TRUE(names.empty());
}

TEST_P(IndexTestCase, concurrent_consolidation_cleanup) {
  auto writer = open_writer(dir());
  ASSERT_NE(nullptr, writer);

  std::set<std::string> names;
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [&names](tests::Document& doc, const std::string& name,
             const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));

        if (name == "name") {
          names.emplace(data.str.data, data.str.size);
        }
      }
    });

  // insert multiple small segments
  size_t size = 0;
  while (const auto* doc = gen.next()) {
    ASSERT_TRUE(Insert(*writer, doc->indexed.begin(), doc->indexed.end(),
                       doc->stored.begin(), doc->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ++size;
  }
  ASSERT_EQ(size - 1, irs::DirectoryCleaner::clean(dir()));

  std::mutex mutex;
  bool ready = false;
  std::condition_variable ready_cv;

  auto wait_for_all = [&mutex, &ready, &ready_cv]() {
    // wait for all threads to be registered
    std::unique_lock lock(mutex);
    while (!ready) {
      ready_cv.wait(lock);
    }
  };

  const auto thread_count = 10;
  std::vector<std::thread> pool;

  for (size_t i = 0; i < thread_count; ++i) {
    pool.emplace_back(std::thread([&, i]() mutable {
      wait_for_all();

      size_t num_segments = std::numeric_limits<size_t>::max();

      while (num_segments > 1) {
        auto policy = [&](irs::Consolidation& candidates,
                          const irs::IndexReader& reader,
                          const irs::ConsolidatingSegments& segments) mutable {
          num_segments = reader.size();
          ConsolidateRange(candidates, segments, reader, i, i + 2);
        };

        if (writer->Consolidate(policy)) {
          writer->Commit();
          irs::DirectoryCleaner::clean(this->dir());
        }

        i = (i + 1) % num_segments;
      }
    }));
  }

  // all threads registered... go, go, go...
  {
    std::lock_guard lock{mutex};
    ready = true;
    ready_cv.notify_all();
  }

  for (auto& thread : pool) {
    thread.join();
  }

  writer->Commit();
  AssertSnapshotEquality(*writer);
  irs::DirectoryCleaner::clean(dir());

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(1, reader.size());

  ASSERT_EQ(names.size(), reader.docs_count());
  ASSERT_EQ(names.size(), reader.live_docs_count());

  size_t removed = 0;
  auto& segment = reader[0];
  const auto* column = segment.column("name");
  ASSERT_NE(nullptr, column);
  auto values = column->iterator(irs::ColumnHint::Normal);
  ASSERT_NE(nullptr, values);
  auto* actual_value = irs::get<irs::PayAttr>(*values);
  ASSERT_NE(nullptr, actual_value);
  auto terms = segment.field("same");
  ASSERT_NE(nullptr, terms);
  auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
  ASSERT_TRUE(term_itr->next());
  auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
  while (docs_itr->next()) {
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ(
      1, names.erase(irs::ToString<std::string>(actual_value->value.data())));
    ++removed;
  }
  ASSERT_FALSE(docs_itr->next());

  ASSERT_EQ(removed, reader.docs_count());
  ASSERT_TRUE(names.empty());
}

TEST_P(IndexTestCase, consolidate_single_segment) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();

  constexpr irs::IndexFeatures kAllFeatures =
    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
    irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

  std::vector<size_t> expected_consolidating_segments;
  auto check_consolidating_segments =
    [&expected_consolidating_segments](
      irs::Consolidation& /*candidates*/, const irs::IndexReader& reader,
      const irs::ConsolidatingSegments& consolidating_segments) {
      ASSERT_EQ(expected_consolidating_segments.size(),
                consolidating_segments.size());
      for (auto i : expected_consolidating_segments) {
        auto& expected_consolidating_segment = reader[i];
        ASSERT_TRUE(consolidating_segments.contains(
          expected_consolidating_segment.Meta().name));
      }
    };

  // single segment without deletes
  {
    auto writer = open_writer(dir());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    ASSERT_TRUE(writer->Consolidate(irs::index_utils::MakePolicy(
      irs::index_utils::ConsolidateCount())));  // nothing to consolidate
    ASSERT_TRUE(writer->Consolidate(
      check_consolidating_segments));  // check segments registered for
                                       // consolidation
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
  }

  size_t count = 0;
  auto get_number_of_files_in_segments =
    [&count](std::string_view name) noexcept {
      count += size_t(name.size() && '_' == name[0]);
      return true;
    };

  // single segment with deletes
  {
    auto writer = open_writer(dir());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    auto query_doc1 = MakeByTerm("name", "A");
    writer->GetBatch().Remove(*query_doc1);
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(
      3,
      irs::DirectoryCleaner::clean(
        dir()));  // segments_1 + stale segment meta + unused column store
    ASSERT_EQ(1, irs::DirectoryReader(this->dir(), codec()).size());

    // get number of files in 1st segment
    count = 0;
    dir().visit(get_number_of_files_in_segments);

    ASSERT_TRUE(writer->Consolidate(irs::index_utils::MakePolicy(
      irs::index_utils::ConsolidateCount())));  // nothing to consolidate
    expected_consolidating_segments = {
      0};  // expect first segment to be marked for consolidation
    ASSERT_TRUE(writer->Consolidate(
      check_consolidating_segments));  // check segments registered for
                                       // consolidation
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1 + count,
              irs::DirectoryCleaner::clean(dir()));  // +1 for segments_2

    // validate structure
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc2);
    tests::AssertIndex(this->dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(this->dir(), codec());
    ASSERT_EQ(1, reader.size());

    // assume 0 is 'merged' segment
    {
      auto& segment = reader[0];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }
  }
}

TEST_P(IndexTestCase, segment_consolidate_long_running) {
  const auto blocker = [this](std::string_view segment) {
    irs::MemoryDirectory dir;
    auto writer =
      codec()->get_columnstore_writer(false, irs::IResourceManager::gNoop);

    irs::SegmentMeta meta;
    meta.name = segment;
    writer->prepare(dir, meta);

    std::string filename;
    dir.visit([&filename](std::string_view name) {
      filename = name;
      return false;
    });

    writer->rollback();

    return filename;
  }("_3");

  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();
  const tests::Document* doc4 = gen.next();

  constexpr irs::IndexFeatures kAllFeatures =
    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
    irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

  size_t count = 0;
  auto get_number_of_files_in_segments =
    [&count](std::string_view name) noexcept {
      count += size_t(name.size() && '_' == name[0]);
      return true;
    };

  // long running transaction
  {
    tests::BlockingDirectory dir(this->dir(), blocker);
    auto writer = open_writer(dir);
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));

    // segment 2
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir));  // segments_1

    // retrieve total number of segment files
    dir.visit(get_number_of_files_in_segments);

    // acquire directory lock, and block consolidation
    dir.intermediate_commits_lock.lock();

    std::thread consolidation_thread([&writer]() {
      // consolidate
      ASSERT_TRUE(writer->Consolidate(
        irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount{})));

      const std::vector<size_t> expected_consolidating_segments{0, 1};
      auto check_consolidating_segments =
        [&expected_consolidating_segments](
          irs::Consolidation& /*candidates*/, const irs::IndexReader& reader,
          const irs::ConsolidatingSegments& consolidating_segments) {
          ASSERT_EQ(expected_consolidating_segments.size(),
                    consolidating_segments.size());
          for (auto i : expected_consolidating_segments) {
            const auto& expected_consolidating_segment = reader[i];
            ASSERT_TRUE(consolidating_segments.contains(
              expected_consolidating_segment.Meta().name));
          }
        };
      // check segments registered for consolidation
      ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));
    });

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));

    bool has = false;
    dir.exists(has, dir.blocker);

    while (!has) {
      dir.exists(has, dir.blocker);
      ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));

      auto policy_guard = std::unique_lock{dir.policy_lock};
      dir.policy_applied.wait_for(policy_guard, 1000ms);
    }

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));

    // add several segments in background
    // segment 3
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);                  // commit transaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));  // segments_2

    // add several segments in background
    // segment 4
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);                  // commit transaction
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir));  // segments_3

    dir.intermediate_commits_lock.unlock();  // finish consolidation
    consolidation_thread.join();  // wait for the consolidation to complete

    // finished consolidation holds a reference to segments_3
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));
    writer->Commit();
    AssertSnapshotEquality(*writer);  // commit consolidation

    // +1 for segments_4, +1 for segments_3
    ASSERT_EQ(1 + 1 + count, irs::DirectoryCleaner::clean(dir));

    // validate structure
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc3);
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc4);
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    tests::AssertIndex(this->dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(this->dir(), codec());
    ASSERT_EQ(3, reader.size());

    // assume 0 is 'segment 3'
    {
      auto& segment = reader[0];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("C",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    // assume 1 is 'segment 4'
    {
      auto& segment = reader[1];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("D",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    // assume 2 is merged segment
    {
      auto& segment = reader[2];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(2, segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // long running transaction + segment removal
  {
    SetUp();  // recreate directory
    auto query_doc1 = MakeByTerm("name", "A");

    tests::BlockingDirectory dir(this->dir(), blocker);
    auto writer = open_writer(dir);
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));

    // retrieve total number of segment files
    count = 0;
    dir.visit(get_number_of_files_in_segments);

    // segment 2
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir));  // segments_1

    // acquire directory lock, and block consolidation
    dir.intermediate_commits_lock.lock();

    std::thread consolidation_thread([&writer]() {
      // consolidation will fail because of
      ASSERT_FALSE(writer->Consolidate(irs::index_utils::MakePolicy(
        irs::index_utils::ConsolidateCount())));  // consolidate

      auto check_consolidating_segments =
        [](irs::Consolidation& /*candidates*/, const irs::IndexReader& /*meta*/,
           const irs::ConsolidatingSegments& consolidating_segments) {
          ASSERT_TRUE(consolidating_segments.empty());
        };
      // check segments registered for consolidation
      ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));
    });

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));

    bool has = false;
    dir.exists(has, dir.blocker);

    while (!has) {
      dir.exists(has, dir.blocker);
      ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));

      auto policy_guard = std::unique_lock{dir.policy_lock};
      dir.policy_applied.wait_for(policy_guard, 1000ms);
    }

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));

    // add several segments in background
    // segment 3
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    writer->GetBatch().Remove(*query_doc1);
    writer->Commit();
    AssertSnapshotEquality(*writer);                  // commit transaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));  // segments_2

    // add several segments in background
    // segment 4
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);                  // commit transaction
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir));  // segments_3

    dir.intermediate_commits_lock.unlock();  // finish consolidation
    consolidation_thread.join();  // wait for the consolidation to complete
    ASSERT_EQ(2 * count - 1 + 1,
              irs::DirectoryCleaner::clean(
                dir));  // files from segment 1 and 3 (without segment meta)
                        // + segments_3
    writer->Commit();
    AssertSnapshotEquality(*writer);  // commit consolidation
    ASSERT_EQ(0,
              irs::DirectoryCleaner::clean(dir));  // consolidation failed

    // validate structure
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc2);
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc3);
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc4);
    tests::AssertIndex(this->dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(this->dir(), codec());
    ASSERT_EQ(3, reader.size());

    // assume 0 is 'segment 2'
    {
      auto& segment = reader[0];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    // assume 1 is 'segment 3'
    {
      auto& segment = reader[1];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("C",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    // assume 1 is 'segment 4'
    {
      auto& segment = reader[2];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("D",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // long running transaction + document removal
  {
    SetUp();  // recreate directory
    auto query_doc1 = MakeByTerm("name", "A");

    tests::BlockingDirectory dir(this->dir(), blocker);
    auto writer = open_writer(dir);
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));

    // segment 2
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir));  // segments_1

    // retrieve total number of segment files
    count = 0;
    dir.visit(get_number_of_files_in_segments);

    // acquire directory lock, and block consolidation
    dir.intermediate_commits_lock.lock();

    std::thread consolidation_thread([&writer]() {
      // consolidation will fail because of
      ASSERT_TRUE(writer->Consolidate(
        irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));

      const std::vector<size_t> expected_consolidating_segments{0, 1};
      auto check_consolidating_segments =
        [&expected_consolidating_segments](
          irs::Consolidation& /*candidates*/, const irs::IndexReader& reader,
          const irs::ConsolidatingSegments& consolidating_segments) {
          ASSERT_EQ(expected_consolidating_segments.size(),
                    consolidating_segments.size());
          for (auto i : expected_consolidating_segments) {
            const auto& expected_consolidating_segment = reader[i];
            ASSERT_TRUE(consolidating_segments.contains(
              expected_consolidating_segment.Meta().name));
          }
        };
      // check segments registered for consolidation
      ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));
    });

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));

    bool has = false;
    dir.exists(has, dir.blocker);

    while (!has) {
      dir.exists(has, dir.blocker);
      ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));

      auto policy_guard = std::unique_lock{dir.policy_lock};
      dir.policy_applied.wait_for(policy_guard, 1000ms);
    }

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));

    // remove doc1 in background
    writer->GetBatch().Remove(*query_doc1);
    writer->Commit();
    AssertSnapshotEquality(*writer);  // commit transaction
    // unused column store
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir));

    dir.intermediate_commits_lock.unlock();  // finish consolidation
    consolidation_thread.join();  // wait for the consolidation to complete
    // Consolidation still holds a reference to the snapshot segment_2
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));
    writer->Commit();
    AssertSnapshotEquality(*writer);  // commit consolidation
    // files from segments 1 and 2, segment_3
    // segment_2 + stale segment 1 meta
    ASSERT_EQ(count + 2 + 1, irs::DirectoryCleaner::clean(dir));

    // validate structure (does not take removals into account)
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    expected.back().insert(*doc3);
    tests::AssertIndex(this->dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(this->dir(), codec());
    ASSERT_EQ(1, reader.size());

    // assume 0 is 'merged segment'
    {
      auto& segment = reader[0];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(3, segment.docs_count());       // total count of documents
      ASSERT_EQ(2, segment.live_docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      // including deleted docs
      {
        auto values = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, values);
        auto* actual_value = irs::get<irs::PayAttr>(*values);
        ASSERT_NE(nullptr, actual_value);

        auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("A",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc1
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("B",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc2
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("C",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc3
        ASSERT_FALSE(docs_itr->next());
      }

      // only live docs
      {
        auto values = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, values);
        auto* actual_value = irs::get<irs::PayAttr>(*values);
        ASSERT_NE(nullptr, actual_value);

        auto docs_itr =
          segment.mask(term_itr->postings(irs::IndexFeatures::None));
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("B",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc2
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("C",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc3
        ASSERT_FALSE(docs_itr->next());
      }
    }
  }

  // long running transaction + document removal
  {
    SetUp();  // recreate directory
    auto query_doc1_doc4 = MakeByTermOrByTerm("name", "A", "name", "D");

    tests::BlockingDirectory dir(this->dir(), blocker);
    auto writer = open_writer(dir);
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));

    // segment 2
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir));  // segments_1

    // retrieve total number of segment files
    count = 0;
    dir.visit(get_number_of_files_in_segments);

    dir.intermediate_commits_lock
      .lock();  // acquire directory lock, and block consolidation

    std::thread consolidation_thread([&writer]() {
      // consolidation will fail because of
      ASSERT_TRUE(writer->Consolidate(irs::index_utils::MakePolicy(
        irs::index_utils::ConsolidateCount())));  // consolidate

      const std::vector<size_t> expected_consolidating_segments{0, 1};
      auto check_consolidating_segments =
        [&expected_consolidating_segments](
          irs::Consolidation& /*candidates*/, const irs::IndexReader& reader,
          const irs::ConsolidatingSegments& consolidating_segments) {
          ASSERT_EQ(expected_consolidating_segments.size(),
                    consolidating_segments.size());
          for (auto i : expected_consolidating_segments) {
            const auto& expected_consolidating_segment = reader[i];
            ASSERT_TRUE(consolidating_segments.contains(
              expected_consolidating_segment.Meta().name));
          }
        };
      // check segments registered for consolidation
      ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));
    });

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));

    bool has = false;
    dir.exists(has, dir.blocker);

    while (!has) {
      dir.exists(has, dir.blocker);
      ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));

      auto policy_guard = std::unique_lock{dir.policy_lock};
      dir.policy_applied.wait_for(policy_guard, 1000ms);
    }

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));

    // remove doc1 in background
    writer->GetBatch().Remove(*query_doc1_doc4);
    writer->Commit();
    AssertSnapshotEquality(*writer);  // commit transaction
    //  unused column store
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir));

    dir.intermediate_commits_lock.unlock();  // finish consolidation
    consolidation_thread.join();  // wait for the consolidation to complete
    // consolidation still holds a reference to the snapshot pointed by
    // segments_2
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));
    writer->Commit();
    AssertSnapshotEquality(*writer);  // commit consolidation

    // files from segments 1 and 2,
    // segment_3
    // segment_2 + stale segment 1 meta + stale segment 2 meta
    ASSERT_EQ(count + 3 + 1, irs::DirectoryCleaner::clean(dir));

    // validate structure (does not take removals into account)
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    expected.back().insert(*doc3);
    expected.back().insert(*doc4);
    tests::AssertIndex(this->dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(this->dir(), codec());
    ASSERT_EQ(1, reader.size());

    // assume 0 is 'merged segment'
    {
      auto& segment = reader[0];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(4, segment.docs_count());       // total count of documents
      ASSERT_EQ(2, segment.live_docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      // including deleted docs
      {
        auto values = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, values);
        auto* actual_value = irs::get<irs::PayAttr>(*values);
        ASSERT_NE(nullptr, actual_value);

        auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("A",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc1
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("B",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc2
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("C",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc3
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("D",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc4
        ASSERT_FALSE(docs_itr->next());
      }

      // only live docs
      {
        auto values = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, values);
        auto* actual_value = irs::get<irs::PayAttr>(*values);
        ASSERT_NE(nullptr, actual_value);

        auto docs_itr =
          segment.mask(term_itr->postings(irs::IndexFeatures::None));
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("B",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc2
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("C",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc3
        ASSERT_FALSE(docs_itr->next());
      }
    }
  }
}

TEST_P(IndexTestCase, segment_consolidate_clear_commit) {
  std::vector<size_t> expected_consolidating_segments;
  auto check_consolidating_segments =
    [&expected_consolidating_segments](
      irs::Consolidation& /*candidates*/, const irs::IndexReader& reader,
      const irs::ConsolidatingSegments& consolidating_segments) {
      ASSERT_EQ(expected_consolidating_segments.size(),
                consolidating_segments.size());
      for (auto i : expected_consolidating_segments) {
        const auto& expected_consolidating_segment = reader[i];
        ASSERT_TRUE(consolidating_segments.contains(
          expected_consolidating_segment.Meta().name));
      }
    };

  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();

  // 2-phase: clear + consolidate
  {
    auto writer = open_writer();
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // segment 2
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // segment 3
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));

    writer->Begin();
    writer->Clear();
    ASSERT_TRUE(writer->Consolidate(irs::index_utils::MakePolicy(
      irs::index_utils::ConsolidateCount())));  // consolidate
    writer->Commit();
    AssertSnapshotEquality(*writer);  // commit transaction

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(0, reader.size());
  }

  // 2-phase: consolidate + clear
  {
    auto writer = open_writer();
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // segment 2
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // segment 3
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));

    writer->Begin();
    ASSERT_TRUE(writer->Consolidate(irs::index_utils::MakePolicy(
      irs::index_utils::ConsolidateCount())));  // consolidate
    writer->Clear();
    writer->Commit();
    AssertSnapshotEquality(*writer);  // commit transaction

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(0, reader.size());
  }

  // consolidate + clear
  {
    auto writer = open_writer();
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // segment 2
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    ASSERT_TRUE(writer->Consolidate(irs::index_utils::MakePolicy(
      irs::index_utils::ConsolidateCount())));  // consolidate

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    writer->Clear();

    // check consolidating segments
    expected_consolidating_segments = {};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    writer->Commit();
    AssertSnapshotEquality(*writer);  // commit transaction

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(0, reader.size());
  }

  // clear + consolidate
  {
    auto writer = open_writer();
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // segment 2
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    writer->Clear();
    ASSERT_TRUE(writer->Consolidate(irs::index_utils::MakePolicy(
      irs::index_utils::ConsolidateCount())));  // consolidate

    // check consolidating segments
    expected_consolidating_segments = {};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    writer->Commit();
    AssertSnapshotEquality(*writer);  // commit transaction

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(0, reader.size());
  }
}

TEST_P(IndexTestCase, segment_consolidate_commit) {
  std::vector<size_t> expected_consolidating_segments;
  auto check_consolidating_segments =
    [&expected_consolidating_segments](
      irs::Consolidation& /*candidates*/, const irs::IndexReader& reader,
      const irs::ConsolidatingSegments& consolidating_segments) {
      ASSERT_EQ(expected_consolidating_segments.size(),
                consolidating_segments.size());
      for (auto i : expected_consolidating_segments) {
        const auto& expected_consolidating_segment = reader[i];
        ASSERT_TRUE(consolidating_segments.contains(
          expected_consolidating_segment.Meta().name));
      }
    };

  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();
  const tests::Document* doc4 = gen.next();
  const tests::Document* doc5 = gen.next();

  constexpr irs::IndexFeatures kAllFeatures =
    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
    irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

  size_t count = 0;
  auto get_number_of_files_in_segments =
    [&count](std::string_view name) noexcept {
      count += size_t(name.size() && '_' == name[0]);
      return true;
    };

  // consolidate without deletes
  {
    auto writer = open_writer();
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // segment 2
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    ASSERT_TRUE(writer->Consolidate(irs::index_utils::MakePolicy(
      irs::index_utils::ConsolidateCount())));  // consolidate

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // all segments are already marked for consolidation
    ASSERT_FALSE(writer->Consolidate(
      irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    writer->Commit();
    AssertSnapshotEquality(
      *writer);  // commit transaction (will commit nothing)
    ASSERT_EQ(1 + count, irs::DirectoryCleaner::clean(
                           dir()));  // +1 for corresponding segments_* file
    ASSERT_TRUE(writer->Consolidate(irs::index_utils::MakePolicy(
      irs::index_utils::ConsolidateCount())));  // nothing to consolidate
    writer->Commit();
    AssertSnapshotEquality(
      *writer);  // commit transaction (will commit nothing)

    // validate structure
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());

    // assume 0 is merged segment
    {
      auto& segment = reader[0];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(2, segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // consolidate without deletes
  {
    auto writer = open_writer();
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // segment 2
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    // segment 3
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    ASSERT_TRUE(writer->Consolidate(irs::index_utils::MakePolicy(
      irs::index_utils::ConsolidateCount())));  // consolidate

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    // can't consolidate segments that are already marked for consolidation
    ASSERT_FALSE(writer->Consolidate(
      irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    writer->Commit();
    AssertSnapshotEquality(*writer);  // commit transaction (will commit segment
                                      // 3 + consolidation)
    ASSERT_EQ(1 + count,
              irs::DirectoryCleaner::clean(dir()));  // +1 for segments_*

    // validate structure
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc3);
    expected.back().insert(*doc4);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(2, reader.size());

    // assume 0 is merged segment
    {
      auto& segment = reader[0];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(2, segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }

    // assume 1 is the newly created segment (doc3+doc4)
    {
      auto& segment = reader[1];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(2, segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("C",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc3
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("D",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc4
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // consolidate without deletes
  {
    auto writer = open_writer();
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // segment 2
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    // segment 3
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));  // segments_1
    ASSERT_TRUE(writer->Consolidate(irs::index_utils::MakePolicy(
      irs::index_utils::ConsolidateCount())));  // consolidate

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    // can't consolidate segments that are already marked for consolidation
    ASSERT_FALSE(writer->Consolidate(
      irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));  // segments_1

    ASSERT_TRUE(Insert(*writer, doc5->indexed.begin(), doc5->indexed.end(),
                       doc5->stored.begin(), doc5->stored.end()));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));  // segments_1
    writer->Commit();
    AssertSnapshotEquality(*writer);  // commit transaction (will commit segment
                                      // 3 + consolidation)
    ASSERT_EQ(count + 1,
              irs::DirectoryCleaner::clean(dir()));  // +1 for segments_*

    // validate structure
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc3);
    expected.back().insert(*doc4);
    expected.back().insert(*doc5);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader.size());

    // assume 0 is merged segment
    {
      auto& segment = reader[0];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(2, segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }

    // assume 1 is the newly crated segment
    {
      auto& segment = reader[1];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(3, segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("C",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc3
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("D",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc4
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("E",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc4
      ASSERT_FALSE(docs_itr->next());
    }
  }
}

TEST_P(IndexTestCase, consolidate_check_consolidating_segments) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });

  auto writer = open_writer();
  ASSERT_NE(nullptr, writer);

  // ensure consolidating segments is empty
  {
    auto check_consolidating_segments =
      [](irs::Consolidation& /*candidates*/, const irs::IndexReader& /*reader*/,
         const irs::ConsolidatingSegments& consolidating_segments) {
        ASSERT_TRUE(consolidating_segments.empty());
      };
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));
  }

  const size_t segments_count = 10;
  for (size_t i = 0; i < segments_count; ++i) {
    const auto* doc = gen.next();
    ASSERT_TRUE(Insert(*writer, doc->indexed.begin(), doc->indexed.end(),
                       doc->stored.begin(), doc->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // register 'SEGMENTS_COUNT/2' consolidations
  for (size_t i = 0, j = 0; i < segments_count / 2; ++i) {
    auto merge_adjacent =
      [&j](irs::Consolidation& candidates, const irs::IndexReader& reader,
           const irs::ConsolidatingSegments& /*consolidating_segments*/) {
        ASSERT_TRUE(j < reader.size());
        candidates.emplace_back(&reader[j++]);
        ASSERT_TRUE(j < reader.size());
        candidates.emplace_back(&reader[j++]);
      };

    ASSERT_TRUE(writer->Consolidate(merge_adjacent));
  }

  // check all segments registered
  {
    auto check_consolidating_segments =
      [](irs::Consolidation& /*candidates*/, const irs::IndexReader& reader,
         const irs::ConsolidatingSegments& consolidating_segments) {
        ASSERT_EQ(reader.size(), consolidating_segments.size());
        for (auto& expected_consolidating_segment : reader) {
          ASSERT_TRUE(consolidating_segments.contains(
            expected_consolidating_segment.Meta().name));
        }
      };
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));
  }

  writer->Commit();
  AssertSnapshotEquality(*writer);  // commit pending consolidations

  // ensure consolidating segments is empty
  {
    auto check_consolidating_segments =
      [](irs::Consolidation& /*candidates*/, const irs::IndexReader& /*reader*/,
         const irs::ConsolidatingSegments& consolidating_segments) {
        ASSERT_TRUE(consolidating_segments.empty());
      };
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));
  }

  // validate structure
  constexpr irs::IndexFeatures kAllFeatures =
    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
    irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;
  gen.reset();
  tests::index_t expected;
  for (size_t i = 0; i < segments_count / 2; ++i) {
    expected.emplace_back(writer->FeatureInfo());
    const auto* doc = gen.next();
    expected.back().insert(*doc);
    doc = gen.next();
    expected.back().insert(*doc);
  }
  tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(segments_count / 2, reader.size());

  std::string expected_name = "A";

  for (size_t i = 0; i < segments_count / 2; ++i) {
    auto& segment = reader[i];
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    ASSERT_EQ(2, segment.docs_count());  // total count of documents
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ(expected_name,
              irs::ToString<std::string_view>(
                actual_value->value.data()));  // 'name' value in doc1
    ++expected_name[0];
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ(expected_name,
              irs::ToString<std::string_view>(
                actual_value->value.data()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
    ++expected_name[0];
  }
}

TEST_P(IndexTestCase, segment_consolidate_pending_commit) {
  std::vector<size_t> expected_consolidating_segments;
  auto check_consolidating_segments =
    [&expected_consolidating_segments](
      irs::Consolidation& /*candidates*/, const irs::IndexReader& reader,
      const irs::ConsolidatingSegments& consolidating_segments) {
      ASSERT_EQ(expected_consolidating_segments.size(),
                consolidating_segments.size());
      for (auto i : expected_consolidating_segments) {
        const auto& expected_consolidating_segment = reader[i];
        ASSERT_TRUE(consolidating_segments.contains(
          expected_consolidating_segment.Meta().name));
      }
    };

  auto check_consolidating_segments_name_only =
    [&expected_consolidating_segments](
      irs::Consolidation& /*candidates*/, const irs::IndexReader& reader,
      const irs::ConsolidatingSegments& consolidating_segments) {
      ASSERT_EQ(expected_consolidating_segments.size(),
                consolidating_segments.size());
      for (auto i : expected_consolidating_segments) {
        const auto& expected_consolidating_segment = reader[i];
        ASSERT_TRUE(consolidating_segments.contains(
          expected_consolidating_segment.Meta().name));
      }
    };

  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();
  const tests::Document* doc4 = gen.next();
  const tests::Document* doc5 = gen.next();
  const tests::Document* doc6 = gen.next();

  constexpr irs::IndexFeatures kAllFeatures =
    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
    irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

  size_t count = 0;
  auto get_number_of_files_in_segments =
    [&count](std::string_view name) noexcept {
      count += size_t(name.size() && '_' == name[0]);
      return true;
    };

  // consolidate without deletes
  {
    auto writer = open_writer();
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // segment 2
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    ASSERT_FALSE(
      writer->Begin());  // begin transaction (will not start transaction)
    ASSERT_TRUE(writer->Consolidate(irs::index_utils::MakePolicy(
      irs::index_utils::ConsolidateCount())));  // consolidate

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    // all segments are already marked for consolidation
    ASSERT_FALSE(writer->Consolidate(
      irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    writer->Commit();
    AssertSnapshotEquality(
      *writer);  // commit transaction (will commit consolidation)
    ASSERT_EQ(1 + count, irs::DirectoryCleaner::clean(
                           dir()));  // +1 for corresponding segments_* file

    // check consolidating segments
    expected_consolidating_segments = {};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    ASSERT_TRUE(writer->Consolidate(irs::index_utils::MakePolicy(
      irs::index_utils::ConsolidateCount())));  // nothing to consolidate

    // check consolidating segments
    expected_consolidating_segments = {};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    writer->Commit();
    AssertSnapshotEquality(
      *writer);  // commit transaction (will commit nothing)

    // validate structure
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(2, reader.live_docs_count());

    // assume 0 is merged segment
    {
      auto& segment = reader[0];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(2, segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // consolidate without deletes
  {
    SetUp();
    auto writer = open_writer();
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // segment 2
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    // segment 3
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));

    ASSERT_TRUE(writer->Begin());  // begin transaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // check consolidating segments
    expected_consolidating_segments = {};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    ASSERT_TRUE(writer->Consolidate(irs::index_utils::MakePolicy(
      irs::index_utils::ConsolidateCount())));  // consolidate

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    // can't consolidate segments that are already marked for consolidation
    ASSERT_FALSE(writer->Consolidate(
      irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    writer->Commit();
    AssertSnapshotEquality(
      *writer);  // commit transaction (will commit segment 3)

    // writer still holds a reference to segments_2
    // because it's under consolidation
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    writer->Commit();
    AssertSnapshotEquality(*writer);  // commit pending merge
    ASSERT_EQ(1 + 1 + count,
              irs::DirectoryCleaner::clean(dir()));  // segments_2

    // check consolidating segments
    expected_consolidating_segments = {};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    // validate structure
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc3);
    expected.back().insert(*doc4);
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(4, reader.live_docs_count());

    // assume 0 is the existing segment
    {
      auto& segment = reader[0];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(2, segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("C",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc3
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("D",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc4
      ASSERT_FALSE(docs_itr->next());
    }

    // assume 1 is merged segment
    {
      auto& segment = reader[1];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(2, segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // consolidate without deletes
  {
    SetUp();
    auto writer = open_writer();
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // segment 2
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    // segment 3
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    ASSERT_TRUE(writer->Begin());  // begin transaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // check consolidating segments
    expected_consolidating_segments = {};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    ASSERT_TRUE(writer->Consolidate(irs::index_utils::MakePolicy(
      irs::index_utils::ConsolidateCount())));  // consolidate

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    // can't consolidate segments that are already marked for consolidation
    ASSERT_FALSE(writer->Consolidate(
      irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    ASSERT_TRUE(Insert(*writer, doc5->indexed.begin(), doc5->indexed.end(),
                       doc5->stored.begin(), doc5->stored.end()));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    writer->Commit();
    AssertSnapshotEquality(
      *writer);  // commit transaction (will commit segment 3)

    // writer still holds a reference to segments_3
    // because it's under consolidation
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    ASSERT_TRUE(Insert(*writer, doc6->indexed.begin(), doc6->indexed.end(),
                       doc6->stored.begin(), doc6->stored.end()));

    writer->Commit();
    AssertSnapshotEquality(
      *writer);               // commit pending merge, segment 4 (doc5 + doc6)
    ASSERT_EQ(count + 1 + 1,  // +1 for segments_3
              irs::DirectoryCleaner::clean(dir()));  // +1 for segments

    // check consolidating segments
    expected_consolidating_segments = {};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    // validate structure
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc3);
    expected.back().insert(*doc4);
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc5);
    expected.back().insert(*doc6);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(3, reader.size());
    ASSERT_EQ(6, reader.live_docs_count());

    // assume 0 is the existing segment
    {
      auto& segment = reader[0];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(2, segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("C",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc3
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("D",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc4
      ASSERT_FALSE(docs_itr->next());
    }

    // assume 1 is merged segment
    {
      auto& segment = reader[1];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(2, segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }

    // assume 2 is the last added segment
    {
      auto& segment = reader[2];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(2, segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("E",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("F",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // consolidate with deletes
  {
    SetUp();
    auto query_doc1 = MakeByTerm("name", "A");

    auto writer = open_writer();
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // segment 2
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    writer->GetBatch().Remove(*query_doc1);
    ASSERT_TRUE(writer->Begin());  // begin transaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // check consolidating segments
    expected_consolidating_segments = {};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    ASSERT_TRUE(writer->Consolidate(irs::index_utils::MakePolicy(
      irs::index_utils::ConsolidateCount())));  // consolidate

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    // can't consolidate segments that are already marked for consolidation
    ASSERT_FALSE(writer->Consolidate(
      irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    writer->Commit();
    AssertSnapshotEquality(
      *writer);  // commit transaction (will commit removal)

    // writer still holds a reference to segments_2
    // because it's under consolidation
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // unused column store

    // check consolidating segments
    expected_consolidating_segments = {0, 1};

    // Check name only because of removals
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments_name_only));

    writer->Commit();
    AssertSnapshotEquality(*writer);  // commit pending merge
    ASSERT_EQ(count + 2 + 1,  // +2 for  segments_2 + stale segment 1 meta
              irs::DirectoryCleaner::clean(dir()));  // +1 for segments

    // check consolidating segments
    expected_consolidating_segments = {};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    // validate structure (doesn't take removals into account)
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    expected.back().insert(*doc3);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(2, reader.live_docs_count());

    // assume 0 is merged segment
    {
      auto& segment = reader[0];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(3, segment.docs_count());       // total count of documents
      ASSERT_EQ(2, segment.live_docs_count());  // total count of live documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      // with deleted docs
      {
        auto values = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, values);
        auto* actual_value = irs::get<irs::PayAttr>(*values);
        ASSERT_NE(nullptr, actual_value);

        auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("A",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc3
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("B",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc3
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("C",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc4
        ASSERT_FALSE(docs_itr->next());
      }

      // without deleted docs
      {
        auto values = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, values);
        auto* actual_value = irs::get<irs::PayAttr>(*values);
        ASSERT_NE(nullptr, actual_value);

        auto docs_itr =
          segment.mask(term_itr->postings(irs::IndexFeatures::None));
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("B",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc3
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("C",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc4
        ASSERT_FALSE(docs_itr->next());
      }
    }
  }

  // consolidate with deletes
  {
    SetUp();
    auto query_doc1_doc4 = MakeByTermOrByTerm("name", "A", "name", "D");

    auto writer = open_writer();
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // segment 2
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    writer->GetBatch().Remove(*query_doc1_doc4);
    ASSERT_TRUE(writer->Begin());  // begin transaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // check consolidating segments
    expected_consolidating_segments = {};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    ASSERT_TRUE(writer->Consolidate(irs::index_utils::MakePolicy(
      irs::index_utils::ConsolidateCount())));  // consolidate

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    // can't consolidate segments that are already marked for consolidation
    ASSERT_FALSE(writer->Consolidate(
      irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    writer->Commit();
    // commit transaction (will commit removal)
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // unused column store

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments_name_only));

    writer->Commit();
    AssertSnapshotEquality(*writer);  // commit pending merge

    // segments_2 + stale segment 1 meta + stale segment 2 meta +1 for segments,
    ASSERT_EQ(count + 4, irs::DirectoryCleaner::clean(dir()));

    // check consolidating segments
    expected_consolidating_segments = {};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    // validate structure (doesn't take removals into account)
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    expected.back().insert(*doc3);
    expected.back().insert(*doc4);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(2, reader.live_docs_count());

    // assume 0 is merged segment
    {
      auto& segment = reader[0];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(4, segment.docs_count());       // total count of documents
      ASSERT_EQ(2, segment.live_docs_count());  // total count of live documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      // with deleted docs
      {
        auto values = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, values);
        auto* actual_value = irs::get<irs::PayAttr>(*values);
        ASSERT_NE(nullptr, actual_value);

        auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("A",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc3
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("B",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc3
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("C",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc4
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("D",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc4
        ASSERT_FALSE(docs_itr->next());
      }

      // without deleted docs
      {
        auto values = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, values);
        auto* actual_value = irs::get<irs::PayAttr>(*values);
        ASSERT_NE(nullptr, actual_value);

        auto docs_itr =
          segment.mask(term_itr->postings(irs::IndexFeatures::None));
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("B",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc3
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("C",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc4
        ASSERT_FALSE(docs_itr->next());
      }
    }
  }

  // consolidate with delete committed and pending
  {
    SetUp();
    auto query_doc1 = MakeByTerm("name", "A");
    auto query_doc4 = MakeByTerm("name", "D");
    auto writer = open_writer();
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // segment 2
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    writer->GetBatch().Remove(*query_doc1);
    ASSERT_TRUE(writer->Begin());  // begin transaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // check consolidating segments
    expected_consolidating_segments = {};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    auto do_commit_and_consolidate_count =
      [&](irs::Consolidation& candidates, const irs::IndexReader& reader,
          const irs::ConsolidatingSegments& consolidating_segments) {
        auto sub_policy =
          irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount());
        sub_policy(candidates, reader, consolidating_segments);
        writer->Commit();
        AssertSnapshotEquality(*writer);
      };

    ASSERT_TRUE(writer->Consolidate(do_commit_and_consolidate_count));

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments_name_only));

    // can't consolidate segments that are already marked for consolidation
    ASSERT_FALSE(writer->Consolidate(
      irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));

    writer->GetBatch().Remove(*query_doc4);

    writer->Commit();
    AssertSnapshotEquality(*writer);  // commit pending merge + delete
    ASSERT_EQ(count + 6, irs::DirectoryCleaner::clean(dir()));

    // check consolidating segments
    expected_consolidating_segments = {};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    // validate structure (doesn't take removals into account)
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    expected.back().insert(*doc3);
    expected.back().insert(*doc4);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);
    {
      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_TRUE(reader);
      ASSERT_EQ(1, reader.size());
      ASSERT_EQ(2, reader.live_docs_count());
      // assume 0 is merged segment
      {
        auto& segment = reader[0];
        const auto* column = segment.column("name");
        ASSERT_NE(nullptr, column);
        ASSERT_EQ(4, segment.docs_count());  // total count of documents
        ASSERT_EQ(2,
                  segment.live_docs_count());  // total count of live documents
        auto terms = segment.field("same");
        ASSERT_NE(nullptr, terms);
        auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
        ASSERT_TRUE(term_itr->next());

        // with deleted docs
        {
          auto values = column->iterator(irs::ColumnHint::Normal);
          ASSERT_NE(nullptr, values);
          auto* actual_value = irs::get<irs::PayAttr>(*values);
          ASSERT_NE(nullptr, actual_value);

          auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
          ASSERT_TRUE(docs_itr->next());
          ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
          ASSERT_EQ("A",
                    irs::ToString<std::string_view>(
                      actual_value->value.data()));  // 'name' value in doc3
          ASSERT_TRUE(docs_itr->next());
          ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
          ASSERT_EQ("B",
                    irs::ToString<std::string_view>(
                      actual_value->value.data()));  // 'name' value in doc3
          ASSERT_TRUE(docs_itr->next());
          ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
          ASSERT_EQ("C",
                    irs::ToString<std::string_view>(
                      actual_value->value.data()));  // 'name' value in doc4
          ASSERT_TRUE(docs_itr->next());
          ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
          ASSERT_EQ("D",
                    irs::ToString<std::string_view>(
                      actual_value->value.data()));  // 'name' value in doc4
          ASSERT_FALSE(docs_itr->next());
        }

        // without deleted docs
        {
          auto values = column->iterator(irs::ColumnHint::Normal);
          ASSERT_NE(nullptr, values);
          auto* actual_value = irs::get<irs::PayAttr>(*values);
          ASSERT_NE(nullptr, actual_value);

          auto docs_itr =
            segment.mask(term_itr->postings(irs::IndexFeatures::None));
          ASSERT_TRUE(docs_itr->next());
          ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
          ASSERT_EQ("B",
                    irs::ToString<std::string_view>(
                      actual_value->value.data()));  // 'name' value in doc3
          ASSERT_TRUE(docs_itr->next());
          ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
          ASSERT_EQ("C",
                    irs::ToString<std::string_view>(
                      actual_value->value.data()));  // 'name' value in doc4
          ASSERT_FALSE(docs_itr->next());
        }
      }
    }

    // check for dangling old segment versions in writers cache
    // first create new segment
    // segment 5
    ASSERT_TRUE(Insert(*writer, doc5->indexed.begin(), doc5->indexed.end(),
                       doc5->stored.begin(), doc5->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc6->indexed.begin(), doc6->indexed.end(),
                       doc6->stored.begin(), doc6->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // remove one doc from new and old segment to make conolidation do
    // something
    auto query_doc3 = MakeByTerm("name", "C");
    auto query_doc5 = MakeByTerm("name", "E");
    writer->GetBatch().Remove(*query_doc3);
    writer->GetBatch().Remove(*query_doc5);
    writer->Commit();
    AssertSnapshotEquality(*writer);

    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    ASSERT_TRUE(writer->Consolidate(
      irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // check all old segments are deleted (no old version of segments is
    // left in cache and blocking )
    ASSERT_EQ(count + 3,
              irs::DirectoryCleaner::clean(dir()));  // +3 segment files
  }

  // repeatable consolidation of already consolidated segment
  {
    SetUp();
    auto query_doc1 = MakeByTerm("name", "A");
    auto writer = open_writer();
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    // segment 2
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // check consolidating segments
    expected_consolidating_segments = {};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    writer->GetBatch().Remove(*query_doc1);
    ASSERT_TRUE(writer->Begin());  // begin transaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // this consolidation will be postponed
    ASSERT_TRUE(writer->Consolidate(
      irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));
    // check consolidating segments are pending
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    // can't consolidate segments that are already marked for consolidation
    ASSERT_FALSE(writer->Consolidate(
      irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));

    auto do_commit_and_consolidate_count =
      [&](irs::Consolidation& candidates, const irs::IndexReader& reader,
          const irs::ConsolidatingSegments& consolidating_segments) {
        writer->Commit();
        AssertSnapshotEquality(*writer);
        writer->Begin();  // another commit to process pending
                          // consolidating_segments
        writer->Commit();
        AssertSnapshotEquality(*writer);
        auto sub_policy =
          irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount());
        sub_policy(candidates, reader, consolidating_segments);
      };

    // this should fail as segments 1 and 0 are actually consolidated on
    // previous  commit inside our test policy
    ASSERT_FALSE(writer->Consolidate(do_commit_and_consolidate_count));
    ASSERT_NE(0, irs::DirectoryCleaner::clean(dir()));
    // check all data is deleted
    const auto one_segment_count = count;
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));
    // files count should be the same as with one segment
    ASSERT_EQ(one_segment_count, count);
  }

  // repeatable consolidation of already consolidated segment during two
  // phase commit
  {
    SetUp();
    auto query_doc1 = MakeByTerm("name", "A");
    auto query_doc4 = MakeByTerm("name", "D");
    auto writer = open_writer();
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    // segment 2
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // check consolidating segments
    expected_consolidating_segments = {};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    writer->GetBatch().Remove(*query_doc1);
    ASSERT_TRUE(writer->Begin());  // begin transaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // this consolidation will be postponed
    ASSERT_TRUE(writer->Consolidate(
      irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));
    // check consolidating segments are pending
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    // can't consolidate segments that are already marked for consolidation
    ASSERT_FALSE(writer->Consolidate(
      irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));

    auto do_commit_and_consolidate_count =
      [&](irs::Consolidation& candidates, const irs::IndexReader& reader,
          const irs::ConsolidatingSegments& consolidating_segments) {
        writer->Commit();
        AssertSnapshotEquality(*writer);
        writer->Begin();  // another commit to process pending
                          // consolidating_segments
        writer->Commit();
        AssertSnapshotEquality(*writer);
        // new transaction with passed 1st phase
        writer->GetBatch().Remove(*query_doc4);
        writer->Begin();
        auto sub_policy =
          irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount());
        sub_policy(candidates, reader, consolidating_segments);
      };

    // this should fail as segments 1 and 0 are actually consolidated on
    // previous  commit inside our test policy
    ASSERT_FALSE(writer->Consolidate(do_commit_and_consolidate_count));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_NE(0, irs::DirectoryCleaner::clean(dir()));
    // check all data is deleted
    const auto one_segment_count = count;
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));
    // files count should be the same as with one segment
    ASSERT_EQ(one_segment_count, count);
  }

  // check commit rollback and consolidation
  {
    SetUp();
    auto query_doc1 = MakeByTerm("name", "A");
    auto writer = open_writer();
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // segment 2
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));

    writer->GetBatch().Remove(*query_doc1);
    ASSERT_TRUE(writer->Begin());  // begin transaction
    // this consolidation will be postponed
    ASSERT_TRUE(writer->Consolidate(
      irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));
    // check consolidating segments are pending
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    writer->Rollback();

    // leftovers cleanup
    ASSERT_EQ(2, irs::DirectoryCleaner::clean(dir()));

    // still pending
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    writer->GetBatch().Remove(*query_doc1);
    // make next commit
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // now no consolidating should be present
    expected_consolidating_segments = {};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    // could consolidate successfully
    ASSERT_TRUE(writer->Consolidate(
      irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));

    // cleanup should remove old files
    ASSERT_NE(0, irs::DirectoryCleaner::clean(dir()));

    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    expected.back().insert(*doc3);
    expected.back().insert(*doc4);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());  // should be one consolidated segment
    ASSERT_EQ(3, reader.live_docs_count());
  }

  // consolidate with deletes + inserts
  {
    SetUp();
    auto query_doc1_doc4 = MakeByTermOrByTerm("name", "A", "name", "D");

    auto writer = open_writer();
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // segment 2
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    writer->GetBatch().Remove(*query_doc1_doc4);
    ASSERT_TRUE(writer->Begin());  // begin transaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // check consolidating segments
    expected_consolidating_segments = {};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    ASSERT_TRUE(writer->Consolidate(irs::index_utils::MakePolicy(
      irs::index_utils::ConsolidateCount())));  // consolidate

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    // can't consolidate segments that are already marked for consolidation
    ASSERT_FALSE(writer->Consolidate(
      irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    ASSERT_TRUE(Insert(*writer, doc5->indexed.begin(), doc5->indexed.end(),
                       doc5->stored.begin(), doc5->stored.end()));
    writer->Commit();
    // commit transaction (will commit removal)
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  //  unused column store

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments_name_only));

    writer->Commit();
    AssertSnapshotEquality(*writer);  // commit pending merge

    // segments_2 + stale segment 1 meta + stale segment 2 meta +1 for segments,
    ASSERT_EQ(count + 4, irs::DirectoryCleaner::clean(dir()));

    // check consolidating segments
    expected_consolidating_segments = {};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    // validate structure (doesn't take removals into account)
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    expected.back().insert(*doc3);
    expected.back().insert(*doc4);
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc5);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(3, reader.live_docs_count());

    // assume 0 is merged segment
    {
      auto& segment = reader[0];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(4, segment.docs_count());       // total count of documents
      ASSERT_EQ(2, segment.live_docs_count());  // total count of live documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      // with deleted docs
      {
        auto values = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, values);
        auto* actual_value = irs::get<irs::PayAttr>(*values);
        ASSERT_NE(nullptr, actual_value);

        auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("A",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc1
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("B",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc2
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("C",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc3
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("D",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc4
        ASSERT_FALSE(docs_itr->next());
      }

      // without deleted docs
      {
        auto values = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, values);
        auto* actual_value = irs::get<irs::PayAttr>(*values);
        ASSERT_NE(nullptr, actual_value);

        auto docs_itr =
          segment.mask(term_itr->postings(irs::IndexFeatures::None));
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("B",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc3
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("C",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc4
        ASSERT_FALSE(docs_itr->next());
      }
    }

    // assume 1 is the recently added segment
    {
      auto& segment = reader[1];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of live documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("E",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
    }
  }

  // consolidate with deletes + inserts
  {
    SetUp();
    auto query_doc1_doc4 = MakeByTermOrByTerm("name", "A", "name", "D");

    auto writer = open_writer();
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // segment 2
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    ASSERT_TRUE(Insert(*writer, doc5->indexed.begin(), doc5->indexed.end(),
                       doc5->stored.begin(), doc5->stored.end()));

    // check consolidating segments
    expected_consolidating_segments = {};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    ASSERT_TRUE(writer->Begin());  // begin transaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    ASSERT_TRUE(writer->Consolidate(irs::index_utils::MakePolicy(
      irs::index_utils::ConsolidateCount())));  // consolidate

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    // can't consolidate segments that are already marked for consolidation
    ASSERT_FALSE(writer->Consolidate(
      irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    writer->Commit();
    AssertSnapshotEquality(*writer);  // commit transaction

    // writer still holds a reference to segments_2
    // because it's under consolidation
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));  // segments_2

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    writer->GetBatch().Remove(*query_doc1_doc4);
    writer->Commit();
    AssertSnapshotEquality(*writer);  // commit pending merge + removal

    // +1 for segments,
    // +1 for segment 1 meta,
    // +1 for segment 2 meta + unused column store,
    // +1 for segments_2
    ASSERT_EQ(count + 5, irs::DirectoryCleaner::clean(dir()));

    // check consolidating segments
    expected_consolidating_segments = {};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    // validate structure (doesn't take removals into account)
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc5);
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    expected.back().insert(*doc3);
    expected.back().insert(*doc4);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(3, reader.live_docs_count());

    // assume 1 is the recently added segment
    {
      auto& segment = reader[0];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of live documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("E",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
    }

    // assume 0 is merged segment
    {
      auto& segment = reader[1];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(4, segment.docs_count());       // total count of documents
      ASSERT_EQ(2, segment.live_docs_count());  // total count of live documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      // with deleted docs
      {
        auto values = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, values);
        auto* actual_value = irs::get<irs::PayAttr>(*values);
        ASSERT_NE(nullptr, actual_value);

        auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("A",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc1
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("B",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc2
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("C",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc3
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("D",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc4
        ASSERT_FALSE(docs_itr->next());
      }

      // without deleted docs
      {
        auto values = column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, values);
        auto* actual_value = irs::get<irs::PayAttr>(*values);
        ASSERT_NE(nullptr, actual_value);

        auto docs_itr =
          segment.mask(term_itr->postings(irs::IndexFeatures::None));
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("B",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc3
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ("C",
                  irs::ToString<std::string_view>(
                    actual_value->value.data()));  // 'name' value in doc4
        ASSERT_FALSE(docs_itr->next());
      }
    }
  }

  // consolidate with deletes + inserts
  {
    SetUp();
    auto query_doc3_doc4 = MakeOr({{"name", "C"}, {"name", "D"}});

    auto writer = open_writer();
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // segment 2
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    size_t num_files_segment_2 = count;
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));
    num_files_segment_2 = count - num_files_segment_2;

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    ASSERT_TRUE(Insert(*writer, doc5->indexed.begin(), doc5->indexed.end(),
                       doc5->stored.begin(), doc5->stored.end()));

    // check consolidating segments
    expected_consolidating_segments = {};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    ASSERT_TRUE(writer->Begin());  // begin transaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // check consolidating segments
    expected_consolidating_segments = {};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    ASSERT_TRUE(writer->Consolidate(
      irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    // can't consolidate segments that are already marked for consolidation
    ASSERT_FALSE(writer->Consolidate(
      irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    size_t num_files_consolidation_segment = count;
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));
    num_files_consolidation_segment = count - num_files_consolidation_segment;

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    writer->Commit();
    AssertSnapshotEquality(*writer);  // commit transaction

    // writer still holds a reference to segments_2
    // because it's under consolidation
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));  // segments_2

    // check consolidating segments
    expected_consolidating_segments = {0, 1};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    writer->GetBatch().Remove(*query_doc3_doc4);

    // commit pending merge + removal
    // pending consolidation will fail (because segment 2 will have no live
    // docs after applying removals)
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // check consolidating segments
    expected_consolidating_segments = {};
    ASSERT_TRUE(writer->Consolidate(check_consolidating_segments));

    // +2 for segments_2 + unused column store, +1 for segments_2
    ASSERT_EQ(num_files_consolidation_segment + num_files_segment_2 + 2 + 1,
              irs::DirectoryCleaner::clean(dir()));

    // validate structure (doesn't take removals into account)
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc5);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(3, reader.live_docs_count());

    // assume 0 is first segment
    {
      auto& segment = reader[0];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(2, segment.docs_count());       // total count of documents
      ASSERT_EQ(2, segment.live_docs_count());  // total count of live documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }

    // assume 1 is the recently added segment
    {
      auto& segment = reader[1];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of live documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("E",
                irs::ToString<std::string_view>(
                  actual_value->value.data()));  // 'name' value in doc1
    }
  }
}

TEST_P(IndexTestCase, consolidate_progress) {
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);
  auto* doc1 = gen.next();
  auto* doc2 = gen.next();
  auto policy =
    irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount());

  // test default progress (false)
  {
    irs::MemoryDirectory dir;
    auto writer = irs::IndexWriter::Make(dir, get_codec(), irs::kOmCreate);
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();  // create segment0
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir, get_codec()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();  // create segment1

    auto reader = writer->GetSnapshot();
    tests::AssertSnapshotEquality(reader,
                                  irs::DirectoryReader(dir, get_codec()));

    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(1, reader[0].docs_count());
    ASSERT_EQ(1, reader[1].docs_count());

    irs::MergeWriter::FlushProgress progress;

    ASSERT_TRUE(writer->Consolidate(policy, get_codec(), progress));
    writer->Commit();  // write consolidated segment
    reader = irs::DirectoryReader(dir, get_codec());

    tests::AssertSnapshotEquality(writer->GetSnapshot(), reader);

    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(2, reader[0].docs_count());
  }

  // test always-false progress
  {
    irs::MemoryDirectory dir;
    auto writer = irs::IndexWriter::Make(dir, get_codec(), irs::kOmCreate);
    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();  // create segment0
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir, get_codec()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();  // create segment1

    auto reader = writer->GetSnapshot();
    tests::AssertSnapshotEquality(reader,
                                  irs::DirectoryReader(dir, get_codec()));

    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(1, reader[0].docs_count());
    ASSERT_EQ(1, reader[1].docs_count());

    irs::MergeWriter::FlushProgress progress = []() -> bool { return false; };

    ASSERT_FALSE(writer->Consolidate(policy, get_codec(), progress));
    writer->Commit();  // write consolidated segment
    reader = writer->GetSnapshot();
    tests::AssertSnapshotEquality(reader,
                                  irs::DirectoryReader(dir, get_codec()));

    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(1, reader[0].docs_count());
    ASSERT_EQ(1, reader[1].docs_count());
  }

  size_t progress_call_count = 0;

  const size_t max_docs = 32768;

  // test always-true progress
  {
    irs::MemoryDirectory dir;
    auto writer = irs::IndexWriter::Make(dir, get_codec(), irs::kOmCreate);

    for (size_t size = 0; size < max_docs; ++size) {
      ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                         doc1->stored.begin(), doc1->stored.end()));
    }
    writer->Commit();  // create segment0
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir, get_codec()));

    for (size_t size = 0; size < max_docs; ++size) {
      ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                         doc2->stored.begin(), doc2->stored.end()));
    }
    writer->Commit();  // create segment1
    auto reader = irs::DirectoryReader(dir, get_codec());
    tests::AssertSnapshotEquality(writer->GetSnapshot(), reader);

    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(max_docs, reader[0].docs_count());
    ASSERT_EQ(max_docs, reader[1].docs_count());

    irs::MergeWriter::FlushProgress progress =
      [&progress_call_count]() -> bool {
      ++progress_call_count;
      return true;
    };

    ASSERT_TRUE(writer->Consolidate(policy, get_codec(), progress));
    writer->Commit();  // write consolidated segment
    reader = writer->GetSnapshot();
    tests::AssertSnapshotEquality(reader,
                                  irs::DirectoryReader(dir, get_codec()));

    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(2 * max_docs, reader[0].docs_count());
  }

  // there should have been at least some calls
  ASSERT_TRUE(progress_call_count);

  // test limited-true progress
  // +1 for pre-decrement in 'progress'
  for (size_t i = 1; i < progress_call_count; ++i) {
    size_t call_count = i;
    irs::MemoryDirectory dir;
    auto writer = irs::IndexWriter::Make(dir, get_codec(), irs::kOmCreate);
    for (size_t size = 0; size < max_docs; ++size) {
      ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                         doc1->stored.begin(), doc1->stored.end()));
    }
    writer->Commit();  // create segment0
    tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                  irs::DirectoryReader(dir, get_codec()));

    for (size_t size = 0; size < max_docs; ++size) {
      ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                         doc2->stored.begin(), doc2->stored.end()));
    }
    writer->Commit();  // create segment0
    auto reader = irs::DirectoryReader(dir, get_codec());
    tests::AssertSnapshotEquality(writer->GetSnapshot(), reader);

    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(max_docs, reader[0].docs_count());
    ASSERT_EQ(max_docs, reader[1].docs_count());

    irs::MergeWriter::FlushProgress progress = [&call_count]() -> bool {
      return --call_count;
    };

    ASSERT_FALSE(writer->Consolidate(policy, get_codec(), progress));
    writer->Commit();  // write consolidated segment

    reader = irs::DirectoryReader(dir, get_codec());
    tests::AssertSnapshotEquality(writer->GetSnapshot(), reader);

    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(max_docs, reader[0].docs_count());
    ASSERT_EQ(max_docs, reader[1].docs_count());
  }
}

TEST_P(IndexTestCase, segment_consolidate) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();
  const tests::Document* doc4 = gen.next();
  const tests::Document* doc5 = gen.next();
  const tests::Document* doc6 = gen.next();

  auto always_merge =
    irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount());
  constexpr irs::IndexFeatures kAllFeatures =
    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
    irs::IndexFeatures::Offs | irs::IndexFeatures::Pay;

  // remove empty new segment
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->GetBatch().Remove(std::move(query_doc1));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(0, reader.size());
  }

  // remove empty old segment
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    writer->GetBatch().Remove(std::move(query_doc1));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(0, reader.size());
  }

  // remove empty old, defragment new
  {
    auto query_doc1_doc2 = MakeByTermOrByTerm("name", "A", "name", "B");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    writer->GetBatch().Remove(std::move(query_doc1_doc2));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    ASSERT_TRUE(writer->Consolidate(always_merge));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // validate structure
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc3);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    ASSERT_EQ(1, segment.docs_count());  // total count of documents
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

  // remove empty old, defragment new
  {
    auto query_doc1_doc2 = MakeByTermOrByTerm("name", "A", "name", "B");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    writer->GetBatch().Remove(std::move(query_doc1_doc2));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(writer->Consolidate(always_merge));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // validate structure
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc3);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    ASSERT_EQ(1, segment.docs_count());  // total count of documents
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

  // remove empty old, defragment old
  {
    auto query_doc1_doc2 = MakeByTermOrByTerm("name", "A", "name", "B");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    writer->GetBatch().Remove(std::move(query_doc1_doc2));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(writer->Consolidate(always_merge));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // validate structure
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc3);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];           // assume 0 is id of first/only segment
    ASSERT_EQ(1, segment.docs_count());  // total count of documents
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
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

  // remove empty old, defragment old
  {
    auto query_doc1_doc2 = MakeByTermOrByTerm("name", "A", "name", "B");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    writer->GetBatch().Remove(std::move(query_doc1_doc2));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(writer->Consolidate(always_merge));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // validate structure
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc3);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];           // assume 0 is id of first/only segment
    ASSERT_EQ(1, segment.docs_count());  // total count of documents
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
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

  auto merge_if_masked = [](irs::Consolidation& candidates,
                            const irs::IndexReader& reader,
                            const irs::ConsolidatingSegments&) -> void {
    for (auto& segment : reader) {
      if (segment.Meta().live_docs_count != segment.Meta().docs_count) {
        candidates.emplace_back(&segment);
      }
    }
  };

  // do defragment old segment with uncommited removal (i.e. do not consider
  // uncomitted removals)
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    writer->GetBatch().Remove(std::move(query_doc1));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(writer->Consolidate(merge_if_masked));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    {
      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(1, reader.size());
      auto& segment = reader[0];  // assume 0 is id of first/only segment
      ASSERT_EQ(1, segment.docs_count());  // total count of documents
    }
  }

  // do not defragment old segment with uncommited removal (i.e. do not
  // consider uncomitted removals)
  {
    auto query_doc1 = MakeByTerm("name", "A");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    writer->GetBatch().Remove(std::move(query_doc1));
    ASSERT_TRUE(writer->Consolidate(merge_if_masked));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    {
      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(1, reader.size());
      auto& segment = reader[0];  // assume 0 is id of first/only segment
      ASSERT_EQ(2, segment.docs_count());  // total count of documents
    }

    ASSERT_TRUE(writer->Consolidate(
      merge_if_masked));  // previous removal now committed and considered
    writer->Commit();
    AssertSnapshotEquality(*writer);

    {
      auto reader = irs::DirectoryReader(dir(), codec());
      ASSERT_EQ(1, reader.size());
      auto& segment = reader[0];  // assume 0 is id of first/only segment
      ASSERT_EQ(1, segment.docs_count());  // total count of documents
    }
  }

  // merge new+old segment
  {
    auto query_doc1_doc3 = MakeByTermOrByTerm("name", "A", "name", "C");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->GetBatch().Remove(std::move(query_doc1_doc3));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(writer->Consolidate(always_merge));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // validate structure
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc2);
    expected.back().insert(*doc4);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];           // assume 0 is id of first/only segment
    ASSERT_EQ(2, segment.docs_count());  // total count of documents
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("D", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc4
    ASSERT_FALSE(docs_itr->next());
  }

  // merge new+old segment
  {
    auto query_doc1_doc3 = MakeByTermOrByTerm("name", "A", "name", "C");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->GetBatch().Remove(std::move(query_doc1_doc3));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(writer->Consolidate(always_merge));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // validate structure
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc2);
    expected.back().insert(*doc4);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];           // assume 0 is id of first/only segment
    ASSERT_EQ(2, segment.docs_count());  // total count of documents
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("D", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc4
    ASSERT_FALSE(docs_itr->next());
  }

  // merge old+old segment
  {
    auto query_doc1_doc3 = MakeByTermOrByTerm("name", "A", "name", "C");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    writer->GetBatch().Remove(std::move(query_doc1_doc3));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(writer->Consolidate(always_merge));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // validate structure
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc2);
    expected.back().insert(*doc4);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];           // assume 0 is id of first/only segment
    ASSERT_EQ(2, segment.docs_count());  // total count of documents
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("D", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc4
    ASSERT_FALSE(docs_itr->next());
  }

  // merge old+old segment
  {
    auto query_doc1_doc3 = MakeByTermOrByTerm("name", "A", "name", "C");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    writer->GetBatch().Remove(std::move(query_doc1_doc3));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(writer->Consolidate(always_merge));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // validate structure
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc2);
    expected.back().insert(*doc4);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];           // assume 0 is id of first/only segment
    ASSERT_EQ(2, segment.docs_count());  // total count of documents
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("D", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc4
    ASSERT_FALSE(docs_itr->next());
  }

  // merge old+old+old segment
  {
    auto query_doc1_doc3_doc5 =
      MakeOr({{"name", "A"}, {"name", "C"}, {"name", "E"}});
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc5->indexed.begin(), doc5->indexed.end(),
                       doc5->stored.begin(), doc5->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc6->indexed.begin(), doc6->indexed.end(),
                       doc6->stored.begin(), doc6->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    writer->GetBatch().Remove(std::move(query_doc1_doc3_doc5));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(writer->Consolidate(always_merge));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // validate structure
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc2);
    expected.back().insert(*doc4);
    expected.back().insert(*doc6);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];           // assume 0 is id of first/only segment
    ASSERT_EQ(3, segment.docs_count());  // total count of documents
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("D", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc4
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("F", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc6
    ASSERT_FALSE(docs_itr->next());
  }

  // merge old+old+old segment
  {
    auto query_doc1_doc3_doc5 =
      MakeOr({{"name", "A"}, {"name", "C"}, {"name", "E"}});
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc5->indexed.begin(), doc5->indexed.end(),
                       doc5->stored.begin(), doc5->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc6->indexed.begin(), doc6->indexed.end(),
                       doc6->stored.begin(), doc6->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    writer->GetBatch().Remove(std::move(query_doc1_doc3_doc5));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(writer->Consolidate(always_merge));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // validate structure
    tests::index_t expected;
    expected.emplace_back(writer->FeatureInfo());
    expected.back().insert(*doc2);
    expected.back().insert(*doc4);
    expected.back().insert(*doc6);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];           // assume 0 is id of first/only segment
    ASSERT_EQ(3, segment.docs_count());  // total count of documents
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("D", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc4
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("F", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc6
    ASSERT_FALSE(docs_itr->next());
  }

  // merge two segments with different fields
  {
    auto writer = open_writer();
    // add 1st segment
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc6->indexed.begin(), doc6->indexed.end(),
                       doc6->stored.begin(), doc6->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // add 2nd segment
    tests::JsonDocGenerator gen(
      resource("simple_sequential_upper_case.json"),
      [](tests::Document& doc, const std::string& name,
         const tests::JsonDocGenerator::JsonValue& data) {
        if (data.is_string()) {
          doc.insert(std::make_shared<tests::StringField>(name, data.str));
        }
      });

    auto doc1_1 = gen.next();
    auto doc1_2 = gen.next();
    auto doc1_3 = gen.next();
    ASSERT_TRUE(Insert(*writer, doc1_1->indexed.begin(), doc1_1->indexed.end(),
                       doc1_1->stored.begin(), doc1_1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc1_2->indexed.begin(), doc1_2->indexed.end(),
                       doc1_2->stored.begin(), doc1_2->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc1_3->indexed.begin(), doc1_3->indexed.end(),
                       doc1_3->stored.begin(), doc1_3->stored.end()));

    // defragment segments
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(writer->Consolidate(always_merge));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // validate merged segment
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];           // assume 0 is id of first/only segment
    ASSERT_EQ(6, segment.docs_count());  // total count of documents

    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    const auto* upper_case_column = segment.column("NAME");
    ASSERT_NE(nullptr, upper_case_column);
    auto upper_case_values =
      upper_case_column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, upper_case_values);
    auto* upper_case_actual_value = irs::get<irs::PayAttr>(*upper_case_values);
    ASSERT_NE(nullptr, upper_case_actual_value);

    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("D", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc4
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("F", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc6
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), upper_case_values->seek(docs_itr->value()));
    ASSERT_EQ(
      "A",
      irs::ToString<std::string_view>(
        upper_case_actual_value->value.data()));  // 'name' value in doc1_1
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), upper_case_values->seek(docs_itr->value()));
    ASSERT_EQ(
      "B",
      irs::ToString<std::string_view>(
        upper_case_actual_value->value.data()));  // 'name' value in doc1_2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), upper_case_values->seek(docs_itr->value()));
    ASSERT_EQ(
      "C",
      irs::ToString<std::string_view>(
        upper_case_actual_value->value.data()));  // 'name' value in doc1_3
    ASSERT_FALSE(docs_itr->next());
  }

  // merge two segments with different fields
  {
    auto writer = open_writer();
    // add 1st segment
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc6->indexed.begin(), doc6->indexed.end(),
                       doc6->stored.begin(), doc6->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // add 2nd segment
    tests::JsonDocGenerator gen(
      resource("simple_sequential_upper_case.json"),
      [](tests::Document& doc, const std::string& name,
         const tests::JsonDocGenerator::JsonValue& data) {
        if (data.is_string()) {
          doc.insert(std::make_shared<tests::StringField>(name, data.str));
        }
      });

    auto doc1_1 = gen.next();
    auto doc1_2 = gen.next();
    auto doc1_3 = gen.next();
    ASSERT_TRUE(Insert(*writer, doc1_1->indexed.begin(), doc1_1->indexed.end(),
                       doc1_1->stored.begin(), doc1_1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc1_2->indexed.begin(), doc1_2->indexed.end(),
                       doc1_2->stored.begin(), doc1_2->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc1_3->indexed.begin(), doc1_3->indexed.end(),
                       doc1_3->stored.begin(), doc1_3->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // defragment segments
    ASSERT_TRUE(writer->Consolidate(always_merge));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // validate merged segment
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];           // assume 0 is id of first/only segment
    ASSERT_EQ(6, segment.docs_count());  // total count of documents

    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    const auto* upper_case_column = segment.column("NAME");
    ASSERT_NE(nullptr, upper_case_column);
    auto upper_case_values =
      upper_case_column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, upper_case_values);
    auto* upper_case_actual_value = irs::get<irs::PayAttr>(*upper_case_values);
    ASSERT_NE(nullptr, upper_case_actual_value);

    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("D", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc4
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
    ASSERT_EQ("F", irs::ToString<std::string_view>(
                     actual_value->value.data()));  // 'name' value in doc6
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), upper_case_values->seek(docs_itr->value()));
    ASSERT_EQ(
      "A",
      irs::ToString<std::string_view>(
        upper_case_actual_value->value.data()));  // 'name' value in doc1_1
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), upper_case_values->seek(docs_itr->value()));
    ASSERT_EQ(
      "B",
      irs::ToString<std::string_view>(
        upper_case_actual_value->value.data()));  // 'name' value in doc1_2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(docs_itr->value(), upper_case_values->seek(docs_itr->value()));
    ASSERT_EQ(
      "C",
      irs::ToString<std::string_view>(
        upper_case_actual_value->value.data()));  // 'name' value in doc1_3
    ASSERT_FALSE(docs_itr->next());
  }
}

TEST_P(IndexTestCase, segment_consolidate_policy) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();
  const tests::Document* doc4 = gen.next();
  const tests::Document* doc5 = gen.next();
  const tests::Document* doc6 = gen.next();

  // bytes size policy (merge)
  {
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc5->indexed.begin(), doc5->indexed.end(),
                       doc5->stored.begin(), doc5->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc6->indexed.begin(), doc6->indexed.end(),
                       doc6->stored.begin(), doc6->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    irs::index_utils::ConsolidateBytes options;
    options.threshold = 1;
    ASSERT_TRUE(writer->Consolidate(
      irs::index_utils::MakePolicy(options)));  // value garanteeing merge
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(2, reader.size());  // 1+(2|3)

    // check 1st segment
    {
      std::unordered_set<std::string_view> expected_name = {"A", "B", "C", "D"};
      auto& segment = reader[0];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(expected_name.size(),
                segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ(1, expected_name.erase(irs::ToString<std::string_view>(
                       actual_value->value.data())));
      }

      ASSERT_TRUE(expected_name.empty());
    }

    // check 2nd (merged) segment
    {
      std::unordered_set<std::string_view> expected_name = {"E", "F"};
      auto& segment = reader[1];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(expected_name.size(),
                segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ(1, expected_name.erase(irs::ToString<std::string_view>(
                       actual_value->value.data())));
      }

      ASSERT_TRUE(expected_name.empty());
    }
  }

  // bytes size policy (not modified)
  {
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc5->indexed.begin(), doc5->indexed.end(),
                       doc5->stored.begin(), doc5->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    irs::index_utils::ConsolidateBytes options;
    options.threshold = 0;
    ASSERT_TRUE(writer->Consolidate(
      irs::index_utils::MakePolicy(options)));  // value garanteeing non-merge
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(2, reader.size());

    {
      std::unordered_set<std::string_view> expected_name = {"A", "B", "C", "D"};

      auto& segment = reader[0];  // assume 0 is id of first segment
      ASSERT_EQ(expected_name.size(),
                segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ(1, expected_name.erase(irs::ToString<std::string_view>(
                       actual_value->value.data())));
      }

      ASSERT_TRUE(expected_name.empty());
    }

    {
      std::unordered_set<std::string_view> expected_name = {"E"};

      auto& segment = reader[1];  // assume 1 is id of second segment
      ASSERT_EQ(expected_name.size(),
                segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ(1, expected_name.erase(irs::ToString<std::string_view>(
                       actual_value->value.data())));
      }

      ASSERT_TRUE(expected_name.empty());
    }
  }

  // valid segment bytes_accum policy (merge)
  {
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    irs::index_utils::ConsolidateBytesAccum options;
    options.threshold = 1;
    ASSERT_TRUE(writer->Consolidate(
      irs::index_utils::MakePolicy(options)));  // value garanteeing merge
    writer->Commit();
    AssertSnapshotEquality(*writer);
    // segments merged because segment[0] is a candidate and needs to be
    // merged with something

    std::unordered_set<std::string_view> expected_name = {"A", "B"};

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    ASSERT_EQ(expected_name.size(),
              segment.docs_count());  // total count of documents
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());

    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
         docs_itr->next();) {
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ(1, expected_name.erase(irs::ToString<std::string_view>(
                     actual_value->value.data())));
    }

    ASSERT_TRUE(expected_name.empty());
  }

  // valid segment bytes_accum policy (not modified)
  {
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    irs::index_utils::ConsolidateBytesAccum options;
    options.threshold = 0;
    ASSERT_TRUE(writer->Consolidate(
      irs::index_utils::MakePolicy(options)));  // value garanteeing non-merge
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(2, reader.size());

    {
      std::unordered_set<std::string_view> expected_name = {"A"};

      auto& segment = reader[0];  // assume 0 is id of first segment
      ASSERT_EQ(expected_name.size(),
                segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ(1, expected_name.erase(irs::ToString<std::string_view>(
                       actual_value->value.data())));
      }

      ASSERT_TRUE(expected_name.empty());
    }

    {
      std::unordered_set<std::string_view> expected_name = {"B"};

      auto& segment = reader[1];  // assume 1 is id of second segment
      ASSERT_EQ(expected_name.size(), segment.docs_count());
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ(1, expected_name.erase(irs::ToString<std::string_view>(
                       actual_value->value.data())));
      }

      ASSERT_TRUE(expected_name.empty());
    }
  }

  // valid docs count policy (merge)
  {
    auto query_doc2_doc3_doc4 =
      MakeOr({{"name", "B"}, {"name", "C"}, {"name", "D"}});
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    writer->GetBatch().Remove(std::move(query_doc2_doc3_doc4));
    ASSERT_TRUE(Insert(*writer, doc5->indexed.begin(), doc5->indexed.end(),
                       doc5->stored.begin(), doc5->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    irs::index_utils::ConsolidateDocsLive options;
    options.threshold = 1;
    ASSERT_TRUE(writer->Consolidate(
      irs::index_utils::MakePolicy(options)));  // value garanteeing merge
    writer->Commit();
    AssertSnapshotEquality(*writer);

    std::unordered_set<std::string_view> expected_name = {"A", "E"};

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    ASSERT_EQ(expected_name.size(),
              segment.docs_count());  // total count of documents
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());

    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
         docs_itr->next();) {
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ(1, expected_name.erase(irs::ToString<std::string_view>(
                     actual_value->value.data())));
    }

    ASSERT_TRUE(expected_name.empty());
  }

  // valid docs count policy (not modified)
  {
    auto query_doc2_doc3_doc4 =
      MakeOr({{"name", "B"}, {"name", "C"}, {"name", "D"}});
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    writer->GetBatch().Remove(std::move(query_doc2_doc3_doc4));
    ASSERT_TRUE(Insert(*writer, doc5->indexed.begin(), doc5->indexed.end(),
                       doc5->stored.begin(), doc5->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    irs::index_utils::ConsolidateDocsLive options;
    options.threshold = 0;
    ASSERT_TRUE(writer->Consolidate(
      irs::index_utils::MakePolicy(options)));  // value garanteeing non-merge
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(2, reader.size());

    {
      std::unordered_set<std::string_view> expected_name = {"A"};

      auto& segment = reader[0];  // assume 0 is id of first segment
      ASSERT_EQ(expected_name.size() + 3,
                segment.docs_count());  // total count of documents (+3 ==
                                        // B, C, D masked)
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      for (auto docs_itr =
             segment.mask(term_itr->postings(irs::IndexFeatures::None));
           docs_itr->next();) {
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ(1, expected_name.erase(irs::ToString<std::string_view>(
                       actual_value->value.data())));
      }

      ASSERT_TRUE(expected_name.empty());
    }

    {
      std::unordered_set<std::string_view> expected_name = {"E"};

      auto& segment = reader[1];  // assume 1 is id of second segment
      ASSERT_EQ(expected_name.size(),
                segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ(1, expected_name.erase(irs::ToString<std::string_view>(
                       actual_value->value.data())));
      }

      ASSERT_TRUE(expected_name.empty());
    }
  }

  // valid segment fill policy (merge)
  {
    auto query_doc2_doc4 = MakeByTermOrByTerm("name", "B", "name", "D");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    writer->GetBatch().Remove(std::move(query_doc2_doc4));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    irs::index_utils::ConsolidateDocsFill options;
    options.threshold = 1;
    ASSERT_TRUE(writer->Consolidate(
      irs::index_utils::MakePolicy(options)));  // value garanteeing merge
    writer->Commit();
    AssertSnapshotEquality(*writer);

    std::unordered_set<std::string_view> expected_name = {"A", "C"};

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    ASSERT_EQ(expected_name.size(),
              segment.docs_count());  // total count of documents
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());

    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
         docs_itr->next();) {
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ(1, expected_name.erase(irs::ToString<std::string_view>(
                     actual_value->value.data())));
    }

    ASSERT_TRUE(expected_name.empty());
  }

  // valid segment fill policy (not modified)
  {
    auto query_doc2_doc4 = MakeByTermOrByTerm("name", "B", "name", "D");
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end()));
    ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                       doc4->stored.begin(), doc4->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    writer->GetBatch().Remove(std::move(query_doc2_doc4));
    writer->Commit();
    AssertSnapshotEquality(*writer);
    irs::index_utils::ConsolidateDocsFill options;
    options.threshold = 0;
    ASSERT_TRUE(writer->Consolidate(
      irs::index_utils::MakePolicy(options)));  // value garanteeing non-merge
    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(2, reader.size());

    {
      std::unordered_set<std::string_view> expected_name = {"A"};

      auto& segment = reader[0];  // assume 0 is id of first segment
      ASSERT_EQ(
        expected_name.size() + 1,
        segment.docs_count());  // total count of documents (+1 == B masked)
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      for (auto docs_itr =
             segment.mask(term_itr->postings(irs::IndexFeatures::None));
           docs_itr->next();) {
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ(1, expected_name.erase(irs::ToString<std::string_view>(
                       actual_value->value.data())));
      }

      ASSERT_TRUE(expected_name.empty());
    }

    {
      std::unordered_set<std::string_view> expected_name = {"C"};

      auto& segment = reader[1];  // assume 1 is id of second segment
      ASSERT_EQ(
        expected_name.size() + 1,
        segment.docs_count());  // total count of documents (+1 == D masked)
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      for (auto docs_itr =
             segment.mask(term_itr->postings(irs::IndexFeatures::None));
           docs_itr->next();) {
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ(1, expected_name.erase(irs::ToString<std::string_view>(
                       actual_value->value.data())));
      }

      ASSERT_TRUE(expected_name.empty());
    }
  }
}

TEST_P(IndexTestCase, segment_options) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();

  // segment_count_max
  {
    auto writer = open_writer();
    auto ctx = writer->GetBatch();  // hold a single segment

    {
      auto doc = ctx.Insert();
      ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(doc1->indexed.begin(),
                                                 doc1->indexed.end()));
      ASSERT_TRUE(doc.Insert<irs::Action::STORE>(doc1->stored.begin(),
                                                 doc1->stored.end()));
    }

    irs::SegmentOptions options;
    options.segment_count_max = 1;
    writer->Options(options);

    std::condition_variable cond;
    std::mutex mutex;
    std::unique_lock lock{mutex};
    std::atomic<bool> stop(false);

    std::thread thread([&writer, &doc2, &cond, &mutex, &stop]() -> void {
      ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                         doc2->stored.begin(), doc2->stored.end()));
      stop = true;
      std::lock_guard lock{mutex};
      cond.notify_all();
    });

    auto result =
      cond.wait_for(lock, 1000ms);  // assume thread blocks in 1000ms

    // As declaration for wait_for contains "It may also be unblocked
    // spuriously." for all platforms
    while (!stop && result == std::cv_status::no_timeout)
      result = cond.wait_for(lock, 1000ms);

    ASSERT_EQ(std::cv_status::timeout, result);
    // ^^^ expecting timeout because pool should block indefinitely

    {
      irs::IndexWriter::Transaction(std::move(ctx));
    }  // force flush of GetBatch(), i.e. ulock segment
    // ASSERT_EQ(std::cv_status::no_timeout, cond.wait_for(lock, 1000ms));
    lock.unlock();
    thread.join();
    ASSERT_TRUE(stop);

    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());

    // check only segment
    {
      std::unordered_set<std::string_view> expected_name = {"A", "B"};
      auto& segment = reader[0];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(expected_name.size(),
                segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ(1, expected_name.erase(irs::ToString<std::string_view>(
                       actual_value->value.data())));
      }

      ASSERT_TRUE(expected_name.empty());
    }
  }

  // segment_docs_max
  {
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    irs::SegmentOptions options;
    options.segment_docs_max = 1;
    writer->Options(options);

    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));

    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(2, reader.size());  // 1+2

    // check 1st segment
    {
      std::unordered_set<std::string_view> expected_name = {"A"};
      auto& segment = reader[0];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(expected_name.size(),
                segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ(1, expected_name.erase(irs::ToString<std::string_view>(
                       actual_value->value.data())));
      }

      ASSERT_TRUE(expected_name.empty());
    }

    // check 2nd segment
    {
      std::unordered_set<std::string_view> expected_name = {"B"};
      auto& segment = reader[1];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(expected_name.size(),
                segment.docs_count());  // total count of documents
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ(1, expected_name.erase(irs::ToString<std::string_view>(
                       actual_value->value.data())));
      }

      ASSERT_TRUE(expected_name.empty());
    }
  }

  // segment_memory_max
  {
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    irs::SegmentOptions options;
    options.segment_memory_max = 1;
    writer->Options(options);

    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));

    writer->Commit();
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(2, reader.size());  // 1+2

    // check 1st segment
    {
      std::unordered_set<std::string_view> expected_name = {"A"};
      auto& segment = reader[0];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      // total count of documents
      ASSERT_EQ(expected_name.size(), segment.docs_count());
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ(1, expected_name.erase(irs::ToString<std::string_view>(
                       actual_value->value.data())));
      }

      ASSERT_TRUE(expected_name.empty());
    }

    // check 2nd segment
    {
      std::unordered_set<std::string_view> expected_name = {"B"};
      auto& segment = reader[1];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      // total count of documents
      ASSERT_EQ(expected_name.size(), segment.docs_count());
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ(1, expected_name.erase(irs::ToString<std::string_view>(
                       actual_value->value.data())));
      }

      ASSERT_TRUE(expected_name.empty());
    }
  }

  // no_flush
  {
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    irs::SegmentOptions options;
    options.segment_docs_max = 1;
    writer->Options(options);

    // prevent segment from being flushed
    {
      auto ctx = writer->GetBatch();
      auto doc = ctx.Insert(true);

      ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(std::begin(doc2->indexed),
                                                 std::end(doc2->indexed)) &&
                  doc.Insert<irs::Action::STORE>(std::begin(doc2->stored),
                                                 std::end(doc2->stored)));
    }

    ASSERT_TRUE(writer->Commit());
    AssertSnapshotEquality(*writer);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_NE(nullptr, reader);
    ASSERT_EQ(1, reader.size());

    // check 1st segment
    {
      std::unordered_set<std::string_view> expected_name = {"A", "B"};
      auto& segment = reader[0];
      const auto* column = segment.column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);
      // total count of documents
      ASSERT_EQ(expected_name.size(), segment.docs_count());
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
        ASSERT_EQ(1, expected_name.erase(irs::ToString<std::string_view>(
                       actual_value->value.data())));
      }

      ASSERT_TRUE(expected_name.empty());
    }
  }
}

TEST_P(IndexTestCase, writer_close) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);
  auto& directory = dir();
  auto* doc = gen.next();

  {
    auto writer = open_writer();

    ASSERT_TRUE(Insert(*writer, doc->indexed.begin(), doc->indexed.end(),
                       doc->stored.begin(), doc->stored.end()));
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }  // ensure writer is closed

  std::vector<std::string> files;
  auto list_files = [&files](std::string_view name) {
    files.emplace_back(std::move(name));
    return true;
  };
  ASSERT_TRUE(directory.visit(list_files));

  // file removal should pass for all files (especially valid for Microsoft
  // Windows)
  for (auto& file : files) {
    ASSERT_TRUE(directory.remove(file));
  }

  // validate that all files have been removed
  files.clear();
  ASSERT_TRUE(directory.visit(list_files));
  ASSERT_TRUE(files.empty());
}

TEST_P(IndexTestCase, writer_insert_immediate_remove) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);
  auto& directory = dir();
  auto* doc1 = gen.next();
  auto* doc2 = gen.next();
  auto* doc3 = gen.next();
  auto* doc4 = gen.next();

  auto writer = open_writer();

  ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                     doc4->stored.begin(), doc4->stored.end()));

  ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                     doc3->stored.begin(), doc3->stored.end()));

  writer->Commit();
  AssertSnapshotEquality(*writer);  // index should be non-empty

  size_t count = 0;
  auto get_number_of_files_in_segments =
    [&count](std::string_view name) noexcept {
      count += size_t(name.size() && '_' == name[0]);
      return true;
    };
  directory.visit(get_number_of_files_in_segments);
  const auto one_segment_files_count = count;

  // Create segment and immediately do a remove operation
  ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                     doc1->stored.begin(), doc1->stored.end()));

  ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                     doc2->stored.begin(), doc2->stored.end()));

  auto query_doc1 = MakeByTerm("name", "A");
  writer->GetBatch().Remove(*(query_doc1.get()));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // remove for initial segment to trigger consolidation
  // consolidation is needed to force opening all file handles and make
  // cached readers indeed hold reference to a file
  auto query_doc3 = MakeByTerm("name", "C");
  writer->GetBatch().Remove(*(query_doc3.get()));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // this consolidation should bring us to one consolidated segment without
  // removals.
  ASSERT_TRUE(writer->Consolidate(
    irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // file removal should pass for all files (especially valid for Microsoft
  // Windows)
  irs::directory_utils::RemoveAllUnreferenced(directory);

  // validate that all files from old segments have been removed
  count = 0;
  directory.visit(get_number_of_files_in_segments);
  ASSERT_EQ(count, one_segment_files_count);
}

TEST_P(IndexTestCase, writer_insert_immediate_remove_all) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);
  auto& directory = dir();
  auto* doc1 = gen.next();
  auto* doc2 = gen.next();
  auto* doc3 = gen.next();
  auto* doc4 = gen.next();

  auto writer = open_writer();

  ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                     doc4->stored.begin(), doc4->stored.end()));

  ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                     doc3->stored.begin(), doc3->stored.end()));

  writer->Commit();
  AssertSnapshotEquality(*writer);  // index should be non-empty
  size_t count = 0;
  auto get_number_of_files_in_segments =
    [&count](std::string_view name) noexcept {
      count += size_t(name.size() && '_' == name[0]);
      return true;
    };
  directory.visit(get_number_of_files_in_segments);
  const auto one_segment_files_count = count;

  // Create segment and immediately do a remove operation for all added
  // documents
  ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                     doc1->stored.begin(), doc1->stored.end()));

  ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                     doc2->stored.begin(), doc2->stored.end()));

  auto query_doc1 = MakeByTerm("name", "A");
  writer->GetBatch().Remove(*(query_doc1.get()));
  auto query_doc2 = MakeByTerm("name", "B");
  writer->GetBatch().Remove(*(query_doc2.get()));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // remove for initial segment to trigger consolidation
  // consolidation is needed to force opening all file handles and make
  // cached readers indeed hold reference to a file
  auto query_doc3 = MakeByTerm("name", "C");
  writer->GetBatch().Remove(*(query_doc3.get()));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // this consolidation should bring us to one consolidated segment without
  // removes.
  ASSERT_TRUE(writer->Consolidate(
    irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // file removal should pass for all files (especially valid for Microsoft
  // Windows)
  irs::directory_utils::RemoveAllUnreferenced(directory);

  // validate that all files from old segments have been removed
  count = 0;
  directory.visit(get_number_of_files_in_segments);
  ASSERT_EQ(count, one_segment_files_count);
}

TEST_P(IndexTestCase, writer_remove_all_from_last_segment) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);
  auto& directory = dir();
  auto* doc1 = gen.next();
  auto* doc2 = gen.next();

  auto writer = open_writer();

  ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                     doc1->stored.begin(), doc1->stored.end()));

  ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                     doc2->stored.begin(), doc2->stored.end()));

  writer->Commit();
  AssertSnapshotEquality(*writer);  // index should be non-empty
  size_t count = 0;
  auto get_number_of_files_in_segments =
    [&count](std::string_view name) noexcept {
      count += size_t(name.size() && '_' == name[0]);
      return true;
    };
  directory.visit(get_number_of_files_in_segments);
  ASSERT_GT(count, 0);

  // Remove all documents from segment
  auto query_doc1 = MakeByTerm("name", "A");
  writer->GetBatch().Remove(*(query_doc1.get()));
  auto query_doc2 = MakeByTerm("name", "B");
  writer->GetBatch().Remove(*(query_doc2.get()));
  writer->Commit();
  AssertSnapshotEquality(*writer);
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(0, reader.size());
    // file removal should pass for all files (especially valid for
    // Microsoft Windows)
    irs::directory_utils::RemoveAllUnreferenced(directory);

    // validate that all files from old segments have been removed
    count = 0;
    directory.visit(get_number_of_files_in_segments);
    ASSERT_EQ(count, 0);
  }

  // this consolidation should still be ok
  ASSERT_TRUE(writer->Consolidate(
    irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));
  writer->Commit();
  AssertSnapshotEquality(*writer);
}

TEST_P(IndexTestCase, writer_remove_all_from_last_segment_consolidation) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);
  auto& directory = dir();
  auto* doc1 = gen.next();
  auto* doc2 = gen.next();
  auto* doc3 = gen.next();
  auto* doc4 = gen.next();

  auto writer = open_writer();

  ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                     doc1->stored.begin(), doc1->stored.end()));

  ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                     doc2->stored.begin(), doc2->stored.end()));

  writer->Commit();
  AssertSnapshotEquality(*writer);  // segment 1

  ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                     doc3->stored.begin(), doc3->stored.end()));

  ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                     doc4->stored.begin(), doc4->stored.end()));

  writer->Commit();
  AssertSnapshotEquality(*writer);  //  segment 2

  auto query_doc1 = MakeByTerm("name", "A");
  writer->GetBatch().Remove(*(query_doc1.get()));
  auto query_doc3 = MakeByTerm("name", "C");
  writer->GetBatch().Remove(*(query_doc3.get()));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // this consolidation should bring us to one consolidated segment without
  // removes.
  ASSERT_TRUE(writer->Consolidate(
    irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));
  // Remove all documents from 'new' segment
  auto query_doc4 = MakeByTerm("name", "D");
  writer->GetBatch().Remove(*(query_doc4.get()));
  auto query_doc2 = MakeByTerm("name", "B");
  writer->GetBatch().Remove(*(query_doc2.get()));
  writer->Commit();
  AssertSnapshotEquality(*writer);
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(0, reader.size());
    // file removal should pass for all files (especially valid for
    // Microsoft Windows)
    irs::directory_utils::RemoveAllUnreferenced(directory);

    // validate that all files from old segments have been removed
    size_t count{0};
    auto get_number_of_files_in_segments =
      [&count](std::string_view name) noexcept {
        count += size_t(name.size() && '_' == name[0]);
        return true;
      };
    directory.visit(get_number_of_files_in_segments);
    ASSERT_EQ(count, 0);
  }

  // this consolidation should still be ok
  ASSERT_TRUE(writer->Consolidate(
    irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));
  writer->Commit();
  AssertSnapshotEquality(*writer);
}

TEST_P(IndexTestCase, ensure_no_empty_norms_written) {
  struct EmptyTokenizer : irs::Tokenizer {
    bool next() noexcept final { return false; }
    irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
      if (type == irs::Type<irs::IncAttr>::id()) {
        return &inc;
      }

      if (type == irs::Type<irs::TermAttr>::id()) {
        return &term;
      }

      return nullptr;
    }

    irs::IncAttr inc;
    irs::TermAttr term;
  };

  struct EmptyField {
    std::string_view Name() const { return "test"; };
    irs::IndexFeatures GetIndexFeatures() const {
      return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
             irs::IndexFeatures::Norm;
    }
    irs::Tokenizer& GetTokens() const noexcept { return stream; }

    mutable EmptyTokenizer stream;
  } empty;

  {
    irs::IndexWriterOptions opts;
    opts.features = FeaturesWithNorms();

    auto writer = open_writer(irs::kOmCreate, opts);

    // no norms is written as there is nothing to index
    {
      auto docs = writer->GetBatch();
      auto doc = docs.Insert();
      ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(empty));
    }

    // we don't write default norms
    {
      const tests::StringField field(static_cast<std::string>(empty.Name()),
                                     "bar", empty.GetIndexFeatures());
      auto docs = writer->GetBatch();
      auto doc = docs.Insert();
      ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(field));
    }

    {
      const tests::StringField field(static_cast<std::string>(empty.Name()),
                                     "bar", empty.GetIndexFeatures());
      auto docs = writer->GetBatch();
      auto doc = docs.Insert();
      ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(field));
      ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(field));
    }

    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = (*reader)[0];
    ASSERT_EQ(3, segment.docs_count());
    ASSERT_EQ(3, segment.live_docs_count());

    auto field = segment.fields();
    ASSERT_NE(nullptr, field);
    ASSERT_TRUE(field->next());
    auto& field_reader = field->value();
    ASSERT_EQ(empty.Name(), field_reader.meta().name);
    ASSERT_TRUE(irs::IsSubsetOf(irs::IndexFeatures::Norm,
                                field_reader.meta().index_features));
    const auto norm = field_reader.meta().norm;
    ASSERT_TRUE(irs::field_limits::valid(norm));
    ASSERT_FALSE(field->next());
    ASSERT_FALSE(field->next());

    auto column_reader = segment.column(norm);
    ASSERT_NE(nullptr, column_reader);
    ASSERT_EQ(3, column_reader->size());
    auto it = column_reader->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, it);
    auto payload = irs::get<irs::PayAttr>(*it);
    ASSERT_NE(nullptr, payload);
    {
      EXPECT_TRUE(it->next());
      EXPECT_EQ(it->value(), 1);
      irs::BytesViewInput in(payload->value);
      EXPECT_EQ(irs::read<uint32_t>(in), 0);
    }
    {
      EXPECT_TRUE(it->next());
      EXPECT_EQ(it->value(), 2);
      irs::BytesViewInput in(payload->value);
      EXPECT_EQ(irs::read<uint32_t>(in), 1);
    }
    {
      EXPECT_TRUE(it->next());
      EXPECT_EQ(it->value(), 3);
      irs::BytesViewInput in(payload->value);
      EXPECT_EQ(irs::read<uint32_t>(in), 2);
    }
    EXPECT_FALSE(it->next());
    EXPECT_FALSE(it->next());
  }
}

class IndexTestCase14 : public IndexTestCase {
 public:
  struct Feature1 {};
  struct Feature2 {};
  struct Feature3 {};

 protected:
  struct TestField {
    std::string_view Name() const { return "test"; };
    irs::IndexFeatures GetIndexFeatures() const {
      return irs::IndexFeatures::None | irs::IndexFeatures::Freq;
    }
    irs::Tokenizer& GetTokens() const noexcept {
      stream.reset(value);
      return stream;
    }

    std::string value;
    mutable irs::StringTokenizer stream;
  };

  struct Stats {
    size_t num_factory_calls{};
    size_t num_write_calls{};
    size_t num_write_consolidation_calls{};
    size_t num_finish_calls{};
  };

  class FeatureWriter : public irs::FeatureWriter {
   public:
    static auto Make(Stats& call_stats, irs::doc_id_t filter_doc,
                     std::span<const irs::bytes_view> headers)
      -> irs::FeatureWriter::ptr {
      ++call_stats.num_factory_calls;

      irs::doc_id_t min_doc{irs::doc_limits::eof()};
      for (auto header : headers) {
        auto* p = header.data();
        min_doc = std::min(irs::read<irs::doc_id_t>(p), min_doc);
      }

      return irs::memory::make_managed<FeatureWriter>(call_stats, filter_doc,
                                                      min_doc);
    }

    FeatureWriter(Stats& call_stats, irs::doc_id_t filter_doc,
                  irs::doc_id_t min_doc) noexcept
      : _call_stats{&call_stats}, _filter_doc{filter_doc}, _min_doc{min_doc} {}

    void write(const irs::FieldStats& stats, irs::doc_id_t doc,
               irs::ColumnOutput& writer) final {
      ++_call_stats->num_write_calls;

      if (doc == _filter_doc) {
        return;
      }

      auto& stream = writer(doc);
      stream.WriteU32(doc);
      stream.WriteU32(stats.len);
      stream.WriteU32(stats.num_overlap);
      stream.WriteU32(stats.max_term_freq);
      stream.WriteU32(stats.num_unique);

      _min_doc = std::min(doc, _min_doc);
    }

    void write(irs::DataOutput& out, irs::bytes_view payload) final {
      ++_call_stats->num_write_consolidation_calls;

      if (!payload.empty()) {
        auto* p = payload.data();
        _min_doc = std::min(irs::read<irs::doc_id_t>(p), _min_doc);

        out.WriteBytes(payload.data(), payload.size());
      }
    }

    void finish(irs::DataOutput& out) final {
      ++_call_stats->num_finish_calls;

      out.WriteU32(static_cast<uint32_t>(sizeof(_min_doc)));
      out.WriteU32(_min_doc);
    }

   private:
    Stats* _call_stats;
    irs::doc_id_t _filter_doc;
    irs::doc_id_t _min_doc;
  };
};

REGISTER_ATTRIBUTE(IndexTestCase14::Feature1);
REGISTER_ATTRIBUTE(IndexTestCase14::Feature2);
REGISTER_ATTRIBUTE(IndexTestCase14::Feature3);

TEST_P(IndexTestCase14, write_field_with_multiple_stored_features) {
  TestField field;

  {
    irs::IndexWriterOptions opts;
    opts.features = [](irs::IndexFeatures id) {
      irs::FeatureWriterFactory handler{};

      return std::make_pair(
        irs::ColumnInfo{irs::Type<irs::compression::None>::get(),
                        {},

                        false},
        std::move(handler));
    };

    auto writer = open_writer(irs::kOmCreate, opts);

    // doc1
    {
      auto docs = writer->GetBatch();
      auto doc = docs.Insert();
      field.value = "foo";
      ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(field));
    }

    // doc2
    {
      auto docs = writer->GetBatch();
      auto doc = docs.Insert();

      field.value = "foo";
      ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(field));
      ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(field));
    }

    // doc3
    {
      auto docs = writer->GetBatch();
      auto doc = docs.Insert();

      field.value = "foo";
      ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(field));
      ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(field));
      ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(field));
      ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(field));

      field.value = "bar";
      ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(field));
      ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(field));
    }

    ASSERT_TRUE(writer->Commit());
    AssertSnapshotEquality(*writer);
  }

  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = (*reader)[0];
    ASSERT_EQ(3, segment.docs_count());
    ASSERT_EQ(3, segment.live_docs_count());

    auto fields = segment.fields();
    ASSERT_NE(nullptr, fields);
    ASSERT_TRUE(fields->next());
    auto& field_reader = fields->value();
    ASSERT_EQ(field.Name(), field_reader.meta().name);

    ASSERT_FALSE(fields->next());
    ASSERT_FALSE(fields->next());
  }
}

TEST_P(IndexTestCase14, consolidate_multiple_stored_features) {
  TestField field;

  irs::IndexWriterOptions opts;
  opts.features = [](irs::IndexFeatures id) {
    irs::FeatureWriterFactory handler{};

    return std::make_pair(
      irs::ColumnInfo{irs::Type<irs::compression::None>::get(),
                      {},

                      false},
      std::move(handler));
  };

  auto writer = open_writer(irs::kOmCreate, opts);

  // doc1
  {
    auto docs = writer->GetBatch();
    auto doc = docs.Insert();
    field.value = "foo";
    ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(field));
  }

  ASSERT_TRUE(writer->Commit());
  AssertSnapshotEquality(*writer);

  // doc2
  {
    auto docs = writer->GetBatch();
    auto doc = docs.Insert();

    field.value = "foo";
    ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(field));
    ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(field));
  }

  ASSERT_TRUE(writer->Commit());
  AssertSnapshotEquality(*writer);

  // doc3
  {
    auto docs = writer->GetBatch();
    auto doc = docs.Insert();

    field.value = "foo";
    ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(field));
    ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(field));
    ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(field));
    ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(field));

    field.value = "bar";
    ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(field));
    ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(field));
  }

  ASSERT_TRUE(writer->Commit());
  AssertSnapshotEquality(*writer);

  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(3, reader.size());

    {
      auto& segment = (*reader)[0];
      ASSERT_EQ(1, segment.docs_count());
      ASSERT_EQ(1, segment.live_docs_count());

      auto fields = segment.fields();
      ASSERT_NE(nullptr, fields);
      ASSERT_TRUE(fields->next());
      auto& field_reader = fields->value();
      ASSERT_EQ(field.Name(), field_reader.meta().name);

      ASSERT_FALSE(fields->next());
      ASSERT_FALSE(fields->next());
    }

    {
      auto& segment = (*reader)[1];
      ASSERT_EQ(1, segment.docs_count());
      ASSERT_EQ(1, segment.live_docs_count());

      auto fields = segment.fields();
      ASSERT_NE(nullptr, fields);
      ASSERT_TRUE(fields->next());
      auto& field_reader = fields->value();
      ASSERT_EQ(field.Name(), field_reader.meta().name);

      ASSERT_FALSE(fields->next());
      ASSERT_FALSE(fields->next());
    }

    {
      auto& segment = (*reader)[2];
      ASSERT_EQ(1, segment.docs_count());
      ASSERT_EQ(1, segment.live_docs_count());

      auto fields = segment.fields();
      ASSERT_NE(nullptr, fields);
      ASSERT_TRUE(fields->next());
      auto& field_reader = fields->value();
      ASSERT_EQ(field.Name(), field_reader.meta().name);

      ASSERT_FALSE(fields->next());
      ASSERT_FALSE(fields->next());
    }
  }

  const auto res = writer->Consolidate(
    irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount{}));
  ASSERT_TRUE(res);
  ASSERT_EQ(3, res.size);
  ASSERT_TRUE(writer->Commit());
  AssertSnapshotEquality(*writer);

  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size());
    auto& segment = (*reader)[0];
    ASSERT_EQ(3, segment.docs_count());
    ASSERT_EQ(3, segment.live_docs_count());

    auto fields = segment.fields();
    ASSERT_NE(nullptr, fields);
    ASSERT_TRUE(fields->next());
    auto& field_reader = fields->value();
    ASSERT_EQ(field.Name(), field_reader.meta().name);

    ASSERT_FALSE(fields->next());
    ASSERT_FALSE(fields->next());
  }
}

class IndexTestCase11 : public tests::IndexTestBase {};

TEST_P(IndexTestCase11, consolidate_old_format) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });
  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();

  auto validate_codec = [&](auto codec, size_t size) {
    auto reader = irs::DirectoryReader(dir());
    ASSERT_NE(nullptr, reader);
    ASSERT_EQ(size, reader.size());
    ASSERT_EQ(size, reader.Meta().index_meta.segments.size());
    for (auto& meta : reader.Meta().index_meta.segments) {
      ASSERT_EQ(codec, meta.meta.codec);
    }
  };

  irs::IndexWriterOptions writer_options;
  auto writer = open_writer(irs::kOmCreate, writer_options);
  // 1st segment
  ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                     doc1->stored.begin(), doc1->stored.end()));
  writer->Commit();
  AssertSnapshotEquality(*writer);
  validate_codec(codec(), 1);
  // 2nd segment
  ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                     doc2->stored.begin(), doc2->stored.end()));
  writer->Commit();
  AssertSnapshotEquality(*writer);
  validate_codec(codec(), 2);
  // consolidate
  auto old_codec = irs::formats::Get("1_5avx");
  irs::index_utils::ConsolidateCount consolidate_all;
  ASSERT_TRUE(writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all),
                                  old_codec));
  writer->Commit();
  AssertSnapshotEquality(*writer);
  validate_codec(old_codec, 1);
}

TEST_P(IndexTestCase11, clean_writer_with_payload) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<tests::StringField>(name, data.str));
      }
    });

  const tests::Document* doc1 = gen.next();

  irs::IndexWriterOptions writer_options;
  uint64_t payload_committed_tick{0};
  irs::bstring input_payload = static_cast<irs::bstring>(
    irs::ViewCast<irs::byte_type>(std::string_view("init")));
  bool payload_provider_result{false};
  writer_options.meta_payload_provider =
    [&payload_provider_result, &payload_committed_tick, &input_payload](
      uint64_t tick, irs::bstring& out) {
      payload_committed_tick = tick;
      out.append(input_payload.data(), input_payload.size());
      return payload_provider_result;
    };
  auto writer = open_writer(irs::kOmCreate, writer_options);

  ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                     doc1->stored.begin(), doc1->stored.end()));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  size_t file_count0 = 0;
  dir().visit([&file_count0](std::string_view) -> bool {
    ++file_count0;
    return true;
  });

  {
    auto reader = irs::DirectoryReader(dir());
    ASSERT_TRUE(irs::IsNull(irs::GetPayload(reader.Meta().index_meta)));
  }
  uint64_t expected_tick = 42;

  payload_committed_tick = 0;
  payload_provider_result = true;
  writer->Clear(expected_tick);
  {
    auto reader = irs::DirectoryReader(dir());
    ASSERT_EQ(input_payload, irs::GetPayload(reader.Meta().index_meta));
    ASSERT_EQ(payload_committed_tick, expected_tick);
  }
}

TEST_P(IndexTestCase11, initial_two_phase_commit_no_payload) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  auto& directory = dir();

  irs::IndexWriterOptions writer_options;
  uint64_t payload_calls_count{0};
  writer_options.meta_payload_provider = [&payload_calls_count](uint64_t,
                                                                irs::bstring&) {
    payload_calls_count++;
    return false;
  };
  auto writer = open_writer(irs::kOmCreate, writer_options);

  ASSERT_TRUE(writer->Begin());

  // transaction is already started
  payload_calls_count = 0;
  writer->Commit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(0, payload_calls_count);

  auto reader = irs::DirectoryReader(directory);
  ASSERT_TRUE(irs::IsNull(irs::GetPayload(reader.Meta().index_meta)));

  // no changes
  writer->Commit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(0, payload_calls_count);
  ASSERT_EQ(reader, reader.Reopen());
}

TEST_P(IndexTestCase11, initial_commit_no_payload) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  auto& directory = dir();

  irs::IndexWriterOptions writer_options;
  uint64_t payload_calls_count{0};
  writer_options.meta_payload_provider = [&payload_calls_count](uint64_t,
                                                                irs::bstring&) {
    payload_calls_count++;
    return false;
  };
  auto writer = open_writer(irs::kOmCreate, writer_options);

  writer->Commit();
  AssertSnapshotEquality(*writer);

  auto reader = irs::DirectoryReader(directory);
  ASSERT_TRUE(irs::IsNull(irs::GetPayload(reader.Meta().index_meta)));

  // no changes
  payload_calls_count = 0;
  writer->Commit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(0, payload_calls_count);
  ASSERT_EQ(reader, reader.Reopen());
}

TEST_P(IndexTestCase11, initial_two_phase_commit_payload_revert) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  auto& directory = dir();

  irs::IndexWriterOptions writer_options;
  uint64_t payload_committed_tick{0};
  irs::bstring input_payload;
  uint64_t payload_calls_count{0};
  bool payload_provider_result{false};
  writer_options.meta_payload_provider =
    [&payload_provider_result, &payload_calls_count, &payload_committed_tick,
     &input_payload](uint64_t tick, irs::bstring& out) {
      payload_calls_count++;
      payload_committed_tick = tick;
      out.append(input_payload.data(), input_payload.size());
      return payload_provider_result;
    };
  auto writer = open_writer(irs::kOmCreate, writer_options);

  input_payload = irs::ViewCast<irs::byte_type>(std::string_view("init"));
  payload_committed_tick = 42;
  ASSERT_TRUE(writer->Begin());
  ASSERT_EQ(0, payload_committed_tick);

  payload_provider_result = true;
  // transaction is already started
  payload_calls_count = 0;
  writer->Commit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(0, payload_calls_count);

  auto reader = irs::DirectoryReader(directory);
  ASSERT_TRUE(irs::IsNull(irs::GetPayload(reader.Meta().index_meta)));

  // no changes
  writer->Commit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(0, payload_calls_count);
  ASSERT_EQ(reader, reader.Reopen());
}

TEST_P(IndexTestCase11, initial_commit_payload_revert) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  auto& directory = dir();

  irs::IndexWriterOptions writer_options;
  uint64_t payload_committed_tick{0};
  irs::bstring input_payload;
  uint64_t payload_calls_count{0};
  bool payload_provider_result{false};
  writer_options.meta_payload_provider =
    [&payload_provider_result, &payload_calls_count, &payload_committed_tick,
     &input_payload](uint64_t tick, irs::bstring& out) {
      payload_calls_count++;
      payload_committed_tick = tick;
      out.append(input_payload.data(), input_payload.size());
      return payload_provider_result;
    };
  auto writer = open_writer(irs::kOmCreate, writer_options);

  input_payload = irs::ViewCast<irs::byte_type>(std::string_view("init"));
  payload_committed_tick = 42;
  writer->Commit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(0, payload_committed_tick);

  auto reader = irs::DirectoryReader(directory);
  ASSERT_TRUE(irs::IsNull(irs::GetPayload(reader.Meta().index_meta)));

  payload_provider_result = true;
  // no changes
  payload_calls_count = 0;
  writer->Commit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(0, payload_calls_count);
  ASSERT_EQ(reader, reader.Reopen());
}

TEST_P(IndexTestCase11, initial_two_phase_commit_payload) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  auto& directory = dir();

  irs::IndexWriterOptions writer_options;
  uint64_t payload_committed_tick{0};
  irs::bstring input_payload;
  uint64_t payload_calls_count{0};
  writer_options.meta_payload_provider =
    [&payload_calls_count, &payload_committed_tick, &input_payload](
      uint64_t tick, irs::bstring& out) {
      payload_calls_count++;
      payload_committed_tick = tick;
      out.append(input_payload.data(), input_payload.size());
      return true;
    };
  auto writer = open_writer(irs::kOmCreate, writer_options);

  input_payload = irs::ViewCast<irs::byte_type>(std::string_view("init"));
  payload_committed_tick = 42;
  ASSERT_TRUE(writer->Begin());
  ASSERT_EQ(0, payload_committed_tick);

  // transaction is already started
  payload_calls_count = 0;
  writer->Commit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(0, payload_calls_count);

  auto reader = irs::DirectoryReader(directory);
  ASSERT_EQ(input_payload, irs::GetPayload(reader.Meta().index_meta));

  // no changes
  writer->Commit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(0, payload_calls_count);
  ASSERT_EQ(reader, reader.Reopen());
}

TEST_P(IndexTestCase11, initial_commit_payload) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  auto& directory = dir();

  irs::IndexWriterOptions writer_options;
  uint64_t payload_committed_tick{0};
  irs::bstring input_payload;
  uint64_t payload_calls_count{0};
  writer_options.meta_payload_provider =
    [&payload_calls_count, &payload_committed_tick, &input_payload](
      uint64_t tick, irs::bstring& out) {
      payload_calls_count++;
      payload_committed_tick = tick;
      out.append(input_payload.data(), input_payload.size());
      return true;
    };
  auto writer = open_writer(irs::kOmCreate, writer_options);

  input_payload = irs::ViewCast<irs::byte_type>(std::string_view("init"));
  payload_committed_tick = 42;
  writer->Commit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(0, payload_committed_tick);

  auto reader = irs::DirectoryReader(directory);
  ASSERT_EQ(input_payload, irs::GetPayload(reader.Meta().index_meta));

  // no changes
  payload_calls_count = 0;
  writer->Commit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(0, payload_calls_count);
  ASSERT_EQ(reader, reader.Reopen());
}

TEST_P(IndexTestCase11, commit_payload) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  auto& directory = dir();
  auto* doc0 = gen.next();

  irs::IndexWriterOptions writer_options;
  uint64_t payload_committed_tick{0};
  irs::bstring input_payload;
  uint64_t payload_calls_count{0};
  bool payload_provider_result = true;
  writer_options.meta_payload_provider =
    [&payload_calls_count, &payload_committed_tick, &input_payload,
     &payload_provider_result](uint64_t tick, irs::bstring& out) {
      payload_calls_count++;
      payload_committed_tick = tick;
      out.append(input_payload.data(), input_payload.size());
      return payload_provider_result;
    };
  auto writer = open_writer(irs::kOmCreate, writer_options);

  payload_provider_result = false;
  ASSERT_TRUE(writer->Begin());  // initial commit
  writer->Commit();
  AssertSnapshotEquality(*writer);
  auto reader = irs::DirectoryReader(directory);
  ASSERT_TRUE(irs::IsNull(irs::GetPayload(reader.Meta().index_meta)));

  ASSERT_FALSE(writer->Begin());  // transaction hasn't been started, no changes
  writer->Commit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(reader, reader.Reopen());
  payload_provider_result = true;
  // commit with a specified payload
  {
    const uint64_t expected_tick = 42;

    // insert document (trx 1)
    {
      auto trx = writer->GetBatch();
      auto doc = trx.Insert();
      doc.Insert<irs::Action::INDEX>(doc0->indexed.begin(),
                                     doc0->indexed.end());
      doc.Insert<irs::Action::INDEX>(doc0->stored.begin(), doc0->stored.end());
      ASSERT_TRUE(doc);
      trx.Commit(expected_tick - 10);
      AssertSnapshotEquality(*writer);
    }

    // insert document (trx 0)
    {
      auto trx = writer->GetBatch();
      auto doc = trx.Insert();
      doc.Insert<irs::Action::INDEX>(doc0->indexed.begin(),
                                     doc0->indexed.end());
      doc.Insert<irs::Action::INDEX>(doc0->stored.begin(), doc0->stored.end());
      ASSERT_TRUE(doc);
      trx.Commit(expected_tick);
      AssertSnapshotEquality(*writer);
    }

    payload_committed_tick = 0;

    input_payload =
      irs::ViewCast<irs::byte_type>(std::string_view(reader.Meta().filename));
    ASSERT_TRUE(writer->Begin());
    ASSERT_EQ(expected_tick, payload_committed_tick);

    // transaction is already started
    ASSERT_NE(0, payload_calls_count);
    payload_calls_count = 0;
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, payload_calls_count);

    // check written payload
    {
      auto new_reader = reader.Reopen();
      ASSERT_NE(reader, new_reader);
      ASSERT_EQ(input_payload, irs::GetPayload(new_reader.Meta().index_meta));
      reader = new_reader;
    }
  }

  // commit with rollback
  {
    const uint64_t expected_tick = 42;

    // insert document (trx 1)
    {
      auto trx = writer->GetBatch();
      auto doc = trx.Insert();
      doc.Insert<irs::Action::INDEX>(doc0->indexed.begin(),
                                     doc0->indexed.end());
      doc.Insert<irs::Action::INDEX>(doc0->stored.begin(), doc0->stored.end());
      ASSERT_TRUE(doc);
      trx.Commit(expected_tick - 10);
    }

    // insert document (trx 0)
    {
      auto trx = writer->GetBatch();
      auto doc = trx.Insert();
      doc.Insert<irs::Action::INDEX>(doc0->indexed.begin(),
                                     doc0->indexed.end());
      doc.Insert<irs::Action::INDEX>(doc0->stored.begin(), doc0->stored.end());
      ASSERT_TRUE(doc);
      trx.Commit(expected_tick);
    }

    payload_committed_tick = 0;

    input_payload =
      irs::ViewCast<irs::byte_type>(std::string_view(reader.Meta().filename));
    ASSERT_TRUE(writer->Begin());
    ASSERT_EQ(expected_tick, payload_committed_tick);

    writer->Rollback();

    // check payload
    {
      auto new_reader = reader.Reopen();
      ASSERT_EQ(reader, new_reader);
    }
  }

  // commit with a reverted payload
  {
    const uint64_t expected_tick = 1;

    // insert document (trx 0)
    {
      auto trx = writer->GetBatch();
      auto doc = trx.Insert();
      doc.Insert<irs::Action::INDEX>(doc0->indexed.begin(),
                                     doc0->indexed.end());
      doc.Insert<irs::Action::INDEX>(doc0->stored.begin(), doc0->stored.end());
      ASSERT_TRUE(doc);
      trx.Commit(expected_tick);
    }

    // insert document (trx 1)
    {
      auto trx = writer->GetBatch();
      auto doc = trx.Insert();
      doc.Insert<irs::Action::INDEX>(doc0->indexed.begin(),
                                     doc0->indexed.end());
      doc.Insert<irs::Action::INDEX>(doc0->stored.begin(), doc0->stored.end());
      ASSERT_TRUE(doc);
      trx.Commit(expected_tick);
    }

    payload_committed_tick = 1;

    input_payload =
      irs::ViewCast<irs::byte_type>(std::string_view(reader.Meta().filename));
    payload_provider_result = false;
    ASSERT_TRUE(writer->Begin());
    ASSERT_EQ(expected_tick, payload_committed_tick);

    // transaction is already started
    payload_calls_count = 0;
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, payload_calls_count);

    // check written payload
    {
      auto new_reader = reader.Reopen();
      ASSERT_NE(reader, new_reader);
      ASSERT_TRUE(irs::IsNull(irs::GetPayload(new_reader.Meta().index_meta)));
      reader = new_reader;
    }
  }

  // commit with empty payload
  {
    const uint64_t expected_tick = 1;

    // insert document (trx 0)
    {
      auto trx = writer->GetBatch();
      auto doc = trx.Insert();
      doc.Insert<irs::Action::INDEX>(doc0->indexed.begin(),
                                     doc0->indexed.end());
      doc.Insert<irs::Action::INDEX>(doc0->stored.begin(), doc0->stored.end());
      ASSERT_TRUE(doc);
      trx.Commit(expected_tick);
    }

    // insert document (trx 1)
    {
      auto trx = writer->GetBatch();
      auto doc = trx.Insert();
      doc.Insert<irs::Action::INDEX>(doc0->indexed.begin(),
                                     doc0->indexed.end());
      doc.Insert<irs::Action::INDEX>(doc0->stored.begin(), doc0->stored.end());
      ASSERT_TRUE(doc);
      trx.Commit(expected_tick);
    }

    payload_committed_tick = 42;
    input_payload.clear();
    payload_provider_result = true;

    ASSERT_TRUE(writer->Begin());
    ASSERT_EQ(expected_tick, payload_committed_tick);

    // transaction is already started
    payload_calls_count = 0;
    input_payload.clear();
    writer->Commit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, payload_calls_count);

    // check written payload
    {
      auto new_reader = reader.Reopen();
      ASSERT_NE(reader, new_reader);
      ASSERT_FALSE(irs::IsNull(irs::GetPayload(new_reader.Meta().index_meta)));
      ASSERT_TRUE(irs::GetPayload(new_reader.Meta().index_meta).empty());
      ASSERT_EQ(irs::kEmptyStringView<irs::byte_type>,
                irs::GetPayload(new_reader.Meta().index_meta));
      reader = new_reader;
    }
  }

  // commit without payload
  {
    payload_provider_result = false;
    // insert document (trx 0)
    {
      auto trx = writer->GetBatch();
      auto doc = trx.Insert();
      doc.Insert<irs::Action::INDEX>(doc0->indexed.begin(),
                                     doc0->indexed.end());
      doc.Insert<irs::Action::INDEX>(doc0->stored.begin(), doc0->stored.end());
      ASSERT_TRUE(doc);
    }

    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // check written payload
  {
    auto new_reader = reader.Reopen();
    ASSERT_NE(reader, new_reader);
    ASSERT_TRUE(irs::IsNull(irs::GetPayload(new_reader.Meta().index_meta)));
    reader = new_reader;
  }

  ASSERT_FALSE(writer->Begin());  // transaction hasn't been started, no changes
  writer->Commit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(reader, reader.Reopen());
}

TEST_P(IndexTestCase11, testExternalGeneration) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  auto& directory = dir();
  auto* doc0 = gen.next();
  auto* doc1 = gen.next();

  irs::IndexWriterOptions writer_options;
  auto writer = open_writer(irs::kOmCreate, writer_options);
  {
    auto trx = writer->GetBatch();
    {
      auto doc = trx.Insert();
      doc.Insert<irs::Action::INDEX>(doc0->indexed.begin(),
                                     doc0->indexed.end());
      doc.Insert<irs::Action::INDEX>(doc0->stored.begin(), doc0->stored.end());
      ASSERT_TRUE(doc);
    }
    {
      auto doc = trx.Insert();
      doc.Insert<irs::Action::INDEX>(doc1->indexed.begin(),
                                     doc1->indexed.end());
      doc.Insert<irs::Action::INDEX>(doc1->stored.begin(), doc1->stored.end());
      ASSERT_TRUE(doc);
    }
    // subcontext with remove
    {
      auto trx2 = writer->GetBatch();
      trx2.Remove(MakeByTerm("name", "A"));
      trx2.Commit(4);
    }
    trx.Commit(3);
  }
  ASSERT_TRUE(writer->Begin());
  writer->Commit();
  AssertSnapshotEquality(*writer);
  auto reader = irs::DirectoryReader(directory);
  ASSERT_EQ(1, reader.size());
  auto& segment = (*reader)[0];
  ASSERT_EQ(2, segment.docs_count());
  ASSERT_EQ(1, segment.live_docs_count());
}

TEST_P(IndexTestCase11, testExternalGenerationDifferentStart) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  auto& directory = dir();
  auto* doc0 = gen.next();
  auto* doc1 = gen.next();

  irs::IndexWriterOptions writer_options;
  auto writer = open_writer(irs::kOmCreate, writer_options);
  {
    auto reader = writer->GetSnapshot();
    EXPECT_EQ(reader->CountMappedMemory(), 0);
    EXPECT_EQ(GetResourceManager().file_descriptors.Counter(), 0);
  }

  {
    irs::IndexWriter::Transaction trx;
    ASSERT_FALSE(trx.Valid());
    trx = writer->GetBatch();
    ASSERT_TRUE(trx.Valid());
    {
      auto doc = trx.Insert();
      doc.Insert<irs::Action::INDEX>(doc0->indexed.begin(),
                                     doc0->indexed.end());
      doc.Insert<irs::Action::INDEX | irs::Action::STORE>(doc0->stored.begin(),
                                                          doc0->stored.end());
      ASSERT_TRUE(doc);
    }
    {
      auto doc = trx.Insert();
      doc.Insert<irs::Action::INDEX>(doc1->indexed.begin(),
                                     doc1->indexed.end());
      doc.Insert<irs::Action::INDEX | irs::Action::STORE>(doc1->stored.begin(),
                                                          doc1->stored.end());
      ASSERT_TRUE(doc);
    }
    // subcontext with remove
    {
      auto trx2 = writer->GetBatch();
      trx2.Remove(MakeByTerm("name", "A"));
      trx2.Commit(4);
    }
    trx.Commit(3);
  }
  ASSERT_TRUE(writer->Begin());
  writer->Commit();
  AssertSnapshotEquality(*writer);
  writer.reset();
  auto reader = irs::DirectoryReader(directory);
  if (dynamic_cast<irs::MemoryDirectory*>(&directory) == nullptr) {
    EXPECT_EQ(GetResourceManager().file_descriptors.Counter(), 4);
  }
  auto mapped_memory = reader.CountMappedMemory();
#ifdef __linux__
  if (dynamic_cast<irs::MMapDirectory*>(&directory) != nullptr) {
    EXPECT_GT(mapped_memory, 0);
    mapped_memory = 0;
  }
#endif
  EXPECT_EQ(mapped_memory, 0);

  ASSERT_EQ(1, reader.size());
  auto& segment = (*reader)[0];
  ASSERT_EQ(2, segment.docs_count());
  ASSERT_EQ(1, segment.live_docs_count());
}

TEST_P(IndexTestCase11, testExternalGenerationRemoveBeforeInsert) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  auto& directory = dir();
  auto* doc0 = gen.next();
  auto* doc1 = gen.next();

  irs::IndexWriterOptions writer_options;
  auto writer = open_writer(irs::kOmCreate, writer_options);
  {
    auto trx = writer->GetBatch();
    {
      auto doc = trx.Insert();
      doc.Insert<irs::Action::INDEX>(doc0->indexed.begin(),
                                     doc0->indexed.end());
      doc.Insert<irs::Action::INDEX>(doc0->stored.begin(), doc0->stored.end());
      ASSERT_TRUE(doc);
    }
    {
      auto doc = trx.Insert();
      doc.Insert<irs::Action::INDEX>(doc1->indexed.begin(),
                                     doc1->indexed.end());
      doc.Insert<irs::Action::INDEX>(doc1->stored.begin(), doc1->stored.end());
      ASSERT_TRUE(doc);
    }
    // subcontext with remove
    {
      auto trx2 = writer->GetBatch();
      trx2.Remove(MakeByTerm("name", "A"));
      trx2.Commit(2);
    }
    trx.Commit(4);
  }
  ASSERT_TRUE(writer->Begin());
  writer->Commit();
  AssertSnapshotEquality(*writer);
  auto reader = irs::DirectoryReader(directory);
  ASSERT_EQ(1, reader.size());
  auto& segment = (*reader)[0];
  ASSERT_EQ(2, segment.docs_count());
  ASSERT_EQ(2, segment.live_docs_count());
}

TEST_P(IndexTestCase14, buffered_column_reopen) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);
  auto doc0 = gen.next();
  auto doc1 = gen.next();
  auto doc2 = gen.next();
  auto doc3 = gen.next();
  auto doc4 = gen.next();

  bool cache = false;
  TestResourceManager memory;
  const_cast<irs::ResourceManagementOptions&>(dir().ResourceManager()) =
    memory.options;
  irs::IndexWriterOptions opts;
  opts.reader_options.warmup_columns =
    [&](const irs::SegmentMeta& /*meta*/, const irs::FieldReader& /*fields*/,
        const irs::ColumnReader& /*column*/) { return cache; };
  auto writer = open_writer(irs::kOmCreate, opts);

  ASSERT_TRUE(Insert(*writer, doc0->indexed.begin(), doc0->indexed.end(),
                     doc0->stored.begin(), doc0->stored.end()));
  const auto tr1 = memory.transactions.Counter();
  ASSERT_GT(tr1, 0);
  ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                     doc1->stored.begin(), doc1->stored.end()));
  const auto tr2 = memory.transactions.Counter();
  ASSERT_GT(tr2, tr1);
  ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                     doc2->stored.begin(), doc2->stored.end()));
  const auto tr3 = memory.transactions.Counter();
  ASSERT_GT(tr3, tr2);
  writer->Commit();
  AssertSnapshotEquality(*writer);
  EXPECT_EQ(0, memory.cached_columns.Counter());

  // empty commit: enable cache
  cache = true;
  writer->Commit({.reopen_columnstore = true});
  AssertSnapshotEquality(*writer);
  EXPECT_LT(0, memory.cached_columns.Counter());

  // empty commit: disable cache
  cache = false;
  writer->Commit({.reopen_columnstore = true});
  AssertSnapshotEquality(*writer);
  EXPECT_EQ(0, memory.cached_columns.Counter());

  // not empty commit: enable cache
  cache = true;
  ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                     doc3->stored.begin(), doc3->stored.end()));
  writer->Commit({.reopen_columnstore = true});
  AssertSnapshotEquality(*writer);
  EXPECT_LT(0, memory.cached_columns.Counter());

  // not empty commit: disable cache
  cache = false;
  ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                     doc4->stored.begin(), doc4->stored.end()));
  writer->Commit({.reopen_columnstore = true});
  AssertSnapshotEquality(*writer);
  EXPECT_EQ(0, memory.cached_columns.Counter());
  writer.reset();
  ASSERT_EQ(0, memory.transactions.Counter());
}

TEST_P(IndexTestCase14, hnsw_search_basic) {
  constexpr size_t kDim = 128;
  constexpr size_t kValuesPerSegment = 256;
  constexpr size_t kSegments = 4;
  constexpr size_t kTopK = 64;
  constexpr size_t kQueries = 256;
  constexpr std::string_view kColumnName = "vec"sv;

  std::vector<std::pair<uint64_t, std::vector<float>>> all_vectors;
  all_vectors.reserve(kSegments * kValuesPerSegment);

  irs::IndexWriterOptions writer_options;
  writer_options.column_info = [&kColumnName](std::string_view name) {
    if (name == kColumnName) {
      return irs::ColumnInfo{
        .compression = irs::Type<irs::compression::None>::get(),
        .options = {},
        .track_prev_doc = false,
        .value_type = irs::ValueType::VectorF32,
        .hnsw_info =
          irs::HNSWInfo{
            .d = kDim,
          },
      };
    }
    return irs::ColumnInfo{
      irs::Type<irs::compression::None>::get(),
      {},
      false,
    };
  };
  absl::BitGen bitgen;
  auto gen_vector = [&bitgen](float* vec, size_t size) {
    for (size_t i = 0; i < size; ++i) {
      vec[i] = absl::Uniform(bitgen, -100.0f, 100.0f);
    }
  };

  auto writer = open_writer(irs::kOmCreate, writer_options);

  struct VectorField {
    std::string_view name;
    std::vector<float> data;
    std::string_view Name() const { return name; }
    bool Write(irs::DataOutput& out) const {
      SDB_ASSERT(data.size() == kDim);
      out.WriteBytes(reinterpret_cast<const irs::byte_type*>(data.data()),
                     data.size() * sizeof(float));
      return true;
    }
  };

  std::vector<irs::IndexWriter::Transaction> trxs;
  trxs.reserve(kSegments);

  for (size_t seg = 0; seg < kSegments; ++seg) {
    auto trx = writer->GetBatch();
    for (size_t i = 0; i < kValuesPerSegment; ++i) {
      auto doc = trx.Insert();
      std::vector<float> data(kDim);
      gen_vector(data.data(), kDim);
      all_vectors.emplace_back(
        irs::PackSegmentWithDoc(static_cast<uint32_t>(seg),
                                static_cast<irs::doc_id_t>(i + 1)),
        data);
      VectorField vf{kColumnName, std::move(data)};
      ASSERT_TRUE(doc.Insert<irs::Action::STORE>(vf));
    }
    trxs.push_back(std::move(trx));
  }
  for (auto& trx : trxs) {
    trx.Commit();
  }

  ASSERT_TRUE(writer->Begin());
  writer->Commit();
  AssertSnapshotEquality(*writer);

  auto check_recall = [&](irs::DirectoryReader& reader,
                          const auto& expected_vectors) {
    float recall = .0f;
    faiss::SearchParametersHNSW params;
    params.efSearch = 32;
    for (size_t i = 0; i < kQueries; ++i) {
      std::vector<float> query_data(kDim);
      gen_vector(query_data.data(), kDim);
      std::vector<std::pair<float, uint64_t>> expected;
      for (size_t idx = 0; idx < expected_vectors.size(); ++idx) {
        float distance = faiss::fvec_L2sqr(
          query_data.data(), expected_vectors[idx].second.data(), kDim);
        expected.emplace_back(distance, expected_vectors[idx].first);
      }
      std::sort(expected.begin(), expected.end());
      expected.resize(kTopK);

      std::vector<float> dis(kTopK, 0.0f);
      std::vector<uint64_t> docs(kTopK);
      irs::HNSWSearchInfo info{
        reinterpret_cast<const irs::byte_type*>(query_data.data()),
        kTopK,
        params,
      };
      reader.Search("vec", info, reinterpret_cast<float*>(dis.data()),
                    reinterpret_cast<int64_t*>(docs.data()));
      size_t correct = 0;
      for (size_t k = 0; k < kTopK; ++k) {
        correct +=
          std::find_if(expected.begin(), expected.end(), [&](const auto& p) {
            return p.second == docs[k];
          }) != expected.end();
      }
      float current_recall = static_cast<float>(correct) / kTopK;
      recall += current_recall;
    }
    recall /= kQueries;
    std::cout << "Recall: " << recall << std::endl;
    ASSERT_GT(recall, .9f);
  };

  irs::IndexReaderOptions reader_opts;
  auto reader = irs::DirectoryReader{dir(), codec(), reader_opts};
  ASSERT_EQ(kSegments, reader.size());
  ASSERT_EQ(reader.docs_count(), kValuesPerSegment * kSegments);
  check_recall(reader, all_vectors);

  ASSERT_TRUE(writer->Consolidate(
    irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));
  writer->Commit();
  reader = irs::DirectoryReader{dir(), codec(), reader_opts};
  ASSERT_EQ(1, reader.size());
  ASSERT_EQ(reader.docs_count(), kValuesPerSegment * kSegments);
  std::transform(
    all_vectors.begin(), all_vectors.end(), all_vectors.begin(), [](auto& p) {
      auto [seg, doc] = irs::UnpackSegmentWithDoc(p.first);
      return std::make_pair(
        irs::PackSegmentWithDoc(0, doc + kValuesPerSegment * seg), p.second);
    });
  check_recall(reader, all_vectors);
}

static const auto kTestFormats =
  ::testing::Values(tests::FormatInfo{"1_5avx"}, tests::FormatInfo{"1_5simd"});

static const auto kTestDirs =
  ::testing::ValuesIn(tests::GetDirectories<tests::kTypesDefaultRot13>());

static const auto kTestValues = ::testing::Combine(kTestDirs, kTestFormats);

INSTANTIATE_TEST_SUITE_P(index_test_11, IndexTestCase11, kTestValues,
                         IndexTestCase11::to_string);

INSTANTIATE_TEST_SUITE_P(index_test_14, IndexTestCase14, kTestValues,
                         IndexTestCase14::to_string);

INSTANTIATE_TEST_SUITE_P(index_test_15, IndexTestCase, kTestValues,
                         IndexTestCase::to_string);
