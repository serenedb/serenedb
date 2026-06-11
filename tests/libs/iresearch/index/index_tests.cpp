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

#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "basics/duckdb_engine.h"
#include "basics/file_utils_ext.hpp"
#include "formats/column/test_cs_helpers.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/store/fs_directory.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/store/mmap_directory.hpp"
#include "iresearch/utils/fstext/fst_table_matcher.hpp"
#include "iresearch/utils/index_utils.hpp"
#include "iresearch/utils/type_limits.hpp"
#include "iresearch/utils/vector.hpp"
#include "iresearch/utils/wildcard_utils.hpp"
#include "tests_shared.hpp"

using namespace std::literals;

namespace {

// Per-doc cs column id used by tests in this file. `kNameColumnId` keys the
// column that holds the document's "name" StringField -- it backs the
// `segment.Column(kNameColumnId)` read-back paths the tests use to validate
// per-doc bytes after insertion.
inline constexpr irs::field_id kNameColumnId = 1;
inline constexpr irs::field_id kNameUpper = 2;

inline void CaptureNameLikeFields(irs::IndexWriter::Document& d,
                                  const tests::Particle& indexed) {
  if (const auto* name =
        dynamic_cast<const tests::StringField*>(indexed.get("name"))) {
    if (auto* cs = d.GetColWriter(); cs != nullptr) {
      irs::tests::StoreFieldAt(*cs, kNameColumnId, d.DocId(), *name);
    }
  }
  if (const auto* name_upper =
        dynamic_cast<const tests::StringField*>(indexed.get("NAME"))) {
    if (auto* cs = d.GetColWriter(); cs != nullptr) {
      irs::tests::StoreFieldAt(*cs, kNameUpper, d.DocId(), *name_upper);
    }
  }
}

// Probe each field's `Write` to surface serializer failures, mirroring
// the legacy STORE pass that batched serialization next to indexing.
// The bytes are discarded; only the return value matters.
inline bool ProbeWritable(const tests::Particle& indexed) {
  irs::bstring scratch;
  irs::tests::BstringDataOutput out{scratch};
  for (auto it = indexed.begin(); it != indexed.end(); ++it) {
    scratch.clear();
    if (!it->Write(out)) {
      return false;
    }
  }
  return true;
}

// Insert one doc into `writer` and capture per-doc "name"/"NAME"
// StringField bytes into the cs columns keyed by `kNameColumnId`/`kNameUpper`.
inline bool InsertWithName(irs::IndexWriter& writer,
                           const tests::Document& src) {
  auto ctx = writer.GetBatch();
  {
    auto d = ctx.Insert();
    if (!d.Insert(src.indexed.begin(), src.indexed.end())) {
      return false;
    }
    if (!ProbeWritable(src.indexed)) {
      d.Writer().rollback();
      return false;
    }
    CaptureNameLikeFields(d, src.indexed);
  }
  ctx.Commit();
  return true;
}

// Update analogue of `InsertWithName`: replaces docs matching `filter`
// with `src.indexed`, and forwards the "name" StringField (if any) into
// the per-doc cs column keyed by `kNameColumnId`.
template<typename Filter>
bool UpdateWithName(irs::IndexWriter& writer, Filter&& filter,
                    const tests::Document& src) {
  auto ctx = writer.GetBatch();
  {
    auto d = ctx.Replace(std::forward<Filter>(filter));
    if (!d.Insert(src.indexed.begin(), src.indexed.end())) {
      return false;
    }
    if (!ProbeWritable(src.indexed)) {
      d.Writer().rollback();
      return false;
    }
    CaptureNameLikeFields(d, src.indexed);
  }
  ctx.Commit();
  return true;
}

// Batched variant: drains `gen` in `batch_size`-sized batches, mirrors
// `tests::InsertBatch`, and additionally captures per-doc "name"
// StringField bytes into the cs column keyed by `kNameColumnId`.
template<typename DocGenerator, typename ExpectedSegment>
bool InsertBatchWithName(irs::IndexWriter& writer, DocGenerator& gen,
                         ExpectedSegment& segment, size_t batch_size) {
  auto ctx = writer.GetBatch();

  const tests::Document* src = nullptr;
  do {
    auto d = ctx.Insert(false, batch_size);
    const auto current_last_doc_id = writer.BufferedDocs();

    size_t inserted_docs = 0;

    while (inserted_docs < batch_size && (src = gen.next()) != nullptr) {
      inserted_docs++;
      segment.insert(*src);
      if (!d.Insert(src->indexed.begin(), src->indexed.end())) {
        return false;
      }
      CaptureNameLikeFields(d, src->indexed);
      d.NextDocument();
    }
    while (inserted_docs < batch_size) {
      SDB_ASSERT(src == nullptr);
      d.Writer().remove(current_last_doc_id - batch_size + inserted_docs++);
    }
  } while (src != nullptr);
  ctx.Commit();
  return true;
}

// Stable per-name field ids, sourced from the canonical
// `tests::FieldIdFor` mapping in doc_generator.hpp. Tests still spell the
// name as a `kXxxFieldId` constant for readability; the value comes from a
// single, project-wide table so shared factories and per-test factories
// land on the same id (otherwise indexed fields collide on
// `field_limits::invalid()` and `segment.field(id)` returns nullptr).
inline constexpr irs::field_id kNameFieldId = tests::FieldIdFor("name");
inline constexpr irs::field_id kName1FieldId = tests::FieldIdFor("name1");
inline constexpr irs::field_id kSameFieldId = tests::FieldIdFor("same");
inline constexpr irs::field_id kDuplicatedFieldId =
  tests::FieldIdFor("duplicated");
inline constexpr irs::field_id kValueFieldId = tests::FieldIdFor("value");
inline constexpr irs::field_id kTestFieldId = tests::FieldIdFor("test");
inline constexpr irs::field_id kNameAnlPayFieldId =
  tests::FieldIdFor("name_anl_pay");

using tests::FieldIdFor;

irs::Filter::ptr MakeByTerm(irs::field_id field_id, std::string_view value) {
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field_id() = field_id;
  filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(value);
  return filter;
}

irs::Filter::ptr MakeByTermOrByTerm(irs::field_id field_id0,
                                    std::string_view value0,
                                    irs::field_id field_id1,
                                    std::string_view value1) {
  auto filter = std::make_unique<irs::Or>();
  filter->add<irs::ByTerm>() =
    std::move(static_cast<irs::ByTerm&>(*MakeByTerm(field_id0, value0)));
  filter->add<irs::ByTerm>() =
    std::move(static_cast<irs::ByTerm&>(*MakeByTerm(field_id1, value1)));
  return filter;
}

irs::Filter::ptr MakeOr(
  const std::vector<std::pair<irs::field_id, std::string_view>>& parts) {
  auto filter = std::make_unique<irs::Or>();
  for (const auto& [field_id, value] : parts) {
    filter->add<irs::ByTerm>() =
      std::move(static_cast<irs::ByTerm&>(*MakeByTerm(field_id, value)));
  }
  return filter;
}

class SubReaderMock final : public irs::SubReader {
 public:
  uint64_t CountMappedMemory() const final { return 0; }

  const irs::SegmentInfo& Meta() const final { return _meta; }

  const irs::DocumentMask* docs_mask() const final { return nullptr; }

  irs::DocIterator::ptr docs_iterator() const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

  irs::DocIterator::ptr mask(irs::DocIterator::ptr&& it) const final {
    EXPECT_FALSE(true);
    return std::move(it);
  }

  std::span<const irs::field_id> field_ids() const final {
    EXPECT_FALSE(true);
    return {};
  }

  const irs::TermReader* field(irs::field_id) const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

  irs::NormReader::ptr norms(irs::field_id) const final { return nullptr; }

 private:
  irs::SegmentInfo _meta;
};

}  // namespace
namespace tests {

irs::IndexWriterOptions CsDefaultWriterOptions() {
  return irs::tests::DefaultWriterOptions();
}

irs::IndexWriterOptions EnsureWriterDb(irs::IndexWriterOptions opts) {
  if (opts.db == nullptr) {
    opts.db = &::sdb::DuckDBEngine::Instance().instance();
    opts.reader_options.db = &::sdb::DuckDBEngine::Instance().instance();
  }
  // With a cs writer present, a Norm-featured field requires a
  // norm_column_options callback (FieldsData::emplace asserts it). Provide
  // the shared monotonic allocator when a fixture didn't set one. This only
  // affects fields that actually carry the Norm feature.
  if (!opts.norm_column_options) {
    opts.norm_column_options = irs::tests::MakeNormColumnOptionsProvider();
  }
  return opts;
}

irs::IndexReaderOptions CsDefaultReaderOptions() {
  return irs::tests::DefaultReaderOptions();
}

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
  tests::AssertSnapshotEquality(
    writer.GetSnapshot(), open_reader(irs::tests::DefaultReaderOptions()));
}

void IndexTestBase::write_segment(irs::IndexWriter& writer,
                                  tests::IndexSegment& segment,
                                  tests::DocGeneratorBase& gen,
                                  const StoreHook& store) {
  const Document* src;
  while ((src = gen.next())) {
    segment.insert(*src);

    auto ctx = writer.GetBatch();
    {
      auto doc = ctx.Insert();
      ASSERT_TRUE(doc.Insert(src->indexed.begin(), src->indexed.end()));
      if (store) {
        store(doc, *src);
      } else {
        CaptureNameLikeFields(doc, src->indexed);
      }
    }
    ctx.Commit();
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
                                tests::DocGeneratorBase& gen,
                                const StoreHook& store) {
  _index.emplace_back();
  write_segment(writer, _index.back(), gen, store);
  writer.RefreshCommit();
}

void IndexTestBase::add_segments(irs::IndexWriter& writer,
                                 std::vector<DocGeneratorBase::ptr>& gens) {
  for (auto& gen : gens) {
    _index.emplace_back();
    write_segment(writer, _index.back(), *gen);
  }
  writer.RefreshCommit();
}

void IndexTestBase::add_segment(tests::DocGeneratorBase& gen,
                                irs::OpenMode mode /*= irs::kOmCreate*/,
                                const irs::IndexWriterOptions& opts /*= {}*/,
                                const StoreHook& store /*= {}*/) {
  auto writer = open_writer(mode, opts);
  add_segment(*writer, gen, store);
}

void IndexTestBase::add_segment_batched(
  tests::DocGeneratorBase& gen, size_t batch_size,
  irs::OpenMode mode /*= irs::kOmCreate*/,
  const irs::IndexWriterOptions& opts /*= {}*/) {
  auto writer = open_writer(mode, opts);
  _index.emplace_back();
  write_segment_batched(*writer, _index.back(), gen, batch_size);
  writer->RefreshCommit();
}

}  // namespace tests

class IndexTestCase : public tests::IndexTestBase {
 public:
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
  }

  void ClearWriter() {
    tests::JsonDocGenerator gen(
      resource("simple_sequential.json"),
      [](tests::Document& doc, const std::string& name,
         const tests::JsonDocGenerator::JsonValue& data) {
        if (data.is_string()) {
          auto field = std::make_shared<tests::StringField>(name, data.str);
          field->id = FieldIdFor(name);
          doc.insert(std::move(field));
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
      auto writer =
        open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

      writer->RefreshCommit();
      AssertSnapshotEquality(*writer);  // create initial empty segment

      // populate 'import' dir
      {
        auto data_writer =
          irs::IndexWriter::Make(data_dir, codec(), irs::kOmCreate,
                                 irs::tests::DefaultWriterOptions());
        ASSERT_TRUE(InsertWithName(*data_writer, *doc1));
        ASSERT_TRUE(InsertWithName(*data_writer, *doc2));
        ASSERT_TRUE(InsertWithName(*data_writer, *doc3));
        data_writer->RefreshCommit();

        auto reader = irs::DirectoryReader(data_dir, nullptr,
                                           irs::tests::DefaultReaderOptions());
        ASSERT_EQ(1, reader.size());
        ASSERT_EQ(3, reader.docs_count());
        ASSERT_EQ(3, reader.live_docs_count());
      }

      {
        auto reader = irs::DirectoryReader(dir(), codec(),
                                           irs::tests::DefaultReaderOptions());
        ASSERT_EQ(0, reader.size());
        ASSERT_EQ(0, reader.docs_count());
        ASSERT_EQ(0, reader.live_docs_count());
      }

      // add sealed segment
      {
        ASSERT_TRUE(InsertWithName(*writer, *doc4));
        ASSERT_TRUE(InsertWithName(*writer, *doc5));
        writer->RefreshCommit();
        AssertSnapshotEquality(*writer);
      }

      {
        auto reader = irs::DirectoryReader(dir(), codec(),
                                           irs::tests::DefaultReaderOptions());
        ASSERT_EQ(1, reader.size());
        ASSERT_EQ(2, reader.docs_count());
        ASSERT_EQ(2, reader.live_docs_count());
      }

      // add insert/remove/import
      {
        auto query_doc4 = MakeByTerm(kNameFieldId, "D");
        auto reader = irs::DirectoryReader(data_dir, nullptr,
                                           irs::tests::DefaultReaderOptions());

        ASSERT_TRUE(InsertWithName(*writer, *doc6));
        tests::Remove(*writer, std::move(query_doc4));
        ASSERT_TRUE(writer->Import(irs::DirectoryReader(
          data_dir, nullptr, irs::tests::DefaultReaderOptions())));
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
        auto reader = irs::DirectoryReader(dir(), codec(),
                                           irs::tests::DefaultReaderOptions());
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

      writer->RefreshCommit();
      AssertSnapshotEquality(*writer);

      // should be empty after commit (no new files or uncomited changes)
      {
        auto reader = irs::DirectoryReader(dir(), codec(),
                                           irs::tests::DefaultReaderOptions());
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
      auto writer = irs::IndexWriter::Make(dir, codec(), irs::kOmCreate,
                                           irs::tests::DefaultWriterOptions());
      ASSERT_THROW(
        irs::DirectoryReader(dir, nullptr, irs::tests::DefaultReaderOptions()),
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

      auto reader =
        irs::DirectoryReader(dir, nullptr, irs::tests::DefaultReaderOptions());
      ASSERT_EQ(0, reader.size());
      ASSERT_EQ(0, reader.docs_count());
      ASSERT_EQ(0, reader.live_docs_count());
    }

    // ensue double clear does not increment meta
    {
      auto writer =
        open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

      ASSERT_TRUE(InsertWithName(*writer, *doc1));
      writer->RefreshCommit();
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
    auto actual_reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_FALSE(!actual_reader);
    ASSERT_EQ(1, actual_reader.size());
    ASSERT_EQ(expected_index.size(), actual_reader.size());

    size_t thread_count = 16;                         // arbitrary value > 1
    std::vector<const tests::Field*> expected_terms;  // used to validate terms
                                                      // used to validate docs
    std::vector<irs::SeekTermIterator::ptr> expected_term_itrs;

    auto& actual_segment = actual_reader[0];
    auto actual_terms = actual_segment.field(kNameAnlPayFieldId);
    ASSERT_FALSE(!actual_terms);

    for (size_t i = 0; i < thread_count; ++i) {
      auto field = expected_index[0].fields().find(kNameAnlPayFieldId);
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
                irs::IndexFeatures::Offs;
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

              auto* actual_freq = irs::get<irs::FreqBlockAttr>(*act_docs_itr);
              auto* expected_freq = irs::get<irs::FreqBlockAttr>(*exp_docs_itr);
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
                act_docs_itr->FetchScoreArgs(0);
                ASSERT_EQ(expected_freq->value[0], actual_freq->value[0]);

                auto* expected_offs = irs::get<irs::OffsAttr>(*expected_pos);
                auto* actual_offs = irs::get<irs::OffsAttr>(*actual_pos);
                ASSERT_FALSE(!expected_offs);
                ASSERT_FALSE(!actual_offs);

                auto* expected_pay = irs::get<irs::PayAttr>(*expected_pos);
                auto* actual_pay = irs::get<irs::PayAttr>(*actual_pos);
                if (expected_pay) {
                  ASSERT_FALSE(!actual_pay);
                }

                while (actual_pos->next()) {
                  ASSERT_TRUE(expected_pos->next());
                  ASSERT_EQ(expected_pos->value(), actual_pos->value());
                  ASSERT_EQ(expected_offs->start, actual_offs->start);
                  ASSERT_EQ(expected_offs->end, actual_offs->end);
                  if (expected_pay) {
                    ASSERT_EQ(expected_pay->value, actual_pay->value);
                  }
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
      auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate,
                                           irs::tests::DefaultWriterOptions());
      ASSERT_NE(nullptr, writer);
      // can't open another writer at the same time on the same directory
      ASSERT_THROW(irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate,
                                          irs::tests::DefaultWriterOptions()),
                   irs::LockObtainFailed);
      ASSERT_EQ(0, writer->BufferedDocs());
    }

    {
      // open writer
      auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate,
                                           irs::tests::DefaultWriterOptions());
      ASSERT_NE(nullptr, writer);

      writer->RefreshCommit();
      AssertSnapshotEquality(*writer);
      irs::DirectoryCleaner::clean(dir());
      // can't open another writer at the same time on the same directory
      ASSERT_THROW(irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate,
                                          irs::tests::DefaultWriterOptions()),
                   irs::LockObtainFailed);
      ASSERT_EQ(0, writer->BufferedDocs());
    }

    {
      // open writer
      auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate,
                                           irs::tests::DefaultWriterOptions());
      ASSERT_NE(nullptr, writer);

      ASSERT_EQ(0, writer->BufferedDocs());
    }

    {
      // open writer with NOLOCK hint
      auto options0 = irs::tests::DefaultWriterOptions();
      options0.lock_repository = false;
      auto writer0 =
        irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate, options0);
      ASSERT_NE(nullptr, writer0);

      // can open another writer at the same time on the same directory
      auto options1 = irs::tests::DefaultWriterOptions();
      options1.lock_repository = false;
      auto writer1 =
        irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate, options1);
      ASSERT_NE(nullptr, writer1);

      ASSERT_EQ(0, writer0->BufferedDocs());
      ASSERT_EQ(0, writer1->BufferedDocs());
    }

    {
      // open writer with NOLOCK hint
      auto options0 = irs::tests::DefaultWriterOptions();
      options0.lock_repository = false;
      auto writer0 =
        irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate, options0);
      ASSERT_NE(nullptr, writer0);

      // can open another writer at the same time on the same directory and
      // acquire lock
      auto writer1 =
        irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate | irs::kOmAppend,
                               irs::tests::DefaultWriterOptions());
      ASSERT_NE(nullptr, writer1);

      ASSERT_EQ(0, writer0->BufferedDocs());
      ASSERT_EQ(0, writer1->BufferedDocs());
    }

    {
      // open writer with NOLOCK hint
      auto options0 = irs::tests::DefaultWriterOptions();
      options0.lock_repository = false;
      auto writer0 =
        irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate, options0);
      ASSERT_NE(nullptr, writer0);
      writer0->RefreshCommit();

      // can open another writer at the same time on the same directory and
      // acquire lock
      auto writer1 = irs::IndexWriter::Make(dir(), codec(), irs::kOmAppend,
                                            irs::tests::DefaultWriterOptions());
      ASSERT_NE(nullptr, writer1);

      ASSERT_EQ(0, writer0->BufferedDocs());
      ASSERT_EQ(0, writer1->BufferedDocs());
    }
  }

  void WriterCheckOpenModes() {
    // APPEND to nonexisting index, shoud fail
    ASSERT_THROW(irs::IndexWriter::Make(dir(), codec(), irs::kOmAppend,
                                        irs::tests::DefaultWriterOptions()),
                 irs::FileNotFound);
    // read index in empty directory, should fail
    ASSERT_THROW((irs::DirectoryReader(dir(), codec(),
                                       irs::tests::DefaultReaderOptions())),
                 irs::IndexNotFound);

    // create empty index
    {
      auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate,
                                           irs::tests::DefaultWriterOptions());

      writer->RefreshCommit();
      AssertSnapshotEquality(*writer);
    }

    // read empty index, it should not fail
    {
      auto reader = irs::DirectoryReader(dir(), codec(),
                                         irs::tests::DefaultReaderOptions());
      ASSERT_EQ(0, reader.live_docs_count());
      ASSERT_EQ(0, reader.docs_count());
      ASSERT_EQ(0, reader.size());
      ASSERT_EQ(reader.begin(), reader.end());
    }

    // append to index
    {
      auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmAppend,
                                           irs::tests::DefaultWriterOptions());
      tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                  &tests::GenericJsonFieldFactory);
      const tests::Document* doc1 = gen.next();
      ASSERT_EQ(0, writer->BufferedDocs());
      ASSERT_TRUE(InsertWithName(*writer, *doc1));
      ASSERT_EQ(1, writer->BufferedDocs());
      writer->RefreshCommit();
      AssertSnapshotEquality(*writer);
      ASSERT_EQ(0, writer->BufferedDocs());
    }

    // read index, it should not fail
    {
      auto reader = irs::DirectoryReader(dir(), codec(),
                                         irs::tests::DefaultReaderOptions());
      ASSERT_EQ(1, reader.live_docs_count());
      ASSERT_EQ(1, reader.docs_count());
      ASSERT_EQ(1, reader.size());
      ASSERT_NE(reader.begin(), reader.end());
    }

    // append to index
    {
      auto writer =
        irs::IndexWriter::Make(dir(), codec(), irs::kOmAppend | irs::kOmCreate,
                               irs::tests::DefaultWriterOptions());
      tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                  &tests::GenericJsonFieldFactory);
      const tests::Document* doc1 = gen.next();
      ASSERT_EQ(0, writer->BufferedDocs());
      ASSERT_TRUE(InsertWithName(*writer, *doc1));
      ASSERT_EQ(1, writer->BufferedDocs());
      writer->RefreshCommit();
      AssertSnapshotEquality(*writer);
      ASSERT_EQ(0, writer->BufferedDocs());
    }

    // read index, it should not fail
    {
      auto reader = irs::DirectoryReader(dir(), codec(),
                                         irs::tests::DefaultReaderOptions());
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
          auto f = std::make_shared<tests::StringField>(name, data.str);
          f->id = tests::FieldIdFor(name);
          doc.insert(std::move(f));
        }
      });
    const tests::Document* doc1 = gen.next();
    const tests::Document* doc2 = gen.next();

    auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_EQ(1, writer->BufferedDocs());
    writer->RefreshBegin();  // start transaction #1
    ASSERT_EQ(0, writer->BufferedDocs());
    ASSERT_TRUE(InsertWithName(*writer, *doc2));  // add another document while
                                                  // transaction in opened
    ASSERT_EQ(1, writer->BufferedDocs());
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);       // finish transaction #1
    ASSERT_EQ(1, writer->BufferedDocs());  // still have 1 buffered document
                                           // not included into transaction #1

    // check index, 1 document in 1 segment
    {
      auto reader = irs::DirectoryReader(dir(), codec(),
                                         irs::tests::DefaultReaderOptions());
      ASSERT_EQ(1, reader.live_docs_count());
      ASSERT_EQ(1, reader.docs_count());
      ASSERT_EQ(1, reader.size());
      ASSERT_NE(reader.begin(), reader.end());
    }

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);  // transaction #2
    ASSERT_EQ(0, writer->BufferedDocs());
    // check index, 2 documents in 2 segments
    {
      auto reader = irs::DirectoryReader(dir(), codec(),
                                         irs::tests::DefaultReaderOptions());
      ASSERT_EQ(2, reader.live_docs_count());
      ASSERT_EQ(2, reader.docs_count());
      ASSERT_EQ(2, reader.size());
      ASSERT_NE(reader.begin(), reader.end());
    }

    // check documents
    {
      auto reader = irs::DirectoryReader(dir(), codec(),
                                         irs::tests::DefaultReaderOptions());

      // segment #1
      {
        auto& segment = reader[0];
        const auto* column = segment.Column(kNameColumnId);
        ASSERT_NE(nullptr, column);
        irs::tests::BlobPointReader values{segment, *column};
        auto terms = segment.field(kSameFieldId);
        ASSERT_NE(nullptr, terms);
        auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
        ASSERT_TRUE(term_itr->next());
        auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));
        ASSERT_FALSE(docs_itr->next());
      }

      // segment #1
      {
        auto& segment = reader[1];
        auto* column = segment.Column(kNameColumnId);
        ASSERT_NE(nullptr, column);
        irs::tests::BlobPointReader values{segment, *column};
        auto terms = segment.field(kSameFieldId);
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

  void WriterBeginRollback() {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);

    const tests::Document* doc1 = gen.next();
    const tests::Document* doc2 = gen.next();
    const tests::Document* doc3 = gen.next();

    {
      auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate,
                                           irs::tests::DefaultWriterOptions());

      ASSERT_TRUE(InsertWithName(*writer, *doc1));
      writer->RefreshAbort();  // does nothing
      ASSERT_EQ(1, writer->BufferedDocs());
      ASSERT_TRUE(writer->RefreshBegin());
      ASSERT_FALSE(
        writer->RefreshBegin());  // try to begin already opened transaction

      // index still does not exist
      ASSERT_THROW((irs::DirectoryReader(dir(), codec(),
                                         irs::tests::DefaultReaderOptions())),
                   irs::IndexNotFound);

      writer->RefreshAbort();  // rollback transaction
      writer->RefreshAbort();  // does nothing
      ASSERT_EQ(0, writer->BufferedDocs());

      writer->RefreshCommit();
      AssertSnapshotEquality(*writer);  // commit

      // check index, it should be empty
      {
        auto reader = irs::DirectoryReader(dir(), codec(),
                                           irs::tests::DefaultReaderOptions());
        ASSERT_EQ(0, reader.live_docs_count());
        ASSERT_EQ(0, reader.docs_count());
        ASSERT_EQ(0, reader.size());
        ASSERT_EQ(reader.begin(), reader.end());
      }
    }

    // test rolled-back index can still be opened after directory cleaner run
    {
      auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate,
                                           irs::tests::DefaultWriterOptions());
      ASSERT_TRUE(InsertWithName(*writer, *doc2));
      ASSERT_TRUE(writer->RefreshBegin());  // prepare for commit tx #1
      writer->RefreshCommit();
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
      ASSERT_TRUE(InsertWithName(*writer, *doc3));
      ASSERT_TRUE(writer->RefreshBegin());  // prepare for commit tx #2
      writer->RefreshAbort();               // rollback tx #2
      irs::directory_utils::RemoveAllUnreferenced(dir());
      file_count = 0;
      dir().visit(dir_visitor);
      ASSERT_EQ(file_count_before,
                file_count);  // ensure rolled back file refs were released

      auto reader = irs::DirectoryReader(dir(), codec(),
                                         irs::tests::DefaultReaderOptions());
      ASSERT_EQ(1, reader.size());
      auto& segment = reader[0];  // assume 0 is id of first/only segment
      auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc2
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
      irs::field_id Id() const { return 1; }

      irs::IndexFeatures GetIndexFeatures() const {
        return irs::IndexFeatures::None;
      }

      irs::Tokenizer& GetTokens() { return token_stream; }

      bool Write(irs::DataOutput& out) const {
        irs::WriteStr(out, std::string_view{"test_field"});
        return true;
      }

      InvalidTokenizer token_stream;
    } invalid_field;

    const tests::Document* doc1 = gen.next();
    const tests::Document* doc2 = gen.next();
    const tests::Document* doc3 = gen.next();
    auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    {
      auto ctx = writer->GetBatch();
      auto doc = ctx.Insert(false, 3);
      doc.NextDocument();
      doc.NextDocument();
      doc.NextDocument();
      doc.NextFieldBatch();

      ASSERT_TRUE(doc.Insert(doc1->stored.begin(), doc1->stored.end()));
      doc.NextDocument();
      ASSERT_TRUE(doc.Insert(doc2->stored.begin(), doc2->stored.end()));
      doc.NextDocument();

      ASSERT_FALSE(doc.Insert(invalid_field));
      ASSERT_FALSE(doc);
      // entire batch should be rollbacked
    }
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    writer->RefreshCommit();
    // should be only one live doc - doc3
    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    ASSERT_EQ(1, segment.live_docs_count());
    auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    // skip docs deleted during batch rollback
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc3
    ASSERT_FALSE(docs_itr->next());
  }

  void ConcurrentReadSingleColumnSmoke() {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    std::vector<const tests::Document*> expected_docs;

    // write some data into columnstore
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    for (auto* doc = gen.next(); doc; doc = gen.next()) {
      ASSERT_TRUE(InsertWithName(*writer, *doc));
      expected_docs.push_back(doc);
    }
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader = open_reader(irs::tests::DefaultReaderOptions());

    // 1-st iteration: noncached
    // 2-nd iteration: cached
    for (size_t i = 0; i < 2; ++i) {
      auto read_columns = [&expected_docs, &reader]() {
        size_t i = 0;
        for (auto& segment : reader) {
          auto* column = segment.Column(kNameColumnId);
          if (!column) {
            return false;
          }
          irs::tests::BlobPointReader values{segment, *column};
          for (irs::doc_id_t doc = (irs::doc_limits::min)(),
                             max = segment.docs_count();
               doc <= max; ++doc) {
            auto* expected_doc = expected_docs[i];
            auto expected_name =
              expected_doc->stored.get<tests::StringField>("name")->value();
            if (expected_name !=
                irs::tests::ReadStoredStr<std::string_view>(values, doc)) {
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

  void InsertDocWithNullEmptyTerm() {
    class Field {
     public:
      Field(irs::field_id id, const std::string_view& value)
        : _stream(std::make_unique<irs::StringTokenizer>()),
          _id(id),
          _value(value) {}
      Field(Field&& other) noexcept
        : _stream(std::move(other._stream)),
          _id(other._id),
          _value(std::move(other._value)) {}
      irs::field_id Id() const { return _id; }
      irs::Tokenizer& GetTokens() const {
        _stream->reset(_value);
        return *_stream;
      }
      irs::IndexFeatures GetIndexFeatures() const {
        return irs::IndexFeatures::None;
      }

     private:
      mutable std::unique_ptr<irs::StringTokenizer> _stream;
      irs::field_id _id;
      std::string_view _value;
    };

    // write docs with empty terms
    {
      auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate,
                                           irs::tests::DefaultWriterOptions());
      // doc0: empty, nullptr
      {
        std::vector<Field> doc;
        doc.emplace_back(kNameFieldId, std::string_view("", 0));
        doc.emplace_back(kNameFieldId, std::string_view{});
        ASSERT_TRUE(tests::Insert(*writer, doc.begin(), doc.end()));
      }
      // doc1: nullptr, empty, nullptr
      {
        std::vector<Field> doc;
        doc.emplace_back(kName1FieldId, std::string_view{});
        doc.emplace_back(kName1FieldId, std::string_view("", 0));
        doc.emplace_back(kNameFieldId, std::string_view{});
        ASSERT_TRUE(tests::Insert(*writer, doc.begin(), doc.end()));
      }
      writer->RefreshCommit();
      AssertSnapshotEquality(*writer);
    }

    // check fields with empty terms
    {
      auto reader = irs::DirectoryReader(dir(), nullptr,
                                         irs::tests::DefaultReaderOptions());
      ASSERT_EQ(1, reader.size());
      auto& segment = reader[0];

      {
        const auto ids = segment.field_ids();
        ASSERT_EQ(2u, ids.size());
      }

      {
        auto* field = segment.field(kNameFieldId);
        ASSERT_NE(nullptr, field);
        ASSERT_EQ(1, field->size());
        ASSERT_EQ(2, field->docs_count());
        auto term = field->iterator(irs::SeekMode::NORMAL);
        ASSERT_TRUE(term->next());
        ASSERT_EQ(0, term->value().size());
        ASSERT_FALSE(term->next());
      }

      {
        auto* field = segment.field(kName1FieldId);
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
      auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate,
                                           irs::tests::DefaultWriterOptions());

      writer->RefreshCommit();
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
        irs::IndexWriter::Make(override_dir, codec(), irs::kOmAppend,
                               irs::tests::DefaultWriterOptions());

      ASSERT_TRUE(InsertWithName(*writer, *doc1));
      ASSERT_TRUE(InsertWithName(*writer, *doc2));
      ASSERT_TRUE(InsertWithName(*writer, *doc3));
      ASSERT_TRUE(InsertWithName(*writer, *doc4));
      ASSERT_THROW(writer->RefreshCommit(), irs::IoError);
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
        irs::IndexWriter::Make(override_dir, codec(), irs::kOmAppend,
                               irs::tests::DefaultWriterOptions());

      ASSERT_TRUE(InsertWithName(*writer, *doc1));
      ASSERT_TRUE(InsertWithName(*writer, *doc2));
      ASSERT_TRUE(InsertWithName(*writer, *doc3));
      ASSERT_TRUE(InsertWithName(*writer, *doc4));
      ASSERT_THROW(writer->RefreshCommit(), irs::IoError);
    }

    // check index, it should be empty
    {
      auto reader = irs::DirectoryReader(dir(), codec(),
                                         irs::tests::DefaultReaderOptions());
      ASSERT_EQ(0, reader.live_docs_count());
      ASSERT_EQ(0, reader.docs_count());
      ASSERT_EQ(0, reader.size());
      ASSERT_EQ(reader.begin(), reader.end());
    }
  }

  void DocsBitUnion(irs::IndexFeatures features, size_t docs_count_per_term);
};

void IndexTestCase::DocsBitUnion(irs::IndexFeatures features,
                                 size_t docs_count_per_term) {
  tests::StringViewField field("0", features);
  // Stable field id assigned for this fixture's lone "0" field so the
  // reader-side `segment.field(id)` lookup matches what the writer registers.
  constexpr irs::field_id kZeroFieldId = 100;
  field.id = kZeroFieldId;
  const auto docs_count = docs_count_per_term * 2 + 1;
  std::vector<uint64_t> expected_a;
  std::vector<uint64_t> expected_b;
  constexpr auto kWordBits = irs::BitsRequired<uint64_t>();
  const size_t num_words = (docs_count + kWordBits - 1) / kWordBits;
  expected_a.resize(num_words, 0);
  expected_b.resize(num_words, 0);
  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    {
      auto docs = writer->GetBatch();
      for (size_t i = 1; i < docs_count; ++i) {
        const std::string_view value = i % 2 ? "A" : "B";
        if (value == "A") {
          irs::SetBit(expected_a[i / kWordBits], i % kWordBits);
        } else {
          irs::SetBit(expected_b[i / kWordBits], i % kWordBits);
        }
        field.value(value);
        ASSERT_TRUE(docs.Insert().Insert(field));
      }

      field.value("C");
      ASSERT_TRUE(docs.Insert().Insert(field));
      docs.Commit();
    }

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
  }

  auto reader = open_reader(irs::tests::DefaultReaderOptions());
  ASSERT_NE(nullptr, reader);
  ASSERT_EQ(1, reader->size());
  auto& segment = (*reader)[0];
  ASSERT_EQ(docs_count, segment.docs_count());
  ASSERT_EQ(docs_count, segment.live_docs_count());

  const auto* term_reader = segment.field(field.Id());
  ASSERT_NE(nullptr, term_reader);
  ASSERT_EQ(docs_count, term_reader->docs_count());
  ASSERT_EQ(3, term_reader->size());
  ASSERT_EQ("A", irs::ViewCast<char>(term_reader->min()));
  ASSERT_EQ("C", irs::ViewCast<char>(term_reader->max()));
  ASSERT_EQ(field.Id(), term_reader->meta().id);
  ASSERT_EQ(field.GetIndexFeatures(), term_reader->meta().index_features);

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

  std::vector<uint64_t> actual_docs_ab(num_words);
  // -1 as we exclude C term
  ASSERT_EQ(docs_count - 1,
            term_reader->BitUnion(cookie_provider, actual_docs_ab.data()));
  for (size_t i = 0; i < num_words; ++i) {
    ASSERT_EQ(expected_a[i] | expected_b[i], actual_docs_ab[i]);
  }
}

TEST_P(IndexTestCase, s2sequence) {
  std::vector<std::string> sequence;
  absl::flat_hash_set<std::string> indexed;

  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    std::ifstream stream{resource("s2sequence"), std::ifstream::in};
    ASSERT_TRUE(static_cast<bool>(stream));

    std::string str;
    auto field = std::make_shared<tests::StringField>("value");
    field->id = kValueFieldId;
    tests::Document doc;
    doc.indexed.push_back(field);

    auto& expected_segment = this->index().emplace_back();
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

    ASSERT_TRUE(writer->RefreshCommit());
    AssertSnapshotEquality(*writer);
  }

  assert_index();

  auto reader = open_reader(irs::tests::DefaultReaderOptions());
  ASSERT_NE(nullptr, reader);
  ASSERT_EQ(1, reader->size());
  auto& segment = (*reader)[0];
  auto* field = segment.field(kValueFieldId);
  ASSERT_NE(nullptr, field);
  auto& expected_field = index().front().fields().at(kValueFieldId);

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
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);
    reader0 = writer->GetSnapshot();
    ASSERT_EQ(0, reader0.size());
    ASSERT_EQ(irs::DirectoryMeta{}, reader0.Meta());
    ASSERT_EQ(0, reader0.docs_count());
    ASSERT_EQ(0, reader0.live_docs_count());

    ASSERT_TRUE(tests::Insert(*writer, *doc1));
    ASSERT_TRUE(writer->RefreshCommit());
    reader1 = writer->GetSnapshot();
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir(), nullptr, irs::tests::DefaultReaderOptions()));
    tests::AssertSnapshotEquality(writer->GetSnapshot(), reader0->Reopen());
    tests::AssertSnapshotEquality(writer->GetSnapshot(), reader1->Reopen());
    ASSERT_EQ(1, reader1.size());
    ASSERT_FALSE(reader1.Meta().filename.empty());
    ASSERT_EQ(1, reader1.docs_count());
    ASSERT_EQ(1, reader1.live_docs_count());

    ASSERT_TRUE(tests::Insert(*writer, *doc2));
    ASSERT_TRUE(writer->RefreshCommit());
    reader2 = writer->GetSnapshot();
    ASSERT_EQ(2, reader2.size());
    ASSERT_FALSE(reader2.Meta().filename.empty());
    ASSERT_EQ(2, reader2.docs_count());
    ASSERT_EQ(2, reader2.live_docs_count());
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir(), nullptr, irs::tests::DefaultReaderOptions()));
    tests::AssertSnapshotEquality(writer->GetSnapshot(), reader0->Reopen());
    tests::AssertSnapshotEquality(writer->GetSnapshot(), reader1->Reopen());
    tests::AssertSnapshotEquality(writer->GetSnapshot(), reader2->Reopen());
  }

  // Check reader after the writer is closed
  auto check_reader = [&](irs::DirectoryReader reader) {
    reader = reader0->Reopen();
    tests::AssertSnapshotEquality(
      reader,
      irs::DirectoryReader(dir(), nullptr, irs::tests::DefaultReaderOptions()));
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

TEST_P(IndexTestCase, check_attributes_order) {
  // The legacy test exercised `segment.columns()` -- a name-ordered
  // iterator over columnstore attributes. The new typed columnstore
  // (`irs::ColReader`) addresses columns by `field_id`, not
  // name, and exposes no equivalent ordered iterator. The original
  // assertion (alphabetical iteration / `seek(name)`) has no analogue
  // in the new API.
  GTEST_SKIP() << "segment.columns() name-ordered iterator removed with "
                  "the new columnstore (columns are addressed by field_id)";
}

TEST_P(IndexTestCase, clear_writer) { ClearWriter(); }

TEST_P(IndexTestCase, open_writer) { OpenWriterCheckLock(); }

TEST_P(IndexTestCase, check_writer_open_modes) { WriterCheckOpenModes(); }

TEST_P(IndexTestCase, writer_transaction_isolation) {
  WriterTransactionIsolation();
}

TEST_P(IndexTestCase, writer_atomicity_check) { WriterAtomicityCheck(); }

TEST_P(IndexTestCase, writer_bulk_insert) {
  // The legacy test relied on `doc.Insert<irs::Action::INDEX|STORE>(field)`
  // and the "drop the doc when its serializer Write() returns false"
  // semantics of the inline STORE path. Action templating is gone --
  // `Document::Insert` is inverted-indexing-only -- and STORE is now
  // handled out-of-band via `Document::GetColWriter()` + `StoreFieldAt`,
  // which has no drop-on-failure hook. The exact mixed-INDEX/STORE
  // validity matrix the test asserts no longer maps to a single API call.
  GTEST_SKIP() << "Legacy Insert<Action::INDEX|STORE> drop-on-fail "
                  "semantics removed; STORE is decoupled from the doc "
                  "insert path in the new typed columnstore";
}

TEST_P(IndexTestCase, writer_begin_rollback) { WriterBeginRollback(); }

TEST_P(IndexTestCase, writer_batch_rollback) { WriterBatchWithErrorRollback(); }

TEST_P(IndexTestCase, writer_begin_clear_empty_index) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  const tests::Document* doc1 = gen.next();

  {
    auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_EQ(1, writer->BufferedDocs());
    writer->Clear();  // rollback started transaction and clear index
    ASSERT_EQ(0, writer->BufferedDocs());
    ASSERT_FALSE(writer->RefreshBegin());  // nothing to commit

    // check index, it should be empty
    {
      auto reader = irs::DirectoryReader(dir(), codec(),
                                         irs::tests::DefaultReaderOptions());
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
    auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_EQ(1, writer->BufferedDocs());
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, writer->BufferedDocs());

    // check index, it should not be empty
    {
      auto reader = irs::DirectoryReader(dir(), codec(),
                                         irs::tests::DefaultReaderOptions());
      ASSERT_EQ(1, reader.live_docs_count());
      ASSERT_EQ(1, reader.docs_count());
      ASSERT_EQ(1, reader.size());
      ASSERT_NE(reader.begin(), reader.end());
    }

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_EQ(1, writer->BufferedDocs());
    ASSERT_TRUE(writer->RefreshBegin());  // start transaction
    ASSERT_EQ(0, writer->BufferedDocs());

    writer->Clear();  // rollback and clear index contents
    ASSERT_EQ(0, writer->BufferedDocs());
    ASSERT_FALSE(writer->RefreshBegin());  // nothing to commit

    // check index, it should be empty
    {
      auto reader = irs::DirectoryReader(dir(), codec(),
                                         irs::tests::DefaultReaderOptions());
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
    auto writer = irs::IndexWriter::Make(synced_dir, codec(), irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    const auto* doc1 = gen.next();
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // check index, it should contain expected number of docs
    auto reader = irs::DirectoryReader(synced_dir, codec(),
                                       irs::tests::DefaultReaderOptions());
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
    auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_EQ(1, writer->BufferedDocs());
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, writer->BufferedDocs());

    // check index, it should not be empty
    {
      auto reader = irs::DirectoryReader(dir(), codec(),
                                         irs::tests::DefaultReaderOptions());
      ASSERT_EQ(1, reader.live_docs_count());
      ASSERT_EQ(1, reader.docs_count());
      ASSERT_EQ(1, reader.size());
      ASSERT_NE(reader.begin(), reader.end());
    }

    writer->Clear();  // clear index contents
    ASSERT_EQ(0, writer->BufferedDocs());
    ASSERT_FALSE(writer->RefreshBegin());  // nothing to commit

    // check index, it should be empty
    {
      auto reader = irs::DirectoryReader(dir(), codec(),
                                         irs::tests::DefaultReaderOptions());
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
    add_segment_batched(gen, 100, irs::kOmCreate,
                        irs::tests::DefaultWriterOptions());
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
  // less than block
  DocsBitUnion(irs::IndexFeatures::None, 63);
  DocsBitUnion(irs::IndexFeatures::Freq, 63);

  // exactly one block
  DocsBitUnion(irs::IndexFeatures::None, 128);
  DocsBitUnion(irs::IndexFeatures::Freq, 128);

  // more than block
  DocsBitUnion(irs::IndexFeatures::None, 135);
  DocsBitUnion(irs::IndexFeatures::Freq, 135);

  // exactly two blocks
  DocsBitUnion(irs::IndexFeatures::None, 256);
  DocsBitUnion(irs::IndexFeatures::Freq, 256);

  // more than two blocks
  DocsBitUnion(irs::IndexFeatures::None, 257);
  DocsBitUnion(irs::IndexFeatures::Freq, 257);
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
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    std::thread thread0([&writer, docs]() {
      for (size_t i = 0, count = docs.size(); i < count; i += 2) {
        auto& doc = docs[i];
        ASSERT_TRUE(InsertWithName(*writer, *doc));
      }
    });
    std::thread thread1([&writer, docs]() {
      for (size_t i = 1, count = docs.size(); i < count; i += 2) {
        auto& doc = docs[i];
        ASSERT_TRUE(InsertWithName(*writer, *doc));
      }
    });

    thread0.join();
    thread1.join();
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
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
        {
          auto f = std::make_shared<tests::StringField>(name, data.str);
          f->id = tests::FieldIdFor(name);
          doc.insert(std::move(f));
        }
      }
    });
  std::vector<const tests::Document*> docs;
  std::atomic<bool> first_doc(false);

  for (const tests::Document* doc; (doc = gen.next()) != nullptr;
       docs.emplace_back(doc)) {
  }

  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    std::thread thread0([&writer, docs, &first_doc]() {
      auto& doc = docs[0];
      InsertWithName(*writer, *doc);
      first_doc = true;

      for (size_t i = 2, count = docs.size(); i < count;
           i += 2) {  // skip first doc
        auto& doc = docs[i];
        InsertWithName(*writer, *doc);
      }
    });
    std::thread thread1([&writer, docs]() {
      for (size_t i = 1, count = docs.size(); i < count; i += 2) {
        auto& doc = docs[i];
        InsertWithName(*writer, *doc);
      }
    });
    std::thread thread2([&writer, &query_doc1, &first_doc]() {
      while (!first_doc)
        ;  // busy-wait until first document loaded
      tests::Remove(*writer, std::move(query_doc1));
    });

    thread0.join();
    thread1.join();
    thread2.join();
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    std::unordered_set<std::string_view> expected = {
      "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L",
      "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W",
      "X", "Y", "Z", "~", "!", "@", "#", "$", "%"};
    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
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
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      while (docs_itr->next()) {
        ASSERT_EQ(1, expected.erase(irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value())));
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
        {
          auto f = std::make_shared<tests::StringField>(name, data.str);
          f->id = tests::FieldIdFor(name);
          doc.insert(std::move(f));
        }
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();

  // remove added docs, add same docs again commit separate thread before end of
  // add
  {
    std::condition_variable cond;
    std::mutex mutex;
    auto query_doc1_doc2 =
      MakeByTermOrByTerm(kNameFieldId, "A", kNameFieldId, "B");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    std::unique_lock lock{mutex};
    std::atomic<bool> stop(false);
    std::thread thread([&]() -> void {
      std::lock_guard lock{mutex};
      writer->RefreshCommit();
      AssertSnapshotEquality(*writer);
      stop = true;
      cond.notify_all();
    });

    // initial add docs
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // remove docs
    tests::Remove(*writer, *(query_doc1_doc2.get()));

    // re-add docs into a single segment
    {
      auto ctx = writer->GetBatch();

      {
        auto doc = ctx.Insert();
        doc.Insert(doc1->indexed.begin(), doc1->indexed.end());
        CaptureNameLikeFields(doc, doc1->indexed);
      }
      {
        auto doc = ctx.Insert();
        doc.Insert(doc2->indexed.begin(), doc2->indexed.end());
        CaptureNameLikeFields(doc, doc2->indexed);
      }

      // commit from a separate thread before end of add
      lock.unlock();
      std::mutex cond_mutex;
      std::unique_lock cond_lock{cond_mutex};
      auto result = cond.wait_for(
        cond_lock, 100ms);  // assume thread commits within 100 msec

      // As declaration for wait_for contains "It may also be unblocked
      // spuriously." for all platforms
      while (!stop && result == std::cv_status::no_timeout) {
        result = cond.wait_for(cond_lock, 100ms);
      }

      // FIXME TODO add once segment_context will not block flush_all()
      // ASSERT_TRUE(stop);
      ctx.Commit();
    }

    thread.join();

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    /* FIXME TODO add once segment_context will not block flush_all()
    ASSERT_EQ(0, reader.docs_count());
    ASSERT_EQ(0, reader.live_docs_count());
    writer->RefreshCommit(); AssertSnapshotEquality(*writer); // commit after
    releasing documents_context reader = irs::directory_reader::open(dir(),
    codec());
    */
    ASSERT_EQ(2, reader.docs_count());
    ASSERT_EQ(2, reader.live_docs_count());
  }

  // remove added docs, add same docs again commit separate thread after end of
  // add
  {
    auto query_doc1_doc2 =
      MakeByTermOrByTerm(kNameFieldId, "A", kNameFieldId, "B");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    // initial add docs
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // remove docs
    tests::Remove(*writer, *(query_doc1_doc2.get()));

    // re-add docs into a single segment
    {
      auto ctx = writer->GetBatch();

      {
        auto doc = ctx.Insert();
        doc.Insert(doc1->indexed.begin(), doc1->indexed.end());
        CaptureNameLikeFields(doc, doc1->indexed);
      }
      {
        auto doc = ctx.Insert();
        doc.Insert(doc2->indexed.begin(), doc2->indexed.end());
        CaptureNameLikeFields(doc, doc2->indexed);
      }
      ctx.Commit();
    }

    std::thread thread([&]() -> void {
      writer->RefreshCommit();
      AssertSnapshotEquality(*writer);
    });
    thread.join();

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
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
        {
          auto f = std::make_shared<tests::StringField>(name, data.str);
          f->id = tests::FieldIdFor(name);
          doc.insert(std::move(f));
        }
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

  // remove without tick
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto query_doc3 = MakeByTerm(kNameFieldId, "C");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    tests::Remove<false>(*writer, *query_doc1);
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    tests::Remove(*writer, *query_doc3);
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    EXPECT_EQ(3, reader.docs_count());
    EXPECT_EQ(2, reader.live_docs_count());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));
    ASSERT_FALSE(docs_itr->next());
  }

  // holding document_context after insert across commit does not block
  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    auto ctx = writer->GetBatch();
    // wait for insertion to start
    auto field_cond_lock = std::unique_lock{field.cond_mutex};

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    std::thread thread1([&]() -> void {
      writer->RefreshCommit();
      AssertSnapshotEquality(*writer);
      auto lock = std::lock_guard{field.cond_mutex};
      field.cond.notify_all();
    });

    ASSERT_EQ(std::cv_status::no_timeout,
              field.cond.wait_for(
                field_cond_lock,
                std::chrono::milliseconds(10000)));  // verify commit() finishes
    {
      ctx.Commit();
    }  // release ctx before join() in case of test failure
    thread1.join();

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // holding document_context after remove across commit does not block
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));

    auto ctx = writer->GetBatch();
    // wait for insertion to start
    auto field_cond_lock = std::unique_lock{field.cond_mutex};
    ctx.Remove(*(query_doc1));
    std::atomic<bool> commit(false);  // FIXME TODO remove once segment_context
                                      // will not block flush_all()
    std::thread thread1([&]() -> void {
      writer->RefreshCommit();
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
    while (!commit && result == std::cv_status::no_timeout) {
      result = field.cond.wait_for(field_cond_lock, 100ms);
    }

    ASSERT_EQ(std::cv_status::timeout, result);
    field_cond_lock
      .unlock();  // verify commit() finishes FIXME TODO use below
                  // once segment_context will not block flush_all()
    // ASSERT_EQ(std::cv_status::no_timeout, result); // verify
    // commit() finishes
    //  FIXME TODO add once segment_context will not block flush_all()
    // ASSERT_TRUE(commit);
    {
      ctx.Commit();
    }  // release ctx before join() in case of test failure
    thread1.join();
    // FIXME TODO add once segment_context will not block flush_all()
    // writer->RefreshCommit(); AssertSnapshotEquality(*writer); // commit doc
    // removal

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // holding document_context after replace across commit does not block (single
  // doc)
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    auto ctx = writer->GetBatch();
    // wait for insertion to start
    auto field_cond_lock = std::unique_lock{field.cond_mutex};

    {
      auto doc = ctx.Replace(*(query_doc1));
      doc.Insert(doc2->indexed.begin(), doc2->indexed.end());
      CaptureNameLikeFields(doc, doc2->indexed);
    }
    std::atomic<bool> commit(false);  // FIXME TODO remove once segment_context
                                      // will not block flush_all()
    std::thread thread1([&]() -> void {
      writer->RefreshCommit();
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
    while (!commit && result == std::cv_status::no_timeout) {
      result = field.cond.wait_for(field_cond_lock, 100ms);
    }

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
      ctx.Commit();
    }  // release ctx before join() in case of test failure
    thread1.join();
    // FIXME TODO add once segment_context will not block
    // flush_all() writer->RefreshCommit(); AssertSnapshotEquality(*writer); //
    // commit doc replace

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // rollback empty
  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    {
      auto ctx = writer->GetBatch();

      ctx.Reset();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc1->indexed.begin(), doc1->indexed.end()));
        CaptureNameLikeFields(doc, doc1->indexed);
      }
      ctx.Commit();
    }

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // rollback inserts
  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    {
      auto ctx = writer->GetBatch();

      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc2->indexed.begin(), doc2->indexed.end()));
        CaptureNameLikeFields(doc, doc2->indexed);
      }
      ctx.Reset();
    }

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // rollback inserts + some more
  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    {
      auto ctx = writer->GetBatch();

      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc1->indexed.begin(), doc1->indexed.end()));
        CaptureNameLikeFields(doc, doc1->indexed);
      }
      ctx.Reset();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc2->indexed.begin(), doc2->indexed.end()));
        CaptureNameLikeFields(doc, doc2->indexed);
      }
      ctx.Commit();
    }

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // rollback multiple inserts + some more
  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    {
      auto ctx = writer->GetBatch();

      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc2->indexed.begin(), doc2->indexed.end()));
        CaptureNameLikeFields(doc, doc2->indexed);
      }
      ctx.Reset();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc3->indexed.begin(), doc3->indexed.end()));
        CaptureNameLikeFields(doc, doc3->indexed);
      }
      ctx.Reset();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc4->indexed.begin(), doc4->indexed.end()));
        CaptureNameLikeFields(doc, doc4->indexed);
      }
      ctx.Commit();
    }

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc4
    ASSERT_FALSE(docs_itr->next());
  }

  // rollback inserts split over multiple segment_writers
  {
    auto options = irs::tests::DefaultWriterOptions();
    options.segment_docs_max = 1;  // each doc will have its own segment
    auto writer = open_writer(irs::kOmCreate, options);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    {
      auto ctx = writer->GetBatch();

      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc2->indexed.begin(), doc2->indexed.end()));
        CaptureNameLikeFields(doc, doc2->indexed);
      }
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc3->indexed.begin(), doc3->indexed.end()));
        CaptureNameLikeFields(doc, doc3->indexed);
      }
      ctx.Reset();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc4->indexed.begin(), doc4->indexed.end()));
        CaptureNameLikeFields(doc, doc4->indexed);
      }
      ctx.Commit();
    }

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of first segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of second segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc4
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // rollback removals
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    {
      auto ctx = writer->GetBatch();

      ctx.Remove(*(query_doc1));
      ctx.Reset();
    }

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // rollback removals + some more
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    {
      auto ctx = writer->GetBatch();

      ctx.Remove(*(query_doc1));
      ctx.Reset();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc2->indexed.begin(), doc2->indexed.end()));
        CaptureNameLikeFields(doc, doc2->indexed);
      }
      ctx.Commit();
    }

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // rollback removals split over multiple segment_writers
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto query_doc2 = MakeByTerm(kNameFieldId, "B");
    auto options = irs::tests::DefaultWriterOptions();
    options.segment_docs_max = 1;  // each doc will have its own segment
    auto writer = open_writer(irs::kOmCreate, options);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    {
      auto ctx = writer->GetBatch();

      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc2->indexed.begin(), doc2->indexed.end()));
        CaptureNameLikeFields(doc, doc2->indexed);
      }
      ctx.Remove(*(query_doc1));
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc3->indexed.begin(), doc3->indexed.end()));
        CaptureNameLikeFields(doc, doc3->indexed);
      }
      ctx.Remove(*(query_doc2));
      ctx.Reset();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc4->indexed.begin(), doc4->indexed.end()));
        CaptureNameLikeFields(doc, doc4->indexed);
      }
      ctx.Commit();
    }

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of first segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of second segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc4
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // rollback replace (single doc)
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    {
      auto ctx = writer->GetBatch();

      {
        auto doc = ctx.Replace(*(query_doc1));
        ASSERT_TRUE(doc.Insert(doc2->indexed.begin(), doc2->indexed.end()));
        CaptureNameLikeFields(doc, doc2->indexed);
      }
      ctx.Reset();
    }

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // rollback replace (single doc) + some more
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    {
      auto ctx = writer->GetBatch();

      {
        auto doc = ctx.Replace(*(query_doc1));
        ASSERT_TRUE(doc.Insert(doc2->indexed.begin(), doc2->indexed.end()));
        CaptureNameLikeFields(doc, doc2->indexed);
      }
      ctx.Reset();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc3->indexed.begin(), doc3->indexed.end()));
        CaptureNameLikeFields(doc, doc3->indexed);
      }
      ASSERT_TRUE(ctx.Commit());
    }

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc3
    ASSERT_FALSE(docs_itr->next());
  }

  // rollback flushed but not committed doc
  {
    auto options = irs::tests::DefaultWriterOptions();
    options.segment_docs_max = 2;
    auto writer = open_writer(irs::kOmCreate, options);
    {
      auto ctx = writer->GetBatch();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc1->indexed.begin(), doc1->indexed.end()));
        CaptureNameLikeFields(doc, doc1->indexed);
      }
      ctx.Commit();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc2->indexed.begin(), doc2->indexed.end()));
        CaptureNameLikeFields(doc, doc2->indexed);
      }
      // implicit flush
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc3->indexed.begin(), doc3->indexed.end()));
        CaptureNameLikeFields(doc, doc3->indexed);
      }
      ctx.Reset();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc4->indexed.begin(), doc4->indexed.end()));
        CaptureNameLikeFields(doc, doc4->indexed);
      }
      // implicit commit and flush
      ctx.Commit();
    }

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());
    EXPECT_EQ(3, reader.docs_count());
    EXPECT_EQ(2, reader.live_docs_count());

    {
      auto& segment = reader[0];  // assume 0 is id of first segment
      EXPECT_EQ(2, segment.docs_count());
      EXPECT_EQ(1, segment.live_docs_count());
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of second segment
      EXPECT_EQ(1, segment.docs_count());
      EXPECT_EQ(1, segment.live_docs_count());
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc4
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // rollback flushed but not committed doc
  {
    constexpr size_t kWordSize = 256;
    auto options = irs::tests::DefaultWriterOptions();
    options.segment_docs_max = kWordSize + 2;
    auto writer = open_writer(irs::kOmCreate, options);
    {
      auto ctx = writer->GetBatch();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc1->indexed.begin(), doc1->indexed.end()));
        CaptureNameLikeFields(doc, doc1->indexed);
      }
      ctx.Commit();
      for (size_t i = 0; i != kWordSize; ++i) {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc2->indexed.begin(), doc2->indexed.end()));
        CaptureNameLikeFields(doc, doc2->indexed);
      }
      ctx.Reset();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc1->indexed.begin(), doc1->indexed.end()));
        CaptureNameLikeFields(doc, doc1->indexed);
      }
      ctx.Commit();
    }

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    EXPECT_EQ(2 + kWordSize, reader.docs_count());
    EXPECT_EQ(2, reader.live_docs_count());

    {
      auto& segment = reader[0];  // assume 0 is id of first segment
      EXPECT_EQ(2 + kWordSize, segment.docs_count());
      EXPECT_EQ(2, segment.live_docs_count());
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      for (size_t i = 0; i != 2; ++i) {
        ASSERT_TRUE(docs_itr->next()) << i;
        // 'name' value in doc1
        ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()))
          << i;
      }
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // rollback replacements (single doc) split over multiple segment_writers
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto query_doc2 = MakeByTerm(kNameFieldId, "B");
    auto options = irs::tests::DefaultWriterOptions();
    options.segment_docs_max = 1;  // each doc will have its own segment
    auto writer = open_writer(irs::kOmCreate, options);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    {
      auto ctx = writer->GetBatch();

      {
        auto doc = ctx.Replace(*(query_doc1));
        ASSERT_TRUE(doc.Insert(doc2->indexed.begin(), doc2->indexed.end()));
        CaptureNameLikeFields(doc, doc2->indexed);
      }
      {
        auto doc = ctx.Replace(*(query_doc2));
        ASSERT_TRUE(doc.Insert(doc3->indexed.begin(), doc3->indexed.end()));
        CaptureNameLikeFields(doc, doc3->indexed);
      }
      ctx.Reset();
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc4->indexed.begin(), doc4->indexed.end()));
        CaptureNameLikeFields(doc, doc4->indexed);
      }
      ctx.Commit();
    }

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of first segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of second segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc4
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // segment flush due to memory bytes limit (same flush_context)
  {
    auto options = irs::tests::DefaultWriterOptions();
    options.segment_memory_max = 1;  // arbitaty size < 1 document (first doc
                                     // will always aquire a new SegmentWriter)
    auto writer = open_writer(irs::kOmCreate, options);

    {
      auto ctx = writer->GetBatch();

      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc1->indexed.begin(), doc1->indexed.end()));
        CaptureNameLikeFields(doc, doc1->indexed);
      }
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc2->indexed.begin(), doc2->indexed.end()));
        CaptureNameLikeFields(doc, doc2->indexed);
      }
      ctx.Commit();
    }

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of first segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of second segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // segment flush due to memory bytes limit (split over different
  // flush_contexts)
  {
    auto options = irs::tests::DefaultWriterOptions();
    options.segment_memory_max = 1;  // arbitaty size < 1 document (first doc
                                     // will always aquire a new SegmentWriter)
    auto writer = open_writer(irs::kOmCreate, options);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    /* FIXME TODO use below once segment_context will not block
       flush_all()
        {
          auto ctx = writer->GetBatch(); // will reuse
       segment_context from above

          {
            auto doc = ctx.Insert();
            ASSERT_TRUE(doc.Insert(doc2->indexed.begin(), doc2->indexed.end()));
            CaptureNameLikeFields(doc, doc2->indexed);
          }
          writer->RefreshCommit(); AssertSnapshotEquality(*writer);
          {
            auto doc = ctx.Insert();
            ASSERT_TRUE(doc.Insert(doc3->indexed.begin(), doc3->indexed.end()));
            CaptureNameLikeFields(doc, doc3->indexed);
          }
        }

        writer->RefreshCommit(); AssertSnapshotEquality(*writer);

        auto reader = irs::directory_reader::open(dir(), codec());
        ASSERT_EQ(3, reader.size());

        {
          auto& segment = reader[0]; // assume 0 is id of first
       segment const auto* column = segment.Column(kNameColumnId);
          ASSERT_NE(nullptr, column);
          auto values = column->iterator(irs::ColumnHint::kNormal);
          ASSERT_NE(nullptr, values);
          auto* actual_value = irs::get<irs::PayAttr>(*values);
          ASSERT_NE(nullptr, actual_value);
          auto terms = segment.field(kSameFieldId);
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
       segment const auto* column = segment.Column(kNameColumnId);
          ASSERT_NE(nullptr, column);
          auto values = column->iterator(irs::ColumnHint::kNormal);
          ASSERT_NE(nullptr, values);
          auto* actual_value = irs::get<irs::PayAttr>(*values);
          ASSERT_NE(nullptr, actual_value);
          auto terms = segment.field(kSameFieldId);
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
       segment const auto* column = segment.Column(kNameColumnId);
          ASSERT_NE(nullptr, column);
          auto values = column->iterator(irs::ColumnHint::kNormal);
          ASSERT_NE(nullptr, values);
          auto* actual_value = irs::get<irs::PayAttr>(*values);
          ASSERT_NE(nullptr, actual_value);
          auto terms = segment.field(kSameFieldId);
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
    auto options = irs::tests::DefaultWriterOptions();
    options.segment_docs_max = 1;  // each doc will have its own segment
    auto writer = open_writer(irs::kOmCreate, options);

    {
      auto ctx = writer->GetBatch();

      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc1->indexed.begin(), doc1->indexed.end()));
        CaptureNameLikeFields(doc, doc1->indexed);
      }
      {
        auto doc = ctx.Insert();
        ASSERT_TRUE(doc.Insert(doc2->indexed.begin(), doc2->indexed.end()));
        CaptureNameLikeFields(doc, doc2->indexed);
      }
      ctx.Commit();
    }

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of first segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of second segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // segment flush due to document count limit (split over different
  // flush_contexts)
  {
    auto options = irs::tests::DefaultWriterOptions();
    options.segment_docs_max = 1;  // each doc will have its own segment
    auto writer = open_writer(irs::kOmCreate, options);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    /* FIXME TODO use below once segment_context will not block
       flush_all()
        {
          auto ctx = writer->GetBatch(); // will reuse
       segment_context from above

          {
            auto doc = ctx.Insert();
            ASSERT_TRUE(doc.Insert(doc2->indexed.begin(), doc2->indexed.end()));
            CaptureNameLikeFields(doc, doc2->indexed);
          }
          writer->RefreshCommit(); AssertSnapshotEquality(*writer);
          {
            auto doc = ctx.Insert();
            ASSERT_TRUE(doc.Insert(doc3->indexed.begin(), doc3->indexed.end()));
            CaptureNameLikeFields(doc, doc3->indexed);
          }
        }

        writer->RefreshCommit(); AssertSnapshotEquality(*writer);

        auto reader = irs::directory_reader::open(dir(), codec());
        ASSERT_EQ(3, reader.size());

        {
          auto& segment = reader[0]; // assume 0 is id of first
       segment const auto* column = segment.Column(kNameColumnId);
          ASSERT_NE(nullptr, column);
          auto values = column->iterator(irs::ColumnHint::kNormal);
          ASSERT_NE(nullptr, values);
          auto* actual_value = irs::get<irs::PayAttr>(*values);
          ASSERT_NE(nullptr, actual_value);
          auto terms = segment.field(kSameFieldId);
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
       segment const auto* column = segment.Column(kNameColumnId);
          ASSERT_NE(nullptr, column);
          auto values = column->iterator(irs::ColumnHint::kNormal);
          ASSERT_NE(nullptr, values);
          auto* actual_value = irs::get<irs::PayAttr>(*values);
          ASSERT_NE(nullptr, actual_value);
          auto terms = segment.field(kSameFieldId);
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
       segment const auto* column = segment.Column(kNameColumnId);
          ASSERT_NE(nullptr, column);
          auto values = column->iterator(irs::ColumnHint::kNormal);
          ASSERT_NE(nullptr, values);
          auto* actual_value = irs::get<irs::PayAttr>(*values);
          ASSERT_NE(nullptr, actual_value);
          auto terms = segment.field(kSameFieldId);
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
}

TEST_P(IndexTestCase, get_term) {
  {
    tests::JsonDocGenerator gen(
      resource("simple_sequential.json"),
      [](tests::Document& doc, const std::string& name,
         const tests::JsonDocGenerator::JsonValue& data) {
        if (data.is_string()) {
          {
            auto f = std::make_shared<tests::StringField>(name, data.str);
            f->id = tests::FieldIdFor(name);
            doc.insert(std::move(f));
          }
        }
      });

    add_segment(gen);
  }

  auto reader = open_reader(irs::tests::DefaultReaderOptions());
  ASSERT_EQ(1, reader.size());
  auto& segment = (*reader)[0];
  auto* field = segment.field(kNameFieldId);
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
          {
            auto f = std::make_shared<tests::StringField>(name, data.str);
            f->id = tests::FieldIdFor(name);
            doc.insert(std::move(f));
          }
        }
      });

    add_segment(gen);
  }

  auto reader = open_reader(irs::tests::DefaultReaderOptions());
  ASSERT_EQ(1, reader.size());
  auto& segment = (*reader)[0];

  // no term
  {
    std::array<irs::doc_id_t, 10> docs{};
    auto begin = docs.begin();
    auto end = docs.end();
    auto* field = segment.field(kNameFieldId);
    ASSERT_NE(nullptr, field);
    const auto term = irs::ViewCast<irs::byte_type>("invalid"sv);
    auto acceptor = [&](irs::doc_id_t doc) {
      *begin++ = doc;
      return begin != end;
    };
    field->read_documents(term, acceptor);
    const auto size = std::distance(docs.begin(), begin);
    ASSERT_EQ(0, size);
    ASSERT_TRUE(
      std::all_of(docs.begin(), docs.end(), [](auto v) { return v == 0; }));
  }

  // singleton term
  {
    std::array<irs::doc_id_t, 10> docs{};
    auto begin = docs.begin();
    auto end = docs.end();
    auto* field = segment.field(kNameFieldId);
    ASSERT_NE(nullptr, field);
    const auto term = irs::ViewCast<irs::byte_type>("A"sv);
    auto acceptor = [&](irs::doc_id_t doc) {
      *begin++ = doc;
      return begin != end;
    };
    field->read_documents(term, acceptor);
    const auto size = std::distance(docs.begin(), begin);
    ASSERT_EQ(1, size);
    ASSERT_EQ(1, docs.front());
    ASSERT_TRUE(std::all_of(std::next(docs.begin(), size), docs.end(),
                            [](auto v) { return v == 0; }));
  }

  // singleton term declined by acceptor
  {
    size_t calls = 0;
    auto* field = segment.field(kNameFieldId);
    ASSERT_NE(nullptr, field);
    const auto term = irs::ViewCast<irs::byte_type>("A"sv);
    auto acceptor = [&](irs::doc_id_t doc) {
      calls++;
      return false;
    };
    field->read_documents(term, acceptor);
    ASSERT_EQ(1, calls);
  }

  // singleton term
  {
    std::array<irs::doc_id_t, 10> docs{};
    auto begin = docs.begin();
    auto end = docs.end();
    auto* field = segment.field(kNameFieldId);
    ASSERT_NE(nullptr, field);
    const auto term = irs::ViewCast<irs::byte_type>("C"sv);
    auto acceptor = [&](irs::doc_id_t doc) {
      *begin++ = doc;
      return begin != end;
    };
    field->read_documents(term, acceptor);
    const auto size = std::distance(docs.begin(), begin);
    ASSERT_EQ(1, size);
    ASSERT_EQ(3, docs.front());
    ASSERT_TRUE(std::all_of(std::next(docs.begin(), size), docs.end(),
                            [](auto v) { return v == 0; }));
  }

  // regular term
  {
    std::array<irs::doc_id_t, 10> docs{};
    auto begin = docs.begin();
    auto end = docs.end();
    auto* field = segment.field(kDuplicatedFieldId);
    ASSERT_NE(nullptr, field);
    const auto term = irs::ViewCast<irs::byte_type>("abcd"sv);
    auto acceptor = [&](irs::doc_id_t doc) {
      *begin++ = doc;
      return begin != end;
    };
    field->read_documents(term, acceptor);
    const auto size = std::distance(docs.begin(), begin);
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
    auto begin = docs.begin();
    auto end = docs.end();
    auto* field = segment.field(kDuplicatedFieldId);
    ASSERT_NE(nullptr, field);
    const auto term = irs::ViewCast<irs::byte_type>("abcd"sv);
    auto acceptor = [&](irs::doc_id_t doc) {
      *begin++ = doc;
      return begin != end;
    };
    field->read_documents(term, acceptor);
    const auto size = std::distance(docs.begin(), begin);
    ASSERT_EQ(3, size);
    ASSERT_EQ(1, docs[0]);
    ASSERT_EQ(5, docs[1]);
    ASSERT_EQ(11, docs[2]);
  }

  // regular term, nothing requested
  {
    size_t calls = 0;
    auto* field = segment.field(kDuplicatedFieldId);
    ASSERT_NE(nullptr, field);
    const auto term = irs::ViewCast<irs::byte_type>("abcd"sv);
    auto acceptor = [&](irs::doc_id_t doc) {
      calls++;
      return false;
    };
    field->read_documents(term, acceptor);
    // no calls after false returned
    ASSERT_EQ(1, calls);
  }

  // regular term acceptor filtered
  {
    std::array<irs::doc_id_t, 10> docs{};
    auto begin = docs.begin();
    auto end = docs.end();
    auto* field = segment.field(kDuplicatedFieldId);
    ASSERT_NE(nullptr, field);
    const auto term = irs::ViewCast<irs::byte_type>("abcd"sv);
    auto acceptor = [&](irs::doc_id_t doc) {
      if (doc != 1) {
        *begin++ = doc;
      }
      return begin != end;
    };
    field->read_documents(term, acceptor);
    const auto size = std::distance(docs.begin(), begin);
    ASSERT_EQ(5, size);
    ASSERT_EQ(5, docs[0]);
    ASSERT_EQ(11, docs[1]);
    ASSERT_EQ(21, docs[2]);
    ASSERT_EQ(27, docs[3]);
    ASSERT_EQ(31, docs[4]);
    ASSERT_TRUE(std::all_of(std::next(docs.begin(), size), docs.end(),
                            [](auto v) { return v == 0; }));
  }
}

TEST_P(IndexTestCase, doc_removal) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        {
          auto f = std::make_shared<tests::StringField>(name, data.str);
          f->id = tests::FieldIdFor(name);
          doc.insert(std::move(f));
        }
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
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // new segment: add + remove 1st (as reference)
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    tests::Remove(*writer, *(query_doc1.get()));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // new segment: add + remove 1st (as unique_ptr)
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    tests::Remove(*writer, std::move(query_doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // new segment: add + remove 1st (as shared_ptr)
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    tests::Remove(*writer, std::shared_ptr<irs::Filter>(std::move(query_doc1)));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // new segment: remove + add
  {
    auto query_doc2 = MakeByTerm(kNameFieldId, "B");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    tests::Remove(*writer, std::move(query_doc2));  // not present yet
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // new segment: add + remove + readd
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    tests::Remove(*writer, std::move(query_doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // new segment: add + remove, old segment: remove
  {
    auto query_doc2 = MakeByTerm(kNameFieldId, "B");
    auto query_doc3 = MakeByTerm(kNameFieldId, "C");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    tests::Remove(*writer, std::move(query_doc3));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);  // document mask with 'doc3' created
    tests::Remove(*writer, std::move(query_doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(
      *writer);  // new document mask with 'doc2','doc3' created

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // new segment: add + add, old segment: remove + remove + add
  {
    auto query_doc1_doc2 =
      MakeByTermOrByTerm(kNameFieldId, "A", kNameFieldId, "B");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    tests::Remove(*writer, std::move(query_doc1_doc2));
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc3
    ASSERT_FALSE(docs_itr->next());
  }

  // new segment: add, old segment: remove
  {
    auto query_doc2 = MakeByTerm(kNameFieldId, "B");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    tests::Remove(*writer, std::move(query_doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of old segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of new segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // new segment: add + remove, old segment: remove
  {
    auto query_doc1_doc3 =
      MakeByTermOrByTerm(kNameFieldId, "A", kNameFieldId, "C");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    tests::Remove(*writer, std::move(query_doc1_doc3));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of old segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of new segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc4
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // new segment: add + remove, old segment: add + remove old-old
  // segment: remove
  {
    auto query_doc2_doc6_doc9 =
      MakeOr({{kNameFieldId, "B"}, {kNameFieldId, "F"}, {kNameFieldId, "I"}});
    auto query_doc3_doc7 =
      MakeByTermOrByTerm(kNameFieldId, "C", kNameFieldId, "G");
    auto query_doc4 = MakeByTerm(kNameFieldId, "D");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));  // A
    ASSERT_TRUE(InsertWithName(*writer, *doc2));  // B
    ASSERT_TRUE(InsertWithName(*writer, *doc3));  // C
    ASSERT_TRUE(InsertWithName(*writer, *doc4));  // D
    tests::Remove(*writer, std::move(query_doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(InsertWithName(*writer, *doc5));  // E
    ASSERT_TRUE(InsertWithName(*writer, *doc6));  // F
    ASSERT_TRUE(InsertWithName(*writer, *doc7));  // G
    tests::Remove(*writer, std::move(query_doc3_doc7));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(InsertWithName(*writer, *doc8));  // H
    ASSERT_TRUE(InsertWithName(*writer, *doc9));  // I
    tests::Remove(*writer, std::move(query_doc2_doc6_doc9));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(3, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of old-old segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of old segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("E", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc5
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[2];  // assume 2 is id of new segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("H", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc8
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
        {
          auto f = std::make_shared<tests::StringField>(name, data.str);
          f->id = tests::FieldIdFor(name);
          doc.insert(std::move(f));
        }
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();
  const tests::Document* doc4 = gen.next();

  // another shitty case for update
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto query_doc2 = MakeByTerm(kNameFieldId, "B");
    auto query_doc3 = MakeByTerm(kNameFieldId, "C");
    auto query_doc4 = MakeByTerm(kNameFieldId, "D");
    auto query_doc5 = MakeByTerm(kNameFieldId, "E");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    auto trx1 = writer->GetBatch();
    auto trx2 = writer->GetBatch();
    auto trx3 = writer->GetBatch();
    auto trx4 = writer->GetBatch();

    {
      auto doc = trx1.Insert();
      doc.Insert(doc1->indexed.begin(), doc1->indexed.end());
      CaptureNameLikeFields(doc, doc1->indexed);
    }
    {
      auto doc = trx1.Insert();
      doc.Insert(doc2->indexed.begin(), doc2->indexed.end());
      CaptureNameLikeFields(doc, doc2->indexed);
      trx1.Remove(*query_doc2);
    }
    {
      auto doc = trx3.Insert();
      doc.Insert(doc2->indexed.begin(), doc2->indexed.end());
      CaptureNameLikeFields(doc, doc2->indexed);
    }
    {
      auto doc = trx4.Replace(*query_doc2);
      doc.Insert(doc3->indexed.begin(), doc3->indexed.end());
      CaptureNameLikeFields(doc, doc3->indexed);
    }
    {
      auto doc = trx2.Replace(*query_doc3);
      doc.Insert(doc4->indexed.begin(), doc4->indexed.end());
      CaptureNameLikeFields(doc, doc4->indexed);
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

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    EXPECT_EQ(reader.size(), 2);
    EXPECT_EQ(reader.live_docs_count(), 2);
  }

  // new segment update (as reference)
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(UpdateWithName(*writer, *(query_doc1.get()), *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // new segment update (as unique_ptr)
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(UpdateWithName(*writer, std::move(query_doc1), *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // new segment update (as shared_ptr)
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(UpdateWithName(
      *writer, std::shared_ptr<irs::Filter>(std::move(query_doc1)), *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // old segment update
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(UpdateWithName(*writer, std::move(query_doc1), *doc3));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of old segment
      auto terms = segment.field(kSameFieldId);
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of new segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // 3x updates (same segment)
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto query_doc2 = MakeByTerm(kNameFieldId, "B");
    auto query_doc3 = MakeByTerm(kNameFieldId, "C");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(UpdateWithName(*writer, std::move(query_doc1), *doc2));
    ASSERT_TRUE(UpdateWithName(*writer, std::move(query_doc2), *doc3));
    ASSERT_TRUE(UpdateWithName(*writer, std::move(query_doc3), *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc4
    ASSERT_FALSE(docs_itr->next());
  }

  // 3x updates (different segments)
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto query_doc2 = MakeByTerm(kNameFieldId, "B");
    auto query_doc3 = MakeByTerm(kNameFieldId, "C");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(UpdateWithName(*writer, std::move(query_doc1), *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(UpdateWithName(*writer, std::move(query_doc2), *doc3));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(UpdateWithName(*writer, std::move(query_doc3), *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc4
    ASSERT_FALSE(docs_itr->next());
  }

  // no matching documnts
  {
    auto query_doc2 = MakeByTerm(kNameFieldId, "B");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(UpdateWithName(*writer, std::move(query_doc2),
                               *doc2));  // non-existent document
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size()) << reader.live_docs_count();
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // update + delete (same segment)
  {
    auto query_doc2 = MakeByTerm(kNameFieldId, "B");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(UpdateWithName(*writer, *(query_doc2), *doc3));
    tests::Remove(*writer, *(query_doc2));  // remove no longer existent
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc3
    ASSERT_FALSE(docs_itr->next());
  }

  // update + delete (different segments)
  {
    auto query_doc2 = MakeByTerm(kNameFieldId, "B");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(UpdateWithName(*writer, *(query_doc2), *doc3));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    tests::Remove(*writer, *(query_doc2));  // remove no longer existent
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of old segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of new segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc3
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // delete + update (same segment)
  {
    auto query_doc2 = MakeByTerm(kNameFieldId, "B");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    tests::Remove(*writer, *(query_doc2));
    ASSERT_TRUE(UpdateWithName(*writer, *(query_doc2),
                               *doc3));  // update no longer existent
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // delete + update (different segments)
  {
    auto query_doc2 = MakeByTerm(kNameFieldId, "B");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    tests::Remove(*writer, *(query_doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(UpdateWithName(*writer, *(query_doc2),
                               *doc3));  // update no longer existent
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // delete + update then update (2nd - update of modified doc)
  // (same segment)
  {
    auto query_doc2 = MakeByTerm(kNameFieldId, "B");
    auto query_doc3 = MakeByTerm(kNameFieldId, "C");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    tests::Remove(*writer, *(query_doc2));
    ASSERT_TRUE(UpdateWithName(*writer, *(query_doc2), *doc3));
    ASSERT_TRUE(UpdateWithName(*writer, *(query_doc3), *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());
  }

  // delete + update then update (2nd - update of modified doc)
  // (different segments)
  {
    auto query_doc2 = MakeByTerm(kNameFieldId, "B");
    auto query_doc3 = MakeByTerm(kNameFieldId, "C");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    tests::Remove(*writer, *(query_doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(UpdateWithName(*writer, *(query_doc2), *doc3));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(UpdateWithName(*writer, *(query_doc3), *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
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
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");

    auto opts = irs::tests::DefaultWriterOptions();

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
      irs::IndexFeatures::Freq | irs::IndexFeatures::Norm;
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

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));   // index features subset
    ASSERT_FALSE(InsertWithName(*writer, *doc3));  // serializer returs false
    ASSERT_FALSE(InsertWithName(*writer, *doc4));  // index features differ
    ASSERT_FALSE(UpdateWithName(*writer, *(query_doc1.get()), *doc3));
    ASSERT_TRUE(writer->RefreshCommit());
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    ASSERT_EQ(5, segment.docs_count());
    ASSERT_EQ(2, segment.live_docs_count());
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
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
        auto field = std::make_shared<tests::StringField>(name, data.str);
        field->id = FieldIdFor(name);
        doc.insert(std::move(field));
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();
  const tests::Document* doc4 = gen.next();

  // add a reader with 1 segment no docs
  {
    irs::MemoryDirectory data_dir;
    auto data_writer = irs::IndexWriter::Make(
      data_dir, codec(), irs::kOmCreate, irs::tests::DefaultWriterOptions());
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    writer->RefreshCommit();
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

    data_writer->RefreshCommit();
    ASSERT_TRUE(writer->Import(irs::DirectoryReader(
      data_dir, codec(), irs::tests::DefaultReaderOptions())));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(0, reader.size());
    ASSERT_EQ(0, reader.docs_count());

    // insert a document and check the meta counter again
    {
      ASSERT_TRUE(InsertWithName(*writer, *doc1));
      writer->RefreshCommit();
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
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    irs::MemoryDirectory data_dir;
    auto data_writer = irs::IndexWriter::Make(
      data_dir, codec(), irs::kOmCreate, irs::tests::DefaultWriterOptions());
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    writer->RefreshCommit();
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

    ASSERT_TRUE(InsertWithName(*data_writer, *doc1));
    data_writer->RefreshCommit();
    tests::Remove(*data_writer, std::move(query_doc1));
    data_writer->RefreshCommit();
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);  // ensure the writer has an initial
                                      // completed state
    ASSERT_TRUE(writer->Import(irs::DirectoryReader(
      data_dir, codec(), irs::tests::DefaultReaderOptions())));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(0, reader.size());
    ASSERT_EQ(0, reader.docs_count());

    // insert a document and check the meta counter again
    {
      ASSERT_TRUE(InsertWithName(*writer, *doc1));
      writer->RefreshCommit();
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
    auto data_writer = irs::IndexWriter::Make(
      data_dir, codec(), irs::kOmCreate, irs::tests::DefaultWriterOptions());
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*data_writer, *doc1));
    ASSERT_TRUE(InsertWithName(*data_writer, *doc2));
    data_writer->RefreshCommit();
    ASSERT_TRUE(writer->Import(irs::DirectoryReader(
      data_dir, codec(), irs::tests::DefaultReaderOptions())));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    ASSERT_EQ(2, segment.docs_count());
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // add a reader with 1 sparse segment
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    irs::MemoryDirectory data_dir;
    auto data_writer = irs::IndexWriter::Make(
      data_dir, codec(), irs::kOmCreate, irs::tests::DefaultWriterOptions());
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*data_writer, *doc1));
    ASSERT_TRUE(InsertWithName(*data_writer, *doc2));
    tests::Remove(*data_writer, std::move(query_doc1));
    data_writer->RefreshCommit();
    ASSERT_TRUE(writer->Import(irs::DirectoryReader(
      data_dir, codec(), irs::tests::DefaultReaderOptions())));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    ASSERT_EQ(1, segment.docs_count());
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // add a reader with 2 full segments
  {
    irs::MemoryDirectory data_dir;
    auto data_writer = irs::IndexWriter::Make(
      data_dir, codec(), irs::kOmCreate, irs::tests::DefaultWriterOptions());
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*data_writer, *doc1));
    ASSERT_TRUE(InsertWithName(*data_writer, *doc2));
    data_writer->RefreshCommit();
    ASSERT_TRUE(InsertWithName(*data_writer, *doc3));
    ASSERT_TRUE(InsertWithName(*data_writer, *doc4));
    data_writer->RefreshCommit();
    ASSERT_TRUE(writer->Import(irs::DirectoryReader(
      data_dir, codec(), irs::tests::DefaultReaderOptions())));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    ASSERT_EQ(4, segment.docs_count());
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc3
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc4
    ASSERT_FALSE(docs_itr->next());
  }

  // add a reader with 2 sparse segments
  {
    auto query_doc2_doc3 = MakeOr({{kNameFieldId, "B"}, {kNameFieldId, "C"}});
    irs::MemoryDirectory data_dir;
    auto data_writer = irs::IndexWriter::Make(
      data_dir, codec(), irs::kOmCreate, irs::tests::DefaultWriterOptions());
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*data_writer, *doc1));
    ASSERT_TRUE(InsertWithName(*data_writer, *doc2));
    data_writer->RefreshCommit();
    ASSERT_TRUE(InsertWithName(*data_writer, *doc3));
    ASSERT_TRUE(InsertWithName(*data_writer, *doc4));
    tests::Remove(*data_writer, std::move(query_doc2_doc3));
    data_writer->RefreshCommit();
    ASSERT_TRUE(writer->Import(irs::DirectoryReader(
      data_dir, codec(), irs::tests::DefaultReaderOptions())));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    ASSERT_EQ(2, segment.docs_count());
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc4
    ASSERT_FALSE(docs_itr->next());
  }

  // add a reader with 2 mixed segments
  {
    auto query_doc4 = MakeByTerm(kNameFieldId, "D");
    irs::MemoryDirectory data_dir;
    auto data_writer = irs::IndexWriter::Make(
      data_dir, codec(), irs::kOmCreate, irs::tests::DefaultWriterOptions());
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*data_writer, *doc1));
    ASSERT_TRUE(InsertWithName(*data_writer, *doc2));
    data_writer->RefreshCommit();
    ASSERT_TRUE(InsertWithName(*data_writer, *doc3));
    ASSERT_TRUE(InsertWithName(*data_writer, *doc4));
    tests::Remove(*data_writer, std::move(query_doc4));
    data_writer->RefreshCommit();
    ASSERT_TRUE(writer->Import(irs::DirectoryReader(
      data_dir, codec(), irs::tests::DefaultReaderOptions())));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    ASSERT_EQ(3, segment.docs_count());
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc3
    ASSERT_FALSE(docs_itr->next());
  }

  // new: add + add + delete, old: import
  {
    auto query_doc2 = MakeByTerm(kNameFieldId, "B");
    irs::MemoryDirectory data_dir;
    auto data_writer = irs::IndexWriter::Make(
      data_dir, codec(), irs::kOmCreate, irs::tests::DefaultWriterOptions());
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*data_writer, *doc1));
    ASSERT_TRUE(InsertWithName(*data_writer, *doc2));
    data_writer->RefreshCommit();
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    tests::Remove(*writer,
                  std::move(query_doc2));  // should not match any documents
    ASSERT_TRUE(writer->Import(irs::DirectoryReader(
      data_dir, codec(), irs::tests::DefaultReaderOptions())));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of imported segment
      ASSERT_EQ(2, segment.docs_count());
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of original segment
      ASSERT_EQ(1, segment.docs_count());
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc3
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
        {
          auto f = std::make_shared<tests::StringField>(name, data.str);
          f->id = tests::FieldIdFor(name);
          doc.insert(std::move(f));
        }
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();
  const tests::Document* doc4 = gen.next();

  // initial state (1st segment 2 docs)
  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
  }

  // refreshable reader
  auto reader =
    irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());

  // validate state
  {
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
  }

  // modify state (delete doc2)
  {
    auto writer =
      open_writer(irs::kOmAppend, irs::tests::DefaultWriterOptions());
    auto query_doc2 = MakeByTerm(kNameFieldId, "B");

    tests::Remove(*writer, std::move(query_doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
  }
  // validate state pre/post refresh (existing segment changed)
  {
    ((void)1);  // for clang-format
    {
      ASSERT_EQ(1, reader.size());
      auto& segment = reader[0];  // assume 0 is id of first/only segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }

    {
      reader = reader.Reopen();
      ASSERT_EQ(1, reader.size());
      auto& segment = reader[0];  // assume 0 is id of first/only segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // modify state (2nd segment 2 docs)
  {
    auto writer =
      open_writer(irs::kOmAppend, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
  }

  // validate state pre/post refresh (new segment added)
  {
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = segment.mask(term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc1
    ASSERT_FALSE(docs_itr->next());

    reader = reader.Reopen();
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of first segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of second segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc3
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc4
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // modify state (delete doc1)
  {
    auto writer =
      open_writer(irs::kOmAppend, irs::tests::DefaultWriterOptions());
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");

    tests::Remove(*writer, std::move(query_doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
  }

  // validate state pre/post refresh (old segment removed)
  {
    ASSERT_EQ(2, reader.size());

    {
      auto& segment = reader[0];  // assume 0 is id of first segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    {
      auto& segment = reader[1];  // assume 1 is id of second segment
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr =
        segment.mask(term_itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc3
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc4
      ASSERT_FALSE(docs_itr->next());
    }

    reader = reader.Reopen();
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of second segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc3
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc4
    ASSERT_FALSE(docs_itr->next());
  }
}

TEST_P(IndexTestCase, reuse_segment_writer) {
  tests::JsonDocGenerator gen0(resource("serene_demo.json"),
                               &tests::GenericJsonFieldFactory);
  tests::JsonDocGenerator gen1(resource("simple_sequential.json"),
                               &tests::GenericJsonFieldFactory);
  auto writer = open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

  // populate initial 2 very small segments
  {
    auto& index_ref = const_cast<tests::index_t&>(index());
    index_ref.emplace_back();
    gen0.reset();
    write_segment(*writer, index_ref.back(), gen0);
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
  }

  {
    auto& index_ref = const_cast<tests::index_t&>(index());
    index_ref.emplace_back();
    gen1.reset();
    write_segment(*writer, index_ref.back(), gen1);
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
  }

  // populate initial small segment
  {
    auto& index_ref = const_cast<tests::index_t&>(index());
    index_ref.emplace_back();
    gen0.reset();
    write_segment(*writer, index_ref.back(), gen0);
    gen1.reset();
    write_segment(*writer, index_ref.back(), gen1);
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
  }

  // populate initial large segment
  {
    auto& index_ref = const_cast<tests::index_t&>(index());
    index_ref.emplace_back();

    for (size_t i = 100; i > 0; --i) {
      gen0.reset();
      write_segment(*writer, index_ref.back(), gen0);
      gen1.reset();
      write_segment(*writer, index_ref.back(), gen1);
    }

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
  }

  // populate and validate small segments in hopes of triggering SegmentWriter
  // reuse 10 iterations, although 2 should be enough since
  // index_wirter::flush_context_pool_.size() == 2
  for (size_t i = 10; i > 0; --i) {
    auto& index_ref = const_cast<tests::index_t&>(index());
    index_ref.emplace_back();

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

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
  }

  assert_index();

  // merge all segments
  {
    ASSERT_TRUE(writer->Compact(
      irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));
    writer->RefreshCommit();
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
        {
          auto f = std::make_shared<tests::StringField>(name, data.str);
          f->id = tests::FieldIdFor(name);
          doc.insert(std::move(f));
        }
      }
    });

  // document to add a system column not present in subsequent documents
  tests::Document doc0;

  // add 2 identical fields (without storing) to trigger non-default norm
  // value
  for (size_t i = 2; i; --i) {
    auto f = std::make_shared<tests::StringField>("test-field", "test-value",
                                                  irs::IndexFeatures::Norm);
    f->id = kTestFieldId;
    doc0.insert(std::move(f), true, false);
  }

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();

  auto opts = irs::tests::DefaultWriterOptions();

  auto writer = open_writer(irs::kOmCreate, opts);

  ASSERT_TRUE(Insert(*writer, doc0.indexed.begin(), doc0.indexed.end()));
  ASSERT_TRUE(InsertWithName(*writer, *doc1));
  ASSERT_TRUE(InsertWithName(*writer, *doc2));
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);

  std::unordered_set<std::string_view> expected_name = {"A", "B"};

  // validate segment
  auto reader =
    irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
  ASSERT_EQ(1, reader.size());
  auto& segment = reader[0];           // assume 0 is id of first/only segment
  ASSERT_EQ(3, segment.docs_count());  // total count of documents

  auto* field =
    segment.field(kTestFieldId);  // 'norm' column added by doc0 above
  ASSERT_NE(nullptr, field);

  ASSERT_TRUE(
    irs::IsSubsetOf(irs::IndexFeatures::Norm, field->meta().index_features));
  ASSERT_TRUE(irs::field_limits::valid(field->meta().norm));

  // Per-field norm column is exposed through the SubReader::norms()
  // provider (point reads only -- no iterator). Make sure the segment
  // actually surfaces it before we move on to the cs read-back path.
  ASSERT_NE(nullptr, segment.norms(field->meta().norm));

  const auto* column = segment.Column(kNameColumnId);
  ASSERT_NE(nullptr, column);
  irs::tests::BlobPointReader values{segment, *column};
  ASSERT_EQ(expected_name.size() + 1,
            segment.docs_count());  // total count of documents (+1 for doc0)
  auto terms = segment.field(kSameFieldId);
  ASSERT_NE(nullptr, terms);
  auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
  ASSERT_TRUE(term_itr->next());

  for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
       docs_itr->next();) {
    ASSERT_EQ(1,
              expected_name.erase(irs::tests::ReadStoredStr<std::string_view>(
                values, docs_itr->value())));
  }

  ASSERT_TRUE(expected_name.empty());
}

TEST_P(IndexTestCase, import_concurrent) {
  struct Store {
    Store(const irs::Format::ptr& codec)
      : dir(std::make_unique<irs::MemoryDirectory>()) {
      writer = irs::IndexWriter::Make(*dir, codec, irs::kOmCreate,
                                      irs::tests::DefaultWriterOptions());
      writer->RefreshCommit();
      reader =
        irs::DirectoryReader(*dir, nullptr, irs::tests::DefaultReaderOptions());
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
        {
          auto f = std::make_shared<tests::StringField>(name, data.str);
          f->id = tests::FieldIdFor(name);
          doc.insert(std::move(f));
        }

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

      ASSERT_TRUE(InsertWithName(*store.writer, *doc));
    }
    store.writer->RefreshCommit();
    store.reader = irs::DirectoryReader(*store.dir, nullptr,
                                        irs::tests::DefaultReaderOptions());
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
  irs::IndexWriter::ptr writer = irs::IndexWriter::Make(
    dir, codec(), irs::kOmCreate, irs::tests::DefaultWriterOptions());

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

  writer->RefreshCommit();  // commit changes

  auto reader =
    irs::DirectoryReader(dir, nullptr, irs::tests::DefaultReaderOptions());
  tests::AssertSnapshotEquality(writer->GetSnapshot(), reader);
  ASSERT_EQ(workers.size(), reader.size());
  ASSERT_EQ(names.size(), reader.docs_count());
  ASSERT_EQ(names.size(), reader.live_docs_count());

  size_t removed = 0;
  for (auto& segment : reader) {
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    while (docs_itr->next()) {
      ASSERT_EQ(1, names.erase(irs::tests::ReadStoredStr<std::string>(
                     values, docs_itr->value())));
      ++removed;
    }
    ASSERT_FALSE(docs_itr->next());
  }
  ASSERT_EQ(removed, reader.docs_count());
  ASSERT_TRUE(names.empty());
}

static void CompactRange(irs::Compaction& candidates,
                         const irs::CompactingSegments& segments,
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

TEST_P(IndexTestCase, concurrent_compaction) {
  auto writer =
    open_writer(dir(), irs::kOmCreate, irs::tests::DefaultWriterOptions());
  ASSERT_NE(nullptr, writer);

  std::set<std::string> names;
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [&names](tests::Document& doc, const std::string& name,
             const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        {
          auto f = std::make_shared<tests::StringField>(name, data.str);
          f->id = tests::FieldIdFor(name);
          doc.insert(std::move(f));
        }

        if (name == "name") {
          names.emplace(data.str.data, data.str.size);
        }
      }
    });

  // insert multiple small segments
  size_t size = 0;
  while (const auto* doc = gen.next()) {
    ASSERT_TRUE(InsertWithName(*writer, *doc));
    writer->RefreshCommit();
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
                        irs::Compaction& candidates,
                        const irs::IndexReader& reader,
                        const irs::CompactingSegments& segments) mutable {
          num_segments = reader.size();
          CompactRange(candidates, segments, reader, i, i + 2);
        };

        if (writer->Compact(policy)) {
          writer->RefreshCommit();
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

  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);

  auto reader = irs::DirectoryReader(this->dir(), codec(),
                                     irs::tests::DefaultReaderOptions());
  ASSERT_EQ(1, reader.size());

  ASSERT_EQ(names.size(), reader.docs_count());
  ASSERT_EQ(names.size(), reader.live_docs_count());

  size_t removed = 0;
  auto& segment = reader[0];
  const auto* column = segment.Column(kNameColumnId);
  ASSERT_NE(nullptr, column);
  irs::tests::BlobPointReader values{segment, *column};
  auto terms = segment.field(kSameFieldId);
  ASSERT_NE(nullptr, terms);
  auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
  ASSERT_TRUE(term_itr->next());
  auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
  while (docs_itr->next()) {
    ASSERT_EQ(1, names.erase(irs::tests::ReadStoredStr<std::string>(
                   values, docs_itr->value())));
    ++removed;
  }
  ASSERT_FALSE(docs_itr->next());

  ASSERT_EQ(removed, reader.docs_count());
  ASSERT_TRUE(names.empty());
}

TEST_P(IndexTestCase, concurrent_compaction_dedicated_commit) {
  auto writer =
    open_writer(dir(), irs::kOmCreate, irs::tests::DefaultWriterOptions());
  ASSERT_NE(nullptr, writer);

  std::set<std::string> names;
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [&names](tests::Document& doc, const std::string& name,
             const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        {
          auto f = std::make_shared<tests::StringField>(name, data.str);
          f->id = tests::FieldIdFor(name);
          doc.insert(std::move(f));
        }

        if (name == "name") {
          names.emplace(data.str.data, data.str.size);
        }
      }
    });

  // insert multiple small segments
  size_t size = 0;
  while (const auto* doc = gen.next()) {
    ASSERT_TRUE(InsertWithName(*writer, *doc));
    writer->RefreshCommit();
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
                        irs::Compaction& candidates,
                        const irs::IndexReader& reader,
                        const irs::CompactingSegments& segments) mutable {
          num_segments = reader.size();
          CompactRange(candidates, segments, reader, i, i + 2);
        };

        writer->Compact(policy);

        i = (i + 1) % num_segments;
      }
    }));
  }

  // add dedicated commit thread
  std::atomic<bool> shutdown(false);
  std::thread commit_thread([&]() {
    wait_for_all();

    while (!shutdown.load()) {
      writer->RefreshCommit();
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

  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);

  auto reader = irs::DirectoryReader(this->dir(), codec(),
                                     irs::tests::DefaultReaderOptions());
  ASSERT_EQ(1, reader.size());

  ASSERT_EQ(names.size(), reader.docs_count());
  ASSERT_EQ(names.size(), reader.live_docs_count());

  size_t removed = 0;
  auto& segment = reader[0];
  const auto* column = segment.Column(kNameColumnId);
  ASSERT_NE(nullptr, column);
  irs::tests::BlobPointReader values{segment, *column};
  auto terms = segment.field(kSameFieldId);
  ASSERT_NE(nullptr, terms);
  auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
  ASSERT_TRUE(term_itr->next());
  auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
  while (docs_itr->next()) {
    ASSERT_EQ(1, names.erase(irs::tests::ReadStoredStr<std::string>(
                   values, docs_itr->value())));
    ++removed;
  }
  ASSERT_FALSE(docs_itr->next());

  ASSERT_EQ(removed, reader.docs_count());
  ASSERT_TRUE(names.empty());
}

TEST_P(IndexTestCase, concurrent_compaction_two_phase_dedicated_commit) {
  auto writer =
    open_writer(dir(), irs::kOmCreate, irs::tests::DefaultWriterOptions());
  ASSERT_NE(nullptr, writer);

  std::set<std::string> names;
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [&names](tests::Document& doc, const std::string& name,
             const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        {
          auto f = std::make_shared<tests::StringField>(name, data.str);
          f->id = tests::FieldIdFor(name);
          doc.insert(std::move(f));
        }

        if (name == "name") {
          names.emplace(data.str.data, data.str.size);
        }
      }
    });

  // insert multiple small segments
  size_t size = 0;
  while (const auto* doc = gen.next()) {
    ASSERT_TRUE(InsertWithName(*writer, *doc));
    writer->RefreshCommit();
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
                        irs::Compaction& candidates,
                        const irs::IndexReader& meta,
                        const irs::CompactingSegments& segments) mutable {
          num_segments = meta.size();
          CompactRange(candidates, segments, meta, i, i + 2);
        };

        writer->Compact(policy);

        i = (i + 1) % num_segments;
      }
    }));
  }

  // add dedicated commit thread
  std::atomic<bool> shutdown(false);
  std::thread commit_thread([&]() {
    wait_for_all();

    while (!shutdown.load()) {
      writer->RefreshBegin();
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
      writer->RefreshCommit();
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

  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);

  auto reader = irs::DirectoryReader(this->dir(), codec(),
                                     irs::tests::DefaultReaderOptions());
  ASSERT_EQ(1, reader.size());

  ASSERT_EQ(names.size(), reader.docs_count());
  ASSERT_EQ(names.size(), reader.live_docs_count());

  size_t removed = 0;
  auto& segment = reader[0];
  const auto* column = segment.Column(kNameColumnId);
  ASSERT_NE(nullptr, column);
  irs::tests::BlobPointReader values{segment, *column};
  auto terms = segment.field(kSameFieldId);
  ASSERT_NE(nullptr, terms);
  auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
  ASSERT_TRUE(term_itr->next());
  auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
  while (docs_itr->next()) {
    ASSERT_EQ(1, names.erase(irs::tests::ReadStoredStr<std::string>(
                   values, docs_itr->value())));
    ++removed;
  }
  ASSERT_FALSE(docs_itr->next());

  ASSERT_EQ(removed, reader.docs_count());
  ASSERT_TRUE(names.empty());
}

TEST_P(IndexTestCase, concurrent_compaction_cleanup) {
  auto writer =
    open_writer(dir(), irs::kOmCreate, irs::tests::DefaultWriterOptions());
  ASSERT_NE(nullptr, writer);

  std::set<std::string> names;
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [&names](tests::Document& doc, const std::string& name,
             const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        {
          auto f = std::make_shared<tests::StringField>(name, data.str);
          f->id = tests::FieldIdFor(name);
          doc.insert(std::move(f));
        }

        if (name == "name") {
          names.emplace(data.str.data, data.str.size);
        }
      }
    });

  // insert multiple small segments
  size_t size = 0;
  while (const auto* doc = gen.next()) {
    ASSERT_TRUE(InsertWithName(*writer, *doc));
    writer->RefreshCommit();
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
        auto policy = [&](irs::Compaction& candidates,
                          const irs::IndexReader& reader,
                          const irs::CompactingSegments& segments) mutable {
          num_segments = reader.size();
          CompactRange(candidates, segments, reader, i, i + 2);
        };

        if (writer->Compact(policy)) {
          writer->RefreshCommit();
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

  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  irs::DirectoryCleaner::clean(dir());

  auto reader =
    irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
  ASSERT_EQ(1, reader.size());

  ASSERT_EQ(names.size(), reader.docs_count());
  ASSERT_EQ(names.size(), reader.live_docs_count());

  size_t removed = 0;
  auto& segment = reader[0];
  const auto* column = segment.Column(kNameColumnId);
  ASSERT_NE(nullptr, column);
  irs::tests::BlobPointReader values{segment, *column};
  auto terms = segment.field(kSameFieldId);
  ASSERT_NE(nullptr, terms);
  auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
  ASSERT_TRUE(term_itr->next());
  auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
  while (docs_itr->next()) {
    ASSERT_EQ(1, names.erase(irs::tests::ReadStoredStr<std::string>(
                   values, docs_itr->value())));
    ++removed;
  }
  ASSERT_FALSE(docs_itr->next());

  ASSERT_EQ(removed, reader.docs_count());
  ASSERT_TRUE(names.empty());
}

TEST_P(IndexTestCase, compact_single_segment) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        {
          auto f = std::make_shared<tests::StringField>(name, data.str);
          f->id = tests::FieldIdFor(name);
          doc.insert(std::move(f));
        }
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();

  constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                              irs::IndexFeatures::Pos |
                                              irs::IndexFeatures::Offs;

  std::vector<size_t> expected_compacting_segments;
  auto check_compacting_segments =
    [&expected_compacting_segments](
      irs::Compaction& /*candidates*/, const irs::IndexReader& reader,
      const irs::CompactingSegments& compacting_segments) {
      ASSERT_EQ(expected_compacting_segments.size(),
                compacting_segments.size());
      for (auto i : expected_compacting_segments) {
        auto& expected_compacting_segment = reader[i];
        ASSERT_TRUE(compacting_segments.contains(
          expected_compacting_segment.Meta().name));
      }
    };

  // single segment without deletes
  {
    auto writer =
      open_writer(dir(), irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(
      irs::index_utils::CompactionCount())));  // nothing to compact
    ASSERT_TRUE(
      writer->Compact(check_compacting_segments));  // check segments registered
                                                    // for compaction
    writer->RefreshCommit();
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
    auto writer =
      open_writer(dir(), irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    tests::Remove(*writer, *query_doc1);
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(
      2,
      irs::DirectoryCleaner::clean(dir()));  // segments_1 + stale segment meta
    ASSERT_EQ(1, irs::DirectoryReader(this->dir(), codec(),
                                      irs::tests::DefaultReaderOptions())
                   .size());

    // get number of files in 1st segment
    count = 0;
    dir().visit(get_number_of_files_in_segments);

    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(
      irs::index_utils::CompactionCount())));  // nothing to compact
    expected_compacting_segments = {
      0};  // expect first segment to be marked for compaction
    ASSERT_TRUE(
      writer->Compact(check_compacting_segments));  // check segments registered
                                                    // for compaction
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1 + count,
              irs::DirectoryCleaner::clean(dir()));  // +1 for segments_2

    // validate structure
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc2);
    tests::AssertIndex(this->dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(this->dir(), codec(),
                                       irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());

    // assume 0 is 'merged' segment
    {
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(1, segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }
  }
}

TEST_P(IndexTestCase, segment_compact_long_running) {
  // The new cs writes `<segment>.col` for every committed segment. Use the
  // third segment's expected `.col` file as the BlockingDirectory trigger;
  // the test gates compaction on its create().
  const auto blocker = std::string{"_3.col"};

  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        {
          auto f = std::make_shared<tests::StringField>(name, data.str);
          f->id = tests::FieldIdFor(name);
          doc.insert(std::move(f));
        }
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();
  const tests::Document* doc4 = gen.next();

  constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                              irs::IndexFeatures::Pos |
                                              irs::IndexFeatures::Offs;

  size_t count = 0;
  auto get_number_of_files_in_segments =
    [&count](std::string_view name) noexcept {
      count += size_t(name.size() && '_' == name[0]);
      return true;
    };

  // long running transaction
  {
    tests::BlockingDirectory dir(this->dir(), blocker);
    auto writer =
      open_writer(dir, irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));

    // segment 2
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir));  // segments_1

    // retrieve total number of segment files
    dir.visit(get_number_of_files_in_segments);

    // acquire directory lock, and block compaction
    dir.intermediate_commits_lock.lock();

    std::thread compaction_thread([&writer]() {
      // compact
      ASSERT_TRUE(writer->Compact(
        irs::index_utils::MakePolicy(irs::index_utils::CompactionCount{})));

      const std::vector<size_t> expected_compacting_segments{0, 1};
      auto check_compacting_segments =
        [&expected_compacting_segments](
          irs::Compaction& /*candidates*/, const irs::IndexReader& reader,
          const irs::CompactingSegments& compacting_segments) {
          ASSERT_EQ(expected_compacting_segments.size(),
                    compacting_segments.size());
          for (auto i : expected_compacting_segments) {
            const auto& expected_compacting_segment = reader[i];
            ASSERT_TRUE(compacting_segments.contains(
              expected_compacting_segment.Meta().name));
          }
        };
      // check segments registered for compaction
      ASSERT_TRUE(writer->Compact(check_compacting_segments));
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
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);                  // commit transaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));  // segments_2

    // add several segments in background
    // segment 4
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);                  // commit transaction
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir));  // segments_3

    dir.intermediate_commits_lock.unlock();  // finish compaction
    compaction_thread.join();  // wait for the compaction to complete

    // finished compaction holds a reference to segments_3
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);  // commit compaction

    // +1 for segments_4, +1 for segments_3
    ASSERT_EQ(1 + 1 + count, irs::DirectoryCleaner::clean(dir));

    // validate structure
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc3);
    expected.emplace_back();
    expected.back().insert(*doc4);
    expected.emplace_back();
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    tests::AssertIndex(this->dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(this->dir(), codec(),
                                       irs::tests::DefaultReaderOptions());
    ASSERT_EQ(3, reader.size());

    // assume 0 is 'segment 3'
    {
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(1, segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    // assume 1 is 'segment 4'
    {
      auto& segment = reader[1];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(1, segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    // assume 2 is merged segment
    {
      auto& segment = reader[2];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(2, segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // long running transaction + segment removal
  {
    SetUp();  // recreate directory
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");

    tests::BlockingDirectory dir(this->dir(), blocker);
    auto writer =
      open_writer(dir, irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));

    // retrieve total number of segment files
    count = 0;
    dir.visit(get_number_of_files_in_segments);

    // segment 2
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir));  // segments_1

    // acquire directory lock, and block compaction
    dir.intermediate_commits_lock.lock();

    std::thread compaction_thread([&writer]() {
      // compaction will fail because of
      ASSERT_FALSE(writer->Compact(irs::index_utils::MakePolicy(
        irs::index_utils::CompactionCount())));  // compact

      auto check_compacting_segments =
        [](irs::Compaction& /*candidates*/, const irs::IndexReader& /*meta*/,
           const irs::CompactingSegments& compacting_segments) {
          ASSERT_TRUE(compacting_segments.empty());
        };
      // check segments registered for compaction
      ASSERT_TRUE(writer->Compact(check_compacting_segments));
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
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    tests::Remove(*writer, *query_doc1);
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);                  // commit transaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));  // segments_2

    // add several segments in background
    // segment 4
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);                  // commit transaction
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir));  // segments_3

    dir.intermediate_commits_lock.unlock();  // finish compaction
    compaction_thread.join();  // wait for the compaction to complete
    ASSERT_EQ(2 * count - 1 + 1,
              irs::DirectoryCleaner::clean(
                dir));  // files from segment 1 and 3 (without segment meta)
                        // + segments_3
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);  // commit compaction
    ASSERT_EQ(0,
              irs::DirectoryCleaner::clean(dir));  // compaction failed

    // validate structure
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc2);
    expected.emplace_back();
    expected.back().insert(*doc3);
    expected.emplace_back();
    expected.back().insert(*doc4);
    tests::AssertIndex(this->dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(this->dir(), codec(),
                                       irs::tests::DefaultReaderOptions());
    ASSERT_EQ(3, reader.size());

    // assume 0 is 'segment 2'
    {
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(1, segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    // assume 1 is 'segment 3'
    {
      auto& segment = reader[1];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(1, segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_FALSE(docs_itr->next());
    }

    // assume 1 is 'segment 4'
    {
      auto& segment = reader[2];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(1, segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // long running transaction + document removal
  {
    SetUp();  // recreate directory
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");

    tests::BlockingDirectory dir(this->dir(), blocker);
    auto writer =
      open_writer(dir, irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));

    // segment 2
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir));  // segments_1

    // retrieve total number of segment files
    count = 0;
    dir.visit(get_number_of_files_in_segments);

    // acquire directory lock, and block compaction
    dir.intermediate_commits_lock.lock();

    std::thread compaction_thread([&writer]() {
      // compaction will fail because of
      ASSERT_TRUE(writer->Compact(
        irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));

      const std::vector<size_t> expected_compacting_segments{0, 1};
      auto check_compacting_segments =
        [&expected_compacting_segments](
          irs::Compaction& /*candidates*/, const irs::IndexReader& reader,
          const irs::CompactingSegments& compacting_segments) {
          ASSERT_EQ(expected_compacting_segments.size(),
                    compacting_segments.size());
          for (auto i : expected_compacting_segments) {
            const auto& expected_compacting_segment = reader[i];
            ASSERT_TRUE(compacting_segments.contains(
              expected_compacting_segment.Meta().name));
          }
        };
      // check segments registered for compaction
      ASSERT_TRUE(writer->Compact(check_compacting_segments));
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
    tests::Remove(*writer, *query_doc1);
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);  // commit transaction
    // No unused column store: a remove-only batch no longer creates a `.col`.
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));

    dir.intermediate_commits_lock.unlock();  // finish compaction
    compaction_thread.join();  // wait for the compaction to complete
    // Compaction still holds a reference to the snapshot segment_2
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);  // commit compaction
    // files from segments 1 and 2, segment_3
    // segment_2 + stale segment 1 meta
    ASSERT_EQ(count + 2 + 1, irs::DirectoryCleaner::clean(dir));

    // validate structure (does not take removals into account)
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    expected.back().insert(*doc3);
    tests::AssertIndex(this->dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(this->dir(), codec(),
                                       irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());

    // assume 0 is 'merged segment'
    {
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(3, segment.docs_count());       // total count of documents
      ASSERT_EQ(2, segment.live_docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      // including deleted docs
      {
        irs::tests::BlobPointReader values{segment, *column};

        auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc1
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc2
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc3
        ASSERT_FALSE(docs_itr->next());
      }

      // only live docs
      {
        irs::tests::BlobPointReader values{segment, *column};

        auto docs_itr =
          segment.mask(term_itr->postings(irs::IndexFeatures::None));
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc2
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc3
        ASSERT_FALSE(docs_itr->next());
      }
    }
  }

  // long running transaction + document removal
  {
    SetUp();  // recreate directory
    auto query_doc1_doc4 =
      MakeByTermOrByTerm(kNameFieldId, "A", kNameFieldId, "D");

    tests::BlockingDirectory dir(this->dir(), blocker);
    auto writer =
      open_writer(dir, irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));

    // segment 2
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir));  // segments_1

    // retrieve total number of segment files
    count = 0;
    dir.visit(get_number_of_files_in_segments);

    dir.intermediate_commits_lock
      .lock();  // acquire directory lock, and block compaction

    std::thread compaction_thread([&writer]() {
      // compaction will fail because of
      ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(
        irs::index_utils::CompactionCount())));  // compact

      const std::vector<size_t> expected_compacting_segments{0, 1};
      auto check_compacting_segments =
        [&expected_compacting_segments](
          irs::Compaction& /*candidates*/, const irs::IndexReader& reader,
          const irs::CompactingSegments& compacting_segments) {
          ASSERT_EQ(expected_compacting_segments.size(),
                    compacting_segments.size());
          for (auto i : expected_compacting_segments) {
            const auto& expected_compacting_segment = reader[i];
            ASSERT_TRUE(compacting_segments.contains(
              expected_compacting_segment.Meta().name));
          }
        };
      // check segments registered for compaction
      ASSERT_TRUE(writer->Compact(check_compacting_segments));
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
    tests::Remove(*writer, *query_doc1_doc4);
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);  // commit transaction
    // No unused column store: a remove-only batch no longer creates a `.col`.
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));

    dir.intermediate_commits_lock.unlock();  // finish compaction
    compaction_thread.join();  // wait for the compaction to complete
    // compaction still holds a reference to the snapshot pointed by
    // segments_2
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);  // commit compaction

    // files from segments 1 and 2,
    // segment_3
    // segment_2 + stale segment 1 meta + stale segment 2 meta
    ASSERT_EQ(count + 3 + 1, irs::DirectoryCleaner::clean(dir));

    // validate structure (does not take removals into account)
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    expected.back().insert(*doc3);
    expected.back().insert(*doc4);
    tests::AssertIndex(this->dir(), codec(), expected, kAllFeatures);

    auto reader = irs::DirectoryReader(this->dir(), codec(),
                                       irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());

    // assume 0 is 'merged segment'
    {
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(4, segment.docs_count());       // total count of documents
      ASSERT_EQ(2, segment.live_docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      // including deleted docs
      {
        irs::tests::BlobPointReader values{segment, *column};

        auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc1
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc2
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc3
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc4
        ASSERT_FALSE(docs_itr->next());
      }

      // only live docs
      {
        irs::tests::BlobPointReader values{segment, *column};

        auto docs_itr =
          segment.mask(term_itr->postings(irs::IndexFeatures::None));
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc2
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc3
        ASSERT_FALSE(docs_itr->next());
      }
    }
  }
}

TEST_P(IndexTestCase, segment_compact_clear_commit) {
  std::vector<size_t> expected_compacting_segments;
  auto check_compacting_segments =
    [&expected_compacting_segments](
      irs::Compaction& /*candidates*/, const irs::IndexReader& reader,
      const irs::CompactingSegments& compacting_segments) {
      ASSERT_EQ(expected_compacting_segments.size(),
                compacting_segments.size());
      for (auto i : expected_compacting_segments) {
        const auto& expected_compacting_segment = reader[i];
        ASSERT_TRUE(compacting_segments.contains(
          expected_compacting_segment.Meta().name));
      }
    };

  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        {
          auto f = std::make_shared<tests::StringField>(name, data.str);
          f->id = tests::FieldIdFor(name);
          doc.insert(std::move(f));
        }
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();

  // 2-phase: clear + compact
  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // segment 2
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // segment 3
    ASSERT_TRUE(InsertWithName(*writer, *doc3));

    writer->RefreshBegin();
    writer->Clear();
    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(
      irs::index_utils::CompactionCount())));  // compact
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);  // commit transaction

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(0, reader.size());
  }

  // 2-phase: compact + clear
  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // segment 2
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // segment 3
    ASSERT_TRUE(InsertWithName(*writer, *doc3));

    writer->RefreshBegin();
    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(
      irs::index_utils::CompactionCount())));  // compact
    writer->Clear();
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);  // commit transaction

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(0, reader.size());
  }

  // compact + clear
  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // segment 2
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(
      irs::index_utils::CompactionCount())));  // compact

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    writer->Clear();

    // check compacting segments
    expected_compacting_segments = {};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);  // commit transaction

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(0, reader.size());
  }

  // clear + compact
  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // segment 2
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    writer->Clear();
    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(
      irs::index_utils::CompactionCount())));  // compact

    // check compacting segments
    expected_compacting_segments = {};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);  // commit transaction

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(0, reader.size());
  }
}

TEST_P(IndexTestCase, segment_compact_commit) {
  std::vector<size_t> expected_compacting_segments;
  auto check_compacting_segments =
    [&expected_compacting_segments](
      irs::Compaction& /*candidates*/, const irs::IndexReader& reader,
      const irs::CompactingSegments& compacting_segments) {
      ASSERT_EQ(expected_compacting_segments.size(),
                compacting_segments.size());
      for (auto i : expected_compacting_segments) {
        const auto& expected_compacting_segment = reader[i];
        ASSERT_TRUE(compacting_segments.contains(
          expected_compacting_segment.Meta().name));
      }
    };

  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        {
          auto f = std::make_shared<tests::StringField>(name, data.str);
          f->id = tests::FieldIdFor(name);
          doc.insert(std::move(f));
        }
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();
  const tests::Document* doc4 = gen.next();
  const tests::Document* doc5 = gen.next();

  constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                              irs::IndexFeatures::Pos |
                                              irs::IndexFeatures::Offs;

  size_t count = 0;
  auto get_number_of_files_in_segments =
    [&count](std::string_view name) noexcept {
      count += size_t(name.size() && '_' == name[0]);
      return true;
    };

  // compact without deletes
  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // segment 2
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(
      irs::index_utils::CompactionCount())));  // compact

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // all segments are already marked for compaction
    ASSERT_FALSE(writer->Compact(
      irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    writer->RefreshCommit();
    AssertSnapshotEquality(
      *writer);  // commit transaction (will commit nothing)
    ASSERT_EQ(1 + count, irs::DirectoryCleaner::clean(
                           dir()));  // +1 for corresponding segments_* file
    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(
      irs::index_utils::CompactionCount())));  // nothing to compact
    writer->RefreshCommit();
    AssertSnapshotEquality(
      *writer);  // commit transaction (will commit nothing)

    // validate structure
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());

    // assume 0 is merged segment
    {
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(2, segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // compact without deletes
  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // segment 2
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    // segment 3
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(
      irs::index_utils::CompactionCount())));  // compact

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    // can't compact segments that are already marked for compaction
    ASSERT_FALSE(writer->Compact(
      irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);  // commit transaction (will commit segment
                                      // 3 + compaction)
    ASSERT_EQ(1 + count,
              irs::DirectoryCleaner::clean(dir()));  // +1 for segments_*

    // validate structure
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    expected.emplace_back();
    expected.back().insert(*doc3);
    expected.back().insert(*doc4);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());

    // assume 0 is merged segment
    {
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(2, segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }

    // assume 1 is the newly created segment (doc3+doc4)
    {
      auto& segment = reader[1];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(2, segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc3
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc4
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // compact without deletes
  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // segment 2
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    // segment 3
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));  // segments_1
    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(
      irs::index_utils::CompactionCount())));  // compact

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    // can't compact segments that are already marked for compaction
    ASSERT_FALSE(writer->Compact(
      irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));  // segments_1

    ASSERT_TRUE(InsertWithName(*writer, *doc5));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));  // segments_1
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);  // commit transaction (will commit segment
                                      // 3 + compaction)
    ASSERT_EQ(count + 1,
              irs::DirectoryCleaner::clean(dir()));  // +1 for segments_*

    // validate structure
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    expected.emplace_back();
    expected.back().insert(*doc3);
    expected.back().insert(*doc4);
    expected.back().insert(*doc5);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader.size());

    // assume 0 is merged segment
    {
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(2, segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }

    // assume 1 is the newly crated segment
    {
      auto& segment = reader[1];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(3, segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc3
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc4
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("E", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc4
      ASSERT_FALSE(docs_itr->next());
    }
  }
}

TEST_P(IndexTestCase, compact_check_compacting_segments) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        {
          auto f = std::make_shared<tests::StringField>(name, data.str);
          f->id = tests::FieldIdFor(name);
          doc.insert(std::move(f));
        }
      }
    });

  auto writer = open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
  ASSERT_NE(nullptr, writer);

  // ensure compacting segments is empty
  {
    auto check_compacting_segments =
      [](irs::Compaction& /*candidates*/, const irs::IndexReader& /*reader*/,
         const irs::CompactingSegments& compacting_segments) {
        ASSERT_TRUE(compacting_segments.empty());
      };
    ASSERT_TRUE(writer->Compact(check_compacting_segments));
  }

  const size_t segments_count = 10;
  for (size_t i = 0; i < segments_count; ++i) {
    const auto* doc = gen.next();
    ASSERT_TRUE(InsertWithName(*writer, *doc));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
  }

  // register 'SEGMENTS_COUNT/2' compactions
  for (size_t i = 0, j = 0; i < segments_count / 2; ++i) {
    auto merge_adjacent =
      [&j](irs::Compaction& candidates, const irs::IndexReader& reader,
           const irs::CompactingSegments& /*compacting_segments*/) {
        ASSERT_TRUE(j < reader.size());
        candidates.emplace_back(&reader[j++]);
        ASSERT_TRUE(j < reader.size());
        candidates.emplace_back(&reader[j++]);
      };

    ASSERT_TRUE(writer->Compact(merge_adjacent));
  }

  // check all segments registered
  {
    auto check_compacting_segments =
      [](irs::Compaction& /*candidates*/, const irs::IndexReader& reader,
         const irs::CompactingSegments& compacting_segments) {
        ASSERT_EQ(reader.size(), compacting_segments.size());
        for (auto& expected_compacting_segment : reader) {
          ASSERT_TRUE(compacting_segments.contains(
            expected_compacting_segment.Meta().name));
        }
      };
    ASSERT_TRUE(writer->Compact(check_compacting_segments));
  }

  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);  // commit pending compactions

  // ensure compacting segments is empty
  {
    auto check_compacting_segments =
      [](irs::Compaction& /*candidates*/, const irs::IndexReader& /*reader*/,
         const irs::CompactingSegments& compacting_segments) {
        ASSERT_TRUE(compacting_segments.empty());
      };
    ASSERT_TRUE(writer->Compact(check_compacting_segments));
  }

  // validate structure
  constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                              irs::IndexFeatures::Pos |
                                              irs::IndexFeatures::Offs;
  gen.reset();
  tests::index_t expected;
  for (size_t i = 0; i < segments_count / 2; ++i) {
    expected.emplace_back();
    const auto* doc = gen.next();
    expected.back().insert(*doc);
    doc = gen.next();
    expected.back().insert(*doc);
  }
  tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

  auto reader =
    irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
  ASSERT_EQ(segments_count / 2, reader.size());

  std::string expected_name = "A";

  for (size_t i = 0; i < segments_count / 2; ++i) {
    auto& segment = reader[i];
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    ASSERT_EQ(2, segment.docs_count());  // total count of documents
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(expected_name,
              irs::tests::ReadStoredStr<std::string_view>(
                values, docs_itr->value()));  // 'name' value in doc1
    ++expected_name[0];
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(expected_name,
              irs::tests::ReadStoredStr<std::string_view>(
                values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_FALSE(docs_itr->next());
    ++expected_name[0];
  }
}

TEST_P(IndexTestCase, segment_compact_pending_commit) {
  std::vector<size_t> expected_compacting_segments;
  auto check_compacting_segments =
    [&expected_compacting_segments](
      irs::Compaction& /*candidates*/, const irs::IndexReader& reader,
      const irs::CompactingSegments& compacting_segments) {
      ASSERT_EQ(expected_compacting_segments.size(),
                compacting_segments.size());
      for (auto i : expected_compacting_segments) {
        const auto& expected_compacting_segment = reader[i];
        ASSERT_TRUE(compacting_segments.contains(
          expected_compacting_segment.Meta().name));
      }
    };

  auto check_compacting_segments_name_only =
    [&expected_compacting_segments](
      irs::Compaction& /*candidates*/, const irs::IndexReader& reader,
      const irs::CompactingSegments& compacting_segments) {
      ASSERT_EQ(expected_compacting_segments.size(),
                compacting_segments.size());
      for (auto i : expected_compacting_segments) {
        const auto& expected_compacting_segment = reader[i];
        ASSERT_TRUE(compacting_segments.contains(
          expected_compacting_segment.Meta().name));
      }
    };

  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        {
          auto f = std::make_shared<tests::StringField>(name, data.str);
          f->id = tests::FieldIdFor(name);
          doc.insert(std::move(f));
        }
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();
  const tests::Document* doc4 = gen.next();
  const tests::Document* doc5 = gen.next();
  const tests::Document* doc6 = gen.next();

  constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                              irs::IndexFeatures::Pos |
                                              irs::IndexFeatures::Offs;

  size_t count = 0;
  auto get_number_of_files_in_segments =
    [&count](std::string_view name) noexcept {
      count += size_t(name.size() && '_' == name[0]);
      return true;
    };

  // compact without deletes
  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // segment 2
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    ASSERT_FALSE(
      writer
        ->RefreshBegin());  // begin transaction (will not start transaction)
    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(
      irs::index_utils::CompactionCount())));  // compact

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    // all segments are already marked for compaction
    ASSERT_FALSE(writer->Compact(
      irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    writer->RefreshCommit();
    AssertSnapshotEquality(
      *writer);  // commit transaction (will commit compaction)
    ASSERT_EQ(1 + count, irs::DirectoryCleaner::clean(
                           dir()));  // +1 for corresponding segments_* file

    // check compacting segments
    expected_compacting_segments = {};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(
      irs::index_utils::CompactionCount())));  // nothing to compact

    // check compacting segments
    expected_compacting_segments = {};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    writer->RefreshCommit();
    AssertSnapshotEquality(
      *writer);  // commit transaction (will commit nothing)

    // validate structure
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(2, reader.live_docs_count());

    // assume 0 is merged segment
    {
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(2, segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // compact without deletes
  {
    SetUp();
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // segment 2
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    // segment 3
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));

    ASSERT_TRUE(writer->RefreshBegin());  // begin transaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // check compacting segments
    expected_compacting_segments = {};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(
      irs::index_utils::CompactionCount())));  // compact

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    // can't compact segments that are already marked for compaction
    ASSERT_FALSE(writer->Compact(
      irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    writer->RefreshCommit();
    AssertSnapshotEquality(
      *writer);  // commit transaction (will commit segment 3)

    // writer still holds a reference to segments_2
    // because it's under compaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);  // commit pending merge
    ASSERT_EQ(1 + 1 + count,
              irs::DirectoryCleaner::clean(dir()));  // segments_2

    // check compacting segments
    expected_compacting_segments = {};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    // validate structure
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc3);
    expected.back().insert(*doc4);
    expected.emplace_back();
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(4, reader.live_docs_count());

    // assume 0 is the existing segment
    {
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(2, segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc3
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc4
      ASSERT_FALSE(docs_itr->next());
    }

    // assume 1 is merged segment
    {
      auto& segment = reader[1];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(2, segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // compact without deletes
  {
    SetUp();
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // segment 2
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    // segment 3
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    ASSERT_TRUE(writer->RefreshBegin());  // begin transaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // check compacting segments
    expected_compacting_segments = {};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(
      irs::index_utils::CompactionCount())));  // compact

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    // can't compact segments that are already marked for compaction
    ASSERT_FALSE(writer->Compact(
      irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    ASSERT_TRUE(InsertWithName(*writer, *doc5));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    writer->RefreshCommit();
    AssertSnapshotEquality(
      *writer);  // commit transaction (will commit segment 3)

    // writer still holds a reference to segments_3
    // because it's under compaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    ASSERT_TRUE(InsertWithName(*writer, *doc6));

    writer->RefreshCommit();
    AssertSnapshotEquality(
      *writer);               // commit pending merge, segment 4 (doc5 + doc6)
    ASSERT_EQ(count + 1 + 1,  // +1 for segments_3
              irs::DirectoryCleaner::clean(dir()));  // +1 for segments

    // check compacting segments
    expected_compacting_segments = {};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    // validate structure
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc3);
    expected.back().insert(*doc4);
    expected.emplace_back();
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    expected.emplace_back();
    expected.back().insert(*doc5);
    expected.back().insert(*doc6);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(3, reader.size());
    ASSERT_EQ(6, reader.live_docs_count());

    // assume 0 is the existing segment
    {
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(2, segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc3
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc4
      ASSERT_FALSE(docs_itr->next());
    }

    // assume 1 is merged segment
    {
      auto& segment = reader[1];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(2, segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }

    // assume 2 is the last added segment
    {
      auto& segment = reader[2];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(2, segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("E", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("F", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }
  }

  // compact with deletes
  {
    SetUp();
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");

    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // segment 2
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    tests::Remove(*writer, *query_doc1);
    ASSERT_TRUE(writer->RefreshBegin());  // begin transaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // check compacting segments
    expected_compacting_segments = {};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(
      irs::index_utils::CompactionCount())));  // compact

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    // can't compact segments that are already marked for compaction
    ASSERT_FALSE(writer->Compact(
      irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    writer->RefreshCommit();
    AssertSnapshotEquality(
      *writer);  // commit transaction (will commit removal)

    // writer still holds a reference to segments_2
    // because it's under compaction
    // No unused column store: a remove-only batch no longer creates a `.col`.
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // check compacting segments
    expected_compacting_segments = {0, 1};

    // Check name only because of removals
    ASSERT_TRUE(writer->Compact(check_compacting_segments_name_only));

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);  // commit pending merge
    ASSERT_EQ(count + 2 + 1,  // +2 for  segments_2 + stale segment 1 meta
              irs::DirectoryCleaner::clean(dir()));  // +1 for segments

    // check compacting segments
    expected_compacting_segments = {};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    // validate structure (doesn't take removals into account)
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    expected.back().insert(*doc3);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(2, reader.live_docs_count());

    // assume 0 is merged segment
    {
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(3, segment.docs_count());       // total count of documents
      ASSERT_EQ(2, segment.live_docs_count());  // total count of live documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      // with deleted docs
      {
        irs::tests::BlobPointReader values{segment, *column};

        auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc3
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc3
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc4
        ASSERT_FALSE(docs_itr->next());
      }

      // without deleted docs
      {
        irs::tests::BlobPointReader values{segment, *column};

        auto docs_itr =
          segment.mask(term_itr->postings(irs::IndexFeatures::None));
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc3
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc4
        ASSERT_FALSE(docs_itr->next());
      }
    }
  }

  // compact with deletes
  {
    SetUp();
    auto query_doc1_doc4 =
      MakeByTermOrByTerm(kNameFieldId, "A", kNameFieldId, "D");

    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // segment 2
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    tests::Remove(*writer, *query_doc1_doc4);
    ASSERT_TRUE(writer->RefreshBegin());  // begin transaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // check compacting segments
    expected_compacting_segments = {};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(
      irs::index_utils::CompactionCount())));  // compact

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    // can't compact segments that are already marked for compaction
    ASSERT_FALSE(writer->Compact(
      irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    writer->RefreshCommit();
    // commit transaction (will commit removal)
    AssertSnapshotEquality(*writer);
    // No unused column store: a remove-only batch no longer creates a `.col`.
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments_name_only));

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);  // commit pending merge

    // segments_2 + stale segment 1 meta + stale segment 2 meta +1 for segments,
    ASSERT_EQ(count + 4, irs::DirectoryCleaner::clean(dir()));

    // check compacting segments
    expected_compacting_segments = {};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    // validate structure (doesn't take removals into account)
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    expected.back().insert(*doc3);
    expected.back().insert(*doc4);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(2, reader.live_docs_count());

    // assume 0 is merged segment
    {
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(4, segment.docs_count());       // total count of documents
      ASSERT_EQ(2, segment.live_docs_count());  // total count of live documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      // with deleted docs
      {
        irs::tests::BlobPointReader values{segment, *column};

        auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc3
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc3
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc4
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc4
        ASSERT_FALSE(docs_itr->next());
      }

      // without deleted docs
      {
        irs::tests::BlobPointReader values{segment, *column};

        auto docs_itr =
          segment.mask(term_itr->postings(irs::IndexFeatures::None));
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc3
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc4
        ASSERT_FALSE(docs_itr->next());
      }
    }
  }

  // compact with delete committed and pending
  {
    SetUp();
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto query_doc4 = MakeByTerm(kNameFieldId, "D");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // segment 2
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    tests::Remove(*writer, *query_doc1);
    ASSERT_TRUE(writer->RefreshBegin());  // begin transaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // check compacting segments
    expected_compacting_segments = {};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    auto do_commit_and_compact_count =
      [&](irs::Compaction& candidates, const irs::IndexReader& reader,
          const irs::CompactingSegments& compacting_segments) {
        auto sub_policy =
          irs::index_utils::MakePolicy(irs::index_utils::CompactionCount());
        sub_policy(candidates, reader, compacting_segments);
        writer->RefreshCommit();
        AssertSnapshotEquality(*writer);
      };

    ASSERT_TRUE(writer->Compact(do_commit_and_compact_count));

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments_name_only));

    // can't compact segments that are already marked for compaction
    ASSERT_FALSE(writer->Compact(
      irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));

    tests::Remove(*writer, *query_doc4);

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);  // commit pending merge + delete
    ASSERT_EQ(count + 4, irs::DirectoryCleaner::clean(dir()));

    // check compacting segments
    expected_compacting_segments = {};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    // validate structure (doesn't take removals into account)
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    expected.back().insert(*doc3);
    expected.back().insert(*doc4);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);
    {
      auto reader = irs::DirectoryReader(dir(), codec(),
                                         irs::tests::DefaultReaderOptions());
      ASSERT_TRUE(reader);
      ASSERT_EQ(1, reader.size());
      ASSERT_EQ(2, reader.live_docs_count());
      // assume 0 is merged segment
      {
        auto& segment = reader[0];
        const auto* column = segment.Column(kNameColumnId);
        ASSERT_NE(nullptr, column);
        ASSERT_EQ(4, segment.docs_count());  // total count of documents
        ASSERT_EQ(2,
                  segment.live_docs_count());  // total count of live documents
        auto terms = segment.field(kSameFieldId);
        ASSERT_NE(nullptr, terms);
        auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
        ASSERT_TRUE(term_itr->next());

        // with deleted docs
        {
          irs::tests::BlobPointReader values{segment, *column};

          auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
          ASSERT_TRUE(docs_itr->next());
          ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                           values, docs_itr->value()));  // 'name' value in doc3
          ASSERT_TRUE(docs_itr->next());
          ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                           values, docs_itr->value()));  // 'name' value in doc3
          ASSERT_TRUE(docs_itr->next());
          ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                           values, docs_itr->value()));  // 'name' value in doc4
          ASSERT_TRUE(docs_itr->next());
          ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                           values, docs_itr->value()));  // 'name' value in doc4
          ASSERT_FALSE(docs_itr->next());
        }

        // without deleted docs
        {
          irs::tests::BlobPointReader values{segment, *column};

          auto docs_itr =
            segment.mask(term_itr->postings(irs::IndexFeatures::None));
          ASSERT_TRUE(docs_itr->next());
          ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                           values, docs_itr->value()));  // 'name' value in doc3
          ASSERT_TRUE(docs_itr->next());
          ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                           values, docs_itr->value()));  // 'name' value in doc4
          ASSERT_FALSE(docs_itr->next());
        }
      }
    }

    // check for dangling old segment versions in writers cache
    // first create new segment
    // segment 5
    ASSERT_TRUE(InsertWithName(*writer, *doc5));
    ASSERT_TRUE(InsertWithName(*writer, *doc6));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // remove one doc from new and old segment to make conolidation do
    // something
    auto query_doc3 = MakeByTerm(kNameFieldId, "C");
    auto query_doc5 = MakeByTerm(kNameFieldId, "E");
    tests::Remove(*writer, *query_doc3);
    tests::Remove(*writer, *query_doc5);
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    ASSERT_TRUE(writer->Compact(
      irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // check all old segments are deleted (no old version of segments is
    // left in cache and blocking )
    ASSERT_EQ(count + 3,
              irs::DirectoryCleaner::clean(dir()));  // +3 segment files
  }

  // repeatable compaction of already compacted segment
  {
    SetUp();
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    // segment 2
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // check compacting segments
    expected_compacting_segments = {};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    tests::Remove(*writer, *query_doc1);
    ASSERT_TRUE(writer->RefreshBegin());  // begin transaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // this compaction will be postponed
    ASSERT_TRUE(writer->Compact(
      irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));
    // check compacting segments are pending
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    // can't compact segments that are already marked for compaction
    ASSERT_FALSE(writer->Compact(
      irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));

    auto do_commit_and_compact_count =
      [&](irs::Compaction& candidates, const irs::IndexReader& reader,
          const irs::CompactingSegments& compacting_segments) {
        writer->RefreshCommit();
        AssertSnapshotEquality(*writer);
        writer->RefreshBegin();  // another commit to process pending
                                 // compacting_segments
        writer->RefreshCommit();
        AssertSnapshotEquality(*writer);
        auto sub_policy =
          irs::index_utils::MakePolicy(irs::index_utils::CompactionCount());
        sub_policy(candidates, reader, compacting_segments);
      };

    // this should fail as segments 1 and 0 are actually compacted on
    // previous  commit inside our test policy
    ASSERT_FALSE(writer->Compact(do_commit_and_compact_count));
    ASSERT_NE(0, irs::DirectoryCleaner::clean(dir()));
    // check all data is deleted
    const auto one_segment_count = count;
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));
    // files count should be the same as with one segment
    ASSERT_EQ(one_segment_count, count);
  }

  // repeatable compaction of already compacted segment during two
  // phase commit
  {
    SetUp();
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto query_doc4 = MakeByTerm(kNameFieldId, "D");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    // segment 2
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // check compacting segments
    expected_compacting_segments = {};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    tests::Remove(*writer, *query_doc1);
    ASSERT_TRUE(writer->RefreshBegin());  // begin transaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // this compaction will be postponed
    ASSERT_TRUE(writer->Compact(
      irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));
    // check compacting segments are pending
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    // can't compact segments that are already marked for compaction
    ASSERT_FALSE(writer->Compact(
      irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));

    auto do_commit_and_compact_count =
      [&](irs::Compaction& candidates, const irs::IndexReader& reader,
          const irs::CompactingSegments& compacting_segments) {
        writer->RefreshCommit();
        AssertSnapshotEquality(*writer);
        writer->RefreshBegin();  // another commit to process pending
                                 // compacting_segments
        writer->RefreshCommit();
        AssertSnapshotEquality(*writer);
        // new transaction with passed 1st phase
        tests::Remove(*writer, *query_doc4);
        writer->RefreshBegin();
        auto sub_policy =
          irs::index_utils::MakePolicy(irs::index_utils::CompactionCount());
        sub_policy(candidates, reader, compacting_segments);
      };

    // this should fail as segments 1 and 0 are actually compacted on
    // previous  commit inside our test policy
    ASSERT_FALSE(writer->Compact(do_commit_and_compact_count));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_NE(0, irs::DirectoryCleaner::clean(dir()));
    // check all data is deleted
    const auto one_segment_count = count;
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));
    // files count should be the same as with one segment
    ASSERT_EQ(one_segment_count, count);
  }

  // check commit rollback and compaction
  {
    SetUp();
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // segment 2
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));

    tests::Remove(*writer, *query_doc1);
    ASSERT_TRUE(writer->RefreshBegin());  // begin transaction
    // this compaction will be postponed
    ASSERT_TRUE(writer->Compact(
      irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));
    // check compacting segments are pending
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    writer->RefreshAbort();

    // leftovers cleanup (aborted compaction segment no longer emits a `.col`)
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));

    // still pending
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    tests::Remove(*writer, *query_doc1);
    // make next commit
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // now no compacting should be present
    expected_compacting_segments = {};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    // could compact successfully
    ASSERT_TRUE(writer->Compact(
      irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));

    // cleanup should remove old files
    ASSERT_NE(0, irs::DirectoryCleaner::clean(dir()));

    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    expected.back().insert(*doc3);
    expected.back().insert(*doc4);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());  // should be one compacted segment
    ASSERT_EQ(3, reader.live_docs_count());
  }

  // compact with deletes + inserts
  {
    SetUp();
    auto query_doc1_doc4 =
      MakeByTermOrByTerm(kNameFieldId, "A", kNameFieldId, "D");

    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // segment 2
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    tests::Remove(*writer, *query_doc1_doc4);
    ASSERT_TRUE(writer->RefreshBegin());  // begin transaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // check compacting segments
    expected_compacting_segments = {};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(
      irs::index_utils::CompactionCount())));  // compact

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    // can't compact segments that are already marked for compaction
    ASSERT_FALSE(writer->Compact(
      irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    ASSERT_TRUE(InsertWithName(*writer, *doc5));
    writer->RefreshCommit();
    // commit transaction (will commit removal)
    AssertSnapshotEquality(*writer);
    // No unused column store: a remove-only batch no longer creates a `.col`.
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments_name_only));

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);  // commit pending merge

    // segments_2 + stale segment 1 meta + stale segment 2 meta +1 for segments,
    ASSERT_EQ(count + 4, irs::DirectoryCleaner::clean(dir()));

    // check compacting segments
    expected_compacting_segments = {};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    // validate structure (doesn't take removals into account)
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    expected.back().insert(*doc3);
    expected.back().insert(*doc4);
    expected.emplace_back();
    expected.back().insert(*doc5);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(3, reader.live_docs_count());

    // assume 0 is merged segment
    {
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(4, segment.docs_count());       // total count of documents
      ASSERT_EQ(2, segment.live_docs_count());  // total count of live documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      // with deleted docs
      {
        irs::tests::BlobPointReader values{segment, *column};

        auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc1
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc2
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc3
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc4
        ASSERT_FALSE(docs_itr->next());
      }

      // without deleted docs
      {
        irs::tests::BlobPointReader values{segment, *column};

        auto docs_itr =
          segment.mask(term_itr->postings(irs::IndexFeatures::None));
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc3
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc4
        ASSERT_FALSE(docs_itr->next());
      }
    }

    // assume 1 is the recently added segment
    {
      auto& segment = reader[1];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of live documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("E", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
    }
  }

  // compact with deletes + inserts
  {
    SetUp();
    auto query_doc1_doc4 =
      MakeByTermOrByTerm(kNameFieldId, "A", kNameFieldId, "D");

    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // segment 2
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    ASSERT_TRUE(InsertWithName(*writer, *doc5));

    // check compacting segments
    expected_compacting_segments = {};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    ASSERT_TRUE(writer->RefreshBegin());  // begin transaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(
      irs::index_utils::CompactionCount())));  // compact

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    // can't compact segments that are already marked for compaction
    ASSERT_FALSE(writer->Compact(
      irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);  // commit transaction

    // writer still holds a reference to segments_2
    // because it's under compaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));  // segments_2

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    tests::Remove(*writer, *query_doc1_doc4);
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);  // commit pending merge + removal

    // +1 for segments,
    // +1 for segment 1 meta,
    // +1 for segment 2 meta,
    // +1 for segments_2
    ASSERT_EQ(count + 4, irs::DirectoryCleaner::clean(dir()));

    // check compacting segments
    expected_compacting_segments = {};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    // validate structure (doesn't take removals into account)
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc5);
    expected.emplace_back();
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    expected.back().insert(*doc3);
    expected.back().insert(*doc4);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(3, reader.live_docs_count());

    // assume 1 is the recently added segment
    {
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of live documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("E", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
    }

    // assume 0 is merged segment
    {
      auto& segment = reader[1];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(4, segment.docs_count());       // total count of documents
      ASSERT_EQ(2, segment.live_docs_count());  // total count of live documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      // with deleted docs
      {
        irs::tests::BlobPointReader values{segment, *column};

        auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc1
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc2
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc3
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc4
        ASSERT_FALSE(docs_itr->next());
      }

      // without deleted docs
      {
        irs::tests::BlobPointReader values{segment, *column};

        auto docs_itr =
          segment.mask(term_itr->postings(irs::IndexFeatures::None));
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc3
        ASSERT_TRUE(docs_itr->next());
        ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                         values, docs_itr->value()));  // 'name' value in doc4
        ASSERT_FALSE(docs_itr->next());
      }
    }
  }

  // compact with deletes + inserts
  {
    SetUp();
    auto query_doc3_doc4 = MakeOr({{kNameFieldId, "C"}, {kNameFieldId, "D"}});

    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    // segment 1
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // segment 2
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_EQ(1, irs::DirectoryCleaner::clean(dir()));  // segments_1

    size_t num_files_segment_2 = count;
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));
    num_files_segment_2 = count - num_files_segment_2;

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));
    ASSERT_TRUE(InsertWithName(*writer, *doc5));

    // check compacting segments
    expected_compacting_segments = {};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    ASSERT_TRUE(writer->RefreshBegin());  // begin transaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    // check compacting segments
    expected_compacting_segments = {};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    // count number of files in segments
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));

    ASSERT_TRUE(writer->Compact(
      irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    // can't compact segments that are already marked for compaction
    ASSERT_FALSE(writer->Compact(
      irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    size_t num_files_compaction_segment = count;
    count = 0;
    ASSERT_TRUE(dir().visit(get_number_of_files_in_segments));
    num_files_compaction_segment = count - num_files_compaction_segment;

    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);  // commit transaction

    // writer still holds a reference to segments_2
    // because it's under compaction
    ASSERT_EQ(0, irs::DirectoryCleaner::clean(dir()));  // segments_2

    // check compacting segments
    expected_compacting_segments = {0, 1};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    tests::Remove(*writer, *query_doc3_doc4);

    // commit pending merge + removal
    // pending compaction will fail (because segment 2 will have no live
    // docs after applying removals)
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // check compacting segments
    expected_compacting_segments = {};
    ASSERT_TRUE(writer->Compact(check_compacting_segments));

    // +1 for segments_2, +1 for segments_2
    ASSERT_EQ(num_files_compaction_segment + num_files_segment_2 + 1 + 1,
              irs::DirectoryCleaner::clean(dir()));

    // validate structure (doesn't take removals into account)
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc1);
    expected.back().insert(*doc2);
    expected.emplace_back();
    expected.back().insert(*doc5);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(3, reader.live_docs_count());

    // assume 0 is first segment
    {
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(2, segment.docs_count());       // total count of documents
      ASSERT_EQ(2, segment.live_docs_count());  // total count of live documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("A", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc2
      ASSERT_FALSE(docs_itr->next());
    }

    // assume 1 is the recently added segment
    {
      auto& segment = reader[1];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(1, segment.docs_count());       // total count of documents
      ASSERT_EQ(1, segment.live_docs_count());  // total count of live documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ("E", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs_itr->value()));  // 'name' value in doc1
    }
  }
}

TEST_P(IndexTestCase, compact_progress) {
  tests::JsonDocGenerator gen(TestBase::resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);
  auto* doc1 = gen.next();
  auto* doc2 = gen.next();
  auto policy =
    irs::index_utils::MakePolicy(irs::index_utils::CompactionCount());

  // test default progress (false)
  {
    irs::MemoryDirectory dir;
    auto writer = irs::IndexWriter::Make(dir, get_codec(), irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();  // create segment0
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, get_codec(),
                           irs::tests::DefaultReaderOptions()));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();  // create segment1

    auto reader = writer->GetSnapshot();
    tests::AssertSnapshotEquality(
      reader, irs::DirectoryReader(dir, get_codec(),
                                   irs::tests::DefaultReaderOptions()));

    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(1, reader[0].docs_count());
    ASSERT_EQ(1, reader[1].docs_count());

    irs::MergeWriter::FlushProgress progress;

    ASSERT_TRUE(writer->Compact(policy, get_codec(), progress));
    writer->RefreshCommit();  // write compacted segment
    reader = irs::DirectoryReader(dir, get_codec(),
                                  irs::tests::DefaultReaderOptions());

    tests::AssertSnapshotEquality(writer->GetSnapshot(), reader);

    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(2, reader[0].docs_count());
  }

  // test always-false progress
  {
    irs::MemoryDirectory dir;
    auto writer = irs::IndexWriter::Make(dir, get_codec(), irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();  // create segment0
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, get_codec(),
                           irs::tests::DefaultReaderOptions()));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();  // create segment1

    auto reader = writer->GetSnapshot();
    tests::AssertSnapshotEquality(
      reader, irs::DirectoryReader(dir, get_codec(),
                                   irs::tests::DefaultReaderOptions()));

    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(1, reader[0].docs_count());
    ASSERT_EQ(1, reader[1].docs_count());

    irs::MergeWriter::FlushProgress progress = []() -> bool { return false; };

    ASSERT_FALSE(writer->Compact(policy, get_codec(), progress));
    writer->RefreshCommit();  // write compacted segment
    reader = writer->GetSnapshot();
    tests::AssertSnapshotEquality(
      reader, irs::DirectoryReader(dir, get_codec(),
                                   irs::tests::DefaultReaderOptions()));

    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(1, reader[0].docs_count());
    ASSERT_EQ(1, reader[1].docs_count());
  }

  size_t progress_call_count = 0;

  const size_t max_docs = 32768;

  // test always-true progress
  {
    irs::MemoryDirectory dir;
    auto writer = irs::IndexWriter::Make(dir, get_codec(), irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());

    for (size_t size = 0; size < max_docs; ++size) {
      ASSERT_TRUE(InsertWithName(*writer, *doc1));
    }
    writer->RefreshCommit();  // create segment0
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, get_codec(),
                           irs::tests::DefaultReaderOptions()));

    for (size_t size = 0; size < max_docs; ++size) {
      ASSERT_TRUE(InsertWithName(*writer, *doc2));
    }
    writer->RefreshCommit();  // create segment1
    auto reader = irs::DirectoryReader(dir, get_codec(),
                                       irs::tests::DefaultReaderOptions());
    tests::AssertSnapshotEquality(writer->GetSnapshot(), reader);

    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(max_docs, reader[0].docs_count());
    ASSERT_EQ(max_docs, reader[1].docs_count());

    irs::MergeWriter::FlushProgress progress =
      [&progress_call_count]() -> bool {
      ++progress_call_count;
      return true;
    };

    ASSERT_TRUE(writer->Compact(policy, get_codec(), progress));
    writer->RefreshCommit();  // write compacted segment
    reader = writer->GetSnapshot();
    tests::AssertSnapshotEquality(
      reader, irs::DirectoryReader(dir, get_codec(),
                                   irs::tests::DefaultReaderOptions()));

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
    auto writer = irs::IndexWriter::Make(dir, get_codec(), irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    for (size_t size = 0; size < max_docs; ++size) {
      ASSERT_TRUE(InsertWithName(*writer, *doc1));
    }
    writer->RefreshCommit();  // create segment0
    tests::AssertSnapshotEquality(
      writer->GetSnapshot(),
      irs::DirectoryReader(dir, get_codec(),
                           irs::tests::DefaultReaderOptions()));

    for (size_t size = 0; size < max_docs; ++size) {
      ASSERT_TRUE(InsertWithName(*writer, *doc2));
    }
    writer->RefreshCommit();  // create segment0
    auto reader = irs::DirectoryReader(dir, get_codec(),
                                       irs::tests::DefaultReaderOptions());
    tests::AssertSnapshotEquality(writer->GetSnapshot(), reader);

    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(max_docs, reader[0].docs_count());
    ASSERT_EQ(max_docs, reader[1].docs_count());

    irs::MergeWriter::FlushProgress progress = [&call_count]() -> bool {
      return --call_count;
    };

    ASSERT_FALSE(writer->Compact(policy, get_codec(), progress));
    writer->RefreshCommit();  // write compacted segment

    reader = irs::DirectoryReader(dir, get_codec(),
                                  irs::tests::DefaultReaderOptions());
    tests::AssertSnapshotEquality(writer->GetSnapshot(), reader);

    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(max_docs, reader[0].docs_count());
    ASSERT_EQ(max_docs, reader[1].docs_count());
  }
}

TEST_P(IndexTestCase, segment_compact) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        {
          auto f = std::make_shared<tests::StringField>(name, data.str);
          f->id = tests::FieldIdFor(name);
          doc.insert(std::move(f));
        }
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();
  const tests::Document* doc4 = gen.next();
  const tests::Document* doc5 = gen.next();
  const tests::Document* doc6 = gen.next();

  auto always_merge =
    irs::index_utils::MakePolicy(irs::index_utils::CompactionCount());
  constexpr irs::IndexFeatures kAllFeatures = irs::IndexFeatures::Freq |
                                              irs::IndexFeatures::Pos |
                                              irs::IndexFeatures::Offs;

  // remove empty new segment
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    tests::Remove(*writer, std::move(query_doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(0, reader.size());
  }

  // remove empty old segment
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    tests::Remove(*writer, std::move(query_doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(0, reader.size());
  }

  // remove empty old, defragment new
  {
    auto query_doc1_doc2 =
      MakeByTermOrByTerm(kNameFieldId, "A", kNameFieldId, "B");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    tests::Remove(*writer, std::move(query_doc1_doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    ASSERT_TRUE(writer->Compact(always_merge));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // validate structure
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc3);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    ASSERT_EQ(1, segment.docs_count());  // total count of documents
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc3
    ASSERT_FALSE(docs_itr->next());
  }

  // remove empty old, defragment new
  {
    auto query_doc1_doc2 =
      MakeByTermOrByTerm(kNameFieldId, "A", kNameFieldId, "B");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    tests::Remove(*writer, std::move(query_doc1_doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(writer->Compact(always_merge));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // validate structure
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc3);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    ASSERT_EQ(1, segment.docs_count());  // total count of documents
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc3
    ASSERT_FALSE(docs_itr->next());
  }

  // remove empty old, defragment old
  {
    auto query_doc1_doc2 =
      MakeByTermOrByTerm(kNameFieldId, "A", kNameFieldId, "B");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    tests::Remove(*writer, std::move(query_doc1_doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(writer->Compact(always_merge));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // validate structure
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc3);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];           // assume 0 is id of first/only segment
    ASSERT_EQ(1, segment.docs_count());  // total count of documents
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc3
    ASSERT_FALSE(docs_itr->next());
  }

  // remove empty old, defragment old
  {
    auto query_doc1_doc2 =
      MakeByTermOrByTerm(kNameFieldId, "A", kNameFieldId, "B");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    tests::Remove(*writer, std::move(query_doc1_doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(writer->Compact(always_merge));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // validate structure
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc3);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];           // assume 0 is id of first/only segment
    ASSERT_EQ(1, segment.docs_count());  // total count of documents
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("C", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc3
    ASSERT_FALSE(docs_itr->next());
  }

  auto merge_if_masked = [](irs::Compaction& candidates,
                            const irs::IndexReader& reader,
                            const irs::CompactingSegments&) -> void {
    for (auto& segment : reader) {
      if (segment.Meta().live_docs_count != segment.Meta().docs_count) {
        candidates.emplace_back(&segment);
      }
    }
  };

  // do defragment old segment with uncommited removal (i.e. do not consider
  // uncomitted removals)
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    tests::Remove(*writer, std::move(query_doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(writer->Compact(merge_if_masked));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    {
      auto reader = irs::DirectoryReader(dir(), codec(),
                                         irs::tests::DefaultReaderOptions());
      ASSERT_EQ(1, reader.size());
      auto& segment = reader[0];  // assume 0 is id of first/only segment
      ASSERT_EQ(1, segment.docs_count());  // total count of documents
    }
  }

  // do not defragment old segment with uncommited removal (i.e. do not
  // consider uncomitted removals)
  {
    auto query_doc1 = MakeByTerm(kNameFieldId, "A");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    tests::Remove(*writer, std::move(query_doc1));
    ASSERT_TRUE(writer->Compact(merge_if_masked));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    {
      auto reader = irs::DirectoryReader(dir(), codec(),
                                         irs::tests::DefaultReaderOptions());
      ASSERT_EQ(1, reader.size());
      auto& segment = reader[0];  // assume 0 is id of first/only segment
      ASSERT_EQ(2, segment.docs_count());  // total count of documents
    }

    ASSERT_TRUE(writer->Compact(
      merge_if_masked));  // previous removal now committed and considered
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    {
      auto reader = irs::DirectoryReader(dir(), codec(),
                                         irs::tests::DefaultReaderOptions());
      ASSERT_EQ(1, reader.size());
      auto& segment = reader[0];  // assume 0 is id of first/only segment
      ASSERT_EQ(1, segment.docs_count());  // total count of documents
    }
  }

  // merge new+old segment
  {
    auto query_doc1_doc3 =
      MakeByTermOrByTerm(kNameFieldId, "A", kNameFieldId, "C");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    tests::Remove(*writer, std::move(query_doc1_doc3));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(writer->Compact(always_merge));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // validate structure
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc2);
    expected.back().insert(*doc4);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];           // assume 0 is id of first/only segment
    ASSERT_EQ(2, segment.docs_count());  // total count of documents
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc4
    ASSERT_FALSE(docs_itr->next());
  }

  // merge new+old segment
  {
    auto query_doc1_doc3 =
      MakeByTermOrByTerm(kNameFieldId, "A", kNameFieldId, "C");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    tests::Remove(*writer, std::move(query_doc1_doc3));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(writer->Compact(always_merge));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // validate structure
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc2);
    expected.back().insert(*doc4);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];           // assume 0 is id of first/only segment
    ASSERT_EQ(2, segment.docs_count());  // total count of documents
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc4
    ASSERT_FALSE(docs_itr->next());
  }

  // merge old+old segment
  {
    auto query_doc1_doc3 =
      MakeByTermOrByTerm(kNameFieldId, "A", kNameFieldId, "C");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    tests::Remove(*writer, std::move(query_doc1_doc3));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(writer->Compact(always_merge));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // validate structure
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc2);
    expected.back().insert(*doc4);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];           // assume 0 is id of first/only segment
    ASSERT_EQ(2, segment.docs_count());  // total count of documents
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc4
    ASSERT_FALSE(docs_itr->next());
  }

  // merge old+old segment
  {
    auto query_doc1_doc3 =
      MakeByTermOrByTerm(kNameFieldId, "A", kNameFieldId, "C");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    tests::Remove(*writer, std::move(query_doc1_doc3));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(writer->Compact(always_merge));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // validate structure
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc2);
    expected.back().insert(*doc4);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];           // assume 0 is id of first/only segment
    ASSERT_EQ(2, segment.docs_count());  // total count of documents
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc4
    ASSERT_FALSE(docs_itr->next());
  }

  // merge old+old+old segment
  {
    auto query_doc1_doc3_doc5 =
      MakeOr({{kNameFieldId, "A"}, {kNameFieldId, "C"}, {kNameFieldId, "E"}});
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(InsertWithName(*writer, *doc5));
    ASSERT_TRUE(InsertWithName(*writer, *doc6));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    tests::Remove(*writer, std::move(query_doc1_doc3_doc5));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(writer->Compact(always_merge));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // validate structure
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc2);
    expected.back().insert(*doc4);
    expected.back().insert(*doc6);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];           // assume 0 is id of first/only segment
    ASSERT_EQ(3, segment.docs_count());  // total count of documents
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc4
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("F", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc6
    ASSERT_FALSE(docs_itr->next());
  }

  // merge old+old+old segment
  {
    auto query_doc1_doc3_doc5 =
      MakeOr({{kNameFieldId, "A"}, {kNameFieldId, "C"}, {kNameFieldId, "E"}});
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(InsertWithName(*writer, *doc5));
    ASSERT_TRUE(InsertWithName(*writer, *doc6));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    tests::Remove(*writer, std::move(query_doc1_doc3_doc5));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(writer->Compact(always_merge));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // validate structure
    tests::index_t expected;
    expected.emplace_back();
    expected.back().insert(*doc2);
    expected.back().insert(*doc4);
    expected.back().insert(*doc6);
    tests::AssertIndex(dir(), codec(), expected, kAllFeatures);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];           // assume 0 is id of first/only segment
    ASSERT_EQ(3, segment.docs_count());  // total count of documents
    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc4
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("F", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc6
    ASSERT_FALSE(docs_itr->next());
  }

  // merge two segments with different fields
  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    // add 1st segment
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    ASSERT_TRUE(InsertWithName(*writer, *doc6));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // add 2nd segment
    tests::JsonDocGenerator gen(
      resource("simple_sequential_upper_case.json"),
      [](tests::Document& doc, const std::string& name,
         const tests::JsonDocGenerator::JsonValue& data) {
        if (data.is_string()) {
          {
            auto f = std::make_shared<tests::StringField>(name, data.str);
            f->id = tests::FieldIdFor(name);
            doc.insert(std::move(f));
          }
        }
      });

    auto doc1_1 = gen.next();
    auto doc1_2 = gen.next();
    auto doc1_3 = gen.next();
    ASSERT_TRUE(InsertWithName(*writer, *doc1_1));
    ASSERT_TRUE(InsertWithName(*writer, *doc1_2));
    ASSERT_TRUE(InsertWithName(*writer, *doc1_3));

    // defragment segments
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(writer->Compact(always_merge));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // validate merged segment
    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];           // assume 0 is id of first/only segment
    ASSERT_EQ(6, segment.docs_count());  // total count of documents

    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};

    const auto* upper_case_column = segment.Column(kNameUpper);
    ASSERT_NE(nullptr, upper_case_column);
    irs::tests::BlobPointReader upper_case_values{segment, *upper_case_column};

    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc4
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("F", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc6
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(
             upper_case_values, docs_itr->value()));  // 'name' value in doc1_1
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(
      "B", irs::tests::ReadStoredStr<std::string_view>(
             upper_case_values, docs_itr->value()));  // 'name' value in doc1_2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(
      "C", irs::tests::ReadStoredStr<std::string_view>(
             upper_case_values, docs_itr->value()));  // 'name' value in doc1_3
    ASSERT_FALSE(docs_itr->next());
  }

  // merge two segments with different fields
  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    // add 1st segment
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    ASSERT_TRUE(InsertWithName(*writer, *doc6));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // add 2nd segment
    tests::JsonDocGenerator gen(
      resource("simple_sequential_upper_case.json"),
      [](tests::Document& doc, const std::string& name,
         const tests::JsonDocGenerator::JsonValue& data) {
        if (data.is_string()) {
          {
            auto f = std::make_shared<tests::StringField>(name, data.str);
            f->id = tests::FieldIdFor(name);
            doc.insert(std::move(f));
          }
        }
      });

    auto doc1_1 = gen.next();
    auto doc1_2 = gen.next();
    auto doc1_3 = gen.next();
    ASSERT_TRUE(InsertWithName(*writer, *doc1_1));
    ASSERT_TRUE(InsertWithName(*writer, *doc1_2));
    ASSERT_TRUE(InsertWithName(*writer, *doc1_3));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // defragment segments
    ASSERT_TRUE(writer->Compact(always_merge));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    // validate merged segment
    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];           // assume 0 is id of first/only segment
    ASSERT_EQ(6, segment.docs_count());  // total count of documents

    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};

    const auto* upper_case_column = segment.Column(kNameUpper);
    ASSERT_NE(nullptr, upper_case_column);
    irs::tests::BlobPointReader upper_case_values{segment, *upper_case_column};

    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());
    auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("B", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("D", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc4
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ("F", irs::tests::ReadStoredStr<std::string_view>(
                     values, docs_itr->value()));  // 'name' value in doc6
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(
             upper_case_values, docs_itr->value()));  // 'name' value in doc1_1
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(
      "B", irs::tests::ReadStoredStr<std::string_view>(
             upper_case_values, docs_itr->value()));  // 'name' value in doc1_2
    ASSERT_TRUE(docs_itr->next());
    ASSERT_EQ(
      "C", irs::tests::ReadStoredStr<std::string_view>(
             upper_case_values, docs_itr->value()));  // 'name' value in doc1_3
    ASSERT_FALSE(docs_itr->next());
  }
}

TEST_P(IndexTestCase, segment_compact_policy) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        {
          auto f = std::make_shared<tests::StringField>(name, data.str);
          f->id = tests::FieldIdFor(name);
          doc.insert(std::move(f));
        }
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
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(InsertWithName(*writer, *doc5));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(InsertWithName(*writer, *doc6));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    irs::index_utils::CompactionBytes options;
    options.threshold = 1;
    ASSERT_TRUE(writer->Compact(
      irs::index_utils::MakePolicy(options)));  // value garanteeing merge
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());  // 1+(2|3)

    // check 1st segment
    {
      std::unordered_set<std::string_view> expected_name = {"A", "B", "C", "D"};
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(expected_name.size(),
                segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(
          1, expected_name.erase(irs::tests::ReadStoredStr<std::string_view>(
               values, docs_itr->value())));
      }

      ASSERT_TRUE(expected_name.empty());
    }

    // check 2nd (merged) segment
    {
      std::unordered_set<std::string_view> expected_name = {"E", "F"};
      auto& segment = reader[1];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(expected_name.size(),
                segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(
          1, expected_name.erase(irs::tests::ReadStoredStr<std::string_view>(
               values, docs_itr->value())));
      }

      ASSERT_TRUE(expected_name.empty());
    }
  }

  // bytes size policy (not modified)
  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(InsertWithName(*writer, *doc5));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    irs::index_utils::CompactionBytes options;
    options.threshold = 0;
    ASSERT_TRUE(writer->Compact(
      irs::index_utils::MakePolicy(options)));  // value garanteeing non-merge
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());

    {
      std::unordered_set<std::string_view> expected_name = {"A", "B", "C", "D"};

      auto& segment = reader[0];  // assume 0 is id of first segment
      ASSERT_EQ(expected_name.size(),
                segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(
          1, expected_name.erase(irs::tests::ReadStoredStr<std::string_view>(
               values, docs_itr->value())));
      }

      ASSERT_TRUE(expected_name.empty());
    }

    {
      std::unordered_set<std::string_view> expected_name = {"E"};

      auto& segment = reader[1];  // assume 1 is id of second segment
      ASSERT_EQ(expected_name.size(),
                segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(
          1, expected_name.erase(irs::tests::ReadStoredStr<std::string_view>(
               values, docs_itr->value())));
      }

      ASSERT_TRUE(expected_name.empty());
    }
  }

  // valid segment bytes_accum policy (merge)
  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    irs::index_utils::CompactionBytesAccum options;
    options.threshold = 1;
    ASSERT_TRUE(writer->Compact(
      irs::index_utils::MakePolicy(options)));  // value garanteeing merge
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    // segments merged because segment[0] is a candidate and needs to be
    // merged with something

    std::unordered_set<std::string_view> expected_name = {"A", "B"};

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    ASSERT_EQ(expected_name.size(),
              segment.docs_count());  // total count of documents
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());

    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
         docs_itr->next();) {
      ASSERT_EQ(1,
                expected_name.erase(irs::tests::ReadStoredStr<std::string_view>(
                  values, docs_itr->value())));
    }

    ASSERT_TRUE(expected_name.empty());
  }

  // valid segment bytes_accum policy (not modified)
  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    irs::index_utils::CompactionBytesAccum options;
    options.threshold = 0;
    ASSERT_TRUE(writer->Compact(
      irs::index_utils::MakePolicy(options)));  // value garanteeing non-merge
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());

    {
      std::unordered_set<std::string_view> expected_name = {"A"};

      auto& segment = reader[0];  // assume 0 is id of first segment
      ASSERT_EQ(expected_name.size(),
                segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(
          1, expected_name.erase(irs::tests::ReadStoredStr<std::string_view>(
               values, docs_itr->value())));
      }

      ASSERT_TRUE(expected_name.empty());
    }

    {
      std::unordered_set<std::string_view> expected_name = {"B"};

      auto& segment = reader[1];  // assume 1 is id of second segment
      ASSERT_EQ(expected_name.size(), segment.docs_count());
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(
          1, expected_name.erase(irs::tests::ReadStoredStr<std::string_view>(
               values, docs_itr->value())));
      }

      ASSERT_TRUE(expected_name.empty());
    }
  }

  // valid docs count policy (merge)
  {
    auto query_doc2_doc3_doc4 =
      MakeOr({{kNameFieldId, "B"}, {kNameFieldId, "C"}, {kNameFieldId, "D"}});
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    tests::Remove(*writer, std::move(query_doc2_doc3_doc4));
    ASSERT_TRUE(InsertWithName(*writer, *doc5));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    irs::index_utils::CompactionDocsLive options;
    options.threshold = 1;
    ASSERT_TRUE(writer->Compact(
      irs::index_utils::MakePolicy(options)));  // value garanteeing merge
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    std::unordered_set<std::string_view> expected_name = {"A", "E"};

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    ASSERT_EQ(expected_name.size(),
              segment.docs_count());  // total count of documents
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());

    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
         docs_itr->next();) {
      ASSERT_EQ(1,
                expected_name.erase(irs::tests::ReadStoredStr<std::string_view>(
                  values, docs_itr->value())));
    }

    ASSERT_TRUE(expected_name.empty());
  }

  // valid docs count policy (not modified)
  {
    auto query_doc2_doc3_doc4 =
      MakeOr({{kNameFieldId, "B"}, {kNameFieldId, "C"}, {kNameFieldId, "D"}});
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    tests::Remove(*writer, std::move(query_doc2_doc3_doc4));
    ASSERT_TRUE(InsertWithName(*writer, *doc5));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    irs::index_utils::CompactionDocsLive options;
    options.threshold = 0;
    ASSERT_TRUE(writer->Compact(
      irs::index_utils::MakePolicy(options)));  // value garanteeing non-merge
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());

    {
      std::unordered_set<std::string_view> expected_name = {"A"};

      auto& segment = reader[0];  // assume 0 is id of first segment
      ASSERT_EQ(expected_name.size() + 3,
                segment.docs_count());  // total count of documents (+3 ==
                                        // B, C, D masked)
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      for (auto docs_itr =
             segment.mask(term_itr->postings(irs::IndexFeatures::None));
           docs_itr->next();) {
        ASSERT_EQ(
          1, expected_name.erase(irs::tests::ReadStoredStr<std::string_view>(
               values, docs_itr->value())));
      }

      ASSERT_TRUE(expected_name.empty());
    }

    {
      std::unordered_set<std::string_view> expected_name = {"E"};

      auto& segment = reader[1];  // assume 1 is id of second segment
      ASSERT_EQ(expected_name.size(),
                segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(
          1, expected_name.erase(irs::tests::ReadStoredStr<std::string_view>(
               values, docs_itr->value())));
      }

      ASSERT_TRUE(expected_name.empty());
    }
  }

  // valid segment fill policy (merge)
  {
    auto query_doc2_doc4 =
      MakeByTermOrByTerm(kNameFieldId, "B", kNameFieldId, "D");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    tests::Remove(*writer, std::move(query_doc2_doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    irs::index_utils::CompactionDocsFill options;
    options.threshold = 1;
    ASSERT_TRUE(writer->Compact(
      irs::index_utils::MakePolicy(options)));  // value garanteeing merge
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    std::unordered_set<std::string_view> expected_name = {"A", "C"};

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = reader[0];  // assume 0 is id of first/only segment
    ASSERT_EQ(expected_name.size(),
              segment.docs_count());  // total count of documents
    auto terms = segment.field(kSameFieldId);
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());

    const auto* column = segment.Column(kNameColumnId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
         docs_itr->next();) {
      ASSERT_EQ(1,
                expected_name.erase(irs::tests::ReadStoredStr<std::string_view>(
                  values, docs_itr->value())));
    }

    ASSERT_TRUE(expected_name.empty());
  }

  // valid segment fill policy (not modified)
  {
    auto query_doc2_doc4 =
      MakeByTermOrByTerm(kNameFieldId, "B", kNameFieldId, "D");
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));
    ASSERT_TRUE(InsertWithName(*writer, *doc2));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    ASSERT_TRUE(InsertWithName(*writer, *doc3));
    ASSERT_TRUE(InsertWithName(*writer, *doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    tests::Remove(*writer, std::move(query_doc2_doc4));
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
    irs::index_utils::CompactionDocsFill options;
    options.threshold = 0;
    ASSERT_TRUE(writer->Compact(
      irs::index_utils::MakePolicy(options)));  // value garanteeing non-merge
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());

    {
      std::unordered_set<std::string_view> expected_name = {"A"};

      auto& segment = reader[0];  // assume 0 is id of first segment
      ASSERT_EQ(
        expected_name.size() + 1,
        segment.docs_count());  // total count of documents (+1 == B masked)
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      for (auto docs_itr =
             segment.mask(term_itr->postings(irs::IndexFeatures::None));
           docs_itr->next();) {
        ASSERT_EQ(
          1, expected_name.erase(irs::tests::ReadStoredStr<std::string_view>(
               values, docs_itr->value())));
      }

      ASSERT_TRUE(expected_name.empty());
    }

    {
      std::unordered_set<std::string_view> expected_name = {"C"};

      auto& segment = reader[1];  // assume 1 is id of second segment
      ASSERT_EQ(
        expected_name.size() + 1,
        segment.docs_count());  // total count of documents (+1 == D masked)
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      for (auto docs_itr =
             segment.mask(term_itr->postings(irs::IndexFeatures::None));
           docs_itr->next();) {
        ASSERT_EQ(
          1, expected_name.erase(irs::tests::ReadStoredStr<std::string_view>(
               values, docs_itr->value())));
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
        {
          auto f = std::make_shared<tests::StringField>(name, data.str);
          f->id = tests::FieldIdFor(name);
          doc.insert(std::move(f));
        }
      }
    });

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();

  // segment_count_max
  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    auto ctx = writer->GetBatch();  // hold a single segment

    {
      auto doc = ctx.Insert();
      ASSERT_TRUE(doc.Insert(doc1->indexed.begin(), doc1->indexed.end()));
      CaptureNameLikeFields(doc, doc1->indexed);
    }

    irs::SegmentOptions options;
    options.segment_count_max = 1;
    writer->Options(options);

    std::condition_variable cond;
    std::mutex mutex;
    std::unique_lock lock{mutex};
    std::atomic<bool> stop(false);

    std::thread thread([&writer, &doc2, &cond, &mutex, &stop]() -> void {
      ASSERT_TRUE(InsertWithName(*writer, *doc2));
      stop = true;
      std::lock_guard lock{mutex};
      cond.notify_all();
    });

    auto result =
      cond.wait_for(lock, 1000ms);  // assume thread blocks in 1000ms

    // As declaration for wait_for contains "It may also be unblocked
    // spuriously." for all platforms
    while (!stop && result == std::cv_status::no_timeout) {
      result = cond.wait_for(lock, 1000ms);
    }

    ASSERT_EQ(std::cv_status::timeout, result);
    // ^^^ expecting timeout because pool should block indefinitely

    {
      ctx.Commit();
    }  // force flush of GetBatch(), i.e. ulock segment
    // ASSERT_EQ(std::cv_status::no_timeout, cond.wait_for(lock, 1000ms));
    lock.unlock();
    thread.join();
    ASSERT_TRUE(stop);

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());

    // check only segment
    {
      std::unordered_set<std::string_view> expected_name = {"A", "B"};
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(expected_name.size(),
                segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(
          1, expected_name.erase(irs::tests::ReadStoredStr<std::string_view>(
               values, docs_itr->value())));
      }

      ASSERT_TRUE(expected_name.empty());
    }
  }

  // segment_docs_max
  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    irs::SegmentOptions options;
    options.segment_docs_max = 1;
    writer->Options(options);

    ASSERT_TRUE(InsertWithName(*writer, *doc2));

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());  // 1+2

    // check 1st segment
    {
      std::unordered_set<std::string_view> expected_name = {"A"};
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(expected_name.size(),
                segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(
          1, expected_name.erase(irs::tests::ReadStoredStr<std::string_view>(
               values, docs_itr->value())));
      }

      ASSERT_TRUE(expected_name.empty());
    }

    // check 2nd segment
    {
      std::unordered_set<std::string_view> expected_name = {"B"};
      auto& segment = reader[1];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      ASSERT_EQ(expected_name.size(),
                segment.docs_count());  // total count of documents
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(
          1, expected_name.erase(irs::tests::ReadStoredStr<std::string_view>(
               values, docs_itr->value())));
      }

      ASSERT_TRUE(expected_name.empty());
    }
  }

  // segment_memory_max
  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    irs::SegmentOptions options;
    options.segment_memory_max = 1;
    writer->Options(options);

    ASSERT_TRUE(InsertWithName(*writer, *doc2));

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(2, reader.size());  // 1+2

    // check 1st segment
    {
      std::unordered_set<std::string_view> expected_name = {"A"};
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      // total count of documents
      ASSERT_EQ(expected_name.size(), segment.docs_count());
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(
          1, expected_name.erase(irs::tests::ReadStoredStr<std::string_view>(
               values, docs_itr->value())));
      }

      ASSERT_TRUE(expected_name.empty());
    }

    // check 2nd segment
    {
      std::unordered_set<std::string_view> expected_name = {"B"};
      auto& segment = reader[1];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      // total count of documents
      ASSERT_EQ(expected_name.size(), segment.docs_count());
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(
          1, expected_name.erase(irs::tests::ReadStoredStr<std::string_view>(
               values, docs_itr->value())));
      }

      ASSERT_TRUE(expected_name.empty());
    }
  }

  // no_flush
  {
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    irs::SegmentOptions options;
    options.segment_docs_max = 1;
    writer->Options(options);

    // prevent segment from being flushed
    {
      auto ctx = writer->GetBatch();
      {
        auto doc = ctx.Insert(true);

        ASSERT_TRUE(
          doc.Insert(std::begin(doc2->indexed), std::end(doc2->indexed)));
        CaptureNameLikeFields(doc, doc2->indexed);
      }
      ctx.Commit();
    }

    ASSERT_TRUE(writer->RefreshCommit());
    AssertSnapshotEquality(*writer);

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_NE(nullptr, reader);
    ASSERT_EQ(1, reader.size());

    // check 1st segment
    {
      std::unordered_set<std::string_view> expected_name = {"A", "B"};
      auto& segment = reader[0];
      const auto* column = segment.Column(kNameColumnId);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      // total count of documents
      ASSERT_EQ(expected_name.size(), segment.docs_count());
      auto terms = segment.field(kSameFieldId);
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());

      for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
           docs_itr->next();) {
        ASSERT_EQ(
          1, expected_name.erase(irs::tests::ReadStoredStr<std::string_view>(
               values, docs_itr->value())));
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
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

    ASSERT_TRUE(InsertWithName(*writer, *doc));
    writer->RefreshCommit();
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

  auto writer = open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

  ASSERT_TRUE(InsertWithName(*writer, *doc4));

  ASSERT_TRUE(InsertWithName(*writer, *doc3));

  writer->RefreshCommit();
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
  ASSERT_TRUE(InsertWithName(*writer, *doc1));

  ASSERT_TRUE(InsertWithName(*writer, *doc2));

  auto query_doc1 = MakeByTerm(kNameFieldId, "A");
  tests::Remove(*writer, *(query_doc1.get()));
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);

  // remove for initial segment to trigger compaction
  // compaction is needed to force opening all file handles and make
  // cached readers indeed hold reference to a file
  auto query_doc3 = MakeByTerm(kNameFieldId, "C");
  tests::Remove(*writer, *(query_doc3.get()));
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);

  // this compaction should bring us to one compacted segment without
  // removals.
  ASSERT_TRUE(writer->Compact(
    irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));
  writer->RefreshCommit();
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

  auto writer = open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

  ASSERT_TRUE(InsertWithName(*writer, *doc4));

  ASSERT_TRUE(InsertWithName(*writer, *doc3));

  writer->RefreshCommit();
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
  ASSERT_TRUE(InsertWithName(*writer, *doc1));

  ASSERT_TRUE(InsertWithName(*writer, *doc2));

  auto query_doc1 = MakeByTerm(kNameFieldId, "A");
  tests::Remove(*writer, *(query_doc1.get()));
  auto query_doc2 = MakeByTerm(kNameFieldId, "B");
  tests::Remove(*writer, *(query_doc2.get()));
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);

  // remove for initial segment to trigger compaction
  // compaction is needed to force opening all file handles and make
  // cached readers indeed hold reference to a file
  auto query_doc3 = MakeByTerm(kNameFieldId, "C");
  tests::Remove(*writer, *(query_doc3.get()));
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);

  // this compaction should bring us to one compacted segment without
  // removes.
  ASSERT_TRUE(writer->Compact(
    irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));
  writer->RefreshCommit();
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

  auto writer = open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

  ASSERT_TRUE(InsertWithName(*writer, *doc1));

  ASSERT_TRUE(InsertWithName(*writer, *doc2));

  writer->RefreshCommit();
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
  auto query_doc1 = MakeByTerm(kNameFieldId, "A");
  tests::Remove(*writer, *(query_doc1.get()));
  auto query_doc2 = MakeByTerm(kNameFieldId, "B");
  tests::Remove(*writer, *(query_doc2.get()));
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  {
    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(0, reader.size());
    // file removal should pass for all files (especially valid for
    // Microsoft Windows)
    irs::directory_utils::RemoveAllUnreferenced(directory);

    // validate that all files from old segments have been removed
    count = 0;
    directory.visit(get_number_of_files_in_segments);
    ASSERT_EQ(count, 0);
  }

  // this compaction should still be ok
  ASSERT_TRUE(writer->Compact(
    irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
}

TEST_P(IndexTestCase, writer_remove_all_from_last_segment_compaction) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);
  auto& directory = dir();
  auto* doc1 = gen.next();
  auto* doc2 = gen.next();
  auto* doc3 = gen.next();
  auto* doc4 = gen.next();

  auto writer = open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());

  ASSERT_TRUE(InsertWithName(*writer, *doc1));

  ASSERT_TRUE(InsertWithName(*writer, *doc2));

  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);  // segment 1

  ASSERT_TRUE(InsertWithName(*writer, *doc3));

  ASSERT_TRUE(InsertWithName(*writer, *doc4));

  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);  //  segment 2

  auto query_doc1 = MakeByTerm(kNameFieldId, "A");
  tests::Remove(*writer, *(query_doc1.get()));
  auto query_doc3 = MakeByTerm(kNameFieldId, "C");
  tests::Remove(*writer, *(query_doc3.get()));
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);

  // this compaction should bring us to one compacted segment without
  // removes.
  ASSERT_TRUE(writer->Compact(
    irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));
  // Remove all documents from 'new' segment
  auto query_doc4 = MakeByTerm(kNameFieldId, "D");
  tests::Remove(*writer, *(query_doc4.get()));
  auto query_doc2 = MakeByTerm(kNameFieldId, "B");
  tests::Remove(*writer, *(query_doc2.get()));
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  {
    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
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

  // this compaction should still be ok
  ASSERT_TRUE(writer->Compact(
    irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));
  writer->RefreshCommit();
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
    irs::field_id Id() const { return kTestFieldId; }
    std::string_view Name() const { return "test"; };
    irs::IndexFeatures GetIndexFeatures() const {
      return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
             irs::IndexFeatures::Norm;
    }
    irs::Tokenizer& GetTokens() const noexcept { return stream; }

    mutable EmptyTokenizer stream;
  } empty;

  {
    auto opts = irs::tests::DefaultWriterOptions();

    auto writer = open_writer(irs::kOmCreate, opts);

    // no norms is written as there is nothing to index
    {
      auto docs = writer->GetBatch();
      {
        auto doc = docs.Insert();
        ASSERT_TRUE(doc.Insert(empty));
      }
      docs.Commit();
    }

    // we don't write default norms
    {
      const tests::StringField field(static_cast<std::string>(empty.Name()),
                                     "bar", empty.GetIndexFeatures());
      auto docs = writer->GetBatch();
      {
        auto doc = docs.Insert();
        ASSERT_TRUE(doc.Insert(field));
      }
      docs.Commit();
    }

    {
      const tests::StringField field(static_cast<std::string>(empty.Name()),
                                     "bar", empty.GetIndexFeatures());
      auto docs = writer->GetBatch();
      {
        auto doc = docs.Insert();
        ASSERT_TRUE(doc.Insert(field));
        ASSERT_TRUE(doc.Insert(field));
      }
      docs.Commit();
    }

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
  }

  {
    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, reader.size());
    auto& segment = (*reader)[0];
    ASSERT_EQ(3, segment.docs_count());
    ASSERT_EQ(3, segment.live_docs_count());

    const auto ids = segment.field_ids();
    ASSERT_EQ(1u, ids.size());
    const auto* field_reader = segment.field(ids.front());
    ASSERT_NE(nullptr, field_reader);
    ASSERT_EQ(ids.front(), field_reader->meta().id);
    ASSERT_TRUE(irs::IsSubsetOf(irs::IndexFeatures::Norm,
                                field_reader->meta().index_features));
    const auto norm = field_reader->meta().norm;
    ASSERT_TRUE(irs::field_limits::valid(norm));

    auto norm_reader = segment.norms(norm);
    ASSERT_NE(nullptr, norm_reader);
    EXPECT_EQ(norm_reader->Get(1), 0);
    EXPECT_EQ(norm_reader->Get(2), 1);
    EXPECT_EQ(norm_reader->Get(3), 2);
  }
}

class IndexTestCase11 : public tests::IndexTestBase {};

TEST_P(IndexTestCase11, compact_old_format) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::StringField>(name, data.str);
        field->id = FieldIdFor(name);
        doc.insert(std::move(field));
      }
    });
  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();

  auto validate_codec = [&](auto codec, size_t size) {
    auto reader =
      irs::DirectoryReader(dir(), nullptr, irs::tests::DefaultReaderOptions());
    ASSERT_NE(nullptr, reader);
    ASSERT_EQ(size, reader.size());
    ASSERT_EQ(size, reader.Meta().index_meta.segments.size());
    for (auto& meta : reader.Meta().index_meta.segments) {
      ASSERT_EQ(codec, meta.meta.codec);
    }
  };

  auto writer_options = irs::tests::DefaultWriterOptions();
  auto writer = open_writer(irs::kOmCreate, writer_options);
  // 1st segment
  ASSERT_TRUE(InsertWithName(*writer, *doc1));
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  validate_codec(codec(), 1);
  // 2nd segment
  ASSERT_TRUE(InsertWithName(*writer, *doc2));
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  validate_codec(codec(), 2);
  // compact
  auto old_codec = irs::formats::Get("1_5simd");
  irs::index_utils::CompactionCount compact_all;
  ASSERT_TRUE(
    writer->Compact(irs::index_utils::MakePolicy(compact_all), old_codec));
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  validate_codec(old_codec, 1);
}

TEST_P(IndexTestCase11, clean_writer_with_payload) {
  tests::JsonDocGenerator gen(
    resource("simple_sequential.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        {
          auto f = std::make_shared<tests::StringField>(name, data.str);
          f->id = tests::FieldIdFor(name);
          doc.insert(std::move(f));
        }
      }
    });

  const tests::Document* doc1 = gen.next();

  auto writer_options = irs::tests::DefaultWriterOptions();
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

  ASSERT_TRUE(InsertWithName(*writer, *doc1));
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);

  size_t file_count0 = 0;
  dir().visit([&file_count0](std::string_view) -> bool {
    ++file_count0;
    return true;
  });

  {
    auto reader =
      irs::DirectoryReader(dir(), nullptr, irs::tests::DefaultReaderOptions());
    ASSERT_TRUE(irs::IsNull(irs::GetPayload(reader.Meta().index_meta)));
  }
  uint64_t expected_tick = 42;

  payload_committed_tick = 0;
  payload_provider_result = true;
  writer->Clear(expected_tick);
  {
    auto reader =
      irs::DirectoryReader(dir(), nullptr, irs::tests::DefaultReaderOptions());
    ASSERT_EQ(input_payload, irs::GetPayload(reader.Meta().index_meta));
    ASSERT_EQ(payload_committed_tick, expected_tick);
  }
}

TEST_P(IndexTestCase11, initial_two_phase_commit_no_payload) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  auto& directory = dir();

  auto writer_options = irs::tests::DefaultWriterOptions();
  uint64_t payload_calls_count{0};
  writer_options.meta_payload_provider = [&payload_calls_count](uint64_t,
                                                                irs::bstring&) {
    payload_calls_count++;
    return false;
  };
  auto writer = open_writer(irs::kOmCreate, writer_options);

  ASSERT_TRUE(writer->RefreshBegin());

  // transaction is already started
  payload_calls_count = 0;
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(0, payload_calls_count);

  auto reader = irs::DirectoryReader(directory, nullptr,
                                     irs::tests::DefaultReaderOptions());
  ASSERT_TRUE(irs::IsNull(irs::GetPayload(reader.Meta().index_meta)));

  // no changes
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(0, payload_calls_count);
  ASSERT_EQ(reader, reader.Reopen());
}

TEST_P(IndexTestCase11, initial_commit_no_payload) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  auto& directory = dir();

  auto writer_options = irs::tests::DefaultWriterOptions();
  uint64_t payload_calls_count{0};
  writer_options.meta_payload_provider = [&payload_calls_count](uint64_t,
                                                                irs::bstring&) {
    payload_calls_count++;
    return false;
  };
  auto writer = open_writer(irs::kOmCreate, writer_options);

  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);

  auto reader = irs::DirectoryReader(directory, nullptr,
                                     irs::tests::DefaultReaderOptions());
  ASSERT_TRUE(irs::IsNull(irs::GetPayload(reader.Meta().index_meta)));

  // no changes
  payload_calls_count = 0;
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(0, payload_calls_count);
  ASSERT_EQ(reader, reader.Reopen());
}

TEST_P(IndexTestCase11, initial_two_phase_commit_payload_revert) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  auto& directory = dir();

  auto writer_options = irs::tests::DefaultWriterOptions();
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
  ASSERT_TRUE(writer->RefreshBegin());
  ASSERT_EQ(0, payload_committed_tick);

  payload_provider_result = true;
  // transaction is already started
  payload_calls_count = 0;
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(0, payload_calls_count);

  auto reader = irs::DirectoryReader(directory, nullptr,
                                     irs::tests::DefaultReaderOptions());
  ASSERT_TRUE(irs::IsNull(irs::GetPayload(reader.Meta().index_meta)));

  // no changes
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(0, payload_calls_count);
  ASSERT_EQ(reader, reader.Reopen());
}

TEST_P(IndexTestCase11, initial_commit_payload_revert) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  auto& directory = dir();

  auto writer_options = irs::tests::DefaultWriterOptions();
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
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(0, payload_committed_tick);

  auto reader = irs::DirectoryReader(directory, nullptr,
                                     irs::tests::DefaultReaderOptions());
  ASSERT_TRUE(irs::IsNull(irs::GetPayload(reader.Meta().index_meta)));

  payload_provider_result = true;
  // no changes
  payload_calls_count = 0;
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(0, payload_calls_count);
  ASSERT_EQ(reader, reader.Reopen());
}

TEST_P(IndexTestCase11, initial_two_phase_commit_payload) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  auto& directory = dir();

  auto writer_options = irs::tests::DefaultWriterOptions();
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
  ASSERT_TRUE(writer->RefreshBegin());
  ASSERT_EQ(0, payload_committed_tick);

  // transaction is already started
  payload_calls_count = 0;
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(0, payload_calls_count);

  auto reader = irs::DirectoryReader(directory, nullptr,
                                     irs::tests::DefaultReaderOptions());
  ASSERT_EQ(input_payload, irs::GetPayload(reader.Meta().index_meta));

  // no changes
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(0, payload_calls_count);
  ASSERT_EQ(reader, reader.Reopen());
}

TEST_P(IndexTestCase11, initial_commit_payload) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  auto& directory = dir();

  auto writer_options = irs::tests::DefaultWriterOptions();
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
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(0, payload_committed_tick);

  auto reader = irs::DirectoryReader(directory, nullptr,
                                     irs::tests::DefaultReaderOptions());
  ASSERT_EQ(input_payload, irs::GetPayload(reader.Meta().index_meta));

  // no changes
  payload_calls_count = 0;
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(0, payload_calls_count);
  ASSERT_EQ(reader, reader.Reopen());
}

TEST_P(IndexTestCase11, commit_payload) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  auto& directory = dir();
  auto* doc0 = gen.next();

  auto writer_options = irs::tests::DefaultWriterOptions();
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
  ASSERT_TRUE(writer->RefreshBegin());  // initial commit
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  auto reader = irs::DirectoryReader(directory, nullptr,
                                     irs::tests::DefaultReaderOptions());
  ASSERT_TRUE(irs::IsNull(irs::GetPayload(reader.Meta().index_meta)));

  ASSERT_FALSE(
    writer->RefreshBegin());  // transaction hasn't been started, no changes
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(reader, reader.Reopen());
  payload_provider_result = true;
  // commit with a specified payload
  {
    const uint64_t expected_tick = 42;

    // insert document (trx 1)
    {
      auto trx = writer->GetBatch();
      {
        auto doc = trx.Insert();
        doc.Insert(doc0->indexed.begin(), doc0->indexed.end());
        CaptureNameLikeFields(doc, doc0->indexed);
        doc.Insert(doc0->stored.begin(), doc0->stored.end());
        ASSERT_TRUE(doc);
      }
      trx.Commit(expected_tick - 10);
      AssertSnapshotEquality(*writer);
    }

    // insert document (trx 0)
    {
      auto trx = writer->GetBatch();
      {
        auto doc = trx.Insert();
        doc.Insert(doc0->indexed.begin(), doc0->indexed.end());
        CaptureNameLikeFields(doc, doc0->indexed);
        doc.Insert(doc0->stored.begin(), doc0->stored.end());
        ASSERT_TRUE(doc);
      }
      trx.Commit(expected_tick);
      AssertSnapshotEquality(*writer);
    }

    payload_committed_tick = 0;

    input_payload =
      irs::ViewCast<irs::byte_type>(std::string_view(reader.Meta().filename));
    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_EQ(expected_tick, payload_committed_tick);

    // transaction is already started
    ASSERT_NE(0, payload_calls_count);
    payload_calls_count = 0;
    writer->RefreshCommit();
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
      {
        auto doc = trx.Insert();
        doc.Insert(doc0->indexed.begin(), doc0->indexed.end());
        CaptureNameLikeFields(doc, doc0->indexed);
        doc.Insert(doc0->stored.begin(), doc0->stored.end());
        ASSERT_TRUE(doc);
      }
      trx.Commit(expected_tick - 10);
    }

    // insert document (trx 0)
    {
      auto trx = writer->GetBatch();
      {
        auto doc = trx.Insert();
        doc.Insert(doc0->indexed.begin(), doc0->indexed.end());
        CaptureNameLikeFields(doc, doc0->indexed);
        doc.Insert(doc0->stored.begin(), doc0->stored.end());
        ASSERT_TRUE(doc);
      }
      trx.Commit(expected_tick);
    }

    payload_committed_tick = 0;

    input_payload =
      irs::ViewCast<irs::byte_type>(std::string_view(reader.Meta().filename));
    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_EQ(expected_tick, payload_committed_tick);

    writer->RefreshAbort();

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
      {
        auto doc = trx.Insert();
        doc.Insert(doc0->indexed.begin(), doc0->indexed.end());
        CaptureNameLikeFields(doc, doc0->indexed);
        doc.Insert(doc0->stored.begin(), doc0->stored.end());
        ASSERT_TRUE(doc);
      }
      trx.Commit(expected_tick);
    }

    // insert document (trx 1)
    {
      auto trx = writer->GetBatch();
      {
        auto doc = trx.Insert();
        doc.Insert(doc0->indexed.begin(), doc0->indexed.end());
        CaptureNameLikeFields(doc, doc0->indexed);
        doc.Insert(doc0->stored.begin(), doc0->stored.end());
        ASSERT_TRUE(doc);
      }
      trx.Commit(expected_tick);
    }

    payload_committed_tick = 1;

    input_payload =
      irs::ViewCast<irs::byte_type>(std::string_view(reader.Meta().filename));
    payload_provider_result = false;
    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_EQ(expected_tick, payload_committed_tick);

    // transaction is already started
    payload_calls_count = 0;
    writer->RefreshCommit();
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
      {
        auto doc = trx.Insert();
        doc.Insert(doc0->indexed.begin(), doc0->indexed.end());
        CaptureNameLikeFields(doc, doc0->indexed);
        doc.Insert(doc0->stored.begin(), doc0->stored.end());
        ASSERT_TRUE(doc);
      }
      trx.Commit(expected_tick);
    }

    // insert document (trx 1)
    {
      auto trx = writer->GetBatch();
      {
        auto doc = trx.Insert();
        doc.Insert(doc0->indexed.begin(), doc0->indexed.end());
        CaptureNameLikeFields(doc, doc0->indexed);
        doc.Insert(doc0->stored.begin(), doc0->stored.end());
        ASSERT_TRUE(doc);
      }
      trx.Commit(expected_tick);
    }

    payload_committed_tick = 42;
    input_payload.clear();
    payload_provider_result = true;

    ASSERT_TRUE(writer->RefreshBegin());
    ASSERT_EQ(expected_tick, payload_committed_tick);

    // transaction is already started
    payload_calls_count = 0;
    input_payload.clear();
    writer->RefreshCommit();
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
      {
        auto doc = trx.Insert();
        doc.Insert(doc0->indexed.begin(), doc0->indexed.end());
        CaptureNameLikeFields(doc, doc0->indexed);
        doc.Insert(doc0->stored.begin(), doc0->stored.end());
        ASSERT_TRUE(doc);
      }
      trx.Commit();
    }

    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
  }

  // check written payload
  {
    auto new_reader = reader.Reopen();
    ASSERT_NE(reader, new_reader);
    ASSERT_TRUE(irs::IsNull(irs::GetPayload(new_reader.Meta().index_meta)));
    reader = new_reader;
  }

  ASSERT_FALSE(
    writer->RefreshBegin());  // transaction hasn't been started, no changes
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  ASSERT_EQ(reader, reader.Reopen());
}

TEST_P(IndexTestCase11, testExternalGeneration) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  auto& directory = dir();
  auto* doc0 = gen.next();
  auto* doc1 = gen.next();

  auto writer_options = irs::tests::DefaultWriterOptions();
  auto writer = open_writer(irs::kOmCreate, writer_options);
  {
    auto trx = writer->GetBatch();
    {
      auto doc = trx.Insert();
      doc.Insert(doc0->indexed.begin(), doc0->indexed.end());
      CaptureNameLikeFields(doc, doc0->indexed);
      doc.Insert(doc0->stored.begin(), doc0->stored.end());
      ASSERT_TRUE(doc);
    }
    {
      auto doc = trx.Insert();
      doc.Insert(doc1->indexed.begin(), doc1->indexed.end());
      CaptureNameLikeFields(doc, doc1->indexed);
      doc.Insert(doc1->stored.begin(), doc1->stored.end());
      ASSERT_TRUE(doc);
    }
    // subcontext with remove
    {
      auto trx2 = writer->GetBatch();
      trx2.Remove(MakeByTerm(kNameFieldId, "A"));
      trx2.Commit(4);
    }
    trx.Commit(3);
  }
  ASSERT_TRUE(writer->RefreshBegin());
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  auto reader = irs::DirectoryReader(directory, nullptr,
                                     irs::tests::DefaultReaderOptions());
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

  auto writer_options = irs::tests::DefaultWriterOptions();
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
      doc.Insert(doc0->indexed.begin(), doc0->indexed.end());
      CaptureNameLikeFields(doc, doc0->indexed);
      ASSERT_TRUE(doc);
    }
    {
      auto doc = trx.Insert();
      doc.Insert(doc1->indexed.begin(), doc1->indexed.end());
      CaptureNameLikeFields(doc, doc1->indexed);
      ASSERT_TRUE(doc);
    }
    // subcontext with remove
    {
      auto trx2 = writer->GetBatch();
      trx2.Remove(MakeByTerm(kNameFieldId, "A"));
      trx2.Commit(4);
    }
    trx.Commit(3);
  }
  ASSERT_TRUE(writer->RefreshBegin());
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  writer.reset();
  auto reader = irs::DirectoryReader(directory, nullptr,
                                     irs::tests::DefaultReaderOptions());
  if (dynamic_cast<irs::MemoryDirectory*>(&directory) == nullptr) {
    // MMapDirectory keeps payload files memory-mapped, so only the
    // non-mappable streams contribute to the file_descriptors counter;
    // file-system based directories also retain an fd for the per-field
    // `.idx` file introduced by the current index format.
    const size_t expected_fd =
      dynamic_cast<irs::MMapDirectory*>(&directory) != nullptr ? 4 : 5;
    EXPECT_EQ(GetResourceManager().file_descriptors.Counter(), expected_fd);
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

  auto writer_options = irs::tests::DefaultWriterOptions();
  auto writer = open_writer(irs::kOmCreate, writer_options);
  {
    auto trx = writer->GetBatch();
    {
      auto doc = trx.Insert();
      doc.Insert(doc0->indexed.begin(), doc0->indexed.end());
      CaptureNameLikeFields(doc, doc0->indexed);
      doc.Insert(doc0->stored.begin(), doc0->stored.end());
      ASSERT_TRUE(doc);
    }
    {
      auto doc = trx.Insert();
      doc.Insert(doc1->indexed.begin(), doc1->indexed.end());
      CaptureNameLikeFields(doc, doc1->indexed);
      doc.Insert(doc1->stored.begin(), doc1->stored.end());
      ASSERT_TRUE(doc);
    }
    // subcontext with remove
    {
      auto trx2 = writer->GetBatch();
      trx2.Remove(MakeByTerm(kNameFieldId, "A"));
      trx2.Commit(2);
    }
    trx.Commit(4);
  }
  ASSERT_TRUE(writer->RefreshBegin());
  writer->RefreshCommit();
  AssertSnapshotEquality(*writer);
  auto reader = irs::DirectoryReader(directory, nullptr,
                                     irs::tests::DefaultReaderOptions());
  ASSERT_EQ(1, reader.size());
  auto& segment = (*reader)[0];
  ASSERT_EQ(2, segment.docs_count());
  ASSERT_EQ(2, segment.live_docs_count());
}

// IndexTestCase14 is parametrized identically to IndexTestCase but targets
// the format-14 codec. Format-14 is no longer registered (only "1_5simd"
// is), so the tests below `GTEST_SKIP()` immediately. The fixture is kept
// (with the IndexTestBase parametrization) so the test names still show up
// in the runner's output and we have an obvious place to wire it back when
// / if format-14 returns.
class IndexTestCase14 : public IndexTestCase {};

TEST_P(IndexTestCase14, write_field_with_multiple_stored_features) {
  GTEST_SKIP() << "format-14 codec is no longer registered";
}

TEST_P(IndexTestCase14, compact_multiple_stored_features) {
  GTEST_SKIP() << "format-14 codec is no longer registered";
}

TEST_P(IndexTestCase14, buffered_column_reopen) {
  GTEST_SKIP() << "format-14 codec is no longer registered; the legacy "
                  "ResourceManagementOptions::cached_columns counter and "
                  "IndexReaderOptions::warmup_columns hook this test "
                  "relied on are gone with the new columnstore";
}

// Minimal stand-ins for the original HNSW search fixtures. Bodies of the
// two tests below `GTEST_SKIP()` -- the production HNSW path is now driven
// through the typed columnstore (`ColWriter::AttachHNSW`,
// `HnswReader`) and `SubReader::Search(field_id, ...)`; the
// legacy `irs::ColumnInfo`/`IndexWriterOptions::column_info` lambda and
// `DirectoryReader::Search(name, info, buffer)` overloads the original
// tests built on are gone. SQL-level coverage lives at
// `tests/sqllogic/sdb/pg/index/vector_search.test`. These fixtures stay so
// the parametrized test names still appear in the runner's output and we
// have an obvious place to restore them when a gtest harness for the new
// HNSW API lands.
struct SearchTestFeatureBase {
  irs::HNSWMetric metric = irs::HNSWMetric::L2Sqr;
};

struct ANNSearchFeature : SearchTestFeatureBase {};
struct RangeSearchFeature : SearchTestFeatureBase {};

template<typename Feature>
class VectorSearchTestBase
  : public virtual TestParamBase<
      std::tuple<tests::dir_param_f, tests::FormatInfo, Feature>> {
 public:
  using Param = std::tuple<tests::dir_param_f, tests::FormatInfo, Feature>;

  static std::string to_string(const testing::TestParamInfo<Param>& info) {
    const auto& [dir_f, fmt, feat] = info.param;
    std::string name = (*dir_f)(nullptr).second;
    if (fmt.codec) {
      name += "_";
      name += fmt.codec;
    }
    switch (feat.metric) {
      case irs::HNSWMetric::L2Sqr:
        name += "_L2Sqr";
        break;
      case irs::HNSWMetric::NegativeIP:
        name += "_IP";
        break;
      case irs::HNSWMetric::Cosine:
        name += "_Cosine";
        break;
      case irs::HNSWMetric::L1:
        name += "_L1";
        break;
    }
    return name;
  }
};

class ANNSearchTest : public VectorSearchTestBase<ANNSearchFeature> {};
class RangeSearchTest : public VectorSearchTestBase<RangeSearchFeature> {};

TEST_P(ANNSearchTest, hnsw_search_basic) {
  GTEST_SKIP() << "Legacy IndexWriterOptions::column_info / irs::ColumnInfo "
                  "and DirectoryReader::Search(name, ...) overloads are "
                  "removed; SQL-level coverage at "
                  "tests/sqllogic/sdb/pg/index/vector_search.test";
}

TEST_P(RangeSearchTest, hnsw_range_search_basic) {
  GTEST_SKIP() << "Legacy IndexWriterOptions::column_info / irs::ColumnInfo "
                  "and DirectoryReader::RangeSearch(name, ...) overloads "
                  "are removed; SQL-level coverage at "
                  "tests/sqllogic/sdb/pg/index/vector_search.test";
}

static const auto kTestFormats =
  ::testing::Values(tests::FormatInfo{"1_5simd"});

static const auto kTestDirs =
  ::testing::ValuesIn(tests::GetDirectories<tests::kTypesDefaultRot13>());

static const auto kTestValues = ::testing::Combine(kTestDirs, kTestFormats);

INSTANTIATE_TEST_SUITE_P(index_test_11, IndexTestCase11, kTestValues,
                         IndexTestCase11::to_string);

INSTANTIATE_TEST_SUITE_P(index_test_14, IndexTestCase14, kTestValues,
                         IndexTestCase14::to_string);

INSTANTIATE_TEST_SUITE_P(index_test_15, IndexTestCase, kTestValues,
                         IndexTestCase::to_string);

INSTANTIATE_TEST_SUITE_P(
  BasicANNSearch, ANNSearchTest,
  ::testing::Combine(kTestDirs, kTestFormats,
                     ::testing::ValuesIn(std::vector<ANNSearchFeature>{
                       ANNSearchFeature{{irs::HNSWMetric::L2Sqr}},
                       ANNSearchFeature{{irs::HNSWMetric::NegativeIP}},
                       ANNSearchFeature{{irs::HNSWMetric::Cosine}},
                       ANNSearchFeature{{irs::HNSWMetric::L1}},
                     })),
  ANNSearchTest::to_string);

INSTANTIATE_TEST_SUITE_P(
  BasicRangeSearch, RangeSearchTest,
  ::testing::Combine(kTestDirs, kTestFormats,
                     ::testing::ValuesIn(std::vector<RangeSearchFeature>{
                       RangeSearchFeature{{irs::HNSWMetric::L2Sqr}},
                       RangeSearchFeature{{irs::HNSWMetric::NegativeIP}},
                       RangeSearchFeature{{irs::HNSWMetric::Cosine}},
                       RangeSearchFeature{{irs::HNSWMetric::L1}},
                     })),
  RangeSearchTest::to_string);
