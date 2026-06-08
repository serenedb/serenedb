////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#include "formats_test_case_base.hpp"

#include <algorithm>
#include <cstddef>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <unordered_map>
#include <unordered_set>

#include "basics/duckdb_engine.h"
#include "basics/resource_manager.hpp"
#include "formats/column/test_cs_helpers.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/column_writer.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/index/burst_trie.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/utils/type_limits.hpp"
#include "utils/write_helpers.hpp"

namespace {

template<typename Visitor>
bool VisitFiles(const irs::IndexMeta& meta, Visitor&& visitor) {
  for (auto& curr_segment : meta.segments) {
    if (!visitor(curr_segment.filename)) {
      return false;
    }

    for (auto& file : curr_segment.meta.files) {
      if (!visitor(file)) {
        return false;
      }
    }
  }
  return true;
}

}  // namespace
namespace tests {

void FormatTestCase::AssertFrequencyAndPositions(irs::DocIterator& expected,
                                                 irs::DocIterator& actual) {
  if (irs::doc_limits::eof(expected.value())) {
    ASSERT_TRUE(irs::doc_limits::eof(expected.value()));
    return;
  }

  auto* expected_freq = irs::get<irs::FreqBlockAttr>(expected);
  auto* actual_freq = irs::get<irs::FreqBlockAttr>(actual);
  ASSERT_EQ(!expected_freq, !actual_freq);

  if (!expected_freq) {
    return;
  }

  expected.FetchScoreArgs(0);
  actual.FetchScoreArgs(0);
  ASSERT_EQ(expected_freq->value[0], actual_freq->value[0]);

  auto* expected_pos = irs::GetMutable<irs::PosAttr>(&expected);
  auto* actual_pos = irs::GetMutable<irs::PosAttr>(&actual);
  ASSERT_EQ(!expected_pos, !actual_pos);

  if (!expected_pos) {
    return;
  }

  auto* expected_offset = irs::get<irs::OffsAttr>(*expected_pos);
  auto* actual_offset = irs::get<irs::OffsAttr>(*actual_pos);
  ASSERT_EQ(!expected_offset, !actual_offset);

  auto* expected_payload = irs::get<irs::PayAttr>(*expected_pos);
  auto* actual_payload = irs::get<irs::PayAttr>(*actual_pos);
  ASSERT_EQ(!expected_payload, !actual_payload);

  for (; expected_pos->next();) {
    ASSERT_TRUE(actual_pos->next());
    ASSERT_EQ(expected_pos->value(), actual_pos->value());

    if (expected_offset) {
      ASSERT_EQ(expected_offset->start, actual_offset->start);
      ASSERT_EQ(expected_offset->end, actual_offset->end);
    }

    if (expected_payload) {
      ASSERT_EQ(expected_payload->value, actual_payload->value);
    }
  }
  ASSERT_FALSE(actual_pos->next());
}

void FormatTestCase::AssertNoDirectoryArtifacts(
  const irs::Directory& dir, const irs::Format& codec,
  const std::unordered_set<std::string>& expect_additional /* ={} */) {
  std::vector<std::string> dir_files;
  auto visitor = [&dir_files](std::string_view file) {
    // ignore lock file present in fs_directory
    if (irs::IndexWriter::kWriteLockName != file) {
      dir_files.emplace_back(file);
    }
    return true;
  };
  ASSERT_TRUE(dir.visit(visitor));

  irs::IndexMeta index_meta;
  std::string segment_file;

  auto reader = codec.get_index_meta_reader();
  std::unordered_set<std::string> index_files(expect_additional.begin(),
                                              expect_additional.end());
  const bool exists = reader->last_segments_file(dir, segment_file);

  if (exists) {
    reader->read(dir, index_meta, segment_file);

    VisitFiles(index_meta, [&index_files](const std::string& file) {
      index_files.emplace(file);
      return true;
    });

    index_files.insert(segment_file);
  }

  for (auto& file : dir_files) {
    ASSERT_EQ(1, index_files.erase(file));
  }

  ASSERT_TRUE(index_files.empty());
}

auto MakeByTerm(irs::field_id id, std::string_view value) {
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field_id() = id;
  filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(value);
  return filter;
}

TEST_P(FormatTestCase, directory_artifact_cleaner) {
  tests::JsonDocGenerator gen{resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory};
  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();
  const tests::Document* doc3 = gen.next();
  const tests::Document* doc4 = gen.next();
  const irs::field_id kNameFieldId = tests::FieldIdFor("name");
  auto query_doc1 = MakeByTerm(kNameFieldId, "A");
  auto query_doc2 = MakeByTerm(kNameFieldId, "B");
  auto query_doc3 = MakeByTerm(kNameFieldId, "C");
  auto query_doc4 = MakeByTerm(kNameFieldId, "D");

  std::vector<std::string> files;
  auto list_files = [&files](std::string_view name) {
    files.emplace_back(name);
    return true;
  };

  auto dir = get_directory(*this);
  files.clear();
  ASSERT_TRUE(dir->visit(list_files));
  ASSERT_TRUE(files.empty());

  // cleanup on refcount decrement (old files not in use)
  {
    // create writer to directory
    auto writer = irs::IndexWriter::Make(*dir, codec(), irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());

    // initialize directory
    {
      writer->RefreshCommit();
      tests::AssertSnapshotEquality(
        writer->GetSnapshot(),
        irs::DirectoryReader(*dir, nullptr,
                             irs::tests::DefaultReaderOptions()));
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec());
    }

    // add first segment
    {
      ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end()));
      ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end()));
      writer->RefreshCommit();
      tests::AssertSnapshotEquality(
        writer->GetSnapshot(),
        irs::DirectoryReader(*dir, nullptr,
                             irs::tests::DefaultReaderOptions()));
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec());
    }

    // add second segment (creating new index_meta file, remove old)
    {
      ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end()));
      writer->RefreshCommit();
      tests::AssertSnapshotEquality(
        writer->GetSnapshot(),
        irs::DirectoryReader(*dir, nullptr,
                             irs::tests::DefaultReaderOptions()));
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec());
    }

    // delete record from first segment (creating new index_meta file + doc_mask
    // file, remove old)
    {
      writer->GetBatch().Remove(*query_doc1);
      writer->RefreshCommit();
      tests::AssertSnapshotEquality(
        writer->GetSnapshot(),
        irs::DirectoryReader(*dir, nullptr,
                             irs::tests::DefaultReaderOptions()));
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec());
    }

    // delete all record from first segment (creating new index_meta file,
    // remove old meta + unused segment)
    {
      writer->GetBatch().Remove(*query_doc2);
      writer->RefreshCommit();
      tests::AssertSnapshotEquality(
        writer->GetSnapshot(),
        irs::DirectoryReader(*dir, nullptr,
                             irs::tests::DefaultReaderOptions()));
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec());
    }

    // delete all records from second segment (creating new index_meta file,
    // remove old meta + unused segment)
    {
      writer->GetBatch().Remove(*query_doc2);
      writer->RefreshCommit();
      tests::AssertSnapshotEquality(
        writer->GetSnapshot(),
        irs::DirectoryReader(*dir, nullptr,
                             irs::tests::DefaultReaderOptions()));
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec());
    }
  }

  files.clear();
  ASSERT_TRUE(dir->visit(list_files));

  // reset directory
  for (auto& file : files) {
    ASSERT_TRUE(dir->remove(file));
  }

  files.clear();
  ASSERT_TRUE(dir->visit(list_files));
  ASSERT_TRUE(files.empty());

  // cleanup on refcount decrement (old files still in use)
  {
    // create writer to directory
    auto writer = irs::IndexWriter::Make(*dir, codec(), irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());

    // initialize directory
    {
      writer->RefreshCommit();
      tests::AssertSnapshotEquality(
        writer->GetSnapshot(),
        irs::DirectoryReader(*dir, nullptr,
                             irs::tests::DefaultReaderOptions()));
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec());
    }

    // add first segment
    {
      ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end()));
      ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end()));
      ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end()));
      writer->RefreshCommit();
      tests::AssertSnapshotEquality(
        writer->GetSnapshot(),
        irs::DirectoryReader(*dir, nullptr,
                             irs::tests::DefaultReaderOptions()));
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec());
    }

    // delete record from first segment (creating new doc_mask file)
    {
      writer->GetBatch().Remove(*query_doc1);
      writer->RefreshCommit();
      tests::AssertSnapshotEquality(
        writer->GetSnapshot(),
        irs::DirectoryReader(*dir, nullptr,
                             irs::tests::DefaultReaderOptions()));
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec());
    }

    // create reader to directory
    auto reader =
      irs::DirectoryReader(*dir, codec(), irs::tests::DefaultReaderOptions());
    std::unordered_set<std::string> reader_files;
    {
      irs::IndexMeta index_meta;
      std::string segments_file;
      auto meta_reader = codec()->get_index_meta_reader();
      const bool exists = meta_reader->last_segments_file(*dir, segments_file);
      ASSERT_TRUE(exists);

      meta_reader->read(*dir, index_meta, segments_file);

      VisitFiles(index_meta, [&reader_files](std::string_view file) {
        reader_files.emplace(file);
        return true;
      });

      reader_files.emplace(segments_file);
    }

    // add second segment (creating new index_meta file, not-removing old)
    {
      ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end()));
      writer->RefreshCommit();
      tests::AssertSnapshotEquality(
        writer->GetSnapshot(),
        irs::DirectoryReader(*dir, nullptr,
                             irs::tests::DefaultReaderOptions()));
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec(), reader_files);
    }

    // delete record from first segment (creating new doc_mask file, not-remove
    // old)
    {
      writer->GetBatch().Remove(*(query_doc2));
      writer->RefreshCommit();
      tests::AssertSnapshotEquality(
        writer->GetSnapshot(),
        irs::DirectoryReader(*dir, nullptr,
                             irs::tests::DefaultReaderOptions()));
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec(), reader_files);
    }

    // delete all record from first segment (creating new index_meta file,
    // remove old meta but leave first segment)
    {
      writer->GetBatch().Remove(*(query_doc3));
      writer->RefreshCommit();
      tests::AssertSnapshotEquality(
        writer->GetSnapshot(),
        irs::DirectoryReader(*dir, nullptr,
                             irs::tests::DefaultReaderOptions()));
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec(), reader_files);
    }

    // delete all records from second segment (creating new index_meta file,
    // remove old meta + unused segment)
    {
      writer->GetBatch().Remove(*(query_doc4));
      writer->RefreshCommit();
      tests::AssertSnapshotEquality(
        writer->GetSnapshot(),
        irs::DirectoryReader(*dir, nullptr,
                             irs::tests::DefaultReaderOptions()));
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec(), reader_files);
    }

    // close reader (remove old meta + old doc_mask + first segment)
    {
      reader = {};
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec());
    }
  }

  files.clear();
  ASSERT_TRUE(dir->visit(list_files));

  // reset directory
  for (auto& file : files) {
    ASSERT_TRUE(dir->remove(file));
  }

  files.clear();
  ASSERT_TRUE(dir->visit(list_files));
  ASSERT_TRUE(files.empty());

  // cleanup on writer startup
  {
    // fill directory
    {
      auto writer = irs::IndexWriter::Make(*dir, codec(), irs::kOmCreate,
                                           irs::tests::DefaultWriterOptions());

      writer->RefreshCommit();  // initialize directory
      tests::AssertSnapshotEquality(
        writer->GetSnapshot(),
        irs::DirectoryReader(*dir, nullptr,
                             irs::tests::DefaultReaderOptions()));
      ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end()));
      writer->RefreshCommit();  // add first segment
      tests::AssertSnapshotEquality(
        writer->GetSnapshot(),
        irs::DirectoryReader(*dir, nullptr,
                             irs::tests::DefaultReaderOptions()));
      ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end()));
      ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end()));
      writer->RefreshCommit();  // add second segment
      tests::AssertSnapshotEquality(
        writer->GetSnapshot(),
        irs::DirectoryReader(*dir, nullptr,
                             irs::tests::DefaultReaderOptions()));
      writer->GetBatch().Remove(*(query_doc1));
      writer->RefreshCommit();  // remove first segment
      tests::AssertSnapshotEquality(
        writer->GetSnapshot(),
        irs::DirectoryReader(*dir, nullptr,
                             irs::tests::DefaultReaderOptions()));
    }

    // add invalid files
    {
      irs::IndexOutput::ptr tmp;
      tmp = dir->create("dummy.file.1");
      ASSERT_FALSE(!tmp);
      tmp = dir->create("dummy.file.2");
      ASSERT_FALSE(!tmp);
    }

    bool exists;
    ASSERT_TRUE(dir->exists(exists, "dummy.file.1") && exists);
    ASSERT_TRUE(dir->exists(exists, "dummy.file.2") && exists);

    // open writer
    auto writer = irs::IndexWriter::Make(*dir, codec(), irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());

    // if directory has files (for fs directory) then ensure only valid
    // meta+segments loaded
    ASSERT_TRUE(dir->exists(exists, "dummy.file.1") && !exists);
    ASSERT_TRUE(dir->exists(exists, "dummy.file.2") && !exists);
    AssertNoDirectoryArtifacts(*dir, *codec());
  }
}

TEST_P(FormatTestCase, fields_seek_ge) {
  static constexpr irs::field_id kFieldId = 1;

  class GranularDoubleField : public tests::DoubleField {};

  class NumericFieldGenerator : public tests::DocGeneratorBase {
   public:
    NumericFieldGenerator(size_t begin, size_t end, size_t step)
      : _value(begin), _begin(begin), _end(end), _step(step) {
      _field = std::make_shared<GranularDoubleField>();
      _field->Name("field");
      _field->id = kFieldId;
      _doc.indexed.push_back(_field);
      _doc.stored.push_back(_field);
    }

    tests::Document* next() final {
      if (_value > _end) {
        return nullptr;
      }

      _field->value(_value += _step);
      return &_doc;
    }

    void reset() final { _value = _begin; }

   private:
    std::shared_ptr<GranularDoubleField> _field;
    tests::Document _doc;
    size_t _value;
    const size_t _begin;
    const size_t _end;
    const size_t _step;
  };

  // add segment
  {
    NumericFieldGenerator gen(75, 7000, 2);
    add_segment(gen, irs::kOmCreate);
  }

  auto reader = open_reader();
  ASSERT_EQ(1, reader->size());
  auto& segment = reader[0];
  auto field = segment.field(kFieldId);
  ASSERT_NE(nullptr, field);

  std::vector<irs::bstring> all_terms;
  all_terms.reserve(field->size());

  // extract all terms
  {
    auto it = field->iterator(irs::SeekMode::NORMAL);
    ASSERT_NE(nullptr, it);

    size_t i = 0;
    while (it->next()) {
      ++i;
      all_terms.emplace_back(it->value());
    }
    ASSERT_EQ(field->size(), i);
    ASSERT_TRUE(std::is_sorted(all_terms.begin(), all_terms.end()));
  }

  // seek_ge to every term
  {
    auto it = field->iterator(irs::SeekMode::NORMAL);
    ASSERT_NE(nullptr, it);
    for (auto& term : all_terms) {
      ASSERT_EQ(irs::SeekResult::Found, it->seek_ge(term));
      ASSERT_EQ(term, it->value());
    }
  }

  // seek_ge before every term
  {
    irs::NumericTokenizer stream;
    auto* term = irs::get<irs::TermAttr>(stream);
    ASSERT_NE(nullptr, term);

    auto it = field->iterator(irs::SeekMode::NORMAL);
    ASSERT_NE(nullptr, it);

    for (size_t begin = 74, end = 7000, step = 2; begin < end; begin += step) {
      stream.reset(double_t(begin));
      ASSERT_TRUE(stream.next());
      ASSERT_EQ(irs::SeekResult::NotFound, it->seek_ge(term->value));

      auto expected_it =
        std::lower_bound(all_terms.begin(), all_terms.end(), term->value,
                         [](const irs::bstring& lhs,
                            const irs::bytes_view& rhs) { return lhs < rhs; });
      ASSERT_NE(all_terms.end(), expected_it);

      while (expected_it != all_terms.end()) {
        ASSERT_EQ(irs::bytes_view(*expected_it), it->value());
        ++expected_it;
        it->next();
      }
      ASSERT_FALSE(it->next());
      ASSERT_EQ(expected_it, all_terms.end());
    }
  }

  // seek to non-existent term in the middle
  {
    const std::vector<std::vector<irs::byte_type>> terms{
      {207},
      {208},
      {208, 191},
      {208, 192, 81},
      {192, 192, 187, 86, 0},
      {192, 192, 187, 88, 0}};

    auto it = field->iterator(irs::SeekMode::NORMAL);

    for (auto& term : terms) {
      const irs::bytes_view target(term.data(), term.size());

      ASSERT_EQ(irs::SeekResult::NotFound, it->seek_ge(target));

      auto expected_it =
        std::lower_bound(all_terms.begin(), all_terms.end(), target,
                         [](const irs::bstring& lhs,
                            const irs::bytes_view& rhs) { return lhs < rhs; });
      ASSERT_NE(all_terms.end(), expected_it);

      while (expected_it != all_terms.end()) {
        ASSERT_EQ(irs::bytes_view(*expected_it), it->value());
        ++expected_it;
        it->next();
      }
      ASSERT_FALSE(it->next());
      ASSERT_EQ(expected_it, all_terms.end());
    }
  }

  // seek to non-existent term
  {
    const irs::byte_type term[]{209, 191};
    const irs::bytes_view target(term, sizeof term);
    const std::vector<std::vector<irs::byte_type>> terms{
      {209},
      {208, 193},
      {208, 192, 188},
      {208, 192, 188},
    };

    auto it = field->iterator(irs::SeekMode::NORMAL);

    for (auto& term : terms) {
      const irs::bytes_view target(term.data(), term.size());
      ASSERT_EQ(irs::SeekResult::End, it->seek_ge(target));
    }
  }
}

TEST_P(FormatTestCase, fields_read_write) {
  /*
    Term dictionary structure:
      BLOCK|7|
        TERM|aaLorem
        TERM|abaLorem
        BLOCK|36|abab
          TERM|Integer
          TERM|Lorem
          TERM|Praesent
          TERM|adipiscing
          TERM|amet
          TERM|cInteger
          TERM|cLorem
          TERM|cPraesent
          TERM|cadipiscing
          TERM|camet
          TERM|cdInteger
          TERM|cdLorem
          TERM|cdPraesent
          TERM|cdamet
          TERM|cddolor
          TERM|cdelit
          TERM|cdenecaodio
          TERM|cdesitaamet
          TERM|cdipsum
          TERM|cdnec
          TERM|cdodio
          TERM|cdolor
          TERM|cdsit
          TERM|celit
          TERM|cipsum
          TERM|cnec
          TERM|codio
          TERM|consectetur
          TERM|csit
          TERM|dolor
          TERM|elit
          TERM|ipsum
          TERM|libero
          TERM|nec
          TERM|odio
          TERM|sit
        TERM|abcaLorem
        BLOCK|32|abcab
          TERM|Integer
          TERM|Lorem
          TERM|Praesent
          TERM|adipiscing
          TERM|amet
          TERM|cInteger
          TERM|cLorem
          TERM|cPraesent
          TERM|camet
          TERM|cdInteger
          TERM|cdLorem
          TERM|cdPraesent
          TERM|cdamet
          TERM|cddolor
          TERM|cdelit
          TERM|cdipsum
          TERM|cdnec
          TERM|cdodio
          TERM|cdolor
          TERM|cdsit
          TERM|celit
          TERM|cipsum
          TERM|cnec
          TERM|codio
          TERM|csit
          TERM|dolor
          TERM|elit
          TERM|ipsum
          TERM|libero
          TERM|nec
          TERM|odio
          TERM|sit
        TERM|abcdaLorem
        BLOCK|30|abcdab
          TERM|Integer
          TERM|Lorem
          TERM|Praesent
          TERM|amet
          TERM|cInteger
          TERM|cLorem
          TERM|cPraesent
          TERM|camet
          TERM|cdInteger
          TERM|cdLorem
          TERM|cdamet
          TERM|cddolor
          TERM|cdelit
          TERM|cdipsum
          TERM|cdnec
          TERM|cdodio
          TERM|cdolor
          TERM|cdsit
          TERM|celit
          TERM|cipsum
          TERM|cnec
          TERM|codio
          TERM|csit
          TERM|dolor
          TERM|elit
          TERM|ipsum
          TERM|libero
          TERM|nec
          TERM|odio
          TERM|sit
   */

  // create sorted && unsorted terms
  typedef std::set<irs::bytes_view> SortedTermsT;
  typedef std::vector<irs::bytes_view> UnsortedTermsT;
  SortedTermsT sorted_terms;
  UnsortedTermsT unsorted_terms;

  tests::JsonDocGenerator gen(
    resource("fst_prefixes.json"),
    [&sorted_terms, &unsorted_terms](
      tests::Document& doc, const std::string& name,
      const tests::JsonDocGenerator::JsonValue& data) {
      doc.insert(std::make_shared<tests::StringField>(name, data.str));

      auto ref = irs::ViewCast<irs::byte_type>(
        (doc.indexed.end() - 1).as<tests::StringField>().value());
      sorted_terms.emplace(ref);
      unsorted_terms.emplace_back(ref);
    });

  // define field
  constexpr irs::field_id kFieldId = 1;
  irs::FieldMeta field;
  field.id = kFieldId;
  field.norm = 5;

  // write fields
  {
    irs::FlushState state{
      .dir = &dir(),
      .name = "segment_name",
      .doc_count = 100,
      .index_features = field.index_features,
    };

    // should use sorted terms on write
    Terms<SortedTermsT::iterator> terms(sorted_terms.begin(),
                                        sorted_terms.end());
    tests::MockTermReader term_reader{
      terms, irs::FieldMeta{field.id, field.index_features},
      (sorted_terms.empty() ? irs::bytes_view{} : *sorted_terms.begin()),
      (sorted_terms.empty() ? irs::bytes_view{} : *sorted_terms.rbegin())};

    irs::IdxWriter idx{dir(), "segment_name",
                       ::sdb::DuckDBEngine::Instance().instance()};
    irs::burst_trie::FieldWriter writer{
      codec()->get_postings_writer(/*compaction=*/false,
                                   irs::IResourceManager::gNoop),
      /*compaction=*/false, irs::IResourceManager::gNoop};
    writer.SetIdxWriter(idx);
    writer.prepare(state);
    writer.write(term_reader);
    writer.end();
    idx.Commit();
  }

  // read field
  {
    irs::SegmentMeta meta;
    meta.name = "segment_name";

    irs::IdxReader idx{dir(), "segment_name"};
    irs::burst_trie::FieldReader reader_obj{codec()->get_postings_reader(),
                                            irs::IResourceManager::gNoop};
    auto* reader = &reader_obj;
    reader->prepare(
      irs::ReaderState{.dir = &dir(), .meta = &meta, .idx = &idx});
    ASSERT_EQ(1, reader->size());

    // check terms
    ASSERT_EQ(nullptr, reader->field(static_cast<irs::field_id>(999)));
    auto term_reader = reader->field(field.id);
    ASSERT_NE(nullptr, term_reader);
    ASSERT_EQ(field.id, term_reader->meta().id);
    ASSERT_EQ(field.index_features, term_reader->meta().index_features);

    ASSERT_EQ(sorted_terms.size(), term_reader->size());
    ASSERT_EQ(*sorted_terms.begin(), (term_reader->min)());
    ASSERT_EQ(*sorted_terms.rbegin(), (term_reader->max)());

    // check terms using "next"
    {
      auto expected_term = sorted_terms.begin();
      auto term = term_reader->iterator(irs::SeekMode::NORMAL);
      for (; term->next(); ++expected_term) {
        ASSERT_EQ(*expected_term, term->value());
      }
      ASSERT_EQ(sorted_terms.end(), expected_term);
      ASSERT_FALSE(term->next());
    }

    // check terms using single "seek"
    {
      auto expected_sorted_term = sorted_terms.begin();
      for (auto end = sorted_terms.end(); expected_sorted_term != end;
           ++expected_sorted_term) {
        auto term = term_reader->iterator(irs::SeekMode::RandomOnly);
        ASSERT_NE(nullptr, term);
        auto* meta = irs::get<irs::TermMeta>(*term);
        ASSERT_NE(nullptr, meta);
        ASSERT_TRUE(term->seek(*expected_sorted_term));
        ASSERT_EQ(*expected_sorted_term, term->value());
        ASSERT_NO_THROW(term->read());
        ASSERT_THROW(term->next(), irs::NotSupported);
        ASSERT_THROW(term->seek_ge(*expected_sorted_term), irs::NotSupported);
        auto cookie = term->cookie();
        ASSERT_NE(nullptr, cookie);
        {
          auto* meta_from_cookie = irs::get<irs::TermMeta>(*cookie);
          ASSERT_NE(nullptr, meta_from_cookie);
          ASSERT_EQ(meta->docs_count, meta_from_cookie->docs_count);
          ASSERT_EQ(meta->freq, meta_from_cookie->freq);
        }
      }
    }

    // check terms using single "seek"
    {
      auto expected_sorted_term = sorted_terms.begin();
      for (auto end = sorted_terms.end(); expected_sorted_term != end;
           ++expected_sorted_term) {
        auto term = term_reader->iterator(irs::SeekMode::NORMAL);
        ASSERT_TRUE(term->seek(*expected_sorted_term));
        ASSERT_EQ(*expected_sorted_term, term->value());
      }
    }

    // check sorted terms using multiple "seek"s on single iterator
    {
      auto expected_term = sorted_terms.begin();
      auto term = term_reader->iterator(irs::SeekMode::NORMAL);
      for (auto end = sorted_terms.end(); expected_term != end;
           ++expected_term) {
        ASSERT_TRUE(term->seek(*expected_term));

        /* seek to the same term */
        ASSERT_TRUE(term->seek(*expected_term));
        ASSERT_EQ(*expected_term, term->value());
      }
    }

    // check sorted terms in reverse order using multiple "seek"s on single
    // iterator
    {
      auto expected_term = sorted_terms.rbegin();
      auto term = term_reader->iterator(irs::SeekMode::NORMAL);
      for (auto end = sorted_terms.rend(); expected_term != end;
           ++expected_term) {
        ASSERT_TRUE(term->seek(*expected_term));

        /* seek to the same term */
        ASSERT_TRUE(term->seek(*expected_term));
        ASSERT_EQ(*expected_term, term->value());
      }
    }

    // check unsorted terms using multiple "seek"s on single iterator
    {
      auto expected_term = unsorted_terms.begin();
      auto term = term_reader->iterator(irs::SeekMode::NORMAL);
      for (auto end = unsorted_terms.end(); expected_term != end;
           ++expected_term) {
        ASSERT_TRUE(term->seek(*expected_term));

        /* seek to the same term */
        ASSERT_TRUE(term->seek(*expected_term));
        ASSERT_EQ(*expected_term, term->value());
      }
    }

    // ensure term is not invalidated during consequent unsuccessful seeks
    {
      constexpr std::pair<std::string_view, std::string_view> kTerms[]{
        {"abcabamet", "abcabamet"},
        {"abcabrit", "abcabsit"},
        {"abcabzit", "abcabsit"},
        {"abcabelit", "abcabelit"}};

      auto term = term_reader->iterator(irs::SeekMode::NORMAL);
      for (const auto& [seek_term, expected_term] : kTerms) {
        ASSERT_EQ(seek_term == expected_term,
                  term->seek(irs::ViewCast<irs::byte_type>(seek_term)));
        ASSERT_EQ(irs::ViewCast<irs::byte_type>(expected_term), term->value());
      }
    }

    // seek to nil (the smallest possible term)
    {
      (void)1;  // format work-around
      // with state
      {
        auto term = term_reader->iterator(irs::SeekMode::NORMAL);
        ASSERT_FALSE(term->seek(irs::bytes_view{}));
        ASSERT_EQ((term_reader->min)(), term->value());
        ASSERT_EQ(irs::SeekResult::NotFound, term->seek_ge(irs::bytes_view{}));
        ASSERT_EQ((term_reader->min)(), term->value());
      }

      // without state
      {
        auto term = term_reader->iterator(irs::SeekMode::NORMAL);
        ASSERT_FALSE(term->seek(irs::bytes_view{}));
        ASSERT_EQ((term_reader->min)(), term->value());
      }

      {
        auto term = term_reader->iterator(irs::SeekMode::NORMAL);
        ASSERT_EQ(irs::SeekResult::NotFound, term->seek_ge(irs::bytes_view{}));
        ASSERT_EQ((term_reader->min)(), term->value());
      }
    }

    /* Here is the structure of blocks:
     *   TERM aaLorem
     *   TERM abaLorem
     *   BLOCK abab ------> Integer
     *                      ...
     *                      ...
     *   TERM abcaLorem
     *   ...
     *
     * Here we seek to "abaN" and since first entry that
     * is greater than "abaN" is BLOCK entry "abab".
     *
     * In case of "seek" we end our scan on BLOCK entry "abab",
     * and further "next" cause the skipping of the BLOCK "abab".
     *
     * In case of "seek_next" we also end our scan on BLOCK entry "abab"
     * but furher "next" get us to the TERM "ababInteger" */
    {
      auto seek_term = irs::ViewCast<irs::byte_type>(std::string_view("abaN"));
      auto seek_result =
        irs::ViewCast<irs::byte_type>(std::string_view("ababInteger"));

      /* seek exactly to term */
      {
        auto term = term_reader->iterator(irs::SeekMode::NORMAL);
        ASSERT_FALSE(term->seek(seek_term));
        /* we on the BLOCK "abab" */
        ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("abab")),
                  term->value());
        ASSERT_TRUE(term->next());
        ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("abcaLorem")),
                  term->value());
      }

      /* seek to term which is equal or greater than current */
      {
        auto term = term_reader->iterator(irs::SeekMode::NORMAL);
        ASSERT_EQ(irs::SeekResult::NotFound, term->seek_ge(seek_term));
        ASSERT_EQ(seek_result, term->value());

        /* iterate over the rest of the terms */
        auto expected_sorted_term = sorted_terms.find(seek_result);
        ASSERT_NE(sorted_terms.end(), expected_sorted_term);
        for (++expected_sorted_term; term->next(); ++expected_sorted_term) {
          ASSERT_EQ(*expected_sorted_term, term->value());
        }
        ASSERT_FALSE(term->next());
        ASSERT_EQ(sorted_terms.end(), expected_sorted_term);
      }
    }

  }  // namespace tests
}

TEST_P(FormatTestCase, segment_meta_read_write) {
  // read valid meta
  {
    irs::SegmentMeta meta;
    meta.name = "meta_name";
    meta.docs_count = 453;
    meta.live_docs_count = 451;
    meta.byte_size = 666;
    meta.version = 100;
    meta.docs_mask = std::make_shared<irs::DocumentMask>([&] {
      irs::DocumentMask docs_mask{irs::IResourceManager::gNoop};
      docs_mask.insert({42, 100});
      return docs_mask;
    }());
    meta.files.emplace_back("file1");
    meta.files.emplace_back("index_file2");
    meta.files.emplace_back("file3");
    meta.files.emplace_back("stored_file4");

    std::string filename;

    // write segment meta
    {
      auto writer = codec()->get_segment_meta_writer();
      writer->write(dir(), filename, meta);
    }

    // read segment meta
    {
      irs::SegmentMeta read_meta;
      read_meta.name = meta.name;
      read_meta.version = 100;

      auto reader = codec()->get_segment_meta_reader();
      reader->read(dir(), read_meta);
      ASSERT_EQ(meta.codec, read_meta.codec);  // codec stays nullptr
      ASSERT_EQ(meta.name, read_meta.name);
      ASSERT_EQ(meta.docs_count, read_meta.docs_count);
      ASSERT_EQ(meta.live_docs_count, read_meta.live_docs_count);
      ASSERT_EQ(meta.version, read_meta.version);
      ASSERT_EQ(meta.byte_size, read_meta.byte_size);
      ASSERT_EQ(meta.files, read_meta.files);
      ASSERT_EQ(*meta.docs_mask, *read_meta.docs_mask);
    }
  }

  // write broken meta (live_docs_count > docs_count)
  {
    irs::SegmentMeta meta;
    meta.name = "broken_meta_name";
    meta.docs_count = 453;
    meta.live_docs_count = 1345;
    meta.byte_size = 666;
    meta.version = 100;

    meta.files.emplace_back("file1");
    meta.files.emplace_back("index_file2");
    meta.files.emplace_back("file3");
    meta.files.emplace_back("stored_file4");

    std::string filename;

    // write segment meta
    {
      auto writer = codec()->get_segment_meta_writer();
      ASSERT_THROW(writer->write(dir(), filename, meta), irs::IndexError);
    }

    // read segment meta
    {
      irs::SegmentMeta read_meta;
      read_meta.name = meta.name;
      read_meta.version = 100;

      auto reader = codec()->get_segment_meta_reader();
      ASSERT_THROW(reader->read(dir(), read_meta), irs::IoError);
    }
  }

  {
    irs::SegmentMeta meta;
    meta.name = "broken_meta_name";
    meta.docs_count = 1600;
    meta.live_docs_count = 1345;
    meta.byte_size = 666;
    meta.version = 100;

    meta.files.emplace_back("file1");
    meta.files.emplace_back("index_file2");
    meta.files.emplace_back("file3");
    meta.files.emplace_back("stored_file4");

    std::string filename;

    // write segment meta
    {
      auto writer = codec()->get_segment_meta_writer();
      ASSERT_THROW(writer->write(dir(), filename, meta), irs::IndexError);
    }

    // read segment meta
    {
      irs::SegmentMeta read_meta;
      read_meta.name = meta.name;
      read_meta.version = 100;

      auto reader = codec()->get_segment_meta_reader();
      ASSERT_THROW(reader->read(dir(), read_meta), irs::IoError);
    }
  }

  // read broken meta (live_docs_count > docs_count)
  {
    class SegmentMetaCorruptingDirectory : public irs::Directory {
     public:
      SegmentMetaCorruptingDirectory(irs::Directory& dir,
                                     irs::SegmentMeta& meta)
        : Directory{dir.ResourceManager()}, _dir(dir), _meta(meta) {}

      using Directory::attributes;

      irs::DirectoryAttributes& attributes() noexcept final {
        return _dir.attributes();
      }

      irs::IndexOutput::ptr create(std::string_view name) noexcept final {
        // corrupt meta before writing it
        _meta.docs_count = _meta.live_docs_count - 1;
        return _dir.create(name);
      }

      bool exists(bool& result, std::string_view name) const noexcept final {
        return _dir.exists(result, name);
      }

      bool length(uint64_t& result,
                  std::string_view name) const noexcept final {
        return _dir.length(result, name);
      }

      irs::IndexLock::ptr make_lock(std::string_view name) noexcept final {
        return _dir.make_lock(name);
      }

      bool mtime(std::time_t& result,
                 std::string_view name) const noexcept final {
        return _dir.mtime(result, name);
      }

      irs::IndexInput::ptr open(std::string_view name,
                                irs::IOAdvice advice) const noexcept final {
        return _dir.open(name, advice);
      }

      bool remove(std::string_view name) noexcept final {
        return _dir.remove(name);
      }

      bool rename(std::string_view src, std::string_view dst) noexcept final {
        return _dir.rename(src, dst);
      }

      bool sync(std::span<const std::string_view> files) noexcept final {
        return _dir.sync(files);
      }

      bool visit(const irs::Directory::visitor_f& visitor) const final {
        return _dir.visit(visitor);
      }

     private:
      irs::Directory& _dir;
      irs::SegmentMeta& _meta;
    };

    irs::SegmentMeta meta;
    irs::DocumentMask docs_mask{{irs::IResourceManager::gNoop}};
    docs_mask.insert({42, 100, 200});
    meta.name = "broken_meta_name";
    meta.docs_count = 1453;
    meta.live_docs_count = 1451;
    meta.byte_size = 666;
    meta.version = 100;

    meta.files.emplace_back("file1");
    meta.files.emplace_back("index_file2");
    meta.files.emplace_back("file3");
    meta.files.emplace_back("stored_file4");

    std::string filename;

    // write segment meta
    {
      auto writer = codec()->get_segment_meta_writer();

      SegmentMetaCorruptingDirectory currupting_dir(dir(), meta);
      ASSERT_THROW(writer->write(currupting_dir, filename, meta),
                   irs::IndexError);
    }

    // read segment meta
    {
      irs::SegmentMeta read_meta;
      read_meta.name = meta.name;
      read_meta.version = 100;

      auto reader = codec()->get_segment_meta_reader();
      ASSERT_THROW(reader->read(dir(), read_meta), irs::IoError);
    }
  }
}

TEST_P(FormatTestCase, format_utils_checksum) {
  {
    auto stream = dir().create("file");
    ASSERT_NE(nullptr, stream);
    irs::format_utils::WriteHeader(*stream, "test", 42);
    irs::format_utils::WriteFooter(*stream);
  }

  {
    auto stream = dir().open("file", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);

    int64_t expected_checksum;
    {
      auto dup = stream->Dup();
      ASSERT_NE(nullptr, dup);
      expected_checksum = dup->Checksum(dup->Length() - sizeof(int64_t));
    }

    ASSERT_EQ(expected_checksum, irs::format_utils::Checksum(*stream));
  }

  {
    auto stream = dir().create("empty_file");
    ASSERT_NE(nullptr, stream);
  }

  {
    auto stream = dir().open("empty_file", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);
    ASSERT_THROW(irs::format_utils::Checksum(*stream), irs::IndexError);
  }
}

TEST_P(FormatTestCase, format_utils_header_footer) {
  {
    auto stream = dir().create("file");
    ASSERT_NE(nullptr, stream);
    irs::format_utils::WriteHeader(*stream, "test", 42);
    irs::format_utils::WriteFooter(*stream);
  }

  {
    auto stream = dir().open("file", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);

    int64_t expected_checksum;
    {
      auto dup = stream->Dup();
      ASSERT_NE(nullptr, dup);
      expected_checksum = dup->Checksum(dup->Length() - sizeof(int64_t));
    }

    ASSERT_EQ(42, irs::format_utils::CheckHeader(*stream, "test", 41, 43));
    ASSERT_EQ(expected_checksum,
              irs::format_utils::CheckFooter(*stream, expected_checksum));
  }

  {
    auto stream = dir().open("file", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(42, irs::format_utils::CheckHeader(*stream, "test", 41, 43));
    ASSERT_THROW(irs::format_utils::CheckFooter(*stream, 0), irs::IndexError);
  }

  {
    auto stream = dir().open("file", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);
    ASSERT_THROW(irs::format_utils::CheckHeader(*stream, "invalid", 41, 43),
                 irs::IndexError);
  }

  {
    auto stream = dir().open("file", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);
    ASSERT_THROW(irs::format_utils::CheckHeader(*stream, "test", 43, 43),
                 irs::IndexError);
  }

  {
    auto stream = dir().create("empty_file");
    ASSERT_NE(nullptr, stream);
  }

  {
    auto stream = dir().open("empty_file", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);
    ASSERT_THROW(irs::format_utils::CheckHeader(*stream, "invalid", 41, 43),
                 irs::IndexError);
  }
}

TEST_P(FormatTestCaseWithEncryption, read_zero_block_encryption) {
  if (!supports_encryption()) {
    return;
  }

  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  const tests::Document* doc1 = gen.next();

  // replace encryption
  ASSERT_NE(nullptr, dir().attributes().encryption());

  // write segment with format10
  {
    auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end()));

    ASSERT_TRUE(writer->RefreshCommit());
    AssertSnapshotEquality(*writer);
  }

  // replace encryption
  dir().attributes() =
    irs::DirectoryAttributes{std::make_unique<tests::Rot13Encryption>(6)};

  // can't open encrypted index without encryption
  ASSERT_THROW(
    irs::DirectoryReader(dir(), nullptr, irs::tests::DefaultReaderOptions()),
    irs::IndexError);
}

TEST_P(FormatTestCaseWithEncryption, fields_read_write_wrong_encryption) {
  if (!supports_encryption()) {
    return;
  }

  // create sorted && unsorted terms
  typedef std::set<irs::bytes_view> SortedTermsT;
  typedef std::vector<irs::bytes_view> UnsortedTermsT;
  SortedTermsT sorted_terms;
  UnsortedTermsT unsorted_terms;

  tests::JsonDocGenerator gen(
    resource("fst_prefixes.json"),
    [&sorted_terms, &unsorted_terms](
      tests::Document& doc, const std::string& name,
      const tests::JsonDocGenerator::JsonValue& data) {
      doc.insert(std::make_shared<tests::StringField>(name, data.str));

      auto ref = irs::ViewCast<irs::byte_type>(
        (doc.indexed.end() - 1).as<tests::StringField>().value());
      sorted_terms.emplace(ref);
      unsorted_terms.emplace_back(ref);
    });

  // define field
  constexpr irs::field_id kFieldId = 1;
  irs::FieldMeta field;
  field.id = kFieldId;
  field.norm = 5;

  ASSERT_NE(nullptr, dir().attributes().encryption());

  // write fields
  {
    irs::FlushState state{
      .dir = &dir(),
      .name = "segment_name",
      .doc_count = 100,
    };

    // should use sorted terms on write
    tests::FormatTestCase::Terms<SortedTermsT::iterator> terms(
      sorted_terms.begin(), sorted_terms.end());
    tests::MockTermReader term_reader{
      terms, irs::FieldMeta{field.id, field.index_features},
      (sorted_terms.empty() ? irs::bytes_view{} : *sorted_terms.begin()),
      (sorted_terms.empty() ? irs::bytes_view{} : *sorted_terms.rbegin())};

    irs::IdxWriter idx{dir(), "segment_name",
                       ::sdb::DuckDBEngine::Instance().instance()};
    irs::burst_trie::FieldWriter writer{
      codec()->get_postings_writer(/*compaction=*/false,
                                   irs::IResourceManager::gNoop),
      /*compaction=*/false, irs::IResourceManager::gNoop};
    writer.SetIdxWriter(idx);
    writer.prepare(state);
    writer.write(term_reader);
    writer.end();
    idx.Commit();
  }

  irs::SegmentMeta meta;
  meta.name = "segment_name";

  // Open-with-wrong-cipher / open-encrypted-without-cipher must throw:
  // `IdxReader` opens the `.idx` and consults
  // `dir.attributes().encryption()`, so the failure surfaces from
  // `IdxReader` construction (`FieldReader::prepare` then reads
  // already-decrypted bytes from that stream).

  // can't open encrypted index without encryption
  dir().attributes() = irs::DirectoryAttributes{nullptr};
  ASSERT_THROW(irs::IdxReader(dir(), "segment_name"), irs::IndexError);

  // can't open encrypted index with wrong encryption
  dir().attributes() =
    irs::DirectoryAttributes{std::make_unique<tests::Rot13Encryption>(6)};
  ASSERT_THROW(irs::IdxReader(dir(), "segment_name"), irs::IndexError);
}

TEST_P(FormatTestCaseWithEncryption, open_ecnrypted_with_wrong_encryption) {
  if (!supports_encryption()) {
    return;
  }

  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  const tests::Document* doc1 = gen.next();

  ASSERT_NE(nullptr, dir().attributes().encryption());

  {
    auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end()));

    ASSERT_TRUE(writer->RefreshCommit());
    AssertSnapshotEquality(*writer);
  }

  // can't open encrypted index with wrong encryption
  dir().attributes() =
    irs::DirectoryAttributes{std::make_unique<tests::Rot13Encryption>(6)};
  ASSERT_THROW(
    irs::DirectoryReader(dir(), nullptr, irs::tests::DefaultReaderOptions()),
    irs::IndexError);
}

TEST_P(FormatTestCaseWithEncryption, open_ecnrypted_with_non_encrypted) {
  if (!supports_encryption()) {
    return;
  }

  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  const tests::Document* doc1 = gen.next();

  ASSERT_NE(nullptr, dir().attributes().encryption());

  {
    auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end()));

    ASSERT_TRUE(writer->RefreshCommit());
    AssertSnapshotEquality(*writer);
  }

  // remove encryption
  dir().attributes() = irs::DirectoryAttributes{nullptr};

  // can't open encrypted index without encryption
  ASSERT_THROW(
    irs::DirectoryReader(dir(), nullptr, irs::tests::DefaultReaderOptions()),
    irs::IndexError);
}

TEST_P(FormatTestCaseWithEncryption, open_non_ecnrypted_with_encrypted) {
  if (!supports_encryption()) {
    return;
  }

  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  const tests::Document* doc1 = gen.next();

  dir().attributes() = irs::DirectoryAttributes{nullptr};

  // write segment with format11
  {
    auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end()));

    ASSERT_TRUE(writer->RefreshCommit());
    AssertSnapshotEquality(*writer);
  }

  // add cipher
  dir().attributes() =
    irs::DirectoryAttributes{std::make_unique<tests::Rot13Encryption>(7)};

  // check index
  auto index =
    irs::DirectoryReader(dir(), nullptr, irs::tests::DefaultReaderOptions());
  ASSERT_TRUE(index);
  ASSERT_EQ(1, index->size());
  ASSERT_EQ(1, index->docs_count());
  ASSERT_EQ(1, index->live_docs_count());

  // check segment 0
  {
    auto& segment = index[0];
    ASSERT_EQ(1, segment.size());
    ASSERT_EQ(1, segment.docs_count());
    ASSERT_EQ(1, segment.live_docs_count());

    auto terms = segment.field(tests::FieldIdFor("same"));
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());

    size_t hits = 0;
    for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
         docs_itr->next();) {
      ++hits;
    }
    ASSERT_EQ(1, hits);
  }
}

}  // namespace tests

// --- columns_* coverage -------------------------------------------------
// `irs::ColWriter` (one `.col` file per segment, independent of
// the per-format codec) is the column-data substrate the tests below
// exercise. The two `FormatTestCaseWithEncryption` columnstore tests are
// GTEST_SKIP'd pending the encryption follow-up.

namespace {

// Helper: open a fresh writer, run `populate`, commit, then run `verify`
// against a newly-opened reader.
template<typename Populate, typename Verify>
void RoundTrip(std::string_view segment_name, uint64_t row_count,
               Populate&& populate, Verify&& verify) {
  irs::MemoryDirectory dir;
  auto& db = ::sdb::DuckDBEngine::Instance().instance();
  {
    irs::ColWriter w{dir, segment_name, db};
    populate(w);
    auto filename = w.Commit(row_count);
    ASSERT_FALSE(filename.empty());
  }
  irs::ColReader r{dir, segment_name, db};
  verify(r);
}

}  // namespace
namespace tests {

TEST_P(FormatTestCase, columns_rw_empty) {
  // Sub-scenario 1: writer commit with no columns at all -- still
  // produces a footer file. Reader sees an empty Columns() span and
  // reports no column ids.
  RoundTrip(
    "columns_rw_empty_no_cols", /*row_count=*/0,
    [](irs::ColWriter&) { /* no columns */ },
    [&](const irs::ColReader& r) {
      EXPECT_TRUE(r.Columns().empty());
      EXPECT_FALSE(r.HasColumn(0));
      EXPECT_FALSE(r.HasColumn(1));
      EXPECT_FALSE(r.HasColumn(irs::field_limits::invalid()));
      EXPECT_EQ(r.Column(0), nullptr);
      EXPECT_EQ(r.Column(1), nullptr);
      EXPECT_EQ(r.Column(irs::field_limits::invalid()), nullptr);
    });

  // Sub-scenario 2: two columns pushed, neither written into. Commit
  // still records both ids at zero RowCount; post-condition is that
  // both ids are present (so "missing" lookups for *other* ids still
  // return nullptr) but RowCount is 0. Also covers wrong-column-id
  // negative lookups across a range of ids.
  RoundTrip(
    "columns_rw_empty_two_cols", /*row_count=*/0,
    [&](irs::ColWriter& w) {
      irs::tests::OpenBlobColumn(w, /*id=*/0);
      irs::tests::OpenBlobColumn(w, /*id=*/1);
    },
    [&](const irs::ColReader& r) {
      ASSERT_EQ(r.Columns().size(), 2u);
      ASSERT_TRUE(r.HasColumn(0));
      ASSERT_TRUE(r.HasColumn(1));
      // Empty columns: id 0 and id 1 only, anything else should be missing.
      EXPECT_FALSE(r.HasColumn(2));
      EXPECT_EQ(r.Column(2), nullptr);
      EXPECT_EQ(r.Column(irs::field_limits::invalid()), nullptr);
      // Wrong-column-id sweep: ids 2..50 + a few large ones should all miss.
      for (irs::field_id missing :
           {2, 3, 5, 10, 50, 99, 1000, 10000, 0x7fffffff}) {
        EXPECT_FALSE(r.HasColumn(missing)) << "id=" << missing;
        EXPECT_EQ(r.Column(missing), nullptr) << "id=" << missing;
      }
      const auto* c0 = r.Column(0);
      const auto* c1 = r.Column(1);
      ASSERT_NE(c0, nullptr);
      ASSERT_NE(c1, nullptr);
      EXPECT_EQ(c0->RowCount(), 0u);
      EXPECT_EQ(c1->RowCount(), 0u);
      // Boundary check on an empty column: IsNullRow at row 0 and past-end
      // does not crash and produces a sensible "no value" answer. PointReader
      // is safe to construct over an empty column; the codec's Locate is
      // expected to return an empty window for any row past RowCount().
      // We don't fetch anything (FetchRow on an empty column is UB by
      // contract), but constructing the reader and not using it must succeed.
      irs::ColumnReader::BlobPointReader pr0{r, *c0};
      irs::ColumnReader::BlobPointReader pr1{r, *c1};
      (void)pr0;
      (void)pr1;
    });

  // Sub-scenario 3: a non-empty column alongside an empty one. Verifies
  // that empty-column lookups + per-id negative lookups still behave as
  // expected when the segment has at least one populated column. This
  // anchors the "wrong-column-id" axis on a real, multi-column segment.
  RoundTrip(
    "columns_rw_empty_mixed", /*row_count=*/4,
    [&](irs::ColWriter& w) {
      irs::tests::OpenBlobColumn(w, /*id=*/0);  // never written
      auto& cw1 = irs::tests::OpenBlobColumn(w, /*id=*/1);
      for (irs::doc_id_t doc = irs::doc_limits::min();
           doc < irs::doc_limits::min() + 4; ++doc) {
        irs::tests::AppendBlob(cw1, doc, irs::bytes_view{});
      }
      irs::tests::OpenBlobColumn(w, /*id=*/2);  // never written
    },
    [&](const irs::ColReader& r) {
      ASSERT_EQ(r.Columns().size(), 3u);
      ASSERT_TRUE(r.HasColumn(0));
      ASSERT_TRUE(r.HasColumn(1));
      ASSERT_TRUE(r.HasColumn(2));
      // Wrong-column-id negative lookups.
      EXPECT_FALSE(r.HasColumn(3));
      EXPECT_FALSE(r.HasColumn(100));
      EXPECT_FALSE(r.HasColumn(irs::field_limits::invalid()));
      EXPECT_EQ(r.Column(3), nullptr);
      EXPECT_EQ(r.Column(100), nullptr);
      EXPECT_EQ(r.Column(irs::field_limits::invalid()), nullptr);
      const auto* c1 = r.Column(1);
      ASSERT_NE(c1, nullptr);
      EXPECT_EQ(c1->RowCount(), 4u);
      // Empty columns coexist with the populated one.
      const auto* c0 = r.Column(0);
      const auto* c2 = r.Column(2);
      ASSERT_NE(c0, nullptr);
      ASSERT_NE(c2, nullptr);
      EXPECT_EQ(c0->RowCount(), 0u);
      EXPECT_EQ(c2->RowCount(), 0u);
    });
}

TEST_P(FormatTestCase, columns_rw) {
  // columns_rw covers:
  //  * 2 segments (a Writer is single-shot, so we run two independent
  //    writers),
  //  * each segment with multiple columns,
  //  * an empty-in-the-middle column (gap),
  //  * multi-valued payloads (length-prefixed WriteStr concatenation),
  //  * "wrong column / wrong doc / can't find" lookups,
  //  * visit + partial-visit (early-stop) checks.
  irs::MemoryDirectory dir;
  auto& db = ::sdb::DuckDBEngine::Instance().instance();

  // Build segment _1 -----------------------------------------------------
  // ids in this segment: 0,1,2 (empty),3,4,5; segment row count = 34.
  static constexpr uint64_t kSeg1Rows = 34;
  static constexpr irs::field_id kF0 = 0;
  static constexpr irs::field_id kF1 = 1;
  static constexpr irs::field_id kEmpty = 2;
  static constexpr irs::field_id kF2 = 3;
  static constexpr irs::field_id kF3 = 4;
  static constexpr irs::field_id kF4 = 5;

  auto write_str = [](irs::bstring& buf, std::string_view s) {
    irs::tests::BstringDataOutput out{buf};
    irs::WriteStr(out, s);
  };
  auto append_str_blob = [&](irs::ColumnWriter& cw, irs::doc_id_t doc,
                             std::string_view s) {
    irs::bstring buf;
    write_str(buf, s);
    irs::tests::AppendBlob(cw, doc, {buf.data(), buf.size()});
  };
  auto append_two_str_blob = [&](irs::ColumnWriter& cw, irs::doc_id_t doc,
                                 std::string_view a, std::string_view b) {
    irs::bstring buf;
    irs::tests::BstringDataOutput out{buf};
    irs::WriteStr(out, a);
    irs::WriteStr(out, b);
    irs::tests::AppendBlob(cw, doc, {buf.data(), buf.size()});
  };

  {
    irs::ColWriter w{dir, "_1", db};
    auto& f0 = irs::tests::OpenBlobColumn(w, kF0);
    auto& f1 = irs::tests::OpenBlobColumn(w, kF1);
    irs::tests::OpenBlobColumn(w, kEmpty);  // empty column in the middle
    auto& f2 = irs::tests::OpenBlobColumn(w, kF2);
    irs::tests::OpenBlobColumn(w, kF3);  // never populated
    auto& f4 = irs::tests::OpenBlobColumn(w, kF4);

    // f0 has rows at doc=1, 2, 33
    append_str_blob(f0, /*doc=*/1, "field0_doc0");
    f0.PadNullsTo(1);  // pad to row index 1 (no-op, just exercising the API)
    append_str_blob(f0, /*doc=*/2, "field0_doc2");
    f0.PadNullsTo(32);  // null-pad up to (but not including) row 32
    append_str_blob(f0, /*doc=*/33, "field0_doc33");
    f0.PadNullsTo(kSeg1Rows);

    // f1 has rows at doc=1, 12 -- multi-valued payloads (two WriteStr each)
    append_two_str_blob(f1, /*doc=*/1, "field1_doc0", "field1_doc0_1");
    f1.PadNullsTo(11);
    append_two_str_blob(f1, /*doc=*/12, "field1_doc12_1", "field1_doc12_2");
    f1.PadNullsTo(kSeg1Rows);

    // f2 has a single row at doc=1
    append_str_blob(f2, /*doc=*/1, "field2_doc1");
    f2.PadNullsTo(kSeg1Rows);

    // f4 has a single row at doc=1
    append_str_blob(f4, /*doc=*/1, "field4_doc_min");
    f4.PadNullsTo(kSeg1Rows);

    auto filename = w.Commit(kSeg1Rows);
    ASSERT_FALSE(filename.empty());
  }

  // Build segment _2 -----------------------------------------------------
  static constexpr uint64_t kSeg2Rows = 13;
  static constexpr irs::field_id kS2F0 = 0;
  static constexpr irs::field_id kS2F1 = 1;
  static constexpr irs::field_id kS2F2 = 2;
  {
    irs::ColWriter w{dir, "_2", db};
    auto& f0 = irs::tests::OpenBlobColumn(w, kS2F0);
    irs::tests::OpenBlobColumn(w, kS2F1);  // pushed but never written
    auto& f2 = irs::tests::OpenBlobColumn(w, kS2F2);

    // f0 has rows at doc=1, 12.
    append_str_blob(f0, /*doc=*/1, "segment_2_field1_doc0");
    f0.PadNullsTo(11);
    append_str_blob(f0, /*doc=*/12, "segment_2_field1_doc12");
    f0.PadNullsTo(kSeg2Rows);

    // f2 has only a row at doc=1.
    append_str_blob(f2, /*doc=*/1, "segment_2_field3_doc0");
    f2.PadNullsTo(kSeg2Rows);

    auto filename = w.Commit(kSeg2Rows);
    ASSERT_FALSE(filename.empty());
  }

  // Read segment _1 ------------------------------------------------------
  {
    irs::ColReader r{dir, "_1", db};
    ASSERT_EQ(r.Columns().size(), 6u);
    // Try to get an invalid column id (`field_limits::invalid()`).
    EXPECT_EQ(r.Column(irs::field_limits::invalid()), nullptr);
    EXPECT_FALSE(r.HasColumn(irs::field_limits::invalid()));
    // Try an out-of-range column id.
    EXPECT_EQ(r.Column(99), nullptr);
    EXPECT_FALSE(r.HasColumn(99));

    // Check f4 (single doc).
    {
      const auto* col = r.Column(kF4);
      ASSERT_NE(col, nullptr);
      irs::ColumnReader::BlobPointReader pr{r, *col};
      ASSERT_FALSE(pr.IsNullDoc(1));
      const auto v = pr.FetchDoc(1);
      const auto s = irs::ToString<std::string_view>(v.data());
      EXPECT_EQ("field4_doc_min", s);
    }

    // Visit f0 (full traversal).
    {
      std::unordered_map<std::string, irs::doc_id_t> expected{
        {"field0_doc0", 1}, {"field0_doc2", 2}, {"field0_doc33", 33}};
      const auto* col = r.Column(kF0);
      ASSERT_NE(col, nullptr);
      const bool ok = irs::tests::VisitBlobColumn(
        r, *col, [&](irs::doc_id_t doc, irs::bytes_view v) {
          const auto s = irs::ToString<std::string>(v.data());
          auto it = expected.find(s);
          if (it == expected.end() || it->second != doc) {
            return false;
          }
          expected.erase(it);
          return true;
        });
      EXPECT_TRUE(ok);
      EXPECT_TRUE(expected.empty());
    }

    // Partial visit (early-stop after 2 calls).
    {
      std::unordered_map<std::string, irs::doc_id_t> expected{
        {"field0_doc0", 1}, {"field0_doc2", 2}, {"field0_doc33", 33}};
      const auto* col = r.Column(kF0);
      ASSERT_NE(col, nullptr);
      size_t calls = 0;
      const bool ok = irs::tests::VisitBlobColumn(
        r, *col, [&](irs::doc_id_t doc, irs::bytes_view v) {
          ++calls;
          if (calls > 2) {
            return false;
          }
          const auto s = irs::ToString<std::string>(v.data());
          auto it = expected.find(s);
          if (it == expected.end() || it->second != doc) {
            return false;
          }
          expected.erase(it);
          return true;
        });
      EXPECT_FALSE(ok);
      EXPECT_EQ(expected.size(), 1u);
      EXPECT_TRUE(expected.contains("field0_doc33"));
    }

    // Point-read f0 (different access orders -- cached vs uncached
    // paths through the BlobPointReader).
    {
      const auto* col = r.Column(kF0);
      ASSERT_NE(col, nullptr);
      // Pass 1: sequential.
      {
        irs::ColumnReader::BlobPointReader pr{r, *col};
        EXPECT_EQ("field0_doc0", irs::tests::ReadStoredStr<std::string>(pr, 1));
        EXPECT_TRUE(pr.IsNullDoc(5));  // doc without value in field0
        EXPECT_EQ("field0_doc33",
                  irs::tests::ReadStoredStr<std::string>(pr, 33));
      }
      // Pass 2: fresh reader (simulates "cached" path with a re-issued read).
      {
        irs::ColumnReader::BlobPointReader pr{r, *col};
        EXPECT_EQ("field0_doc0", irs::tests::ReadStoredStr<std::string>(pr, 1));
        EXPECT_TRUE(pr.IsNullDoc(5));
        EXPECT_EQ("field0_doc33",
                  irs::tests::ReadStoredStr<std::string>(pr, 33));
      }
    }

    // Multi-value f1: two WriteStrs per populated row, concatenated
    // under one doc id. Read both back through a BytesViewInput on
    // top of the blob.
    {
      const auto* col = r.Column(kF1);
      ASSERT_NE(col, nullptr);
      irs::ColumnReader::BlobPointReader pr{r, *col};
      {
        const auto v = pr.FetchDoc(1);
        ASSERT_FALSE(v.empty());
        irs::BytesViewInput in{v};
        EXPECT_EQ("field1_doc0", irs::ReadString<std::string>(in));
        EXPECT_EQ("field1_doc0_1", irs::ReadString<std::string>(in));
      }
      EXPECT_TRUE(pr.IsNullDoc(2));  // no value
      {
        const auto v = pr.FetchDoc(12);
        ASSERT_FALSE(v.empty());
        irs::BytesViewInput in{v};
        EXPECT_EQ("field1_doc12_1", irs::ReadString<std::string>(in));
        EXPECT_EQ("field1_doc12_2", irs::ReadString<std::string>(in));
      }
      EXPECT_TRUE(pr.IsNullDoc(13));  // past the last populated doc
    }

    // Visit the empty column -- visitor must never be called.
    {
      const auto* col = r.Column(kEmpty);
      ASSERT_NE(col, nullptr);
      size_t calls = 0;
      const bool ok = irs::tests::VisitBlobColumn(
        r, *col, [&](irs::doc_id_t, irs::bytes_view) {
          ++calls;
          return true;
        });
      EXPECT_TRUE(ok);
      EXPECT_EQ(0u, calls);
    }

    // Visit f2 (single doc at id=1).
    {
      const auto* col = r.Column(kF2);
      ASSERT_NE(col, nullptr);
      std::unordered_map<std::string, irs::doc_id_t> expected{
        {"field2_doc1", 1}};
      const bool ok = irs::tests::VisitBlobColumn(
        r, *col, [&](irs::doc_id_t doc, irs::bytes_view v) {
          const auto s = irs::ToString<std::string>(v.data());
          auto it = expected.find(s);
          if (it == expected.end() || it->second != doc) {
            return false;
          }
          expected.erase(it);
          return true;
        });
      EXPECT_TRUE(ok);
      EXPECT_TRUE(expected.empty());
    }

    // f3 was opened but never populated.
    {
      const auto* col = r.Column(kF3);
      ASSERT_NE(col, nullptr);
      size_t calls = 0;
      const bool ok = irs::tests::VisitBlobColumn(
        r, *col, [&](irs::doc_id_t, irs::bytes_view) {
          ++calls;
          return true;
        });
      EXPECT_TRUE(ok);
      EXPECT_EQ(0u, calls);
    }
  }

  // Read segment _2 ------------------------------------------------------
  {
    irs::ColReader r{dir, "_2", db};
    ASSERT_EQ(r.Columns().size(), 3u);

    EXPECT_EQ(r.Column(irs::field_limits::invalid()), nullptr);
    EXPECT_FALSE(r.HasColumn(99));

    // Visit f0 (the only column with values in segment _2).
    {
      std::vector<std::pair<std::string_view, irs::doc_id_t>> expected = {
        {"segment_2_field1_doc0", 1}, {"segment_2_field1_doc12", 12}};
      const auto* col = r.Column(kS2F0);
      ASSERT_NE(col, nullptr);
      size_t i = 0;
      const bool ok = irs::tests::VisitBlobColumn(
        r, *col, [&](irs::doc_id_t doc, irs::bytes_view v) {
          if (i >= expected.size()) {
            return false;
          }
          const auto s = irs::ToString<std::string>(v.data());
          if (expected[i].first != s || expected[i].second != doc) {
            return false;
          }
          ++i;
          return true;
        });
      EXPECT_TRUE(ok);
      EXPECT_EQ(i, expected.size());
    }

    // Point-read f0.
    {
      const auto* col = r.Column(kS2F0);
      ASSERT_NE(col, nullptr);
      irs::ColumnReader::BlobPointReader pr{r, *col};
      EXPECT_EQ("segment_2_field1_doc0",
                irs::tests::ReadStoredStr<std::string>(pr, 1));
      EXPECT_TRUE(pr.IsNullDoc(2));   // no value at doc=2
      EXPECT_TRUE(pr.IsNullDoc(11));  // no value before 12
      EXPECT_EQ("segment_2_field1_doc12",
                irs::tests::ReadStoredStr<std::string>(pr, 12));
      EXPECT_TRUE(pr.IsNullDoc(13));  // past the last populated doc
    }
  }

  // Segment _1 -- new sub-scenarios (multi-column independence,
  // random-order point access, boundary docs, wrong-column-id sweep).
  {
    irs::ColReader r{dir, "_1", db};
    const auto* col_f0 = r.Column(kF0);
    const auto* col_f2 = r.Column(kF2);
    ASSERT_NE(col_f0, nullptr);
    ASSERT_NE(col_f2, nullptr);

    // Multi-column same-segment independence: two BlobPointReaders on
    // different columns interleaved (kF0 and kF2).
    {
      irs::ColumnReader::BlobPointReader pr_f0{r, *col_f0};
      irs::ColumnReader::BlobPointReader pr_f2{r, *col_f2};
      EXPECT_EQ("field0_doc0",
                irs::tests::ReadStoredStr<std::string>(pr_f0, 1));
      EXPECT_EQ("field2_doc1",
                irs::tests::ReadStoredStr<std::string>(pr_f2, 1));
      EXPECT_EQ("field0_doc2",
                irs::tests::ReadStoredStr<std::string>(pr_f0, 2));
      EXPECT_TRUE(pr_f2.IsNullDoc(2));
      EXPECT_TRUE(pr_f0.IsNullDoc(5));
      EXPECT_TRUE(pr_f2.IsNullDoc(5));
      EXPECT_EQ("field0_doc33",
                irs::tests::ReadStoredStr<std::string>(pr_f0, 33));
      EXPECT_TRUE(pr_f2.IsNullDoc(33));
      // f2 cache state must not have bled in from f0; verify on a fresh
      // f2 reader (the original one is past doc 1).
      irs::ColumnReader::BlobPointReader pr_f2_b{r, *col_f2};
      EXPECT_EQ("field2_doc1",
                irs::tests::ReadStoredStr<std::string>(pr_f2_b, 1));
    }

    // Random-order point access: reversed walks of f0 live docs (1, 2,
    // 33). PointReader is forward-only so each probe gets a fresh one.
    {
      const std::vector<std::pair<irs::doc_id_t, std::string_view>> reversed{
        {33, "field0_doc33"}, {2, "field0_doc2"}, {1, "field0_doc0"}};
      for (const auto& [doc, expected] : reversed) {
        irs::ColumnReader::BlobPointReader pr{r, *col_f0};
        EXPECT_EQ(expected, irs::tests::ReadStoredStr<std::string>(pr, doc));
      }
    }

    // Boundary docs: doc=min (row 0), doc=33 (last live), doc=34 (last
    // valid row but padded null), and a null in the middle.
    {
      irs::ColumnReader::BlobPointReader pr{r, *col_f0};
      EXPECT_FALSE(pr.IsNullDoc(irs::doc_limits::min()));
      EXPECT_EQ("field0_doc0", irs::tests::ReadStoredStr<std::string>(
                                 pr, irs::doc_limits::min()));
      // doc=33 is the last populated doc on f0. Use a fresh reader.
      irs::ColumnReader::BlobPointReader pr2{r, *col_f0};
      EXPECT_EQ("field0_doc33",
                irs::tests::ReadStoredStr<std::string>(pr2, 33));
      EXPECT_TRUE(pr2.IsNullDoc(34));  // padded null at last row index
    }

    // Wrong-column-id negative lookups on a populated segment.
    for (irs::field_id missing : {6, 7, 100, 1000, 0x7ffffff}) {
      EXPECT_FALSE(r.HasColumn(missing)) << "id=" << missing;
      EXPECT_EQ(r.Column(missing), nullptr) << "id=" << missing;
    }
    EXPECT_FALSE(r.HasColumn(irs::field_limits::invalid()));
    EXPECT_EQ(r.Column(irs::field_limits::invalid()), nullptr);
  }
}

TEST_P(FormatTestCase, columns_rw_typed) {
  // Typed-column round-trip: one BIGINT column (typed point-read), one
  // BLOB column with a string payload (read-back as a blob). Every doc
  // is read back via point-read (seek-to-doc) and via visit, on both
  // shapes.
  constexpr uint64_t kRowCount = 500;
  constexpr irs::field_id kIntId = 2;
  constexpr irs::field_id kStrId = 3;
  irs::MemoryDirectory dir;
  auto& db = ::sdb::DuckDBEngine::Instance().instance();
  {
    irs::ColWriter w{dir, "columns_rw_typed", db};

    // Typed BIGINT column.
    auto& cw_int = w.OpenColumn(kIntId, duckdb::LogicalType::BIGINT);
    duckdb::Vector batch{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(batch);
    uint64_t produced = 0;
    while (produced < kRowCount) {
      const auto take =
        std::min<duckdb::idx_t>(kRowCount - produced, STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        data[k] = static_cast<int64_t>((produced + k) * 11 + 5);
      }
      cw_int.Append(produced, batch, take);
      produced += take;
    }

    // BLOB column with WriteStr layout (length-prefixed string).
    auto& cw_str = irs::tests::OpenBlobColumn(w, kStrId);
    for (uint64_t i = 0; i < kRowCount; ++i) {
      irs::bstring buf;
      irs::tests::BstringDataOutput out{buf};
      const auto s = std::string{"row_"} + std::to_string(i);
      irs::WriteStr(out, s);
      irs::tests::AppendBlob(
        cw_str, static_cast<irs::doc_id_t>(i + irs::doc_limits::min()),
        {buf.data(), buf.size()});
    }

    w.Commit(kRowCount);
  }

  irs::ColReader r{dir, "columns_rw_typed", db};
  ASSERT_TRUE(r.HasColumn(kIntId));
  ASSERT_TRUE(r.HasColumn(kStrId));
  EXPECT_FALSE(r.HasColumn(0));  // wrong column id
  EXPECT_EQ(r.Column(0), nullptr);

  // Typed point-read of BIGINT column.
  {
    const auto* col = r.Column(kIntId);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->RowCount(), kRowCount);
    irs::ColumnReader::PointReader pr{r, *col};
    duckdb::Vector out{duckdb::LogicalType::BIGINT, /*capacity=*/1};
    // Scattered sample of rows + full sequential pass.
    const std::vector<uint64_t> probes = {0, 1, 17, 99, 100, 250, 499};
    for (auto i : probes) {
      ASSERT_TRUE(pr.FetchRow(i, out, 0)) << "i=" << i;
      const auto* got = duckdb::FlatVector::GetData<int64_t>(out);
      EXPECT_EQ(got[0], static_cast<int64_t>(i * 11 + 5)) << "i=" << i;
    }
  }
  // Sequential read of BIGINT via a fresh PointReader (forward-only).
  {
    const auto* col = r.Column(kIntId);
    irs::ColumnReader::PointReader pr{r, *col};
    duckdb::Vector out{duckdb::LogicalType::BIGINT, /*capacity=*/1};
    for (uint64_t i = 0; i < kRowCount; ++i) {
      ASSERT_TRUE(pr.FetchRow(i, out, 0)) << "i=" << i;
      const auto* got = duckdb::FlatVector::GetData<int64_t>(out);
      EXPECT_EQ(got[0], static_cast<int64_t>(i * 11 + 5)) << "i=" << i;
    }
  }

  // BLOB column: point-read every doc, then visit-traverse end-to-end.
  {
    const auto* col = r.Column(kStrId);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->RowCount(), kRowCount);
    irs::ColumnReader::BlobPointReader pr{r, *col};
    for (uint64_t i = 0; i < kRowCount; ++i) {
      const auto v = pr.FetchRow(i);
      ASSERT_FALSE(v.empty()) << "i=" << i;
      const auto s = irs::ToString<std::string>(v.data());
      EXPECT_EQ(s, std::string{"row_"} + std::to_string(i)) << "i=" << i;
    }
  }
  {
    const auto* col = r.Column(kStrId);
    uint64_t i = 0;
    const bool ok = irs::tests::VisitBlobColumn(
      r, *col, [&](irs::doc_id_t doc, irs::bytes_view v) {
        EXPECT_EQ(doc, static_cast<irs::doc_id_t>(i + irs::doc_limits::min()));
        const auto s = irs::ToString<std::string>(v.data());
        EXPECT_EQ(s, std::string{"row_"} + std::to_string(i));
        ++i;
        return true;
      });
    EXPECT_TRUE(ok);
    EXPECT_EQ(i, kRowCount);
  }

  // Random-order point access for BIGINT (each backward jump on a fresh
  // PointReader since PointReader is forward-only).
  {
    const auto* col = r.Column(kIntId);
    ASSERT_NE(col, nullptr);
    // Reversed sweep -- one fresh reader per probe to allow backward jumps.
    for (uint64_t i = kRowCount; i-- > 0;) {
      irs::ColumnReader::PointReader pr{r, *col};
      duckdb::Vector out{duckdb::LogicalType::BIGINT, /*capacity=*/1};
      ASSERT_TRUE(pr.FetchRow(i, out, 0)) << "i=" << i;
      const auto* got = duckdb::FlatVector::GetData<int64_t>(out);
      EXPECT_EQ(got[0], static_cast<int64_t>(i * 11 + 5)) << "i=" << i;
    }
    // Prime-stepped sweep over [0, kRowCount) -- 7 is coprime to 500 so
    // the orbit visits every row eventually. We take the first 30 steps.
    constexpr uint64_t kPrime = 7;
    for (uint64_t k = 0; k < 30; ++k) {
      const uint64_t i = (k * kPrime) % kRowCount;
      irs::ColumnReader::PointReader pr{r, *col};
      duckdb::Vector out{duckdb::LogicalType::BIGINT, /*capacity=*/1};
      ASSERT_TRUE(pr.FetchRow(i, out, 0)) << "k=" << k << " i=" << i;
      const auto* got = duckdb::FlatVector::GetData<int64_t>(out);
      EXPECT_EQ(got[0], static_cast<int64_t>(i * 11 + 5)) << "k=" << k;
    }
  }

  // Boundary docs on BLOB column: doc=min (row 0), doc=last_valid
  // (row kRowCount-1), past-end (row kRowCount) is invalid input to
  // FetchRow but IsNullRow on it should be safe.
  {
    const auto* col = r.Column(kStrId);
    ASSERT_NE(col, nullptr);
    irs::ColumnReader::BlobPointReader pr{r, *col};
    {
      const auto v = pr.FetchRow(0);
      ASSERT_FALSE(v.empty());
      EXPECT_EQ(irs::ToString<std::string>(v.data()), "row_0");
    }
    irs::ColumnReader::BlobPointReader pr2{r, *col};
    {
      const auto v = pr2.FetchRow(kRowCount - 1);
      ASSERT_FALSE(v.empty());
      EXPECT_EQ(irs::ToString<std::string>(v.data()),
                std::string{"row_"} + std::to_string(kRowCount - 1));
    }
  }

  // Seek+iterate: jump to row K, then K+1..K+5 sequentially.
  {
    const auto* col = r.Column(kIntId);
    ASSERT_NE(col, nullptr);
    constexpr uint64_t kStart = 200;
    irs::ColumnReader::PointReader pr{r, *col};
    duckdb::Vector out{duckdb::LogicalType::BIGINT, /*capacity=*/1};
    for (uint64_t i = kStart; i < kStart + 6; ++i) {
      ASSERT_TRUE(pr.FetchRow(i, out, 0)) << "i=" << i;
      const auto* got = duckdb::FlatVector::GetData<int64_t>(out);
      EXPECT_EQ(got[0], static_cast<int64_t>(i * 11 + 5)) << "i=" << i;
    }
  }

  // Multi-column same-segment independence: two PointReaders on
  // different columns (BIGINT + BLOB) interleaved; their cache state
  // must not bleed.
  {
    const auto* col_int = r.Column(kIntId);
    const auto* col_str = r.Column(kStrId);
    ASSERT_NE(col_int, nullptr);
    ASSERT_NE(col_str, nullptr);
    irs::ColumnReader::PointReader pr_int{r, *col_int};
    irs::ColumnReader::BlobPointReader pr_str{r, *col_str};
    duckdb::Vector out{duckdb::LogicalType::BIGINT, /*capacity=*/1};
    const std::vector<uint64_t> rows = {0, 5, 17, 99, 250, 499};
    for (auto i : rows) {
      // Both reset between probes (forward-only).
      irs::ColumnReader::PointReader pr_i{r, *col_int};
      irs::ColumnReader::BlobPointReader pr_s{r, *col_str};
      ASSERT_TRUE(pr_i.FetchRow(i, out, 0)) << "i=" << i;
      const auto* got = duckdb::FlatVector::GetData<int64_t>(out);
      EXPECT_EQ(got[0], static_cast<int64_t>(i * 11 + 5)) << "i=" << i;
      const auto v = pr_s.FetchRow(i);
      ASSERT_FALSE(v.empty()) << "i=" << i;
      EXPECT_EQ(irs::ToString<std::string>(v.data()),
                std::string{"row_"} + std::to_string(i))
        << "i=" << i;
    }
  }

  // --- Row-group transitions ----------------------------------------------
  // Same shape but with explicit row_group_size=512 so a column with
  // kRgRowCount = 2048 rows spans 4 row groups. Fetch docs at rg
  // boundaries (511, 512, 513, 1023, 1024, 1025, ...).
  constexpr uint64_t kRgRowCount = 2048;
  constexpr irs::field_id kRgIntId = 100;
  constexpr irs::field_id kRgStrId = 101;
  {
    irs::ColWriter w{dir, "columns_rw_typed_rg", db};
    auto& cw_int = w.OpenColumn(kRgIntId, duckdb::LogicalType::BIGINT,
                                /*skip_validity=*/false,
                                /*row_group_size=*/512,
                                duckdb::CompressionType::COMPRESSION_AUTO);
    duckdb::Vector batch{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(batch);
    uint64_t produced = 0;
    while (produced < kRgRowCount) {
      const auto take =
        std::min<duckdb::idx_t>(kRgRowCount - produced, STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        data[k] = static_cast<int64_t>((produced + k) * 11 + 5);
      }
      cw_int.Append(produced, batch, take);
      produced += take;
    }

    auto& cw_str = w.OpenColumn(kRgStrId, duckdb::LogicalType::BLOB,
                                /*skip_validity=*/false,
                                /*row_group_size=*/512,
                                duckdb::CompressionType::COMPRESSION_AUTO);
    for (uint64_t i = 0; i < kRgRowCount; ++i) {
      irs::bstring buf;
      irs::tests::BstringDataOutput out{buf};
      const auto s = std::string{"row_"} + std::to_string(i);
      irs::WriteStr(out, s);
      irs::tests::AppendBlob(
        cw_str, static_cast<irs::doc_id_t>(i + irs::doc_limits::min()),
        {buf.data(), buf.size()});
    }
    w.Commit(kRgRowCount);
  }
  {
    irs::ColReader rr{dir, "columns_rw_typed_rg", db};
    const auto* col_int = rr.Column(kRgIntId);
    const auto* col_str = rr.Column(kRgStrId);
    ASSERT_NE(col_int, nullptr);
    ASSERT_NE(col_str, nullptr);
    EXPECT_EQ(col_int->RowCount(), kRgRowCount);
    EXPECT_EQ(col_str->RowCount(), kRgRowCount);
    // Multiple data row groups expected at this scale (4 of 512).
    EXPECT_GE(col_int->DataRgCount(), 2u);
    EXPECT_GE(col_str->DataRgCount(), 2u);

    // Fetch at row-group boundaries; each probe on a fresh reader.
    const std::vector<uint64_t> boundary_rows = {
      0, 1, 511, 512, 513, 1023, 1024, 1025, 1535, 1536, 1537, kRgRowCount - 1};
    for (auto i : boundary_rows) {
      irs::ColumnReader::PointReader pr_i{rr, *col_int};
      duckdb::Vector out{duckdb::LogicalType::BIGINT, /*capacity=*/1};
      ASSERT_TRUE(pr_i.FetchRow(i, out, 0)) << "i=" << i;
      const auto* got = duckdb::FlatVector::GetData<int64_t>(out);
      EXPECT_EQ(got[0], static_cast<int64_t>(i * 11 + 5)) << "i=" << i;

      irs::ColumnReader::BlobPointReader pr_s{rr, *col_str};
      const auto v = pr_s.FetchRow(i);
      ASSERT_FALSE(v.empty()) << "i=" << i;
      EXPECT_EQ(irs::ToString<std::string>(v.data()),
                std::string{"row_"} + std::to_string(i))
        << "i=" << i;
    }

    // Seek+iterate spanning a row-group boundary (start before 512, walk
    // through it).
    {
      irs::ColumnReader::PointReader pr{rr, *col_int};
      duckdb::Vector out{duckdb::LogicalType::BIGINT, /*capacity=*/1};
      for (uint64_t i = 510; i <= 515; ++i) {
        ASSERT_TRUE(pr.FetchRow(i, out, 0)) << "i=" << i;
        const auto* got = duckdb::FlatVector::GetData<int64_t>(out);
        EXPECT_EQ(got[0], static_cast<int64_t>(i * 11 + 5)) << "i=" << i;
      }
    }
  }
}

TEST_P(FormatTestCase, columns_rw_bit_mask) {
  // Sparse BLOB column: only docs (2, 4, 8, 9) carry an empty payload,
  // the rest are null. Verifies:
  //   * point-read each populated doc (twice -- "cached"),
  //   * IsNullRow for every other position,
  //   * a visit pass covers exactly the 4 populated docs in order.
  constexpr uint64_t kRowCount = 10;
  const std::vector<irs::doc_id_t> kLiveDocs = {2, 4, 8, 9};
  RoundTrip(
    "columns_rw_bit_mask", kRowCount,
    [&](irs::ColWriter& w) {
      auto& cw = irs::tests::OpenBlobColumn(w, /*id=*/3);
      size_t live_i = 0;
      for (uint64_t i = 0; i < kRowCount; ++i) {
        const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
        if (live_i < kLiveDocs.size() && kLiveDocs[live_i] == doc) {
          irs::tests::AppendBlob(cw, doc, irs::bytes_view{});
          ++live_i;
        } else {
          irs::tests::AppendNullBlob(cw, doc);
        }
      }
    },
    [&](const irs::ColReader& r) {
      ASSERT_TRUE(r.HasColumn(3));
      const auto* col = r.Column(3);
      ASSERT_NE(col, nullptr);
      EXPECT_EQ(col->RowCount(), kRowCount);
      EXPECT_TRUE(col->HasValidity());

      // Pass 1: not-cached random reads.
      {
        irs::ColumnReader::BlobPointReader pr{r, *col};
        for (auto doc : kLiveDocs) {
          EXPECT_FALSE(pr.IsNullDoc(doc)) << "doc=" << doc;
          EXPECT_TRUE(pr.FetchDoc(doc).empty()) << "doc=" << doc;
        }
        // Spot-check a few non-live docs.
        EXPECT_TRUE(pr.IsNullDoc(1));
        EXPECT_TRUE(pr.IsNullDoc(3));
        EXPECT_TRUE(pr.IsNullDoc(5));
        EXPECT_TRUE(pr.IsNullDoc(7));
      }

      // Pass 2: cached random reads (re-issued on a fresh point reader to
      // exercise that the reader is reusable + deterministic).
      {
        irs::ColumnReader::BlobPointReader pr{r, *col};
        for (auto doc : kLiveDocs) {
          EXPECT_FALSE(pr.IsNullDoc(doc)) << "doc=" << doc;
          EXPECT_TRUE(pr.FetchDoc(doc).empty()) << "doc=" << doc;
        }
      }

      // Pass 3: full sequential check of every doc.
      {
        irs::ColumnReader::BlobPointReader pr{r, *col};
        size_t live_i = 0;
        for (uint64_t i = 0; i < kRowCount; ++i) {
          const auto doc =
            static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
          const bool expect_live =
            live_i < kLiveDocs.size() && kLiveDocs[live_i] == doc;
          EXPECT_EQ(pr.IsNullDoc(doc), !expect_live) << "doc=" << doc;
          if (expect_live) {
            EXPECT_TRUE(pr.FetchDoc(doc).empty()) << "doc=" << doc;
            ++live_i;
          }
        }
        EXPECT_EQ(live_i, kLiveDocs.size());
      }

      // Pass 4: visit-iterate covers exactly the live docs (empty payloads).
      {
        size_t i = 0;
        const bool ok = irs::tests::VisitBlobColumn(
          r, *col, [&](irs::doc_id_t doc, irs::bytes_view v) {
            if (i >= kLiveDocs.size()) {
              return false;
            }
            EXPECT_EQ(doc, kLiveDocs[i]);
            EXPECT_TRUE(v.empty()) << "doc=" << doc;
            ++i;
            return true;
          });
        EXPECT_TRUE(ok);
        EXPECT_EQ(i, kLiveDocs.size());
      }

      // Pass 5: random-order point reads (reversed, then prime-stepped),
      // each on a fresh PointReader because PointReader is forward-only.
      {
        std::vector<irs::doc_id_t> reversed(kLiveDocs.rbegin(),
                                            kLiveDocs.rend());
        for (auto doc : reversed) {
          irs::ColumnReader::BlobPointReader pr{r, *col};
          EXPECT_FALSE(pr.IsNullDoc(doc)) << "doc=" << doc;
          EXPECT_TRUE(pr.FetchDoc(doc).empty()) << "doc=" << doc;
        }
        // Prime-stepped probes over [0..kRowCount). 3 is coprime to 10.
        for (uint64_t k = 0; k < 10; ++k) {
          const uint64_t i = (k * 3) % kRowCount;
          const auto doc =
            static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
          irs::ColumnReader::BlobPointReader pr{r, *col};
          const bool expect_live = std::find(kLiveDocs.begin(), kLiveDocs.end(),
                                             doc) != kLiveDocs.end();
          EXPECT_EQ(pr.IsNullDoc(doc), !expect_live) << "i=" << i;
        }
      }

      // Pass 6: boundary docs -- doc=min and the last populated doc.
      // kLiveDocs.back()==9 is the last live doc; row index kRowCount-1
      // (doc=10) is null (kRowCount=10 rows, indices 0..9, doc=10).
      {
        irs::ColumnReader::BlobPointReader pr{r, *col};
        EXPECT_TRUE(pr.IsNullDoc(irs::doc_limits::min())) << "doc=min";
        EXPECT_FALSE(pr.IsNullDoc(9));  // last live doc
        EXPECT_TRUE(pr.FetchDoc(9).empty());
        // Row index 9 (doc=10) is the last row -- null.
        irs::ColumnReader::BlobPointReader pr2{r, *col};
        EXPECT_TRUE(pr2.IsNullDoc(10));
      }

      // Pass 7: wrong-column-id negative lookups.
      for (irs::field_id missing : {0, 1, 2, 100}) {
        EXPECT_FALSE(r.HasColumn(missing)) << "id=" << missing;
        EXPECT_EQ(r.Column(missing), nullptr) << "id=" << missing;
      }
    });

  // --- Row-group transitions ----------------------------------------------
  // Bit-mask shape at row_group_size=512 spanning kBigRows rows; the
  // column has a live row every 11th position so multiple row groups
  // each contain a mix of live and null rows.
  constexpr uint64_t kBigRows = 2048;
  constexpr irs::field_id kBigId = 200;
  irs::MemoryDirectory dir;
  auto& db = ::sdb::DuckDBEngine::Instance().instance();
  {
    irs::ColWriter w{dir, "columns_rw_bit_mask_rg", db};
    auto& cw = w.OpenColumn(kBigId, duckdb::LogicalType::BLOB,
                            /*skip_validity=*/false,
                            /*row_group_size=*/512,
                            duckdb::CompressionType::COMPRESSION_AUTO);
    for (uint64_t i = 0; i < kBigRows; ++i) {
      const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
      if (i % 11 == 0) {
        irs::tests::AppendBlob(cw, doc, irs::bytes_view{});
      } else {
        irs::tests::AppendNullBlob(cw, doc);
      }
    }
    w.Commit(kBigRows);
  }
  {
    irs::ColReader r{dir, "columns_rw_bit_mask_rg", db};
    const auto* col = r.Column(kBigId);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->RowCount(), kBigRows);
    EXPECT_TRUE(col->HasValidity());
    EXPECT_GE(col->ValidityRgCount(), 2u);  // at least 2 row groups

    // Probes spanning row-group boundaries (511/512/513 etc).
    const std::vector<uint64_t> boundary_rows = {
      0, 1, 511, 512, 513, 1023, 1024, 1025, 1535, 1536, 1537, kBigRows - 1};
    for (auto i : boundary_rows) {
      irs::ColumnReader::BlobPointReader pr{r, *col};
      const bool expect_live = (i % 11 == 0);
      EXPECT_EQ(pr.IsNullRow(i), !expect_live) << "i=" << i;
      if (expect_live) {
        EXPECT_TRUE(pr.FetchRow(i).empty()) << "i=" << i;
      }
    }

    // Seek+iterate around a boundary (start before 512, walk through it).
    {
      irs::ColumnReader::BlobPointReader pr{r, *col};
      for (uint64_t i = 510; i <= 515; ++i) {
        const bool expect_live = (i % 11 == 0);
        EXPECT_EQ(pr.IsNullRow(i), !expect_live) << "i=" << i;
        if (expect_live) {
          EXPECT_TRUE(pr.FetchRow(i).empty()) << "i=" << i;
        }
      }
    }

    // Sequential per-row walk covering every logical row -- IsNullRow
    // matches the i % 11 == 0 pattern, fetch returns empty payload on
    // live rows. (We use the per-row BlobPointReader path instead of
    // VisitBlobColumn because the cs-scan helper is tuned for dense
    // columns and miscounts when the data column drops all-null RGs;
    // see sparse_bitmap_test::TestRwNext for the same restriction.)
    {
      uint64_t expected_live = 0;
      uint64_t seen_live = 0;
      irs::ColumnReader::BlobPointReader pr{r, *col};
      for (uint64_t i = 0; i < kBigRows; ++i) {
        const bool expect_live = (i % 11 == 0);
        if (expect_live) {
          ++expected_live;
        }
        ASSERT_EQ(pr.IsNullRow(i), !expect_live) << "i=" << i;
        if (expect_live) {
          EXPECT_TRUE(pr.FetchRow(i).empty()) << "i=" << i;
          ++seen_live;
        }
      }
      EXPECT_EQ(seen_live, expected_live);
    }
  }
}

TEST_P(FormatTestCase, columns_rw_dense_mask) {
  // Dense mask: every doc is valid with no payload, covering a
  // contiguous run. Three passes: point-read each row, then a fresh
  // point-reader run, then a visit-iterate over the full range.
  constexpr uint64_t kRowCount = 1026;
  RoundTrip(
    "columns_rw_dense_mask", kRowCount,
    [&](irs::ColWriter& w) {
      auto& cw = irs::tests::OpenBlobColumn(w, /*id=*/4);
      for (uint64_t i = 0; i < kRowCount; ++i) {
        irs::tests::AppendBlob(
          cw, static_cast<irs::doc_id_t>(i + irs::doc_limits::min()),
          irs::bytes_view{});
      }
    },
    [&](const irs::ColReader& r) {
      ASSERT_TRUE(r.HasColumn(4));
      const auto* col = r.Column(4);
      ASSERT_NE(col, nullptr);
      EXPECT_EQ(col->RowCount(), kRowCount);

      // Pass 1: full sequential point read.
      {
        irs::ColumnReader::BlobPointReader pr{r, *col};
        for (uint64_t i = 0; i < kRowCount; ++i) {
          EXPECT_FALSE(pr.IsNullRow(i)) << "i=" << i;
          EXPECT_TRUE(pr.FetchRow(i).empty()) << "i=" << i;
        }
      }

      // Pass 2: fresh point reader (re-issued reads -- "cached" analogue).
      {
        irs::ColumnReader::BlobPointReader pr{r, *col};
        for (uint64_t i = 0; i < kRowCount; ++i) {
          EXPECT_FALSE(pr.IsNullRow(i)) << "i=" << i;
        }
      }

      // Pass 3: visit must yield exactly kRowCount empty payloads.
      {
        uint64_t i = 0;
        const bool ok = irs::tests::VisitBlobColumn(
          r, *col, [&](irs::doc_id_t doc, irs::bytes_view v) {
            EXPECT_EQ(doc,
                      static_cast<irs::doc_id_t>(i + irs::doc_limits::min()));
            EXPECT_TRUE(v.empty()) << "i=" << i;
            ++i;
            return true;
          });
        EXPECT_TRUE(ok);
        EXPECT_EQ(i, kRowCount);
      }

      // Pass 4: random-order point reads (reversed + prime-stepped),
      // each on a fresh reader (forward-only).
      {
        // Reversed.
        for (uint64_t r_i = kRowCount; r_i-- > 0;) {
          irs::ColumnReader::BlobPointReader pr{r, *col};
          EXPECT_FALSE(pr.IsNullRow(r_i)) << "i=" << r_i;
        }
        // Prime-stepped (17 coprime to 1026).
        for (uint64_t k = 0; k < 50; ++k) {
          const uint64_t i = (k * 17) % kRowCount;
          irs::ColumnReader::BlobPointReader pr{r, *col};
          EXPECT_FALSE(pr.IsNullRow(i)) << "i=" << i;
          EXPECT_TRUE(pr.FetchRow(i).empty()) << "i=" << i;
        }
      }

      // Pass 5: boundary rows -- 0, kRowCount-1 (last valid).
      {
        irs::ColumnReader::BlobPointReader pr{r, *col};
        EXPECT_FALSE(pr.IsNullRow(0));
        EXPECT_TRUE(pr.FetchRow(0).empty());

        irs::ColumnReader::BlobPointReader pr2{r, *col};
        EXPECT_FALSE(pr2.IsNullRow(kRowCount - 1));
        EXPECT_TRUE(pr2.FetchRow(kRowCount - 1).empty());
      }

      // Pass 6: wrong-column-id negative lookups.
      EXPECT_FALSE(r.HasColumn(0));
      EXPECT_EQ(r.Column(0), nullptr);
      EXPECT_FALSE(r.HasColumn(99));
      EXPECT_EQ(r.Column(99), nullptr);
      EXPECT_FALSE(r.HasColumn(irs::field_limits::invalid()));
      EXPECT_EQ(r.Column(irs::field_limits::invalid()), nullptr);
    });

  // --- Row-group transitions ----------------------------------------------
  // Dense mask spanning multiple row groups at row_group_size=512. With
  // kBigRows=2048 the column has 4 row groups.
  constexpr uint64_t kBigRows = 2048;
  constexpr irs::field_id kBigId = 300;
  irs::MemoryDirectory dir;
  auto& db = ::sdb::DuckDBEngine::Instance().instance();
  {
    irs::ColWriter w{dir, "columns_rw_dense_mask_rg", db};
    auto& cw = w.OpenColumn(kBigId, duckdb::LogicalType::BLOB,
                            /*skip_validity=*/false,
                            /*row_group_size=*/512,
                            duckdb::CompressionType::COMPRESSION_AUTO);
    for (uint64_t i = 0; i < kBigRows; ++i) {
      irs::tests::AppendBlob(
        cw, static_cast<irs::doc_id_t>(i + irs::doc_limits::min()),
        irs::bytes_view{});
    }
    w.Commit(kBigRows);
  }
  {
    irs::ColReader r{dir, "columns_rw_dense_mask_rg", db};
    const auto* col = r.Column(kBigId);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->RowCount(), kBigRows);
    EXPECT_GE(col->DataRgCount(), 2u);

    // Probes at row-group boundaries.
    const std::vector<uint64_t> boundary_rows = {
      0, 1, 511, 512, 513, 1023, 1024, 1025, 1535, 1536, 1537, kBigRows - 1};
    for (auto i : boundary_rows) {
      irs::ColumnReader::BlobPointReader pr{r, *col};
      EXPECT_FALSE(pr.IsNullRow(i)) << "i=" << i;
      EXPECT_TRUE(pr.FetchRow(i).empty()) << "i=" << i;
    }

    // Seek+iterate spanning a row-group boundary.
    {
      irs::ColumnReader::BlobPointReader pr{r, *col};
      for (uint64_t i = 510; i <= 515; ++i) {
        EXPECT_FALSE(pr.IsNullRow(i)) << "i=" << i;
        EXPECT_TRUE(pr.FetchRow(i).empty()) << "i=" << i;
      }
    }

    // Visit-iterate covers every row.
    {
      uint64_t i = 0;
      const bool ok = irs::tests::VisitBlobColumn(
        r, *col, [&](irs::doc_id_t doc, irs::bytes_view v) {
          EXPECT_EQ(doc,
                    static_cast<irs::doc_id_t>(i + irs::doc_limits::min()));
          EXPECT_TRUE(v.empty()) << "i=" << i;
          ++i;
          return true;
        });
      EXPECT_TRUE(ok);
      EXPECT_EQ(i, kBigRows);
    }
  }
}

TEST_P(FormatTestCase, columns_rw_big_document) {
  // Large per-doc payload, read back three ways:
  //  * random-access via BlobPointReader::FetchRow at scattered indices,
  //  * sequential point-read over all rows,
  //  * visit-iterate over all rows.
  // ~70 KiB per row keeps the on-disk payload firmly in the
  // overflow-string regime; 50 rows keeps the test fast.
  constexpr uint64_t kRowCount = 50;
  constexpr size_t kPayloadSize = 70 * 1024;
  RoundTrip(
    "columns_rw_big_document", kRowCount,
    [&](irs::ColWriter& w) {
      auto& cw = irs::tests::OpenBlobColumn(w, /*id=*/5);
      std::string payload(kPayloadSize, '\0');
      for (uint64_t i = 0; i < kRowCount; ++i) {
        std::fill(payload.begin(), payload.end(),
                  static_cast<char>('A' + (i % 26)));
        irs::tests::AppendBlob(
          cw, static_cast<irs::doc_id_t>(i + irs::doc_limits::min()),
          irs::bytes_view{
            reinterpret_cast<const irs::byte_type*>(payload.data()),
            payload.size()});
      }
    },
    [&](const irs::ColReader& r) {
      ASSERT_TRUE(r.HasColumn(5));
      const auto* col = r.Column(5);
      ASSERT_NE(col, nullptr);
      EXPECT_EQ(col->RowCount(), kRowCount);

      // Pass 1: scattered random access.
      {
        irs::ColumnReader::BlobPointReader pr{r, *col};
        const std::vector<uint64_t> probes = {0, 7, 25, 12, 49, 1};
        // Probes are out-of-order; PointReader is forward-only across
        // calls but each call creates fresh segments. To test backward
        // probes too we re-instantiate between groups.
        for (auto i : probes) {
          irs::ColumnReader::BlobPointReader pr2{r, *col};
          const auto bytes = pr2.FetchRow(i);
          ASSERT_EQ(bytes.size(), kPayloadSize) << "i=" << i;
          const auto first = static_cast<irs::byte_type>('A' + (i % 26));
          EXPECT_EQ(bytes[0], first);
          EXPECT_EQ(bytes[kPayloadSize - 1], first);
          // Spot-check the middle byte too.
          EXPECT_EQ(bytes[kPayloadSize / 2], first);
        }
      }

      // Pass 2: full forward point-read sweep ("iterator next" analogue).
      {
        irs::ColumnReader::BlobPointReader pr{r, *col};
        for (uint64_t i = 0; i < kRowCount; ++i) {
          const auto bytes = pr.FetchRow(i);
          ASSERT_EQ(bytes.size(), kPayloadSize) << "i=" << i;
          const auto first = static_cast<irs::byte_type>('A' + (i % 26));
          EXPECT_EQ(bytes[0], first) << "i=" << i;
          EXPECT_EQ(bytes[kPayloadSize - 1], first) << "i=" << i;
        }
      }

      // Pass 3: visit-iterate produces every row in order.
      {
        uint64_t i = 0;
        const bool ok = irs::tests::VisitBlobColumn(
          r, *col, [&](irs::doc_id_t doc, irs::bytes_view v) {
            EXPECT_EQ(doc,
                      static_cast<irs::doc_id_t>(i + irs::doc_limits::min()));
            EXPECT_EQ(v.size(), kPayloadSize) << "i=" << i;
            EXPECT_EQ(v[0], static_cast<irs::byte_type>('A' + (i % 26)));
            ++i;
            return true;
          });
        EXPECT_TRUE(ok);
        EXPECT_EQ(i, kRowCount);
      }

      // Pass 4: random-order point access (reversed + prime-stepped).
      {
        // Reversed.
        for (uint64_t r_i = kRowCount; r_i-- > 0;) {
          irs::ColumnReader::BlobPointReader pr{r, *col};
          const auto bytes = pr.FetchRow(r_i);
          ASSERT_EQ(bytes.size(), kPayloadSize) << "i=" << r_i;
          const auto first = static_cast<irs::byte_type>('A' + (r_i % 26));
          EXPECT_EQ(bytes[0], first) << "i=" << r_i;
          EXPECT_EQ(bytes[kPayloadSize - 1], first) << "i=" << r_i;
        }
        // Prime-stepped (13 coprime to 50). First 20 steps.
        for (uint64_t k = 0; k < 20; ++k) {
          const uint64_t i = (k * 13) % kRowCount;
          irs::ColumnReader::BlobPointReader pr{r, *col};
          const auto bytes = pr.FetchRow(i);
          ASSERT_EQ(bytes.size(), kPayloadSize) << "k=" << k << " i=" << i;
          const auto first = static_cast<irs::byte_type>('A' + (i % 26));
          EXPECT_EQ(bytes[0], first) << "k=" << k;
        }
      }

      // Pass 5: boundary docs -- doc=min and last_valid.
      {
        irs::ColumnReader::BlobPointReader pr{r, *col};
        const auto bytes_min = pr.FetchDoc(irs::doc_limits::min());
        ASSERT_EQ(bytes_min.size(), kPayloadSize);
        EXPECT_EQ(bytes_min[0], static_cast<irs::byte_type>('A'));

        irs::ColumnReader::BlobPointReader pr2{r, *col};
        const auto bytes_last = pr2.FetchRow(kRowCount - 1);
        ASSERT_EQ(bytes_last.size(), kPayloadSize);
        EXPECT_EQ(bytes_last[0],
                  static_cast<irs::byte_type>('A' + ((kRowCount - 1) % 26)));
      }

      // Pass 6: seek+iterate -- fetch K, then K+1..K+5 sequentially.
      {
        irs::ColumnReader::BlobPointReader pr{r, *col};
        constexpr uint64_t kStart = 20;
        for (uint64_t i = kStart; i < kStart + 6; ++i) {
          const auto bytes = pr.FetchRow(i);
          ASSERT_EQ(bytes.size(), kPayloadSize) << "i=" << i;
          const auto first = static_cast<irs::byte_type>('A' + (i % 26));
          EXPECT_EQ(bytes[0], first) << "i=" << i;
        }
      }

      // Pass 7: wrong-column-id negative lookups.
      EXPECT_FALSE(r.HasColumn(4));
      EXPECT_FALSE(r.HasColumn(99));
      EXPECT_EQ(r.Column(99), nullptr);
      EXPECT_FALSE(r.HasColumn(irs::field_limits::invalid()));
    });

  // --- Row-group transitions ----------------------------------------------
  // Smaller per-row payload (1KB) but enough rows to span multiple row
  // groups at row_group_size=512. Verifies that the big-payload codec
  // path still works correctly across rg boundaries.
  constexpr uint64_t kBigRows = 1500;
  constexpr size_t kSmallPayloadSize = 1024;
  constexpr irs::field_id kBigId = 400;
  irs::MemoryDirectory dir;
  auto& db = ::sdb::DuckDBEngine::Instance().instance();
  {
    irs::ColWriter w{dir, "columns_rw_big_document_rg", db};
    auto& cw = w.OpenColumn(kBigId, duckdb::LogicalType::BLOB,
                            /*skip_validity=*/false,
                            /*row_group_size=*/512,
                            duckdb::CompressionType::COMPRESSION_AUTO);
    std::string payload(kSmallPayloadSize, '\0');
    for (uint64_t i = 0; i < kBigRows; ++i) {
      std::fill(payload.begin(), payload.end(),
                static_cast<char>('A' + (i % 26)));
      irs::tests::AppendBlob(
        cw, static_cast<irs::doc_id_t>(i + irs::doc_limits::min()),
        irs::bytes_view{reinterpret_cast<const irs::byte_type*>(payload.data()),
                        payload.size()});
    }
    w.Commit(kBigRows);
  }
  {
    irs::ColReader r{dir, "columns_rw_big_document_rg", db};
    const auto* col = r.Column(kBigId);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->RowCount(), kBigRows);
    EXPECT_GE(col->DataRgCount(), 2u);

    // Probes at row-group boundaries.
    const std::vector<uint64_t> boundary_rows = {
      0, 1, 511, 512, 513, 1023, 1024, 1025, kBigRows - 1};
    for (auto i : boundary_rows) {
      irs::ColumnReader::BlobPointReader pr{r, *col};
      const auto bytes = pr.FetchRow(i);
      ASSERT_EQ(bytes.size(), kSmallPayloadSize) << "i=" << i;
      const auto first = static_cast<irs::byte_type>('A' + (i % 26));
      EXPECT_EQ(bytes[0], first) << "i=" << i;
      EXPECT_EQ(bytes[kSmallPayloadSize - 1], first) << "i=" << i;
    }

    // Seek+iterate across a boundary.
    {
      irs::ColumnReader::BlobPointReader pr{r, *col};
      for (uint64_t i = 510; i <= 515; ++i) {
        const auto bytes = pr.FetchRow(i);
        ASSERT_EQ(bytes.size(), kSmallPayloadSize) << "i=" << i;
        const auto first = static_cast<irs::byte_type>('A' + (i % 26));
        EXPECT_EQ(bytes[0], first) << "i=" << i;
      }
    }
  }
}

TEST_P(FormatTestCase, columns_rw_writer_reuse) {
  // Writer is one-shot per segment; the test exercises three sequential
  // segments written into the same dir, each readable independently, and:
  //  * Rollback (writer never committed) leaves the dir untouched,
  //  * a fresh writer at the rolled-back name produces a clean file,
  //  * three independent segments with id+name BLOB columns round-trip.
  irs::MemoryDirectory dir;
  auto& db = ::sdb::DuckDBEngine::Instance().instance();

  // Rollback path: the eagerly-created `.col` stays on disk as an orphan;
  // the directory cleaner sweeps it later.
  {
    irs::ColWriter w{dir, "_rollback_seg", db};
    irs::tests::OpenBlobColumn(w, /*id=*/1);
    w.Rollback();
  }
  {
    bool exists = false;
    ASSERT_TRUE(dir.exists(exists, "_rollback_seg.col"));
    EXPECT_TRUE(exists);
  }

  // Three sequential segments.
  struct SegSpec {
    std::string name;
    uint64_t row_count;
  };
  const std::vector<SegSpec> segs = {{"_1", 16}, {"_2", 16}, {"_3", 40}};

  static constexpr irs::field_id kIdCol = 7;
  static constexpr irs::field_id kNameCol = 8;
  auto write_seg = [&](const SegSpec& seg, uint64_t prefix) {
    irs::ColWriter w{dir, seg.name, db};
    auto& id_w = irs::tests::OpenBlobColumn(w, kIdCol);
    auto& name_w = irs::tests::OpenBlobColumn(w, kNameCol);
    for (uint64_t i = 0; i < seg.row_count; ++i) {
      const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
      irs::bstring id_buf;
      irs::bstring name_buf;
      {
        irs::tests::BstringDataOutput out{id_buf};
        irs::WriteStr(out, std::to_string(prefix + i));
      }
      {
        irs::tests::BstringDataOutput out{name_buf};
        irs::WriteStr(out, std::string{"name_"} + std::to_string(prefix + i));
      }
      irs::tests::AppendBlob(id_w, doc, {id_buf.data(), id_buf.size()});
      irs::tests::AppendBlob(name_w, doc, {name_buf.data(), name_buf.size()});
    }
    auto filename = w.Commit(seg.row_count);
    ASSERT_FALSE(filename.empty());
  };

  write_seg(segs[0], /*prefix=*/0);
  write_seg(segs[1], /*prefix=*/1000);
  write_seg(segs[2], /*prefix=*/2000);

  // Read each segment back and verify both columns. Per segment we also
  // exercise multi-column same-segment independence (id_pr fetches don't
  // bleed into name_pr cache state) and wrong-column-id negative lookups.
  for (size_t s = 0; s < segs.size(); ++s) {
    const auto& seg = segs[s];
    const uint64_t prefix = s == 0 ? 0 : s == 1 ? 1000 : 2000;
    irs::ColReader r{dir, seg.name, db};
    ASSERT_TRUE(r.HasColumn(kIdCol)) << "seg=" << seg.name;
    ASSERT_TRUE(r.HasColumn(kNameCol)) << "seg=" << seg.name;
    // Wrong-column-id negative lookups (sweep).
    for (irs::field_id missing : {0, 1, 2, 3, 4, 5, 6, 9, 99, 1000}) {
      EXPECT_FALSE(r.HasColumn(missing))
        << "seg=" << seg.name << " id=" << missing;
      EXPECT_EQ(r.Column(missing), nullptr)
        << "seg=" << seg.name << " id=" << missing;
    }
    EXPECT_FALSE(r.HasColumn(irs::field_limits::invalid()));
    const auto* id_col = r.Column(kIdCol);
    const auto* name_col = r.Column(kNameCol);
    ASSERT_NE(id_col, nullptr);
    ASSERT_NE(name_col, nullptr);
    EXPECT_EQ(id_col->RowCount(), seg.row_count) << "seg=" << seg.name;
    EXPECT_EQ(name_col->RowCount(), seg.row_count) << "seg=" << seg.name;

    irs::ColumnReader::BlobPointReader id_pr{r, *id_col};
    irs::ColumnReader::BlobPointReader name_pr{r, *name_col};
    for (uint64_t i = 0; i < seg.row_count; ++i) {
      const auto exp_id = std::to_string(prefix + i);
      const auto exp_name = std::string{"name_"} + std::to_string(prefix + i);
      EXPECT_EQ(
        irs::tests::ReadStoredStr<std::string>(
          id_pr, static_cast<irs::doc_id_t>(i + irs::doc_limits::min())),
        exp_id)
        << "seg=" << seg.name << " i=" << i;
      EXPECT_EQ(
        irs::tests::ReadStoredStr<std::string>(
          name_pr, static_cast<irs::doc_id_t>(i + irs::doc_limits::min())),
        exp_name)
        << "seg=" << seg.name << " i=" << i;
    }

    // Random-order point access per segment: reversed walks on a fresh
    // reader per probe (forward-only constraint), with both columns
    // re-checked under the same loop -- verifies that each lookup
    // doesn't disturb the *other* column.
    for (uint64_t r_i = seg.row_count; r_i-- > 0;) {
      irs::ColumnReader::BlobPointReader id_pr2{r, *id_col};
      irs::ColumnReader::BlobPointReader name_pr2{r, *name_col};
      const auto doc = static_cast<irs::doc_id_t>(r_i + irs::doc_limits::min());
      EXPECT_EQ(irs::tests::ReadStoredStr<std::string>(id_pr2, doc),
                std::to_string(prefix + r_i))
        << "seg=" << seg.name << " r_i=" << r_i;
      EXPECT_EQ(irs::tests::ReadStoredStr<std::string>(name_pr2, doc),
                std::string{"name_"} + std::to_string(prefix + r_i))
        << "seg=" << seg.name << " r_i=" << r_i;
    }

    // Boundary docs per segment: doc=min, last_valid (doc=seg.row_count).
    {
      irs::ColumnReader::BlobPointReader id_pr2{r, *id_col};
      irs::ColumnReader::BlobPointReader name_pr2{r, *name_col};
      EXPECT_EQ(
        irs::tests::ReadStoredStr<std::string>(id_pr2, irs::doc_limits::min()),
        std::to_string(prefix));
      EXPECT_EQ(irs::tests::ReadStoredStr<std::string>(name_pr2,
                                                       irs::doc_limits::min()),
                std::string{"name_"} + std::to_string(prefix));
      irs::ColumnReader::BlobPointReader id_pr3{r, *id_col};
      const auto last_doc =
        static_cast<irs::doc_id_t>(seg.row_count + irs::doc_limits::min() - 1);
      EXPECT_EQ(irs::tests::ReadStoredStr<std::string>(id_pr3, last_doc),
                std::to_string(prefix + seg.row_count - 1));
    }
  }

  // --- Row-group transitions ----------------------------------------------
  // Fourth segment with kBigRows=2048 rows at row_group_size=512 so each
  // column spans 4 row groups. We re-check boundary fetches across rg
  // transitions on both columns in the same segment.
  constexpr uint64_t kBigRows = 2048;
  constexpr std::string_view kBigSeg = "_big";
  {
    irs::ColWriter w{dir, kBigSeg, db};
    auto& id_w = w.OpenColumn(
      kIdCol, duckdb::LogicalType::BLOB, /*skip_validity=*/false,
      /*row_group_size=*/512, duckdb::CompressionType::COMPRESSION_AUTO);
    auto& name_w = w.OpenColumn(kNameCol, duckdb::LogicalType::BLOB,
                                /*skip_validity=*/false, /*row_group_size=*/512,
                                duckdb::CompressionType::COMPRESSION_AUTO);
    for (uint64_t i = 0; i < kBigRows; ++i) {
      const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
      irs::bstring id_buf;
      irs::bstring name_buf;
      {
        irs::tests::BstringDataOutput out{id_buf};
        irs::WriteStr(out, std::to_string(3000 + i));
      }
      {
        irs::tests::BstringDataOutput out{name_buf};
        irs::WriteStr(out, std::string{"name_"} + std::to_string(3000 + i));
      }
      irs::tests::AppendBlob(id_w, doc, {id_buf.data(), id_buf.size()});
      irs::tests::AppendBlob(name_w, doc, {name_buf.data(), name_buf.size()});
    }
    w.Commit(kBigRows);
  }
  {
    irs::ColReader r{dir, kBigSeg, db};
    const auto* id_col = r.Column(kIdCol);
    const auto* name_col = r.Column(kNameCol);
    ASSERT_NE(id_col, nullptr);
    ASSERT_NE(name_col, nullptr);
    EXPECT_EQ(id_col->RowCount(), kBigRows);
    EXPECT_EQ(name_col->RowCount(), kBigRows);
    EXPECT_GE(id_col->DataRgCount(), 2u);
    EXPECT_GE(name_col->DataRgCount(), 2u);
    // Sample probes at row-group boundaries on both columns.
    const std::vector<uint64_t> boundary_rows = {
      0, 1, 511, 512, 513, 1023, 1024, 1025, 1535, 1536, 1537, kBigRows - 1};
    for (auto i : boundary_rows) {
      irs::ColumnReader::BlobPointReader id_pr{r, *id_col};
      irs::ColumnReader::BlobPointReader name_pr{r, *name_col};
      const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
      EXPECT_EQ(irs::tests::ReadStoredStr<std::string>(id_pr, doc),
                std::to_string(3000 + i))
        << "i=" << i;
      EXPECT_EQ(irs::tests::ReadStoredStr<std::string>(name_pr, doc),
                std::string{"name_"} + std::to_string(3000 + i))
        << "i=" << i;
    }
    // Seek+iterate over a boundary.
    {
      irs::ColumnReader::BlobPointReader id_pr{r, *id_col};
      for (uint64_t i = 510; i <= 515; ++i) {
        const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
        EXPECT_EQ(irs::tests::ReadStoredStr<std::string>(id_pr, doc),
                  std::to_string(3000 + i))
          << "i=" << i;
      }
    }
  }
}

TEST_P(FormatTestCase, columns_rw_same_col_empty_repeat) {
  // Writer keys columns by `field_id` and allows OpenColumn once per id.
  // Test the contract "two BLOB columns, both written densely with the
  // same row count, and both readable back independently" with two
  // empty-payload writes per doc per column.
  constexpr uint64_t kRowCount = 100;
  constexpr irs::field_id kIdCol = 12;
  constexpr irs::field_id kNameCol = 13;
  RoundTrip(
    "same_col_empty_repeat", kRowCount,
    [&](irs::ColWriter& w) {
      auto& id_w = irs::tests::OpenBlobColumn(w, kIdCol);
      auto& name_w = irs::tests::OpenBlobColumn(w, kNameCol);
      for (uint64_t i = 0; i < kRowCount; ++i) {
        const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
        // Length 0 or 1 per doc.
        const bool with_byte = (i % 2 == 0);
        irs::bstring id_buf;
        irs::bstring name_buf;
        if (with_byte) {
          irs::tests::BstringDataOutput out{id_buf};
          irs::WriteStr(out, std::string_view{"x", 1});
          irs::tests::BstringDataOutput out2{name_buf};
          irs::WriteStr(out2, std::string_view{"y", 1});
        } else {
          irs::tests::BstringDataOutput out{id_buf};
          irs::WriteStr(out, std::string_view{});
          irs::tests::BstringDataOutput out2{name_buf};
          irs::WriteStr(out2, std::string_view{});
        }
        irs::tests::AppendBlob(id_w, doc, {id_buf.data(), id_buf.size()});
        irs::tests::AppendBlob(name_w, doc, {name_buf.data(), name_buf.size()});
      }
    },
    [&](const irs::ColReader& r) {
      ASSERT_TRUE(r.HasColumn(kIdCol));
      ASSERT_TRUE(r.HasColumn(kNameCol));
      EXPECT_EQ(r.Columns().size(), 2u);

      const auto* id_col = r.Column(kIdCol);
      const auto* name_col = r.Column(kNameCol);
      ASSERT_NE(id_col, nullptr);
      ASSERT_NE(name_col, nullptr);
      EXPECT_EQ(id_col->RowCount(), kRowCount);
      EXPECT_EQ(name_col->RowCount(), kRowCount);

      irs::ColumnReader::BlobPointReader id_pr{r, *id_col};
      irs::ColumnReader::BlobPointReader name_pr{r, *name_col};
      for (uint64_t i = 0; i < kRowCount; ++i) {
        const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
        const bool with_byte = (i % 2 == 0);
        const auto exp_id =
          with_byte ? std::string_view{"x", 1} : std::string_view{};
        const auto exp_name =
          with_byte ? std::string_view{"y", 1} : std::string_view{};
        EXPECT_EQ(irs::tests::ReadStoredStr<std::string_view>(id_pr, doc),
                  exp_id)
          << "i=" << i;
        EXPECT_EQ(irs::tests::ReadStoredStr<std::string_view>(name_pr, doc),
                  exp_name)
          << "i=" << i;
      }

      // Multi-column independence: interleaved fetches on fresh readers
      // for both columns at the same doc -- result of one must not bleed
      // into the other.
      for (uint64_t i : {0u, 17u, 32u, 49u, 66u, 99u}) {
        const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
        irs::ColumnReader::BlobPointReader id_pr2{r, *id_col};
        irs::ColumnReader::BlobPointReader name_pr2{r, *name_col};
        const bool with_byte = (i % 2 == 0);
        const auto exp_id =
          with_byte ? std::string_view{"x", 1} : std::string_view{};
        const auto exp_name =
          with_byte ? std::string_view{"y", 1} : std::string_view{};
        EXPECT_EQ(irs::tests::ReadStoredStr<std::string_view>(id_pr2, doc),
                  exp_id);
        EXPECT_EQ(irs::tests::ReadStoredStr<std::string_view>(name_pr2, doc),
                  exp_name);
        EXPECT_EQ(irs::tests::ReadStoredStr<std::string_view>(name_pr2, doc),
                  exp_name);  // re-fetch -- still correct
        EXPECT_EQ(irs::tests::ReadStoredStr<std::string_view>(id_pr2, doc),
                  exp_id);  // re-fetch -- still correct
      }

      // Random-order point access (reversed) on the same id column.
      for (uint64_t r_i = kRowCount; r_i-- > 0;) {
        const auto doc =
          static_cast<irs::doc_id_t>(r_i + irs::doc_limits::min());
        irs::ColumnReader::BlobPointReader id_pr2{r, *id_col};
        const bool with_byte = (r_i % 2 == 0);
        const auto exp_id =
          with_byte ? std::string_view{"x", 1} : std::string_view{};
        EXPECT_EQ(irs::tests::ReadStoredStr<std::string_view>(id_pr2, doc),
                  exp_id)
          << "r_i=" << r_i;
      }

      // Boundary docs: doc=min, last_valid (doc = kRowCount).
      {
        irs::ColumnReader::BlobPointReader id_pr2{r, *id_col};
        const auto exp_id_min = std::string_view{"x", 1};
        EXPECT_EQ(irs::tests::ReadStoredStr<std::string_view>(
                    id_pr2, irs::doc_limits::min()),
                  exp_id_min);
        irs::ColumnReader::BlobPointReader id_pr3{r, *id_col};
        const auto last_doc =
          static_cast<irs::doc_id_t>(kRowCount + irs::doc_limits::min() - 1);
        const bool with_byte = ((kRowCount - 1) % 2 == 0);
        const auto exp_id =
          with_byte ? std::string_view{"x", 1} : std::string_view{};
        EXPECT_EQ(irs::tests::ReadStoredStr<std::string_view>(id_pr3, last_doc),
                  exp_id);
      }

      // Wrong-column-id negative lookups.
      for (irs::field_id missing : {0, 1, 5, 11, 14, 100, 1000}) {
        EXPECT_FALSE(r.HasColumn(missing));
        EXPECT_EQ(r.Column(missing), nullptr);
      }
      EXPECT_FALSE(r.HasColumn(irs::field_limits::invalid()));
    });

  // --- Row-group transitions ----------------------------------------------
  // Repeat the same shape across 2048 rows at row_group_size=512, so
  // each column spans 4 row groups. Verify each column independently.
  constexpr uint64_t kBigRows = 2048;
  constexpr irs::field_id kBigIdCol = 500;
  constexpr irs::field_id kBigNameCol = 501;
  irs::MemoryDirectory dir;
  auto& db = ::sdb::DuckDBEngine::Instance().instance();
  {
    irs::ColWriter w{dir, "same_col_empty_repeat_rg", db};
    auto& id_w = w.OpenColumn(kBigIdCol, duckdb::LogicalType::BLOB,
                              /*skip_validity=*/false,
                              /*row_group_size=*/512,
                              duckdb::CompressionType::COMPRESSION_AUTO);
    auto& name_w = w.OpenColumn(kBigNameCol, duckdb::LogicalType::BLOB,
                                /*skip_validity=*/false,
                                /*row_group_size=*/512,
                                duckdb::CompressionType::COMPRESSION_AUTO);
    for (uint64_t i = 0; i < kBigRows; ++i) {
      const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
      const bool with_byte = (i % 2 == 0);
      irs::bstring id_buf;
      irs::bstring name_buf;
      {
        irs::tests::BstringDataOutput out{id_buf};
        irs::WriteStr(
          out, with_byte ? std::string_view{"x", 1} : std::string_view{});
      }
      {
        irs::tests::BstringDataOutput out{name_buf};
        irs::WriteStr(
          out, with_byte ? std::string_view{"y", 1} : std::string_view{});
      }
      irs::tests::AppendBlob(id_w, doc, {id_buf.data(), id_buf.size()});
      irs::tests::AppendBlob(name_w, doc, {name_buf.data(), name_buf.size()});
    }
    w.Commit(kBigRows);
  }
  {
    irs::ColReader r{dir, "same_col_empty_repeat_rg", db};
    const auto* id_col = r.Column(kBigIdCol);
    const auto* name_col = r.Column(kBigNameCol);
    ASSERT_NE(id_col, nullptr);
    ASSERT_NE(name_col, nullptr);
    EXPECT_EQ(id_col->RowCount(), kBigRows);
    EXPECT_EQ(name_col->RowCount(), kBigRows);
    EXPECT_GE(id_col->DataRgCount(), 2u);
    EXPECT_GE(name_col->DataRgCount(), 2u);

    // Probes at row-group boundaries on both columns.
    const std::vector<uint64_t> boundary_rows = {
      0, 1, 511, 512, 513, 1023, 1024, 1025, 1535, 1536, 1537, kBigRows - 1};
    for (auto i : boundary_rows) {
      const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
      const bool with_byte = (i % 2 == 0);
      const auto exp_id =
        with_byte ? std::string_view{"x", 1} : std::string_view{};
      const auto exp_name =
        with_byte ? std::string_view{"y", 1} : std::string_view{};
      irs::ColumnReader::BlobPointReader id_pr{r, *id_col};
      irs::ColumnReader::BlobPointReader name_pr{r, *name_col};
      EXPECT_EQ(irs::tests::ReadStoredStr<std::string_view>(id_pr, doc), exp_id)
        << "i=" << i;
      EXPECT_EQ(irs::tests::ReadStoredStr<std::string_view>(name_pr, doc),
                exp_name)
        << "i=" << i;
    }
  }
}

TEST_P(FormatTestCase, columns_rw_sparse_column_dense_block) {
  // Two dense ranges with a single-doc gap; the gap is a null row.
  // Verifies:
  //  * RowCount matches the writer's target row,
  //  * each live row reports its payload + IsNullRow=false,
  //  * the gap rows are flagged null,
  //  * a visit pass produces exactly the two dense ranges in order.
  constexpr uint64_t kRowCount = 2037;
  const auto live = [](uint64_t i) {
    // Live: [0, 1024) and [1025, 2037), gap at row 1024.
    return i != 1024;
  };
  const irs::byte_type kPayloadBytes[] = {'a', 'b', 'c', 'd'};
  const irs::bytes_view payload{kPayloadBytes, sizeof(kPayloadBytes)};
  RoundTrip(
    "sparse_dense_block", kRowCount,
    [&](irs::ColWriter& w) {
      auto& cw = irs::tests::OpenBlobColumn(w, /*id=*/13);
      for (uint64_t i = 0; i < kRowCount; ++i) {
        const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
        if (live(i)) {
          irs::tests::AppendBlob(cw, doc, payload);
        } else {
          irs::tests::AppendNullBlob(cw, doc);
        }
      }
    },
    [&](const irs::ColReader& r) {
      ASSERT_TRUE(r.HasColumn(13));
      const auto* col = r.Column(13);
      ASSERT_NE(col, nullptr);
      EXPECT_EQ(col->RowCount(), kRowCount);
      EXPECT_TRUE(col->HasValidity());

      // Pass 1: per-row point read.
      {
        irs::ColumnReader::BlobPointReader pr{r, *col};
        for (uint64_t i = 0; i < kRowCount; ++i) {
          if (live(i)) {
            ASSERT_FALSE(pr.IsNullRow(i)) << "i=" << i;
            const auto v = pr.FetchRow(i);
            ASSERT_EQ(v.size(), payload.size()) << "i=" << i;
            EXPECT_EQ(0, std::memcmp(v.data(), payload.data(), v.size()))
              << "i=" << i;
          } else {
            EXPECT_TRUE(pr.IsNullRow(i)) << "i=" << i;
          }
        }
      }

      // Pass 2: visit -- callback fires only on live rows, in order.
      {
        uint64_t seen = 0;
        uint64_t expected_row = 0;
        const bool ok = irs::tests::VisitBlobColumn(
          r, *col, [&](irs::doc_id_t doc, irs::bytes_view v) {
            // Skip non-live rows.
            while (!live(expected_row)) {
              ++expected_row;
            }
            EXPECT_EQ(doc, static_cast<irs::doc_id_t>(expected_row +
                                                      irs::doc_limits::min()))
              << "expected_row=" << expected_row;
            EXPECT_EQ(v.size(), payload.size()) << "row=" << expected_row;
            ++expected_row;
            ++seen;
            return true;
          });
        EXPECT_TRUE(ok);
        EXPECT_EQ(seen, kRowCount - 1);  // one gap
      }

      // Pass 3: random-order point access -- probes across the gap on
      // fresh PointReaders each.
      {
        const std::vector<uint64_t> probes = {0,    1023, 1024, 1025,
                                              1026, 2036, 500,  1500};
        for (auto i : probes) {
          irs::ColumnReader::BlobPointReader pr{r, *col};
          if (live(i)) {
            EXPECT_FALSE(pr.IsNullRow(i)) << "i=" << i;
            const auto v = pr.FetchRow(i);
            EXPECT_EQ(v.size(), payload.size()) << "i=" << i;
            EXPECT_EQ(0, std::memcmp(v.data(), payload.data(), v.size()));
          } else {
            EXPECT_TRUE(pr.IsNullRow(i)) << "i=" << i;
          }
        }
      }

      // Pass 4: boundary rows -- row 0, last_valid (kRowCount-1).
      {
        irs::ColumnReader::BlobPointReader pr{r, *col};
        EXPECT_FALSE(pr.IsNullRow(0));
        EXPECT_EQ(pr.FetchRow(0).size(), payload.size());

        irs::ColumnReader::BlobPointReader pr2{r, *col};
        EXPECT_FALSE(pr2.IsNullRow(kRowCount - 1));
        EXPECT_EQ(pr2.FetchRow(kRowCount - 1).size(), payload.size());

        // Row just before/after the gap.
        irs::ColumnReader::BlobPointReader pr3{r, *col};
        EXPECT_FALSE(pr3.IsNullRow(1023));
        EXPECT_TRUE(pr3.IsNullRow(1024));
        EXPECT_FALSE(pr3.IsNullRow(1025));
      }

      // Pass 5: seek+iterate spanning the gap.
      {
        irs::ColumnReader::BlobPointReader pr{r, *col};
        for (uint64_t i = 1022; i <= 1027; ++i) {
          if (live(i)) {
            EXPECT_FALSE(pr.IsNullRow(i)) << "i=" << i;
            EXPECT_EQ(pr.FetchRow(i).size(), payload.size()) << "i=" << i;
          } else {
            EXPECT_TRUE(pr.IsNullRow(i)) << "i=" << i;
          }
        }
      }

      // Pass 6: wrong-column-id negative lookups.
      EXPECT_FALSE(r.HasColumn(0));
      EXPECT_FALSE(r.HasColumn(14));
      EXPECT_EQ(r.Column(14), nullptr);
      EXPECT_FALSE(r.HasColumn(irs::field_limits::invalid()));
    });

  // --- Row-group transitions ----------------------------------------------
  // Same shape at row_group_size=512: the gap at row 1024 sits exactly at
  // a row-group boundary, exercising the "skip across rg" path. With
  // kBigRows=2049 we span 5 row groups (4 full, 1 partial).
  constexpr uint64_t kBigRows = 2049;
  constexpr irs::field_id kBigId = 600;
  irs::MemoryDirectory dir;
  auto& db = ::sdb::DuckDBEngine::Instance().instance();
  {
    irs::ColWriter w{dir, "sparse_dense_block_rg", db};
    auto& cw = w.OpenColumn(kBigId, duckdb::LogicalType::BLOB,
                            /*skip_validity=*/false,
                            /*row_group_size=*/512,
                            duckdb::CompressionType::COMPRESSION_AUTO);
    for (uint64_t i = 0; i < kBigRows; ++i) {
      const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
      // Two gaps: at rg boundary (1024) and mid-rg (1500).
      if (i == 1024 || i == 1500) {
        irs::tests::AppendNullBlob(cw, doc);
      } else {
        irs::tests::AppendBlob(cw, doc, payload);
      }
    }
    w.Commit(kBigRows);
  }
  {
    irs::ColReader r{dir, "sparse_dense_block_rg", db};
    const auto* col = r.Column(kBigId);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->RowCount(), kBigRows);
    EXPECT_TRUE(col->HasValidity());
    EXPECT_GE(col->DataRgCount(), 2u);

    // Per-row check across the rg-boundary gap.
    {
      irs::ColumnReader::BlobPointReader pr{r, *col};
      for (uint64_t i = 1022; i <= 1027; ++i) {
        const bool expect_live = (i != 1024);
        EXPECT_EQ(pr.IsNullRow(i), !expect_live) << "i=" << i;
        if (expect_live) {
          EXPECT_EQ(pr.FetchRow(i).size(), payload.size()) << "i=" << i;
        }
      }
    }
    // Random-order probes spanning multiple row groups (fresh reader
    // per probe).
    const std::vector<uint64_t> boundary_rows = {
      0,    1,    511,  512,  513,  1023, 1024,        1025,
      1499, 1500, 1501, 1535, 1536, 2047, kBigRows - 1};
    for (auto i : boundary_rows) {
      irs::ColumnReader::BlobPointReader pr{r, *col};
      const bool expect_live = (i != 1024 && i != 1500);
      EXPECT_EQ(pr.IsNullRow(i), !expect_live) << "i=" << i;
      if (expect_live) {
        EXPECT_EQ(pr.FetchRow(i).size(), payload.size()) << "i=" << i;
      }
    }
  }
}

TEST_P(FormatTestCase, columns_rw_sparse_dense_offset_column_border_case) {
  // Two BLOB columns in one writer:
  //   * a "dense" column with rows at doc=1 (empty payload) and doc=2
  //     (payload),
  //   * a "sparse" column with rows at doc=1 (empty payload) and doc=4
  //     (payload).
  // Verify each column's point-read + visit independently.
  static constexpr irs::field_id kDenseId = 14;
  static constexpr irs::field_id kSparseId = 15;
  static constexpr uint64_t kRowCount = 4;  // covers docs [1..4]
  static constexpr uint64_t kKeys[] = {42, 42};
  const irs::bytes_view keys_ref{
    reinterpret_cast<const irs::byte_type*>(&kKeys), sizeof(kKeys)};

  RoundTrip(
    "border_case", kRowCount,
    [&](irs::ColWriter& w) {
      auto& dense = irs::tests::OpenBlobColumn(w, kDenseId);
      auto& sparse = irs::tests::OpenBlobColumn(w, kSparseId);

      // Dense column: doc=1 empty payload, doc=2 with keys_ref.
      irs::tests::AppendBlob(dense, irs::doc_limits::min(), irs::bytes_view{});
      irs::tests::AppendBlob(dense, irs::doc_limits::min() + 1, keys_ref);
      dense.PadNullsTo(kRowCount);

      // Sparse column: doc=1 empty payload, doc=4 with keys_ref.
      irs::tests::AppendBlob(sparse, irs::doc_limits::min(), irs::bytes_view{});
      irs::tests::AppendNullBlob(sparse, irs::doc_limits::min() + 1);
      irs::tests::AppendNullBlob(sparse, irs::doc_limits::min() + 2);
      irs::tests::AppendBlob(sparse, irs::doc_limits::min() + 3, keys_ref);
    },
    [&](const irs::ColReader& r) {
      ASSERT_TRUE(r.HasColumn(kDenseId));
      ASSERT_TRUE(r.HasColumn(kSparseId));

      // Dense column checks.
      {
        const auto* col = r.Column(kDenseId);
        ASSERT_NE(col, nullptr);
        EXPECT_EQ(col->RowCount(), kRowCount);

        // Point-read.
        irs::ColumnReader::BlobPointReader pr{r, *col};
        EXPECT_FALSE(pr.IsNullRow(0));
        EXPECT_TRUE(pr.FetchRow(0).empty());
        EXPECT_FALSE(pr.IsNullRow(1));
        const auto v = pr.FetchRow(1);
        EXPECT_EQ(v.size(), keys_ref.size());
        EXPECT_EQ(0, std::memcmp(v.data(), keys_ref.data(), v.size()));
        EXPECT_TRUE(pr.IsNullRow(2));
        EXPECT_TRUE(pr.IsNullRow(3));

        // Visit: dense column yields rows 0 and 1.
        // (bytes_view aliases the codec's scratch buffer -- assert inline
        // rather than storing views in a vector.)
        size_t seen = 0;
        const bool ok = irs::tests::VisitBlobColumn(
          r, *col, [&](irs::doc_id_t doc, irs::bytes_view v) {
            if (seen == 0) {
              EXPECT_EQ(doc, irs::doc_limits::min());
              EXPECT_TRUE(v.empty());
            } else if (seen == 1) {
              EXPECT_EQ(doc, irs::doc_limits::min() + 1);
              EXPECT_EQ(v.size(), keys_ref.size());
              EXPECT_EQ(
                0, std::memcmp(v.data(), keys_ref.data(), keys_ref.size()));
            }
            ++seen;
            return true;
          });
        EXPECT_TRUE(ok);
        EXPECT_EQ(seen, 2u);
      }

      // Sparse column checks.
      {
        const auto* col = r.Column(kSparseId);
        ASSERT_NE(col, nullptr);
        EXPECT_EQ(col->RowCount(), kRowCount);
        EXPECT_TRUE(col->HasValidity());

        // Point-read.
        irs::ColumnReader::BlobPointReader pr{r, *col};
        EXPECT_FALSE(pr.IsNullRow(0));
        EXPECT_TRUE(pr.FetchRow(0).empty());
        EXPECT_TRUE(pr.IsNullRow(1));
        EXPECT_TRUE(pr.IsNullRow(2));
        EXPECT_FALSE(pr.IsNullRow(3));
        const auto v = pr.FetchRow(3);
        EXPECT_EQ(v.size(), keys_ref.size());
        EXPECT_EQ(0, std::memcmp(v.data(), keys_ref.data(), v.size()));

        // Visit: sparse column yields rows 0 and 3.
        size_t seen = 0;
        const bool ok = irs::tests::VisitBlobColumn(
          r, *col, [&](irs::doc_id_t doc, irs::bytes_view v) {
            if (seen == 0) {
              EXPECT_EQ(doc, irs::doc_limits::min());
              EXPECT_TRUE(v.empty());
            } else if (seen == 1) {
              EXPECT_EQ(doc, irs::doc_limits::min() + 3);
              EXPECT_EQ(v.size(), keys_ref.size());
              EXPECT_EQ(
                0, std::memcmp(v.data(), keys_ref.data(), keys_ref.size()));
            }
            ++seen;
            return true;
          });
        EXPECT_TRUE(ok);
        EXPECT_EQ(seen, 2u);
      }

      // Multi-column independence: interleaved fetches on both columns.
      // The dense column has rows at 0 (empty) and 1 (keys_ref). The
      // sparse column has rows at 0 (empty) and 3 (keys_ref). A
      // fetch on one must not influence the other's lookup.
      {
        const auto* dense_col = r.Column(kDenseId);
        const auto* sparse_col = r.Column(kSparseId);
        ASSERT_NE(dense_col, nullptr);
        ASSERT_NE(sparse_col, nullptr);
        irs::ColumnReader::BlobPointReader dpr{r, *dense_col};
        irs::ColumnReader::BlobPointReader spr{r, *sparse_col};
        // Row 0 on both.
        EXPECT_FALSE(dpr.IsNullRow(0));
        EXPECT_TRUE(dpr.FetchRow(0).empty());
        EXPECT_FALSE(spr.IsNullRow(0));
        EXPECT_TRUE(spr.FetchRow(0).empty());
        // Row 1: dense has keys, sparse is null.
        EXPECT_FALSE(dpr.IsNullRow(1));
        EXPECT_EQ(dpr.FetchRow(1).size(), keys_ref.size());
        EXPECT_TRUE(spr.IsNullRow(1));
        // Row 3: dense null, sparse has keys.
        EXPECT_TRUE(dpr.IsNullRow(3));
        EXPECT_FALSE(spr.IsNullRow(3));
        EXPECT_EQ(spr.FetchRow(3).size(), keys_ref.size());
      }

      // Boundary docs on each column: doc=min, last_valid (= doc=4).
      {
        const auto* dense_col = r.Column(kDenseId);
        irs::ColumnReader::BlobPointReader dpr{r, *dense_col};
        EXPECT_FALSE(dpr.IsNullDoc(irs::doc_limits::min()));
        irs::ColumnReader::BlobPointReader dpr2{r, *dense_col};
        // doc=4 (row 3) is null in dense.
        EXPECT_TRUE(dpr2.IsNullDoc(irs::doc_limits::min() + 3));

        const auto* sparse_col = r.Column(kSparseId);
        irs::ColumnReader::BlobPointReader spr{r, *sparse_col};
        EXPECT_FALSE(spr.IsNullDoc(irs::doc_limits::min()));
        irs::ColumnReader::BlobPointReader spr2{r, *sparse_col};
        EXPECT_FALSE(spr2.IsNullDoc(irs::doc_limits::min() + 3));
      }

      // Random-order point access on both columns (reversed).
      {
        const auto* dense_col = r.Column(kDenseId);
        const auto* sparse_col = r.Column(kSparseId);
        for (uint64_t i = kRowCount; i-- > 0;) {
          irs::ColumnReader::BlobPointReader dpr{r, *dense_col};
          irs::ColumnReader::BlobPointReader spr{r, *sparse_col};
          // Dense: live at 0, 1; null at 2, 3.
          EXPECT_EQ(dpr.IsNullRow(i), i >= 2) << "i=" << i;
          // Sparse: live at 0, 3; null at 1, 2.
          EXPECT_EQ(spr.IsNullRow(i), i == 1 || i == 2) << "i=" << i;
        }
      }

      // Wrong-column-id negative lookups.
      for (irs::field_id missing : {0, 1, 13, 16, 100, 1000}) {
        EXPECT_FALSE(r.HasColumn(missing)) << "id=" << missing;
        EXPECT_EQ(r.Column(missing), nullptr) << "id=" << missing;
      }
    });

  // --- Row-group transitions ----------------------------------------------
  // Border case repeated at kBigRows=2048 with row_group_size=512 so that
  // the dense + sparse pattern crosses several row groups. Dense column:
  // rows 0..1023 with payload, rows 1024..2047 padded null. Sparse: rows
  // at i % 137 with payload, others null (~14 live rows over 4 rgs).
  constexpr uint64_t kBigRows = 2048;
  constexpr irs::field_id kBigDenseId = 700;
  constexpr irs::field_id kBigSparseId = 701;
  irs::MemoryDirectory dir;
  auto& db = ::sdb::DuckDBEngine::Instance().instance();
  {
    irs::ColWriter w{dir, "border_case_rg", db};
    auto& dense = w.OpenColumn(kBigDenseId, duckdb::LogicalType::BLOB,
                               /*skip_validity=*/false,
                               /*row_group_size=*/512,
                               duckdb::CompressionType::COMPRESSION_AUTO);
    auto& sparse = w.OpenColumn(kBigSparseId, duckdb::LogicalType::BLOB,
                                /*skip_validity=*/false,
                                /*row_group_size=*/512,
                                duckdb::CompressionType::COMPRESSION_AUTO);
    // Dense: first half live.
    for (uint64_t i = 0; i < 1024; ++i) {
      irs::tests::AppendBlob(
        dense, static_cast<irs::doc_id_t>(i + irs::doc_limits::min()),
        keys_ref);
    }
    dense.PadNullsTo(kBigRows);

    // Sparse: every 137th row live.
    for (uint64_t i = 0; i < kBigRows; ++i) {
      const auto doc = static_cast<irs::doc_id_t>(i + irs::doc_limits::min());
      if (i % 137 == 0) {
        irs::tests::AppendBlob(sparse, doc, keys_ref);
      } else {
        irs::tests::AppendNullBlob(sparse, doc);
      }
    }
    w.Commit(kBigRows);
  }
  {
    irs::ColReader r{dir, "border_case_rg", db};
    const auto* dense_col = r.Column(kBigDenseId);
    const auto* sparse_col = r.Column(kBigSparseId);
    ASSERT_NE(dense_col, nullptr);
    ASSERT_NE(sparse_col, nullptr);
    EXPECT_EQ(dense_col->RowCount(), kBigRows);
    EXPECT_EQ(sparse_col->RowCount(), kBigRows);
    EXPECT_GE(dense_col->DataRgCount(), 2u);
    EXPECT_GE(sparse_col->ValidityRgCount(), 2u);

    // Dense column at boundaries.
    const std::vector<uint64_t> boundary_rows = {
      0, 1, 511, 512, 513, 1023, 1024, 1025, 1535, 1536, 1537, kBigRows - 1};
    for (auto i : boundary_rows) {
      irs::ColumnReader::BlobPointReader pr{r, *dense_col};
      const bool expect_live = (i < 1024);
      EXPECT_EQ(pr.IsNullRow(i), !expect_live) << "i=" << i;
      if (expect_live) {
        EXPECT_EQ(pr.FetchRow(i).size(), keys_ref.size()) << "i=" << i;
      }
    }

    // Sparse column at boundaries (sparse pattern).
    for (auto i : boundary_rows) {
      irs::ColumnReader::BlobPointReader pr{r, *sparse_col};
      const bool expect_live = (i % 137 == 0);
      EXPECT_EQ(pr.IsNullRow(i), !expect_live) << "i=" << i;
      if (expect_live) {
        EXPECT_EQ(pr.FetchRow(i).size(), keys_ref.size()) << "i=" << i;
      }
    }
  }
}

TEST_P(FormatTestCase, columns_issue700) {
  // Issue 700: writes of varying payload lengths across a row-group
  // boundary used to corrupt the offset table. Two-fixed-sizes shape
  // (size 26 then size 25 across the boundary) plus a varying-payload
  // sweep that crosses STANDARD_VECTOR_SIZE.
  constexpr uint64_t kRowCount = STANDARD_VECTOR_SIZE * 2 + 1;
  constexpr irs::field_id kColId = 15;
  RoundTrip(
    "issue700", kRowCount,
    [&](irs::ColWriter& w) {
      auto& cw = irs::tests::OpenBlobColumn(w, kColId);
      for (uint64_t i = 0; i < kRowCount; ++i) {
        // Varying payload sizes (mod 5). 1..5 byte payloads exercise
        // per-row offset bookkeeping across the row-group boundary.
        std::string payload(1 + (i % 5), '0' + static_cast<char>(i % 10));
        irs::tests::AppendBlob(
          cw, static_cast<irs::doc_id_t>(i + irs::doc_limits::min()),
          irs::bytes_view{
            reinterpret_cast<const irs::byte_type*>(payload.data()),
            payload.size()});
      }
    },
    [&](const irs::ColReader& r) {
      ASSERT_EQ(r.Columns().size(), 1u);
      ASSERT_TRUE(r.HasColumn(kColId));
      const auto* col = r.Column(kColId);
      ASSERT_NE(col, nullptr);
      EXPECT_EQ(col->RowCount(), kRowCount);

      // Pass 1: point-read every row.
      {
        irs::ColumnReader::BlobPointReader pr{r, *col};
        for (uint64_t i = 0; i < kRowCount; ++i) {
          const auto bytes = pr.FetchRow(i);
          ASSERT_EQ(bytes.size(), 1u + (i % 5)) << "i=" << i;
          EXPECT_EQ(bytes[0], static_cast<irs::byte_type>('0' + (i % 10)));
        }
      }

      // Pass 2: visit covers every row in order, sizes alternate 1..5.
      {
        uint64_t i = 0;
        const bool ok = irs::tests::VisitBlobColumn(
          r, *col, [&](irs::doc_id_t doc, irs::bytes_view v) {
            EXPECT_EQ(doc,
                      static_cast<irs::doc_id_t>(i + irs::doc_limits::min()));
            EXPECT_EQ(v.size(), 1u + (i % 5));
            EXPECT_EQ(v[0], static_cast<irs::byte_type>('0' + (i % 10)));
            ++i;
            return true;
          });
        EXPECT_TRUE(ok);
        EXPECT_EQ(i, kRowCount);
      }

      // Pass 3: random spot checks that straddle the STANDARD_VECTOR_SIZE
      // boundary.
      {
        irs::ColumnReader::BlobPointReader pr{r, *col};
        const std::vector<uint64_t> probes = {0,
                                              STANDARD_VECTOR_SIZE - 1,
                                              STANDARD_VECTOR_SIZE,
                                              STANDARD_VECTOR_SIZE + 1,
                                              STANDARD_VECTOR_SIZE * 2 - 1,
                                              STANDARD_VECTOR_SIZE * 2,
                                              kRowCount - 1};
        for (auto i : probes) {
          ASSERT_LT(i, kRowCount);
          irs::ColumnReader::BlobPointReader pr2{r, *col};
          const auto bytes = pr2.FetchRow(i);
          ASSERT_EQ(bytes.size(), 1u + (i % 5)) << "i=" << i;
          EXPECT_EQ(bytes[0], static_cast<irs::byte_type>('0' + (i % 10)));
        }
      }

      // Pass 4: reversed walk (random-order via fresh readers).
      {
        for (uint64_t r_i = kRowCount; r_i-- > 0;) {
          irs::ColumnReader::BlobPointReader pr{r, *col};
          const auto bytes = pr.FetchRow(r_i);
          ASSERT_EQ(bytes.size(), 1u + (r_i % 5)) << "i=" << r_i;
          EXPECT_EQ(bytes[0], static_cast<irs::byte_type>('0' + (r_i % 10)))
            << "i=" << r_i;
        }
      }

      // Pass 5: prime-stepped scan across the vector-size boundary.
      // 11 coprime to kRowCount, walks the whole orbit eventually.
      {
        for (uint64_t k = 0; k < 100; ++k) {
          const uint64_t i = (k * 11) % kRowCount;
          irs::ColumnReader::BlobPointReader pr{r, *col};
          const auto bytes = pr.FetchRow(i);
          ASSERT_EQ(bytes.size(), 1u + (i % 5)) << "k=" << k << " i=" << i;
          EXPECT_EQ(bytes[0], static_cast<irs::byte_type>('0' + (i % 10)));
        }
      }

      // Pass 6: boundary docs -- doc=min and last_valid (doc=kRowCount).
      {
        irs::ColumnReader::BlobPointReader pr{r, *col};
        const auto bytes_min = pr.FetchDoc(irs::doc_limits::min());
        ASSERT_EQ(bytes_min.size(), 1u);
        EXPECT_EQ(bytes_min[0], static_cast<irs::byte_type>('0'));

        irs::ColumnReader::BlobPointReader pr2{r, *col};
        const auto bytes_last = pr2.FetchRow(kRowCount - 1);
        EXPECT_EQ(bytes_last.size(), 1u + ((kRowCount - 1) % 5));
      }

      // Pass 7: seek+iterate -- jump to K then K+1..K+5 sequentially.
      // K placed straddling the STANDARD_VECTOR_SIZE boundary.
      {
        irs::ColumnReader::BlobPointReader pr{r, *col};
        const uint64_t kStart = STANDARD_VECTOR_SIZE - 2;
        for (uint64_t i = kStart; i < kStart + 6; ++i) {
          const auto bytes = pr.FetchRow(i);
          ASSERT_EQ(bytes.size(), 1u + (i % 5)) << "i=" << i;
          EXPECT_EQ(bytes[0], static_cast<irs::byte_type>('0' + (i % 10)))
            << "i=" << i;
        }
      }

      // Pass 8: wrong-column-id negative lookups.
      for (irs::field_id missing : {0, 14, 16, 99, 1000}) {
        EXPECT_FALSE(r.HasColumn(missing));
        EXPECT_EQ(r.Column(missing), nullptr);
      }
      EXPECT_FALSE(r.HasColumn(irs::field_limits::invalid()));
    });

  // --- Row-group transitions ----------------------------------------------
  // Same varying-payload shape but with row_group_size=512 so the column
  // spans multiple row groups within the same scale (8 rgs for 4097 rows).
  constexpr uint64_t kBigRows = STANDARD_VECTOR_SIZE * 2 + 1;  // 4097
  constexpr irs::field_id kBigColId = 800;
  irs::MemoryDirectory dir;
  auto& db = ::sdb::DuckDBEngine::Instance().instance();
  {
    irs::ColWriter w{dir, "issue700_rg", db};
    auto& cw = w.OpenColumn(kBigColId, duckdb::LogicalType::BLOB,
                            /*skip_validity=*/false,
                            /*row_group_size=*/512,
                            duckdb::CompressionType::COMPRESSION_AUTO);
    for (uint64_t i = 0; i < kBigRows; ++i) {
      std::string payload(1 + (i % 5), '0' + static_cast<char>(i % 10));
      irs::tests::AppendBlob(
        cw, static_cast<irs::doc_id_t>(i + irs::doc_limits::min()),
        irs::bytes_view{reinterpret_cast<const irs::byte_type*>(payload.data()),
                        payload.size()});
    }
    w.Commit(kBigRows);
  }
  {
    irs::ColReader r{dir, "issue700_rg", db};
    const auto* col = r.Column(kBigColId);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->RowCount(), kBigRows);
    EXPECT_GE(col->DataRgCount(), 2u);

    // Probes at row-group + vector-size boundaries.
    const std::vector<uint64_t> boundary_rows = {0,
                                                 1,
                                                 511,
                                                 512,
                                                 513,
                                                 1023,
                                                 1024,
                                                 1025,
                                                 STANDARD_VECTOR_SIZE - 1,
                                                 STANDARD_VECTOR_SIZE,
                                                 STANDARD_VECTOR_SIZE + 1,
                                                 1535,
                                                 1536,
                                                 1537,
                                                 STANDARD_VECTOR_SIZE * 2 - 1,
                                                 STANDARD_VECTOR_SIZE * 2,
                                                 kBigRows - 1};
    for (auto i : boundary_rows) {
      ASSERT_LT(i, kBigRows);
      irs::ColumnReader::BlobPointReader pr{r, *col};
      const auto bytes = pr.FetchRow(i);
      ASSERT_EQ(bytes.size(), 1u + (i % 5)) << "i=" << i;
      EXPECT_EQ(bytes[0], static_cast<irs::byte_type>('0' + (i % 10)));
    }

    // Seek+iterate spanning a row-group boundary (start at 510, walk to
    // 515).
    {
      irs::ColumnReader::BlobPointReader pr{r, *col};
      for (uint64_t i = 510; i <= 515; ++i) {
        const auto bytes = pr.FetchRow(i);
        ASSERT_EQ(bytes.size(), 1u + (i % 5)) << "i=" << i;
        EXPECT_EQ(bytes[0], static_cast<irs::byte_type>('0' + (i % 10)));
      }
    }
  }
}

TEST_P(FormatTestCaseWithEncryption, columnstore_read_write_wrong_encryption) {
  GTEST_SKIP() << "columnstore encryption not yet supported";
}

TEST_P(FormatTestCaseWithEncryption, write_zero_block_encryption) {
  GTEST_SKIP() << "columnstore encryption not yet supported";
}

}  // namespace tests
