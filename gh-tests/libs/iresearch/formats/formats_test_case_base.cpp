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
#include <iresearch/formats/format_utils.hpp>
#include <iresearch/index/column_finalizer.hpp>
#include <iresearch/index/norm.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/utils/lz4compression.hpp>
#include <unordered_map>
#include <unordered_set>

#include "basics/resource_manager.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/index/index_meta.hpp"
#include "utils/write_helpers.hpp"

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

irs::ColumnFinalizer ColumnFinalizer(
  uint32_t value, std::string_view name = std::string_view{}) {
  return irs::ColumnFinalizer{
    [value](irs::DataOutput& out) {
      out.WriteU32(static_cast<uint32_t>(sizeof(value)));
      out.WriteU32(value);
    },
    [name] { return name; },
  };
}

irs::ColumnInfo FormatTestCase::lz4_column_info() const noexcept {
  return {
    .compression = irs::Type<irs::compression::Lz4>::get(),
    .options = irs::compression::Options{},
    .encryption = bool(dir().attributes().encryption()),
    .track_prev_doc = false,
  };
}

irs::ColumnInfo FormatTestCase::none_column_info() const noexcept {
  return {
    .compression = irs::Type<irs::compression::Lz4>::get(),
    .options = irs::compression::Options{},
    .encryption = bool(dir().attributes().encryption()),
    .track_prev_doc = false,
  };
}

void FormatTestCase::AssertFrequencyAndPositions(irs::DocIterator& expected,
                                                 irs::DocIterator& actual) {
  if (irs::doc_limits::eof(expected.value())) {
    ASSERT_TRUE(irs::doc_limits::eof(expected.value()));
    return;
  }

  auto* expected_freq = irs::get<irs::FreqAttr>(expected);
  auto* actual_freq = irs::get<irs::FreqAttr>(actual);
  ASSERT_EQ(!expected_freq, !actual_freq);

  if (!expected_freq) {
    return;
  }

  ASSERT_EQ(expected_freq->value, actual_freq->value);

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

auto MakeByTerm(std::string_view name, std::string_view value) {
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field() = name;
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
  auto query_doc1 = MakeByTerm("name", "A");
  auto query_doc2 = MakeByTerm("name", "B");
  auto query_doc3 = MakeByTerm("name", "C");
  auto query_doc4 = MakeByTerm("name", "D");

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
    auto writer = irs::IndexWriter::Make(*dir, codec(), irs::kOmCreate);

    // initialize directory
    {
      writer->Commit();
      tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                    irs::DirectoryReader(*dir));
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec());
    }

    // add first segment
    {
      ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                         doc1->stored.begin(), doc1->stored.end()));
      ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                         doc2->stored.begin(), doc2->stored.end()));
      writer->Commit();
      tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                    irs::DirectoryReader(*dir));
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec());
    }

    // add second segment (creating new index_meta file, remove old)
    {
      ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                         doc3->stored.begin(), doc3->stored.end()));
      writer->Commit();
      tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                    irs::DirectoryReader(*dir));
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec());
    }

    // delete record from first segment (creating new index_meta file + doc_mask
    // file, remove old)
    {
      writer->GetBatch().Remove(*query_doc1);
      writer->Commit();
      tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                    irs::DirectoryReader(*dir));
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec());
    }

    // delete all record from first segment (creating new index_meta file,
    // remove old meta + unused segment)
    {
      writer->GetBatch().Remove(*query_doc2);
      writer->Commit();
      tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                    irs::DirectoryReader(*dir));
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec());
    }

    // delete all records from second segment (creating new index_meta file,
    // remove old meta + unused segment)
    {
      writer->GetBatch().Remove(*query_doc2);
      writer->Commit();
      tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                    irs::DirectoryReader(*dir));
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
    auto writer = irs::IndexWriter::Make(*dir, codec(), irs::kOmCreate);

    // initialize directory
    {
      writer->Commit();
      tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                    irs::DirectoryReader(*dir));
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec());
    }

    // add first segment
    {
      ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                         doc1->stored.begin(), doc1->stored.end()));
      ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                         doc2->stored.begin(), doc2->stored.end()));
      ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                         doc3->stored.begin(), doc3->stored.end()));
      writer->Commit();
      tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                    irs::DirectoryReader(*dir));
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec());
    }

    // delete record from first segment (creating new doc_mask file)
    {
      writer->GetBatch().Remove(*query_doc1);
      writer->Commit();
      tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                    irs::DirectoryReader(*dir));
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec());
    }

    // create reader to directory
    auto reader = irs::DirectoryReader(*dir, codec());
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
      ASSERT_TRUE(Insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                         doc4->stored.begin(), doc4->stored.end()));
      writer->Commit();
      tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                    irs::DirectoryReader(*dir));
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec(), reader_files);
    }

    // delete record from first segment (creating new doc_mask file, not-remove
    // old)
    {
      writer->GetBatch().Remove(*(query_doc2));
      writer->Commit();
      tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                    irs::DirectoryReader(*dir));
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec(), reader_files);
    }

    // delete all record from first segment (creating new index_meta file,
    // remove old meta but leave first segment)
    {
      writer->GetBatch().Remove(*(query_doc3));
      writer->Commit();
      tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                    irs::DirectoryReader(*dir));
      irs::DirectoryCleaner::clean(*dir);  // clean unused files
      AssertNoDirectoryArtifacts(*dir, *codec(), reader_files);
    }

    // delete all records from second segment (creating new index_meta file,
    // remove old meta + unused segment)
    {
      writer->GetBatch().Remove(*(query_doc4));
      writer->Commit();
      tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                    irs::DirectoryReader(*dir));
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
      auto writer = irs::IndexWriter::Make(*dir, codec(), irs::kOmCreate);

      writer->Commit();  // initialize directory
      tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                    irs::DirectoryReader(*dir));
      ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                         doc1->stored.begin(), doc1->stored.end()));
      writer->Commit();  // add first segment
      tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                    irs::DirectoryReader(*dir));
      ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                         doc2->stored.begin(), doc2->stored.end()));
      ASSERT_TRUE(Insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                         doc3->stored.begin(), doc3->stored.end()));
      writer->Commit();  // add second segment
      tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                    irs::DirectoryReader(*dir));
      writer->GetBatch().Remove(*(query_doc1));
      writer->Commit();  // remove first segment
      tests::AssertSnapshotEquality(writer->GetSnapshot(),
                                    irs::DirectoryReader(*dir));
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
    auto writer = irs::IndexWriter::Make(*dir, codec(), irs::kOmCreate);

    // if directory has files (for fs directory) then ensure only valid
    // meta+segments loaded
    ASSERT_TRUE(dir->exists(exists, "dummy.file.1") && !exists);
    ASSERT_TRUE(dir->exists(exists, "dummy.file.2") && !exists);
    AssertNoDirectoryArtifacts(*dir, *codec());
  }
}

TEST_P(FormatTestCase, fields_seek_ge) {
  class GranularDoubleField : public tests::DoubleField {};

  class NumericFieldGenerator : public tests::DocGeneratorBase {
   public:
    NumericFieldGenerator(size_t begin, size_t end, size_t step)
      : _value(begin), _begin(begin), _end(end), _step(step) {
      _field = std::make_shared<GranularDoubleField>();
      _field->Name("field");
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
  auto field = segment.field("field");
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
  irs::FieldMeta field;
  field.name = "field";
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
      terms, irs::FieldMeta{field.name, field.index_features},
      (sorted_terms.empty() ? irs::bytes_view{} : *sorted_terms.begin()),
      (sorted_terms.empty() ? irs::bytes_view{} : *sorted_terms.rbegin())};

    auto writer =
      codec()->get_field_writer(false, irs::IResourceManager::gNoop);
    writer->prepare(state);
    writer->write(term_reader);
    writer->end();
  }

  // read field
  {
    irs::SegmentMeta meta;
    meta.name = "segment_name";

    auto reader = codec()->get_field_reader(irs::IResourceManager::gNoop);
    reader->prepare(irs::ReaderState{.dir = &dir(), .meta = &meta});
    ASSERT_EQ(1, reader->size());

    // check terms
    ASSERT_EQ(nullptr, reader->field("invalid_field"));
    auto term_reader = reader->field(field.name);
    ASSERT_NE(nullptr, term_reader);
    ASSERT_EQ(field.name, term_reader->meta().name);
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
    meta.column_store = true;
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
      ASSERT_EQ(meta.column_store, read_meta.column_store);
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

TEST_P(FormatTestCase, columns_rw_sparse_column_dense_block) {
  irs::SegmentMeta seg;
  seg.name = "_1";
  seg.codec = codec();

  size_t column_id;
  const irs::bytes_view payload(
    irs::ViewCast<irs::byte_type>(std::string_view("abcd")));

  // write docs
  {
    auto writer =
      codec()->get_columnstore_writer(false, irs::IResourceManager::gNoop);
    writer->prepare(dir(), seg);
    auto column = writer->push_column(lz4_column_info(), ColumnFinalizer(42));
    column_id = column.id;
    auto& column_handler = column.out;

    auto id = irs::doc_limits::min();

    for (; id <= 1024; ++id, ++seg.docs_count) {
      auto& stream = column_handler(id);
      stream.WriteBytes(payload.data(), payload.size());
    }

    ++id;  // gap

    for (; id <= 2037; ++id, ++seg.docs_count) {
      auto& stream = column_handler(id);
      stream.WriteBytes(payload.data(), payload.size());
    }

    irs::FlushState state{
      .dir = &dir(),
      .name = seg.name,
      .doc_count = id - 1,
    };

    ASSERT_TRUE(writer->commit(state));
  }

  // read documents
  {
    auto reader = codec()->get_columnstore_reader();
    ASSERT_TRUE(reader->prepare(dir(), seg));

    auto column = reader->column(column_id);
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    irs::doc_id_t id = 0;

    for (++id; id < seg.docs_count; ++id) {
      if (id == 1025) {
        // gap
        ASSERT_EQ(id + 1, values->seek(id));
      } else {
        ASSERT_EQ(id, values->seek(id));
        ASSERT_EQ(payload, actual_value->value);
      }
    }
  }
}

TEST_P(FormatTestCase, columns_rw_dense_mask) {
  irs::SegmentMeta seg;
  seg.name = "_1";
  seg.codec = codec();
  const irs::doc_id_t max_doc = 1026;

  size_t column_id;

  // write docs
  {
    auto writer =
      codec()->get_columnstore_writer(false, irs::IResourceManager::gNoop);
    writer->prepare(dir(), seg);
    auto column = writer->push_column(lz4_column_info(), ColumnFinalizer(42));
    column_id = column.id;
    auto& column_handler = column.out;

    for (auto id = irs::doc_limits::min(); id <= max_doc;
         ++id, ++seg.docs_count) {
      column_handler(id);
    }

    irs::FlushState state{
      .dir = &dir(),
      .name = seg.name,
      .doc_count = max_doc,
    };

    ASSERT_TRUE(writer->commit(state));
  }

  // read documents
  {
    auto reader_1 = codec()->get_columnstore_reader();
    ASSERT_TRUE(reader_1->prepare(dir(), seg));

    auto column = reader_1->column(column_id);
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);

    for (irs::doc_id_t id = 0; id < seg.docs_count;) {
      ++id;
      ASSERT_EQ(id, values->seek(id));
      ASSERT_TRUE(!actual_value || irs::IsNull(actual_value->value));
    }
  }
}

TEST_P(FormatTestCase, columns_rw_bit_mask) {
  irs::SegmentMeta segment;
  segment.name = "bit_mask";
  irs::field_id id;

  segment.codec = codec();

  // write bit mask into the column without actual data
  {
    auto writer =
      codec()->get_columnstore_writer(false, irs::IResourceManager::gNoop);
    writer->prepare(dir(), segment);

    auto column = writer->push_column(lz4_column_info(), ColumnFinalizer(42));

    id = column.id;
    auto& handle = column.out;
    // we don't support irs::type_limits<<irs::type_t::doc_id_t>::invalid() key
    // value
    handle(2);
    ++segment.docs_count;
    handle(4);
    ++segment.docs_count;
    handle(8);
    ++segment.docs_count;
    handle(9);
    ++segment.docs_count;
    // we don't support irs::type_limits<<irs::type_t::doc_id_t>::eof() key
    // value

    irs::FlushState state{
      .dir = &dir(),
      .name = segment.name,
      .doc_count = segment.docs_count,
    };

    ASSERT_TRUE(writer->commit(state));
  }

  // check previously written mask
  // - random read (not cached)
  // - iterate (cached)
  {
    auto reader = codec()->get_columnstore_reader();
    ASSERT_TRUE(reader->prepare(dir(), segment));

    // read field values (not cached)
    {
      auto column = reader->column(id);
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_TRUE(!actual_value || irs::IsNull(actual_value->value));
      ASSERT_EQ(2, values->seek(1));
      ASSERT_TRUE(!actual_value || irs::IsNull(actual_value->value));
      ASSERT_EQ(2, values->seek(2));
      ASSERT_TRUE(!actual_value || irs::IsNull(actual_value->value));
      ASSERT_EQ(4, values->seek(4));
      ASSERT_TRUE(!actual_value || irs::IsNull(actual_value->value));
      ASSERT_EQ(8, values->seek(6));
      ASSERT_TRUE(!actual_value || irs::IsNull(actual_value->value));
      ASSERT_EQ(8, values->seek(8));
      ASSERT_TRUE(!actual_value || irs::IsNull(actual_value->value));
      ASSERT_EQ(9, values->seek(9));
      ASSERT_TRUE(!actual_value || irs::IsNull(actual_value->value));
      ASSERT_TRUE(irs::doc_limits::eof(values->seek(irs::doc_limits::eof())));
      ASSERT_TRUE(!actual_value || irs::IsNull(actual_value->value));
    }

    // seek over field values (cached)
    {
      auto column = reader->column(id);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      if (payload) {
        // if attribute is present, payload value has
        // to be always empty for mask column
        ASSERT_EQ(irs::doc_limits::invalid(), it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);
      }

      std::vector<std::pair<irs::doc_id_t, irs::doc_id_t>> expected_values = {
        {1, 2},
        {2, 2},
        {3, 4},
        {4, 4},
        {5, 8},
        {9, 9},
        {10, irs::doc_limits::eof()},
        {6, irs::doc_limits::eof()},
        {10, irs::doc_limits::eof()}};

      for (auto& pair : expected_values) {
        const auto value_to_find = pair.first;
        const auto expected_value = pair.second;

        ASSERT_EQ(expected_value, it->seek(value_to_find));
        if (payload) {
          // if attribute is present, payload value has
          // to be always empty for mask column
          ASSERT_TRUE(irs::IsNull(payload->value));
        }
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      if (payload) {
        // if attribute is present, payload value has
        // to be always empty for mask column
        ASSERT_TRUE(irs::IsNull(payload->value));
      }
    }

    // iterate over field values (cached)
    {
      auto column = reader->column(id);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      if (payload) {
        // if attribute is present, payload value has
        // to be always empty for mask column
        ASSERT_EQ(irs::doc_limits::invalid(), it->value());
        ASSERT_TRUE(irs::IsNull(payload->value));
      }

      std::vector<irs::doc_id_t> expected_values = {2, 4, 8, 9};

      size_t i = 0;
      for (; it->next(); ++i) {
        ASSERT_EQ(expected_values[i], it->value());
        if (payload) {
          // if attribute is present, payload value has
          // to be always empty for mask column
          ASSERT_TRUE(irs::IsNull(payload->value));
        }
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(i, expected_values.size());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      if (payload) {
        // if attribute is present, payload value has
        // to be always empty for mask column
        ASSERT_TRUE(irs::IsNull(payload->value));
      }
    }
  }

  // check previously written mask
  // - iterate (not cached)
  // - random read (cached)
  // - iterate (cached)
  {
    auto reader = codec()->get_columnstore_reader();
    ASSERT_TRUE(reader->prepare(dir(), segment));

    {
      // iterate over field values (not cached)
      auto column = reader->column(id);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      if (payload) {
        // if attribute is present, payload value has
        // to be always empty for mask column
        ASSERT_EQ(irs::doc_limits::invalid(), it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);
      }

      std::vector<irs::doc_id_t> expected_values = {2, 4, 8, 9};

      size_t i = 0;
      for (; it->next(); ++i) {
        ASSERT_EQ(expected_values[i], it->value());
        if (payload) {
          // if attribute is present, payload value has
          // to be always empty for mask column
          ASSERT_TRUE(irs::IsNull(payload->value));
        }
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(i, expected_values.size());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      if (payload) {
        // if attribute is present, payload value has
        // to be always empty for mask column
        ASSERT_TRUE(irs::IsNull(payload->value));
      }
    }

    // read field values (potentially cached)
    {
      auto column = reader->column(id);
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_TRUE(!actual_value || irs::IsNull(actual_value->value));
      ASSERT_EQ(2, values->seek(1));
      ASSERT_TRUE(!actual_value || irs::IsNull(actual_value->value));
      ASSERT_EQ(2, values->seek(2));
      ASSERT_TRUE(!actual_value || irs::IsNull(actual_value->value));
      ASSERT_EQ(4, values->seek(4));
      ASSERT_TRUE(!actual_value || irs::IsNull(actual_value->value));
      ASSERT_EQ(8, values->seek(6));
      ASSERT_TRUE(!actual_value || irs::IsNull(actual_value->value));
      ASSERT_EQ(8, values->seek(8));
      ASSERT_TRUE(!actual_value || irs::IsNull(actual_value->value));
      ASSERT_EQ(9, values->seek(9));
      ASSERT_TRUE(!actual_value || irs::IsNull(actual_value->value));
      ASSERT_TRUE(irs::doc_limits::eof(values->seek(irs::doc_limits::eof())));
      ASSERT_TRUE(!actual_value || irs::IsNull(actual_value->value));
    }

    // iterate over field values (cached)
    {
      auto column = reader->column(id);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      if (payload) {
        // if attribute is present, payload value has
        // to be always empty for mask column
        ASSERT_EQ(irs::doc_limits::invalid(), it->value());
        ASSERT_EQ(irs::bytes_view{}, payload->value);
      }

      std::vector<irs::doc_id_t> expected_values = {2, 4, 8, 9};

      size_t i = 0;
      for (; it->next(); ++i) {
        ASSERT_EQ(expected_values[i], it->value());
        if (payload) {
          // if attribute is present, payload value has
          // to be always empty for mask column
          ASSERT_EQ(irs::bytes_view{}, payload->value);
        }
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(i, expected_values.size());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      if (payload) {
        // if attribute is present, payload value has
        // to be always empty for mask column
        ASSERT_EQ(irs::bytes_view{}, payload->value);
      }
    }
  }
}

TEST_P(FormatTestCase, columns_rw_empty) {
  irs::SegmentMeta meta0;
  meta0.name = "_1";
  meta0.version = 42;
  meta0.docs_count = 89;
  meta0.live_docs_count = 67;
  meta0.codec = codec();

  std::vector<std::string> files;
  auto list_files = [&files](std::string_view name) {
    files.emplace_back(std::move(name));
    return true;
  };
  ASSERT_TRUE(dir().visit(list_files));
  ASSERT_TRUE(files.empty());

  irs::field_id column0_id;
  irs::field_id column1_id;

  // add columns
  {
    auto writer =
      codec()->get_columnstore_writer(false, irs::IResourceManager::gNoop);
    writer->prepare(dir(), meta0);

    column0_id = writer->push_column(lz4_column_info(), ColumnFinalizer(42)).id;
    ASSERT_EQ(0, column0_id);
    column1_id = writer->push_column(lz4_column_info(), ColumnFinalizer(43)).id;
    ASSERT_EQ(1, column1_id);

    irs::FlushState state{
      .dir = &dir(),
      .name = meta0.name,
      .doc_count = meta0.docs_count,
    };

    ASSERT_FALSE(writer->commit(state));  // flush empty columns
  }

  files.clear();
  ASSERT_TRUE(dir().visit(list_files));
  ASSERT_TRUE(files.empty());  // must be empty after flush

  {
    auto reader = codec()->get_columnstore_reader();
    ASSERT_FALSE(reader->prepare(dir(), meta0));  // no columns found

    // check empty column 0
    ASSERT_EQ(nullptr, reader->column(column0_id));
    // check empty column 1
    ASSERT_EQ(nullptr, reader->column(column1_id));
  }
}

TEST_P(FormatTestCase, columns_rw_same_col_empty_repeat) {
  struct CsvDocTemplate : public CsvDocGenerator::DocTemplate {
    void init() final {
      clear();
      reserve(3);
      insert(std::make_shared<tests::StringField>("id"));
      insert(std::make_shared<tests::StringField>("name"));
    }

    void value(size_t idx, const std::string_view& /*value*/) final {
      auto& field = indexed.get<tests::StringField>(idx);

      // amount of data written per doc_id is < sizeof(doc_id)
      field.value(std::string_view("x", idx));  // length 0 or 1
    }
    void end() final {}
    void reset() final {}
  } doc_template;  // two_columns_doc_template

  tests::CsvDocGenerator gen{resource("simple_two_column.csv"), doc_template};
  irs::SegmentMeta seg;
  seg.name = "_1";

  seg.codec = codec();

  absl::node_hash_map<std::string, irs::ColumnstoreWriter::ColumnT> columns;

  // write documents
  {
    auto writer =
      codec()->get_columnstore_writer(false, irs::IResourceManager::gNoop);
    irs::doc_id_t id = 0;
    writer->prepare(dir(), seg);

    for (const Document* doc; seg.docs_count < 30000 && (doc = gen.next());) {
      ++id;

      for (auto& field : doc->stored) {
        auto name = field.Name();
        auto it = columns.lazy_emplace(name, [&](const auto& ctor) {
          ctor(name,
               writer->push_column(lz4_column_info(), ColumnFinalizer(42)));
        });
        auto& column = it->second.out;
        auto& stream = column(id);

        field.Write(stream);

        // repeat requesting the same column without writing anything
        for (size_t i = 10; i; --i) {
          column(id);
        }
      }

      ++seg.docs_count;
    }

    irs::FlushState state{
      .dir = &dir(),
      .name = seg.name,
      .doc_count = seg.docs_count,
    };

    ASSERT_TRUE(writer->commit(state));

    gen.reset();
  }

  // read documents
  {
    auto reader_1 = codec()->get_columnstore_reader();
    ASSERT_TRUE(reader_1->prepare(dir(), seg));

    auto id_column = reader_1->column(columns.at("id").id);
    ASSERT_NE(nullptr, id_column);

    auto id_values = id_column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, id_values);
    auto* id_payload = irs::get<irs::PayAttr>(*id_values);
    ASSERT_NE(nullptr, id_payload);

    auto name_column = reader_1->column(columns.at("name").id);
    ASSERT_NE(nullptr, name_column);

    auto name_values = name_column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, name_values);
    auto* name_payload = irs::get<irs::PayAttr>(*name_values);
    ASSERT_NE(nullptr, name_payload);

    gen.reset();
    irs::doc_id_t i = 0;
    for (const Document* doc; i < seg.docs_count && (doc = gen.next());) {
      ++i;
      ASSERT_EQ(i, id_values->seek(i));
      ASSERT_EQ(doc->stored.get<tests::StringField>(0).value(),
                irs::ToString<std::string_view>(id_payload->value.data()));
      ASSERT_EQ(i, name_values->seek(i));
      ASSERT_EQ(doc->stored.get<tests::StringField>(1).value(),
                irs::ToString<std::string_view>(name_payload->value.data()));
    }
  }
}

TEST_P(FormatTestCase, columns_rw_big_document) {
  struct BigStoredField {
    bool Write(irs::DataOutput& out) const {
      out.WriteBytes(reinterpret_cast<const irs::byte_type*>(buf), sizeof buf);
      return true;
    }

    char buf[65536];
  } field;

  std::fstream stream(resource("simple_two_column.csv").string());
  ASSERT_FALSE(!stream);

  irs::field_id id;

  irs::SegmentMeta segment;
  segment.name = "big_docs";

  segment.codec = codec();

  // write big document
  {
    auto writer =
      codec()->get_columnstore_writer(false, irs::IResourceManager::gNoop);
    writer->prepare(dir(), segment);

    auto column = writer->push_column(lz4_column_info(), ColumnFinalizer(42));
    id = column.id;

    {
      auto& out = column.out(1);
      stream.read(field.buf, sizeof field.buf);
      ASSERT_FALSE(!stream);  // ensure that all requested data has been read
      ASSERT_TRUE(field.Write(out));  // must be written
      ++segment.docs_count;
    }

    {
      auto& out = column.out(2);
      stream.read(field.buf, sizeof field.buf);
      ASSERT_FALSE(!stream);  // ensure that all requested data has been read
      ASSERT_TRUE(field.Write(out));  // must be written
      ++segment.docs_count;
    }

    irs::FlushState state{
      .dir = &dir(),
      .name = segment.name,
      .doc_count = segment.docs_count,
    };

    ASSERT_TRUE(writer->commit(state));
  }

  // read big document
  {
    auto reader = codec()->get_columnstore_reader();
    ASSERT_TRUE(reader->prepare(dir(), segment));

    // random access
    {
      stream.clear();               // clear eof flag if set
      stream.seekg(0, stream.beg);  // seek to the beginning of the file

      auto column = reader->column(id);
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);

      std::memset(field.buf, 0, sizeof field.buf);  // clear buffer
      stream.read(field.buf, sizeof field.buf);
      ASSERT_TRUE(bool(stream));
      ASSERT_EQ(1, values->seek(1));
      ASSERT_EQ(std::string_view(field.buf, sizeof field.buf),
                irs::ViewCast<char>(actual_value->value));

      std::memset(field.buf, 0, sizeof field.buf);  // clear buffer
      stream.read(field.buf, sizeof field.buf);
      ASSERT_TRUE(bool(stream));
      ASSERT_EQ(2, values->seek(2));
      ASSERT_EQ(std::string_view(field.buf, sizeof field.buf),
                irs::ViewCast<char>(actual_value->value));
    }

    // iterator "next"
    {
      stream.clear();               // clear eof flag if set
      stream.seekg(0, stream.beg);  // seek to the beginning of the file

      auto column = reader->column(id);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_TRUE(it->next());
      std::memset(field.buf, 0, sizeof field.buf);  // clear buffer
      stream.read(field.buf, sizeof field.buf);
      ASSERT_TRUE(bool(stream));
      ASSERT_EQ(1, it->value());
      ASSERT_EQ(std::string_view(field.buf, sizeof field.buf),
                irs::ViewCast<char>(payload->value));

      ASSERT_TRUE(it->next());
      std::memset(field.buf, 0, sizeof field.buf);  // clear buffer
      stream.read(field.buf, sizeof field.buf);
      ASSERT_TRUE(bool(stream));
      ASSERT_EQ(2, it->value());
      ASSERT_EQ(std::string_view(field.buf, sizeof field.buf),
                irs::ViewCast<char>(payload->value));

      ASSERT_FALSE(it->next());
    }

    // iterator "seek"
    {
      stream.clear();               // clear eof flag if set
      stream.seekg(0, stream.beg);  // seek to the beginning of the file

      auto column = reader->column(id);
      ASSERT_NE(nullptr, column);
      auto it = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_EQ(1, it->seek(1));
      std::memset(field.buf, 0, sizeof field.buf);  // clear buffer
      stream.read(field.buf, sizeof field.buf);
      ASSERT_TRUE(bool(stream));
      ASSERT_EQ(std::string_view(field.buf, sizeof field.buf),
                irs::ViewCast<char>(payload->value));

      ASSERT_EQ(2, it->seek(2));
      std::memset(field.buf, 0, sizeof field.buf);  // clear buffer
      stream.read(field.buf, sizeof field.buf);
      ASSERT_TRUE(bool(stream));
      ASSERT_EQ(std::string_view(field.buf, sizeof field.buf),
                irs::ViewCast<char>(payload->value));

      ASSERT_FALSE(it->next());
    }
  }
}

TEST_P(FormatTestCase, columns_rw_writer_reuse) {
  struct CsvDocTemplate : public CsvDocGenerator::DocTemplate {
    void init() final {
      clear();
      reserve(2);
      insert(std::make_shared<tests::StringField>("id"));
      insert(std::make_shared<tests::StringField>("name"));
    }

    void value(size_t idx, const std::string_view& value) final {
      auto& field = indexed.get<tests::StringField>(idx);
      field.value(value);
    }
    void end() final {}
    void reset() final {}
  } doc_template;  // two_columns_doc_template

  tests::CsvDocGenerator gen(resource("simple_two_column.csv"), doc_template);

  irs::SegmentMeta seg_1;
  seg_1.name = "_1";
  irs::SegmentMeta seg_2;
  seg_2.name = "_2";
  irs::SegmentMeta seg_3;
  seg_3.name = "_3";

  seg_1.codec = codec();
  seg_2.codec = codec();
  seg_3.codec = codec();

  absl::node_hash_map<std::string, irs::ColumnstoreWriter::ColumnT> columns_1;
  absl::node_hash_map<std::string, irs::ColumnstoreWriter::ColumnT> columns_2;
  absl::node_hash_map<std::string, irs::ColumnstoreWriter::ColumnT> columns_3;

  // write documents
  {
    auto writer =
      codec()->get_columnstore_writer(false, irs::IResourceManager::gNoop);

    // write 1st segment
    irs::doc_id_t id = 0;
    writer->prepare(dir(), seg_1);

    for (const Document* doc; seg_1.docs_count < 30000 && (doc = gen.next());) {
      ++id;
      for (auto& field : doc->stored) {
        auto name = field.Name();
        auto it = columns_1.lazy_emplace(name, [&](const auto& ctor) {
          ctor(name,
               writer->push_column(lz4_column_info(), ColumnFinalizer(42)));
        });
        auto& column = it->second.out;
        auto& stream = column(id);

        field.Write(stream);
      }
      ++seg_1.docs_count;
    }

    {
      irs::FlushState state{
        .dir = &dir(),
        .name = seg_1.name,
        .doc_count = seg_1.docs_count,
      };

      ASSERT_TRUE(writer->commit(state));
    }

    gen.reset();

    // write 2nd segment
    id = 0;
    writer->prepare(dir(), seg_2);

    for (const Document* doc; seg_2.docs_count < 30000 && (doc = gen.next());) {
      ++id;
      for (auto& field : doc->stored) {
        auto name = field.Name();
        auto it = columns_2.lazy_emplace(name, [&](const auto& ctor) {
          ctor(name,
               writer->push_column(lz4_column_info(), ColumnFinalizer(42)));
        });
        auto& column = it->second.out;
        auto& stream = column(id);

        field.Write(stream);
      }
      ++seg_2.docs_count;
    }

    {
      irs::FlushState state{
        .dir = &dir(),
        .name = seg_2.name,
        .doc_count = seg_2.docs_count,
      };

      ASSERT_TRUE(writer->commit(state));
    }

    // write 3rd segment
    id = 0;
    writer->prepare(dir(), seg_3);

    for (const Document* doc; seg_3.docs_count < 70000 && (doc = gen.next());) {
      ++id;
      for (auto& field : doc->stored) {
        auto name = field.Name();
        auto it = columns_3.lazy_emplace(name, [&](const auto& ctor) {
          ctor(name,
               writer->push_column(lz4_column_info(), ColumnFinalizer(42)));
        });
        auto& column = it->second.out;
        auto& stream = column(id);

        field.Write(stream);
      }
      ++seg_3.docs_count;
    }

    {
      irs::FlushState state{
        .dir = &dir(),
        .name = seg_3.name,
        .doc_count = seg_3.docs_count,
      };

      ASSERT_TRUE(writer->commit(state));
    }
  }

  // read documents
  {
    // check 1st segment
    {
      auto reader_1 = codec()->get_columnstore_reader();
      ASSERT_TRUE(reader_1->prepare(dir(), seg_1));

      auto id_column = reader_1->column(columns_1.at("id").id);
      ASSERT_NE(nullptr, id_column);
      auto id_values = id_column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, id_values);
      auto* id_payload = irs::get<irs::PayAttr>(*id_values);
      ASSERT_NE(nullptr, id_payload);

      auto name_column = reader_1->column(columns_1.at("name").id);
      ASSERT_NE(nullptr, name_column);
      auto name_values = name_column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, name_values);
      auto* name_payload = irs::get<irs::PayAttr>(*name_values);
      ASSERT_NE(nullptr, name_payload);

      gen.reset();
      irs::doc_id_t i = 0;
      for (const Document* doc; i < seg_1.docs_count && (doc = gen.next());) {
        ++i;
        ASSERT_EQ(i, id_values->seek(i));
        ASSERT_EQ(doc->stored.get<tests::StringField>(0).value(),
                  irs::ToString<std::string_view>(id_payload->value.data()));
        ASSERT_EQ(i, name_values->seek(i));
        ASSERT_EQ(doc->stored.get<tests::StringField>(1).value(),
                  irs::ToString<std::string_view>(name_payload->value.data()));
      }

      // check 2nd segment (same as 1st)
      auto reader_2 = codec()->get_columnstore_reader();
      ASSERT_TRUE(reader_2->prepare(dir(), seg_2));

      auto id_column_2 = reader_2->column(columns_2.at("id").id);
      ASSERT_NE(nullptr, id_column_2);
      auto id_values_2 = id_column_2->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, id_values_2);
      auto* id_payload_2 = irs::get<irs::PayAttr>(*id_values_2);
      ASSERT_NE(nullptr, id_payload_2);

      auto name_column_2 = reader_2->column(columns_2.at("name").id);
      ASSERT_NE(nullptr, name_column_2);
      auto name_values_2 = name_column_2->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, name_values_2);
      auto* name_payload_2 = irs::get<irs::PayAttr>(*name_values_2);
      ASSERT_NE(nullptr, name_payload_2);

      // check for equality
      gen.reset();

      const Document* doc;
      for (irs::doc_id_t i = 0, count = seg_2.docs_count;
           i < count && (doc = gen.next());) {
        ++i;
        ASSERT_EQ(i, id_values_2->seek(i));
        ASSERT_EQ(doc->stored.get<tests::StringField>(0).value(),
                  irs::ToString<std::string_view>(id_payload_2->value.data()));
        ASSERT_EQ(i, name_values_2->seek(i));
        ASSERT_EQ(
          doc->stored.get<tests::StringField>(1).value(),
          irs::ToString<std::string_view>(name_payload_2->value.data()));
      }
    }

    // check 3rd segment
    {
      auto reader = codec()->get_columnstore_reader();
      ASSERT_TRUE(reader->prepare(dir(), seg_3));

      auto id_column = reader->column(columns_3.at("id").id);
      ASSERT_NE(nullptr, id_column);
      auto id_values = id_column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, id_values);
      auto* id_payload = irs::get<irs::PayAttr>(*id_values);
      ASSERT_NE(nullptr, id_payload);

      auto name_column = reader->column(columns_3.at("name").id);
      ASSERT_NE(nullptr, name_column);
      auto name_values = name_column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, name_values);
      auto* name_payload = irs::get<irs::PayAttr>(*name_values);
      ASSERT_NE(nullptr, name_payload);

      irs::doc_id_t i = 0;
      for (const Document* doc; i < seg_3.docs_count && (doc = gen.next());) {
        ++i;
        ASSERT_EQ(i, id_values->seek(i));
        ASSERT_EQ(doc->stored.get<tests::StringField>(0).value(),
                  irs::ToString<std::string_view>(id_payload->value.data()));
        ASSERT_EQ(i, name_values->seek(i));
        ASSERT_EQ(doc->stored.get<tests::StringField>(1).value(),
                  irs::ToString<std::string_view>(name_payload->value.data()));
      }
    }
  }
}

TEST_P(FormatTestCase, columns_rw_typed) {
  struct Value {
    enum class Type { String, Binary, Double };

    Value(const std::string_view& name, const std::string_view& value)
      : name(name), value(value), type(Type::String) {}

    Value(const std::string_view& name, const irs::bytes_view& value)
      : name(name), value(value), type(Type::Binary) {}

    Value(const std::string_view& name, double_t value)
      : name(name), value(value), type(Type::Double) {}

    std::string_view name;
    struct Rep {
      Rep(const std::string_view& value) : s_value(value) {}
      Rep(const irs::bytes_view& value) : bin_value(value) {}
      Rep(double_t value) : dbl_value(value) {}
      ~Rep() {}

      std::string_view s_value;
      irs::bytes_view bin_value;
      double_t dbl_value;
    } value;
    Type type;
  };

  std::deque<Value> values;
  tests::JsonDocGenerator gen(
    resource("simple_sequential_33.json"),
    [&values](tests::Document& doc, const std::string& name,
              const tests::JsonDocGenerator::JsonValue& data) {
      if (data.is_string()) {
        doc.insert(std::make_shared<StringField>(name, data.str));

        auto& field = (doc.indexed.end() - 1).as<StringField>();
        values.emplace_back(field.Name(), field.value());
      } else if (data.is_null()) {
        doc.insert(std::make_shared<tests::BinaryField>());
        auto& field = (doc.indexed.end() - 1).as<tests::BinaryField>();
        field.Name(name);
        field.value(
          irs::ViewCast<irs::byte_type>(irs::NullTokenizer::value_null()));
        values.emplace_back(field.Name(), field.value());
      } else if (data.is_bool() && data.b) {
        doc.insert(std::make_shared<tests::BinaryField>());
        auto& field = (doc.indexed.end() - 1).as<tests::BinaryField>();
        field.Name(name);
        field.value(
          irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value_true()));
        values.emplace_back(field.Name(), field.value());
      } else if (data.is_bool() && !data.b) {
        doc.insert(std::make_shared<tests::BinaryField>());
        auto& field = (doc.indexed.end() - 1).as<tests::BinaryField>();
        field.Name(name);
        field.value(
          irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value_true()));
        values.emplace_back(field.Name(), field.value());
      } else if (data.is_number()) {
        const double d_value = data.as_number<double_t>();

        // 'value' can be interpreted as a double
        doc.insert(std::make_shared<tests::DoubleField>());
        auto& field = (doc.indexed.end() - 1).as<tests::DoubleField>();
        field.Name(name);
        field.value(d_value);
        values.emplace_back(field.Name(), field.value());
      }
    });

  irs::SegmentMeta meta;
  meta.name = "_1";
  meta.version = 42;
  meta.codec = codec();

  absl::node_hash_map<std::string, irs::ColumnstoreWriter::ColumnT> columns;

  // write stored documents
  {
    auto writer =
      codec()->get_columnstore_writer(false, irs::IResourceManager::gNoop);
    writer->prepare(dir(), meta);

    irs::doc_id_t id = 0;

    for (const Document* doc; (doc = gen.next());) {
      ++id;

      for (const auto& field : doc->stored) {
        auto name = field.Name();
        auto it = columns.lazy_emplace(name, [&](const auto& ctor) {
          ctor(name,
               writer->push_column(lz4_column_info(), ColumnFinalizer(42)));
        });
        auto& column = it->second.out;
        auto& stream = column(id);

        ASSERT_TRUE(field.Write(stream));
      }
      ++meta.docs_count;
    }

    irs::FlushState state{
      .dir = &dir(),
      .name = meta.name,
      .doc_count = meta.docs_count,
    };

    ASSERT_TRUE(writer->commit(state));
  }

  // read stored documents
  {
    gen.reset();

    auto reader = codec()->get_columnstore_reader();
    ASSERT_TRUE(reader->prepare(dir(), meta));

    std::unordered_map<std::string, irs::DocIterator::ptr> readers;

    irs::BytesViewInput in;
    irs::doc_id_t i = 0;
    size_t value_id = 0;
    for (const Document* doc; (doc = gen.next());) {
      ++i;

      for (size_t size = doc->stored.size(); size; --size) {
        auto& expected_field = values[value_id++];
        const std::string name(expected_field.name);
        const auto res =
          readers.emplace(std::piecewise_construct, std::forward_as_tuple(name),
                          std::forward_as_tuple());

        if (res.second) {
          auto column = reader->column(columns.at(name).id);
          ASSERT_NE(nullptr, column);
          res.first->second = column->iterator(irs::ColumnHint::Normal);
        }

        auto& column_iterator = *res.first->second;
        auto* actual_value = irs::get<irs::PayAttr>(column_iterator);
        ASSERT_NE(nullptr, actual_value);
        ASSERT_EQ(i, column_iterator.seek(i));
        in.reset(actual_value->value);

        switch (expected_field.type) {
          case Value::Type::String: {
            ASSERT_EQ(expected_field.value.s_value,
                      irs::ReadString<std::string>(in));
          } break;
          case Value::Type::Binary: {
            ASSERT_EQ(expected_field.value.bin_value,
                      irs::ReadString<irs::bstring>(in));
          } break;
          case Value::Type::Double: {
            ASSERT_EQ(expected_field.value.dbl_value, tests::ReadZvdouble(in));
          } break;
          default:
            ASSERT_TRUE(false);
            break;
        };
      }
    }
    ASSERT_EQ(meta.docs_count, i);
  }

  // iterate over stored columns
  {
    gen.reset();

    auto reader = codec()->get_columnstore_reader();
    ASSERT_TRUE(reader->prepare(dir(), meta));

    std::unordered_map<std::string, irs::DocIterator::ptr> readers;

    irs::BytesViewInput in;
    irs::doc_id_t i = 0;
    size_t value_id = 0;
    for (const Document* doc; (doc = gen.next());) {
      ++i;

      for (size_t size = doc->stored.size(); size; --size) {
        auto& expected_field = values[value_id++];
        const std::string name(expected_field.name);
        const auto res =
          readers.emplace(std::piecewise_construct, std::forward_as_tuple(name),
                          std::forward_as_tuple());

        if (res.second) {
          auto column = reader->column(columns.at(name).id);
          ASSERT_NE(nullptr, column);

          auto& it = res.first->second;
          it = column->iterator(irs::ColumnHint::Normal);

          auto* payload = irs::get<irs::PayAttr>(*it);
          ASSERT_FALSE(!payload);
          ASSERT_EQ(irs::doc_limits::invalid(), it->value());
          ASSERT_EQ(irs::bytes_view{}, payload->value);
        }

        auto& it = res.first->second;
        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_FALSE(!payload);

        it->next();
        ASSERT_EQ(i, it->value());
        in.reset(payload->value);

        switch (expected_field.type) {
          case Value::Type::String: {
            ASSERT_EQ(expected_field.value.s_value,
                      irs::ReadString<std::string>(in));
          } break;
          case Value::Type::Binary: {
            ASSERT_EQ(expected_field.value.bin_value,
                      irs::ReadString<irs::bstring>(in));
          } break;
          case Value::Type::Double: {
            ASSERT_EQ(expected_field.value.dbl_value, tests::ReadZvdouble(in));
          } break;
          default:
            ASSERT_TRUE(false);
            break;
        };
      }
    }
    ASSERT_EQ(meta.docs_count, i);

    // ensure that all iterators are in the end
    for (auto& entry : readers) {
      auto& it = entry.second;
      ASSERT_FALSE(it->next());
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }
  }

  // seek over stored columns
  {
    gen.reset();

    auto reader = codec()->get_columnstore_reader();
    ASSERT_TRUE(reader->prepare(dir(), meta));

    std::unordered_map<std::string, irs::DocIterator::ptr> readers;

    irs::BytesViewInput in;
    irs::doc_id_t i = 0;
    size_t value_id = 0;
    for (const Document* doc; (doc = gen.next());) {
      ++i;

      for (size_t size = doc->stored.size(); size; --size) {
        auto& expected_field = values[value_id++];
        const std::string name(expected_field.name);
        const auto res =
          readers.emplace(std::piecewise_construct, std::forward_as_tuple(name),
                          std::forward_as_tuple());

        if (res.second) {
          auto column = reader->column(columns.at(name).id);
          ASSERT_NE(nullptr, column);

          auto& it = res.first->second;
          it = column->iterator(irs::ColumnHint::Normal);
          auto* payload = irs::get<irs::PayAttr>(*it);
          ASSERT_FALSE(!payload);
          ASSERT_EQ(irs::doc_limits::invalid(), it->value());
          ASSERT_EQ(irs::bytes_view{}, payload->value);
        }

        auto& it = res.first->second;
        auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_FALSE(!payload);

        ASSERT_EQ(i, it->seek(i));

        in.reset(payload->value);

        switch (expected_field.type) {
          case Value::Type::String: {
            ASSERT_EQ(expected_field.value.s_value,
                      irs::ReadString<std::string>(in));
          } break;
          case Value::Type::Binary: {
            ASSERT_EQ(expected_field.value.bin_value,
                      irs::ReadString<irs::bstring>(in));
          } break;
          case Value::Type::Double: {
            ASSERT_EQ(expected_field.value.dbl_value, tests::ReadZvdouble(in));
          } break;
          default:
            ASSERT_TRUE(false);
            break;
        };
      }
    }
    ASSERT_EQ(meta.docs_count, i);

    // ensure that all iterators are in the end
    for (auto& entry : readers) {
      auto& it = entry.second;
      ASSERT_FALSE(it->next());
      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }
  }
}

TEST_P(FormatTestCase, columns_issue700) {
  std::vector<std::pair<irs::doc_id_t, size_t>> docs;
  irs::doc_id_t doc = irs::doc_limits::min();
  for (; doc < 1265; ++doc) {
    docs.emplace_back(doc, 26);
  }
  for (; doc < 17761; ++doc) {
    docs.emplace_back(doc, 25);
  }

  irs::SegmentMeta meta;
  meta.name = "issue-#700";
  meta.version = 0;
  meta.docs_count = docs.size();
  meta.live_docs_count = docs.size();
  meta.codec = codec();

  {
    auto writer =
      codec()->get_columnstore_writer(false, irs::IResourceManager::gNoop);
    ASSERT_NE(nullptr, writer);
    writer->prepare(dir(), meta);

    auto dense_fixed_offset_column =
      writer->push_column(none_column_info(), ColumnFinalizer(42));

    ASSERT_EQ(0, dense_fixed_offset_column.id);

    std::string str;
    for (auto& doc : docs) {
      auto& stream = dense_fixed_offset_column.out(doc.first);
      str.resize(doc.second, 'c');
      stream.WriteBytes(reinterpret_cast<const irs::byte_type*>(str.data()),
                        str.size());
    }

    irs::FlushState state{
      .dir = &dir(),
      .name = meta.name,
      .doc_count = meta.docs_count,
    };

    ASSERT_TRUE(writer->commit(state));
  }

  {
    auto reader = codec()->get_columnstore_reader();
    ASSERT_NE(nullptr, reader);
    ASSERT_TRUE(reader->prepare(dir(), meta));
    ASSERT_EQ(1, reader->size());
    auto* column = reader->column(0);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(docs.size(), column->size());
  }
}

TEST_P(FormatTestCase, columns_rw_sparse_dense_offset_column_border_case) {
  // border case for dense/sparse fixed offset columns, e.g.
  // |-----|------------|  |-----|------------|
  // |doc  | value_size |  |doc  | value_size |
  // |-----|------------|  |-----|------------|
  // | 1   | 0          |  | 1   | 0          |
  // | 2   | 16         |  | 4   | 16         |
  // |-----|------------|  |-----|------------|

  irs::SegmentMeta meta0;
  meta0.name = "_fixed_offset_columns";
  meta0.version = 0;
  meta0.docs_count = 2;
  meta0.live_docs_count = 2;
  meta0.codec = codec();

  const uint64_t keys[] = {42, 42};
  const irs::bytes_view keys_ref(reinterpret_cast<const irs::byte_type*>(&keys),
                                 sizeof keys);

  // write columns values
  auto writer =
    codec()->get_columnstore_writer(false, irs::IResourceManager::gNoop);
  writer->prepare(dir(), meta0);

  auto dense_fixed_offset_column = writer->push_column(lz4_column_info(), {});
  auto sparse_fixed_offset_column =
    writer->push_column(lz4_column_info(), ColumnFinalizer(42));

  {
    irs::doc_id_t doc = irs::doc_limits::min();

    // write first document
    {
      dense_fixed_offset_column.out(doc);
      sparse_fixed_offset_column.out(doc);
    }

    // write second document
    {
      {
        auto& stream = dense_fixed_offset_column.out(doc + 1);

        stream.WriteBytes(reinterpret_cast<const irs::byte_type*>(&keys),
                          sizeof keys);
      }

      {
        auto& stream = sparse_fixed_offset_column.out(doc + 3);

        stream.WriteBytes(reinterpret_cast<const irs::byte_type*>(&keys),
                          sizeof keys);
      }
    }

    irs::FlushState state{
      .dir = &dir(),
      .name = meta0.name,
      .doc_count = meta0.docs_count,
    };

    ASSERT_TRUE(writer->commit(state));
    writer.reset();
  }

  // dense fixed offset column
  {
    auto reader = codec()->get_columnstore_reader();
    ASSERT_TRUE(reader->prepare(dir(), meta0));

    auto column = reader->column(dense_fixed_offset_column.id);
    ASSERT_NE(nullptr, column);

    std::vector<std::pair<irs::doc_id_t, irs::bytes_view>> expected_values{
      {irs::doc_limits::min(), irs::bytes_view{}},
      {irs::doc_limits::min() + 1, keys_ref},
    };

    // check iterator
    {
      auto it = column->iterator(irs::ColumnHint::Normal);
      auto* payload = irs::get<irs::PayAttr>(*it);

      for (auto& expected_value : expected_values) {
        ASSERT_TRUE(it->next());
        ASSERT_EQ(expected_value.first, it->value());
        ASSERT_EQ(expected_value.second, payload->value);
      }

      ASSERT_FALSE(it->next());
    }

    // check visit
    {
      auto expected_value = expected_values.begin();

      const auto res =
        Visit(*column, [&expected_value](irs::doc_id_t actual_doc,
                                         const irs::bytes_view& actual_value) {
          if (expected_value->first != actual_doc) {
            return false;
          }

          if (expected_value->second != actual_value) {
            return false;
          }

          ++expected_value;

          return true;
        });

      ASSERT_TRUE(res);

      ASSERT_EQ(expected_values.end(), expected_value);
    }
  }

  // sparse fixed offset column
  {
    auto reader = codec()->get_columnstore_reader();
    ASSERT_TRUE(reader->prepare(dir(), meta0));

    auto column = reader->column(sparse_fixed_offset_column.id);
    ASSERT_NE(nullptr, column);

    std::vector<std::pair<irs::doc_id_t, irs::bytes_view>> expected_values{
      {irs::doc_limits::min(), irs::bytes_view{}},
      {irs::doc_limits::min() + 3, keys_ref},
    };

    // check iterator
    {
      auto it = column->iterator(irs::ColumnHint::Normal);
      auto* payload = irs::get<irs::PayAttr>(*it);

      for (auto& expected_value : expected_values) {
        ASSERT_TRUE(it->next());
        ASSERT_EQ(expected_value.first, it->value());
        ASSERT_EQ(expected_value.second, payload->value);
      }

      ASSERT_FALSE(it->next());
    }

    // check visit
    {
      auto expected_value = expected_values.begin();

      ASSERT_TRUE(
        Visit(*column, [&expected_value](irs::doc_id_t actual_doc,
                                         irs::bytes_view actual_value) {
          if (expected_value->first != actual_doc) {
            return false;
          }

          if (expected_value->second != actual_value) {
            return false;
          }

          ++expected_value;

          return true;
        }));

      ASSERT_EQ(expected_values.end(), expected_value);
    }
  }
}

TEST_P(FormatTestCase, columns_rw) {
  irs::field_id segment0_field0_id;
  irs::field_id segment0_field1_id;
  irs::field_id segment0_empty_column_id;
  irs::field_id segment0_field2_id;
  irs::field_id segment0_field3_id;
  irs::field_id segment0_field4_id;
  irs::field_id segment1_field0_id;
  irs::field_id segment1_field1_id;
  irs::field_id segment1_field2_id;

  irs::SegmentMeta meta0;
  meta0.name = "_1";
  meta0.version = 42;
  meta0.docs_count = 89;
  meta0.live_docs_count = 67;
  meta0.codec = codec();

  irs::SegmentMeta meta1;
  meta1.name = "_2";
  meta1.version = 23;
  meta1.docs_count = 115;
  meta1.live_docs_count = 111;
  meta1.codec = codec();

  // read attributes from empty directory
  {
    auto reader = codec()->get_columnstore_reader();
    ASSERT_FALSE(reader->prepare(dir(), meta1));  // no attributes found

    // try to get invalild column
    ASSERT_EQ(nullptr, reader->column(irs::field_limits::invalid()));
  }

  // write columns values
  auto writer =
    codec()->get_columnstore_writer(false, irs::IResourceManager::gNoop);

  // write _1 segment
  {
    writer->prepare(dir(), meta0);

    auto field0 = writer->push_column(lz4_column_info(), {});
    segment0_field0_id = field0.id;
    auto& field0_writer = field0.out;
    ASSERT_EQ(0, segment0_field0_id);
    auto field1 = writer->push_column(lz4_column_info(), {});
    segment0_field1_id = field1.id;
    auto& field1_writer = field1.out;
    ASSERT_EQ(1, segment0_field1_id);
    auto empty_field =
      writer->push_column(lz4_column_info(), {});  // gap between filled columns
    segment0_empty_column_id = empty_field.id;
    ASSERT_EQ(2, segment0_empty_column_id);
    auto field2 = writer->push_column(lz4_column_info(), {});
    segment0_field2_id = field2.id;
    auto& field2_writer = field2.out;
    ASSERT_EQ(3, segment0_field2_id);
    auto field3 = writer->push_column(lz4_column_info(), {});
    segment0_field3_id = field3.id;
    [[maybe_unused]] auto& field3_writer = field3.out;
    ASSERT_EQ(4, segment0_field3_id);
    auto field4 = writer->push_column(lz4_column_info(), {});
    segment0_field4_id = field4.id;
    auto& field4_writer = field4.out;
    ASSERT_EQ(5, segment0_field4_id);

    // column==field0
    {
      auto& stream = field0_writer(1);
      irs::WriteStr(stream, std::string_view("field0_doc0"));  // doc==1
    }

    // column==field4
    {
      auto& stream = field4_writer(1);  // doc==1
      irs::WriteStr(stream, std::string_view("field4_doc_min"));
    }

    // column==field1, multivalued attribute
    {
      auto& stream = field1_writer(1);  // doc==1
      irs::WriteStr(stream, std::string_view("field1_doc0"));
      irs::WriteStr(stream, std::string_view("field1_doc0_1"));
    }

    // column==field2
    {
      (void)1;
      // rollback
      {
        auto& stream = field2_writer(1);  // doc==1
        irs::WriteStr(stream, std::string_view("invalid_string"));
        stream.Reset();  // rollback changes
        stream.Reset();  // rollback changes
      }
      {
        auto& stream = field2_writer(1);  // doc==1
        irs::WriteStr(stream, std::string_view("field2_doc1"));
      }
    }

    // column==field0, rollback
    {
      auto& stream = field0_writer(2);  // doc==2
      irs::WriteStr(stream, std::string_view("field0_doc1"));
      stream.Reset();
    }

    // column==field0
    {
      auto& stream = field0_writer(2);  // doc==2
      irs::WriteStr(stream, std::string_view("field0_doc2"));
    }

    // column==field0
    {
      auto& stream = field0_writer(33);  // doc==33
      irs::WriteStr(stream, std::string_view("field0_doc33"));
    }

    // column==field1, multivalued attribute
    {
      // Get stream by the same key. In this case written values
      // will be concatenated and accessible via the specified key
      // (e.g. 'field1_doc12_1', 'field1_doc12_2' in this case)
      {
        auto& stream = field1_writer(12);  // doc==12
        irs::WriteStr(stream, std::string_view("field1_doc12_1"));
      }
      {
        auto& stream = field1_writer(12);  // doc==12
        irs::WriteStr(stream, std::string_view("field1_doc12_2"));
      }
    }

    irs::FlushState state{
      .dir = &dir(),
      .name = meta0.name,
      .doc_count = meta0.docs_count,
    };

    ASSERT_TRUE(writer->commit(state));
  }

  // write _2 segment, reuse writer
  {
    writer->prepare(dir(), meta1);

    auto field0 = writer->push_column(lz4_column_info(), {});
    segment1_field0_id = field0.id;
    auto& field0_writer = field0.out;
    ASSERT_EQ(0, segment1_field0_id);
    auto field1 = writer->push_column(lz4_column_info(), {});
    segment1_field1_id = field1.id;
    auto& field1_writer = field1.out;
    ASSERT_EQ(1, segment1_field1_id);
    auto field2 = writer->push_column(lz4_column_info(), {});
    segment1_field2_id = field2.id;
    auto& field2_writer = field2.out;
    ASSERT_EQ(2, segment1_field2_id);

    // column==field3
    {
      auto& stream = field2_writer(1);  // doc==1
      irs::WriteStr(stream, std::string_view("segment_2_field3_doc0"));
    }

    // column==field1, multivalued attribute
    {
      auto& stream = field0_writer(1);  // doc==1
      irs::WriteStr(stream, std::string_view("segment_2_field1_doc0"));
    }

    // column==field2, rollback
    {
      auto& stream = field1_writer(1);
      irs::WriteStr(stream, std::string_view("segment_2_field2_doc0"));
      stream.Reset();  // rollback
    }

    // column==field3, rollback
    {
      auto& stream = field2_writer(2);  // doc==2
      irs::WriteStr(stream, std::string_view("segment_2_field0_doc1"));
      stream.Reset();  // rollback
    }

    // colum==field1
    {
      auto& stream = field0_writer(12);  // doc==12
      irs::WriteStr(stream, std::string_view("segment_2_field1_doc12"));
    }

    // colum==field3
    {
      auto& stream = field2_writer(23);  // doc==23
      irs::WriteStr(stream, std::string_view("segment_2_field3_doc23"));
      stream.Reset();  // rollback
    }

    irs::FlushState state{
      .dir = &dir(),
      .name = meta1.name,
      .doc_count = meta1.docs_count,
    };

    ASSERT_TRUE(writer->commit(state));
  }

  // read columns values from segment _1
  {
    auto reader = codec()->get_columnstore_reader();
    ASSERT_TRUE(reader->prepare(dir(), meta0));

    // try to get invalild column
    ASSERT_EQ(nullptr, reader->column(irs::field_limits::invalid()));

    // check field4
    {
      auto column_reader = reader->column(segment0_field4_id);
      ASSERT_NE(nullptr, column_reader);
      auto column = column_reader->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, column);
      auto* actual_value = irs::get<irs::PayAttr>(*column);
      ASSERT_NE(nullptr, actual_value);

      ASSERT_EQ(1, column->seek(1));  // check doc==1, column==field4
      ASSERT_EQ("field4_doc_min",
                irs::ToString<std::string_view>(actual_value->value.data()));
    }

    // visit field0 values (not cached)
    {
      std::unordered_map<std::string_view, irs::doc_id_t> expected_values = {
        {"field0_doc0", 1}, {"field0_doc2", 2}, {"field0_doc33", 33}};

      auto visitor = [&expected_values](irs::doc_id_t doc,
                                        irs::bytes_view value) {
        const auto actual_value = irs::ToString<std::string_view>(value.data());

        auto it = expected_values.find(actual_value);
        if (it == expected_values.end()) {
          // can't find value
          return false;
        }

        if (it->second != doc) {
          // wrong document
          return false;
        }

        expected_values.erase(it);
        return true;
      };

      auto column = reader->column(segment0_field0_id);
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(Visit(*column, visitor));
      ASSERT_TRUE(expected_values.empty());
    }

    // partailly visit field0 values (not cached)
    {
      std::unordered_map<std::string_view, irs::doc_id_t> expected_values = {
        {"field0_doc0", 1}, {"field0_doc2", 2}, {"field0_doc33", 33}};

      size_t calls_count = 0;
      auto visitor = [&expected_values, &calls_count](irs::doc_id_t doc,
                                                      irs::bytes_view in) {
        ++calls_count;

        if (calls_count > 2) {
          // break the loop
          return false;
        }

        const auto actual_value = irs::ToString<std::string_view>(in.data());

        auto it = expected_values.find(actual_value);
        if (it == expected_values.end()) {
          // can't find value
          return false;
        }

        if (it->second != doc) {
          // wrong document
          return false;
        }

        expected_values.erase(it);
        return true;
      };

      auto column = reader->column(segment0_field0_id);
      ASSERT_NE(nullptr, column);
      ASSERT_FALSE(Visit(*column, visitor));
      ASSERT_FALSE(expected_values.empty());
      ASSERT_EQ(1, expected_values.size());
      ASSERT_NE(expected_values.end(), expected_values.find("field0_doc33"));
    }

    // check field0
    {
      auto column_reader = reader->column(segment0_field0_id);
      ASSERT_NE(nullptr, column_reader);

      // read (not cached)
      {
        auto column = column_reader->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, column);
        auto* actual_value = irs::get<irs::PayAttr>(*column);
        ASSERT_NE(nullptr, actual_value);

        ASSERT_EQ(1, column->seek(1));  // check doc==1, column==field0
        ASSERT_EQ("field0_doc0",
                  irs::ToString<std::string_view>(actual_value->value.data()));
        ASSERT_EQ(33, column->seek(5));   // doc without value in field0
        ASSERT_EQ(33, column->seek(33));  // check doc==33, column==field0
        ASSERT_EQ("field0_doc33",
                  irs::ToString<std::string_view>(actual_value->value.data()));
      }

      // read (cached)
      {
        auto column = column_reader->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, column);
        auto* actual_value = irs::get<irs::PayAttr>(*column);
        ASSERT_NE(nullptr, actual_value);

        ASSERT_EQ(1, column->seek(1));  // check doc==0, column==field0
        ASSERT_EQ("field0_doc0",
                  irs::ToString<std::string_view>(actual_value->value.data()));
        ASSERT_EQ(33, column->seek(5));   // doc without value in field0
        ASSERT_EQ(33, column->seek(33));  // check doc==33, column==field0
        ASSERT_EQ("field0_doc33",
                  irs::ToString<std::string_view>(actual_value->value.data()));
      }
    }

    // visit field0 values (cached)
    {
      std::unordered_map<std::string_view, irs::doc_id_t> expected_values = {
        {"field0_doc0", 1}, {"field0_doc2", 2}, {"field0_doc33", 33}};

      auto visitor = [&expected_values](irs::doc_id_t doc,
                                        const irs::bytes_view& in) {
        const auto actual_value = irs::ToString<std::string_view>(in.data());

        auto it = expected_values.find(actual_value);
        if (it == expected_values.end()) {
          // can't find value
          return false;
        }

        if (it->second != doc) {
          // wrong document
          return false;
        }

        expected_values.erase(it);
        return true;
      };

      auto column_reader = reader->column(segment0_field0_id);
      ASSERT_NE(nullptr, column_reader);
      ASSERT_TRUE(Visit(*column_reader, visitor));
      ASSERT_TRUE(expected_values.empty());
    }

    // iterate over field0 values (cached)
    {
      auto column_reader = reader->column(segment0_field0_id);
      ASSERT_NE(nullptr, column_reader);
      auto it = column_reader->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      std::vector<std::pair<std::string_view, irs::doc_id_t>> expected_values =
        {{"field0_doc0", 1}, {"field0_doc2", 2}, {"field0_doc33", 33}};

      size_t i = 0;
      for (; it->next(); ++i) {
        const auto& expected_value = expected_values[i];
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(expected_value.second, it->value());
        ASSERT_EQ(expected_value.first, actual_str_value);
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(i, expected_values.size());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // seek over field0 values (cached)
    {
      auto column_reader = reader->column(segment0_field0_id);
      ASSERT_NE(nullptr, column_reader);
      auto it = column_reader->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      std::vector<
        std::pair<std::string_view, std::pair<irs::doc_id_t, irs::doc_id_t>>>
        expected_values = {{"field0_doc0", {0, 1}},
                           {"field0_doc2", {2, 2}},
                           {"field0_doc33", {22, 33}},
                           {"field0_doc33", {33, 33}}};

      for (auto& expected : expected_values) {
        const auto expected_doc = expected.second.second;
        const auto expected_value = expected.first;

        ASSERT_EQ(expected_doc, it->seek(expected_doc));
        ASSERT_EQ(expected_value,
                  irs::ToString<std::string_view>(payload->value.data()));
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // iterate over field1 values (not cached)
    {
      auto column_reader = reader->column(segment0_field1_id);
      ASSERT_NE(nullptr, column_reader);
      auto it = column_reader->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      std::vector<std::pair<std::vector<std::string_view>, irs::doc_id_t>>
        expected_values = {{{"field1_doc0", "field1_doc0_1"}, 1},
                           {{"field1_doc12_1", "field1_doc12_2"}, 12}};

      size_t i = 0;
      for (; it->next(); ++i) {
        const auto& expected_value = expected_values[i];

        std::vector<std::string_view> actual_str_values;
        actual_str_values.push_back(
          irs::ToString<std::string_view>(payload->value.data()));
        actual_str_values.push_back(irs::ToString<std::string_view>(
          reinterpret_cast<const irs::byte_type*>(
            actual_str_values.back().data() +
            actual_str_values.back().size())));

        ASSERT_EQ(expected_value.second, it->value());
        ASSERT_EQ(expected_value.first, actual_str_values);
      }
      ASSERT_FALSE(it->next());
      ASSERT_EQ(i, expected_values.size());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // seek over field1 values (cached)
    {
      auto column_reader = reader->column(segment0_field1_id);
      ASSERT_NE(nullptr, column_reader);
      auto it = column_reader->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      std::vector<std::pair<std::vector<std::string_view>,
                            std::pair<irs::doc_id_t, irs::doc_id_t>>>
        expected_values = {{{"field1_doc0", "field1_doc0_1"}, {0, 1}},
                           {{"field1_doc12_1", "field1_doc12_2"}, {1, 12}},
                           {{"field1_doc12_1", "field1_doc12_2"}, {3, 12}},
                           {{"field1_doc12_1", "field1_doc12_2"}, {12, 12}}};

      for (auto& expected : expected_values) {
        const auto expected_doc = expected.second.second;
        const auto& expected_value = expected.first;

        ASSERT_EQ(expected_doc, it->seek(expected_doc));

        std::vector<std::string_view> actual_str_values;
        actual_str_values.push_back(
          irs::ToString<std::string_view>(payload->value.data()));
        actual_str_values.push_back(irs::ToString<std::string_view>(
          reinterpret_cast<const irs::byte_type*>(
            actual_str_values.back().data() +
            actual_str_values.back().size())));

        ASSERT_EQ(expected_value, actual_str_values);
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // check field1 (multiple values per document - cached)
    {
      irs::BytesViewInput in;
      auto column_reader = reader->column(segment0_field1_id);
      ASSERT_NE(nullptr, column_reader);
      auto column = column_reader->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, column);
      auto* actual_value = irs::get<irs::PayAttr>(*column);
      ASSERT_NE(nullptr, actual_value);

      // read compound column value
      // check doc==0, column==field1
      ASSERT_EQ(1, column->seek(1));
      in.reset(actual_value->value);
      ASSERT_EQ("field1_doc0", irs::ReadString<std::string>(in));
      ASSERT_EQ("field1_doc0_1", irs::ReadString<std::string>(in));

      ASSERT_EQ(12, column->seek(2));

      // read overwritten compund value
      // check doc==12, column==field1
      ASSERT_EQ(12, column->seek(12));
      in.reset(actual_value->value);
      ASSERT_EQ("field1_doc12_1", irs::ReadString<std::string>(in));
      ASSERT_EQ("field1_doc12_2", irs::ReadString<std::string>(in));

      ASSERT_TRUE(irs::doc_limits::eof(column->seek(13)));
    }

    // visit empty column
    {
      size_t calls_count = 0;
      auto visitor = [&calls_count](irs::doc_id_t /*doc*/,
                                    const irs::bytes_view& /*in*/) {
        ++calls_count;
        return true;
      };

      auto column_reader = reader->column(segment0_empty_column_id);
      ASSERT_NE(nullptr, column_reader);
      ASSERT_TRUE(Visit(*column_reader, visitor));
      ASSERT_EQ(0, calls_count);
    }

    // iterate over empty column
    {
      auto column_reader = reader->column(segment0_empty_column_id);
      ASSERT_NE(nullptr, column_reader);
      auto it = column_reader->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      ASSERT_TRUE(!irs::get<irs::PayAttr>(*it));
      ASSERT_EQ(irs::doc_limits::eof(), it->value());

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
    }

    // visit field2 values (field after an the field)
    {
      std::unordered_map<std::string_view, irs::doc_id_t> expected_values = {
        {"field2_doc1", 1},
      };

      auto visitor = [&expected_values](irs::doc_id_t doc,
                                        const irs::bytes_view& in) {
        const auto actual_value = irs::ToString<std::string_view>(in.data());

        auto it = expected_values.find(actual_value);
        if (it == expected_values.end()) {
          // can't find value
          return false;
        }

        if (it->second != doc) {
          // wrong document
          return false;
        }

        expected_values.erase(it);
        return true;
      };

      auto column_reader = reader->column(segment0_field2_id);
      ASSERT_NE(nullptr, column_reader);
      ASSERT_TRUE(Visit(*column_reader, visitor));
      ASSERT_TRUE(expected_values.empty());
    }

    // iterate over field2 values (not cached)
    {
      auto column_reader = reader->column(segment0_field2_id);
      ASSERT_NE(nullptr, column_reader);
      auto it = column_reader->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      std::vector<std::pair<std::string_view, irs::doc_id_t>> expected_values =
        {{"field2_doc1", 1}};

      size_t i = 0;
      for (; it->next(); ++i) {
        const auto& expected_value = expected_values[i];
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(expected_value.second, it->value());
        ASSERT_EQ(expected_value.first, actual_str_value);
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(i, expected_values.size());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // seek over field2 values (cached)
    {
      auto column_reader = reader->column(segment0_field2_id);
      ASSERT_NE(nullptr, column_reader);
      auto it = column_reader->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      std::vector<
        std::pair<std::string_view, std::pair<irs::doc_id_t, irs::doc_id_t>>>
        expected_values = {{"field2_doc1", {1, 1}}};

      for (auto& expected : expected_values) {
        const auto expected_doc = expected.second.second;
        const auto expected_value = expected.first;

        ASSERT_EQ(expected_doc, it->seek(expected_doc));
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());
        ASSERT_EQ(expected_value, actual_str_value);
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }
  }

  // read columns values from segment _2
  {
    auto reader = codec()->get_columnstore_reader();
    ASSERT_TRUE(reader->prepare(dir(), meta1));

    // try to read invalild column
    {
      ASSERT_EQ(nullptr, reader->column(irs::field_limits::invalid()));
    }

    // iterate over field0 values (not cached)
    {
      auto column_reader = reader->column(segment1_field0_id);
      ASSERT_NE(nullptr, column_reader);
      auto it = column_reader->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      std::vector<std::pair<std::string_view, irs::doc_id_t>> expected_values =
        {{"segment_2_field1_doc0", 1}, {"segment_2_field1_doc12", 12}};

      size_t i = 0;
      for (; it->next(); ++i) {
        const auto& expected_value = expected_values[i];
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(expected_value.second, it->value());
        ASSERT_EQ(expected_value.first, actual_str_value);
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(i, expected_values.size());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // seek over field0 values (cached)
    {
      auto column_reader = reader->column(segment1_field0_id);
      ASSERT_NE(nullptr, column_reader);
      auto it = column_reader->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      std::vector<
        std::pair<std::string_view, std::pair<irs::doc_id_t, irs::doc_id_t>>>
        expected_values = {{"segment_2_field1_doc0", {0, 1}},
                           {"segment_2_field1_doc12", {12, 12}}};

      for (auto& expected : expected_values) {
        const auto expected_doc = expected.second.second;
        const auto expected_value = expected.first;

        ASSERT_EQ(expected_doc, it->seek(expected_doc));
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());
        ASSERT_EQ(expected_value, actual_str_value);
      }

      ASSERT_EQ(irs::doc_limits::eof(), it->seek(13));
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      ASSERT_FALSE(it->next());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
    }

    // check field0 (cached)
    {
      auto column_reader = reader->column(segment1_field0_id);
      ASSERT_NE(nullptr, column_reader);
      auto column = column_reader->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, column);
      auto* actual_value = irs::get<irs::PayAttr>(*column);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_EQ(1, column->seek(1));  // check doc==1, column==field0
      ASSERT_EQ("segment_2_field1_doc0",
                irs::ToString<std::string_view>(actual_value->value.data()));
      ASSERT_EQ(12, column->seek(12));  // check doc==12, column==field1
      ASSERT_EQ("segment_2_field1_doc12",
                irs::ToString<std::string_view>(actual_value->value.data()));
    }

    // iterate over field0 values (cached)
    {
      auto column_reader = reader->column(segment1_field0_id);
      ASSERT_NE(nullptr, column_reader);
      auto it = column_reader->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, it);

      auto* payload = irs::get<irs::PayAttr>(*it);
      ASSERT_FALSE(!payload);
      ASSERT_EQ(irs::doc_limits::invalid(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);

      std::vector<std::pair<std::string_view, irs::doc_id_t>> expected_values =
        {{"segment_2_field1_doc0", 1}, {"segment_2_field1_doc12", 12}};

      size_t i = 0;
      for (; it->next(); ++i) {
        const auto& expected_value = expected_values[i];
        const auto actual_str_value =
          irs::ToString<std::string_view>(payload->value.data());

        ASSERT_EQ(expected_value.second, it->value());
        ASSERT_EQ(expected_value.first, actual_str_value);
      }

      ASSERT_FALSE(it->next());
      ASSERT_EQ(i, expected_values.size());
      ASSERT_EQ(irs::doc_limits::eof(), it->value());
      ASSERT_EQ(irs::bytes_view{}, payload->value);
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

TEST_P(FormatTestCaseWithEncryption, columnstore_read_write_wrong_encryption) {
  if (!supports_encryption()) {
    return;
  }

  ASSERT_NE(nullptr, dir().attributes().encryption());

  irs::SegmentMeta meta;
  meta.name = "_1";

  // write meta
  {
    auto writer =
      codec()->get_columnstore_writer(false, irs::IResourceManager::gNoop);
    irs::SegmentMeta meta1;

    const irs::ColumnInfo info{
      irs::Type<irs::compression::None>::get(), {}, true};

    // write segment _1
    writer->prepare(dir(), meta);

    {
      auto [id, handle] =
        writer->push_column(info, irs::ColumnFinalizer{
                                    [](irs::DataOutput&) {},
                                    []() { return std::string_view{}; },
                                  });
      handle(1).WriteByte(1);
      handle(2).WriteByte(2);
      handle(3).WriteByte(3);
    }

    const std::set<irs::TypeInfo::type_id> features;

    irs::FlushState state{
      .dir = &dir(),
      .docmap = nullptr,
      .name = meta.name,
      .doc_count = 3,
      .index_features = irs::IndexFeatures::None,
    };

    writer->commit(state);
  }

  auto reader = codec()->get_columnstore_reader();
  ASSERT_NE(nullptr, reader);

  // replace encryption (hack)
  // can't open encrypted index without encryption
  dir().attributes() = irs::DirectoryAttributes{nullptr};
  ASSERT_THROW(reader->prepare(dir(), meta), irs::IndexError);

  // can't open encrypted index with wrong encryption
  dir().attributes() =
    irs::DirectoryAttributes{std::make_unique<tests::Rot13Encryption>(6)};
  ASSERT_THROW(reader->prepare(dir(), meta), irs::IndexError);
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
    auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_TRUE(writer->Commit());
    AssertSnapshotEquality(*writer);
  }

  // replace encryption
  dir().attributes() =
    irs::DirectoryAttributes{std::make_unique<tests::Rot13Encryption>(6)};

  // can't open encrypted index without encryption
  ASSERT_THROW(irs::DirectoryReader{dir()}, irs::IndexError);
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
  irs::FieldMeta field;
  field.name = "field";
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
      terms, irs::FieldMeta{field.name, field.index_features},
      (sorted_terms.empty() ? irs::bytes_view{} : *sorted_terms.begin()),
      (sorted_terms.empty() ? irs::bytes_view{} : *sorted_terms.rbegin())};

    auto writer =
      codec()->get_field_writer(false, irs::IResourceManager::gNoop);
    ASSERT_NE(nullptr, writer);
    writer->prepare(state);
    writer->write(term_reader);
    writer->end();
  }

  irs::SegmentMeta meta;
  meta.name = "segment_name";

  auto reader = codec()->get_field_reader(irs::IResourceManager::gNoop);
  ASSERT_NE(nullptr, reader);

  // can't open encrypted index without encryption
  dir().attributes() = irs::DirectoryAttributes{nullptr};
  ASSERT_THROW(reader->prepare(irs::ReaderState{.dir = &dir(), .meta = &meta}),
               irs::IndexError);

  // can't open encrypted index with wrong encryption
  dir().attributes() =
    irs::DirectoryAttributes{std::make_unique<tests::Rot13Encryption>(6)};
  ASSERT_THROW(reader->prepare(irs::ReaderState{.dir = &dir(), .meta = &meta}),
               irs::IndexError);
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
    auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_TRUE(writer->Commit());
    AssertSnapshotEquality(*writer);
  }

  // can't open encrypted index with wrong encryption
  dir().attributes() =
    irs::DirectoryAttributes{std::make_unique<tests::Rot13Encryption>(6)};
  ASSERT_THROW(irs::DirectoryReader{dir()}, irs::IndexError);
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
    auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_TRUE(writer->Commit());
    AssertSnapshotEquality(*writer);
  }

  // remove encryption
  dir().attributes() = irs::DirectoryAttributes{nullptr};

  // can't open encrypted index without encryption
  ASSERT_THROW(irs::DirectoryReader{dir()}, irs::IndexError);
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
    auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_TRUE(writer->Commit());
    AssertSnapshotEquality(*writer);
  }

  // add cipher
  dir().attributes() =
    irs::DirectoryAttributes{std::make_unique<tests::Rot13Encryption>(7)};

  // check index
  auto index = irs::DirectoryReader(dir());
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

    std::unordered_set<std::string_view> expected_name = {"A"};
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

TEST_P(FormatTestCaseWithEncryption, write_zero_block_encryption) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  const tests::Document* doc1 = gen.next();

  // replace encryption
  ASSERT_NE(nullptr, dir().attributes().encryption());
  dir().attributes() =
    irs::DirectoryAttributes{std::make_unique<tests::Rot13Encryption>(0)};

  auto writer = irs::IndexWriter::Make(dir(), codec(), irs::kOmCreate);
  ASSERT_NE(nullptr, writer);

  ASSERT_THROW(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                      doc1->stored.begin(), doc1->stored.end()),
               irs::IndexError);
}

}  // namespace tests
