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

#include <gtest/gtest.h>

#include "basics/bit_packing.hpp"
#include "basics/down_cast.h"
#include "basics/duckdb_engine.h"
#include "formats/column/test_cs_helpers.hpp"
#include "formats_test_case_base.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/index/burst_trie.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/store/mmap_directory.hpp"
#include "iresearch/utils/type_limits.hpp"
#include "tests_shared.hpp"

namespace {

using tests::FormatTestCase;

class Format10TestCase : public tests::FormatTestCase {
 protected:
  void PostingsSeek(const std::vector<std::pair<irs::doc_id_t, uint32_t>>& docs,
                    irs::IndexFeatures features) {
    irs::FieldMeta field;
    field.index_features = features;
    auto dir = get_directory(*this);

    // attributes for term
    auto codec = get_codec();
    ASSERT_NE(nullptr, codec);
    auto writer =
      codec->get_postings_writer(false, irs::IResourceManager::gNoop);
    ASSERT_NE(nullptr, writer);
    irs::PostingMeta posting_meta;

    // write postings for field
    {
      irs::FlushState state{
        .dir = dir.get(),
        .name = "segment_name",
        .doc_count = docs.back().first + 1,
        .index_features = field.index_features,
      };

      auto out = dir->create("attributes");
      ASSERT_FALSE(!out);
      irs::WriteStr(*out, std::string_view("file_header"));

      // prepare writer
      writer->Prepare(*out, state);

      writer->BeginField(field);

      // write postings for term
      {
        TestPostings it(docs, field.index_features);
        writer->Write(it, posting_meta);

        // write attributes to out
        writer->Encode(*out, posting_meta);
      }

      auto stats = writer->EndField();
      ASSERT_FALSE(stats.has_score_bounds);
      ASSERT_EQ(docs.size(), stats.docs_count);

      writer->End();
    }

    // read postings
    {
      irs::SegmentMeta meta;
      meta.name = "segment_name";

      irs::ReaderState state;
      state.dir = dir.get();
      state.meta = &meta;

      auto in = dir->open("attributes", irs::IOAdvice::NORMAL);
      ASSERT_FALSE(!in);
      const auto tmp = irs::ReadString<std::string>(*in);

      // prepare reader
      auto reader = codec->get_postings_reader();
      ASSERT_NE(nullptr, reader);
      reader->prepare(*in, state, field.index_features);

      irs::bstring in_data(in->Length() - in->Position(), 0);
      in->ReadData(&in_data[0], in_data.size());
      const auto* begin = in_data.c_str();

      // read term attributes
      {
        irs::PostingMeta read_meta;
        begin += reader->decode(begin, field.index_features, read_meta);

        // check PostingMeta
        {
          ASSERT_EQ(posting_meta.docs_count, read_meta.docs_count);
          ASSERT_EQ(posting_meta.doc_start, read_meta.doc_start);
          ASSERT_EQ(posting_meta.pos_start, read_meta.pos_start);
          ASSERT_EQ(posting_meta.pay_start, read_meta.pay_start);
          ASSERT_EQ(posting_meta.pos_offset, read_meta.pos_offset);
          ASSERT_EQ(posting_meta.doc_delta, read_meta.doc_delta);
        }

        const auto handles = reader->Handles();

        auto assert_docs = [&](size_t seed, size_t inc) {
          auto actual = tests::MakeSeekPostings(read_meta, handles,
                                                field.index_features, features,
                                                /*has_score_bounds=*/false);
          ASSERT_FALSE(irs::doc_limits::valid(actual->Value()));

          TestPostings expected(docs, field.index_features);
          for (size_t i = seed, size = docs.size(); i < size; i += inc) {
            auto& doc = docs[i];
            ASSERT_EQ(doc.first, actual->Seek(doc.first));
            ASSERT_EQ(doc.first,
                      actual->Seek(doc.first));  // seek to the same doc
            ASSERT_EQ(
              doc.first,
              actual->Seek(
                irs::doc_limits::invalid()));  // seek to the smaller doc

            ASSERT_EQ(doc.first, expected.SeekTo(doc.first));
            AssertFrequencyAndPositions(expected, *actual, features);
          }

          if (inc == 1) {
            ASSERT_FALSE(!irs::doc_limits::eof(actual->Advance()));
            ASSERT_TRUE(irs::doc_limits::eof(actual->Value()));

            // seek after the existing documents
            ASSERT_TRUE(
              irs::doc_limits::eof(actual->Seek(docs.back().first + 42)));
          }
        };

        // next + seek to eof
        {
          auto it = tests::MakeSeekPostings(
            read_meta, handles, field.index_features, irs::IndexFeatures::None,
            /*has_score_bounds=*/false);
          ASSERT_FALSE(irs::doc_limits::valid(it->Value()));
          ASSERT_TRUE(!irs::doc_limits::eof(it->Advance()));
          ASSERT_EQ(docs.front().first, it->Value());
          ASSERT_TRUE(irs::doc_limits::eof(it->Seek(docs.back().first + 42)));
        }

        // seek to every document 127th document in a block
        assert_docs(GetPostingsBlockSize() - 1, GetPostingsBlockSize());

        // seek to every 128th document in a block
        assert_docs(GetPostingsBlockSize(), GetPostingsBlockSize());

        // seek to every document
        assert_docs(0, 1);

        // seek to every 5th document
        assert_docs(0, 5);

        // seek for backwards && next
        {
          for (auto doc = docs.rbegin(), end = docs.rend(); doc != end; ++doc) {
            TestPostings expected(docs, field.index_features);
            auto it = tests::MakeSeekPostings(read_meta, handles,
                                              field.index_features, features,
                                              /*has_score_bounds=*/false);
            ASSERT_FALSE(irs::doc_limits::valid(it->Value()));
            ASSERT_EQ(doc->first, it->Seek(doc->first));

            ASSERT_EQ(doc->first, expected.SeekTo(doc->first));
            AssertFrequencyAndPositions(expected, *it, features);
            if (doc != docs.rbegin()) {
              ASSERT_TRUE(!irs::doc_limits::eof(it->Advance()));
              const auto expected_doc = (doc - 1)->first;
              ASSERT_EQ(expected_doc, it->Value());

              ASSERT_TRUE(!irs::doc_limits::eof(expected.Advance()));
              ASSERT_EQ(expected_doc, expected.Value());
              AssertFrequencyAndPositions(expected, *it, features);
            }
          }
        }

        // seek to irs::doc_limits::invalid()
        {
          auto it = tests::MakeSeekPostings(
            read_meta, handles, field.index_features, irs::IndexFeatures::None,
            /*has_score_bounds=*/false);
          ASSERT_FALSE(irs::doc_limits::valid(it->Value()));
          ASSERT_FALSE(
            irs::doc_limits::valid(it->Seek(irs::doc_limits::invalid())));
          ASSERT_TRUE(!irs::doc_limits::eof(it->Advance()));
          ASSERT_EQ(docs.front().first, it->Value());
        }

        // seek to irs::doc_limits::eof()
        {
          auto it = tests::MakeSeekPostings(
            read_meta, handles, field.index_features, irs::IndexFeatures::None,
            /*has_score_bounds=*/false);
          ASSERT_FALSE(irs::doc_limits::valid(it->Value()));
          ASSERT_TRUE(irs::doc_limits::eof(it->Seek(irs::doc_limits::eof())));
          ASSERT_FALSE(!irs::doc_limits::eof(it->Advance()));
          ASSERT_TRUE(irs::doc_limits::eof(it->Value()));
        }
      }

      ASSERT_EQ(begin, in_data.data() + in_data.size());
    }
  }
};

TEST_P(Format10TestCase, postings_read_write_single_doc) {
  irs::FieldMeta field;

  // docs & attributes for term0
  const std::vector<std::pair<irs::doc_id_t, uint32_t>> docs0{{3, 10}};

  // docs & attributes for term0
  const std::vector<std::pair<irs::doc_id_t, uint32_t>> docs1{{6, 10}};

  auto codec = get_codec();
  ASSERT_NE(nullptr, codec);
  auto writer = codec->get_postings_writer(false, irs::IResourceManager::gNoop);
  irs::PostingMeta meta0, meta1;

  // write postings
  {
    irs::FlushState state{
      .dir = &dir(),
      .name = "segment_name",
      .doc_count = 100,
      .index_features = field.index_features,
    };

    auto out = dir().create("attributes");
    ASSERT_FALSE(!out);

    // prepare writer
    writer->Prepare(*out, state);

    // begin field
    writer->BeginField(field);

    // write postings for term0
    {
      TestPostings docs(docs0);
      writer->Write(docs, meta0);

      // check PostingMeta
      {
        auto& meta = static_cast<irs::PostingMeta&>(meta0);
        ASSERT_EQ(1, meta.docs_count);
        ASSERT_EQ(2, meta.doc_delta);
      }

      // write term0 attributes to out
      writer->Encode(*out, meta0);
    }

    // write postings for term0
    {
      TestPostings docs(docs1);
      writer->Write(docs, meta1);

      // check PostingMeta
      {
        auto& meta = static_cast<irs::PostingMeta&>(meta1);
        ASSERT_EQ(1, meta.docs_count);
        ASSERT_EQ(5, meta.doc_delta);
      }

      // write term0 attributes to out
      writer->Encode(*out, meta1);
    }

    // check doc positions for term0 & term1
    {
      ASSERT_EQ(meta0.docs_count, meta1.docs_count);
      ASSERT_EQ(meta0.doc_start, meta1.doc_start);
      ASSERT_EQ(meta0.pos_start, meta1.pos_start);
      ASSERT_EQ(meta0.pay_start, meta1.pay_start);
      ASSERT_EQ(meta0.pos_offset, meta1.pos_offset);
    }

    // finish writing
    writer->End();
  }

  // read postings
  {
    irs::SegmentMeta meta;
    meta.name = "segment_name";

    irs::ReaderState state;
    state.dir = &dir();
    state.meta = &meta;

    auto in = dir().open("attributes", irs::IOAdvice::NORMAL);
    ASSERT_FALSE(!in);

    // prepare reader
    auto reader = codec->get_postings_reader();
    ASSERT_NE(nullptr, reader);
    reader->prepare(*in, state, field.index_features);

    irs::bstring in_data(in->Length() - in->Position(), 0);
    in->ReadData(&in_data[0], in_data.size());
    const auto* begin = in_data.c_str();

    // read term0 attributes & postings
    {
      irs::PostingMeta read_meta;

      begin += reader->decode(begin, field.index_features, read_meta);

      // check PostingMeta for term0
      {
        ASSERT_EQ(meta0.docs_count, read_meta.docs_count);
        ASSERT_EQ(meta0.doc_start, read_meta.doc_start);
        ASSERT_EQ(meta0.pos_start, read_meta.pos_start);
        ASSERT_EQ(meta0.pay_start, read_meta.pay_start);
        ASSERT_EQ(meta0.pos_offset, read_meta.pos_offset);
        ASSERT_EQ(meta0.doc_delta, read_meta.doc_delta);
      }

      // read documents
      auto it = reader->Postings(field.index_features, irs::IndexFeatures::None,
                                 read_meta, /*has_score_bounds=*/false);
      for (size_t i = 0; !irs::doc_limits::eof(it->Advance());) {
        ASSERT_EQ(docs0[i++].first, it->Value());
      }
    }

    // check PostingMeta for term1
    {
      irs::PostingMeta read_meta;
      begin += reader->decode(begin, field.index_features, read_meta);

      {
        ASSERT_EQ(meta1.docs_count, read_meta.docs_count);
        ASSERT_EQ(0, read_meta.doc_start); /* we don't read doc start in
                                              case of singleton */
        ASSERT_EQ(meta1.pos_start, read_meta.pos_start);
        ASSERT_EQ(meta1.pay_start, read_meta.pay_start);
        ASSERT_EQ(meta1.pos_offset, read_meta.pos_offset);
        ASSERT_EQ(meta1.doc_delta, read_meta.doc_delta);
      }

      // read documents
      auto it = reader->Postings(field.index_features, irs::IndexFeatures::None,
                                 read_meta, /*has_score_bounds=*/false);
      for (size_t i = 0; !irs::doc_limits::eof(it->Advance());) {
        ASSERT_EQ(docs1[i++].first, it->Value());
      }
    }

    ASSERT_EQ(begin, in_data.data() + in_data.size());
  }
}

TEST_P(Format10TestCase, postings_read_write) {
  constexpr irs::IndexFeatures kFeatures = irs::IndexFeatures::None;

  irs::FieldMeta field;
  field.index_features = kFeatures;

  // docs & attributes for term0
  const std::vector<std::pair<irs::doc_id_t, uint32_t>> docs0{
    {1, 10}, {3, 10}, {5, 10}, {7, 10}, {79, 10}, {101, 10}, {124, 10}};

  // docs & attributes for term1
  const std::vector<std::pair<irs::doc_id_t, uint32_t>> docs1{
    {2, 10}, {7, 10}, {9, 10}, {19, 10}};

  auto codec = get_codec();
  ASSERT_NE(nullptr, codec);
  auto writer = codec->get_postings_writer(false, irs::IResourceManager::gNoop);
  ASSERT_NE(nullptr, writer);
  irs::PostingMeta meta0, meta1;  // must be destroyed before writer

  // write postings
  {
    irs::FlushState state{
      .dir = &dir(),
      .name = "segment_name",
      .doc_count = 150,
      .index_features = field.index_features,
    };

    auto out = dir().create("attributes");
    ASSERT_FALSE(!out);

    // prepare writer
    writer->Prepare(*out, state);

    // begin field
    writer->BeginField(field);

    // write postings for term0
    {
      TestPostings docs(docs0);
      writer->Write(docs, meta0);

      // write attributes to out
      writer->Encode(*out, meta0);
    }
    // write postings for term1
    {
      TestPostings docs(docs1);
      writer->Write(docs, meta1);

      // write attributes to out
      writer->Encode(*out, meta1);
    }

    // check doc positions for term0 & term1
    ASSERT_LT(meta0.doc_start, meta1.doc_start);

    // finish writing
    writer->End();
  }

  // read postings
  {
    irs::SegmentMeta meta;
    meta.name = "segment_name";

    irs::ReaderState state;
    state.dir = &dir();
    state.meta = &meta;

    auto in = dir().open("attributes", irs::IOAdvice::NORMAL);
    ASSERT_FALSE(!in);

    // prepare reader
    auto reader = codec->get_postings_reader();
    ASSERT_NE(nullptr, reader);
    reader->prepare(*in, state, field.index_features);

    irs::bstring in_data(in->Length() - in->Position(), 0);
    in->ReadData(&in_data[0], in_data.size());
    const auto* begin = in_data.c_str();

    // cumulative attribute
    irs::PostingMeta read_meta;

    // read term0 attributes
    {
      begin += reader->decode(begin, field.index_features, read_meta);

      // check PostingMeta
      {
        ASSERT_EQ(meta0.docs_count, read_meta.docs_count);
        ASSERT_EQ(meta0.doc_start, read_meta.doc_start);
        ASSERT_EQ(meta0.pos_start, read_meta.pos_start);
        ASSERT_EQ(meta0.pay_start, read_meta.pay_start);
        ASSERT_EQ(meta0.pos_offset, read_meta.pos_offset);
        ASSERT_EQ(meta0.doc_delta, read_meta.doc_delta);
      }

      // read documents
      auto it = reader->Postings(field.index_features, irs::IndexFeatures::None,
                                 read_meta, /*has_score_bounds=*/false);
      for (size_t i = 0; !irs::doc_limits::eof(it->Advance());) {
        ASSERT_EQ(docs0[i++].first, it->Value());
      }
    }

    // read term1 attributes
    {
      begin += reader->decode(begin, field.index_features, read_meta);

      // check PostingMeta
      {
        ASSERT_EQ(meta1.docs_count, read_meta.docs_count);
        ASSERT_EQ(meta1.doc_start, read_meta.doc_start);
        ASSERT_EQ(meta1.pos_start, read_meta.pos_start);
        ASSERT_EQ(meta1.pay_start, read_meta.pay_start);
        ASSERT_EQ(meta1.pos_offset, read_meta.pos_offset);
        ASSERT_EQ(meta1.doc_delta, read_meta.doc_delta);
      }

      // read documents
      auto it = reader->Postings(field.index_features, irs::IndexFeatures::None,
                                 read_meta, /*has_score_bounds=*/false);
      for (size_t i = 0; !irs::doc_limits::eof(it->Advance());) {
        ASSERT_EQ(docs1[i++].first, it->Value());
      }
    }

    ASSERT_EQ(begin, in_data.data() + in_data.size());
  }
}

TEST_P(Format10TestCase, postings_writer_reuse) {
  auto codec = get_codec();
  ASSERT_NE(nullptr, codec);
  auto writer = codec->get_postings_writer(false, irs::IResourceManager::gNoop);
  ASSERT_NE(nullptr, writer);

  std::vector<std::pair<irs::doc_id_t, uint32_t>> docs0;
  irs::doc_id_t i = (irs::doc_limits::min)();
  for (; i < 1000; ++i) {
    docs0.emplace_back(i, 10);
  }

  // gap

  for (i += 1000; i < 10000; ++i) {
    docs0.emplace_back(i, 10);
  }

  // write docs 'segment0' with all possible streams
  {
    constexpr irs::IndexFeatures kFeatures = irs::IndexFeatures::Freq |
                                             irs::IndexFeatures::Pos |
                                             irs::IndexFeatures::Offs;

    irs::FieldMeta field;
    field.id = 1;
    field.index_features = kFeatures;

    irs::FlushState state{
      .dir = &dir(),
      .name = "0",
      .doc_count = 10000,
      // all possible features in segment
      .index_features = field.index_features,
    };

    auto out = dir().create(std::string("postings") + state.name.data());
    ASSERT_FALSE(!out);

    TestPostings docs(docs0);

    writer->Prepare(*out, state);
    writer->BeginField(field);
    irs::PostingMeta meta;
    writer->Write(docs, meta);
    writer->End();
  }

  // write docs 'segment1' with position & offset
  {
    constexpr irs::IndexFeatures kFeatures = irs::IndexFeatures::Freq |
                                             irs::IndexFeatures::Pos |
                                             irs::IndexFeatures::Offs;

    irs::FieldMeta field;
    field.id = 1;
    field.index_features = kFeatures;

    irs::FlushState state{
      .dir = &dir(),
      .name = "1",
      .doc_count = 10000,
      // all possible features in segment
      .index_features = field.index_features,
    };

    auto out = dir().create(std::string("postings") + state.name.data());
    ASSERT_FALSE(!out);

    TestPostings docs(docs0);

    writer->Prepare(*out, state);
    writer->BeginField(field);
    irs::PostingMeta meta;
    writer->Write(docs, meta);
    writer->End();
  }

  // write docs 'segment2' with position & payload
  {
    constexpr irs::IndexFeatures kFeatures =
      irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;

    irs::FieldMeta field;
    field.id = 1;
    field.index_features = kFeatures;

    irs::FlushState state{
      .dir = &dir(),
      .name = "2",
      .doc_count = 10000,
      // all possible features in segment
      .index_features = field.index_features,
    };

    auto out = dir().create(std::string("postings") + state.name.data());
    ASSERT_FALSE(!out);

    TestPostings docs(docs0);

    writer->Prepare(*out, state);
    writer->BeginField(field);
    irs::PostingMeta meta;
    writer->Write(docs, meta);
    writer->End();
  }

  // write docs 'segment3' with position
  {
    constexpr irs::IndexFeatures kFeatures =
      irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;

    irs::FieldMeta field;
    field.id = 1;
    field.index_features = kFeatures;

    irs::FlushState state{
      .dir = &dir(),
      .name = "3",
      .doc_count = 10000,
      // all possible features in segment
      .index_features = field.index_features,
    };

    auto out = dir().create(std::string("postings") + state.name.data());
    ASSERT_FALSE(!out);

    TestPostings docs(docs0);

    writer->Prepare(*out, state);
    writer->BeginField(field);
    irs::PostingMeta meta;
    writer->Write(docs, meta);
    writer->End();
  }

  // write docs 'segment3' with frequency
  {
    constexpr irs::IndexFeatures kFeatures = irs::IndexFeatures::Freq;

    irs::FieldMeta field;
    field.id = 1;
    field.index_features = kFeatures;

    irs::FlushState state{
      .dir = &dir(),
      .name = "4",
      .doc_count = 10000,
      // all possible features in segment
      .index_features = field.index_features,
    };

    auto out = dir().create(std::string("postings") + state.name.data());
    ASSERT_FALSE(!out);

    TestPostings docs(docs0);

    writer->Prepare(*out, state);
    writer->BeginField(field);
    irs::PostingMeta meta;
    writer->Write(docs, meta);
    writer->End();
  }

  // writer segment without any attributes
  {
    constexpr irs::IndexFeatures kFeatures = irs::IndexFeatures::None;

    irs::FieldMeta field;
    field.id = 1;
    field.index_features = kFeatures;

    irs::FlushState state{
      .dir = &dir(),
      .name = "5",
      .doc_count = 10000,
    };

    auto out = dir().create(std::string("postings") + state.name.data());
    ASSERT_FALSE(!out);

    TestPostings docs(docs0);

    writer->Prepare(*out, state);
    writer->BeginField(field);
    irs::PostingMeta meta;
    writer->Write(docs, meta);
    writer->End();
  }
}

TEST_P(Format10TestCase, ires336) {
  // bug: ires336
  auto dir = get_directory(*this);
  const std::string_view segment_name = "bug";
  constexpr irs::field_id kFieldId = 1;
  const irs::bytes_view term =
    irs::ViewCast<irs::byte_type>(std::string_view("protein_coding"));

  std::vector<std::pair<irs::doc_id_t, uint32_t>> docs;
  {
    std::string buf;
    std::ifstream in(resource("postings.txt").c_str());
    char* pend;
    while (std::getline(in, buf)) {
      docs.emplace_back(strtol(buf.c_str(), &pend, 10), 10);
    }
  }
  std::vector<irs::bytes_view> terms{term};
  tests::FormatTestCase::Terms<decltype(terms.begin())> trms(
    terms.begin(), terms.end(), docs.begin(), docs.end());

  irs::FlushState flush_state{
    .dir = dir.get(),
    .name = segment_name,
    .doc_count = 10000,
  };

  irs::FieldMeta field_meta;
  field_meta.id = kFieldId;
  {
    tests::MockTermReader term_reader{
      trms, field_meta, (terms.empty() ? irs::bytes_view{} : *terms.begin()),
      (terms.empty() ? irs::bytes_view{} : *terms.rbegin())};
    irs::IdxWriter idx{*dir, segment_name,
                       ::sdb::DuckDBEngine::Instance().instance()};
    irs::burst_trie::FieldWriter fw{
      get_codec()->get_postings_writer(/*compaction=*/true,
                                       irs::IResourceManager::gNoop),
      /*compaction=*/true, irs::IResourceManager::gNoop};
    fw.SetIdxWriter(idx);
    fw.prepare(flush_state);
    fw.write(term_reader);
    fw.end();
    idx.Commit();
  }

  irs::SegmentMeta meta;
  meta.name = segment_name;

  irs::IdxReader idx_reader{*dir, segment_name};
  irs::burst_trie::FieldReader fr_obj{get_codec()->get_postings_reader(),
                                      irs::IResourceManager::gNoop};
  auto* fr = &fr_obj;
  fr->prepare(
    irs::ReaderState{.dir = dir.get(), .meta = &meta, .idx = &idx_reader});

  const auto* field = fr->field(field_meta.id);
  ASSERT_NE(nullptr, field);
  auto it = field->iterator();
  ASSERT_TRUE(it->seek(term));

  const irs::PostingMeta term_meta = it->cookie();
  const auto handles = field->Handles();
  const auto layout = field->meta().index_features;
  const bool bounds = field->HasScoreBounds();
  auto make_docs = [&] {
    return tests::MakeSeekPostings(term_meta, handles, layout,
                                   irs::IndexFeatures::None, bounds);
  };

  // ires-336 sequence
  {
    auto docs = make_docs();
    ASSERT_EQ(4048, docs->Seek(4048));
    ASSERT_EQ(6830, docs->Seek(6829));
  }

  // ires-336 extended sequence
  {
    auto docs = make_docs();
    ASSERT_EQ(1068, docs->Seek(1068));
    ASSERT_EQ(1875, docs->Seek(1873));
    ASSERT_EQ(4048, docs->Seek(4048));
    ASSERT_EQ(6830, docs->Seek(6829));
  }

  // extended sequence
  {
    auto docs = make_docs();
    ASSERT_EQ(4048, docs->Seek(4048));
    ASSERT_EQ(4400, docs->Seek(4400));
    ASSERT_EQ(6830, docs->Seek(6829));
  }

  // ires-336 full sequence
  {
    auto docs = make_docs();
    ASSERT_EQ(334, docs->Seek(334));
    ASSERT_EQ(1046, docs->Seek(1046));
    ASSERT_EQ(1068, docs->Seek(1068));
    ASSERT_EQ(2307, docs->Seek(2307));
    ASSERT_EQ(2843, docs->Seek(2843));
    ASSERT_EQ(3059, docs->Seek(3059));
    ASSERT_EQ(3564, docs->Seek(3564));
    ASSERT_EQ(4048, docs->Seek(4048));
    ASSERT_EQ(7773, docs->Seek(7773));
    ASSERT_EQ(8204, docs->Seek(8204));
    ASSERT_EQ(9353, docs->Seek(9353));
    ASSERT_EQ(9366, docs->Seek(9366));
  }
}

TEST_P(Format10TestCase, postings_seek) {
  auto generate_docs = [](size_t count, size_t step) {
    std::vector<std::pair<irs::doc_id_t, uint32_t>> docs;
    docs.reserve(count);
    irs::doc_id_t i = (irs::doc_limits::min)();
    std::generate_n(std::back_inserter(docs), count, [&i, step] {
      const irs::doc_id_t doc = i;
      const uint32_t freq = std::max(1U, doc % 7);
      i += step;

      return std::make_pair(doc, freq);
    });
    return docs;
  };

  constexpr auto kNone = irs::IndexFeatures::None;
  constexpr auto kFreq = irs::IndexFeatures::Freq;
  constexpr auto kPos = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
  constexpr auto kOffs = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
                         irs::IndexFeatures::Offs;

  // singleton doc
  {
    constexpr size_t kCount = 1;
    ASSERT_TRUE(kCount < GetPostingsBlockSize());

    const auto docs = generate_docs(kCount, 1);

    PostingsSeek(docs, kNone);
    PostingsSeek(docs, kFreq);
    PostingsSeek(docs, kPos);
    PostingsSeek(docs, kOffs);
  }

  // short list (< postings_writer::BLOCK_SIZE)
  {
    constexpr size_t kCount = 117;
    ASSERT_TRUE(kCount < GetPostingsBlockSize());

    const auto docs = generate_docs(kCount, 1);

    PostingsSeek(docs, kNone);
    PostingsSeek(docs, kFreq);
    PostingsSeek(docs, kPos);
    PostingsSeek(docs, kOffs);
  }

  // equals to postings_writer::BLOCK_SIZE
  {
    const auto docs = generate_docs(GetPostingsBlockSize(), 1);

    PostingsSeek(docs, kNone);
    PostingsSeek(docs, kFreq);
    PostingsSeek(docs, kPos);
    PostingsSeek(docs, kOffs);
  }

  // long list
  {
    constexpr size_t kCount = 10000;
    const auto docs = generate_docs(kCount, 1);

    PostingsSeek(docs, kNone);
    PostingsSeek(docs, kFreq);
    PostingsSeek(docs, kPos);
    PostingsSeek(docs, kOffs);
  }

  // 2^15
  {
    constexpr size_t kCount = 32768;
    const auto docs = generate_docs(kCount, 2);

    PostingsSeek(docs, kNone);
    PostingsSeek(docs, kFreq);
    PostingsSeek(docs, kPos);
    PostingsSeek(docs, kOffs);
  }
}

TEST_P(Format10TestCase, position_reset_with_offsets) {
  // Regression test: reset() must restore both pos and pay stream positions.
  // Previously, reset() only seeked _pos_in but not _pay_in, causing
  // ReadTail to hit unreachable code when offsets were enabled.

  auto generate_docs = [](size_t count) {
    std::vector<std::pair<irs::doc_id_t, uint32_t>> docs;
    docs.reserve(count);
    irs::doc_id_t i = (irs::doc_limits::min)();
    std::generate_n(std::back_inserter(docs), count, [&i] {
      const irs::doc_id_t doc = i;
      const uint32_t freq = std::max(1U, doc % 7);
      ++i;
      return std::pair{doc, freq};
    });
    return docs;
  };

  auto test_reset =
    [&](const std::vector<std::pair<irs::doc_id_t, uint32_t>>& docs,
        irs::IndexFeatures features) {
      irs::FieldMeta field;
      field.index_features = features;
      auto dir = get_directory(*this);

      auto codec = get_codec();
      ASSERT_NE(nullptr, codec);
      auto writer =
        codec->get_postings_writer(false, irs::IResourceManager::gNoop);
      ASSERT_NE(nullptr, writer);
      irs::PostingMeta posting_meta;

      // write postings
      {
        irs::FlushState state{
          .dir = dir.get(),
          .name = "segment_name",
          .doc_count = docs.back().first + 1,
          .index_features = field.index_features,
        };

        auto out = dir->create("attributes");
        ASSERT_FALSE(!out);
        irs::WriteStr(*out, std::string_view("file_header"));

        writer->Prepare(*out, state);
        writer->BeginField(field);

        {
          TestPostings it(docs, field.index_features);
          writer->Write(it, posting_meta);
          writer->Encode(*out, posting_meta);
        }

        writer->EndField();
        writer->End();
      }

      // read postings and test reset
      {
        irs::SegmentMeta meta;
        meta.name = "segment_name";

        irs::ReaderState state;
        state.dir = dir.get();
        state.meta = &meta;

        auto in = dir->open("attributes", irs::IOAdvice::NORMAL);
        ASSERT_FALSE(!in);
        const auto tmp = irs::ReadString<std::string>(*in);

        auto reader = codec->get_postings_reader();
        ASSERT_NE(nullptr, reader);
        reader->prepare(*in, state, field.index_features);

        irs::bstring in_data(in->Length() - in->Position(), 0);
        in->ReadData(&in_data[0], in_data.size());
        const auto* begin = in_data.c_str();

        irs::PostingMeta read_meta;
        begin += reader->decode(begin, field.index_features, read_meta);

        const auto handles = reader->Handles();

        for (size_t i = 0; i < docs.size();
             i += std::max<size_t>(1, docs.size() / 10)) {
          auto actual = tests::MakeSeekPostings(read_meta, handles,
                                                field.index_features, features,
                                                /*has_score_bounds=*/false);
          ASSERT_FALSE(irs::doc_limits::valid(actual->Value()));

          const auto& doc = docs[i];
          ASSERT_EQ(doc.first, actual->Seek(doc.first));

          auto* pos = actual->Positions();
          ASSERT_NE(nullptr, pos);

          auto* offs = irs::get<irs::OffsAttr>(*pos);
          const bool has_offs =
            irs::IndexFeatures::None != (features & irs::IndexFeatures::Offs);
          ASSERT_EQ(has_offs, offs != nullptr);

          // collect positions (and offsets) from the first pass
          struct PosData {
            irs::PosAttr::value_t value;
            uint32_t start;
            uint32_t end;
          };
          std::vector<PosData> expected_positions;
          while (pos->next()) {
            PosData pd{.value = pos->value()};
            if (offs) {
              pd.start = offs->start;
              pd.end = offs->end;
            }
            expected_positions.push_back(pd);
          }
          ASSERT_EQ(doc.second, expected_positions.size());

          // reset and iterate again -- must produce the same results
          pos->reset();
          std::vector<PosData> actual_positions;
          while (pos->next()) {
            PosData pd{.value = pos->value()};
            if (offs) {
              pd.start = offs->start;
              pd.end = offs->end;
            }
            actual_positions.push_back(pd);
          }

          ASSERT_EQ(expected_positions.size(), actual_positions.size());
          for (size_t j = 0; j < expected_positions.size(); ++j) {
            ASSERT_EQ(expected_positions[j].value, actual_positions[j].value)
              << "doc=" << doc.first << " pos_index=" << j;
            if (offs) {
              ASSERT_EQ(expected_positions[j].start, actual_positions[j].start)
                << "doc=" << doc.first << " pos_index=" << j;
              ASSERT_EQ(expected_positions[j].end, actual_positions[j].end)
                << "doc=" << doc.first << " pos_index=" << j;
            }
          }
        }

        ASSERT_EQ(begin, in_data.data() + in_data.size());
      }
    };

  constexpr auto kPos = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
  constexpr auto kOffs = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
                         irs::IndexFeatures::Offs;

  // short list (< block size) -- only tail positions
  {
    const auto docs = generate_docs(117);
    test_reset(docs, kPos);
    test_reset(docs, kOffs);
  }

  // exactly one block
  {
    const auto docs = generate_docs(GetPostingsBlockSize());
    test_reset(docs, kPos);
    test_reset(docs, kOffs);
  }

  // multiple blocks
  {
    const auto docs = generate_docs(1000);
    test_reset(docs, kPos);
    test_reset(docs, kOffs);
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();
static const auto kTestValues =
  ::testing::Combine(::testing::ValuesIn(kTestDirs),
                     ::testing::Values(tests::FormatInfo{"1_5simd"}));

// 1.0 specific tests
INSTANTIATE_TEST_SUITE_P(Format10Test, Format10TestCase, kTestValues,
                         Format10TestCase::to_string);

}  // namespace
