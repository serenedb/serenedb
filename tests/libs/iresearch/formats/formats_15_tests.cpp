////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#include <iresearch/index/index_reader_options.hpp>
#include <limits>
#include <random>

#include "formats_test_case_base.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/posting/score_bound_writer.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/top/make.hpp"
#include "tests_shared.hpp"

namespace {

struct FreqScorerContext : public irs::ScoreOperator {
  FreqScorerContext(const irs::FreqBlockAttr* freq) : freq_source{freq} {}

  template<irs::ScoreMergeType MergeType = irs::ScoreMergeType::Noop>
  void ScoreImpl(irs::score_t* res, irs::scores_size_t n) const noexcept {
    for (irs::scores_size_t i = 0; i != n; ++i) {
      irs::Merge<MergeType>(res[i],
                            static_cast<irs::score_t>(freq_source->value[i]));
    }
  }

  void Score(irs::score_t* res, irs::scores_size_t n) const noexcept final {
    ScoreImpl(res, n);
  }
  void ScoreSum(irs::score_t* res, irs::scores_size_t n) const noexcept final {
    ScoreImpl<irs::ScoreMergeType::Sum>(res, n);
  }
  void ScoreMax(irs::score_t* res, irs::scores_size_t n) const noexcept final {
    ScoreImpl<irs::ScoreMergeType::Max>(res, n);
  }
  void ScorePostingBlock(irs::score_t* res) const noexcept final {
    ScoreImpl(res, irs::kPostingBlock);
  }

  const irs::FreqBlockAttr* freq_source;
};

struct FreqScorer : irs::ScorerBase<void> {
  irs::IndexFeatures GetIndexFeatures() const final {
    return irs::IndexFeatures::Freq;
  }

  irs::ScoreFunction PrepareScorer(const irs::ScoreContext& ctx) const final {
    auto* freq = irs::get<irs::FreqBlockAttr>(ctx.doc_attrs);
    EXPECT_NE(nullptr, freq);

    return irs::ScoreFunction::Make<FreqScorerContext>(freq);
  }

  irs::ScoreBoundWriter::ptr PrepareScoreBoundWriter(
    size_t max_levels) const final {
    return std::make_unique<irs::FreqNormWriter<irs::kScoreBoundMaxFreq>>(
      max_levels);
  }

  irs::ScoreBoundSource::ptr PrepareScoreBoundSource() const final {
    return std::make_unique<irs::FreqNormSource<irs::kScoreBoundFreq>>();
  }

  bool HasScoreBounds() const noexcept final { return true; }
};

class MockPostingsField final : public irs::TermReader {
 public:
  MockPostingsField(irs::FieldMeta meta, irs::PostingsHandles handles,
                    bool has_score_bounds, uint64_t docs_count)
    : _meta{std::move(meta)},
      _handles{handles},
      _docs_count{docs_count},
      _has_score_bounds{has_score_bounds} {}

  irs::Attribute* GetMutable(irs::TypeInfo::type_id) noexcept final {
    return nullptr;
  }
  irs::SeekTermIterator::ptr iterator() const final {
    return irs::SeekTermIterator::empty();
  }
  irs::SeekTermIterator::ptr iterator(
    const irs::automaton_table_matcher&) const final {
    return irs::SeekTermIterator::empty();
  }
  void ReadDocs(irs::bytes_view, Acceptor) const final {}
  irs::PostingMeta Lookup(irs::bytes_view) const final { return {}; }
  size_t BitUnion(CookieProvider, uint64_t*) const final { return 0; }
  const irs::FieldMeta& meta() const final { return _meta; }
  size_t size() const final { return 1; }
  uint64_t docs_count() const final { return _docs_count; }
  irs::bytes_view min() const final { return {}; }
  irs::bytes_view max() const final { return {}; }
  bool HasScoreBounds() const final { return _has_score_bounds; }
  irs::PostingsHandles Handles() const noexcept final { return _handles; }

 private:
  irs::FieldMeta _meta;
  irs::PostingsHandles _handles;
  uint64_t _docs_count;
  bool _has_score_bounds;
};

class SkipList {
 public:
  struct Step {
    irs::doc_id_t key;
    irs::score_t freq;
  };

  struct Level {
    const irs::doc_id_t step;
    std::vector<Step> steps;
  };

  static SkipList Make(tests::FormatTestCase::TestPostings& it,
                       irs::doc_id_t skip_0, irs::doc_id_t skip_n,
                       irs::doc_id_t count);

  SkipList() = default;

  size_t Size() const noexcept { return _skip_list.size(); }
  irs::score_t At(size_t level, irs::doc_id_t doc) const noexcept {
    EXPECT_LT(level, _skip_list.size());

    auto& [_, data] = _skip_list[level];
    auto it = absl::c_lower_bound(
      data, Step{doc, 0.f},
      [](const auto& lhs, const auto& rhs) { return lhs.key < rhs.key; });

    EXPECT_NE(it, std::end(data));
    return it->freq;
  }

 private:
  explicit SkipList(std::vector<Level>&& skip_list)
    : _skip_list{std::move(skip_list)} {
    for (auto& [_, level] : _skip_list) {
      EXPECT_TRUE(absl::c_is_sorted(
        level,
        [](const auto& lhs, const auto& rhs) { return lhs.key < rhs.key; }));
    }
  }

  std::vector<Level> _skip_list;
};

SkipList SkipList::Make(tests::FormatTestCase::TestPostings& it,
                        irs::doc_id_t skip_0, irs::doc_id_t skip_n,
                        irs::doc_id_t count) {
  size_t num_levels =
    skip_0 < count ? 1 + irs::math::Log(count / skip_0, skip_n) : 0;
  EXPECT_GT(num_levels, 0);

  std::vector<Level> skip_list;
  skip_list.reserve(num_levels);

  auto step = static_cast<irs::doc_id_t>(
    skip_0 * static_cast<size_t>(std::pow(skip_n, num_levels - 1)));

  for (; num_levels; --num_levels) {
    skip_list.emplace_back(Level{step, std::vector{Step{0U, 0.f}}});
    step /= skip_n;
  }

  auto add = [&](irs::doc_id_t i, irs::doc_id_t doc, irs::score_t freq) {
    for (auto& [step, level] : skip_list) {
      if (level.size() * step < count) {
        ASSERT_FALSE(level.empty());
        level.back() = {doc, std::max(level.back().freq, freq)};
        if (0 == (i % step)) {
          level.emplace_back(Step{0, 0.f});
        }
      }
    }
  };

  for (irs::doc_id_t i = 1; !irs::doc_limits::eof(it.Advance()); ++i) {
    add(i, it.Value(), static_cast<irs::score_t>(it.GetFreq()));
  }

  for (auto& [step, level] : skip_list) {
    level.back() = {irs::doc_limits::eof(),
                    std::numeric_limits<irs::score_t>::max()};
  }

  return SkipList{std::move(skip_list)};
}

void AssertSkipList(const SkipList& expected_freqs, irs::doc_id_t doc,
                    uint32_t threshold) {
  const auto size = expected_freqs.Size();
  if (size == 0) {
    return;
  }
  // Block containing this doc at each level must have max freq >= threshold,
  // otherwise score pruning would have skipped it.
  for (size_t i = 0; i < size; ++i) {
    const auto expected_freq = expected_freqs.At(i, doc);
    if (expected_freq != std::numeric_limits<irs::score_t>::max()) {
      ASSERT_GE(expected_freq, static_cast<irs::score_t>(threshold));
    }
  }
}

class Format15TestCase : public tests::FormatTestCase {
 public:
  static constexpr auto kNone = irs::IndexFeatures::None;
  static constexpr auto kFreq = irs::IndexFeatures::Freq;
  static constexpr auto kPos =
    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
  static constexpr auto kOffs = irs::IndexFeatures::Freq |
                                irs::IndexFeatures::Pos |
                                irs::IndexFeatures::Offs;

  using Doc = std::pair<irs::doc_id_t, uint32_t>;
  using Docs = std::vector<Doc>;
  using DocsView = std::span<const Doc>;

  Docs GenerateDocs(size_t count, float_t mean, float_t dev, size_t step);

  std::pair<irs::PostingMeta, irs::PostingsReader::ptr> WriteReadMeta(
    irs::Directory& dir, DocsView docs, irs::ScorerPtr scorer,
    irs::IndexFeatures features);

  void AssertPostingsWalk(irs::PostingsReader& reader, DocsView docs,
                          irs::IndexFeatures field_features,
                          irs::IndexFeatures features,
                          const irs::PostingMeta& meta);
  void AssertBackwardsNext(irs::PostingsReader& reader, DocsView docs,
                           irs::IndexFeatures field_features,
                           irs::IndexFeatures features,
                           const irs::PostingMeta& meta);
  void AssertDocsSeq(irs::PostingsReader& reader, DocsView docs,
                     irs::IndexFeatures field_features,
                     irs::IndexFeatures features, const irs::PostingMeta& meta);
  void AssertDocsRandom(irs::PostingsReader& reader, DocsView docs,
                        irs::IndexFeatures field_features,
                        irs::IndexFeatures features,
                        const irs::PostingMeta& meta, size_t seed, size_t inc);
  void AssertCornerCases(irs::PostingsReader& reader, DocsView docs,
                         irs::IndexFeatures field_features,
                         irs::IndexFeatures features,
                         const irs::PostingMeta& meta);
  void AssertPostings(DocsView docs, irs::IndexFeatures field_features,
                      irs::IndexFeatures features);
  void AssertPruned(DocsView docs, uint32_t threshold);
  void AssertPrunedPostings(DocsView docs, uint32_t threshold);
  void AssertStressPostings(DocsView docs);

 private:
  tests::SeekPostings::ptr GetIterator(irs::PostingsReader& reader,
                                       irs::IndexFeatures field_features,
                                       irs::IndexFeatures features,
                                       const irs::PostingMeta& meta) const {
    return tests::MakeSeekPostings(meta, reader.Handles(), field_features,
                                   features, HasScoreBounds(field_features));
  }

  static irs::top::Root::ptr MakePruned(const MockPostingsField& field,
                                        const irs::PostingMeta& meta,
                                        const irs::Scorer& scorer,
                                        irs::ColumnArgsFetcher& fetcher,
                                        uint32_t k) {
    static constexpr irs::byte_type kStats[1]{};

    if (irs::search::DocOf(field) == nullptr) {
      return {};
    }

    const irs::search::PostingClause posting{
      .state = irs::TermState{&field, meta},
      .stats = {.stats = kStats, .scorer = &scorer}};
    const irs::top::Context ctx{.scorer = scorer, .fetcher = fetcher, .k = k};

    return irs::top::MakePrunedPosting(posting, irs::SubReader::empty(), ctx);
  }

  static bool HasScoreBounds(irs::IndexFeatures field_features) noexcept {
    return irs::IndexFeatures::None != (field_features & kFreq);
  }
};

std::pair<irs::PostingMeta, irs::PostingsReader::ptr>
Format15TestCase::WriteReadMeta(irs::Directory& dir, DocsView docs,
                                irs::ScorerPtr scorer,
                                irs::IndexFeatures features) {
  EXPECT_TRUE(scorer);
  auto codec = get_codec();
  EXPECT_NE(nullptr, codec);
  auto writer = codec->get_postings_writer(false, irs::IResourceManager::gNoop);
  EXPECT_NE(nullptr, writer);
  irs::PostingMeta posting_meta;

  {
    const irs::FlushState state{
      .dir = &dir,
      .norms = &irs::SubReader::empty(),
      .name = "segment_name",
      .scorer = scorer,
      .doc_count = docs.back().first + 1,
      .index_features = features,
    };

    auto out = dir.create("attributes");
    EXPECT_FALSE(!out);
    irs::WriteStr(*out, std::string_view("file_header"));

    writer->Prepare(*out, state);
    writer->BeginField(irs::FieldProperties{.index_features = features});

    TestPostings it{docs, features};
    writer->Write(it, posting_meta);
    const auto stats = writer->EndField();
    EXPECT_EQ(docs.size(), stats.docs_count);
    const uint64_t expected_has_score_bounds =
      irs::IndexFeatures::None != (features & irs::IndexFeatures::Freq);
    EXPECT_EQ(expected_has_score_bounds, stats.has_score_bounds);
    writer->Encode(*out, posting_meta);
    writer->End();
  }

  irs::SegmentMeta meta;
  meta.name = "segment_name";

  const irs::ReaderState state{.dir = &dir, .meta = &meta, .scorer = scorer};

  auto in = dir.open("attributes", irs::IOAdvice::NORMAL);
  EXPECT_FALSE(!in);
  [[maybe_unused]] const auto tmp = irs::ReadString<std::string>(*in);

  auto reader = codec->get_postings_reader();
  EXPECT_NE(nullptr, reader);
  reader->prepare(*in, state, features);

  irs::bstring in_data(in->Length() - in->Position(), 0);
  in->ReadData(&in_data[0], in_data.size());
  const auto* begin = in_data.c_str();

  irs::PostingMeta read_meta;
  begin += reader->decode(begin, features, read_meta);

  {
    EXPECT_EQ(posting_meta.docs_count, read_meta.docs_count);
    EXPECT_EQ(posting_meta.doc_start, read_meta.doc_start);
    EXPECT_EQ(posting_meta.pos_start, read_meta.pos_start);
    EXPECT_EQ(posting_meta.pay_start, read_meta.pay_start);
    EXPECT_EQ(posting_meta.pos_offset, read_meta.pos_offset);
    EXPECT_EQ(posting_meta.doc_delta, read_meta.doc_delta);
  }

  EXPECT_EQ(begin, in_data.data() + in_data.size());

  return std::make_pair(read_meta, std::move(reader));
}

void Format15TestCase::AssertPostingsWalk(irs::PostingsReader& reader,
                                          DocsView docs,
                                          irs::IndexFeatures field_features,
                                          irs::IndexFeatures features,
                                          const irs::PostingMeta& meta) {
  auto actual = reader.Postings(field_features, features, meta,
                                HasScoreBounds(field_features));
  ASSERT_NE(nullptr, actual);

  TestPostings expected{docs, features};
  const bool has_freq = irs::IndexFeatures::None != (features & kFreq);

  while (!irs::doc_limits::eof(expected.Advance())) {
    ASSERT_FALSE(irs::doc_limits::eof(actual->Advance()));
    ASSERT_EQ(expected.Value(), actual->Value());
    if (has_freq) {
      ASSERT_EQ(expected.GetFreq(), actual->GetFreq());
    }
    AssertFrequencyAndPositions(expected, *actual, features);
  }

  ASSERT_TRUE(irs::doc_limits::eof(actual->Advance()));
}

void Format15TestCase::AssertBackwardsNext(irs::PostingsReader& reader,
                                           DocsView docs,
                                           irs::IndexFeatures field_features,
                                           irs::IndexFeatures features,
                                           const irs::PostingMeta& meta) {
  for (auto doc = docs.rbegin(), end = docs.rend(); doc != end; ++doc) {
    TestPostings expected{docs, features};

    auto actual = GetIterator(reader, field_features, features, meta);
    ASSERT_NE(nullptr, actual);

    ASSERT_FALSE(irs::doc_limits::valid(actual->Value()));
    ASSERT_EQ(doc->first, actual->Seek(doc->first));

    ASSERT_EQ(doc->first, expected.SeekTo(doc->first));
    AssertFrequencyAndPositions(expected, *actual, features);

    while (!irs::doc_limits::eof(expected.Advance())) {
      ASSERT_FALSE(irs::doc_limits::eof(actual->Advance()));
      ASSERT_EQ(expected.Value(), actual->Value());
      AssertFrequencyAndPositions(expected, *actual, features);
    }
    ASSERT_TRUE(irs::doc_limits::eof(actual->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(actual->Value()));
  }
}

void Format15TestCase::AssertDocsRandom(irs::PostingsReader& reader,
                                        DocsView docs,
                                        irs::IndexFeatures field_features,
                                        irs::IndexFeatures features,
                                        const irs::PostingMeta& meta,
                                        size_t seed, size_t inc) {
  TestPostings expected{docs, features};

  auto actual = GetIterator(reader, field_features, features, meta);
  ASSERT_NE(nullptr, actual);

  ASSERT_FALSE(irs::doc_limits::valid(actual->Value()));

  for (size_t i = seed, size = docs.size(); i < size; i += inc) {
    const auto& doc = docs[i];
    ASSERT_EQ(doc.first, actual->Seek(doc.first));
    // Seek to the same doc
    ASSERT_EQ(doc.first, actual->Seek(doc.first));
    // Seek to the smaller doc
    ASSERT_EQ(doc.first, actual->Seek(irs::doc_limits::invalid()));

    ASSERT_EQ(doc.first, expected.SeekTo(doc.first));
    AssertFrequencyAndPositions(expected, *actual, features);
  }

  if (inc == 1) {
    ASSERT_TRUE(irs::doc_limits::eof(actual->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(actual->Value()));

    // Seek after the existing documents
    ASSERT_TRUE(irs::doc_limits::eof(actual->Seek(docs.back().first + 42)));
  }
}

void Format15TestCase::AssertDocsSeq(irs::PostingsReader& reader, DocsView docs,
                                     irs::IndexFeatures field_features,
                                     irs::IndexFeatures features,
                                     const irs::PostingMeta& meta) {
  TestPostings expected{docs, features};

  auto actual = GetIterator(reader, field_features, features, meta);
  ASSERT_NE(nullptr, actual);

  ASSERT_FALSE(irs::doc_limits::valid(actual->Value()));

  while (!irs::doc_limits::eof(expected.Advance())) {
    const auto expected_doc_id = expected.Value();
    ASSERT_FALSE(irs::doc_limits::eof(actual->Advance()));

    ASSERT_EQ(expected_doc_id, actual->Value());
    ASSERT_EQ(expected_doc_id, actual->Seek(expected_doc_id));
    // seek to the same doc
    ASSERT_EQ(expected_doc_id, actual->Seek(expected_doc_id));
    // seek to the smaller doc
    ASSERT_EQ(expected_doc_id, actual->Seek(irs::doc_limits::invalid()));

    AssertFrequencyAndPositions(expected, *actual, features);
  }

  ASSERT_TRUE(irs::doc_limits::eof(actual->Advance()));
  ASSERT_TRUE(irs::doc_limits::eof(actual->Value()));

  // seek after the existing documents
  ASSERT_TRUE(irs::doc_limits::eof(actual->Seek(docs.back().first + 42)));
}

Format15TestCase::Docs Format15TestCase::GenerateDocs(size_t count,
                                                      float_t mean, float_t dev,
                                                      size_t step) {
  std::vector<std::pair<irs::doc_id_t, uint32_t>> docs;
  docs.reserve(count);
  std::generate_n(
    std::back_inserter(docs), count,
    [i = (irs::doc_limits::min)(), gen = std::mt19937{},
     distr = std::normal_distribution<float_t>{mean, dev}, step]() mutable {
      const irs::doc_id_t doc = i;
      const auto freq = static_cast<uint32_t>(std::roundf(distr(gen)));
      i += step;

      return std::make_pair(doc, freq);
    });

  auto check_docs = [](const auto& docs) {
    return std::is_sorted(
             std::begin(docs), std::end(docs),
             [](auto& lhs, auto& rhs) { return lhs.first < rhs.first; }) &&
           std::all_of(std::begin(docs), std::end(docs), [](auto& v) {
             return static_cast<int32_t>(v.second) > 0;
           });
  };
  EXPECT_TRUE(check_docs(docs));

  return docs;
}

void Format15TestCase::AssertCornerCases(irs::PostingsReader& reader,
                                         DocsView docs,
                                         irs::IndexFeatures field_features,
                                         irs::IndexFeatures features,
                                         const irs::PostingMeta& meta) {
  // next + seek to eof
  {
    auto it = GetIterator(reader, field_features, features, meta);
    ASSERT_FALSE(irs::doc_limits::valid(it->Value()));
    ASSERT_TRUE(!irs::doc_limits::eof(it->Advance()));
    ASSERT_EQ(docs.front().first, it->Value());
    ASSERT_TRUE(irs::doc_limits::eof(it->Seek(docs.back().first + 42)));
  }

  // Seek to irs::doc_limits::invalid()
  {
    auto it = GetIterator(reader, field_features, features, meta);
    ASSERT_FALSE(irs::doc_limits::valid(it->Value()));
    ASSERT_FALSE(irs::doc_limits::valid(it->Seek(irs::doc_limits::invalid())));
    ASSERT_TRUE(!irs::doc_limits::eof(it->Advance()));
    ASSERT_EQ(docs.front().first, it->Value());
  }

  // Seek to irs::doc_limits::eof()
  {
    auto it = GetIterator(reader, field_features, features, meta);
    ASSERT_FALSE(irs::doc_limits::valid(it->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(it->Seek(irs::doc_limits::eof())));
    ASSERT_FALSE(!irs::doc_limits::eof(it->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(it->Value()));
  }
}

void Format15TestCase::AssertPostings(DocsView docs,
                                      irs::IndexFeatures field_features,
                                      irs::IndexFeatures features) {
  FreqScorer scorer;
  const irs::Scorer* scorer_ptr = &scorer;

  auto dir = get_directory(*this);
  ASSERT_NE(nullptr, dir);
  auto [meta, reader] = WriteReadMeta(*dir, docs, scorer_ptr, field_features);
  ASSERT_NE(nullptr, reader);

  ASSERT_EQ((field_features & features), features);

  {
    irs::FieldMeta field_meta;
    field_meta.index_features = field_features;
    MockPostingsField field{field_meta, reader->Handles(),
                            HasScoreBounds(field_features), docs.size()};
    const bool expected_pruned =
      HasScoreBounds(field_features) && docs.size() > GetPostingsBlockSize();
    irs::ColumnArgsFetcher fetcher;
    ASSERT_EQ(expected_pruned,
              MakePruned(field, meta, scorer, fetcher, 1) != nullptr);
  }

  AssertPostingsWalk(*reader, docs, field_features, features, meta);

  AssertCornerCases(*reader, docs, field_features, features, meta);

  AssertDocsSeq(*reader, docs, field_features, features, meta);

  AssertDocsRandom(*reader, docs, field_features, features, meta,
                   GetPostingsBlockSize() - 1, GetPostingsBlockSize());

  AssertDocsRandom(*reader, docs, field_features, features, meta,
                   GetPostingsBlockSize(), GetPostingsBlockSize());

  AssertDocsRandom(*reader, docs, field_features, features, meta, 0, 1);

  AssertDocsRandom(*reader, docs, field_features, features, meta, 0, 5);

  AssertBackwardsNext(*reader, docs, field_features, features, meta);
}

void Format15TestCase::AssertPruned(DocsView docs, uint32_t threshold) {
  ASSERT_GT(docs.size(), GetPostingsBlockSize());

  FreqScorer scorer;
  const irs::Scorer* scorer_ptr = &scorer;

  auto dir = get_directory(*this);
  ASSERT_NE(nullptr, dir);
  auto [meta, reader] = WriteReadMeta(*dir, docs, scorer_ptr, kFreq);
  ASSERT_NE(nullptr, reader);

  irs::FieldMeta field_meta;
  field_meta.index_features = kFreq;
  MockPostingsField field{field_meta, reader->Handles(), true, docs.size()};

  const auto k = static_cast<uint32_t>(docs.size() + 1);
  irs::ColumnArgsFetcher fetcher;
  auto root = MakePruned(field, meta, scorer, fetcher, k);
  ASSERT_NE(nullptr, root);

  irs::score_t score_threshold = static_cast<irs::score_t>(threshold);
  std::vector<irs::ScoreDoc> hits(k);
  irs::LoserScoreCollector collector{score_threshold, hits};
  root->Run(collector);

  Docs expected;
  for (const auto& doc : docs) {
    if (static_cast<irs::score_t>(doc.second) > score_threshold) {
      expected.emplace_back(doc);
    }
  }

  hits.resize(collector.AcceptedCount());
  std::sort(std::begin(hits), std::end(hits),
            [](const auto& lhs, const auto& rhs) { return lhs.doc < rhs.doc; });

  ASSERT_EQ(expected.size(), hits.size());
  for (size_t i = 0, size = hits.size(); i != size; ++i) {
    ASSERT_EQ(expected[i].first, hits[i].doc);
    ASSERT_EQ(static_cast<irs::score_t>(expected[i].second), hits[i].score);
  }

  TestPostings walk{docs, kFreq};
  const auto skip_list = SkipList::Make(
    walk, GetPostingsBlockSize(), 8, static_cast<irs::doc_id_t>(docs.size()));
  for (const auto& hit : hits) {
    AssertSkipList(skip_list, hit.doc, threshold);
  }

  const size_t block = GetPostingsBlockSize();
  size_t visited = 0;
  for (size_t i = 0; i < docs.size(); i += block) {
    const auto len = std::min(block, docs.size() - i);
    uint32_t max_freq = 0;
    for (size_t j = i; j != i + len; ++j) {
      max_freq = std::max(max_freq, docs[j].second);
    }
    if (i == 0 || len != block || max_freq > threshold) {
      visited += len;
    }
  }

  ASSERT_LE(collector.TotalMatches(), visited);
  ASSERT_GE(collector.TotalMatches(), hits.size());
}

void Format15TestCase::AssertPrunedPostings(DocsView docs, uint32_t threshold) {
  AssertPostings(docs, kFreq, kFreq);

  AssertPruned(docs, threshold);
}

void Format15TestCase::AssertStressPostings(DocsView docs) {
  AssertPostings(docs, kNone, kNone);
  AssertPostings(docs, kOffs, kNone);
  AssertPostings(docs, kFreq, kFreq);
  AssertPostings(docs, kPos, kPos);
  AssertPostings(docs, kOffs, kOffs);
}

static const auto kTestFormats =
  ::testing::Values(tests::FormatInfo{"1_5simd"});

static const auto kTestDirs =
  ::testing::ValuesIn(tests::GetDirectories<tests::kTypesAll>());

static const auto kTestDirsWithoutEncryption =
  ::testing::ValuesIn(tests::GetDirectories<tests::kTypesDefault>());

static const auto kTestDirsWithEncryption =
  ::testing::ValuesIn(tests::GetDirectories<tests::kTypesAllRot13>());

static const auto kTestValues = ::testing::Combine(kTestDirs, kTestFormats);
static const auto kTestValuesWithoutEncryption =
  ::testing::Combine(kTestDirsWithoutEncryption, kTestFormats);
static const auto kTestValuesWithEncryption =
  ::testing::Combine(kTestDirsWithEncryption, kTestFormats);

// Generic tests
using tests::FormatTestCase;
INSTANTIATE_TEST_SUITE_P(Format15Test, FormatTestCase, kTestValues,
                         FormatTestCase::to_string);

using tests::FormatTestCaseWithEncryption;
INSTANTIATE_TEST_SUITE_P(Format15Test, FormatTestCaseWithEncryption,
                         kTestValuesWithEncryption,
                         FormatTestCaseWithEncryption::to_string);

// 1.5 specific tests

TEST_P(Format15TestCase, SingletonPostings) {
  static constexpr size_t kCount = 1;
  ASSERT_TRUE(kCount < GetPostingsBlockSize());

  const auto docs = GenerateDocs(kCount, 50.f, 14.f, 1);

  AssertStressPostings(docs);
}

TEST_P(Format15TestCase, ShortPostings) {
  static constexpr size_t kCount = 117;  // < postings_writer::BLOCK_SIZE
  ASSERT_TRUE(kCount < GetPostingsBlockSize());

  const auto docs = GenerateDocs(kCount, 50.f, 14.f, 1);

  AssertStressPostings(docs);
}

TEST_P(Format15TestCase, BlockPostings) {
  const auto docs = GenerateDocs(GetPostingsBlockSize(), 50.f, 14.f, 1);

  AssertStressPostings(docs);
}

TEST_P(Format15TestCase, LongPostingsPruneThreshold60) {
  static constexpr size_t kCount = 10000;
  static constexpr uint32_t kThreshold = 60;
  // N(40,7): block max ~ 40+3.12*7 ~ 62, so roughly half blocks are pruned
  const auto docs = GenerateDocs(kCount, 40.f, 7.f, 1);

  AssertPrunedPostings(docs, kThreshold);
}

TEST_P(Format15TestCase, LongPostingsPruneThreshold100) {
  static constexpr size_t kCount = 10000;
  static constexpr uint32_t kThreshold = 100;
  // N(50,13): block max ~ 50+3.12*13 ~ 91, so most blocks are pruned
  const auto docs = GenerateDocs(kCount, 50.f, 13.f, 1);

  AssertPrunedPostings(docs, kThreshold);
}

TEST_P(Format15TestCase, LongPostingsStress) {
  static constexpr size_t kCount = 10000;
  const auto docs = GenerateDocs(kCount, 50.f, 13.f, 1);

  AssertStressPostings(docs);
}

TEST_P(Format15TestCase, MediumPostings) {
  static constexpr size_t kCount = 319;
  ASSERT_TRUE(kCount > GetPostingsBlockSize());
  const auto docs = GenerateDocs(kCount, 50.f, 13.f, 1);

  AssertStressPostings(docs);
}

TEST_P(Format15TestCase, LongPostings) {
  GTEST_SKIP() << "too long for our CI";
  static constexpr size_t kCount = 10000;
  const auto docs = GenerateDocs(kCount, 50.f, 13.f, 1);

  AssertStressPostings(docs);
}

TEST_P(Format15TestCase, VeryLongPostings) {
  GTEST_SKIP() << "too long for our CI";
  static constexpr size_t kCount = size_t{1} << 15;
  const auto docs = GenerateDocs(kCount, 1000.f, 20.f, 2);

  AssertStressPostings(docs);
}

INSTANTIATE_TEST_SUITE_P(Format15Test, Format15TestCase,
                         kTestValuesWithoutEncryption,
                         Format15TestCase::to_string);

}  // namespace
