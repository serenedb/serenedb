////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include <absl/algorithm/container.h>

#include <cmath>
#include <core_functions/array_kernels.hpp>
#include <duckdb.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/ann/maxsim.hpp>
#include <iresearch/search/detail/ann_index.hpp>
#include <iresearch/search/detail/vector_slots.hpp>
#include <iresearch/search/filters/term_filter.hpp>
#include <iresearch/search/filters/vector_similarity_filter.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <limits>
#include <random>
#include <span>
#include <string>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "tests_shared.hpp"

namespace {

constexpr irs::field_id kVec = 1;
constexpr irs::field_id kTag = 2;
constexpr uint32_t kDim = 16;
constexpr uint32_t kRows = 1200;
constexpr uint32_t kQueries = 8;

uint32_t TokensOf(uint32_t row) { return row % 11 == 0 ? 0 : 1 + row % 5; }

bool IsNull(uint32_t row) { return row % 13 == 0; }

std::vector<float> UnitVectors(size_t n, uint32_t seed) {
  std::mt19937 rng{seed};
  std::normal_distribution<float> dist;
  std::vector<float> out(n * kDim);
  for (size_t i = 0; i < n; ++i) {
    const std::span<float> v{out.data() + i * kDim, kDim};
    float norm = 0;
    for (auto& x : v) {
      x = dist(rng);
      norm += x * x;
    }
    for (auto& x : v) {
      x /= std::sqrt(norm);
    }
  }
  return out;
}

void WriteTokensAt(irs::ColWriter& cs, irs::doc_id_t doc,
                   std::span<const float> tokens, bool null) {
  const auto type = duckdb::LogicalType::LIST(
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, kDim));
  auto& cw = cs.OpenColumn(kVec, type);
  duckdb::Vector v{type, 1};
  const auto count = tokens.size() / kDim;
  duckdb::ListVector::Reserve(v, count);
  auto& arrays = duckdb::ListVector::GetChildMutable(v);
  auto& values = duckdb::ArrayVector::GetChildMutable(arrays);
  absl::c_copy(tokens, duckdb::FlatVector::GetDataMutable<float>(values));
  duckdb::FlatVector::ValidityMutable(arrays).SetAllValid(count);
  duckdb::FlatVector::ValidityMutable(values).SetAllValid(count * kDim);
  duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(v)[0] = {0, count};
  duckdb::ListVector::SetListSize(v, count);
  duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
  if (null) {
    duckdb::FlatVector::SetNull(v, 0, true);
  }
  duckdb::FlatVector::SetSize(v, 1);
  cw.Append(static_cast<uint64_t>(doc) - irs::doc_limits::min(), v, 1);
}

class MaxSimTest : public ::testing::TestWithParam<irs::AnnKind> {
 protected:
  void SetUp() final {
    auto opts = irs::tests::DefaultWriterOptions();
    const irs::AnnInfo ann{
      .kind = GetParam(),
      .index_id = kVec,
      .column_id = kVec,
      .d = static_cast<int>(kDim),
      .metric = irs::VectorMetric::InnerProduct,
      .quant = {.kind = irs::VectorQuantization::None},
      .sample_factor = 1.f,
      .posting_size = 64,
      .m = 8,
      .ef_construction = 64,
    };
    opts.anns[kVec].push_back(kVec);
    opts.column_options = [ann](irs::field_id id) {
      irs::ColumnOptions col;
      if (id == kVec) {
        col.ann_info = ann;
      }
      return col;
    };
    _writer = irs::IndexWriter::Make(_dir, irs::kOmCreate, std::move(opts));
    ASSERT_NE(nullptr, _writer);
    uint32_t total = 0;
    for (uint32_t row = 0; row < kRows; ++row) {
      _first.push_back(total);
      total += IsNull(row) ? 0 : TokensOf(row);
    }
    _first.push_back(total);
    _tokens = UnitVectors(total, 7);
    {
      auto trx = _writer->GetBatch();
      const duckdb::string_t even{"even", 4};
      const duckdb::string_t odd{"odd", 3};
      for (uint32_t row = 0; row < kRows; ++row) {
        auto doc = trx.Insert();
        const auto d = doc.DocId();
        EXPECT_TRUE(doc.WithField(kTag, irs::StringLayout::kStrings,
                                  irs::IndexFeatures::Freq,
                                  [&](irs::FieldInverter& fld) {
                                    return fld.InvertKeywords([&](auto&& emit) {
                                      emit(d % 2 == 0 ? even : odd, d);
                                    });
                                  }));
        WriteTokensAt(*doc.GetColWriter(), d, Tokens(row), IsNull(row));
      }
      trx.Commit();
    }
    _writer->RefreshCommit();
    _reader = _writer->GetSnapshot();
    ASSERT_EQ(1U, _reader.size());
    _queries = UnitVectors(kQueries, 11);
  }

  std::span<const float> Tokens(uint32_t row) const {
    return std::span<const float>{_tokens}.subspan(
      size_t{_first[row]} * kDim, size_t{_first[row + 1] - _first[row]} * kDim);
  }

  float BruteForce(uint32_t row, std::span<const float> queries) const {
    const auto tokens = Tokens(row);
    std::vector<float> best(queries.size() / kDim,
                            std::numeric_limits<float>::lowest());
    duckdb::MaxSimAccumulate(queries.data(), best.size(), tokens.data(),
                             tokens.size() / kDim, kDim, best.data());
    return duckdb::MaxSimSum(best.data(), best.size());
  }

  irs::ByVectorSimilarityOptions Options() const {
    irs::ByVectorSimilarityOptions opts;
    opts.column_id = kVec;
    opts.ann_id = kVec;
    opts.metric = irs::VectorMetric::InnerProduct;
    opts.ef_search = static_cast<uint32_t>(_tokens.size());
    opts.min_ef = opts.ef_search;
    opts.top_k = 10;
    opts.posting_size = 64;
    opts.maxsim = true;
    opts.maxsim_depth = static_cast<uint32_t>(_tokens.size());
    opts.inner = _inner;
    return opts;
  }

  void RemoveEveryThird() {
    irs::DocRemovalBuilder removal;
    for (irs::doc_id_t doc = irs::doc_limits::min();
         doc < irs::doc_limits::min() + kRows; ++doc) {
      if (doc % 3 == 0) {
        removal.Add(0, doc);
      }
    }
    auto trx = _writer->GetBatch();
    trx.Remove(std::move(removal).Finish(_reader));
    trx.Commit();
    _writer->RefreshCommit();
    _reader = _writer->GetSnapshot();
    ASSERT_EQ(1U, _reader.size());
  }

  std::vector<uint32_t> Ends(uint32_t per_query) const {
    std::vector<uint32_t> ends;
    for (uint32_t end = per_query; end <= kQueries; end += per_query) {
      ends.push_back(end);
    }
    return ends;
  }

  void ExpectExhaustive(uint32_t per_query,
                        const std::function<bool(irs::doc_id_t)>& admitted) {
    const auto& segment = _reader[0];
    const auto* ann = segment.Ann(kVec);
    ASSERT_NE(nullptr, ann);
    const auto* entries = ann->Entries(segment, kVec);
    ASSERT_NE(nullptr, entries);
    irs::ann::MaxSimCandidates candidates;
    const auto ends = Ends(per_query);
    candidates.Search(*ann, *entries, segment, {}, Options(), 1 << 20, _queries,
                      ends);
    for (uint32_t g = 0; g < ends.size(); ++g) {
      const auto queries = std::span<const float>{_queries}.subspan(
        size_t{g} * per_query * kDim, size_t{per_query} * kDim);
      const auto group = candidates.Group(g);
      size_t expected = 0;
      for (uint32_t row = 0; row < kRows; ++row) {
        expected += _first[row + 1] != _first[row] &&
                    admitted(row + irs::doc_limits::min());
      }
      ASSERT_EQ(expected, group.size()) << "group " << g;
      for (size_t i = 0; i < group.size(); ++i) {
        const auto doc = group[i].doc;
        if (i != 0) {
          EXPECT_LT(group[i - 1].doc, doc);
        }
        EXPECT_TRUE(admitted(doc)) << doc;
        const auto row = doc - irs::doc_limits::min();
        EXPECT_NEAR(BruteForce(row, queries), group[i].upper, 1e-4f) << doc;
      }
    }
  }

  irs::MemoryDirectory _dir;
  irs::IndexWriter::ptr _writer;
  irs::DirectoryReader _reader;
  std::vector<uint32_t> _first;
  std::vector<float> _tokens;
  std::vector<float> _queries;
  std::shared_ptr<const irs::Filter> _inner;
};

TEST_P(MaxSimTest, EntriesFollowTheListOffsets) {
  const auto& segment = _reader[0];
  const auto* entries = segment.Ann(kVec)->Entries(segment, kVec);
  ASSERT_NE(nullptr, entries);
  ASSERT_EQ(kRows, entries->Rows());
  ASSERT_EQ(_first.back(), entries->Count());
  for (uint32_t row = 0; row < kRows; ++row) {
    ASSERT_EQ(_first[row], entries->First(row)) << row;
    ASSERT_EQ(_first[row + 1], entries->End(row)) << row;
    for (auto entry = entries->First(row); entry < entries->End(row); ++entry) {
      ASSERT_EQ(row, entries->Row(entry)) << entry;
    }
  }
}

TEST_P(MaxSimTest, ExhaustiveBoundsAreTheExactScores) {
  ExpectExhaustive(1, [](irs::doc_id_t) { return true; });
  ExpectExhaustive(4, [](irs::doc_id_t) { return true; });
}

TEST_P(MaxSimTest, FilterAdmitsRows) {
  auto even = std::make_shared<irs::ByTerm>();
  *even->mutable_field_id() = kTag;
  even->mutable_options()->term =
    irs::ViewCast<irs::byte_type>(std::string_view{"even"});
  _inner = std::move(even);
  ExpectExhaustive(2, [](irs::doc_id_t doc) { return doc % 2 == 0; });
}

TEST_P(MaxSimTest, RemovalsDropRows) {
  RemoveEveryThird();
  ExpectExhaustive(2, [](irs::doc_id_t doc) { return doc % 3 != 0; });
}

TEST_P(MaxSimTest, RescoreReadsEveryVectorOfTheRow) {
  const auto& segment = _reader[0];
  const auto* entries = segment.Ann(kVec)->Entries(segment, kVec);
  ASSERT_NE(nullptr, entries);
  irs::detail::RawVectorReader reader{*segment.Column(kVec)->Child(),
                                      *segment.GetColReader(), kDim};
  reader.SetMaxSim(_queries, *entries);
  const auto pair = std::span<const float>{_queries}.first(2 * kDim);
  for (uint32_t row = 0; row < kRows; ++row) {
    if (_first[row + 1] == _first[row]) {
      continue;
    }
    const auto doc = row + irs::doc_limits::min();
    EXPECT_EQ(BruteForce(row, _queries), reader.MaxSim(doc)) << row;
    EXPECT_EQ(BruteForce(row, pair), reader.MaxSim(doc, pair)) << row;
  }
}

INSTANTIATE_TEST_SUITE_P(
  Kinds, MaxSimTest, ::testing::Values(irs::AnnKind::Ivf, irs::AnnKind::Hnsw),
  [](const auto& info) {
    return std::string{magic_enum::enum_name(info.param)};
  });

}  // namespace
