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

#pragma once

#include <absl/algorithm/container.h>

#include <atomic>
#include <bit>
#include <functional>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/search/count/root.hpp>
#include <iresearch/search/detail/column_collector.hpp>
#include <iresearch/search/detail/window.hpp>
#include <iresearch/search/docs/root.hpp>
#include <iresearch/search/fill/node.hpp>
#include <iresearch/search/hits/root.hpp>
#include <iresearch/search/probe/node.hpp>
#include <iresearch/search/scorers/bm25.hpp>
#include <iresearch/search/top/root.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/bit_utils.hpp>
#include <limits>
#include <map>
#include <span>
#include <string_view>
#include <vector>

#include "filter_test_case_base.hpp"
#include "formats/column/test_cs_helpers.hpp"
#include "tests_shared.hpp"

namespace tests {

inline constexpr size_t kTop = 5;

struct AnalyzedField {
  irs::field_id Id() const { return id; }

  irs::analysis::Tokenizer& GetTokens() const { return *analyzer; }

  std::string_view Value() const noexcept { return value; }

  irs::IndexFeatures GetIndexFeatures() const noexcept { return features; }

  bool Write(irs::DataOutput& out) const {
    out.WriteData(reinterpret_cast<const irs::byte_type*>(value.data()),
                  value.size());
    return true;
  }

  irs::analysis::Tokenizer* analyzer{};
  std::string_view value;
  irs::field_id id{};
  irs::IndexFeatures features = irs::IndexFeatures::Freq;
};

struct Families {
  uint64_t count = 0;
  std::vector<irs::doc_id_t> docs;
  std::vector<irs::doc_id_t> fill;
  std::vector<irs::doc_id_t> probe;
  std::map<irs::doc_id_t, irs::score_t> hits;
  std::map<irs::doc_id_t, irs::score_t> fill_scores;
  std::map<irs::doc_id_t, irs::score_t> probe_scores;
  std::vector<irs::score_t> top;
  uint64_t top_total = 0;
};

inline void ExpectScores(const std::map<irs::doc_id_t, irs::score_t>& expected,
                         const std::map<irs::doc_id_t, irs::score_t>& actual) {
  ASSERT_EQ(expected.size(), actual.size());
  for (const auto& [doc, score] : expected) {
    ASSERT_TRUE(actual.contains(doc)) << doc;
    EXPECT_FLOAT_EQ(score, actual.at(doc)) << doc;
  }
}

class FamilyIndex {
 public:
  const irs::DirectoryReader& Reader() const noexcept { return _reader; }

  std::vector<irs::doc_id_t> Docs(const irs::Filter& filter) const {
    PreparedFilter prepared{filter, *_reader};
    std::vector<irs::doc_id_t> out;
    for (size_t i = 0; i != prepared.size(); ++i) {
      auto docs = prepared.Execute(i);
      while (!irs::doc_limits::eof(docs->Next())) {
        out.push_back(Global(i, docs->Value()));
      }
    }
    return out;
  }

  std::map<irs::doc_id_t, irs::score_t> ScoresBy(
    const irs::Scorer& scorer, const irs::Filter& filter) const {
    MaxMemoryCounter counter;
    PreparedFilter prepared{filter, *_reader, &scorer, counter};
    std::map<irs::doc_id_t, irs::score_t> out;
    for (size_t i = 0; i != prepared.size(); ++i) {
      irs::ColumnArgsFetcher fetcher;
      auto docs = prepared.ExecuteScored(i, fetcher);
      auto score = docs->PrepareScore();
      while (!irs::doc_limits::eof(docs->Next())) {
        docs->FetchScoreArgs(0);
        fetcher.Fetch(docs->Value());
        irs::score_t value{};
        score.Score(&value, 1);
        out.emplace(Global(i, docs->Value()), value);
      }
    }
    return out;
  }

  std::map<irs::doc_id_t, irs::score_t> Freqs(const irs::Filter& filter) const {
    return ScoresBy(sort::FrequencyScore{}, filter);
  }

  Families Run(const irs::Filter& filter) const {
    Families out;
    {
      PreparedFilter prepared{filter, *_reader};
      for (size_t i = 0; i != prepared.size(); ++i) {
        const auto* query = prepared.Query(i);
        if (!query || irs::QueryBuilder::IsEmpty(*query)) {
          continue;
        }
        const auto end = End(i);
        out.count += query->PlanCount({})->Run(irs::doc_limits::min(), end);

        auto docs = query->PlanDocs({});
        std::vector<irs::doc_id_t> buf(irs::detail::kWindowDocs +
                                       irs::doc_limits::kDocsSlack);
        auto fill = query->PlanFill({}, irs::ScoreMergeType::Noop);
        std::vector<uint64_t> mask(irs::detail::kWindowWords);
        for (auto min = irs::doc_limits::min(); min < end;
             min += irs::detail::kWindowDocs) {
          const auto max = std::min(min + irs::detail::kWindowDocs, end);
          const auto n = docs->Run(min, max, buf.data());
          for (uint32_t j = 0; j != n; ++j) {
            out.docs.push_back(Global(i, buf[j]));
          }
          absl::c_fill(mask, 0);
          fill->FillOr(min, max, mask.data());
          ForEachBit(mask, min, [&](irs::doc_id_t doc) {
            out.fill.push_back(Global(i, doc));
          });
        }

        auto probe = query->PlanProbe({}, end - irs::doc_limits::min());
        for (auto doc = irs::doc_limits::min(); doc < end; ++doc) {
          if (probe->Probe(doc) == doc) {
            out.probe.push_back(Global(i, doc));
          }
        }
      }
    }

    irs::BM25 scorer;
    MaxMemoryCounter counter;
    PreparedFilter prepared{filter, *_reader, &scorer, counter};
    for (size_t i = 0; i != prepared.size(); ++i) {
      const auto* query = prepared.Query(i);
      if (!query || irs::QueryBuilder::IsEmpty(*query)) {
        continue;
      }
      const auto end = End(i);
      {
        irs::ColumnArgsFetcher fetcher;
        auto hits = query->PlanScored({.scorer = scorer, .fetcher = fetcher});
        std::vector<irs::doc_id_t> docs(irs::detail::kWindowDocs +
                                        irs::doc_limits::kDocsSlack);
        std::vector<irs::score_t> scores(irs::detail::kWindowDocs +
                                         irs::doc_limits::kScoresSlack);
        for (auto min = irs::doc_limits::min(); min < end;
             min += irs::detail::kWindowDocs) {
          const auto max = std::min(min + irs::detail::kWindowDocs, end);
          const auto n = hits->Run(min, max, docs.data(), scores.data());
          for (uint32_t j = 0; j != n; ++j) {
            out.hits.emplace(Global(i, docs[j]), scores[j]);
          }
        }
      }
      {
        irs::ColumnArgsFetcher fetcher;
        auto fill = query->PlanFill({.scorer = &scorer, .fetcher = &fetcher},
                                    irs::ScoreMergeType::Sum);
        std::vector<uint64_t> mask(irs::detail::kWindowWords);
        std::vector<irs::score_t> scores(irs::detail::kWindowDocs);
        for (auto min = irs::doc_limits::min(); min < end;
             min += irs::detail::kWindowDocs) {
          const auto max = std::min(min + irs::detail::kWindowDocs, end);
          absl::c_fill(mask, 0);
          absl::c_fill(scores, 0.f);
          fill->Fill(min, max, mask.data(), scores.data());
          ForEachBit(mask, min, [&](irs::doc_id_t doc) {
            out.fill_scores.emplace(Global(i, doc), scores[doc - min]);
          });
        }
      }
      {
        irs::ColumnArgsFetcher fetcher;
        auto probe = query->PlanProbe({.scorer = &scorer, .fetcher = &fetcher},
                                      end - irs::doc_limits::min());
        auto score = probe->PrepareScore();
        for (auto doc = irs::doc_limits::min(); doc < end; ++doc) {
          if (probe->Probe(doc) != doc) {
            continue;
          }
          probe->FetchScoreArgs(0);
          fetcher.Fetch(doc);
          irs::score_t value{};
          score.Score(&value, 1);
          out.probe_scores.emplace(Global(i, doc), value);
        }
      }
      {
        irs::ColumnArgsFetcher fetcher;
        auto top = query->PlanTop(
          {.scorer = scorer, .fetcher = fetcher, .prune = false, .k = kTop});
        std::vector<irs::ScoreDoc> hits(kTop);
        std::atomic<irs::score_t> threshold{
          std::numeric_limits<irs::score_t>::lowest()};
        irs::LoserScoreCollector collector{threshold, hits};
        top->Run(irs::doc_limits::min(), end, collector);
        out.top_total += collector.TotalMatches();
        for (size_t j = 0; j != collector.AcceptedCount(); ++j) {
          out.top.push_back(hits[j].score);
        }
      }
    }
    absl::c_sort(out.top, std::greater<>{});
    return out;
  }

 protected:
  void Open() {
    _reader = irs::DirectoryReader{_dir, irs::tests::DefaultReaderOptions()};
  }

  irs::MemoryDirectory _dir;

 private:
  irs::doc_id_t End(size_t segment) const {
    return static_cast<irs::doc_id_t>(irs::doc_limits::min() +
                                      (*_reader)[segment].docs_count());
  }

  irs::doc_id_t Global(size_t segment, irs::doc_id_t doc) const {
    irs::doc_id_t base = 0;
    for (size_t i = 0; i != segment; ++i) {
      base += static_cast<irs::doc_id_t>((*_reader)[i].docs_count());
    }
    return base + doc - irs::doc_limits::min();
  }

  template<typename Visit>
  static void ForEachBit(std::span<const uint64_t> mask, irs::doc_id_t base,
                         Visit&& visit) {
    for (size_t w = 0; w != mask.size(); ++w) {
      for (auto bits = mask[w]; bits; bits = irs::PopBit(bits)) {
        visit(base +
              static_cast<irs::doc_id_t>(w * 64 + std::countr_zero(bits)));
      }
    }
  }

  irs::DirectoryReader _reader;
};

}  // namespace tests
