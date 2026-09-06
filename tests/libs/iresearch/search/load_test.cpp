////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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
#include <absl/container/flat_hash_map.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_replace.h>
#include <absl/strings/str_split.h>
#include <fcntl.h>
#include <simdjson.h>
#include <unistd.h>
#include <zlib.h>

#include <bit>
#include <cstdio>
#include <fstream>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "basics/bit_utils.hpp"
#include "basics/files.h"
#include "basics/serializer.h"
#include "basics/simdjson_sink.h"
#include "executor.h"
#include "formats/column/test_cs_helpers.hpp"
#include "index_builder.h"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/search/bm25.hpp"
#include "iresearch/search/count/root.hpp"
#include "iresearch/search/docs/root.hpp"
#include "iresearch/search/fill/node.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/lead/node.hpp"
#include "iresearch/search/term_filter.hpp"
#include "search/filter_test_case_base.hpp"
#include "tests_shared.hpp"

namespace {

inline constexpr irs::field_id kIdId = 1;

void DecodeMask(const uint64_t* mask, size_t mask_words, irs::doc_id_t base,
                std::vector<irs::doc_id_t>& docs) {
  for (size_t w = 0; w < mask_words; ++w) {
    for (auto bits = mask[w]; bits; bits = irs::PopBit(bits)) {
      docs.push_back(
        base + static_cast<irs::doc_id_t>(w * 64 + std::countr_zero(bits)));
    }
  }
}

// Reference fill: advance one doc at a time up to `window_max`.
void FillViaAdvance(tests::LeadCursor& iter, irs::doc_id_t window_max,
                    std::vector<irs::doc_id_t>& docs) {
  auto doc = iter.Value();
  while (!irs::doc_limits::eof(doc) && doc < window_max) {
    docs.push_back(doc);
    doc = iter.Advance();
  }
}

// The fill plan of one query, walked window by window. A window is opened
// wherever the caller says, so the same plan answers a contiguous walk and a
// walk that jumps forward.
class WindowFiller {
 public:
  explicit WindowFiller(irs::fill::Node::ptr fill) noexcept
    : _fill{std::move(fill)} {}

  void Fill(irs::doc_id_t window_min, irs::doc_id_t window_max,
            std::vector<irs::doc_id_t>& docs) {
    const size_t mask_words = (window_max - window_min + 63) / 64;
    _mask.assign(mask_words, 0);
    _next = _fill->FillOr(window_min, window_max, _mask.data());
    DecodeMask(_mask.data(), mask_words, window_min, docs);
  }

  bool Eof() const noexcept { return irs::doc_limits::eof(_next); }

 private:
  irs::fill::Node::ptr _fill;
  std::vector<uint64_t> _mask;
  irs::doc_id_t _next = irs::doc_limits::invalid();
};

// Operation applied to the reference iterator before each window fill. Where
// it lands is where the next window opens, so the fill plan stays in step
// without a reposition of its own.
using BeforeWindowFunc =
  std::function<void(tests::LeadCursor& iter, irs::doc_id_t window_min)>;

BeforeWindowFunc SeekToWindow() {
  return [](tests::LeadCursor& iter, irs::doc_id_t window_min) {
    iter.Seek(window_min);
  };
}

BeforeWindowFunc AdvanceSkip(size_t count) {
  return [count](tests::LeadCursor& iter, irs::doc_id_t /*window_min*/) {
    for (size_t i = 0; i < count && !irs::doc_limits::eof(iter.Value()); ++i) {
      iter.Advance();
    }
  };
}

BeforeWindowFunc SeekSkip(irs::doc_id_t delta) {
  return [delta](tests::LeadCursor& iter, irs::doc_id_t /*window_min*/) {
    if (const auto doc = iter.Value();
        irs::doc_limits::valid(doc) && !irs::doc_limits::eof(doc)) {
      iter.Seek(doc + delta);
    }
  };
}

BeforeWindowFunc Cycle(std::vector<BeforeWindowFunc> ops, size_t num_iters) {
  return [ops = std::move(ops), num_iters, idx = size_t{0}](
           tests::LeadCursor& iter, irs::doc_id_t window_min) mutable {
    if (const auto& op = ops[(idx / num_iters) % ops.size()]) {
      op(iter, window_min);
    }
    ++idx;
  };
}

// Walk a lead plan and a fill plan window-by-window over
// [doc_limits::min(), max_doc). Before each window, `before_window` runs on
// the lead; the window then opens where the lead stands, so both sides see
// the same range. Returns total number of documents seen.
size_t CompareWindowByWindow(tests::LeadCursor& reference_iter,
                             WindowFiller& test_filler, irs::doc_id_t max_doc,
                             irs::doc_id_t window_size,
                             const BeforeWindowFunc& before_window = {}) {
  size_t total_docs = 0;
  std::vector<irs::doc_id_t> reference_docs;
  std::vector<irs::doc_id_t> test_docs;

  irs::doc_id_t window_min = irs::doc_limits::min();
  while (window_min < max_doc) {
    if (before_window) {
      before_window(reference_iter, window_min);
    }

    const auto pos = reference_iter.Value();
    if (irs::doc_limits::eof(pos)) {
      break;
    }
    if (irs::doc_limits::valid(pos) && pos > window_min) {
      window_min = pos;
    }
    if (window_min >= max_doc) {
      break;
    }
    const auto window_max = std::min(window_min + window_size, max_doc);

    reference_docs.clear();
    test_docs.clear();
    FillViaAdvance(reference_iter, window_max, reference_docs);
    test_filler.Fill(window_min, window_max, test_docs);

    if (test_docs != reference_docs) {
      std::vector<irs::doc_id_t> only_in_reference;
      std::vector<irs::doc_id_t> only_in_test;
      absl::c_set_difference(reference_docs, test_docs,
                             std::back_inserter(only_in_reference));
      absl::c_set_difference(test_docs, reference_docs,
                             std::back_inserter(only_in_test));
      auto format = [](const std::vector<irs::doc_id_t>& v) {
        return absl::StrCat("{", absl::StrJoin(v, ", "), "}");
      };
      ADD_FAILURE() << "window [" << window_min << ", " << window_max << "): "
                    << "reference=" << reference_docs.size()
                    << " test=" << test_docs.size()
                    << " only_in_reference=" << format(only_in_reference)
                    << " only_in_test=" << format(only_in_test);
    }
    total_docs += reference_docs.size();

    const bool reference_eof = irs::doc_limits::eof(reference_iter.Value());
    if (reference_eof || test_filler.Eof()) {
      EXPECT_EQ(reference_eof, test_filler.Eof())
        << "EOF disagreement at window [" << window_min << ", " << window_max
        << ")";
      break;
    }
    window_min = window_max;
  }

  return total_docs;
}

// The batched emit plan against the same query's lead plan: same documents,
// same order, whatever capacity the batches are drained at.
size_t CompareEmitDocs(tests::LeadCursor& reference_iter, irs::docs::Root& root,
                       uint32_t capacity) {
  std::vector<irs::doc_id_t> reference_docs;
  while (!irs::doc_limits::eof(reference_iter.Advance())) {
    reference_docs.push_back(reference_iter.Value());
  }

  std::vector<irs::doc_id_t> test_docs;
  std::vector<irs::doc_id_t> buf(capacity + irs::doc_limits::kDocsSlack);
  for (;;) {
    const auto n = root.Run(buf.data(), capacity);
    if (n == 0) {
      break;
    }
    test_docs.insert(test_docs.end(), buf.begin(), buf.begin() + n);
  }

  EXPECT_EQ(reference_docs, test_docs);
  return reference_docs.size();
}

struct IteratorFactory {
  std::string name;
  std::function<irs::QueryBuilder::ptr(const irs::SubReader&)> prepare;
};

IteratorFactory QueryIterator(bench::Executor& executor,
                              std::string_view query) {
  return {
    .name = std::string{query},
    .prepare =
      [&executor, query = std::string{query}](const irs::SubReader& segment) {
        auto filter = executor.ParseFilter(query, false);
        SDB_ASSERT(filter);
        return filter->PrepareSegment(segment, {});
      },
  };
}

template<typename Body>
void ForEachCombination(const irs::DirectoryReader& reader,
                        const std::vector<IteratorFactory>& factories,
                        std::span<const irs::doc_id_t> window_sizes,
                        Body&& body) {
  size_t segment_idx = 0;
  for (const auto& segment : reader) {
    SCOPED_TRACE(testing::Message() << "segment=" << segment_idx);
    const auto max_doc =
      static_cast<irs::doc_id_t>(segment.docs_count() + irs::doc_limits::min());

    for (const auto& factory : factories) {
      SCOPED_TRACE(testing::Message() << "query=\"" << factory.name << "\"");

      for (auto window_size : window_sizes) {
        SCOPED_TRACE(testing::Message() << "window_size=" << window_size);
        body(reader, segment, factory, max_doc, window_size);
      }
    }
    ++segment_idx;
  }
}

void TestAdvanceVsFillBlock(const irs::DirectoryReader& reader,
                            const std::vector<IteratorFactory>& factories,
                            std::span<const irs::doc_id_t> window_sizes) {
  ForEachCombination(
    reader, factories, window_sizes,
    [](const auto& /*reader*/, const auto& segment, const auto& factory,
       auto max_doc, auto window_size) {
      auto query = factory.prepare(segment);
      ASSERT_NE(nullptr, query);
      auto reference_iter = query->PlanLead({});
      ASSERT_NE(nullptr, reference_iter);
      auto fill = query->PlanFill({}, irs::ScoreMergeType::Noop);
      ASSERT_NE(nullptr, fill);
      auto count = query->PlanCount({});
      ASSERT_NE(nullptr, count);
      WindowFiller filler{std::move(fill)};

      reference_iter->Advance();

      auto total =
        CompareWindowByWindow(*reference_iter, filler, max_doc, window_size);

      EXPECT_GT(total, 0u) << "query should have matches";
      EXPECT_EQ(total, count->Run()) << "total docs vs count mismatch";
    });
}

void TestAdvanceVsEmitDocs(const irs::DirectoryReader& reader,
                           const std::vector<IteratorFactory>& factories,
                           std::span<const irs::doc_id_t> window_sizes) {
  ForEachCombination(
    reader, factories, window_sizes,
    [](const auto& /*reader*/, const auto& segment, const auto& factory,
       auto /*max_doc*/, auto window_size) {
      auto query = factory.prepare(segment);
      ASSERT_NE(nullptr, query);
      auto reference_iter = query->PlanLead({});
      ASSERT_NE(nullptr, reference_iter);
      auto emit = query->PlanDocs({});
      ASSERT_NE(nullptr, emit);
      auto count = query->PlanCount({});
      ASSERT_NE(nullptr, count);

      const auto capacity = std::max<uint32_t>(
        static_cast<uint32_t>(window_size), irs::doc_limits::kMinCapacity);
      auto total = CompareEmitDocs(*reference_iter, *emit, capacity);

      EXPECT_GT(total, 0u) << "query should have matches";
      EXPECT_EQ(total, count->Run()) << "total docs vs count mismatch";
    });
}

void TestSeekVsFillBlock(const irs::DirectoryReader& reader,
                         const std::vector<IteratorFactory>& factories,
                         std::span<const irs::doc_id_t> window_sizes) {
  ForEachCombination(reader, factories, window_sizes,
                     [](const auto& /*reader*/, const auto& segment,
                        const auto& factory, auto max_doc, auto window_size) {
                       auto query = factory.prepare(segment);
                       ASSERT_NE(nullptr, query);
                       auto reference_iter = query->PlanLead({});
                       ASSERT_NE(nullptr, reference_iter);
                       auto fill =
                         query->PlanFill({}, irs::ScoreMergeType::Noop);
                       ASSERT_NE(nullptr, fill);
                       WindowFiller filler{std::move(fill)};

                       CompareWindowByWindow(*reference_iter, filler, max_doc,
                                             window_size, SeekToWindow());
                     });
}

void TestInterleavedSeekFillBlock(const irs::DirectoryReader& reader,
                                  const std::vector<IteratorFactory>& factories,
                                  std::span<const irs::doc_id_t> window_sizes) {
  ForEachCombination(
    reader, factories, window_sizes,
    [](const auto& /*reader*/, const auto& segment, const auto& factory,
       auto max_doc, auto window_size) {
      auto query = factory.prepare(segment);
      ASSERT_NE(nullptr, query);
      auto reference_iter = query->PlanLead({});
      ASSERT_NE(nullptr, reference_iter);
      auto fill = query->PlanFill({}, irs::ScoreMergeType::Noop);
      ASSERT_NE(nullptr, fill);
      WindowFiller filler{std::move(fill)};

      reference_iter->Advance();

      BeforeWindowFunc noop;
      CompareWindowByWindow(*reference_iter, filler, max_doc, window_size,
                            Cycle({noop, SeekToWindow()}, 2));
    });
}

void TestAdvanceSkipFillBlock(const irs::DirectoryReader& reader,
                              const std::vector<IteratorFactory>& factories,
                              std::span<const irs::doc_id_t> window_sizes,
                              std::span<const size_t> skip_counts) {
  ForEachCombination(
    reader, factories, window_sizes,
    [&skip_counts](const auto& /*reader*/, const auto& segment,
                   const auto& factory, auto max_doc, auto window_size) {
      for (auto skip : skip_counts) {
        SCOPED_TRACE(testing::Message() << "advance_skip=" << skip);

        auto query = factory.prepare(segment);
        ASSERT_NE(nullptr, query);
        auto reference_iter = query->PlanLead({});
        ASSERT_NE(nullptr, reference_iter);
        auto fill = query->PlanFill({}, irs::ScoreMergeType::Noop);
        ASSERT_NE(nullptr, fill);
        WindowFiller filler{std::move(fill)};

        reference_iter->Advance();

        CompareWindowByWindow(*reference_iter, filler, max_doc, window_size,
                              AdvanceSkip(skip));
      }
    });
}

void TestSeekSkipFillBlock(const irs::DirectoryReader& reader,
                           const std::vector<IteratorFactory>& factories,
                           std::span<const irs::doc_id_t> window_sizes,
                           std::span<const irs::doc_id_t> skip_deltas) {
  ForEachCombination(
    reader, factories, window_sizes,
    [&skip_deltas](const auto& /*reader*/, const auto& segment,
                   const auto& factory, auto max_doc, auto window_size) {
      for (auto delta : skip_deltas) {
        SCOPED_TRACE(testing::Message() << "seek_skip=" << delta);

        auto query = factory.prepare(segment);
        ASSERT_NE(nullptr, query);
        auto reference_iter = query->PlanLead({});
        ASSERT_NE(nullptr, reference_iter);
        auto fill = query->PlanFill({}, irs::ScoreMergeType::Noop);
        ASSERT_NE(nullptr, fill);
        WindowFiller filler{std::move(fill)};

        reference_iter->Advance();

        CompareWindowByWindow(*reference_iter, filler, max_doc, window_size,
                              SeekSkip(delta));
      }
    });
}

enum class ResultType {
  Raw,
  Hash,
};

struct QueryResult {
  std::string query;
  uint64_t count{0};
  uint64_t top_100{0};
  ResultType result_type{ResultType::Raw};
  std::vector<std::string> top_100_result;
  std::vector<std::string> top_100_count_result;
};

struct ParsedQuery {
  std::string query;
  std::vector<std::string> tags;
};

constexpr std::string_view kSkipTopK = "skip:topk";

std::string HashIds(const std::vector<std::string>& ids) {
  sdb::Sha256Functor sha;
  for (const auto& id : ids) {
    sha(id.data(), id.size());
    sha("\n", 1);
  }
  return sha.Finalize();
}

void HashIdsInPlace(std::vector<std::string>& ids) {
  if (!ids.empty()) {
    ids = {HashIds(ids)};
  }
}

void HashResults(std::vector<QueryResult>& results) {
  for (auto& r : results) {
    if (r.result_type == ResultType::Hash) {
      continue;
    }
    HashIdsInPlace(r.top_100_result);
    HashIdsInPlace(r.top_100_count_result);
    r.result_type = ResultType::Hash;
  }
}

std::string SerializeResults(const std::vector<QueryResult>& results) {
  // Drive the templated reflection-based writer through `sdb::basics::JsonSink`
  // (simdjson::builder), emitting JSON text directly without an intermediate
  // builder + slice round-trip.
  simdjson::builder::string_builder sb(1024);
  {
    sdb::basics::JsonSink sink{sb};
    sdb::basics::WriteObject(sink, results);
  }
  std::string_view body;
  if (sb.view().get(body) != simdjson::SUCCESS) {
    return {};
  }
  return std::string{body};
}

std::vector<QueryResult> DeserializeResults(std::string_view json_str) {
  // Mirror SerializeResults' simdjson path on the read side: parse JSON via
  // simdjson::ondemand and feed it through `sdb::basics::JsonSource` + the
  // reflection reader.
  simdjson::padded_string padded{json_str};
  simdjson::ondemand::parser parser;
  simdjson::ondemand::document doc;
  auto err = parser.iterate(padded).get(doc);
  if (err != simdjson::SUCCESS) {
    return {};
  }
  std::vector<QueryResult> results;
  sdb::basics::JsonSource source{doc};
  sdb::basics::ReadObject(source, results);
  return results;
}

std::vector<ParsedQuery> LoadQueries(const std::filesystem::path& path) {
  std::vector<ParsedQuery> queries;
  std::ifstream file{path};
  std::string line;
  simdjson::ondemand::parser parser;
  while (std::getline(file, line)) {
    if (line.empty()) {
      continue;
    }
    simdjson::padded_string padded{line};
    auto doc{parser.iterate(padded)};
    ParsedQuery q;
    q.query = std::string{std::string_view{doc["query"]}};
    if (q.query.empty()) {
      ADD_FAILURE() << "Empty query at line " << queries.size();
      continue;
    }
    for (auto tag : doc["tags"].get_array()) {
      q.tags.emplace_back(std::string_view{tag});
    }
    queries.push_back(std::move(q));
  }
  return queries;
}

struct StoredIdBatchHandler : bench::IBatchHandler {
  bench::Document doc;

  void operator()(std::vector<std::string>& buf,
                  irs::IndexWriter::Transaction& ctx) override {
    for (auto& line : buf) {
      doc.Fill(line);
      auto trx = ctx.Insert();
      trx.Insert(doc.fields[0]);
      irs::tests::StoreFieldAt(*trx.GetColWriter(), kIdId, trx.DocId(),
                               doc.fields[0]);
      trx.Insert(doc.fields[1]);
    }
  }
};

void BuildIndex(const std::string& corpus_path,
                const std::filesystem::path& index_dir,
                const bench::BenchConfig& config) {
  bench::IndexBuilderOptions builder_options{
    .batch_size = 100000,
    .indexer_threads = 1,
    .refresh_interval_ms = 0,
    .compaction_interval_ms = 5000,
    .compaction_threads = 0,
    .compact_all = true,
  };

  bench::IndexBuilder builder{index_dir.string(), builder_options, config};

  std::ifstream file{corpus_path};
  ASSERT_TRUE(file.is_open()) << "Cannot open corpus: " << corpus_path;

  builder.IndexFromStream(file, [] -> std::unique_ptr<bench::IBatchHandler> {
    return std::make_unique<StoredIdBatchHandler>();
  });
}

std::vector<std::string> BuildDocIdMap(const irs::DirectoryReader& reader) {
  std::vector<std::string> id_map(reader.docs_count());
  uint64_t base = 0;
  for (auto& segment : reader) {
    const auto* column = segment.Column(kIdId);
    EXPECT_NE(column, nullptr) << "'id' column not found";
    if (!column) {
      return {};
    }

    irs::tests::VisitBlobColumn(
      *segment.GetColReader(), *column,
      [&](irs::doc_id_t doc, irs::bytes_view payload) {
        auto idx = base + doc - irs::doc_limits::min();
        EXPECT_LT(idx, id_map.size()) << "doc_id out of range";
        id_map[idx] = irs::ToString<std::string>(payload.data());
        return true;
      });
    base += segment.docs_count();
  }
  return id_map;
}

std::vector<QueryResult> ExecuteAllQueries(
  bench::Executor& executor, const std::vector<ParsedQuery>& queries,
  const std::vector<std::string>& id_map, bool include_scores) {
  constexpr size_t kTopK = 100;

  auto collect_ids = [&](std::string_view label, std::span<irs::ScoreDoc> hits,
                         std::vector<std::string>& out) {
    absl::c_sort(hits, [](const irs::ScoreDoc& a, const irs::ScoreDoc& b) {
      return std::tie(b.score, a.doc) < std::tie(a.score, b.doc);
    });
    for (const auto& [score, doc_id, segment_idx] : hits) {
      auto idx = doc_id - irs::doc_limits::min();
      if (idx >= id_map.size()) {
        ADD_FAILURE() << label << " doc_id=" << doc_id << " score=" << score
                      << " out of range";
        continue;
      }
      if (include_scores) {
        out.emplace_back(absl::StrCat(id_map[idx], " ", score));
      } else {
        out.emplace_back(id_map[idx]);
      }
    }
  };

  std::vector<QueryResult> results;
  results.reserve(queries.size());

  for (size_t i = 0; i < queries.size(); ++i) {
    const auto& q = queries[i];
    SCOPED_TRACE(testing::Message()
                 << "query[" << i << "] \"" << q.query << "\"");
    QueryResult r;
    r.query = q.query;
    r.result_type = ResultType::Raw;
    r.count = executor.ExecuteCount(q.query);
    EXPECT_GT(r.count, 0) << "COUNT returned 0";
    if (absl::c_linear_search(q.tags, kSkipTopK)) {
      results.emplace_back(std::move(r));
      continue;
    }
    r.top_100 = executor.ExecuteTopK(kTopK, q.query);
    SCOPED_TRACE(testing::Message()
                 << "count=" << r.count << " top_100=" << r.top_100
                 << " results=" << executor.GetResults().size());
    collect_ids("TOP_100", executor.GetResults(), r.top_100_result);

    auto top_100_count = executor.ExecuteTopKWithCount(kTopK, q.query);
    EXPECT_EQ(r.count, top_100_count) << "TOP_100_COUNT differs from COUNT";
    SCOPED_TRACE(testing::Message()
                 << "top_100_count=" << top_100_count
                 << " results=" << executor.GetResults().size());
    collect_ids("TOP_100_COUNT", executor.GetResults(), r.top_100_count_result);

    results.emplace_back(std::move(r));
  }
  return results;
}

constexpr std::string_view kQueries[] = {
  // term
  "the",
  "university",
  "washington",
  "summit",
  // conjunction
  "+griffith +observatory",
  "+plus +size +clothing",
  // phrase
  "\"griffith observatory\"",
  "\"french culinary institute\"",
  // union (disjunction)
  "bowel obstruction",
  "wisconsin attorney general",
  // negation
  "+the english -restoration",
};
constexpr irs::doc_id_t kWindowSizes[] = {64, 128, 256, 4096};
constexpr size_t kAdvanceSkips[] = {1, 5, 128};
constexpr irs::doc_id_t kSeekSkips[] = {10, 100, 1000};

std::vector<IteratorFactory> MakeFactories(bench::Executor& executor) {
  std::vector<IteratorFactory> factories;
  factories.reserve(std::size(kQueries));
  for (auto query : kQueries) {
    factories.push_back(QueryIterator(executor, query));
  }
  return factories;
}

}  // namespace

class LoadTest : public TestBase {
 protected:
  enum class Mode {
    Validate,
    GenerateHash,
    GenerateJson,
  };

  static void SetUpTestSuite() {
    if (const char* v = std::getenv("CORPUS_PATH"); v != nullptr) {
      gCorpusPath = v;
    } else {
      GTEST_SKIP() << "CORPUS_PATH not set";
    }
    if (const char* gen = std::getenv("GENERATE_REFERENCE"); gen != nullptr) {
      gMode = std::string_view{gen} == "json" ? Mode::GenerateJson
                                              : Mode::GenerateHash;
    }
    if (const char* gzip = std::getenv("GENERATE_GZIP"); gzip != nullptr) {
      gGzip = std::string_view{gzip} != "0";
    }
    if (!std::filesystem::exists(gCorpusPath)) {
      GTEST_SKIP() << "Path does not exist: " << gCorpusPath;
    }

    std::string index_dir;
    if (std::filesystem::is_directory(gCorpusPath.c_str())) {
      index_dir = gCorpusPath;
      gDropIndex = false;
    } else {
      gIndexDir = test_results_dir() / "LoadTest_index";
      std::filesystem::create_directories(gIndexDir);
      BuildIndex(gCorpusPath, gIndexDir, gConfig);
      index_dir = gIndexDir.string();
      std::cout << absl::StrCat("Index directory: ", index_dir, "\n");
    }

    if (const char* drop_index = std::getenv("DROP_INDEX");
        drop_index != nullptr) {
      gDropIndex = std::string_view{drop_index} != "0";
    }

    gExecutor = std::make_unique<bench::Executor>(index_dir, gConfig);
    const auto& reader = gExecutor->GetReader();
    ASSERT_GT(reader.size(), 0);
    gIdMap = BuildDocIdMap(reader);
  }

  static void TearDownTestSuite() {
    gExecutor.reset();
    if (gDropIndex && !gIndexDir.empty()) {
      std::filesystem::remove_all(gIndexDir);
    }
  }

  void SetUp() override {
    TestBase::SetUp();
    _null_sink = std::fopen("/dev/null", "w");
    ASSERT_NE(nullptr, _null_sink);
  }

  void TearDown() override {
    if (gExecutor) {
      gExecutor->SetPrintSink(stderr);
    }
    if (_null_sink) {
      std::fclose(_null_sink);
      _null_sink = nullptr;
    }
    TestBase::TearDown();
  }

  void RunAndValidate(std::string_view name) {
    const auto res_dir = resource("iresearch-load") / name;
    const auto queries_path = res_dir / "queries.json";
    ASSERT_TRUE(std::filesystem::exists(queries_path))
      << "Queries file not found: " << queries_path;

    auto queries = LoadQueries(queries_path);
    ASSERT_FALSE(queries.empty()) << "No queries loaded from " << queries_path;

    auto results = ExecuteAllQueries(*gExecutor, queries, gIdMap,
                                     gMode == Mode::GenerateJson);
    ASSERT_FALSE(results.empty()) << "No query results produced";

    if (gMode == Mode::GenerateHash) {
      HashResults(results);
    }
    auto json_output = SerializeResults(results);

    if (gGzip) {
      std::filesystem::create_directories(res_dir);
      auto gz_path = (res_dir / "reference.json.gz").string();
      gzFile gz = gzopen(gz_path.c_str(), "wb");
      ASSERT_NE(gz, nullptr) << "Cannot write: " << gz_path;
      gzwrite(gz, json_output.data(), json_output.size());
      gzclose(gz);
      std::cout << absl::StrCat("Gzip written to \"", gz_path, "\"\n");
    }

    if (gMode != Mode::Validate) {
      std::filesystem::create_directories(res_dir);
      auto json_path = (res_dir / "reference.json").string();
      std::ofstream out{json_path};
      ASSERT_TRUE(out.is_open()) << "Cannot write: " << json_path;
      out << json_output;
      std::cout << absl::StrCat("JSON written to \"", json_path, "\" (",
                                results.size(), " queries)\n");
      return;
    }

    auto json_path = res_dir / "reference.json";
    std::string ref_str;
    auto gz_path = res_dir / "reference.json.gz";
    if (std::filesystem::exists(gz_path)) {
      ASSERT_TRUE(sdb::SlurpGzipFile(gz_path.c_str(), ref_str))
        << "Cannot read: " << gz_path;
    } else {
      auto raw_path = res_dir / "reference.json";
      ASSERT_TRUE(sdb::SlurpFile(raw_path.c_str(), ref_str))
        << "Reference file not found: " << raw_path;
    }
    auto expected = DeserializeResults(ref_str);

    ASSERT_EQ(results.size(), expected.size()) << "Query count mismatch";

    for (size_t i = 0; i < results.size(); ++i) {
      auto& a = results[i];
      auto& e = expected[i];

      ASSERT_EQ(a.query, e.query) << "Query text mismatch at index " << i;

      EXPECT_EQ(a.count, e.count)
        << "COUNT mismatch for query[" << i << "] \"" << a.query << "\"";

      EXPECT_EQ(a.top_100, e.top_100)
        << "TOP_100 mismatch for query[" << i << "] \"" << a.query << "\"";

      {
        auto pruned = a.top_100_result;
        auto exact = a.top_100_count_result;
        absl::c_sort(pruned);
        absl::c_sort(exact);
        EXPECT_EQ(pruned, exact)
          << "TOP_100 vs TOP_100_COUNT documents differ for query[" << i
          << "] \"" << a.query << "\"";
      }

      // Hash test results if reference is hashed; compare raw otherwise
      if (e.result_type == ResultType::Hash) {
        if (a.result_type == ResultType::Raw) {
          HashIdsInPlace(a.top_100_result);
          HashIdsInPlace(a.top_100_count_result);
        }
      }

      ASSERT_EQ(a.top_100_result.size(), e.top_100_result.size())
        << "TOP_100 id count mismatch for query[" << i << "] \"" << a.query
        << "\"";

      // Raw reference may include scores
      auto strip_score = [&](std::string_view s) -> std::string_view {
        if (e.result_type != ResultType::Raw) {
          return s;
        }
        auto pos = s.rfind(' ');
        return pos != std::string_view::npos ? s.substr(0, pos) : s;
      };

      for (size_t j = 0; j < a.top_100_result.size(); ++j) {
        auto expected_id = strip_score(e.top_100_result[j]);
        EXPECT_EQ(a.top_100_result[j], expected_id)
          << "TOP_100 id[" << j << "] mismatch for query[" << i << "] \""
          << a.query << "\"";
      }

      ASSERT_EQ(a.top_100_count_result.size(), e.top_100_count_result.size())
        << "TOP_100_COUNT id count mismatch for query[" << i << "] \""
        << a.query << "\"";

      for (size_t j = 0; j < a.top_100_count_result.size(); ++j) {
        auto expected_id = strip_score(e.top_100_count_result[j]);
        EXPECT_EQ(a.top_100_count_result[j], expected_id)
          << "TOP_100_COUNT id[" << j << "] mismatch for query[" << i << "] \""
          << a.query << "\"";
      }
    }
  }

  static inline bench::BenchConfig gConfig;
  static inline std::string gCorpusPath;
  static inline std::filesystem::path gIndexDir;
  static inline std::unique_ptr<bench::Executor> gExecutor;
  static inline std::vector<std::string> gIdMap;
  static inline Mode gMode = Mode::Validate;
  static inline bool gGzip = false;
  static inline bool gDropIndex = true;

  std::FILE* _null_sink = nullptr;
};

TEST_F(LoadTest, WikiSmall) { RunAndValidate("wiki_small"); }

// Scores a document from first principles: the sum of the contributions of the
// query terms that document contains. Holds one iterator per term and seeks
// them forward, so a query costs `terms` iterators and nothing corpus-sized --
// "the" matches 4M documents and still only the documents under test are ever
// scored. Documents must be requested in ascending order per segment.
class TermScoreOracle {
 public:
  TermScoreOracle(const irs::IndexReader& reader, const irs::Scorer& scorer,
                  std::span<const std::string> terms, size_t segment_idx,
                  const irs::SubReader& /*segment*/) {
    _terms.reserve(terms.size());
    for (const auto& term : terms) {
      auto& t = _terms.emplace_back();
      *t.filter.mutable_field_id() = bench::kTextFieldId;
      t.filter.mutable_options()->term =
        irs::ViewCast<irs::byte_type>(std::string_view{term});
      t.prepared =
        std::make_unique<tests::PreparedFilter>(t.filter, reader, &scorer);
      t.it = t.prepared->ExecuteScored(segment_idx, t.fetcher);
      if (!t.it) {
        continue;
      }
      t.score = t.it->PrepareScore();
    }
  }

  irs::score_t ScoreOf(irs::doc_id_t doc) {
    irs::score_t total = 0;
    for (auto& t : _terms) {
      if (!t.it || irs::doc_limits::eof(t.it->Value())) {
        continue;
      }
      if (t.it->Value() < doc && t.it->Seek(doc) != doc) {
        continue;
      }
      if (t.it->Value() != doc) {
        continue;
      }
      t.fetcher.Fetch(doc);
      t.it->FetchScoreArgs(0);
      total += t.score.Score();
    }
    return total;
  }

 private:
  struct Term {
    irs::ByTerm filter;
    std::unique_ptr<tests::PreparedFilter> prepared;
    irs::ColumnArgsFetcher fetcher;
    std::unique_ptr<tests::LeadCursor> it;
    irs::ScoreFunction score;
  };

  std::vector<Term> _terms;
};

// Every scored document must carry the sum of the contributions of the query
// terms it contains -- whatever the query's shape and however the engine
// reached it. Checked across query shapes (AND / OR / MUST+SHOULD / negated)
// and across the three retrieval modes, because they drive the iterators
// differently: TOP_100_COUNT walks every match, TOP_100 prunes, and the
// pruned path reaches a scored disjunction through `seek` rather than by
// advancing it -- the access path that scored documents with whatever score
// arguments happened to be fetched last.
TEST_F(LoadTest, ScoreAccuracyAcrossQueryShapes) {
  constexpr size_t kTopK = 100;

  const auto& reader = gExecutor->GetReader();
  auto scorer_ptr = irs::BM25::Make(irs::BM25::Options{});
  ASSERT_TRUE(scorer_ptr);
  const auto& scorer = *scorer_ptr;

  auto queries =
    LoadQueries(resource("iresearch-load") / "wiki_small" / "queries.json");
  ASSERT_FALSE(queries.empty());

  // Terms that can contribute score: plain and `+`, never `-` (excluded
  // documents contribute nothing).
  const auto scoring_terms = [](std::string_view query) {
    std::vector<std::string> out;
    for (auto part : absl::StrSplit(query, ' ', absl::SkipEmpty())) {
      if (part.starts_with('-')) {
        continue;
      }
      part.remove_prefix(part.starts_with('+') ? 1 : 0);
      out.emplace_back(part);
    }
    return out;
  };

  // The oracle sums what each term of a plain disjunction contributes, so it
  // models nothing positional (a phrase), nothing that scores a chosen subset
  // of its terms (a minimum match), nothing weighted (a boost), and nothing
  // that stands for terms it cannot name (a wildcard, a fuzziness, an
  // interval function).
  const auto skippable = [](std::string_view q) {
    return q.find_first_of("\"()@*?~^[]{}<>/") != std::string_view::npos ||
           q.find("fn:") != std::string_view::npos;
  };

  size_t eligible_queries = 0;
  size_t checked_queries = 0;

  for (const auto& q : queries) {
    if (skippable(q.query)) {
      continue;
    }
    const auto terms = scoring_terms(q.query);
    if (terms.empty()) {
      continue;
    }
    ++eligible_queries;

    for (const bool pruned : {true, false}) {
      SCOPED_TRACE(testing::Message()
                   << "query=\"" << q.query
                   << "\" mode=" << (pruned ? "TOP_100" : "TOP_100_COUNT"));

      if (pruned) {
        gExecutor->ExecuteTopK(kTopK, q.query);
      } else {
        gExecutor->ExecuteTopKWithCount(kTopK, q.query);
      }

      // Group by segment and sort by doc: the oracle seeks forward only.
      // Every query in the set matches something, so an empty result is itself
      // a failure rather than a reason to check nothing.
      std::vector<irs::ScoreDoc> hits{gExecutor->GetResults().begin(),
                                      gExecutor->GetResults().end()};
      ASSERT_FALSE(hits.empty()) << "query returned no documents";
      absl::c_sort(hits, [](const irs::ScoreDoc& a, const irs::ScoreDoc& b) {
        return std::tie(a.segment_idx, a.doc) < std::tie(b.segment_idx, b.doc);
      });

      size_t segment_idx = 0;
      std::optional<TermScoreOracle> oracle;
      uint32_t oracle_segment = std::numeric_limits<uint32_t>::max();

      for (const auto& [score, doc, segment] : hits) {
        if (segment != oracle_segment) {
          segment_idx = 0;
          for (auto& sub : reader) {
            if (segment_idx == segment) {
              oracle.emplace(reader, scorer, terms, segment_idx, sub);
              break;
            }
            ++segment_idx;
          }
          oracle_segment = segment;
        }
        ASSERT_TRUE(oracle.has_value());

        const auto expected = oracle->ScoreOf(doc);
        EXPECT_FLOAT_EQ(score, expected)
          << "segment=" << segment << " doc=" << doc;
      }
      ++checked_queries;
    }
  }

  EXPECT_EQ(eligible_queries, 802);
  EXPECT_EQ(checked_queries, 802 * 2);
}

TEST_F(LoadTest, DisjunctionScoreAccuracy) {
  const auto& reader = gExecutor->GetReader();
  auto scorer_ptr = irs::BM25::Make(irs::BM25::Options{});
  ASSERT_TRUE(scorer_ptr);
  const auto& scorer = *scorer_ptr;

  struct QueryCase {
    std::string_view query;
    std::vector<std::string_view> terms;
  };

  const QueryCase cases[] = {
    {"griffith observatory", {"griffith", "observatory"}},
    {"who dares wins", {"who", "dares", "wins"}},
    {"ellen degeneres show", {"ellen", "degeneres", "show"}},
  };

  for (const auto& [query, terms] : cases) {
    SCOPED_TRACE(query);

    std::map<irs::doc_id_t, irs::score_t> reference_scores;

    for (size_t segment_idx = 0; [[maybe_unused]] auto& segment : reader) {
      for (auto term_str : terms) {
        irs::ByTerm filter;
        *filter.mutable_field_id() = bench::kTextFieldId;
        filter.mutable_options()->term =
          irs::ViewCast<irs::byte_type>(irs::bytes_view{
            reinterpret_cast<const irs::byte_type*>(term_str.data()),
            term_str.size()});
        tests::PreparedFilter prepared{filter, reader, &scorer};

        irs::ColumnArgsFetcher fetcher;
        auto it = prepared.ExecuteScored(segment_idx, fetcher);
        ASSERT_TRUE(it);

        auto score_func = it->PrepareScore();
        EXPECT_FALSE(score_func.IsDefault())
          << "Score function is default for term: " << term_str;

        for (auto doc = it->Advance(); !irs::doc_limits::eof(doc);
             doc = it->Advance()) {
          fetcher.Fetch(doc);
          it->FetchScoreArgs(0);
          irs::score_t s = score_func.Score();
          reference_scores[doc] += s;
        }
      }
      ++segment_idx;
    }
    ASSERT_GT(reference_scores.size(), 0u) << "No reference docs found";

    auto filter = gExecutor->ParseFilter(std::string{query}, true);
    ASSERT_TRUE(filter);

    tests::PreparedFilter prepared{*filter, reader, &scorer};

    // 1) Compare via advance + Score
    {
      std::map<irs::doc_id_t, irs::score_t> bd_scores;
      for (size_t i = 0; [[maybe_unused]] auto& segment : reader) {
        irs::ColumnArgsFetcher fetcher;
        auto it = prepared.ExecuteScored(i, fetcher);
        auto score_func = it->PrepareScore();

        for (auto doc = it->Advance(); !irs::doc_limits::eof(doc);
             doc = it->Advance()) {
          fetcher.Fetch(doc);
          it->FetchScoreArgs(0);
          irs::score_t s = score_func.Score();
          bd_scores[doc] = s;
        }
        ++i;
      }

      EXPECT_EQ(bd_scores.size(), reference_scores.size())
        << "advance: doc count mismatch";

      for (auto& [doc, ref_score] : reference_scores) {
        auto it = bd_scores.find(doc);
        ASSERT_NE(it, bd_scores.end())
          << "advance: ref doc " << doc << " missing from BD";
        EXPECT_FLOAT_EQ(it->second, ref_score)
          << "advance: score mismatch doc " << doc;
      }
    }

    auto cmp = [](const auto& a, const auto& b) {
      return std::tie(b.score, a.doc) < std::tie(a.score, b.doc);
    };

    std::vector<irs::ScoreDoc> ref_top;
    ref_top.reserve(reference_scores.size());
    for (auto& [doc, score] : reference_scores) {
      ref_top.emplace_back(score, doc);
    }
    absl::c_sort(ref_top, cmp);

    // 2) Compare via Collect (ExecuteTopKWithCount)
    {
      static constexpr size_t kCount = 100;
      std::vector<irs::ScoreDoc> hits(kCount);
      const auto count = irs::ExecuteTopK(reader, *filter, scorer, kCount,
                                          false, std::span{hits});

      EXPECT_EQ(count, reference_scores.size()) << "Collect: count mismatch";

      absl::c_sort(hits, cmp);

      const size_t result_count = std::min<size_t>(kCount, count);

      for (size_t i = 0; i < result_count; ++i) {
        EXPECT_EQ(hits[i].doc, ref_top[i].doc)
          << "Collect: rank " << i << " doc mismatch";
        EXPECT_FLOAT_EQ(hits[i].score, ref_top[i].score)
          << "Collect: rank " << i << " score mismatch doc " << hits[i].doc;
      }
    }

    // 3) Compare via ExecuteTopK (score pruning path)
    // Scores may differ at ULP level due to different FP accumulation order.
    {
      static constexpr size_t kCount = 100;
      std::vector<irs::ScoreDoc> hits(kCount);
      irs::ExecuteTopK(reader, *filter, scorer, kCount, true, std::span{hits});

      absl::c_sort(hits, cmp);

      const size_t result_count = std::min<size_t>(kCount, ref_top.size());

      for (size_t i = 0; i < result_count; ++i) {
        EXPECT_FLOAT_EQ(hits[i].score, ref_top[i].score)
          << "Pruned: rank " << i << " score mismatch doc " << hits[i].doc;
      }
    }
  }
}

// Asking for a report must not change which documents there are: a report is a
// way of looking, not a different query. Printing stays off here -- it is a
// line per matching document, thousands of them over the query set.
TEST_F(LoadTest, ReportIsOnlyAWayOfLooking) {
  ASSERT_NE(nullptr, gExecutor);

  // Every hit printed, over this corpus, is tens of millions of lines in a CI
  // log. These assertions are about the documents, never the text.
  gExecutor->SetPrintSink(_null_sink);

  for (auto query : kQueries) {
    SCOPED_TRACE(query);

    const auto docs = gExecutor->ExecuteEmitDocs(query, {});
    const auto docs_hash = gExecutor->ExecuteEmitDocs(query, {.hash = true});
    const auto docs_both =
      gExecutor->ExecuteEmitDocs(query, {.hash = true, .print = false});
    EXPECT_EQ(docs.count, docs_hash.count);
    EXPECT_EQ(docs.count, docs_both.count);
    EXPECT_EQ(docs_hash.hash, docs_both.hash);
    EXPECT_NE(0, docs.count);

    const auto scored = gExecutor->ExecuteEmitScoredDocs(query, {});
    const auto scored_hash =
      gExecutor->ExecuteEmitScoredDocs(query, {.hash = true});
    const auto scored_both =
      gExecutor->ExecuteEmitScoredDocs(query, {.hash = true, .print = false});
    EXPECT_EQ(scored.count, scored_hash.count);
    EXPECT_EQ(scored.count, scored_both.count);
    EXPECT_EQ(scored_hash.hash, scored_both.hash);

    // the same documents, however they were asked for
    EXPECT_EQ(docs.count, scored.count);
  }
}

TEST_F(LoadTest, TopKPrunedAndExactAgree) {
  ASSERT_NE(nullptr, gExecutor);

  for (auto query : kQueries) {
    SCOPED_TRACE(query);

    gExecutor->ExecuteTopK(100, query);
    const auto pruned = gExecutor->HashResults();
    gExecutor->ExecuteTopKWithCount(100, query);
    EXPECT_EQ(pruned, gExecutor->HashResults());
  }
}

TEST_F(LoadTest, AdvanceVsFillBlock) {
  auto factories = MakeFactories(*gExecutor);
  TestAdvanceVsFillBlock(gExecutor->GetReader(), factories, kWindowSizes);
}

TEST_F(LoadTest, AdvanceVsEmitDocs) {
  auto factories = MakeFactories(*gExecutor);
  TestAdvanceVsEmitDocs(gExecutor->GetReader(), factories, kWindowSizes);
}

TEST_F(LoadTest, SeekVsFillBlock) {
  auto factories = MakeFactories(*gExecutor);
  TestSeekVsFillBlock(gExecutor->GetReader(), factories, kWindowSizes);
}

TEST_F(LoadTest, InterleavedSeekFillBlock) {
  auto factories = MakeFactories(*gExecutor);
  TestInterleavedSeekFillBlock(gExecutor->GetReader(), factories, kWindowSizes);
}

TEST_F(LoadTest, AdvanceSkipFillBlock) {
  auto factories = MakeFactories(*gExecutor);
  TestAdvanceSkipFillBlock(gExecutor->GetReader(), factories, kWindowSizes,
                           kAdvanceSkips);
}

TEST_F(LoadTest, SeekSkipFillBlock) {
  auto factories = MakeFactories(*gExecutor);
  TestSeekSkipFillBlock(gExecutor->GetReader(), factories, kWindowSizes,
                        kSeekSkips);
}

// The command grammar the benchmark driver speaks. Named `LoadTest...` so the
// filter CI runs picks it up; it needs no corpus.
TEST(LoadTestCommands, Kinds) {
  using bench::Kind;

  auto cmd = bench::ParseCommand("count");
  EXPECT_EQ(Kind::Count, cmd.kind);
  EXPECT_FALSE(cmd.report.hash);
  EXPECT_FALSE(cmd.report.print);

  EXPECT_EQ(Kind::Docs, bench::ParseCommand("docs").kind);
  EXPECT_EQ(Kind::Scored, bench::ParseCommand("scored").kind);

  cmd = bench::ParseCommand("top_100");
  EXPECT_EQ(Kind::TopK, cmd.kind);
  EXPECT_EQ(100, cmd.k);
  EXPECT_TRUE(cmd.prune);
}

TEST(LoadTestCommands, AnyK) {
  for (const auto [name, k] :
       {std::pair<std::string_view, uint32_t>{"top_1", 1},
        {"top_7", 7},
        {"top_1000", 1000},
        {"top_4294967295", 4294967295}}) {
    const auto cmd = bench::ParseCommand(name);
    EXPECT_EQ(bench::Kind::TopK, cmd.kind) << name;
    EXPECT_EQ(k, cmd.k) << name;
  }
}

TEST(LoadTestCommands, CountSuffixTurnsPruningOff) {
  const auto cmd = bench::ParseCommand("top_100_count");
  EXPECT_EQ(bench::Kind::TopK, cmd.kind);
  EXPECT_EQ(100, cmd.k);
  EXPECT_FALSE(cmd.prune);
}

TEST(LoadTestCommands, HashAndPrintEitherOrBoth) {
  for (const auto* name :
       {"docs_hash", "scored_hash", "top_10_hash", "top_10_count_hash"}) {
    const auto cmd = bench::ParseCommand(name);
    EXPECT_NE(bench::Kind::Unsupported, cmd.kind) << name;
    EXPECT_TRUE(cmd.report.hash) << name;
    EXPECT_FALSE(cmd.report.print) << name;
  }

  const auto printed = bench::ParseCommand("docs_print");
  EXPECT_EQ(bench::Kind::Docs, printed.kind);
  EXPECT_FALSE(printed.report.hash);
  EXPECT_TRUE(printed.report.print);

  for (const auto* name : {"docs_hash_print", "docs_print_hash",
                           "top_3_print_hash", "scored_hash_print"}) {
    const auto cmd = bench::ParseCommand(name);
    EXPECT_NE(bench::Kind::Unsupported, cmd.kind) << name;
    EXPECT_TRUE(cmd.report.hash) << name;
    EXPECT_TRUE(cmd.report.print) << name;
  }
}

TEST(LoadTestCommands, WhatIsNotACommand) {
  for (const auto* name : {// a count is a number and nothing else
                           "count_hash", "count_print",
                           // each modifier at most once
                           "docs_hash_hash", "docs_print_print",
                           // a top-k needs a count of its own
                           "top_", "top_0", "top_abc", "top_10x", "top_-1",
                           // and the rest is not a command at all
                           "", "bogus", "docs_debug", "_hash"}) {
    EXPECT_EQ(bench::Kind::Unsupported, bench::ParseCommand(name).kind) << name;
  }
}
