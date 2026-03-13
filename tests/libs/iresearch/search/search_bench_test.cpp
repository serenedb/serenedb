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
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <vpack/parser.h>
#include <vpack/serializer.h>
#include <zlib.h>

#include <bit>
#include <fstream>
#include <functional>
#include <span>
#include <string>
#include <vector>

#include "basics/bit_utils.hpp"
#include "basics/files.h"
#include "basics/math_utils.hpp"
#include "executor.hpp"
#include "index_builder.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/search/term_filter.hpp"
#include "tests_shared.hpp"

namespace {

// How to fill a window: collect docs in [window_min, window_max).
using FillWindowFunc = std::function<void(
  irs::DocIterator& iter, irs::doc_id_t window_min, irs::doc_id_t window_max,
  std::vector<irs::doc_id_t>& docs)>;

void DecodeMask(const uint64_t* mask, size_t mask_words, irs::doc_id_t base,
                std::vector<irs::doc_id_t>& docs) {
  for (size_t w = 0; w < mask_words; ++w) {
    for (auto bits = mask[w]; bits; bits = irs::PopBit(bits)) {
      docs.push_back(
        base + static_cast<irs::doc_id_t>(w * 64 + std::countr_zero(bits)));
    }
  }
}

// Reference fill: advance one doc at a time.
void FillViaAdvance(irs::DocIterator& iter, irs::doc_id_t /*window_min*/,
                    irs::doc_id_t window_max,
                    std::vector<irs::doc_id_t>& docs) {
  auto doc = iter.value();
  while (!irs::doc_limits::eof(doc) && doc < window_max) {
    docs.push_back(doc);
    doc = iter.advance();
  }
}

// Test fill: delegate to the FillBlock API, decode mask to doc IDs.
void FillViaFillBlock(irs::DocIterator& iter, irs::doc_id_t window_min,
                      irs::doc_id_t window_max,
                      std::vector<irs::doc_id_t>& docs) {
  const size_t window_size = window_max - window_min;
  const size_t mask_words = (window_size + 63) / 64;
  std::vector<uint64_t> mask(mask_words, 0);
  iter.FillBlock(window_min, window_max, mask.data(), {}, {});
  DecodeMask(mask.data(), mask_words, window_min, docs);
}

// Operation applied to BOTH iterators before each window fill.
using BeforeWindowFunc =
  std::function<void(irs::DocIterator& iter, irs::doc_id_t window_min)>;

BeforeWindowFunc SeekToWindow() {
  return [](irs::DocIterator& iter, irs::doc_id_t window_min) {
    iter.seek(window_min);
  };
}

BeforeWindowFunc AdvanceSkip(size_t count) {
  return [count](irs::DocIterator& iter, irs::doc_id_t /*window_min*/) {
    for (size_t i = 0; i < count && !irs::doc_limits::eof(iter.value()); ++i) {
      iter.advance();
    }
  };
}

BeforeWindowFunc SeekSkip(irs::doc_id_t delta) {
  return [delta](irs::DocIterator& iter, irs::doc_id_t /*window_min*/) {
    if (!irs::doc_limits::eof(iter.value())) {
      iter.seek(iter.value() + delta);
    }
  };
}

BeforeWindowFunc Cycle(std::vector<BeforeWindowFunc> ops, size_t num_iters) {
  return [ops = std::move(ops), num_iters, idx = size_t{0}](
           irs::DocIterator& iter, irs::doc_id_t window_min) mutable {
    if (const auto& op = ops[(idx / num_iters) % ops.size()]) {
      op(iter, window_min);
    }
    ++idx;
  };
}

// Walk two iterators window-by-window over [doc_limits::min(), max_doc).
// Before each window, `before_window` runs on BOTH iterators.
// Then reference fills via advance-loop, test fills via FillBlock.
// Returns total number of documents seen.
size_t CompareWindowByWindow(irs::DocIterator& reference_iter,
                             irs::DocIterator& test_iter, irs::doc_id_t max_doc,
                             irs::doc_id_t window_size,
                             const BeforeWindowFunc& before_window = {}) {
  size_t total_docs = 0;
  std::vector<irs::doc_id_t> reference_docs;
  std::vector<irs::doc_id_t> test_docs;

  for (irs::doc_id_t window_min = irs::doc_limits::min(); window_min < max_doc;
       window_min += window_size) {
    const auto window_max = std::min(window_min + window_size, max_doc);

    if (before_window) {
      before_window(reference_iter, window_min);
      before_window(test_iter, window_min);
    }

    reference_docs.clear();
    test_docs.clear();
    FillViaAdvance(reference_iter, window_min, window_max, reference_docs);
    FillViaFillBlock(test_iter, window_min, window_max, test_docs);

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

    const bool reference_eof = irs::doc_limits::eof(reference_iter.value());
    const bool test_eof = irs::doc_limits::eof(test_iter.value());
    if (reference_eof || test_eof) {
      EXPECT_EQ(reference_eof, test_eof)
        << "EOF disagreement at window [" << window_min << ", " << window_max
        << ")";
      break;
    }
  }

  return total_docs;
}

struct IteratorFactory {
  std::string name;
  std::function<irs::DocIterator::ptr(const irs::DirectoryReader&,
                                      const irs::SubReader&)>
    create;
};

IteratorFactory QueryIterator(bench::Executor& executor,
                              std::string_view query) {
  return {
    .name = std::string{query},
    .create =
      [&executor, query = std::string{query}](
        const irs::DirectoryReader& reader, const irs::SubReader& segment) {
        auto filter = executor.ParseFilter(query);
        SDB_ASSERT(filter);
        auto prepared = filter->prepare({.index = reader});
        return prepared->execute({.segment = segment});
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
  ForEachCombination(reader, factories, window_sizes,
                     [](const auto& reader, const auto& segment,
                        const auto& factory, auto max_doc, auto window_size) {
                       auto reference_iter = factory.create(reader, segment);
                       auto test_iter = factory.create(reader, segment);
                       auto count_iter = factory.create(reader, segment);

                       reference_iter->advance();
                       test_iter->advance();

                       auto total = CompareWindowByWindow(
                         *reference_iter, *test_iter, max_doc, window_size);

                       EXPECT_GT(total, 0u) << "query should have matches";
                       EXPECT_EQ(total, count_iter->count())
                         << "total docs vs count() mismatch";
                     });
}

void TestSeekVsFillBlock(const irs::DirectoryReader& reader,
                         const std::vector<IteratorFactory>& factories,
                         std::span<const irs::doc_id_t> window_sizes) {
  ForEachCombination(reader, factories, window_sizes,
                     [](const auto& reader, const auto& segment,
                        const auto& factory, auto max_doc, auto window_size) {
                       auto reference_iter = factory.create(reader, segment);
                       auto test_iter = factory.create(reader, segment);

                       CompareWindowByWindow(*reference_iter, *test_iter,
                                             max_doc, window_size,
                                             SeekToWindow());
                     });
}

void TestInterleavedSeekFillBlock(const irs::DirectoryReader& reader,
                                  const std::vector<IteratorFactory>& factories,
                                  std::span<const irs::doc_id_t> window_sizes) {
  ForEachCombination(reader, factories, window_sizes,
                     [](const auto& reader, const auto& segment,
                        const auto& factory, auto max_doc, auto window_size) {
                       auto reference_iter = factory.create(reader, segment);
                       auto test_iter = factory.create(reader, segment);

                       reference_iter->advance();
                       test_iter->advance();

                       BeforeWindowFunc noop;
                       CompareWindowByWindow(*reference_iter, *test_iter,
                                             max_doc, window_size,
                                             Cycle({noop, SeekToWindow()}, 2));
                     });
}

void TestAdvanceSkipFillBlock(const irs::DirectoryReader& reader,
                              const std::vector<IteratorFactory>& factories,
                              std::span<const irs::doc_id_t> window_sizes,
                              std::span<const size_t> skip_counts) {
  ForEachCombination(
    reader, factories, window_sizes,
    [&skip_counts](const auto& reader, const auto& segment, const auto& factory,
                   auto max_doc, auto window_size) {
      for (auto skip : skip_counts) {
        SCOPED_TRACE(testing::Message() << "advance_skip=" << skip);

        auto reference_iter = factory.create(reader, segment);
        auto test_iter = factory.create(reader, segment);

        reference_iter->advance();
        test_iter->advance();

        CompareWindowByWindow(*reference_iter, *test_iter, max_doc, window_size,
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
    [&skip_deltas](const auto& reader, const auto& segment, const auto& factory,
                   auto max_doc, auto window_size) {
      for (auto delta : skip_deltas) {
        SCOPED_TRACE(testing::Message() << "seek_skip=" << delta);

        auto reference_iter = factory.create(reader, segment);
        auto test_iter = factory.create(reader, segment);

        reference_iter->advance();
        test_iter->advance();

        CompareWindowByWindow(*reference_iter, *test_iter, max_doc, window_size,
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

std::string HashIds(const std::vector<std::string>& ids) {
  sdb::Sha256Functor sha;
  for (const auto& id : ids) {
    sha(id.data(), id.size());
    sha("\n", 1);
  }
  return sha.finalize();
}

void HashResults(std::vector<QueryResult>& results) {
  for (auto& r : results) {
    if (r.result_type == ResultType::Hash) {
      continue;
    }
    r.top_100_result = {HashIds(r.top_100_result)};
    r.top_100_count_result = {HashIds(r.top_100_count_result)};
    r.result_type = ResultType::Hash;
  }
}

std::string SerializeResults(const std::vector<QueryResult>& results) {
  vpack::Builder builder;
  vpack::WriteObject(builder, results);
  auto options = vpack::Options::gDefaults;
  options.pretty_print = true;
  return builder.slice().toJson(&options);
}

std::vector<QueryResult> DeserializeResults(std::string_view json_str) {
  auto builder{vpack::Parser::fromJson(json_str)};
  std::vector<QueryResult> results;
  vpack::ReadObject(builder->slice(), results);
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
      trx.Insert<irs::Action::INDEX | irs::Action::STORE>(doc.fields[0]);
      trx.Insert<irs::Action::INDEX>(doc.fields[1]);
    }
  }
};

void BuildIndex(const std::string& corpus_path,
                const std::filesystem::path& index_dir,
                const bench::BenchConfig& config) {
  bench::IndexBuilderOptions builder_options{
    .batch_size = 100000,
    .indexer_threads = 1,
    .commit_interval_ms = 0,
    .consolidation_interval_ms = 5000,
    .consolidation_threads = 0,
    .consolidate_all = true,
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
    auto* column = segment.column("id");
    EXPECT_NE(column, nullptr) << "'id' column not found";
    if (!column)
      return {};

    auto it = column->iterator(irs::ColumnHint::Normal);
    auto* payload = irs::get<irs::PayAttr>(*it);

    for (auto doc = it->advance(); !irs::doc_limits::eof(doc);
         doc = it->advance()) {
      auto idx = base + doc - irs::doc_limits::min();
      EXPECT_LT(idx, id_map.size()) << "doc_id out of range";
      id_map[idx] = irs::ToString<std::string_view>(payload->value.data());
    }
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
      return std::tie(b.first, a.second) < std::tie(a.first, b.second);
    });
    for (const auto& [score, doc_id] : hits) {
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

class SearchBenchTest : public TestBase {
 protected:
  enum class Mode {
    Validate,
    GenerateHash,
    GenerateJson,
  };

  void SetUp() override {
    TestBase::SetUp();

    if (!sdb::SdbGETENV("CORPUS_PATH", _corpus_path)) {
      GTEST_SKIP() << "CORPUS_PATH not set";
    }
    std::string gen;
    if (sdb::SdbGETENV("GENERATE_REFERENCE", gen)) {
      _mode = gen == "json" ? Mode::GenerateJson : Mode::GenerateHash;
    }
    std::string gzip;
    if (sdb::SdbGETENV("GENERATE_GZIP", gzip)) {
      _gzip = gzip != "0";
    }
    if (!std::filesystem::exists(_corpus_path)) {
      GTEST_SKIP() << "Path does not exist: " << _corpus_path;
    }

    std::string index_dir;
    if (std::filesystem::is_directory(_corpus_path.c_str())) {
      index_dir = _corpus_path;
      _drop_index = false;
    } else {
      BuildIndex(_corpus_path, test_dir(), _config);
      index_dir = test_dir().string();
      std::cout << absl::StrCat("Index directory: ", index_dir, "\n");
    }

    std::string drop_index;
    if (sdb::SdbGETENV("DROP_INDEX", drop_index)) {
      _drop_index = drop_index != "0";
    }

    _executor = std::make_unique<bench::Executor>(index_dir, _config);
    const auto& reader = _executor->GetReader();
    ASSERT_GT(reader.size(), 0);
    _id_map = BuildDocIdMap(reader);
  }

  void TearDown() override {
    _executor.reset();
    if (_drop_index) {
      TestBase::TearDown();
    }
  }

  void RunAndValidate(std::string_view name) {
    const auto res_dir = resource("searchbench") / name;
    const auto queries_path = res_dir / "queries.json";
    ASSERT_TRUE(std::filesystem::exists(queries_path))
      << "Queries file not found: " << queries_path;

    auto queries = LoadQueries(queries_path);
    ASSERT_FALSE(queries.empty()) << "No queries loaded from " << queries_path;

    auto results = ExecuteAllQueries(*_executor, queries, _id_map,
                                     _mode == Mode::GenerateJson);
    ASSERT_FALSE(results.empty()) << "No query results produced";

    if (_mode == Mode::GenerateHash) {
      HashResults(results);
    }
    auto json_output = SerializeResults(results);

    if (_gzip) {
      std::filesystem::create_directories(res_dir);
      auto gz_path = (res_dir / "reference.json.gz").string();
      gzFile gz = gzopen(gz_path.c_str(), "wb");
      ASSERT_NE(gz, nullptr) << "Cannot write: " << gz_path;
      gzwrite(gz, json_output.data(), json_output.size());
      gzclose(gz);
      std::cout << absl::StrCat("Gzip written to \"", gz_path, "\"\n");
    }

    if (_mode != Mode::Validate) {
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
      ASSERT_TRUE(sdb::SdbSlurpGzipFile(gz_path.c_str(), ref_str))
        << "Cannot read: " << gz_path;
    } else {
      auto raw_path = res_dir / "reference.json";
      ASSERT_TRUE(sdb::SdbSlurpFile(raw_path.c_str(), ref_str))
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

      // Hash test results if reference is hashed; compare raw otherwise
      if (e.result_type == ResultType::Hash) {
        if (a.result_type == ResultType::Raw) {
          a.top_100_result = {HashIds(a.top_100_result)};
          a.top_100_count_result = {HashIds(a.top_100_count_result)};
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

      ASSERT_EQ(a.top_100_result.size(), a.top_100_count_result.size())
        << "TOP_100 vs TOP_100_COUNT id count mismatch for query[" << i
        << "] \"" << a.query << "\"";

      for (size_t j = 0; j < a.top_100_result.size(); ++j) {
        auto expected_id = strip_score(a.top_100_count_result[j]);
        EXPECT_EQ(a.top_100_result[j], expected_id)
          << "TOP_100 vs TOP_100_COUNT id[" << j << "] mismatch for query[" << i
          << "] \"" << a.query << "\"";
      }
    }
  }

  bench::BenchConfig _config;
  std::string _corpus_path;
  std::unique_ptr<bench::Executor> _executor;
  std::vector<std::string> _id_map;
  Mode _mode = Mode::Validate;
  bool _gzip = false;
  bool _drop_index = true;
};

TEST_F(SearchBenchTest, WikiSmall) { RunAndValidate("wiki_small"); }

TEST_F(SearchBenchTest, DisjunctionScoreAccuracy) {
  const auto& reader = _executor->GetReader();
  auto scorer_ptr =
    irs::scorers::Get(_config.scorer, irs::Type<irs::text_format::Json>::get(),
                      _config.scorer_options, false);
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

    for (auto& segment : reader) {
      for (auto term_str : terms) {
        auto filter = irs::ByTerm::prepare(
          {.index = reader, .scorer = &scorer}, "text",
          irs::ViewCast<irs::byte_type>(irs::bytes_view{
            reinterpret_cast<const irs::byte_type*>(term_str.data()),
            term_str.size()}));
        ASSERT_TRUE(filter);

        auto it = filter->execute({.segment = segment, .scorer = &scorer});
        ASSERT_TRUE(it);

        irs::ColumnArgsFetcher fetcher;
        auto score_func = it->PrepareScore({
          .scorer = &scorer,
          .segment = &segment,
          .fetcher = &fetcher,
        });
        EXPECT_FALSE(score_func.IsDefault())
          << "Score function is default for term: " << term_str;

        for (auto doc = it->advance(); !irs::doc_limits::eof(doc);
             doc = it->advance()) {
          fetcher.Fetch(doc);
          it->FetchScoreArgs(0);
          irs::score_t s = score_func.Score();
          reference_scores[doc] += s;
        }
      }
    }
    ASSERT_GT(reference_scores.size(), 0u) << "No reference docs found";

    auto filter = _executor->ParseFilter(std::string{query});
    ASSERT_TRUE(filter);

    auto prepared = filter->prepare({.index = reader, .scorer = &scorer});
    ASSERT_TRUE(prepared);

    // 1) Compare via advance + Score
    {
      std::map<irs::doc_id_t, irs::score_t> bd_scores;
      for (auto& segment : reader) {
        irs::ColumnArgsFetcher fetcher;
        auto it = prepared->execute({.segment = segment, .scorer = &scorer});
        auto score_func = it->PrepareScore({
          .scorer = &scorer,
          .segment = &segment,
          .fetcher = &fetcher,
        });

        for (auto doc = it->advance(); !irs::doc_limits::eof(doc);
             doc = it->advance()) {
          fetcher.Fetch(doc);
          it->FetchScoreArgs(0);
          irs::score_t s = score_func.Score();
          bd_scores[doc] = s;
        }
      }

      EXPECT_EQ(bd_scores.size(), reference_scores.size())
        << "advance: doc count mismatch";

      for (auto& [doc, ref_score] : reference_scores) {
        auto it = bd_scores.find(doc);
        ASSERT_NE(it, bd_scores.end())
          << "advance: ref doc " << doc << " missing from BD";
        EXPECT_TRUE(irs::math::ApproxEquals(it->second, ref_score))
          << "advance: score mismatch doc " << doc << ": BD=" << it->second
          << " ref=" << ref_score;
      }
    }

    auto cmp = [](const auto& a, const auto& b) {
      return std::tie(b.first, a.second) <=> std::tie(a.first, b.second) < 0;
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
      std::vector<irs::ScoreDoc> hits(irs::BlockSize(kCount));
      const auto count = irs::ExecuteTopKWithCount(reader, *filter, scorer,
                                                   kCount, std::span{hits});

      EXPECT_EQ(count, reference_scores.size()) << "Collect: count mismatch";

      absl::c_sort(hits, cmp);

      const size_t result_count = std::min<size_t>(kCount, count);

      for (size_t i = 0; i < result_count; ++i) {
        EXPECT_EQ(hits[i].second, ref_top[i].second)
          << "Collect: rank " << i << " doc mismatch";
        EXPECT_TRUE(irs::math::ApproxEquals(hits[i].first, ref_top[i].first))
          << "Collect: rank " << i << " score mismatch doc " << hits[i].second
          << ": collect=" << hits[i].first << " ref=" << ref_top[i].first;
      }
    }

    // 3) Compare via ExecuteTopK (WAND path)
    // Scores may differ at ULP level due to different FP accumulation order.
    {
      static constexpr size_t kCount = 100;
      std::vector<irs::ScoreDoc> hits(irs::BlockSize(kCount));
      irs::ExecuteTopK(reader, *filter, scorer, kCount, {.index = 0},
                       std::span{hits});

      absl::c_sort(hits, cmp);

      const size_t result_count = std::min<size_t>(kCount, ref_top.size());

      // FP accumulation order differs between WAND and non-WAND paths,
      // so allow a wider epsilon than the default single-ULP tolerance.
      static constexpr float kEps = 1e-5f;
      for (size_t i = 0; i < result_count; ++i) {
        EXPECT_TRUE(
          irs::math::ApproxEquals(hits[i].first, ref_top[i].first, kEps))
          << "WAND: rank " << i << " score mismatch doc " << hits[i].second
          << ": wand=" << hits[i].first << " ref=" << ref_top[i].first;
      }
    }
  }
}

TEST_F(SearchBenchTest, AdvanceVsFillBlock) {
  auto factories = MakeFactories(*_executor);
  TestAdvanceVsFillBlock(_executor->GetReader(), factories, kWindowSizes);
}

TEST_F(SearchBenchTest, SeekVsFillBlock) {
  auto factories = MakeFactories(*_executor);
  TestSeekVsFillBlock(_executor->GetReader(), factories, kWindowSizes);
}

TEST_F(SearchBenchTest, InterleavedSeekFillBlock) {
  auto factories = MakeFactories(*_executor);
  TestInterleavedSeekFillBlock(_executor->GetReader(), factories, kWindowSizes);
}

TEST_F(SearchBenchTest, AdvanceSkipFillBlock) {
  auto factories = MakeFactories(*_executor);
  TestAdvanceSkipFillBlock(_executor->GetReader(), factories, kWindowSizes,
                           kAdvanceSkips);
}

TEST_F(SearchBenchTest, SeekSkipFillBlock) {
  auto factories = MakeFactories(*_executor);
  TestSeekSkipFillBlock(_executor->GetReader(), factories, kWindowSizes,
                        kSeekSkips);
}
