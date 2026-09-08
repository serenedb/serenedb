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

#include "executor.h"

#include <absl/strings/str_format.h>
#include <fast_float/fast_float.h>

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstdio>
#include <duckdb/common/allocator.hpp>
#include <iresearch/analysis/segmentation_tokenizer.hpp>
#include <iresearch/index/norm.hpp>
#include <iresearch/parser/parser.hpp>
#include <iresearch/search/bm25.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/count/make.hpp>
#include <iresearch/search/docs/make.hpp>
#include <iresearch/search/filter_optimizer.hpp>
#include <iresearch/search/ngram_similarity_filter.hpp>
#include <iresearch/search/phrase_filter.hpp>
#include <iresearch/search/scored/root.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/store/store_utils.hpp>
#include <stdexcept>
#include <tuple>
#include <vector>

#include "basics/duckdb_engine.h"
#include "basics/wyhash.h"
#include "index_builder.h"

namespace bench {
namespace {

template<typename T>
size_t HashBatch(size_t hash, const T* data, size_t size) {
  for (size_t i = 0; i != size; ++i) {
    hash = sdb::basics::WyHash(data + i, sizeof(T), hash);
  }
  return hash;
}

template<typename T, typename U>
size_t HashPairs(size_t hash, const T* docs, const U* scores, size_t size) {
  for (size_t i = 0; i != size; ++i) {
    hash = sdb::basics::WyHash(docs + i, sizeof(T), hash);
    hash = sdb::basics::WyHash(scores + i, sizeof(U), hash);
  }
  return hash;
}

bool StripSuffix(std::string_view& name, std::string_view suffix) {
  if (!name.ends_with(suffix)) {
    return false;
  }
  name.remove_suffix(suffix.size());
  return true;
}

}  // namespace

Command ParseCommand(std::string_view name) {
  Command cmd{.kind = Kind::Count};

  for (;;) {
    if (!cmd.report.print && StripSuffix(name, "_print")) {
      cmd.report.print = true;
      continue;
    }
    if (!cmd.report.hash && StripSuffix(name, "_hash")) {
      cmd.report.hash = true;
      continue;
    }
    break;
  }

  if (name == "count") {
    // A count is a number and nothing else: there are no documents in hand to
    // checksum or to print.
    if (cmd.report.hash || cmd.report.print) {
      return {};
    }
    return cmd;
  }
  if (name == "docs") {
    cmd.kind = Kind::Docs;
    return cmd;
  }
  if (name == "scored") {
    cmd.kind = Kind::Scored;
    return cmd;
  }

  cmd.prune = !StripSuffix(name, "_count");
  if (!name.starts_with("top_")) {
    return {};
  }
  name.remove_prefix(4);

  const auto* const end = name.data() + name.size();
  const auto [stop, ec] = fast_float::from_chars(name.data(), end, cmd.k);
  if (ec != std::errc{} || stop != end || cmd.k == 0) {
    return {};
  }
  cmd.kind = Kind::TopK;
  return cmd;
}

Executor::Executor(std::string_view path, const BenchConfig& config)
  : _scorer{irs::BM25::Make(irs::BM25::Options{})},
    _tokenizer{irs::analysis::SegmentationTokenizer::Make(
      irs::analysis::SegmentationTokenizer::Options{})},
    _format{irs::formats::Get(config.format_name, false)},
    _dir{path},
    _reader{irs::DirectoryReader(
      _dir, _format,
      {.scorer = _scorer_ptr,
       .db = &::sdb::DuckDBEngine::Instance().instance()})} {}

// A debug knob, off unless `IRESEARCH_DISABLE_SHAPES` names shapes to skip,
// comma separated (`count`, `docs`, `scored`). What it answers is the only
// question that matters about a shape: is its plan faster than the path it
// replaced -- measured in one binary over one index, so nothing but the plan
// differs.
bool ShapeDisabled(std::string_view shape) {
  static const std::string kDisabled = [] {
    const auto* const value = std::getenv("IRESEARCH_DISABLE_SHAPES");
    return value != nullptr ? std::string{value} : std::string{};
  }();
  std::string_view rest{kDisabled};
  while (!rest.empty()) {
    const auto end = rest.find(',');
    if (rest.substr(0, end) == shape) {
      return true;
    }
    if (end == std::string_view::npos) {
      break;
    }
    rest.remove_prefix(end + 1);
  }
  return false;
}

size_t Executor::ExecuteTopK(size_t k, std::string_view query) {
  ResetResults(k);
  auto filter = ParseFilter(query, true);
  if (!filter) {
    _result_count = 0;
    return 0;
  }
  auto count =
    irs::ExecuteTopK(_reader, *filter, *_scorer, k, true, std::span{_results});
  _result_count = std::min<size_t>(k, count);
  return count;
}

size_t Executor::ExecuteTopKWithCount(size_t k, std::string_view query) {
  ResetResults(k);
  auto filter = ParseFilter(query, true);
  if (!filter) {
    _result_count = 0;
    return 0;
  }
  auto count =
    irs::ExecuteTopK(_reader, *filter, *_scorer, k, false, std::span{_results});
  _result_count = std::min<size_t>(k, count);
  return count;
}

size_t Executor::ExecuteCount(std::string_view query) {
  auto filter = ParseFilter(query, false);
  if (!filter) {
    return 0;
  }
  // Counting does not score, so no subtree collects statistics.
  std::vector<irs::QueryBuilder::ptr> queries;
  queries.reserve(_reader.size());
  for (auto& segment : _reader) {
    queries.emplace_back(filter->PrepareSegment(segment, {}));
  }

  size_t count = 0;
  size_t i = 0;
  for ([[maybe_unused]] auto& segment : _reader) {
    auto& query = queries[i++];
    if (!query) {
      continue;
    }
    auto plan = ShapeDisabled("count") ? irs::count::Root::ptr{}
                                       : irs::count::MakeRoot(*query);
    if (!plan) {
      throw std::runtime_error{"no count plan for this query"};
    }
    count += plan->Run();
  }
  return count;
}

EmitResult Executor::ExecuteEmitDocs(std::string_view query, Report report) {
  auto filter = ParseFilter(query, false);
  if (!filter) {
    return {};
  }
  // Emitting documents does not score, so no subtree collects statistics.
  std::vector<irs::QueryBuilder::ptr> queries;
  queries.reserve(_reader.size());
  for (auto& segment : _reader) {
    queries.emplace_back(filter->PrepareSegment(segment, {}));
  }

  EmitResult result;
  size_t seg = 0;
  for ([[maybe_unused]] auto& segment : _reader) {
    auto& query = queries[seg++];
    if (!query) {
      continue;
    }
    auto plan = ShapeDisabled("docs") ? irs::docs::Root::ptr{}
                                      : irs::docs::MakeRoot(*query);
    if (!plan) {
      throw std::runtime_error{"no docs plan for this query"};
    }
    for (;;) {
      const auto n = plan->Run(_emit_docs.data(), kEmitWindow);
      if (n == 0) {
        break;
      }
      result.count += n;
      if (report.hash) {
        result.hash = HashBatch(result.hash, _emit_docs.data(), n);
      }
      if (report.print) {
        for (uint32_t i = 0; i != n; ++i) {
          absl::FPrintF(_print_out, "doc=%u\n", _emit_docs[i]);
        }
      }
    }
  }
  return result;
}

EmitResult Executor::ExecuteEmitScoredDocs(std::string_view query,
                                           Report report) {
  auto filter = ParseFilter(query, true);
  if (!filter) {
    return {};
  }
  auto& allocator = duckdb::Allocator::DefaultAllocator();
  irs::StatsArena stats{allocator};
  irs::PreparedCollector collector{*filter, *_scorer_ptr, stats, 1};
  std::vector<irs::QueryBuilder::ptr> queries;
  queries.reserve(_reader.size());
  for (auto& segment : _reader) {
    queries.emplace_back(
      filter->PrepareSegment(segment, {.collector = collector.Get()}));
  }
  collector.Finish();

  irs::ColumnArgsFetcher fetcher;
  EmitResult result;
  uint32_t seg_idx = 0;
  for ([[maybe_unused]] auto& segment : _reader) {
    fetcher.Clear();
    auto& query = queries[seg_idx++];
    if (!query) {
      continue;
    }
    auto plan = ShapeDisabled("scored")
                  ? irs::scored::Root::ptr{}
                  : irs::scored::MakeRoot(
                      *query, {.scorer = *_scorer_ptr, .fetcher = fetcher});
    if (!plan) {
      throw std::runtime_error{"no scored plan for this query"};
    }
    for (;;) {
      const auto n =
        plan->Run(_emit_docs.data(), _emit_scores.data(), kEmitWindow);
      if (n == 0) {
        break;
      }
      result.count += n;
      if (report.hash) {
        // Pairwise, so the checksum does not depend on where the batch
        // boundaries happen to fall.
        result.hash =
          HashPairs(result.hash, _emit_docs.data(), _emit_scores.data(), n);
      }
      if (report.print) {
        for (uint32_t i = 0; i != n; ++i) {
          absl::FPrintF(_print_out, "doc=%u score=%.6f\n", _emit_docs[i],
                        _emit_scores[i]);
        }
      }
    }
  }
  return result;
}

size_t Executor::HashResults() const {
  std::vector<irs::ScoreDoc> hits;
  for (size_t i = 0; i != _result_count; ++i) {
    if (irs::doc_limits::valid(_results[i].doc)) {
      hits.push_back(_results[i]);
    }
  }
  absl::c_sort(hits, [](const irs::ScoreDoc& l, const irs::ScoreDoc& r) {
    return std::tie(l.score, l.segment_idx, l.doc) <
           std::tie(r.score, r.segment_idx, r.doc);
  });
  size_t hash = 0;
  for (const auto& h : hits) {
    const auto q =
      static_cast<int64_t>(std::llround(static_cast<double>(h.score) * 1e3));
    hash = HashBatch(hash, &q, 1);
  }
  return hash;
}

void Executor::PrintResults() const {
  for (size_t i = 0; i != _result_count; ++i) {
    const auto& hit = _results[i];
    if (irs::doc_limits::valid(hit.doc)) {
      absl::FPrintF(stderr, "doc=%u segment=%u score=%.6f\n", hit.doc,
                    hit.segment_idx, hit.score);
    }
  }
}

irs::Filter::ptr Executor::ParseFilter(std::string_view str, bool scored) {
  auto root = std::make_unique<irs::BooleanFilter>();
  sdb::ParserContext context{*root, kTextFieldId, *_tokenizer};
  if (!sdb::ParseQuery(context, str)) {
    absl::FPrintF(stderr, "parse error: %s: %s\n", context.error_message, str);
    return {};
  }
  if (!root->Valid()) {
    return {};
  }
  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter, {.scored = scored});
  return filter;
}

}  // namespace bench
