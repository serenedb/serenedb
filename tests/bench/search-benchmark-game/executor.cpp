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

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstring>
#include <iresearch/analysis/segmentation_tokenizer.hpp>
#include <iresearch/index/norm.hpp>
#include <iresearch/parser/parser.hpp>
#include <iresearch/search/bm25.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/filter_optimizer.hpp>
#include <iresearch/search/phrase_filter.hpp>
#include <iresearch/store/store_utils.hpp>
#include <tuple>
#include <vector>

#include "basics/duckdb_engine.h"
#include "basics/wyhash.h"
#include "index_builder.h"

namespace bench {
namespace {

template<typename T>
size_t HashBatch(size_t hash, const T* data, size_t size) {
  return sdb::basics::WyHash(data, size * sizeof(T), hash);
}

}  // namespace

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

size_t Executor::ExecuteTopK(size_t k, std::string_view query) {
  ResetResults(k);
  auto filter = ParseFilter(query);
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
  auto filter = ParseFilter(query);
  if (!filter) {
    _result_count = 0;
    return 0;
  }
  auto count = irs::ExecuteTopKWithCount(_reader, *filter, *_scorer, k,
                                         std::span{_results});
  _result_count = std::min<size_t>(k, count);
  return count;
}

size_t Executor::ExecuteCount(std::string_view query) {
  auto filter = ParseFilter(query);
  if (!filter) {
    return 0;
  }
  auto collector = filter->MakeCollector(nullptr);
  std::vector<irs::QueryBuilder::ptr> queries;
  queries.reserve(_reader.size());
  for (auto& segment : _reader) {
    queries.emplace_back(
      filter->PrepareSegment(segment, {.collector = collector.get()}));
  }
  const auto stats = collector->Finish(irs::IResourceManager::gNoop);

  size_t count = 0;
  for (auto& query : queries) {
    if (!query) {
      continue;
    }
    auto docs = query->Execute({}, stats);
    count += docs->count();
  }
  return count;
}

EmitResult Executor::ExecuteEmitDocs(std::string_view query, bool checksum) {
  auto filter = ParseFilter(query);
  if (!filter) {
    return {};
  }
  auto collector = filter->MakeCollector(nullptr);
  std::vector<irs::QueryBuilder::ptr> queries;
  queries.reserve(_reader.size());
  for (auto& segment : _reader) {
    queries.emplace_back(
      filter->PrepareSegment(segment, {.collector = collector.get()}));
  }
  const auto stats = collector->Finish(irs::IResourceManager::gNoop);

  EmitResult result;
  for (auto& query : queries) {
    if (!query) {
      continue;
    }
    auto docs = query->Execute({}, stats);
    auto min = irs::doc_limits::min();
    while (!irs::doc_limits::eof(min)) {
      const auto n = docs->EmitDocs(_emit_docs.data(), min, min + kEmitWindow);
      result.count += n;
      if (checksum) {
        result.hash = HashBatch(result.hash, _emit_docs.data(), n);
      }
      min = docs->value();
    }
  }
  return result;
}

EmitResult Executor::ExecuteEmitScoredDocs(std::string_view query,
                                           bool checksum) {
  auto filter = ParseFilter(query);
  if (!filter) {
    return {};
  }
  auto collector = filter->MakeCollector(_scorer_ptr);
  std::vector<irs::QueryBuilder::ptr> queries;
  queries.reserve(_reader.size());
  for (auto& segment : _reader) {
    queries.emplace_back(
      filter->PrepareSegment(segment, {.collector = collector.get()}));
  }
  const auto stats = collector->Finish(irs::IResourceManager::gNoop);

  irs::ColumnArgsFetcher fetcher;
  EmitResult result;
  uint32_t seg_idx = 0;
  for (auto& segment : _reader) {
    fetcher.Clear();
    auto& query = queries[seg_idx++];
    if (!query) {
      continue;
    }
    auto docs = query->Execute({}, stats);
    auto score_func = docs->PrepareScore({
      .scorer = _scorer_ptr,
      .segment = &segment,
      .fetcher = &fetcher,
    });
    auto min = irs::doc_limits::min();
    while (!irs::doc_limits::eof(min)) {
      const auto n =
        docs->EmitScoredDocs(_emit_docs.data(), _emit_scores.data(),
                             min + kEmitWindow, score_func, &fetcher, min);
      result.count += n;
      if (checksum) {
        result.hash = HashBatch(result.hash, _emit_docs.data(), n);
        result.hash = HashBatch(result.hash, _emit_scores.data(), n);
      }
      min = docs->value();
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

// "~term MIN-MAX term MIN-MAX term" builds a phrase whose gaps are ranges,
// one range per adjacent pair. The Lucene syntax cannot express this (it only
// carries a whole-phrase slop), and it is the only way to reach the interval
// phrase iterator from a query string.
irs::Filter::ptr Executor::ParseIntervalPhrase(std::string_view str) {
  auto root = std::make_unique<irs::MixedBooleanFilter>();
  auto& phrase = root->GetOptional().add<irs::ByPhrase>();
  *phrase.mutable_field_id() = kTextFieldId;
  auto* options = phrase.mutable_options();

  size_t offs_min = 0;
  size_t offs_max = 0;
  bool want_term = true;
  for (size_t pos = 0; pos <= str.size();) {
    const auto next = std::min(str.find(' ', pos), str.size());
    const auto word = str.substr(pos, next - pos);
    pos = next + 1;
    if (word.empty()) {
      continue;
    }
    if (want_term) {
      _tokenizer->reset(word);
      const auto* token = irs::get<irs::TermAttr>(*_tokenizer);
      if (!_tokenizer->next()) {
        return {};
      }
      options->push_back<irs::ByTermOptions>(offs_min, offs_max).term =
        token->value;
    } else {
      const auto dash = word.find('-');
      if (dash == std::string_view::npos) {
        return {};
      }
      std::from_chars(word.data(), word.data() + dash, offs_min);
      std::from_chars(word.data() + dash + 1, word.data() + word.size(),
                      offs_max);
      if (offs_min == 0 || offs_min > offs_max) {
        return {};
      }
    }
    want_term = !want_term;
  }
  if (options->size() < 2 || want_term) {
    return {};
  }
  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter, {.scored = _scorer_ptr != nullptr});
  return filter;
}

irs::Filter::ptr Executor::ParseFilter(std::string_view str) {
  if (str.starts_with('~')) {
    return ParseIntervalPhrase(str.substr(1));
  }
  auto root = std::make_unique<irs::MixedBooleanFilter>();
  sdb::ParserContext context{*root, kTextFieldId, *_tokenizer};
  if (!sdb::ParseQuery(context, str)) {
    return {};
  }
  if (root->empty()) {
    return {};
  }
  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter, {.scored = _scorer_ptr != nullptr});
  return filter;
}

}  // namespace bench
