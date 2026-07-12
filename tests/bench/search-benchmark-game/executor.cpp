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

#include <cstring>
#include <iresearch/analysis/segmentation_tokenizer.hpp>
#include <iresearch/index/norm.hpp>
#include <iresearch/parser/parser.hpp>
#include <iresearch/search/bm25.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/filter_optimizer.hpp>
#include <iresearch/store/store_utils.hpp>
#include <vector>

#include "basics/duckdb_engine.h"
#include "index_builder.h"

namespace bench {

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
  auto count = irs::ExecuteTopK(_reader, *filter, *_scorer, k,
                                {.wand_enabled = true, .strict = true},
                                std::span{_results});
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

irs::Filter::ptr Executor::ParseFilter(std::string_view str) {
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
