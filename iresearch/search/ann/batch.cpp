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

#include "iresearch/search/ann/batch.hpp"

#include <algorithm>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/ann/family.hpp"
#include "iresearch/search/detail/ann_index.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/scorers/vector_similarity_scorer.hpp"

namespace irs {
namespace ann {

uint32_t BatchRows(AnnKind /*kind*/, uint32_t d) noexcept {
  constexpr size_t kQueryBlockBytes = size_t{512} << 10;
  constexpr size_t kMaxRows = 2048;
  return static_cast<uint32_t>(std::clamp<size_t>(
    kQueryBlockBytes / (size_t{d} * sizeof(float)), 1, kMaxRows));
}

}  // namespace ann

void AnnIndex::SearchBatch(const SubReader& segment, const PrepareContext& ctx,
                           const VectorFilterOptions& opts,
                           const ann::QueryBatch& batch, uint32_t effort,
                           std::span<ann::Collector> collectors) const {
  SDB_ASSERT(collectors.size() == batch.Size());
  const VectorSimilarityScorer scorer;
  ColumnArgsFetcher fetcher;
  auto query_opts = opts;
  for (uint32_t i = 0; i != batch.Size(); ++i) {
    const auto query = batch.Query(i);
    query_opts.query.assign(query.begin(), query.end());
    const auto prepared = PrepareKnn(segment, ctx, query_opts, effort, nullptr);
    if (!prepared) {
      continue;
    }
    auto& collector = collectors[i];
    fetcher.Clear();
    auto root = ann::MakeRoot(
      *prepared, {.scorer = scorer, .fetcher = fetcher, .k = collector.K()});
    if (root) {
      root->Run(collector);
    }
  }
}

}  // namespace irs
