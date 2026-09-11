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

#include "iresearch/search/vector_similarity_query.hpp"

#include <span>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/vector_of.hpp"

namespace irs {

void RerankExactDistances(const SubReader& segment,
                          const ColumnReader& vector_column, uint32_t d,
                          std::span<const float> query, VectorMetric metric,
                          std::span<ScoreDoc> hits) {
  const auto* col_reader = segment.GetColReader();
  if (!col_reader) {
    return;
  }
  detail::RawVectorReader reader{vector_column, *col_reader, d};
  reader.SetQuery(query, metric);
  std::vector<doc_id_t> docs(hits.size());
  std::vector<score_t> scores(hits.size());
  for (size_t i = 0; i < hits.size(); ++i) {
    docs[i] = hits[i].doc;
  }
  reader.ComputeDistances(docs, scores);
  for (size_t i = 0; i < hits.size(); ++i) {
    hits[i].score = scores[i];
  }
}

}  // namespace irs
