////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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

#include "all_iterator.hpp"

#include "iresearch/formats/empty_term_reader.hpp"

namespace irs {

AllIterator::AllIterator(const SubReader& reader, const byte_type* query_stats,
                         const Scorers& order, uint64_t docs_count,
                         score_t boost)
  : _max_doc{doc_id_t(doc_limits::min() + docs_count - 1)} {
  std::get<CostAttr>(_attrs).reset(_max_doc);

  if (!order.empty()) {
    auto& score = std::get<ScoreAttr>(_attrs);
    CompileScore(score, order.buckets(), reader,
                 irs::EmptyTermReader(docs_count), query_stats, *this, boost);
  }
}

}  // namespace irs
