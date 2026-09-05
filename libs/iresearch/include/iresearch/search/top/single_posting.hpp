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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename Table>
class SinglePosting : public Root {
 public:
  explicit SinglePosting(Table table) noexcept : _admit{table} {}

  void Prepare(const PostingMeta& meta, const SubReader& segment,
               const TermReader& field, const search::ScoreArgs& args) {
    SDB_ASSERT(meta.docs_count == 1);
    _doc = doc_limits::min() + meta.doc_delta;
    _score = search::SingleDocScore(segment, field, _doc, meta.freq, args);
  }

  void Run(LoserScoreCollector& collector) final {
    if (!doc_limits::valid(_doc)) {
      return;
    }
    _admit.Add(collector, _score, _doc);
    _doc = doc_limits::invalid();
    _admit.Flush(collector);
  }

 private:
  doc_id_t _doc = doc_limits::invalid();
  score_t _score = 0;
  [[no_unique_address]] Admit<Table> _admit;
};

}  // namespace irs::top
