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
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/scored/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::scored {

class SinglePosting : public Root {
 public:
  void Prepare(const PostingMeta& meta, const SubReader& segment,
               const TermReader& field, const ScoreArgs& args) {
    SDB_ASSERT(meta.docs_count == 1);
    _doc = doc_limits::min() + meta.doc_delta;
    _score = search::SingleDocScore(segment, field, _doc, meta.freq, args);
  }

  uint32_t Run(doc_id_t* docs, score_t* scores, uint32_t capacity) final {
    SDB_ASSERT(capacity != 0);
    if (!doc_limits::valid(_doc)) {
      return 0;
    }
    docs[0] = _doc;
    scores[0] = _score;
    _doc = doc_limits::invalid();
    return 1;
  }

 private:
  doc_id_t _doc = doc_limits::invalid();
  score_t _score = 0;
};

}  // namespace irs::scored
