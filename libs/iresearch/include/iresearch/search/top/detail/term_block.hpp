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

#include <algorithm>

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/posting_batch.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top::detail {

template<typename InputType, typename Table>
class TermBlock : public search::PostingBatch<InputType, Table, true> {
  using Base = search::PostingBatch<InputType, Table, true>;

  using Base::_left_in_list;
  using Base::kBlock;
  using Base::ReadDocs;
  using Base::ScoreBlock;
  using Base::ScoreTail;

 public:
  static constexpr uint32_t kFill = doc_limits::kBlockSize;

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               const SubReader& segment, const TermReader& field,
               const search::ScoreArgs& args, IndexFeatures layout,
               bool bounds) {
    SDB_ASSERT(meta.docs_count > 1, "a single document has its own root");
    this->OpenInput(meta, doc_in, bounds);
    this->ArmWalk(meta, layout, bounds);
    this->MakeScore(segment, field, args);
  }

  uint32_t Fill(doc_id_t* IRS_RESTRICT docs, score_t* IRS_RESTRICT scores) {
    const auto left = _left_in_list;
    if (left == 0) {
      return 0;
    }

    if (left >= kBlock) {
      ReadDocs(docs, kBlock);
      ScoreBlock(docs, scores);
      return kBlock;
    }

    ReadDocs(docs, left);
    ScoreTail(docs, scores, left);
    return left;
  }
};

}  // namespace irs::top::detail
