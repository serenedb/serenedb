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

#include <utility>

#include "basics/memory.hpp"
#include "basics/shared.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

struct PrunedClause : memory::Managed {
  using ptr = memory::managed_ptr<PrunedClause>;

  virtual doc_id_t Probe(doc_id_t target) = 0;
  virtual doc_id_t AdvanceBlock(doc_id_t target) = 0;
  virtual score_t MaxScore(doc_id_t last) = 0;
  virtual void FetchScoreArgs(uint32_t slot) = 0;
  virtual ScoreFunction PrepareScore() = 0;
};

template<typename Leaf>
class PrunedClauseImpl : public PrunedClause {
 public:
  template<typename... Args>
  explicit PrunedClauseImpl(Args&&... args)
    : _leaf{std::forward<Args>(args)...} {}

  doc_id_t Probe(doc_id_t target) final { return _leaf.Probe(target); }

  doc_id_t AdvanceBlock(doc_id_t target) final {
    return _leaf.AdvanceBlock(target);
  }

  score_t MaxScore(doc_id_t last) final { return _leaf.MaxScore(last); }

  void FetchScoreArgs(uint32_t slot) final { _leaf.FetchScoreArgs(slot); }

  ScoreFunction PrepareScore() final {
    if constexpr (requires { _leaf.PrepareScore(); }) {
      return _leaf.PrepareScore();
    } else {
      return _leaf.PrepareScore(ScoreMergeType::Sum, score_t{0});
    }
  }

 private:
  Leaf _leaf;
};

class ErasedClause {
 public:
  ErasedClause() = default;

  explicit ErasedClause(PrunedClause::ptr node) noexcept
    : _node{std::move(node)} {}

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    return _node->Probe(target);
  }

  IRS_FORCE_INLINE doc_id_t AdvanceBlock(doc_id_t target) {
    return _node->AdvanceBlock(target);
  }

  IRS_FORCE_INLINE score_t MaxScore(doc_id_t last) {
    return _node->MaxScore(last);
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) {
    _node->FetchScoreArgs(slot);
  }

  ScoreFunction PrepareScore() { return _node->PrepareScore(); }

 private:
  PrunedClause::ptr _node;
};

}  // namespace irs::top
