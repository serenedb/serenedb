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
#include <vector>

#include "basics/assert.h"
#include "basics/memory.hpp"
#include "basics/shared.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/lead/concept.hpp"
#include "iresearch/search/lead/node.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

template<Type Leaf>
class Impl : public Node {
 public:
  template<typename... Args>
  explicit Impl(Args&&... args) : _leaf{std::forward<Args>(args)...} {}

  doc_id_t Advance() final { return _leaf.Advance(); }

  doc_id_t Seek(doc_id_t target) final { return _leaf.Seek(target); }

  void FetchScoreArgs(uint32_t slot) final {
    if constexpr (ScoredType<Leaf>) {
      _leaf.FetchScoreArgs(slot);
    }
  }

  ScoreFunction PrepareScore() final {
    if constexpr (ScoredType<Leaf>) {
      return _leaf.PrepareScore();
    } else {
      return ScoreFunction::Default();
    }
  }

 private:
  Leaf _leaf;
};

class Erased {
 public:
  Erased() = default;

  explicit Erased(Node::ptr node) noexcept : _node{std::move(node)} {}

  IRS_FORCE_INLINE doc_id_t Advance() { return _node->Advance(); }

  IRS_FORCE_INLINE doc_id_t Seek(doc_id_t target) {
    return _node->Seek(target);
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) {
    _node->FetchScoreArgs(slot);
  }

  ScoreFunction PrepareScore() { return _node->PrepareScore(); }

  void CollectScorers(std::vector<ScoreFunction>& out) {
    search::AppendScorer(out, PrepareScore());
  }

  IRS_FORCE_INLINE bool Valid() const noexcept { return _node != nullptr; }

 private:
  Node::ptr _node;
};

}  // namespace irs::lead
