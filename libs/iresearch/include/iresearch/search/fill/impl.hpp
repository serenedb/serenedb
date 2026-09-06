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

#include <cstdint>
#include <utility>

#include "basics/empty.hpp"
#include "basics/memory.hpp"
#include "basics/shared.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/fill/concept.hpp"
#include "iresearch/search/fill/node.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::fill {

template<typename Leaf>
class Impl : public Node {
 public:
  static constexpr bool kRestricts = Type<Leaf>;
  static_assert(Producer<Leaf> || ScoredType<Leaf>);

  template<typename... Args>
  explicit Impl(Args&&... args) : _leaf{std::forward<Args>(args)...} {}

  doc_id_t FillOr(doc_id_t min, doc_id_t max,
                  uint64_t* IRS_RESTRICT mask) final {
    if constexpr (Producer<Leaf>) {
      return _leaf.FillOr(min, max, mask);
    } else {
      SDB_UNREACHABLE();
    }
  }

  doc_id_t FillAnd(doc_id_t min, doc_id_t max,
                   uint64_t* IRS_RESTRICT mask) final {
    if constexpr (!Producer<Leaf>) {
      SDB_UNREACHABLE();
    } else if constexpr (kRestricts) {
      return _leaf.FillAnd(min, max, mask);
    } else {
      const auto words = search::WindowWords(min, max);
      search::Clear(_own.data(), words);
      const auto next = _leaf.FillOr(min, max, _own.data());
      search::FoldAnd(mask, _own.data(), words);
      return next;
    }
  }

  doc_id_t FillAndNot(doc_id_t min, doc_id_t max,
                      uint64_t* IRS_RESTRICT mask) final {
    if constexpr (!Producer<Leaf>) {
      SDB_UNREACHABLE();
    } else if constexpr (kRestricts) {
      return _leaf.FillAndNot(min, max, mask);
    } else {
      const auto words = search::WindowWords(min, max);
      search::Clear(_own.data(), words);
      const auto next = _leaf.FillOr(min, max, _own.data());
      search::FoldAndNot(mask, _own.data(), words);
      return next;
    }
  }

  doc_id_t Fill(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                score_t* IRS_RESTRICT scores) final {
    if constexpr (ScoredType<Leaf>) {
      return _leaf.Fill(min, max, mask, scores);
    } else {
      return FillOr(min, max, mask);
    }
  }

  search::BitsetStorage* Folded() noexcept final {
    if constexpr (requires(Leaf& leaf) { leaf.Folded(); }) {
      return _leaf.Folded();
    } else {
      return nullptr;
    }
  }

 private:
  [[no_unique_address]] utils::Need<Producer<Leaf> && !kRestricts,
                                    search::Scratch> _own;
  Leaf _leaf;
};

class Erased {
 public:
  Erased() = default;

  explicit Erased(Node::ptr node) noexcept : _node{std::move(node)} {}

  IRS_FORCE_INLINE doc_id_t FillOr(doc_id_t min, doc_id_t max,
                                   uint64_t* IRS_RESTRICT mask) {
    return _node->FillOr(min, max, mask);
  }

  IRS_FORCE_INLINE doc_id_t FillAnd(doc_id_t min, doc_id_t max,
                                    uint64_t* IRS_RESTRICT mask) {
    return _node->FillAnd(min, max, mask);
  }

  IRS_FORCE_INLINE doc_id_t FillAndNot(doc_id_t min, doc_id_t max,
                                       uint64_t* IRS_RESTRICT mask) {
    return _node->FillAndNot(min, max, mask);
  }

  IRS_FORCE_INLINE doc_id_t Fill(doc_id_t min, doc_id_t max,
                                 uint64_t* IRS_RESTRICT mask,
                                 score_t* IRS_RESTRICT scores) {
    return _node->Fill(min, max, mask, scores);
  }

  IRS_FORCE_INLINE bool Valid() const noexcept { return _node != nullptr; }

 private:
  Node::ptr _node;
};

}  // namespace irs::fill
