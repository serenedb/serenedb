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
#include <concepts>
#include <cstdint>

#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

template<typename T>
concept Type = requires(T& t, doc_id_t target) {
  { t.Next() } -> std::same_as<doc_id_t>;
  { t.Seek(target) } -> std::same_as<doc_id_t>;
};

template<typename T>
concept ScoredType = Type<T> && requires(T& t, uint32_t slot) {
  { t.FetchScoreArgs(slot) };
  { t.PrepareScore() } -> std::same_as<ScoreFunction>;
};

template<Type Node>
IRS_FORCE_INLINE doc_id_t SeekBelow(Node& node, doc_id_t target, doc_id_t max) {
  if constexpr (requires { node.Seek(target, max); }) {
    return node.Seek(target, max);
  } else {
    return node.Seek(target);
  }
}

template<Type Node>
IRS_FORCE_INLINE doc_id_t NextBelow(Node& node, doc_id_t max) {
  if constexpr (requires { node.Next(max); }) {
    return node.Next(max);
  } else {
    return node.Next();
  }
}

template<Type Node>
IRS_FORCE_INLINE uint32_t Gather(Node& node, doc_id_t& doc, doc_id_t max,
                                 doc_id_t* out, uint32_t cap) {
  if constexpr (requires { node.Gather(doc, max, out, cap); }) {
    return node.Gather(doc, max, out, cap);
  } else {
    uint32_t n = 0;
    do {
      out[n++] = doc;
      doc = NextBelow(node, max);
    } while (n != cap && doc < max);
    return n;
  }
}

template<Type Node>
uint32_t GatherAll(Node& node, doc_id_t min, doc_id_t max, doc_id_t* out) {
  uint32_t n = 0;
  for (auto doc = SeekBelow(node, min, max); doc < max;) {
    n += Gather(node, doc, max, out + n, max - min - n);
  }
  return n;
}

template<typename Node>
concept LeafType = Type<Node> && requires(Node& node) {
  node.Leaf();
  node.NextLeaf();
};

template<LeafType Node, typename CountLeaf>
uint64_t CountLeaves(Node& node, doc_id_t min, doc_id_t max,
                     CountLeaf&& count) {
  uint64_t total = 0;
  for (auto doc = node.Seek(min); doc < max;) {
    const auto leaf = node.Leaf();
    if (leaf.back() < max) {
      total += count(leaf.data(), static_cast<uint32_t>(leaf.size()));
      doc = node.NextLeaf();
      continue;
    }
    total +=
      count(leaf.data(),
            static_cast<uint32_t>(
              std::lower_bound(leaf.begin(), leaf.end(), max) - leaf.begin()));
    node.Seek(max);
    break;
  }
  return total;
}

template<Type Node>
uint64_t Count(Node& node, doc_id_t min, doc_id_t max) {
  if constexpr (requires { node.Count(min, max); }) {
    return node.Count(min, max);
  } else {
    uint64_t total = 0;
    for (auto doc = SeekBelow(node, min, max); doc < max;
         doc = NextBelow(node, max)) {
      ++total;
    }
    return total;
  }
}

}  // namespace irs::lead
