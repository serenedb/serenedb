////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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

#pragma once

#include "basics/resource_manager.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/states/term_state.hpp"
#include "iresearch/types.hpp"

namespace irs {

struct TermReader;

struct FixedPhraseState {
  explicit FixedPhraseState(IResourceManager& memory) noexcept
    : terms{{memory}}, metas{{memory}} {}

  struct TermState {
    TermState(const PostingMeta& first, score_t) noexcept : first{first} {}

    PostingMeta first;
  };

  using Terms = ManagedVector<TermState>;
  Terms terms;
  ManagedVector<const PostingMeta*> metas;
  const TermReader* reader{};
  search::PhraseHandles handles;
};

static_assert(std::is_nothrow_move_constructible_v<FixedPhraseState>);
static_assert(std::is_nothrow_move_assignable_v<FixedPhraseState>);

struct VariadicPhraseState {
  explicit VariadicPhraseState(IResourceManager& memory) noexcept
    : num_terms{{memory}}, terms{{memory}}, metas{{memory}}, boosts{{memory}} {}

  using TermState = std::pair<PostingMeta, score_t>;

  ManagedVector<uint32_t> num_terms;
  ManagedVector<uint32_t> term_groups;
  using Terms = ManagedVector<TermState>;
  Terms terms;
  ManagedVector<const PostingMeta*> metas;
  ManagedVector<score_t> boosts;
  const TermReader* reader{};
  search::PhraseHandles handles;
  bool volatile_boost{};
};

static_assert(std::is_nothrow_move_constructible_v<VariadicPhraseState>);
static_assert(std::is_nothrow_move_assignable_v<VariadicPhraseState>);

}  // namespace irs
