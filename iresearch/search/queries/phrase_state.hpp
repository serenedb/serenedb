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

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/queries/term_state.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/resource_manager.hpp"

namespace irs {

struct TermReader;

struct PostingsState {
  explicit PostingsState(IResourceManager& memory) noexcept : metas{{memory}} {}

  ManagedVector<PostingMeta> metas;
  const TermReader* reader{};
  detail::PhraseHandles handles;
};

struct NGramState : PostingsState {
  using PostingsState::PostingsState;
};

struct PhraseState : PostingsState {
  explicit PhraseState(IResourceManager& memory) noexcept
    : PostingsState{memory}, boosts{{memory}}, offsets{{memory}} {}

  size_t Slots() const noexcept {
    SDB_ASSERT(!offsets.empty());
    return offsets.size() - 1;
  }

  bool Fixed() const noexcept {
    return metas.size() == Slots() && boosts.empty();
  }

  ManagedVector<score_t> boosts;
  ManagedVector<uint32_t> offsets;
};

static_assert(std::is_nothrow_move_constructible_v<NGramState>);
static_assert(std::is_nothrow_move_assignable_v<NGramState>);
static_assert(std::is_nothrow_move_constructible_v<PhraseState>);
static_assert(std::is_nothrow_move_assignable_v<PhraseState>);

}  // namespace irs
