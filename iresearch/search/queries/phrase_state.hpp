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
#include "iresearch/utils/query_memory.hpp"

namespace irs {

struct TermReader;

template<typename T>
void CopyExact(memory::ArenaVector<T>& to, const memory::ArenaVector<T>& from) {
  to.reserve(from.size());
  to.assign(from.begin(), from.end());
}

struct PostingsState {
  explicit PostingsState(MemoryArena& arena) : metas{arena} {}

  PostingsState(MemoryArena& arena, const PostingsState& from)
    : metas{arena}, reader{from.reader}, handles{from.handles} {
    CopyExact(metas, from.metas);
  }

  memory::ArenaVector<PostingMeta> metas;
  const TermReader* reader{};
  detail::PhraseHandles handles;
};

struct NGramState : PostingsState {
  using PostingsState::PostingsState;
};

struct PhraseState : PostingsState {
  explicit PhraseState(MemoryArena& arena)
    : PostingsState{arena}, boosts{arena}, offsets{arena} {}

  PhraseState(MemoryArena& arena, const PhraseState& from)
    : PostingsState{arena, from}, boosts{arena}, offsets{arena} {
    CopyExact(boosts, from.boosts);
    CopyExact(offsets, from.offsets);
  }

  size_t Slots() const noexcept {
    SDB_ASSERT(!offsets.empty());
    return offsets.size() - 1;
  }

  bool Fixed() const noexcept {
    return metas.size() == Slots() && boosts.empty();
  }

  memory::ArenaVector<score_t> boosts;
  memory::ArenaVector<uint32_t> offsets;
};

static_assert(std::is_nothrow_move_constructible_v<NGramState>);
static_assert(std::is_nothrow_move_assignable_v<NGramState>);
static_assert(std::is_nothrow_move_constructible_v<PhraseState>);
static_assert(std::is_nothrow_move_assignable_v<PhraseState>);

}  // namespace irs
