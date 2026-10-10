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
#include <span>
#include <utility>

#include "iresearch/utils/fst/fst_string_weight.hpp"
#include "iresearch/utils/noncopyable.hpp"
#include "iresearch/utils/resource_manager.hpp"

namespace irs {

struct ByteArc {
  using Weight = ByteWeight;
  using Label = int32_t;
  using StateId = int32_t;

  Label ilabel;
  StateId nextstate;
  Weight weight;
};

class VectorByteFst : private util::Noncopyable {
 public:
  using Arc = ByteArc;
  using Weight = ByteWeight;
  using Label = Arc::Label;
  using StateId = Arc::StateId;

  static constexpr StateId kNoStateId = -1;

  explicit VectorByteFst(IResourceManager& rm)
    : _states{ManagedTypedAllocator<State>{rm}} {}

  StateId Start() const noexcept { return _start; }

  StateId NumStates() const noexcept {
    return static_cast<StateId>(_states.size());
  }

  const Weight& Final(StateId s) const noexcept { return _states[s].final; }

  size_t NumArcs(StateId s) const noexcept { return _states[s].arcs.size(); }

  std::span<const Arc> Arcs(StateId s) const noexcept {
    return _states[s].arcs;
  }

  StateId AddState() {
    _states.emplace_back(_states.get_allocator());
    return NumStates() - 1;
  }

  void SetStart(StateId s) noexcept { _start = s; }

  void SetFinal(StateId s, const Weight& weight) { _states[s].final = weight; }

  void EmplaceArc(StateId s, Label ilabel, const Weight& weight,
                  StateId nextstate) {
    _states[s].arcs.push_back(Arc{ilabel, nextstate, weight});
  }

  void DeleteStates() noexcept {
    _states.clear();
    _start = kNoStateId;
  }

 private:
  struct State {
    explicit State(const ManagedTypedAllocator<State>& alloc)
      : arcs{ManagedTypedAllocator<Arc>{alloc}} {}

    Weight final;
    ManagedVector<Arc> arcs;
  };

  ManagedVector<State> _states;
  StateId _start{kNoStateId};
};

using byte_arc = ByteArc;
using vector_byte_fst = VectorByteFst;

}  // namespace irs
