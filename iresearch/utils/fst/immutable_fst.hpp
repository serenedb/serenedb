////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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

#include <cstdint>
#include <limits>
#include <memory>
#include <span>

#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/fst/fst_string_ref_weight.hpp"
#include "iresearch/utils/fst/vector_fst.hpp"
#include "iresearch/utils/misc.hpp"
#include "iresearch/utils/noncopyable.hpp"
#include "iresearch/utils/resource_manager.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {

struct ImmutableByteArc {
  using Weight = ByteRefWeight;
  using Label = int32_t;
  using StateId = int32_t;

  Label ilabel{-1};
  StateId nextstate{-1};
  Weight weight;
};

class ImmutableByteFst : private util::Noncopyable {
 public:
  using Arc = ImmutableByteArc;
  using Weight = ByteRefWeight;
  using Label = Arc::Label;
  using StateId = Arc::StateId;

  static constexpr StateId kNoStateId = -1;
  static constexpr size_t kMaxArcs = 1 + std::numeric_limits<uint8_t>::max();
  static constexpr size_t kMaxStateWeight =
    std::numeric_limits<size_t>::max() >> 1;

  enum Property : uint64_t {
    kExpanded = uint64_t{1} << 0,
    kAcceptor = uint64_t{1} << 16,
    kIDeterministic = uint64_t{1} << 18,
    kODeterministic = uint64_t{1} << 20,
    kILabelSorted = uint64_t{1} << 28,
    kOLabelSorted = uint64_t{1} << 30,
    kAcyclic = uint64_t{1} << 35,
    kInitialAcyclic = uint64_t{1} << 37,
    kAccessible = uint64_t{1} << 40,
    kCoAccessible = uint64_t{1} << 42,
  };

  static constexpr uint64_t kProperties =
    kExpanded | kAcceptor | kIDeterministic | kODeterministic | kILabelSorted |
    kOLabelSorted | kAcyclic | kInitialAcyclic | kAccessible | kCoAccessible;

  ~ImmutableByteFst() {
    _resource_manager->DecreaseChecked(sizeof(State) * _nstates +
                                       sizeof(Arc) * _narcs +
                                       sizeof(byte_type) * _weights_size);
  }

  static std::unique_ptr<ImmutableByteFst> Read(DataInput& in,
                                                IResourceManager& rm);

  template<typename Stats>
  static bool Write(const VectorByteFst& fst, BufferedOutput& out,
                    const Stats& stats);

  StateId Start() const noexcept { return _start; }

  Weight Final(StateId s) const noexcept { return _states[s].weight; }

  const Weight& FinalRef(StateId s) const noexcept { return _states[s].weight; }

  StateId NumStates() const noexcept { return _nstates; }

  size_t NumArcs(StateId s) const noexcept { return _states[s].narcs; }

  std::span<const Arc> Arcs(StateId s) const noexcept {
    return {_states[s].arcs, _states[s].narcs};
  }

 private:
  struct State {
    const Arc* arcs;
    size_t narcs;
    Weight weight;
  };

  ImmutableByteFst() = default;

  std::unique_ptr<State[]> _states;
  std::unique_ptr<Arc[]> _arcs;
  std::unique_ptr<byte_type[]> _weights;
  size_t _narcs{0};
  size_t _weights_size{0};
  StateId _nstates{0};
  StateId _start{kNoStateId};
  IResourceManager* _resource_manager{&IResourceManager::gNoop};
};

inline std::unique_ptr<ImmutableByteFst> ImmutableByteFst::Read(
  DataInput& in, IResourceManager& rm) {
  [[maybe_unused]] const uint64_t props = in.ReadI64();
  const size_t total_weight_size = in.ReadI64();
  const StateId nstates = in.ReadI32();
  const StateId start = nstates - in.ReadV32();
  const size_t narcs = ReadZV64(in) + nstates;

  size_t allocated{nstates * sizeof(State) + narcs * sizeof(Arc) +
                   total_weight_size * sizeof(byte_type)};
  Finally cleanup = [&]() noexcept { rm.DecreaseChecked(allocated); };

  rm.Increase(allocated);
  auto states = std::make_unique<State[]>(nstates);
  auto arcs = std::make_unique<Arc[]>(narcs);
  auto weights = std::make_unique<byte_type[]>(total_weight_size);

  auto* weight = weights.get();
  auto* arc = arcs.get();
  for (auto state = states.get(), end = state + nstates; state != end;
       ++state) {
    state->arcs = arc;

    size_t weight_size = in.ReadV64();
    const bool has_arcs = !ShiftUnpack64(weight_size, weight_size);
    state->weight = {weight, weight_size};
    weight += weight_size;

    if (has_arcs) {
      state->narcs = static_cast<uint32_t>(in.ReadByte()) + 1;

      for (auto* end = arc + state->narcs; arc != end; ++arc) {
        arc->ilabel = in.ReadByte();
        arc->nextstate = in.ReadV32();
        const size_t weight_size = in.ReadV64();
        arc->weight = {weight, weight_size};
        weight += weight_size;
      }
    } else {
      state->narcs = 0;
    }
  }

  in.ReadData(weights.get(), total_weight_size);

  std::unique_ptr<ImmutableByteFst> fst{new ImmutableByteFst{}};
  allocated = 0;
  fst->_start = start;
  fst->_nstates = nstates;
  fst->_narcs = narcs;
  fst->_states = std::move(states);
  fst->_arcs = std::move(arcs);
  fst->_weights = std::move(weights);
  fst->_weights_size = total_weight_size;
  fst->_resource_manager = &rm;
  return fst;
}

template<typename Stats>
bool ImmutableByteFst::Write(const VectorByteFst& fst, BufferedOutput& out,
                             const Stats& stats) {
  static_assert(sizeof(StateId) == sizeof(uint32_t));

  out.WriteU64(kProperties);
  out.WriteU64(stats.total_weight_size);
  out.WriteU32(static_cast<StateId>(stats.num_states));
  SDB_ASSERT(stats.num_states >= static_cast<size_t>(fst.Start()));
  out.WriteV32(static_cast<uint32_t>(stats.num_states - fst.Start()));
  WriteZV64(out, stats.num_arcs - stats.num_states);

  for (StateId s = 0, n = fst.NumStates(); s != n; ++s) {
    const size_t weight_size = fst.Final(s).Size();

    if (weight_size > kMaxStateWeight) [[unlikely]] {
      SDB_ASSERT(false);
      return false;
    }

    const auto arcs = fst.Arcs(s);
    SDB_ASSERT(arcs.size() <= kMaxArcs);

    out.WriteV64(ShiftPack64(weight_size, arcs.empty()));
    if (!arcs.empty()) {
      // -1 to fit byte_type
      out.WriteByte(static_cast<byte_type>((arcs.size() - 1) & 0xFF));

      for (const auto& arc : arcs) {
        SDB_ASSERT(arc.ilabel <= std::numeric_limits<byte_type>::max());
        out.WriteByte(static_cast<byte_type>(arc.ilabel & 0xFF));
        out.WriteV32(arc.nextstate);
        out.WriteV64(arc.weight.Size());
      }
    }
  }

  for (StateId s = 0, n = fst.NumStates(); s != n; ++s) {
    if (const auto& weight = fst.Final(s); !weight.Empty()) {
      out.WriteData(weight.c_str(), weight.Size());
    }

    for (const auto& arc : fst.Arcs(s)) {
      if (!arc.weight.Empty()) {
        out.WriteData(arc.weight.c_str(), arc.weight.Size());
      }
    }
  }

  return true;
}

using byte_ref_arc = ImmutableByteArc;
using immutable_byte_fst = ImmutableByteFst;

}  // namespace irs
