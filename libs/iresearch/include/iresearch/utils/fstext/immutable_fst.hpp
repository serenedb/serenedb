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

// clang-format off
#include <fst/fst.h>
#include <fst/vector-fst.h>
#include <fst/expanded-fst.h>
// clang-format on

#include "basics/down_cast.h"
#include "basics/misc.hpp"
#include "basics/resource_manager.hpp"
#include "basics/shared.hpp"
#include "iresearch/store/store_utils.hpp"

namespace fst {
namespace fstext {

template<typename Arc>
class ImmutableFst;

template<typename A>
class ImmutableFstImpl : public internal::FstImpl<A> {
 public:
  using Arc = A;
  using StateId = typename Arc::StateId;
  using Weight = typename Arc::Weight;

  using internal::FstImpl<A>::SetInputSymbols;
  using internal::FstImpl<A>::SetOutputSymbols;
  using internal::FstImpl<A>::SetType;
  using internal::FstImpl<A>::SetProperties;
  using internal::FstImpl<A>::Properties;

  static constexpr const char kTypePrefix[] = "immutable";
  static constexpr size_t kMaxArcs = 1 + std::numeric_limits<uint8_t>::max();
  static constexpr size_t kMaxStateWeight =
    std::numeric_limits<size_t>::max() >> 1;

  ImmutableFstImpl() : _narcs(0), _nstates(0), _start(kNoStateId) {
    SetType(std::string(kTypePrefix) + "<" + Arc::Type() + ">");
    SetProperties(kNullProperties | kStaticProperties);
  }

  ~ImmutableFstImpl() override {
    _states.reset();
    _arcs.reset();
    _weights.reset();
    SDB_ASSERT(_resource_manager);
    _resource_manager->Decrease(sizeof(State) * _nstates +
                                sizeof(Arc) * _narcs +
                                sizeof(irs::byte_type) * _weights_size);
  }

  StateId Start() const noexcept { return _start; }

  Weight Final(StateId s) const noexcept { return _states[s].weight; }

  const Weight& FinalRef(StateId s) const noexcept { return _states[s].weight; }

  StateId NumStates() const noexcept { return _nstates; }

  size_t NumArcs(StateId s) const noexcept { return _states[s].narcs; }

  size_t NumInputEpsilons(StateId) const noexcept { return 0; }

  size_t NumOutputEpsilons(StateId) const noexcept { return 0; }

  static std::shared_ptr<ImmutableFstImpl<Arc>> Read(irs::DataInput& strm,
                                                     irs::IResourceManager& rm);

  const Arc* Arcs(StateId s) const noexcept { return _states[s].arcs; }

  // Provide information needed for generic state iterator.
  void InitStateIterator(StateIteratorData<Arc>* data) const noexcept {
    data->base = nullptr;
    data->nstates = _nstates;
  }

  // Provide information needed for the generic arc iterator.
  void InitArcIterator(StateId s, ArcIteratorData<Arc>* data) const noexcept {
    data->base = nullptr;
    data->arcs = _states[s].arcs;
    data->narcs = _states[s].narcs;
    data->ref_count = nullptr;
  }

 private:
  friend class ImmutableFst<Arc>;

  struct State {
    const Arc* arcs;  // Start of state's arcs in *arcs_.
    size_t narcs;     // Number of arcs (per state).
    Weight weight;    // Final weight.
  };

  // Properties always true of this FST class.
  static constexpr uint64_t kStaticProperties = kExpanded;

  std::unique_ptr<State[]> _states;
  std::unique_ptr<Arc[]> _arcs;
  std::unique_ptr<irs::byte_type[]> _weights;
  size_t _narcs;  // Number of arcs.
  size_t _weights_size;
  StateId _nstates;  // Number of states.
  StateId _start;    // Initial state.
  irs::IResourceManager* _resource_manager{&irs::IResourceManager::gNoop};

  ImmutableFstImpl(const ImmutableFstImpl&) = delete;
  ImmutableFstImpl& operator=(const ImmutableFstImpl&) = delete;
};

template<typename Arc>
std::shared_ptr<ImmutableFstImpl<Arc>> ImmutableFstImpl<Arc>::Read(
  irs::DataInput& stream, irs::IResourceManager& rm) {
  auto impl = std::make_shared<ImmutableFstImpl<Arc>>();

  const uint64_t props = stream.ReadI64();
  const size_t total_weight_size = stream.ReadI64();
  const StateId nstates = stream.ReadI32();
  const StateId start = nstates - stream.ReadV32();
  const size_t narcs = irs::ReadZV64(stream) + nstates;

  size_t allocated{nstates * sizeof(State) + narcs * sizeof(Arc) +
                   total_weight_size * sizeof(irs::byte_type)};
  irs::Finally cleanup = [&]() noexcept { rm.DecreaseChecked(allocated); };

  rm.Increase(allocated);
  auto states = std::make_unique<State[]>(nstates);
  auto arcs = std::make_unique<Arc[]>(narcs);
  auto weights = std::make_unique<irs::byte_type[]>(total_weight_size);

  // read states & arcs
  auto* weight = weights.get();
  auto* arc = arcs.get();
  for (auto state = states.get(), end = state + nstates; state != end;
       ++state) {
    state->arcs = arc;

    size_t weight_size = stream.ReadV64();
    const bool has_arcs = !irs::ShiftUnpack64(weight_size, weight_size);
    state->weight = {weight, weight_size};
    weight += weight_size;

    if (has_arcs) {
      state->narcs = static_cast<uint32_t>(stream.ReadByte()) + 1;

      for (auto* end = arc + state->narcs; arc != end; ++arc) {
        arc->ilabel = stream.ReadByte();
        arc->nextstate = stream.ReadV32();
        const size_t weight_size = stream.ReadV64();
        arc->weight = {weight, weight_size};
        weight += weight_size;
      }
    } else {
      state->narcs = 0;
    }
  }

  // read weights
  stream.ReadBytes(weights.get(), total_weight_size);

  // noexcept block
  allocated = 0;
  impl->properties_ = props;
  impl->_start = start;
  impl->_nstates = nstates;
  impl->_narcs = narcs;
  impl->_states = std::move(states);
  impl->_arcs = std::move(arcs);
  impl->_weights = std::move(weights);
  impl->_weights_size = total_weight_size;
  impl->_resource_manager = &rm;
  return impl;
}

template<typename A>
class ImmutableFst : public ImplToExpandedFst<ImmutableFstImpl<A>> {
 public:
  using Arc = A;
  using StateId = typename Arc::StateId;

  using Impl = ImmutableFstImpl<A>;

  friend class StateIterator<ImmutableFst<Arc>>;
  friend class ArcIterator<ImmutableFst<Arc>>;

  template<typename F, typename G>
  void friend Cast(const F&, G*);

  ImmutableFst() : ImplToExpandedFst<Impl>(std::make_shared<Impl>()) {}

  explicit ImmutableFst(const ImmutableFst<A>& fst,
                        [[maybe_unused]] bool safe = false)
    : ImplToExpandedFst<Impl>(fst) {}

  // Gets a copy of this ConstFst. See Fst<>::Copy() for further doc.
  ImmutableFst<A>* Copy(bool safe = false) const final {
    return new ImmutableFst<A>(*this, safe);
  }

  static ImmutableFst<A>* Read(irs::DataInput& strm,
                               irs::IResourceManager& rm) {
    auto impl = Impl::Read(strm, rm);
    return impl ? new ImmutableFst<A>(std::move(impl)) : nullptr;
  }

  // OpenFST API compliance broken. But as only we use it here it is ok.
  static ImmutableFst<A>* Read(std::istream& strm,
                               const FstReadOptions& /*opts*/,
                               irs::IResourceManager& rm) {
    auto* rdbuf = sdb::basics::downCast<irs::InputBuf>(strm.rdbuf());
    SDB_ASSERT(rdbuf && rdbuf->Internal());

    return Read(*rdbuf->Internal(), rm);
  }

  template<typename FST, typename Stats>
  static bool Write(const FST& fst, irs::BufferedOutput& strm,
                    const Stats& stats);

  void InitStateIterator(StateIteratorData<Arc>* data) const final {
    GetImpl()->InitStateIterator(data);
  }

  void InitArcIterator(StateId s, ArcIteratorData<Arc>* data) const final {
    GetImpl()->InitArcIterator(s, data);
  }

  using ImplToFst<Impl, ExpandedFst<Arc>>::GetImpl;

 private:
  explicit ImmutableFst(std::shared_ptr<Impl> impl)
    : ImplToExpandedFst<Impl>(impl) {}

  using ImplToExpandedFst<ImmutableFstImpl<A>>::Write;

  ImmutableFst(const ImmutableFst&) = delete;
  ImmutableFst& operator=(const ImmutableFst&) = delete;
};

template<typename A>
template<typename FST, typename Stats>
bool ImmutableFst<A>::Write(const FST& fst, irs::BufferedOutput& stream,
                            const Stats& stats) {
  static_assert(sizeof(StateId) == sizeof(uint32_t));

  auto* impl = fst.GetImpl();
  SDB_ASSERT(impl);

  const auto properties =
    fst.Properties(kCopyProperties, true) | Impl::kStaticProperties;

  // write header
  stream.WriteU64(properties);
  stream.WriteU64(stats.total_weight_size);
  stream.WriteU32(static_cast<StateId>(stats.num_states));
  SDB_ASSERT(stats.num_states >= static_cast<size_t>(fst.Start()));
  stream.WriteV32(static_cast<uint32_t>(stats.num_states - fst.Start()));
  irs::WriteZV64(stream, stats.num_arcs - stats.num_states);

  // write states & arcs
  for (StateIterator<FST> siter(fst); !siter.Done(); siter.Next()) {
    const StateId s = siter.Value();

    static_assert(std::is_reference_v<decltype(impl->Final(s))>);
    size_t weight_size = impl->Final(s).Size();

    if (weight_size <= Impl::kMaxStateWeight) [[likely]] {
      const size_t narcs = impl->NumArcs(s);
      SDB_ASSERT(narcs <= Impl::kMaxArcs);

      stream.WriteV64(irs::ShiftPack64(weight_size, !narcs));
      if (narcs) {
        // -1 to fit byte_type
        stream.WriteByte(static_cast<irs::byte_type>((narcs - 1) & 0xFF));

        for (ArcIterator<FST> aiter(fst, s); !aiter.Done(); aiter.Next()) {
          const auto& arc = aiter.Value();

          SDB_ASSERT(arc.ilabel <= std::numeric_limits<irs::byte_type>::max());
          stream.WriteByte(static_cast<irs::byte_type>(arc.ilabel & 0xFF));
          stream.WriteV32(arc.nextstate);
          stream.WriteV64(arc.weight.Size());
        }
      }
    } else {
      SDB_ASSERT(false);
      return false;
    }
  }

  // write weights
  for (StateIterator<FST> siter(fst); !siter.Done(); siter.Next()) {
    const StateId s = siter.Value();

    static_assert(std::is_reference_v<decltype(impl->Final(s))>);
    if (const auto& weight = impl->Final(s); !weight.Empty()) {
      stream.WriteBytes(weight.c_str(), weight.Size());
    }

    for (ArcIterator<FST> aiter(fst, s); !aiter.Done(); aiter.Next()) {
      const auto& weight = aiter.Value().weight;

      if (!weight.Empty()) {
        stream.WriteBytes(weight.c_str(), weight.Size());
      }
    }
  }

  return true;
}

}  // namespace fstext

// Specialization for ConstFst; see generic version in fst.h for sample usage
// (but use the ConstFst type instead). This version should inline.
template<typename Arc>
class StateIterator<fstext::ImmutableFst<Arc>> {
 public:
  using StateId = typename Arc::StateId;

  explicit StateIterator(const fstext::ImmutableFst<Arc>& fst)
    : _nstates(fst.GetImpl()->NumStates()), _s(0) {}

  bool Done() const noexcept { return _s >= _nstates; }

  StateId Value() const noexcept { return _s; }

  void Next() noexcept { ++_s; }

  void Reset() noexcept { _s = 0; }

 private:
  const StateId _nstates;
  StateId _s;
};

// Specialization for ConstFst; see generic version in fst.h for sample usage
// (but use the ConstFst type instead). This version should inline.
template<typename Arc>
class ArcIterator<fstext::ImmutableFst<Arc>> {
 public:
  using StateId = typename Arc::StateId;

  ArcIterator(const fstext::ImmutableFst<Arc>& fst, StateId s)
    : _arcs(fst.GetImpl()->Arcs(s)),
      _begin(_arcs),
      _end(_arcs + fst.GetImpl()->NumArcs(s)) {}

  bool Done() const noexcept { return _begin >= _end; }

  const Arc& Value() const noexcept { return *_begin; }

  void Next() noexcept { ++_begin; }

  size_t Position() const noexcept {
    return size_t(std::distance(_arcs, _begin));
  }

  void Reset() noexcept { _begin = _arcs; }

  void Seek(size_t a) noexcept { _begin = _arcs + a; }

  constexpr uint32_t Flags() const { return kArcValueFlags; }

  void SetFlags(uint32_t, uint32_t) {}

 private:
  const Arc* _arcs;
  const Arc* _begin;
  const Arc* _end;
};

}  // namespace fst
