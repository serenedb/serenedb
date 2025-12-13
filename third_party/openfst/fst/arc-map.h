// Copyright 2005-2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the 'License');
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// See www.openfst.org for extensive documentation on this weighted
// finite-state transducer library.
//
// Class to map over/transform arcs e.g., change semirings or
// implement project/invert. Consider using when operation does
// not change the number of arcs (except possibly superfinal arcs).

#ifndef FST_ARC_MAP_H_
#define FST_ARC_MAP_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include <fst/log.h>
#include <fst/arc.h>
#include <fst/cache.h>
#include <fst/expanded-fst.h>
#include <fst/float-weight.h>
#include <fst/fst.h>
#include <fst/impl-to-fst.h>
#include <fst/mutable-fst.h>
#include <fst/properties.h>
#include <fst/string-weight.h>
#include <fst/symbol-table.h>
#include <fst/util.h>
#include <fst/weight.h>
#include <unordered_map>

namespace fst {

// Determines how final weights are mapped.
enum MapFinalAction {
  // A final weight is mapped into a final weight. An error is raised if this
  // is not possible.
  MAP_NO_SUPERFINAL,
  // A final weight is mapped to an arc to the superfinal state when the result
  // cannot be represented as a final weight. The superfinal state will be
  // added only if it is needed.
  MAP_ALLOW_SUPERFINAL,
  // A final weight is mapped to an arc to the superfinal state unless the
  // result can be represented as a final weight of weight Zero(). The
  // superfinal state is always added (if the input is not the empty FST).
  MAP_REQUIRE_SUPERFINAL
};

// Determines how symbol tables are mapped.
enum MapSymbolsAction {
  // Symbols should be cleared in the result by the map.
  MAP_CLEAR_SYMBOLS,
  // Symbols should be copied from the input FST by the map.
  MAP_COPY_SYMBOLS,
  // Symbols should not be modified in the result by the map itself.
  // (They may set by the mapper).
  MAP_NOOP_SYMBOLS
};

// Determines whether to propagate the kExpanded property, and by extension,
// whether the `ArcMapFst` is an `ExpandedFst` or not.
enum class PropagateKExpanded {
  // The `ArcMapFst` will neither be an `ExpandedFst` nor require one as input.
  kNo,
  // The `ArcMapFst` will both be an `ExpandedFst` and require one as input, as
  // long as the mapper it is templated on can support this.
  kIfPossible
  // TODO(wolfsonkin): Add a kYes option.
};

// The ArcMapper interfaces defines how arcs and final weights are mapped.
// This is useful for implementing operations that apply to each arc separately
// and do not change the number of arcs (except possibly superfinal arcs).
//
// Having the default constructor, `Properties`, and `FinalAction` as constexpr
// can allow for a special optimization to be taken for `ArcMapFst` where it can
// be an `ExpandedFst`.
//
// template <class A, class B>
// class ArcMapper {
//  public:
//   using FromArc = A;
//   using ToArc = B;
//
//   // Maps an arc type FromArc to arc type ToArc.
//   ToArc operator()(const FromArc &arc);
//
//   // Specifies final action the mapper requires (see above).
//   // The mapper will be passed final weights as arcs of the form
//   // Arc(0, 0, weight, kNoStateId).
//   MapFinalAction FinalAction() const;
//
//   // Specifies input symbol table action the mapper requires (see above).
//   MapSymbolsAction InputSymbolsAction() const;
//
//   // Specifies output symbol table action the mapper requires (see above).
//   MapSymbolsAction OutputSymbolsAction() const;
//
//   // This specifies the known properties of an FST mapped by this mapper. It
//   takes as argument the input FSTs's known properties.
//   uint64_t Properties(uint64_t props) const;
// };
//
// The ArcMap functions and classes below will use the FinalAction()
// method of the mapper to determine how to treat final weights, e.g., whether
// to add a superfinal state. They will use the Properties() method to set the
// result FST properties.
//
// We include a various map versions below. One dimension of variation is
// whether the mapping mutates its input, writes to a new result FST, or is an
// on-the-fly FST. Another dimension is how we pass the mapper. We allow passing
// the mapper by pointer for cases that we need to change the state of the
// user's mapper. This is the case with the EncodeMapper, which is reused
// during decoding. We also include map versions that pass the mapper by value
// or const reference when this suffices.

// Maps an arc type A using a mapper function object C, passed
// by pointer. This version modifies its Fst input.
template <class A, class C>
void ArcMap(MutableFst<A> *fst, C *mapper) {
  using FromArc = A;
  using ToArc = A;
  using Weight = typename FromArc::Weight;
  if (mapper->InputSymbolsAction() == MAP_CLEAR_SYMBOLS) {
    fst->SetInputSymbols(nullptr);
  }
  if (mapper->OutputSymbolsAction() == MAP_CLEAR_SYMBOLS) {
    fst->SetOutputSymbols(nullptr);
  }
  if (fst->Start() == kNoStateId) return;
  const auto props = fst->Properties(kFstProperties, false);
  const auto final_action = mapper->FinalAction();
  auto superfinal = kNoStateId;
  if (final_action == MAP_REQUIRE_SUPERFINAL) {
    superfinal = fst->AddState();
    fst->SetFinal(superfinal);
  }
  for (StateIterator<MutableFst<FromArc>> siter(*fst); !siter.Done();
       siter.Next()) {
    const auto state = siter.Value();
    for (MutableArcIterator<MutableFst<FromArc>> aiter(fst, state);
         !aiter.Done(); aiter.Next()) {
      const auto &arc = aiter.Value();
      aiter.SetValue((*mapper)(arc));
    }
    switch (final_action) {
      case MAP_NO_SUPERFINAL:
      default: {
        const FromArc arc(0, 0, fst->Final(state), kNoStateId);
        const auto final_arc = (*mapper)(arc);
        if (final_arc.ilabel != 0 || final_arc.olabel != 0) {
          FSTERROR() << "ArcMap: Non-zero arc labels for superfinal arc";
          fst->SetProperties(kError, kError);
        }
        fst->SetFinal(state, final_arc.weight);
        break;
      }
      case MAP_ALLOW_SUPERFINAL: {
        if (state != superfinal) {
          const FromArc arc(0, 0, fst->Final(state), kNoStateId);
          auto final_arc = (*mapper)(arc);
          if (final_arc.ilabel != 0 || final_arc.olabel != 0) {
            // Add a superfinal state if not already done.
            if (superfinal == kNoStateId) {
              superfinal = fst->AddState();
              fst->SetFinal(superfinal);
            }
            final_arc.nextstate = superfinal;
            fst->AddArc(state, std::move(final_arc));
            fst->SetFinal(state, Weight::Zero());
          } else {
            fst->SetFinal(state, final_arc.weight);
          }
        }
        break;
      }
      case MAP_REQUIRE_SUPERFINAL: {
        if (state != superfinal) {
          const FromArc arc(0, 0, fst->Final(state), kNoStateId);
          const auto final_arc = (*mapper)(arc);
          if (final_arc.ilabel != 0 || final_arc.olabel != 0 ||
              final_arc.weight != Weight::Zero()) {
            fst->AddArc(state, ToArc(final_arc.ilabel, final_arc.olabel,
                                     final_arc.weight, superfinal));
          }
          fst->SetFinal(state, Weight::Zero());
        }
        break;
      }
    }
  }
  fst->SetProperties(mapper->Properties(props), kFstProperties);
}

// Maps an arc type A using a mapper function object C, passed by value. This
// version modifies its FST input.
template <class A, class C>
void ArcMap(MutableFst<A> *fst, C mapper) {
  ArcMap(fst, &mapper);
}

// Maps an arc type A to an arc type B using mapper function object C,
// passed by pointer. This version writes the mapped input FST to an
// output MutableFst.
template <class A, class B, class C>
void ArcMap(const Fst<A> &ifst, MutableFst<B> *ofst, C *mapper) {
  using FromArc = A;
  using StateId = typename FromArc::StateId;
  ofst->DeleteStates();
  if (mapper->InputSymbolsAction() == MAP_COPY_SYMBOLS) {
    ofst->SetInputSymbols(ifst.InputSymbols());
  } else if (mapper->InputSymbolsAction() == MAP_CLEAR_SYMBOLS) {
    ofst->SetInputSymbols(nullptr);
  }
  if (mapper->OutputSymbolsAction() == MAP_COPY_SYMBOLS) {
    ofst->SetOutputSymbols(ifst.OutputSymbols());
  } else if (mapper->OutputSymbolsAction() == MAP_CLEAR_SYMBOLS) {
    ofst->SetOutputSymbols(nullptr);
  }
  const auto iprops = ifst.Properties(kCopyProperties, false);
  if (ifst.Start() == kNoStateId) {
    if (iprops & kError) ofst->SetProperties(kError, kError);
    return;
  }
  const auto final_action = mapper->FinalAction();
  if (std::optional<StateId> num_states = ifst.NumStatesIfKnown()) {
    ofst->ReserveStates(*num_states +
                        (final_action == MAP_NO_SUPERFINAL ? 0 : 1));
  }
  // Adds all states.
  for (StateIterator<Fst<A>> siter(ifst); !siter.Done(); siter.Next()) {
    ofst->AddState();
  }
  StateId superfinal = kNoStateId;
  if (final_action == MAP_REQUIRE_SUPERFINAL) {
    superfinal = ofst->AddState();
    ofst->SetFinal(superfinal);
  }
  for (StateIterator<Fst<A>> siter(ifst); !siter.Done(); siter.Next()) {
    StateId s = siter.Value();
    if (s == ifst.Start()) ofst->SetStart(s);
    ofst->ReserveArcs(
        s, ifst.NumArcs(s) + (final_action != MAP_NO_SUPERFINAL ? 1 : 0));
    for (ArcIterator<Fst<A>> aiter(ifst, s); !aiter.Done(); aiter.Next()) {
      ofst->AddArc(s, (*mapper)(aiter.Value()));
    }
    switch (final_action) {
      case MAP_NO_SUPERFINAL:
      default: {
        B final_arc = (*mapper)(A(0, 0, ifst.Final(s), kNoStateId));
        if (final_arc.ilabel != 0 || final_arc.olabel != 0) {
          FSTERROR() << "ArcMap: Non-zero arc labels for superfinal arc";
          ofst->SetProperties(kError, kError);
        }
        ofst->SetFinal(s, final_arc.weight);
        break;
      }
      case MAP_ALLOW_SUPERFINAL: {
        B final_arc = (*mapper)(A(0, 0, ifst.Final(s), kNoStateId));
        if (final_arc.ilabel != 0 || final_arc.olabel != 0) {
          // Add a superfinal state if not already done.
          if (superfinal == kNoStateId) {
            superfinal = ofst->AddState();
            ofst->SetFinal(superfinal);
          }
          final_arc.nextstate = superfinal;
          ofst->AddArc(s, std::move(final_arc));
          ofst->SetFinal(s, B::Weight::Zero());
        } else {
          ofst->SetFinal(s, final_arc.weight);
        }
        break;
      }
      case MAP_REQUIRE_SUPERFINAL: {
        B final_arc = (*mapper)(A(0, 0, ifst.Final(s), kNoStateId));
        if (final_arc.ilabel != 0 || final_arc.olabel != 0 ||
            final_arc.weight != B::Weight::Zero()) {
          ofst->AddArc(s, B(final_arc.ilabel, final_arc.olabel,
                            final_arc.weight, superfinal));
        }
        ofst->SetFinal(s, B::Weight::Zero());
        break;
      }
    }
  }
  const auto oprops = ofst->Properties(kFstProperties, false);
  ofst->SetProperties(mapper->Properties(iprops) | oprops, kFstProperties);
}

// Maps an arc type A to an arc type B using mapper function
// object C, passed by value. This version writes the mapped input
// Fst to an output MutableFst.
template <class A, class B, class C>
void ArcMap(const Fst<A> &ifst, MutableFst<B> *ofst, C mapper) {
  ArcMap(ifst, ofst, &mapper);
}

struct ArcMapFstOptions : public CacheOptions {
  // ArcMapFst default caching behaviour is to do no caching. Most mappers are
  // cheap and therefore we save memory by not doing caching.
  ArcMapFstOptions() : CacheOptions(true, 0) {}

  explicit ArcMapFstOptions(const CacheOptions &opts) : CacheOptions(opts) {}
};

template <class A, class B, class C, class CacheStore,
          PropagateKExpanded propagate_expanded_fst>
class ArcMapFst;

namespace internal {

// ExtractOr<E, O>::type evaluates to E<O> if possible. Otherwise,
// std::false_type.
template <template <typename> class Extract, typename Obj, typename>
struct ExtractOr {
  using type = std::false_type;
};
template <template <typename> class Extract, typename Obj>
struct ExtractOr<Extract, Obj, std::void_t<Extract<Obj>>> {
  using type = Extract<Obj>;
};

template <template <typename> class Extract, typename Obj>
using ExtractOrT = typename ExtractOr<Extract, Obj, void>::type;

// If the mapper conserves the expanded property and doesn't create a superfinal
// state, then in the `ArcMapFst` case, we can make `ArcMapFst` both accept and
// be an `ExpandedFst`.
// TODO(wolfsonkin): Support kExpanded propagation for other values of
// `FinalAction()`.
template <typename C>
using CoreConditions =
    std::bool_constant<(C{}.FinalAction() == MAP_NO_SUPERFINAL) &&
                       (C{}.Properties(kExpanded) == kExpanded)>;
// If the mapper is default constructible and the input FST is expanded, then
// the output FST can be expanded.
template <typename C>
using IsDefaultConstructibleExpandedNoSuperfinalArcMapper =
    ExtractOrT<CoreConditions, C>;

// Implementation of delayed ArcMapFst. If `is_expanded` is true, then the
// implementation will assume the input FST is an `ExpandedFst`, and define
// `NumStates()`. Otherwise, the `ArcMapFst` will act on any `Fst` input, and
// calling its `NumStates()` method will not compile.
template <class A, class B, class C, class CacheStore = DefaultCacheStore<B>,
          bool is_expanded = false>
class ArcMapFstImpl
    : public CacheBaseImpl<typename CacheStore::State, CacheStore> {
 public:
  using Arc = B;
  using StateId = typename Arc::StateId;
  using Weight = typename Arc::Weight;
  using FromFst = std::conditional_t<is_expanded, ExpandedFst<A>, Fst<A>>;

  using FstImpl<B>::SetType;
  using FstImpl<B>::SetProperties;
  using FstImpl<B>::SetInputSymbols;
  using FstImpl<B>::SetOutputSymbols;

  using State = typename CacheStore::State;
  using CacheImpl = CacheBaseImpl<State, CacheStore>;
  using CacheImpl::EmplaceArc;
  using CacheImpl::HasArcs;
  using CacheImpl::HasFinal;
  using CacheImpl::HasStart;
  using CacheImpl::PushArc;
  using CacheImpl::SetArcs;
  using CacheImpl::SetFinal;
  using CacheImpl::SetStart;

  friend class StateIterator<
      ArcMapFst<A, B, C, CacheStore, PropagateKExpanded::kIfPossible>>;
  friend class StateIterator<
      ArcMapFst<A, B, C, CacheStore, PropagateKExpanded::kNo>>;

  ArcMapFstImpl(const FromFst &fst, const C &mapper,
                const ArcMapFstOptions &opts)
      : CacheImpl(opts),
        fst_(fst.Copy()),
        mapper_(new C(mapper)),
        own_mapper_(true),
        superfinal_(kNoStateId),
        nstates_(0) {
    Init();
  }

  ArcMapFstImpl(const FromFst &fst, C *mapper, const ArcMapFstOptions &opts)
      : CacheImpl(opts),
        fst_(fst.Copy()),
        mapper_(mapper),
        own_mapper_(false),
        superfinal_(kNoStateId),
        nstates_(0) {
    Init();
  }

  ArcMapFstImpl(const ArcMapFstImpl &impl)
      : CacheImpl(impl),
        fst_(impl.fst_->Copy(true)),
        mapper_(new C(*impl.mapper_)),
        own_mapper_(true),
        superfinal_(kNoStateId),
        nstates_(0) {
    Init();
  }

  ~ArcMapFstImpl() override {
    if (own_mapper_) delete mapper_;
  }

  StateId Start() {
    if (!HasStart()) SetStart(FindOState(fst_->Start()));
    return CacheImpl::Start();
  }

  Weight Final(StateId s) {
    if (!HasFinal(s)) {
      switch (final_action_) {
        case MAP_NO_SUPERFINAL:
        default: {
          const auto final_arc =
              (*mapper_)(A(0, 0, fst_->Final(FindIState(s)), kNoStateId));
          if (final_arc.ilabel != 0 || final_arc.olabel != 0) {
            FSTERROR() << "ArcMapFst: Non-zero arc labels for superfinal arc";
            SetProperties(kError, kError);
          }
          SetFinal(s, final_arc.weight);
          break;
        }
        case MAP_ALLOW_SUPERFINAL: {
          if (s == superfinal_) {
            SetFinal(s);
          } else {
            const auto final_arc =
                (*mapper_)(A(0, 0, fst_->Final(FindIState(s)), kNoStateId));
            if (final_arc.ilabel == 0 && final_arc.olabel == 0) {
              SetFinal(s, final_arc.weight);
            } else {
              SetFinal(s, Weight::Zero());
            }
          }
          break;
        }
        case MAP_REQUIRE_SUPERFINAL: {
          SetFinal(s, s == superfinal_ ? Weight::One() : Weight::Zero());
          break;
        }
      }
    }
    return CacheImpl::Final(s);
  }

  size_t NumArcs(StateId s) {
    if (final_action_ == MAP_NO_SUPERFINAL) return fst_->NumArcs(s);
    if (!HasArcs(s)) Expand(s);
    return CacheImpl::NumArcs(s);
  }

  size_t NumInputEpsilons(StateId s) {
    if (!HasArcs(s)) Expand(s);
    return CacheImpl::NumInputEpsilons(s);
  }

  size_t NumOutputEpsilons(StateId s) {
    if (!HasArcs(s)) Expand(s);
    return CacheImpl::NumOutputEpsilons(s);
  }

  // This should only be called when `fst_` is known to be an `ExpandedFst`.
  StateId NumStates() const {
    static_assert(is_expanded,
                  "NumStates() only supported if the input is an ExpandedFst");
    static_assert(CoreConditions<C>::value,
                  "NumStates() only supported for mappers that conserve the "
                  "expanded property and don't create a superfinal state");
    return fst_->NumStates();
  }

  uint64_t Properties() const override { return Properties(kFstProperties); }

  // Sets error if found, and returns other FST impl properties.
  uint64_t Properties(uint64_t mask) const override {
    if ((mask & kError) && (fst_->Properties(kError, false) ||
                            (mapper_->Properties(0) & kError))) {
      SetProperties(kError, kError);
    }
    return FstImpl<Arc>::Properties(mask);
  }

  void InitArcIterator(StateId s, ArcIteratorData<B> *data) {
    if (!HasArcs(s)) Expand(s);
    CacheImpl::InitArcIterator(s, data);
  }

  void Expand(StateId s) {
    // Add exiting arcs.
    if (s == superfinal_) {
      SetArcs(s);
      return;
    }
    for (ArcIterator<FromFst> aiter(*fst_, FindIState(s)); !aiter.Done();
         aiter.Next()) {
      auto aarc = aiter.Value();
      aarc.nextstate = FindOState(aarc.nextstate);
      PushArc(s, (*mapper_)(aarc));
    }

    // Check for superfinal arcs.
    if (!HasFinal(s) || Final(s) == Weight::Zero()) {
      switch (final_action_) {
        case MAP_NO_SUPERFINAL:
        default:
          break;
        case MAP_ALLOW_SUPERFINAL: {
          auto final_arc =
              (*mapper_)(A(0, 0, fst_->Final(FindIState(s)), kNoStateId));
          if (final_arc.ilabel != 0 || final_arc.olabel != 0) {
            if (superfinal_ == kNoStateId) superfinal_ = nstates_++;
            final_arc.nextstate = superfinal_;
            PushArc(s, std::move(final_arc));
          }
          break;
        }
        case MAP_REQUIRE_SUPERFINAL: {
          const auto final_arc =
              (*mapper_)(A(0, 0, fst_->Final(FindIState(s)), kNoStateId));
          if (final_arc.ilabel != 0 || final_arc.olabel != 0 ||
              final_arc.weight != B::Weight::Zero()) {
            EmplaceArc(s, final_arc.ilabel, final_arc.olabel, final_arc.weight,
                       superfinal_);
          }
          break;
        }
      }
    }
    SetArcs(s);
  }

 private:
  void Init() {
    SetType("map");
    if (mapper_->InputSymbolsAction() == MAP_COPY_SYMBOLS) {
      SetInputSymbols(fst_->InputSymbols());
    } else if (mapper_->InputSymbolsAction() == MAP_CLEAR_SYMBOLS) {
      SetInputSymbols(nullptr);
    }
    if (mapper_->OutputSymbolsAction() == MAP_COPY_SYMBOLS) {
      SetOutputSymbols(fst_->OutputSymbols());
    } else if (mapper_->OutputSymbolsAction() == MAP_CLEAR_SYMBOLS) {
      SetOutputSymbols(nullptr);
    }
    if (fst_->Start() == kNoStateId) {
      final_action_ = MAP_NO_SUPERFINAL;
      SetProperties(kNullProperties);
    } else {
      final_action_ = mapper_->FinalAction();
      uint64_t props = fst_->Properties(kCopyProperties, false);
      SetProperties(mapper_->Properties(props));
      if (final_action_ == MAP_REQUIRE_SUPERFINAL) superfinal_ = 0;
    }
  }

  // Maps from output state to input state.
  StateId FindIState(StateId s) {
    if (superfinal_ == kNoStateId || s < superfinal_) {
      return s;
    } else {
      return s - 1;
    }
  }

  // Maps from input state to output state.
  StateId FindOState(StateId is) {
    auto os = is;
    if (!(superfinal_ == kNoStateId || is < superfinal_)) ++os;
    if (os >= nstates_) nstates_ = os + 1;
    return os;
  }

  std::unique_ptr<const FromFst> fst_;
  C *mapper_;
  const bool own_mapper_;
  MapFinalAction final_action_;
  StateId superfinal_;
  StateId nstates_;
};

template <class A, class B, class C, class CacheStore,
          PropagateKExpanded should_propagate_expanded_fst>
using ArcMapFstBase = std::conditional_t<
    should_propagate_expanded_fst == PropagateKExpanded::kIfPossible &&
        internal::IsDefaultConstructibleExpandedNoSuperfinalArcMapper<C>::value,
    ImplToExpandedFst<internal::ArcMapFstImpl<A, B, C, CacheStore,
                                              /*is_expanded=*/true>>,
    ImplToFst<internal::ArcMapFstImpl<A, B, C, CacheStore,
                                      /*is_expanded=*/false>>>;

}  // namespace internal

// Maps an arc type A to an arc type B using Mapper function object
// C. This version is a delayed FST. If `propagate_expanded_fst` is true,
// and the `ArcMapper` is known to be capable of maintaining `ExpandedFst<B>`
// status, the resulting `ArcMapFst` will be an `ExpandedFst<B>`, and will
// exclusively accept `ExpandedFst<B>`s as input in the constructor. Otherwise,
// it will be an `Fst<B>` that accepts `Fst<B>` as input.
// `propagate_expanded_fst` will be set automatically when using CTAD, but
// otherwise must be opted into manually if one needs to ensure to maintain
// `ExpandedFst` status.
template <class A, class B, class C, class CacheStore = DefaultCacheStore<B>,
          PropagateKExpanded propagate_expanded_fst = PropagateKExpanded::kNo>
class ArcMapFst : public internal::ArcMapFstBase<A, B, C, CacheStore,
                                                 propagate_expanded_fst> {
  using Base =
      internal::ArcMapFstBase<A, B, C, CacheStore, propagate_expanded_fst>;

 public:
  using Arc = B;
  using StateId = typename Arc::StateId;
  using Weight = typename Arc::Weight;

  using Store = CacheStore;
  using State = typename Store::State;
  using typename Base::Impl;
  using FromFst = typename Impl::FromFst;

  friend class ArcIterator<
      ArcMapFst<A, B, C, CacheStore, propagate_expanded_fst>>;
  friend class StateIterator<
      ArcMapFst<A, B, C, CacheStore, propagate_expanded_fst>>;

  explicit ArcMapFst(const FromFst &fst, const C &mapper = C(),
                     const ArcMapFstOptions &opts = ArcMapFstOptions())
      : Base(std::make_shared<Impl>(fst, mapper, opts)) {}

  ArcMapFst(const FromFst &fst, C *mapper,
            const ArcMapFstOptions &opts = ArcMapFstOptions())
      : Base(std::make_shared<Impl>(fst, mapper, opts)) {}

  // See Fst<>::Copy() for doc.
  ArcMapFst(const ArcMapFst &fst, bool safe = false) : Base(fst, safe) {}

  // Get a copy of this ArcMapFst. See Fst<>::Copy() for further doc.
  ArcMapFst *Copy(bool safe = false) const override {
    return new ArcMapFst(*this, safe);
  }

  inline void InitStateIterator(StateIteratorData<B> *data) const override;

  void InitArcIterator(StateId s, ArcIteratorData<B> *data) const override {
    GetMutableImpl()->InitArcIterator(s, data);
  }

 protected:
  using Base::GetImpl;
  using Base::GetMutableImpl;

 private:
  ArcMapFst &operator=(const ArcMapFst &) = delete;
};

// Specialization for ArcMapFst.
//
// This may be derived from.
template <class A, class B, class C, class CacheStore,
          PropagateKExpanded propagate_expanded_fst>
class StateIterator<ArcMapFst<A, B, C, CacheStore, propagate_expanded_fst>>
    : public StateIteratorBase<B> {
 public:
  using FST = ArcMapFst<A, B, C, CacheStore, propagate_expanded_fst>;
  using StateId = typename B::StateId;

  explicit StateIterator(const FST &fst)
      : impl_(fst.GetImpl()),
        siter_(*impl_->fst_),
        s_(0),
        superfinal_(impl_->final_action_ == MAP_REQUIRE_SUPERFINAL) {
    CheckSuperfinal();
  }

  bool Done() const final { return siter_.Done() && !superfinal_; }

  StateId Value() const final { return s_; }

  void Next() final {
    ++s_;
    if (!siter_.Done()) {
      siter_.Next();
      CheckSuperfinal();
    } else if (superfinal_) {
      superfinal_ = false;
    }
  }

  void Reset() final {
    s_ = 0;
    siter_.Reset();
    superfinal_ = impl_->final_action_ == MAP_REQUIRE_SUPERFINAL;
    CheckSuperfinal();
  }

 private:
  void CheckSuperfinal() {
    if (impl_->final_action_ != MAP_ALLOW_SUPERFINAL || superfinal_) return;
    if (!siter_.Done()) {
      const auto final_arc =
          (*impl_->mapper_)(A(0, 0, impl_->fst_->Final(s_), kNoStateId));
      if (final_arc.ilabel != 0 || final_arc.olabel != 0) superfinal_ = true;
    }
  }

  const typename FST::Impl *impl_;
  StateIterator<typename FST::FromFst> siter_;
  StateId s_;
  bool superfinal_;  // True if there is a superfinal state and not done.
};

// Specialization for ArcMapFst.
template <class A, class B, class C, class CacheStore,
          PropagateKExpanded propagate_expanded_fst>
class ArcIterator<ArcMapFst<A, B, C, CacheStore, propagate_expanded_fst>>
    : public CacheArcIterator<
          ArcMapFst<A, B, C, CacheStore, propagate_expanded_fst>> {
 public:
  using StateId = typename A::StateId;
  using FST = ArcMapFst<A, B, C, CacheStore, propagate_expanded_fst>;

  ArcIterator(const FST &fst, StateId s)
      : CacheArcIterator<FST>(fst.GetMutableImpl(), s) {
    if (!fst.GetImpl()->HasArcs(s)) fst.GetMutableImpl()->Expand(s);
  }
};

template <class A, class B, class C, class CacheStore,
          PropagateKExpanded propagate_expanded_fst>
inline void
ArcMapFst<A, B, C, CacheStore, propagate_expanded_fst>::InitStateIterator(
    StateIteratorData<B> *data) const {
  data->base = std::make_unique<StateIterator<ArcMapFst>>(*this);
}

// CTAD deduction guides
// This allows constructing ArcMapFsts without specifying all the types.
// If the constructor receives an `ExpandedFst` as the first argument, and the
// `ArcMapper` is known to be capable of maintaining `ExpandedFst` status, the
// resulting `ArcMapFst` will be an `ExpandedFst`. Otherwise, it will be an
// `Fst`

template <class ArcMapper>
ArcMapFst(const Fst<typename ArcMapper::FromArc> &, const ArcMapper &)
    -> ArcMapFst<typename ArcMapper::FromArc, typename ArcMapper::ToArc,
                 ArcMapper, DefaultCacheStore<typename ArcMapper::ToArc>,
                 PropagateKExpanded::kNo>;
template <class ArcMapper,
          typename = std::enable_if_t<
              internal::IsDefaultConstructibleExpandedNoSuperfinalArcMapper<
                  ArcMapper>::value>>
ArcMapFst(const ExpandedFst<typename ArcMapper::FromArc> &, const ArcMapper &)
    -> ArcMapFst<typename ArcMapper::FromArc, typename ArcMapper::ToArc,
                 ArcMapper, DefaultCacheStore<typename ArcMapper::ToArc>,
                 PropagateKExpanded::kIfPossible>;

// As above, but using the ArcMapFst(..., ArcMapper *) constructor.
template <class ArcMapper>
ArcMapFst(const Fst<typename ArcMapper::FromArc> &, ArcMapper *)
    -> ArcMapFst<typename ArcMapper::FromArc, typename ArcMapper::ToArc,
                 ArcMapper, DefaultCacheStore<typename ArcMapper::ToArc>,
                 PropagateKExpanded::kNo>;
template <class ArcMapper,
          typename = std::enable_if_t<
              internal::IsDefaultConstructibleExpandedNoSuperfinalArcMapper<
                  ArcMapper>::value>>
ArcMapFst(const ExpandedFst<typename ArcMapper::FromArc> &, ArcMapper *)
    -> ArcMapFst<typename ArcMapper::FromArc, typename ArcMapper::ToArc,
                 ArcMapper, DefaultCacheStore<typename ArcMapper::ToArc>,
                 PropagateKExpanded::kIfPossible>;

// Utility Mappers.

// Mapper that returns its input.
template <class A>
class IdentityArcMapper {
 public:
  using FromArc = A;
  using ToArc = A;

  constexpr ToArc operator()(const FromArc &arc) const { return arc; }

  constexpr MapFinalAction FinalAction() const { return MAP_NO_SUPERFINAL; }

  constexpr MapSymbolsAction InputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  constexpr MapSymbolsAction OutputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  constexpr uint64_t Properties(uint64_t props) const { return props; }
};

// Mapper that converts all input symbols to epsilon.
template <class A>
class InputEpsilonMapper {
 public:
  using FromArc = A;
  using ToArc = A;

  constexpr ToArc operator()(const FromArc &arc) const {
    return ToArc(0, arc.olabel, arc.weight, arc.nextstate);
  }

  constexpr MapFinalAction FinalAction() const { return MAP_NO_SUPERFINAL; }

  constexpr MapSymbolsAction InputSymbolsAction() const {
    return MAP_CLEAR_SYMBOLS;
  }

  constexpr MapSymbolsAction OutputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  constexpr uint64_t Properties(uint64_t props) const {
    return (props & kSetArcProperties) | kIEpsilons | kILabelSorted;
  }
};

// Mapper that converts all output symbols to epsilon.
template <class A>
class OutputEpsilonMapper {
 public:
  using FromArc = A;
  using ToArc = A;

  constexpr ToArc operator()(const FromArc &arc) const {
    return ToArc(arc.ilabel, 0, arc.weight, arc.nextstate);
  }

  constexpr MapFinalAction FinalAction() const { return MAP_NO_SUPERFINAL; }

  constexpr MapSymbolsAction InputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  constexpr MapSymbolsAction OutputSymbolsAction() const {
    return MAP_CLEAR_SYMBOLS;
  }

  constexpr uint64_t Properties(uint64_t props) const {
    return (props & kSetArcProperties) | kOEpsilons | kOLabelSorted;
  }
};

// Mapper that returns its input with final states redirected to a single
// super-final state.
template <class A>
class SuperFinalMapper {
 public:
  using FromArc = A;
  using ToArc = A;
  using Label = typename FromArc::Label;
  using Weight = typename FromArc::Weight;

  // Arg allows setting super-final label.
  constexpr explicit SuperFinalMapper(Label final_label = 0)
      : final_label_(final_label) {}

  ToArc operator()(const FromArc &arc) const {
    // Super-final arc.
    if (arc.nextstate == kNoStateId && arc.weight != Weight::Zero()) {
      return ToArc(final_label_, final_label_, arc.weight, kNoStateId);
    } else {
      return arc;
    }
  }

  constexpr MapFinalAction FinalAction() const {
    return MAP_REQUIRE_SUPERFINAL;
  }

  constexpr MapSymbolsAction InputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  constexpr MapSymbolsAction OutputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  uint64_t Properties(uint64_t props) const {
    if (final_label_ == 0) {
      return props & kAddSuperFinalProperties;
    } else {
      return props & kAddSuperFinalProperties & kILabelInvariantProperties &
             kOLabelInvariantProperties;
    }
  }

 private:
  const Label final_label_;
};

// Mapper that leaves labels and nextstate unchanged and constructs a new weight
// from the underlying value of the arc weight. If no weight converter is
// explictly specified, requires that there is a WeightConvert class
// specialization that converts the weights.
template <class A, class B,
          class C = WeightConvert<typename A::Weight, typename B::Weight>>
class WeightConvertMapper {
 public:
  using FromArc = A;
  using ToArc = B;
  using Converter = C;
  using FromWeight = typename FromArc::Weight;
  using ToWeight = typename ToArc::Weight;

  // NB: Declares the default constructor only if the converter is default
  // constructible.
  constexpr WeightConvertMapper() = default;

  constexpr explicit WeightConvertMapper(const Converter &c)
      : convert_weight_(c) {}

  constexpr ToArc operator()(const FromArc &arc) const {
    return ToArc(arc.ilabel, arc.olabel, convert_weight_(arc.weight),
                 arc.nextstate);
  }

  constexpr MapFinalAction FinalAction() const { return MAP_NO_SUPERFINAL; }

  constexpr MapSymbolsAction InputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  constexpr MapSymbolsAction OutputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  constexpr uint64_t Properties(uint64_t props) const { return props; }

 private:
  // NB: This is non-const only to work around compiler configurations not
  // implementing CWG defect report 2394:
  // https://www.open-std.org/jtc1/sc22/wg21/docs/cwg_defects.html#2394.
  Converter convert_weight_;
};

// Non-precision-changing weight conversions; consider using more efficient
// Cast method instead.

using StdToLogMapper = WeightConvertMapper<StdArc, LogArc>;

using LogToStdMapper = WeightConvertMapper<LogArc, StdArc>;

// Precision-changing weight conversions.

using StdToLog64Mapper = WeightConvertMapper<StdArc, Log64Arc>;

using LogToLog64Mapper = WeightConvertMapper<LogArc, Log64Arc>;

using Log64ToStdMapper = WeightConvertMapper<Log64Arc, StdArc>;

using Log64ToLogMapper = WeightConvertMapper<Log64Arc, LogArc>;

// Mapper from A to GallicArc<A>.
template <class A, GallicType G = GALLIC_LEFT>
class ToGallicMapper {
 public:
  using FromArc = A;
  using ToArc = GallicArc<A, G>;

  using SW = StringWeight<typename A::Label, GallicStringType(G)>;
  using AW = typename FromArc::Weight;
  using GW = typename ToArc::Weight;

  ToArc operator()(const FromArc &arc) const {
    // Super-final arc.
    if (arc.nextstate == kNoStateId && arc.weight != AW::Zero()) {
      return ToArc(0, 0, GW(SW::One(), arc.weight), kNoStateId);
      // Super-non-final arc.
    } else if (arc.nextstate == kNoStateId) {
      return ToArc(0, 0, GW::Zero(), kNoStateId);
      // Epsilon label.
    } else if (arc.olabel == 0) {
      return ToArc(arc.ilabel, arc.ilabel, GW(SW::One(), arc.weight),
                   arc.nextstate);
      // Regular label.
    } else {
      return ToArc(arc.ilabel, arc.ilabel, GW(SW(arc.olabel), arc.weight),
                   arc.nextstate);
    }
  }

  constexpr MapFinalAction FinalAction() const { return MAP_NO_SUPERFINAL; }

  constexpr MapSymbolsAction InputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  constexpr MapSymbolsAction OutputSymbolsAction() const {
    return MAP_CLEAR_SYMBOLS;
  }

  uint64_t Properties(uint64_t props) const {
    return ProjectProperties(props, true) & kWeightInvariantProperties;
  }
};

// Mapper from GallicArc<A> to A.
template <class A, GallicType G = GALLIC_LEFT>
class FromGallicMapper {
 public:
  using FromArc = GallicArc<A, G>;
  using ToArc = A;

  using Label = typename ToArc::Label;
  using AW = typename ToArc::Weight;
  using GW = typename FromArc::Weight;

  explicit FromGallicMapper(Label superfinal_label = 0)
      : superfinal_label_(superfinal_label), error_(false) {}

  ToArc operator()(const FromArc &arc) const {
    // 'Super-non-final' arc.
    if (arc.nextstate == kNoStateId && arc.weight == GW::Zero()) {
      return A(arc.ilabel, 0, AW::Zero(), kNoStateId);
    }
    Label l = kNoLabel;
    AW weight = AW::Zero();
    if (!Extract(arc.weight, &weight, &l) || arc.ilabel != arc.olabel) {
      FSTERROR() << "FromGallicMapper: Unrepresentable weight: " << arc.weight
                 << " for arc with ilabel = " << arc.ilabel
                 << ", olabel = " << arc.olabel
                 << ", nextstate = " << arc.nextstate;
      error_ = true;
    }
    if (arc.ilabel == 0 && l != 0 && arc.nextstate == kNoStateId) {
      return ToArc(superfinal_label_, l, weight, arc.nextstate);
    } else {
      return ToArc(arc.ilabel, l, weight, arc.nextstate);
    }
  }

  constexpr MapFinalAction FinalAction() const { return MAP_ALLOW_SUPERFINAL; }

  constexpr MapSymbolsAction InputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  constexpr MapSymbolsAction OutputSymbolsAction() const {
    return MAP_CLEAR_SYMBOLS;
  }

  uint64_t Properties(uint64_t inprops) const {
    uint64_t outprops = inprops & kOLabelInvariantProperties &
                        kWeightInvariantProperties & kAddSuperFinalProperties;
    if (error_) outprops |= kError;
    return outprops;
  }

 private:
  template <GallicType GT>
  static bool Extract(const GallicWeight<Label, AW, GT> &gallic_weight,
                      typename A::Weight *weight, typename A::Label *label) {
    using GW = StringWeight<Label, GallicStringType(GT)>;
    const GW &w1 = gallic_weight.Value1();
    const AW &w2 = gallic_weight.Value2();
    typename GW::Iterator iter1(w1);
    const Label l = w1.Size() == 1 ? iter1.Value() : 0;
    if (l == kStringInfinity || l == kStringBad || w1.Size() > 1) return false;
    *label = l;
    *weight = w2;
    return true;
  }

  static bool Extract(const GallicWeight<Label, AW, GALLIC> &gallic_weight,
                      typename A::Weight *weight, typename A::Label *label) {
    if (gallic_weight.Size() > 1) return false;
    if (gallic_weight.Size() == 0) {
      *label = 0;
      *weight = A::Weight::Zero();
      return true;
    }
    return Extract<GALLIC_RESTRICT>(gallic_weight.Back(), weight, label);
  }

  const Label superfinal_label_;
  mutable bool error_;
};

// Mapper from GallicArc<A> to A.
template <class A, GallicType G = GALLIC_LEFT>
class GallicToNewSymbolsMapper {
 public:
  using FromArc = GallicArc<A, G>;
  using ToArc = A;

  using Label = typename ToArc::Label;
  using StateId = typename ToArc::StateId;
  using AW = typename ToArc::Weight;
  using GW = typename FromArc::Weight;
  using SW = StringWeight<Label, GallicStringType(G)>;

  explicit GallicToNewSymbolsMapper(MutableFst<ToArc> *fst)
      : fst_(fst),
        lmax_(0),
        osymbols_(fst->OutputSymbols()),
        isymbols_(nullptr),
        error_(false) {
    fst_->DeleteStates();
    state_ = fst_->AddState();
    fst_->SetStart(state_);
    fst_->SetFinal(state_);
    if (osymbols_) {
      std::string name = osymbols_->Name() + "_from_gallic";
      fst_->SetInputSymbols(new SymbolTable(name));
      isymbols_ = fst_->MutableInputSymbols();
      const int64_t zero = 0;
      isymbols_->AddSymbol(osymbols_->Find(zero), 0);
    } else {
      fst_->SetInputSymbols(nullptr);
    }
  }

  ToArc operator()(const FromArc &arc) {
    // Super-non-final arc.
    if (arc.nextstate == kNoStateId && arc.weight == GW::Zero()) {
      return ToArc(arc.ilabel, 0, AW::Zero(), kNoStateId);
    }
    SW w1 = arc.weight.Value1();
    AW w2 = arc.weight.Value2();
    Label l;
    if (w1.Size() == 0) {
      l = 0;
    } else if (auto [it, inserted] = map_.emplace(w1, kNoLabel); !inserted) {
      l = it->second;
    } else {
      l = ++lmax_;
      it->second = l;
      StringWeightIterator<SW> iter1(w1);
      StateId n;
      std::string s;
      for (size_t i = 0, p = state_; i < w1.Size(); ++i, iter1.Next(), p = n) {
        n = i == w1.Size() - 1 ? state_ : fst_->AddState();
        fst_->AddArc(p, ToArc(i ? 0 : l, iter1.Value(), n));
        if (isymbols_) {
          if (i) s = s + "_";
          s = s + osymbols_->Find(iter1.Value());
        }
      }
      if (isymbols_) isymbols_->AddSymbol(s, l);
    }
    if (l == kStringInfinity || l == kStringBad || arc.ilabel != arc.olabel) {
      FSTERROR() << "GallicToNewSymbolMapper: Unrepresentable weight: " << l;
      error_ = true;
    }
    return ToArc(arc.ilabel, l, w2, arc.nextstate);
  }

  constexpr MapFinalAction FinalAction() const { return MAP_ALLOW_SUPERFINAL; }

  constexpr MapSymbolsAction InputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  constexpr MapSymbolsAction OutputSymbolsAction() const {
    return MAP_CLEAR_SYMBOLS;
  }

  uint64_t Properties(uint64_t inprops) const {
    uint64_t outprops = inprops & kOLabelInvariantProperties &
                        kWeightInvariantProperties & kAddSuperFinalProperties;
    if (error_) outprops |= kError;
    return outprops;
  }

 private:
  class StringKey {
   public:
    size_t operator()(const SW &x) const { return x.Hash(); }
  };

  using Map = std::unordered_map<SW, Label, StringKey>;

  MutableFst<ToArc> *fst_;
  Map map_;
  Label lmax_;
  StateId state_;
  const SymbolTable *osymbols_;
  SymbolTable *isymbols_;
  mutable bool error_;
};

// TODO(kbg): Add common base class for those mappers which do nothing except
// mutate their weights.

// Mapper to add a constant to all weights.
template <class A>
class PlusMapper {
 public:
  using FromArc = A;
  using ToArc = A;
  using Weight = typename FromArc::Weight;

  constexpr explicit PlusMapper(Weight weight) : weight_(std::move(weight)) {}

  ToArc operator()(const FromArc &arc) const {
    if (arc.weight == Weight::Zero()) return arc;
    return ToArc(arc.ilabel, arc.olabel, Plus(arc.weight, weight_),
                 arc.nextstate);
  }

  constexpr MapFinalAction FinalAction() const { return MAP_NO_SUPERFINAL; }

  constexpr MapSymbolsAction InputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  constexpr MapSymbolsAction OutputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  constexpr uint64_t Properties(uint64_t props) const {
    return props & kWeightInvariantProperties;
  }

 private:
  const Weight weight_;
};

// Mapper to (right) multiply a constant to all weights.
template <class A>
class TimesMapper {
 public:
  using FromArc = A;
  using ToArc = A;
  using Weight = typename FromArc::Weight;

  constexpr explicit TimesMapper(Weight weight) : weight_(std::move(weight)) {}

  ToArc operator()(const FromArc &arc) const {
    if (arc.weight == Weight::Zero()) return arc;
    return ToArc(arc.ilabel, arc.olabel, Times(arc.weight, weight_),
                 arc.nextstate);
  }

  constexpr MapFinalAction FinalAction() const { return MAP_NO_SUPERFINAL; }

  constexpr MapSymbolsAction InputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  constexpr MapSymbolsAction OutputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  constexpr uint64_t Properties(uint64_t props) const {
    return props & kWeightInvariantProperties;
  }

 private:
  const Weight weight_;
};

// Mapper to take all weights to a constant power. The power argument is stored
// as a double, so if there is a floating-point power implementation for this
// weight type, it will take precedence. Otherwise, the power argument's 53 bits
// of integer precision will be implicitly converted to a size_t and the default
// power implementation (iterated multiplication) will be used instead.
template <class A>
class PowerMapper {
 public:
  using FromArc = A;
  using ToArc = A;
  using Weight = typename FromArc::Weight;

  explicit PowerMapper(double power) : power_(power) {}

  ToArc operator()(const FromArc &arc) const {
    return ToArc(arc.ilabel, arc.olabel, Power(arc.weight, power_),
                 arc.nextstate);
  }

  constexpr MapFinalAction FinalAction() const { return MAP_NO_SUPERFINAL; }

  constexpr MapSymbolsAction InputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  constexpr MapSymbolsAction OutputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  constexpr uint64_t Properties(uint64_t props) const {
    return props & kWeightInvariantProperties;
  }

 private:
  const double power_;
};

// Mapper to reciprocate all non-Zero() weights.
template <class A>
class InvertWeightMapper {
 public:
  using FromArc = A;
  using ToArc = A;
  using Weight = typename FromArc::Weight;

  ToArc operator()(const FromArc &arc) const {
    if (arc.weight == Weight::Zero()) return arc;
    return ToArc(arc.ilabel, arc.olabel, Divide(Weight::One(), arc.weight),
                 arc.nextstate);
  }

  constexpr MapFinalAction FinalAction() const { return MAP_NO_SUPERFINAL; }

  constexpr MapSymbolsAction InputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  constexpr MapSymbolsAction OutputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  constexpr uint64_t Properties(uint64_t props) const {
    return props & kWeightInvariantProperties;
  }
};

// Mapper to map all non-Zero() weights to One().
template <class A, class B = A>
class RmWeightMapper {
 public:
  using FromArc = A;
  using ToArc = B;
  using FromWeight = typename FromArc::Weight;
  using ToWeight = typename ToArc::Weight;

  ToArc operator()(const FromArc &arc) const {
    return ToArc(
        arc.ilabel, arc.olabel,
        arc.weight != FromWeight::Zero() ? ToWeight::One() : ToWeight::Zero(),
        arc.nextstate);
  }

  constexpr MapFinalAction FinalAction() const { return MAP_NO_SUPERFINAL; }

  constexpr MapSymbolsAction InputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  constexpr MapSymbolsAction OutputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  constexpr uint64_t Properties(uint64_t props) const {
    return (props & kWeightInvariantProperties) | kUnweighted;
  }
};

// Mapper to quantize all weights.
template <class A, class B = A>
class QuantizeMapper {
 public:
  using FromArc = A;
  using ToArc = B;
  using FromWeight = typename FromArc::Weight;
  using ToWeight = typename ToArc::Weight;

  QuantizeMapper() : delta_(kDelta) {}

  explicit QuantizeMapper(float d) : delta_(d) {}

  ToArc operator()(const FromArc &arc) const {
    return ToArc(arc.ilabel, arc.olabel, arc.weight.Quantize(delta_),
                 arc.nextstate);
  }

  constexpr MapFinalAction FinalAction() const { return MAP_NO_SUPERFINAL; }

  constexpr MapSymbolsAction InputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  constexpr MapSymbolsAction OutputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  constexpr uint64_t Properties(uint64_t props) const {
    return props & kWeightInvariantProperties;
  }

 private:
  const float delta_;
};

// Mapper from A to B under the assumption:
//
//    B::Weight = A::Weight::ReverseWeight
//    B::Label == A::Label
//    B::StateId == A::StateId
//
// The weight is reversed, while the label and nextstate are preserved.
template <class A, class B>
class ReverseWeightMapper {
 public:
  using FromArc = A;
  using ToArc = B;
  static_assert(std::is_same_v<typename ToArc::Weight,
                               typename FromArc::Weight::ReverseWeight>,
                "ToArc::Weight must be FromArc::Weight::ReverseWeight");
  static_assert(std::is_same_v<typename ToArc::Label, typename FromArc::Label>,
                "ToArc::Label must be FromArc::Label");
  static_assert(
      std::is_same_v<typename ToArc::StateId, typename FromArc::StateId>,
      "ToArc::StateId must be FromArc::StateId");

  constexpr ToArc operator()(const FromArc &arc) const {
    return ToArc(arc.ilabel, arc.olabel, arc.weight.Reverse(), arc.nextstate);
  }

  constexpr MapFinalAction FinalAction() const { return MAP_NO_SUPERFINAL; }

  constexpr MapSymbolsAction InputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  constexpr MapSymbolsAction OutputSymbolsAction() const {
    return MAP_COPY_SYMBOLS;
  }

  constexpr uint64_t Properties(uint64_t props) const { return props; }
};

}  // namespace fst

#endif  // FST_ARC_MAP_H_
