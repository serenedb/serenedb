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
// Commonly used FST arc types.

#ifndef FST_ARC_H_
#define FST_ARC_H_

#include <climits>
#include <cstddef>
#include <cstdint>
#include <string>
#include <type_traits>
#include <utility>

#include <fst/error-weight.h>
#include <fst/expectation-weight.h>
#include <fst/float-weight.h>
#include <fst/fst-decl.h>  // For optional argument declarations
#include <fst/lexicographic-weight.h>
#include <fst/power-weight.h>
#include <fst/product-weight.h>
#include <fst/signed-log-weight.h>
#include <fst/sparse-power-weight.h>
#include <fst/string-weight.h>

namespace fst {

template <class W, class L /* = int */, class S /* = int */>
struct ArcTpl {
 public:
  using Weight = W;
  using Label = L;
  using StateId = S;

  Label ilabel;
  Label olabel;
  Weight weight;
  StateId nextstate;

  ArcTpl() noexcept(std::is_nothrow_default_constructible_v<Weight>) = default;

  template <class T>
  ArcTpl(Label ilabel, Label olabel, T &&weight, StateId nextstate)
      : ilabel(ilabel),
        olabel(olabel),
        weight(std::forward<T>(weight)),
        nextstate(nextstate) {}

  // Arc with weight One.
  ArcTpl(Label ilabel, Label olabel, StateId nextstate)
      : ArcTpl(ilabel, olabel, Weight::One(), nextstate) {}

  static const std::string &Type() {
    static const auto *const type = new std::string(
        Weight::Type() == "tropical" ? "standard" : Weight::Type());
    return *type;
  }
};

using StdArc = ArcTpl<TropicalWeight>;
using LogArc = ArcTpl<LogWeight>;
using Log64Arc = ArcTpl<Log64Weight>;
using RealArc = ArcTpl<RealWeight>;
using Real64Arc = ArcTpl<Real64Weight>;
using SignedLogArc = ArcTpl<SignedLogWeight>;
using SignedLog64Arc = ArcTpl<SignedLog64Weight>;
using ErrorArc = ArcTpl<ErrorWeight>;
using MinMaxArc = ArcTpl<MinMaxWeight>;

// Arc with integer labels and state IDs and string weights.
template <StringType S = STRING_LEFT>
struct StringArc : public ArcTpl<StringWeight<int, S>> {
 public:
  using Base = ArcTpl<StringWeight<int, S>>;

  using Base::Base;

  static const std::string &Type() {
    static const auto *const type = new std::string(
        S == STRING_LEFT ? "left_standard_string"
                         : (S == STRING_RIGHT ? "right_standard_string"
                                              : "restricted_standard_string"));
    return *type;
  }
};

// Arc with label and state Id type the same as template arg and with
// weights over the Gallic semiring w.r.t the output labels and weights of A.
template <class A, GallicType G = GALLIC_LEFT>
struct GallicArc : public ArcTpl<GallicWeight<int, typename A::Weight, G>,
                                 typename A::Label, typename A::StateId> {
  using Base = ArcTpl<GallicWeight<int, typename A::Weight, G>,
                      typename A::Label, typename A::StateId>;
  using Arc = A;

  using Base::Base;

  explicit GallicArc(const Arc &arc)
      : Base(arc.ilabel, arc.ilabel, Weight(arc.olabel, arc.weight),
             arc.nextstate) {}

  static const std::string &Type() {
    static const auto *const type = new std::string(
        (G == GALLIC_LEFT
             ? "left_gallic_"
             : (G == GALLIC_RIGHT
                    ? "right_gallic_"
                    : (G == GALLIC_RESTRICT
                           ? "restricted_gallic_"
                           : (G == GALLIC_MIN ? "min_gallic_" : "gallic_")))) +
        Arc::Type());
    return *type;
  }
};

// Arc with the reverse of the weight found in its template arg.
template <class A>
struct ReverseArc : public ArcTpl<typename A::Weight::ReverseWeight,
                                  typename A::Label, typename A::StateId> {
  using Base = ArcTpl<typename A::Weight::ReverseWeight, typename A::Label,
                      typename A::StateId>;
  using Arc = A;

  using Base::Base;

  static const std::string &Type() {
    static const auto *const type = new std::string("reverse_" + Arc::Type());
    return *type;
  }
};

// Arc with integer labels and state IDs and lexicographic weights.
template <class Weight1, class Weight2>
using LexicographicArc = ArcTpl<LexicographicWeight<Weight1, Weight2>>;

// Arc with integer labels and state IDs and product weights.
template <class Weight1, class Weight2>
using ProductArc = ArcTpl<ProductWeight<Weight1, Weight2>>;

// Arc with label and state ID type the same as first template argument and with
// weights over the n-th Cartesian power of the weight type of the template
// argument.
template <class A, size_t n>
struct PowerArc : public ArcTpl<PowerWeight<typename A::Weight, n>,
                                typename A::Label, typename A::StateId> {
  using Base = ArcTpl<PowerWeight<typename A::Weight, n>, typename A::Label,
                      typename A::StateId>;
  using Arc = A;

  using Base::Base;

  static const std::string &Type() {
    static const auto *const type =
        new std::string(Arc::Type() + "_^" + std::to_string(n));
    return *type;
  }
};

// Arc with label and state ID type the same as first template argument and with
// weights over the arbitrary Cartesian power of the weight type.
template <class A, class K = int>
struct SparsePowerArc : public ArcTpl<SparsePowerWeight<typename A::Weight, K>,
                                      typename A::Label, typename A::StateId> {
  using Base = ArcTpl<SparsePowerWeight<typename A::Weight, K>,
                      typename A::Label, typename A::StateId>;
  using Arc = A;

  using Base::Base;

  static const std::string &Type() {
    static const std::string *const type = [] {
      std::string type = Arc::Type() + "_^n";
      if (sizeof(K) != sizeof(uint32_t)) {
        type += "_" + std::to_string(CHAR_BIT * sizeof(K));
      }
      return new std::string(type);
    }();
    return *type;
  }
};

// Arc with label and state ID type the same as first template argument and with
// expectation weight over the first template argument's weight type and the
// second template argument.
template <class A, class X2>
struct ExpectationArc : public ArcTpl<ExpectationWeight<typename A::Weight, X2>,
                                      typename A::Label, typename A::StateId> {
  using Base = ArcTpl<ExpectationWeight<typename A::Weight, X2>,
                      typename A::Label, typename A::StateId>;
  using Arc = A;
  using X1 = typename Arc::Weight;

  using Base::Base;

  static const std::string &Type() {
    static const auto *const type =
        new std::string("expectation_" + Arc::Type() + "_" + X2::Type());
    return *type;
  }
};

}  // namespace fst

#endif  // FST_ARC_H_
