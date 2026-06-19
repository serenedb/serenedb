////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#include <vector>

#include "basics/shared.hpp"
#include "iresearch/types.hpp"

// clang-format off
#include <fst/fst.h>
// clang-format on
#include <fst/connect.h>
#include <fst/test-properties.h>

#include "iresearch/utils/automaton_decl.hpp"
#include "iresearch/utils/fstext/fst_utils.hpp"

namespace fst {
namespace fsa {

class BooleanWeight {
 public:
  using ReverseWeight = BooleanWeight;
  using PayloadType = irs::byte_type;

  static const std::string& Type() {
    static const std::string kType = "boolean";
    return kType;
  }

  static constexpr BooleanWeight Zero() noexcept { return false; }
  static constexpr BooleanWeight One() noexcept { return true; }
  static constexpr BooleanWeight NoWeight() noexcept { return {}; }

  static constexpr uint64_t Properties() noexcept {
    return kLeftSemiring | kRightSemiring | kCommutative | kIdempotent | kPath;
  }

  constexpr BooleanWeight() noexcept = default;
  constexpr BooleanWeight(bool v, PayloadType payload = 0) noexcept
    : _v(static_cast<PayloadType>(v)), _p(payload) {}

  constexpr bool Member() const noexcept { return kInvalid != _v; }
  constexpr BooleanWeight Quantize(
    [[maybe_unused]] float delta = kDelta) const noexcept {
    return {};
  }
  std::istream& Read(std::istream& strm) noexcept {
    _v = strm.get();
    if (strm.fail()) {
      _v = kInvalid;
    }
    return strm;
  }
  std::ostream& Write(std::ostream& strm) const noexcept {
    strm.put(_v);
    return strm;
  }
  constexpr size_t Impl() const noexcept { return static_cast<size_t>(_v); }
  constexpr ReverseWeight Reverse() const noexcept { return *this; }
  constexpr PayloadType Payload() const noexcept { return _p; }
  constexpr operator bool() const noexcept { return _v == kTrue; }

  friend constexpr bool operator==(const BooleanWeight& lhs,
                                   const BooleanWeight& rhs) noexcept {
    return lhs.Impl() == rhs.Impl();
  }

  // Note: | and & used instead of || and && because gcc cannot optimize it

  friend constexpr BooleanWeight Plus(const BooleanWeight& lhs,
                                      const BooleanWeight& rhs) noexcept {
    return {
      static_cast<bool>(static_cast<unsigned>(static_cast<bool>(lhs.Impl())) |
                        static_cast<unsigned>(static_cast<bool>(rhs.Impl()))),
      static_cast<PayloadType>(lhs.Payload() | rhs.Payload())};
  }
  friend constexpr BooleanWeight Times(const BooleanWeight& lhs,
                                       const BooleanWeight& rhs) noexcept {
    return {
      static_cast<bool>(static_cast<unsigned>(static_cast<bool>(lhs.Impl())) &
                        static_cast<unsigned>(static_cast<bool>(rhs.Impl()))),
      static_cast<PayloadType>(lhs.Payload() & rhs.Payload())};
  }
  friend constexpr BooleanWeight Divide(
    [[maybe_unused]] const BooleanWeight& lhs,
    [[maybe_unused]] const BooleanWeight& rhs,
    [[maybe_unused]] DivideType type) noexcept {
    return NoWeight();
  }
  friend constexpr BooleanWeight Divide(
    [[maybe_unused]] const BooleanWeight& lhs,
    [[maybe_unused]] const BooleanWeight& rhs) noexcept {
    return NoWeight();
  }
  friend constexpr BooleanWeight DivideLeft(
    [[maybe_unused]] const BooleanWeight& lhs,
    [[maybe_unused]] const BooleanWeight& rhs) noexcept {
    return NoWeight();
  }

  friend std::ostream& operator<<(std::ostream& strm, const BooleanWeight& w) {
    if (w.Member()) {
      strm << "{"
           << static_cast<char>(static_cast<int>(static_cast<bool>(w)) + 48)
           << "," << static_cast<int>(w.Payload()) << "}";
    }
    return strm;
  }
  friend constexpr bool ApproxEqual(const BooleanWeight& lhs,
                                    const BooleanWeight& rhs,
                                    [[maybe_unused]] float delta = kDelta) {
    return lhs == rhs;
  }

 private:
  static constexpr PayloadType kFalse = 0;
  static constexpr PayloadType kTrue = 1;     // "is true" mask
  static constexpr PayloadType kInvalid = 2;  // "not a member" value

  // TODO(mbkkt) can be bool?
  // TODO(mbkkt) try to make it more signaling
  PayloadType _v{kInvalid};
  PayloadType _p{};
};

struct RangeLabelLE {
  constexpr RangeLabelLE(int64_t ilabel) noexcept : ilabel{ilabel} {}
  constexpr explicit RangeLabelLE(uint16_t min, uint16_t max) noexcept
    : max{max}, min{min}, not_epsion{0xFFFF'FFFFU} {}

  union {
    int64_t ilabel;
    struct {
      uint16_t max;
      uint16_t min;
      uint32_t not_epsion;
    };
  };
};

struct RangeLabelBE {
  constexpr RangeLabelBE(int64_t ilabel) noexcept : ilabel{ilabel} {}
  constexpr explicit RangeLabelBE(uint16_t min, uint16_t max) noexcept
    : not_epsion{0xFFFF'FFFFU}, min{min}, max{max} {}

  union {
    int64_t ilabel;
    struct {
      uint32_t not_epsion;
      uint16_t min;
      uint16_t max;
    };
  };
};

using RangeLabelType =
  std::conditional_t<std::endian::native == std::endian::little, RangeLabelLE,
                     RangeLabelBE>;

// We inherit from annonymous union to be OpenFST compliant.
struct RangeLabel : RangeLabelType {
  constexpr RangeLabel() noexcept : RangeLabelType{fst::kNoLabel} {}
  constexpr RangeLabel(RangeLabelType&& type) : RangeLabelType{type} {}

  static constexpr RangeLabel From(uint16_t min, uint16_t max) noexcept {
    return RangeLabelType{min, max};
  }
  static constexpr RangeLabel From(uint16_t point) noexcept {
    return From(point, point);
  }

  // TODO(mbkkt) should use std::bit_cast
  constexpr operator int64_t() const noexcept { return ilabel; }

  friend std::ostream& operator<<(std::ostream& strm, const RangeLabel& l) {
    strm << '[' << l.min << ".." << l.max << ']';
    return strm;
  }
};

template<typename W = BooleanWeight>
struct Transition : RangeLabel {
  using Weight = W;
  using Label = int64_t;
  using StateId = int32_t;

  static const std::string& Type() {
    static const std::string kType = "Transition";
    return kType;
  }

  union {
    StateId nextstate{fst::kNoStateId};
    fstext::EmptyLabel<Label> olabel;
    fstext::EmptyWeight<Weight> weight;  // all arcs are trivial
  };

  constexpr Transition() = default;

  constexpr Transition(RangeLabel ilabel, StateId nextstate)
    : RangeLabel{ilabel}, nextstate(nextstate) {}

  constexpr Transition(Label ilabel, StateId nextstate)
    : RangeLabel{ilabel}, nextstate{nextstate} {}

  // satisfy openfst API
  constexpr Transition(Label ilabel, Label /*unused*/, Weight /*unused*/,
                       StateId nextstate)
    : RangeLabel{ilabel}, nextstate{nextstate} {}

  // satisfy openfst API
  constexpr Transition(Label ilabel, Label /*unused*/, StateId nextstate)
    : RangeLabel{ilabel}, nextstate{nextstate} {}
};

}  // namespace fsa
}  // namespace fst

// clang-format off
#include <fst/vector-fst.h>
// clang-format on
#include <fst/matcher.h>
