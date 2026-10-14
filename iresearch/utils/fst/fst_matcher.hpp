////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/algorithm/container.h>

#include <span>

#include "iresearch/utils/assert.hpp"

namespace irs {

template<typename FST>
class ArcMatcher {
 public:
  using Arc = typename FST::Arc;
  using Label = typename FST::Label;
  using StateId = typename FST::StateId;

  explicit ArcMatcher(const FST* fst) noexcept : _fst{fst} {}

  void SetState(StateId s) noexcept { _arcs = _fst->Arcs(s); }

  bool Find(Label label) noexcept {
    if (_arcs.size() <= kLinearArcs) {
      for (const auto& arc : _arcs) {
        if (arc.ilabel >= label) {
          _value = &arc;
          return arc.ilabel == label;
        }
      }
      return false;
    }
    const auto it = absl::c_lower_bound(
      _arcs, label, [](const Arc& arc, Label l) { return arc.ilabel < l; });
    if (it == _arcs.end()) {
      return false;
    }
    _value = &*it;
    return it->ilabel == label;
  }

  const Arc& Value() const noexcept {
    SDB_ASSERT(_value);
    return *_value;
  }

 private:
  static constexpr size_t kLinearArcs = 8;

  const FST* _fst;
  std::span<const Arc> _arcs;
  const Arc* _value{};
};

}  // namespace irs
