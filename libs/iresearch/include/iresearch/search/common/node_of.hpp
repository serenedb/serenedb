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

#include <type_traits>

#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/lead/two_phase_docs.hpp"
#include "iresearch/search/probe/two_phase_docs.hpp"

namespace irs::search {

template<typename Result, typename Slots>
using TwoPhaseFor =
  std::conditional_t<std::is_same_v<Result, ProbeNode::ptr>,
                     probe::TwoPhaseDocs<Slots>, lead::TwoPhaseDocs<Slots>>;

template<typename Slots>
struct DeducedNode {};

template<template<typename> class Wrap, typename Result, typename Slots>
using NodeOf =
  std::conditional_t<std::is_same_v<Wrap<Slots>, DeducedNode<Slots>>,
                     TwoPhaseFor<Result, Slots>, Wrap<Slots>>;

}  // namespace irs::search
