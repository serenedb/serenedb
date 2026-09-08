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

#include <tuple>
#include <type_traits>
#include <utility>

#include "basics/empty.hpp"
#include "basics/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::probe {

template<typename Musts, typename Optional, typename Excludes>
class BooleanSparse {
 public:
  static constexpr bool kMusts = !std::is_same_v<Musts, utils::Empty>;
  static constexpr bool kOptional = !std::is_same_v<Optional, utils::Empty>;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;
  static_assert(kMusts || kOptional);

  template<typename MustsArgs, typename OptionalArgs, typename ExcludesArgs>
  BooleanSparse(std::piecewise_construct_t, MustsArgs&& musts,
                OptionalArgs&& optional, ExcludesArgs&& excludes)
    : _musts{std::make_from_tuple<Musts>(std::forward<MustsArgs>(musts))},
      _optional{
        std::make_from_tuple<Optional>(std::forward<OptionalArgs>(optional))},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))} {}

  BooleanSparse(BooleanSparse&&) = delete;
  BooleanSparse& operator=(BooleanSparse&&) = delete;

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    if constexpr (kMusts) {
      if (const auto probe = _musts.Probe(target); probe != target) {
        return probe;
      }
    }
    if constexpr (kOptional) {
      if (const auto probe = _optional.Probe(target); probe != target) {
        return probe;
      }
    }
    if constexpr (kExcludes) {
      if (_excludes.Probe(target) == target) {
        return target + 1;
      }
    }
    return target;
  }

 private:
  [[no_unique_address]] Musts _musts;
  [[no_unique_address]] Optional _optional;
  [[no_unique_address]] Excludes _excludes;
};

}  // namespace irs::probe
