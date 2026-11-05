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
#include <utility>

#include "iresearch/search/detail/verified_batch.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

template<typename Approx, typename Recipe, bool Lead>
class VerifiedSlots {
 public:
  template<typename ApproxArgs>
  VerifiedSlots(std::piecewise_construct_t, ApproxArgs&& approx,
                const Recipe& recipe)
    : _approx{std::make_from_tuple<Approx>(std::forward<ApproxArgs>(approx))},
      _verifier{recipe.Make()} {}

  doc_id_t Seek(doc_id_t target) {
    return _batch.Seek(_approx, _verifier, target);
  }

  doc_id_t Next(doc_id_t) { return _batch.Next(_approx, _verifier); }

  doc_id_t Probe(doc_id_t target) { return _approx.Probe(target); }

  bool Match(doc_id_t doc) {
    if constexpr (Lead) {
      return _batch.Match(doc);
    } else {
      return _verifier.Check(doc);
    }
  }

 private:
  using Verifier = decltype(std::declval<const Recipe&>().Make());

  Approx _approx;
  Verifier _verifier;
  [[no_unique_address]] utils::Need<Lead, VerifiedBatch<Verifier>> _batch;
};

}  // namespace irs::detail
