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
#include "iresearch/search/queries/geo_query.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

template<typename Parser, typename Acceptor, typename Cells>
class GeoSlots {
 public:
  using Recipe = typename GeoQuery<Parser, Acceptor>::Recipe;

  template<typename CellsArgs>
  GeoSlots(std::piecewise_construct_t, CellsArgs&& cells, const Recipe& recipe)
    : _cells{std::make_from_tuple<Cells>(std::forward<CellsArgs>(cells))},
      _shape{recipe.Make()} {}

  doc_id_t Seek(doc_id_t target) { return _batch.Seek(_cells, _shape, target); }

  doc_id_t Next(doc_id_t) { return _batch.Next(_cells, _shape); }

  doc_id_t Probe(doc_id_t target) { return _cells.Probe(target); }

  bool Match(doc_id_t doc) {
    if constexpr (requires(Cells& cells) { cells.Next(); }) {
      return _batch.Match(doc);
    } else {
      return _shape.Check(doc);
    }
  }

 private:
  using Verifier = typename GeoQuery<Parser, Acceptor>::Verifier;

  Cells _cells;
  Verifier _shape;
  VerifiedBatch<Verifier> _batch;
};

}  // namespace irs::detail
