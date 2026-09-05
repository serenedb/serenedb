////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <optional>
#include <type_traits>
#include <utility>

#include "basics/log.h"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/geo_filter.hpp"
#include "iresearch/search/geo_parsers.hpp"
#include "iresearch/search/query_builder_impl.hpp"

namespace irs {

template<typename Parser, typename Acceptor>
class GeoVerifier {
 public:
  GeoVerifier(const ColumnReader& stored_field, const ColReader& col_reader,
              Parser& parser, Acceptor& acceptor)
    : _cursor{col_reader, stored_field}, _acceptor{acceptor}, _parser{parser} {
    if constexpr (std::is_same_v<std::decay_t<Parser>, S2PointParser>) {
      _shape.reset(S2Point{1, 0, 0});
    }
  }

  bool Check(doc_id_t doc) {
    const auto bytes = _cursor.FetchDoc(doc);
    if (bytes.empty()) {
      SDB_DEBUG(IRESEARCH, "Missing stored geo value, doc='", doc, "'");
      return false;
    }
    return _parser(bytes, _shape) && _acceptor(_shape);
  }

 private:
  sdb::geo::ShapeContainer _shape;
  ColumnReader::BlobPointReader _cursor;
  Acceptor& _acceptor;
  [[no_unique_address]] Parser _parser;
};

template<typename Parser, typename Acceptor>
class GeoQuery : public QueryBuilderImpl<GeoQuery<Parser, Acceptor>> {
 public:
  GeoQuery(const SubReader& segment, QueryBuilder::ptr&& cells,
           field_id store_field_id, Parser&& parser, Acceptor&& acceptor,
           score_t boost) noexcept
    : QueryBuilderImpl<GeoQuery>{segment},
      _cells{std::move(cells)},
      _parser{std::move(parser)},
      _acceptor{std::move(acceptor)},
      _boost{boost},
      _store_field_id{store_field_id} {
    SDB_ASSERT(_cells);
    SDB_ASSERT(_cells->Kind() != QueryKind::Empty);
    SDB_ASSERT(MakeCheck().possible);
    this->_estimate_max = _cells->EstimateMax();
  }

  using Verifier =
    GeoVerifier<std::add_const_t<Parser>, std::add_const_t<Acceptor>>;

  struct Recipe {
    const ColumnReader* stored_field = nullptr;
    const ColReader* col_reader = nullptr;
    const Parser* parser = nullptr;
    const Acceptor* acceptor = nullptr;

    Verifier Make() const {
      return Verifier{*stored_field, *col_reader, *parser, *acceptor};
    }
  };

  struct Check {
    std::optional<Recipe> recipe;
    bool possible = true;
  };

  const QueryBuilder& Cells() const noexcept { return *_cells; }

  Check MakeCheck() const {
    SDB_ASSERT(irs::field_limits::valid(_store_field_id));
    const auto* col_reader = this->_segment.GetColReader();
    if (!col_reader) {
      return {.possible = false};
    }
    const auto* stored_field = col_reader->Column(_store_field_id);
    if (!stored_field) {
      return {.possible = false};
    }
    return {.recipe = Recipe{stored_field, col_reader, &_parser, &_acceptor}};
  }

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

  void SetBoost(score_t value) noexcept final { _boost = value; }

 private:
  QueryBuilder::ptr _cells;
  [[no_unique_address]] Parser _parser;
  [[no_unique_address]] Acceptor _acceptor;
  score_t _boost;
  field_id _store_field_id;
};

}  // namespace irs
