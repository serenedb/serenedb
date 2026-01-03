////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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

#include "column_existence_filter.hpp"

#include "iresearch/formats/empty_term_reader.hpp"
#include "iresearch/index/index_reader.hpp"  // for SubReader
#include "iresearch/search/disjunction.hpp"

namespace irs {
namespace {

class ColumnExistenceQuery : public Filter::Query {
 public:
  ColumnExistenceQuery(std::string_view field, bstring&& stats, score_t boost)
    : _field{field}, _stats{std::move(stats)}, _boost{boost} {}

  DocIterator::ptr execute(const ExecutionContext& ctx) const override {
    const auto& segment = ctx.segment;
    const auto* column = segment.column(_field);

    if (!column) {
      return DocIterator::empty();
    }

    return Iterator(segment, *column, ctx.scorers);
  }

  void visit(const SubReader&, PreparedStateVisitor&, score_t) const final {
    // No terms to visit
  }

  score_t Boost() const noexcept final { return _boost; }

 protected:
  DocIterator::ptr Iterator(const SubReader& segment,
                            const ColumnReader& column,
                            const Scorers& ord) const {
    auto it = column.iterator(ColumnHint::Mask);

    if (!it) [[unlikely]] {
      return DocIterator::empty();
    }

    if (!ord.empty()) {
      if (auto* score = irs::GetMutable<irs::ScoreAttr>(it.get()); score) {
        CompileScore(*score, ord.buckets(), segment,
                     EmptyTermReader(column.size()), _stats.c_str(), *it,
                     _boost);
      }
    }

    return it;
  }

  std::string _field;
  bstring _stats;
  score_t _boost;
};

class ColumnPrefixExistenceQuery : public ColumnExistenceQuery {
 public:
  ColumnPrefixExistenceQuery(std::string_view prefix, bstring&& stats,
                             const ColumnAcceptor& acceptor, score_t boost)
    : ColumnExistenceQuery{prefix, std::move(stats), boost},
      _acceptor{acceptor} {
    SDB_ASSERT(_acceptor);
  }

  DocIterator::ptr execute(const ExecutionContext& ctx) const final {
    using AdapterT = irs::ScoreAdapter<>;

    SDB_ASSERT(_acceptor);

    auto& segment = ctx.segment;
    auto& ord = ctx.scorers;
    const std::string_view prefix = _field;

    auto it = segment.columns();

    if (!it->seek(prefix)) {
      // reached the end
      return DocIterator::empty();
    }

    const auto* column = &it->value();

    std::vector<AdapterT> itrs;
    for (; column->name().starts_with(prefix); column = &it->value()) {
      if (_acceptor(column->name(), prefix)) {
        itrs.emplace_back(Iterator(segment, *column, ord));
      }

      if (!it->next()) {
        break;
      }
    }

    return ResolveMergeType(
      ScoreMergeType::Sum, ord.buckets().size(),
      [&]<typename A>(A&& aggregator) -> DocIterator::ptr {
        using Disjunction = DisjunctionIterator<DocIterator::ptr, A>;
        return irs::MakeDisjunction<Disjunction>(ctx.wand, std::move(itrs),
                                                 std::move(aggregator));
      });
  }

 private:
  ColumnAcceptor _acceptor;
};

}  // namespace

Filter::Query::ptr ByColumnExistence::prepare(const PrepareContext& ctx) const {
  // skip field-level/term-level statistics because there are no explicit
  // fields/terms, but still collect index-level statistics
  // i.e. all fields and terms implicitly match
  bstring stats(ctx.scorers.stats_size(), 0);
  auto* stats_buf = stats.data();

  PrepareCollectors(ctx.scorers.buckets(), stats_buf);

  const auto filter_boost = ctx.boost * Boost();

  auto& acceptor = options().acceptor;

  return acceptor
           ? memory::make_tracked<ColumnPrefixExistenceQuery>(
               ctx.memory, field(), std::move(stats), acceptor, filter_boost)
           : memory::make_tracked<ColumnExistenceQuery>(
               ctx.memory, field(), std::move(stats), filter_boost);
}

}  // namespace irs
