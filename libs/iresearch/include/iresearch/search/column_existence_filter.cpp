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

#include "basics/down_cast.h"
#include "iresearch/formats/empty_term_reader.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/disjunction.hpp"
#include "iresearch/search/make_disjunction.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {
namespace {

class ColumnExistenceQuery : public Filter::Query {
 public:
  ColumnExistenceQuery(std::string_view field, score_t boost)
    : _field{field}, _boost{boost} {}

  DocIterator::ptr execute(const ExecutionContext& ctx) const override {
    const auto& segment = ctx.segment;
    const auto* column = segment.column(_field);

    if (!column) {
      return DocIterator::empty();
    }

    return Iterator(*column);
  }

  void visit(const SubReader&, PreparedStateVisitor&, score_t) const final {
    // No terms to visit
  }

  score_t Boost() const noexcept final { return _boost; }

 protected:
  DocIterator::ptr Iterator(const ColumnReader& column) const {
    auto it = column.iterator(ColumnHint::Mask);

    if (!it) [[unlikely]] {
      return DocIterator::empty();
    }

    return it;
  }

  std::string _field;
  score_t _boost;
};

class ColumnPrefixExistenceQuery : public ColumnExistenceQuery {
 public:
  ColumnPrefixExistenceQuery(std::string_view prefix,
                             const ColumnAcceptor& acceptor, score_t boost)
    : ColumnExistenceQuery{prefix, boost}, _acceptor{acceptor} {
    SDB_ASSERT(_acceptor);
  }

  DocIterator::ptr execute(const ExecutionContext& ctx) const final {
    SDB_ASSERT(_acceptor);

    auto& segment = ctx.segment;
    const std::string_view prefix = _field;

    auto it = segment.columns();

    if (!it->seek(prefix)) {
      // reached the end
      return DocIterator::empty();
    }

    const auto* column = &it->value();

    ScoreAdapters itrs;
    for (; column->name().starts_with(prefix); column = &it->value()) {
      if (_acceptor(column->name(), prefix)) {
        itrs.emplace_back(Iterator(*column));
      }

      if (!it->next()) {
        break;
      }
    }

    return ResolveMergeType(
      ctx.scorer ? ScoreMergeType::Sum : ScoreMergeType::Noop,
      [&]<ScoreMergeType MergeType>() -> DocIterator::ptr {
        using Disjunction = DisjunctionIterator<ScoreAdapter, MergeType>;
        return irs::MakeDisjunction<Disjunction>(
          ctx.wand, static_cast<doc_id_t>(ctx.segment.docs_count()),
          std::move(itrs));
      });
  }

 private:
  ColumnAcceptor _acceptor;
};

class Buffer final : public Filter::PrepareBuffer {
 public:
  Buffer(std::string_view field, ColumnAcceptor acceptor)
    : _field{field}, _acceptor{acceptor} {}

  void PrepareSegment(const SubReader&) final {}

  void Merge(PrepareBuffer&& other) final {
    [[maybe_unused]] auto& rhs = sdb::basics::downCast<Buffer>(other);
  }

  bool Empty() const noexcept final { return false; }

  Filter::Query::ptr Compile(const PrepareContext& ctx) && final {
    return _acceptor ? memory::make_tracked<ColumnPrefixExistenceQuery>(
                         ctx.memory, _field, _acceptor, ctx.boost)
                     : memory::make_tracked<ColumnExistenceQuery>(
                         ctx.memory, _field, ctx.boost);
  }

 private:
  std::string_view _field;
  ColumnAcceptor _acceptor;
};

}  // namespace

std::unique_ptr<Filter::PrepareBuffer> ByColumnExistence::CreateBuffer(
  const PrepareContext& /*ctx*/) const {
  return std::make_unique<Buffer>(field(), options().acceptor);
}

Filter::Query::ptr ByColumnExistence::prepare(const PrepareContext& ctx) const {
  // skip field-level/term-level statistics because there are no explicit
  // fields/terms, but still collect index-level statistics
  // i.e. all fields and terms implicitly match
  auto sub_ctx = ctx;
  sub_ctx.boost *= Boost();
  return DefaultPrepare(sub_ctx);
}

}  // namespace irs
