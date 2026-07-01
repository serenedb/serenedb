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

#include "all_filter.hpp"

#include "all_iterator.hpp"
#include "iresearch/search/automaton_filter.hpp"
#include "iresearch/utils/automaton_utils.hpp"

namespace irs {

class AllQuery : public QueryBuilder {
 public:
  explicit AllQuery(const SubReader& segment, score_t boost)
    : QueryBuilder{segment}, _boost{boost} {}

  DocIterator::ptr Execute(const ExecutionContext&,
                           const StatsBuffer& stats) const final {
    return memory::make_managed<AllIterator>(_segment.docs_count(),
                                             stats.GetStats().data(), _boost);
  }

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

 private:
  score_t _boost;
};

QueryBuilder::ptr MakeAllQuery(const SubReader& segment,
                               const PrepareContext& ctx, score_t boost) {
  return memory::make_tracked<AllQuery>(ctx.memory, segment, ctx.boost * boost);
}

QueryBuilder::ptr All::PrepareSegment(const SubReader& segment,
                                      const PrepareContext& ctx) const {
  return MakeAllQuery(segment, ctx, Boost());
}

PrepareCollector::ptr All::MakeCollector(const Scorer* scorer) const {
  return std::make_unique<AllCollector>(scorer);
}

AllTermIterator::AllTermIterator(const TermReader& reader)
  : _impl{reader.iterator(SeekMode::NORMAL)} {
  if (!_impl || !_impl->next()) {
    _impl = SeekTermIterator::empty();
  }
}

AllTermIterator::AllTermIterator(const TermReader& reader,
                                 const AutomatonOptions& options)
  : _impl{[&] {
      SDB_ENSURE(options.compiled, sdb::ERROR_INTERNAL,
                 "ts_dict automaton: filter has no compiled acceptor");
      auto it = MakeAutomatonIterator(reader, options.compiled->matcher);
      return it ? std::move(it) : SeekTermIterator::empty();
    }()} {}

}  // namespace irs
