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

namespace irs {

// Compiled all_filter that returns all documents
class AllQuery : public Filter::Query {
 public:
  explicit AllQuery(bstring&& stats, score_t boost)
    : _stats{std::move(stats)}, _boost{boost} {}

  DocIterator::ptr execute(const ExecutionContext& ctx) const final {
    auto& rdr = ctx.segment;

    return memory::make_managed<AllIterator>(rdr, _stats.c_str(), ctx.scorers,
                                             rdr.docs_count(), _boost);
  }

  void visit(const SubReader&, PreparedStateVisitor&, score_t) const final {
    // No terms to visit
  }

  score_t Boost() const noexcept final { return _boost; }

 private:
  bstring _stats;
  score_t _boost;
};

Filter::Query::ptr All::prepare(const PrepareContext& ctx) const {
  // skip field-level/term-level statistics because there are no explicit
  // fields/terms, but still collect index-level statistics
  // i.e. all fields and terms implicitly match
  bstring stats(ctx.scorers.stats_size(), 0);
  auto* stats_buf = stats.data();

  PrepareCollectors(ctx.scorers.buckets(), stats_buf);

  return memory::make_tracked<AllQuery>(ctx.memory, std::move(stats),
                                        ctx.boost * Boost());
}

}  // namespace irs
