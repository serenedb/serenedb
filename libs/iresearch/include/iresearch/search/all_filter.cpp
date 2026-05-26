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
#include "basics/down_cast.h"

namespace irs {
namespace {

class AllQuery : public Filter::Query {
 public:
  explicit AllQuery(bstring&& stats, score_t boost)
    : _stats{std::move(stats)}, _boost{boost} {}

  DocIterator::ptr execute(const ExecutionContext& ctx) const final {
    return memory::make_managed<AllIterator>(ctx.segment.docs_count(),
                                             _stats.c_str(), _boost);
  }

  void visit(const SubReader&, PreparedStateVisitor&, score_t) const final {
    // No terms to visit
  }

  score_t Boost() const noexcept final { return _boost; }

 private:
  bstring _stats;
  score_t _boost;
};

class Buffer final : public Filter::ScoredBuffer {
 public:
  using ScoredBuffer::ScoredBuffer;

  void PrepareSegment(const SubReader&) final {}

  void Merge(PrepareBuffer&& other) final {
    [[maybe_unused]] auto& rhs = sdb::basics::downCast<Buffer>(other);
  }

  bool Empty() const noexcept final { return false; }

  // `boost` here is the *cumulative* boost from root.
  static Filter::Query::ptr CompileQuery(const PrepareContext& ctx,
                                         score_t boost) {
    bstring stats(GetStatsSize(ctx.scorer), 0);
    if (ctx.scorer) {
      ctx.scorer->collect(stats.data(), nullptr, nullptr);
    }
    return memory::make_tracked<AllQuery>(ctx.memory, std::move(stats), boost);
  }

  Filter::Query::ptr Compile(const PrepareContext& ctx) && final {
    return CompileQuery(ctx, _boost);
  }
};

}  // namespace

std::unique_ptr<Filter::PrepareBuffer> All::CreateBuffer(
  const PrepareContext& ctx) const {
  return std::make_unique<Buffer>(ctx, Boost());
}

Filter::Query::ptr All::prepare(const PrepareContext& ctx) const {
  return Buffer::CompileQuery(ctx, ctx.boost * Boost());
}

}  // namespace irs
