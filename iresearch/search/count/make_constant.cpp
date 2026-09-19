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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/count/constant.hpp"
#include "iresearch/search/count/plan.hpp"
#include "iresearch/search/count/walk.hpp"
#include "iresearch/search/filters/all_filter.hpp"
#include "iresearch/search/lead/all_docs.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/lead/plan.hpp"
#include "iresearch/search/queries/term_query.hpp"

namespace irs::count {

Root::ptr MakeConstant(uint64_t count) {
  return memory::make_managed<Constant>(count);
}

namespace {

Root::ptr MakeTermWalk(const detail::PostingClause& posting,
                       const Context& ctx) {
  return lead::ResolvePostingDocs<Root::ptr>(
    posting, [&]<typename Leaf>(auto&&... args) -> Root::ptr {
      return MakeShape<Walk, Leaf>(ctx, std::forward<decltype(args)>(args)...);
    });
}

class TermCount : public Root {
 public:
  TermCount(const detail::PostingClause& posting, const Context& ctx) noexcept
    : _posting{posting}, _ctx{ctx} {}

  uint64_t Run(doc_id_t min, doc_id_t max) final {
    if (min == doc_limits::min() && doc_limits::eof(max)) {
      return _posting.state.cookie.docs_count;
    }
    if (!_exact) {
      _exact = MakeTermWalk(_posting, _ctx);
    }
    return _exact->Run(min, max);
  }

 private:
  detail::PostingClause _posting;
  Context _ctx;
  Root::ptr _exact;
};

class AllCount : public Root {
 public:
  explicit AllCount(doc_id_t count) noexcept
    : _end{doc_limits::min() + count} {}

  uint64_t Run(doc_id_t min, doc_id_t max) final {
    const auto stop = std::min(max, _end);
    return stop > min ? stop - min : 0;
  }

 private:
  doc_id_t _end;
};

class Sum : public Root {
 public:
  Sum(Root::ptr lhs, Root::ptr rhs) noexcept
    : _lhs{std::move(lhs)}, _rhs{std::move(rhs)} {}

  uint64_t Run(doc_id_t min, doc_id_t max) final {
    return _lhs->Run(min, max) + _rhs->Run(min, max);
  }

 private:
  Root::ptr _lhs;
  Root::ptr _rhs;
};

}  // namespace

Root::ptr MakeAllCount(doc_id_t count) {
  return memory::make_managed<AllCount>(count);
}

Root::ptr MakeTermCount(const detail::PostingClause& posting,
                        const Context& ctx) {
  return memory::make_managed<TermCount>(posting, ctx);
}

Root::ptr MakeSum(Root::ptr lhs, Root::ptr rhs) {
  return memory::make_managed<Sum>(std::move(lhs), std::move(rhs));
}

Root::ptr MakeTerm(const detail::PostingClause& posting, const SubReader&,
                   const Context& ctx) {
  if (ctx.table == nullptr) {
    return memory::make_managed<TermCount>(posting, ctx);
  }
  return MakeTermWalk(posting, ctx);
}

Root::ptr MakeAll(const SubReader& segment, const Context& ctx) {
  if (ctx.table == nullptr &&
      segment.live_docs_count() == segment.docs_count()) {
    return memory::make_managed<AllCount>(
      static_cast<doc_id_t>(segment.docs_count()));
  }
  return MakeShape<Walk, lead::AllDocs>(ctx, segment);
}

Root::ptr Make(const TermQuery& query, const Context& ctx) {
  return MakeTerm(detail::PostingClause{query.State()}, query.Segment(), ctx);
}

Root::ptr Make(const AllQuery& query, const Context& ctx) {
  return MakeAll(query.Segment(), ctx);
}

}  // namespace irs::count
