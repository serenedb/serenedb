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
#include "iresearch/search/detail/posting_batch.hpp"
#include "iresearch/search/detail/resolve.hpp"
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

template<typename InputType>
class TermRange : public detail::PostingBatch<InputType, false> {
  using Base = detail::PostingBatch<InputType, false>;

  using Base::_in;
  using Base::_last;
  using Base::_left_in_list;
  using Base::_walk;
  using Base::In;
  using Base::kBlock;
  using Base::ReadDocs;
  using Base::SkipFreqs;

 public:
  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               IndexFeatures layout, bool bounds, bool freq) {
    _docs_count = meta.docs_count;
    this->SetFreqLen(freq);
    this->OpenInput(meta, doc_in, bounds);
    this->ArmWalk(meta, layout, bounds);
  }

  uint64_t AtLeast(doc_id_t doc) {
    if (!_walk.Armed()) {
      if (_cached == 0) {
        _cached = _docs_count;
        _left_in_list = _docs_count;
        ReadDocs(_block.data(), _cached);
        SkipFreqs(_cached);
      }
      return _docs_count - Below(_cached, doc);
    }
    const auto left = _walk.Seek(doc, *_in);
    if (left == 0) {
      return 0;
    }
    In().Seek(_walk.Landing().doc_ptr);
    _last = _walk.Landing().doc;
    _left_in_list = left;
    const auto len = std::min(left, kBlock);
    ReadDocs(_block.data(), len);
    SkipFreqs(len);
    return left - Below(len, doc);
  }

 private:
  uint32_t Below(uint32_t len, doc_id_t doc) const noexcept {
    uint32_t n = 0;
    while (n != len && _block[n] < doc) {
      ++n;
    }
    return n;
  }

  DocsBuf _block;
  uint32_t _docs_count = 0;
  uint32_t _cached = 0;
};

class TermCount : public Root {
 public:
  TermCount(const detail::PostingClause& posting, const Context& ctx) noexcept
    : _posting{posting}, _ctx{ctx} {}

  uint64_t Run(doc_id_t min, doc_id_t max) final {
    if (_ctx.table != nullptr) {
      if (!_exact) {
        _exact = MakeTermWalk(_posting, _ctx);
      }
      return _exact->Run(min, max);
    }
    if (!_ctx.partial) {
      return Count(min, max);
    }
    if (_pending_end == min) {
      _pending_end = max;
      return 0;
    }
    const auto flushed = Finish();
    _pending_begin = min;
    _pending_end = max;
    return flushed;
  }

  uint64_t Finish() final {
    if (_pending_begin == _pending_end) {
      return 0;
    }
    const auto count = Count(_pending_begin, _pending_end);
    _pending_begin = _pending_end = doc_limits::invalid();
    return count;
  }

 private:
  uint64_t Count(doc_id_t min, doc_id_t max) {
    const auto count = _posting.state.cookie.docs_count;
    const bool from_start = min == doc_limits::min();
    const bool to_end = doc_limits::eof(max);
    if (from_start && to_end) {
      return count;
    }
    if (count == 1) {
      const auto doc = doc_limits::min() + _posting.state.cookie.doc_delta;
      return min <= doc && doc < max;
    }
    const auto above = from_start ? count : Rank(min);
    const auto below = to_end ? 0 : Rank(max);
    SDB_ASSERT(above >= below);
    return above - below;
  }

  uint64_t Rank(doc_id_t doc) {
    if (doc != _rank_doc) {
      _rank_doc = doc;
      _rank = Source().AtLeast(doc);
    }
    return _rank;
  }

  struct RankSource {
    virtual ~RankSource() = default;
    virtual uint64_t AtLeast(doc_id_t doc) = 0;
  };

  template<typename InputType>
  struct RankSourceOf final : RankSource {
    uint64_t AtLeast(doc_id_t doc) final { return impl.AtLeast(doc); }
    TermRange<InputType> impl;
  };

  RankSource& Source() {
    if (!_ranks) {
      const auto& own = *_posting.state.reader;
      const auto& doc = *detail::DocOf(own);
      _ranks = detail::ResolveInput(
        doc, [&]<typename Input> -> std::unique_ptr<RankSource> {
          auto out = std::make_unique<RankSourceOf<Input>>();
          out->impl.Prepare(_posting.state.cookie, doc, detail::LayoutOf(own),
                            detail::BoundsOf(own), detail::FreqOf(own));
          return out;
        });
    }
    return *_ranks;
  }

  detail::PostingClause _posting;
  Context _ctx;
  Root::ptr _exact;
  std::unique_ptr<RankSource> _ranks;
  doc_id_t _rank_doc = doc_limits::invalid();
  uint64_t _rank = 0;
  doc_id_t _pending_begin = doc_limits::invalid();
  doc_id_t _pending_end = doc_limits::invalid();
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

  uint64_t Finish() final { return _lhs->Finish() + _rhs->Finish(); }

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
