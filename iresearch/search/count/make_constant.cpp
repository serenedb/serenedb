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

#include <algorithm>

#include "iresearch/error/error.hpp"
#include "iresearch/formats/posting/format_block_128.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/count/constant.hpp"
#include "iresearch/search/count/plan.hpp"
#include "iresearch/search/count/walk.hpp"
#include "iresearch/search/detail/enc_buf.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/skip_walk.hpp"
#include "iresearch/search/filters/all_filter.hpp"
#include "iresearch/search/lead/all_docs.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/lead/plan.hpp"
#include "iresearch/search/queries/term_query.hpp"
#include "iresearch/utils/down_cast.hpp"

namespace irs::count {
namespace {

template<typename Input>
uint64_t CountInRange(const PostingMeta& meta, const IndexInput& doc_in,
                      IndexFeatures layout, bool bounds, DocRange range) {
  auto owned = doc_in.Reopen();
  if (!owned) [[unlikely]] {
    throw IoError{"failed to reopen document input"};
  }
  auto& in = irs::utils::downCast<Input>(*owned);
  in.Seek(meta.doc_start);
  if (meta.docs_count < doc_limits::kBlockSize) {
    SkipScoreBounds(bounds, in);
  }
  DocsBuf docs;
  detail::NeedEnc<Input> enc_buf;
  auto* const enc = detail::EncOf<Input>(enc_buf);
  const auto* const last = std::cend(docs);
  const auto below = [&](uint32_t len, doc_id_t bound) {
    const auto* const first = last - len;
    return static_cast<uint32_t>(std::lower_bound(first, last, bound) - first);
  };

  if (meta.docs_count <= doc_limits::kBlockSize) {
    FormatTraits128::ReadTailDelta(meta.docs_count, in, enc, docs,
                                   doc_limits::invalid());
    return below(meta.docs_count, range.end) -
           below(meta.docs_count, range.begin);
  }

  const auto cut = detail::CutWindow<Input>(
    meta, in, detail::SkipShapeOf(layout, bounds), range);
  uint64_t total = cut.left - cut.after;
  if (total == 0) {
    return 0;
  }
  if (cut.landed) {
    const auto len =
      static_cast<uint32_t>(std::min<uint64_t>(total, doc_limits::kBlockSize));
    in.Seek(cut.landing.doc_ptr);
    FormatTraits128::ReadTailDelta(len, in, enc, docs, cut.landing.doc);
    total -= below(len, range.begin);
  }
  if (cut.end_landed) {
    const auto len = cut.end_left - cut.after;
    in.Seek(cut.end_landing.doc_ptr);
    FormatTraits128::ReadTailDelta(len, in, enc, docs, cut.end_landing.doc);
    total -= len - below(len, range.end);
  }
  return total;
}

}  // namespace

Root::ptr MakeConstant(uint64_t count) {
  return memory::make_managed<Constant>(count);
}

Root::ptr MakeTerm(const detail::PostingClause& posting, const SubReader&,
                   const Context& ctx) {
  if (ctx.table == nullptr) {
    const auto& meta = posting.state.cookie;
    if (!ctx.range.Bounded()) {
      return MakeConstant(meta.docs_count);
    }
    if (meta.docs_count == 1) {
      const auto doc = doc_limits::min() + meta.doc_delta;
      return MakeConstant(ctx.range.Contains(doc) ? 1 : 0);
    }
    SDB_ASSERT(posting.state.reader != nullptr);
    const auto& field = *posting.state.reader;
    const auto& doc_in = *detail::DocOf(field);
    return detail::ResolveInput(doc_in, [&]<typename Input> -> Root::ptr {
      return MakeConstant(
        CountInRange<Input>(meta, doc_in, detail::LayoutOf(field),
                            detail::BoundsOf(field), ctx.range));
    });
  }
  return lead::ResolvePostingDocs<Root::ptr>(
    posting, ctx.range, [&]<typename Leaf>(auto&&... args) -> Root::ptr {
      return MakeShape<Walk, Leaf>(ctx, std::forward<decltype(args)>(args)...);
    });
}

Root::ptr MakeAll(const SubReader& segment, const Context& ctx) {
  if (ctx.table == nullptr) {
    const auto live = segment.live_docs_count();
    if (!ctx.range.Bounded()) {
      return MakeConstant(live);
    }
    if (live == segment.docs_count()) {
      const auto begin = std::max(ctx.range.begin, doc_limits::min());
      const auto end = std::min<doc_id_t>(
        ctx.range.end, doc_limits::min() + static_cast<doc_id_t>(live));
      return MakeConstant(end > begin ? end - begin : 0);
    }
  }
  return MakeShape<Walk, lead::AllDocs>(ctx, segment, ctx.range);
}

Root::ptr Make(const TermQuery& query, const Context& ctx) {
  return MakeTerm(detail::PostingClause{query.State()}, query.Segment(), ctx);
}

Root::ptr Make(const AllQuery& query, const Context& ctx) {
  return MakeAll(query.Segment(), ctx);
}

}  // namespace irs::count
