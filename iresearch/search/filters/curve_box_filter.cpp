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

#include "curve_box_filter.hpp"

#include <absl/algorithm/container.h>

#include <bit>
#include <span>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/bitset_build.hpp"
#include "iresearch/search/detail/bitset_of.hpp"
#include "iresearch/search/detail/collectors.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/term_iterator.hpp"
#include "iresearch/search/queries/doc_set_query.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"

namespace irs {
namespace {

inline constexpr uint64_t kSparseShare = 16;

class BoxPruner final : public RangePruner {
 public:
  explicit BoxPruner(const ByCurveBoxOptions& options) noexcept
    : _test{options.box, options.curve} {}

  RangeFit Fit(bytes_view first, bytes_view last) const final {
    switch (_test.ClassifyRange(first.data(), last.data())) {
      case curve::Relation::Outside:
        return RangeFit::Outside;
      case curve::Relation::Inside:
        return RangeFit::Inside;
      case curve::Relation::Boundary:
        return RangeFit::Partial;
    }
    SDB_UNREACHABLE();
  }

  uint64_t Select(bytes_view prefix, const byte_type* keys, uint32_t pitch,
                  uint32_t width, uint32_t count) const final {
    return _test.Select(prefix, keys, pitch, width, count);
  }

 private:
  curve::BoxTest _test;
};

struct AppendDocs {
  static constexpr bool kOrdered = false;

  ManagedVector<doc_id_t>& docs;

  void Run(uint64_t prev, uint32_t len) {
    for (uint32_t i = 1; i <= len; ++i) {
      docs.emplace_back(static_cast<doc_id_t>(prev + i));
    }
  }

  void Bitset(uint64_t prev, const UnalignedWord* src, uint32_t n, uint64_t) {
    for (uint32_t word = 0; word != n; ++word) {
      for (uint64_t bits = src[word]; bits != 0; bits &= bits - 1) {
        docs.emplace_back(
          static_cast<doc_id_t>(prev + 1 + word * detail::BitsetStorage::kBits +
                                static_cast<uint32_t>(std::countr_zero(bits))));
      }
    }
  }

  void Doc(size_t doc) { docs.emplace_back(static_cast<doc_id_t>(doc)); }
};

uint64_t SumDocs(std::span<const MultiTermState::Entry> terms) noexcept {
  uint64_t docs = 0;
  for (const auto& term : terms) {
    docs += term.cookie.docs_count;
  }
  return docs;
}

}  // namespace

QueryBuilder::ptr ByCurveBox::PrepareSegment(const SubReader& segment,
                                             const PrepareContext& ctx) const {
  auto sub_ctx = ctx;
  sub_ctx.boost *= GetBoost();
  const auto* reader = segment.field(field_id(), dict());
  if (!reader) {
    return QueryBuilder::Empty();
  }
  auto query = memory::make_tracked<MultiTermQuery>(
    sub_ctx.memory, segment, sub_ctx.memory, sub_ctx.boost,
    ScoreMergeType::Sum);
  auto& state = query->State();
  state.Prepare(reader);
  const auto* const stats = sub_ctx.Record().stats;
  ManagedVector<doc_id_t> docs{{sub_ctx.memory}};
  const BoxPruner pruner{options()};
  reader->VisitPruned(pruner, [&](bytes_view, const PostingMeta& meta) {
    if (meta.docs_count == 1) {
      docs.emplace_back(doc_limits::min() + meta.doc_delta);
    } else {
      state.Push(meta, kNoBoost, stats);
    }
  });
  if (docs.empty()) {
    return state.Empty() ? QueryBuilder::Empty()
                         : MultiTermQuery::Finish(std::move(query), sub_ctx);
  }
  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  const std::span<const MultiTermState::Entry> terms{state.Terms()};
  const auto* doc = detail::DocOf(*reader);
  if ((docs.size() + SumDocs(terms)) * kSparseShare <
      detail::SegmentWords(docs_count)) {
    if (!terms.empty()) {
      detail::ResolveInput(*doc, [&]<typename Input> {
        detail::PostingReader<Input> posting{*doc};
        AppendDocs sink{docs};
        detail::ReadTerms(terms, reader, posting, sink);
      });
    }
    absl::c_sort(docs);
    auto sparse = memory::make_tracked<DocSetQuery>(
      sub_ctx.memory, segment, std::move(docs), sub_ctx.boost);
    sparse->SetStats(sub_ctx.Record());
    return sparse;
  }
  detail::BitsetStorage set{docs_count};
  detail::OrBits sink{set.Words()};
  if (!terms.empty()) {
    detail::ResolveInput(*doc, [&]<typename Input> {
      detail::PostingReader<Input> posting{*doc};
      detail::ReadTerms(terms, reader, posting, sink);
    });
  }
  for (const auto id : docs) {
    sink.Doc(id);
  }
  const auto matched = static_cast<uint32_t>(detail::CountBits(set));
  auto dense = memory::make_tracked<DocSetQuery>(
    sub_ctx.memory, segment, std::move(set), matched, sub_ctx.boost);
  dense->SetStats(sub_ctx.Record());
  return dense;
}

PrepareCollector::ptr ByCurveBox::MakeCollectorImpl(const Scorer* scorer,
                                                    StatsArena& stats,
                                                    uint32_t) const {
  return std::make_unique<AllCollector>(scorer, stats);
}

}  // namespace irs
