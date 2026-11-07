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

#include <algorithm>
#include <atomic>
#include <bit>
#include <span>
#include <vector>

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
inline constexpr uint32_t kPartsPerThread = 8;
inline constexpr size_t kSerialLeaves = 256;

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

struct SharedOrBits {
  static constexpr auto kBits = detail::BitsetStorage::kBits;
  static constexpr auto kMin = detail::BitsetStorage::kMin;
  static constexpr bool kOrdered = false;

  uint64_t* words;

  static void Or(uint64_t& word, uint64_t bits) noexcept {
    if (bits != 0) {
      std::atomic_ref{word}.fetch_or(bits, std::memory_order_relaxed);
    }
  }

  void Run(uint64_t prev, uint32_t len) noexcept {
    const uint64_t begin = prev + 1 - kMin;
    const uint64_t end = begin + len;
    const auto first = begin / kBits;
    const auto last = (end - 1) / kBits;
    const uint64_t head = ~uint64_t{0} << (begin % kBits);
    const uint64_t tail = ~uint64_t{0} >> (kBits - 1 - (end - 1) % kBits);
    if (first == last) {
      Or(words[first], head & tail);
      return;
    }
    Or(words[first], head);
    for (auto i = first + 1; i != last; ++i) {
      std::atomic_ref{words[i]}.store(~uint64_t{0}, std::memory_order_relaxed);
    }
    Or(words[last], tail);
  }

  void Bitset(uint64_t prev, const UnalignedWord* src, uint32_t n,
              uint64_t) noexcept {
    const uint64_t begin = prev + 1 - kMin;
    auto* const dst = words + begin / kBits;
    const auto shift = static_cast<uint32_t>(begin % kBits);
    if (shift == 0) {
      for (uint32_t i = 0; i != n; ++i) {
        Or(dst[i], src[i]);
      }
      return;
    }
    uint64_t carry = 0;
    for (uint32_t i = 0; i != n; ++i) {
      const uint64_t word = src[i];
      Or(dst[i], (word << shift) | carry);
      carry = word >> (kBits - shift);
    }
    Or(dst[n], carry);
  }

  void Doc(size_t doc) noexcept {
    const uint64_t offset = doc - kMin;
    Or(words[offset / kBits], uint64_t{1} << (offset % kBits));
  }

  void Finish(uint32_t) noexcept {}
};

struct Found {
  explicit Found(IResourceManager& memory) : docs{{memory}}, terms{{memory}} {}

  ManagedVector<doc_id_t> docs;
  ManagedVector<MultiTermState::Entry> terms;
};

auto Collect(ManagedVector<doc_id_t>& docs,
             ManagedVector<MultiTermState::Entry>& terms,
             const byte_type* stats) {
  return [&docs, &terms, stats](bytes_view, const PostingMeta& meta) {
    if (meta.docs_count == 1) {
      docs.emplace_back(doc_limits::min() + meta.doc_delta);
    } else {
      terms.emplace_back(meta, kNoBoost, stats);
    }
  };
}

uint64_t SumDocs(std::span<const MultiTermState::Entry> terms) noexcept {
  uint64_t docs = 0;
  for (const auto& term : terms) {
    docs += term.cookie.docs_count;
  }
  return docs;
}

template<typename T>
void Append(ManagedVector<T>& to, const ManagedVector<T>& from) {
  to.insert(to.end(), from.begin(), from.end());
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
  auto& terms = state.Terms();
  const auto visit = Collect(docs, terms, stats);
  const BoxPruner pruner{options()};
  std::vector<Found> parts;
  if (ctx.tasks == nullptr) {
    reader->VisitPruned(pruner, visit);
  } else {
    const auto most = kPartsPerThread * ctx.tasks->Threads();
    PrunedSplit split;
    reader->SplitPruned(pruner, most, split, visit);
    const std::span<const uint32_t> pieces{split.pieces};
    if (pieces.empty() || (split.level == 0 && pieces.size() < kSerialLeaves)) {
      reader->VisitSplit(pruner, split.level, pieces, visit);
    } else {
      const auto chunks =
        static_cast<uint32_t>(std::min<size_t>(pieces.size(), most));
      parts.reserve(chunks);
      for (uint32_t chunk = 0; chunk != chunks; ++chunk) {
        parts.emplace_back(sub_ctx.memory);
      }
      ctx.tasks->Run(chunks, [&](PrepareTasks::Next next) {
        for (auto chunk = next(); chunk != chunks; chunk = next()) {
          auto& part = parts[chunk];
          const auto begin = pieces.size() * chunk / chunks;
          const auto end = pieces.size() * (chunk + 1) / chunks;
          reader->VisitSplit(pruner, split.level,
                             pieces.subspan(begin, end - begin),
                             Collect(part.docs, part.terms, stats));
        }
      });
    }
  }
  uint64_t singles = docs.size();
  uint64_t matched = SumDocs(terms);
  for (const auto& part : parts) {
    singles += part.docs.size();
    matched += SumDocs(part.terms);
  }
  matched += singles;
  if (singles == 0) {
    for (const auto& part : parts) {
      Append(terms, part.terms);
    }
    return state.Empty() ? QueryBuilder::Empty()
                         : MultiTermQuery::Finish(std::move(query), sub_ctx);
  }
  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  const auto* doc = detail::DocOf(*reader);
  if (matched * kSparseShare < detail::SegmentWords(docs_count)) {
    for (const auto& part : parts) {
      Append(docs, part.docs);
      Append(terms, part.terms);
    }
    if (!terms.empty()) {
      detail::ResolveInput(*doc, [&]<typename Input> {
        detail::PostingReader<Input> posting{*doc};
        AppendDocs sink{docs};
        detail::ReadTerms(std::span<const MultiTermState::Entry>{terms}, reader,
                          posting, sink);
      });
    }
    absl::c_sort(docs);
    auto sparse = memory::make_tracked<DocSetQuery>(
      sub_ctx.memory, segment, std::move(docs), sub_ctx.boost);
    sparse->SetStats(sub_ctx.Record());
    return sparse;
  }
  detail::BitsetStorage set{docs_count};
  if (parts.empty()) {
    detail::OrBits sink{set.Words()};
    if (!terms.empty()) {
      detail::ResolveInput(*doc, [&]<typename Input> {
        detail::PostingReader<Input> posting{*doc};
        detail::ReadTerms(std::span<const MultiTermState::Entry>{terms}, reader,
                          posting, sink);
      });
    }
    for (const auto id : docs) {
      sink.Doc(id);
    }
  } else {
    const auto chunks = static_cast<uint32_t>(parts.size()) + 1;
    ctx.tasks->Run(chunks, [&](PrepareTasks::Next next) {
      SharedOrBits sink{set.Words()};
      detail::ResolveInput(*doc, [&]<typename Input> {
        detail::PostingReader<Input> posting{*doc};
        for (auto chunk = next(); chunk != chunks; chunk = next()) {
          const auto& part_docs =
            chunk + 1 == chunks ? docs : parts[chunk].docs;
          const auto& part_terms =
            chunk + 1 == chunks ? terms : parts[chunk].terms;
          if (!part_terms.empty()) {
            detail::ReadTerms(
              std::span<const MultiTermState::Entry>{part_terms}, reader,
              posting, sink);
          }
          for (const auto id : part_docs) {
            sink.Doc(id);
          }
        }
      });
    });
  }
  SDB_ASSERT(matched == detail::CountBits(set));
  auto dense = memory::make_tracked<DocSetQuery>(
    sub_ctx.memory, segment, std::move(set), static_cast<uint32_t>(matched),
    sub_ctx.boost);
  dense->SetStats(sub_ctx.Record());
  return dense;
}

PrepareCollector::ptr ByCurveBox::MakeCollectorImpl(const Scorer* scorer,
                                                    StatsArena& stats,
                                                    uint32_t) const {
  return std::make_unique<AllCollector>(scorer, stats);
}

}  // namespace irs
