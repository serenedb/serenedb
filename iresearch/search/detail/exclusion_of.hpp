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

#pragma once

#include <cmath>
#include <span>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/bitset_of.hpp"
#include "iresearch/search/detail/boolean_groups.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/with_mask.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/leaves.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/probe/boolean_window.hpp"
#include "iresearch/search/probe/leaves.hpp"

namespace irs::detail {

template<typename Term>
bool ExcludeTerms(std::span<const Term> terms,
                  std::span<const QueryBuilder::ptr> filters,
                  const TermReader* field, const IndexInput*& doc) noexcept {
  if (!filters.empty() || terms.empty()) {
    return false;
  }
  doc = DocOf(FieldOf(terms.front(), field));
  return doc != nullptr;
}

enum class ExcludeForm {
  Probes,
  Window,
  Bitset,
};

template<typename Term>
class ExcludeCosts {
 public:
  ExcludeCosts(std::span<const Term> terms,
               std::span<const QueryBuilder::ptr> filters, uint64_t candidates,
               uint64_t span, doc_id_t docs_count, ExcludeUse use) noexcept
    : _candidates{static_cast<double>(candidates)},
      _words{static_cast<double>(SegmentWords(docs_count))},
      _windows{static_cast<double>(SegmentWindows(docs_count))} {
    double sparse = static_cast<double>(SumDocs(terms));
    for (const auto& child : filters) {
      SDB_ASSERT(child);
      sparse += ChildClauseCost(*child, docs_count).sparse;
    }
    const Reach reach{_candidates, _windows, span, docs_count, use, sparse};
    Totals totals;
    for (size_t i = 0; i != terms.size(); ++i) {
      Add(reach, TermClauseCost(CookieOf(terms[i]).docs_count, docs_count),
          totals);
    }
    for (const auto& child : filters) {
      Add(reach, ChildClauseCost(*child, docs_count), totals);
    }
    Finish(reach, totals);
  }

  double Best(bool bitsets) const noexcept {
    const auto lazy = std::min(_probes, _window);
    return bitsets ? std::min(lazy, _bitset) : lazy;
  }

  double Sparse(double sparse_lead, bool bitsets) const noexcept {
    return Best(bitsets) + sparse_lead * _candidates;
  }

  double WindowLead(double lead_docs, bool drains, bool refills,
                    bool bitsets) const noexcept {
    const double fill =
      std::min(kWindowLeadFillCost * lead_docs, kDenseBuildWordCost * _words);
    const double per_window = refills ? kWindowLeadCost : kWindowChildLeadCost;
    const double drain = drains ? DocsEmitCost(lead_docs, _words) : 0.0;
    return fill + _windows * per_window + drain + WindowedCost(bitsets);
  }

  ExcludeForm Probed(bool bitsets) const noexcept {
    return Form(bitsets, _probed_window, _bitset, _probed_probes);
  }

  ExcludeForm Windowed(bool bitsets) const noexcept {
    return Form(bitsets, _filled, _bitset_lead, _probes + ProbedLead());
  }

  bool TakesWindowLead(bool bitsets, bool window_drains, double sparse_lead,
                       bool window_refills,
                       uint64_t candidates) const noexcept {
    return WindowLead(static_cast<double>(candidates), window_drains,
                      window_refills, bitsets) < Sparse(sparse_lead, bitsets);
  }

 private:
  struct Reach {
    Reach(double candidates, double windows, uint64_t span, doc_id_t docs_count,
          ExcludeUse use, double sparse_docs) noexcept
      : c{candidates},
        s{static_cast<double>(std::max<uint64_t>(span, 1))},
        n{static_cast<double>(std::max<doc_id_t>(docs_count, 1))},
        blocks{c / kCostBlock},
        per_doc{use == ExcludeUse::PerDoc},
        sparse{sparse_docs} {
      touched =
        per_doc ? windows * (1.0 - std::exp(-c / windows))
                : std::min(windows,
                           blocks * (1.0 + kCostBlock * n / s /
                                             static_cast<double>(kWindowDocs)));
      const double lead_blocks = std::max(1.0, s / kCostBlock);
      batches = per_doc ? c : lead_blocks * (1.0 - std::exp(-c / lead_blocks));
      probed = per_doc ? c
                       : batches + std::max(c - batches, 0.0) *
                                     Dirty(*this, sparse_docs);
      spread = windows * (1.0 - std::exp(-c / windows));
    }

    double c;
    double s;
    double n;
    double blocks;
    bool per_doc;
    double sparse;
    double touched = 0;
    double batches = 0;
    double probed = 0;
    double spread = 0;
  };

  struct Totals {
    double probed_lazy = 0;
    double eager = 0;
    double hits = 0;
    double exact = 0;
  };

  static double Dirty(const Reach& reach, double docs) noexcept {
    return 1.0 - std::exp(-kCostBlock * docs / reach.s);
  }

  void Add(const Reach& reach, const ClauseCost& clause, Totals& totals) {
    if (clause.mask) {
      AddMask(reach, clause, totals);
      return;
    }
    const double c = reach.c;
    const auto d = static_cast<double>(clause.docs);
    const auto leaves = static_cast<double>(clause.leaves);
    const double reached = d * std::min(1.0, c / reach.s);
    const double steps = std::min(reached, c);
    const double r = c > 0 ? reached / c : 0.0;
    const double step = kStepBaseCost + kStepJumpCost * std::max(0.0, 1.0 - r);
    const double share = std::min(1.0, c / reach.n);
    const double block_probe =
      clause.block_probe +
      (clause.exact ? kBlockSeekCost * std::min(1.0, 4.0 * d / reach.s) : 0.0);
    double probes = reach.per_doc
                      ? c * clause.probe
                      : reach.blocks * block_probe +
                          c * Dirty(reach, clause.sparse) * clause.probe;
    probes += steps * step +
              kDecodeCost * std::min(reached, c * kCostBlock * leaves) +
              clause.matches * share * clause.hit;
    _probes += probes;
    const double refill =
      reach.touched * (clause.exact    ? kWindowRefillCost * leaves
                       : clause.nested ? kWindowNestedRefillCost
                                       : kWindowChildRefillCost +
                                           kWindowChildSeekCost *
                                             (1.0 - reach.touched / _windows));
    const double lazy = refill + clause.lazy_fill * (reach.touched / _windows);
    _lazy += lazy;
    if (clause.exact && clause.leaves == 1) {
      _probed_probes += ProbedTermCost(reach, d, share, clause);
      totals.probed_lazy += reach.spread * kWindowRefillCost +
                            clause.lazy_fill * (reach.spread / _windows);
    } else {
      _probed_probes += probes;
      totals.probed_lazy += lazy;
    }
    _filled += (clause.exact ? kFilledCost * d : clause.fill) + refill;
    totals.eager += clause.exact ? clause.fill
                    : clause.nested
                      ? clause.fill + _windows * kEagerNestedWindowCost
                      : clause.fill + _windows * kEagerChildWindowCost;
    totals.hits += d * share;
    totals.exact += clause.exact ? 1.0 : 0.0;
  }

  void AddMask(const Reach& reach, const ClauseCost& clause,
               Totals& totals) noexcept {
    const double probes =
      reach.per_doc ? reach.c * clause.probe
                    : reach.blocks * clause.block_probe +
                        reach.c * Dirty(reach, clause.sparse) * clause.probe;
    const double refill = reach.touched * kMaskWindowCost;
    const double lazy = refill + clause.lazy_fill * (reach.touched / _windows);
    _probes += probes;
    _probed_probes += probes;
    _lazy += lazy;
    totals.probed_lazy += lazy;
    _filled += clause.fill + refill;
    totals.eager += clause.fill;
  }

  static double ProbedTermCost(const Reach& reach, double d, double share,
                               const ClauseCost& clause) noexcept {
    const double leaf = static_cast<double>(doc_limits::kBlockSize);
    const double probed = reach.probed;
    const double advances =
      probed > 0 ? probed * (1.0 - std::exp(-d / probed)) : 0.0;
    return probed * kLeafProbeCost +
           advances * static_cast<double>(kProbeSteps) +
           std::min(advances, std::max(1.0, d / leaf)) *
             (kStepBaseCost + kStepJumpCost + leaf * kDecodeCost) +
           clause.matches * share * clause.hit;
  }

  void Finish(const Reach& reach, const Totals& totals) noexcept {
    const double c = reach.c;
    if (totals.exact > 1.0) {
      _probes += (reach.per_doc ? c : c * Dirty(reach, reach.sparse)) *
                 kLeafProbeCost * totals.exact;
    }
    const double tests =
      reach.per_doc ? c * kWindowTestCost
                    : reach.blocks * kWindowBlockCost +
                        c * Dirty(reach, reach.sparse) * kWindowDirtyTestCost;
    _window = _lazy + tests;
    _probed_window = totals.probed_lazy + tests;
    const double built = _words * kBitClearCost + totals.eager;
    _bitset = built + (reach.per_doc ? c * kBitTestCost + totals.hits * kHitCost
                                     : reach.batches * kBitsetBatchCost +
                                         c * kBitsetCandidateCost);
    _bitset_lead = built + c * kBitsetProbeCost;
  }

  double ProbedLead() const noexcept { return _candidates * kProbedLeadCost; }

  double WindowedCost(bool bitsets) const noexcept {
    const auto lazy = std::min(_probes + ProbedLead(), _filled);
    return bitsets ? std::min(lazy, _bitset_lead) : lazy;
  }

  ExcludeForm Form(bool bitsets, double window, double bitset,
                   double probes) const noexcept {
    const auto other = bitsets ? std::min(window, bitset) : window;
    if (probes <= other) {
      return ExcludeForm::Probes;
    }
    if (bitsets && bitset < window) {
      return ExcludeForm::Bitset;
    }
    return ExcludeForm::Window;
  }

  double _candidates = 0;
  double _words = 0;
  double _windows = 0;
  double _probes = 0;
  double _probed_probes = 0;
  double _lazy = 0;
  double _filled = 0;
  double _window = 0;
  double _probed_window = 0;
  double _bitset = 0;
  double _bitset_lead = 0;
};

template<typename Result, typename Input, bool kConcrete = true, typename Term,
         typename Make>
Result BuildExcludeProbes(std::span<const Term> metas,
                          std::span<const QueryBuilder::ptr> filters,
                          const TermReader* field, const SubReader& segment,
                          uint64_t candidates, Make&& make) {
  if constexpr (kConcrete) {
    const IndexInput* doc = nullptr;
    if (ExcludeTerms(metas, filters, field, doc)) {
      const auto concrete = [&]<typename In> -> Result {
        using Probe = PostingProbe<In>;
        if (metas.size() == 1) {
          const auto& own = FieldOf(metas.front(), field);
          return make.template operator()<Probe>(
            std::forward_as_tuple(CookieOf(metas.front()), *DocOf(own),
                                  LayoutOf(own), BoundsOf(own)));
        }
        return make.template operator()<probe::OrLeaves<Probe>>(
          std::forward_as_tuple(metas.size(), [&](Probe& probe, size_t i) {
            const auto& own = FieldOf(metas[i], field);
            probe.Prepare(CookieOf(metas[i]), *DocOf(own), LayoutOf(own),
                          BoundsOf(own));
          }));
      };
      if constexpr (std::is_void_v<Input>) {
        return ResolveInput(*doc, concrete);
      } else {
        return concrete.template operator()<Input>();
      }
    }
  }
  std::vector<probe::Erased> probes;
  probes.reserve(metas.size() + filters.size());
  const auto ask = [&](const PostingClause& posting,
                       const QueryBuilder* child) noexcept {
    auto node = ProbeOf(posting, child, segment, candidates);
    if (!node) {
      return false;
    }
    probes.emplace_back(std::move(node));
    return true;
  };
  if (!VisitOrderedOf(
        metas, filters, false, 0, std::numeric_limits<size_t>::max(),
        [&](const Term& term) { return ask(ClauseOf(term, field), nullptr); },
        [&](const QueryBuilder& child) {
          return ask(PostingClause{TermState{nullptr, PostingMeta{}}}, &child);
        })) {
    return {};
  }
  if (probes.size() == 1) {
    return make.template operator()<probe::Erased>(
      std::forward_as_tuple(std::move(probes.front())));
  }
  using Exclude = probe::OrLeaves<probe::Erased>;
  return make.template operator()<Exclude>(std::forward_as_tuple(
    probes.size(),
    [&](probe::Erased& leaf, size_t i) { leaf = std::move(probes[i]); }));
}

template<typename Result, typename Term, typename Make>
Result BuildExcludeFills(std::span<const Term> metas,
                         std::span<const QueryBuilder::ptr> filters,
                         const TermReader* field, const SubReader& segment,
                         Make&& make) {
  std::vector<FillNode::ptr> fills;
  if (!CollectFills(metas, filters, field, segment, fills)) {
    return {};
  }
  return make(
    std::forward_as_tuple(fills.size(), [&](fill::Erased& leaf, size_t i) {
      leaf = fill::Erased{std::move(fills[i])};
    }));
}

inline constexpr auto kNoMaskFold = [](BitsetStorage&) noexcept {};

template<typename Result, typename Input, bool kConcrete, typename Term,
         typename Fold, typename Make>
Result BuildRestExcludes(ExcludeUse use, std::span<const Term> metas,
                         std::span<const QueryBuilder::ptr> filters,
                         std::span<const QueryBuilder::ptr> costed,
                         const TermReader* field, const SubReader& segment,
                         uint64_t candidates, uint64_t span, Fold&& fold,
                         Make&& make) {
  SDB_ASSERT(!metas.empty() || !filters.empty());
  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  const auto* const doc = SegmentDoc(segment);
  const ExcludeCosts<Term> costs{metas, costed,     candidates,
                                 span,  docs_count, use};
  switch (costs.Probed(doc != nullptr)) {
    case ExcludeForm::Probes:
      return BuildExcludeProbes<Result, Input, kConcrete>(
        metas, filters, field, segment, candidates, make);
    case ExcludeForm::Bitset:
      return BuildExcludeBitset<Result>(
        metas, filters, field, segment, *doc, [&](auto&& set) -> Result {
          fold(set);
          return make.template operator()<probe::BitsetDocs>(
            std::forward_as_tuple(std::forward<decltype(set)>(set)));
        });
    case ExcludeForm::Window:
      break;
  }
  using Window = probe::BooleanWindow<OrGroup<fill::SetLeaves<fill::Erased>>>;
  return BuildExcludeFills<Result>(
    metas, filters, field, segment, [&](auto&& leaves) -> Result {
      return make.template operator()<Window>(std::forward_as_tuple(
        std::piecewise_construct, std::forward<decltype(leaves)>(leaves)));
    });
}

template<typename Result, typename Folded, typename Term, typename Build,
         typename Make>
Result PeelMask(std::span<const Term> metas,
                std::span<const QueryBuilder::ptr> filters,
                const SubReader& segment, Build&& build, Make&& make) {
  SDB_ASSERT(!metas.empty() || !filters.empty());
  const auto split = SplitMask(filters);
  if (!split.masked) {
    return build.template operator()<true>(filters, kNoMaskFold, make);
  }
  return ResolveDocsMask(
    segment, [&]<DocsMaskType Mask>(Mask docs_mask) -> Result {
      if (metas.empty() && split.rest.empty()) {
        return make.template operator()<Mask>(
          std::forward_as_tuple(std::move(docs_mask)));
      }
      return build.template operator()<false>(
        split.rest,
        [&](BitsetStorage& set) noexcept {
          docs_mask.FillOr(BitsetStorage::kMin, set.End(), set.Words());
        },
        [&]<typename Rest>(auto&& rest) -> Result {
          if constexpr (std::is_same_v<Rest, Folded>) {
            return make.template operator()<Rest>(
              std::forward<decltype(rest)>(rest));
          } else {
            return make.template operator()<WithMask<Mask, Rest>>(
              std::forward_as_tuple(std::piecewise_construct,
                                    std::move(docs_mask),
                                    std::forward<decltype(rest)>(rest)));
          }
        });
    });
}

template<typename Result, typename Input, typename Term, typename Make>
Result BuildExcludesOf(ExcludeUse use, std::span<const Term> metas,
                       std::span<const QueryBuilder::ptr> filters,
                       const TermReader* field, const SubReader& segment,
                       uint64_t candidates, uint64_t span, Make&& make) {
  return PeelMask<Result, probe::BitsetDocs>(
    metas, filters, segment,
    [&]<bool kConcrete>(std::span<const QueryBuilder::ptr> rest, auto&& fold,
                        auto&& wrap) -> Result {
      return BuildRestExcludes<Result, Input, kConcrete>(
        use, metas, rest, filters, field, segment, candidates, span, fold,
        wrap);
    },
    make);
}

template<typename Result, typename Input, typename Term, typename Make>
Result BuildExcludeSideOf(std::span<const Term> metas,
                          std::span<const QueryBuilder::ptr> filters,
                          const TermReader* field, const SubReader& segment,
                          uint64_t candidates, uint64_t span, Make&& make) {
  return BuildExcludesOf<Result, Input, Term>(
    ExcludeUse::PerDoc, metas, filters, field, segment, candidates, span,
    std::forward<Make>(make));
}

template<typename Result, typename Input, typename Term, typename Make>
Result BuildExcludeSideOf(std::span<const Term> metas,
                          std::span<const QueryBuilder::ptr> filters,
                          const TermReader* field, const SubReader& segment,
                          uint64_t candidates, Make&& make) {
  return BuildExcludeSideOf<Result, Input, Term>(metas, filters, field, segment,
                                                 candidates, candidates,
                                                 std::forward<Make>(make));
}

template<typename Result, typename Term, typename Make>
Result BuildExcludeSide(std::span<const Term> terms,
                        std::span<const QueryBuilder::ptr> filters,
                        const TermReader* field, const SubReader& segment,
                        uint64_t candidates, uint64_t span, Make&& make) {
  return BuildExcludesOf<Result, void, Term>(ExcludeUse::PerDoc, terms, filters,
                                             field, segment, candidates, span,
                                             std::forward<Make>(make));
}

template<typename Result, typename Term, typename Make>
Result BuildExcludeSide(std::span<const Term> terms,
                        std::span<const QueryBuilder::ptr> filters,
                        const TermReader* field, const SubReader& segment,
                        uint64_t candidates, Make&& make) {
  return BuildExcludeSide<Result, Term>(terms, filters, field, segment,
                                        candidates, candidates,
                                        std::forward<Make>(make));
}

template<typename Result, typename Input, typename Term, typename Make>
Result BuildBlockExcludesOf(std::span<const Term> metas,
                            std::span<const QueryBuilder::ptr> filters,
                            const TermReader* field, const SubReader& segment,
                            uint64_t candidates, uint64_t lead, Make&& make) {
  return BuildExcludesOf<Result, Input, Term>(
    ExcludeUse::PerBlock, metas, filters, field, segment, candidates, lead,
    std::forward<Make>(make));
}

template<typename Result, typename Term, typename Make>
Result BuildBlockExcludes(std::span<const Term> terms,
                          std::span<const QueryBuilder::ptr> filters,
                          const TermReader* field, const SubReader& segment,
                          uint64_t candidates, uint64_t lead, Make&& make) {
  return BuildExcludesOf<Result, void, Term>(
    ExcludeUse::PerBlock, terms, filters, field, segment, candidates, lead,
    std::forward<Make>(make));
}

template<typename Result, typename Term, typename Make>
Result BuildExcludeBitset(std::span<const Term> metas,
                          std::span<const QueryBuilder::ptr> filters,
                          const TermReader* field, const SubReader& segment,
                          const IndexInput& doc, Make&& make) {
  BitsetBuckets buckets;
  auto& clause = buckets.must.emplace_back();
  clause.reserve(metas.size());
  for (size_t i = 0; i != metas.size(); ++i) {
    clause.emplace_back(ClauseOf(metas[i], field));
  }
  for (const auto& child : filters) {
    SDB_ASSERT(child);
    auto node = child->PlanFill({}, ScoreMergeType::Noop);
    if (!node) {
      return {};
    }
    buckets.fills.emplace_back(std::move(node));
  }
  if (clause.empty()) {
    buckets.must.clear();
  }
  return make(
    BuildBitset(buckets, doc, static_cast<doc_id_t>(segment.docs_count())));
}

template<typename Result, bool kConcrete, typename Term, typename Fold,
         typename Make>
Result BuildRestWindowExcludes(std::span<const Term> metas,
                               std::span<const QueryBuilder::ptr> filters,
                               std::span<const QueryBuilder::ptr> costed,
                               const TermReader* field,
                               const SubReader& segment, uint64_t candidates,
                               Fold&& fold, Make&& make) {
  SDB_ASSERT(!metas.empty() || !filters.empty());
  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  const auto* const doc = SegmentDoc(segment);
  const ExcludeCosts<Term> costs{metas,      costed,     candidates,
                                 candidates, docs_count, ExcludeUse::PerDoc};
  const auto probed = [&]<typename Probe>(auto&& probe) -> Result {
    return make.template operator()<fill::ProbedAndNot<Probe>>(
      std::forward_as_tuple(std::piecewise_construct,
                            std::forward<decltype(probe)>(probe)));
  };
  switch (costs.Windowed(doc != nullptr)) {
    case ExcludeForm::Probes:
      return BuildExcludeProbes<Result, void, kConcrete>(
        metas, filters, field, segment, candidates, probed);
    case ExcludeForm::Bitset:
      return BuildExcludeBitset<Result>(
        metas, filters, field, segment, *doc, [&](auto&& set) -> Result {
          fold(set);
          return probed.template operator()<probe::BitsetDocs>(
            std::forward_as_tuple(std::forward<decltype(set)>(set)));
        });
    case ExcludeForm::Window:
      break;
  }
  using Excludes = fill::FilledAndNot<fill::SetLeaves<fill::Erased>>;
  return BuildExcludeFills<Result>(
    metas, filters, field, segment, [&](auto&& leaves) -> Result {
      return make.template operator()<Excludes>(std::forward_as_tuple(
        std::piecewise_construct, std::forward<decltype(leaves)>(leaves)));
    });
}

template<typename Result, typename Term, typename Make>
Result BuildWindowExcludes(std::span<const Term> metas,
                           std::span<const QueryBuilder::ptr> filters,
                           const TermReader* field, const SubReader& segment,
                           uint64_t candidates, Make&& make) {
  return PeelMask<Result, fill::ProbedAndNot<probe::BitsetDocs>>(
    metas, filters, segment,
    [&]<bool kConcrete>(std::span<const QueryBuilder::ptr> rest, auto&& fold,
                        auto&& wrap) -> Result {
      return BuildRestWindowExcludes<Result, kConcrete>(
        metas, rest, filters, field, segment, candidates, fold, wrap);
    },
    make);
}

}  // namespace irs::detail
