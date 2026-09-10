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
#include "iresearch/search/common/bitset_of.hpp"
#include "iresearch/search/common/boolean_groups.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/leaves.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/probe/boolean_window.hpp"
#include "iresearch/search/probe/leaves.hpp"

namespace irs::search {

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

enum class ExcludeForm { Probes, Window, Bitset };

template<typename Term>
class ExcludeCosts {
 public:
  ExcludeCosts(std::span<const Term> terms,
               std::span<const QueryBuilder::ptr> filters, uint64_t candidates,
               uint64_t span, doc_id_t docs_count, ExcludeUse use) noexcept
    : _candidates{static_cast<double>(candidates)},
      _words{static_cast<double>(SegmentWords(docs_count))},
      _windows{static_cast<double>(SegmentWindows(docs_count))} {
    const double c = _candidates;
    const double s = static_cast<double>(std::max<uint64_t>(span, 1));
    const double n = static_cast<double>(std::max<doc_id_t>(docs_count, 1));
    const double block = static_cast<double>(doc_limits::kBlockSize);
    const double blocks = c / block;
    const bool per_doc = use == ExcludeUse::PerDoc;
    const double touched =
      per_doc
        ? _windows * (1.0 - std::exp(-c / _windows))
        : std::min(
            _windows,
            blocks * (1.0 + block * n / s / static_cast<double>(kWindowDocs)));
    const auto dirty = [&](double sparse) {
      return 1.0 - std::exp(-block * sparse / s);
    };
    double eager = 0;
    double sparse = 0;
    double hits = 0;
    double exact = 0;
    const auto add = [&](const ClauseCost& clause) {
      const auto d = static_cast<double>(clause.docs);
      const auto leaves = static_cast<double>(clause.leaves);
      const double reach = d * std::min(1.0, c / s);
      const double steps = std::min(reach, c);
      const double r = c > 0 ? reach / c : 0.0;
      const double step =
        kStepBaseCost + kStepJumpCost * std::max(0.0, 1.0 - r);
      const double share = std::min(1.0, c / n);
      const double block_probe =
        clause.block_probe +
        (clause.exact ? kBlockSeekCost * std::min(1.0, 4.0 * d / s) : 0.0);
      double probes = per_doc ? c * clause.probe
                              : blocks * block_probe +
                                  c * dirty(clause.sparse) * clause.probe;
      probes += steps * step +
                kDecodeCost * std::min(reach, c * block * leaves) +
                clause.matches * share * clause.hit;
      _probes += probes;
      const double refill =
        touched * (clause.exact ? kWindowRefillCost * leaves
                   : clause.nested
                     ? kWindowNestedRefillCost
                     : kWindowChildRefillCost +
                         kWindowChildSeekCost * (1.0 - touched / _windows));
      _lazy += refill + clause.lazy_fill * (touched / _windows);
      _filled += (clause.exact ? kFilledCost * d : clause.fill) + refill;
      eager += clause.exact    ? clause.fill
               : clause.nested ? clause.fill + _windows * kEagerNestedWindowCost
                               : clause.fill + _windows * kEagerChildWindowCost;
      sparse += clause.sparse;
      hits += d * share;
      exact += clause.exact ? 1.0 : 0.0;
    };
    for (size_t i = 0; i != terms.size(); ++i) {
      add(TermClauseCost(CookieOf(terms[i]).docs_count, docs_count));
    }
    for (const auto& child : filters) {
      SDB_ASSERT(child);
      add(ChildClauseCost(*child, docs_count));
    }
    if (exact > 1.0) {
      _probes += (per_doc ? c : c * dirty(sparse)) * kLeafProbeCost * exact;
    }
    _window = _lazy + (per_doc ? c * kWindowTestCost
                               : blocks * kWindowBlockCost +
                                   c * dirty(sparse) * kWindowDirtyTestCost);
    const double built = _words * kBitClearCost + eager;
    _bitset =
      built +
      (per_doc ? c * kBitTestCost + hits * kHitCost
               : blocks * (kBitsetBlockCost +
                           kBitsetBlockWordCost *
                             std::min(block, _words / std::max(sparse, 1.0))) +
                   c * dirty(sparse) * kBitsetDirtyTestCost);
    _bitset_lead = built + c * kBitsetProbeCost;
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
    return Form(bitsets, _window, _bitset, _probes);
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
  double _lazy = 0;
  double _filled = 0;
  double _window = 0;
  double _bitset = 0;
  double _bitset_lead = 0;
};

template<typename Result, typename Input, typename Term, typename Make>
Result BuildExcludeProbes(std::span<const Term> metas,
                          std::span<const QueryBuilder::ptr> filters,
                          const TermReader* field, const SubReader& segment,
                          uint64_t candidates, Make&& make) {
  const IndexInput* doc = nullptr;
  if (ExcludeTerms(metas, filters, field, doc)) {
    const auto concrete = [&]<typename In> -> Result {
      using Probe = PostingProbe<In>;
      if (metas.size() == 1) {
        const auto& own = FieldOf(metas.front(), field);
        return make.template operator()<Probe>(std::forward_as_tuple(
          CookieOf(metas.front()), *DocOf(own), LayoutOf(own), BoundsOf(own)));
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

template<typename Result, typename Input, typename Term, typename Make>
Result BuildExcludesOf(ExcludeUse use, std::span<const Term> metas,
                       std::span<const QueryBuilder::ptr> filters,
                       const TermReader* field, const SubReader& segment,
                       uint64_t candidates, uint64_t span, Make&& make) {
  SDB_ASSERT(!metas.empty() || !filters.empty());
  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  const auto* const doc = SegmentDoc(segment);
  const ExcludeCosts<Term> costs{metas, filters,    candidates,
                                 span,  docs_count, use};
  switch (costs.Probed(doc != nullptr)) {
    case ExcludeForm::Probes:
      return BuildExcludeProbes<Result, Input>(metas, filters, field, segment,
                                               candidates, make);
    case ExcludeForm::Bitset:
      return BuildExcludeBitset<Result>(
        metas, filters, field, segment, *doc, [&](auto&& set) -> Result {
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

template<typename Result, typename Term, typename Make>
Result BuildWindowExcludes(std::span<const Term> metas,
                           std::span<const QueryBuilder::ptr> filters,
                           const TermReader* field, const SubReader& segment,
                           uint64_t candidates, Make&& make) {
  SDB_ASSERT(!metas.empty() || !filters.empty());
  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  const auto* const doc = SegmentDoc(segment);
  const ExcludeCosts<Term> costs{metas,      filters,    candidates,
                                 candidates, docs_count, ExcludeUse::PerDoc};
  const auto probed = [&]<typename Probe>(auto&& probe) -> Result {
    return make.template operator()<fill::ProbedAndNot<Probe>>(
      std::forward_as_tuple(std::piecewise_construct,
                            std::forward<decltype(probe)>(probe)));
  };
  switch (costs.Windowed(doc != nullptr)) {
    case ExcludeForm::Probes:
      return BuildExcludeProbes<Result, void>(metas, filters, field, segment,
                                              candidates, probed);
    case ExcludeForm::Bitset:
      return BuildExcludeBitset<Result>(
        metas, filters, field, segment, *doc, [&](auto&& set) -> Result {
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

}  // namespace irs::search
