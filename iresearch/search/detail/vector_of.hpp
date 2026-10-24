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

#include <limits>
#include <memory>
#include <tuple>
#include <utility>

#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/vector_slots.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/queries/vector_similarity_query.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::detail {

struct ClusterFeed {
  const VectorState* state;
  IndexInput* payload;
  bool has_centroids;

  using Args =
    std::tuple<const PostingMeta&, const IndexInput&,
               std::unique_ptr<QuantizerReader>, VectorBlockReader, uint32_t>;

  Args operator()(size_t c) const {
    auto quantizer = MakeQuantizerReader(state->codebook);
    SDB_ASSERT(quantizer);
    const float* centroid =
      has_centroids ? state->cluster_centroids.data() + c * state->d : nullptr;
    quantizer->StartCluster(centroid);
    VectorBlockReader pay{payload->Dup(), quantizer->BlockSetting()};
    pay.Reset(state->pay_starts[c]);
    return Args{state->cookies[c], *detail::DocOf(*state->reader),
                std::move(quantizer), std::move(pay), state->pay_lanes[c]};
  }
};

template<typename Query>
RawRecipe RecipeOf(const Query& query) {
  const auto& state = query.State();
  return {.column = state.vector_column,
          .reader = state.col_reader,
          .query = query.Query(),
          .d = state.vector_column != nullptr
                 ? static_cast<uint32_t>(state.vector_column->ArraySize())
                 : state.d,
          .metric = query.Metric()};
}

template<typename Gate, typename Query, typename Emit>
auto ResolveClusters(const Query& query, Emit&& emit) {
  const auto& state = query.State();
  SDB_ASSERT(state.reader != nullptr);
  SDB_ASSERT(state.payload != nullptr);
  SDB_ASSERT(detail::DocOf(*state.reader) != nullptr);
  SDB_ASSERT(!state.cookies.empty());

  const ClusterFeed feed{.state = &state,
                         .payload = state.payload.get(),
                         .has_centroids = state.cluster_centroids.size() ==
                                          state.cookies.size() * state.d};
  const auto count = state.cookies.size();

  return detail::ResolveInput(
    *detail::DocOf(*state.reader), [&]<typename InputType>() {
      return emit.template operator()<VectorCluster<InputType, Gate>>(count,
                                                                      feed);
    });
}

template<typename Gate, bool Rescore, typename Query, typename Emit>
auto ResolveVector(const Query& query, score_t edge, probe::Node::ptr inner,
                   Emit&& emit) {
  const auto recipe = RecipeOf(query);
  return ResolveClusters<Gate>(
    query, [&]<typename Cluster>(size_t count, const ClusterFeed& feed) {
      return ResolveBool(inner != nullptr, [&]<bool HasInner>() {
        using Slots = VectorSlots<Cluster, Gate, HasInner, Rescore>;
        return emit.template operator()<Slots>(count, feed, edge,
                                               std::move(inner), recipe);
      });
    });
}

template<typename Query>
probe::Node::ptr InnerProbe(const Query& query) {
  const auto* inner = query.Inner();
  if (inner == nullptr) {
    return {};
  }
  return inner->PlanProbe({}, query.State().estimation);
}

template<typename Gate, typename Query, typename F>
auto ResolveRescore(const Query& query, F&& f) {
  if constexpr (Gate::kAll) {
    return f.template operator()<false>();
  } else {
    return ResolveBool(query.Rescored(), f);
  }
}

template<typename Family, typename Gate, typename Query>
typename Family::Result MakeVectorDocs(const Query& query,
                                       const typename Family::DocsContext& ctx,
                                       score_t edge) {
  using Result = typename Family::Result;
  auto inner = InnerProbe(query);
  if (query.Inner() != nullptr && !inner) {
    return {};
  }
  return ResolveRescore<Gate>(query, [&]<bool Rescore>() {
    return ResolveVector<Gate, Rescore>(
      query, edge, std::move(inner),
      [&]<typename Slots>(auto&&... args) -> Result {
        return Family::template Docs<
          typename Family::template TwoPhaseDocs<Slots>>(
          ctx, std::forward<decltype(args)>(args)...);
      });
  });
}

template<typename Family, typename Gate, typename Query>
typename Family::Result MakeVectorScored(
  const Query& query, const typename Family::ScoredContext& ctx, score_t edge) {
  using Result = typename Family::Result;
  auto inner = InnerProbe(query);
  if (query.Inner() != nullptr && !inner) {
    return {};
  }
  const auto& scoring = Family::Scoring(ctx);
  const auto record = query.Stats(scoring);
  const ScoreArgs score{.scorer = record.scorer,
                        .stats = record.stats,
                        .fetcher = scoring.fetcher,
                        .boost = query.Boost()};
  return ResolveRescore<Gate>(query, [&]<bool Rescore>() {
    return ResolveVector<Gate, Rescore>(
      query, edge, std::move(inner),
      [&]<typename Slots>(auto&&... args) -> Result {
        return Family::template Scored<
          typename Family::template TwoPhaseScored<Slots>>(
          ctx, query.Segment(), *query.State().reader, score,
          std::forward<decltype(args)>(args)...);
      });
  });
}

inline score_t Unbounded() noexcept {
  return std::numeric_limits<score_t>::lowest();
}

template<typename Family, typename Ctx>
typename Family::Result MakeKnnVector(const KnnVectorQuery& query,
                                      const Ctx& ctx) {
  return MakeVectorScored<Family, AcceptAll>(query, ctx, Unbounded());
}

}  // namespace irs::detail
