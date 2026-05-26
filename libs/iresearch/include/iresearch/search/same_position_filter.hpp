////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "filter.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/states/term_state.hpp"
#include "iresearch/search/states_cache.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class BySamePosition;

// Options for "by same position" filter
struct BySamePositionOptions {
  using FilterType = BySamePosition;

  using search_term = std::pair<std::string, bstring>;
  using search_terms = std::vector<search_term>;

  search_terms terms;

  bool operator==(const BySamePositionOptions& rhs) const noexcept {
    return terms == rhs.terms;
  }
};

class BySamePosition : public FilterWithOptions<BySamePositionOptions> {
 public:
  // Returns features required for the filter
  static constexpr IndexFeatures kRequiredFeatures =
    IndexFeatures::Freq | IndexFeatures::Pos;

  using TermsStatesT = ManagedVector<TermState>;
  using StatesT = StatesCacheImpl<TermsStatesT>;

  class Buffer final : public ScoredBuffer {
   public:
    Buffer(const PrepareContext& ctx,
           const BySamePositionOptions::search_terms& terms,
           score_t boost = kNoBoost)
      : ScoredBuffer{ctx, boost},
        _terms{&terms},
        _memory{&ctx.memory},
        _term_stats{ctx.scorer, terms.size()},
        _states{ctx.memory, ctx.index.size()},
        _has_scorer{ctx.scorer != nullptr} {
      _field_stats.reserve(terms.size());
      for (size_t i = 0, n = terms.size(); i < n; ++i) {
        _field_stats.emplace_back(ctx.scorer);
      }
    }

    void PrepareSegment(const SubReader& segment) final;
    void Merge(PrepareBuffer&& other) final;
    bool Empty() const noexcept final { return _states.empty(); }
    Query::ptr Compile(const PrepareContext& ctx) && final;

   private:
    const BySamePositionOptions::search_terms* _terms;
    IResourceManager* _memory;
    std::vector<FieldCollectors> _field_stats;
    TermCollectors _term_stats;
    StatesT _states;
    bool _has_scorer;
  };

  std::unique_ptr<PrepareBuffer> CreateBuffer(
    const PrepareContext& ctx) const final {
    if (options().terms.empty()) {
      return std::make_unique<EmptyBuffer>();
    }
    return std::make_unique<Buffer>(ctx, options().terms, Boost());
  }

  Query::ptr prepare(const PrepareContext& ctx) const final {
    auto buf = CreateBuffer(ctx);
    return Filter::PrepareWithBuffer(*buf, ctx);
  }
};

}  // namespace irs
