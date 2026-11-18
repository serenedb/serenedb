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

#include "iresearch/search/explain/factors.hpp"

#include <absl/strings/str_cat.h>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/search/scorers/score_inputs.hpp"

namespace irs::explain {

std::optional<uint32_t> FreqOf(const ScoreContext& ctx) noexcept {
  const auto* freq = irs::get<FreqBlockAttr>(ctx.doc_attrs);
  if (!freq) {
    return std::nullopt;
  }
  return freq->value[0];
}

void AddBoost(Node& out, score_t boost) {
  if (boost != kNoBoost) {
    AddDetail(out, "boost", boost, "boost, the query's weight for this clause");
  }
}

void AddScale(Node& out, const ScoreContext& ctx) {
  if (const auto* scale = ScaleOf(ctx)) {
    AddDetail(out, "scale", scale[0],
              "scale, the weight of this row's match within the clause");
  }
}

score_t AddFreq(Node& out, const ScoreContext& ctx) {
  const auto freq = FreqOf(ctx);
  SDB_ASSERT(freq);
  AddDetail(out, "freq", uint64_t{*freq},
            "freq, occurrences of the term in the row");
  const auto* scale = ScaleOf(ctx);
  if (!scale) {
    return TermCountToScore(*freq);
  }
  AddScale(out, ctx);
  return TermCountToScore(*freq) * scale[0];
}

uint32_t AddLength(Node& out, const ScoreContext& ctx) {
  const auto length = NormsOf(ctx)[0];
  AddDetail(out, "dl", uint64_t{length}, "dl, length of the column in the row");
  return length;
}

void AddAverageLength(Node& out, score_t fallback, const SlotStats* stats) {
  if (!stats || stats->docs_with_field == 0 || stats->total_term_freq == 0) {
    AddDetail(out, "avgdl", fallback, "avgdl, average length of the column");
    return;
  }
  auto& avg =
    AddDetail(out, "avgdl",
              static_cast<score_t>(stats->total_term_freq) /
                static_cast<score_t>(stats->docs_with_field),
              "avgdl, average length of the column, computed as L / N from:");
  AddDetail(avg, "L", stats->total_term_freq,
            "L, total length of the column over all rows");
  AddDetail(avg, "N", stats->docs_with_field,
            "N, number of rows with the column");
}

void AddIdf(Node& out, score_t idf, const SlotStats* stats,
            std::string_view formula, IdfOf per_term) {
  auto& node =
    AddDetail(out, "idf", idf, absl::StrCat("idf, computed as ", formula));
  if (!stats || stats->terms.empty()) {
    return;
  }
  const auto add_counts = [&](Node& to, const TermStats& term) {
    AddDetail(to, "n", term.docs_with_term,
              "n, number of rows containing the term");
    AddDetail(to, "N", stats->docs_with_field,
              "N, number of rows with the column");
  };
  if (stats->terms.size() == 1) {
    absl::StrAppend(&node.description, " from:");
    add_counts(node, stats->terms.front());
    return;
  }
  node.description =
    absl::StrCat("idf, sum of the terms' idf, each computed as ", formula, ":");
  for (const auto& term : stats->terms) {
    auto& one =
      AddDetail(node, "idf",
                per_term(stats->docs_with_field, term.docs_with_term), "from:");
    one.subject = {.field = stats->field, .terms = {term.term}};
    add_counts(one, term);
  }
}

void AddCollectionShare(Node& out, std::string_view key, std::string_view what,
                        score_t value, const SlotStats* stats) {
  auto& node = AddDetail(out, key, value, std::string{what});
  if (!stats || stats->terms.size() != 1) {
    return;
  }
  absl::StrAppend(&node.description, ", computed as (ttf + 1) / (L + 1) from:");
  AddDetail(node, "ttf", stats->terms.front().total_term_freq,
            "ttf, occurrences of the term over all rows");
  AddDetail(node, "L", stats->total_term_freq,
            "L, total length of the column over all rows");
}

void ExplainLanguageModel(const ScoreContext& ctx, score_t collection_share,
                          const SlotStats* stats, Node parameter,
                          std::string_view formula, Node& out) {
  if (!FreqOf(ctx) || collection_share <= 0.f) {
    out.description =
      "the column records no term frequency, so the clause scores 0";
    return;
  }
  AddBoost(out, ctx.boost);
  out.details.push_back(std::move(parameter));
  AddCollectionShare(out, "p", "p, the term's share of all text in the column",
                     collection_share, stats);
  AddFreq(out, ctx);
  AddLength(out, ctx);
  out.description = absl::StrCat("computed as ", formula, " from:");
}

void ExplainNoFreq(const ScoreContext& ctx, bool boost_as_score, Node& out) {
  out.description =
    boost_as_score && ctx.boost != 0.f
      ? "the column records no term frequency, so the clause scores its boost"
      : "the column records no term frequency, so the clause scores 0";
}

}  // namespace irs::explain
