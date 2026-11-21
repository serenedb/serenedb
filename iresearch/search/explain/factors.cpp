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

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/scorers/lm_similarity.hpp"
#include "iresearch/search/scorers/score_inputs.hpp"

namespace irs::explain {

const uint32_t* FreqOf(const ScoreContext& ctx) noexcept {
  const auto* freq = irs::get<FreqBlockAttr>(ctx.doc_attrs);
  return freq ? freq->value : nullptr;
}

void AddBoost(Node& out, score_t boost) {
  if (boost != kNoBoost) {
    AddDetail(out, "boost", boost);
  }
}

void AddScale(Node& out, const ScoreContext& ctx, std::string_view product,
              std::string_view scaled) {
  if (const auto* scale = ScaleOf(ctx)) {
    AddDetail(out, "scale", scale[0]);
    out.expression = scaled;
  } else {
    out.expression = product;
  }
}

score_t AddFreq(Node& out, const ScoreContext& ctx) {
  const auto* freq = FreqOf(ctx);
  SDB_ASSERT(freq);
  const auto* scale = ScaleOf(ctx);
  if (!scale) {
    AddDetail(out, "freq", uint64_t{*freq});
    return TermCountToScore(*freq);
  }
  const auto scaled = TermCountToScore(*freq) * scale[0];
  auto& node = AddDetail(out, "freq", scaled);
  node.expression = "count * scale";
  AddDetail(node, "count", uint64_t{*freq});
  AddDetail(node, "scale", scale[0]);
  return scaled;
}

uint32_t AddLength(Node& out, const ScoreContext& ctx) {
  const auto* norms = FieldNormsOf(ctx);
  const auto length = norms ? norms[0] : kNorms[0];
  auto& dl = AddDetail(out, "dl", uint64_t{length});
  if (!norms) {
    dl.note = "the column records no lengths, so every row counts as 1";
  }
  return length;
}

void AddAverageLength(Node& out, score_t fallback, const SlotStats* stats) {
  if (!stats || stats->docs_with_field == 0 || stats->total_term_freq == 0) {
    AddDetail(out, "avgdl", fallback);
    return;
  }
  auto& avg = AddDetail(out, "avgdl",
                        static_cast<score_t>(stats->total_term_freq) /
                          static_cast<score_t>(stats->docs_with_field));
  avg.expression = "L / N";
  AddDetail(avg, "L", stats->total_term_freq);
  AddDetail(avg, "N", stats->docs_with_field);
}

void AddIdf(Node& out, score_t idf, const SlotStats* stats,
            std::string_view expression, IdfOf per_term) {
  auto& node = AddDetail(out, "idf", idf);
  node.expression = expression;
  if (!stats || stats->terms.empty()) {
    return;
  }
  const auto add_counts = [&](Node& to, const TermStats& term) {
    auto& df = AddDetail(to, "df", term.docs_with_term);
    if (stats->blended) {
      df.note = "the most rows any one of the matched terms occurs in";
    }
    AddDetail(to, "N", stats->docs_with_field);
  };
  if (stats->terms.size() == 1) {
    add_counts(node, stats->terms.front());
    return;
  }
  node.expression = "sum";
  for (const auto& term : stats->terms) {
    auto& one =
      AddPart(node, {.field = stats->field, .terms = std::span{&term.term, 1}});
    one.value = per_term(stats->docs_with_field, term.docs_with_term);
    one.expression = expression;
    add_counts(one, term);
  }
}

void AddCollectionShare(Node& out, score_t value, const SlotStats* stats) {
  const bool blended = stats && stats->blended;
  auto& node = AddDetail(out, "p", value);
  if (blended) {
    node.note = "the matched terms' share of all text in the column";
  }
  if (!stats || stats->terms.size() != 1) {
    return;
  }
  node.expression = "(ttf + 1) / (L + 1)";
  auto& ttf = AddDetail(node, "ttf", stats->terms.front().total_term_freq);
  if (blended) {
    ttf.note = "occurrences of all the matched terms over all rows";
  }
  AddDetail(node, "L", stats->total_term_freq);
}

void AddShare(Node& out, score_t value, uint64_t ttf, uint64_t total) {
  auto& node = AddDetail(out, "p", value);
  node.expression = "(ttf + 1) / (L + 1)";
  AddDetail(node, "ttf", ttf);
  AddDetail(node, "L", total);
}

void ExplainParts(const SlotStats* stats, Node& out, ExplainPart describe) {
  out.expression = "sum";
  if (!stats) {
    return;
  }
  for (const auto& term : stats->terms) {
    auto& part =
      AddPart(out, {.field = stats->field, .terms = std::span{&term.term, 1}});
    describe(part, term,
             CollectionShareOf(term.total_term_freq, stats->total_term_freq));
  }
}

void ExplainLanguageModel(const ScoreContext& ctx,
                          std::span<const score_t> shares,
                          const SlotStats* stats, Node parameter,
                          std::string_view expression, TermFormula score,
                          Node& out) {
  if (!FreqOf(ctx) || shares.empty()) {
    ExplainNoFreq(ctx, false, out);
    return;
  }
  if (shares.size() == 1) {
    AddBoost(out, ctx.boost);
    out.details.push_back(std::move(parameter));
    AddCollectionShare(out, shares.front(), stats);
    AddFreq(out, ctx);
    AddLength(out, ctx);
    out.expression = expression;
    return;
  }
  ExplainParts(
    stats, out, [&](Node& part, const TermStats& term, score_t share) {
      AddBoost(part, ctx.boost);
      part.details.push_back(parameter);
      AddShare(part, share, term.total_term_freq, stats->total_term_freq);
      const auto tf = AddFreq(part, ctx);
      const auto dl = TermCountToScore(AddLength(part, ctx));
      part.value = score(tf, dl, share);
      part.expression = expression;
    });
}

void ExplainNoFreq(const ScoreContext& ctx, bool boost_as_score, Node& out) {
  out.note =
    boost_as_score && ctx.boost != 0.f
      ? "the match has no term frequency, so the clause scores its boost"
      : "the match has no term frequency, so the clause scores 0";
}

}  // namespace irs::explain
