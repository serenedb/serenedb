////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "search_filter_printer.hpp"

#include <absl/base/internal/endian.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>

#include <duckdb/common/render_tree.hpp>
#include <duckdb/common/tree_renderer/text_tree_renderer.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/automaton_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/column_existence_filter.hpp>
#include <iresearch/search/geo_filter.hpp>
#include <iresearch/search/granular_range_filter.hpp>
#include <iresearch/search/levenshtein_filter.hpp>
#include <iresearch/search/mixed_boolean_filter.hpp>
#include <iresearch/search/nested_filter.hpp>
#include <iresearch/search/ngram_similarity_filter.hpp>
#include <iresearch/search/phrase_filter.hpp>
#include <iresearch/search/phrase_ngram_filter.hpp>
#include <iresearch/search/prefix_filter.hpp>
#include <iresearch/search/proxy_filter.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/search/regexp_filter.hpp>
#include <iresearch/search/regexp_ngram_filter.hpp>
#include <iresearch/search/search_range.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/terms_filter.hpp>
#include <iresearch/search/wildcard_filter.hpp>
#include <iresearch/search/wildcard_ngram_filter.hpp>
#include <sstream>

#include "basics/down_cast.h"
#include "basics/exceptions.h"

namespace irs {
namespace {

using sdb::basics::downCast;

// Prints a binary term byte-by-byte, escaping non-printable chars.
template<typename Term>
std::string TermToString(Term term) {
  std::string s;
  for (auto c : term) {
    if (absl::ascii_isprint(c)) {
      s += c;
    } else {
      absl::StrAppend(&s, "\\x", absl::Hex(static_cast<unsigned char>(c)));
    }
  }
  return s;
}

std::string TermToString(const std::vector<bstring>& terms) {
  return absl::StrCat("( ",
                      absl::StrJoin(terms, " ",
                                    [](std::string* out, const bstring& t) {
                                      absl::StrAppend(out, TermToString(t));
                                    }),
                      " )");
}

template<typename T>
std::string RangeToString(const SearchRange<T>& range) {
  std::string s;
  if (!range.min.empty()) {
    absl::StrAppend(&s, " ",
                    range.min_type == BoundType::Inclusive ? ">=" : ">",
                    TermToString(range.min));
  }
  if (!range.max.empty()) {
    if (!range.min.empty()) {
      absl::StrAppend(&s, ", ");
    } else {
      absl::StrAppend(&s, " ");
    }
    absl::StrAppend(&s, range.max_type == BoundType::Inclusive ? "<=" : "<",
                    TermToString(range.max));
  }
  return s;
}

// Renders one phrase-part option variant. Shared between PHRASE and
// WILDCARD_NGRAM filters which both hold ByPhraseOptions parts.
struct PhrasePartVisitor : util::Noncopyable {
  auto operator()(const ByTermOptions& opts) const {
    absl::StrAppend(out, "Term:", TermToString(opts.term));
  }
  auto operator()(const ByTermsOptions& opts) const {
    absl::StrAppend(out, "Terms:[",
                    absl::StrJoin(opts.terms, "",
                                  [](std::string* o, const auto& tb) {
                                    const auto& [term, boost] = tb;
                                    absl::StrAppend(o, "['", TermToString(term),
                                                    "', ", boost, "],");
                                  }),
                    "]");
  }
  auto operator()(const ByPrefixOptions& opts) const {
    absl::StrAppend(out, "Prefix:", TermToString(opts.term));
  }
  auto operator()(const ByWildcardOptions&) const {
    SDB_THROW(sdb::ERROR_INTERNAL,
              "Wildcard phrase part must be lowered by the optimizer before "
              "printing");
  }
  auto operator()(const ByEditDistanceOptions&) const {
    SDB_THROW(sdb::ERROR_INTERNAL,
              "Levenshtein phrase part must be lowered by the optimizer before "
              "printing");
  }
  auto operator()(const LevenshteinAutomatonOptions& opts) const {
    absl::StrAppend(out, "LevenshteinAutomaton:", TermToString(opts.target));
  }
  auto operator()(const ByRegexpOptions&) const {
    SDB_THROW(sdb::ERROR_INTERNAL,
              "Regexp phrase part must be lowered by the optimizer before "
              "printing");
  }
  auto operator()(const AutomatonOptions& opts) const {
    absl::StrAppend(out, "Automaton:", TermToString(opts.pattern));
  }
  auto operator()(const ByRangeOptions& opts) const {
    absl::StrAppend(out, "Range: ");
    if (opts.range.min_type == BoundType::Unbounded) {
      absl::StrAppend(out, "*");
    } else {
      absl::StrAppend(
        out, opts.range.min_type == BoundType::Inclusive ? "[" : "(",
        std::string(reinterpret_cast<const char*>(opts.range.min.data()),
                    opts.range.min.size()));
    }
    absl::StrAppend(out, "..");
    if (opts.range.max_type == BoundType::Unbounded) {
      absl::StrAppend(out, "*");
    } else {
      absl::StrAppend(
        out,
        std::string(reinterpret_cast<const char*>(opts.range.max.data()),
                    opts.range.max.size()),
        opts.range.max_type == BoundType::Inclusive ? "]" : ")");
    }
  }
  std::string* out;
};

std::string_view GeoFilterTypeName(GeoFilterType type) {
  switch (type) {
    case GeoFilterType::Intersects:
      return "Intersects";
    case GeoFilterType::Contains:
      return "Contains";
    case GeoFilterType::IsContained:
      return "IsContained";
  }
  return "?";
}

std::string_view GeoShapeTypeName(sdb::geo::ShapeContainer::Type type) {
  using T = sdb::geo::ShapeContainer::Type;
  switch (type) {
    case T::Empty:
      return "Empty";
    case T::S2Point:
      return "S2Point";
    case T::S2Polyline:
      return "S2Polyline";
    case T::S2Polygon:
      return "S2Polygon";
    case T::S2Multipoint:
      return "S2Multipoint";
    case T::S2Multipolyline:
      return "S2Multipolyline";
  }
  return "?";
}

// Resolves a binary field id to a display name. IdentityField prints the raw
// id; ToStringDemangled supplies a column-name resolver.
using FieldResolver = std::function<std::string(field_id)>;

std::string PhrasePartsString(const ByPhrase& filter) {
  std::string s;
  for (const auto& part : filter.options()) {
    std::string part_str;
    part.part.visit(PhrasePartVisitor{.out = &part_str});
    absl::StrAppend(&s, part_str, "(", part.offs_max, ", ", part.offs_min, ")",
                    "; ");
  }
  return s;
}

std::string WildcardNgramPartsString(const ByWildcardNgram& filter) {
  std::string s;
  for (const auto& phrase : filter.options().parts) {
    absl::StrAppend(&s, "<");
    for (const auto& part : phrase) {
      std::string part_str;
      part.part.visit(PhrasePartVisitor{.out = &part_str});
      absl::StrAppend(&s, part_str, "(", part.offs_max, ", ", part.offs_min,
                      ")", "; ");
    }
    absl::StrAppend(&s, ">; ");
  }
  return s;
}

std::string PhraseNgramShinglesString(const ByPhraseNgram& filter) {
  std::string s;
  for (const auto& shingle : filter.options().shingles) {
    absl::StrAppend(&s, TermToString(shingle), "; ");
  }
  return s;
}

std::string RegexpNgramTree(const ByRegexpNgramOptions::Node& node) {
  using Kind = ByRegexpNgramOptions::Node::Kind;
  switch (node.kind) {
    case Kind::kAll:
      return "ALL";
    case Kind::kNone:
      return "NONE";
    case Kind::kTerms: {
      std::string terms;
      for (const auto& term : node.terms) {
        absl::StrAppend(&terms, TermToString(term), " ");
      }
      return absl::StrCat("{", terms, "}");
    }
    case Kind::kAnd:
    case Kind::kOr: {
      std::string subs;
      for (const auto& sub : node.subs) {
        absl::StrAppend(&subs, RegexpNgramTree(sub), " ");
      }
      return absl::StrCat(node.kind == Kind::kAnd ? "AND[" : "OR[", subs, "]");
    }
  }
  return "?";
}

std::string TermsListString(const ByTerms& filter) {
  return absl::StrJoin(
    filter.options().terms, "", [](std::string* o, const auto& term_boost) {
      const auto& [term, boost] = term_boost;
      absl::StrAppend(o, "['", TermToString(term), "', ", boost, "],");
    });
}

std::string NgramListString(const ByNGramSimilarity& filter) {
  return absl::StrJoin(filter.options().ngrams, "",
                       [](std::string* o, const auto& ngram) {
                         absl::StrAppend(o, TermToString(ngram));
                       });
}

std::string GeoDistanceRangeString(const GeoDistanceFilter& filter) {
  const auto& range = filter.options().range;
  std::string s;
  if (range.min_type == BoundType::Unbounded) {
    absl::StrAppend(&s, "*");
  } else {
    absl::StrAppend(&s, range.min_type == BoundType::Inclusive ? ">=" : ">",
                    range.min);
  }
  if (range.max_type != BoundType::Unbounded) {
    absl::StrAppend(
      &s, ", ", range.max_type == BoundType::Inclusive ? "<=" : "<", range.max);
  }
  return s;
}

struct FilterNode {
  std::string label;
  std::string detail;
  std::vector<FilterNode> children;
};

FilterNode BuildNode(const Filter& filter, const FieldResolver& field) {
  const auto& type = filter.type();
  if (type == Type<All>::id()) {
    return {.label = "ALL",
            .detail = absl::StrCat(downCast<const All>(filter).Boost())};
  }
  if (type == Type<And>::id()) {
    FilterNode node{.label = "AND"};
    for (const auto& sub : downCast<const And>(filter)) {
      node.children.push_back(BuildNode(*sub, field));
    }
    return node;
  }
  if (type == Type<Or>::id()) {
    const auto& f = downCast<const Or>(filter);
    FilterNode node{.label = f.min_match_count() != 1
                               ? absl::StrCat("OR(", f.min_match_count(), ")")
                               : "OR"};
    for (const auto& sub : f) {
      node.children.push_back(BuildNode(*sub, field));
    }
    return node;
  }
  if (type == Type<Not>::id()) {
    SDB_THROW(sdb::ERROR_INTERNAL,
              "Not filter must be lowered to Exclusion by the optimizer before "
              "printing");
  }
  if (type == Type<Exclusion>::id()) {
    const auto& f = downCast<const Exclusion>(filter);
    const auto& incl = f.GetInclude();
    const auto excludes = f.GetExcludes();
    // A pure negation (implicit all-docs include) reads better as NOT[X].
    if (incl == nullptr) {
      FilterNode node{.label = "NOT"};
      for (const auto& excl : excludes) {
        node.children.push_back(BuildNode(*excl, field));
      }
      return node;
    }
    FilterNode node{.label = "EXCLUSION"};
    node.children.push_back(BuildNode(*incl, field));
    for (const auto& excl : excludes) {
      node.children.push_back(BuildNode(*excl, field));
    }
    return node;
  }
  if (type == Type<MixedBooleanFilter>::id()) {
    const auto& f = downCast<const MixedBooleanFilter>(filter);
    FilterNode node{.label = "MIXED"};
    node.children.push_back(BuildNode(f.GetRequired(), field));
    node.children.push_back(BuildNode(f.GetOptional(), field));
    return node;
  }
  if (type == Type<ByTerm>::id()) {
    const auto& f = downCast<const ByTerm>(filter);
    return {.label = "Term",
            .detail = absl::StrCat(field(f.field_id()), " = ",
                                   TermToString(f.options().term))};
  }
  if (type == Type<ByTerms>::id()) {
    const auto& f = downCast<const ByTerms>(filter);
    return {
      .label = "TERMS",
      .detail = absl::StrCat(field(f.field_id()), " {", TermsListString(f),
                             "} min_match=", f.options().min_match)};
  }
  if (type == Type<ByRange>::id()) {
    const auto& f = downCast<const ByRange>(filter);
    return {.label = "Range",
            .detail = absl::StrCat(field(f.field_id()),
                                   RangeToString(f.options().range))};
  }
  if (type == Type<ByGranularRange>::id()) {
    const auto& f = downCast<const ByGranularRange>(filter);
    return {.label = "GranularRange",
            .detail = absl::StrCat(field(f.field_id()),
                                   RangeToString(f.options().range))};
  }
  if (type == Type<ByNGramSimilarity>::id()) {
    const auto& f = downCast<const ByNGramSimilarity>(filter);
    return {.label = "NGRAM_SIMILARITY",
            .detail =
              absl::StrCat(field(f.field_id()), " ngrams=", NgramListString(f),
                           " threshold=", f.options().threshold)};
  }
  if (type == Type<ByEditDistance>::id()) {
    SDB_THROW(sdb::ERROR_INTERNAL,
              "ByEditDistance filter must be lowered to "
              "LevenshteinAutomatonFilter by the optimizer before printing");
  }
  if (type == Type<LevenshteinAutomatonFilter>::id()) {
    const auto& f = downCast<const LevenshteinAutomatonFilter>(filter);
    const auto& o = f.options();
    return {
      .label = "LEVENSHTEIN_AUTOMATON",
      .detail = absl::StrCat(field(f.field_id()), " '", TermToString(o.target),
                             "' max_terms=", o.max_terms)};
  }
  if (type == Type<ByPrefix>::id()) {
    const auto& f = downCast<const ByPrefix>(filter);
    return {.label = "STARTS_WITH",
            .detail = absl::StrCat(field(f.field_id()), " '",
                                   TermToString(f.options().term),
                                   "' limit=", f.options().scored_terms_limit)};
  }
  if (type == Type<ByNestedFilter>::id()) {
    const auto& f = downCast<const ByNestedFilter>(filter);
    auto& [parent, child, match, _] = f.options();
    std::string match_str;
    if (auto* range = std::get_if<Match>(&match); range != nullptr) {
      match_str = absl::StrCat(range->min, ", ", range->max);
    } else if (std::get_if<DocIteratorProvider>(&match) != nullptr) {
      match_str = "<Predicate>";
    }
    FilterNode node{.label = absl::StrCat("NESTED match=[", match_str, "]")};
    node.children.push_back(BuildNode(*child, field));
    return node;
  }
  if (type == Type<ByColumnExistence>::id()) {
    return {.label = "EXISTS",
            .detail = field(downCast<const ByColumnExistence>(filter).id())};
  }
  if (type == Type<ByWildcard>::id()) {
    SDB_THROW(sdb::ERROR_INTERNAL,
              "ByWildcard filter must be lowered to "
              "ByTerm/ByPrefix/AutomatonFilter by the optimizer before "
              "printing");
  }
  if (type == Type<ByRegexp>::id()) {
    SDB_THROW(sdb::ERROR_INTERNAL,
              "ByRegexp filter must be lowered to "
              "ByTerm/ByPrefix/AutomatonFilter by the optimizer before "
              "printing");
  }
  if (type == Type<AutomatonFilter>::id()) {
    const auto& f = downCast<const AutomatonFilter>(filter);
    return {.label = "AUTOMATON",
            .detail = absl::StrCat(field(f.field_id()), " ",
                                   TermToString(f.options().pattern))};
  }
  if (type == Type<ByWildcardNgram>::id()) {
    const auto& f = downCast<const ByWildcardNgram>(filter);
    return {.label = "WILDCARD_NGRAM",
            .detail = absl::StrCat(
              field(f.field_id()), " '", TermToString(f.options().token),
              "' has_pos=", f.options().has_pos, " parts=[",
              WildcardNgramPartsString(f), "]")};
  }
  if (type == Type<ByPhraseNgram>::id()) {
    const auto& f = downCast<const ByPhraseNgram>(filter);
    return {.label = "PHRASE_NGRAM",
            .detail = absl::StrCat(field(f.field_id()),
                                   " exact=", f.options().exact, " shingles=[",
                                   PhraseNgramShinglesString(f), "]")};
  }
  if (type == Type<ByRegexpNgram>::id()) {
    const auto& f = downCast<const ByRegexpNgram>(filter);
    return {
      .label = "REGEXP_NGRAM",
      .detail = absl::StrCat(
        field(f.field_id()), " /",
        f.options().matcher ? f.options().matcher->pattern() : std::string{},
        "/ ngrams=", RegexpNgramTree(f.options().root))};
  }
  if (type == Type<Empty>::id()) {
    return {.label = "EMPTY"};
  }
  if (type == Type<ByPhrase>::id()) {
    const auto& f = downCast<const ByPhrase>(filter);
    return {.label = "PHRASE",
            .detail = absl::StrCat(field(f.field_id()), " = <",
                                   PhrasePartsString(f), ">")};
  }
  if (type == Type<GeoFilter>::id()) {
    const auto& f = downCast<const GeoFilter>(filter);
    return {.label = "GEO",
            .detail = absl::StrCat(
              field(f.field_id()), " op=", GeoFilterTypeName(f.options().type),
              " shape=", GeoShapeTypeName(f.options().shape.type()))};
  }
  if (type == Type<GeoDistanceFilter>::id()) {
    const auto& f = downCast<const GeoDistanceFilter>(filter);
    return {.label = "GEO_DISTANCE",
            .detail = absl::StrCat(field(f.field_id()), " ",
                                   GeoDistanceRangeString(f))};
  }
  if (type == Type<ProxyFilter>::id()) {
    return BuildNode(downCast<const ProxyFilter>(filter).inner(), field);
  }
  return {.label = "Unknown", .detail = std::string{type().name()}};
}

void ComputeSize(const FilterNode& node, duckdb::idx_t& width,
                 duckdb::idx_t& height) {
  if (node.children.empty()) {
    width = 1;
    height = 1;
    return;
  }
  width = 0;
  height = 0;
  for (const auto& child : node.children) {
    duckdb::idx_t cw = 0;
    duckdb::idx_t ch = 0;
    ComputeSize(child, cw, ch);
    width += cw;
    height = std::max(height, ch);
  }
  ++height;
}

duckdb::unique_ptr<duckdb::RenderTreeNode> MakeRenderNode(
  const FilterNode& node) {
  duckdb::InsertionOrderPreservingMap<std::string> extra;
  if (!node.detail.empty()) {
    // A "__"-prefixed key renders the value with no "Key:" prefix.
    extra.insert("__detail__", node.detail);
  }
  return duckdb::make_uniq<duckdb::RenderTreeNode>(node.label,
                                                   std::move(extra));
}

duckdb::idx_t FillTree(duckdb::RenderTree& tree, const FilterNode& node,
                       duckdb::idx_t x, duckdb::idx_t y) {
  auto render_node = MakeRenderNode(node);
  if (node.children.empty()) {
    tree.SetNode(x, y, std::move(render_node));
    return 1;
  }
  duckdb::idx_t width = 0;
  for (const auto& child : node.children) {
    const auto child_x = x + width;
    const auto child_y = y + 1;
    render_node->AddChildPosition(child_x, child_y);
    width += FillTree(tree, child, child_x, child_y);
  }
  tree.SetNode(x, y, std::move(render_node));
  return width;
}

size_t DisplayWidth(std::string_view s) {
  size_t n = 0;
  for (unsigned char c : s) {
    if ((c & 0xC0) != 0x80) {
      ++n;
    }
  }
  return n;
}

std::string RenderBoxTree(const FilterNode& root) {
  duckdb::idx_t width = 0;
  duckdb::idx_t height = 0;
  ComputeSize(root, width, height);

  duckdb::RenderTree tree(width, height);
  FillTree(tree, root, 0, 0);

  duckdb::TextTreeRendererConfig config;
  config.node_render_width = 21;
  duckdb::TextTreeRenderer renderer{config};

  std::ostringstream ss;
  renderer.ToStream(tree, ss);
  std::vector<std::string> lines = absl::StrSplit(ss.str(), '\n');
  while (!lines.empty() && lines.back().empty()) {
    lines.pop_back();
  }

  size_t inner = 0;
  for (const auto& line : lines) {
    inner = std::max(inner, DisplayWidth(line));
  }
  std::string bound;
  bound.reserve(inner * sizeof("─"));
  for (size_t i = 0; i < inner; ++i) {
    absl::StrAppend(&bound, "─");
  }
  std::string out = absl::StrCat("┌", bound, "┐");
  for (const auto& line : lines) {
    absl::StrAppend(&out, "\n│", line,
                    std::string(inner - DisplayWidth(line), ' '), "│");
  }
  absl::StrAppend(&out, "\n└", bound, "┘");
  return out;
}

std::string IdentityField(field_id id) { return absl::StrCat(id); }

}  // namespace

std::string ToString(const Filter& f) {
  return RenderBoxTree(BuildNode(f, IdentityField));
}

std::string ToStringDemangled(
  const Filter& f,
  const std::function<std::string(sdb::catalog::Column::Id)>& col_name) {
  return RenderBoxTree(BuildNode(f, [&](field_id id) -> std::string {
    return std::string{col_name(sdb::catalog::Column::Id{id})};
  }));
}

}  // namespace irs
