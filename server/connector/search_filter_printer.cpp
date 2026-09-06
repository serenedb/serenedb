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

#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/automaton_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/constant_score.hpp>
#include <iresearch/search/geo_filter.hpp>
#include <iresearch/search/granular_range_filter.hpp>
#include <iresearch/search/levenshtein_filter.hpp>
#include <iresearch/search/nested_filter.hpp>
#include <iresearch/search/ngram_similarity_filter.hpp>
#include <iresearch/search/phrase_filter.hpp>
#include <iresearch/search/prefix_filter.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/search/regexp_filter.hpp>
#include <iresearch/search/search_range.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/term_set.hpp>
#include <iresearch/search/vector_radius_filter.hpp>
#include <iresearch/search/wildcard_filter.hpp>
#include <iresearch/search/wildcard_ngram_filter.hpp>
#include <iresearch/utils/numeric_utils.hpp>

#include "basics/down_cast.h"
#include "pg/sql_exception_macro.h"

namespace irs {
namespace {

using duckdb::ExplainNode;
using sdb::basics::downCast;
using sdb::catalog::term_dict::Kind;

const Scorer* Explicit(const Scorer* scorer) noexcept {
  return scorer == &DefaultConstScore() ? nullptr : scorer;
}

void AddMergeAttribute(ExplainNode& node, ScoreMergeType merge) {
  switch (merge) {
    case ScoreMergeType::Max:
      node.attributes["Merge"] = "max";
      break;
    case ScoreMergeType::Noop:
      node.attributes["Merge"] = "noop";
      break;
    case ScoreMergeType::Sum:
      break;
  }
}

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

// Decodes a numeric trie term back to the number it encodes. The leading
// byte is `shift + TYPE_MAGIC` with disjoint magic ranges per width, so the
// value type is recovered from the term itself (which also disambiguates the
// shared JSON-leaf numeric field).
std::string DecodeNumericTerm(bytes_view term) {
  namespace nu = irs::numeric_utils;
  if (term.empty()) {
    return "";
  }
  const auto marker = term[0];
  if (marker < 0x20) {
    return absl::StrCat(nu::numeric_traits<int32_t>::decode(term.data()));
  }
  if (marker < 0x40) {
    return absl::StrCat(nu::numeric_traits<float>::decode(term.data()));
  }
  if (marker >= 0x60 && marker < 0xA0) {
    return absl::StrCat(nu::numeric_traits<int64_t>::decode(term.data()));
  }
  if (marker >= 0xA0 && marker < 0xE0) {
    return absl::StrCat(nu::numeric_traits<double>::decode(term.data()));
  }
  return TermToString(term);
}

std::string TermValue(bytes_view term, Kind kind) {
  if (kind == Kind::Bool) {
    return !term.empty() && term[0] ? "true" : "false";
  }
  if (sdb::catalog::term_dict::IsNumeric(kind)) {
    return DecodeNumericTerm(term);
  }
  return TermToString(term);
}

std::string TermValue(const bstring& term, Kind kind) {
  return TermValue(bytes_view{term}, kind);
}

// A granular-range bound is a vector of granularity levels; the first entry is
// the exact (shift 0) value.
std::string TermValue(const std::vector<bstring>& terms, Kind kind) {
  if (terms.empty()) {
    return "";
  }
  return TermValue(bytes_view{terms.front()}, kind);
}

template<typename T>
std::string RangeValue(const SearchRange<T>& range, Kind kind) {
  std::string s;
  if (!range.min.empty()) {
    absl::StrAppend(&s, range.min_type == BoundType::Inclusive ? ">=" : ">",
                    TermValue(range.min, kind));
  }
  if (!range.max.empty()) {
    if (!s.empty()) {
      absl::StrAppend(&s, ", ");
    }
    absl::StrAppend(&s, range.max_type == BoundType::Inclusive ? "<=" : "<",
                    TermValue(range.max, kind));
  }
  return s;
}

// Renders one phrase-part option variant. Shared between Phrase and
// WildcardNGram filters which both hold ByPhraseOptions parts.
struct PhrasePartVisitor : util::Noncopyable {
  auto operator()(const ByTermOptions& opts) const {
    absl::StrAppend(out, "Term:", TermToString(opts.term));
  }
  auto operator()(const TermSetOptions& opts) const {
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
    THROW_SQL_ERROR(
      ERR_MSG("Wildcard phrase part must be lowered by the optimizer before "
              "printing"));
  }
  auto operator()(const ByEditDistanceOptions&) const {
    THROW_SQL_ERROR(
      ERR_MSG("Levenshtein phrase part must be lowered by the optimizer before "
              "printing"));
  }
  auto operator()(const LevenshteinAutomatonOptions& opts) const {
    absl::StrAppend(out, "LevenshteinAutomaton:", TermToString(opts.target));
  }
  auto operator()(const ByRegexpOptions&) const {
    THROW_SQL_ERROR(
      ERR_MSG("Regexp phrase part must be lowered by the optimizer before "
              "printing"));
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

std::string_view VectorMetricName(VectorMetric metric) {
  switch (metric) {
    case VectorMetric::L2Sqr:
      return "l2";
    case VectorMetric::InnerProduct:
      return "ip";
    case VectorMetric::Cosine:
      return "cosine";
    case VectorMetric::L1:
      return "l1";
  }
  return "?";
}

struct FilterPrinter {
  const FieldNameResolver& name_of;
  const FieldKindResolver& kind_of;

  std::string FieldName(field_id fid) const {
    return name_of(sdb::catalog::ColumnId{fid});
  }
  Kind FieldKind(field_id fid) const {
    return kind_of(sdb::catalog::ColumnId{fid});
  }

  std::string PhraseParts(const ByPhrase& filter) const {
    std::string s;
    for (const auto& part : filter.options()) {
      std::string part_str;
      part.part.visit(PhrasePartVisitor{.out = &part_str});
      absl::StrAppend(&s, part_str, "(", part.offs_max, ", ", part.offs_min,
                      ")", "; ");
    }
    return s;
  }

  std::string WildcardNGramParts(const ByWildcardNGram& filter) const {
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

  std::string NGramList(const ByNGramSimilarity& filter) const {
    return absl::StrJoin(filter.options().ngrams, "",
                         [](std::string* o, const auto& ngram) {
                           absl::StrAppend(o, TermToString(ngram));
                         });
  }

  std::string GeoDistanceRange(const GeoDistanceFilter& filter) const {
    const auto& range = filter.options().range;
    std::string s;
    if (range.min_type == BoundType::Unbounded) {
      absl::StrAppend(&s, "*");
    } else {
      absl::StrAppend(&s, range.min_type == BoundType::Inclusive ? ">=" : ">",
                      range.min);
    }
    if (range.max_type != BoundType::Unbounded) {
      absl::StrAppend(&s, ", ",
                      range.max_type == BoundType::Inclusive ? "<=" : "<",
                      range.max);
    }
    return s;
  }

  ExplainNode Build(const Filter& filter) const {
    auto node = BuildImpl(filter);
    if (const auto* scorer = Explicit(filter.GetScorer())) {
      node.attributes["Score"] = scorer->ToString();
    }
    return node;
  }

  ExplainNode BuildImpl(const Filter& filter) const {
    auto node = BuildNode(filter);
    if (const auto boost = filter.GetBoost(); boost != kNoBoost) {
      node.attributes["Boost"] = absl::StrCat(boost);
    }
    return node;
  }

  // Terms are leaves rather than filters, so
  // they read as children beside the filters -- one per field, because a
  // bucket spans fields: `a = 1 AND b = 2` under an `OR`, and a negation over
  // a nullable column, both put two fields in one bucket. A term whose
  // lowering gave it a scorer of its own says so beside its value.
  ExplainNode BuildBucket(const BooleanFilter& filter, Occur occur,
                          std::string_view label) const {
    ExplainNode node{std::string{label}};
    const auto terms = filter.Terms(occur);
    for (size_t i = 0; i != terms.size();) {
      size_t j = i + 1;
      while (j != terms.size() && terms[j].field == terms[i].field) {
        ++j;
      }
      const auto run = terms.subspan(i, j - i);
      i = j;
      ExplainNode leaves{"Terms"};
      leaves.attributes["Field"] = FieldName(run.front().field);
      const auto kind = FieldKind(run.front().field);
      SDB_ASSERT(kind != Kind::Null || run.size() == 1);
      if (kind != Kind::Null || run.front().boost != kNoBoost ||
          Explicit(run.front().scorer) != nullptr) {
        const std::string_view quote = kind == Kind::String ? "'" : "";
        leaves.attributes["Values"] = absl::StrJoin(
          run, ", ", [&](std::string* o, const TermClause& clause) {
            const auto start = o->size();
            absl::StrAppend(o, quote, TermValue(clause.term, kind), quote);
            const auto pad = [&] { return o->size() == start ? "" : " "; };
            if (clause.boost != kNoBoost) {
              absl::StrAppend(o, pad(), "(", clause.boost, ")");
            }
            if (const auto* own = Explicit(clause.scorer)) {
              absl::StrAppend(o, pad(), "[", own->ToString(), "]");
            }
          });
      }
      node.children.push_back(std::move(leaves));
    }
    for (const auto& sub : filter.Filters(occur)) {
      node.children.push_back(Build(*sub));
    }
    return node;
  }

  static bool MustIsAll(const BooleanFilter& filter) noexcept {
    const auto filters = filter.Filters(Occur::Must);
    if (!filter.Terms(Occur::Must).empty() || filters.size() != 1) {
      return false;
    }
    const auto& child = *filters.front();
    return child.type() == Type<All>::id() && child.GetBoost() == kNoBoost &&
           child.GetScorer() == nullptr;
  }

  ExplainNode BuildBool(const BooleanFilter& filter) const {
    const auto must = filter.Size(Occur::Must);
    const auto should = filter.Size(Occur::Should);
    const auto excluded = filter.Size(Occur::MustNot);
    const auto min_match = filter.MinShouldMatch();
    if (excluded == 0 && must == 0 && should != 0 && min_match == 1) {
      auto node = BuildBucket(filter, Occur::Should, "Or");
      AddMergeAttribute(node, filter.MergeType());
      return node;
    }
    if (excluded == 0 && should == 0 && must != 0) {
      auto node = BuildBucket(filter, Occur::Must, "And");
      AddMergeAttribute(node, filter.MergeType());
      return node;
    }
    if (excluded != 0 && should == 0 && MustIsAll(filter)) {
      auto node = BuildBucket(filter, Occur::MustNot, "Not");
      AddMergeAttribute(node, filter.MergeType());
      return node;
    }
    ExplainNode node{"Bool"};
    AddMergeAttribute(node, filter.MergeType());
    static constexpr std::string_view kLabels[]{"Must", "Should", "Must Not"};
    for (const auto occur : kAllOccur) {
      if (filter.Size(occur) == 0) {
        continue;
      }
      auto bucket = BuildBucket(filter, occur, kLabels[OccurIndex(occur)]);
      if (occur == Occur::Should) {
        bucket.attributes["Min Match"] = absl::StrCat(min_match);
      }
      node.children.push_back(std::move(bucket));
    }
    return node;
  }

  ExplainNode BuildNode(const Filter& filter) const {
    const auto& type = filter.type();
    if (type == Type<All>::id()) {
      return ExplainNode{"All"};
    }
    if (type == Type<BooleanFilter>::id()) {
      return BuildBool(downCast<const BooleanFilter>(filter));
    }
    if (type == Type<ByTerm>::id()) {
      const auto& f = downCast<const ByTerm>(filter);
      ExplainNode node{"Term"};
      node.attributes["Field"] = FieldName(f.field_id());
      node.attributes["Value"] =
        TermValue(f.options().term, FieldKind(f.field_id()));
      return node;
    }
    if (type == Type<ByRange>::id()) {
      const auto& f = downCast<const ByRange>(filter);
      ExplainNode node{"Range"};
      node.attributes["Field"] = FieldName(f.field_id());
      node.attributes["Range"] =
        RangeValue(f.options().range, FieldKind(f.field_id()));
      return node;
    }
    if (type == Type<ByGranularRange>::id()) {
      const auto& f = downCast<const ByGranularRange>(filter);
      ExplainNode node{"Granular Range"};
      node.attributes["Field"] = FieldName(f.field_id());
      node.attributes["Range"] =
        RangeValue(f.options().range, FieldKind(f.field_id()));
      return node;
    }
    if (type == Type<ByNGramSimilarity>::id()) {
      const auto& f = downCast<const ByNGramSimilarity>(filter);
      ExplainNode node{"NGram Similarity"};
      node.attributes["Field"] = FieldName(f.field_id());
      node.attributes["NGrams"] = NGramList(f);
      node.attributes["Threshold"] = absl::StrCat(f.options().threshold);
      return node;
    }
    if (type == Type<ByEditDistance>::id()) {
      THROW_SQL_ERROR(
        ERR_MSG("ByEditDistance filter must be lowered to "
                "LevenshteinAutomatonFilter by the optimizer before printing"));
    }
    if (type == Type<LevenshteinAutomatonFilter>::id()) {
      const auto& f = downCast<const LevenshteinAutomatonFilter>(filter);
      const auto& o = f.options();
      ExplainNode node{"Levenshtein"};
      node.attributes["Field"] = FieldName(f.field_id());
      node.attributes["Target"] = TermToString(o.target);
      node.attributes["Max Terms"] = absl::StrCat(o.max_terms);
      return node;
    }
    if (type == Type<ByPrefix>::id()) {
      const auto& f = downCast<const ByPrefix>(filter);
      ExplainNode node{"Starts With"};
      node.attributes["Field"] = FieldName(f.field_id());
      node.attributes["Prefix"] = TermToString(f.options().term);
      node.attributes["Limit"] = absl::StrCat(f.options().scored_terms_limit);
      return node;
    }
    if (type == Type<ByNestedFilter>::id()) {
      const auto& f = downCast<const ByNestedFilter>(filter);
      auto& [parent, child, match, _] = f.options();
      ExplainNode node{"Nested"};
      if (auto* range = std::get_if<Match>(&match); range != nullptr) {
        node.attributes["Match"] = absl::StrCat(range->min, ", ", range->max);
      } else if (std::get_if<irs::MatchProvider>(&match) != nullptr) {
        node.attributes["Match"] = "<Predicate>";
      }
      node.children.push_back(Build(*child));
      return node;
    }
    if (type == Type<ByWildcard>::id()) {
      THROW_SQL_ERROR(
        ERR_MSG("ByWildcard filter must be lowered to "
                "ByTerm/ByPrefix/AutomatonFilter by the optimizer before "
                "printing"));
    }
    if (type == Type<ByRegexp>::id()) {
      THROW_SQL_ERROR(
        ERR_MSG("ByRegexp filter must be lowered to "
                "ByTerm/ByPrefix/AutomatonFilter by the optimizer before "
                "printing"));
    }
    if (type == Type<AutomatonFilter>::id()) {
      const auto& f = downCast<const AutomatonFilter>(filter);
      ExplainNode node{"Automaton"};
      node.attributes["Field"] = FieldName(f.field_id());
      node.attributes["Pattern"] = TermToString(f.options().pattern);
      return node;
    }
    if (type == Type<ByWildcardNGram>::id()) {
      const auto& f = downCast<const ByWildcardNGram>(filter);
      ExplainNode node{"Wildcard NGram"};
      node.attributes["Field"] = FieldName(f.field_id());
      node.attributes["Token"] = TermToString(f.options().token);
      node.attributes["Has Pos"] = f.options().has_pos ? "true" : "false";
      node.attributes["Parts"] = WildcardNGramParts(f);
      return node;
    }
    if (type == Type<Empty>::id()) {
      return ExplainNode{"Empty"};
    }
    if (type == Type<ByPhrase>::id()) {
      const auto& f = downCast<const ByPhrase>(filter);
      ExplainNode node{"Phrase"};
      node.attributes["Field"] = FieldName(f.field_id());
      node.attributes["Parts"] = PhraseParts(f);
      if (const auto slop = f.options().slop(); slop > 0) {
        node.attributes["Slop"] = absl::StrCat(slop);
      }
      return node;
    }
    if (type == Type<GeoFilter>::id()) {
      const auto& f = downCast<const GeoFilter>(filter);
      ExplainNode node{"Geo"};
      node.attributes["Field"] = FieldName(f.field_id());
      node.attributes["Op"] = std::string{GeoFilterTypeName(f.options().type)};
      node.attributes["Shape"] =
        std::string{GeoShapeTypeName(f.options().shape.type())};
      return node;
    }
    if (type == Type<GeoDistanceFilter>::id()) {
      const auto& f = downCast<const GeoDistanceFilter>(filter);
      ExplainNode node{"Geo Distance"};
      node.attributes["Field"] = FieldName(f.field_id());
      node.attributes["Range"] = GeoDistanceRange(f);
      return node;
    }
    if (type == Type<ByRadius>::id()) {
      const auto& f = downCast<const ByRadius>(filter);
      const auto& o = f.options();
      ExplainNode node{"Vector Range"};
      node.attributes["Field"] = FieldName(f.field_id());
      node.attributes["Metric"] = std::string{VectorMetricName(o.metric)};
      node.attributes["Radius"] =
        absl::StrCat(o.inclusive ? "<= " : "< ", o.radius);
      if (o.inner) {
        node.children.push_back(Build(*o.inner));
      }
      return node;
    }
    ExplainNode node{"Unknown"};
    node.attributes["Type"] = std::string{type().name()};
    return node;
  }
};

std::string IdentityField(sdb::catalog::ColumnId id) {
  return absl::StrCat(static_cast<irs::field_id>(id));
}

Kind UnknownKind(sdb::catalog::ColumnId) { return Kind::Unsupported; }

}  // namespace

duckdb::ExplainNode ToExplainNode(const Filter& f) {
  return ToExplainNode(f, IdentityField, UnknownKind);
}

duckdb::ExplainNode ToExplainNode(const Filter& f,
                                  const FieldNameResolver& name_of,
                                  const FieldKindResolver& kind_of) {
  return FilterPrinter{.name_of = name_of, .kind_of = kind_of}.Build(f);
}

}  // namespace irs
