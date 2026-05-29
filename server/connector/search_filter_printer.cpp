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

#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/column_existence_filter.hpp>
#include <iresearch/search/geo_filter.hpp>
#include <iresearch/search/granular_range_filter.hpp>
#include <iresearch/search/levenshtein_filter.hpp>
#include <iresearch/search/nested_filter.hpp>
#include <iresearch/search/ngram_similarity_filter.hpp>
#include <iresearch/search/phrase_filter.hpp>
#include <iresearch/search/phrase_ngram_filter.hpp>
#include <iresearch/search/prefix_filter.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/search/regexp_filter.hpp>
#include <iresearch/search/regexp_ngram_filter.hpp>
#include <iresearch/search/search_range.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/terms_filter.hpp>
#include <iresearch/search/wildcard_filter.hpp>
#include <iresearch/search/wildcard_ngram_filter.hpp>

#include "catalog/mangling.h"

namespace irs {
namespace {

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

template<typename FT>
std::string StringifyFilter(const Filter& filter, FT&& ft);

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
  auto operator()(const ByWildcardOptions& opts) const {
    absl::StrAppend(out, "Wildcard:", TermToString(opts.term));
  }
  auto operator()(const ByEditDistanceOptions& opts) const {
    absl::StrAppend(out, "Levenshtein:", TermToString(opts.term));
  }
  auto operator()(const ByRegexpOptions& opts) const {
    absl::StrAppend(out, "Regexp:", TermToString(opts.pattern));
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

template<typename FT>
void StringifyRange(std::string* out, const ByRange& range, FT&& ft) {
  absl::StrAppend(out, "Range(", ft(range.field()),
                  RangeToString(range.options().range), ")");
}

template<typename FT>
void StringifyGranularRange(std::string* out, const ByGranularRange& range,
                            FT&& ft) {
  absl::StrAppend(out, "GranularRange(", ft(range.field()),
                  RangeToString(range.options().range), ")");
}

template<typename FT>
void StringifyTerm(std::string* out, const ByTerm& term, FT&& ft) {
  absl::StrAppend(out, "Term(", ft(term.field()), "=",
                  TermToString(term.options().term), ")");
}

template<typename FT>
void StringifyNested(std::string* out, const ByNestedFilter& filter, FT&& ft) {
  auto& [parent, child, match, _] = filter.options();
  std::string match_str;
  if (auto* range = std::get_if<Match>(&match); range) {
    match_str = absl::StrCat(range->min, ", ", range->max);
  } else if (nullptr != std::get_if<DocIteratorProvider>(&match)) {
    match_str = "<Predicate>";
  }
  absl::StrAppend(out, "NESTED[MATCH[", match_str, "], CHILD[",
                  StringifyFilter(*child, ft), "]]");
}

template<typename FT>
void StringifyAnd(std::string* out, const And& filter, FT&& ft) {
  absl::StrAppend(out, "AND[",
                  absl::StrJoin(filter, " && ",
                                [&ft](std::string* o, const auto& f) {
                                  absl::StrAppend(o, StringifyFilter(*f, ft));
                                }),
                  "]");
}

template<typename FT>
void StringifyOr(std::string* out, const Or& filter, FT&& ft) {
  std::string header = "OR";
  if (filter.min_match_count() != 1) {
    absl::StrAppend(&header, "(", filter.min_match_count(), ")");
  }
  absl::StrAppend(out, header, "[",
                  absl::StrJoin(filter, " || ",
                                [&ft](std::string* o, const auto& f) {
                                  absl::StrAppend(o, StringifyFilter(*f, ft));
                                }),
                  "]");
}

template<typename FT>
void StringifyNot(std::string* out, const Not& filter, FT&& ft) {
  absl::StrAppend(out, "NOT[", StringifyFilter(*filter.filter(), ft), "]");
}

template<typename FT>
void StringifyNGram(std::string* out, const ByNGramSimilarity& filter,
                    FT&& ft) {
  absl::StrAppend(out, "NGRAM_SIMILARITY[", ft(filter.field()), ", ",
                  absl::StrJoin(filter.options().ngrams, "",
                                [](std::string* o, const auto& ngram) {
                                  absl::StrAppend(o, TermToString(ngram));
                                }),
                  ",", filter.options().threshold, "]");
}

template<typename FT>
void StringifyColumnExistence(std::string* out, const ByColumnExistence& filter,
                              FT&& /*ft*/) {
  absl::StrAppend(out, "EXISTS[", filter.id(), "]");
}

template<typename FT>
void StringifyEditDistance(std::string* out, const ByEditDistance& lev,
                           FT&& ft) {
  absl::StrAppend(out, "LEVENSHTEIN_MATCH[", ft(lev.field()), ", '",
                  TermToString(lev.options().term), "', ",
                  static_cast<int>(lev.options().max_distance), ", ",
                  lev.options().with_transpositions, ", ",
                  lev.options().max_terms, ", '",
                  TermToString(lev.options().prefix), "']");
}

template<typename FT>
void StringifyPrefix(std::string* out, const ByPrefix& filter, FT&& ft) {
  absl::StrAppend(out, "STARTS_WITH[", ft(filter.field()), ", '",
                  TermToString(filter.options().term), "', ",
                  filter.options().scored_terms_limit, "]");
}

template<typename FT>
void StringifyTerms(std::string* out, const ByTerms& filter, FT&& ft) {
  std::string terms_str = absl::StrJoin(
    filter.options().terms, "", [](std::string* o, const auto& term_boost) {
      const auto& [term, boost] = term_boost;
      absl::StrAppend(o, "['", TermToString(term), "', ", boost, "],");
    });
  absl::StrAppend(out, "TERMS[", ft(filter.field()), ", {", terms_str, "}, ",
                  filter.options().min_match, "]");
}

template<typename FT>
void StringifyWildcard(std::string* out, const ByWildcard& filter, FT&& ft) {
  absl::StrAppend(out, "WILDCARD[", ft(filter.field()), ", ",
                  TermToString(filter.options().term), "]");
}

template<typename FT>
void StringifyRegexp(std::string* out, const ByRegexp& filter, FT&& ft) {
  absl::StrAppend(out, "REGEXP[", ft(filter.field()), ", ",
                  TermToString(filter.options().pattern), "]");
}

template<typename FT>
void StringifyPhrase(std::string* out, const ByPhrase& filter, FT&& ft) {
  std::string parts_str;
  for (const auto& part : filter.options()) {
    std::string part_str;
    part.part.visit(PhrasePartVisitor{.out = &part_str});
    absl::StrAppend(&parts_str, part_str, "(", part.offs_max, ", ",
                    part.offs_min, ")", "; ");
  }
  absl::StrAppend(out, "PHRASE[", ft(filter.field()), " = <", parts_str, ">]");
}

template<typename FT>
void StringifyWildcardNgram(std::string* out, const ByWildcardNgram& filter,
                            FT&& ft) {
  std::string parts_str;
  for (const auto& phrase : filter.options().parts) {
    absl::StrAppend(&parts_str, "<");
    for (const auto& part : phrase) {
      std::string part_str;
      part.part.visit(PhrasePartVisitor{.out = &part_str});
      absl::StrAppend(&parts_str, part_str, "(", part.offs_max, ", ",
                      part.offs_min, ")", "; ");
    }
    absl::StrAppend(&parts_str, ">; ");
  }
  absl::StrAppend(out, "WILDCARD_NGRAM[", ft(filter.field()), ", '",
                  TermToString(filter.options().token),
                  "', has_pos=", filter.options().has_pos, ", parts=[",
                  parts_str, "]]");
}

template<typename FT>
void StringifyPhraseNgram(std::string* out, const ByPhraseNgram& filter,
                          FT&& ft) {
  const auto& opts = filter.options();
  std::string shingles;
  for (const auto& shingle : opts.shingles) {
    absl::StrAppend(&shingles, TermToString(shingle), "; ");
  }
  absl::StrAppend(out, "PHRASE_NGRAM[", ft(filter.field()),
                  ", exact=", opts.exact, ", shingles=[", shingles, "]]");
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

template<typename FT>
void StringifyRegexpNgram(std::string* out, const ByRegexpNgram& filter,
                          FT&& ft) {
  const auto& opts = filter.options();
  absl::StrAppend(out, "REGEXP_NGRAM[", ft(filter.field()), ", /",
                  opts.matcher ? opts.matcher->pattern() : std::string{},
                  "/, ngrams=", RegexpNgramTree(opts.root), "]");
}

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

template<typename FT>
void StringifyGeo(std::string* out, const GeoFilter& filter, FT&& ft) {
  absl::StrAppend(out, "GEO[", ft(filter.field()),
                  ", op=", GeoFilterTypeName(filter.options().type),
                  ", shape=", GeoShapeTypeName(filter.options().shape.type()),
                  "]");
}

template<typename FT>
void StringifyGeoDistance(std::string* out, const GeoDistanceFilter& filter,
                          FT&& ft) {
  const auto& range = filter.options().range;
  absl::StrAppend(out, "GEO_DISTANCE[", ft(filter.field()), ",");
  if (range.min_type == BoundType::Unbounded) {
    absl::StrAppend(out, " *");
  } else {
    absl::StrAppend(out, range.min_type == BoundType::Inclusive ? " >=" : " >",
                    range.min);
  }
  if (range.max_type != BoundType::Unbounded) {
    absl::StrAppend(out, ",",
                    range.max_type == BoundType::Inclusive ? " <=" : " <",
                    range.max);
  }
  absl::StrAppend(out, "]");
}

template<typename FT>
std::string StringifyFilter(const Filter& filter, FT&& ft) {
  std::string out;
  const auto& type = filter.type();
  if (type == Type<All>::id()) {
    absl::StrAppend(&out, "ALL[", static_cast<const All&>(filter).Boost(), "]");
  } else if (type == Type<And>::id()) {
    StringifyAnd(&out, static_cast<const And&>(filter), ft);
  } else if (type == Type<Or>::id()) {
    StringifyOr(&out, static_cast<const Or&>(filter), ft);
  } else if (type == Type<Not>::id()) {
    StringifyNot(&out, static_cast<const Not&>(filter), ft);
  } else if (type == Type<ByTerm>::id()) {
    StringifyTerm(&out, static_cast<const ByTerm&>(filter), ft);
  } else if (type == Type<ByTerms>::id()) {
    StringifyTerms(&out, static_cast<const ByTerms&>(filter), ft);
  } else if (type == Type<ByRange>::id()) {
    StringifyRange(&out, static_cast<const ByRange&>(filter), ft);
  } else if (type == Type<ByGranularRange>::id()) {
    StringifyGranularRange(&out, static_cast<const ByGranularRange&>(filter),
                           ft);
  } else if (type == Type<ByNGramSimilarity>::id()) {
    StringifyNGram(&out, static_cast<const ByNGramSimilarity&>(filter), ft);
  } else if (type == Type<ByEditDistance>::id()) {
    StringifyEditDistance(&out, static_cast<const ByEditDistance&>(filter), ft);
  } else if (type == Type<ByPrefix>::id()) {
    StringifyPrefix(&out, static_cast<const ByPrefix&>(filter), ft);
  } else if (type == Type<ByNestedFilter>::id()) {
    StringifyNested(&out, static_cast<const ByNestedFilter&>(filter), ft);
  } else if (type == Type<ByColumnExistence>::id()) {
    StringifyColumnExistence(&out,
                             static_cast<const ByColumnExistence&>(filter), ft);
  } else if (type == Type<ByWildcard>::id()) {
    StringifyWildcard(&out, static_cast<const ByWildcard&>(filter), ft);
  } else if (type == Type<ByWildcardNgram>::id()) {
    StringifyWildcardNgram(&out, static_cast<const ByWildcardNgram&>(filter),
                           ft);
  } else if (type == Type<ByRegexp>::id()) {
    StringifyRegexp(&out, static_cast<const ByRegexp&>(filter), ft);
  } else if (type == Type<ByPhraseNgram>::id()) {
    StringifyPhraseNgram(&out, static_cast<const ByPhraseNgram&>(filter), ft);
  } else if (type == Type<ByRegexpNgram>::id()) {
    StringifyRegexpNgram(&out, static_cast<const ByRegexpNgram&>(filter), ft);
  } else if (type == Type<Empty>::id()) {
    out = "EMPTY[]";
  } else if (type == Type<ByPhrase>::id()) {
    StringifyPhrase(&out, static_cast<const ByPhrase&>(filter), ft);
  } else if (type == Type<GeoFilter>::id()) {
    StringifyGeo(&out, static_cast<const GeoFilter&>(filter), ft);
  } else if (type == Type<GeoDistanceFilter>::id()) {
    StringifyGeoDistance(&out, static_cast<const GeoDistanceFilter&>(filter),
                         ft);
  } else {
    out = absl::StrCat("[Unknown filter ", type().name(), " ]");
  }
  return out;
}

std::string_view IdentityField(std::string_view f) { return f; }

std::string_view MangleName(std::string_view mangle_suffix) {
  if (mangle_suffix.empty()) {
    return "unknown";
  }
  switch (mangle_suffix[0]) {
    case sdb::search::mangling::kNull:
      return "null";
    case sdb::search::mangling::kBool:
      return "bool";
    case sdb::search::mangling::kNumeric:
      return "numeric";
    case sdb::search::mangling::kString:
      return "string";
    case sdb::search::mangling::kAnalyzer:
      return "analyzer";
    case sdb::search::mangling::kNested:
      return "nested";
    default:
      return "unknown";
  }
}

}  // namespace

std::string ToString(const Filter& f) {
  return StringifyFilter(f, IdentityField);
}

std::string ToStringDemangled(
  const Filter& f,
  const std::function<std::string(sdb::catalog::Column::Id)>& col_name) {
  return StringifyFilter(f, [&](std::string_view field) -> std::string {
    constexpr size_t kIdSize = sizeof(uint64_t);
    if (field.size() < kIdSize) {
      return TermToString(field);
    }
    const uint64_t col_id = absl::big_endian::Load64(field.data());
    return absl::StrCat(col_name(sdb::catalog::Column::Id{col_id}), "(",
                        MangleName(field.substr(kIdSize)), ")");
  });
}

}  // namespace irs
