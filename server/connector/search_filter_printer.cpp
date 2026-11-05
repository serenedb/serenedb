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

#include <duckdb/common/types/decimal.hpp>
#include <duckdb/common/types/uuid.hpp>
#include <iresearch/search/detail/pattern_cache.hpp>
#include <iresearch/search/detail/search_range.hpp>
#include <iresearch/search/filters/all_filter.hpp>
#include <iresearch/search/filters/automaton_filter.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/geo_filter.hpp>
#include <iresearch/search/filters/granular_range_filter.hpp>
#include <iresearch/search/filters/levenshtein_filter.hpp>
#include <iresearch/search/filters/nested_filter.hpp>
#include <iresearch/search/filters/ngram_similarity_filter.hpp>
#include <iresearch/search/filters/phrase_filter.hpp>
#include <iresearch/search/filters/prefix_filter.hpp>
#include <iresearch/search/filters/range_filter.hpp>
#include <iresearch/search/filters/regexp_filter.hpp>
#include <iresearch/search/filters/term_filter.hpp>
#include <iresearch/search/filters/vector_exact_filter.hpp>
#include <iresearch/search/filters/vector_radius_filter.hpp>
#include <iresearch/search/filters/wildcard_filter.hpp>
#include <iresearch/search/filters/wildcard_ngram_filter.hpp>
#include <iresearch/search/scorers/constant_score.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/numeric_utils.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <magic_enum/magic_enum.hpp>

#include "connector/inet_terms.h"

namespace irs {
namespace {

using duckdb::ExplainNode;
using duckdb::LogicalType;
using duckdb::LogicalTypeId;
using irs::utils::downCast;

const Scorer* Explicit(const Scorer* scorer) noexcept {
  return IsConstScoreSingleton(scorer) ? nullptr : scorer;
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

// Decodes a numeric term back to the number it encodes. The leading byte is
// the type's TYPE_MAGIC, disjoint per type, so the value type is recovered
// from the term itself (which also disambiguates the shared JSON-leaf numeric
// field).
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
  if (marker == nu::UnsignedEncodeTraits::TYPE_MAGIC &&
      term.size() == nu::kNumericTermSize<uint64_t>) {
    return absl::StrCat(nu::DecodeNumericTerm<uint64_t>(term));
  }
  if (marker == nu::encode_traits<unsigned __int128>::TYPE_MAGIC &&
      term.size() == nu::kNumericTermSize<__int128>) {
    return duckdb::hugeint_t::FromInt128(
             absl::int128{nu::DecodeNumericTerm<__int128>(term)})
      .ToString();
  }
  if (marker == nu::Unsigned128EncodeTraits::TYPE_MAGIC &&
      term.size() == nu::kNumericTermSize<unsigned __int128>) {
    return duckdb::uhugeint_t::FromUint128(
             absl::uint128{nu::DecodeNumericTerm<unsigned __int128>(term)})
      .ToString();
  }
  return TermToString(term);
}

std::string DecodeTypedNumericTerm(bytes_view term, const LogicalType& type) {
  namespace nu = irs::numeric_utils;
  const auto as = [&]<typename T>() -> std::optional<T> {
    if (term.size() != nu::kNumericTermSize<T> ||
        term[0] != nu::NumericEncodeOf<T>::Traits::TYPE_MAGIC) {
      return std::nullopt;
    }
    if constexpr (std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>) {
      return nu::numeric_traits<T>::decode(term.data());
    } else {
      return nu::DecodeNumericTerm<T>(term);
    }
  };
  if (type.id() == LogicalTypeId::UUID) {
    if (const auto v = as.template operator()<__int128>()) {
      return duckdb::UUID::ToString(
        duckdb::hugeint_t::FromInt128(absl::int128{*v}));
    }
  } else if (type.id() == LogicalTypeId::DECIMAL) {
    const auto width = duckdb::DecimalType::GetWidth(type);
    const auto scale = duckdb::DecimalType::GetScale(type);
    if (const auto v = as.template operator()<int32_t>()) {
      return duckdb::Decimal::ToString(*v, width, scale);
    }
    if (const auto v = as.template operator()<int64_t>()) {
      return duckdb::Decimal::ToString(*v, width, scale);
    }
    if (const auto v = as.template operator()<__int128>()) {
      return duckdb::Decimal::ToString(
        duckdb::hugeint_t::FromInt128(absl::int128{*v}), width, scale);
    }
  }
  return DecodeNumericTerm(term);
}

bool IsString(const LogicalType& type) {
  switch (type.id()) {
    case LogicalTypeId::VARCHAR:
    case LogicalTypeId::BLOB:
    case LogicalTypeId::GEOMETRY:
      return true;
    default:
      return false;
  }
}

std::string TermValue(bytes_view term, const LogicalType& type) {
  if (type.id() == LogicalTypeId::BOOLEAN) {
    return !term.empty() && term[0] ? "true" : "false";
  }
  if (sdb::connector::term_dict::IsNumeric(type.id())) {
    return DecodeTypedNumericTerm(term, type);
  }
  if (sdb::pg::IsInetOrCidr(type)) {
    return sdb::connector::inet_terms::Describe(term);
  }
  return TermToString(term);
}

std::string PrefixValue(bytes_view prefix, const LogicalType& type) {
  return sdb::pg::IsInetOrCidr(type) ? TermValue(prefix, type)
                                     : TermToString(prefix);
}

std::string TermValue(const bstring& term, const LogicalType& type) {
  return TermValue(bytes_view{term}, type);
}

std::string DictTermValue(DictRef dict, bytes_view term,
                          const LogicalType& type) {
  switch (dict.member) {
    case DictMember::False:
      return "false";
    case DictMember::True:
      return "true";
    default:
      return TermValue(term, type);
  }
}

std::string ClauseValue(const TermClause& clause, const LogicalType& type) {
  return DictTermValue(clause.dict, clause.term, type);
}

template<typename T>
std::string RangeValue(const SearchRange<T>& range, const LogicalType& type) {
  std::string s;
  if (!range.min.empty()) {
    absl::StrAppend(&s, range.min_type == BoundType::Inclusive ? ">=" : ">",
                    TermValue(range.min, type));
  }
  if (!range.max.empty()) {
    if (!s.empty()) {
      absl::StrAppend(&s, ", ");
    }
    absl::StrAppend(&s, range.max_type == BoundType::Inclusive ? "<=" : "<",
                    TermValue(range.max, type));
  }
  return s;
}

std::string QuotedTerms(const auto& terms) {
  return absl::StrJoin(terms, "", [](std::string* out, const auto& term) {
    absl::StrAppend(out, "'", TermToString(term), "',");
  });
}

// Renders one phrase-part option variant.
struct PhrasePartVisitor : util::Noncopyable {
  auto operator()(const ByTermOptions& opts) const {
    absl::StrAppend(out, "Term:", TermToString(opts.term));
  }
  auto operator()(const TermSetOptions& opts) const {
    absl::StrAppend(out, "Terms:[", QuotedTerms(opts.terms), "]");
  }
  auto operator()(const ShingleOptions& opts) const {
    absl::StrAppend(out, "Shingle:", TermToString(opts.term), "[",
                    QuotedTerms(opts.words), "]");
  }
  auto operator()(const ByPrefixOptions& opts) const {
    absl::StrAppend(out, "Prefix:", TermToString(opts.term));
  }
  auto operator()(const ByWildcardOptions&) const {
    THROW_SQL_ERROR(
      ERR_MSG("Wildcard phrase part must be lowered by the optimizer before "
              "printing"));
  }
  auto operator()(const ByRegexpOptions&) const {
    THROW_SQL_ERROR(
      ERR_MSG("Regexp phrase part must be lowered by the optimizer before "
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
  auto operator()(const AutomatonOptions& opts) const {
    absl::StrAppend(out, "Automaton:", TermToString(opts.pattern));
  }
  auto operator()(const ByRangeOptions& opts) const {
    absl::StrAppend(out, "Range: ");
    if (opts.range.min_type == BoundType::Unbounded) {
      absl::StrAppend(out, "*");
    } else {
      absl::StrAppend(out,
                      opts.range.min_type == BoundType::Inclusive ? "[" : "(",
                      ViewCast<char>(bytes_view{opts.range.min}));
    }
    absl::StrAppend(out, "..");
    if (opts.range.max_type == BoundType::Unbounded) {
      absl::StrAppend(out, "*");
    } else {
      absl::StrAppend(out, ViewCast<char>(bytes_view{opts.range.max}),
                      opts.range.max_type == BoundType::Inclusive ? "]" : ")");
    }
  }
  std::string* out;
};

template<typename Options>
std::string_view GeoVerify(const Options& options) {
  return field_limits::valid(options.store_field_id) &&
             PlanGeo(options)->kind == GeoPlan::Kind::Cells
           ? "inline"
           : "false";
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

std::string_view GeoShapeTypeName(irs::geo::ShapeContainer::Type type) {
  using T = irs::geo::ShapeContainer::Type;
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
  const FieldTypeResolver& type_of;

  std::string FieldName(field_id fid, DictRef dict) const {
    return name_of(sdb::connector::ColumnId{fid}, dict);
  }
  LogicalType FieldType(field_id fid, DictRef dict) const {
    return type_of(sdb::connector::ColumnId{fid}, dict);
  }

  std::string PhraseParts(const ByPhraseOptions& options) const {
    std::string s;
    for (const auto& part : options) {
      std::string part_str;
      part.part.visit(PhrasePartVisitor{.out = &part_str});
      absl::StrAppend(&s, part_str, "(", part.offs_min, ", ", part.offs_max,
                      ")", "; ");
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
      while (j != terms.size() && SameField(terms[j], terms[i])) {
        ++j;
      }
      const auto run = terms.subspan(i, j - i);
      i = j;
      ExplainNode leaves{"Terms"};
      const auto field = run.front().field;
      const auto dict = run.front().dict;
      leaves.attributes["Field"] = FieldName(field, dict);
      const auto type = FieldType(field, dict);
      const bool null = type.id() == LogicalTypeId::SQLNULL;
      SDB_ASSERT(!null || run.size() == 1);
      if (!null || run.front().boost != kNoBoost ||
          Explicit(run.front().scorer)) {
        const std::string_view quote = IsString(type) ? "'" : "";
        leaves.attributes["Values"] = absl::StrJoin(
          run, ", ", [&](std::string* o, const TermClause& clause) {
            const auto start = o->size();
            absl::StrAppend(o, quote, ClauseValue(clause, type), quote);
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
           !child.GetScorer();
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
      node.attributes["Field"] = FieldName(f.field_id(), f.dict());
      node.attributes["Value"] = DictTermValue(
        f.dict(), f.options().term, FieldType(f.field_id(), f.dict()));
      return node;
    }
    if (type == Type<ByRange>::id()) {
      const auto& f = downCast<const ByRange>(filter);
      ExplainNode node{"Range"};
      node.attributes["Field"] = FieldName(f.field_id(), f.dict());
      node.attributes["Range"] =
        RangeValue(f.options().range, FieldType(f.field_id(), f.dict()));
      return node;
    }
    if (type == Type<ByGranularRange>::id()) {
      const auto& f = downCast<const ByGranularRange>(filter);
      ExplainNode node{"Granular Range"};
      node.attributes["Field"] = FieldName(f.field_id(), f.dict());
      node.attributes["Range"] =
        RangeValue(f.options().range, FieldType(f.field_id(), f.dict()));
      return node;
    }
    if (type == Type<ByNGramSimilarity>::id()) {
      const auto& f = downCast<const ByNGramSimilarity>(filter);
      ExplainNode node{"NGram Similarity"};
      node.attributes["Field"] = FieldName(f.field_id(), f.dict());
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
      node.attributes["Field"] = FieldName(f.field_id(), f.dict());
      node.attributes["Target"] = TermToString(o.target);
      node.attributes["Max Terms"] = o.with_ties
                                       ? absl::StrCat(o.max_terms, " with ties")
                                       : absl::StrCat(o.max_terms);
      return node;
    }
    if (type == Type<ByPrefix>::id()) {
      const auto& f = downCast<const ByPrefix>(filter);
      ExplainNode node{"Starts With"};
      node.attributes["Field"] = FieldName(f.field_id(), f.dict());
      node.attributes["Prefix"] =
        PrefixValue(f.options().term, FieldType(f.field_id(), f.dict()));
      return node;
    }
    if (type == Type<ByPrefixes>::id()) {
      const auto& f = downCast<const ByPrefixes>(filter);
      ExplainNode node{"Starts With Any"};
      node.attributes["Field"] = FieldName(f.field_id(), f.dict());
      const auto field_type = FieldType(f.field_id(), f.dict());
      const auto prefixes = f.options().Prefixes();
      node.attributes["Prefixes"] =
        prefixes.empty()
          ? std::string{"0"}
          : absl::StrCat(prefixes.size(), ", ",
                         PrefixValue(prefixes.front(), field_type), " .. ",
                         PrefixValue(prefixes.back(), field_type));
      return node;
    }
    if (type == Type<ByNestedFilter>::id()) {
      const auto& f = downCast<const ByNestedFilter>(filter);
      auto& [parent, child, match, _] = f.options();
      ExplainNode node{"Nested"};
      if (auto* range = std::get_if<Match>(&match)) {
        node.attributes["Match"] = absl::StrCat(range->min, ", ", range->max);
      } else if (std::get_if<irs::MatchProvider>(&match)) {
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
      node.attributes["Field"] = FieldName(f.field_id(), f.dict());
      const auto& options = f.options();
      node.attributes["Pattern"] =
        options.kind == PatternKind::Union
          ? TermToString(irs::DescribeUnion(options.pattern))
          : TermToString(options.pattern);
      return node;
    }
    if (type == Type<ByWildcardNGram>::id()) {
      const auto& f = downCast<const ByWildcardNGram>(filter);
      const auto& options = f.options();
      ExplainNode node{"Wildcard NGram"};
      node.attributes["Field"] = FieldName(f.field_id(), f.dict());
      node.attributes["Pattern"] = TermToString(options.pattern);
      node.attributes["Syntax"] =
        options.syntax == RegexpSyntax::Perl ? "perl" : "posix";
      node.attributes["Has Pos"] = options.has_pos ? "true" : "false";
      node.attributes["Query"] = irs::ToString(options.query);
      node.attributes["Verify"] =
        options.matcher && field_limits::valid(options.store_field_id)
          ? "inline"
          : "false";
      return node;
    }
    if (type == Type<Empty>::id()) {
      return ExplainNode{"Empty"};
    }
    if (type == Type<ByPhrase>::id()) {
      const auto& f = downCast<const ByPhrase>(filter);
      ExplainNode node{"Phrase"};
      node.attributes["Field"] = FieldName(f.field_id(), f.dict());
      node.attributes["Parts"] = PhraseParts(f.options());
      if (const auto separator = f.options().word_separator();
          !separator.empty()) {
        node.attributes["Separator"] =
          absl::StrCat("'", TermToString(separator), "'");
      }
      if (const auto slop = f.options().slop(); slop > 0) {
        node.attributes["Slop"] = absl::StrCat(slop);
      }
      if (field_limits::valid(f.options().text())) {
        node.attributes["Verify"] = "inline";
      }
      return node;
    }
    if (type == Type<GeoFilter>::id()) {
      const auto& f = downCast<const GeoFilter>(filter);
      ExplainNode node{"Geo"};
      node.attributes["Field"] = FieldName(f.field_id(), f.dict());
      node.attributes["Op"].assign(GeoFilterTypeName(f.options().type));
      node.attributes["Shape"].assign(
        GeoShapeTypeName(f.options().shape.type()));
      node.attributes["Verify"].assign(GeoVerify(f.options()));
      return node;
    }
    if (type == Type<GeoDistanceFilter>::id()) {
      const auto& f = downCast<const GeoDistanceFilter>(filter);
      ExplainNode node{"Geo Distance"};
      node.attributes["Field"] = FieldName(f.field_id(), f.dict());
      node.attributes["Range"] = GeoDistanceRange(f);
      node.attributes["Verify"].assign(GeoVerify(f.options()));
      return node;
    }
    if (type == Type<ByRadius>::id()) {
      const auto& f = downCast<const ByRadius>(filter);
      const auto& o = f.options();
      ExplainNode node{"Vector Range"};
      node.attributes["Field"] = FieldName(f.field_id(), f.dict());
      node.attributes["Metric"].assign(VectorMetricName(o.metric));
      node.attributes["Radius"] =
        absl::StrCat(o.inclusive ? "<= " : "< ", o.radius);
      if (o.inner) {
        node.children.push_back(Build(*o.inner));
      }
      return node;
    }
    if (type == Type<ByVectorExact>::id()) {
      const auto& f = downCast<const ByVectorExact>(filter);
      const auto& o = f.options();
      ExplainNode node{"Vector Range"};
      node.attributes["Field"] = FieldName(f.field_id(), f.dict());
      node.attributes["Metric"].assign(VectorMetricName(o.metric));
      node.attributes["Radius"] =
        absl::StrCat(o.inclusive ? "<= " : "< ", o.radius);
      node.attributes["Exact"] = "brute force over the stored vectors";
      if (o.inner) {
        node.children.push_back(Build(*o.inner));
      }
      return node;
    }
    ExplainNode node{"Unknown"};
    node.attributes["Type"].assign(type().name());
    return node;
  }
};

std::string IdentityField(sdb::connector::ColumnId id, DictRef dict) {
  if (dict.member == DictMember::Strings) {
    return absl::StrCat(id);
  }
  return absl::StrCat(id, "(", magic_enum::enum_name(dict.member), ")");
}

LogicalType UnknownType(sdb::connector::ColumnId, DictRef) { return {}; }

}  // namespace

duckdb::ExplainNode ToExplainNode(const Filter& f) {
  return ToExplainNode(f, IdentityField, UnknownType);
}

duckdb::ExplainNode ToExplainNode(const Filter& f,
                                  const FieldNameResolver& name_of,
                                  const FieldTypeResolver& type_of) {
  return FilterPrinter{.name_of = name_of, .type_of = type_of}.Build(f);
}

}  // namespace irs
