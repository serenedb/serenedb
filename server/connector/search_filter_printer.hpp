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

#pragma once

#include <absl/base/internal/endian.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/column_existence_filter.hpp>
#include <iresearch/search/granular_range_filter.hpp>
#include <iresearch/search/levenshtein_filter.hpp>
#include <iresearch/search/nested_filter.hpp>
#include <iresearch/search/ngram_similarity_filter.hpp>
#include <iresearch/search/phrase_filter.hpp>
#include <iresearch/search/prefix_filter.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/search/search_range.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/terms_filter.hpp>
#include <iresearch/search/wildcard_filter.hpp>
#include <ostream>
#include <string>

#include "catalog/mangling.h"

namespace irs {

inline std::string ToString(const Filter& f);

template<typename Term>
std::string ToString(Term term) {
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

// Field names are encoded as big-endian uint64 column id + 1 mangle byte.
inline std::string FieldToString(std::string_view field) {
  constexpr size_t kIdSize = sizeof(uint64_t);
  if (field.size() < kIdSize) {
    return ToString(field);
  }
  const uint64_t col_id = absl::big_endian::Load64(field.data());
  std::string_view mangle_suffix = field.substr(kIdSize);
  std::string_view mangle_name = "unknown";
  if (!mangle_suffix.empty()) {
    switch (mangle_suffix[0]) {
      case sdb::search::mangling::kNull:
        mangle_name = "null";
        break;
      case sdb::search::mangling::kBool:
        mangle_name = "bool";
        break;
      case sdb::search::mangling::kNumeric:
        mangle_name = "numeric";
        break;
      case sdb::search::mangling::kString:
        mangle_name = "string";
        break;
      case sdb::search::mangling::kAnalyzer:
        mangle_name = "analyzer";
        break;
      case sdb::search::mangling::kNested:
        mangle_name = "nested";
        break;
    }
  }
  return absl::StrCat("col:", col_id, "(", mangle_name, ")");
}

inline std::string ToString(const std::vector<bstring>& terms) {
  return absl::StrCat("( ",
                      absl::StrJoin(terms, " ",
                                    [](std::string* out, const bstring& t) {
                                      absl::StrAppend(out, ToString(t));
                                    }),
                      " )");
}

template<typename T>
std::string RangeToString(const SearchRange<T>& range) {
  std::string s;
  if (!range.min.empty()) {
    absl::StrAppend(&s, " ",
                    range.min_type == irs::BoundType::Inclusive ? ">=" : ">",
                    ToString(range.min));
  }
  if (!range.max.empty()) {
    if (!range.min.empty()) {
      absl::StrAppend(&s, ", ");
    } else {
      absl::StrAppend(&s, " ");
    }
    absl::StrAppend(&s,
                    range.max_type == irs::BoundType::Inclusive ? "<=" : "<",
                    ToString(range.max));
  }
  return s;
}

template<typename Sink>
void AbslStringify(Sink& sink, const ByRange& range) {
  sink.Append(absl::StrCat("Range(", FieldToString(range.field()),
                           RangeToString(range.options().range), ")"));
}

template<typename Sink>
void AbslStringify(Sink& sink, const ByGranularRange& range) {
  sink.Append(absl::StrCat("GranularRange(", FieldToString(range.field()),
                           RangeToString(range.options().range), ")"));
}

template<typename Sink>
void AbslStringify(Sink& sink, const ByTerm& term) {
  sink.Append(absl::StrCat("Term(", FieldToString(term.field()), "=",
                           ToString(term.options().term), ")"));
}

template<typename Sink>
void AbslStringify(Sink& sink, const irs::ByNestedFilter& filter) {
  auto& [parent, child, match, _] = filter.options();
  std::string match_str;
  if (auto* range = std::get_if<irs::Match>(&match); range) {
    match_str = absl::StrCat(range->min, ", ", range->max);
  } else if (nullptr != std::get_if<irs::DocIteratorProvider>(&match)) {
    match_str = "<Predicate>";
  }
  sink.Append(absl::StrCat("NESTED[MATCH[", match_str, "], CHILD[",
                           ToString(*child), "]]"));
}

template<typename Sink>
void AbslStringify(Sink& sink, const And& filter) {
  sink.Append(absl::StrCat("AND[",
                           absl::StrJoin(filter, " && ",
                                         [](std::string* out, const auto& f) {
                                           absl::StrAppend(out, ToString(*f));
                                         }),
                           "]"));
}

template<typename Sink>
void AbslStringify(Sink& sink, const Or& filter) {
  std::string header = "OR";
  if (filter.min_match_count() != 1) {
    absl::StrAppend(&header, "(", filter.min_match_count(), ")");
  }
  sink.Append(absl::StrCat(header, "[",
                           absl::StrJoin(filter, " || ",
                                         [](std::string* out, const auto& f) {
                                           absl::StrAppend(out, ToString(*f));
                                         }),
                           "]"));
}

template<typename Sink>
void AbslStringify(Sink& sink, const Not& filter) {
  sink.Append(absl::StrCat("NOT[", ToString(*filter.filter()), "]"));
}

template<typename Sink>
void AbslStringify(Sink& sink, const ByNGramSimilarity& filter) {
  sink.Append(
    absl::StrCat("NGRAM_SIMILARITY[", FieldToString(filter.field()), ", ",
                 absl::StrJoin(filter.options().ngrams, "",
                               [](std::string* out, const auto& ngram) {
                                 absl::StrAppend(out, ToString(ngram));
                               }),
                 ",", filter.options().threshold, "]"));
}

template<typename Sink>
void AbslStringify(Sink& sink, const Empty&) {
  sink.Append("EMPTY[]");
}

template<typename Sink>
void AbslStringify(Sink& sink, const ByColumnExistence& filter) {
  sink.Append(absl::StrCat("EXISTS[", FieldToString(filter.field()), ", ",
                           size_t(filter.options().acceptor), "]"));
}

template<typename Sink>
void AbslStringify(Sink& sink, const ByEditDistance& lev) {
  sink.Append(absl::StrCat("LEVENSHTEIN_MATCH[", FieldToString(lev.field()),
                           ", '", ToString(lev.options().term), "', ",
                           static_cast<int>(lev.options().max_distance), ", ",
                           lev.options().with_transpositions, ", ",
                           lev.options().max_terms, ", '",
                           ToString(lev.options().prefix), "']"));
}

template<typename Sink>
void AbslStringify(Sink& sink, const ByPrefix& filter) {
  sink.Append(absl::StrCat("STARTS_WITH[", FieldToString(filter.field()), ", '",
                           ToString(filter.options().term), "', ",
                           filter.options().scored_terms_limit, "]"));
}

template<typename Sink>
void AbslStringify(Sink& sink, const ByTerms& filter) {
  std::string terms_str = absl::StrJoin(
    filter.options().terms, "", [](std::string* out, const auto& term_boost) {
      const auto& [term, boost] = term_boost;
      absl::StrAppend(out, "['", ToString(term), "', ", boost, "],");
    });
  sink.Append(absl::StrCat("TERMS[", FieldToString(filter.field()), ", {",
                           terms_str, "}, ", filter.options().min_match, "]"));
}

template<typename Sink>
void AbslStringify(Sink& sink, const All& filter) {
  sink.Append(absl::StrCat("ALL[", filter.Boost(), "]"));
}

template<typename Sink>
void AbslStringify(Sink& sink, const ByWildcard& filter) {
  sink.Append(absl::StrCat("WILDCARD[", FieldToString(filter.field()), ", ",
                           ToString(filter.options().term), "]"));
}

template<typename Sink>
void AbslStringify(Sink& sink, const ByPhrase& filter) {
  struct PartVisitor : util::Noncopyable {
    auto operator()(const ByTermOptions& opts) const {
      absl::StrAppend(out, "Term:", ToString(opts.term));
    }

    auto operator()(const ByTermsOptions& opts) const {
      absl::StrAppend(out, "Terms:[",
                      absl::StrJoin(opts.terms, "",
                                    [](std::string* o, const auto& tb) {
                                      const auto& [term, boost] = tb;
                                      absl::StrAppend(o, "['", ToString(term),
                                                      "', ", boost, "],");
                                    }),
                      "]");
    }

    auto operator()(const ByPrefixOptions& opts) const {
      absl::StrAppend(out, "Prefix:", ToString(opts.term));
    }

    auto operator()(const ByWildcardOptions& opts) const {
      absl::StrAppend(out, "Wildcard:", ToString(opts.term));
    }

    auto operator()(const ByEditDistanceOptions& opts) const {
      absl::StrAppend(out, "Levenshtein:", ToString(opts.term));
    }

    auto operator()(const ByRangeOptions& opts) const {
      absl::StrAppend(out, "Range: ");
      if (opts.range.min_type == irs::BoundType::Unbounded) {
        absl::StrAppend(out, "*");
      } else {
        absl::StrAppend(
          out, opts.range.min_type == irs::BoundType::Inclusive ? "[" : "(",
          std::string(reinterpret_cast<const char*>(opts.range.min.data()),
                      opts.range.min.size()));
      }
      absl::StrAppend(out, "..");
      if (opts.range.max_type == irs::BoundType::Unbounded) {
        absl::StrAppend(out, "*");
      } else {
        absl::StrAppend(
          out,
          std::string(reinterpret_cast<const char*>(opts.range.max.data()),
                      opts.range.max.size()),
          opts.range.max_type == irs::BoundType::Inclusive ? "]" : ")");
      }
    }

    std::string* out;
  };

  std::string parts_str;
  for (const auto& part : filter.options()) {
    std::string part_str;
    part.part.visit(PartVisitor{.out = &part_str});
    absl::StrAppend(&parts_str, part_str, "(", part.offs_max, ", ",
                    part.offs_min, ")", "; ");
  }
  sink.Append(absl::StrCat("PHRASE[", FieldToString(filter.field()), " = <",
                           parts_str, ">]"));
}

template<typename Sink>
void AbslStringify(Sink& sink, const Filter& filter) {
  const auto& type = filter.type();
  if (type == irs::Type<All>::id()) {
    AbslStringify(sink, static_cast<const All&>(filter));
  } else if (type == irs::Type<And>::id()) {
    AbslStringify(sink, static_cast<const And&>(filter));
  } else if (type == irs::Type<Or>::id()) {
    AbslStringify(sink, static_cast<const Or&>(filter));
  } else if (type == irs::Type<Not>::id()) {
    AbslStringify(sink, static_cast<const Not&>(filter));
  } else if (type == irs::Type<ByTerm>::id()) {
    AbslStringify(sink, static_cast<const ByTerm&>(filter));
  } else if (type == irs::Type<ByTerms>::id()) {
    AbslStringify(sink, static_cast<const ByTerms&>(filter));
  } else if (type == irs::Type<ByRange>::id()) {
    AbslStringify(sink, static_cast<const ByRange&>(filter));
  } else if (type == irs::Type<ByGranularRange>::id()) {
    AbslStringify(sink, static_cast<const ByGranularRange&>(filter));
  } else if (type == irs::Type<ByNGramSimilarity>::id()) {
    AbslStringify(sink, static_cast<const ByNGramSimilarity&>(filter));
  } else if (type == irs::Type<ByEditDistance>::id()) {
    AbslStringify(sink, static_cast<const ByEditDistance&>(filter));
  } else if (type == irs::Type<ByPrefix>::id()) {
    AbslStringify(sink, static_cast<const ByPrefix&>(filter));
  } else if (type == irs::Type<ByNestedFilter>::id()) {
    AbslStringify(sink, static_cast<const ByNestedFilter&>(filter));
  } else if (type == irs::Type<ByColumnExistence>::id()) {
    AbslStringify(sink, static_cast<const ByColumnExistence&>(filter));
  } else if (type == irs::Type<ByWildcard>::id()) {
    AbslStringify(sink, static_cast<const ByWildcard&>(filter));
  } else if (type == irs::Type<Empty>::id()) {
    AbslStringify(sink, static_cast<const Empty&>(filter));
  } else if (type == irs::Type<ByPhrase>::id()) {
    AbslStringify(sink, static_cast<const ByPhrase&>(filter));
  } else {
    sink.Append(absl::StrCat("[Unknown filter ", type().name(), " ]"));
  }
}

inline std::ostream& operator<<(std::ostream& os, const Filter& filter) {
  return os << absl::StrCat(filter);
}

inline std::string ToString(const irs::Filter& f) { return absl::StrCat(f); }

}  // namespace irs
