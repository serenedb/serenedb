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

#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/column_existence_filter.hpp>
#include <iresearch/search/granular_range_filter.hpp>
#include <iresearch/search/levenshtein_filter.hpp>
#include <iresearch/search/nested_filter.hpp>
#include <iresearch/search/ngram_similarity_filter.hpp>
#include <iresearch/search/prefix_filter.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/search/search_range.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/terms_filter.hpp>

namespace irs {

std::ostream& operator<<(std::ostream& os, const Filter& filter);

std::string ToString(bytes_view term) {
  std::string s;
  for (auto c : term) {
    if (absl::ascii_isprint(c)) {
      s += c;
    } else {
      s += " ";
      s.append(absl::AlphaNum(int{c}).Piece());
      s += " ";
    }
  }
  return s;
}

std::string ToString(const std::vector<bstring>& terms) {
  std::string s = "( ";
  for (auto& term : terms) {
    s += ToString(term) + " ";
  }
  s += ")";
  return s;
}

template<typename T>
std::ostream& operator<<(std::ostream& os, const SearchRange<T>& range) {
  if (!range.min.empty()) {
    os << " " << (range.min_type == irs::BoundType::Inclusive ? ">=" : ">")
       << ToString(range.min);
  }
  if (!range.max.empty()) {
    if (!range.min.empty()) {
      os << ", ";
    } else {
      os << " ";
    }
    os << (range.max_type == irs::BoundType::Inclusive ? "<=" : "<")
       << ToString(range.max);
  }
  return os;
}

std::ostream& operator<<(std::ostream& os, const ByRange& range) {
  return os << "Range(" << range.field() << range.options().range << ")";
}

std::ostream& operator<<(std::ostream& os, const ByGranularRange& range) {
  return os << "GranularRange(" << range.field() << range.options().range
            << ")";
}

std::ostream& operator<<(std::ostream& os, const ByTerm& term) {
  std::string term_value(ViewCast<char>(irs::bytes_view{term.options().term}));
  return os << "Term(" << term.field() << "=" << term_value << ")";
}

std::ostream& operator<<(std::ostream& os, const irs::ByNestedFilter& filter) {
  auto& [parent, child, match, _] = filter.options();
  os << "NESTED[MATCH[";
  if (auto* range = std::get_if<irs::Match>(&match); range) {
    os << range->min << ", " << range->max;
  } else if (nullptr != std::get_if<irs::DocIteratorProvider>(&match)) {
    os << "<Predicate>";
  }
  os << "], CHILD[" << *child << "]]";
  return os;
}

std::ostream& operator<<(std::ostream& os, const And& filter) {
  os << "AND[";
  for (auto it = filter.begin(); it != filter.end(); ++it) {
    if (it != filter.begin()) {
      os << " && ";
    }
    os << **it;
  }
  os << "]";
  return os;
}

std::ostream& operator<<(std::ostream& os, const Or& filter) {
  os << "OR";
  if (filter.min_match_count() != 1) {
    os << "(" << filter.min_match_count() << ")";
  }
  os << "[";
  for (auto it = filter.begin(); it != filter.end(); ++it) {
    if (it != filter.begin()) {
      os << " || ";
    }
    os << **it;
  }
  os << "]";
  return os;
}

std::ostream& operator<<(std::ostream& os, const Not& filter) {
  os << "NOT[" << *filter.filter() << "]";
  return os;
}

std::ostream& operator<<(std::ostream& os, const ByNGramSimilarity& filter) {
  os << "NGRAM_SIMILARITY[";
  os << filter.field() << ", ";
  for (const auto& ngram : filter.options().ngrams) {
    os << ngram.c_str();
  }
  os << "," << filter.options().threshold << "]";
  return os;
}

std::ostream& operator<<(std::ostream& os, const Empty&) {
  os << "EMPTY[]";
  return os;
}

std::ostream& operator<<(std::ostream& os, const ByColumnExistence& filter) {
  os << "EXISTS[" << filter.field() << ", " << size_t(filter.options().acceptor)
     << "]";
  return os;
}

std::ostream& operator<<(std::ostream& os, const ByEditDistance& lev) {
  os << "LEVENSHTEIN_MATCH[";
  os << lev.field() << ", '";
  std::string term_value(ViewCast<char>(irs::bytes_view{lev.options().term}));
  std::string prefix_value(
    ViewCast<char>(irs::bytes_view{lev.options().prefix}));
  os << term_value << "', " << static_cast<int>(lev.options().max_distance)
     << ", " << lev.options().with_transpositions << ", "
     << lev.options().max_terms << ", '" << prefix_value << "']";
  return os;
}

std::ostream& operator<<(std::ostream& os, const ByPrefix& filter) {
  os << "STARTS_WITH[";
  os << filter.field() << ", '";
  std::string term_value(
    ViewCast<char>(irs::bytes_view{filter.options().term}));
  os << term_value << "', " << filter.options().scored_terms_limit << "]";
  return os;
}

std::ostream& operator<<(std::ostream& os, const ByTerms& filter) {
  os << "TERMS[";
  os << filter.field() << ", {";
  for (auto& [term, boost] : filter.options().terms) {
    os << "['" << ViewCast<char>(irs::bytes_view{term}) << "', " << boost
       << "],";
  }
  os << "}, " << filter.options().min_match << "]";
  return os;
}

std::ostream& operator<<(std::ostream& os, const All& filter) {
  os << "ALL[";
  os << filter.Boost();
  os << "]";
  return os;
}

std::ostream& operator<<(std::ostream& os, const Filter& filter) {
  const auto& type = filter.type();
  if (type == irs::Type<All>::id()) {
    return os << static_cast<const All&>(filter);
  } else if (type == irs::Type<And>::id()) {
    return os << static_cast<const And&>(filter);
  } else if (type == irs::Type<Or>::id()) {
    return os << static_cast<const Or&>(filter);
  } else if (type == irs::Type<Not>::id()) {
    return os << static_cast<const Not&>(filter);
  } else if (type == irs::Type<ByTerm>::id()) {
    return os << static_cast<const ByTerm&>(filter);
  } else if (type == irs::Type<ByTerms>::id()) {
    return os << static_cast<const ByTerms&>(filter);
  } else if (type == irs::Type<ByRange>::id()) {
    return os << static_cast<const ByRange&>(filter);
  } else if (type == irs::Type<ByGranularRange>::id()) {
    return os << static_cast<const ByGranularRange&>(filter);
  } else if (type == irs::Type<ByNGramSimilarity>::id()) {
    return os << static_cast<const ByNGramSimilarity&>(filter);
  } else if (type == irs::Type<ByEditDistance>::id()) {
    return os << static_cast<const ByEditDistance&>(filter);
  } else if (type == irs::Type<ByPrefix>::id()) {
    return os << static_cast<const ByPrefix&>(filter);
  } else if (type == irs::Type<ByNestedFilter>::id()) {
    return os << static_cast<const ByNestedFilter&>(filter);
  } else if (type == irs::Type<ByColumnExistence>::id()) {
    return os << static_cast<const ByColumnExistence&>(filter);
  } else if (type == irs::Type<Empty>::id()) {
    return os << static_cast<const Empty&>(filter);
  } else {
    return os << "[Unknown filter " << type().name() << " ]";
  }
}

std::string ToString(const irs::Filter& f) {
  std::ostringstream ss;
  ss << f;
  return ss.str();
}

}  // namespace irs
