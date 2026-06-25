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

#include <re2/re2.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "iresearch/search/filter.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {
namespace analysis {

class WildcardAnalyzer;

}  // namespace analysis

class ByRegexpNgram;

// Regex search accelerated by a character n-gram index (Google Code Search /
// pg_trgm model). The char-ngram index is a *necessary-but-not-sufficient*
// prefilter: RE2's Prefilter decomposes the regex into a boolean AND/OR tree of
// required literal atoms, each atom maps to the set of char n-grams it must
// contain, and intersecting/uniting those n-gram postings yields candidate
// documents. Every candidate is then verified by running the full regex (RE2)
// against the document's stored original tokens, so the result is exact
// (expected false positives from the prefilter, never false negatives).
//
// Reuses `WildcardAnalyzer` wholesale: it already indexes char n-grams and
// stores the original token stream in `StoreAttr` -> a columnstore BLOB column
// (`store_field_id`), which the verifier reads.
//
// Correctness note: RE2's Prefilter lower-cases its atoms, so the n-gram column
// must be case-folded (a lower-casing analyzer base) for the prefilter to be
// free of false negatives -- the standard text-search configuration. The
// per-document RE2 verification remains case-sensitive per the regex.
struct ByRegexpNgramOptions {
  using FilterType = ByRegexpNgram;

  // Precomputed candidate prefilter: required n-grams structured by the regex's
  // AND/OR atom decomposition. Translated to AndQuery/OrQuery/All at Prepare
  // time (the per-segment term lookups happen there).
  struct Node {
    enum class Kind : uint8_t {
      kAll,    // no usable constraint -> matches every document (scan + verify)
      kNone,   // unsatisfiable -> matches nothing
      kAnd,    // all `subs` must match
      kOr,     // at least one of `subs` must match
      kTerms,  // every n-gram in `terms` must be present (one required atom)
    };

    Kind kind{Kind::kAll};
    std::vector<bstring> terms;  // kTerms
    std::vector<Node> subs;      // kAnd / kOr

    bool operator==(const Node&) const = default;
  };

  Node root;                     // candidate prefilter tree
  std::shared_ptr<RE2> matcher;  // the full regex, applied per surviving doc
  field_id store_field_id{0};

  bool operator==(const ByRegexpNgramOptions& rhs) const noexcept {
    if (root != rhs.root || store_field_id != rhs.store_field_id) {
      return false;
    }
    if (!matcher || !rhs.matcher) {
      return !matcher && !rhs.matcher;
    }
    return matcher->pattern() == rhs.matcher->pattern();
  }

  ByRegexpNgramOptions() = default;
  ByRegexpNgramOptions(const ByRegexpNgramOptions&) = default;
  ByRegexpNgramOptions(ByRegexpNgramOptions&&) noexcept = default;
  ByRegexpNgramOptions& operator=(const ByRegexpNgramOptions&) = default;
  ByRegexpNgramOptions& operator=(ByRegexpNgramOptions&&) noexcept = default;

  // Compile `pattern` (RE2) and decompose it into the candidate n-gram tree by
  // running RE2's Prefilter and tokenizing each atom with `analyzer` -- the
  // same char-ngram code path that indexed the documents. `posix_syntax`
  // selects POSIX ERE instead of the default Perl flavour.
  ByRegexpNgramOptions(std::string_view pattern,
                       analysis::WildcardAnalyzer& analyzer,
                       bool posix_syntax = false);
};

class ByRegexpNgram final : public FilterWithField<ByRegexpNgramOptions> {
 public:
  // Unscored (constant boost): the inherited Filter::MakeCollector default
  // (collects nothing) is correct, so only PrepareSegment is overridden.
  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;
};

}  // namespace irs
