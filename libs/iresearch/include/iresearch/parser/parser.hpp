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

#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/token_sinks.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/ngram_similarity_filter.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/range_filter.hpp"
#include "iresearch/search/regexp_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/wildcard_filter.hpp"
#include "iresearch/utils/type_id.hpp"
#include "iresearch/utils/utf8_utils.hpp"
#include "iresearch/utils/wildcard_utils.hpp"
#include "pg/sql_exception_macro.h"

namespace sdb {

enum class Conjunction {
  Or,
  And,
};
enum class Modifier {
  None,
  Required,
  Not,
};

struct ParserContext {
  irs::field_id default_field_id{irs::field_limits::invalid()};
  std::string_view default_field_name;
  irs::BooleanFilter* current_root;
  irs::analysis::Tokenizer* tokenizer;
  irs::ValueAnalyzer value_analyzer;
  irs::ValueTokens<> value_tokens;
  std::string error_message;
  Modifier last_mod{Modifier::None};
  bool strict_field = false;

  irs::ByPhrase* phrase{nullptr};
  irs::ByNGramSimilarity* ngram{nullptr};
  std::vector<std::string_view> fn_terms;
  std::string_view fn_other;
  size_t offs_min{0};
  size_t offs_max{0};

  ParserContext(irs::BooleanFilter& root, irs::field_id field_id,
                irs::analysis::Tokenizer& tokenizer)
    : default_field_id(field_id), current_root{&root}, tokenizer{&tokenizer} {}

  // A clause is held back until the connector after it has been read, which
  // is what decides where it goes: `AND` makes both of the clauses it joins
  // required, and a clause reads as optional only if nothing said otherwise.
  // With three buckets that is the whole of it -- nothing is placed and then
  // moved.
  void AddClause(Conjunction conj) {
    Place(conj == Conjunction::And);
    _pending = std::move(_built);
    _pending_mod = last_mod;
    _pending_and = conj == Conjunction::And;
  }

  // The last clause of a list has no connector after it, so it is placed on
  // what came before it alone, and the node is closed.
  void EndClauseList() {
    Place(false);
    // With nothing required, the optional clauses are the query and one of
    // them must match; beside a required clause they only score. An excluded
    // clause requires nothing, so it does not turn the optional side into the
    // scoring one -- `a b -c` is still "a or b", less c.
    if (current_root->Size(irs::Occur::Should) != 0 &&
        current_root->Size(irs::Occur::Must) == 0) {
      current_root->SetMinShouldMatch(1);
    }
  }

  irs::Filter& AddTerm(std::string_view value) {
    const auto text = Unescape(value);
    const auto tokens = Tokens(text);
    if (tokens.size() > 1) {
      auto& several = Build<irs::BooleanFilter>();
      for (const auto& token : tokens) {
        AddTermTo(several, irs::Occur::Should, irs::AsBytesView(token));
      }
      SetThreshold(several, 1);
      return several;
    }
    auto& f = Build<irs::ByTerm>();
    *f.mutable_field_id() = default_field_id;
    f.mutable_options()->term =
      tokens.empty() ? irs::bytes_view{text} : irs::AsBytesView(tokens.front());
    return f;
  }

  irs::ByRegexp& AddRegex(std::string_view value) {
    auto& f = Build<irs::ByRegexp>();
    *f.mutable_field_id() = default_field_id;
    f.mutable_options()->pattern = irs::ViewCast<irs::byte_type>(value);
    return f;
  }

  // A group is built apart from its parent, because which bucket it belongs
  // to is only known once the modifier before it has been read. The clause
  // the enclosing list was holding back waits with it.
  irs::BooleanFilter& BeginGroup() {
    _open.emplace_back(std::move(_pending), _pending_mod, _pending_and,
                       std::make_unique<irs::BooleanFilter>());
    _pending = {};
    _pending_mod = Modifier::None;
    _pending_and = false;
    return *_open.back().node;
  }

  irs::Filter& EndGroup(irs::Filter* parent_root) {
    EndClauseList();
    auto& open = _open.back();
    _pending = std::move(open.pending);
    _pending_mod = open.pending_mod;
    _pending_and = open.pending_and;
    auto& node = *open.node;
    _built = std::move(open.node);
    _open.pop_back();
    current_root = &sdb::basics::downCast<irs::BooleanFilter>(*parent_root);
    return node;
  }

  // A named field is read where the default field is: there is nothing here
  // that maps a name to a field of the index, so the name is only checked.
  bool CheckField(std::string_view name) {
    if (strict_field && name != default_field_name) {
      error_message =
        "field-prefix in strict-field mode must match the default field";
      return false;
    }
    return true;
  }

  void BeginPhrase() {
    auto& f = Build<irs::ByPhrase>();
    *f.mutable_field_id() = default_field_id;
    phrase = &f;
    offs_min = 0;
    offs_max = 0;
  }

  irs::ByPhrase& EndPhrase() {
    SDB_ASSERT(phrase != nullptr);
    auto& f = *phrase;
    phrase = nullptr;
    return f;
  }

  void SetGap(int min, int max) {
    offs_min = static_cast<size_t>(min);
    offs_max = static_cast<size_t>(max);
  }

  void SetSlop(irs::Filter* f, int slop) {
    sdb::basics::downCast<irs::ByPhrase>(*f).mutable_options()->set_slop(
      static_cast<irs::PosAttr::value_t>(slop));
  }

  // `(...)@k`: how many of the group's optional clauses a document needs.
  // Where the group requires nothing else, that is the group's own threshold.
  // Beside a required clause it is still a threshold -- Lucene's
  // `setMinimumNumberShouldMatch` says "if this method is used, then the
  // specified number of clauses is required" -- so the optional side moves to
  // a node of its own that the group then requires.
  void SetMinMatch(irs::Filter* f, int count) {
    auto& node = sdb::basics::downCast<irs::BooleanFilter>(*f);
    const auto k = static_cast<size_t>(count);
    // An excluded clause requires nothing, so it does not stop the group's
    // optional side from being the group -- exactly as in `EndClauseList`.
    if (node.Size(irs::Occur::Must) == 0) {
      SetThreshold(node, k);
      return;
    }
    auto nested = std::make_unique<irs::BooleanFilter>();
    nested->Bucket(irs::Occur::Should) =
      std::exchange(node.Bucket(irs::Occur::Should), {});
    SetThreshold(*nested, k);
    node.SetMinShouldMatch(0);
    node.Add(std::move(nested), irs::Occur::Must);
  }

  void AddPhraseTerm(std::string_view word) {
    const auto text = Unescape(word);
    for (const auto& token : Tokens(text)) {
      Emplace<irs::ByTermOptions>().term = irs::AsBytesView(token);
    }
  }

  void AddPhrasePrefix(std::string_view word) {
    word.remove_suffix(1);
    Emplace<irs::ByPrefixOptions>().term = Analyze(word);
  }

  void AddPhraseWildcard(std::string_view word) {
    const auto wildcard = FindPattern(word);
    auto pattern = Analyze(word.substr(0, wildcard));
    pattern.append(Pattern(word.substr(wildcard)));
    Emplace<irs::ByWildcardOptions>().term = std::move(pattern);
  }

  static size_t FindPattern(std::string_view word) noexcept {
    for (size_t i = 0; i != word.size(); ++i) {
      if (word[i] == '\\') {
        ++i;
      } else if (word[i] == '*' || word[i] == '?') {
        return i;
      }
    }
    return word.size();
  }

  void AddPhraseFuzzy(std::string_view word, int distance) {
    auto& part = Emplace<irs::ByEditDistanceOptions>();
    part.term = Analyze(word);
    part.max_distance = static_cast<uint8_t>(distance);
    part.with_transpositions = true;
  }

  void BeginNGram(float threshold) {
    auto& f = Build<irs::ByNGramSimilarity>();
    *f.mutable_field_id() = default_field_id;
    f.mutable_options()->threshold = threshold;
    ngram = &f;
  }

  void AddNGram(std::string_view word) {
    SDB_ASSERT(ngram != nullptr);
    ngram->mutable_options()->ngrams.emplace_back(Analyze(word));
  }

  irs::ByNGramSimilarity& EndNGram() {
    SDB_ASSERT(ngram != nullptr);
    auto& f = *ngram;
    ngram = nullptr;
    return f;
  }

  irs::ByPrefix& AddPrefix(std::string_view value) {
    auto& f = Build<irs::ByPrefix>();
    *f.mutable_field_id() = default_field_id;
    SDB_ASSERT(!value.empty() && value.back() == '*');
    value.remove_suffix(1);
    f.mutable_options()->term = Normalize(value);
    return f;
  }

  irs::ByWildcard& AddWildcard(std::string_view value) {
    auto& f = Build<irs::ByWildcard>();
    *f.mutable_field_id() = default_field_id;
    const auto wildcard = FindPattern(value);
    auto pattern = Normalize(value.substr(0, wildcard));
    pattern.append(Pattern(value.substr(wildcard)));
    *f.mutable_options() = irs::ByWildcardOptions{std::move(pattern)};
    return f;
  }

  irs::ByRange& AddRange(std::string_view min_val, std::string_view max_val,
                         bool inc_min, bool inc_max) {
    auto& f = Build<irs::ByRange>();
    *f.mutable_field_id() = default_field_id;
    auto& range = f.mutable_options()->range;
    if (min_val == "*") {
      range.min_type = irs::BoundType::Unbounded;
    } else {
      range.min = Normalize(min_val);
      range.min_type =
        inc_min ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
    }
    if (max_val == "*") {
      range.max_type = irs::BoundType::Unbounded;
    } else {
      range.max = Normalize(max_val);
      range.max_type =
        inc_max ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
    }
    return f;
  }

  static irs::bstring Pattern(std::string_view word) {
    irs::bstring out;
    out.reserve(word.size());
    for (size_t i = 0; i != word.size(); ++i) {
      const auto c = word[i];
      switch (c) {
        case '\\':
          if (i + 1 != word.size()) {
            const auto next = word[++i];
            if (next != '*' && next != '?') {
              out += static_cast<irs::byte_type>('\\');
            }
            out += static_cast<irs::byte_type>(next);
          }
          break;
        case '*':
          out += static_cast<irs::byte_type>(irs::WildcardMatch::kAnyStr);
          break;
        case '?':
          out += static_cast<irs::byte_type>(irs::WildcardMatch::kAnyChr);
          break;
        case irs::WildcardMatch::kAnyStr:
        case irs::WildcardMatch::kAnyChr:
          out += static_cast<irs::byte_type>(irs::WildcardMatch::kEscape);
          out += static_cast<irs::byte_type>(c);
          break;
        default:
          out += static_cast<irs::byte_type>(c);
      }
    }
    return out;
  }

  static constexpr size_t kAnyGap = 1U << 24U;

  // The clause the grammar has just finished. It stays here until a modifier
  // and a connector have said which bucket it belongs in, and a rule that
  // reads a suffix -- a boost, a fuzziness, `@k` -- still reaches it.
  template<typename Filter>
  Filter& Build() {
    auto filter = std::make_unique<Filter>();
    auto& ref = *filter;
    _built = std::move(filter);
    return ref;
  }

  // How many of a node's `Should` bucket a document needs. A threshold larger
  // than the bucket can never be reached, which the model spells as an
  // unsatisfiable required clause rather than as a smaller threshold.
  static void SetThreshold(irs::BooleanFilter& node, size_t count) {
    const auto size = node.Size(irs::Occur::Should);
    if (count > size) {
      node.Add(std::make_unique<irs::Empty>(), irs::Occur::Must);
      count = size;
    }
    // A threshold of none demands nothing of the optional bucket, so what the
    // node matches is what its required side says -- and it has none. The
    // include side is supplied here, exclusions or not: an exclusion narrows
    // what is included, it does not decide that nothing is.
    if (count == 0 && node.Size(irs::Occur::Must) == 0) {
      node.Bucket(irs::Occur::Should) = {};
      node.Add(std::make_unique<irs::All>(), irs::Occur::Must);
    }
    node.SetMinShouldMatch(static_cast<uint32_t>(count));
  }

  static irs::Occur BucketOf(Modifier mod, bool required) noexcept {
    switch (mod) {
      case Modifier::Required:
        return irs::Occur::Must;
      case Modifier::Not:
        return irs::Occur::MustNot;
      case Modifier::None:
        break;
    }
    return required ? irs::Occur::Must : irs::Occur::Should;
  }

  // `AND` binds both of the clauses it joins, so the one held back is
  // required if either the connector before it or the one after it was `AND`.
  void Place(bool next_is_and) {
    if (_pending) {
      current_root->Add(std::move(_pending),
                        BucketOf(_pending_mod, _pending_and || next_is_and));
    }
  }

  struct OpenGroup {
    irs::Filter::ptr pending;
    Modifier pending_mod;
    bool pending_and;
    std::unique_ptr<irs::BooleanFilter> node;
  };

  irs::Filter::ptr _built;
  irs::Filter::ptr _pending;
  Modifier _pending_mod{Modifier::None};
  bool _pending_and = false;
  std::vector<OpenGroup> _open;

  void AddTermTo(irs::BooleanFilter& node, irs::Occur occur,
                 std::string_view word) {
    AddTermTo(node, occur, Analyze(word));
  }

  void AddTermTo(irs::BooleanFilter& node, irs::Occur occur,
                 irs::bytes_view word) {
    node.Add(
      irs::TermClause{.field = default_field_id, .term = irs::bstring{word}},
      occur);
  }

  void RequirePair(std::string_view name) {
    if (fn_terms.size() != 2) {
      THROW_SQL_ERROR(ERR_MSG("`", name,
                              "` is supported over two terms, where what it "
                              "bounds is a distance rather than a total"));
    }
  }

  irs::ByPhrase& FnPhrase(size_t offs_min, size_t offs_max) {
    BeginPhrase();
    bool first = true;
    for (const auto term : fn_terms) {
      if (!first) {
        SetGap(static_cast<int>(offs_min), static_cast<int>(offs_max));
      }
      first = false;
      AddPhraseTerm(term);
    }
    return EndPhrase();
  }

  static uint32_t Hex(char c) noexcept {
    if (c >= '0' && c <= '9') {
      return static_cast<uint32_t>(c - '0');
    }
    if (c >= 'a' && c <= 'f') {
      return static_cast<uint32_t>(c - 'a') + 10;
    }
    if (c >= 'A' && c <= 'F') {
      return static_cast<uint32_t>(c - 'A') + 10;
    }
    return 16;
  }

  static irs::bstring Unescape(std::string_view word) {
    irs::bstring out;
    out.reserve(word.size());
    for (size_t i = 0; i != word.size(); ++i) {
      if (word[i] != '\\' || i + 1 == word.size()) {
        out += static_cast<irs::byte_type>(word[i]);
        continue;
      }
      const auto next = word[++i];
      if (next != 'u' || i + 4 >= word.size()) {
        out += static_cast<irs::byte_type>(next);
        continue;
      }
      uint32_t code = 0;
      size_t digits = 0;
      for (; digits != 4; ++digits) {
        const auto digit = Hex(word[i + 1 + digits]);
        if (digit > 15) {
          break;
        }
        code = code * 16 + digit;
      }
      if (digits != 4) {
        out += static_cast<irs::byte_type>(next);
        continue;
      }
      i += 4;
      irs::byte_type utf8[irs::utf8_utils::kMaxCharSize];
      out.append(utf8, irs::utf8_utils::FromChar32(code, utf8));
    }
    return out;
  }

  std::span<const duckdb::string_t> Tokens(const irs::bstring& text) {
    value_analyzer.Analyze(
      *tokenizer,
      duckdb::string_t{reinterpret_cast<const char*>(text.data()),
                       static_cast<uint32_t>(text.size())},
      value_tokens);
    return value_tokens.terms();
  }

  irs::bstring Analyze(std::string_view word) {
    const auto text = Unescape(word);
    const auto tokens = Tokens(text);
    return tokens.empty() ? irs::bstring{}
                          : irs::bstring{irs::AsBytesView(tokens.front())};
  }

  irs::bstring Normalize(std::string_view word) {
    auto text = Unescape(word);
    const auto tokens = Tokens(text);
    return tokens.size() == 1 ? irs::bstring{irs::AsBytesView(tokens.front())}
                              : text;
  }

  template<typename Part>
  Part& Emplace() {
    SDB_ASSERT(phrase != nullptr);
    auto* options = phrase->mutable_options();
    auto& part = offs_min == 0 && offs_max == 0
                   ? options->push_back<Part>()
                   : options->push_back<Part>(offs_min, offs_max);
    offs_min = 0;
    offs_max = 0;
    return part;
  }

  // Lucene's older spelling of an edit distance: a similarity between zero
  // and one, which it turns into edits by the length of the term.
  // `FuzzyQuery.floatToEdits`.
  // A term or a phrase may give a boost and a fuzziness in either order, and
  // both orders mean the same thing. Reading the boost first leaves a plain
  // term already built, and a fuzzy term is a different filter -- so the term
  // is asked for again as the one it should have been, replacing the clause
  // still being held. A phrase only needs its slop set.
  irs::Filter& ApplyFuzzy(irs::Filter* built, bool has_value, float value) {
    if (built->type() == irs::Type<irs::ByPhrase>::id()) {
      if (has_value) {
        SetSlop(built, static_cast<int>(value));
      }
      return *built;
    }
    if (built->type() != irs::Type<irs::ByTerm>::id()) {
      THROW_SQL_ERROR(
        ERR_MSG("a fuzziness applies to a term or a phrase, not to this"));
    }
    auto& term = sdb::basics::downCast<irs::ByTerm>(*built);
    auto text = term.options().term;
    const auto boost = term.GetBoost();

    auto& f = has_value ? FuzzyFromSimilarity(std::move(text), value)
                        : AddFuzzyTerm(std::move(text), 2);
    f.SetBoost(boost);
    return f;
  }

  irs::ByEditDistance& AddFuzzySimilarity(std::string_view value,
                                          float similarity) {
    return FuzzyFromSimilarity(Normalize(value), similarity);
  }

  irs::ByEditDistance& FuzzyFromSimilarity(irs::bstring value,
                                           float similarity) {
    constexpr int kMaxEdits = 2;
    int distance = 0;
    if (similarity >= 1.F) {
      distance = std::min(static_cast<int>(similarity), kMaxEdits);
    } else if (similarity > 0.F) {
      const auto length = static_cast<int>(irs::utf8_utils::Length(value));
      distance = std::min(
        static_cast<int>((1.F - similarity) * static_cast<float>(length)),
        kMaxEdits);
    }
    return AddFuzzyTerm(std::move(value), distance);
  }

  irs::ByEditDistance& AddFuzzyTerm(irs::bstring value, int distance) {
    auto& f = Build<irs::ByEditDistance>();
    *f.mutable_field_id() = default_field_id;
    f.mutable_options()->term = std::move(value);
    f.mutable_options()->max_distance = static_cast<uint8_t>(distance);
    f.mutable_options()->with_transpositions = true;
    return f;
  }

  // Every document, which is what a query that names no term at all means.
  irs::All& AddAll() { return Build<irs::All>(); }

  irs::Filter& AddFieldExists() {
    THROW_SQL_ERROR(
      ERR_MSG("field existence queries (`field:*`) are not supported"));
  }

  void BeginFn() {
    fn_terms.clear();
    fn_other = {};
  }

  void AddFnTerm(std::string_view word) { fn_terms.emplace_back(word); }

  void AddFnOther(std::string_view kind) { fn_other = kind; }

  void RequireTerms(std::string_view name) const {
    if (!fn_other.empty()) {
      THROW_SQL_ERROR(ERR_MSG("`", name, "` over ", fn_other,
                              " is not supported: it would ask where the "
                              "matches lie, and only terms can be answered "
                              "for here"));
    }
  }

  irs::Filter& EndFnAny() {
    RequireTerms("fn:or");
    auto& f = Build<irs::BooleanFilter>();
    for (const auto term : fn_terms) {
      AddTermTo(f, irs::Occur::Should, term);
    }
    SetThreshold(f, 1);
    return f;
  }

  irs::Filter& EndFnAll() {
    RequireTerms("fn:unordered");
    auto& f = Build<irs::BooleanFilter>();
    for (const auto term : fn_terms) {
      AddTermTo(f, irs::Occur::Must, term);
    }
    return f;
  }

  irs::Filter& EndFnAtLeast(int count) {
    RequireTerms("fn:atLeast");
    auto& f = Build<irs::BooleanFilter>();
    for (const auto term : fn_terms) {
      AddTermTo(f, irs::Occur::Should, term);
    }
    SetThreshold(f, static_cast<size_t>(count));
    return f;
  }

  irs::Filter& EndFnOrdered() {
    RequireTerms("fn:ordered");
    return FnPhrase(1, kAnyGap);
  }

  irs::Filter& EndFnMaxGaps(int gaps) {
    RequireTerms("fn:maxgaps");
    RequirePair("fn:maxgaps");
    return FnPhrase(1, static_cast<size_t>(gaps) + 1);
  }

  irs::Filter& EndFnMaxWidth(int width) {
    RequireTerms("fn:maxwidth");
    RequirePair("fn:maxwidth");
    if (width < 2) {
      THROW_SQL_ERROR(ERR_MSG("`fn:maxwidth` needs a width of at least two"));
    }
    return FnPhrase(1, static_cast<size_t>(width) - 1);
  }

  void Unsupported(std::string_view name) {
    THROW_SQL_ERROR(ERR_MSG("`", name, "` is not supported"));
  }

  irs::ByEditDistance& AddFuzzy(std::string_view value, int distance) {
    return AddFuzzyTerm(Normalize(value), distance);
  }
};

bool ParseQuery(ParserContext& ctx, std::string_view input);

}  // namespace sdb
