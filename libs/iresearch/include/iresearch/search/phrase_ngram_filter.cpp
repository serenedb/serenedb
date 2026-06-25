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

#include "iresearch/search/phrase_ngram_filter.hpp"

#include <algorithm>
#include <memory>

#include "iresearch/analysis/shingle_analyzer.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {
namespace {

// Everything the per-candidate verification needs, built once per prepared
// query and shared by every segment's iterator: the ordered query tokens, the
// KMP prefix function over them (filler-free queries only), and the filler
// byte pattern (a query filler matches any document token).
struct VerifySpec {
  std::vector<bstring> tokens;
  std::vector<uint32_t> prefix;  // empty when has_filler
  bstring filler;
  bool has_filler{false};
};

std::shared_ptr<const VerifySpec> MakeVerifySpec(
  const ByPhraseNgramOptions& opts) {
  auto spec = std::make_shared<VerifySpec>();
  spec->tokens = opts.query_tokens;
  spec->filler = opts.filler;
  const bytes_view filler{spec->filler};
  for (const auto& token : spec->tokens) {
    if (bytes_view{token} == filler) {
      spec->has_filler = true;
      break;
    }
  }
  if (!spec->has_filler) {
    const auto& query = spec->tokens;
    const auto m = query.size();
    spec->prefix.resize(m);
    spec->prefix[0] = 0;
    for (size_t i = 1, k = 0; i < m; ++i) {
      while (k > 0 && query[i] != query[k]) {
        k = spec->prefix[k - 1];
      }
      if (query[i] == query[k]) {
        ++k;
      }
      spec->prefix[i] = static_cast<uint32_t>(k);
    }
  }
  return spec;
}

// Post-filter iterator: for each candidate produced by the shingle conjunction,
// fetch the document's stored token stream and confirm the query tokens appear
// as an ordered contiguous run. This removes the false positives that the
// position-free shingle AND admits (e.g. the shingles co-occur but not
// adjacently / in order). Surviving candidates are scored by the underlying
// conjunction (the Exclusion post-filter idiom): every yielded doc is
// positioned on `_approx`, so its score function applies directly.
class PhraseVerifyIterator : public DocIterator {
 public:
  // `tokenizer` non-null switches the column from "packed token stream" to
  // "raw value": each candidate is re-tokenized on the fly, so any column
  // whose raw values are stored can verify without a dedicated blob.
  PhraseVerifyIterator(std::shared_ptr<const VerifySpec> spec,
                       DocIterator::ptr&& approx,
                       const ColumnReader& stored_field,
                       const ColReader& col_reader,
                       std::unique_ptr<analysis::ShingleAnalyzer>&& tokenizer)
    : _spec{std::move(spec)},
      _approx{std::move(approx)},
      _cursor{col_reader, stored_field},
      _tokenizer{std::move(tokenizer)} {
    SDB_ASSERT(_approx);
    SDB_ASSERT(_spec && !_spec->tokens.empty());
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return _approx->GetMutable(type);
  }

  IRS_DOC_ITERATOR_DEFAULTS

  ScoreFunction PrepareScore(const PrepareScoreContext& ctx) final {
    return _approx->PrepareScore(ctx);
  }

  void FetchScoreArgs(uint16_t index) final { _approx->FetchScoreArgs(index); }

  doc_id_t advance() final {
    while (!doc_limits::eof(_approx->advance())) {
      if (Check(_approx->value())) {
        return _doc = _approx->value();
      }
    }
    return _doc = doc_limits::eof();
  }

  doc_id_t seek(doc_id_t target) final {
    target = _approx->seek(target);
    if (doc_limits::eof(target)) {
      return _doc = target;
    }
    if (Check(target)) {
      return _doc = target;
    }
    return advance();
  }

  doc_id_t LazySeek(doc_id_t target) final {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }
    const auto doc = _approx->LazySeek(target);
    if (target != doc) {
      return doc;  // the shingle conjunction lacks target: propagate the miss
    }
    if (doc_limits::eof(doc) || Check(doc)) {
      return _doc = doc;
    }
    return doc + 1;  // candidate failed verification: a miss, value() unchanged
  }

 private:
  bool Check(doc_id_t doc) {
    auto value = _cursor.FetchDoc(doc);
    if (value.empty()) {
      return false;
    }
    if (_tokenizer != nullptr) {
      // Raw stored value: rebuild the token stream with the same analyzer
      // configuration that indexed the document, then match as usual.
      _tokenizer->DrainTokens(ViewCast<char>(value));
      value = _tokenizer->TokenBlob();
      if (value.empty()) {
        return false;
      }
    }
    const auto& query = _spec->tokens;
    const auto m = query.size();
    const auto* p = value.data();
    const auto* const end = p + value.size();
    if (!_spec->has_filler) {
      // Stream the stored token records and match the query phrase as an
      // ordered contiguous run via KMP, without materializing the document's
      // tokens. A corrupt or truncated record rejects the candidate instead
      // of overreading the blob.
      size_t j = 0;  // length of the query prefix matched so far
      while (p < end) {
        bytes_view token;
        p = analysis::ShingleAnalyzer::ReadTokenChecked(p, end, token);
        if (p == nullptr) {
          return false;
        }
        while (j > 0 && token != bytes_view{query[j]}) {
          j = _spec->prefix[j - 1];
        }
        if (token == bytes_view{query[j]} && ++j == m) {
          return true;
        }
      }
      return false;
    }
    // A query filler matches ANY document token (a removed stopword means
    // "any single token here", matching the positional paths), which breaks
    // the KMP prefix invariant -- use a windowed scan instead; query phrases
    // are short.
    _doc_tokens.clear();
    while (p < end) {
      bytes_view token;
      p = analysis::ShingleAnalyzer::ReadTokenChecked(p, end, token);
      if (p == nullptr) {
        return false;
      }
      _doc_tokens.push_back(token);
    }
    if (m > _doc_tokens.size()) {
      return false;
    }
    const bytes_view filler{_spec->filler};
    for (size_t i = 0; i + m <= _doc_tokens.size(); ++i) {
      size_t j = 0;
      for (; j < m; ++j) {
        const bytes_view qt{query[j]};
        if (qt != filler && _doc_tokens[i + j] != qt) {
          break;
        }
      }
      if (j == m) {
        return true;
      }
    }
    return false;
  }

  std::shared_ptr<const VerifySpec> _spec;
  DocIterator::ptr _approx;
  ColumnReader::BlobPointReader _cursor;
  std::unique_ptr<analysis::ShingleAnalyzer> _tokenizer;  // raw-value mode
  std::vector<bytes_view> _doc_tokens;  // scratch for the filler-bearing scan
};

class PhraseVerifyQuery : public QueryBuilder {
 public:
  PhraseVerifyQuery(const SubReader& segment,
                    std::shared_ptr<const VerifySpec> spec,
                    QueryBuilder::ptr&& approx, field_id store_field_id,
                    std::shared_ptr<const analysis::TokenizerConfig> raw_config)
    : QueryBuilder{segment},
      _spec{std::move(spec)},
      _approx{std::move(approx)},
      _raw_config{std::move(raw_config)},
      _store_field_id{store_field_id} {
    SDB_ASSERT(_approx);
  }

  DocIterator::ptr Execute(const ExecutionContext& ctx,
                           const StatsBuffer& stats) const final {
    auto approx = _approx->Execute(ctx, stats);
    if (approx == DocIterator::empty()) {
      return approx;
    }
    SDB_ASSERT(irs::field_limits::valid(_store_field_id));
    const auto* col_reader = _segment.GetColReader();
    if (!col_reader) {
      return DocIterator::empty();
    }
    const auto* column = col_reader->Column(_store_field_id);
    if (column == nullptr) {
      return DocIterator::empty();
    }
    std::unique_ptr<analysis::ShingleAnalyzer> tokenizer;
    if (_raw_config != nullptr) {
      // Analyzers are stateful: one instance per iterator (executions can run
      // concurrently across segments).
      auto built = analysis::CreateAnalyzer(analysis::Clone(*_raw_config));
      if (!built || built->type() != Type<analysis::ShingleAnalyzer>::id()) {
        return DocIterator::empty();
      }
      tokenizer.reset(static_cast<analysis::ShingleAnalyzer*>(built.release()));
    }
    return memory::make_managed<PhraseVerifyIterator>(
      _spec, std::move(approx), *column, *col_reader, std::move(tokenizer));
  }

  // Forward statistics collection + boost to the conjunction so its shingle
  // terms contribute to scoring (the candidate AND is the ranked component).
  void Visit(PreparedStateVisitor& visitor, score_t boost) const final {
    _approx->Visit(visitor, boost);
  }

  score_t Boost() const noexcept final { return _approx->Boost(); }

 private:
  std::shared_ptr<const VerifySpec> _spec;
  QueryBuilder::ptr _approx;
  std::shared_ptr<const analysis::TokenizerConfig> _raw_config;
  field_id _store_field_id;
};

// Strategy B (positional): a phrase over the greedy shingle cover. The position
// offset between consecutive terms is their token-start gap (a ByTerm part's
// delta is gap; push_back adds the implicit +1, hence gap-1), which also
// enforces the seam between non-overlapping shingles.
ByPhrase MakePositionalPhrase(irs::field_id field,
                              const ByPhraseNgramOptions& opts) {
  ByPhrase phrase;
  *phrase.mutable_field_id() = field;
  auto* part = phrase.mutable_options();
  for (size_t i = 0; i < opts.positional_terms.size(); ++i) {
    const size_t offs =
      i == 0 ? 0
             : opts.positional_starts[i] - opts.positional_starts[i - 1] - 1;
    part->push_back<ByTermOptions>(ByTermOptions{opts.positional_terms[i]},
                                   offs);
  }
  return phrase;
}

ByTerm MakeTermFilter(irs::field_id field, bytes_view term) {
  ByTerm by_term;
  *by_term.mutable_field_id() = field;
  by_term.mutable_options()->term = bstring{term};
  return by_term;
}

}  // namespace

PrepareCollector::ptr ByPhraseNgram::MakeCollector(const Scorer* scorer) const {
  const auto& opts = options();
  if (opts.positional) {
    // Strategy B delegates to ByPhrase, which collects its own phrase stats.
    return MakePositionalPhrase(field_id(), opts).MakeCollector(scorer);
  }
  if (opts.exact) {
    // A single shingle/unigram term is the answer; collect that term's stats.
    SDB_ASSERT(!opts.shingles.empty());
    return MakeTermFilter(field_id(), opts.shingles.front())
      .MakeCollector(scorer);
  }
  // Strategy A: the candidate AND is the ranked component. Collect each shingle
  // term's stats; verified candidates score against the conjunction.
  auto compound = std::make_unique<CompoundCollector>(scorer);
  for (const auto& shingle : opts.shingles) {
    compound->Add(MakeTermFilter(field_id(), shingle).MakeCollector(scorer));
  }
  return compound;
}

QueryBuilder::ptr ByPhraseNgram::PrepareSegment(const SubReader& segment,
                                                const PrepareContext& ctx) const {
  const auto& opts = options();
  if (opts.query_tokens.empty()) {
    return QueryBuilder::Empty();
  }
  auto sub_ctx = ctx;
  sub_ctx.Boost(Boost());

  if (opts.positional) {
    // Strategy B (adaptive): the indexed shingle terms carry positions, so a
    // phrase over the greedy cover proves contiguity with no verification.
    return MakePositionalPhrase(field_id(), opts).PrepareSegment(segment,
                                                                 sub_ctx);
  }

  if (opts.shingles.empty()) {
    return QueryBuilder::Empty();
  }

  if (opts.exact) {
    // A single unigram or whole-phrase shingle term is itself the answer: any
    // document containing it contains the exact ordered phrase.
    SDB_ASSERT(opts.shingles.size() == 1);
    return ByTerm::PrepareSegment(segment, sub_ctx, field_id(),
                                  opts.shingles.front());
  }

  // Strategy A: AND the shingle terms (candidate generation), then verify each
  // candidate's contiguity against the stored token stream.
  auto* compound = dynamic_cast<CompoundCollector*>(ctx.collector);
  SDB_ASSERT(ctx.collector == nullptr || compound != nullptr);
  AndQuery::queries_t queries{{ctx.memory}};
  queries.reserve(opts.shingles.size());
  size_t idx = 0;
  for (const auto& shingle : opts.shingles) {
    auto child = sub_ctx;
    child.collector = compound ? &compound->Child(idx++) : nullptr;
    queries.emplace_back(
      ByTerm::PrepareSegment(segment, child, field_id(), shingle));
  }
  auto conjunction =
    memory::make_tracked<AndQuery>(ctx.memory, segment, std::move(queries),
                                   ScoreMergeType::Sum, sub_ctx.boost);

  return memory::make_tracked<PhraseVerifyQuery>(
    ctx.memory, segment, MakeVerifySpec(opts), std::move(conjunction),
    opts.store_field_id, opts.raw_store ? opts.verify_config : nullptr);
}

ByPhraseNgramOptions::ByPhraseNgramOptions(std::string_view phrase,
                                           analysis::ShingleAnalyzer& analyzer) {
  // Build only the StoreAttr blob (the ordered token stream): the candidate
  // terms are derived from the tokens below, so the analyzer's own shingle
  // construction would be wasted work.
  analyzer.DrainTokens(phrase);
  const auto blob = analyzer.TokenBlob();
  if (blob.empty()) {
    return;
  }
  filler = bstring{analyzer.Filler()};
  const auto* p = blob.data();
  const auto* const end = p + blob.size();
  while (p != end) {
    bytes_view token;
    p = analysis::ShingleAnalyzer::ReadToken(p, token);
    query_tokens.emplace_back(token);
  }

  // Leading fillers would anchor the phrase to "follows a removed stopword";
  // PG and the classic path degrade to the bare phrase instead. (Trailing
  // gaps are never recorded.)
  const bytes_view filler_view{filler};
  size_t lead = 0;
  while (lead < query_tokens.size() &&
         bytes_view{query_tokens[lead]} == filler_view) {
    ++lead;
  }
  if (lead != 0) {
    query_tokens.erase(query_tokens.begin(), query_tokens.begin() + lead);
  }

  const auto m = query_tokens.size();
  if (m == 0) {
    return;
  }
  const size_t max = analyzer.MaxShingleSize();
  const size_t min = analyzer.MinShingleSize();
  const auto separator = analyzer.Separator();
  const bool bounded = analyzer.HasFrequentWords();

  std::vector<bytes_view> window;
  auto build = [&](size_t begin, size_t count) {
    window.clear();
    for (size_t i = 0; i < count; ++i) {
      window.emplace_back(query_tokens[begin + i]);
    }
    bstring shingle;
    analysis::ShingleAnalyzer::AppendShingle(window, separator, shingle);
    return shingle;
  };
  auto is_filler = [&](size_t i) {
    return bytes_view{query_tokens[i]} == filler_view;
  };
  // Whether the span [begin, begin+count) exists as an indexed shingle term:
  // never when it crosses a filler (the analyzer does not shingle across a
  // gap); min-size shingles are always dense; under a frequent-words list,
  // wider sizes exist only for windows containing a frequent word -- the same
  // escalation rules the analyzer applies at index time.
  auto indexed = [&](size_t begin, size_t count) {
    for (size_t i = 0; i < count; ++i) {
      if (is_filler(begin + i)) {
        return false;
      }
    }
    if (!bounded || count == min) {
      return true;
    }
    for (size_t i = 0; i < count; ++i) {
      if (analyzer.IsFrequent(query_tokens[begin + i])) {
        return true;
      }
    }
    return false;
  };

  // --- Strategy A: position-free candidate terms (ANDed) + `exact`. ---
  if (m == 1) {
    shingles.push_back(query_tokens.front());  // indexed unigram is exact
    exact = true;
  } else if (m >= min && m <= max && indexed(0, m)) {
    shingles.push_back(build(0, m));  // whole phrase is one indexed shingle
    exact = true;
  } else {
    // Overlapping coverage by the LARGEST indexed shingle at each start (an
    // escalated wide shingle where one exists, the dense min-size one
    // otherwise), then unigrams for tokens no shingle covers. A window whose
    // tokens are all inside one already-pushed window is skipped: its term is
    // entailed (the analyzer emits every indexed sub-window of a run), so the
    // leg adds no selectivity. Fillers contribute no leg: a removed stopword
    // means "any single token here", which only the verifier can express.
    // Verify contiguity.
    std::vector<bool> implied(m, false);
    for (size_t begin = 0; begin + min <= m; ++begin) {
      const size_t cap = std::min(max, m - begin);
      for (size_t s = cap; s >= min; --s) {
        if (!indexed(begin, s)) {
          continue;
        }
        bool subsumed = true;
        for (size_t i = begin; i < begin + s; ++i) {
          subsumed = subsumed && implied[i];
        }
        if (!subsumed) {
          shingles.push_back(build(begin, s));
          std::fill_n(implied.begin() + begin, s, true);
        }
        break;
      }
    }
    for (size_t i = 0; i < m; ++i) {
      if (!implied[i] && !is_filler(i)) {
        shingles.push_back(query_tokens[i]);
      }
    }
    exact = false;
  }

  if (!exact) {
    // Identical legs add nothing to a conjunction and would double-score
    // (repeated words produce repeated shingle/unigram terms).
    size_t w = 0;
    for (size_t r = 0; r < shingles.size(); ++r) {
      bool dup = false;
      for (size_t k = 0; k < w; ++k) {
        if (shingles[k] == shingles[r]) {
          dup = true;
          break;
        }
      }
      if (!dup) {
        if (w != r) {
          shingles[w] = std::move(shingles[r]);
        }
        ++w;
      }
    }
    shingles.resize(w);
  }

  // --- Strategy B: greedy positional cover. At each step use the largest
  // indexed shingle starting here. A tail shorter than min_shingle_size is
  // covered by an overlapping shingle ending at the phrase end -- shingle
  // posting lists are far smaller than unigrams' -- with a unigram fallback
  // only when no such shingle is indexed. A filler slot contributes no term:
  // the start gap to the next term leaves its position unconstrained. ---
  for (size_t i = 0; i < m;) {
    size_t start = i;
    size_t span = 0;
    for (size_t cand = std::min(max, m - i); cand >= min; --cand) {
      if (indexed(i, cand)) {
        span = cand;
        break;
      }
    }
    if (span == 0 && m - i < min && !positional_starts.empty()) {
      // Any shingle ending at m with size >= min starts before i (the tail
      // is shorter than min), so it covers the tail; cap the size so its
      // start stays after the previous term's (offsets are forward-only).
      const size_t prev = positional_starts.back();
      for (size_t cand = std::min(max, m - prev - 1); cand >= min; --cand) {
        if (indexed(m - cand, cand)) {
          span = cand;
          start = m - cand;
          break;
        }
      }
    }
    if (span == 0) {
      if (!is_filler(i)) {
        positional_starts.push_back(static_cast<uint32_t>(i));
        positional_terms.push_back(query_tokens[i]);  // unigram fallback
      }
      ++i;
    } else {
      positional_starts.push_back(static_cast<uint32_t>(start));
      positional_terms.push_back(build(start, span));
      i = start + span;
    }
  }
  SDB_ASSERT(!positional_terms.empty());
}

}  // namespace irs
