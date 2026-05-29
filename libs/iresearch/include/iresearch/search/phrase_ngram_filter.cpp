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

#include <memory>

#include "iresearch/analysis/shingle_analyzer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/columnstore/column_reader.hpp"
#include "iresearch/columnstore/format.hpp"
#include "iresearch/columnstore/read_context.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/term_filter.hpp"

namespace irs {
namespace {

using QueryTokens = std::shared_ptr<const std::vector<bstring>>;

// A shingle field indexed with positions answers a phrase over its shingle
// terms exactly -- ByPhrase proves contiguity -- so per-candidate verification
// is unnecessary (Strategy B). Detect this from the indexed field itself rather
// than a caller-supplied flag, so the strategy always matches what is on disk.
bool FieldHasPositions(const IndexReader& index, std::string_view field) {
  for (const auto& segment : index) {
    if (const auto* reader = segment.field(field); reader != nullptr) {
      return (reader->meta().index_features & IndexFeatures::Pos) ==
             IndexFeatures::Pos;
    }
  }
  return false;
}

// Post-filter iterator: for each candidate produced by the shingle conjunction,
// fetch the document's stored token stream and confirm the query tokens appear
// as an ordered contiguous run. This removes the false positives that the
// position-free shingle AND admits (e.g. the shingles co-occur but not
// adjacently / in order).
class PhraseVerifyIterator : public DocIterator {
 public:
  PhraseVerifyIterator(QueryTokens tokens, DocIterator::ptr&& approx,
                       const columnstore::ColumnReader& stored_field,
                       const columnstore::Reader& cs_reader)
    : _tokens{std::move(tokens)},
      _approx{std::move(approx)},
      _cursor{cs_reader, stored_field} {
    SDB_ASSERT(_approx);
    SDB_ASSERT(_tokens && !_tokens->empty());
    // Precompute the KMP prefix function over the query tokens so each
    // candidate's stored token stream can be matched in a single streaming
    // pass with no per-document allocation (see Check).
    const auto& query = *_tokens;
    const auto m = query.size();
    _prefix.resize(m);
    _prefix[0] = 0;
    for (size_t i = 1, k = 0; i < m; ++i) {
      while (k > 0 && query[i] != query[k]) {
        k = _prefix[k - 1];
      }
      if (query[i] == query[k]) {
        ++k;
      }
      _prefix[i] = k;
    }
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return _approx->GetMutable(type);
  }

  // Rank surviving candidates by the shingle conjunction's (Sum-merged) score:
  // every doc this iterator yields is positioned on `_approx`, so its score
  // function applies directly. Mirrors the Exclusion post-filter iterator.
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
    const auto value = _cursor.FetchDoc(doc);
    if (value.empty()) {
      return false;
    }
    const auto& query = *_tokens;
    const auto m = query.size();
    // Stream the stored token records and match the query phrase as an ordered
    // contiguous run via KMP, without materializing the document's tokens.
    const auto* p = value.data();
    const auto* const end = p + value.size();
    size_t j = 0;  // length of the query prefix matched so far
    while (p != end) {
      bytes_view token;
      p = analysis::ShingleAnalyzer::ReadToken(p, token);
      while (j > 0 && token != bytes_view{query[j]}) {
        j = _prefix[j - 1];
      }
      if (token == bytes_view{query[j]} && ++j == m) {
        return true;
      }
    }
    return false;
  }

  QueryTokens _tokens;
  DocIterator::ptr _approx;
  columnstore::ColumnReader::BlobPointReader _cursor;
  std::vector<size_t> _prefix;  // KMP prefix function over *_tokens
};

class PhraseVerifyQuery : public Filter::Query {
 public:
  PhraseVerifyQuery(QueryTokens tokens, Query::ptr&& approx,
                    field_id store_field_id)
    : _tokens{std::move(tokens)},
      _approx{std::move(approx)},
      _store_field_id{store_field_id} {
    SDB_ASSERT(_approx);
  }

  DocIterator::ptr execute(const ExecutionContext& ctx) const final {
    auto approx = _approx->execute(ctx);
    if (approx == DocIterator::empty()) {
      return approx;
    }
    SDB_ASSERT(_store_field_id != 0);
    const auto* cs_reader = ctx.segment.CsReader();
    if (!cs_reader) {
      return DocIterator::empty();
    }
    const auto* column = cs_reader->Column(_store_field_id);
    if (column == nullptr) {
      return DocIterator::empty();
    }
    return memory::make_managed<PhraseVerifyIterator>(_tokens, std::move(approx),
                                                      *column, *cs_reader);
  }

  // Forward statistics collection + boost to the conjunction so its shingle
  // terms contribute to scoring (the candidate AND is the ranked component).
  void visit(const SubReader& segment, PreparedStateVisitor& visitor,
             score_t boost) const final {
    _approx->visit(segment, visitor, boost);
  }

  score_t Boost() const noexcept final { return _approx->Boost(); }

 private:
  QueryTokens _tokens;
  Query::ptr _approx;
  field_id _store_field_id;
};

}  // namespace

Filter::Query::ptr ByPhraseNgram::Prepare(const PrepareContext& ctx,
                                          std::string_view field,
                                          const ByPhraseNgramOptions& opts) {
  if (opts.query_tokens.empty() || opts.shingles.empty()) {
    return Filter::Query::empty();
  }

  if (FieldHasPositions(ctx.index, field)) {
    // Strategy B (adaptive): the indexed shingle terms carry positions, so a
    // phrase over them proves contiguity with no per-candidate verification.
    ByPhraseOptions phrase;
    for (const auto& shingle : opts.shingles) {
      phrase.push_back<ByTermOptions>(ByTermOptions{shingle});
    }
    return ByPhrase::Prepare(ctx, field, phrase);
  }

  if (opts.exact) {
    // A single unigram or whole-phrase shingle term is itself the answer: any
    // document containing it contains the exact ordered phrase.
    SDB_ASSERT(opts.shingles.size() == 1);
    return ByTerm::prepare(ctx, field, opts.shingles.front());
  }

  AndQuery::queries_t queries{{ctx.memory}};
  queries.reserve(opts.shingles.size());
  for (const auto& shingle : opts.shingles) {
    auto query = ByTerm::prepare(ctx, field, shingle);
    if (query == Filter::Query::empty()) {
      return query;  // a required shingle is absent -> no document can match
    }
    queries.push_back(std::move(query));
  }
  const auto size = queries.size();
  auto conjunction = memory::make_tracked<AndQuery>(ctx.memory);
  conjunction->prepare(ctx, ScoreMergeType::Sum, std::move(queries), size);

  auto tokens = std::make_shared<std::vector<bstring>>(opts.query_tokens);
  return memory::make_tracked<PhraseVerifyQuery>(
    ctx.memory, std::move(tokens), std::move(conjunction), opts.store_field_id);
}

ByPhraseNgramOptions::ByPhraseNgramOptions(std::string_view phrase,
                                           analysis::ShingleAnalyzer& analyzer) {
  if (!analyzer.reset(phrase)) {
    return;
  }
  // Drain the stream so the StoreAttr blob (ordered tokens) is fully built;
  // the emitted shingle terms are rebuilt below from the tokens instead.
  while (analyzer.next()) {
  }
  const auto* store = irs::get<StoreAttr>(analyzer);
  if (store == nullptr || store->value.empty()) {
    return;
  }
  const auto* p = store->value.data();
  const auto* const end = p + store->value.size();
  while (p != end) {
    bytes_view token;
    p = analysis::ShingleAnalyzer::ReadToken(p, token);
    query_tokens.emplace_back(token);
  }

  const auto m = query_tokens.size();
  if (m == 0) {
    return;
  }
  const auto max = analyzer.MaxShingleSize();
  const auto min = analyzer.MinShingleSize();
  const auto separator = analyzer.Separator();

  std::vector<bytes_view> window;
  auto make_shingle = [&](size_t begin, size_t count) {
    window.clear();
    for (size_t i = 0; i < count; ++i) {
      window.emplace_back(query_tokens[begin + i]);
    }
    bstring shingle;
    analysis::ShingleAnalyzer::AppendShingle(window, separator, shingle);
    shingles.push_back(std::move(shingle));
  };

  if (m == 1) {
    // Single token -> the indexed unigram is the exact answer.
    shingles.push_back(query_tokens.front());
    exact = true;
  } else if (m <= max && m >= min) {
    // Whole phrase fits one indexed shingle -> exact, no verification.
    make_shingle(0, m);
    exact = true;
  } else if (m < min) {
    // No indexed shingle of this size: AND the unigrams and verify.
    for (const auto& token : query_tokens) {
      shingles.push_back(token);
    }
    exact = false;
  } else {
    // Longer than max: AND the overlapping max-size shingles and verify.
    for (size_t begin = 0; begin + max <= m; ++begin) {
      make_shingle(begin, max);
    }
    exact = false;
  }
}

}  // namespace irs
