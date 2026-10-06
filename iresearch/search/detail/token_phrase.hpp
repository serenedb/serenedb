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

#include <absl/container/flat_hash_map.h>
#include <absl/functional/function_ref.h>

#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/storage/arena_allocator.hpp>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "iresearch/analysis/text/term_view.hpp"
#include "iresearch/analysis/token_sinks.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/detail/phrase_slop_matcher.hpp"
#include "iresearch/search/detail/term_acceptor.hpp"
#include "iresearch/search/detail/term_predicate.hpp"
#include "iresearch/search/detail/text_source.hpp"
#include "iresearch/search/filters/phrase_filter.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

struct IndexReader;
struct TermReader;

enum class PhraseMatch : uint8_t {
  Anchor,
  Automaton,
  Positions,
};

struct PhraseTokens {
  using Factory = std::function<std::shared_ptr<analysis::Tokenizer>()>;

  TextSource text;
  Factory tokenizer;
  std::optional<ByPhraseOptions> spec;
  std::optional<PhraseMatch> match;
  bool deferred = false;

  const ByPhraseOptions& Check(const ByPhraseOptions& phrase) const noexcept {
    return spec ? *spec : phrase;
  }

  bool operator==(const PhraseTokens& rhs) const noexcept {
    return text == rhs.text && spec == rhs.spec && match == rhs.match &&
           deferred == rhs.deferred;
  }
};

struct PhraseVerdict {
  uint32_t freq = 0;
  score_t scale = kNoBoost;
};

class TokenPhraseMatcher {
 public:
  using Lookup = absl::FunctionRef<PostingMeta(bytes_view)>;

  TokenPhraseMatcher(const ByPhraseOptions& phrase,
                     std::span<const std::vector<bstring>> expanded,
                     const TermReader& reader,
                     std::optional<PhraseMatch> match = std::nullopt);

  TokenPhraseMatcher(const ByPhraseOptions& phrase, bytes_view separator,
                     Lookup lookup,
                     std::optional<PhraseMatch> match = std::nullopt);

  TokenPhraseMatcher(const ByPhraseOptions& phrase, bytes_view separator,
                     const IndexReader& index, field_id field,
                     std::optional<PhraseMatch> match = std::nullopt);

  TokenPhraseMatcher(TokenPhraseMatcher&&) = delete;
  TokenPhraseMatcher& operator=(TokenPhraseMatcher&&) = delete;

  static bool Standalone(const ByPhraseOptions& phrase) noexcept;

  PhraseMatch Primary() const noexcept { return _primary; }
  PhraseMatch Fallback() const noexcept { return _fallback; }
  bool Sloppy() const noexcept { return _slop != 0; }
  size_t Slots() const noexcept { return _offs_min.size(); }

 private:
  friend class TokenPhraseSink;

  static constexpr uint32_t kNoSlot = std::numeric_limits<uint32_t>::max();
  static constexpr uint32_t kMaxBits = 64;

  struct Accept {
    uint32_t begin = 0;
    uint32_t size = 0;
    uint64_t mask = 0;
  };

  struct Extra {
    uint64_t range = 0;
    uint64_t bit = 0;
    uint32_t index = 0;
  };

  const Accept* Find(bytes_view term) const noexcept {
    const auto it = _accept.find(term);
    return it == _accept.end() ? nullptr : &it->second;
  }

  bool Plain(bytes_view term) const noexcept {
    return _separator.empty() || term.find(_separator) == bytes_view::npos;
  }

  bool Accepts(uint32_t slot, const duckdb::string_t& term) const;
  uint64_t MaskOf(const duckdb::string_t& term) const;

  template<typename Visitor>
  void ForEachSlot(const duckdb::string_t& term, Visitor&& visit) const {
    const auto view = AsBytesView(term);
    if (const auto* accept = Find(view)) {
      for (uint32_t i = 0; i != accept->size; ++i) {
        visit(_slot_ids[accept->begin + i]);
      }
    }
    if (_pattern_slots.empty() || !Plain(view)) {
      return;
    }
    for (const auto slot : _pattern_slots) {
      if (_patterns[slot]->Accepts(view)) {
        visit(slot);
      }
    }
  }

  void Init(const ByPhraseOptions& phrase,
            std::span<const std::vector<bstring>> expanded, bool patterns,
            Lookup lookup, std::optional<PhraseMatch> match);
  void AddPattern(uint32_t slot, const ByPhraseOptions::PhrasePart& part);
  void Index(std::span<const uint32_t> slots);
  void SlopLayout();
  void Layout();
  void PickAnchor(Lookup lookup);

  std::vector<bstring> _owned;
  absl::flat_hash_map<bytes_view, Accept> _accept;
  std::vector<uint32_t> _slot_ids;
  std::vector<TermAcceptorSource::ptr> _sources;
  std::vector<TermPredicate::ptr> _patterns;
  std::vector<uint32_t> _pattern_slots;
  bstring _separator;
  std::vector<duckdb::string_t> _words;
  std::vector<uint8_t> _is_word;
  std::vector<PosAttr::value_t> _offs_min;
  std::vector<PosAttr::value_t> _offs_max;
  std::vector<int64_t> _offsets;
  std::vector<detail::slop::GroupPair> _pairs;
  PosAttr::value_t _slop = 0;
  uint32_t _anchor = kNoSlot;
  uint64_t _left = 0;
  uint64_t _right = 0;
  std::vector<uint64_t> _slot_bits;
  std::vector<Extra> _extras;
  uint64_t _wild = 0;
  uint64_t _last = 0;
  uint32_t _length = 0;
  PhraseMatch _primary = PhraseMatch::Positions;
  PhraseMatch _fallback = PhraseMatch::Positions;
};

class TokenPhraseSink final : public TokenConsumer {
 public:
  static constexpr TokenLayout kLayout = TokenLayout::TermsPos;

  TokenPhraseSink(const TokenPhraseMatcher& matcher, TokenTraits producer,
                  bool count);

  void Begin();
  bool Done() const noexcept { return _done; }
  bool Restart();
  bool End(PhraseVerdict& out);

  void Prepare(duckdb::string_t) noexcept { _value_base = _last_pos; }
  void Discard() noexcept {}
  void Consume(TokenBatch& batch, DocRuns runs) final;

 private:
  static constexpr uint64_t kStepsPerToken = 4;
  static constexpr uint64_t kStepSlack = 256;

  void Start(PhraseMatch mode);

  void AnchorBatch(const TokenBatch& batch);
  bool Hit(size_t at);
  uint64_t Right(uint32_t slot, size_t at);
  uint64_t Left(uint32_t slot, size_t at);
  bool Over() noexcept;
  void Carry();
  const duckdb::string_t& TermAt(size_t at) const noexcept {
    return at < _batch_base ? _carry_terms[at - _carry_base]
                            : _batch_terms[at - _batch_base];
  }
  uint32_t PosAt(size_t at) const noexcept {
    return at < _batch_base ? _carry_pos[at - _carry_base]
                            : _batch_pos[at - _batch_base];
  }

  void AutomatonBatch(const TokenBatch& batch);
  void Step(uint64_t mask);
  void Flush();

  void PositionsBatch(const TokenBatch& batch);
  bool EndPositions(PhraseVerdict& out);

  const TokenPhraseMatcher* _matcher;
  PhraseMatch _mode = PhraseMatch::Positions;
  bool _dense;
  bool _count;
  bool _done = false;
  bool _restart = false;
  uint32_t _last_pos = 0;
  uint32_t _value_base = 0;
  uint64_t _freq = 0;

  const duckdb::string_t* _batch_terms = nullptr;
  const uint32_t* _batch_pos = nullptr;
  size_t _batch_base = 0;
  size_t _end = 0;
  size_t _carry_base = 0;
  std::vector<duckdb::string_t> _carry_terms;
  std::vector<uint32_t> _carry_pos;
  std::vector<duckdb::string_t> _next_terms;
  std::vector<uint32_t> _next_pos;
  duckdb::ArenaAllocator _arena{duckdb::Allocator::DefaultAllocator()};
  std::vector<size_t> _pending;
  uint32_t _last_anchor = 0;
  uint64_t _steps = 0;

  uint64_t _d = 0;
  uint64_t _mask = 0;
  uint32_t _at = 0;
  std::vector<uint64_t> _c;
  std::vector<uint64_t> _sums;

  std::vector<std::vector<PosAttr::value_t>> _slots;
  std::vector<PosAttr::value_t> _valid;
  std::vector<PosAttr::value_t> _next;
  std::vector<uint64_t> _ways;
  std::vector<uint64_t> _next_ways;
  detail::slop::MatchScratch _slop;
};

bool CheckValues(TokenPhraseSink& sink, ValueAnalyzer& analyzer,
                 analysis::Tokenizer& tokenizer,
                 std::span<const duckdb::string_t> values, PhraseVerdict& out);

class TextRows {
 public:
  void Bind(duckdb::Vector& values, duckdb::idx_t count);

  std::span<const duckdb::string_t> Values(duckdb::idx_t row);

 private:
  duckdb::UnifiedVectorFormat _format;
  duckdb::UnifiedVectorFormat _children;
  duckdb::LogicalTypeId _type = duckdb::LogicalTypeId::VARCHAR;
  uint64_t _array_size = 0;
  std::vector<duckdb::string_t> _values;
};

class PhraseCheck {
 public:
  PhraseCheck(const TokenPhraseMatcher& matcher, const PhraseTokens& tokens,
              bool count);

  void Bind(duckdb::DataChunk& columns);
  bool Check(duckdb::idx_t row, PhraseVerdict& out);

 private:
  std::shared_ptr<analysis::Tokenizer> _tokenizer;
  std::unique_ptr<TextExpression> _expression;
  ValueAnalyzer _analyzer;
  TokenPhraseSink _sink;
  TextRows _rows;
};

class TokenPhraseReader {
 public:
  TokenPhraseReader(const ColReader& col_reader,
                    std::span<const ColumnReader* const> columns,
                    const TokenPhraseMatcher& matcher,
                    const PhraseTokens& tokens, bool count);

  TokenPhraseReader(TokenPhraseReader&&) = delete;
  TokenPhraseReader& operator=(TokenPhraseReader&&) = delete;

  bool Match(doc_id_t doc, PhraseVerdict& out);

  void Match(std::span<const doc_id_t> docs, std::span<PhraseVerdict> verdicts);

 private:
  struct Input {
    Input(const ColumnReader& column, ReadContext& ctx);

    const ColumnReader* column;
    ColumnReader::ScanState state;
    std::unique_ptr<ColumnReader::VectorScratch> out;
  };

  ReadContext _ctx;
  std::vector<Input> _inputs;
  uint64_t _row_count = std::numeric_limits<uint64_t>::max();
  duckdb::DataChunk _chunk;
  duckdb::SelectionVector _sel;
  PhraseCheck _check;
};

}  // namespace irs
