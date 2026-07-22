////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2020 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <tuple>

#include "basics/down_cast.h"
#include "basics/serializer.h"
#include "basics/shared.hpp"
#include "iresearch/analysis/batch/token_sinks.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "tokenizer.hpp"

namespace irs::analysis {

struct TokenizerConfig;
class NormalizingTokenizer;
class SolrSynonymsTokenizer;
class StemmingTokenizer;
class StopwordsTokenizer;
class WordnetSynonymsTokenizer;

// An tokenizer capable of chaining other analyzers
class PipelineTokenizer final : public TypedTokenizer<PipelineTokenizer>,
                                private util::Noncopyable {
 public:
  struct Options {
    using Owner = PipelineTokenizer;
    std::vector<std::unique_ptr<TokenizerConfig>> children;
  };
  static Tokenizer::ptr Make(Options opts);

  static constexpr std::string_view type_name() noexcept { return "pipeline"; }

  explicit PipelineTokenizer(std::vector<Tokenizer::ptr> children);

  TokenTraits Traits() const noexcept final {
    return {.dense_pos = false, .offsets = _track_offset};
  }

  template<TokenLayout Layout>
  bool DoFill(std::string_view value, TokenEmitter& sink);

  bool FastPathEligible() const noexcept { return _fast_eligible; }
  void ForceGenericPath(bool force) noexcept { _force_generic = force; }

  /// @brief calls visitor on pipeline members in respective order. Visiting is
  /// interrupted on first visitor returning false.
  /// @param visitor visitor to call
  /// @return true if all visits returned true, false otherwise
  template<typename Visitor>
  bool visit_members(Visitor&& visitor) const {
    for (const auto& sub : _pipeline) {
      const auto& stream = sub.GetStream();
      if (stream.type() == type()) {
        // pipe inside pipe - forward visiting
        const auto& sub_pipe = sdb::basics::downCast<PipelineTokenizer>(stream);
        if (!sub_pipe.visit_members(visitor)) {
          return false;
        }
      } else if (!visitor(sub.GetStream())) {
        return false;
      }
    }
    return true;
  }

 private:
  struct FillScratchT {
    FillScratchT() : accumulator{arena}, writer{accumulator} {}

    duckdb::ArenaAllocator arena{duckdb::Allocator::DefaultAllocator()};
    TokenAccumulator accumulator;
    TokenWriter writer;
  };

  struct SubAnalyzerT {
    explicit SubAnalyzerT(Tokenizer::ptr a, bool track_offset);
    SubAnalyzerT();

    bool Tokenize(uint32_t start, uint32_t end, std::string_view data,
                  FillScratchT& scratch) {
      _data_size = data.size();
      _data_start = start;
      _data_end = end;
      _position = std::numeric_limits<uint32_t>::max();
      _cursor = 0;
      _terms.clear();
      _pos.clear();
      _offs_start.clear();
      _offs_end.clear();
      scratch.accumulator.Bind(_terms, _pos, _analyzer->Traits().dense_pos,
                               _track_offset ? &_offs_start : nullptr,
                               _track_offset ? &_offs_end : nullptr);
      if (!_analyzer->Fill(data, scratch.writer,
                           _track_offset ? TokenLayout::TermsPosOffs
                                         : TokenLayout::TermsPos)) {
        return false;
      }
      scratch.writer.Finish();
      return true;
    }

    bool Advance() {
      if (_cursor >= _terms.size()) {
        return false;
      }
      const uint32_t prev_pos = _cursor == 0 ? 0 : _pos[_cursor - 1];
      _inc = _pos[_cursor] - prev_pos;
      _term = bytes_view{
        reinterpret_cast<const byte_type*>(_terms[_cursor].GetData()),
        _terms[_cursor].GetSize()};
      if (_track_offset) {
        _offs_s = _offs_start[_cursor];
        _offs_e = _offs_end[_cursor];
      }
      ++_cursor;
      _position += _inc;
      return true;
    }

    uint32_t Inc() const noexcept { return _inc; }
    bytes_view Term() const noexcept { return _term; }
    uint32_t Position() const noexcept { return _position; }

    uint32_t Start() const noexcept { return _data_start + _offs_s; }

    uint32_t End() const noexcept {
      return _offs_e == _data_size ? _data_end : Start() + _offs_e - _offs_s;
    }

    const Tokenizer& GetStream() const noexcept {
      SDB_ASSERT(_analyzer);
      return *_analyzer;
    }

    Tokenizer& Stream() noexcept {
      SDB_ASSERT(_analyzer);
      return *_analyzer;
    }

   private:
    Tokenizer::ptr _analyzer;
    std::vector<duckdb::string_t> _terms;
    std::vector<uint32_t> _pos;
    std::vector<uint32_t> _offs_start;
    std::vector<uint32_t> _offs_end;
    size_t _cursor{0};
    uint32_t _inc{0};
    bytes_view _term;
    uint32_t _offs_s{0};
    uint32_t _offs_e{0};
    size_t _data_size{0};
    uint32_t _data_start{0};
    uint32_t _data_end{0};
    uint32_t _position{std::numeric_limits<uint32_t>::max()};
    bool _track_offset{false};
  };
  using pipeline_t = std::vector<SubAnalyzerT>;

  struct ChainStage {
    enum class Kind : uint8_t { Drop, Norm, Stem };

    Kind kind;
    const StopwordsTokenizer* drop = nullptr;
    const NormalizingTokenizer* norm = nullptr;
    StemmingTokenizer* stem = nullptr;
  };

  class ChainSink final : public TokenConsumer {
   public:
    ChainSink() : writer{*this} {}

    void Bind(TokenEmitter& out, TokenLayout out_layout,
              std::span<const ChainStage> stages,
              const SolrSynonymsTokenizer* solr,
              const WordnetSynonymsTokenizer* wordnet, bool track_offset) {
      _out = &out;
      _out_layout = out_layout;
      _stages = stages;
      _solr = solr;
      _wordnet = wordnet;
      _track_offset = track_offset;
      _out_pos = 0;
      _last_pos = 0;
    }

    void Consume(TokenBatch& batch, std::span<const DocRun> runs) final;

    TokenWriter writer;

   private:
    enum class ExpandT : uint8_t { None, Solr, Wordnet };

    template<TokenLayout OutLayout, ExpandT Expand>
    void Emit(TokenBatch& batch);

    bool ApplyStages(std::string_view& term);

    TokenEmitter* _out = nullptr;
    TokenLayout _out_layout{};
    std::span<const ChainStage> _stages;
    const SolrSynonymsTokenizer* _solr = nullptr;
    const WordnetSynonymsTokenizer* _wordnet = nullptr;
    std::string _rewrite_bufs[2];
    uint32_t _out_pos = 0;
    uint32_t _last_pos = 0;
    bool _track_offset = false;
  };

  template<TokenLayout Layout>
  void FillValue(TokenEmitter& sink);
  template<TokenLayout Layout>
  bool FillFast(std::string_view value, TokenEmitter& sink);

  bool RewriteGatesPass(std::string_view value) const noexcept;

  pipeline_t _pipeline;
  bool _track_offset{false};
  std::unique_ptr<FillScratchT> _scratch;
  std::vector<ChainStage> _stages;
  std::vector<const NormalizingTokenizer*> _norm_gates;
  const SolrSynonymsTokenizer* _solr_expander = nullptr;
  const WordnetSynonymsTokenizer* _wordnet_expander = nullptr;
  std::unique_ptr<ChainSink> _chain;
  bool _fast_eligible{false};
  bool _force_generic{false};
};

template<typename Context>
void SerdeWrite(Context ctx, const PipelineTokenizer::Options& o) {
  sdb::basics::WriteTuple(ctx.io(), std::tie(o.children), ctx.arg());
}

template<typename Context>
void SerdeRead(Context ctx, PipelineTokenizer::Options& o) {
  auto refs = std::tie(o.children);
  sdb::basics::ReadTuple(ctx.io(), refs, ctx.arg());
}

extern template class TypedTokenizer<PipelineTokenizer>;

}  // namespace irs::analysis
