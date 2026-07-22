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

#include "pipeline_tokenizer.hpp"

#include <algorithm>
#include <duckdb/common/vector/flat_vector.hpp>
#include <string_view>

#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/normalizing_tokenizer.hpp"
#include "iresearch/analysis/solr_synonyms_tokenizer.hpp"
#include "iresearch/analysis/stemming_tokenizer.hpp"
#include "iresearch/analysis/stopwords_tokenizer.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "iresearch/analysis/wordnet_synonyms_tokenizer.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {
namespace {

bool AllHaveOffset(std::span<const Tokenizer::ptr> pipeline) {
  return absl::c_all_of(
    pipeline, [](const Tokenizer::ptr& v) { return v->Traits().offsets; });
}

}  // namespace

PipelineTokenizer::PipelineTokenizer(std::vector<Tokenizer::ptr> options)
  : _track_offset{AllHaveOffset(options)} {
  const auto track_offset = _track_offset;
  _pipeline.reserve(options.size());
  for (auto& p : options) {
    SDB_ASSERT(p);
    _pipeline.emplace_back(std::move(p), track_offset);
  }
  options.clear();  // mimic move semantic
  if (_pipeline.empty()) {
    _pipeline.emplace_back();
  }
  if (_pipeline.size() >= 2) {
    bool eligible = true;
    bool has_expander = false;
    for (auto it = std::next(_pipeline.begin()); it != _pipeline.end(); ++it) {
      const auto& stream = it->GetStream();
      const auto type = stream.type();
      if (type == irs::Type<StringTokenizer>::id()) {
        continue;
      }
      if (has_expander) {
        eligible = false;
        break;
      }
      if (type == irs::Type<StopwordsTokenizer>::id()) {
        _stages.push_back(
          {.kind = ChainStage::Kind::Drop,
           .drop = &sdb::basics::downCast<const StopwordsTokenizer>(stream)});
      } else if (type == irs::Type<NormalizingTokenizer>::id()) {
        const auto* norm =
          &sdb::basics::downCast<const NormalizingTokenizer>(stream);
        _stages.push_back({.kind = ChainStage::Kind::Norm, .norm = norm});
        _norm_gates.push_back(norm);
      } else if (type == irs::Type<StemmingTokenizer>::id()) {
        _stages.push_back(
          {.kind = ChainStage::Kind::Stem,
           .stem = &sdb::basics::downCast<StemmingTokenizer>(it->Stream())});
      } else if (type == irs::Type<SolrSynonymsTokenizer>::id()) {
        _solr_expander =
          &sdb::basics::downCast<const SolrSynonymsTokenizer>(stream);
        has_expander = true;
      } else if (type == irs::Type<WordnetSynonymsTokenizer>::id()) {
        _wordnet_expander =
          &sdb::basics::downCast<const WordnetSynonymsTokenizer>(stream);
        has_expander = true;
      } else {
        eligible = false;
        break;
      }
    }
    if (eligible) {
      _fast_eligible = true;
    } else {
      _stages.clear();
      _norm_gates.clear();
      _solr_expander = nullptr;
      _wordnet_expander = nullptr;
    }
  }
}

bool PipelineTokenizer::RewriteGatesPass(
  std::string_view value) const noexcept {
  for (const auto* norm : _norm_gates) {
    if (!norm->AsciiFastEligible(value)) {
      return false;
    }
  }
  return true;
}

void PipelineTokenizer::ChainSink::Consume(TokenBatch& batch,
                                           std::span<const DocRun> runs) {
  SDB_ASSERT(runs.empty());
  ResolveLayout(_out_layout, [&]<TokenLayout L>() {
    if (_solr != nullptr) {
      Emit<L, ExpandT::Solr>(batch);
    } else if (_wordnet != nullptr) {
      Emit<L, ExpandT::Wordnet>(batch);
    } else {
      Emit<L, ExpandT::None>(batch);
    }
  });
}

bool PipelineTokenizer::ChainSink::ApplyStages(std::string_view& term) {
  size_t buf = 0;
  for (const auto& stage : _stages) {
    switch (stage.kind) {
      case ChainStage::Kind::Drop:
        if (stage.drop->IsStopword(term)) {
          return false;
        }
        break;
      case ChainStage::Kind::Norm: {
        auto& out = _rewrite_bufs[buf];
        buf ^= 1;
        const bool ok = stage.norm->AsciiRewrite(term, out);
        SDB_ASSERT(ok);
        if (!ok) {
          return false;
        }
        term = out;
        break;
      }
      case ChainStage::Kind::Stem:
        term = ViewCast<char>(stage.stem->Stem(term));
        break;
    }
  }
  return true;
}

template<TokenLayout OutLayout, PipelineTokenizer::ChainSink::ExpandT Expand>
void PipelineTokenizer::ChainSink::Emit(TokenBatch& batch) {
  auto& out = *_out;
  auto& obuf = out.buf;
  const bool dense = batch.dense_pos;
  for (uint32_t i = 0, n = batch.count; i < n; ++i) {
    const auto& term = batch.terms[i];
    uint32_t inc = 1;
    if constexpr (OutLayout != TokenLayout::Terms) {
      if (!dense) {
        const auto p = batch.pos[i];
        inc = p - _last_pos;
        _last_pos = p;
      }
    }
    std::string_view tv = AsView(term);
    if (!ApplyStages(tv)) {
      continue;
    }
    const auto put = [&](duckdb::string_t slot, uint32_t pos) {
      const auto j = out.Next();
      obuf.terms[j] = slot;
      if constexpr (OutLayout != TokenLayout::Terms) {
        obuf.pos[j] = pos;
      }
      if constexpr (OutLayout == TokenLayout::TermsPosOffs) {
        obuf.offs_start[j] = batch.offs_start[i];
        obuf.offs_end[j] = batch.offs_end[i];
      }
    };
    if constexpr (Expand == ExpandT::Solr) {
      _out_pos += inc;
      if (const auto* list = _solr->Lookup(tv); list != nullptr) {
        for (const auto synonym : *list) {
          put(
            MakeTermView(synonym.data(), static_cast<uint32_t>(synonym.size())),
            _out_pos);
        }
      } else {
        out.EmitInterned<OutLayout>(ViewCast<byte_type>(tv), _out_pos,
                                    batch.offs_start[i], batch.offs_end[i]);
      }
    } else if constexpr (Expand == ExpandT::Wordnet) {
      const auto* groups = _wordnet->Lookup(tv);
      if (groups == nullptr) {
        continue;
      }
      _out_pos += inc;
      auto pos = _out_pos;
      for (const auto group : *groups) {
        put(MakeTermView(group.data(), static_cast<uint32_t>(group.size())),
            pos++);
      }
      _out_pos = pos - 1;
    } else {
      _out_pos += inc;
      out.EmitInterned<OutLayout>(ViewCast<byte_type>(tv), _out_pos,
                                  batch.offs_start[i], batch.offs_end[i]);
    }
  }
}

template<TokenLayout Layout>
bool PipelineTokenizer::FillFast(std::string_view value, TokenEmitter& sink) {
  if (!_chain) {
    _chain = std::make_unique<ChainSink>();
  }
  constexpr auto kProducerLayout =
    Layout == TokenLayout::TermsPosOffs ? TokenLayout::TermsPos : Layout;
  _chain->Bind(sink, Layout, _stages, _solr_expander, _wordnet_expander,
               _track_offset);
  if (!_pipeline.front().Stream().Fill(
        value, _chain->writer, _track_offset ? Layout : kProducerLayout)) {
    _chain->writer.Discard();
    return false;
  }
  _chain->writer.Finish();
  return true;
}

Tokenizer::ptr PipelineTokenizer::Make(Options opts) {
  if (opts.children.empty()) {
    THROW_SQL_ERROR(ERR_MSG("pipeline: requires at least one child analyzer"));
  }
  std::vector<Tokenizer::ptr> live_children;
  live_children.reserve(opts.children.size());
  for (auto& child : opts.children) {
    if (!child) {
      THROW_SQL_ERROR(ERR_MSG("pipeline: null child analyzer config"));
    }
    live_children.emplace_back(CreateTokenizer(std::move(*child)));
  }
  return std::make_unique<PipelineTokenizer>(std::move(live_children));
}

/// Push-style walk over the nested pipeline; the bottom level's tokens are
/// emitted, positions composed by the legacy increment rules:
///  - If none of pipeline members changes position - whole pipeline holds
///  position
///  - If one or more pipeline member moves - pipeline moves( change from max->0
///  is not move, see rules below!).
///    All position gaps are accumulated (e.g. if one member has inc 2(1 pos
///    gap) and other has inc 3(2 pos gap)  - pipeline has inc 4 (1+2 pos gap))
///  - All position changes caused by parent analyzer move next (e.g. transfer
///  from max->0 by first next after reset) are collapsed.
///    e.g if parent moves after next, all its children are resetted to new
///    token and also moves step froward - this whole operation is just one step
///    for pipeline (if any of children has moved more than 1 step - gaps are
///    preserved!)
///  - If parent after next is NOT moved (inc == 0) than pipeline makes one step
///  forward if at least one child changes
///    position from any positive value back to 0 due to reset (additional gaps
///    also preserved!) as this is not change max->0 and position is indeed
///    changed.
template<TokenLayout Layout>
void PipelineTokenizer::FillValue(TokenEmitter& sink) {
  auto& buf = sink.buf;
  const size_t bottom = _pipeline.size() - 1;
  size_t cur = 0;
  uint32_t pos = 0;
  uint32_t pipeline_inc = 0;
  bool step_for_rollback = false;
  for (;;) {
    while (!_pipeline[cur].Advance()) {
      if (cur == 0) {
        return;
      }
      --cur;
    }
    pipeline_inc = _pipeline[cur].Inc();
    const auto top_holds_position = pipeline_inc == 0;
    bool empty_child = false;
    while (cur != bottom) {
      const auto prev_term = _pipeline[cur].Term();
      const auto prev_start = _pipeline[cur].Start();
      const auto prev_end = _pipeline[cur].End();
      ++cur;
      step_for_rollback |=
        top_holds_position && _pipeline[cur].Position() != 0 &&
        _pipeline[cur].Position() != std::numeric_limits<uint32_t>::max();
      if (!_pipeline[cur].Tokenize(prev_start, prev_end,
                                   ViewCast<char>(prev_term), *_scratch)) {
        return;
      }
      if (!_pipeline[cur].Advance()) {
        SDB_ASSERT(cur != 0);
        --cur;
        empty_child = true;
        break;
      }
      pipeline_inc += _pipeline[cur].Inc();
      SDB_ASSERT(_pipeline[cur].Inc() > 0);
      SDB_ASSERT(pipeline_inc > 0);
      pipeline_inc--;
    }
    if (empty_child) {
      continue;
    }
    if (step_for_rollback) {
      pipeline_inc++;
      step_for_rollback = false;
    }
    const auto i = sink.Next();
    buf.terms[i] = sink.Intern(_pipeline[cur].Term());
    if constexpr (Layout != TokenLayout::Terms) {
      pos += pipeline_inc;
      buf.pos[i] = pos;
    }
    if constexpr (Layout == TokenLayout::TermsPosOffs) {
      buf.offs_start[i] = _track_offset ? _pipeline[cur].Start() : 0;
      buf.offs_end[i] = _track_offset ? _pipeline[cur].End() : 0;
    }
  }
}

template<TokenLayout Layout>
bool PipelineTokenizer::DoFill(std::string_view value, TokenEmitter& sink) {
  if (_fast_eligible && !_force_generic && RewriteGatesPass(value)) {
    return FillFast<Layout>(value, sink);
  }
  if (!_scratch) {
    _scratch = std::make_unique<FillScratchT>();
  }
  _scratch->arena.Reset();
  if (!_pipeline.front().Tokenize(0, static_cast<uint32_t>(value.size()), value,
                                  *_scratch)) {
    return false;
  }
  FillValue<Layout>(sink);
  return true;
}

PipelineTokenizer::SubAnalyzerT::SubAnalyzerT(Tokenizer::ptr a,
                                              bool track_offset)
  : _analyzer(std::move(a)), _track_offset(track_offset) {}

PipelineTokenizer::SubAnalyzerT::SubAnalyzerT()
  : _analyzer(std::make_unique<EmptyTokenizer>()) {}

template class TypedTokenizer<PipelineTokenizer>;

}  // namespace irs::analysis
