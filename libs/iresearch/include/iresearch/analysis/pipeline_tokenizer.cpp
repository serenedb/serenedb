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

#include <cstring>

#include "basics/misc.hpp"
#include "iresearch/analysis/process_tokens.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {
namespace {

class EmptyTokenizer final : public TypedTokenizer<EmptyTokenizer> {
 public:
  static constexpr std::string_view type_name() noexcept { return "empty"; }

  TokenTraits Traits() const noexcept final { return {}; }

  template<TokenLayout Layout>
  bool DoFill(duckdb::string_t, TokenSink&) const noexcept {
    return true;
  }
};

}  // namespace

PipelineTokenizer::PipelineTokenizer(std::vector<Tokenizer::ptr> options) {
  // One visit per stage: validate it against what the stage before it
  // produces, fold the chain traits and classify it into the segment being
  // built.
  SDB_ASSERT(options.size() > 1);
  std::vector<uint32_t> offsets;
  uint32_t segment_first = 0;
  bool segment_stable = true;
  bool producer_dense = true;
  bool producer_stable = false;
  for (size_t i = 0; i < options.size(); ++i) {
    SDB_ASSERT(options[i]);
    auto& child = *options[i];
    const auto traits = child.Traits();
    if (i != 0 && _traits.output != traits.input) {
      THROW_SQL_ERROR(ERR_MSG("pipeline: stage ", i, " expects ",
                              duckdb::LogicalTypeIdToString(traits.input),
                              " input, but the preceding stage produces ",
                              duckdb::LogicalTypeIdToString(_traits.output)));
    }
    if (traits.store) {
      THROW_SQL_ERROR(
        ERR_MSG("pipeline: stage ", i,
                " produces a per-document store blob, which a pipeline "
                "cannot deliver"));
    }
    _wanted_traits.ascii |= child.WantedBlockTraits().ascii;
    if (i == 0) {
      producer_dense = !traits.explicit_pos;
      producer_stable = traits.stable;
      _traits = traits;
    } else {
      _split_mixed_blocks |= child.WantedBlockTraits().ascii;
      _traits.output = traits.output;
      _traits.unique &= traits.unique;
      _traits.offsets &= traits.offsets;
      _traits.stable &= traits.stable;
      // Fan-out alone keeps the ramp: a child emitting dense positions has its
      // increments rebased onto the parent stream, and a parent that emits
      // nothing simply never commits. Only explicit child positions (stacked
      // synonyms, ngrams) break it.
      _traits.explicit_pos |= traits.explicit_pos;
      if (auto* stage = dynamic_cast<TokenStage*>(&child)) {
        SDB_ASSERT(!traits.explicit_pos);
        _filters.push_back(stage);
        segment_stable &= traits.stable;
      } else {
        auto* expander = dynamic_cast<TokenExpander*>(&child);
        const bool first = _links.empty();
        offsets.push_back(segment_first);
        _links.push_back(std::make_unique<ChainSink>(
          expander, expander ? nullptr : &child, first && producer_dense,
          first && producer_stable && segment_stable));
        segment_first = static_cast<uint32_t>(_filters.size());
        segment_stable = true;
      }
    }
  }
  _pipeline = std::move(options);
  _front = _pipeline.front().get();
  _interpose = segment_first != _filters.size();
  if (_interpose) {
    const bool first = _links.empty();
    offsets.push_back(segment_first);
    _links.push_back(
      std::make_unique<ChainSink>(nullptr, nullptr, first && producer_dense,
                                  first && producer_stable && segment_stable));
  }
  SDB_ASSERT(!_links.empty());
  for (size_t i = 0, n = _links.size(); i < n; ++i) {
    const uint32_t end =
      i + 1 == n ? static_cast<uint32_t>(_filters.size()) : offsets[i + 1];
    _links[i]->SetFilters({_filters.data() + offsets[i], end - offsets[i]});
  }
  _chain = _interpose ? _links.size() - 1 : _links.size();
  _head = _links.front().get();
}

void PipelineTokenizer::BindLinks(TokenSink& sink, TokenLayout layout,
                                  bool column) {
  _bound_sink = &sink;
  _bound_layout = layout;
  _bound_column = column;
  const size_t chain = column ? _chain : _links.size();
  for (size_t i = 0; i < chain; ++i) {
    auto& out = i + 1 == chain ? sink : _links[i + 1]->writer;
    _links[i]->Bind(&out, layout, column || i + 1 != chain);
  }
  if (column && _interpose) {
    _links.back()->Bind(nullptr, layout, false);
  }
}

const uint64_t* PipelineTokenizer::ChainSink::RunFilters(TokenBatch& batch) {
  if (_filters.empty()) {
    return nullptr;
  }
  std::memset(_valid, 0xFF, sizeof _valid);
  BatchCtx ctx{{.ascii = _ascii}, _arena, _valid};
  bool all_kept = true;
  for (auto* stage : _filters) {
    all_kept &= stage->ProcessTokens(batch, ctx);
  }
  return all_kept ? nullptr : _valid;
}

void PipelineTokenizer::ChainSink::Consume(TokenBatch& batch, DocRuns runs) {
  SDB_ASSERT(_data || runs.size() <= 1);
  const uint64_t* valid = RunFilters(batch);
  if (_through) {
    PassThrough(batch, runs, valid);
  } else if (_expander) {
    ExpandBatch(batch, runs, valid);
  } else if (_terminal) {
    DriveTerminal(batch, runs, valid);
  } else {
    DeliverBatch(batch, runs, valid);
  }
  if (!_filters.empty()) {
    _arena.Reset();
  }
}

void PipelineTokenizer::ChainSink::PassThrough(TokenBatch& batch, DocRuns runs,
                                               const uint64_t* valid) {
  if (!valid) {
    _through->Consume(batch, runs);
    return;
  }
  ResolveValues(
    [&](auto layout_tag, auto explicit_pos) IRS_FORCE_INLINE {
      Compact<layout_tag(), explicit_pos()>(batch, runs, valid);
    },
    _out_layout, !_in_dense);
  _through->Consume(batch, {{_cruns.get(), runs.size()}, runs.tail_open});
}

template<TokenLayout L, bool Explicit>
IRS_NO_INLINE void PipelineTokenizer::ChainSink::Compact(
  TokenBatch& batch, DocRuns runs, const uint64_t* valid) {
  uint32_t dst = 0;
  uint32_t first = 0;
  for (size_t r = 0, n = runs.size(); r < n; ++r) {
    const auto run = runs[r];
    const uint32_t end = first + run.ntokens;
    const uint32_t run_dst = dst;
    if constexpr (Explicit && L != TokenLayout::Terms) {
      if (run.doc != _open_doc) {
        _pos.Bind(_in_dense);
        _open_doc = run.doc;
      }
    }
    for (uint32_t i = first; i < end; ++i) {
      [[maybe_unused]] uint32_t inc = 1;
      if constexpr (Explicit && L != TokenLayout::Terms) {
        inc = _pos.Observe(batch, i);
      }
      if (!IsValid(valid, i)) {
        continue;
      }
      if (dst != i) {
        batch.terms[dst] = batch.terms[i];
        if constexpr (L == TokenLayout::TermsPosOffs) {
          batch.offs_start[dst] = batch.offs_start[i];
          batch.offs_end[dst] = batch.offs_end[i];
        }
      }
      if constexpr (Explicit && L != TokenLayout::Terms) {
        batch.pos[dst] = _pos.Commit(inc);
      }
      ++dst;
    }
    _cruns[r] = {run.doc, dst - run_dst};
    first = end;
  }
  batch.count = dst;
}

void PipelineTokenizer::ChainSink::ExpandBatch(TokenBatch& batch, DocRuns runs,
                                               const uint64_t* valid) {
  uint32_t base = 0;
  for (size_t r = 0, n = runs.size(); r < n; ++r) {
    const auto run = runs[r];
    const uint32_t first = base;
    const uint32_t end = base + run.ntokens;
    base = end;
    const bool open_after = r + 1 == n && runs.tail_open;
    OpenRun(run.doc);
    _expander->ExpandTokens(batch, first, end, *_out,
                            {_out_layout,
                             {.ascii = _ascii && _filters.empty()},
                             _value,
                             &_pos,
                             valid});
    if (!open_after) {
      CloseSource();
    }
  }
}

void PipelineTokenizer::ChainSink::DriveTerminal(TokenBatch& batch,
                                                 DocRuns runs,
                                                 const uint64_t* valid) {
  duckdb::UnifiedVectorFormat fmt;
  fmt.sel = duckdb::FlatVector::IncrementalSelectionVector();
  fmt.data = reinterpret_cast<duckdb::const_data_ptr_t>(batch.terms);
  fmt.physical_type = duckdb::PhysicalType::VARCHAR;
  if (valid) {
    fmt.validity = duckdb::ValidityMask{
      const_cast<uint64_t*>(valid), static_cast<duckdb::idx_t>(batch.count)};
  }
  _src = &batch;
  _src_runs = runs;
  _src_run = 0;
  _src_run_end = 0;
  _scan = 0;
  _cur_parent = kNoParent;
  _terminal->Fill(fmt, batch.count, doc_limits::min(), *_scratch,
                  {_out_layout, {.ascii = _ascii && _filters.empty()}});
  _scratch->Finish();
  FinishSourceBatch(runs.tail_open);
  _src = nullptr;
}

void PipelineTokenizer::ChainSink::RebaseConsume(TokenBatch& batch,
                                                 DocRuns runs) {
  ResolveLayout(_out_layout, [&]<TokenLayout L>() {
    uint32_t base = 0;
    for (size_t r = 0, n = runs.size(); r < n; ++r) {
      const auto run = runs[r];
      const uint32_t first = base;
      const uint32_t end = base + run.ntokens;
      base = end;
      const auto parent = static_cast<uint32_t>(run.doc - doc_limits::min());
      if (parent != _cur_parent) {
        AdvanceToParent(parent);
      }
      RebaseRun<L>(batch, first, end, parent);
      _cur_parent = r + 1 == n && runs.tail_open ? parent : kNoParent;
    }
  });
}

template<TokenLayout L>
void PipelineTokenizer::ChainSink::RebaseRun(const TokenBatch& batch,
                                             uint32_t first, uint32_t end,
                                             uint32_t parent) {
  for (uint32_t i = first; i < end; ++i) {
    const auto term = batch.terms[i];
    const auto size = static_cast<uint32_t>(term.GetSize());
    if constexpr (L == TokenLayout::Terms) {
      _out->Emit<L>(_value, term.GetData(), size);
    } else {
      const uint32_t child_pos = _child_dense ? _cp.Last() + 1 : batch.pos[i];
      const uint32_t pos = _pos.Commit(_cp.Next(child_pos));
      if constexpr (L == TokenLayout::TermsPosOffs) {
        const Offs offs =
          RebaseOffs(_src->offs_start[parent], _src->offs_end[parent],
                     static_cast<uint32_t>(_src->terms[parent].GetSize()),
                     {batch.offs_start[i], batch.offs_end[i]});
        _out->Emit<L>(_value, term.GetData(), size, pos, offs);
      } else {
        _out->Emit<L>(_value, term.GetData(), size, pos);
      }
    }
  }
}

void PipelineTokenizer::ChainSink::AdvanceToParent(uint32_t parent) {
  while (true) {
    while (_scan >= _src_run_end) {
      NextSourceRun();
    }
    uint32_t inc = 1;
    if (_out_layout != TokenLayout::Terms) {
      inc = _pos.Observe(*_src, _scan);
    }
    if (_scan++ == parent) {
      _cp = ChildPos{inc};
      return;
    }
  }
}

void PipelineTokenizer::ChainSink::NextSourceRun() {
  SDB_ASSERT(_src_run < _src_runs.size());
  const auto run = _src_runs[_src_run++];
  _src_run_end += run.ntokens;
  OpenRun(run.doc);
}

void PipelineTokenizer::ChainSink::FinishSourceBatch(bool tail_open) {
  const uint32_t count = _src->count;
  while (_scan < count) {
    while (_scan >= _src_run_end) {
      NextSourceRun();
    }
    if (_out_layout != TokenLayout::Terms) {
      _pos.Observe(*_src, _scan);
    }
    ++_scan;
  }
  while (_src_run < _src_runs.size()) {
    NextSourceRun();
  }
  if (!tail_open && doc_limits::valid(_open_doc)) {
    CloseSource();
  }
}

void PipelineTokenizer::ChainSink::DeliverBatch(TokenBatch& batch, DocRuns runs,
                                                const uint64_t* valid) {
  ResolveValues(
    [&](auto layout_tag, auto has_drop, auto stable,
        auto dense) IRS_FORCE_INLINE {
      Deliver<layout_tag(), has_drop(), stable(), dense()>(batch, runs, valid);
    },
    _out_layout, static_cast<bool>(valid), _stable, _in_dense);
}

template<TokenLayout L, bool HasDrop, bool Stable, bool Dense>
IRS_NO_INLINE void PipelineTokenizer::ChainSink::Deliver(
  const TokenBatch& batch, DocRuns runs, const uint64_t* valid) {
  uint32_t base = 0;
  for (size_t r = 0, n = runs.size(); r < n; ++r) {
    const auto run = runs[r];
    const uint32_t first = base;
    const uint32_t end = base + run.ntokens;
    base = end;
    const bool open_after = r + 1 == n && runs.tail_open;
    OpenRun(run.doc);
    if constexpr (L == TokenLayout::Terms ||
                  (Dense && L == TokenLayout::TermsPos)) {
      _out->EmitTerms<L, Stable>(_value, batch, first, run.ntokens,
                                 HasDrop ? valid : nullptr);
    } else {
      for (uint32_t i = first; i < end; ++i) {
        const uint32_t inc = _pos.Observe(batch, i);
        if constexpr (HasDrop) {
          if (!IsValid(valid, i)) {
            continue;
          }
        }
        const auto term = batch.terms[i];
        const uint32_t pos = _pos.Commit(inc);
        if constexpr (Stable) {
          if constexpr (L == TokenLayout::TermsPos) {
            _out->Emit<L>(term, pos);
          } else {
            _out->Emit<L>(term, pos,
                          Offs{batch.offs_start[i], batch.offs_end[i]});
          }
        } else {
          const auto size = static_cast<uint32_t>(term.GetSize());
          if constexpr (L == TokenLayout::TermsPos) {
            _out->Emit<L>(_value, term.GetData(), size, pos);
          } else {
            _out->Emit<L>(_value, term.GetData(), size, pos,
                          Offs{batch.offs_start[i], batch.offs_end[i]});
          }
        }
      }
    }
    if (!open_after) {
      CloseSource();
    }
  }
}

Tokenizer::ptr PipelineTokenizer::Make(Options opts,
                                       duckdb::SharedObjectCache& cache) {
  std::vector<Tokenizer::ptr> live_children;
  live_children.reserve(opts.children.size());
  for (auto& child : opts.children) {
    if (!child) {
      THROW_SQL_ERROR(ERR_MSG("pipeline: null child analyzer config"));
    }
    auto tokenizer = CreateTokenizer(std::move(*child), cache);
    SDB_ENSURE(tokenizer);
    const auto traits = tokenizer->Traits();
    if (!live_children.empty() && traits.keyword) {
      SDB_ASSERT(traits.input == traits.output);
      continue;
    }
    live_children.emplace_back(std::move(tokenizer));
  }
  if (live_children.empty()) {
    return std::make_unique<EmptyTokenizer>();
  }
  if (live_children.size() == 1) {
    return std::move(live_children.front());
  }
  return std::make_unique<PipelineTokenizer>(std::move(live_children));
}

void PipelineTokenizer::Fill(const duckdb::UnifiedVectorFormat& fmt,
                             uint32_t count, doc_id_t first_doc,
                             TokenSink& sink, FillCtx ctx) {
  if (&sink != _bound_sink || ctx.layout != _bound_layout || !_bound_column)
    [[unlikely]] {
    BindLinks(sink, ctx.layout, true);
  }
  auto& input = _chain == 0 ? sink : _head->writer;
  const auto* data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);
  ctx.traits = ComputeBlockTraits(fmt, count, data, _wanted_traits, ctx.traits);
  for (auto& link : _links) {
    link->BindColumn(fmt, data, first_doc);
  }
  _head->SetAscii(ctx.traits.ascii || _split_mixed_blocks);
  TokenConsumer* prev = nullptr;
  Finally restore = [&]() noexcept {
    if (!prev) {
      return;
    }
    sink.Discard();
    sink.Rebind(*prev);
    _links.back()->SetThrough(nullptr);
  };
  if (_interpose) {
    sink.Finish();
    prev = sink.Rebind(*_links.back());
    _links.back()->SetThrough(prev);
  }
  const auto drain = [&] {
    for (size_t i = 0; i < _chain; ++i) {
      _links[i]->writer.Finish();
    }
    if (_interpose) {
      sink.Finish();
    }
  };
  if (ctx.traits.ascii || !_split_mixed_blocks) [[likely]] {
    _front->Fill(fmt, count, first_doc, input, ctx);
  } else {
    // Mixed block: a single non-ascii value would otherwise force every
    // value in it down the unicode path, so refine per value and drain the
    // chain whenever the fact flips.
    bool ascii = true;
    ForEachValidRow(fmt, count, [&](uint32_t i, uint32_t idx) {
      const auto value = data[idx];
      const auto traits = ComputeValueTraits(value, _wanted_traits, {});
      if (traits.ascii != ascii) {
        drain();
        ascii = traits.ascii;
        _head->SetAscii(ascii);
      }
      _front->Fill(value, first_doc + i, input, {ctx.layout, traits});
      return true;
    });
  }
  drain();
}

bool PipelineTokenizer::Fill(const duckdb::string_t& value, TokenSink& sink,
                             FillCtx ctx) {
  if (&sink != _bound_sink || ctx.layout != _bound_layout || _bound_column)
    [[unlikely]] {
    BindLinks(sink, ctx.layout, false);
  }
  ctx.traits = ComputeValueTraits(value, _wanted_traits, ctx.traits);
  for (auto& link : _links) {
    link->BindValue(value);
  }
  _head->SetAscii(ctx.traits.ascii);
  if (!_front->Fill(value, doc_limits::min(), _head->writer, ctx)) {
    for (auto& link : _links) {
      link->writer.Discard();
    }
    return false;
  }
  for (auto& link : _links) {
    link->writer.Finish();
  }
  return true;
}

}  // namespace irs::analysis
