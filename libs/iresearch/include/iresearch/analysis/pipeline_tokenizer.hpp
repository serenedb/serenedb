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

#include <duckdb/storage/arena_allocator.hpp>
#include <limits>
#include <memory>
#include <span>
#include <tuple>

#include "basics/down_cast.h"
#include "basics/serializer.h"
#include "basics/shared.hpp"
#include "iresearch/analysis/expand_tokens.hpp"
#include "iresearch/analysis/tokenizer.hpp"

namespace duckdb {

class SharedObjectCache;

}  // namespace duckdb
namespace irs::analysis {

struct TokenStage;
struct TokenizerConfig;

class PipelineTokenizer final : public Tokenizer, private util::Noncopyable {
 public:
  struct Options {
    using Owner = PipelineTokenizer;
    std::vector<std::unique_ptr<TokenizerConfig>> children;
  };
  static Tokenizer::ptr Make(Options opts, duckdb::SharedObjectCache& cache);

  static constexpr std::string_view type_name() noexcept { return "pipeline"; }

  explicit PipelineTokenizer(std::vector<Tokenizer::ptr> children);

  TypeInfo::type_id type() const noexcept final {
    return irs::Type<PipelineTokenizer>::id();
  }

  TokenTraits Traits() const noexcept final { return _traits; }

  BlockTraits WantedBlockTraits() const noexcept final {
    return _wanted_traits;
  }

  using Tokenizer::Fill;

  bool Fill(const duckdb::string_t& value, TokenSink& sink, FillCtx ctx) final;

  void Fill(const duckdb::UnifiedVectorFormat& fmt, uint32_t count,
            doc_id_t first_doc, TokenSink& sink, FillCtx ctx) final;

  void Bind(duckdb::ClientContext& ctx) final {
    for (auto& sub : _pipeline) {
      sub->Bind(ctx);
    }
  }

  void Unbind() noexcept final {
    _bound_sink = nullptr;
    for (auto& sub : _pipeline) {
      sub->Unbind();
    }
  }

  size_t MemoryUsage() const noexcept final {
    size_t size = _links.size() * sizeof(TokenSink);
    for (const auto& link : _links) {
      size += link->ScratchBytes();
    }
    for (const auto& sub : _pipeline) {
      size += sub->MemoryUsage();
    }
    return size;
  }

  template<typename Visitor>
  bool VisitMembers(Visitor&& visitor) const {
    for (const auto& sub : _pipeline) {
      const auto& stream = *sub;
      if (stream.type() == type()) {
        const auto& sub_pipe = sdb::basics::downCast<PipelineTokenizer>(stream);
        if (!sub_pipe.VisitMembers(visitor)) {
          return false;
        }
      } else if (!visitor(stream)) {
        return false;
      }
    }
    return true;
  }

 private:
  class ChainSink final : public TokenConsumer {
   public:
    ChainSink(TokenExpander* expander, Tokenizer* terminal, bool dense,
              bool stable)
      : _expander{expander},
        _terminal{terminal},
        _in_dense{dense},
        _child_dense{terminal && !terminal->Traits().explicit_pos},
        _stable{stable} {
      writer.Bind(*this, nullptr);
      _rebase.link = this;
    }

    void SetFilters(std::span<TokenStage* const> filters) noexcept {
      _filters = filters;
    }

    void Bind(TokenSink* out, TokenLayout out_layout, bool brackets) {
      _out = out;
      _out_layout = out_layout;
      _brackets = brackets;
      if (_terminal && !_scratch) {
        _scratch = std::make_unique<TokenSink>();
        _scratch->Bind(_rebase, nullptr);
      }
    }

    void SetThrough(TokenConsumer* through) {
      _through = through;
      if (through && !_cruns) {
        _cruns = std::make_unique<DocRun[]>(TokenBatch::kCapacity + 1);
      }
    }

    IRS_FORCE_INLINE void BindValue(duckdb::string_t value) noexcept {
      _value = value;
      _data = nullptr;
      _open_doc = doc_limits::invalid();
    }

    void SetAscii(bool ascii) noexcept { _ascii = ascii; }

    void BindColumn(const duckdb::UnifiedVectorFormat& fmt,
                    const duckdb::string_t* data, doc_id_t first_doc) noexcept {
      _sel = fmt.sel;
      _data = data;
      _first_doc = first_doc;
      _open_doc = doc_limits::invalid();
    }

    size_t ScratchBytes() const noexcept {
      return _scratch ? sizeof(TokenSink) : 0;
    }

    void Consume(TokenBatch& batch, DocRuns runs) final;

    TokenSink writer;

   private:
    struct Rebase final : TokenConsumer {
      void Consume(TokenBatch& batch, DocRuns runs) final {
        link->RebaseConsume(batch, runs);
      }

      ChainSink* link = nullptr;
    };

    const uint64_t* RunFilters(TokenBatch& batch);

    void PassThrough(TokenBatch& batch, DocRuns runs, const uint64_t* valid);

    template<TokenLayout L, bool Explicit>
    void Compact(TokenBatch& batch, DocRuns runs, const uint64_t* valid);

    void ExpandBatch(TokenBatch& batch, DocRuns runs, const uint64_t* valid);

    void DriveTerminal(TokenBatch& batch, DocRuns runs, const uint64_t* valid);

    void RebaseConsume(TokenBatch& batch, DocRuns runs);

    template<TokenLayout L>
    void RebaseRun(const TokenBatch& batch, uint32_t first, uint32_t end,
                   uint32_t parent);

    void AdvanceToParent(uint32_t parent);

    void NextSourceRun();

    void FinishSourceBatch(bool tail_open);

    void DeliverBatch(TokenBatch& batch, DocRuns runs, const uint64_t* valid);

    template<TokenLayout L, bool HasDrop, bool Stable, bool Dense>
    void Deliver(const TokenBatch& batch, DocRuns runs, const uint64_t* valid);

    static constexpr uint32_t kNoParent = std::numeric_limits<uint32_t>::max();

    IRS_FORCE_INLINE void OpenSource(doc_id_t doc) {
      if (_data) {
        _value = _data[_sel->get_index(doc - _first_doc)];
      }
      if (_brackets) {
        _out->BeginValue(doc, static_cast<uint32_t>(_value.GetSize()));
      }
      _pos.Bind(_in_dense);
      _open_doc = doc;
    }

    IRS_FORCE_INLINE void OpenRun(doc_id_t doc) {
      SDB_ASSERT(doc_limits::valid(doc));
      if (doc == _open_doc) {
        return;
      }
      if (doc_limits::valid(_open_doc)) {
        CloseSource();
      }
      OpenSource(doc);
    }

    IRS_FORCE_INLINE void CloseSource() {
      if (_brackets) {
        _out->EndValue();
      }
      _open_doc = doc_limits::invalid();
    }

    std::span<TokenStage* const> _filters;
    TokenConsumer* _through = nullptr;
    std::unique_ptr<DocRun[]> _cruns;
    TokenExpander* _expander;
    Tokenizer* _terminal;
    TokenSink* _out = nullptr;
    duckdb::string_t _value{};
    const duckdb::SelectionVector* _sel = nullptr;
    const duckdb::string_t* _data = nullptr;
    const TokenBatch* _src = nullptr;
    DocRuns _src_runs{};
    doc_id_t _first_doc = 0;
    ChainPos _pos;
    ChildPos _cp;
    std::unique_ptr<TokenSink> _scratch;
    Rebase _rebase;
    duckdb::ArenaAllocator _arena{duckdb::Allocator::DefaultAllocator()};
    TokenLayout _out_layout{};
    doc_id_t _open_doc = 0;
    uint32_t _cur_parent = 0;
    uint32_t _scan = 0;
    uint32_t _src_run = 0;
    uint32_t _src_run_end = 0;
    bool _ascii = false;
    bool _in_dense;
    bool _child_dense;
    bool _stable;
    bool _brackets = false;
    uint64_t _valid[TokenBatch::kValidWords];
  };

  void BindLinks(TokenSink& sink, TokenLayout layout, bool column);

  std::vector<Tokenizer::ptr> _pipeline;
  Tokenizer* _front = nullptr;
  TokenTraits _traits;
  BlockTraits _wanted_traits{};
  // Whether any non-front child reads the ascii block fact: only then is a
  // mixed block worth refining per value.
  bool _split_mixed_blocks = false;
  std::vector<TokenStage*> _filters;
  std::vector<std::unique_ptr<ChainSink>> _links;
  ChainSink* _head = nullptr;
  size_t _chain = 0;
  bool _interpose = false;
  TokenSink* _bound_sink = nullptr;
  TokenLayout _bound_layout{};
  bool _bound_column = false;
};

template<typename Context>
void SerdeWrite(Context ctx, const PipelineTokenizer::Options& o) {
  sdb::basics::WriteTupleOrObject(ctx, std::tie(o.children));
}

template<typename Context>
void SerdeRead(Context ctx, PipelineTokenizer::Options& o) {
  auto refs = std::tie(o.children);
  sdb::basics::ReadTupleOrObject(ctx, refs);
}

}  // namespace irs::analysis
