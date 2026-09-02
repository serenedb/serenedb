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

#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/analysis/token_sink.hpp"
#include "iresearch/analysis/tokenizer.hpp"

namespace irs::analysis {

class ChainPos {
 public:
  void Bind(bool dense) noexcept {
    _dense = dense;
    _last = 0;
    _out = 0;
  }

  IRS_FORCE_INLINE uint32_t Observe(const TokenBatch& batch,
                                    uint32_t i) noexcept {
    if (_dense) {
      return 1;
    }
    const uint32_t pos = batch.pos[i];
    const uint32_t inc = pos - _last;
    _last = pos;
    return inc;
  }

  IRS_FORCE_INLINE uint32_t Commit(uint32_t inc) noexcept {
    return _out += inc;
  }

 private:
  uint32_t _last = 0;
  uint32_t _out = 0;
  bool _dense = true;
};

struct ChildPos {
  uint32_t _parent_inc = 0;
  uint32_t _run_last = 0;
  bool _first = true;

  IRS_FORCE_INLINE uint32_t Next(uint32_t child_pos) noexcept {
    uint32_t inc = child_pos - _run_last;
    _run_last = child_pos;
    if (_first) {
      inc += _parent_inc;
      SDB_ASSERT(inc > 0);
      --inc;
      _first = false;
    }
    return inc;
  }

  IRS_FORCE_INLINE uint32_t Last() const noexcept { return _run_last; }
};

IRS_FORCE_INLINE inline Offs RebaseOffs(uint32_t parent_start,
                                        uint32_t parent_end,
                                        uint32_t parent_size,
                                        Offs child) noexcept {
  return {parent_start + child.start,
          child.end == parent_size ? parent_end : parent_start + child.end};
}

struct ExpandCtx {
  TokenLayout layout = TokenLayout::Terms;
  BlockTraits traits{};
  duckdb::string_t source{};
  ChainPos* pos = nullptr;
  const uint64_t* valid = nullptr;
};

struct TokenExpander {
  virtual void ExpandTokens(const TokenBatch& batch, uint32_t first,
                            uint32_t end, TokenSink& out, ExpandCtx ctx) = 0;

 protected:
  ~TokenExpander() = default;
};

template<typename Impl>
struct TypedTokenExpander : TokenExpander {
  void ExpandTokens(const TokenBatch& batch, uint32_t first, uint32_t end,
                    TokenSink& out, ExpandCtx ctx) final {
    auto& impl = static_cast<Impl&>(*this);
    const uint64_t* const valid = ctx.valid;
    ChainPos& pos = *ctx.pos;
    DispatchFill(
      impl, ctx.layout, ctx.traits,
      [&](auto layout_tag, auto... tags) IRS_FORCE_INLINE {
        constexpr auto kLayout = layout_tag();
        for (uint32_t i = first; i < end; ++i) {
          uint32_t inc = 1;
          if constexpr (kLayout != TokenLayout::Terms) {
            inc = pos.Observe(batch, i);
          }
          if (valid && !IsValid(valid, i)) {
            continue;
          }
          Offs parent_offs{};
          if constexpr (kLayout == TokenLayout::TermsPosOffs) {
            parent_offs = {batch.offs_start[i], batch.offs_end[i]};
          }
          ExpandSink sink{&out, ctx.source, &pos, ChildPos{inc}, parent_offs};
          impl.template DoFill<kLayout, tags()...>(batch.terms[i], sink);
        }
      });
  }

 private:
  struct ExpandSink final {
    TokenSink* _out;
    duckdb::string_t _value;
    ChainPos* _pos;
    ChildPos _cp;
    Offs _parent_offs;

    template<TokenLayout L>
    IRS_FORCE_INLINE void Emit(duckdb::string_t term) {
      EmitAt<L>(term, _cp.Last() + 1);
    }

    template<TokenLayout L>
    IRS_FORCE_INLINE void Emit(duckdb::string_t term, uint32_t child_pos) {
      EmitAt<L>(term, child_pos);
    }

   private:
    template<TokenLayout L>
    IRS_FORCE_INLINE void EmitAt(duckdb::string_t term, uint32_t child_pos) {
      Forward<L>(child_pos, [&](auto... tags) IRS_FORCE_INLINE {
        _out->Emit<L>(_value, term.GetData(),
                      static_cast<uint32_t>(term.GetSize()), tags...);
      });
    }

    template<TokenLayout L, typename Fwd>
    IRS_FORCE_INLINE void Forward(uint32_t child_pos, Fwd fwd) {
      if constexpr (L == TokenLayout::Terms) {
        fwd();
      } else {
        const uint32_t pos = _pos->Commit(_cp.Next(child_pos));
        if constexpr (L == TokenLayout::TermsPosOffs) {
          fwd(pos, _parent_offs);
        } else {
          fwd(pos);
        }
      }
    }
  };
};

}  // namespace irs::analysis
