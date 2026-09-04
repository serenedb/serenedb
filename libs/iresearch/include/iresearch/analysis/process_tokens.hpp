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

#include <algorithm>
#include <cstring>
#include <duckdb/storage/arena_allocator.hpp>

#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/analysis/token_sink.hpp"
#include "iresearch/analysis/tokenizer.hpp"

namespace irs::analysis {

IRS_FORCE_INLINE inline duckdb::string_t ReplaceTerm(
  duckdb::string_t term, std::string_view bytes,
  duckdb::ArenaAllocator& arena) {
  const auto size = static_cast<uint32_t>(bytes.size());
  if (size <= duckdb::string_t::INLINE_LENGTH) {
    return MakeTermView(bytes.data(), size);
  }
  if (bytes.data() == term.GetData() && size == term.GetSize()) {
    return term;
  }
  auto* mem = arena.Allocate(std::max<size_t>(size, kTermViewSlack));
  std::memcpy(mem, bytes.data(), size);
  return MakeTermViewPadded(mem, size);
}

template<bool ToLower>
IRS_FORCE_INLINE inline duckdb::string_t CaseConvertTermAscii(
  duckdb::string_t term, duckdb::ArenaAllocator& arena) {
  const uint32_t size = term.GetSize();
  if (size <= duckdb::string_t::INLINE_LENGTH) [[likely]] {
    return CaseConvertTermViewAscii<ToLower>(term);
  }
  auto* mem = arena.Allocate(std::max<size_t>(size, kTermViewSlack));
  casing::CaseConvertAsciiTerm<ToLower>(reinterpret_cast<char*>(mem),
                                        term.GetData(), size);
  return MakeTermViewPadded(mem, size);
}

struct TokenStage {
  virtual bool ProcessTokens(TokenBatch& batch, BatchCtx& ctx) = 0;

 protected:
  ~TokenStage() = default;
};

template<typename Impl>
struct TypedTokenStage : TokenStage {
  bool ProcessTokens(TokenBatch& batch, BatchCtx& ctx) final {
    auto& impl = static_cast<Impl&>(*this);
    uint64_t* const valid = ctx.valid;
    duckdb::ArenaAllocator& arena = ctx.arena;
    SDB_ASSERT(valid);
    return DispatchFill(
      impl, TokenLayout::Terms, ctx.traits,
      [&](auto layout_tag, auto... tags) IRS_FORCE_INLINE {
        bool all_kept = true;
        for (uint32_t base = 0, n = batch.count; base < n; base += 64) {
          const auto end = std::min<uint32_t>(n, base + 64);
          uint64_t word = valid[base >> 6];
          uint64_t marks = 0;
          for (uint32_t i = base; i < end; ++i) {
            if (((word >> (i & 63)) & 1) == 0) {
              continue;
            }
            StageSink sink{&batch.terms[i], &arena};
            impl.template DoFill<layout_tag(), tags()...>(batch.terms[i], sink);
            marks |= static_cast<uint64_t>(!sink.Emitted()) << (i & 63);
          }
          if (marks != 0) {
            valid[base >> 6] = word & ~marks;
            all_kept = false;
          }
        }
        return all_kept;
      });
  }

 private:
  class StageSink final {
   public:
    duckdb::string_t* _slot;
    duckdb::ArenaAllocator* _arena;
    bool _emitted = false;

    IRS_FORCE_INLINE bool Emitted() const noexcept { return _emitted; }

    template<TokenLayout L, EmitTag... Rest>
    IRS_FORCE_INLINE void Emit(const duckdb::string_t& term, Rest...) noexcept {
      if (_slot != &term) {
        *_slot = term;
      }
      _emitted = true;
    }

    template<TokenLayout L, EmitTag... Rest>
    IRS_FORCE_INLINE void Emit(const duckdb::string_t& value, const char* data,
                               uint32_t size, Rest...) {
      *_slot = ReplaceTerm(value, {data, size}, *_arena);
      _emitted = true;
    }

    template<TokenLayout L, EmitTag... Rest>
    IRS_FORCE_INLINE void Emit(const char* data, uint32_t size,
                               const char* limit, Rest...) {
      if (size <= duckdb::string_t::INLINE_LENGTH) {
        *_slot = MakeTermView(data, size, limit);
      } else {
        auto* mem = _arena->Allocate(std::max<size_t>(size, kTermViewSlack));
        std::memcpy(mem, data, size);
        *_slot = MakeTermViewPadded(mem, size);
      }
      _emitted = true;
    }

    template<TokenLayout L, EmitTag... Rest>
    IRS_FORCE_INLINE void Emit(const char* data, uint32_t size, Rest... rest) {
      Emit<L>(data, size, data + size, rest...);
    }

    template<TokenLayout L, EmitTag... Rest>
    IRS_FORCE_INLINE void Emit(const byte_type* data, uint32_t size,
                               Rest... rest) {
      Emit<L>(reinterpret_cast<const char*>(data), size, rest...);
    }

    template<TokenLayout L, typename Build, typename... Rest>
      requires std::is_integral_v<std::invoke_result_t<Build&, byte_type*>>
    IRS_FORCE_INLINE void Emit(size_t size, Build build, Rest...) {
      if (size <= duckdb::string_t::INLINE_LENGTH) {
        alignas(duckdb::string_t) byte_type tmp[sizeof(duckdb::string_t)]{};
        const auto n = static_cast<uint32_t>(build(tmp + sizeof(uint32_t)));
        SDB_ASSERT(n <= size);
        std::memcpy(tmp, &n, sizeof n);
        *_slot = std::bit_cast<duckdb::string_t>(tmp);
      } else {
        auto* mem = _arena->Allocate(std::max(size, kTermViewSlack));
        const auto n = static_cast<uint32_t>(build(mem));
        SDB_ASSERT(n <= size);
        *_slot = MakeTermViewPadded(mem, n);
        _arena->ShrinkHead(std::max(size, kTermViewSlack) -
                           std::max<size_t>(n, kTermViewSlack));
      }
      _emitted = true;
    }

    template<TokenLayout L, bool Lower, EmitTag... Rest>
    IRS_FORCE_INLINE void EmitCaseConverted(const duckdb::string_t& value,
                                            Rest...) {
      *_slot = CaseConvertTermAscii<Lower>(value, *_arena);
      _emitted = true;
    }
  };
};

}  // namespace irs::analysis
