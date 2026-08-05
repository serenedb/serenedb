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
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/storage/arena_allocator.hpp>
#include <limits>
#include <magic_enum/magic_enum_switch.hpp>
#include <memory>
#include <span>
#include <type_traits>
#include <utility>

#include "basics/assert.h"
#include "basics/noncopyable.hpp"
#include "iresearch/analysis/term_view.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

struct TokenTraits {
  // Value type the kernel consumes and token type it produces; drives
  // catalog binding and pipeline stage compatibility.
  duckdb::LogicalTypeId input = duckdb::LogicalTypeId::VARCHAR;
  duckdb::LogicalTypeId output = duckdb::LogicalTypeId::VARCHAR;
  // At most one token per value, the 1-1 ingest fast path.
  bool unique = false;
  // Emits the value verbatim as its single token, use the verbatim
  // keyword block route.
  bool keyword = false;
  // Writes the pos lane itself, otherwise positions are dense ordinals
  // derived by the consumer.
  bool explicit_pos = false;
  // Produces value-relative offsets, required for TermsPosOffs fields.
  bool offsets = false;
  // Calls Store() with a per-value blob during Fill.
  bool store = false;
};

enum class TokenLayout : uint8_t {
  Terms = 0,
  TermsPos = 1,
  TermsPosOffs = 2,
};

template<typename Visitor>
IRS_FORCE_INLINE constexpr decltype(auto) ResolveLayout(TokenLayout layout,
                                                        Visitor&& visit) {
  switch (layout) {
    case TokenLayout::Terms:
      return visit.template operator()<TokenLayout::Terms>();
    case TokenLayout::TermsPos:
      return visit.template operator()<TokenLayout::TermsPos>();
    case TokenLayout::TermsPosOffs:
      return visit.template operator()<TokenLayout::TermsPosOffs>();
  }
  std::unreachable();
}

template<typename Visitor>
IRS_FORCE_INLINE constexpr decltype(auto) ResolveValues(Visitor&& visit) {
  return visit();
}

template<typename Visitor, typename T, typename... Ts>
IRS_FORCE_INLINE constexpr decltype(auto) ResolveValues(Visitor&& visit,
                                                        T value, Ts... rest) {
  const auto with = [&](auto tag) IRS_FORCE_INLINE -> decltype(auto) {
    if constexpr (sizeof...(rest) == 0) {
      return visit(tag);
    } else {
      return ResolveValues(
        [&](auto... tags)
          IRS_FORCE_INLINE -> decltype(auto) { return visit(tag, tags...); },
        rest...);
    }
  };
  if constexpr (std::is_same_v<T, bool>) {
    return value ? with(std::true_type{}) : with(std::false_type{});
  } else if constexpr (std::is_same_v<T, TokenLayout>) {
    return ResolveLayout(
      value, [&]<TokenLayout L>() IRS_FORCE_INLINE -> decltype(auto) {
        return with(std::integral_constant<TokenLayout, L>{});
      });
  } else {
    static_assert(std::is_enum_v<T>);
    return magic_enum::enum_switch(with, value);
  }
}

struct DocRun {
  uint32_t doc;
  uint32_t ntokens;
};

struct DocRuns : std::span<const DocRun> {
  bool tail_open = false;
};

struct TokenBatch {
  static constexpr size_t kCapacity = 1024;

  duckdb::string_t terms[kCapacity];
  uint32_t pos[kCapacity];
  uint32_t offs_start[kCapacity];
  uint32_t offs_end[kCapacity];
  uint32_t count = 0;

  bool Full() const noexcept { return count == kCapacity; }

  std::span<const duckdb::string_t> Terms() const noexcept {
    return {terms, count};
  }
};

struct TokenConsumer {
  virtual void Consume(TokenBatch& batch, DocRuns runs) = 0;

  virtual void OnStore(doc_id_t /*doc*/, bytes_view /*store*/) {}

 protected:
  ~TokenConsumer() = default;
};

struct Offs {
  uint32_t start;
  uint32_t end;
};

struct EmitKSlot {
  uint32_t begin;
  uint32_t end;
  Offs offs{};
};

struct PosOffs {
  Offs offs;
  uint32_t pos;
};

struct Pos {
  uint32_t value;
};

struct PosSeq {
  uint32_t base;
};

class TokenSink final : util::Noncopyable {
 public:
  explicit TokenSink(
    duckdb::Allocator& alloc = duckdb::Allocator::DefaultAllocator())
    : _arena{alloc} {}

  void Bind(TokenConsumer& consumer, TokenConsumer* store) noexcept {
    SDB_ASSERT((_batch.count == 0 && _nruns == 0) ||
               (&consumer == _consumer && store == _store_consumer));
    _consumer = &consumer;
    _store_consumer = store;
  }

  // `value_size` feeds the default whole-value offsets: an Offs-layout
  // emit that passes no Offs tag gets {0, value_size} (kernels whose every
  // token spans the whole value never spell it out; scanners pass real
  // per-token Offs, which always win). Unbracketed fills have no value
  // bracket and therefore no default offsets.
  void BeginValue(doc_id_t doc, uint32_t value_size) noexcept {
    _doc = doc;
    _value_size = value_size;
    _run_start = _batch.count;
  }

  void EndValue() {
    SDB_ASSERT(_run_start != kOutsideValue);
    _runs[_nruns++] = {_doc, _batch.count - _run_start};
    _run_start = kOutsideValue;
    _value_size = 0;
    if (_nruns == TokenBatch::kCapacity) [[unlikely]] {
      Flush();
    }
  }

  void Finish() {
    SDB_ASSERT(_run_start == kOutsideValue);
    if (_batch.count || _nruns) {
      _consumer->Consume(_batch, {{_runs, _nruns}});
      Reset();
    }
  }

  void Discard() {
    _run_start = kOutsideValue;
    Reset();
  }

  std::span<const DocRun> Runs() const noexcept { return {_runs, _nruns}; }

  template<TokenLayout L, typename... Rest>
  IRS_FORCE_INLINE void Emit(duckdb::string_t term, Rest... rest) {
    const auto i = Next();
    _batch.terms[i] = term;
    FillLanes<L>(i, 0, rest...);
  }

  // Inline-size builds write straight into the batch slot's inline bytes:
  // stores only, no read-back of just-written data (a stack staging buffer
  // re-loaded by the view build stalls store-to-load forwarding per token).
  template<TokenLayout L, typename Build, typename... Rest>
    requires std::is_integral_v<std::invoke_result_t<Build&, byte_type*>>
  IRS_FORCE_INLINE void Emit(size_t size, Build build, Rest... rest) {
    const auto i = Next();
    if (size <= duckdb::string_t::INLINE_LENGTH) {
      auto& slot = _batch.terms[i];
      std::memset(&slot, 0, sizeof slot);
      const auto n = static_cast<uint32_t>(
        build(reinterpret_cast<byte_type*>(&slot) + sizeof(uint32_t)));
      SDB_ASSERT(n <= size);
      std::memcpy(&slot, &n, sizeof n);
    } else {
      auto* mem = _arena.Allocate(std::max(size, kTermViewSlack));
      _batch.terms[i] =
        MakeTermViewPadded(mem, static_cast<uint32_t>(build(mem)));
    }
    FillLanes<L>(i, 0, rest...);
  }

  // The generator returns Offs (pos comes from the trailing tags) or PosOffs
  // (pos comes from the generator; generators are invoked in index order, so
  // stateful cursors are fine).
  template<TokenLayout L, typename Gen, typename... P>
  IRS_FORCE_INLINE void EmitK(size_t k, const byte_type* base, Gen gen,
                              P... pos) {
    static_assert(
      !std::is_same_v<std::invoke_result_t<Gen&, size_t>, PosOffs> ||
        sizeof...(P) == 0,
      "pos comes from the generator");
    const auto put = [&](uint32_t slot, size_t i, auto t) IRS_FORCE_INLINE {
      if constexpr (std::is_same_v<decltype(t), PosOffs>) {
        _batch.terms[slot] =
          MakeTermView(base + t.offs.start, t.offs.end - t.offs.start);
        FillLanes<L>(slot, i, Pos{t.pos}, t.offs);
      } else {
        _batch.terms[slot] = MakeTermView(base + t.start, t.end - t.start);
        FillLanes<L>(slot, i, pos..., t);
      }
    };
    size_t done = 0;
    while (done < k) {
      const auto slots = Next(k - done);
      const auto first = static_cast<uint32_t>(slots.data() - _batch.terms);
      for (size_t j = 0; j < slots.size(); ++j) {
        put(first + static_cast<uint32_t>(j), done + j, gen(done + j));
      }
      done += slots.size();
    }
  }

  // Staged bulk emit: per wave the sink allocates a `size`-byte block,
  // `stage(mem, first)` (re)builds it, then each `gen(j, mem)` returns the
  // view bounds within the block (+ the offs lane when it differs).
  template<TokenLayout L, typename Stage, typename Gen, typename... P>
  IRS_FORCE_INLINE void EmitK(size_t k, size_t size, Stage stage, Gen gen,
                              P... pos) {
    size_t done = 0;
    while (done < k) {
      const auto slots = Next(k - done);
      const auto first = static_cast<uint32_t>(slots.data() - _batch.terms);
      auto* mem = _arena.Allocate(size + kTermViewSlack);
      stage(mem, done);
      for (size_t j = 0; j < slots.size(); ++j) {
        const EmitKSlot s = gen(done + j, mem);
        slots[j] = MakeTermViewPadded(mem + s.begin, s.end - s.begin);
        FillLanes<L>(first + static_cast<uint32_t>(j), done + j, pos...,
                     s.offs);
      }
      done += slots.size();
    }
  }

  void Store(bytes_view blob) {
    if (_store_consumer) {
      _store_consumer->OnStore(_doc, blob);
    }
  }

 private:
  uint32_t Next() {
    if (_batch.Full()) [[unlikely]] {
      Flush();
    }
    return _batch.count++;
  }

  std::span<duckdb::string_t> Next(size_t want) {
    if (_batch.Full()) [[unlikely]] {
      Flush();
    }
    const auto first = _batch.count;
    const auto got = static_cast<uint32_t>(
      std::min<size_t>(want, TokenBatch::kCapacity - first));
    _batch.count += got;
    return {_batch.terms + first, got};
  }

  template<typename T>
  static constexpr bool kIsPosTag =
    std::is_integral_v<T> || std::is_same_v<T, Pos> ||
    std::is_same_v<T, PosSeq>;

  template<TokenLayout L, typename... Tags>
  IRS_FORCE_INLINE void FillLanes(uint32_t i, size_t j, Tags... tags) {
    constexpr auto kNumPos = (0 + ... + (kIsPosTag<Tags> ? 1 : 0));
    constexpr auto kNumOffs = (0 + ... + (std::is_same_v<Tags, Offs> ? 1 : 0));
    static_assert(kNumPos <= 1 && kNumOffs <= 1, "duplicate emit tag");
    static_assert(kNumPos + kNumOffs == sizeof...(Tags), "unknown emit tag");
    const auto apply = [&]<typename T>(T tag) {
      if constexpr (std::is_same_v<T, Offs>) {
        if constexpr (L == TokenLayout::TermsPosOffs) {
          _batch.offs_start[i] = tag.start;
          _batch.offs_end[i] = tag.end;
        }
      } else if constexpr (L != TokenLayout::Terms) {
        if constexpr (std::is_same_v<T, PosSeq>) {
          _batch.pos[i] = tag.base + static_cast<uint32_t>(j);
        } else if constexpr (std::is_same_v<T, Pos>) {
          _batch.pos[i] = tag.value;
        } else {
          _batch.pos[i] = tag;
        }
      }
    };
    (apply(tags), ...);
    if constexpr (L == TokenLayout::TermsPosOffs && kNumOffs == 0) {
      apply(Offs{0, _value_size});
    }
  }

  void Flush() {
    bool tail_open = false;
    if (_run_start != kOutsideValue && _batch.count > _run_start) {
      _runs[_nruns++] = {_doc, _batch.count - _run_start};
      tail_open = true;
    }
    _consumer->Consume(_batch, {{_runs, _nruns}, tail_open});
    Reset();
  }

  void Reset() {
    _batch.count = 0;
    _nruns = 0;
    _arena.Reset();
    if (const auto* head = _arena.GetHead();
        head && head->maximum_size > kMaxRetainedArenaBytes) [[unlikely]] {
      _arena.Destroy();
    }
    if (_run_start != kOutsideValue) {
      _run_start = 0;
    }
  }

  static constexpr size_t kMaxRetainedArenaBytes = 64 * 1024;
  static constexpr uint32_t kOutsideValue =
    std::numeric_limits<uint32_t>::max();

  static TokenConsumer& Noop() {
    static struct Impl final : TokenConsumer {
      void Consume(TokenBatch&, DocRuns) final {}
    } gNoop;
    return gNoop;
  }

  TokenBatch _batch;
  DocRun _runs[TokenBatch::kCapacity + 1];
  duckdb::ArenaAllocator _arena;
  TokenConsumer* _consumer = &Noop();
  TokenConsumer* _store_consumer = nullptr;
  doc_id_t _doc = 0;
  uint32_t _value_size = 0;
  uint32_t _nruns = 0;
  uint32_t _run_start = kOutsideValue;
};
}  // namespace irs
