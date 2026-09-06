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
#include <bit>
#include <cstdint>
#include <cstring>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/storage/arena_allocator.hpp>
#include <limits>
#include <magic_enum/magic_enum_switch.hpp>
#include <span>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

#include "basics/assert.h"
#include "basics/noncopyable.hpp"
#include "basics/shared.hpp"
#include "iresearch/analysis/text/case/case.hpp"
#include "iresearch/analysis/text/term_view.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/utf8_character_utils.hpp"
#include "iresearch/utils/utf8_utils.hpp"

namespace irs {

template<bool ToLower>
IRS_FORCE_INLINE inline duckdb::string_t CaseConvertTermViewAscii(
  duckdb::string_t view) noexcept {
  SDB_ASSERT(view.GetSize() <= duckdb::string_t::INLINE_LENGTH);
  using Block = uint8_t __attribute__((vector_size(sizeof(duckdb::string_t))));
  constexpr uint8_t kLo = ToLower ? 'A' : 'a';
  constexpr uint8_t kHi = ToLower ? 'Z' : 'z';
  auto b = std::bit_cast<Block>(view);
  b ^= std::bit_cast<Block>((b >= kLo) & (b <= kHi)) & uint8_t{0x20};
  return std::bit_cast<duckdb::string_t>(b);
}

template<typename T, size_t I, typename With>
IRS_FORCE_INLINE constexpr decltype(auto) ResolveEnum(T value, With& with) {
  constexpr auto kValue = magic_enum::enum_value<T>(I);
  if constexpr (I + 1 == magic_enum::enum_count<T>()) {
    return with(std::integral_constant<T, kValue>{});
  } else {
    if (value == kValue) {
      return with(std::integral_constant<T, kValue>{});
    }
    return ResolveEnum<T, I + 1>(value, with);
  }
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
    SDB_ASSERT(magic_enum::enum_contains(value),
               "fill dispatch: option enum out of range");
    return ResolveEnum<T, 0>(value, with);
  }
}

template<typename Impl, typename Fill>
IRS_FORCE_INLINE constexpr decltype(auto) DispatchFill(Impl& impl,
                                                       TokenLayout layout,
                                                       BlockTraits traits,
                                                       Fill&& fill) {
  constexpr auto kNumTags =
    std::tuple_size_v<decltype(std::declval<Impl&>().PrepareBatch(
      std::declval<BlockTraits>()))>;
  return [&]<size_t... I>(std::index_sequence<I...>)
           IRS_FORCE_INLINE -> decltype(auto) {
             [[maybe_unused]] const auto tags = impl.PrepareBatch(traits);
             return ResolveValues(std::forward<Fill>(fill), layout,
                                  std::get<I>(tags)...);
           }(std::make_index_sequence<kNumTags>{});
}

struct Offs {
  uint32_t start;
  uint32_t end;
};

struct EmitKSlot {
  uint32_t begin;
  uint32_t end;
};

struct EmitKSlotPos {
  uint32_t begin;
  uint32_t end;
  uint32_t pos;
};

struct EmitKSlotOffs {
  uint32_t begin;
  uint32_t end;
  Offs offs;
};

template<typename T>
concept EmitTag = std::is_integral_v<T> || std::is_same_v<T, Offs>;

class TokenSink final : util::Noncopyable {
 public:
  explicit TokenSink(
    duckdb::Allocator& alloc = duckdb::Allocator::DefaultAllocator())
    : _arena{alloc} {}

  void Bind(TokenConsumer& consumer, StoreSink* store) noexcept {
    SDB_ASSERT((_batch.count == 0 && _nruns == 0) ||
               (&consumer == _consumer && store == _store_consumer));
    _consumer = &consumer;
    _store_consumer = store;
  }

  TokenConsumer* Rebind(TokenConsumer& consumer) noexcept {
    SDB_ASSERT(_batch.count == 0 && _nruns == 0);
    return std::exchange(_consumer, &consumer);
  }

  void BeginValue(doc_id_t doc, uint32_t value_size) noexcept {
    _doc = doc;
    _value_size = value_size;
    _run_start = _batch.count;
  }

  void EndValue() {
    SDB_ASSERT(_run_start != kOutsideValue);
    SDB_ASSERT(_nruns < TokenBatch::kCapacity);
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

  void RewindValue() noexcept {
    SDB_ASSERT(_run_start != kOutsideValue);
    _batch.count = _run_start;
  }

  std::span<const DocRun> Runs() const noexcept { return {_runs, _nruns}; }

  template<TokenLayout L, EmitTag... Rest>
  IRS_FORCE_INLINE void Emit(duckdb::string_t term, Rest... rest) {
    const auto i = Next();
    _batch.terms[i] = term;
    FillLanes<L>(i, rest...);
  }

  template<TokenLayout L, EmitTag... Rest>
  IRS_FORCE_INLINE void Emit(const duckdb::string_t& value, const char* data,
                             uint32_t size, Rest... rest) {
    const auto i = Next();
    const char* vbeg = value.GetData();
    const char* vend = vbeg + value.GetSize();
    const bool in_value = data >= vbeg && data + size <= vend;
    if (in_value || size <= duckdb::string_t::INLINE_LENGTH) [[likely]] {
      StoreTermView(&_batch.terms[i], data, size,
                    in_value ? vend : data + size);
    } else {
      _batch.terms[i] = CopyTerm(data, size);
    }
    FillLanes<L>(i, rest...);
  }

  template<TokenLayout L, EmitTag... Rest>
  IRS_FORCE_INLINE void Emit(const char* data, uint32_t size, const char* limit,
                             Rest... rest) {
    const auto i = Next();
    if (size <= duckdb::string_t::INLINE_LENGTH) [[likely]] {
      StoreTermView(&_batch.terms[i], data, size, limit);
    } else {
      _batch.terms[i] = CopyTerm(data, size);
    }
    FillLanes<L>(i, rest...);
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

  template<TokenLayout L, EmitTag... Rest>
  IRS_FORCE_INLINE void EmitSlice(const char* base, const char* limit,
                                  Offs offs, Rest... rest) {
    Emit<L>(MakeTermView(base + offs.start, offs.end - offs.start, limit),
            rest..., offs);
  }

  template<TokenLayout L, EmitTag... Rest>
  IRS_FORCE_INLINE void EmitSlice(const byte_type* base, const byte_type* limit,
                                  Offs offs, Rest... rest) {
    EmitSlice<L>(reinterpret_cast<const char*>(base),
                 reinterpret_cast<const char*>(limit), offs, rest...);
  }

  template<TokenLayout L, bool Lower, EmitTag... Rest>
  IRS_FORCE_INLINE void EmitCaseConverted(const duckdb::string_t& value,
                                          Rest... rest) {
    const auto size = static_cast<uint32_t>(value.GetSize());
    if (size <= duckdb::string_t::INLINE_LENGTH) [[likely]] {
      Emit<L>(CaseConvertTermViewAscii<Lower>(value), rest...);
    } else {
      const char* const data = value.GetData();
      Emit<L>(
        size,
        [&](byte_type* out) IRS_FORCE_INLINE {
          analysis::casing::CaseConvertAsciiTerm<Lower>(
            reinterpret_cast<char*>(out), data, size);
          return size;
        },
        rest...);
    }
  }

  template<TokenLayout L, bool Lower, EmitTag... Rest>
  IRS_FORCE_INLINE void EmitSliceCaseConverted(const char* base,
                                               const char* limit, Offs offs,
                                               Rest... rest) {
    SDB_ASSERT(offs.start <= offs.end && base + offs.end <= limit);
    const uint32_t size = offs.end - offs.start;
    if (size <= duckdb::string_t::INLINE_LENGTH) [[likely]] {
      Emit<L>(CaseConvertTermViewAscii<Lower>(
                MakeTermView(base + offs.start, size, limit)),
              rest..., offs);
    } else {
      Emit<L>(
        size,
        [&](byte_type* out) IRS_FORCE_INLINE {
          analysis::casing::CaseConvertAsciiTerm<Lower>(
            reinterpret_cast<char*>(out), base + offs.start, size);
          return size;
        },
        rest..., offs);
    }
  }

  template<TokenLayout L, bool Lower, EmitTag... Rest>
  IRS_NO_INLINE void EmitCaseConvertedUtf8(std::string_view bytes,
                                           Rest... rest) {
    if (bytes.size() <= duckdb::string_t::INLINE_LENGTH) {
      byte_type tmp[analysis::casing::CaseConvertUtf8Bound(
        duckdb::string_t::INLINE_LENGTH)];
      const auto n = static_cast<uint32_t>(
        analysis::casing::CaseConvertUtf8<Lower>(bytes, tmp));
      Emit<L>(tmp, n, rest...);
      return;
    }
    Emit<L>(
      analysis::casing::CaseConvertUtf8Bound(bytes.size()),
      [&](byte_type* out) IRS_FORCE_INLINE {
        return analysis::casing::CaseConvertUtf8<Lower>(bytes, out);
      },
      rest...);
  }

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
      auto* mem = AllocateTerm(size);
      const auto n = static_cast<uint32_t>(build(mem));
      SDB_ASSERT(n <= size);
      StoreTermViewPadded(&_batch.terms[i], mem, n);
      _arena.ShrinkHead(std::max(size, kTermViewSlack) -
                        std::max<size_t>(n, kTermViewSlack));
    }
    FillLanes<L>(i, rest...);
  }

  template<TokenLayout L, typename Gen>
  IRS_FORCE_INLINE void EmitK(size_t k, const byte_type* base,
                              const byte_type* limit, Gen gen) {
    const auto* chars = reinterpret_cast<const char*>(base);
    const auto* end = reinterpret_cast<const char*>(limit);
    size_t done = 0;
    while (done < k) {
      const auto slots = Next(k - done);
      const auto first = static_cast<uint32_t>(slots.data() - _batch.terms);
      for (size_t j = 0; j < slots.size(); ++j) {
        const auto s = gen(done + j);
        const auto slot = first + static_cast<uint32_t>(j);
        StoreTermView(&_batch.terms[slot], chars + s.begin, s.end - s.begin,
                      end);
        PutSlotLanes<L>(slot, s, Offs{s.begin, s.end});
      }
      done += slots.size();
    }
  }

  template<TokenLayout L, typename Stage, typename Gen>
  IRS_FORCE_INLINE void EmitK(size_t k, size_t size, Stage stage, Gen gen) {
    size_t done = 0;
    while (done < k) {
      const auto slots = Next(k - done);
      const auto first = static_cast<uint32_t>(slots.data() - _batch.terms);
      auto* mem = AllocateStaged(size);
      stage(mem);
      for (size_t j = 0; j < slots.size(); ++j) {
        const auto s = gen(done + j, mem);
        SDB_ASSERT(s.begin <= s.end && s.end <= size);
        const auto slot = first + static_cast<uint32_t>(j);
        StoreTermViewPadded(&_batch.terms[slot], mem + s.begin,
                            s.end - s.begin);
        PutSlotLanes<L>(slot, s, Offs{0, _value_size});
      }
      done += slots.size();
    }
  }

  template<TokenLayout L, bool Stable = false>
  void EmitTerms(const duckdb::string_t& value, const TokenBatch& src,
                 uint32_t first, uint32_t count, const uint64_t* valid) {
    static_assert(L != TokenLayout::TermsPosOffs);
    uint32_t remaining = count;
    if (valid) {
      remaining = CountValid(valid, first, first + count);
    }
    uint32_t si = first;
    if constexpr (Stable) {
      if (!valid) {
        while (remaining != 0) {
          const auto dst = Next(remaining);
          std::memcpy(dst.data(), src.terms + si,
                      dst.size() * sizeof(duckdb::string_t));
          si += static_cast<uint32_t>(dst.size());
          remaining -= static_cast<uint32_t>(dst.size());
        }
        return;
      }
    }
    [[maybe_unused]] const char* vbeg = value.GetData();
    [[maybe_unused]] const char* vend = vbeg + value.GetSize();
    while (remaining != 0) {
      const auto dst = Next(remaining);
      for (auto& slot : dst) {
        while (valid && !IsValid(valid, si)) {
          ++si;
        }
        const auto& term = src.terms[si++];
        if constexpr (Stable) {
          slot = term;
          continue;
        }
        const auto size = term.GetSize();
        const char* data = term.GetData();
        if (size <= duckdb::string_t::INLINE_LENGTH ||
            (data >= vbeg && data + size <= vend)) [[likely]] {
          slot = term;
        } else {
          slot = CopyTerm(data, size);
        }
      }
      remaining -= static_cast<uint32_t>(dst.size());
    }
  }

  void Store(bytes_view blob) {
    if (_store_consumer) {
      _store_consumer->OnStore(_doc, blob);
    }
  }

 private:
  IRS_FORCE_INLINE byte_type* AllocateTerm(size_t size) {
    return _arena.Allocate(std::max(size, kTermViewSlack));
  }

  IRS_FORCE_INLINE byte_type* AllocateStaged(size_t size) {
    return _arena.Allocate(size + kTermViewSlack);
  }

  IRS_NO_INLINE duckdb::string_t CopyTerm(const char* data, uint32_t size) {
    auto* mem = AllocateTerm(size);
    std::memcpy(mem, data, size);
    return MakeTermViewPadded(mem, size);
  }

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

  template<TokenLayout L, typename S>
  IRS_FORCE_INLINE void PutSlotLanes(uint32_t slot, const S& s,
                                     Offs default_offs) {
    constexpr bool kHasPos = requires { s.pos; };
    constexpr bool kHasOffs = requires { s.offs; };
    static_assert(sizeof(S) == sizeof(uint32_t) * (2 + kHasPos + 2 * kHasOffs),
                  "EmitK descriptor must be {begin,end[,pos][,offs]} u32 "
                  "lanes; a misspelled pos/offs field silently falls back to "
                  "the default lanes");
    if constexpr (kHasPos && kHasOffs) {
      FillLanes<L>(slot, s.pos, s.offs);
    } else if constexpr (kHasPos) {
      FillLanes<L>(slot, s.pos, default_offs);
    } else if constexpr (kHasOffs) {
      FillLanes<L>(slot, s.offs);
    } else {
      FillLanes<L>(slot, default_offs);
    }
  }

  template<TokenLayout L, typename... Tags>
  IRS_FORCE_INLINE void FillLanes(uint32_t i, Tags... tags) {
    constexpr auto kNumPos = (0 + ... + (std::is_integral_v<Tags> ? 1 : 0));
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
        _batch.pos[i] = tag;
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
      SDB_ASSERT(_nruns < kMaxRuns);
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
  static constexpr uint32_t kMaxRuns = TokenBatch::kCapacity + 1;
  static constexpr uint32_t kOutsideValue =
    std::numeric_limits<uint32_t>::max();

  static TokenConsumer& Noop() {
    static struct Impl final : TokenConsumer {
      void Consume(TokenBatch&, DocRuns) final {}
    } gNoop;
    return gNoop;
  }

  TokenBatch _batch;
  DocRun _runs[kMaxRuns];
  duckdb::ArenaAllocator _arena;
  TokenConsumer* _consumer = &Noop();
  StoreSink* _store_consumer = nullptr;
  doc_id_t _doc = 0;
  uint32_t _value_size = 0;
  uint32_t _nruns = 0;
  uint32_t _run_start = kOutsideValue;
};

}  // namespace irs
