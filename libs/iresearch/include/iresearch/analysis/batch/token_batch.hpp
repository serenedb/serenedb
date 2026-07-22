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
#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "basics/assert.h"
#include "basics/noncopyable.hpp"
#include "iresearch/analysis/batch/term_view.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

inline std::string_view AsView(const duckdb::string_t& s) noexcept {
  return {s.GetData(), s.GetSize()};
}

// Level-1 metadata: the kernel's STATIC contract, consumed at catalog time
// (feature validation against capabilities) and by drivers for routing
// (verbatim keyword block path, single-token 1-1 ingest). Orthogonal to what
// a field requests (TokenLayout) and to per-batch runtime hints (see
// TokenBatch): traits never change per fill.
struct TokenTraits {
  duckdb::LogicalTypeId input = duckdb::LogicalTypeId::VARCHAR;
  duckdb::LogicalTypeId output = duckdb::LogicalTypeId::VARCHAR;
  bool unique = false;
  bool keyword = false;
  bool dense_pos = true;
  bool offsets = true;
  bool store = false;
};

// What a token stream carries; doubles as the occurrence-log layout. The
// driver requests exactly the field's layout so kernels never produce and the
// inverter never validates data the field does not index.
enum class TokenLayout : uint8_t {
  Terms = 0,
  TermsPos = 1,
  TermsPosOffs = 2,
};

// Resolves a runtime layout to a compile-time constant once per call so
// fill loops branch with `if constexpr` instead of per-token runtime ifs.
template<typename Visitor>
constexpr decltype(auto) ResolveLayout(TokenLayout layout, Visitor&& visit) {
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

// Doc segmentation for block ingest: `ntokens` consecutive batch tokens
// belong to `doc`. Runs are doc-ascending; consecutive runs may share a doc
// (multiple values). A span of these travels NEXT TO the TokenBatch -- the
// ingest side owns the doc mapping, tokenizers only produce tokens.
struct DocRun {
  uint32_t doc;
  uint32_t ntokens;
  static constexpr uint32_t kOpenValue = doc_limits::eof();
};

// Fixed-capacity columnar token batch, the unit of transfer between tokenizer
// producers and FieldInverter::InvertBlock. One contiguous POD block (~40KB),
// owned by the producer and reused across values/chunks; a value whose tokens
// exceed kCapacity streams through several batches (positions keep
// prefix-summing, the inverter's continuation state handles the rest).
//
// pos[i] is the prefix sum of legacy position increments within the value;
// offs_* are value-relative, meaningful only when has_offs.
//
// Terms travel as bytes (string_t), valid only until the consume cycle that
// drains them returns. Resolution into the field dictionary happens once, in
// the inverter (validate-then-record), never at emit.
struct TokenBatch {
  static constexpr size_t kCapacity = 1024;

  duckdb::string_t terms[kCapacity];
  uint32_t pos[kCapacity];
  uint32_t offs_start[kCapacity];
  uint32_t offs_end[kCapacity];
  uint32_t count = 0;

  bool Full() const noexcept { return count == kCapacity; }
};

// Consumer side of a push-model fill: receives fully formed batches. `runs`
// segments the batch by doc (one run per value; a kOpenValue continuation
// follows when a value spans batches; empty for fixed-stride fills, where
// the consumer maps docs itself). Term bytes are valid only for the duration
// of the call -- consume or copy, never retain views.
struct TokenConsumer {
  virtual void Consume(TokenBatch& batch, std::span<const DocRun> runs) = 0;

  // Receives a value's stored blob (store-producing tokenizers call
  // TokenEmitter::Store during Fill). The blob bytes are valid only for the
  // duration of the call.
  virtual void OnStore(doc_id_t /*doc*/, bytes_view /*store*/) {}

 protected:
  ~TokenConsumer() = default;
};

// Kernel-facing half of the push protocol: everything a tokenizer's fill may
// touch -- claim slots (Next / Emit<Layout>) and stage bytes (Intern/Reserve).
// Owns all staging (batch, run log, intern arena) and hands full batches to
// the consumer transparently mid-fill. The stream shape (layout, dense
// positions) is per-binding knowledge shared by driver and consumer; a
// kernel receiving TokenEmitter& cannot end, drop, or re-target the stream
// it fills, and never sees the layout it fills for except as a
// compile-time tag.
class TokenEmitter : util::Noncopyable {
 public:
  // One token, lanes guarded by the compile-time layout -- the only lane
  // guards in the codebase. Kernels pass their layout tag through without
  // branching on it.
  template<TokenLayout L>
  uint32_t Emit(duckdb::string_t term, uint32_t pos, uint32_t offs_start,
                uint32_t offs_end) {
    const auto i = Next();
    buf.terms[i] = term;
    if constexpr (L != TokenLayout::Terms) {
      buf.pos[i] = pos;
    }
    if constexpr (L == TokenLayout::TermsPosOffs) {
      buf.offs_start[i] = offs_start;
      buf.offs_end[i] = offs_end;
    }
    return i;
  }

  // Dense-position variant: positions are implicit ordinals, never written.
  template<TokenLayout L>
  uint32_t Emit(duckdb::string_t term, uint32_t offs_start, uint32_t offs_end) {
    const auto i = Next();
    buf.terms[i] = term;
    if constexpr (L == TokenLayout::TermsPosOffs) {
      buf.offs_start[i] = offs_start;
      buf.offs_end[i] = offs_end;
    }
    return i;
  }

  // Interning variants: claim the slot BEFORE staging the bytes, so a cycle
  // triggered by the claim cannot reset the arena under the interned term.
  // Never pass Intern() results to Emit() yourself -- use these.
  template<TokenLayout L>
  uint32_t EmitInterned(bytes_view term, uint32_t pos, uint32_t offs_start,
                        uint32_t offs_end) {
    const auto i = Next();
    buf.terms[i] = Intern(term);
    if constexpr (L != TokenLayout::Terms) {
      buf.pos[i] = pos;
    }
    if constexpr (L == TokenLayout::TermsPosOffs) {
      buf.offs_start[i] = offs_start;
      buf.offs_end[i] = offs_end;
    }
    return i;
  }

  template<TokenLayout L>
  uint32_t EmitInterned(bytes_view term, uint32_t offs_start,
                        uint32_t offs_end) {
    const auto i = Next();
    buf.terms[i] = Intern(term);
    if constexpr (L == TokenLayout::TermsPosOffs) {
      buf.offs_start[i] = offs_start;
      buf.offs_end[i] = offs_end;
    }
    return i;
  }

  // Claims the next slot, cycling the batch to the consumer when full.
  uint32_t Next() {
    if (buf.Full()) [[unlikely]] {
      Cycle();
    }
    return buf.count++;
  }

  // Claims up to `want` consecutive slots and returns their term span
  // (parallel arrays index as `span.data() - buf.terms`).
  std::span<duckdb::string_t> Next(size_t want) {
    if (buf.Full()) [[unlikely]] {
      Cycle();
    }
    const auto first = buf.count;
    const auto got = static_cast<uint32_t>(
      std::min<size_t>(want, TokenBatch::kCapacity - first));
    buf.count += got;
    return {buf.terms + first, got};
  }

  // Stages out-of-line term bytes in the writer arena (reclaimed after each
  // consume cycle); short terms are inline string_t copies.
  duckdb::string_t Intern(bytes_view term) {
    const auto size = static_cast<uint32_t>(term.size());
    if (size <= duckdb::string_t::INLINE_LENGTH) {
      return MakeTermView(term.data(), size);
    }
    auto* mem = Arena().Allocate(size);
    std::memcpy(mem, term.data(), size);
    return MakeTermView(reinterpret_cast<const char*>(mem), size);
  }

  // Concatenating intern: stages `prefix+suffix` with a single copy of each
  // part (no intermediate buffer).
  duckdb::string_t Intern(bytes_view prefix, bytes_view suffix) {
    const auto size = static_cast<uint32_t>(prefix.size() + suffix.size());
    if (size <= duckdb::string_t::INLINE_LENGTH) {
      char inline_buf[duckdb::string_t::INLINE_LENGTH];
      std::memcpy(inline_buf, prefix.data(), prefix.size());
      std::memcpy(inline_buf + prefix.size(), suffix.data(), suffix.size());
      return MakeTermView(inline_buf, size);
    }
    auto* mem = Arena().Allocate(size);
    std::memcpy(mem, prefix.data(), prefix.size());
    std::memcpy(mem + prefix.size(), suffix.data(), suffix.size());
    return MakeTermView(reinterpret_cast<const char*>(mem), size);
  }

  // Reserves `size` arena bytes for a term built in place (e.g. unescaping);
  // pair with a manual string_t once the actual length is known. Claim the
  // slot via Next() BEFORE reserving: a cycle inside Next resets the arena.
  byte_type* Reserve(size_t size) { return Arena().Allocate(size); }

  // Delivers the current value's stored blob (store-producing kernels,
  // Traits().store) straight to the driver's bound store sink -- no
  // intermediate state, no relay; call once per successful value, within
  // bracketing for doc-attributed delivery.
  void Store(bytes_view blob) {
    if (_store_consumer != nullptr) {
      _store_consumer->OnStore(_doc, blob);
    }
  }

 protected:
  explicit TokenEmitter(TokenConsumer& consumer)
    : _staging{std::make_unique<StagingT>()},
      _consumer{&consumer},
      buf{_staging->batch} {}

  ~TokenEmitter() = default;

  duckdb::ArenaAllocator& Arena() {
    if (!_arena) {
      _arena = std::make_unique<duckdb::ArenaAllocator>(
        duckdb::Allocator::DefaultAllocator());
    }
    return *_arena;
  }

  void Cycle() {
    if (_in_value && buf.count > _run_start) {
      _staging->runs[_nruns++] = {_doc, buf.count - _run_start};
      _staging->runs[_nruns++] = {DocRun::kOpenValue, 0};
    }
    _consumer->Consume(buf, {_staging->runs, _nruns});
    Reset();
  }

  void Reset() {
    buf.count = 0;
    _nruns = 0;
    if (_arena) {
      _arena->Reset();
    }
    _run_start = 0;
  }

  struct StagingT {
    TokenBatch batch;
    DocRun runs[TokenBatch::kCapacity + 2];
  };

  std::unique_ptr<StagingT> _staging;
  std::unique_ptr<duckdb::ArenaAllocator> _arena;
  TokenConsumer* _consumer;
  TokenConsumer* _store_consumer = nullptr;
  doc_id_t _doc = 0;
  uint32_t _nruns = 0;
  uint32_t _run_start = 0;
  bool _in_value = false;

 public:
  TokenBatch& buf;
  // How to read buf.pos[]: dense = implicit ordinals (kernel never wrote the
  // lane). Set from the producing kernel's traits by the fill dispatch;
  // per-fill, persists across consume cycles. Lives on the writer, not the
  // batch: it is a bind-time constant, not data in flight.
  bool dense_pos = true;
};

// Driver-facing half: constructs and re-targets the stream, hands the final
// partial batch to the consumer (Finish), aborts (Discard), binds the store
// sink and inspects staged runs. Passed to tokenizers as TokenEmitter&.
class TokenWriter final : public TokenEmitter {
 public:
  // The consumer receives batches AND, by default, Store() blobs (OnStore is
  // its interface); drivers that route blobs elsewhere pass a distinct store
  // sink (nullptr = discard).
  explicit TokenWriter(TokenConsumer& consumer)
    : TokenWriter{consumer, &consumer} {}
  TokenWriter(TokenConsumer& consumer, TokenConsumer* store)
    : TokenEmitter{consumer} {
    _store_consumer = store;
  }

  // (Re)binds the writer's targets. Retargeting requires drained staging;
  // re-affirming the same targets is always allowed (per-value drivers
  // re-bind mid-column).
  void Bind(TokenConsumer& consumer) noexcept { Bind(consumer, &consumer); }
  void Bind(TokenConsumer& consumer, TokenConsumer* store) noexcept {
    SDB_ASSERT((buf.count == 0 && _nruns == 0) ||
               (&consumer == _consumer && store == _store_consumer));
    _consumer = &consumer;
    _store_consumer = store;
  }

  // Value bracketing: everything between BeginValue(doc) and EndValue() is
  // one value of `doc`. EndValue records the DocRun (the doc<->token mapping),
  // delivers the staged store blob, and cycles when the run log fills.
  void BeginValue(doc_id_t doc) noexcept {
    _doc = doc;
    _run_start = buf.count;
    _in_value = true;
  }

  void EndValue() {
    SDB_ASSERT(_in_value);
    _staging->runs[_nruns++] = {_doc, buf.count - _run_start};
    _in_value = false;
    if (_nruns == TokenBatch::kCapacity) [[unlikely]] {
      Cycle();
    }
  }

  // Hands the final partial batch to the consumer.
  void Finish() {
    SDB_ASSERT(!_in_value);
    if (buf.count != 0 || _nruns != 0) {
      _consumer->Consume(buf, {_staging->runs, _nruns});
      Reset();
    }
  }

  // Drops staged content without consuming it (column-abort paths).
  void Discard() {
    _in_value = false;
    Reset();
  }

  std::span<const DocRun> Runs() const noexcept {
    return {_staging->runs, _nruns};
  }
};

}  // namespace irs
