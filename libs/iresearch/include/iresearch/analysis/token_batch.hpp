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

#include <bit>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <span>
#include <utility>

#include "basics/shared.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace duckdb {

class ArenaAllocator;

}  // namespace duckdb
namespace irs {

struct TokenTraits {
  duckdb::LogicalTypeId input = duckdb::LogicalTypeId::VARCHAR;
  duckdb::LogicalTypeId output = duckdb::LogicalTypeId::VARCHAR;
  bool unique = false;
  bool keyword = false;
  bool explicit_pos = false;
  bool offsets = false;
  bool store = false;
  bool stable = false;
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

// Per-block facts a column driver derives from the data right before the
// fill; consumers may use them to hoist per-value checks, never to change
// results.
struct BlockTraits {
  bool ascii = false;
};

struct FillCtx {
  TokenLayout layout = TokenLayout::Terms;
  BlockTraits traits{};
};

struct BatchCtx {
  BlockTraits traits{};
  duckdb::ArenaAllocator& arena;
  uint64_t* valid = nullptr;
};

struct DocRun {
  uint32_t doc;
  uint32_t ntokens;
};

struct DocRuns : std::span<const DocRun> {
  bool tail_open = false;
};

struct TokenBatch {
  static constexpr size_t kCapacity = 1024;
  static constexpr size_t kValidWords = kCapacity / 64;

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

IRS_FORCE_INLINE inline bool IsValid(const uint64_t* valid,
                                     uint32_t i) noexcept {
  return ((valid[i >> 6] >> (i & 63)) & 1) != 0;
}

IRS_FORCE_INLINE inline uint32_t CountValid(const uint64_t* valid,
                                            uint32_t begin,
                                            uint32_t end) noexcept {
  if (begin == end) {
    return 0;
  }
  const uint32_t first = begin >> 6;
  const uint32_t last = (end - 1) >> 6;
  const uint64_t head = ~uint64_t{0} << (begin & 63);
  const uint64_t tail = ~uint64_t{0} >> (63 - ((end - 1) & 63));
  if (first == last) {
    return static_cast<uint32_t>(std::popcount(valid[first] & head & tail));
  }
  auto count = static_cast<uint32_t>(std::popcount(valid[first] & head));
  for (uint32_t w = first + 1; w < last; ++w) {
    count += static_cast<uint32_t>(std::popcount(valid[w]));
  }
  return count + static_cast<uint32_t>(std::popcount(valid[last] & tail));
}

struct TokenConsumer {
  virtual void Consume(TokenBatch& batch, DocRuns runs) = 0;

 protected:
  ~TokenConsumer() = default;
};

struct StoreSink {
  virtual void OnStore(doc_id_t doc, bytes_view store) = 0;

 protected:
  ~StoreSink() = default;
};

}  // namespace irs
