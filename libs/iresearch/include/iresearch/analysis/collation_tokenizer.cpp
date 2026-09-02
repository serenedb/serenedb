////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "collation_tokenizer.hpp"

#include <absl/algorithm/container.h>
#include <unicode/locid.h>
#include <unicode/ucol.h>
#include <unicode/ustring.h>

#include <cstring>
#include <vector>

#include "basics/log.h"
#include "iresearch/analysis/token_batch.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {
namespace {

constexpr size_t kMaxTokenSize = 1 << 15;

// We must enforce UTF-8 "valid" output.
// By default ICU SortKey is just arbitrary bytes in range 0x00 - 0xFF
// so in general SortKey is not valid UTF-8 sequence.
// To achieve "validity" we split each byte in range 0x80 - 0xFF
// in two bytes UTF-8 character. We don't care about "sanity" of this
// UTF-8 string as SortKey was never intended to be human redable. And this
// split still leaves the binary sorting order of modified SortKeys.
size_t EncodeSortKey(uint8_t* dst, const uint8_t* src, size_t size) {
  constexpr uint64_t kHighBits = UINT64_C(0x8080808080808080);
  size_t out = 0;
  size_t k = 0;
  while (k < size) {
    if (k + 8 <= size) {
      uint64_t w;
      std::memcpy(&w, src + k, 8);
      if (!(w & kHighBits)) {
        std::memcpy(dst + out, src + k, 8);
        out += 8;
        k += 8;
        continue;
      }
    }
    const uint8_t b = src[k++];
    const bool wide = b >= 0x80;
    dst[out] = wide ? static_cast<uint8_t>(0xC0 | (b >> 3)) : b;
    dst[out + 1] = static_cast<uint8_t>(0x80 | (b & 7));
    out += 1 + wide;
  }
  return out;
}

}  // namespace

CollationTokenizer::CollationTokenizer(Options options)
  : _options{std::move(options)} {
  if (_options.locale.isBogus()) {
    THROW_SQL_ERROR(ERR_MSG("collation: invalid locale"));
  }
}

std::tuple<> CollationTokenizer::PrepareBatch(BlockTraits) {
  if (!_collator) {
    auto err = UErrorCode::U_ZERO_ERROR;
    _collator.reset(ucol_open(_options.locale.getName(), &err));
    if (!_collator || !U_SUCCESS(err)) {
      THROW_SQL_ERROR(
        ERR_MSG("collation: failed to create collator for the locale"));
    }
  }
  return {};
}

Tokenizer::ptr CollationTokenizer::Make(Options opts) {
  return std::make_unique<CollationTokenizer>(std::move(opts));
}

template<TokenLayout Layout, typename Sink>
bool CollationTokenizer::DoFill(duckdb::string_t raw, Sink& sink) {
  SDB_ASSERT(_collator);
  const size_t raw_size = raw.GetSize();
  if (raw_size > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
    return false;
  }

  // utf8 -> utf16 into the reused scratch (utf16 units never exceed utf8
  // bytes, so value.size() capacity always suffices); illegal input is
  // substituted with U+FFFD. Bulk-convert-then-collate measures ~2x faster
  // than streaming ucol_nextSortKeyPart over a UTF-8 UCharIterator.
  if (_u16_buf.size() < raw_size) {
    _u16_buf.resize(raw_size);
  }
  int32_t u16_len = 0;
  auto err = UErrorCode::U_ZERO_ERROR;
  u_strFromUTF8WithSub(_u16_buf.data(), static_cast<int32_t>(_u16_buf.size()),
                       &u16_len, raw.GetData(), static_cast<int32_t>(raw_size),
                       0xFFFD, nullptr, &err);
  if (!U_SUCCESS(err)) [[unlikely]] {
    return false;
  }

  byte_type raw_term_buf[kMaxTokenSize];
  const int32_t term_size =
    ucol_getSortKey(_collator.get(),
                    reinterpret_cast<const UChar*>(_u16_buf.data()), u16_len,
                    raw_term_buf, kMaxTokenSize) -
    1;
  if (term_size < 0 || term_size >= static_cast<int32_t>(kMaxTokenSize)) {
    SDB_ERROR(IRESEARCH,
              absl::StrCat("Collated token exceeds maximum allowed length of ",
                           kMaxTokenSize, " bytes"));
    return false;
  }
  SDB_ASSERT(raw_term_buf[term_size] == 0);

  const auto size = static_cast<size_t>(term_size);
  if (_options.force_utf8) {
    if (2 * size > kMaxTokenSize) [[unlikely]] {
      const auto wide = static_cast<size_t>(absl::c_count_if(
        std::span{raw_term_buf, size}, [](uint8_t b) { return b >= 0x80; }));
      if (size + wide > kMaxTokenSize) {
        SDB_ERROR(IRESEARCH,
                  absl::StrCat("Collated token is more than ", kMaxTokenSize,
                               " bytes length after encoding."));
        return false;
      }
    }
    sink.template Emit<Layout>(2 * size, [&](byte_type* mem) IRS_FORCE_INLINE {
      return EncodeSortKey(mem, raw_term_buf, size);
    });
  } else {
    sink.template Emit<Layout>(raw_term_buf, static_cast<uint32_t>(term_size));
  }
  return true;
}

template class TypedTokenizer<CollationTokenizer>;
template class TypedTokenStage<CollationTokenizer>;

}  // namespace irs::analysis
