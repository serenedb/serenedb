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

#include <absl/strings/str_cat.h>
#include <unicode/ucol.h>
#include <unicode/ustring.h>

#include <algorithm>

#include "basics/log.h"
#include "iresearch/analysis/token_batch.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {
namespace {

constexpr size_t kMaxTokenSize = 1 << 15;

}  // namespace

CollationTokenizer::CollationTokenizer(const Options& options) {
  if (options.locale.isBogus()) {
    THROW_SQL_ERROR(ERR_MSG("collation: invalid locale"));
  }
  auto err = UErrorCode::U_ZERO_ERROR;
  _collator.reset(ucol_open(options.locale.getName(), &err));
  if (!_collator || !U_SUCCESS(err)) {
    THROW_SQL_ERROR(
      ERR_MSG("collation: failed to create collator for the locale"));
  }
}

Tokenizer::ptr CollationTokenizer::Make(Options opts) {
  return std::make_unique<CollationTokenizer>(opts);
}

template<TokenLayout Layout, bool Ascii, typename Sink>
bool CollationTokenizer::DoFill(duckdb::string_t raw, Sink& sink) {
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
  if constexpr (Ascii) {
    std::copy_n(reinterpret_cast<const uint8_t*>(raw.GetData()), raw_size,
                _u16_buf.data());
    u16_len = static_cast<int32_t>(raw_size);
  } else {
    auto err = UErrorCode::U_ZERO_ERROR;
    u_strFromUTF8WithSub(_u16_buf.data(), static_cast<int32_t>(_u16_buf.size()),
                         &u16_len, raw.GetData(),
                         static_cast<int32_t>(raw_size), 0xFFFD, nullptr, &err);
    if (!U_SUCCESS(err)) [[unlikely]] {
      return false;
    }
  }

  byte_type sort_key[kMaxTokenSize];
  const int32_t size =
    ucol_getSortKey(_collator.get(),
                    reinterpret_cast<const UChar*>(_u16_buf.data()), u16_len,
                    sort_key, kMaxTokenSize) -
    1;
  if (size < 0 || size >= static_cast<int32_t>(kMaxTokenSize)) {
    SDB_ERROR(IRESEARCH,
              absl::StrCat("Collated token exceeds maximum allowed length of ",
                           kMaxTokenSize, " bytes"));
    return false;
  }
  SDB_ASSERT(sort_key[size] == 0);
  sink.template Emit<Layout>(sort_key, static_cast<uint32_t>(size));
  return true;
}

template class TypedTokenizer<CollationTokenizer>;
template class TypedTokenStage<CollationTokenizer>;

}  // namespace irs::analysis
