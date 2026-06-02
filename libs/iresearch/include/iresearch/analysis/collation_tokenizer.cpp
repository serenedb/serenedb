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

#include <unicode/coll.h>
#include <unicode/locid.h>

#include <string_view>

#include "basics/exceptions.h"
#include "collation_tokenizer_encoder.hpp"

namespace irs::analysis {
namespace {

constexpr size_t kMaxTokenSize = 1 << 15;

}  // namespace

struct CollationTokenizer::StateT {
  const Options options;
  std::unique_ptr<icu::Collator> collator;
  byte_type term_buf[kMaxTokenSize];

  explicit StateT(Options opts) : options(std::move(opts)) {}
};

void CollationTokenizer::StateDeleterT::operator()(StateT* p) const noexcept {
  delete p;
}

CollationTokenizer::CollationTokenizer(Options options)
  : _state{new StateT(std::move(options))}, _term_eof{true} {}

Analyzer::ptr CollationTokenizer::Make(Options opts) {
  if (opts.locale.isBogus()) {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER, "collation: invalid locale");
  }
  auto err = UErrorCode::U_ZERO_ERROR;
  std::unique_ptr<icu::Collator> collator{
    icu::Collator::createInstance(opts.locale, err)};
  if (!collator || !U_SUCCESS(err)) {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER,
              "collation: failed to create collator for the locale");
  }
  return std::make_unique<CollationTokenizer>(std::move(opts));
}

bool CollationTokenizer::reset(std::string_view data) {
  if (!_state->collator) {
    auto err = UErrorCode::U_ZERO_ERROR;
    _state->collator.reset(
      icu::Collator::createInstance(_state->options.locale, err));

    if (!U_SUCCESS(err) || !_state->collator) {
      _state->collator.reset();

      return false;
    }
  }

  if (data.size() >
      static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
    return false;  // ICU UnicodeString signatures can handle at most INT32_MAX
  }

  const icu::UnicodeString icu_token = icu::UnicodeString::fromUTF8(
    icu::StringPiece(data.data(), static_cast<int32_t>(data.size())));

  byte_type raw_term_buf[kMaxTokenSize];
  static_assert(sizeof raw_term_buf == sizeof _state->term_buf);

  auto* buf = _state->options.force_utf8 ? raw_term_buf : _state->term_buf;
  int32_t term_size =
    _state->collator->getSortKey(icu_token, buf, kMaxTokenSize);

  // https://unicode-org.github.io/icu-docs/apidoc/released/icu4c/classicu_1_1Collator.html
  // according to ICU docs sort keys are always zero-terminated,
  // there is no reason to store terminal zero in term dictionary
  SDB_ASSERT(term_size > 0);
  --term_size;
  SDB_ASSERT(0 == buf[term_size]);
  if (term_size > static_cast<int32_t>(kMaxTokenSize)) {
    SDB_ERROR(
      IRESEARCH,
      absl::StrCat("Collated token is ", term_size,
                   " bytes length which exceeds maximum allowed length of ",
                   static_cast<int32_t>(sizeof raw_term_buf), " bytes"));
    return false;
  }
  auto term_buf_idx = static_cast<size_t>(term_size);
  if (_state->options.force_utf8) {
    // enforce valid UTF-8 string
    SDB_ASSERT(buf == raw_term_buf);
    term_buf_idx = 0;
    for (decltype(term_size) i{}; i < term_size; ++i) {
      static_assert(sizeof(raw_term_buf[i]) * (1 << CHAR_BIT) <=
                    kRecalcMap.size());
      const auto [offset, size] = kRecalcMap[raw_term_buf[i]];
      if ((term_buf_idx + size) > sizeof _state->term_buf) {
        SDB_ERROR(IRESEARCH, absl::StrCat("Collated token is more than ",
                                          sizeof _state->term_buf,
                                          " bytes length after encoding."));
        return false;
      }
      SDB_ASSERT(size <= 2);
      _state->term_buf[term_buf_idx++] = kBytesRecalcMap[offset];
      if (size == 2) {
        _state->term_buf[term_buf_idx++] = kBytesRecalcMap[offset + 1];
      }
    }
  }

  std::get<TermAttr>(_attrs).value = {_state->term_buf, term_buf_idx};
  auto& offset = std::get<OffsAttr>(_attrs);
  offset.start = 0;
  offset.end = static_cast<uint32_t>(data.size());
  _term_eof = false;

  return true;
}

}  // namespace irs::analysis
