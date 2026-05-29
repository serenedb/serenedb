////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#include "utf8_helper.h"

#include <string.h>
#include <unicode/brkiter.h>
#include <unicode/coll.h>
#include <unicode/locid.h>
#include <unicode/regex.h>
#include <unicode/stringpiece.h>
#include <unicode/ucasemap.h>
#include <unicode/uchar.h>
#include <unicode/uclean.h>
#include <unicode/ucol.h>
#include <unicode/udata.h>
#include <unicode/uloc.h>
#include <unicode/unistr.h>
#include <unicode/unorm2.h>
#include <unicode/urename.h>
#include <unicode/ustring.h>
#include <unicode/utypes.h>

#include <memory>
#include <new>

#include "basics/debugging.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "basics/static_strings.h"
#include "basics/system-compiler.h"

namespace sdb {
namespace basics {

Utf8Helper Utf8Helper::gDefault;

Utf8Helper::Utf8Helper() noexcept {
  setCollatorLanguage({}, LanguageType::Default, nullptr);
}

Utf8Helper::~Utf8Helper() {
  delete _coll;
  u_cleanup();
}

int Utf8Helper::compare(const char* l, size_t l_len, const char* r,
                        size_t r_len) const {
  SDB_ASSERT(l != nullptr);
  SDB_ASSERT(r != nullptr);
  SDB_ASSERT(_coll);

  UErrorCode status = U_ZERO_ERROR;

  int result = _coll->compareUTF8(icu::StringPiece(l, (int32_t)l_len),
                                  icu::StringPiece(r, (int32_t)r_len), status);
  SDB_VERIFY(!U_FAILURE(status), "unexpected internal ICU error");
  return result;
}

bool Utf8Helper::setCollatorLanguage(const std::string& lang, LanguageType type,
                                     void* icu) {
  UErrorCode status = U_ZERO_ERROR;
  if (icu != nullptr) {
    udata_setCommonData(reinterpret_cast<void*>(icu), &status);
    if (U_FAILURE(status)) {
      SDB_ERROR("xxxxx", Logger::FIXME,
                "error while udata_setCommonData(...): ", u_errorName(status));
      return false;
    }
  }

  if (_coll) {
    ULocDataLocaleType type = ULOC_ACTUAL_LOCALE;
    const auto& locale = _coll->getLocale(type, status);

    if (U_FAILURE(status)) {
      SDB_ERROR("xxxxx", Logger::FIXME,
                "error in Collator::getLocale(...): ", u_errorName(status));
      return false;
    }
    if (lang == locale.getName()) {
      return true;
    }
  }

  std::unique_ptr<icu::Collator> coll;
  if (lang.empty()) {
    // get default locale for empty language
    coll.reset(icu::Collator::createInstance(status));
  } else {
    auto locale = icu::Locale::createCanonical(lang.c_str());
    if (type == LanguageType::Default) {
      locale = {locale.getLanguage(), locale.getCountry()};
    }
    coll.reset(icu::Collator::createInstance(locale, status));
  }

  if (U_FAILURE(status)) {
    SDB_ERROR("xxxxx", Logger::FIXME, "error in Collator::createInstance('",
              lang, "'): ", u_errorName(status));
    return false;
  }

  if (type == LanguageType::Default) {
    // set the default attributes for sorting:
    coll->setAttribute(UCOL_CASE_FIRST, UCOL_UPPER_FIRST, status);  // A < a
    // no normalization
    coll->setAttribute(UCOL_NORMALIZATION_MODE, UCOL_OFF, status);
    // UCOL_IDENTICAL, UCOL_PRIMARY, UCOL_SECONDARY, UCOL_TERTIARY
    coll->setAttribute(UCOL_STRENGTH, UCOL_IDENTICAL, status);

    if (U_FAILURE(status)) {
      SDB_ERROR("xxxxx", Logger::FIXME,
                "error in Collator::setAttribute(...): ", u_errorName(status));
      return false;
    }
  }
  SDB_ASSERT(coll);
  delete _coll;
  _coll = coll.release();
  return true;
}

std::string Utf8Helper::getCollatorLanguage() {
  if (!_coll) {
    return {};
  }
  UErrorCode status = U_ZERO_ERROR;
  ULocDataLocaleType type = ULOC_VALID_LOCALE;
  const auto& locale = _coll->getLocale(type, status);
  if (U_FAILURE(status)) {
    SDB_ERROR("xxxxx", Logger::FIXME,
              "error in Collator::getLocale(...): ", u_errorName(status));
    return {};
  }

  return locale.getName();
}

std::string Utf8Helper::getCollatorCountry() {
  if (_coll) {
    UErrorCode status = U_ZERO_ERROR;
    ULocDataLocaleType type = ULOC_VALID_LOCALE;
    const icu::Locale& locale = _coll->getLocale(type, status);

    if (U_FAILURE(status)) {
      SDB_ERROR("xxxxx", Logger::FIXME,
                "error in Collator::getLocale(...): ", u_errorName(status));
      return "";
    }
    return locale.getCountry();
  }
  return "";
}

std::unique_ptr<icu::RegexMatcher> Utf8Helper::buildMatcher(
  std::string_view pattern) {
  UErrorCode status = U_ZERO_ERROR;

  auto matcher = std::make_unique<icu::RegexMatcher>(
    icu::UnicodeString::fromUTF8(pattern), 0, status);
  if (U_FAILURE(status)) {
    matcher.reset();
  }

  return matcher;
}

bool Utf8Helper::matches(icu::RegexMatcher* matcher, const char* value,
                         size_t value_length, bool partial, bool& error) {
  SDB_ASSERT(value != nullptr);
  icu::UnicodeString v = icu::UnicodeString::fromUTF8(
    icu::StringPiece(value, static_cast<int32_t>(value_length)));

  matcher->reset(v);

  UErrorCode status = U_ZERO_ERROR;
  error = false;

  SDB_ASSERT(matcher != nullptr);
  UBool result;

  if (partial) {
    // partial match
    result = matcher->find(status);
  } else {
    // full match
    result = matcher->matches(status);
  }
  if (U_FAILURE(status)) {
    error = true;
  }

  return static_cast<bool>(result);
}

std::string Utf8Helper::replace(icu::RegexMatcher* matcher, const char* value,
                                size_t value_length, const char* replacement,
                                size_t replacement_length, bool partial,
                                bool& error) {
  SDB_ASSERT(value != nullptr);
  icu::UnicodeString v = icu::UnicodeString::fromUTF8(
    icu::StringPiece(value, static_cast<int32_t>(value_length)));

  icu::UnicodeString r = icu::UnicodeString::fromUTF8(
    icu::StringPiece(replacement, static_cast<int32_t>(replacement_length)));

  matcher->reset(v);

  UErrorCode status = U_ZERO_ERROR;
  error = false;

  SDB_ASSERT(matcher != nullptr);
  icu::UnicodeString result;

  if (partial) {
    // partial match
    result = matcher->replaceFirst(r, status);
  } else {
    // full match
    result = matcher->replaceAll(r, status);
  }

  if (U_FAILURE(status)) {
    error = true;
    return {};
  }

  std::string utf8;
  result.toUTF8String(utf8);
  return utf8;
}

}  // namespace basics

UChar* Utf8ToUChar(const char* utf8, size_t in_length, UChar* buffer,
                   size_t buffer_size, size_t* out_length, UErrorCode* status) {
  UErrorCode local_status = U_ZERO_ERROR;
  if (status == nullptr) {
    status = &local_status;
  }
  *status = U_ZERO_ERROR;

  // 1. convert utf8 string to utf16
  // calculate utf16 string length
  int32_t utf16_length;
  u_strFromUTF8(nullptr, 0, &utf16_length, utf8, (int32_t)in_length, status);
  if (*status != U_BUFFER_OVERFLOW_ERROR) {
    return nullptr;
  }

  UChar* utf16;
  if (utf16_length + 1 <= (int32_t)buffer_size) {
    // use local buffer
    utf16 = buffer;
  } else {
    // dynamic memory
    utf16 = new (std::nothrow) UChar[utf16_length + 1];
    if (utf16 == nullptr) {
      return nullptr;
    }
  }

  // now convert
  *status = U_ZERO_ERROR;
  // the +1 will append a 0 byte at the end
  u_strFromUTF8(utf16, utf16_length + 1, nullptr, utf8, (int32_t)in_length,
                status);
  if (*status != U_ZERO_ERROR) {
    delete[] utf16;
    return nullptr;
  }

  *out_length = (size_t)utf16_length;

  return utf16;
}

UChar* Utf8ToUChar(const char* utf8, size_t in_length, size_t* out_length,
                   UErrorCode* status) {
  UErrorCode local_status = U_ZERO_ERROR;
  if (status == nullptr) {
    status = &local_status;
  }
  *status = U_ZERO_ERROR;

  int32_t utf16_length;

  // 1. convert utf8 string to utf16
  // calculate utf16 string length
  u_strFromUTF8(nullptr, 0, &utf16_length, utf8, (int32_t)in_length, status);
  if (*status != U_BUFFER_OVERFLOW_ERROR) {
    return nullptr;
  }

  UChar* utf16 = new (std::nothrow) UChar[utf16_length + 1];
  if (utf16 == nullptr) {
    return nullptr;
  }

  // now convert
  *status = U_ZERO_ERROR;
  // the +1 will append a 0 byte at the end
  u_strFromUTF8(utf16, utf16_length + 1, nullptr, utf8, (int32_t)in_length,
                status);
  if (*status != U_ZERO_ERROR) {
    delete[] utf16;
    return nullptr;
  }

  *out_length = (size_t)utf16_length;

  return utf16;
}

char* UCharToUtf8(const UChar* uchar, size_t in_length, size_t* out_length,
                  UErrorCode* status) {
  UErrorCode local_status = U_ZERO_ERROR;
  if (status == nullptr) {
    status = &local_status;
  }
  *status = U_ZERO_ERROR;
  int32_t utf8_length;

  // calculate utf8 string length
  u_strToUTF8(nullptr, 0, &utf8_length, uchar, (int32_t)in_length, status);

  if (*status != U_ZERO_ERROR && *status != U_BUFFER_OVERFLOW_ERROR) {
    return nullptr;
  }

  char* utf8 = new (std::nothrow) char[utf8_length + 1];

  if (utf8 == nullptr) {
    return nullptr;
  }

  // convert to utf8
  *status = U_ZERO_ERROR;
  // the +1 will append a 0 byte at the end
  u_strToUTF8(utf8, utf8_length + 1, nullptr, uchar, (int32_t)in_length,
              status);

  if (*status != U_ZERO_ERROR) {
    delete[] utf8;
    return nullptr;
  }

  *out_length = (size_t)utf8_length;

  return utf8;
}

char* NormalizeUtf8ToNFC(const char* utf8, size_t in_length, size_t* out_length,
                         UErrorCode* status) {
  *out_length = 0;

  if (in_length == 0) {
    char* utf8_dest = new (std::nothrow) char[sizeof(char)];

    if (utf8_dest != nullptr) {
      utf8_dest[0] = '\0';
    }
    return utf8_dest;
  }

  UErrorCode local_status = U_ZERO_ERROR;
  if (status == nullptr) {
    status = &local_status;
  }
  *status = U_ZERO_ERROR;

  size_t utf16_length;
  // use this buffer and pass it to Utf8ToUChar so we can avoid dynamic
  // memory allocation for shorter strings
  UChar buffer[128];
  UChar* utf16 =
    Utf8ToUChar(utf8, in_length, &buffer[0], sizeof(buffer) / sizeof(UChar),
                &utf16_length, status);

  if (utf16 == nullptr) {
    return nullptr;
  }

  // continue in TR_normalize_utf16_to_NFC
  char* utf8_dest = NormalizeUtf16ToNfc(
    (const uint16_t*)utf16, (int32_t)utf16_length, out_length, status);

  if (utf16 != &buffer[0]) {
    // Utf8ToUChar dynamically allocated memory
    delete[] utf16;
  }

  return utf8_dest;
}

std::string NormalizeUtf8ToNFC(std::string_view value) {
  auto deleter = [](char* data) { delete[] data; };
  size_t out_length = 0;
  UErrorCode status = U_ZERO_ERROR;
  std::unique_ptr<char, decltype(deleter)> normalized(
    NormalizeUtf8ToNFC(value.data(), value.size(), &out_length, &status),
    deleter);
  if (normalized == nullptr) {
    if (status != U_ZERO_ERROR) {
      SDB_THROW(ERROR_BAD_PARAMETER,
                std::string("invalid UTF-8 string: ") + u_errorName(status));
    }
    SDB_THROW(ERROR_OUT_OF_MEMORY);
  }
  return std::string(normalized.get(), out_length);
}

char* NormalizeUtf16ToNfc(const uint16_t* utf16, size_t in_length,
                          size_t* out_length, UErrorCode* status) {
  *out_length = 0;

  if (in_length == 0) {
    char* utf8_dest = new (std::nothrow) char[sizeof(char)];
    if (utf8_dest != nullptr) {
      utf8_dest[0] = '\0';
    }
    return utf8_dest;
  }

  UErrorCode local_status = U_ZERO_ERROR;
  if (status == nullptr) {
    status = &local_status;
  }
  *status = U_ZERO_ERROR;
  const UNormalizer2* norm2 =
    unorm2_getInstance(nullptr, "nfc", UNORM2_COMPOSE, status);

  if (*status != U_ZERO_ERROR) {
    return nullptr;
  }

  // normalize UChar (UTF-16)
  UChar* utf16_dest;
  bool must_free;
  char buffer[512];

  if (in_length < sizeof(buffer) / sizeof(UChar)) {
    // use a static buffer
    utf16_dest = (UChar*)&buffer[0];
    must_free = false;
  } else {
    // use dynamic memory
    utf16_dest = new (std::nothrow) UChar[in_length + 1];
    if (utf16_dest == nullptr) {
      return nullptr;
    }
    must_free = true;
  }

  size_t overhead = 0;
  int32_t utf16_dest_length;

  while (true) {
    *status = U_ZERO_ERROR;
    utf16_dest_length =
      unorm2_normalize(norm2, (UChar*)utf16, (int32_t)in_length, utf16_dest,
                       (int32_t)(in_length + overhead + 1), status);

    if (*status == U_ZERO_ERROR) {
      break;
    }

    if (*status == U_BUFFER_OVERFLOW_ERROR ||
        *status == U_STRING_NOT_TERMINATED_WARNING) {
      // output buffer was too small. now re-try with a bigger buffer (inLength
      // + overhead size)
      if (must_free) {
        // free original buffer first so we don't leak
        delete[] utf16_dest;
        must_free = false;
      }

      if (overhead == 0) {
        // set initial overhead size
        if (in_length < 256) {
          overhead = 16;
        } else if (in_length < 4096) {
          overhead = 128;
        } else {
          overhead = 256;
        }
      } else {
        // use double buffer size
        overhead += overhead;

        if (overhead >= 1024 * 1024) {
          // enough is enough
          return nullptr;
        }
      }

      utf16_dest = new (std::nothrow) UChar[in_length + overhead + 1];

      if (utf16_dest != nullptr) {
        // got new memory. now try again with the adjusted, bigger buffer
        must_free = true;
        continue;
      }
      // intentionally falls through
    }

    if (must_free) {
      delete[] utf16_dest;
    }

    return nullptr;
  }

  // Convert data back from UChar (UTF-16) to UTF-8
  char* utf8_dest =
    UCharToUtf8(utf16_dest, (size_t)utf16_dest_length, out_length);

  if (must_free) {
    delete[] utf16_dest;
  }

  return utf8_dest;
}

}  // namespace sdb
