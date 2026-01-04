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

#pragma once

#include <absl/strings/ascii.h>
#include <absl/strings/charconv.h>

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "basics/common.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/debugging.h"
#include "basics/result_or.h"
#include "basics/shared.hpp"

namespace sdb {

int CompareIgnoreCase(std::string_view lhs, std::string_view rhs) noexcept;

namespace basics {

static constexpr size_t kMaxU64B10StringSize = 21;
static constexpr size_t kMaxU64B64StringSize = 11;

template<typename Str>
IRS_FORCE_INLINE void StrReserveAmortized(Str& str, size_t len) {
  const size_t cap = str.capacity();
  if (len > cap) {
    // Make sure to always grow by at least a factor of 2x.
    str.reserve(std::max(len, 2 * cap));
  }
}

template<typename Str>
IRS_FORCE_INLINE void StrResize(Str& str, size_t len) {
  absl::strings_internal::STLStringResizeUninitialized(&str, len);
}

template<typename Str>
IRS_FORCE_INLINE void StrResizeAmortized(Str& str, size_t len) {
  absl::strings_internal::STLStringResizeUninitializedAmortized(&str, len);
}

template<typename Str>
IRS_FORCE_INLINE void StrAppend(Str& str, size_t len) {
  str.__append_default_init(len);
}

/// collection of string utility functions
///
/// This namespace holds function used for string manipulation.
namespace string_utils {

// -----------------------------------------------------------------------------
// STRING CONVERSION
// -----------------------------------------------------------------------------

/// escape unicode
///
/// This method escapes a unicode character string by replacing the unicode
/// characters by a \\uXXXX sequence.
std::string EscapeUnicode(std::string_view value, bool escape_slash = true);

std::string_view Trim(std::string_view source_str,
                      std::string_view trim_str = " \t\n\r");

void TrimInPlace(std::string& str, std::string_view trim_str = " \t\n\r");

std::string_view LTrim(std::string_view source_str,
                       std::string_view trim_str = " \t\n\r");

std::string_view RTrim(std::string_view source_str,
                       std::string_view trim_str = " \t\n\r");

std::string Replace(std::string_view source_str, std::string_view from_string,
                    std::string_view to_string);

/// url decodes the string
std::string UrlDecodePath(std::string_view str);
std::string UrlDecode(std::string_view str);

/// url encodes the string
std::string UrlEncode(const char* src, size_t len);
inline std::string UrlEncode(std::string_view value) {
  return UrlEncode(value.data(), value.size());
}

/// url encodes the string into the result buffer
void EncodeUriComponent(std::string& result, const char* src, size_t len);

/// uri encodes the component string
inline std::string EncodeUriComponent(std::string_view value) {
  std::string result;
  EncodeUriComponent(result, value.data(), value.size());
  return result;
}

/// converts input string to soundex code
std::string Soundex(std::string_view value);

/// converts input string to vector of character codes
std::vector<uint32_t> CharacterCodes(const char* s, size_t length);

/// calculates the levenshtein distance between the input strings
unsigned int LevenshteinDistance(const char* s1, size_t l1, const char* s2,
                                 size_t l2);

/// calculates the levenshtein distance between the input strings
size_t LevenshteinDistance(std::vector<uint32_t> vect1,
                           std::vector<uint32_t> vect2);

// -----------------------------------------------------------------------------
// CONVERT TO STRING
// -----------------------------------------------------------------------------

/// converts integer to string
std::string Itoa(int16_t i);

/// converts unsigned integer to string
std::string Itoa(uint16_t i);

/// converts integer to string
std::string Itoa(int64_t i);

/// converts unsigned integer to string
std::string Itoa(uint64_t i);

/// converts unsigned integer to string
size_t Itoa(uint64_t i, char* result);

void Itoa(uint64_t i, std::string& result);

/// converts integer to string
std::string Itoa(int32_t i);

/// converts unsigned integer to string
std::string Itoa(uint32_t i);

// -----------------------------------------------------------------------------
// CONVERT FROM STRING
// -----------------------------------------------------------------------------

/// converts a single hex to integer
inline int Hex2int(char ch, int error_value = 0) {
  if ('0' <= ch && ch <= '9') {
    return ch - '0';
  } else if ('A' <= ch && ch <= 'F') {
    return ch - 'A' + 10;
  } else if ('a' <= ch && ch <= 'f') {
    return ch - 'a' + 10;
  }

  return error_value;
}

/// parses a boolean
bool Boolean(std::string_view str);

/// parses an integer
int64_t Int64(const char* value, size_t size) noexcept;
inline int64_t Int64(std::string_view value) noexcept {
  return Int64(value.data(), value.size());
}

/// parses an unsigned integer
uint64_t Uint64(const char* value, size_t size) noexcept;
inline uint64_t Uint64(std::string_view value) noexcept {
  return Uint64(value.data(), value.size());
}

/// parses an unsigned integer
/// the caller must make sure that the input buffer only contains valid
/// numeric characters - otherwise the uint64_t result will be wrong.
/// because the input is restricted to some valid characters, this function
/// is highly optimized
uint64_t Uint64Trusted(const char* value, size_t length) noexcept;
inline uint64_t Uint64Trusted(std::string_view value) noexcept {
  return Uint64Trusted(value.data(), value.size());
}

/// parses an unsigned integers, but returns any errors
ResultOr<uint64_t> TryUint64(const char* value, size_t size) noexcept;
ResultOr<uint64_t> TryUint64(std::string_view value) noexcept;

/// parses an integer
int32_t Int32(const char* value, size_t size) noexcept;
inline int32_t Int32(std::string_view value) noexcept {
  return Int32(value.data(), value.size());
}

/// parses an unsigned integer
uint32_t Uint32(const char* value, size_t size) noexcept;
inline uint32_t Uint32(std::string_view value) noexcept {
  return Uint32(value.data(), value.size());
}

/// parses a decimal
double DoubleDecimal(const char* value, size_t size);
inline double DoubleDecimal(std::string_view value) {
  return DoubleDecimal(value.data(), value.size());
}
inline float FloatDecimal(const char* value, size_t size) {
  return static_cast<float>(DoubleDecimal(value, size));
}
inline float FloatDecimal(std::string_view value) {
  return FloatDecimal(value.data(), value.size());
}

template<typename T>
bool ToNumber(std::string_view key, T& val) noexcept {
  if constexpr (std::is_floating_point_v<T>) {
    return absl::from_chars(key.data(), key.data() + key.size(), val).ec ==
           std::errc{};
  } else {
    return static_cast<bool>(
      std::from_chars(key.data(), key.data() + key.size(), val));
  }
}

// -----------------------------------------------------------------------------
// ADDITIONAL STRING UTILITIES
// -----------------------------------------------------------------------------

/// replaces incorrect path delimiter character for window and linux
inline std::string CorrectPath(std::string_view incorrect_path) {
  return Replace(incorrect_path, "\\", "/");
}

std::string EncodeHex(const char* value, size_t length);
inline std::string EncodeHex(std::string_view value) {
  return EncodeHex(value.data(), value.size());
}
std::string DecodeHex(std::string_view value);

void EscapeRegexParams(std::string& out, const char* ptr, size_t length);
std::string EscapeRegexParams(std::string_view in);

/// returns a human-readable size string, e.g.
/// - 0 => "0 bytes"
/// - 1 => "1 byte"
/// - 255 => "255 bytes"
/// - 2048 => "2.0 KB"
/// - 1048576 => "1.0 MB"
/// ...
std::string FormatSize(uint64_t value);

/// Translates a set of HTTP headers into a string, which is
/// properly escaped to put it into a log file.
std::string HeadersToString(
  const containers::FlatHashMap<std::string, std::string>& headers);

/// returns the endpoint from a URL
std::string_view GetEndpointFromUrl(std::string_view url);

template<typename Char>
constexpr bool IsNull(std::basic_string_view<Char> str) noexcept {
  return str.data() == nullptr;
}

// helper function to strip-non-numeric data from a string
std::string RemoveWhitespaceAndComments(const std::string& value);

struct EscapeJsonOptions {
  // escape forward slashes when serializing VPack values into
  // JSON with a Dumper (requires escapeControl = true)
  bool escape_forward_slashes = false;

  // with a Dumper (creates \uxxxx sequences or displays '\n', '\r' or \'t',
  // when set to false, replaces the control characters with whitespaces)
  bool escape_control = true;

  // escape multi-byte Unicode characters when dumping them to JSON
  // with a Dumper (creates \uxxxx sequences)
  bool escape_unicode = false;
};

template<typename Sink>
void EscapeJsonStr(std::string_view str, Sink* sink, EscapeJsonOptions options);

}  // namespace string_utils
}  // namespace basics
}  // namespace sdb
