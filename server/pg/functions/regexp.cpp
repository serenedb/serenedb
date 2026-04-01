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

#include "pg/functions/regexp.h"

#include <re2/re2.h>
#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <velox/functions/lib/Re2Functions.h>
#include <velox/type/SimpleFunctionApi.h>

#include "basics/fwd.h"
#include "iresearch/utils/utf8_utils.hpp"
#include "pg/sql_exception_macro.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg::functions {
namespace {

// Count UTF-8 characters in [begin, begin + byte_len).
size_t Utf8CharCount(const char* begin, size_t byte_len) {
  auto* it = reinterpret_cast<const irs::byte_type*>(begin);
  auto* end = it + byte_len;
  size_t count = 0;
  while (it < end) {
    it = irs::utf8_utils::Next(it, end);
    ++count;
  }
  return count;
}

// Advance pos by one UTF-8 character (for empty-match advancement).
size_t Utf8NextCharPos(const char* begin, size_t byte_len, size_t pos) {
  if (pos >= byte_len) {
    return pos + 1;  // past end - just bump to terminate loop
  }
  auto* it = reinterpret_cast<const irs::byte_type*>(begin) + pos;
  auto* end = reinterpret_cast<const irs::byte_type*>(begin) + byte_len;
  it = irs::utf8_utils::Next(it, end);
  return static_cast<size_t>(it -
                             reinterpret_cast<const irs::byte_type*>(begin));
}

// Advance n UTF-8 characters from begin, return byte offset from begin.
// Does not advance past byte_len.
size_t Utf8Advance(const char* begin, size_t byte_len, size_t n) {
  auto* it = reinterpret_cast<const irs::byte_type*>(begin);
  auto* end = it + byte_len;
  for (size_t i = 0; i < n && it < end; ++i) {
    it = irs::utf8_utils::Next(it, end);
  }
  return static_cast<size_t>(it -
                             reinterpret_cast<const irs::byte_type*>(begin));
}

// Returns capture groups from the first match. If no capture groups,
// returns the full match. Returns NULL if no match.
template<typename T>
struct PgRegexpMatch {
  PgRegexpMatch() : _cache(0) {}

  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr int32_t reuse_strings_from_arg = 0;

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<velox::Varchar>*,
                                      const arg_type<velox::Varchar>* pattern) {
    if (pattern) {
      _re.emplace(re2::StringPiece(pattern->data(), pattern->size()),
                  RE2::Quiet);
      if (!_re->ok()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_REGULAR_EXPRESSION),
                        ERR_MSG("invalid regular expression: ", _re->error()));
      }
    }
    _cache.setMaxCompiledRegexes(config.exprMaxCompiledRegexes());
  }

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Array<velox::Varchar>>& result,
                                const arg_type<velox::Varchar>& text,
                                const arg_type<velox::Varchar>& pattern) {
    auto& re = EnsurePattern(pattern);
    re2::StringPiece input(text.data(), text.size());
    int numGroups = re.NumberOfCapturingGroups();

    if (numGroups == 0) {
      re2::StringPiece match;
      if (!re.Match(input, 0, text.size(), RE2::UNANCHORED, &match, 1)) {
        return false;
      }
      result.add_item().setNoCopy(
        velox::StringView(match.data(), match.size()));
      return true;
    }

    _groups.resize(numGroups + 1);
    if (!re.Match(input, 0, text.size(), RE2::UNANCHORED, _groups.data(),
                  numGroups + 1)) {
      return false;
    }

    for (int i = 1; i <= numGroups; ++i) {
      const auto& group = _groups[i];
      if (group.data()) {
        result.add_item().setNoCopy(
          velox::StringView(group.data(), group.size()));
      } else {
        result.add_null();
      }
    }
    return true;
  }

 private:
  RE2& EnsurePattern(const arg_type<velox::Varchar>& pattern) {
    if (_re.has_value()) {
      return *_re;
    }
    return *_cache.findOrCompile(
      velox::StringView(pattern.data(), pattern.size()));
  }

  std::optional<RE2> _re;
  velox::functions::detail::ReCache _cache;
  std::vector<re2::StringPiece> _groups;
};

// Counts the number of matches of pattern in string.
template<typename T>
struct PgRegexpCount {
  PgRegexpCount() : _cache(0) {}

  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<velox::Varchar>*,
                                      const arg_type<velox::Varchar>* pattern) {
    if (pattern) {
      _re.emplace(re2::StringPiece(pattern->data(), pattern->size()),
                  RE2::Quiet);
      if (!_re->ok()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_REGULAR_EXPRESSION),
                        ERR_MSG("invalid regular expression: ", _re->error()));
      }
    }
    _cache.setMaxCompiledRegexes(config.exprMaxCompiledRegexes());
  }

  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& result,
                                const arg_type<velox::Varchar>& text,
                                const arg_type<velox::Varchar>& pattern) {
    auto& re = EnsurePattern(pattern);
    re2::StringPiece input(text.data(), text.size());
    re2::StringPiece match;
    int64_t count = 0;
    size_t pos = 0;

    while (re.Match(input, pos, text.size(), RE2::UNANCHORED, &match, 1)) {
      ++count;
      pos = match.data() + match.size() - input.data();
      if (FOLLY_UNLIKELY(match.size() == 0)) {
        pos = Utf8NextCharPos(text.data(), text.size(), pos);
      }
    }
    result = count;
  }

 private:
  RE2& EnsurePattern(const arg_type<velox::Varchar>& pattern) {
    if (_re.has_value()) {
      return *_re;
    }
    return *_cache.findOrCompile(
      velox::StringView(pattern.data(), pattern.size()));
  }

  std::optional<RE2> _re;
  velox::functions::detail::ReCache _cache;
};

// Returns the starting position (1-based) of the first match, or 0 if no match.
template<typename T>
struct PgRegexpInstr {
  PgRegexpInstr() : _cache(0) {}

  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<velox::Varchar>*,
                                      const arg_type<velox::Varchar>* pattern) {
    if (pattern) {
      _re.emplace(re2::StringPiece(pattern->data(), pattern->size()),
                  RE2::Quiet);
      if (!_re->ok()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_REGULAR_EXPRESSION),
                        ERR_MSG("invalid regular expression: ", _re->error()));
      }
    }
    _cache.setMaxCompiledRegexes(config.exprMaxCompiledRegexes());
  }

  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& result,
                                const arg_type<velox::Varchar>& text,
                                const arg_type<velox::Varchar>& pattern) {
    auto& re = EnsurePattern(pattern);
    re2::StringPiece input(text.data(), text.size());
    re2::StringPiece match;

    if (!re.Match(input, 0, text.size(), RE2::UNANCHORED, &match, 1)) {
      result = 0;
      return;
    }
    // 1-based character position (count UTF-8 chars up to match start)
    size_t byte_offset = match.data() - input.data();
    result = static_cast<int64_t>(Utf8CharCount(text.data(), byte_offset)) + 1;
  }

 private:
  RE2& EnsurePattern(const arg_type<velox::Varchar>& pattern) {
    if (_re.has_value()) {
      return *_re;
    }
    return *_cache.findOrCompile(
      velox::StringView(pattern.data(), pattern.size()));
  }

  std::optional<RE2> _re;
  velox::functions::detail::ReCache _cache;
};

// Returns the starting position (1-based) of the Nth match starting from
// position `start` (1-based), or 0 if no match.
template<typename T>
struct PgRegexpInstr4 {
  PgRegexpInstr4() : _cache(0) {}

  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<velox::Varchar>*,
                                      const arg_type<velox::Varchar>* pattern,
                                      const int64_t*, const int64_t*) {
    if (pattern) {
      _re.emplace(re2::StringPiece(pattern->data(), pattern->size()),
                  RE2::Quiet);
      if (!_re->ok()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_REGULAR_EXPRESSION),
                        ERR_MSG("invalid regular expression: ", _re->error()));
      }
    }
    _cache.setMaxCompiledRegexes(config.exprMaxCompiledRegexes());
  }

  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& result,
                                const arg_type<velox::Varchar>& text,
                                const arg_type<velox::Varchar>& pattern,
                                const int64_t& start, const int64_t& n) {
    auto& re = EnsurePattern(pattern);
    re2::StringPiece input(text.data(), text.size());
    re2::StringPiece match;

    // Convert 1-based character start to 0-based byte offset.
    size_t pos = start > 1 ? Utf8Advance(text.data(), text.size(),
                                         static_cast<size_t>(start - 1))
                           : 0;
    int64_t count = 0;

    while (re.Match(input, pos, text.size(), RE2::UNANCHORED, &match, 1)) {
      ++count;
      if (count == n) {
        size_t byte_offset = match.data() - input.data();
        result =
          static_cast<int64_t>(Utf8CharCount(text.data(), byte_offset)) + 1;
        return;
      }
      pos = match.data() + match.size() - input.data();
      if (FOLLY_UNLIKELY(match.size() == 0)) {
        pos = Utf8NextCharPos(text.data(), text.size(), pos);
      }
    }
    result = 0;
  }

 private:
  RE2& EnsurePattern(const arg_type<velox::Varchar>& pattern) {
    if (_re.has_value()) {
      return *_re;
    }
    return *_cache.findOrCompile(
      velox::StringView(pattern.data(), pattern.size()));
  }

  std::optional<RE2> _re;
  velox::functions::detail::ReCache _cache;
};

// Returns the first matching substring, or NULL if no match.
// Equivalent to regexp_extract(string, pattern).
template<typename T>
struct PgRegexpSubstr {
  PgRegexpSubstr() : _cache(0) {}

  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr int32_t reuse_strings_from_arg = 0;

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<velox::Varchar>*,
                                      const arg_type<velox::Varchar>* pattern) {
    if (pattern) {
      _re.emplace(re2::StringPiece(pattern->data(), pattern->size()),
                  RE2::Quiet);
      if (!_re->ok()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_REGULAR_EXPRESSION),
                        ERR_MSG("invalid regular expression: ", _re->error()));
      }
    }
    _cache.setMaxCompiledRegexes(config.exprMaxCompiledRegexes());
  }

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varchar>& result,
                                const arg_type<velox::Varchar>& text,
                                const arg_type<velox::Varchar>& pattern) {
    auto& re = EnsurePattern(pattern);
    re2::StringPiece input(text.data(), text.size());
    re2::StringPiece match;

    if (!re.Match(input, 0, text.size(), RE2::UNANCHORED, &match, 1)) {
      return false;
    }
    result.setNoCopy(velox::StringView(match.data(), match.size()));
    return true;
  }

 private:
  RE2& EnsurePattern(const arg_type<velox::Varchar>& pattern) {
    if (_re.has_value()) {
      return *_re;
    }
    return *_cache.findOrCompile(
      velox::StringView(pattern.data(), pattern.size()));
  }

  std::optional<RE2> _re;
  velox::functions::detail::ReCache _cache;
};

template<typename T>
struct PgRegexpCount3 {
  PgRegexpCount3() : _cache(0) {}

  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<velox::Varchar>*,
                                      const arg_type<velox::Varchar>*,
                                      const int64_t*) {
    _cache.setMaxCompiledRegexes(config.exprMaxCompiledRegexes());
  }

  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& result,
                                const arg_type<velox::Varchar>& text,
                                const arg_type<velox::Varchar>& pattern,
                                const int64_t& start) {
    auto& re =
      *_cache.findOrCompile(velox::StringView(pattern.data(), pattern.size()));
    re2::StringPiece input(text.data(), text.size());
    re2::StringPiece match;
    int64_t count = 0;
    // Convert 1-based character start to 0-based byte offset.
    size_t pos = start > 1 ? Utf8Advance(text.data(), text.size(),
                                         static_cast<size_t>(start - 1))
                           : 0;

    while (re.Match(input, pos, text.size(), RE2::UNANCHORED, &match, 1)) {
      ++count;
      pos = match.data() + match.size() - input.data();
      if (FOLLY_UNLIKELY(match.size() == 0)) {
        pos = Utf8NextCharPos(text.data(), text.size(), pos);
      }
    }
    result = count;
  }

 private:
  velox::functions::detail::ReCache _cache;
};

// regexp_substr(string, pattern, start [, N [, flags [, subexpr]]])
// 3-arg overload: start position
template<typename T>
struct PgRegexpSubstr3 {
  PgRegexpSubstr3() : _cache(0) {}

  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr int32_t reuse_strings_from_arg = 0;

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<velox::Varchar>*,
                                      const arg_type<velox::Varchar>*,
                                      const int64_t*) {
    _cache.setMaxCompiledRegexes(config.exprMaxCompiledRegexes());
  }

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varchar>& result,
                                const arg_type<velox::Varchar>& text,
                                const arg_type<velox::Varchar>& pattern,
                                const int64_t& start) {
    auto& re =
      *_cache.findOrCompile(velox::StringView(pattern.data(), pattern.size()));
    re2::StringPiece input(text.data(), text.size());
    re2::StringPiece match;
    // Convert 1-based character start to 0-based byte offset.
    size_t pos = start > 1 ? Utf8Advance(text.data(), text.size(),
                                         static_cast<size_t>(start - 1))
                           : 0;

    if (!re.Match(input, pos, text.size(), RE2::UNANCHORED, &match, 1)) {
      return false;
    }
    result.setNoCopy(velox::StringView(match.data(), match.size()));
    return true;
  }

 private:
  velox::functions::detail::ReCache _cache;
};

}  // namespace

void registerRegexpFunctions(const std::string& prefix) {
  velox::registerFunction<PgRegexpMatch, velox::Array<velox::Varchar>,
                          velox::Varchar, velox::Varchar>(
    {prefix + "regexp_match"});
  velox::registerFunction<PgRegexpCount, int64_t, velox::Varchar,
                          velox::Varchar>({prefix + "regexp_count"});
  velox::registerFunction<PgRegexpInstr, int64_t, velox::Varchar,
                          velox::Varchar>({prefix + "regexp_instr"});
  velox::registerFunction<PgRegexpInstr4, int64_t, velox::Varchar,
                          velox::Varchar, int64_t, int64_t>(
    {prefix + "regexp_instr"});
  velox::registerFunction<PgRegexpSubstr, velox::Varchar, velox::Varchar,
                          velox::Varchar>({prefix + "regexp_substr"});
  velox::registerFunction<PgRegexpCount3, int64_t, velox::Varchar,
                          velox::Varchar, int64_t>({prefix + "regexp_count"});
  velox::registerFunction<PgRegexpSubstr3, velox::Varchar, velox::Varchar,
                          velox::Varchar, int64_t>({prefix + "regexp_substr"});
  // TODO: regexp_matches (set-returning)
  // TODO: regexp_split_to_table (set-returning)
}

}  // namespace sdb::pg::functions
