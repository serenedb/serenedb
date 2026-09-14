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

#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <string_view>
#include <vector>

namespace tests::fuzz {

enum class Kind : uint8_t {
  Empty,
  AsciiWord,
  AsciiSentence,
  AsciiLong,
  MixedCase,
  Utf8Text,
  Utf8Wide,
  Emoji,
  InvalidUtf8,
  Binary,
  DelimiterSoup,
  Quoted,
  PathLike,
  Numeric,
  Json,
  Repeat,
  Whitespace,
  ManyTokens,
  HugeToken,
  Control,
};

inline constexpr size_t kKindCount = static_cast<size_t>(Kind::Control) + 1;

std::string_view KindName(Kind kind) noexcept;

class ValueGen {
 public:
  explicit ValueGen(uint64_t seed, size_t size_cap = 8192) noexcept
    : _rng{seed}, _cap{size_cap == 0 ? 1 : size_cap} {}

  std::string Next();
  std::string Next(Kind kind);

  size_t Below(size_t n) {
    return n == 0 ? 0 : static_cast<size_t>(_rng() % n);
  }

  bool Chance(size_t one_in) { return Below(one_in) == 0; }

  size_t cap() const noexcept { return _cap; }

 private:
  size_t Length(size_t soft_max);
  char Letter();
  void AppendWord(std::string& out, size_t len);
  void AppendUtf8Word(std::string& out);

  std::mt19937_64 _rng;
  size_t _cap;
};

const std::vector<std::string>& EdgeCases();
const std::vector<std::string>& GeoJsonValues();
const std::vector<std::string>& GeoWkbValues();
const std::vector<std::string>& PathValues();
const std::vector<std::string>& CsvValues();
const std::vector<std::string>& WordValues();

std::vector<std::string> MakeCorpus(uint64_t seed, size_t count,
                                    const std::vector<std::string>& seeded = {},
                                    size_t size_cap = 8192);

uint64_t EnvU64(const char* name, uint64_t fallback);

std::string Describe(std::string_view value);

}  // namespace tests::fuzz
