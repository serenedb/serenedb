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
#include <functional>
#include <random>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace tests::fuzz {

const std::vector<std::string>& Utf8Adversary();

const std::vector<std::string>& BaseDictionary();

std::vector<std::string> ExpandDictionary(
  const std::vector<std::string>& words);

class Mutator {
 public:
  Mutator(uint64_t seed, std::vector<std::string> dict, size_t size_cap);

  std::string Mutate(std::string_view in);
  std::string Splice(std::string_view a, std::string_view b);
  std::string Generate();

  uint64_t Bits() { return _rng(); }
  size_t Below(size_t n) {
    return n == 0 ? 0 : static_cast<size_t>(_rng() % n);
  }
  bool Chance(size_t one_in) { return Below(one_in) == 0; }

  size_t size_cap() const noexcept { return _cap; }
  static constexpr size_t kOperators = 22;
  const char* LastOperator() const noexcept { return _last; }

 private:
  void Apply(std::string& s, size_t op);
  std::string_view Dict();
  size_t Pos(const std::string& s);
  size_t Span(const std::string& s, size_t from);
  void Clamp(std::string& s);

  std::mt19937_64 _rng;
  std::vector<std::string> _dict;
  size_t _cap;
  const char* _last = "none";
};

class FeedbackCorpus {
 public:
  explicit FeedbackCorpus(size_t capacity = 4096) : _capacity{capacity} {}

  void Seed(std::string value);
  bool Offer(std::string value, uint64_t behaviour);
  std::string_view Pick(Mutator& mutator) const;

  size_t size() const noexcept { return _entries.size(); }
  size_t classes() const noexcept { return _classes.size(); }
  size_t bytes() const noexcept { return _bytes; }

 private:
  std::vector<std::string> _entries;
  std::unordered_set<uint64_t> _classes;
  size_t _capacity;
  size_t _bytes = 0;
};

uint64_t BehaviourClass(bool rejected, size_t ntokens, size_t term_bytes,
                        size_t max_term, size_t store_bytes, uint32_t last_pos);

std::string Shrink(std::string_view failing,
                   const std::function<bool(std::string_view)>& still_fails,
                   size_t max_steps = 2000);

}  // namespace tests::fuzz
