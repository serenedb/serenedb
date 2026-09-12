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

#include <cstdint>
#include <iresearch/analysis/token_batch.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <optional>
#include <ostream>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "tokenizer_fuzz_specs.hpp"

namespace tests::fuzz {

struct Token {
  std::string term;
  uint32_t pos = 0;
  uint32_t offs_start = 0;
  uint32_t offs_end = 0;

  bool operator==(const Token&) const = default;
};

std::ostream& operator<<(std::ostream& os, const Token& token);

struct Result {
  bool ok = false;
  std::vector<Token> tokens;
  std::string store;
};

struct Row {
  const std::string* value = nullptr;
};

enum class BlockMode : uint8_t {
  Flat,
  Reverse,
  Repeat,
  Constant,
};

std::string_view BlockModeName(BlockMode mode) noexcept;

std::vector<irs::TokenLayout> DeclaredLayouts(const irs::TokenTraits& traits);

std::string_view LayoutName(irs::TokenLayout layout) noexcept;

bool IsAscii(std::string_view value) noexcept;

bool IsValidUtf8(std::string_view value) noexcept;

Result AnalyzeValue(irs::analysis::Tokenizer& tokenizer, std::string_view value,
                    irs::TokenLayout layout, irs::BlockTraits hint = {});

std::vector<Result> FillBlock(irs::analysis::Tokenizer& tokenizer,
                              std::span<const Row> rows,
                              irs::TokenLayout layout, BlockMode mode,
                              std::string* error = nullptr);

std::optional<std::string> ValueInvariants(const irs::TokenTraits& traits,
                                           std::string_view value,
                                           irs::TokenLayout layout,
                                           const Result& res);

class Probe {
 public:
  explicit Probe(const Spec& spec);

  std::optional<std::string> operator()(std::string_view value, bool full);

  uint64_t behaviour() const noexcept { return _behaviour; }
  const irs::TokenTraits& traits() const noexcept { return _traits; }
  const std::vector<irs::TokenLayout>& layouts() const noexcept {
    return _layouts;
  }
  bool valid() const noexcept { return _primary != nullptr; }

 private:
  std::optional<std::string> CheckLayout(std::string_view value,
                                         irs::TokenLayout layout, bool full,
                                         Result& out);

  const Spec* _spec;
  irs::analysis::Tokenizer::ptr _primary;
  irs::analysis::Tokenizer::ptr _shadow;
  irs::analysis::Tokenizer::ptr _blocked;
  std::vector<irs::analysis::Tokenizer::ptr> _children;
  irs::TokenTraits _traits;
  std::vector<irs::TokenLayout> _layouts;
  uint64_t _behaviour = 0;
};

struct PostingsDigest {
  uint64_t lane0 = 0;
  uint64_t lane1 = 0;
  uint64_t entries = 0;
  uint64_t occurrences = 0;

  void Add(std::string_view term, uint64_t row, uint32_t freq,
           std::span<const uint32_t> pos, std::span<const uint32_t> offs_start,
           std::span<const uint32_t> offs_end);

  bool operator==(const PostingsDigest&) const = default;

  std::string Describe() const;
};

void FoldValue(PostingsDigest& digest, uint64_t row, const Result& res,
               bool with_freq, bool with_pos, bool with_offs);

size_t Drain(irs::analysis::Tokenizer& tokenizer,
             std::span<const std::string> values, irs::TokenLayout layout,
             size_t width);

uint64_t Seed();

std::vector<const Spec*> SelectedSpecs();

std::vector<const Spec*> SelectedFamilies();

size_t ValueBudget(const Spec& spec, size_t base);

size_t SizeCap(const Spec& spec);

std::vector<std::string> SpecCorpus(const Spec& spec, uint64_t seed,
                                    size_t random_count);

void CheckSpec(const Spec& spec, std::span<const std::string> values);

void CheckSpecBlocks(const Spec& spec, std::span<const std::string> values);

void CheckSpecStableTerms(const Spec& spec,
                          std::span<const std::string> values);

}  // namespace tests::fuzz
