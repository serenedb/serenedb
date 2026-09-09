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

#include <bitset>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/analysis/tokenizer.hpp"

namespace tests::fuzz {

enum class Model : uint8_t {
  None,
  Keyword,
  SplitChar,
  RunsOfSet,
  NGramBytes,
  Chain,
  Sql,
};

struct ModelParams {
  char delim = 0;
  std::bitset<256> token_bytes;
  bool ascii_only = false;
  irs::Case convert = irs::Case::None;
  size_t min_gram = 0;
  size_t max_gram = 0;
  std::string expression;
};

struct SqlVerdict {
  enum class Kind : uint8_t {
    Rejected,
    Tokens,
    Error,
  };

  Kind kind = Kind::Error;
  std::vector<std::string> terms;
  std::string error;
};

std::string_view SqlVerdictName(SqlVerdict::Kind kind) noexcept;

class SqlOracle {
 public:
  static SqlOracle& Shared();

  SqlOracle();
  ~SqlOracle();

  std::optional<std::string> Prepare(const std::string& expression);

  SqlVerdict Evaluate(const std::string& expression, std::string_view value);

 private:
  struct Impl;
  std::unique_ptr<Impl> _impl;
};

std::bitset<256> ByteSet(std::string_view members, bool negate = false);

std::bitset<256> AlnumBytes();

struct ModelToken {
  std::string term;
  uint32_t pos = 0;
};

struct Spec;

std::optional<std::vector<ModelToken>> ModelTokens(
  Model model, const ModelParams& params,
  std::span<const irs::analysis::Tokenizer::ptr> children,
  std::string_view value);

std::string_view ModelName(Model model) noexcept;

}  // namespace tests::fuzz
