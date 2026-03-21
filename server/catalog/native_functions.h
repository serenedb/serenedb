////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "catalog/function.h"

namespace sdb::native {

void MakeAlias(std::string_view alias, std::string_view existing);

const catalog::Function* GetFunction(std::string_view name);

void VisitFunctions(absl::FunctionRef<void(const catalog::Function&)> visitor);

void ClearFunctions();

inline constexpr auto kMaxArguments = std::numeric_limits<uint16_t>::max();

constexpr catalog::FunctionSignature MakeSignature(
  uint16_t required_arguments = 0, uint16_t optional_arguments = 0) {
  const auto max_arguments =
    static_cast<uint16_t>(required_arguments + optional_arguments);
  return catalog::FunctionSignature{
    .parameters = std::vector<catalog::FunctionParameter>(max_arguments),
    .required_arguments = required_arguments,
    .max_arguments = max_arguments,
  };
}

constexpr void Variadic(catalog::FunctionSignature& signature) {
  signature.parameters.back().mode = catalog::FunctionParameter::Mode::Variadic;
  signature.max_arguments = kMaxArguments;
}

constexpr auto Collection(std::span<const uint16_t> positions) {
  return [positions](catalog::FunctionSignature& signature) {
    for (auto p : positions) {
      signature.parameters[p].MarkAsCollection();
    }
  };
}

constexpr catalog::FunctionOptions MakeOptions() {
  return {
    .language = catalog::FunctionLanguage::AqlNative,
    .state = catalog::FunctionState::Immutable,
    .parallel = catalog::FunctionParallel::Safe,
  };
}

constexpr void Stable(catalog::FunctionOptions& options) {
  options.state = catalog::FunctionState::Stable;
}

constexpr void Volatile(catalog::FunctionOptions& options) {
  options.state = catalog::FunctionState::Volatile;
}

constexpr void DQL(catalog::FunctionOptions& options) {
  options.type = catalog::FunctionType::DQL;
}

constexpr void DDL(catalog::FunctionOptions& options) {
  options.type = catalog::FunctionType::DDL;
}

constexpr void Internal(catalog::FunctionOptions& options) {
  options.internal = true;
};

constexpr void NoPushdown(catalog::FunctionOptions& options) {
  options.no_pushdown = true;
}

constexpr void NoAnalyzer(catalog::FunctionOptions& options) {
  options.no_analyzer = true;
}

constexpr void NoEval(catalog::FunctionOptions& options) {
  options.no_eval = true;
}

constexpr void Table(catalog::FunctionOptions& options) {
  options.table = true;
}

}  // namespace sdb::native
