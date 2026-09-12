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

#include <functional>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <string>
#include <vector>

#include "tokenizer_fuzz_oracles.hpp"

namespace tests::fuzz {

struct Spec {
  std::string name;
  std::function<irs::analysis::TokenizerConfig()> config;
  std::function<void(irs::analysis::Tokenizer&)> setup;
  std::vector<std::string> dict;
  std::vector<std::string> native;
  Model model = Model::None;
  ModelParams params;
  std::function<std::vector<irs::analysis::Tokenizer::ptr>()> model_children;
  bool utf8_only = false;
  uint32_t cost = 1;
};

const std::vector<Spec>& AllSpecs();

irs::analysis::Tokenizer::ptr Make(const Spec& spec);

bool ModelsAvailable();

}  // namespace tests::fuzz
