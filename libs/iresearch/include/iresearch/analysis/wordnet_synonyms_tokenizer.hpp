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

#include <absl/container/flat_hash_map.h>
#include <vpack/builder.h>
#include <vpack/slice.h>

#include <memory>
#include <string>
#include <string_view>

#include "analyzers.hpp"
#include "basics/result.h"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/utils/attribute_helper.hpp"

namespace irs::analysis {

class WordnetSynonymsTokenizer final
  : public TypedAnalyzer<WordnetSynonymsTokenizer>,
    private util::Noncopyable {
 public:
  using SynonymsGroups = std::vector<std::string_view>;
  using SynonymsMap = absl::flat_hash_map<std::string, SynonymsGroups>;

  static constexpr std::string_view type_name() noexcept {
    return "wordnet_synonyms";
  }

  static sdb::ResultOr<SynonymsMap> Parse(std::string_view input);

  // Test ctor: caller owns the storage that values' string_views point into.
  explicit WordnetSynonymsTokenizer(SynonymsMap&& mapping);

  // Production: instance owns `text` and the map derived from it.
  static sdb::ResultOr<std::unique_ptr<WordnetSynonymsTokenizer>> FromText(
    std::string text);

  // Factory hooks registered with iresearch's analyzer registry.
  static Analyzer::ptr MakeVPack(vpack::Slice slice);
  static Analyzer::ptr MakeVPack(std::string_view args);
  static Analyzer::ptr MakeJson(std::string_view args);
  static bool NormalizeVPackConfig(vpack::Slice slice, vpack::Builder* builder);
  static bool NormalizeVPackConfig(std::string_view args, std::string& config);
  static bool NormalizeJsonConfig(std::string_view args, std::string& config);
  static void init();

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }
  bool next() final;
  bool reset(std::string_view data) final;

 private:
  // When constructed via FromText / factory, `_text_storage` keeps the data
  // alive that `_mapping`'s value views point at.
  std::string _text_storage;
  SynonymsMap _mapping;

  using attributes = std::tuple<IncAttr, OffsAttr, TermAttr>;
  attributes _attrs;

  const std::string_view* _begin{};
  const std::string_view* _curr{};
  const std::string_view* _end{};
  bool _term_exists = false;
};

}  // namespace irs::analysis
