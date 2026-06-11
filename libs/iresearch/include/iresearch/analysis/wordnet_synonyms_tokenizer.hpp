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

#include <memory>
#include <string>
#include <string_view>

#include "basics/result.h"
#include "basics/result_or.h"
#include "iresearch/analysis/analyzer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/utils/attribute_helper.hpp"

namespace irs::analysis {

class WordnetSynonymsTokenizer final
  : public TypedAnalyzer<WordnetSynonymsTokenizer>,
    private util::Noncopyable {
 public:
  using SynonymsGroups = std::vector<std::string_view>;
  using SynonymsMap = absl::flat_hash_map<std::string, SynonymsGroups>;

  // `mapping`'s value string_views reference `text`; the keys own their own
  // storage.
  // Members are listed in lifetime order: text must outlive mapping.
  struct State {
    std::string text;
    SynonymsMap mapping;
  };

  struct Options {
    using Owner = WordnetSynonymsTokenizer;
    // Inline synonyms file content (Wordnet `s(...)` lines).
    std::string synonyms_text;
  };
  static Analyzer::ptr Make(Options opts);

  static constexpr std::string_view type_name() noexcept {
    return "wordnet_synonyms";
  }

  static sdb::ResultOr<SynonymsMap> Parse(std::string_view input);
  static sdb::ResultOr<std::shared_ptr<const State>> MakeState(
    std::string text);

  explicit WordnetSynonymsTokenizer(
    std::shared_ptr<const State> state) noexcept;

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }
  bool next() final;
  bool reset(std::string_view data) final;

 private:
  std::shared_ptr<const State> _state;

  using attributes = std::tuple<IncAttr, OffsAttr, TermAttr>;
  attributes _attrs;

  const std::string_view* _begin{};
  const std::string_view* _curr{};
  const std::string_view* _end{};
  bool _term_exists = false;
};

}  // namespace irs::analysis
