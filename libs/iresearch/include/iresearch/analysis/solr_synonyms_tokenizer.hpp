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
#include <vector>

#include "iresearch/analysis/analyzer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

class SolrSynonymsTokenizer final : public TypedAnalyzer<SolrSynonymsTokenizer>,
                                    private util::Noncopyable {
 public:
  // synonyms_list represents either a full synonym line from Solr format,
  // or split halves (left/right side of '=>' for one-way mappings)
  // Examples:
  //   Full line:  ["i-pod", "i pod", "ipod"]
  //   Split form: "i-pod, i pod => ipod"
  //               |------------|  |----|
  //                  left side     right side
  //               ["i-pod", "i pod"] ["ipod"]
  using SynonymsList = std::vector<std::string_view>;

  struct SynonymsLine;
  using SynonymsLines = std::vector<SynonymsLine>;
  using SynonymsMap =
    absl::flat_hash_map<std::string_view, const SynonymsList*>;

  // Represents a parsed synonym line from Solr format.
  // - If 'in' is empty: this is a bidirectional synonym (full line)
  //   Example: "i-pod, i pod, ipod" -> in=[], out=["i-pod", "i pod", "ipod"]
  // - If 'in' is non-empty: this is a one-way mapping ('=>' was present)
  //   Example: "i-pod, i pod => ipod" -> in=["i-pod", "i pod"], out=["ipod"]
  struct SynonymsLine final {
    SynonymsList in;
    SynonymsList out;

    bool operator==(const SynonymsLine& line) const = default;
  };

  // `synonyms` keys are string_views into `text`; its values point at
  // SynonymsLine elements in `lines`, whose own string_views also reference
  // `text`.
  // Members are listed in lifetime order: text must outlive lines,
  // lines must outlive the synonyms map.
  struct State {
    std::string text;
    SynonymsLines lines;
    SynonymsMap synonyms;
  };

  struct Options {
    using Owner = SolrSynonymsTokenizer;
    // Inline synonyms file content (Solr format).
    std::string synonyms_text;
  };
  static Analyzer::ptr Make(Options opts);

  static constexpr std::string_view type_name() noexcept {
    return "solr_synonyms";
  }

  static SynonymsLines ParseSynonymsLines(std::string_view input);
  static SynonymsMap Parse(const SynonymsLines& lines);
  static std::shared_ptr<const State> MakeState(std::string text);

  explicit SolrSynonymsTokenizer(std::shared_ptr<const State> state) noexcept;

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }
  bool next() final;
  bool reset(std::string_view data) final;

 private:
  using Attributes = std::tuple<IncAttr, OffsAttr, TermAttr>;

  std::shared_ptr<const State> _state;
  Attributes _attrs;
  const std::string_view* _begin{};
  const std::string_view* _curr{};
  const std::string_view* _end{};
  std::string_view _holder{};
};

}  // namespace irs::analysis
