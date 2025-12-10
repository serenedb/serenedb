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

#include <iresearch/analysis/token_attributes.hpp>

#include "analyzers.hpp"
#include "basics/result.h"
#include "iresearch/utils/attribute_helper.hpp"

namespace irs::analysis {

class SolrSynonymsTokenizer final : public TypedAnalyzer<SolrSynonymsTokenizer>,
                                    private util::Noncopyable {
 public:
  /* synonyms_list represents either a full synonym line from Solr format,
   * or split halves (left/right side of '=>' for one-way mappings)
   * Examples:
   *   Full line:  ["i-pod", "i pod", "ipod"]
   *
   *   Split form: "i-pod, i pod => ipod"
   *               |------------|  |----|
   *                  left side     right side
   *               ["i-pod", "i pod"] ["ipod"]
   */
  using SynonymsList = std::vector<std::string_view>;

  struct SynonymsLine;
  using SynonymsLines = std::vector<SynonymsLine>;
  using SynonymsMap =
    absl::flat_hash_map<std::string_view, const SynonymsList*>;

  /* Represents a parsed synonym line from Solr format.
   * - If 'in' is empty: this is a bidirectional synonym (full line)
   *   Example: "i-pod, i pod, ipod" -> in=[], out=["i-pod", "i pod", "ipod"]
   *
   * - If 'in' is non-empty: this is a one-way mapping ('=>' was present)
   *   Example: "i-pod, i pod => ipod" -> in=["i-pod", "i pod"], out=["ipod"]
   */
  struct SynonymsLine final {
    SynonymsList in;
    SynonymsList out;

    bool operator==(const SynonymsLine& line) const = default;
  };

  static constexpr std::string_view type_name() noexcept {
    return "solr_synonyms";
  }

  static sdb::ResultOr<SynonymsLines> ParseSynonymsLines(
    std::string_view input);
  static sdb::ResultOr<SynonymsMap> Parse(const SynonymsLines& lines);

  explicit SolrSynonymsTokenizer(SynonymsMap&&);
  Attribute* GetMutable(TypeInfo::type_id type) final {
    return irs::GetMutable(_attrs, type);
  }
  bool next() final;
  bool reset(std::string_view data) final;

 private:
  SynonymsMap _synonyms;

  using attributes = std::tuple<IncAttr, OffsAttr, TermAttr>;
  attributes _attrs;

  const std::string_view* _begin{};
  const std::string_view* _curr{};
  const std::string_view* _end{};

  std::string_view _holder{};
};

}  // namespace irs::analysis
