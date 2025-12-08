#pragma once

#include <absl/container/flat_hash_map.h>

#include <iresearch/analysis/token_attributes.hpp>
#include <string_view>

#include "analyzers.hpp"
#include "basics/result.h"
#include "iresearch/utils/attribute_helper.hpp"

namespace irs::analysis {

class WordnetSynonymsTokenizer final
  : public TypedAnalyzer<WordnetSynonymsTokenizer>,
    private util::Noncopyable {
 public:
  using SynonymsGroup = std::vector<std::string_view>;
  using SynonymsMap = absl::flat_hash_map<std::string_view, std::string_view>;

  static constexpr std::string_view type_name() noexcept {
    return "wordnet_synonyms";
  }

  explicit WordnetSynonymsTokenizer(SynonymsGroup&& groups,
                                    SynonymsMap&& mapping);
  Attribute* GetMutable(TypeInfo::type_id type) final {
    return irs::GetMutable(_attrs, type);
  }
  bool next() final;
  bool reset(std::string_view data) final;

 private:
  const SynonymsGroup _groups;
  const SynonymsMap _mapping;

  using attributes = std::tuple<IncAttr, OffsAttr, TermAttr>;
  attributes _attrs;

  bool _term_exists = false;
};

}  // namespace irs::analysis
