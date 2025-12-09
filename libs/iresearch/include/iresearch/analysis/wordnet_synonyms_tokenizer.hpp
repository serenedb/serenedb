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
  // Currently I expect the input data to remain valid after parsing. I suppose
  // in the future it would be a good idea to transfer full ownership to the
  // tokenizer - then we should reconsider using string_view.
  using SynonymsGroups = std::vector<std::string_view>;
  using SynonymsMap = absl::flat_hash_map<std::string, std::string_view>;

  struct WordnetInput final {
    SynonymsGroups groups;
    SynonymsMap mapping;
  };

  static constexpr std::string_view type_name() noexcept {
    return "wordnet_synonyms";
  }

  static sdb::ResultOr<WordnetInput> Parse(std::string_view input);

  explicit WordnetSynonymsTokenizer(SynonymsGroups&& groups,
                                    SynonymsMap&& mapping);
  Attribute* GetMutable(TypeInfo::type_id type) final {
    return irs::GetMutable(_attrs, type);
  }
  bool next() final;
  bool reset(std::string_view data) final;

 private:
  const SynonymsGroups _groups;
  const SynonymsMap _mapping;

  using attributes = std::tuple<IncAttr, OffsAttr, TermAttr>;
  attributes _attrs;

  bool _term_exists = false;
};

}  // namespace irs::analysis
