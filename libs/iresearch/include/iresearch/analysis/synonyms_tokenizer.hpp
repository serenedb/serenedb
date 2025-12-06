#pragma once

#include <absl/container/flat_hash_map.h>

#include <iresearch/analysis/token_attributes.hpp>

#include "analyzers.hpp"
#include "iresearch/utils/attribute_helper.hpp"

namespace irs::analysis {

class SynonymsTokenizer final : public TypedAnalyzer<SynonymsTokenizer>,
                                private util::Noncopyable {
 public:
  using synonyms_map =
    absl::flat_hash_map<std::string, std::vector<std::string_view>*>;

  static constexpr std::string_view type_name() noexcept { return "synonyms"; }

  explicit SynonymsTokenizer(synonyms_map&&);
  Attribute* GetMutable(TypeInfo::type_id type) final {
    return irs::GetMutable(_attrs, type);
  }
  bool next() final;
  bool reset(std::string_view data) final;

 private:
  synonyms_map _synonyms;

  using attributes = std::tuple<IncAttr, OffsAttr, TermAttr>;
  attributes _attrs;

  std::string_view* _begin{};
  std::string_view* _curr{};
  std::string_view* _end{};

  std::string_view _holder{};
};

}  // namespace irs::analysis
