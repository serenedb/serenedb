#pragma once

#include <vector>

#include "iresearch/search/filter.hpp"
#include "iresearch/utils/regexp_utils.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class ByAutomaton;

enum class AutomatonUnionMethod : uint8_t {
  RefinedDeterminize = 0,
  DeMorgan = 1,
};

struct AutomatonPattern {
  bstring pattern;

  enum class Kind : uint8_t {
    Wildcard = 0,
    Regexp = 1,
  };

  Kind kind = Kind::Wildcard;
  RegexpSyntax syntax = RegexpSyntax::Perl;

  bool operator==(const AutomatonPattern&) const noexcept = default;
};

struct AutomatonNode {
  enum class Kind : uint8_t {
    Leaf = 0,
    Union = 1,
    Intersection = 2,
  };

  Kind kind = Kind::Leaf;
  AutomatonPattern pattern;
  AutomatonUnionMethod union_method = AutomatonUnionMethod::RefinedDeterminize;

  static AutomatonNode MakeLeaf(AutomatonPattern p) {
    return {Kind::Leaf, std::move(p)};
  }
  static AutomatonNode MakeUnion(
    AutomatonUnionMethod m = AutomatonUnionMethod::RefinedDeterminize) {
    AutomatonNode n;
    n.kind = Kind::Union;
    n.union_method = m;
    return n;
  }
  static AutomatonNode MakeIntersection() {
    AutomatonNode n;
    n.kind = Kind::Intersection;
    return n;
  }

  bool operator==(const AutomatonNode&) const noexcept = default;
};

struct ByAutomatonOptions {
  using FilterType = ByAutomaton;

  std::vector<AutomatonNode> nodes;
  size_t scored_terms_limit = 1024;

  bool operator==(const ByAutomatonOptions&) const noexcept = default;
};

class ByAutomaton final : public FilterWithField<ByAutomatonOptions> {
 public:
  static Query::ptr prepare(const PrepareContext& ctx, std::string_view field,
                            const ByAutomatonOptions& opts);

  Query::ptr prepare(const PrepareContext& ctx) const final {
    return prepare(ctx.Boost(Boost()), field(), options());
  }
};

}  // namespace irs
