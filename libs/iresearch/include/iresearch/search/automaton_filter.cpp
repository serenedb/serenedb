#include "automaton_filter.hpp"

#include "iresearch/utils/automaton_combinators.hpp"
#include "iresearch/utils/automaton_utils.hpp"
#include "iresearch/utils/regexp_utils.hpp"
#include "iresearch/utils/wildcard_utils.hpp"

namespace irs {
namespace {

automaton BuildLeaf(const AutomatonPattern& p) {
  if (p.kind == AutomatonPattern::Kind::Wildcard) {
    return FromWildcard(bytes_view{p.pattern});
  }
  return FromRegexp(bytes_view{p.pattern}, kDefaultMaxDfaStates, p.syntax);
}

automaton Evaluate(const std::vector<AutomatonNode>& nodes) {
  std::vector<automaton> stack;
  stack.reserve(nodes.size());

  for (const auto& node : nodes) {
    switch (node.kind) {
      case AutomatonNode::Kind::Leaf: {
        stack.push_back(BuildLeaf(node.pattern));
        break;
      }
      case AutomatonNode::Kind::Union: {
        if (stack.size() < 2) {
          SDB_ASSERT(false);
          return {};
        }
        auto rhs = std::move(stack.back());
        stack.pop_back();
        auto lhs = std::move(stack.back());
        stack.pop_back();
        auto combined = node.union_method == AutomatonUnionMethod::DeMorgan
                          ? UnionAutomatonsDeMorgan(lhs, rhs)
                          : UnionAutomatons(lhs, rhs);
        if (combined.Start() == fst::kNoStateId) {
          return {};
        }
        stack.push_back(std::move(combined));
        break;
      }
      case AutomatonNode::Kind::Intersection: {
        if (stack.size() < 2) {
          SDB_ASSERT(false);
          return {};
        }
        auto rhs = std::move(stack.back());
        stack.pop_back();
        auto lhs = std::move(stack.back());
        stack.pop_back();
        auto combined = IntersectAutomatons(lhs, rhs);
        if (combined.Start() == fst::kNoStateId) {
          return {};
        }
        stack.push_back(std::move(combined));
        break;
      }
    }
  }

  if (stack.size() != 1) {
    SDB_ASSERT(false);
    return {};
  }
  return std::move(stack[0]);
}

}  // namespace

Filter::Query::ptr ByAutomaton::prepare(const PrepareContext& ctx,
                                        std::string_view field,
                                        const ByAutomatonOptions& opts) {
  if (opts.nodes.empty()) {
    return Query::empty();
  }

  auto combined = Evaluate(opts.nodes);

  if (combined.Start() == fst::kNoStateId || !Validate(combined)) {
    return Query::empty();
  }

  return PrepareAutomatonFilter(ctx, field, combined, opts.scored_terms_limit);
}

}  // namespace irs
