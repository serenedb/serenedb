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

#include "iresearch/search/optimizer/levenshtein_prefix_rules.hpp"

#include <array>
#include <cstddef>
#include <string_view>
#include <vector>

#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"

namespace irs::optimizer {
namespace {

struct LevenshteinPrefixFusionRule {
  static constexpr std::string_view kName = "levenshtein_prefix_fusion";
  static constexpr std::array kTargets{Type<And>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct PrefixEntry {
  const Filter* node;
  field_id field;
  bytes_view term;
};

bool LevenshteinPrefixFusionRule::Apply(Filter::ptr& slot,
                                        const OptimizeContext& ctx) {
  auto& node = sdb::basics::downCast<And>(*slot);
  auto& children = node.mutable_filters();

  std::vector<PrefixEntry> prefixes;
  for (size_t i = 0; i < children.size(); ++i) {
    if (children[i]->type() == Type<ByPrefix>::id()) {
      auto& prefix = sdb::basics::downCast<ByPrefix>(*children[i]);
      if (ctx.HasAnalyzer(prefix.field_id())) {
        continue;
      }
      prefixes.emplace_back(children[i].get(), prefix.field_id(),
                            prefix.options().term);
    }
  }
  if (prefixes.empty()) {
    return false;
  }

  bool changed = false;
  sdb::containers::FlatHashSet<const Filter*> remove_prefixes;
  for (auto& child : children) {
    if (child->type() != Type<ByEditDistance>::id()) {
      continue;
    }
    auto& filter = sdb::basics::downCast<ByEditDistance>(*child);
    if (ctx.HasAnalyzer(filter.field_id())) {
      continue;
    }
    auto& opts = *filter.mutable_options();

    bstring target = opts.prefix;
    target += opts.term;

    const PrefixEntry* best = nullptr;
    for (const auto& entry : prefixes) {
      if (entry.field != filter.field_id() ||
          !bytes_view{target}.starts_with(entry.term)) {
        continue;
      }
      changed = true;
      remove_prefixes.insert(entry.node);
      if (entry.term.size() <= opts.prefix.size()) {
        continue;
      }
      if (best == nullptr || entry.term.size() > best->term.size()) {
        best = &entry;
      }
    }
    if (best == nullptr) {
      continue;
    }

    opts.prefix.assign(target, 0, best->term.size());
    opts.term.assign(target, best->term.size());
  }
  auto it = std::remove_if(
    children.begin(), children.end(),
    [&](const auto& child) { return remove_prefixes.erase(child.get()) == 1; });
  children.erase(it, children.end());

  return changed;
}

}  // namespace

void InitLevenshteinPrefixRules() {
  RegisterRule<LevenshteinPrefixFusionRule>();
}

}  // namespace irs::optimizer
