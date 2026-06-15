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
  size_t index;
  field_id field;
  bytes_view term;
};

bool ImpliesPrefix(const Filter& child, field_id field, bytes_view prefix) {
  if (child.type() != Type<ByEditDistance>::id()) {
    return false;
  }
  const auto& edit = sdb::basics::downCast<ByEditDistance>(child);
  return edit.field_id() == field &&
         bytes_view{edit.options().prefix}.starts_with(prefix);
}

bool LevenshteinPrefixFusionRule::Apply(Filter::ptr& slot,
                                        const OptimizeContext& /*ctx*/) {
  auto& node = sdb::basics::downCast<And>(*slot);
  auto& children = node.mutable_filters();

  std::vector<PrefixEntry> prefixes;
  for (size_t i = 0; i < children.size(); ++i) {
    if (children[i]->type() == Type<ByPrefix>::id()) {
      auto& prefix = sdb::basics::downCast<ByPrefix>(*children[i]);
      prefixes.emplace_back(i, prefix.field_id(), prefix.options().term);
    }
  }
  if (prefixes.empty()) {
    return false;
  }

  bool changed = false;
  for (auto& child : children) {
    if (child->type() != Type<ByEditDistance>::id()) {
      continue;
    }
    auto& edit = sdb::basics::downCast<ByEditDistance>(*child);
    auto& opts = *edit.mutable_options();
    bstring full_target;
    full_target.reserve(opts.prefix.size() + opts.term.size());
    full_target += opts.prefix;
    full_target += opts.term;

    const PrefixEntry* best = nullptr;
    for (const auto& entry : prefixes) {
      if (entry.field != edit.field_id() ||
          entry.term.size() < opts.prefix.size() ||
          !bytes_view{full_target}.starts_with(entry.term)) {
        continue;
      }
      if (best == nullptr || entry.term.size() > best->term.size()) {
        best = &entry;
      }
    }
    if (best == nullptr || bytes_view{opts.prefix} == best->term) {
      continue;
    }
    opts.term = full_target.substr(best->term.size());
    opts.prefix.assign(best->term);
    changed = true;
  }

  std::vector<bool> remove(children.size(), false);
  for (const auto& entry : prefixes) {
    for (size_t j = 0; j < children.size(); ++j) {
      if (j != entry.index &&
          ImpliesPrefix(*children[j], entry.field, entry.term)) {
        remove[entry.index] = true;
        break;
      }
    }
  }

  size_t removed = 0;
  auto out = children.begin();
  for (size_t i = 0; i < children.size(); ++i) {
    if (remove[i]) {
      ++removed;
      continue;
    }
    *out++ = std::move(children[i]);
  }
  if (removed != 0) {
    children.erase(out, children.end());
    changed = true;
  }
  return changed;
}

}  // namespace

void InitLevenshteinPrefixRules() {
  RegisterRule<LevenshteinPrefixFusionRule>();
}

}  // namespace irs::optimizer
