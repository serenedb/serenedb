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

#include "connector/scan/scan_profile.h"

#include <absl/algorithm/container.h>
#include <absl/debugging/internal/demangle.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>
#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>

#include <duckdb/common/string_format.hpp>
#include <iterator>
#include <string>
#include <utility>

namespace sdb::connector {
namespace {

using Info = ScanProfile::Info;

std::vector<const irs::Filter*> FiltersOf(const irs::FilterTree& tree) {
  std::vector<const irs::Filter*> filters;
  filters.reserve(tree.boxes.size());
  for (const auto& box : tree.boxes) {
    filters.push_back(box.filter);
  }
  return filters;
}

double Seconds(uint64_t ticks) {
  return static_cast<double>(
           duckdb::TimePoint::ToNanos(static_cast<int64_t>(ticks))) /
         1e9;
}

duckdb::ExplainValue Timing(uint64_t ticks) {
  return duckdb::ExplainValue::Timing(Seconds(ticks));
}

std::string Count(uint64_t count) {
  return duckdb::StringFormat::Format("{:,}", {duckdb::FormatArgument{count}});
}

uint64_t Minus(uint64_t total, uint64_t part) {
  return total > part ? total - part : 0;
}

bool CountsPostingLists(irs::QueryKind kind) {
  return kind == irs::QueryKind::Term || kind == irs::QueryKind::Terms ||
         kind == irs::QueryKind::Boolean || kind == irs::QueryKind::Phrase;
}

std::string ClassName(std::string_view type) {
  return absl::StrReplaceAll(type.substr(0, type.find_first_of("<,>")),
                             {{"irs::", ""}, {"(anonymous namespace)::", ""}});
}

bool RunsItsArgument(std::string_view name) {
  const auto last = name.substr(name.rfind(':') + 1);
  return last == "Walk" || last == "ConstantWalk" || last == "Impl";
}

std::string ShapeName(const std::type_info& type) {
  const auto demangled = absl::debugging_internal::DemangleString(type.name());
  std::string_view shape = demangled;
  absl::ConsumePrefix(&shape, "irs::memory::OnArena<");
  const std::pair<std::string_view, std::string_view> parts =
    absl::StrSplit(shape, absl::MaxSplits('<', 1));
  auto name = ClassName(parts.first);
  if (RunsItsArgument(name)) {
    absl::StrAppend(&name, "<", ClassName(parts.second), ">");
  }
  return name;
}

void AddTiming(Info& info, std::string_view key, uint64_t ticks) {
  if (ticks != 0) {
    info[std::string{key}] = Timing(ticks);
  }
}

uint64_t EstimatedTicks(const irs::QueryProfile::Executed& slot,
                        uint64_t timer_ticks) {
  const uint64_t timed = slot.timed_calls;
  if (timed == 0) {
    return 0;
  }
  const auto measured = Minus(slot.ticks, timed * timer_ticks);
  return static_cast<uint64_t>(static_cast<double>(measured) *
                               static_cast<double>(slot.calls) /
                               static_cast<double>(timed));
}

template<typename Node>
void PreOrder(Node& node, std::vector<Node*>& out) {
  out.push_back(&node);
  for (auto& child : node.children) {
    PreOrder(child, out);
  }
}

class ProfiledLookup final : public irs::IndexSource {
 public:
  ProfiledLookup(ScanProfile& profile, std::unique_ptr<irs::IndexSource> inner)
    : _profile{profile}, _inner{std::move(inner)} {}

  duckdb::idx_t Materialize(duckdb::ClientContext& context, duckdb::Vector& pk,
                            duckdb::idx_t count,
                            duckdb::DataChunk& output) final {
    const auto start = duckdb::TimePoint::Now();
    const auto rows = _inner->Materialize(context, pk, count, output);
    _profile.AddLookup(ElapsedTicks(start), count, count - rows);
    return rows;
  }

  std::span<const duckdb::idx_t> Survivors() const noexcept final {
    return _inner->Survivors();
  }

 private:
  ScanProfile& _profile;
  std::unique_ptr<irs::IndexSource> _inner;
};

}  // namespace

std::unique_ptr<irs::IndexSource> ProfileLookup(
  ScanProfile* profile, std::unique_ptr<irs::IndexSource> source) {
  if (!profile) {
    return source;
  }
  return std::make_unique<ProfiledLookup>(*profile, std::move(source));
}

ScanProfile::ScanProfile(irs::FilterTree filter, ScanChecks checks,
                         std::string lookup_source, bool nested,
                         uint64_t snapshot_ticks)
  : _filter{std::move(filter)},
    _checks{std::move(checks)},
    _lookup_source{std::move(lookup_source)},
    _query{FiltersOf(_filter), nested},
    _snapshot_ticks{snapshot_ticks} {}

void ScanProfile::FinishInit() {
  uint64_t ticks = _snapshot_ticks + _classify_ticks;
  for (uint32_t i = 0; i != _filter.boxes.size(); ++i) {
    if (_filter.boxes[i].parent == irs::FilterBox::kRoot) {
      ticks += _query.Node(i).ticks;
    }
  }
  _init_ticks = ticks;
}

void ScanProfile::Report(duckdb::OperatorMetrics& metrics) {
  const auto version = _version.fetch_add(1, std::memory_order_acq_rel) + 1;
  metrics.time += Seconds(_init_ticks.exchange(0, std::memory_order_relaxed));
  metrics.final_extra_info = duckdb::make_shared_ptr<const Info>(Render());
  metrics.final_extra_info_version = version;
}

Info ScanProfile::Render() const {
  Info info;
  const bool draws_filter =
    !_filter.boxes.empty() && _query.Node(0).segments != 0;
  if (draws_filter) {
    info["Index Filter"] = duckdb::ExplainValue{};
  }
  const auto& columns = _query.Checks().columns;
  const bool draws_duals =
    !_checks.duals.empty() &&
    (_checks.duals_on_rows || columns.batches.calls != 0 ||
     columns.rows.calls != 0);
  auto nodes = _checks.nodes;
  if (draws_duals) {
    nodes.insert(nodes.end(), _checks.duals.begin(), _checks.duals.end());
    info["Index or Column Filter"] = duckdb::ExplainValue{};
  }
  if (!nodes.empty()) {
    for (const std::string_view key :
         {"Table Filter", "Row Filter", "Dynamic Column Filter"}) {
      info[std::string{key}] = duckdb::ExplainValue{};
    }
  }
  info["Index Filter Prepare"] =
    duckdb::ExplainValue::Section(Prepared(draws_filter));
  if (auto executed = Executed(); !executed.children.empty()) {
    info["Index Filter Execute"] =
      duckdb::ExplainValue::Section(std::move(executed));
  }
  if (!nodes.empty()) {
    info["Column Filter"] =
      duckdb::ExplainValue::Section(Checked(std::move(nodes)));
  }
  if (const uint64_t rows = _lookup_rows; rows != 0) {
    info["Lookup"] = duckdb::ExplainValue::Section(LookedUp(rows));
  }
  return info;
}

duckdb::ExplainNode ScanProfile::Prepared(bool draws_filter) const {
  const auto size = static_cast<uint32_t>(_filter.boxes.size());
  std::vector<std::vector<uint32_t>> children(size);
  uint64_t filter_ticks = 0;
  for (uint32_t i = 0; i != size; ++i) {
    const auto parent = _filter.boxes[i].parent;
    if (parent == irs::FilterBox::kRoot) {
      filter_ticks += _query.Node(i).ticks;
    } else {
      children[parent].push_back(i);
    }
  }
  const uint64_t stats_ticks = _stats_ticks;
  duckdb::ExplainNode prepared;
  prepared.attributes["Total"] =
    Timing(_snapshot_ticks + stats_ticks + filter_ticks);
  AddTiming(prepared.attributes, "Snapshot", _snapshot_ticks);
  prepared.attributes["Segments"] =
    absl::StrCat(Count(_segments_kept), " of ", Count(_segments_total));
  AddTiming(prepared.attributes, "Stats", stats_ticks);
  if (!draws_filter) {
    return prepared;
  }
  auto& tree = prepared.children.emplace_back(_filter.root);
  std::vector<duckdb::ExplainNode*> nodes;
  PreOrder(tree, nodes);
  for (uint32_t i = 0; i != size; ++i) {
    const auto& stats = _query.Node(i);
    if (stats.segments == 0) {
      continue;
    }
    uint64_t ticks = stats.ticks;
    uint64_t terms = stats.terms;
    uint64_t postings = stats.postings;
    for (const auto child : children[i]) {
      const auto& sub = _query.Node(child);
      ticks = Minus(ticks, sub.ticks);
      terms = Minus(terms, sub.terms);
      postings = Minus(postings, sub.postings);
    }
    auto& attributes = nodes[_filter.boxes[i].node]->attributes;
    const bool lists = terms != 0 && CountsPostingLists(stats.kind);
    if (lists) {
      attributes["Posting Lists"] = Count(terms);
    }
    attributes["Estimated Matches"] = Count(stats.matches);
    if (lists) {
      attributes["Postings"] = Count(postings);
    }
    if (!children[i].empty()) {
      attributes["Total"] = Timing(stats.ticks);
    }
    attributes["Time"] = Timing(ticks);
  }
  return prepared;
}

duckdb::ExplainNode ScanProfile::Executed() const {
  const auto slots = _query.Slots();
  std::vector<std::string> names(slots.size());
  std::vector<uint64_t> slot_ticks(slots.size());
  std::vector<size_t> plans;
  uint64_t plan_ticks = 0;
  uint64_t run_ticks = 0;
  uint64_t rows = 0;
  for (size_t s = 0; s != slots.size(); ++s) {
    const auto& slot = *slots[s].executed;
    names[s] = ShapeName(*slot.type);
    slot_ticks[s] = EstimatedTicks(slot, _query.TimerTicks());
    if (slots[s].node == 0) {
      plans.push_back(s);
      plan_ticks += slot.plan_ticks;
      run_ticks += slot_ticks[s];
      rows += slot.docs;
    }
  }
  duckdb::ExplainNode executed;
  if (plans.empty()) {
    return executed;
  }
  absl::c_sort(plans,
               [&](size_t lhs, size_t rhs) { return names[lhs] < names[rhs]; });
  executed.attributes["Total"] = Timing(plan_ticks + run_ticks);
  executed.attributes["Rows"] = Count(rows);

  const auto owner = *absl::c_max_element(plans, [&](size_t lhs, size_t rhs) {
    return slot_ticks[lhs] < slot_ticks[rhs];
  });
  std::vector<std::vector<size_t>> slot_children(slots.size());
  for (size_t s = 0; s != slots.size(); ++s) {
    if (slots[s].node == 0) {
      continue;
    }
    auto parent = owner;
    for (auto up = _filter.boxes[slots[s].node].parent;
         up != irs::FilterBox::kRoot && up != 0;
         up = _filter.boxes[up].parent) {
      const auto found = absl::c_find_if(
        slots, [&](const auto& other) { return other.node == up; });
      if (found != slots.end()) {
        parent = static_cast<size_t>(found - slots.begin());
        break;
      }
    }
    slot_children[parent].push_back(s);
  }

  std::vector<const duckdb::ExplainNode*> filters;
  PreOrder(_filter.root, filters);
  const auto slot_node = [&](this const auto& self,
                             size_t s) -> duckdb::ExplainNode {
    const auto& slot = *slots[s].executed;
    duckdb::ExplainNode node{names[s]};
    if (slots[s].node != 0) {
      node.attributes["Filter"] =
        filters[_filter.boxes[slots[s].node].node]->label;
    }
    node.attributes["Plans"] = Count(slot.plans);
    node.attributes["Calls"] = Count(slot.calls);
    AddTiming(node.attributes, "Plan", slot.plan_ticks);
    if (slot.timed_calls < slot.calls) {
      node.attributes["Timed"] = Count(slot.timed_calls);
    }
    auto own_ticks = slot_ticks[s];
    if (!slot_children[s].empty()) {
      node.attributes["Total"] = Timing(slot_ticks[s]);
      for (const auto child : slot_children[s]) {
        own_ticks = Minus(own_ticks, slot_ticks[child]);
      }
    }
    node.attributes["Time"] = Timing(own_ticks);
    node.attributes["Rows"] = Count(slot.docs);
    for (const auto child : slot_children[s]) {
      node.children.push_back(self(child));
    }
    return node;
  };
  for (const auto s : plans) {
    executed.children.push_back(slot_node(s));
  }
  return executed;
}

duckdb::ExplainNode ScanProfile::Checked(
  std::vector<duckdb::ExplainNode> nodes) const {
  duckdb::ExplainNode checked;
  uint64_t calls = 0;
  uint64_t rows = 0;
  uint64_t dropped = 0;
  uint64_t ticks = 0;
  for (const auto* counts :
       {&_query.Checks().table, &_query.Checks().columns}) {
    calls += counts->batches.calls + counts->rows.calls;
    rows += counts->checked;
    dropped += counts->dropped;
    ticks += EstimatedTicks(counts->batches, _query.TimerTicks()) +
             EstimatedTicks(counts->rows, _query.TimerTicks());
  }
  if (calls != 0) {
    checked.attributes["Rows"] = Count(rows);
    checked.attributes["Rows Removed"] = Count(dropped);
    checked.attributes["Time"] = Timing(ticks);
  }
  if (nodes.size() == 1) {
    checked.children.push_back(std::move(nodes.front()));
  } else {
    checked.children.emplace_back("And").children.assign(
      std::make_move_iterator(nodes.begin()),
      std::make_move_iterator(nodes.end()));
  }
  return checked;
}

duckdb::ExplainNode ScanProfile::LookedUp(uint64_t rows) const {
  duckdb::ExplainNode lookup;
  if (!_lookup_source.empty()) {
    lookup.attributes["Source"] = _lookup_source;
  }
  lookup.attributes["Rows"] = Count(rows);
  lookup.attributes["Rows Removed"] = Count(_lookup_dropped);
  lookup.attributes["Time"] = Timing(_lookup_ticks);
  return lookup;
}

}  // namespace sdb::connector
