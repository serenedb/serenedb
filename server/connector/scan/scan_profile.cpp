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
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>
#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>

#include <duckdb/common/string_util.hpp>
#include <string>
#include <utility>

namespace sdb::connector {
namespace {

using Info = duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue>;

std::vector<const irs::Filter*> FiltersOf(
  const std::vector<irs::FilterBox>& boxes) {
  std::vector<const irs::Filter*> filters;
  filters.reserve(boxes.size());
  for (const auto& box : boxes) {
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
  return duckdb::StringUtil::FormatCount(count);
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
  absl::ConsumePrefix(&shape, "irs::memory::OnHeap<");
  const std::pair<std::string_view, std::string_view> parts =
    absl::StrSplit(shape, absl::MaxSplits('<', 1));
  auto name = ClassName(parts.first);
  if (RunsItsArgument(name)) {
    absl::StrAppend(&name, "<", ClassName(parts.second), ">");
  }
  return name;
}

std::string BoxName(std::string_view label) {
  return absl::StrReplaceAll(absl::AsciiStrToUpper(label), {{" ", "_"}});
}

duckdb::ProfilingNode& AddBox(duckdb::ProfilingNode& parent, std::string name,
                              uint64_t ticks, uint64_t rows, Info info) {
  auto node = duckdb::make_uniq<duckdb::ProfilingNode>();
  auto& metrics = node->GetOperatorMetrics();
  metrics.name = std::move(name);
  metrics.operator_type = duckdb::PhysicalOperatorType::EXTENSION;
  metrics.time = Seconds(ticks);
  metrics.elements_returned = rows;
  metrics.SetExtraInfo(std::move(info));
  return *parent.AddChild(std::move(node));
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

class ProfiledLookup final : public irs::IndexSource {
 public:
  ProfiledLookup(ScanProfile& profile, std::unique_ptr<irs::IndexSource> inner)
    : _profile{profile}, _inner{std::move(inner)} {}

  duckdb::idx_t Materialize(duckdb::ClientContext& context, duckdb::Vector& pk,
                            duckdb::idx_t count,
                            duckdb::DataChunk& output) final {
    const auto start = duckdb::TimePoint::Now();
    const auto rows = _inner->Materialize(context, pk, count, output);
    _profile.AddLookup(ElapsedTicks(start), count - rows);
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

ScanProfile::ScanProfile(std::vector<irs::FilterBox> boxes, bool nested,
                         uint64_t snapshot_ticks)
  : _boxes{std::move(boxes)},
    _query{FiltersOf(_boxes), nested},
    _snapshot_ticks{snapshot_ticks} {}

void ScanProfile::FinishInit() {
  uint64_t ticks = _snapshot_ticks + _classify_ticks;
  for (uint32_t i = 0; i != _boxes.size(); ++i) {
    if (_boxes[i].parent == irs::FilterBox::kRoot) {
      ticks += _query.Node(i).ticks;
    }
  }
  _init_ticks = ticks;
}

void ScanProfile::Report(duckdb::OperatorMetrics& metrics) {
  const auto version = _version.fetch_add(1, std::memory_order_acq_rel) + 1;
  metrics.time += Seconds(_init_ticks.exchange(0, std::memory_order_relaxed));
  auto nested = duckdb::make_shared_ptr<duckdb::ProfilingNode>();
  Render(*nested);
  metrics.nested = std::move(nested);
  metrics.nested_version = version;
}

void ScanProfile::Render(duckdb::ProfilingNode& node) const {
  const auto size = _boxes.size();
  std::vector<std::vector<size_t>> children(size);
  std::vector<size_t> roots;
  for (size_t i = 0; i != size; ++i) {
    const auto parent = _boxes[i].parent;
    (parent == irs::FilterBox::kRoot ? roots : children[parent]).push_back(i);
  }

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
  absl::c_sort(plans,
               [&](size_t lhs, size_t rhs) { return names[lhs] < names[rhs]; });

  Info scan_info;
  if (const uint64_t filtered = _filtered; filtered != 0) {
    scan_info["Filtered"] = Count(filtered);
  }
  AddTiming(scan_info, "Lookup Time", _lookup_ticks);

  uint64_t queries_ticks = 0;
  for (const auto root : roots) {
    queries_ticks += _query.Node(static_cast<uint32_t>(root)).ticks;
  }
  const uint64_t stats_ticks = _stats_ticks;
  const auto own_ticks = _snapshot_ticks + _classify_ticks + stats_ticks;
  auto& scan = node.GetOperatorMetrics();
  scan.SetExtraInfo(std::move(scan_info));
  scan.time = Seconds(own_ticks + queries_ticks + plan_ticks + run_ticks);
  Info prepare_info;
  prepare_info["Total"] = Timing(own_ticks + queries_ticks);
  AddTiming(prepare_info, "Snapshot", _snapshot_ticks);
  prepare_info["Segments"] =
    absl::StrCat(Count(_segments_kept), " of ", Count(_segments_total));
  AddTiming(prepare_info, "Stats", stats_ticks);
  auto& prepare =
    AddBox(node, "INDEX_PREPARE", own_ticks, 0, std::move(prepare_info));

  const auto add_filter = [&](this const auto& self,
                              duckdb::ProfilingNode& parent, size_t i) -> void {
    const auto& stats = _query.Node(static_cast<uint32_t>(i));
    if (stats.segments == 0) {
      return;
    }
    uint64_t ticks = stats.ticks;
    uint64_t terms = stats.terms;
    uint64_t postings = stats.postings;
    for (const auto child : children[i]) {
      const auto& sub = _query.Node(static_cast<uint32_t>(child));
      ticks = Minus(ticks, sub.ticks);
      terms = Minus(terms, sub.terms);
      postings = Minus(postings, sub.postings);
    }
    const auto& box = _boxes[i].node;
    Info info;
    for (const auto& [key, value] : box.attributes) {
      info[key] = value;
    }
    if (!children[i].empty()) {
      info["Total"] = Timing(stats.ticks);
    }
    if (terms != 0 && CountsPostingLists(stats.kind)) {
      info["Posting Lists"] = Count(terms);
    }
    info["Estimated Matches"] = Count(stats.matches);
    auto& added =
      AddBox(parent, BoxName(box.label), ticks, postings, std::move(info));
    for (const auto child : children[i]) {
      self(added, child);
    }
  };
  for (const auto root : roots) {
    add_filter(prepare, root);
  }

  if (plans.empty()) {
    return;
  }
  Info execute_info;
  execute_info["Total"] = Timing(plan_ticks + run_ticks);
  auto& execute =
    AddBox(node, "INDEX_EXECUTE", plan_ticks, rows, std::move(execute_info));

  const auto owner = *absl::c_max_element(plans, [&](size_t lhs, size_t rhs) {
    return slot_ticks[lhs] < slot_ticks[rhs];
  });
  std::vector<std::vector<size_t>> slot_children(slots.size());
  for (size_t s = 0; s != slots.size(); ++s) {
    if (slots[s].node == 0) {
      continue;
    }
    auto parent = owner;
    for (auto up = _boxes[slots[s].node].parent;
         up != irs::FilterBox::kRoot && up != 0; up = _boxes[up].parent) {
      const auto found = absl::c_find_if(
        slots, [&](const auto& other) { return other.node == up; });
      if (found != slots.end()) {
        parent = static_cast<size_t>(found - slots.begin());
        break;
      }
    }
    slot_children[parent].push_back(s);
  }

  const auto add_slot = [&](this const auto& self,
                            duckdb::ProfilingNode& parent, size_t s) -> void {
    const auto& slot = *slots[s].executed;
    Info info;
    if (slots[s].node != 0) {
      info["Filter"] = _boxes[slots[s].node].node.label;
    }
    info["Plans"] = Count(slot.plans);
    info["Calls"] = Count(slot.calls);
    AddTiming(info, "Plan", slot.plan_ticks);
    if (slot.timed_calls < slot.calls) {
      info["Timed"] = Count(slot.timed_calls);
    }
    auto own_ticks = slot_ticks[s];
    if (!slot_children[s].empty()) {
      info["Total"] = Timing(slot_ticks[s]);
      for (const auto child : slot_children[s]) {
        own_ticks = Minus(own_ticks, slot_ticks[child]);
      }
    }
    auto& added =
      AddBox(parent, names[s], own_ticks, slot.docs, std::move(info));
    for (const auto child : slot_children[s]) {
      self(added, child);
    }
  };
  for (const auto s : plans) {
    add_slot(execute, s);
  }
}

}  // namespace sdb::connector
