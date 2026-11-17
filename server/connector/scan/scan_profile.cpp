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
#include <iresearch/search/detail/lazy_bitset.hpp>
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

void Add(ShapeStats& sum, const ShapeStats& shape) {
  sum.plans += shape.plans;
  sum.calls += shape.calls;
  sum.rows += shape.rows;
  sum.plan_ticks += shape.plan_ticks;
  sum.run_ticks += shape.run_ticks;
}

struct Shape {
  std::string name;
  ShapeStats stats;
};

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

class ProfiledFill : public irs::fill::Node {
 public:
  ProfiledFill(irs::fill::Node::ptr inner, ShapeStats& shape)
    : _inner{std::move(inner)}, _shape{shape} {}

  irs::doc_id_t FillOr(irs::doc_id_t min, irs::doc_id_t max,
                       uint64_t* IRS_RESTRICT mask) final {
    const auto start = duckdb::TimePoint::Now();
    const auto next = _inner->FillOr(min, max, mask);
    _shape.run_ticks += ElapsedTicks(start);
    ++_shape.calls;
    _shape.rows += irs::detail::CountBitRange(mask, 0, max - min);
    return next;
  }

  irs::doc_id_t FillAndNot(irs::doc_id_t min, irs::doc_id_t max,
                           uint64_t* IRS_RESTRICT mask) final {
    return _inner->FillAndNot(min, max, mask);
  }

  irs::doc_id_t Fill(irs::doc_id_t min, irs::doc_id_t max,
                     uint64_t* IRS_RESTRICT mask,
                     irs::score_t* IRS_RESTRICT scores) final {
    return _inner->Fill(min, max, mask, scores);
  }

  uint32_t Keep(const irs::doc_id_t* docs, uint32_t count,
                irs::doc_id_t* out) final {
    return _inner->Keep(docs, count, out);
  }

  uint32_t Drop(const irs::doc_id_t* docs, uint32_t count,
                irs::doc_id_t* out) final {
    return _inner->Drop(docs, count, out);
  }

  bool FillCostAbove(uint64_t cost, irs::doc_id_t min,
                     irs::doc_id_t max) final {
    return _inner->FillCostAbove(cost, min, max);
  }

  irs::detail::BitsetStorage* Folded() noexcept final {
    return _inner->Folded();
  }

 private:
  irs::fill::Node::ptr _inner;
  ShapeStats& _shape;
};

}  // namespace

irs::fill::Node::ptr ProfileFill(const ScanLocalProfile& profile,
                                 irs::fill::Node::ptr node) {
  if (!profile.current) {
    return node;
  }
  return irs::memory::make_managed<ProfiledFill>(std::move(node),
                                                 *profile.current);
}

ScanProfile::ScanProfile(std::vector<irs::FilterBox> boxes, bool nested,
                         uint64_t snapshot_ticks)
  : _boxes{std::move(boxes)},
    _query{FiltersOf(_boxes), nested},
    _snapshot_ticks{snapshot_ticks} {}

void ScanProfile::FinishInit(uint32_t workers) {
  _workers = std::make_unique<Worker[]>(workers);
  _workers_size = workers;
  uint64_t ticks = _snapshot_ticks + _classify_ticks;
  for (uint32_t i = 0; i != _boxes.size(); ++i) {
    if (_boxes[i].parent == irs::FilterBox::kRoot) {
      ticks += _query.Node(i).ticks;
    }
  }
  _init_ticks = ticks;
}

void ScanProfile::Report(uint32_t worker, ScanLocalProfile local,
                         duckdb::OperatorMetrics& metrics) {
  SDB_ENSURE(local.enabled && worker < _workers_size,
             "every worker of a profiled scan reports its profile once");
  auto& slot = _workers[worker];
  slot.profile = std::move(local);
  slot.published.store(true, std::memory_order_release);
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

  ScanLocalProfile total;
  for (uint32_t w = 0; w != _workers_size; ++w) {
    const auto& slot = _workers[w];
    if (!slot.published.load(std::memory_order_acquire)) {
      continue;
    }
    const auto& local = slot.profile;
    for (const auto& [type, shape] : local.shapes) {
      Add(total.shapes[type], shape);
    }
    total.filtered += local.filtered;
    total.fetch_ticks += local.fetch_ticks;
    total.lookup_ticks += local.lookup_ticks;
    total.stats_ticks += local.stats_ticks;
  }

  std::vector<Shape> shapes;
  shapes.reserve(total.shapes.size());
  uint64_t plan_ticks = 0;
  uint64_t run_ticks = 0;
  uint64_t rows = 0;
  for (const auto& [type, shape] : total.shapes) {
    shapes.push_back({.name = ShapeName(*type), .stats = shape});
    plan_ticks += shape.plan_ticks;
    run_ticks += shape.run_ticks;
    rows += shape.rows;
  }
  absl::c_sort(shapes, [](const Shape& lhs, const Shape& rhs) {
    return lhs.name < rhs.name;
  });

  Info scan_info;
  if (total.filtered != 0) {
    scan_info["Filtered"] = Count(total.filtered);
  }
  AddTiming(scan_info, "Fetch", total.fetch_ticks);
  AddTiming(scan_info, "Lookup Time", total.lookup_ticks);

  uint64_t queries_ticks = 0;
  for (const auto root : roots) {
    queries_ticks += _query.Node(static_cast<uint32_t>(root)).ticks;
  }
  const auto own_ticks = _snapshot_ticks + _classify_ticks + total.stats_ticks;
  auto& scan = node.GetOperatorMetrics();
  scan.SetExtraInfo(std::move(scan_info));
  scan.time = Seconds(own_ticks + queries_ticks + plan_ticks + run_ticks);
  Info prepare_info;
  prepare_info["Total"] = Timing(own_ticks + queries_ticks);
  AddTiming(prepare_info, "Snapshot", _snapshot_ticks);
  prepare_info["Segments"] =
    absl::StrCat(Count(_segments_kept), " of ", Count(_segments_total));
  AddTiming(prepare_info, "Stats", total.stats_ticks);
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

  if (shapes.empty()) {
    return;
  }
  const auto slots = _query.Slots();
  Info execute_info;
  execute_info["Total"] = Timing(plan_ticks + run_ticks);
  auto& execute =
    AddBox(node, "INDEX_EXECUTE", plan_ticks, rows, std::move(execute_info));

  std::vector<uint64_t> slot_ticks(slots.size());
  std::vector<size_t> slot_parent(slots.size(), slots.size());
  std::vector<std::vector<size_t>> slot_children(slots.size() + 1);
  for (size_t s = 0; s != slots.size(); ++s) {
    slot_ticks[s] = EstimatedTicks(*slots[s].executed, _query.TimerTicks());
    for (auto up = _boxes[slots[s].node].parent; up != irs::FilterBox::kRoot;
         up = _boxes[up].parent) {
      const auto parent = absl::c_find_if(
        slots, [&](const auto& other) { return other.node == up; });
      if (parent != slots.end()) {
        slot_parent[s] = static_cast<size_t>(parent - slots.begin());
        break;
      }
    }
    slot_children[slot_parent[s]].push_back(s);
  }
  const auto own_slot_ticks = [&](uint64_t total, size_t parent) {
    for (const auto child : slot_children[parent]) {
      total = Minus(total, slot_ticks[child]);
    }
    return total;
  };

  const auto add_slot = [&](this const auto& self,
                            duckdb::ProfilingNode& parent, size_t s) -> void {
    const auto& slot = *slots[s].executed;
    Info info;
    info["Filter"] = _boxes[slots[s].node].node.label;
    info["Plans"] = Count(slot.plans);
    info["Calls"] = Count(slot.calls);
    if (slot.timed_calls < slot.calls) {
      info["Timed"] = Count(slot.timed_calls);
    }
    if (!slot_children[s].empty()) {
      info["Total"] = Timing(slot_ticks[s]);
    }
    auto& added =
      AddBox(parent, ShapeName(*slot.type), own_slot_ticks(slot_ticks[s], s),
             slot.docs, std::move(info));
    for (const auto child : slot_children[s]) {
      self(added, child);
    }
  };

  const auto& top_slots = slot_children[slots.size()];
  const auto owner =
    top_slots.empty()
      ? shapes.end()
      : absl::c_max_element(shapes, [](const Shape& lhs, const Shape& rhs) {
          return lhs.stats.run_ticks < rhs.stats.run_ticks;
        });
  for (auto shape = shapes.begin(); shape != shapes.end(); ++shape) {
    const bool owns_slots = shape == owner;
    const auto& stats = shape->stats;
    Info info;
    info["Plans"] = Count(stats.plans);
    info["Calls"] = Count(stats.calls);
    AddTiming(info, "Plan", stats.plan_ticks);
    if (owns_slots) {
      info["Total"] = Timing(stats.run_ticks);
    }
    auto& added =
      AddBox(execute, std::move(shape->name),
             owns_slots ? own_slot_ticks(stats.run_ticks, slots.size())
                        : stats.run_ticks,
             stats.rows, std::move(info));
    if (owns_slots) {
      for (const auto s : top_slots) {
        add_slot(added, s);
      }
    }
  }
}

}  // namespace sdb::connector
