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

#include <absl/algorithm/container.h>

#include <duckdb/common/multi_file/multi_file_states.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>

#include "connector/reindex/observe.h"
#include "connector/view_fast_path.h"
#include "search/inverted_index_storage.h"

namespace sdb::connector {
namespace {

std::vector<uint64_t> DiffFiles(RefreshPlan& plan, const SourceListing& listing,
                                const HeldFiles& held) {
  std::vector<uint64_t> scan;
  for (size_t i = 0; i < listing.files.size(); ++i) {
    const auto it = held.by_path.find(listing.files[i].path);
    if (it == held.by_path.end()) {
      ++plan.outcome.files_added;
      scan.push_back(i);
      continue;
    }
    const auto& file = it->second;
    if (file.ids.size() == 1 && file.version == listing.versions[i]) {
      continue;
    }
    ++plan.outcome.files_changed;
    plan.drop.insert(plan.drop.end(), file.ids.begin(), file.ids.end());
    scan.push_back(i);
  }
  DropUnlisted(plan, listing, held);
  return scan;
}

}  // namespace

void PlanRebuild(RefreshPlan& plan, const SourceListing& listing,
                 uint64_t next_id) {
  plan.outcome.action = ReindexAction::Rebuild;
  plan.outcome.files_rescanned = static_cast<int64_t>(listing.files.size());
  plan.scan_files = ListedFiles(listing, next_id);
  plan.live.clear();
  for (const auto& file : plan.scan_files) {
    plan.live.push_back(file.id);
  }
}

void PlanDelta(RefreshPlan& plan, const SourceListing& listing,
               std::vector<uint64_t> scan, uint64_t next_id,
               const HeldFiles& held) {
  plan.outcome.action = ReindexAction::Delta;
  plan.outcome.files_rescanned =
    static_cast<int64_t>(scan.size()) - plan.outcome.files_added;
  absl::c_sort(scan);
  plan.scan_files.reserve(scan.size());
  for (const auto ordinal : scan) {
    plan.scan_files.push_back({.id = next_id++,
                               .path = listing.files[ordinal].path,
                               .version = listing.versions[ordinal]});
  }
  const irs::containers::FlatHashSet<uint64_t> dropped(plan.drop.begin(),
                                                       plan.drop.end());
  for (const auto& [path, file] : held.by_path) {
    for (const auto id : file.ids) {
      if (!dropped.contains(id)) {
        plan.live.push_back(id);
      }
    }
  }
  for (const auto& file : plan.scan_files) {
    plan.live.push_back(file.id);
  }
}

void DropUnlisted(RefreshPlan& plan, const SourceListing& listing,
                  const HeldFiles& held) {
  irs::containers::FlatHashSet<std::string_view> listed;
  listed.reserve(listing.files.size());
  for (const auto& file : listing.files) {
    listed.emplace(file.path);
  }
  for (const auto& [path, file] : held.by_path) {
    if (!listed.contains(path)) {
      ++plan.outcome.files_removed;
      plan.drop.insert(plan.drop.end(), file.ids.begin(), file.ids.end());
    }
  }
}

RefreshPlan PlanFileDiff(const ObserveInput& in, const SourceListing& listing,
                         RefreshPlan plan) {
  const auto held = IsGlobPK(in.fast_path->pk_spec)
                      ? CollectHeldFiles(in.snapshot.reader, *in.snapshot.files)
                      : KnownFiles(*in.snapshot.files);
  auto scan = DiffFiles(plan, listing, held);
  const bool changed = plan.outcome.FilesChanged();
  if (in.snapshot.position.definition != in.definition ||
      (changed && !in.delta)) {
    PlanRebuild(plan, listing, in.next_file_id);
  } else if (changed) {
    PlanDelta(plan, listing, std::move(scan), in.next_file_id, held);
  }
  return plan;
}

RefreshPlan ObserveRebuild(const ObserveInput& in) {
  RefreshPlan plan;
  plan.outcome.action = ReindexAction::Rebuild;
  plan.position.definition = in.definition;
  if (in.bind && IsFilePkSpec(in.fast_path->pk_spec)) {
    PlanRebuild(plan,
                ListSource(in.context, *in.bind->file_list,
                           /*versioned=*/false),
                in.next_file_id);
  }
  return plan;
}

RefreshPlan ObserveFiles(const ObserveInput& in) {
  RefreshPlan plan;
  const auto listing =
    ListSource(in.context, *in.bind->file_list, /*versioned=*/true);
  plan.position = {.definition = in.definition, .listing = listing.digest};
  if (plan.position == in.snapshot.position) {
    return plan;
  }
  return PlanFileDiff(in, listing, std::move(plan));
}

}  // namespace sdb::connector
