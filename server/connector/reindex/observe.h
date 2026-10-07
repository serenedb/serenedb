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

#pragma once

#include <cstdint>
#include <duckdb/parser/qualified_name.hpp>
#include <map>
#include <roaring/roaring64map.hh>
#include <string>
#include <vector>

#include "connector/source_file.h"
#include "search/source_position.h"

namespace duckdb {

class ClientContext;
struct CreateViewInfo;
struct MultiFileBindData;

}  // namespace duckdb
namespace sdb::search {

struct InvertedIndexSnapshot;

}  // namespace sdb::search
namespace sdb::connector {

struct ViewFastPath;

enum class ReindexAction : uint8_t {
  UpToDate,
  Delta,
  Rebuild,
};

struct ReindexOutcome {
  ReindexAction action = ReindexAction::UpToDate;
  int64_t files_added = 0;
  int64_t files_changed = 0;
  int64_t files_removed = 0;
  // changed - rescanned = files refreshed in place (masks / removes).
  int64_t files_rescanned = 0;

  bool FilesChanged() const noexcept {
    return files_added != 0 || files_changed != 0 || files_removed != 0;
  }
};

struct RefreshPlan {
  ReindexOutcome outcome;
  search::SourcePosition position;
  std::vector<uint64_t> drop;
  std::vector<search::SourceFile> scan_files;
  std::vector<uint64_t> live;
  std::map<uint64_t, roaring::Roaring64Map> masks;
};

struct ObserveInput {
  duckdb::ClientContext& context;
  const duckdb::QualifiedName& index;
  const ViewFastPath* fast_path = nullptr;
  const duckdb::CreateViewInfo& view_info;
  duckdb::MultiFileBindData* bind = nullptr;
  const search::InvertedIndexSnapshot& snapshot;
  uint64_t definition = 0;
  uint64_t next_file_id = 0;
  bool delta = false;
};

RefreshPlan ObserveRebuild(const ObserveInput& in);

RefreshPlan ObserveFiles(const ObserveInput& in);

RefreshPlan ObserveIceberg(const ObserveInput& in);

void PlanRebuild(RefreshPlan& plan, const SourceListing& listing,
                 uint64_t next_id);

void PlanDelta(RefreshPlan& plan, const SourceListing& listing,
               std::vector<uint64_t> scan, uint64_t next_id,
               const HeldFiles& held);

void DropUnlisted(RefreshPlan& plan, const SourceListing& listing,
                  const HeldFiles& held);

RefreshPlan PlanFileDiff(const ObserveInput& in, const SourceListing& listing,
                         RefreshPlan plan);

}  // namespace sdb::connector
