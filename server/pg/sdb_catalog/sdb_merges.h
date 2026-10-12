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

#include "pg/system_table.h"

namespace sdb::pg {

// NOLINTBEGIN
struct SdbMerges {
  static constexpr uint64_t kId = kMinSystem + 403;
  static constexpr std::string_view kName = "sdb_merges";

  uint64_t merge_id;
  Oid relation_id;
  Text origin;
  Text state;
  Text lane;
  double score;
  Timestamptz queued;
  Timestamptz started;
  double elapsed_ms;
  int64_t threads;
  Array<Text> inputs;
  int64_t input_docs;
  int64_t input_live_docs;
  int64_t input_bytes;
  int64_t estimated_bytes;
};

struct SdbMergeHistory {
  static constexpr uint64_t kId = kMinSystem + 404;
  static constexpr std::string_view kName = "sdb_merge_history";

  uint64_t merge_id;
  Oid relation_id;
  Text origin;
  Text outcome;
  Timestamptz started;
  Timestamptz finished;
  Array<Text> inputs;
  int64_t input_docs;
  int64_t input_live_docs;
  int64_t input_bytes;
  Text output;
  int64_t output_docs;
  int64_t output_bytes;
  double total_ms;
  double prepare_ms;
  double norms_ms;
  double columns_ms;
  double columns_commit_ms;
  double ann_ms;
  double postings_ms;
  double commit_ms;
  Text error;
};
// NOLINTEND

template<>
MaterializedData SystemTableSnapshot<SdbMerges>::GetTableData();

template<>
MaterializedData SystemTableSnapshot<SdbMergeHistory>::GetTableData();

}  // namespace sdb::pg
