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

#include <absl/container/flat_hash_map.h>

#include <cstddef>
#include <functional>
#include <memory>
#include <span>

#include "iresearch/index/merge/doc_map.hpp"
#include "iresearch/types.hpp"

namespace irs {

struct BasicTermReader;
struct FlushState;
class IdxWriter;

using MergeProgress = std::function<bool()>;
using MergedNormIdMap = absl::flat_hash_map<field_id, field_id>;

class PostingsMerger {
 public:
  PostingsMerger(size_t inputs, const MergeProgress& progress,
                 bool sorted = false);
  ~PostingsMerger();

  PostingsMerger(const PostingsMerger&) = delete;
  PostingsMerger& operator=(const PostingsMerger&) = delete;

  void Add(const SubReader& reader, const DocRemap& remap);

  bool Write(const FlushState& flush_state,
             const MergedNormIdMap& merged_norm_ids, IResourceManager& rm,
             IdxWriter& idx, std::span<const BasicTermReader* const> extra);

 private:
  struct Impl;
  std::unique_ptr<Impl> _impl;
};

}  // namespace irs
