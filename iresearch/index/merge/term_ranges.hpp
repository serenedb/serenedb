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

#include <absl/synchronization/mutex.h>

#include <cstddef>
#include <cstdint>
#include <duckdb/common/sorting/presorted_merge.hpp>
#include <memory>
#include <vector>

#include "iresearch/formats/index/term_parts.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader_options.hpp"
#include "iresearch/index/merge/doc_map.hpp"
#include "iresearch/index/merge/merge_progress.hpp"
#include "iresearch/index/merge/term_keys.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/containers/bitset.hpp"

namespace irs {

struct NormProvider;
struct TermReader;

class TermRangeMerge {
 public:
  struct Input {
    const TermReader* terms;
    const DocRemap* remap;
  };

  struct Target {
    const NormProvider* norms;
    ScorerPtr scorer;
    size_t doc_count;
  };

  TermRangeMerge(field_id field, FieldProperties props,
                 std::vector<Input> inputs, bool sorted,
                 const MergeProgress& progress);
  ~TermRangeMerge();

  TermRangeMerge(const TermRangeMerge&) = delete;
  TermRangeMerge& operator=(const TermRangeMerge&) = delete;

  field_id Field() const noexcept { return _field; }

  uint64_t Volume() const;

  void Partition(size_t parts, const Target& target,
                 duckdb::TaskScheduler* scheduler, size_t threads);

  size_t PartCount() const noexcept { return _parts.size(); }

  bool MergePart(size_t part);

  bool Done() const noexcept;

  std::vector<TermPart> Parts() const;

  void ReleaseParts() noexcept;

  uint64_t DocCount() const noexcept;

 private:
  struct Part;

  template<typename Postings>
  bool WritePart(Part& part, size_t index);

  field_id _field;
  FieldProperties _props;
  std::vector<Input> _inputs;
  std::vector<duckdb::shared_ptr<TermKeys>> _keys;
  duckdb::PresortedMerge _merge;
  std::vector<std::unique_ptr<Part>> _parts;
  Target _target{};
  const MergeProgress* _progress;
  bool _sorted;
  absl::Mutex _mutex;
  std::vector<uint64_t> _docs;
};

}  // namespace irs
