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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <duckdb/common/sorting/presorted_merge.hpp>
#include <memory>
#include <mutex>
#include <span>
#include <vector>

#include "iresearch/formats/index/term_parts.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader_options.hpp"
#include "iresearch/index/merge/doc_map.hpp"
#include "iresearch/index/merge/merge_progress.hpp"
#include "iresearch/index/merge/term_keys.hpp"
#include "iresearch/types.hpp"

namespace irs {

struct NormProvider;
struct TermReader;

class TermRangeWriter {
 public:
  virtual ~TermRangeWriter() = default;

  virtual void Consume(std::span<const duckdb::PresortedRun> runs) = 0;
};

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
  const FieldProperties& Properties() const noexcept { return _props; }

  void Partition(size_t parts, const Target& target,
                 duckdb::TaskScheduler* scheduler, size_t threads);

  size_t PartCount() const noexcept { return _parts.size(); }

  const duckdb::shared_ptr<duckdb::PresortedMerge>& KeyMerge() const noexcept {
    return _merge;
  }

  std::unique_ptr<TermRangeWriter> BeginPart(size_t part);

  bool FinishPart(size_t part, TermRangeWriter& writer);

  bool MergePart(size_t part);

  bool Done() const noexcept;

  size_t PartsLeft() const noexcept;

  std::vector<TermPart> Parts() const;

  uint64_t DocCount() const noexcept;

  uint64_t Nanos() const noexcept { return _nanos.load(); }

 private:
  struct Part;

  std::atomic<uint64_t> _nanos{0};
  field_id _field;
  FieldProperties _props;
  std::vector<Input> _inputs;
  std::vector<duckdb::shared_ptr<TermKeys>> _keys;
  duckdb::shared_ptr<duckdb::PresortedMerge> _merge;
  std::vector<std::unique_ptr<Part>> _parts;
  Target _target{};
  const MergeProgress* _progress;
  bool _sorted;
  mutable std::mutex _mutex;
  std::vector<uint64_t> _docs;
};

}  // namespace irs
