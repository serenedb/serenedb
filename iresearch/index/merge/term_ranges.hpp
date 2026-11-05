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
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/flush_state.hpp"
#include "iresearch/formats/index/term_parts.hpp"
#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/index_reader_options.hpp"
#include "iresearch/index/merge/doc_map.hpp"
#include "iresearch/index/merge/merge_progress.hpp"
#include "iresearch/index/merge/merge_workers.hpp"
#include "iresearch/index/merge/term_keys.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/types.hpp"

namespace irs {

struct DictKey {
  field_id id;
  DictMember member;

  auto operator<=>(const DictKey&) const = default;
};

uint64_t TermVolume(const TermReader& terms) noexcept;

class TermRangeSink {
 public:
  TermRangeSink(Directory& dir, std::string_view segment,
                IndexFeatures features);
  ~TermRangeSink();

  TermRangeSink(const TermRangeSink&) = delete;
  TermRangeSink& operator=(const TermRangeSink&) = delete;

  absl::Mutex& Mutex() noexcept { return _mutex; }

  PostingsOutputs& Outputs() noexcept { return _out; }

 private:
  void Remove() noexcept;

  Directory& _dir;
  std::string _segment;
  absl::Mutex _mutex;
  PostingsOutputs _out;
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
    TermRangeSink* sink;
  };

  TermRangeMerge(DictKey key, FieldProperties props, std::vector<Input> inputs,
                 bool sorted, const MergeProgress& progress, size_t parts,
                 const Target& target, const MergeWorkers& workers);
  ~TermRangeMerge();

  TermRangeMerge(const TermRangeMerge&) = delete;
  TermRangeMerge& operator=(const TermRangeMerge&) = delete;

  DictKey Key() const noexcept { return _key; }

  uint64_t Volume() const;

  size_t PartCount() const noexcept { return _parts.size(); }

  bool MergePart(size_t part);

  bool Done() const noexcept { return _appended == _parts.size(); }

  std::vector<TermPart> Parts() const;

  void ReleaseParts() noexcept;

  uint64_t DocCount() const noexcept;

 private:
  struct Part;

  template<typename Postings>
  bool WritePart(size_t index);

  DictKey _key;
  FieldProperties _props;
  std::vector<Input> _inputs;
  std::vector<duckdb::shared_ptr<TermKeys>> _keys;
  duckdb::PresortedMerge _merge;
  std::vector<std::unique_ptr<Part>> _parts;
  Target _target;
  const MergeProgress& _progress;
  bool _sorted;
  bool _ranged;
  size_t _appended = 0;
  std::vector<uint64_t> _docs;
};

TermRangeMerge* FindTermRange(
  std::span<const std::unique_ptr<TermRangeMerge>> ranges,
  DictKey key) noexcept;

}  // namespace irs
