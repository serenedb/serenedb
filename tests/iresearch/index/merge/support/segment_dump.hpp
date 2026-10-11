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
#include <iresearch/index/index_features.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/utils/string.hpp>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace tests::merge {

struct DumpPosting {
  irs::doc_id_t doc{};
  uint32_t freq{};
  std::vector<uint32_t> positions{};
  std::vector<std::pair<uint32_t, uint32_t>> offsets{};

  bool operator==(const DumpPosting&) const = default;
};

struct DumpTerm {
  irs::bstring value{};
  std::vector<DumpPosting> postings{};

  bool operator==(const DumpTerm&) const = default;
};

struct DumpField {
  irs::field_id id{};
  irs::IndexFeatures features{};
  uint64_t docs{};
  std::vector<DumpTerm> terms{};
  std::vector<uint32_t> norms{};

  bool operator==(const DumpField&) const = default;
};

struct DumpColumn {
  irs::field_id id{};
  std::string type{};
  std::vector<std::string> values{};
  bool sketch_matches_values{true};

  bool operator==(const DumpColumn&) const = default;
};

struct SegmentDump {
  uint64_t docs{};
  std::vector<DumpField> fields{};
  std::vector<DumpColumn> columns{};

  bool operator==(const SegmentDump&) const = default;
};

SegmentDump Dump(const irs::SubReader& segment);

SegmentDump Concat(std::span<const SegmentDump> inputs);

SegmentDump ExpectedMerge(std::span<const irs::SubReader* const> inputs);

std::string Describe(const SegmentDump& dump);

std::string FirstDifference(const SegmentDump& expected,
                            const SegmentDump& actual);

}  // namespace tests::merge
