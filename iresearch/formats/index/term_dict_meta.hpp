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
#include <duckdb/common/constants.hpp>
#include <string>
#include <vector>

#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::term_dict {

struct TextHead {
  uint64_t total_freq = 0;
  field_id norm = field_limits::invalid();
  bool has_score_bounds = false;
  std::string prefixes;
};

struct RangesHead {
  uint64_t offset = 0;
  uint64_t size = 0;
  uint32_t prefix = 0;
  std::vector<uint64_t> counts;
};

struct FieldHead {
  field_id id = field_limits::invalid();
  DictLayout layout = DictLayout::String;
  DictMember member = DictMember::Strings;
  IndexFeatures features = IndexFeatures::None;
  uint64_t term_count = 0;
  uint64_t doc_count = 0;
  uint32_t width = 0;
  std::string min;
  std::string max;
  std::string record;
  std::string record_inline;
  uint32_t first_block = 0;
  uint32_t first_entry = 0;
  uint32_t first_record = 0;
  uint32_t last_block = 0;
  uint32_t last_entry = 0;
  uint64_t inline_base = 0;
  uint64_t inline_size = 0;
  TextHead text;
  RangesHead ranges;
};

struct SpanningBlock {
  uint32_t block;
  uint32_t extra;
};

struct TermsMeta {
  uint32_t blocks = 0;
  uint64_t first_page = 0;
  std::vector<SpanningBlock> spans;
  std::string separators;
  std::string fsst_table;
  uint64_t inline_offset = 0;
  uint64_t inline_size = 0;
  uint64_t ranges_size = 0;
  std::vector<FieldHead> fields;
  std::string inline_starts;
  uint32_t ordinal_bytes = 1;
};

void WriteTerms(duckdb::BinarySerializer& out, const TermsMeta& terms);

TermsMeta ReadTerms(duckdb::BinaryDeserializer& in, uint64_t data_size);

}  // namespace irs::term_dict
