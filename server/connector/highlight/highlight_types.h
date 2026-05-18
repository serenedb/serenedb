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

#include <absl/algorithm/container.h>

#include <cstdint>
#include <string>
#include <utility>

#include "catalog/table_options.h"
#include "catalog/tokenizer.h"

namespace sdb::connector::highlight {

using HitRange = std::pair<uint32_t, uint32_t>;

struct Field {
  catalog::Column::Id column_id = 0;
  std::string field_name;
  catalog::Tokenizer::TokenizerWrapper* analyzer = nullptr;
  size_t limit = 0;
};

struct DocToken {
  uint32_t byte_start = 0;
  uint32_t byte_end = 0;
  bool is_hit = false;
};

struct SentenceRange {
  uint32_t byte_start = 0;
  uint32_t byte_end = 0;
};

struct Passage {
  SentenceRange range;
  double score = 0;
  uint32_t start = 0;
  uint32_t end = 0;
};

}  // namespace sdb::connector::highlight
