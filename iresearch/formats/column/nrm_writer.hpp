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
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/norm_writer.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"

namespace irs {

inline constexpr std::string_view kNrmExt = "nrm";

std::string NrmFileName(std::string_view segment_name);

class NrmWriter final {
 public:
  NrmWriter(Directory& dir, std::string_view segment_name);

  NrmWriter(const NrmWriter&) = delete;
  NrmWriter& operator=(const NrmWriter&) = delete;

  NormColumnWriter& OpenNormColumn(field_id id, uint32_t row_group_size);

  void Commit(uint64_t target_row);

 private:
  struct Column {
    Column(field_id id, uint32_t row_group_size);

    MemoryFile file;
    MemoryIndexOutput out;
    NormColumnWriter writer;
  };

  Directory* _dir;
  std::string _filename;
  std::vector<std::unique_ptr<Column>> _columns;
  irs::containers::FlatHashMap<field_id, NormColumnWriter*> _by_id;
};

}  // namespace irs
