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

#include <duckdb/common/serializer/serialization_traits.hpp>
#include <memory>
#include <string_view>
#include <vector>

#include "iresearch/store/data_input.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"

namespace irs {

class NormColumnReader;

inline constexpr duckdb::field_id_t kNrmFieldColumns = 0;

class NrmReader final {
 public:
  NrmReader(const Directory& dir, std::string_view segment_name);
  ~NrmReader();

  NrmReader(const NrmReader&) = delete;
  NrmReader& operator=(const NrmReader&) = delete;

  const NormColumnReader* NormColumn(field_id id) const noexcept;

 private:
  IndexInput::ptr _in;
  std::vector<std::unique_ptr<NormColumnReader>> _columns;
  irs::containers::FlatHashMap<field_id, const NormColumnReader*> _by_id;
};

}  // namespace irs
