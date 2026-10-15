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

#include "pg/types/name.h"

#include <cstdint>
#include <duckdb/common/vector_operations/unary_executor.hpp>

#include "pg/types/cast.h"
#include "pg/types/logical.h"

namespace sdb::pg {
namespace {

bool NameFromText(duckdb::Vector& source, duckdb::Vector& result,
                  duckdb::idx_t count, duckdb::CastParameters&) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    source, result, count, [&](duckdb::string_t text) {
      const auto size = ClipName({text.GetData(), text.GetSize()});
      return size == text.GetSize()
               ? text
               : duckdb::StringVector::AddString(result, text.GetData(), size);
    });
  duckdb::StringVector::AddHeapReference(result, source);
  return true;
}

}  // namespace

size_t ClipName(std::string_view text) {
  if (text.size() < kNameDataLen) {
    return text.size();
  }
  auto size = kNameDataLen - 1;
  while (size != 0 && (static_cast<uint8_t>(text[size]) & 0xC0) == 0x80) {
    --size;
  }
  return size;
}

void RegisterName(duckdb::CastFunctionSet& casts) {
  ReplaceTextInput(casts, NAME(), NameFromText);
}

}  // namespace sdb::pg
