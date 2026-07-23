////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "build_id/build_id.h"

#include <string>

#ifndef __APPLE__
#include <elf.h>
#include <link.h>

extern char build_id_start[];  // NOLINT
extern char build_id_end;      // NOLINT
#endif

namespace sdb::build_id {

constexpr const char* kBuildIdFailed = "";

std::string_view GetBuildId() {
#ifndef __APPLE__
  const auto* note_memory = reinterpret_cast<const char*>(&build_id_start);
  const auto* note_header = reinterpret_cast<ElfW(Nhdr) const*>(note_memory);

  if (note_header->n_type == NT_GNU_BUILD_ID) {
    const auto* build_id_memory = reinterpret_cast<const char*>(
      note_memory + sizeof(ElfW(Nhdr)) + note_header->n_namesz);
    return std::string_view{build_id_memory, note_header->n_descsz};
  }
#endif
  return std::string_view{kBuildIdFailed};
}

}  // namespace sdb::build_id
