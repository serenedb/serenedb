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

#include <absl/time/time.h>

#include <cstdint>
#include <string>
#include <string_view>

#include "backup/backup_session.h"

namespace sdb::backup {

inline constexpr uint32_t kBackupLabelFormat = 1;

std::string WalFileName(uint64_t lsn);

std::string BuildBackupLabel(std::string_view label, uint64_t start_lsn,
                             absl::Time start_time, const Cut& cut);

}  // namespace sdb::backup
