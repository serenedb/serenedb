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

#include <optional>
#include <string>

#include "backup/backup_label.h"
#include "catalog/boot.h"
#include "catalog/log.h"

namespace sdb::backup {

struct PendingRestore {
  std::string label_path;
  BackupLabel label;
  std::string pin;
  catalog::Log::Identity identity;
};

std::optional<PendingRestore> FindRestore(
  const catalog::ServerDirectory& layout);
void PrepareServerDirectory(const catalog::ServerDirectory& layout,
                            const std::optional<PendingRestore>& restore);

bool SubscriptionsHeld();
void ReleaseSubscriptionHold();

}  // namespace sdb::backup
