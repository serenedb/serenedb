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

#include "version.h"

#include <vpack/builder.h>
#include <vpack/iterator.h>
#include <vpack/parser.h>
#include <vpack/slice.h>

#include "app/app_server.h"
#include "basics/common.h"
#include "basics/file_utils.h"
#include "basics/files.h"
#include "basics/logger/logger.h"
#include "general_server/state.h"
#include "rest/version.h"
#include "storage_engine/engine_selector_feature.h"
#include "storage_engine/storage_engine.h"
#include "vpack/vpack_helper.h"

namespace sdb::methods {

uint64_t Version::parseVersion(std::string_view version) {
  uint64_t result = 0;
  uint64_t tmp = 0;
  for (size_t i = 0; i < version.size(); i++) {
    char c = version[i];
    if ('0' <= c && c <= '9') {
      tmp = tmp * 10 + static_cast<size_t>(c - '0');
    } else if (c == '.') {
      result = result * 100 + tmp;
      tmp = 0;
    } else {
      // stop at first other character (e.g. "3.4.devel")
      while (result > 0 && result < 100) {
        // do we have 5 digits already? if we, then boost the version
        // number accordingly. this can happen for version strings
        // such as "3.4.devel" or "4.devel"
        result *= 100;
      }
      break;
    }
  }

  return result * 100 + tmp;
}

/// "(((major * 100) + minor) * 100) + patch"
uint64_t Version::current() { return parseVersion(SERENEDB_VERSION); }

VersionResult::StatusCode Version::compare(uint64_t current, uint64_t other) {
  if (current / 100 == other / 100) {
    return VersionResult::kVersionMatch;
  } else if (current > other) {  // downgrade??
    return VersionResult::kDowngradeNeeded;
  } else if (current < other) {  // upgrade
    return VersionResult::kUpgradeNeeded;
  }

  return VersionResult::kInvalid;
}

VersionResult Version::check(ObjectId database) {
  uint64_t last_version = UINT64_MAX;
  uint64_t server_version = Version::current();
  std::map<std::string, bool> tasks;

  if (ServerState::instance()->IsCoordinator()) {
    // in a coordinator, we don't have any persistent data, so there is no
    // VERSION file available. In this case we don't know the previous version
    // we are upgrading from, so we can't do anything sensible here.
    return VersionResult{VersionResult::kVersionMatch, server_version,
                         server_version, tasks};
  }

  auto& engine =
    SerenedServer::Instance().getFeature<EngineSelectorFeature>().engine();
  std::string version_file = engine.versionFilename(database);
  if (!basics::file_utils::Exists(version_file)) {
    SDB_DEBUG("xxxxx", Logger::STARTUP, "VERSION file '", version_file,
              "' not found");
    return VersionResult{VersionResult::kNoVersionFile, 0, 0, {}};
  }
  std::string version_info = basics::file_utils::Slurp(version_file);
  SDB_DEBUG("xxxxx", Logger::STARTUP, "found VERSION file '", version_file,
            "', content: ", version_info);
  if (version_info.empty()) {
    SDB_ERROR("xxxxx", Logger::STARTUP, "VERSION file '", version_file,
              "' is empty");
    return VersionResult{VersionResult::kCannotReadVersionFile, 0, 0, {}};
  }

  try {
    std::shared_ptr<vpack::Builder> parsed =
      vpack::Parser::fromJson(version_info);
    vpack::Slice version_vals = parsed->slice();
    if (!version_vals.isObject() || !version_vals.get("version").isNumber()) {
      SDB_ERROR("xxxxx", Logger::STARTUP, "cannot parse VERSION file '",
                version_file, "' content: ", version_info);
      return VersionResult{VersionResult::kCannotParseVersionFile, 0, 0, tasks};
    }
    last_version = version_vals.get("version").getUInt();
    vpack::Slice run = version_vals.get("tasks");
    if (run.isNone() || !run.isObject()) {
      SDB_ERROR("xxxxx", Logger::STARTUP, "invalid VERSION file '",
                version_file, "' content: ", version_info);
      return VersionResult{VersionResult::kCannotParseVersionFile, 0, 0, tasks};
    }
    for (auto pair : vpack::ObjectIterator(run)) {
      tasks.try_emplace(pair.key.copyString(), pair.value().getBool());
    }
  } catch (const vpack::Exception& e) {
    SDB_ERROR("xxxxx", Logger::STARTUP, "cannot parse VERSION file '",
              version_file, "': ", e.what(), ". file content: ", version_info);

    return VersionResult{VersionResult::kCannotParseVersionFile, 0, 0, tasks};
  }
  SDB_ASSERT(last_version != UINT32_MAX);

  VersionResult res = {VersionResult::kNoVersionFile, server_version,
                       last_version, tasks};

  switch (compare(last_version, server_version)) {
    case VersionResult::kVersionMatch:
      SDB_DEBUG("xxxxx", Logger::STARTUP, "version match: last version ",
                last_version, ", current version ", server_version);
      res.status = VersionResult::kVersionMatch;
      break;
    case VersionResult::kDowngradeNeeded:
      SDB_DEBUG("xxxxx", Logger::STARTUP, "downgrade: last version ",
                last_version, ", current version ", server_version);
      res.status = VersionResult::kDowngradeNeeded;
      break;
    case VersionResult::kUpgradeNeeded:
      SDB_DEBUG("xxxxx", Logger::STARTUP, "upgrade: last version ",
                last_version, ", current version ", server_version);
      res.status = VersionResult::kUpgradeNeeded;
      break;
    default:
      SDB_ERROR("xxxxx", Logger::STARTUP, "should not happen: last version ",
                last_version);
  }

  return res;
}

Result Version::write(ObjectId database,
                      const std::map<std::string, bool>& tasks, bool sync) {
  auto& engine =
    SerenedServer::Instance().getFeature<EngineSelectorFeature>().engine();
  std::string version_file = engine.versionFilename(database);
  if (version_file.empty()) {
    // cluster engine
    return {};
  }

  vpack::Options opts;
  opts.build_unindexed_objects = true;
  vpack::Builder builder(&opts);
  builder.openObject(true);
  builder.add("version", Version::current());
  builder.add("tasks", vpack::Value(vpack::ValueType::Object));
  for (const auto& task : tasks) {
    builder.add(task.first, task.second);
  }
  builder.close();
  builder.close();

  if (!basics::VPackHelper::vpackToFile(version_file, builder.slice(), sync)) {
    SDB_ERROR("xxxxx", Logger::STARTUP, "writing VERSION file '", version_file,
              "' failed: ", LastError());
    return Result(GetError(), LastError());
  }
  return {};
}

}  // namespace sdb::methods
