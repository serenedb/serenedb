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

#include "server_id_feature.h"

#include "app/app_server.h"
#include "app/options/program_options.h"
#include "basics/file_utils.h"
#include "basics/files.h"
#include "basics/logger/logger.h"
#include "basics/random/random_generator.h"
#include "basics/string_utils.h"
#include "basics/system-functions.h"
#include "rest_server/check_version_feature.h"
#include "rest_server/database_path_feature.h"
#include "vpack/vpack_helper.h"

using namespace sdb::options;

namespace sdb {

ServerId ServerIdFeature::gServerid{0};

ServerIdFeature::ServerIdFeature(Server& server)
  : SerenedFeature{server, name()} {
  setOptional(false);
}

ServerIdFeature::~ServerIdFeature() { gServerid = ServerId::none(); }

void ServerIdFeature::start() {
  auto& database_path = server().getFeature<DatabasePathFeature>();
  _id_filename = database_path.subdirectoryName("SERVER");

  // read the server id or create a new one
  const bool check_version =
    server().getFeature<CheckVersionFeature>().GetCheckVersion();
  auto res = determineId(check_version);

  if (res == ERROR_SERVER_EMPTY_DATADIR) {
    if (check_version) {
      // when we are version checking, we will not fail here
      // additionally notify the database feature that we had no VERSION file
      _is_initially_empty = true;
      return;
    }

    // otherwise fail
    SDB_THROW(res);
  }

  if (res != ERROR_OK) {
    SDB_ERROR("xxxxx", sdb::Logger::FIXME,
              "reading/creating server id file failed: ", GetErrorStr(res));
    SDB_THROW(res);
  }
}

void ServerIdFeature::generateId() {
  SDB_ASSERT(!gServerid.isSet());
  do {
    gServerid =
      ServerId{random::Interval(static_cast<uint64_t>(0x0000FFFFFFFFFFFFULL))};
  } while (!gServerid.isSet());
}

ErrorCode ServerIdFeature::readId() {
  if (!SdbExistsFile(_id_filename.c_str())) {
    return ERROR_FILE_NOT_FOUND;
  }

  ServerId found_id;
  try {
    vpack::Builder builder =
      basics::VPackHelper::vpackFromFile(_id_filename.c_str());
    vpack::Slice content = builder.slice();
    if (!content.isObject()) {
      return ERROR_INTERNAL;
    }
    vpack::Slice id_slice = content.get("serverId");
    if (!id_slice.isString()) {
      return ERROR_INTERNAL;
    }
    found_id = ServerId(basics::string_utils::Uint64(id_slice.stringView()));
  } catch (...) {
    // Nothing to free
    return ERROR_INTERNAL;
  }

  if (!found_id.isSet()) {
    return ERROR_INTERNAL;
  }

  gServerid = found_id;

  return ERROR_OK;
}

/// writes server id to file
ErrorCode ServerIdFeature::writeId() {
  // create a VPack Object
  vpack::Builder builder;
  try {
    builder.openObject();

    SDB_ASSERT(gServerid.isSet());
    builder.add("serverId", std::to_string(gServerid.id()));

    time_t tt = time(nullptr);
    struct tm tb;
    utilities::GetGmtime(tt, &tb);
    char buffer[32];
    size_t len = strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%SZ", &tb);
    builder.add("createdTime", std::string_view{buffer, len});

    builder.close();
  } catch (...) {
    // out of memory
    SDB_ERROR("xxxxx", sdb::Logger::FIXME, "cannot save server id in file '",
              _id_filename, "': out of memory");
    return ERROR_OUT_OF_MEMORY;
  }

  // save json info to file
  SDB_DEBUG("xxxxx", sdb::Logger::FIXME, "Writing server id to file '",
            _id_filename, "'");
  bool ok =
    sdb::basics::VPackHelper::vpackToFile(_id_filename, builder.slice(), true);

  if (!ok) {
    SDB_ERROR("xxxxx", sdb::Logger::FIXME, "could not save server id in file '",
              _id_filename, "': ", LastError());

    return ERROR_INTERNAL;
  }

  return ERROR_OK;
}

/// read / create the server id on startup
ErrorCode ServerIdFeature::determineId(bool check_version) {
  auto res = readId();

  if (res == ERROR_FILE_NOT_FOUND) {
    if (check_version) {
      return ERROR_SERVER_EMPTY_DATADIR;
    }

    // id file does not yet exist. now create it
    generateId();

    // id was generated. now save it
    res = writeId();
  }

  return res;
}

}  // namespace sdb
