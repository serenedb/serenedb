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

#include "privilege_feature.h"

#include <errno.h>
#include <string.h>

#include "basics/application-exit.h"
#include "basics/error.h"
#include "basics/file_utils.h"

#ifdef SERENEDB_HAVE_UNISTD_H
#include <unistd.h>
#endif

#ifdef SERENEDB_HAVE_GETGRGID
#include <grp.h>
#endif

#ifdef SERENEDB_HAVE_GETPWUID
#include <pwd.h>
#endif

#include "app/app_server.h"
#include "app/options/option.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "basics/application-exit.h"
#include "basics/errors.h"
#include "basics/logger/logger.h"
#include "basics/number_utils.h"

using namespace sdb::basics;
using namespace sdb::options;

namespace sdb {

PrivilegeFeature::PrivilegeFeature(Server& server)
  : SerenedFeature{server, name()} {
  setOptional(true);
}

void PrivilegeFeature::collectOptions(std::shared_ptr<ProgramOptions> options) {
#ifdef SERENEDB_HAVE_SETUID
  options
    ->addOption("--uid",
                "Switch to this user ID after reading the configuration files.",
                new StringParameter(&_uid),
                sdb::options::MakeDefaultFlags(sdb::options::Flags::Uncommon))
    .setLongDescription(R"(The name (identity) of the user to run the
server as.

If you don't specify this option, the server does not attempt to change its UID,
so that the UID used by the server is the same as the UID of the user who
started the server.

If you specify this option, the server changes its UID after opening ports and
reading configuration files, but before accepting connections or opening other
files (such as recovery files). This is useful if the server must be started
with raised privileges (in certain environments) but security considerations
require that these privileges are dropped once the server has started work.

**Note**: You cannot use this option to bypass operating system security.
In general, this option (and the related `--gid`) can lower privileges but not
raise them.)");

  options->addOption(
    "--server.uid", "Switch to this user ID after reading configuration files.",
    new StringParameter(&_uid),
    sdb::options::MakeDefaultFlags(sdb::options::Flags::Uncommon));
#endif

#ifdef SERENEDB_HAVE_SETGID
  options
    ->addOption("--gid",
                "Switch to this group ID after reading configuration files.",
                new StringParameter(&_gid),
                sdb::options::MakeDefaultFlags(sdb::options::Flags::Uncommon))
    .setLongDescription(R"(The name (identity) of the group to run the
server as.

If you don't specify this option, the server does not attempt to change its GID,
so that the GID the server runs as is the primary group of the user who started
the server.

If you specify this option, the server changes its GID after opening ports and
reading configuration files, but before accepting connections or opening other
files (such as recovery files).)");

  options->addOption(
    "--server.gid",
    "Switch to this group ID after reading configuration files.",
    new StringParameter(&_gid),
    sdb::options::MakeDefaultFlags(sdb::options::Flags::Uncommon));
#endif
}

void PrivilegeFeature::prepare() { extractPrivileges(); }

void PrivilegeFeature::extractPrivileges() {
#ifdef SERENEDB_HAVE_SETGID
  if (_gid.empty()) {
    _numeric_gid = getgid();
  } else {
    bool valid = false;
    int gid_number = number_utils::AtoiPositive<int>(
      _gid.data(), _gid.data() + _gid.size(), valid);

    if (valid && gid_number >= 0) {
#ifdef SERENEDB_HAVE_GETGRGID
      std::optional<gid_t> gid = file_utils::FindGroup(_gid);
      if (!gid) {
        SDB_FATAL(GENERAL, "unknown numeric gid '", _gid,
                  "'");
      }
#endif
    } else {
#ifdef SERENEDB_HAVE_GETGRNAM
      std::optional<gid_t> gid = file_utils::FindGroup(_gid);
      if (gid) {
        gid_number = gid.value();
      } else {
        SetError(ERROR_SYS_ERROR);
        SDB_FATAL(GENERAL, "cannot convert groupname '",
                  _gid, "' to numeric gid: ", LastError());
      }
#else
      SDB_FATAL(GENERAL, "cannot convert groupname '", _gid,
                "' to numeric gid");
#endif
    }

    _numeric_gid = gid_number;
  }
#endif

#ifdef SERENEDB_HAVE_SETUID
  if (_uid.empty()) {
    _numeric_uid = getuid();
  } else {
    bool valid = false;
    int uid_number = number_utils::AtoiPositive<int>(
      _uid.data(), _uid.data() + _uid.size(), valid);

    if (valid) {
#ifdef SERENEDB_HAVE_GETPWUID
      std::optional<uid_t> uid = file_utils::FindUser(_uid);
      if (!uid) {
        SDB_FATAL(GENERAL, "unknown numeric uid '", _uid,
                  "'");
      }
#endif
    } else {
#ifdef SERENEDB_HAVE_GETPWNAM
      std::optional<uid_t> uid = file_utils::FindUser(_uid);
      if (uid) {
        uid_number = uid.value();
      } else {
        SDB_FATAL(GENERAL, "cannot convert username '",
                  _uid, "' to numeric uid");
      }
#else
      SDB_FATAL(GENERAL, "cannot convert username '", _uid,
                "' to numeric uid");
#endif
    }

    _numeric_uid = uid_number;
  }
#endif
}

void PrivilegeFeature::dropPrivilegesPermanently() {
#if defined(SERENEDB_HAVE_INITGROUPS) && defined(SERENEDB_HAVE_SETGID) && \
  defined(SERENEDB_HAVE_SETUID)
  // clear all supplementary groups
  if (!_gid.empty() && !_uid.empty()) {
    std::optional<std::string> name = file_utils::FindUserName(_numeric_uid);

    if (name) {
      file_utils::InitGroups(name.value(), _numeric_gid);
    }
  }
#endif

#ifdef SERENEDB_HAVE_SETGID
  // first GID
  if (!_gid.empty()) {
    SDB_DEBUG(GENERAL, "permanently changing the gid to ",
              _numeric_gid);

    int res = setgid(_numeric_gid);

    if (res != 0) {
      SDB_FATAL(GENERAL, "cannot set gid ", _numeric_gid,
                ": ", strerror(errno));
    }
  }
#endif

#ifdef SERENEDB_HAVE_SETUID
  // then UID (because we are dropping)
  if (!_uid.empty()) {
    SDB_DEBUG(GENERAL, "permanently changing the uid to ",
              _numeric_uid);

    int res = setuid(_numeric_uid);

    if (res != 0) {
      SDB_FATAL(GENERAL, "cannot set uid '", _uid,
                "': ", strerror(errno));
    }
  }
#endif
}

}  // namespace sdb
