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

#include "app/config.h"

#include <absl/strings/match.h>

#include <cstdlib>

#include "app/app_server.h"
#include "app/global_context.h"
#include "app/options/ini_file_parser.h"
#include "app/options/option.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "app/options/translator.h"
#include "basics/application-exit.h"
#include "basics/directories.h"
#include "basics/exitcodes.h"
#include "basics/file_utils.h"
#include "basics/files.h"
#include "basics/logger/logger.h"
#include "basics/string_utils.h"

#if defined __linux__ && defined __GLIBC__
#include <nss.h>
#endif

using namespace sdb::basics;
using namespace sdb::options;

namespace sdb {

void ConfigFeature::collectOptions(std::shared_ptr<ProgramOptions> options) {
  options->addOption("--configuration,-c",
                     "The configuration file or \"none\".",
                     new StringParameter(&_file));

  // add --config as an alias for --configuration. both point to the same
  // variable!
  options->addOption(
    "--config", "The configuration file or \"none\".",
    new StringParameter(&_file),
    sdb::options::MakeDefaultFlags(sdb::options::Flags::Uncommon));

  options->addOption(
    "--define,-D",
    "Define a value for a `@key@` entry in the configuration file using the "
    "syntax `\"key=value\"`.",
    new VectorParameter<StringParameter>(&_defines),
    sdb::options::MakeDefaultFlags(sdb::options::Flags::Uncommon));

  options->addOption(
    "--check-configuration", "Check the configuration and exit.",
    new BooleanParameter(&_check_configuration),
    sdb::options::MakeDefaultFlags(sdb::options::Flags::Uncommon,
                                   sdb::options::Flags::Command));

  options->addOption(
    "--use-nsswitch",
    "Allow using hostname lookup configuration via /etc/nsswitch.conf if on "
    "linux with glibc.",
    new BooleanParameter(&_nsswitch_use),
    sdb::options::MakeDefaultFlags(sdb::options::Flags::Uncommon));

#ifdef __linux__
  // option is only available on Linux
  options
    ->addOption("--use-splice-syscall",
                "Use the splice() syscall for file copying (may not be "
                "supported on all filesystems).",
                new BooleanParameter(&_use_splice),
                options::MakeFlags(Flags::DefaultNoOs, Flags::OsLinux))

    .setLongDescription(R"(While the syscall is generally available since
Linux 2.6.x, it is also required that the underlying filesystem supports the
splice operation. This is not true for some encrypted filesystems
(e.g. ecryptfs), on which `splice()` calls can fail.

You can set the `--use-splice-syscall` startup option to `false` to use a less
efficient, but more portable file copying method instead, which should work on
all filesystems.)");
#endif
}

void ConfigFeature::loadOptions(std::shared_ptr<ProgramOptions> options,
                                const char* binary_path) {
  for (const auto& def : _defines) {
    sdb::options::DefineEnvironment(def);
  }

  loadConfigFile(options, _progname, binary_path);

  if (_check_configuration) {
    exit(EXIT_SUCCESS);
  }
}

void ConfigFeature::loadConfigFile(std::shared_ptr<ProgramOptions> options,
                                   const std::string& progname,
                                   const char* binary_path) {
  if (absl::EqualsIgnoreCase(_file, "none")) {
    SDB_DEBUG("xxxxx", Logger::CONFIG, "using no config file at all");
    return;
  }

  bool fatal = !_print_version();

  // always prefer an explicitly given config file
  if (!_file.empty()) {
    if (!file_utils::Exists(_file)) {
      SDB_FATAL_EXIT_CODE("xxxxx", Logger::CONFIG, EXIT_CONFIG_NOT_FOUND,
                          "cannot read config file '", _file, "'");
    }

    auto local = _file + ".local";

    IniFileParser parser(options.get());

    if (file_utils::Exists(local) && file_utils::IsRegularFile(local)) {
      SDB_DEBUG("xxxxx", Logger::CONFIG, "loading override '", local, "'");

      if (!parser.parse(local, true)) {
        FatalErrorExitCode(options->processingResult().exitCodeOrFailure());
      }
    }

    SDB_DEBUG("xxxxx", Logger::CONFIG, "using user supplied config file '",
              _file, "'");

    if (!parser.parse(_file, true)) {
      FatalErrorExitCode(options->processingResult().exitCodeOrFailure());
    }

    return;
  }

  // clang-format off
  //
  // check the following location in this order:
  //
  //   ./etc/relative/<PRGNAME>.conf
  //   <PRGNAME>.conf
  //   ${HOME}/.serenedb/<PRGNAME>.conf
  //   /etc/serenedb/<PRGNAME>.conf
  //
  // clang-format on

  auto context = GlobalContext::gContext;
  std::string basename = progname;
  bool check_serene_imp = (progname == "import");

  if (!basename.ends_with(".conf")) {
    basename += ".conf";
  }

  std::vector<std::string> locations;
  locations.reserve(4);

  std::string current = file_utils::CurrentDirectory().result();
  // ./etc/relative/ is always first choice, if it exists
  locations.emplace_back(file_utils::BuildFilename(current, "etc", "relative"));

  if (context != nullptr) {
    auto root = context->runRoot();
    // will resolve to ./build/etc/serenedb/ in maintainer builds
    auto location = file_utils::BuildFilename(root, _SYSCONFDIR_);

    SDB_TRACE("xxxxx", Logger::CONFIG, "checking root location '", root, "'");

    locations.emplace_back(location);
  }

  // ./
  locations.emplace_back(current);

  // ~/.serenedb/
  locations.emplace_back(
    file_utils::BuildFilename(file_utils::HomeDirectory(), ".serenedb"));
  locations.emplace_back(file_utils::ConfigDirectory(binary_path));

  std::string filename;

  for (const auto& location : locations) {
    auto name = file_utils::BuildFilename(location, basename);
    SDB_TRACE("xxxxx", Logger::CONFIG, "checking config file '", name, "'");

    if (file_utils::Exists(name)) {
      SDB_DEBUG("xxxxx", Logger::CONFIG, "found config file '", name, "'");
      filename = name;
      break;
    }

    if (check_serene_imp) {
      name = file_utils::BuildFilename(location, "import.conf");
      SDB_TRACE("xxxxx", Logger::CONFIG, "checking config file '", name, "'");
      if (file_utils::Exists(name)) {
        SDB_DEBUG("xxxxx", Logger::CONFIG, "found config file '", name, "'");
        filename = name;
        break;
      }
    }
  }

  if (filename.empty()) {
    SDB_DEBUG("xxxxx", Logger::CONFIG, "cannot find any config file");
  }

  IniFileParser parser(options.get());
  std::string local = filename + ".local";

  SDB_TRACE("xxxxx", Logger::CONFIG, "checking override '", local, "'");

  if (file_utils::Exists(local) && file_utils::IsRegularFile(local)) {
    SDB_DEBUG("xxxxx", Logger::CONFIG, "loading override '", local, "'");

    if (!parser.parse(local, true)) {
      FatalErrorExitCode(options->processingResult().exitCodeOrFailure());
    }
  } else {
    SDB_TRACE("xxxxx", Logger::CONFIG, "no override file found");
  }

  SDB_DEBUG("xxxxx", Logger::CONFIG, "loading '", filename, "'");

  if (filename.empty()) {
    if (fatal) {
      size_t i = 0;
      std::string location_msg = "(tried locations: ";
      for (const auto& it : locations) {
        if (i++ > 0) {
          location_msg += ", ";
        }
        location_msg += "'" + file_utils::BuildFilename(it, basename) + "'";
      }
      location_msg += ")";
      options->failNotice(EXIT_CONFIG_NOT_FOUND,
                          "cannot find configuration file\n\n" + location_msg);
      FatalErrorExitCode(options->processingResult().exitCodeOrFailure());
    } else {
      return;
    }
  }

  if (!parser.parse(filename, true)) {
    FatalErrorExitCode(options->processingResult().exitCodeOrFailure());
  }
}

void ConfigFeature::prepare() {
#if defined __linux__ && defined __GLIBC__
  if (!_nsswitch_use) {
    __nss_configure_lookup("hosts", "files dns");
    __nss_configure_lookup("passwd", "files");
    __nss_configure_lookup("group", "files");
  }
#endif

#ifdef __linux__
  SdbSetCanUseSplice(_use_splice);
#endif
}
}  // namespace sdb
