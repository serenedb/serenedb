////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "general_server/server_options_feature.h"

#include "app/app_server.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "basics/exitcodes.h"
#include "basics/file_descriptors.h"
#include "basics/logger/logger.h"
#include "basics/static_strings.h"
#include "general_server/state.h"
#include "rest/version.h"
#include "rest_server/environment_feature.h"

namespace sdb {

ServerOptionsFeature::ServerOptionsFeature(Server& server)
  : SerenedFeature{server, name()} {
  _options.descriptors_minimum = FileDescriptors::recommendedMinimum();
}

void ServerOptionsFeature::collectOptions(
  std::shared_ptr<options::ProgramOptions> options) {
  options->addOption(
    "--version", "Print the version and other related information, then exit.",
    new options::BooleanParameter(&_options.app_print_version),
    options::MakeDefaultFlags(options::Flags::Command));

  options->addOption(
    "--version-json",
    "Print the version and other related information in JSON "
    "format, then exit.",
    new options::BooleanParameter(&_options.app_print_json_version),
    options::MakeDefaultFlags(options::Flags::Command));

  options->addSection("database", "database options");

  options->addOption(
    "--database.ignore-datafile-errors",
    "Load collections even if datafiles may contain errors.",
    new options::BooleanParameter(&_options.database_ignore_datafile_errors),
    options::MakeDefaultFlags(options::Flags::Uncommon));

  options->addOption(
    "--server.descriptors-minimum",
    "The minimum number of file descriptors needed to start (0 = no "
    "minimum)",
    new options::UInt64Parameter(&_options.descriptors_minimum),
    sdb::options::MakeFlags(options::Flags::DefaultNoOs,
                            options::Flags::OsLinux, options::Flags::OsMac));
}

void ServerOptionsFeature::validateOptions(
  std::shared_ptr<options::ProgramOptions>) {
  if (_options.app_print_json_version) {
    vpack::Builder builder;
    {
      vpack::ObjectBuilder ob(&builder);
      rest::Version::getVPack(builder);

      builder.add("version", rest::Version::getServerVersion());
    }

    std::cout << builder.slice().toJson() << std::endl;
    exit(EXIT_SUCCESS);
  }

  if (_options.app_print_version) {
    std::cout << rest::Version::getServerVersion() << std::endl
              << std::endl
              << StaticStrings::kLgplNotice << std::endl
              << std::endl
              << rest::Version::getDetailed() << std::endl;
    exit(EXIT_SUCCESS);
  }

  ServerState::instance()->SetRole(ServerState::Role::Single);

  if (_options.descriptors_minimum > 0 &&
      (_options.descriptors_minimum < FileDescriptors::kRequiredMinimum ||
       _options.descriptors_minimum > FileDescriptors::kMaximumValue)) {
    SDB_FATAL("xxxxx", Logger::STARTUP,
              "invalid value for --server.descriptors-minimum",
              ". must be between ", FileDescriptors::kRequiredMinimum, " and ",
              FileDescriptors::kMaximumValue);
  }

  if (auto r = FileDescriptors::adjustTo(
        static_cast<FileDescriptors::ValueType>(_options.descriptors_minimum));
      !r.ok()) {
    SDB_FATAL_EXIT_CODE("xxxxx", Logger::SYSCALL, EXIT_RESOURCES_TOO_LOW, r);
  }

  FileDescriptors current;
  if (auto r = FileDescriptors::load(current); !r.ok()) {
    SDB_FATAL_EXIT_CODE("xxxxx", Logger::SYSCALL, EXIT_RESOURCES_TOO_LOW,
                        "cannot get the file descriptors limit value: ", r);
  }

  SDB_INFO("xxxxx", Logger::SYSCALL,
           "file-descriptors (nofiles) hard limit is ",
           FileDescriptors::stringify(current.hard), ", soft limit is ",
           FileDescriptors::stringify(current.soft));

  auto required = std::max(
    static_cast<FileDescriptors::ValueType>(_options.descriptors_minimum),
    FileDescriptors::kRequiredMinimum);

  if (current.soft < required) {
    auto message = absl::StrCat(
      "file-descriptors (nofiles) soft limit is too low, currently ",
      FileDescriptors::stringify(current.soft), ". please raise to at least ",
      required, " (e.g. via ulimit -n ", required,
      ") or adjust the value of the startup option "
      "--server.descriptors-minimum");
    if (_options.descriptors_minimum == 0) {
      SDB_WARN("xxxxx", Logger::SYSCALL, message);
    } else {
      SDB_FATAL_EXIT_CODE("xxxxx", Logger::SYSCALL, EXIT_RESOURCES_TOO_LOW,
                          message);
    }
  }
}

void ServerOptionsFeature::prepare() {
  SDB_INFO("xxxxx", Logger::FIXME, rest::Version::getVerboseVersionString());
#ifdef __GLIBC__
  SDB_INFO("xxxxx", Logger::FIXME, StaticStrings::kLgplNotice);
#endif

  const auto& options = server().options();

#if defined(SDB_DEV) || defined(SDB_GTEST)
  SDB_WARN("xxxxx", Logger::FIXME, "This version is FOR DEVELOPMENT ONLY!");
  SDB_WARN("xxxxx", Logger::FIXME,
           "==================================================================="
           "================");

  if (!options) {
    return;
  }
#endif

  if (const auto& modernized_options = options->modernizedOptions();
      !modernized_options.empty()) {
    for (const auto& it : modernized_options) {
      SDB_WARN("xxxxx", Logger::STARTUP,
               "please note that the specified option '--", it.first,
               " has been renamed to '--", it.second, "'");
    }

    SDB_INFO("xxxxx", Logger::STARTUP,
             "please read the release notes about changed options");
  }

  options->walk(
    [](const auto&, const auto& option) {
      if (option.hasDeprecatedIn()) {
        SDB_WARN("xxxxx", Logger::STARTUP, "option '", option.displayName(),
                 "' is deprecated since ", option.deprecatedInString(),
                 " and may be removed or unsupported in a future version");
      }
    },
    true, true);

  options->walk(
    [](const auto&, const auto& option) {
      if (option.hasFlag(sdb::options::Flags::Obsolete)) {
        SDB_WARN("xxxxx", Logger::STARTUP, "obsolete option '",
                 option.displayName(), "' used in configuration. ",
                 "Setting this option does not have any effect.");
      }
    },
    true, true);

  options->walk(
    [](const auto&, const auto& option) {
      if (option.hasFlag(sdb::options::Flags::Experimental)) {
        SDB_WARN("xxxxx", Logger::STARTUP, "experimental option '",
                 option.displayName(), "' used in configuration.");
      }
    },
    true, true);

  PrintEnvironment();
}

void ServerOptionsFeature::unprepare() {
  SDB_INFO("xxxxx", Logger::FIXME, "SereneDB has been shut down");
}

const ServerOptions& GetServerOptions() {
  auto& feature = SerenedServer::Instance().getFeature<ServerOptionsFeature>();
  return feature.GetOptions();
}

ServerOptions& MutServerOptions() {
  auto& feature = SerenedServer::Instance().getFeature<ServerOptionsFeature>();
  return feature.GetOptions();
}

}  // namespace sdb
