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

#include "app/command.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <cstdio>
#include <cstdlib>
#include <string>

namespace sdb::app {
namespace {

using Commands = Registry<Command>;

std::string Usage() {
  return absl::StrCat("usage: serenedb <command> [args ...]\n\ncommands: ",
                      absl::StrJoin(Commands::Names(), ", "),
                      "\n\nserened is serenedb server.\n");
}

}  // namespace

int RunCommand(int argc, char* argv[]) {
  const std::string_view program{argv[0]};
  if (program.substr(program.rfind('/') + 1) == "serened") {
    return Commands::Find("server")->run(argc, argv);
  }
  if (argc < 2) {
    std::fputs(Usage().c_str(), stderr);
    return EXIT_FAILURE;
  }
  const std::string_view name{argv[1]};
  if (name == "--help" || name == "-h") {
    std::fputs(Usage().c_str(), stdout);
    return EXIT_SUCCESS;
  }
  const auto* command = Commands::Find(name);
  if (!command) {
    std::fputs(
      absl::StrCat("serenedb: unknown command '", name, "'\n\n", Usage())
        .c_str(),
      stderr);
    return EXIT_FAILURE;
  }
  argv[1] = argv[0];
  return command->run(argc - 1, argv + 1);
}

}  // namespace sdb::app
