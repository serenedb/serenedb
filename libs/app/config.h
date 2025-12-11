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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "app/app_feature.h"

namespace sdb {
namespace options {
class ProgramOptions;
}

class LoggerFeature;

class ConfigFeature final : public app::AppFeature {
 public:
  static constexpr std::string_view name() noexcept { return "Config"; }

  template<typename Server>
  ConfigFeature(Server& server, const std::string& progname,
                std::function<bool()> print_version,
                std::string_view config_filename = "")
    : app::AppFeature{server, name()},
      _print_version{std::move(print_version)},
      _file(config_filename),
      _progname(progname) {
    setOptional(false);
  }

  void collectOptions(std::shared_ptr<options::ProgramOptions>) final;
  void loadOptions(std::shared_ptr<options::ProgramOptions>,
                   const char* binary_path) final;

  void prepare() final;

 private:
  void loadConfigFile(std::shared_ptr<options::ProgramOptions>,
                      const std::string& progname, const char* binary_path);

  std::function<bool()> _print_version;
  std::string _file;
  std::string _progname;
  std::vector<std::string> _defines;
  bool _check_configuration = false;
  bool _nsswitch_use = false;
  // whether or not to use the splice() syscall on Linux
#ifdef __linux__
  bool _use_splice = true;
#endif
};

}  // namespace sdb
