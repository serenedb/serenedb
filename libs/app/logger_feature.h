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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "app/app_feature.h"

namespace sdb {
namespace app {
class AppServer;
}
namespace options {
class ProgramOptions;
}

class LoggerFeature final : public app::AppFeature {
 public:
  static constexpr std::string_view name() { return "Logger"; }

  LoggerFeature(app::AppServer& server, bool threaded);
  ~LoggerFeature() final;

  void collectOptions(std::shared_ptr<options::ProgramOptions>) final;
  void validateOptions(std::shared_ptr<options::ProgramOptions>) final;
  void prepare() final;
  void unprepare() final;

  void disableThreaded() noexcept { _threaded = false; }
  void setSupervisor(bool supervisor) noexcept { _supervisor = supervisor; }

  bool isAPIEnabled() const noexcept { return _api_enabled; }
  bool onlySuperUser() const noexcept { return _api_switch == "jwt"; }

 private:
  std::vector<std::string> _output;
  std::vector<std::string> _levels;
  std::string _prefix;
  std::string _hostname;
  std::string _file;
  std::string _file_mode;
  std::string _file_group;
  std::string _time_format_string;
  std::string _api_switch = "true";
  uint32_t _max_entry_length = 128U * 1048576U;
  uint32_t _max_queued_log_messages = 16384;
  bool _use_json = false;
  bool _use_color = true;
  bool _use_control_escaped = true;
  bool _use_unicode_escaped = false;
  bool _line_number = false;
  bool _shorten_filenames = true;
  bool _process_id = true;
  bool _thread_id = true;
  bool _thread_name = false;
  bool _keep_log_rotate = false;
  bool _foreground_tty = false;
  bool _force_direct = false;
  bool _show_ids = true;
  bool _show_role = false;
  bool _log_request_parameters = true;
  bool _supervisor = false;
  bool _threaded = false;
  bool _api_enabled = true;
};

}  // namespace sdb
