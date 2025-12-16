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
#include <vector>

#include "basics/logger/appender.h"
#include "basics/logger/log_level.h"
#include "rest_server/serened.h"

namespace app {
class AppServer;
}
namespace options {
class ProgramOptions;
}

namespace sdb {

struct LogBuffer {
  uint64_t id{};
  const LogTopic* topic{};
  double timestamp{};
  LogLevel level{LogLevel::DEFAULT};
  char message[512]{};
};

class LogBufferFeature final : public SerenedFeature {
 public:
  static constexpr std::string_view name() noexcept { return "LogBuffer"; }

  static constexpr uint32_t kBufferSize = 2048;

  explicit LogBufferFeature(Server& server);

  void collectOptions(std::shared_ptr<options::ProgramOptions>) final;
  void prepare() override;

  /// return all buffered log entries
  std::vector<LogBuffer> entries(LogLevel, uint64_t start, bool up_to_level,
                                 const std::string& search_string);

  /// clear all log entries
  void clear();

 private:
  std::shared_ptr<log::Appender> _in_memory_appender;
  std::shared_ptr<log::Appender> _metrics_counter;
  std::string _min_in_memory_log_level = "info";
  bool _use_in_memory_appender = true;
};

}  // namespace sdb
