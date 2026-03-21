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

#include <absl/synchronization/mutex.h>
#include <stddef.h>

#include <array>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <typeindex>
#include <utility>
#include <vector>

#include "basics/common.h"
#include "basics/logger/log_group.h"
#include "basics/logger/log_level.h"
#include "basics/read_write_lock.h"
#include "basics/result.h"

namespace sdb {

class LogTopic;

namespace log {

struct Message;

class Appender {
 public:
  static void addAppender(const LogGroup&, const std::string& definition);

  static void addGlobalAppender(const LogGroup&, std::shared_ptr<Appender>);

  static std::shared_ptr<Appender> buildAppender(const LogGroup&,
                                                 const std::string& output);

  static void logGlobal(const LogGroup&, const log::Message&);
  static void log(const LogGroup&, const log::Message&);

  static void reopen();
  static void shutdown();

  static bool haveAppenders(const LogGroup&, const LogTopic& topic);

  virtual ~Appender() = default;

  void logMessageGuarded(const Message&);

  static bool allowStdLogging() { return gAllowStdLogging; }
  static void allowStdLogging(bool value) { gAllowStdLogging = value; }

  static Result parseDefinition(const std::string& definition,
                                std::string& topic_name, std::string& output,
                                LogTopic*& topic);

 protected:
  virtual void logMessage(const Message& message) = 0;

 private:
  inline static bool gAllowStdLogging = true;

  basics::ReadWriteLock _log_output_mutex;
  std::atomic<std::thread::id> _log_output_mutex_owner;
};

}  // namespace log
}  // namespace sdb
