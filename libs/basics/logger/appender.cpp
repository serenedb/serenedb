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

#include "appender.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>

#include <algorithm>
#include <string>
#include <string_view>
#include <type_traits>

#include "basics/errors.h"
#include "basics/logger/appender_file.h"
#include "basics/logger/log_group.h"
#include "basics/logger/logger.h"
#include "basics/operating-system.h"
#include "basics/recursive_locker.h"
#include "basics/string_utils.h"
#include "basics/write_locker.h"

namespace sdb::log {
namespace {

constexpr std::string_view kFilePrefix = "file://";

constinit absl::Mutex gAppendersLock{absl::kConstInit};
std::array<std::vector<std::shared_ptr<Appender>>, LogGroup::kCount>
  gGlobalAppenders;
std::array<std::map<const LogTopic*, std::vector<std::shared_ptr<Appender>>>,
           LogGroup::kCount>
  gTopics2appenders;
std::array<std::map<std::string, std::shared_ptr<Appender>>, LogGroup::kCount>
  gDefinition2appenders;

}  // namespace

void Appender::addGlobalAppender(const LogGroup& group,
                                 std::shared_ptr<Appender> appender) {
  absl::WriterMutexLock guard{&gAppendersLock};
  gGlobalAppenders[group.id()].emplace_back(std::move(appender));
}

void Appender::addAppender(const LogGroup& group,
                           const std::string& definition) {
  std::string topic_name;
  std::string output;
  LogTopic* topic = nullptr;
  Result res = parseDefinition(definition, topic_name, output, topic);

  if (res.fail()) {
    SDB_ERROR("xxxxx", Logger::FIXME, res.errorMessage());
    return;
  }

  auto key = output;

  std::shared_ptr<Appender> appender;

  absl::WriterMutexLock guard{&gAppendersLock};

  auto& definitions_map = gDefinition2appenders[group.id()];

  auto it = definitions_map.find(key);

  if (it != definitions_map.end()) {
    // found an existing appender
    appender = it->second;
  } else {
    // build a new appender from the definition
    appender = buildAppender(group, output);
    if (appender == nullptr) {
      // cannot create appender, for whatever reason
      return;
    }

    definitions_map[key] = appender;
  }

  SDB_ASSERT(appender);

  auto& topics_map = gTopics2appenders[group.id()];
  if (auto& appenders = topics_map[topic];
      !absl::c_linear_search(appenders, appender)) {
    appenders.emplace_back(appender);
  }
}

std::shared_ptr<Appender> Appender::buildAppender(const LogGroup& group,
                                                  const std::string& output) {

  if (output == "+" || output == "-") {
    for (const auto& it : gDefinition2appenders[group.id()]) {
      if (it.first == "+" || it.first == "-") {
        // already got a logger for stderr/stdout
        return nullptr;
      }
    }
  }

  // everything else must be file-/stream-based logging
  std::shared_ptr<AppenderStream> result;

  if (output == "+") {
    result = std::make_shared<AppenderStderr>();
  } else if (output == "-") {
    result = std::make_shared<AppenderStdout>();
  } else if (output.starts_with(kFilePrefix)) {
    result =
      AppenderFileFactory::getFileAppender(output.substr(kFilePrefix.size()));
  }

  return result;
}

void Appender::logGlobal(const LogGroup& group, const log::Message& message) {
  absl::ReaderMutexLock guard{&gAppendersLock};

  try {
    auto& appenders = gGlobalAppenders.at(group.id());

    // append to global appenders first
    for (const auto& appender : appenders) {
      appender->logMessageGuarded(message);
    }
  } catch (const std::out_of_range&) {
    // no global appender for this group
    SDB_ASSERT(false, "no global appender for group ", group.id());
    // This should never happen, however if it does we should not crash
    // but we also cannot log anything, as we are the logger.
  }
}

void Appender::log(const LogGroup& group, const log::Message& message) {
  // output to appenders
  absl::ReaderMutexLock guard{&gAppendersLock};
  try {
    auto& topics_map = gTopics2appenders.at(group.id());
    auto output = [&topics_map](const LogGroup& group,
                                const log::Message& message,
                                const LogTopic* topic) -> bool {
      bool shown = false;

      const auto& it = topics_map.find(topic);
      if (it != topics_map.end() && !it->second.empty()) {
        const auto& appenders = it->second;

        for (const auto& appender : appenders) {
          appender->logMessageGuarded(message);
        }
        shown = true;
      }

      return shown;
    };

    bool shown = false;

    if (auto topic = message.topic; topic) {
      shown = output(group, message, topic);
    }

    // otherwise use the general topic appender
    if (!shown) {
      output(group, message, nullptr);
    }
  } catch (const std::out_of_range&) {
    // no topic 2 appenders entry for this group.
    SDB_ASSERT(false, "no topic 2 appender match for group ", group.id());
    // This should never happen, however if it does we should not crash
    // but we also cannot log anything, as we are the logger.
  }
}

void Appender::shutdown() {
  absl::WriterMutexLock guard{&gAppendersLock};

  AppenderFileFactory::closeAll();

  for (size_t i = 0; i < LogGroup::kCount; ++i) {
    gGlobalAppenders[i].clear();
    gTopics2appenders[i].clear();
    gDefinition2appenders[i].clear();
  }
}

void Appender::reopen() {
  absl::WriterMutexLock guard{&gAppendersLock};

  AppenderFileFactory::reopenAll();
}

void Appender::logMessageGuarded(const Message& message) {
  // Only one thread is allowed to actually write logs to the file.
  // We use a recusive lock here, just in case writing the log message
  // causes a crash, in this case we may trigger another force-direct
  // log. This is not very likely, but it is better to be safe than sorry.
  RECURSIVE_WRITE_LOCKER(_log_output_mutex, _log_output_mutex_owner);
  logMessage(message);
}

Result Appender::parseDefinition(const std::string& definition,
                                 std::string& topic_name, std::string& output,
                                 LogTopic*& topic) {
  topic_name.clear();
  output.clear();
  topic = nullptr;

  // split into parts and do some basic validation
  std::vector<std::string_view> v = absl::StrSplit(definition, '=');

  if (v.size() == 1) {
    output = v[0];
  } else if (v.size() == 2) {
    topic_name = absl::AsciiStrToLower(v[0]);

    if (topic_name.empty()) {
      output = v[0];
    } else {
      output = v[1];
    }
  } else {
    return {ERROR_BAD_PARAMETER, "strange output definition '", definition,
            "' ignored"};
  }

  if (!topic_name.empty()) {
    topic = log::FindTopic(topic_name);
    if (!topic) {
      return {ERROR_BAD_PARAMETER, "strange topic '", topic_name,
              "', ignoring whole defintion"};
    }
  }

  // must be a file-based logger now.
  if (output != "+" && output != "-" && !output.starts_with(kFilePrefix)) {
    return {ERROR_BAD_PARAMETER,
            absl::StrCat("unknown output definition '", output, "'")};
  }

  return {};
}

bool Appender::haveAppenders(const LogGroup& group, const LogTopic& topic) {
  // It might be preferable if we could avoid the lock here, but ATM this is not
  // possible. If this actually causes performance issues we have to think about
  // other solutions.
  absl::ReaderMutexLock guard{&gAppendersLock};
  try {
    const auto& appenders = gTopics2appenders.at(group.id());
    auto have_topic_appenders = [&appenders](const LogTopic* topic) {
      auto it = appenders.find(topic);
      return it != appenders.end() && !it->second.empty();
    };
    return have_topic_appenders(&topic) || have_topic_appenders(nullptr) ||
           !gGlobalAppenders.at(group.id()).empty();
  } catch (const std::out_of_range&) {
    // no topic 2 appenders entry for this group.
    SDB_ASSERT(false, "no topic 2 appender match for group ", group.id());
    // This should never happen, however if it does we should not crash
    // but we also cannot log anything, as we are the logger.
    return false;
  }
}

}  // namespace sdb::log
