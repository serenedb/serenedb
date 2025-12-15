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

#include "log_buffer_feature.h"

#include <absl/strings/match.h>

#include <cstring>
#include <mutex>
#include <utility>

#include "app/app_server.h"
#include "app/logger_feature.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "app/options/section.h"
#include "basics/debugging.h"
#include "basics/logger/appender.h"
#include "basics/logger/logger.h"
#include "basics/string_utils.h"
#include "basics/strings.h"
#include "basics/system-functions.h"
#include "metrics/counter_builder.h"
#include "metrics/metrics_feature.h"

DECLARE_COUNTER(serenedb_logger_warnings_total, "Number of warnings logged.");
DECLARE_COUNTER(serenedb_logger_errors_total, "Number of errors logged.");

namespace sdb {

using namespace sdb::basics;
using namespace sdb::options;

namespace log {

/// logs to a fixed size ring buffer in memory
class AppenderRingBuffer final : public Appender {
 public:
  explicit AppenderRingBuffer(LogLevel min_log_level)
    : _min_log_level(min_log_level), _id(0) {
    std::lock_guard guard{_lock};
    _buffer.resize(LogBufferFeature::kBufferSize);
  }

 public:
  void logMessage(const Message& message) override {
    if (message.level > _min_log_level) {
      // logger not configured to log these messages
      return;
    }

    double timestamp = utilities::GetMicrotime();

    std::lock_guard guard{_lock};

    uint64_t n = _id++;
    LogBuffer& ptr = _buffer[n % LogBufferFeature::kBufferSize];

    ptr.id = n;
    ptr.level = message.level;
    ptr.topic = message.topic;
    ptr.timestamp = timestamp;
    CopyString(ptr.message, message.message.c_str() + message.offset,
               sizeof(ptr.message) - 1);
  }

  void Clear() {
    std::lock_guard guard{_lock};
    _id = 0;
    _buffer.clear();
    _buffer.resize(LogBufferFeature::kBufferSize);
  }

  /// return all buffered log entries
  std::vector<LogBuffer> Entries(LogLevel level, uint64_t start,
                                 bool up_to_level, std::string_view search) {
    std::vector<LogBuffer> result;
    result.reserve(16);

    uint64_t s = 0;
    uint64_t n;

    std::lock_guard guard{_lock};

    if (_id >= LogBufferFeature::kBufferSize) {
      s = _id % LogBufferFeature::kBufferSize;
      n = LogBufferFeature::kBufferSize;
    } else {
      n = static_cast<uint64_t>(_id);
    }

    for (uint64_t i = s; 0 < n; --n) {
      const LogBuffer& p = _buffer[i];

      if (p.id >= start) {
        bool matches =
          search.empty() || absl::StrContainsIgnoreCase(p.message, search);

        if (matches) {
          if (up_to_level) {
            if (static_cast<int>(p.level) <= static_cast<int>(level)) {
              result.emplace_back(p);
            }
          } else {
            if (p.level == level) {
              result.emplace_back(p);
            }
          }
        }
      }

      ++i;

      if (i >= LogBufferFeature::kBufferSize) {
        i = 0;
      }
    }

    return result;
  }

 private:
  absl::Mutex _lock;
  const LogLevel _min_log_level;
  uint64_t _id;
  std::vector<LogBuffer> _buffer;
};

/// log appender that increases counters for warnings/errors
/// in our metrics
class AppenderMetricsCounter final : public Appender {
 public:
  AppenderMetricsCounter(metrics::MetricsFeature& metrics)
    : _warnings_counter(metrics.add(serenedb_logger_warnings_total{})),
      _errors_counter(metrics.add(serenedb_logger_errors_total{})) {}

  void logMessage(const Message& message) override {
    // only handle WARN and ERR log messages
    if (message.level == LogLevel::WARN) {
      ++_warnings_counter;
    } else if (message.level == LogLevel::ERR) {
      ++_errors_counter;
    }
  }

 private:
  metrics::Counter& _warnings_counter;
  metrics::Counter& _errors_counter;
};

}  // namespace log

LogBufferFeature::LogBufferFeature(Server& server)
  : SerenedFeature{server, name()} {
  setOptional(true);

  _metrics_counter = std::make_shared<log::AppenderMetricsCounter>(
    server.getFeature<metrics::MetricsFeature>());

  log::Appender::addGlobalAppender(log::GetDefaultLogGroup(), _metrics_counter);
}

void LogBufferFeature::collectOptions(
  std::shared_ptr<options::ProgramOptions> options) {
  options
    ->addOption("--log.in-memory",
                "Use an in-memory log appender which can be queried via the "
                "API and web interface.",
                new BooleanParameter(&_use_in_memory_appender),
                sdb::options::MakeDefaultFlags(sdb::options::Flags::Uncommon))

    .setLongDescription(R"(You can use this option to toggle storing log
messages in memory, from which they can be consumed via the `/_admin/log`
HTTP API and via the web interface.

By default, this option is turned on, so log messages are consumable via the API
and web interface. Turning this option off disables that functionality, saves a
bit of memory for the in-memory log buffers, and prevents potential log
information leakage via these means.)");

  options
    ->addOption(
      "--log.in-memory-level",
      "Use an in-memory log appender only for this log level and higher.",
      new DiscreteValuesParameter<StringParameter>(&_min_in_memory_log_level,
                                                   {
                                                     "fatal",
                                                     "error",
                                                     "err",
                                                     "warning",
                                                     "warn",
                                                     "info",
                                                     "debug",
                                                     "trace",
                                                   }),
      sdb::options::MakeDefaultFlags(sdb::options::Flags::Uncommon))
    .setLongDescription(R"(You can use this option to control which log
messages are preserved in memory (in case `--log.in-memory` is enabled).

The default value is `info`, meaning all log messages of types `info`,
`warning`, `error`, and `fatal` are stored in-memory by an instance. By setting
this option to `warning`, only `warning`, `error` and `fatal` log messages are
preserved in memory, and by setting the option to `error`, only `error` and
`fatal` messages are kept.

This option is useful because the number of in-memory log messages is limited
to the latest 2048 messages, and these slots are shared between informational,
warning, and error messages by default.)");
}

void LogBufferFeature::prepare() {
  SDB_ASSERT(_in_memory_appender == nullptr);

  if (_use_in_memory_appender) {
    // only create the in-memory appender when we really need it. if we created
    // it in the ctor, we would waste a lot of memory in case we don't need the
    // in-memory appender. this is the case for simple command such as `--help`
    // etc.
    LogLevel level;
    bool is_valid =
      log::TranslateLogLevel(_min_in_memory_log_level, true, level);
    if (!is_valid) {
      level = LogLevel::INFO;
    }

    _in_memory_appender = std::make_shared<log::AppenderRingBuffer>(level);
    log::Appender::addGlobalAppender(log::GetDefaultLogGroup(),
                                     _in_memory_appender);
  }
}

void LogBufferFeature::clear() {
  if (_in_memory_appender != nullptr) {
    static_cast<log::AppenderRingBuffer*>(_in_memory_appender.get())->Clear();
  }
}

std::vector<LogBuffer> LogBufferFeature::entries(
  LogLevel level, uint64_t start, bool up_to_level,
  const std::string& search_string) {
  if (_in_memory_appender == nullptr) {
    return std::vector<LogBuffer>();
  }
  SDB_ASSERT(_use_in_memory_appender);
  return static_cast<log::AppenderRingBuffer*>(_in_memory_appender.get())
    ->Entries(level, start, up_to_level, search_string);
}

}  // namespace sdb
