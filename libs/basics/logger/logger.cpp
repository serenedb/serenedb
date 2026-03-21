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

#include "logger.h"

#include <atomic>
#include <chrono>
#include <cstring>
#include <string>
#include <string_view>
#include <thread>

#ifdef SERENEDB_HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <absl/hash/hash.h>
#include <absl/strings/str_split.h>

#include "basics/application-exit.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/debugging.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/logger/appender.h"
#include "basics/logger/appender_file.h"
#include "basics/logger/escaper.h"
#include "basics/logger/log_group.h"
#include "basics/logger/log_level.h"
#include "basics/logger/log_thread.h"
#include "basics/logger/logger.h"
#include "basics/operating-system.h"
#include "basics/sink.h"
#include "basics/string_utils.h"
#include "basics/system-functions.h"
#include "basics/thread.h"

namespace sdb {

LogTopic Logger::AGENCY{"agency", LogLevel::INFO};
LogTopic Logger::AGENCYCOMM{"agencycomm", LogLevel::INFO};
LogTopic Logger::AGENCYSTORE{"agencystore", LogLevel::WARN};
LogTopic Logger::AQL{"aql", LogLevel::INFO};
LogTopic Logger::AUTHENTICATION{"authentication", LogLevel::WARN};
LogTopic Logger::AUTHORIZATION{"authorization"};
LogTopic Logger::BACKUP{"backup"};
LogTopic Logger::CLUSTER{"cluster", LogLevel::INFO};
LogTopic Logger::COMMUNICATION{"communication", LogLevel::INFO};
LogTopic Logger::CONFIG{"config"};
LogTopic Logger::CRASH{"crash"};
LogTopic Logger::DEVEL{"development", LogLevel::FATAL};
LogTopic Logger::DUMP{"dump", LogLevel::INFO};
LogTopic Logger::ENGINES{"engines", LogLevel::INFO};
LogTopic Logger::FIXME{"general", LogLevel::INFO};
LogTopic Logger::FLUSH{"flush", LogLevel::INFO};
LogTopic Logger::GRAPHS{"graphs", LogLevel::INFO};
LogTopic Logger::HEARTBEAT{"heartbeat", LogLevel::INFO};
LogTopic Logger::HTTPCLIENT{"httpclient", LogLevel::WARN};
LogTopic Logger::MAINTENANCE{"maintenance", LogLevel::INFO};
LogTopic Logger::MEMORY{"memory", LogLevel::INFO};
LogTopic Logger::QUERIES{"queries", LogLevel::INFO};
LogTopic Logger::REPLICATION{"replication", LogLevel::INFO};
LogTopic Logger::REQUESTS{"requests", LogLevel::FATAL};
LogTopic Logger::RESTORE{"restore", LogLevel::INFO};
LogTopic Logger::ROCKSDB{"rocksdb", LogLevel::WARN};
LogTopic Logger::SECURITY{"security", LogLevel::INFO};
LogTopic Logger::SSL{"ssl", LogLevel::WARN};
LogTopic Logger::STARTUP{"startup", LogLevel::INFO};
LogTopic Logger::STATISTICS{"statistics", LogLevel::INFO};
LogTopic Logger::SUPERVISION{"supervision", LogLevel::INFO};
LogTopic Logger::SYSCALL{"syscall", LogLevel::INFO};
LogTopic Logger::THREADS{"threads", LogLevel::WARN};
LogTopic Logger::TRANSACTIONS{"trx", LogLevel::WARN};
LogTopic Logger::TTL{"ttl", LogLevel::WARN};
LogTopic Logger::SEARCH{"search", LogLevel::INFO};
LogTopic Logger::IRESEARCH{"iresearch", LogLevel::INFO};
LogTopic Logger::FUERTE{"fuerte", LogLevel::INFO};

namespace log {
namespace {

struct TopicHashEq {
  using is_transparent = void;

  size_t operator()(const LogTopic* topic) const {
    return absl::HashOf(topic->GetName());
  }

  size_t operator()(const std::string_view topic) const {
    return absl::HashOf(topic);
  }

  bool operator()(const LogTopic* l, std::string_view r) const {
    return l->GetName() == r;
  }

  bool operator()(const LogTopic* l, const LogTopic* r) const { return l == r; }
};

const containers::FlatHashSet<LogTopic*, TopicHashEq, TopicHashEq> kTopics{
  &Logger::AGENCY,
  &Logger::AGENCYCOMM,
  &Logger::AGENCYSTORE,
  &Logger::AQL,
  &Logger::AUTHENTICATION,
  &Logger::AUTHORIZATION,
  &Logger::BACKUP,
  &Logger::CLUSTER,
  &Logger::COMMUNICATION,
  &Logger::CONFIG,
  &Logger::CRASH,
  &Logger::DEVEL,
  &Logger::DUMP,
  &Logger::ENGINES,
  &Logger::FIXME,
  &Logger::FLUSH,
  &Logger::GRAPHS,
  &Logger::HEARTBEAT,
  &Logger::HTTPCLIENT,
  &Logger::MAINTENANCE,
  &Logger::MEMORY,
  &Logger::QUERIES,
  &Logger::REPLICATION,
  &Logger::REQUESTS,
  &Logger::RESTORE,
  &Logger::ROCKSDB,
  &Logger::SECURITY,
  &Logger::SSL,
  &Logger::STARTUP,
  &Logger::STATISTICS,
  &Logger::SUPERVISION,
  &Logger::SYSCALL,
  &Logger::THREADS,
  &Logger::TRANSACTIONS,
  &Logger::TTL,
};

constinit LogGroup gDefaultLogGroupInstance{0};

// these variables might be changed asynchronously
std::atomic<bool> gActive = false;
std::atomic<LogLevel> gLevel = LogLevel::INFO;

log_time_formats::TimeFormat gTimeFormat =
  log_time_formats::TimeFormat::UTCDateString;
bool gShowLineNumber = false;
bool gShortenFilenames = true;
bool gShowProcessIdentifier = true;
bool gShowThreadIdentifier = false;
bool gShowThreadName = false;
bool gShowRole = false;
bool gUseColor = true;
bool gUseControlEscaped = true;
bool gUseUnicodeEscaped = false;
bool gKeepLogRotate = false;
bool gLogRequestParameters = true;
bool gShowIds = false;
bool gUseJson = false;
char gRole = '\0';  // current server role to log
std::atomic<pid_t> gCachedPid = 0;
std::string gOutputPrefix;
std::string gHostname;
void (*gWriterFn)(std::string_view, std::string&) =
  &Escaper<ControlCharsEscaper, UnicodeCharsRetainer>::writeIntoOutputBuffer;

// logger thread. only populated when threaded logging is selected.
// the pointer must only be used with atomic accessors after the ref counter
// has been increased. Best to use the ThreadRef class for this!
std::atomic<size_t> gLoggingThreadRefs = 0;
std::atomic<LogThread*> gLoggingThread = nullptr;

struct ThreadRef {
  ThreadRef() {
    // (1) - this acquire-fetch-add synchronizes with the release-fetch-add (5)
    gLoggingThreadRefs.fetch_add(1, std::memory_order_acquire);
    // (2) - this acquire-load synchronizes with the release-store (4)
    _thread = gLoggingThread.load(std::memory_order_acquire);
  }
  ~ThreadRef() {
    // (3) - this relaxed-fetch-add is potentially part of a release-sequence
    //       headed by (5)
    gLoggingThreadRefs.fetch_sub(1, std::memory_order_relaxed);
  }

  ThreadRef(const ThreadRef&) = delete;
  ThreadRef(ThreadRef&&) = delete;
  ThreadRef& operator=(const ThreadRef&) = delete;
  ThreadRef& operator=(ThreadRef&&) = delete;

  LogThread* operator->() const noexcept { return _thread; }
  operator bool() const noexcept { return _thread != nullptr; }

 private:
  LogThread* _thread;
};

void Append(LogGroup& group, std::unique_ptr<Message> msg, bool force_direct) {
  SDB_ASSERT(msg != nullptr);

  // check if we need to shrink the message here
  if (!msg->shrunk) {
    msg->shrink(group.maxLogEntryLength());
  }

  // first log to all "global" appenders, which are the in-memory ring buffer
  // logger and the metrics counter. note that these loggers do not require any
  // configuration so we can always and safely invoke them.
  Appender::logGlobal(group, *msg);

  if (!gActive.load(std::memory_order_acquire)) {
    // logging is still turned off. now use hard-coded to-stderr logging
    AppenderStdStream::writeLogMessage(
      STDERR_FILENO, (isatty(STDERR_FILENO) == 1), msg->level, msg->message);
  } else {
    // now either queue or output the message
    bool handled = false;
    if (!force_direct) {
      // check if we have a logging thread
      ThreadRef logging_thread;

      if (logging_thread) {
        handled = logging_thread->log(group, msg);
      }
    }

    if (!handled) {
      SDB_ASSERT(msg != nullptr);

      SDB_IF_FAILURE("log::Append") {
        // cut off all logging
        return;
      }

      Appender::log(group, *msg);
    }
  }
}

}  // namespace

void Message::shrink(size_t max_length) {
  // no need to shrink an already shrunk message
  if (!shrunk && message.size() > max_length) {
    message.resize(max_length);

    // normally, offset should be around 20 to 30 bytes,
    // whereas the minimum for maxLength should be around 256 bytes.
    SDB_ASSERT(max_length > offset);
    if (offset > message.size()) {
      // we need to make sure that the offset is not outside of the message
      // after shrinking
      offset = static_cast<uint32_t>(message.size());
    }
    message.append("...\n", 4);
    shrunk = true;
  }
}

void SetLogLevel(LogLevel level) noexcept {
  gLevel.store(level, std::memory_order_relaxed);
}

LogGroup& GetDefaultLogGroup() noexcept { return gDefaultLogGroupInstance; }

LogLevel GetLogLevel() noexcept {
  return gLevel.load(std::memory_order_relaxed);
}

void SetShowIds(bool show) { gShowIds = show; }

void SetLogLevel(std::string_view level_assign) {
  const auto level_assign_lower = absl::AsciiStrToLower(level_assign);
  std::vector<std::string_view> v = absl::StrSplit(level_assign_lower, '=');

  if (v.empty() || v.size() > 2) {
    SetLogLevel(LogLevel::INFO);
    SDB_ERROR("xxxxx", Logger::FIXME, "strange log level '", level_assign,
              "', using log level 'info'");
    return;
  }

  // if log level is "foo = bar", we better get rid of the whitespace
  v[0] = basics::string_utils::Trim(v[0]);
  auto input_level = v[0];
  bool is_general = v.size() == 1;

  if (!is_general) {
    v[1] = basics::string_utils::Trim(v[1]);
    input_level = v[1];
  }

  LogLevel level;
  bool is_valid = log::TranslateLogLevel(input_level, is_general, level);

  if (!is_valid) {
    if (!is_general) {
      SDB_WARN("xxxxx", Logger::FIXME, "strange log level '", level_assign,
               "'");
      return;
    }
    level = LogLevel::INFO;
    SDB_WARN("xxxxx", Logger::FIXME, "strange log level '", level_assign,
             "', using log level 'info'");
  }

  if (is_general) {
    // set the log level globally (e.g. `--log.level info`). note that
    // this does not set the log level for all log topics, but only the
    // log level for the "general" log topic.
    SetLogLevel(level);
    // setting the log level for topic "general" is required here, too,
    // as "fixme" is the previous general log topic...
    SetLogLevel("general", level);
  } else if (v[0] == LogTopic::kAll) {
    // handle pseudo log-topic "all": this will set the log level for
    // all existing topics
    for (auto* topic : GetTopics()) {
      topic->SetLevel(level);
    }
  } else {
    // handle a topic-specific request (e.g. `--log.level requests=info`).
    SetLogLevel(v[0], level);
  }
}

void SetRole(char role) { gRole = role; }

// NOTE: this function should not be called if the logging is active.
void SetOutputPrefix(std::string_view prefix) {
  if (gActive) {
    SDB_THROW(ERROR_INTERNAL, "cannot change settings once logging is active");
  }

  gOutputPrefix = prefix;
}

// NOTE: this function should not be called if the logging is active.
void SetHostname(std::string_view hostname) {
  if (gActive) {
    SDB_THROW(ERROR_INTERNAL, "cannot change settings once logging is active");
  }

  if (hostname == "auto") {
    gHostname = utilities::Hostname();
  } else {
    gHostname = hostname;
  }
}

// NOTE: this function should not be called if the logging is active.
void SetShowLineNumber(bool show) {
  if (gActive) {
    SDB_THROW(ERROR_INTERNAL, "cannot change settings once logging is active");
  }

  gShowLineNumber = show;
}

// NOTE: this function should not be called if the logging is active.
void SetShortenFilenames(bool shorten) {
  if (gActive) {
    SDB_THROW(ERROR_INTERNAL, "cannot change settings once logging is active");
  }

  gShortenFilenames = shorten;
}

// NOTE: this function should not be called if the logging is active.
void SetShowProcessIdentifier(bool show) {
  if (gActive) {
    SDB_THROW(ERROR_INTERNAL, "cannot change settings once logging is active");
  }

  gShowProcessIdentifier = show;
}

// NOTE: this function should not be called if the logging is active.
void SetShowThreadIdentifier(bool show) {
  if (gActive) {
    SDB_THROW(ERROR_INTERNAL, "cannot change settings once logging is active");
  }

  gShowThreadIdentifier = show;
}

// NOTE: this function should not be called if the logging is active.
void SetShowThreadName(bool show) {
  if (gActive) {
    SDB_THROW(ERROR_INTERNAL, "cannot change settings once logging is active");
  }

  gShowThreadName = show;
}

// NOTE: this function should not be called if the logging is active.
void SetUseColor(bool value) {
  if (gActive) {
    SDB_THROW(ERROR_INTERNAL, "cannot change settings once logging is active");
  }

  gUseColor = value;
}

bool GetKeepRotate() noexcept { return gKeepLogRotate; }
bool IsActive() noexcept { return gActive.load(std::memory_order_acquire); }
void Deactive() noexcept { gActive.store(false, std::memory_order_release); }

bool GetUseColor() { return gUseColor; }

bool GetUseControlEscaped() { return gUseControlEscaped; };
bool GetUseUnicodeEscaped() { return gUseUnicodeEscaped; };

bool GetUseLocalTime() { return log_time_formats::IsLocalFormat(gTimeFormat); }

bool GetLogRequestParameters() { return gLogRequestParameters; }

log_time_formats::TimeFormat GetTtimeFormat() { return gTimeFormat; }

void ClearCachedPid() { gCachedPid.store(0, std::memory_order_relaxed); }

// NOTE: this function should not be called if the logging is active.
void SetUseControlEscaped(bool value) {
  if (gActive) {
    SDB_THROW(ERROR_INTERNAL, "cannot change settings once logging is active");
  }
  gUseControlEscaped = value;
}
// NOTE: this function should not be called if the logging is active.
void SetUseUnicodeEscaped(bool value) {
  if (gActive) {
    SDB_THROW(ERROR_INTERNAL, "cannot change settings once logging is active");
  }
  gUseUnicodeEscaped = value;
}

// alerts that startup parameters for escaping have already been assigned
void SetEscaping() {
  if (gActive) {
    SDB_THROW(ERROR_INTERNAL, "cannot change settings once logging is active");
  }

  if (gUseControlEscaped) {
    if (gUseUnicodeEscaped) {
      gWriterFn = &Escaper<ControlCharsEscaper,
                           UnicodeCharsEscaper>::writeIntoOutputBuffer;
    } else {
      gWriterFn = &Escaper<ControlCharsEscaper,
                           UnicodeCharsRetainer>::writeIntoOutputBuffer;
    }
  } else {
    if (gUseUnicodeEscaped) {
      gWriterFn = &Escaper<ControlCharsSuppressor,
                           UnicodeCharsEscaper>::writeIntoOutputBuffer;
    } else {
      gWriterFn = &Escaper<ControlCharsSuppressor,
                           UnicodeCharsRetainer>::writeIntoOutputBuffer;
    }
  }
}

// NOTE: this function should not be called if the logging is active.
void SetShowRole(bool show) {
  if (gActive) {
    SDB_THROW(ERROR_INTERNAL, "cannot change settings once logging is active");
  }

  gShowRole = show;
}

// NOTE: this function should not be called if the logging is active.
void SetTimeFormat(log_time_formats::TimeFormat format) {
  if (gActive) {
    SDB_THROW(ERROR_INTERNAL, "cannot change settings once logging is active");
  }

  gTimeFormat = format;
}

// NOTE: this function should not be called if the logging is active.
void SetKeepLogrotate(bool keep) {
  if (gActive) {
    SDB_THROW(ERROR_INTERNAL, "cannot change settings once logging is active");
  }

  gKeepLogRotate = keep;
}

// NOTE: this function should not be called if the logging is active.
void SetLogRequestParameters(bool log) {
  if (gActive) {
    SDB_THROW(ERROR_INTERNAL, "cannot change settings once logging is active");
  }

  gLogRequestParameters = log;
}

// NOTE: this function should not be called if the logging is active.
void SetUseJson(bool value) {
  if (gActive) {
    SDB_THROW(ERROR_INTERNAL, "cannot change settings once logging is active");
  }

  gUseJson = value;
}

void Log(const char* logid, const char* function, const char* file, int line,
         LogLevel level, const LogTopic& topic, std::string_view message) try {
  SDB_ASSERT(logid != nullptr);
  // we only determine our pid once, as currentProcessId() will
  // likely do a syscall.
  // this read-check-update sequence is not thread-safe, but this
  // should not matter, as the pid value is only changed from 0 to the
  // actual pid and never changes afterwards
  if (gCachedPid.load(std::memory_order_relaxed) == 0) {
    gCachedPid.store(Thread::currentProcessId(), std::memory_order_relaxed);
  }

  basics::StrSink sink;
  auto& out = sink.Impl();
  out.reserve(256 + message.size());

  uint32_t offset = 0;
  bool shrunk = false;

  if (gUseJson) {
    // construct JSON output
    basics::string_utils::EscapeJsonOptions options{
      .escape_control = gUseControlEscaped,
      .escape_unicode = gUseUnicodeEscaped,
    };

    auto append_str = [&](std::string_view str) {
      basics::string_utils::EscapeJsonStr(str, &sink, options);
    };

    out.push_back('{');

    // current date/time
    {
      out.append("\"time\":");
      if (log_time_formats::IsStringFormat(gTimeFormat)) {
        out.push_back('"');
      }
      // value of date/time is always safe to print
      log_time_formats::WriteTime(out, gTimeFormat,
                                  std::chrono::system_clock::now());
      if (log_time_formats::IsStringFormat(gTimeFormat)) {
        out.push_back('"');
      }
    }

    // prefix
    if (!gOutputPrefix.empty()) {
      out.append(",\"prefix\":");
      append_str(gOutputPrefix);
    }

    // pid
    if (gShowProcessIdentifier) {
      out.append(",\"pid\":");
      sink.PushU64(gCachedPid.load(std::memory_order_relaxed));
    }

    // tid
    if (gShowThreadIdentifier) {
      out.append(",\"tid\":");
      sink.PushU64(Thread::currentThreadNumber());
    }

    // thread name
    if (gShowThreadName) {
      out.append(",\"thread\":");
      ThreadNameFetcher fetcher;
      append_str(fetcher.get());
    }

    // role
    if (gShowRole) {
      out.append(",\"role\":\"");
      if (gRole != '\0') {
        // value of _role is always safe to print
        out.push_back(gRole);
      }
      out.push_back('"');
    }

    // log level
    {
      out.append(",\"level\":");
      append_str(log::TranslateLogLevel(level));
    }

    // file and line
    if (gShowLineNumber) {
      if (file) {
        const char* filename = file;
        if (gShortenFilenames) {
          const char* shortened = strrchr(filename, SERENEDB_DIR_SEPARATOR_CHR);
          if (shortened != nullptr) {
            filename = shortened + 1;
          }
        }
        out.append(",\"file\":");
        append_str(filename);
      }

      out.append(",\"line\":");
      sink.PushU64(line);

      if (function) {
        out.append(",\"function\":");
        append_str(function);
      }
    }

    // the topic
    out.append(",\"topic\":");
    append_str(topic.GetName());

    // the logid
    if (gShowIds) {
      out.append(",\"id\":");
      // value of id is always safe to print
      append_str(logid);
    }

    // hostname
    if (!gHostname.empty()) {
      out.append(",\"hostname\":");
      append_str(gHostname);
    }

    // the message itself
    {
      out.append(",\"message\":");

      // the log message can be really large, and it can lead to
      // truncation of the log message further down the road.
      // however, as we are supposed to produce valid JSON log
      // entries even with the truncation in place, we need to make
      // sure that the dynamic text part is truncated and not the
      // entries JSON thing
      size_t max_message_length = GetDefaultLogGroup().maxLogEntryLength();
      // cut of prologue, the quotes ('"' --- ' '") and the final '}'
      if (max_message_length >= out.size() + 3) {
        max_message_length -= out.size() + 3;
      }
      if (max_message_length > message.size()) {
        max_message_length = message.size();
      }
      append_str({message.data(), max_message_length});

      // this tells the logger to not shrink our (potentially already
      // shrunk) message once more - if it would shrink the message again,
      // it may produce invalid JSON
      shrunk = true;
    }

    out.push_back('}');

    SDB_ASSERT(offset == 0);
  } else {
    // hostname
    if (!gHostname.empty()) {
      out.append(gHostname);
      out.push_back(' ');
    }

    // human readable format
    log_time_formats::WriteTime(out, gTimeFormat,
                                std::chrono::system_clock::now());
    out.push_back(' ');

    // output prefix
    if (!gOutputPrefix.empty()) {
      out.append(gOutputPrefix);
      out.push_back(' ');
    }

    // [pid-tid-threadname], all components are optional
    bool have_process_output = false;
    if (gShowProcessIdentifier) {
      // append the process / thread identifier
      SDB_ASSERT(gCachedPid.load(std::memory_order_relaxed) != 0);
      out.push_back('[');
      basics::string_utils::Itoa(
        uint64_t(gCachedPid.load(std::memory_order_relaxed)), out);
      have_process_output = true;
    }

    if (gShowThreadIdentifier) {
      out.push_back(have_process_output ? '-' : '[');
      basics::string_utils::Itoa(uint64_t(Thread::currentThreadNumber()), out);
      have_process_output = true;
    }

    // log thread name
    if (gShowThreadName) {
      ThreadNameFetcher name_fetcher;
      std::string_view thread_name = name_fetcher.get();

      out.push_back(have_process_output ? '-' : '[');
      out.append(thread_name.data(), thread_name.size());
      have_process_output = true;
    }

    if (have_process_output) {
      out.append("] ", 2);
    }

    if (gShowRole && gRole != '\0') {
      out.push_back(gRole);
      out.push_back(' ');
    }

    // log level
    out.append(log::TranslateLogLevel(level));
    out.push_back(' ');

    // check if we must display the line number
    if (gShowLineNumber && file != nullptr && function != nullptr) {
      const char* filename = file;

      if (gShortenFilenames) {
        // shorten file names from `/home/.../file.cpp` to just `file.cpp`
        const char* shortened = strrchr(filename, SERENEDB_DIR_SEPARATOR_CHR);
        if (shortened != nullptr) {
          filename = shortened + 1;
        }
      }
      out.push_back('[');
      out.append(function);
      out.push_back('@');
      out.append(filename);
      out.push_back(':');
      basics::string_utils::Itoa(uint64_t(line), out);
      out.append("] ", 2);
    }

    // the offset is used by the in-memory logger, and it cuts off everything
    // from the start of the concatenated log string until the offset. only
    // what's after the offset gets displayed in the web interface
    SDB_ASSERT(out.size() < static_cast<size_t>(UINT32_MAX));
    offset = static_cast<uint32_t>(out.size());

    if (gShowIds) {
      out.push_back('[');
      out.append(logid);
      out.append("] ", 2);
    }

    {
      out.push_back('{');
      out.append(topic.GetName());
      out.append("} ", 2);
    }

    gWriterFn(message, out);
  }

  SDB_ASSERT(offset == 0 || !gUseJson);

  // append final newline
  out.push_back('\n');

  auto msg =
    std::make_unique<Message>(level, std::move(out), offset, topic, shrunk);

  Append(GetDefaultLogGroup(), std::move(msg), false);
} catch (...) {
  // logging itself must never cause an exeption to escape
}

void Initialize() {
  if (gActive.exchange(true, std::memory_order_acquire)) {
    SDB_THROW(ERROR_INTERNAL, "Logger already initialized");
  }
}

void InitializeAsync(app::AppServer& server, uint32_t max_queued_log_messages) {
  Initialize();

  // logging is now active
  auto logging_thread = std::make_unique<LogThread>(
    server, std::string(log::kLogThreadName), max_queued_log_messages);
  if (!logging_thread->start()) {
    SDB_FATAL("xxxxx", Logger::FIXME, "could not start logging thread");
  }

  // (4) - this release-store synchronizes with the acquire-load (2)
  gLoggingThread.store(logging_thread.release(), std::memory_order_release);
}

void Shutdown() {
  if (!gActive.exchange(false, std::memory_order_acquire)) {
    // if logging not activated or already shut down, then we can abort here
    return;
  }
  // logging is now inactive

  // reset the instance variable in Logger, so that others won't see it
  // anymore
  std::unique_ptr<LogThread> logging_thread(
    gLoggingThread.exchange(nullptr, std::memory_order_relaxed));

  // logging is now inactive (this will terminate the logging thread)
  // join with the logging thread
  if (logging_thread != nullptr) {
    // (5) - this release-fetch-add synchronizes with the acquire-fetch-add
    // (1) Even though a fetch-add with 0 is essentially a noop, this is
    // necessary to ensure that threads which try to get a reference to the
    // _logging_thread actually see the new nullptr value.
    gLoggingThreadRefs.fetch_add(0, std::memory_order_release);

    // wait until all threads have dropped their reference to the logging
    // thread
    while (gLoggingThreadRefs.load(std::memory_order_relaxed)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    ThreadNameFetcher name_fetcher;
    std::string_view current_thread_name = name_fetcher.get();
    if (log::kLogThreadName == current_thread_name) {
      // oops, the LogThread itself crashed...
      // so we need to flush the log messages here ourselves - if we waited
      // for the LogThread to flush them, we would wait forever.
      logging_thread->processPendingMessages();
      logging_thread->beginShutdown();
    } else {
      int tries = 0;
      while (logging_thread->hasMessages() && ++tries < 10) {
        logging_thread->wakeup();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      logging_thread->beginShutdown();
      // wait until logging thread has logged all active messages
      while (logging_thread->isRunning()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    }
  }

  // cleanup appenders
  Appender::shutdown();

  gCachedPid.store(0, std::memory_order_relaxed);
}

void Flush() noexcept {
  if (!gActive.load(std::memory_order_acquire)) {
    // logging not (or not yet) initialized
    return;
  }

  ThreadRef logging_thread;
  if (logging_thread) {
    logging_thread->flush();
  }
}

std::vector<LogTopic*> GetTopics() { return {kTopics.begin(), kTopics.end()}; }

void SetLogLevel(std::string_view name, LogLevel level) noexcept {
  auto* topic = FindTopic(name);

  if (!topic) [[unlikely]] {
    SDB_WARN("xxxxx", sdb::Logger::FIXME, "strange topic '", name, "'");
    return;
  }

  topic->SetLevel(level);
}

LogTopic* FindTopic(std::string_view name) noexcept {
  auto it = kTopics.find(name);
  return it == kTopics.end() ? nullptr : *it;
}

}  // namespace log
}  // namespace sdb
