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

#include <absl/strings/str_format.h>
#include <fcntl.h>
#include <stdio.h>

#include <algorithm>
#include <cstring>
#include <iostream>
#include <memory>

#include "basics/operating-system.h"

#ifdef SERENEDB_HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "app/shell_colors.h"
#include "appender_file.h"
#include "basics/debugging.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/file_utils.h"
#include "basics/files.h"
#include "basics/logger/logger.h"

namespace sdb::log {

using namespace sdb::basics;

AppenderStream::AppenderStream(const std::string& filename, int fd)
  : _fd(fd), _use_colors(false) {}

void AppenderStream::logMessage(const Message& message) {
  this->writeLogMessage(message.level, message.message);
}

AppenderFile::AppenderFile(const std::string& filename, int fd)
  : AppenderStream(filename, fd), _filename(filename) {
  _use_colors = ((isatty(_fd) == 1) && log::GetUseColor());
}

void AppenderFile::writeLogMessage(LogLevel level, const std::string& message) {
  bool give_up = false;
  const char* buffer = message.c_str();
  size_t len = message.size();

  while (len > 0) {
    auto n = SERENEDB_WRITE(_fd, buffer, static_cast<size_t>(len));

    if (n < 0) {
      if (allowStdLogging()) {
        fprintf(stderr, "cannot log data: %s\n", SERENEDB_ERRORNO_STR);
      }
      return;  // give up, but do not try to log the failure via the Logger
    }
    if (n == 0) {
      if (!give_up) {
        give_up = true;
        continue;
      }
    }

    SDB_ASSERT(len >= static_cast<size_t>(n));
    buffer += n;
    len -= n;
  }

  if (level == LogLevel::FATAL) {
    FILE* f = SERENEDB_FDOPEN(_fd, "a");
    if (f != nullptr) {
      // valid file pointer...
      // now flush the file one last time before we shut down
      fflush(f);
    }
  }
}

std::shared_ptr<AppenderFile> AppenderFileFactory::getFileAppender(
  const std::string& filename) {
  SDB_ASSERT(filename != "+");
  SDB_ASSERT(filename != "-");
  // logging to an actual file
  std::unique_lock guard(gOpenAppendersMutex);
  for (const auto& it : gOpenAppenders) {
    if (it->filename() == filename) {
      // already have an appender for the same file
      return it;
    }
  }
  // Does not exist yet, create a new one.
  // NOTE: We hold the lock here, to avoid someone creating
  // an appender on this file.
  int fd = SERENEDB_CREATE(filename.c_str(),
                           O_APPEND | O_CREAT | O_WRONLY | SERENEDB_O_CLOEXEC,
                           gFileMode);
  if (fd < 0) {
    std::cerr << "cannot write to file '" << filename
              << "': " << SERENEDB_ERRORNO_STR << std::endl;

    SDB_THROW(ERROR_CANNOT_WRITE_FILE);
  }

#ifdef SERENEDB_HAVE_SETGID
  if (gFileGroup != 0) {
    int result = fchown(fd, -1, gFileGroup);
    if (result != 0) {
      // we cannot log this error here, as we are the logging itself
      // so just to please compilers, we pretend we are using the result
      (void)result;
    }
  }
#endif

  try {
    auto appender = std::make_shared<AppenderFile>(filename, fd);
    gOpenAppenders.emplace_back(appender);
    return appender;
  } catch (...) {
    SERENEDB_CLOSE(fd);
    throw;
  }
}

void AppenderFileFactory::reopenAll() {
  // We must not log anything in this function or in anything it calls! This
  // is because this is called under the `_appenders_lock`.
  std::unique_lock guard(gOpenAppendersMutex);

  for (auto& it : gOpenAppenders) {
    int old = it->fd();
    const std::string& filename = it->filename();

    if (filename.empty()) {
      continue;
    }

    if (old <= STDERR_FILENO) {
      continue;
    }

    // rename log file
    std::string backup(filename);
    backup.append(".old");

    std::ignore = file_utils::Remove(backup);
    // TODO(mbkkt) why ignore?
    std::ignore = SdbRenameFile(filename.c_str(), backup.c_str());

    // open new log file
    int fd = SERENEDB_CREATE(filename.c_str(),
                             O_APPEND | O_CREAT | O_WRONLY | SERENEDB_O_CLOEXEC,
                             gFileMode);

    if (fd < 0) {
      // TODO(mbkkt) why ignore?
      std::ignore = SdbRenameFile(backup.c_str(), filename.c_str());
      continue;
    }

#ifdef SERENEDB_HAVE_SETGID
    if (gFileGroup != 0) {
      int result = fchown(fd, -1, gFileGroup);
      if (result != 0) {
        // we cannot log this error here, as we are the logging itself
        // so just to please compilers, we pretend we are using the result
        (void)result;
      }
    }
#endif

    if (!log::GetKeepRotate()) {
      std::ignore = file_utils::Remove(backup);
    }

    // and also tell the appender of the file descriptor change
    it->updateFd(fd);

    if (old > STDERR_FILENO) {
      SERENEDB_CLOSE(old);
    }
  }
}

void AppenderFileFactory::closeAll() {
  std::unique_lock guard(gOpenAppendersMutex);

  for (auto& it : gOpenAppenders) {
    int fd = it->fd();
    // set the fd to "disabled"
    // and also tell the appender of the file descriptor change
    it->updateFd(-1);

    if (fd > STDERR_FILENO) {
      fsync(fd);
      SERENEDB_CLOSE(fd);
    }
  }
  gOpenAppenders.clear();
}

#ifdef SDB_GTEST
std::vector<std::tuple<int, std::string, std::shared_ptr<AppenderFile>>>
AppenderFileFactory::getAppenders() {
  std::vector<std::tuple<int, std::string, std::shared_ptr<AppenderFile>>>
    result;

  std::unique_lock guard(gOpenAppendersMutex);
  for (const auto& it : gOpenAppenders) {
    result.emplace_back(it->fd(), it->filename(), it);
  }

  return result;
}

void AppenderFileFactory::setAppenders(
  const std::vector<
    std::tuple<int, std::string, std::shared_ptr<AppenderFile>>>& appenders) {
  std::unique_lock guard(gOpenAppendersMutex);

  gOpenAppenders.clear();
  for (const auto& it : appenders) {
    gOpenAppenders.emplace_back(std::get<2>(it));
  }
}
#endif

AppenderStdStream::AppenderStdStream(const std::string& filename, int fd)
  : AppenderStream(filename, fd) {
  _use_colors = ((isatty(_fd) == 1) && log::GetUseColor());
}

AppenderStdStream::~AppenderStdStream() {
  // flush output stream on shutdown
  if (allowStdLogging()) {
    FILE* fp = (_fd == STDOUT_FILENO ? stdout : stderr);
    fflush(fp);
  }
}

void AppenderStdStream::writeLogMessage(LogLevel level,
                                        const std::string& message) {
  writeLogMessage(_fd, _use_colors, level, message);
}

void AppenderStdStream::writeLogMessage(int fd, bool use_colors, LogLevel level,
                                        const std::string& message) {
  if (!allowStdLogging()) {
    return;
  }

  // out stream
  FILE* fp = (fd == STDOUT_FILENO ? stdout : stderr);

  if (use_colors) {
    // joyful color output
    if (level == LogLevel::FATAL || level == LogLevel::ERR) {
      absl::FPrintF(fp, "%s%s%s", colors::kRed, message, colors::kReset);
    } else if (level == LogLevel::WARN) {
      absl::FPrintF(fp, "%s%s%s", colors::kYellow, message, colors::kReset);
    } else {
      absl::FPrintF(fp, "%s%s%s", colors::kReset, message, colors::kReset);
    }
  } else {
    // non-colored output
    absl::FPrintF(fp, "%s", message);
  }

  if (level == LogLevel::FATAL || level == LogLevel::ERR ||
      level == LogLevel::WARN || level == LogLevel::INFO) {
    // flush the output so it becomes visible immediately
    // at least for log levels that are used seldomly
    // it would probably be overkill to flush everytime we
    // encounter a log message for level DEBUG or TRACE
    fflush(fp);
  }
}

AppenderStderr::AppenderStderr() : AppenderStdStream("+", STDERR_FILENO) {}

AppenderStdout::AppenderStdout() : AppenderStdStream("-", STDOUT_FILENO) {}

}  // namespace sdb::log
