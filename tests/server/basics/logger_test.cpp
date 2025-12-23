////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2020 ArangoDB GmbH, Cologne, Germany
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

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <date/date.h>

#include <regex>
#include <sstream>

#include "basics/common.h"
#include "basics/file_utils.h"
#include "basics/files.h"
#include "basics/logger/appender_file.h"
#include "basics/logger/logger.h"
#include "basics/string_utils.h"
#include "gtest/gtest.h"

#ifdef SERENEDB_HAVE_UNISTD_H
#include <unistd.h>
#endif

using namespace sdb;
using namespace sdb::basics;

namespace {
struct Synchronizer {
  std::mutex mutex;
  std::condition_variable cv;
  bool ready = false;

  void WaitForStart() {
    std::unique_lock lock(mutex);
    cv.wait(lock, [&] { return ready; });
  }
  void Start() {
    {
      std::unique_lock lock(mutex);
      ready = true;
    }
    cv.notify_all();
  }
};

}  // namespace

class LoggerTest : public ::testing::Test {
 protected:
  // store old state as backup
  std::vector<std::tuple<int, std::string, std::shared_ptr<log::AppenderFile>>>
    _backup;
  const std::string _path;
  const std::string _logfile1;
  const std::string _logfile2;

  LoggerTest()
    : _backup(log::AppenderFileFactory::getAppenders()),
      _path(SdbGetTempPath()),
      _logfile1(_path + "logfile1"),
      _logfile2(_path + "logfile2") {
    std::ignore = file_utils::Remove(_logfile1);
    std::ignore = file_utils::Remove(_logfile2);
    // remove any previous loggers
    log::AppenderFileFactory::closeAll();
  }

  ~LoggerTest() override {
    // restore old state
    log::AppenderFileFactory::setAppenders(_backup);
    log::AppenderFileFactory::reopenAll();

    std::ignore = file_utils::Remove(_logfile1);
    std::ignore = file_utils::Remove(_logfile2);
  }
};

TEST_F(LoggerTest, test_fds) {
  auto logger1 = log::AppenderFileFactory::getFileAppender(_logfile1);
  auto logger2 = log::AppenderFileFactory::getFileAppender(_logfile2);

  auto fds = log::AppenderFileFactory::getAppenders();
  EXPECT_EQ(fds.size(), 2);

  EXPECT_EQ(std::get<1>(fds[0]), _logfile1);
  EXPECT_EQ(std::get<2>(fds[0])->fd(), std::get<0>(fds[0]));

  LogTopic dummy{"dummy"};

  logger1->logMessageGuarded(
    log::Message(LogLevel::ERR, "some error message", 0, dummy, true));
  logger2->logMessageGuarded(
    log::Message(LogLevel::WARN, "some warning message", 0, dummy, true));

  std::string content = file_utils::Slurp(_logfile1);
  EXPECT_NE(content.find("some error message"), std::string::npos);
  EXPECT_EQ(content.find("some warning message"), std::string::npos);

  content = file_utils::Slurp(_logfile2);
  EXPECT_EQ(content.find("some error message"), std::string::npos);
  EXPECT_NE(content.find("some warning message"), std::string::npos);

  log::AppenderFileFactory::closeAll();
}

TEST_F(LoggerTest, test_fds_after_reopen) {
  auto logger1 = log::AppenderFileFactory::getFileAppender(_logfile1);
  auto logger2 = log::AppenderFileFactory::getFileAppender(_logfile2);

  auto fds = log::AppenderFileFactory::getAppenders();
  EXPECT_EQ(fds.size(), 2);
  LogTopic dummy{"dummy"};

  EXPECT_EQ(std::get<1>(fds[0]), _logfile1);
  EXPECT_EQ(std::get<2>(fds[0])->fd(), std::get<0>(fds[0]));

  logger1->logMessageGuarded(
    log::Message(LogLevel::ERR, "some error message", 0, dummy, true));
  logger2->logMessageGuarded(
    log::Message(LogLevel::WARN, "some warning message", 0, dummy, true));

  std::string content = file_utils::Slurp(_logfile1);

  EXPECT_NE(content.find("some error message"), std::string::npos);
  EXPECT_EQ(content.find("some warning message"), std::string::npos);

  content = file_utils::Slurp(_logfile2);
  EXPECT_EQ(content.find("some error message"), std::string::npos);
  EXPECT_NE(content.find("some warning message"), std::string::npos);

  log::AppenderFileFactory::reopenAll();

  fds = log::AppenderFileFactory::getAppenders();
  EXPECT_EQ(fds.size(), 2);

  EXPECT_TRUE(std::get<0>(fds[0]) > STDERR_FILENO);
  EXPECT_EQ(std::get<1>(fds[0]), _logfile1);
  EXPECT_EQ(std::get<2>(fds[0])->fd(), std::get<0>(fds[0]));

  logger1->logMessageGuarded(
    log::Message(LogLevel::ERR, "some other error message", 0, dummy, true));
  logger2->logMessageGuarded(
    log::Message(LogLevel::WARN, "some other warning message", 0, dummy, true));

  content = file_utils::Slurp(_logfile1);
  EXPECT_EQ(content.find("some error message"), std::string::npos);
  EXPECT_EQ(content.find("some warning message"), std::string::npos);
  EXPECT_NE(content.find("some other error message"), std::string::npos);

  content = file_utils::Slurp(_logfile2);
  EXPECT_EQ(content.find("some error message"), std::string::npos);
  EXPECT_EQ(content.find("some warning message"), std::string::npos);
  EXPECT_NE(content.find("some other warning message"), std::string::npos);

  log::AppenderFileFactory::closeAll();
}

TEST_F(LoggerTest, testTimeFormats) {
  using namespace std::chrono;

  {
    // server start time point
    sys_seconds start_tp;
    {
      std::istringstream in{"2016-12-11 13:59:55"};
      in >> date::parse("%F %T", start_tp);
    }

    // time point we are testing
    sys_seconds tp;
    {
      std::istringstream in{"2016-12-11 14:02:43"};
      in >> date::parse("%F %T", tp);
    }

    std::string out;

    out.clear();
    log_time_formats::WriteTime(out, log_time_formats::TimeFormat::Uptime, tp,
                                start_tp);
    EXPECT_EQ("168", out);

    ASSERT_TRUE(std::regex_match(out, std::regex("^[0-9]+$")));

    out.clear();
    log_time_formats::WriteTime(out, log_time_formats::TimeFormat::UptimeMillis,
                                tp, start_tp);
    EXPECT_EQ("168.000", out);
    ASSERT_TRUE(std::regex_match(out, std::regex("^[0-9]+\\.[0-9]{3}$")));

    out.clear();
    log_time_formats::WriteTime(out, log_time_formats::TimeFormat::UptimeMicros,
                                tp, start_tp);
    EXPECT_EQ("168.000000", out);
    ASSERT_TRUE(std::regex_match(out, std::regex("^[0-9]+\\.[0-9]{6}$")));

    out.clear();
    log_time_formats::WriteTime(
      out, log_time_formats::TimeFormat::UnixTimestamp, tp, start_tp);
    EXPECT_EQ("1481464963", out);

    out.clear();
    log_time_formats::WriteTime(
      out, log_time_formats::TimeFormat::UnixTimestampMillis, tp, start_tp);
    EXPECT_EQ("1481464963.000", out);

    out.clear();
    log_time_formats::WriteTime(
      out, log_time_formats::TimeFormat::UnixTimestampMicros, tp, start_tp);
    EXPECT_EQ("1481464963.000000", out);

    out.clear();
    log_time_formats::WriteTime(
      out, log_time_formats::TimeFormat::UTCDateString, tp, start_tp);
    EXPECT_EQ("2016-12-11T14:02:43Z", out);

    out.clear();
    log_time_formats::WriteTime(
      out, log_time_formats::TimeFormat::UTCDateStringMillis, tp, start_tp);
    EXPECT_EQ("2016-12-11T14:02:43.000Z", out);

    out.clear();
    log_time_formats::WriteTime(
      out, log_time_formats::TimeFormat::UTCDateStringMicros, tp, start_tp);
    EXPECT_EQ("2016-12-11T14:02:43.000000Z", out);

    out.clear();
    log_time_formats::WriteTime(
      out, log_time_formats::TimeFormat::LocalDateString, tp, start_tp);
    ASSERT_TRUE(std::regex_match(
      out,
      std::regex("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$")));
  }

  {
    // server start time point
    sys_time<milliseconds> start_tp;
    {
      std::istringstream in{"2020-12-02 11:57:02.701"};
      in >> date::parse("%F %T", start_tp);
    }

    // time point we are testing
    sys_time<milliseconds> tp;
    {
      std::istringstream in{"2020-12-02 11:57:26.004"};
      in >> date::parse("%F %T", tp);
    }

    std::string out;

    out.clear();
    log_time_formats::WriteTime(out, log_time_formats::TimeFormat::Uptime, tp,
                                start_tp);
    EXPECT_EQ("23", out);
    ASSERT_TRUE(std::regex_match(out, std::regex("^[0-9]+$")));

    out.clear();
    log_time_formats::WriteTime(out, log_time_formats::TimeFormat::UptimeMillis,
                                tp, start_tp);
    EXPECT_EQ("23.303", out);
    ASSERT_TRUE(std::regex_match(out, std::regex("^[0-9]+\\.[0-9]{3}$")));

    out.clear();
    log_time_formats::WriteTime(out, log_time_formats::TimeFormat::UptimeMicros,
                                tp, start_tp);
    EXPECT_EQ("23.303000", out);
    ASSERT_TRUE(std::regex_match(out, std::regex("^[0-9]+\\.[0-9]{6}$")));

    out.clear();
    log_time_formats::WriteTime(
      out, log_time_formats::TimeFormat::UnixTimestamp, tp, start_tp);
    EXPECT_EQ("1606910246", out);

    out.clear();
    log_time_formats::WriteTime(
      out, log_time_formats::TimeFormat::UnixTimestampMillis, tp, start_tp);
    EXPECT_EQ("1606910246.004", out);

    out.clear();
    log_time_formats::WriteTime(
      out, log_time_formats::TimeFormat::UnixTimestampMicros, tp, start_tp);
    EXPECT_EQ("1606910246.004000", out);

    out.clear();
    log_time_formats::WriteTime(
      out, log_time_formats::TimeFormat::UTCDateString, tp, start_tp);
    EXPECT_EQ("2020-12-02T11:57:26Z", out);

    out.clear();
    log_time_formats::WriteTime(
      out, log_time_formats::TimeFormat::UTCDateStringMillis, tp, start_tp);
    EXPECT_EQ("2020-12-02T11:57:26.004Z", out);

    out.clear();
    log_time_formats::WriteTime(
      out, log_time_formats::TimeFormat::UTCDateStringMicros, tp, start_tp);
    EXPECT_EQ("2020-12-02T11:57:26.004000Z", out);

    out.clear();
    log_time_formats::WriteTime(
      out, log_time_formats::TimeFormat::LocalDateString, tp, start_tp);
    ASSERT_TRUE(std::regex_match(
      out,
      std::regex("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$")));
  }
}

TEST_F(LoggerTest, test_many_loggers_same_file) {
  {
    auto logger1 = log::AppenderFileFactory::getFileAppender(_logfile1);
    auto logger2 = log::AppenderFileFactory::getFileAppender(_logfile1);

    // Both loggers need to have the same pointer
    EXPECT_EQ(logger1.get(), logger2.get());
  }
  // Now test concurrent usage
  constexpr size_t kN = 4;
  std::vector<std::thread> threads;
  threads.reserve(kN);

  LogTopic dummy{"dummy"};
  absl::Cleanup guard = [&]() noexcept {
    for (auto& t : threads) {
      t.join();
    }
  };

  Synchronizer s;
  constexpr size_t kIterations = 100;

  std::vector<uint64_t> expected_values;
  for (size_t i = 0; i < kN; ++i) {
    expected_values.emplace_back(0);
    threads.emplace_back([logfile1 = this->_logfile1, &s, i, &dummy]() {
      auto logger = log::AppenderFileFactory::getFileAppender(logfile1);

      s.WaitForStart();

      for (size_t j = 0; j < kIterations; ++j) {
        logger->logMessageGuarded(log::Message(
          LogLevel::ERR, absl::StrCat("Thread ", i, " Message ", j, "\n"), 0,
          dummy, true));
      }
    });
  }

  s.Start();

  std::move(guard).Invoke();

  // All Messages are written to the same file, let's check if they are in
  // correct ordering
  std::string content = file_utils::Slurp(_logfile1);
  auto stream = std::istringstream{content};

  // Let us read file from top to bottom.
  // In each line we should have exactly one message from one thread
  // For every thread the messages have to be strictly ordered
  // The messages from different threads can be interleaved
  // Every thread needs to have exactly iterations messages
  for (std::string line; std::getline(stream, line, '\n');) {
    std::vector<std::string> splits;
    auto stream_line = std::istringstream{line};
    for (std::string segment; std::getline(stream_line, segment, ' ');) {
      splits.push_back(segment);
    }
    auto t_id = string_utils::Uint64(splits.at(1));
    auto message_id = string_utils::Uint64(splits.at(3));
    EXPECT_EQ(expected_values.at(t_id), message_id);
    expected_values[t_id]++;
  }

  for (size_t i = 0; i < kN; ++i) {
    EXPECT_EQ(expected_values.at(i), kIterations)
      << "Thread " << i << " did not have enough messages";
  }
}
