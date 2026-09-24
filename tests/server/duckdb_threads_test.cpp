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

#include <dirent.h>
#include <gtest/gtest.h>
#include <sched.h>

#include <atomic>
#include <chrono>
#include <duckdb.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parallel/task.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <fstream>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>

namespace {

constexpr int64_t kRows = 10'000'000;

std::mutex gSeenLock;
std::set<std::thread::id> gSeen;

void RecordThread(duckdb::DataChunk&, duckdb::ExpressionState&,
                  duckdb::Vector& result) {
  {
    std::lock_guard guard{gSeenLock};
    gSeen.insert(std::this_thread::get_id());
  }
  result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
  duckdb::ConstantVector::GetData<bool>(result)[0] = true;
}

size_t ThreadsSeen(duckdb::Connection& con) {
  {
    std::lock_guard guard{gSeenLock};
    gSeen.clear();
  }
  auto result = con.Query("SELECT count(*) FROM s.t WHERE record_thread(i)");
  EXPECT_FALSE(result->HasError()) << result->GetError();
  EXPECT_EQ(result->GetValue(0, 0), duckdb::Value::BIGINT(kRows));
  std::lock_guard guard{gSeenLock};
  return gSeen.size();
}

void Exec(duckdb::Connection& con, const std::string& sql) {
  auto result = con.Query(sql);
  ASSERT_FALSE(result->HasError()) << sql << ": " << result->GetError();
}

duckdb::Value CurrentThreads(duckdb::Connection& con) {
  return con.Query("SELECT current_setting('threads')")->GetValue(0, 0);
}

TEST(DuckDBThreads, SessionValueCapsItsQueries) {
  duckdb::DBConfig config;
  config.options.maximum_threads = 8;
  duckdb::DuckDB db{nullptr, &config};
  duckdb::Connection con{db};
  duckdb::Connection other{db};
  duckdb::ScalarFunction record{"record_thread",
                                {duckdb::LogicalType::BIGINT},
                                duckdb::LogicalType::BOOLEAN,
                                RecordThread};
  record.SetVolatile();
  duckdb::CreateScalarFunctionInfo info{record};
  con.context->RegisterFunction(info);
  Exec(con, "CREATE SCHEMA s");
  Exec(con, "CREATE TABLE s.t AS SELECT range AS i FROM range(" +
              std::to_string(kRows) + ")");

  Exec(con, "SET threads = 1");
  EXPECT_EQ(CurrentThreads(con), duckdb::Value::BIGINT(1));
  EXPECT_EQ(CurrentThreads(other), duckdb::Value::BIGINT(8));
  EXPECT_EQ(db.NumberOfThreads(), 8U);
  EXPECT_EQ(duckdb::TaskScheduler::QueryThreads(*con.context), 1U);
  EXPECT_EQ(duckdb::TaskScheduler::QueryThreads(*other.context), 8U);
  EXPECT_EQ(ThreadsSeen(con), 1U);
  EXPECT_GT(ThreadsSeen(other), 1U);

  Exec(con, "SET SESSION threads = 3");
  EXPECT_EQ(duckdb::TaskScheduler::QueryThreads(*con.context), 3U);
  EXPECT_LE(ThreadsSeen(con), 3U);

  Exec(con, "SET threads = 100");
  EXPECT_EQ(CurrentThreads(con), duckdb::Value::BIGINT(100));
  EXPECT_EQ(duckdb::TaskScheduler::QueryThreads(*con.context), 8U);

  EXPECT_TRUE(con.Query("SET threads = 0")->HasError());
  EXPECT_EQ(CurrentThreads(con), duckdb::Value::BIGINT(100));

  Exec(con, "SET GLOBAL threads = 4");
  EXPECT_EQ(db.NumberOfThreads(), 4U);
  EXPECT_EQ(CurrentThreads(con), duckdb::Value::BIGINT(100));
  EXPECT_EQ(CurrentThreads(other), duckdb::Value::BIGINT(4));
  EXPECT_EQ(duckdb::TaskScheduler::QueryThreads(*con.context), 4U);
  EXPECT_LE(ThreadsSeen(other), 4U);

  Exec(con, "RESET threads");
  EXPECT_EQ(CurrentThreads(con), duckdb::Value::BIGINT(4));
  EXPECT_EQ(db.NumberOfThreads(), 4U);
}

class FunctionTask final : public duckdb::Task {
 public:
  explicit FunctionTask(std::function<void()> fn) : _fn{std::move(fn)} {}

  duckdb::TaskExecutionResult Execute(duckdb::TaskExecutionMode) final {
    _fn();
    return duckdb::TaskExecutionResult::TASK_FINISHED;
  }

 private:
  std::function<void()> _fn;
};

struct ThreadInfo {
  std::string name;
  std::string cpus;
};

std::map<std::string, ThreadInfo> Threads() {
  std::map<std::string, ThreadInfo> threads;
  auto* dir = opendir("/proc/self/task");
  if (!dir) {
    return threads;
  }
  while (auto* entry = readdir(dir)) {
    if (entry->d_name[0] == '.') {
      continue;
    }
    const auto task = std::string{"/proc/self/task/"} + entry->d_name;
    ThreadInfo info;
    std::ifstream comm{task + "/comm"};
    std::getline(comm, info.name);
    std::ifstream status{task + "/status"};
    std::string line;
    while (std::getline(status, line)) {
      if (line.starts_with("Cpus_allowed_list:")) {
        info.cpus = line.substr(line.find(':') + 1);
        info.cpus.erase(0, info.cpus.find_first_not_of(" \t"));
        break;
      }
    }
    threads.emplace(entry->d_name, std::move(info));
  }
  closedir(dir);
  return threads;
}

TEST(DuckDBThreads, RelaunchFromPinnedWorkerKeepsWorkersApart) {
  cpu_set_t mask;
  if (sched_getaffinity(0, sizeof(mask), &mask) != 0 || CPU_COUNT(&mask) < 4) {
    GTEST_SKIP();
  }
  const auto before = Threads();
  std::string process_name;
  std::getline(std::ifstream{"/proc/self/comm"}, process_name);
  duckdb::DBConfig config;
  config.SetOptionByName("threads", duckdb::Value::UBIGINT(4));
  config.SetOptionByName("external_threads", duckdb::Value::UBIGINT(0));
  config.SetOptionByName("pin_threads", duckdb::Value{"on"});
  duckdb::DuckDB db{nullptr, &config};
  auto& scheduler = duckdb::TaskScheduler::GetScheduler(*db.instance);
  auto producer = scheduler.CreateProducer();
  std::atomic<bool> done{false};
  scheduler.ScheduleTask(*producer,
                         duckdb::make_shared_ptr<FunctionTask>([&] {
                           scheduler.SetThreads(3, 0);
                           scheduler.RelaunchThreads();
                           done = true;
                         }));
  while (!done) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  EXPECT_EQ(scheduler.NumberOfThreads(), 3U);
  std::map<std::string, size_t> per_cpu;
  size_t pinned = 0;
  for (const auto& [tid, info] : Threads()) {
    if (before.contains(tid) || info.name != process_name ||
        info.cpus.find_first_of(",-") != std::string::npos) {
      continue;
    }
    ++pinned;
    ++per_cpu[info.cpus];
  }
  EXPECT_EQ(pinned, 3U);
  for (const auto& [cpu, count] : per_cpu) {
    EXPECT_EQ(count, 1U) << "cpu " << cpu;
  }
}

TEST(DuckDBThreads, RelaunchWhileAnotherWorkerRelaunchesDoesNotDeadlock) {
  duckdb::DBConfig config;
  config.SetOptionByName("threads", duckdb::Value::UBIGINT(4));
  config.SetOptionByName("external_threads", duckdb::Value::UBIGINT(0));
  auto* db = new duckdb::DuckDB{nullptr, &config};
  auto& scheduler = duckdb::TaskScheduler::GetScheduler(*db->instance);
  auto producer = scheduler.CreateProducer();
  std::atomic<bool> relaunching{false};
  std::atomic<int> done{0};
  scheduler.ScheduleTask(*producer, duckdb::make_shared_ptr<FunctionTask>([&] {
                           while (!relaunching) {
                             std::this_thread::yield();
                           }
                           std::this_thread::sleep_for(
                             std::chrono::milliseconds(50));
                           scheduler.RelaunchThreads();
                           ++done;
                         }));
  scheduler.ScheduleTask(*producer, duckdb::make_shared_ptr<FunctionTask>([&] {
                           scheduler.SetThreads(3, 0);
                           relaunching = true;
                           scheduler.RelaunchThreads();
                           ++done;
                         }));
  const auto deadline =
    std::chrono::steady_clock::now() + std::chrono::seconds(30);
  while (done < 2 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_EQ(done.load(), 2);
  EXPECT_EQ(scheduler.NumberOfThreads(), 3U);
  delete db;
}

}  // namespace
