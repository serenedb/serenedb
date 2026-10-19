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

#include <gtest/gtest.h>

#ifdef __linux__
#include <dirent.h>
#include <sched.h>
#endif

#include <algorithm>
#include <atomic>
#include <chrono>
#include <duckdb.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parallel/task.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/parallel/task_scheduler_queue.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <fstream>
#include <functional>
#include <limits>
#include <map>
#include <string>
#include <thread>
#include <vector>

namespace {

constexpr int64_t kRows = 10'000'000;

std::atomic<size_t> gRunning{0};
std::atomic<size_t> gPeak{0};

void RecordThread(duckdb::DataChunk&, duckdb::ExpressionState&,
                  duckdb::Vector& result) {
  const auto running = ++gRunning;
  for (auto peak = gPeak.load();
       running > peak && !gPeak.compare_exchange_weak(peak, running);) {
  }
  const auto until =
    std::chrono::steady_clock::now() + std::chrono::microseconds(20);
  while (std::chrono::steady_clock::now() < until) {
  }
  --gRunning;
  result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
  duckdb::ConstantVector::GetData<bool>(result)[0] = true;
}

size_t PeakThreads(duckdb::Connection& con) {
  gPeak = 0;
  auto result = con.Query("SELECT count(*) FROM s.t WHERE record_thread(i)");
  EXPECT_FALSE(result->HasError()) << result->GetError();
  EXPECT_EQ(result->Collection().GetValue(0, 0), duckdb::Value::BIGINT(kRows));
  return gPeak;
}

void Exec(duckdb::Connection& con, const std::string& sql) {
  auto result = con.Query(sql);
  ASSERT_FALSE(result->HasError()) << sql << ": " << result->GetError();
}

duckdb::Value CurrentThreads(duckdb::Connection& con) {
  return con.Query("SELECT current_setting('threads')")
    ->Collection()
    .GetValue(0, 0);
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
  EXPECT_EQ(PeakThreads(con), 1U);
  EXPECT_GT(PeakThreads(other), 1U);

  Exec(con, "SET SESSION threads = 3");
  EXPECT_EQ(duckdb::TaskScheduler::QueryThreads(*con.context), 3U);
  EXPECT_LE(PeakThreads(con), 3U);

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
  EXPECT_LE(PeakThreads(other), 4U);

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

#ifdef __linux__
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
#endif

TEST(DuckDBThreads, RelaunchFromPinnedWorkerKeepsWorkersApart) {
#ifdef __linux__
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
  scheduler.ScheduleTask(*producer, duckdb::make_shared_ptr<FunctionTask>([&] {
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
#else
  GTEST_SKIP() << "Requires Linux CPU affinity and /proc";
#endif
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
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
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

struct SpinState {
  std::atomic<bool> stop{false};
  std::atomic<int64_t> chunks{0};
  std::atomic<int64_t> yields{0};
  std::atomic<int64_t> running{0};
  std::atomic<int64_t> finished{0};
};

class SpinTask final : public duckdb::Task {
 public:
  explicit SpinTask(SpinState& state) : _state{state} {}

  duckdb::TaskExecutionResult Execute(duckdb::TaskExecutionMode mode) final {
    ++_state.running;
    while (!_state.stop) {
      const auto until =
        std::chrono::steady_clock::now() + std::chrono::microseconds(20);
      while (std::chrono::steady_clock::now() < until) {
      }
      ++_state.chunks;
      if (duckdb::TaskScheduler::ShouldYield(mode)) {
        ++_state.yields;
        --_state.running;
        return duckdb::TaskExecutionResult::TASK_NOT_FINISHED;
      }
    }
    --_state.running;
    ++_state.finished;
    return duckdb::TaskExecutionResult::TASK_FINISHED;
  }

 private:
  SpinState& _state;
};

struct SpinQuery {
  SpinQuery(duckdb::TaskScheduler& scheduler, size_t tasks)
    : producer{scheduler.CreateProducer(
        scheduler.CreateAccount(duckdb::SchedulerClass::FOREGROUND))},
      count{tasks} {
    duckdb::vector<duckdb::shared_ptr<duckdb::Task>> list;
    for (size_t i = 0; i < tasks; ++i) {
      list.push_back(duckdb::make_shared_ptr<SpinTask>(state));
    }
    scheduler.ScheduleTasks(*producer, list);
  }

  ~SpinQuery() {
    state.stop = true;
    const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (state.finished < static_cast<int64_t>(count) &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(state.finished.load(), static_cast<int64_t>(count));
  }

  SpinState state;
  duckdb::unique_ptr<duckdb::ProducerToken> producer;
  size_t count;
};

duckdb::unique_ptr<duckdb::DuckDB> SchedulerDB(size_t threads) {
  duckdb::DBConfig config;
  config.SetOptionByName("threads", duckdb::Value::UBIGINT(threads));
  config.SetOptionByName("external_threads", duckdb::Value::UBIGINT(0));
  return duckdb::make_uniq<duckdb::DuckDB>(nullptr, &config);
}

bool WaitFor(const std::function<bool()>& condition) {
  const auto deadline =
    std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (!condition()) {
    if (std::chrono::steady_clock::now() >= deadline) {
      return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return true;
}

TEST(DuckDBScheduler, QueryAloneIsNotInterrupted) {
  auto db = SchedulerDB(4);
  auto& scheduler = duckdb::TaskScheduler::GetScheduler(*db->instance);
  SpinQuery query{scheduler, 4};
  ASSERT_TRUE(WaitFor([&] { return query.state.running == 4; }));
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  EXPECT_EQ(query.state.yields.load(), 0);
}

TEST(DuckDBScheduler, LightTaskDoesNotWaitForHeavyTasks) {
  auto db = SchedulerDB(4);
  auto& scheduler = duckdb::TaskScheduler::GetScheduler(*db->instance);
  SpinQuery heavy{scheduler, 4};
  ASSERT_TRUE(WaitFor([&] { return heavy.state.running == 4; }));
  for (int i = 0; i < 20; ++i) {
    auto producer = scheduler.CreateProducer(
      scheduler.CreateAccount(duckdb::SchedulerClass::FOREGROUND));
    std::atomic<bool> ran{false};
    const auto start = std::chrono::steady_clock::now();
    scheduler.ScheduleTask(
      *producer, duckdb::make_shared_ptr<FunctionTask>([&] { ran = true; }));
    ASSERT_TRUE(WaitFor([&] { return ran.load(); }));
    EXPECT_LT(std::chrono::steady_clock::now() - start,
              std::chrono::milliseconds(100));
  }
}

TEST(DuckDBScheduler, ArrivalTakenWithoutSelectDoesNotHideNextArrival) {
  duckdb::TaskSchedulerQueue queue;
  auto first = duckdb::make_shared_ptr<duckdb::SchedulerAccount>(
    queue, duckdb::SchedulerClass::FOREGROUND);
  queue.Publish(*first);
  ASSERT_TRUE(queue.HasArrivals());
  ASSERT_TRUE(queue.TakeArrival());
  EXPECT_FALSE(queue.HasArrivals());
  EXPECT_FALSE(queue.TakeArrival());
  auto second = duckdb::make_shared_ptr<duckdb::SchedulerAccount>(
    queue, duckdb::SchedulerClass::FOREGROUND);
  queue.Publish(*second);
  EXPECT_TRUE(queue.HasArrivals());
  EXPECT_TRUE(queue.TakeArrival());
}

TEST(DuckDBScheduler, AccountGivenAWorkerIsNoLongerAnArrival) {
  duckdb::TaskSchedulerQueue queue;
  auto account = duckdb::make_shared_ptr<duckdb::SchedulerAccount>(
    queue, duckdb::SchedulerClass::FOREGROUND);
  queue.Publish(*account);
  ASSERT_TRUE(queue.HasArrivals());
  queue.Attach(*account);
  EXPECT_FALSE(queue.HasArrivals());
  EXPECT_FALSE(queue.TakeArrival());
  queue.Detach(*account);
}

TEST(DuckDBScheduler, TwoHeavyQueriesShareWorkers) {
  auto db = SchedulerDB(4);
  auto& scheduler = duckdb::TaskScheduler::GetScheduler(*db->instance);
  SpinQuery first{scheduler, 4};
  ASSERT_TRUE(WaitFor([&] { return first.state.running == 4; }));
  SpinQuery second{scheduler, 4};
  ASSERT_TRUE(WaitFor([&] { return second.state.chunks > 0; }));
  const auto first_start = first.state.chunks.load();
  const auto second_start = second.state.chunks.load();
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  const auto first_done = first.state.chunks - first_start;
  const auto second_done = second.state.chunks - second_start;
  EXPECT_GT(first_done * 2, second_done);
  EXPECT_GT(second_done * 2, first_done);
}

TEST(DuckDBScheduler, MoreQueriesThanWorkersAllProgress) {
  auto db = SchedulerDB(2);
  auto& scheduler = duckdb::TaskScheduler::GetScheduler(*db->instance);
  std::vector<duckdb::unique_ptr<SpinQuery>> queries;
  for (int i = 0; i < 6; ++i) {
    queries.push_back(duckdb::make_uniq<SpinQuery>(scheduler, 1));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  int64_t least = std::numeric_limits<int64_t>::max();
  int64_t most = 0;
  for (auto& query : queries) {
    least = std::min<int64_t>(least, query->state.chunks);
    most = std::max<int64_t>(most, query->state.chunks);
  }
  EXPECT_GT(least * 3, most);
}

}  // namespace
