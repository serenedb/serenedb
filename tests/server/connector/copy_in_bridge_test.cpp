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

// Stress test for the COPY-FROM-STDIN rendezvous (pg::CopyInBridge): a feeder
// coroutine publishes borrowed CopyData views and a blocking DuckDB worker
// drains them in strict lock-step. The bug guarded here was a SIGSEGV under
// load: the worker's wait gate keyed off the live `_len` instead of a per-frame
// latch, so the feeder could publish the next frame before the worker
// re-checked, leaving a `_data_ready` Set unconsumed; the following
// Publish/Finish then double-Set the same OneShotEvent and corrupted it.
//
// The fix is the shared `_armed` latch consumed once per frame by both worker
// paths (Read for csv/text, Window/Consume for binary). This test drives the
// feeder on a real yaclib executor (matching production: feeder on the io
// thread) and the worker on a separate blocking std::thread, with the worker
// doing variable work between frames so the feeder races ahead -- exactly the
// timing that triggered the crash. It asserts byte-for-byte ordered delivery to
// clean EOF over both worker paths, many frames, many repetitions.
//
// A return to the live-`_len` gate would, under this racing schedule, leave a
// stale `_data_ready` Set that the next Publish/Finish double-Sets, hanging or
// crashing the run; under ThreadSanitizer it surfaces deterministically as a
// data race on `_len`/`_head`. Build with -DENABLE_THREAD_SANITIZER=ON (or
// WITH_TSAN=ON) to get the deterministic verdict; the racing schedule below
// keeps it meaningful under the normal build too.

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <yaclib/async/future.hpp>
#include <yaclib/coro/coro.hpp>
#include <yaclib/coro/future.hpp>
#include <yaclib/coro/on.hpp>
#include <yaclib/runtime/fair_thread_pool.hpp>

#include "pg/copy_in_bridge.h"

namespace {

using sdb::pg::CopyInBridge;

// A deterministic byte stream: byte k of the whole concatenated input is
// (k & 0xFF). Both sides agree on this, so the worker can verify every byte it
// receives against its running global offset -- catching any reorder,
// duplication, or drop, not just a wrong total length.
constexpr char PatternByte(uint64_t global_offset) {
  return static_cast<char>(global_offset & 0xFFu);
}

// Frame sizes deliberately mix 1-byte frames (the tightest race window) with
// large multi-kilobyte frames, cycling through a small irregular table so the
// worker's fixed read/window chunking straddles frame boundaries every which
// way.
const std::vector<size_t>& FrameSizeTable() {
  static const std::vector<size_t> sizes = {
    1, 1, 2, 3, 7, 1, 64, 1, 255, 1024, 1, 13, 511, 4096, 1, 2, 8192, 17};
  return sizes;
}

// Build one frame's payload filled with the global pattern, starting at
// `global_offset`.
std::string MakeFrame(uint64_t global_offset, size_t len) {
  std::string s;
  s.resize(len);
  for (size_t i = 0; i < len; ++i) {
    s[i] = PatternByte(global_offset + i);
  }
  return s;
}

// A tiny pseudo-random "do some work" spin, so the worker falls behind the
// feeder by a variable amount between frames. This is what lets the feeder race
// ahead and arm the double-Set window in the buggy version.
inline void VariableWork(uint64_t seed) {
  volatile uint64_t x = seed * 2654435761u + 1;
  const int spins = static_cast<int>(seed % 37u);
  for (int i = 0; i < spins; ++i) {
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
  }
  (void)x;
}

// Run the feeder loop as a coroutine on `io`, mirroring
// PgWireSession::RunCopyInFeeder exactly: Publish -> co_await Drained(io) ->
// ResetDrained per frame, then Finish(). `frames` owns the payload bytes for
// the whole run (the bridge only borrows views into them, like the recv
// buffer), so it must outlive the join.
yaclib::Future<> FeederCoro(CopyInBridge& bridge, yaclib::IExecutor& io,
                            const std::vector<std::string>& frames) {
  co_await yaclib::On(io);
  for (const auto& f : frames) {
    bridge.Publish(f.data(), f.size());
    co_await bridge.Drained(io);
    bridge.ResetDrained();
  }
  bridge.Finish();
  co_return {};
}

// Generate `frame_count` frames whose sizes cycle through the table; returns
// the frames plus the full expected concatenation.
struct Input {
  std::vector<std::string> frames;
  std::string expected;
};

Input MakeInput(size_t frame_count) {
  Input in;
  in.frames.reserve(frame_count);
  const auto& table = FrameSizeTable();
  uint64_t offset = 0;
  for (size_t i = 0; i < frame_count; ++i) {
    const size_t len = table[i % table.size()];
    in.frames.push_back(MakeFrame(offset, len));
    in.expected.append(in.frames.back());
    offset += len;
  }
  return in;
}

// Worker path A: csv/text. Loop Read(buf, n) with a varying n, accumulating,
// until Read returns 0 (clean EOF after Finish). Verifies every received byte
// against the global pattern as it goes.
void WorkerReadPath(CopyInBridge& bridge, uint64_t& bytes_seen,
                    bool& pattern_ok) {
  bytes_seen = 0;
  pattern_ok = true;
  // Varying read sizes, including reads much smaller and much larger than the
  // frames, so a single Read may span several frames or take a frame in pieces.
  const int read_sizes[] = {1, 3, 64, 1, 4096, 7, 250, 1, 8192, 2};
  size_t which = 0;
  std::vector<char> buf(8192);
  for (;;) {
    const int n = read_sizes[which++ % std::size(read_sizes)];
    const int64_t got = bridge.Read(buf.data(), n);
    if (got == 0) {
      break;  // clean EOF
    }
    for (int64_t i = 0; i < got; ++i) {
      if (buf[static_cast<size_t>(i)] != PatternByte(bytes_seen + i)) {
        pattern_ok = false;
      }
    }
    bytes_seen += static_cast<uint64_t>(got);
    VariableWork(bytes_seen + which);  // fall behind the feeder
  }
}

// Worker path B: binary. Loop Window()/Consume(), consuming a varying slice of
// each window (sometimes the whole window, sometimes a few bytes at a time so a
// frame is drained over several Window/Consume turns), until an empty window
// (EOF). Verifies the pattern as it consumes.
void WorkerWindowPath(CopyInBridge& bridge, uint64_t& bytes_seen,
                      bool& pattern_ok) {
  bytes_seen = 0;
  pattern_ok = true;
  const size_t take_sizes[] = {1, 2, 5, 100, 1, 4096, 3};
  size_t which = 0;
  for (;;) {
    const std::span<const char> w = bridge.Window();
    if (w.empty()) {
      break;  // clean EOF
    }
    // Consume the window in one or more bites; never Consume(0).
    size_t pos = 0;
    while (pos < w.size()) {
      size_t take = take_sizes[which++ % std::size(take_sizes)];
      take = std::min(take, w.size() - pos);
      if (take == 0) {
        take = w.size() - pos;
      }
      for (size_t i = 0; i < take; ++i) {
        if (w[pos + i] != PatternByte(bytes_seen + i)) {
          pattern_ok = false;
        }
      }
      bytes_seen += take;
      pos += take;
      bridge.Consume(take);
      VariableWork(bytes_seen + which);  // fall behind the feeder
    }
  }
}

enum class Path { Read, Window };

// One full feeder/worker round: spin up the feeder coroutine on a single-thread
// FairThreadPool (the "io thread") and run the worker on a separate blocking
// std::thread (the "duck worker"), then join both and assert exact, ordered,
// clean-EOF delivery.
void RunOne(Path path, size_t frame_count) {
  const Input in = MakeInput(frame_count);
  CopyInBridge bridge;

  yaclib::FairThreadPool io{1};

  uint64_t bytes_seen = 0;
  bool pattern_ok = false;
  std::thread worker([&] {
    if (path == Path::Read) {
      WorkerReadPath(bridge, bytes_seen, pattern_ok);
    } else {
      WorkerWindowPath(bridge, bytes_seen, pattern_ok);
    }
  });

  auto feeder = FeederCoro(bridge, io, in.frames);
  std::ignore = std::move(feeder).Get();  // join the feeder coroutine
  worker.join();

  io.Stop();
  io.Wait();

  EXPECT_TRUE(pattern_ok) << "byte pattern mismatch (reorder/dup/drop) on "
                          << (path == Path::Read ? "Read" : "Window")
                          << " path, frames=" << frame_count;
  EXPECT_EQ(bytes_seen, in.expected.size())
    << "wrong total bytes on " << (path == Path::Read ? "Read" : "Window")
    << " path, frames=" << frame_count;
}

constexpr int kRepeats = 100;

TEST(CopyInBridgeStress, ReadPathManyFramesRepeated) {
  for (int r = 0; r < kRepeats; ++r) {
    RunOne(Path::Read, 2000);
  }
}

TEST(CopyInBridgeStress, WindowPathManyFramesRepeated) {
  for (int r = 0; r < kRepeats; ++r) {
    RunOne(Path::Window, 2000);
  }
}

// All-1-byte frames: the tightest possible race window (every frame drains
// immediately, so the feeder is always one Publish ahead). This is the schedule
// most likely to expose a live-`_len` gate.
TEST(CopyInBridgeStress, SingleByteFramesReadPath) {
  for (int r = 0; r < kRepeats; ++r) {
    CopyInBridge bridge;
    yaclib::FairThreadPool io{1};
    Input in;
    in.frames.reserve(4000);
    for (size_t i = 0; i < 4000; ++i) {
      in.frames.push_back(MakeFrame(i, 1));
      in.expected.push_back(PatternByte(i));
    }

    uint64_t bytes_seen = 0;
    bool pattern_ok = false;
    std::thread worker([&] { WorkerReadPath(bridge, bytes_seen, pattern_ok); });
    auto feeder = FeederCoro(bridge, io, in.frames);
    std::ignore = std::move(feeder).Get();
    worker.join();
    io.Stop();
    io.Wait();

    EXPECT_TRUE(pattern_ok);
    EXPECT_EQ(bytes_seen, in.expected.size());
  }
}

TEST(CopyInBridgeStress, SingleByteFramesWindowPath) {
  for (int r = 0; r < kRepeats; ++r) {
    CopyInBridge bridge;
    yaclib::FairThreadPool io{1};
    Input in;
    in.frames.reserve(4000);
    for (size_t i = 0; i < 4000; ++i) {
      in.frames.push_back(MakeFrame(i, 1));
      in.expected.push_back(PatternByte(i));
    }

    uint64_t bytes_seen = 0;
    bool pattern_ok = false;
    std::thread worker(
      [&] { WorkerWindowPath(bridge, bytes_seen, pattern_ok); });
    auto feeder = FeederCoro(bridge, io, in.frames);
    std::ignore = std::move(feeder).Get();
    worker.join();
    io.Stop();
    io.Wait();

    EXPECT_TRUE(pattern_ok);
    EXPECT_EQ(bytes_seen, in.expected.size());
  }
}

// Cross-frame Fill semantics on the Read path: the worker asks for n much
// larger than any single frame, so each Read stitches several frames into one
// contiguous output -- the binary parser's straddling-value case. Verifies the
// stitched bytes against the global pattern.
TEST(CopyInBridgeStress, CrossFrameLargeReads) {
  for (int r = 0; r < kRepeats; ++r) {
    const Input in = MakeInput(1500);
    CopyInBridge bridge;
    yaclib::FairThreadPool io{1};

    uint64_t bytes_seen = 0;
    bool pattern_ok = true;
    std::thread worker([&] {
      std::vector<char> buf(64u * 1024);
      for (;;) {
        // Always request a big read so it spans many frames.
        const int64_t got = bridge.Read(buf.data(), 64 * 1024);
        if (got == 0) {
          break;
        }
        for (int64_t i = 0; i < got; ++i) {
          if (buf[static_cast<size_t>(i)] != PatternByte(bytes_seen + i)) {
            pattern_ok = false;
          }
        }
        bytes_seen += static_cast<uint64_t>(got);
        VariableWork(bytes_seen);
      }
    });
    auto feeder = FeederCoro(bridge, io, in.frames);
    std::ignore = std::move(feeder).Get();
    worker.join();
    io.Stop();
    io.Wait();

    EXPECT_TRUE(pattern_ok);
    EXPECT_EQ(bytes_seen, in.expected.size());
  }
}

// Teardown via Fail: the feeder publishes some frames, then Fails mid-stream
// (CopyFail / lost connection). The worker's next Read must rethrow, and the
// run must end cleanly (no crash/hang). Mirrors the session's error funnel.
TEST(CopyInBridgeTeardown, FailMidStreamRethrows) {
  for (int r = 0; r < 50; ++r) {
    const Input in = MakeInput(200);
    CopyInBridge bridge;
    yaclib::FairThreadPool io{1};

    std::atomic<bool> caught{false};
    std::atomic<uint64_t> drained{0};
    std::thread worker([&] {
      std::vector<char> buf(4096);
      try {
        for (;;) {
          const int64_t got = bridge.Read(buf.data(), 333);
          if (got == 0) {
            break;
          }
          drained.fetch_add(static_cast<uint64_t>(got));
          VariableWork(drained.load());
        }
      } catch (const std::exception&) {
        caught.store(true);
      } catch (...) {
        caught.store(true);
      }
    });

    // Feeder: publish ~half the frames, then Fail.
    auto feeder = [&]() -> yaclib::Future<> {
      co_await yaclib::On(io);
      const size_t fail_at = in.frames.size() / 2;
      for (size_t i = 0; i < in.frames.size(); ++i) {
        if (i == fail_at) {
          bridge.Fail(std::make_exception_ptr(
            std::runtime_error("COPY from stdin failed")));
          co_return {};
        }
        bridge.Publish(in.frames[i].data(), in.frames[i].size());
        co_await bridge.Drained(io);
        bridge.ResetDrained();
      }
      co_return {};
    }();
    std::ignore = std::move(feeder).Get();
    worker.join();
    io.Stop();
    io.Wait();

    EXPECT_TRUE(caught.load()) << "worker did not rethrow the feeder's Fail";
  }
}

// Teardown via Abort: the worker errors mid-stream (bad row / constraint) and
// stops reading; the bridge is Aborted so the feeder, seeing Aborted(), stops
// publishing and finishes. No crash/hang.
TEST(CopyInBridgeTeardown, AbortMidStreamStopsFeeder) {
  for (int r = 0; r < 50; ++r) {
    const Input in = MakeInput(200);
    CopyInBridge bridge;
    yaclib::FairThreadPool io{1};

    const uint64_t abort_after = 5000;  // bytes
    std::thread worker([&] {
      std::vector<char> buf(1024);
      uint64_t seen = 0;
      while (seen < abort_after) {
        const int64_t got = bridge.Read(buf.data(), 128);
        if (got == 0) {
          break;
        }
        seen += static_cast<uint64_t>(got);
      }
      // Worker errored: stop reading and tell the feeder to stop.
      bridge.Abort();
    });

    // Feeder mirrors RunCopyInFeeder's Aborted() guard: once aborted, stop
    // publishing (just drain frames without handing them out) and finish.
    auto feeder = [&]() -> yaclib::Future<> {
      co_await yaclib::On(io);
      for (const auto& f : in.frames) {
        if (!bridge.Aborted()) {
          bridge.Publish(f.data(), f.size());
          co_await bridge.Drained(io);
          bridge.ResetDrained();
        }
      }
      co_return {};
    }();
    std::ignore = std::move(feeder).Get();
    worker.join();
    io.Stop();
    io.Wait();

    EXPECT_TRUE(bridge.Aborted());
  }
}

// Regression for the early-worker-error double-Set: when the COPY worker errors
// WITHOUT consuming the feeder's last Publish (e.g. COPY FROM STDIN FORMAT
// parquet -- parquet can't read a pipe and fails during the sniff), the session
// Aborts the bridge and the feeder then reaches CopyDone -> Finish. Finish (and
// Fail/Publish) must be no-ops after Abort: a second _data_ready Set with no
// intervening worker Reset is a OneShotEvent double-Set -> SIGSEGV.
TEST(CopyInBridgeTeardown, SignalsAfterAbortAreNoOps) {
  CopyInBridge bridge;
  const char frame[] = "unconsumed";
  bridge.Publish(frame, sizeof(frame));  // Sets _data_ready; worker never reads
  bridge.Abort();                        // worker errored before consuming
  // Each of these previously double-Set the still-armed _data_ready and
  // crashed.
  bridge.Finish();
  bridge.Fail(std::make_exception_ptr(std::runtime_error("worker error")));
  bridge.Publish(frame, sizeof(frame));
  EXPECT_TRUE(bridge.Aborted());
}

}  // namespace
