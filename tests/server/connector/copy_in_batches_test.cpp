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

#include <absl/base/internal/endian.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <vector>
#include <yaclib/async/future.hpp>
#include <yaclib/coro/coro.hpp>
#include <yaclib/coro/future.hpp>
#include <yaclib/coro/on.hpp>
#include <yaclib/runtime/fair_thread_pool.hpp>

#include "pg/copy_batch_builder.h"
#include "pg/copy_in_batches.h"

namespace {

using sdb::pg::CopyBatch;
using sdb::pg::CopyBatchBuilder;
using sdb::pg::CopyBatchFormat;
using sdb::pg::CopyInBatches;
using sdb::pg::CopyTake;

std::string Join(const CopyBatch& batch) {
  std::string out;
  for (const auto segment : batch.segments) {
    out.append(segment);
  }
  return out;
}

std::string Drain(CopyBatchBuilder& builder, std::string_view input,
                  size_t piece, bool stable) {
  std::string out;
  for (size_t pos = 0; pos < input.size(); pos += piece) {
    builder.Feed(input.substr(pos, piece), stable);
    if (builder.Ready()) {
      out += Join(*builder.Take());
    }
  }
  builder.Finish();
  if (builder.HasRows()) {
    out += Join(*builder.Take());
  }
  return out;
}

void AppendInt16(std::string& out, int16_t value) {
  char bytes[sizeof(int16_t)];
  absl::big_endian::Store16(bytes, static_cast<uint16_t>(value));
  out.append(bytes, sizeof(bytes));
}

void AppendInt32(std::string& out, int32_t value) {
  char bytes[sizeof(int32_t)];
  absl::big_endian::Store32(bytes, static_cast<uint32_t>(value));
  out.append(bytes, sizeof(bytes));
}

std::string BinaryHeader() {
  std::string header{"PGCOPY\n\xFF\r\n\0", 11};
  AppendInt32(header, 0);
  AppendInt32(header, 0);
  return header;
}

std::string BinaryRow(const std::vector<std::optional<std::string>>& fields) {
  std::string row;
  AppendInt16(row, static_cast<int16_t>(fields.size()));
  for (const auto& field : fields) {
    if (!field) {
      AppendInt32(row, -1);
      continue;
    }
    AppendInt32(row, static_cast<int32_t>(field->size()));
    row.append(*field);
  }
  return row;
}

std::string BinaryTrailer() {
  std::string trailer;
  AppendInt16(trailer, -1);
  return trailer;
}

std::string BinaryRows() {
  std::string rows;
  for (int i = 0; i < 50; ++i) {
    rows += BinaryRow({std::string(static_cast<size_t>(i), 'x'), std::nullopt,
                       std::string{}, std::to_string(i)});
  }
  rows += BinaryRow({});
  return rows;
}

TEST(PgCopyBatchBuilder, TextKeepsEveryByteAcrossPieces) {
  const std::string input = "1\ta\n22\tbb\n333\tccc\n\n4444\tdddd";
  for (const size_t piece : {1, 2, 3, 5, 7, 64}) {
    for (const bool stable : {true, false}) {
      for (const size_t target : {1, 8, 1 << 20}) {
        CopyBatchBuilder builder{CopyBatchFormat::Text, target};
        EXPECT_EQ(Drain(builder, input, piece, stable), input)
          << "piece=" << piece << " stable=" << stable << " target=" << target;
      }
    }
  }
}

TEST(PgCopyBatchBuilder, TextBatchesEndOnRowBoundaries) {
  const std::string input = "aa\nbbb\ncccc\nd\n";
  CopyBatchBuilder builder{CopyBatchFormat::Text, 1};
  for (size_t pos = 0; pos < input.size(); pos += 3) {
    builder.Feed(std::string_view{input}.substr(pos, 3), true);
    if (builder.Ready()) {
      const auto batch = builder.Take();
      const auto bytes = Join(*batch);
      ASSERT_FALSE(bytes.empty());
      EXPECT_EQ(bytes.back(), '\n');
    }
  }
}

TEST(PgCopyBatchBuilder, TextStableBytesAreBorrowed) {
  const std::string input = "row one\nrow two\npartial";
  CopyBatchBuilder builder{CopyBatchFormat::Text, 1 << 20};
  builder.Feed(input, true);
  const auto batch = builder.Take();
  ASSERT_EQ(batch->segments.size(), 1);
  EXPECT_EQ(batch->segments[0].data(), input.data());
  EXPECT_TRUE(batch->owned.empty());
}

TEST(PgCopyBatchBuilder, BinaryStripsHeaderAndTrailer) {
  const auto rows = BinaryRows();
  const auto input = BinaryHeader() + rows + BinaryTrailer();
  for (const size_t piece : {1, 2, 3, 7, 64, 4096}) {
    for (const bool stable : {true, false}) {
      for (const size_t target : {1, 100, 1 << 20}) {
        CopyBatchBuilder builder{CopyBatchFormat::Binary, target};
        EXPECT_EQ(Drain(builder, input, piece, stable), rows)
          << "piece=" << piece << " stable=" << stable << " target=" << target;
        EXPECT_TRUE(builder.Ended());
      }
    }
  }
}

TEST(PgCopyBatchBuilder, BinaryWithoutTrailerEndsAtFinish) {
  const auto rows = BinaryRows();
  CopyBatchBuilder builder{CopyBatchFormat::Binary, 1 << 20};
  EXPECT_EQ(Drain(builder, BinaryHeader() + rows, 5, true), rows);
  EXPECT_FALSE(builder.Ended());
}

TEST(PgCopyBatchBuilder, BinaryBatchesHoldWholeRows) {
  const auto rows = BinaryRows();
  const auto input = BinaryHeader() + rows + BinaryTrailer();
  CopyBatchBuilder builder{CopyBatchFormat::Binary, 1};
  size_t batches = 0;
  for (size_t pos = 0; pos < input.size(); pos += 3) {
    builder.Feed(std::string_view{input}.substr(pos, 3), true);
    if (!builder.Ready()) {
      continue;
    }
    const auto bytes = Join(*builder.Take());
    ++batches;
    size_t offset = 0;
    while (offset < bytes.size()) {
      ASSERT_LE(offset + 2, bytes.size());
      const auto fields =
        static_cast<int16_t>(absl::big_endian::Load16(bytes.data() + offset));
      offset += 2;
      for (int16_t field = 0; field < fields; ++field) {
        ASSERT_LE(offset + 4, bytes.size());
        const auto length =
          static_cast<int32_t>(absl::big_endian::Load32(bytes.data() + offset));
        offset += 4 + (length > 0 ? static_cast<size_t>(length) : 0);
      }
      ASSERT_LE(offset, bytes.size());
    }
  }
  EXPECT_GT(batches, 1);
}

TEST(PgCopyBatchBuilder, BinaryRejectsBadSignature) {
  auto header = BinaryHeader();
  header[0] = 'X';
  CopyBatchBuilder builder{CopyBatchFormat::Binary};
  EXPECT_ANY_THROW(builder.Feed(header, true));
}

TEST(PgCopyBatchBuilder, BinaryRejectsShortHeaderAtFinish) {
  CopyBatchBuilder builder{CopyBatchFormat::Binary};
  builder.Feed("PGCOPY", true);
  EXPECT_ANY_THROW(builder.Finish());
}

TEST(PgCopyBatchBuilder, BinaryRejectsDataAfterTrailer) {
  CopyBatchBuilder builder{CopyBatchFormat::Binary};
  builder.Feed(BinaryHeader() + BinaryTrailer(), true);
  EXPECT_ANY_THROW(builder.Feed("x", true));
}

TEST(PgCopyBatchBuilder, BinaryRejectsNegativeFieldLength) {
  auto input = BinaryHeader();
  AppendInt16(input, 1);
  AppendInt32(input, -5);
  CopyBatchBuilder builder{CopyBatchFormat::Binary};
  EXPECT_ANY_THROW(builder.Feed(input, true));
}

TEST(PgCopyBatchBuilder, BinaryRejectsNegativeFieldCount) {
  auto input = BinaryHeader();
  AppendInt16(input, -2);
  CopyBatchBuilder builder{CopyBatchFormat::Binary};
  EXPECT_ANY_THROW(builder.Feed(input, true));
}

TEST(PgCopyBatchBuilder, BinaryRejectsTruncatedRow) {
  auto input = BinaryHeader();
  AppendInt16(input, 1);
  AppendInt32(input, 4);
  input += "ab";
  CopyBatchBuilder builder{CopyBatchFormat::Binary};
  builder.Feed(input, true);
  EXPECT_ANY_THROW(builder.Finish());
}

TEST(PgCopyBatchBuilder, RawPassesBytesThrough) {
  const std::string input = "a,\"multi\nline\"\nb,c\nno newline";
  for (const size_t piece : {1, 4, 64}) {
    CopyBatchBuilder builder{CopyBatchFormat::Raw, 8};
    EXPECT_EQ(Drain(builder, input, piece, true), input);
  }
}

std::unique_ptr<CopyBatch> MakeBatch(std::string bytes) {
  auto batch = std::make_unique<CopyBatch>();
  batch->AddOwned(std::move(bytes));
  return batch;
}

duckdb::TableFunctionInput NoInput() {
  return duckdb::TableFunctionInput{nullptr, nullptr, nullptr};
}

TEST(PgCopyInBatches, NextReadsInOrderUntilFinish) {
  CopyInBatches queue;
  for (int i = 0; i < 3; ++i) {
    queue.Push(MakeBatch(std::to_string(i)), {});
  }
  queue.Finish();
  for (uint64_t i = 0; i < 3; ++i) {
    auto* batch = queue.Next();
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->seq, i);
    EXPECT_EQ(Join(*batch), std::to_string(i));
    queue.Done(batch->seq);
  }
  EXPECT_EQ(queue.Next(), nullptr);
}

TEST(PgCopyInBatches, TakeHandsOutBatches) {
  CopyInBatches queue;
  for (int i = 0; i < 5; ++i) {
    queue.Push(MakeBatch(std::to_string(i)), {});
  }
  queue.Finish();
  auto input = NoInput();
  std::vector<CopyBatch*> taken;
  ASSERT_EQ(queue.Take(input, 3, taken), CopyTake::Batches);
  ASSERT_EQ(taken.size(), 3);
  ASSERT_EQ(queue.Take(input, 3, taken), CopyTake::Batches);
  ASSERT_EQ(taken.size(), 5);
  for (uint64_t i = 0; i < taken.size(); ++i) {
    EXPECT_EQ(taken[i]->seq, i);
  }
  EXPECT_EQ(queue.Take(input, 3, taken), CopyTake::End);
  EXPECT_FALSE(queue.TryTake(1, taken));
  for (auto* batch : taken) {
    queue.Done(batch->seq);
  }
}

TEST(PgCopyInBatches, TakeGrantsProduceToOneConsumerAtATime) {
  CopyInBatches queue;
  auto input = NoInput();
  std::vector<CopyBatch*> taken;
  ASSERT_EQ(queue.Take(input, 1, taken, 1), CopyTake::Produce);
  CopyTake waited = CopyTake::Blocked;
  std::thread waiter{[&] {
    auto waiter_input = NoInput();
    std::vector<CopyBatch*> mine;
    waited = queue.Take(waiter_input, 1, mine, 1);
  }};
  queue.Produced();
  waiter.join();
  EXPECT_EQ(waited, CopyTake::Produce);
  queue.Push(MakeBatch("0"), {});
  queue.Push(MakeBatch("1"), {});
  queue.Produced();
  ASSERT_EQ(queue.Take(input, 1, taken, 2), CopyTake::Batches);
  ASSERT_EQ(queue.Take(input, 1, taken, 2), CopyTake::Produce);
  queue.Finish();
  queue.Produced();
  ASSERT_EQ(queue.Take(input, 1, taken, 2), CopyTake::Batches);
  EXPECT_EQ(queue.Take(input, 1, taken, 2), CopyTake::End);
  for (auto* batch : taken) {
    queue.Done(batch->seq);
  }
}

TEST(PgCopyInBatches, FailRethrowsToConsumers) {
  CopyInBatches queue;
  queue.Push(MakeBatch("0"), {});
  queue.Fail(std::make_exception_ptr(std::runtime_error{"boom"}));
  EXPECT_THROW(queue.Next(), std::runtime_error);
  std::vector<CopyBatch*> taken;
  EXPECT_THROW(queue.TryTake(1, taken), std::runtime_error);
}

TEST(PgCopyInBatches, AbortEndsConsumersAndDropsPushes) {
  CopyInBatches queue;
  queue.Abort();
  EXPECT_TRUE(queue.Aborted());
  queue.Push(MakeBatch("0"), {});
  EXPECT_EQ(queue.Next(), nullptr);
  EXPECT_FALSE(queue.NeedSpace());
}

TEST(PgCopyInBatches, NeedSpaceUntilBatchesAreDone) {
  CopyInBatches queue{2};
  EXPECT_FALSE(queue.NeedSpace());
  queue.Push(MakeBatch("0"), {});
  queue.Push(MakeBatch("1"), {});
  EXPECT_TRUE(queue.NeedSpace());
  auto* first = queue.Next();
  ASSERT_NE(first, nullptr);
  EXPECT_TRUE(queue.NeedSpace());
  queue.Done(first->seq);
  EXPECT_FALSE(queue.NeedSpace());
}

TEST(PgCopyInBatches, NextBlocksUntilPush) {
  CopyInBatches queue;
  std::thread consumer{[&] {
    auto* batch = queue.Next();
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(Join(*batch), "late");
    queue.Done(batch->seq);
    EXPECT_EQ(queue.Next(), nullptr);
  }};
  std::this_thread::yield();
  queue.Push(MakeBatch("late"), {});
  queue.Finish();
  consumer.join();
}

yaclib::Future<> Produce(CopyInBatches& queue, yaclib::IExecutor& io,
                         size_t count) {
  co_await yaclib::On(io);
  for (size_t i = 0; i < count; ++i) {
    if (queue.NeedSpace()) {
      co_await queue.SpaceReady(io);
      queue.ResetSpace();
    }
    queue.Push(MakeBatch(std::to_string(i)), {});
  }
  queue.Finish();
  co_return {};
}

TEST(PgCopyInBatches, ParallelConsumersSeeEveryBatchOnce) {
  constexpr size_t kBatches = 5000;
  constexpr size_t kConsumers = 4;
  for (int round = 0; round < 20; ++round) {
    CopyInBatches queue{4};
    yaclib::FairThreadPool io{1};
    std::vector<std::vector<uint64_t>> seen(kConsumers);
    std::vector<std::thread> consumers;
    for (size_t c = 0; c < kConsumers; ++c) {
      consumers.emplace_back([&, c] {
        auto input = NoInput();
        std::vector<CopyBatch*> taken;
        for (;;) {
          taken.clear();
          if (queue.Take(input, 1 + c, taken) != CopyTake::Batches) {
            return;
          }
          for (auto* batch : taken) {
            EXPECT_EQ(Join(*batch), std::to_string(batch->seq));
            seen[c].emplace_back(batch->seq);
            queue.Done(batch->seq);
          }
        }
      });
    }
    std::ignore = Produce(queue, io, kBatches).Get();
    for (auto& consumer : consumers) {
      consumer.join();
    }
    io.Stop();
    io.Wait();
    std::vector<int> count(kBatches);
    for (const auto& seqs : seen) {
      for (const auto seq : seqs) {
        ASSERT_LT(seq, kBatches);
        ++count[seq];
      }
    }
    for (size_t i = 0; i < kBatches; ++i) {
      ASSERT_EQ(count[i], 1) << "batch " << i << " round " << round;
    }
  }
}

}  // namespace
