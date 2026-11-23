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

#include <algorithm>
#include <array>
#include <cstdint>
#include <duckdb.hpp>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <iresearch/formats/column/column_patch.hpp>
#include <iresearch/utils/compression.hpp>
#include <iresearch/utils/parallel_for.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <limits>
#include <memory>
#include <random>
#include <span>
#include <string>
#include <vector>

#include "catalog/log.h"
#include "search/log_entry.h"
#include "search/table.h"
#include "search/table_replay.h"
#include "server/utils/frame_stream.h"

namespace {

using sdb::search::LoggedRows;
using sdb::search::TableDelta;
using sdb::search::UnitDelta;

constexpr uint32_t kFrame = 256;
constexpr size_t kNoise = std::numeric_limits<size_t>::max();

duckdb::LogicalType PayloadType(size_t run) {
  return run == kNoise ? duckdb::LogicalType::BIGINT
                       : duckdb::LogicalType::BLOB;
}

void Fill(duckdb::Vector& ids, duckdb::Vector& payloads, size_t run,
          size_t count, std::mt19937_64& random) {
  auto* id = duckdb::FlatVector::GetDataMutable<int64_t>(ids);
  if (run == kNoise) {
    auto* payload = duckdb::FlatVector::GetDataMutable<int64_t>(payloads);
    for (size_t i = 0; i != count; ++i) {
      id[i] = static_cast<int64_t>(random());
      payload[i] = static_cast<int64_t>(random());
    }
  } else {
    auto* payload =
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(payloads);
    const std::string bytes(run, 'a');
    for (size_t i = 0; i != count; ++i) {
      id[i] = static_cast<int64_t>(i);
      payload[i] = duckdb::StringVector::AddStringOrBlob(payloads, bytes.data(),
                                                         bytes.size());
    }
  }
  duckdb::FlatVector::SetSize(ids, count);
  duckdb::FlatVector::SetSize(payloads, count);
}

LoggedRows Rows(size_t run, size_t count, std::mt19937_64& random) {
  constexpr size_t kChunkRows = 100;
  auto chunks =
    std::make_shared<std::vector<duckdb::unique_ptr<duckdb::DataChunk>>>();
  for (size_t at = 0; at < count; at += kChunkRows) {
    const auto size = std::min(kChunkRows, count - at);
    auto& chunk = *chunks->emplace_back(duckdb::make_uniq<duckdb::DataChunk>());
    chunk.Initialize(duckdb::Allocator::DefaultAllocator(),
                     {duckdb::LogicalType::BIGINT, PayloadType(run)}, size);
    Fill(chunk.data[0], chunk.data[1], run, size, random);
    chunk.SetCardinality(size);
  }
  LoggedRows rows{.first = 7, .columns = {1, 2}, .chunks = chunks->size()};
  rows.scan = [chunks](sdb::search::ChunkVisit visit) {
    for (const auto& chunk : *chunks) {
      visit(*chunk);
    }
  };
  return rows;
}

UnitDelta Patches(size_t run, size_t count, std::mt19937_64& random) {
  std::vector<uint64_t> rows(count);
  for (size_t i = 0; i != count; ++i) {
    rows[i] = i * 2;
  }
  duckdb::Vector ids{duckdb::LogicalType::BIGINT, count};
  duckdb::Vector payloads{PayloadType(run), count};
  Fill(ids, payloads, run, count, random);
  UnitDelta unit{.unit = 3};
  for (const auto* values : {&ids, &payloads}) {
    irs::ColumnPatch::Builder builder{values->GetType()};
    builder.Add(rows, *values);
    unit.patches.push_back(
      {.field = static_cast<irs::field_id>(unit.patches.size() + 1),
       .patch = std::move(builder).Finish()});
  }
  return unit;
}

enum class Blobs : uint8_t {
  Rows,
  Patches,
  All,
};

TableDelta Delta(size_t run, size_t rows, Blobs blobs = Blobs::All) {
  std::mt19937_64 random{rows};
  TableDelta delta;
  if (blobs != Blobs::Patches) {
    delta.rows.push_back(Rows(run, rows, random));
  }
  if (blobs != Blobs::Rows) {
    delta.segments.push_back(Patches(run, rows, random));
  }
  return delta;
}

std::string Record(const TableDelta& delta) {
  sdb::catalog::RecordBuffer out;
  duckdb::BinarySerializer object{out};
  object.Begin();
  sdb::search::WriteTableDelta(object, out, delta);
  object.End();
  std::string record;
  out.ForEachPayload([&](std::span<const uint8_t> part) {
    record.append(reinterpret_cast<const char*>(part.data()), part.size());
  });
  return record;
}

std::string PackedRecord(TableDelta& delta) {
  sdb::search::PackDelta(delta, kFrame, irs::SerialFor);
  return Record(delta);
}

TEST(SearchLogEntry, BlobsThatDoNotPayAreWrittenFromTheirStagedBytes) {
  for (const size_t rows : {size_t{1}, size_t{3}, size_t{600}}) {
    const auto plain = Delta(kNoise, rows);
    auto staged = Delta(kNoise, rows);
    const auto record = PackedRecord(staged);
    const auto& chunks = staged.rows.front();
    EXPECT_FALSE(chunks.scan);
    ASSERT_TRUE(chunks.staged.slots);
    EXPECT_TRUE(irs::compression::IsPlain(chunks.staged));
    const auto& values = staged.segments.front().staged;
    ASSERT_EQ(values.size(), 2u);
    for (const auto& value : values) {
      ASSERT_TRUE(value);
      ASSERT_TRUE(value->slots);
      EXPECT_TRUE(irs::compression::IsPlain(*value));
    }
    EXPECT_EQ(record, Record(plain)) << rows;
    if (rows == 600) {
      EXPECT_GT(chunks.staged.frames.raw_bytes, uint64_t{kFrame} * 32);
      EXPECT_GT(values[1]->frames.raw_bytes, uint64_t{kFrame} * 16);
    }
  }
}

TEST(SearchLogEntry, BlobsThatPayArePacked) {
  constexpr size_t kRows = 250;
  constexpr size_t kRun = 60;
  const auto plain = Delta(kRun, kRows);
  auto packed = Delta(kRun, kRows);
  const auto record = PackedRecord(packed);
  EXPECT_FALSE(packed.rows.front().scan);
  EXPECT_FALSE(irs::compression::IsPlain(packed.rows.front().staged));
  for (const auto& value : packed.segments.front().staged) {
    ASSERT_TRUE(value);
    EXPECT_FALSE(irs::compression::IsPlain(*value));
  }
  EXPECT_LT(record.size() * 2, Record(plain).size());
}

TEST(SearchLogEntry, ReplayRefusesPackedRowsLongerThanTheirChunks) {
  std::mt19937_64 random{1};
  auto rows = Rows(60, 250, random);
  sdb::message::FrameStream raw{kFrame};
  {
    duckdb::BinarySerializer serializer{raw};
    serializer.OnListBegin(rows.chunks);
    rows.scan([&](duckdb::DataChunk& chunk) {
      serializer.OnObjectBegin();
      chunk.Serialize(serializer);
      serializer.OnObjectEnd();
    });
    serializer.OnListEnd();
  }
  constexpr std::array<uint8_t, 4> kTail{1, 2, 3, 4};
  raw.WriteData(kTail.data(), kTail.size());
  const std::vector<std::vector<irs::bytes_view>> frames{raw.Frames()};
  rows.staged = std::move(
    irs::compression::Compress(irs::compression::kZstd1, kFrame, frames)
      .front());
  rows.scan = nullptr;
  ASSERT_FALSE(irs::compression::IsPlain(rows.staged));
  TableDelta delta;
  delta.rows.push_back(std::move(rows));
  auto record = Record(delta);

  duckdb::DuckDB db{nullptr};
  duckdb::Connection connection{db};
  auto& context = *connection.context;
  sdb::search::TableReplay replay;
  duckdb::MemoryStream stream{
    reinterpret_cast<duckdb::data_ptr_t>(record.data()), record.size()};
  duckdb::BinaryDeserializer object{stream};
  object.Begin();
  try {
    sdb::search::ReplayTableDelta(context, &replay,
                                  &duckdb::Catalog::GetSystemCatalog(context),
                                  object, stream);
    ADD_FAILURE() << "replayed packed rows with bytes after their chunks";
  } catch (const irs::SqlException& e) {
    EXPECT_STREQ(e.what(), "search log: rows longer than their chunks");
  }
}

TEST(SearchLogEntry, AReplayWithoutATableSkipsPackedRecords) {
  auto delta = Delta(60, 250);
  sdb::search::PackDelta(delta, kFrame, irs::SerialFor);
  ASSERT_FALSE(irs::compression::IsPlain(delta.rows.front().staged));
  for (const auto& value : delta.segments.front().staged) {
    ASSERT_FALSE(irs::compression::IsPlain(*value));
  }
  auto record = Record(delta);

  duckdb::DuckDB db{nullptr};
  duckdb::Connection connection{db};
  duckdb::MemoryStream stream{
    reinterpret_cast<duckdb::data_ptr_t>(record.data()), record.size()};
  duckdb::BinaryDeserializer object{stream};
  object.Begin();
  sdb::search::ReplayTableDelta(*connection.context, nullptr, nullptr, object,
                                stream);
  object.End();
  EXPECT_EQ(stream.GetPosition(), record.size());
}

TEST(SearchLogEntry, APackedRecordIsNeverLargerThanThePlainOne) {
  for (const auto blobs : {Blobs::Rows, Blobs::Patches}) {
    for (const size_t rows : {size_t{1}, size_t{2}, size_t{5}}) {
      for (size_t run = 0; run != 160; ++run) {
        const auto plain = Delta(run, rows, blobs);
        auto packed = Delta(run, rows, blobs);
        EXPECT_LE(PackedRecord(packed).size(), Record(plain).size())
          << (blobs == Blobs::Rows ? "rows: " : "patches: ") << rows
          << " rows of " << run << " bytes";
      }
    }
  }
}

}  // namespace
