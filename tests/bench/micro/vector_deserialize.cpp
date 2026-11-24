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

// Prices the checks a deserializer makes on a peer's batch: a Quack batch is
// read as the database's own data (none), with its indexes, offsets and
// lengths checked (a client reading a server's batch) or with its values
// checked too (a server reading a client's batch).
//
//   serenedb-bench-micro vector_deserialize

#include <benchmark/benchmark.h>

#include <cstdint>
#include <duckdb.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <random>
#include <string>

namespace {

constexpr duckdb::idx_t kRows = 2048;

enum class Shape : uint8_t {
  Bigint,
  DictionaryBigint,
  Boolean,
  Ascii,
  Multibyte,
};

duckdb::LogicalType TypeOf(Shape shape) {
  switch (shape) {
    case Shape::Bigint:
    case Shape::DictionaryBigint:
      return duckdb::LogicalType::BIGINT;
    case Shape::Boolean:
      return duckdb::LogicalType::BOOLEAN;
    case Shape::Ascii:
    case Shape::Multibyte:
      return duckdb::LogicalType::VARCHAR;
  }
  return duckdb::LogicalType::INVALID;
}

std::string RandomString(std::mt19937_64& rng, bool multibyte) {
  std::string out;
  const auto length = 4 + rng() % 28;
  for (size_t i = 0; i < length; ++i) {
    if (multibyte && rng() % 4 == 0) {
      out += "\xc3\xa9";
    } else {
      out += static_cast<char>('a' + rng() % 26);
    }
  }
  return out;
}

// One chunk of `shape`, serialized as a Quack peer sends it
duckdb::unique_ptr<duckdb::MemoryStream> Serialized(Shape shape) {
  std::mt19937_64 rng{7};
  duckdb::DataChunk chunk;
  chunk.Initialize(duckdb::Allocator::DefaultAllocator(), {TypeOf(shape)});
  auto& column = chunk.data[0];
  switch (shape) {
    case Shape::Bigint: {
      auto data = duckdb::FlatVector::GetDataMutable<int64_t>(column);
      for (duckdb::idx_t i = 0; i < kRows; ++i) {
        data[i] = static_cast<int64_t>(rng());
      }
      break;
    }
    case Shape::DictionaryBigint: {
      constexpr duckdb::idx_t kValues = 64;
      duckdb::Vector dictionary{duckdb::LogicalType::BIGINT, kValues};
      auto values = duckdb::FlatVector::GetDataMutable<int64_t>(dictionary);
      for (duckdb::idx_t i = 0; i < kValues; ++i) {
        values[i] = static_cast<int64_t>(rng());
      }
      duckdb::FlatVector::SetSize(dictionary, kValues);
      duckdb::SelectionVector sel{kRows};
      for (duckdb::idx_t i = 0; i < kRows; ++i) {
        sel.set_index(i, rng() % kValues);
      }
      column.Dictionary(dictionary, kValues, sel, kRows);
      break;
    }
    case Shape::Boolean: {
      auto data = duckdb::FlatVector::GetDataMutable<bool>(column);
      for (duckdb::idx_t i = 0; i < kRows; ++i) {
        data[i] = rng() % 2 == 0;
      }
      break;
    }
    case Shape::Ascii:
    case Shape::Multibyte: {
      auto data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(column);
      for (duckdb::idx_t i = 0; i < kRows; ++i) {
        data[i] = duckdb::StringVector::AddString(
          column, RandomString(rng, shape == Shape::Multibyte));
      }
      break;
    }
  }
  chunk.SetChildCardinality(kRows);

  duckdb::SerializationOptions options;
  options.storage_compatibility = duckdb::StorageCompatibility::DuckDBLatest();
  auto stream = duckdb::make_uniq<duckdb::MemoryStream>();
  duckdb::BinarySerializer serializer{*stream, options};
  serializer.Begin();
  chunk.Serialize(serializer);
  serializer.End();
  return stream;
}

void BM_Deserialize(benchmark::State& state, Shape shape,
                    duckdb::DeserializationChecks checks) {
  const auto serialized = Serialized(shape);
  const auto size = serialized->GetPosition();
  for (auto _ : state) {
    duckdb::MemoryStream stream{serialized->GetData(), size};
    duckdb::BinaryDeserializer deserializer{stream};
    deserializer.SetChecks(checks);
    duckdb::DataChunk chunk;
    deserializer.Begin();
    chunk.Deserialize(deserializer);
    deserializer.End();
    benchmark::DoNotOptimize(chunk.data[0].GetBufferRef().get());
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * size));
}

}  // namespace

using enum duckdb::DeserializationChecks;

BENCHMARK_CAPTURE(BM_Deserialize, bigint_none, Shape::Bigint, NONE);
BENCHMARK_CAPTURE(BM_Deserialize, bigint_values, Shape::Bigint, VALUES);
BENCHMARK_CAPTURE(BM_Deserialize, dictionary_none, Shape::DictionaryBigint,
                  NONE);
BENCHMARK_CAPTURE(BM_Deserialize, dictionary_structure, Shape::DictionaryBigint,
                  STRUCTURE);
BENCHMARK_CAPTURE(BM_Deserialize, boolean_none, Shape::Boolean, NONE);
BENCHMARK_CAPTURE(BM_Deserialize, boolean_values, Shape::Boolean, VALUES);
BENCHMARK_CAPTURE(BM_Deserialize, ascii_none, Shape::Ascii, NONE);
BENCHMARK_CAPTURE(BM_Deserialize, ascii_structure, Shape::Ascii, STRUCTURE);
BENCHMARK_CAPTURE(BM_Deserialize, ascii_values, Shape::Ascii, VALUES);
BENCHMARK_CAPTURE(BM_Deserialize, multibyte_none, Shape::Multibyte, NONE);
BENCHMARK_CAPTURE(BM_Deserialize, multibyte_values, Shape::Multibyte, VALUES);
