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

// Candidates for virtual_column_map_t, which every LogicalGet carries. It holds
// the table's virtual columns (2 today: rowid, row_number), is built per bind,
// copied with the operator, probed by column id, and -- the reason this matters
// -- iterated when the operator is serialized into the CommonSubplanOptimizer's
// plan signature, so its order has to be deterministic.
//
// Arms:
//   build_insert  -- default ctor + emplace per entry
//   build_reserve -- reserve() first, where the container has one
//   build_ctor    -- construct from an initializer list in one go
//   lookup_hit    -- find() of present ids
//   lookup_miss   -- find() of one id below and one above the present ones
//   iterate       -- full traversal, the serialization path
// lookup and iterate come in two flavours: keys only, or also touching the
// mapped value, which is what decides whether the node indirection is paid.

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/linked_hash_map.h>
#include <absl/container/node_hash_map.h>
#include <absl/hash/hash.h>
#include <benchmark/benchmark.h>

#include <cstdlib>
#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "duckdb/common/table_column.hpp"

namespace {

using duckdb::column_t;
using duckdb::Identifier;
using duckdb::LogicalType;
using duckdb::TableColumn;

constexpr column_t kRowId = UINT64_C(18446744073709551615);
constexpr column_t kRowNumber = UINT64_C(18446744073709551613);
// One miss below every present key and one above them: ordered maps walk
// different depths for the two, hash maps do not care.
constexpr column_t kAbsentLow = 7;
constexpr column_t kAbsentHigh = UINT64_C(18446744073709551614);

// The indirection arm: the map stores a pointer, so the node payload is 8 bytes
// and the value never moves, at the cost of an allocation per entry.
struct BoxedColumn {
  BoxedColumn() = default;
  BoxedColumn(Identifier name, LogicalType type)
    : ptr(std::make_unique<TableColumn>(std::move(name), std::move(type))) {}
  BoxedColumn(const BoxedColumn& other)
    : ptr(other.ptr ? std::make_unique<TableColumn>(*other.ptr) : nullptr) {}
  BoxedColumn& operator=(const BoxedColumn& other) {
    ptr = other.ptr ? std::make_unique<TableColumn>(*other.ptr) : nullptr;
    return *this;
  }
  BoxedColumn(BoxedColumn&&) = default;
  BoxedColumn& operator=(BoxedColumn&&) = default;

  std::unique_ptr<TableColumn> ptr;
};

uint8_t TypeId(const TableColumn& column) {
  return static_cast<uint8_t>(column.type.id());
}

uint8_t TypeId(const BoxedColumn& column) {
  return static_cast<uint8_t>(column.ptr->type.id());
}

template<typename T, typename = void>
struct HasReserve : std::false_type {};

template<typename T>
struct HasReserve<T,
                  std::void_t<decltype(std::declval<T&>().reserve(size_t{}))>>
  : std::true_type {};

template<typename Map>
std::vector<std::pair<const column_t, typename Map::mapped_type>> Entries(
  size_t extra) {
  using Value = typename Map::mapped_type;
  std::vector<std::pair<const column_t, Value>> entries;
  entries.emplace_back(kRowId,
                       Value(Identifier("rowid"), LogicalType::ROW_TYPE));
  entries.emplace_back(kRowNumber,
                       Value(Identifier("row_number"), LogicalType::BIGINT));
  for (size_t i = 0; i < extra; ++i) {
    entries.emplace_back(i, Value(Identifier("extra"), LogicalType::BIGINT));
  }
  return entries;
}

enum class BuildKind {
  Insert,
  Reserve,
  Ctor,
};

template<typename Map, BuildKind Kind>
Map Build(
  const std::vector<std::pair<const column_t, typename Map::mapped_type>>&
    entries) {
  if constexpr (Kind == BuildKind::Ctor) {
    return Map(entries.begin(), entries.end());
  } else {
    Map map;
    if constexpr (Kind == BuildKind::Reserve) {
      map.reserve(entries.size());
    }
    for (const auto& entry : entries) {
      map.emplace(entry.first, entry.second);
    }
    return map;
  }
}

template<typename Map, BuildKind kKind>
void BmBuild(benchmark::State& state) {
  const auto entries = Entries<Map>(static_cast<size_t>(state.range(0)));
  for (auto _ : state) {
    auto map = Build<Map, kKind>(entries);
    benchmark::DoNotOptimize(map);
    benchmark::ClobberMemory();
  }
}

template<typename Map>
void BmCopy(benchmark::State& state) {
  const auto source = Build<Map, BuildKind::Insert>(
    Entries<Map>(static_cast<size_t>(state.range(0))));
  for (auto _ : state) {
    Map copy(source);
    benchmark::DoNotOptimize(copy);
    benchmark::ClobberMemory();
  }
}

template<typename Map, bool Hit, bool ReadValue>
void BmLookup(benchmark::State& state) {
  const auto map = Build<Map, BuildKind::Insert>(
    Entries<Map>(static_cast<size_t>(state.range(0))));
  const std::vector<column_t> probes =
    Hit ? std::vector<column_t>{kRowId, kRowNumber}
        : std::vector<column_t>{kAbsentLow, kAbsentHigh};
  for (auto _ : state) {
    uint32_t acc = 0;
    for (auto probe : probes) {
      auto it = map.find(probe);
      if constexpr (ReadValue) {
        acc += it == map.end() ? 0u : TypeId(it->second);
      } else {
        acc += it == map.end() ? 0u : 1u;
      }
    }
    benchmark::DoNotOptimize(acc);
  }
}

template<typename Map, bool ReadValue>
void BmIterate(benchmark::State& state) {
  const auto map = Build<Map, BuildKind::Insert>(
    Entries<Map>(static_cast<size_t>(state.range(0))));
  for (auto _ : state) {
    uint64_t acc = 0;
    for (const auto& entry : map) {
      if constexpr (ReadValue) {
        acc += entry.first + TypeId(entry.second);
      } else {
        acc += entry.first;
      }
    }
    benchmark::DoNotOptimize(acc);
  }
}

using StdMap = std::map<column_t, TableColumn>;
using StdUnordered = std::unordered_map<column_t, TableColumn>;
using AbslBtree = absl::btree_map<column_t, TableColumn>;
using AbslBtreeBoxed = absl::btree_map<column_t, BoxedColumn>;
using AbslLinked = absl::linked_hash_map<column_t, TableColumn>;
using AbslNode = absl::node_hash_map<column_t, TableColumn>;
using AbslFlatBoxed = absl::flat_hash_map<column_t, BoxedColumn>;
using AbslFlat = absl::flat_hash_map<column_t, TableColumn>;
// The same absl containers with the identity hash std::unordered_map gets, to
// separate absl's strong hash from the table layout itself.
using AbslLinkedStdHash =
  absl::linked_hash_map<column_t, TableColumn, std::hash<column_t>>;
using AbslNodeStdHash =
  absl::node_hash_map<column_t, TableColumn, std::hash<column_t>>;
using AbslFlatBoxedStdHash =
  absl::flat_hash_map<column_t, BoxedColumn, std::hash<column_t>>;
using AbslFlatStdHash =
  absl::flat_hash_map<column_t, TableColumn, std::hash<column_t>>;
// And the std table with both hashes, so the hasher can be read off
// independently of the container.
using StdUnorderedStdHash =
  std::unordered_map<column_t, TableColumn, std::hash<column_t>>;
using StdUnorderedAbslHash =
  std::unordered_map<column_t, TableColumn, absl::Hash<column_t>>;

template<typename Map>
void Register(const std::string& name) {
  for (auto extra : {0, 1, 2, 3, 4, 6}) {
    benchmark::RegisterBenchmark(("build_insert/" + name).c_str(),
                                 BmBuild<Map, BuildKind::Insert>)
      ->Arg(extra);
    if constexpr (HasReserve<Map>::value) {
      benchmark::RegisterBenchmark(("build_reserve/" + name).c_str(),
                                   BmBuild<Map, BuildKind::Reserve>)
        ->Arg(extra);
    }
    benchmark::RegisterBenchmark(("build_ctor/" + name).c_str(),
                                 BmBuild<Map, BuildKind::Ctor>)
      ->Arg(extra);
    benchmark::RegisterBenchmark(("copy/" + name).c_str(), BmCopy<Map>)
      ->Arg(extra);
    benchmark::RegisterBenchmark(("lookup_hit_key/" + name).c_str(),
                                 BmLookup<Map, true, false>)
      ->Arg(extra);
    benchmark::RegisterBenchmark(("lookup_hit_value/" + name).c_str(),
                                 BmLookup<Map, true, true>)
      ->Arg(extra);
    benchmark::RegisterBenchmark(("lookup_miss_key/" + name).c_str(),
                                 BmLookup<Map, false, false>)
      ->Arg(extra);
    benchmark::RegisterBenchmark(("lookup_miss_value/" + name).c_str(),
                                 BmLookup<Map, false, true>)
      ->Arg(extra);
    benchmark::RegisterBenchmark(("iterate_key/" + name).c_str(),
                                 BmIterate<Map, false>)
      ->Arg(extra);
    benchmark::RegisterBenchmark(("iterate_value/" + name).c_str(),
                                 BmIterate<Map, true>)
      ->Arg(extra);
  }
}

}  // namespace

int main(int argc, char** argv) {
  Register<StdMap>("std_map");
  Register<AbslBtree>("absl_btree_map");
  Register<AbslBtreeBoxed>("absl_btree_map_boxed");

  Register<StdUnorderedStdHash>("std_unordered_map_stdhash");
  Register<StdUnorderedAbslHash>("std_unordered_map_abslhash");

  Register<AbslLinked>("absl_linked_hash_map");
  Register<AbslNode>("absl_node_hash_map");
  Register<AbslFlatBoxed>("absl_flat_hash_map_boxed");
  Register<AbslFlat>("absl_flat_hash_map");

  Register<AbslLinkedStdHash>("absl_linked_hash_map_stdhash");
  Register<AbslNodeStdHash>("absl_node_hash_map_stdhash");
  Register<AbslFlatBoxedStdHash>("absl_flat_hash_map_boxed_stdhash");
  Register<AbslFlatStdHash>("absl_flat_hash_map_stdhash");

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
