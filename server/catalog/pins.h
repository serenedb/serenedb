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

#pragma once

#include <absl/base/thread_annotations.h>
#include <absl/synchronization/mutex.h>

#include <cstdint>
#include <duckdb/common/file_system.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace sdb::catalog {

struct ParkedFiles {
  std::string root;
  std::vector<std::string> files;
};

class Pins final {
 public:
  static Pins& Get();

  void Load(duckdb::FileSystem& fs, const std::string& backups);
  void Add(std::string name, std::span<const uint64_t> segments);
  std::vector<ParkedFiles> Remove(std::string_view name);
  bool ParkIfHeld(const std::string& root, uint64_t segment,
                  std::span<const std::string> files);
  bool Holds(uint64_t segment) const;
  bool Contains(std::string_view name) const;
  std::vector<std::string> Names() const;

 private:
  mutable absl::Mutex _mutex;
  irs::containers::FlatHashMap<std::string, std::vector<uint64_t>> _pins
    ABSL_GUARDED_BY(_mutex);
  irs::containers::FlatHashMap<uint64_t, uint32_t> _held
    ABSL_GUARDED_BY(_mutex);
  irs::containers::FlatHashMap<uint64_t, ParkedFiles> _parked
    ABSL_GUARDED_BY(_mutex);
};

void RemoveParked(std::vector<ParkedFiles> parked);

}  // namespace sdb::catalog
