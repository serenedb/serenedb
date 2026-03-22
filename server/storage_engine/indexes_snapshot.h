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

#pragma once

#include <memory>
#include <vector>

#include "basics/read_write_lock.h"
#include "basics/recursive_locker.h"

namespace sdb {

class Index;

class IndexesSnapshot {
 public:
  explicit IndexesSnapshot(RecursiveReadLocker<basics::ReadWriteLock>&& locker,
                           std::vector<std::shared_ptr<Index>> indexes);
  ~IndexesSnapshot();

  IndexesSnapshot(const IndexesSnapshot& other) = delete;
  IndexesSnapshot& operator=(const IndexesSnapshot& other) = delete;
  IndexesSnapshot(IndexesSnapshot&& other) = default;
  IndexesSnapshot& operator=(IndexesSnapshot&& other) = delete;

  const std::vector<std::shared_ptr<Index>>& getIndexes() const noexcept;
  void release();

 private:
  RecursiveReadLocker<basics::ReadWriteLock> _locker;
  std::vector<std::shared_ptr<Index>> _indexes;
  bool _valid;
};

}  // namespace sdb
