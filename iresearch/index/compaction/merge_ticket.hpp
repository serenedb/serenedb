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

#include <atomic>
#include <memory>
#include <span>
#include <vector>

#include "iresearch/index/segment_reader.hpp"

namespace irs {

class DirectoryReaderImpl;
class IndexWriter;
struct SubReader;

using Compaction = std::vector<const SubReader*>;
using CompactionView = std::span<const SubReader* const>;

class MergeTicket {
 public:
  CompactionView Candidates() const noexcept;
  bool Revoked() const noexcept;

 private:
  friend class IndexWriter;

  struct State {
    std::weak_ptr<const DirectoryReaderImpl> snapshot;
    std::vector<SegmentReader> inputs;
    Compaction candidates;
    std::atomic_bool revoked{false};
    bool registered = true;
    bool running = false;
  };

  struct Release {
    void operator()(State* state) const noexcept;

    IndexWriter* writer = nullptr;
  };

  MergeTicket(IndexWriter& writer, std::unique_ptr<State> state) noexcept;

  std::unique_ptr<State, Release> _state;
};

}  // namespace irs
