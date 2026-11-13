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
#include <bit>
#include <cstdint>
#include <duckdb/main/client_context.hpp>
#include <memory>
#include <span>

#include "iresearch/utils/assert.hpp"

namespace sdb::search {

class WriterGenerations final {
  struct Leaver {
    unsigned slot = 0;

    void operator()(WriterGenerations* generations) const noexcept {
      SDB_ASSERT(generations);
      generations->Leave(slot);
    }
  };

 public:
  using Writer = std::unique_ptr<WriterGenerations, Leaver>;

  [[nodiscard]] Writer Register() noexcept;
  void Drain(duckdb::ClientContext& context, const Writer* own,
             std::span<const WriterGenerations* const> written);

 private:
  static constexpr uint64_t kGeneration = 1;
  static constexpr unsigned kFlagBits = std::bit_width(kGeneration);
  static constexpr unsigned kWriterBits = 30;

  static constexpr uint64_t One(unsigned slot) noexcept {
    return uint64_t{1} << (kFlagBits + kWriterBits * slot);
  }
  static constexpr uint64_t Writers(uint64_t state, unsigned slot) noexcept {
    return (state >> (kFlagBits + kWriterBits * slot)) &
           ((uint64_t{1} << kWriterBits) - 1);
  }

  void Leave(unsigned slot) noexcept;

  std::atomic<uint64_t> _state{0};
};

}  // namespace sdb::search
