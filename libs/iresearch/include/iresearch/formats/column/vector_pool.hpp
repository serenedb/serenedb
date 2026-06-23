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

#include <duckdb/common/allocator.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/types/vector_cache.hpp>
#include <memory>
#include <utility>
#include <vector>

namespace irs {

class VectorPool final {
 private:
  struct Slot {
    Slot(duckdb::Allocator& alloc, duckdb::LogicalType t)
      : type{std::move(t)}, cache{alloc, type}, vec{cache} {}

    duckdb::LogicalType type;
    duckdb::VectorCache cache;
    duckdb::Vector vec;
    bool in_use = false;
  };

 public:
  class Lease final {
   public:
    Lease() noexcept = default;
    explicit Lease(Slot* slot) noexcept : _slot{slot} {}

    Lease(const Lease&) = delete;
    Lease& operator=(const Lease&) = delete;
    Lease(Lease&& other) noexcept
      : _slot{std::exchange(other._slot, nullptr)} {}
    Lease& operator=(Lease&& other) noexcept {
      if (this != &other) {
        Reset();
        _slot = std::exchange(other._slot, nullptr);
      }
      return *this;
    }
    ~Lease() { Reset(); }

    duckdb::Vector& operator*() const noexcept { return _slot->vec; }
    duckdb::Vector* operator->() const noexcept { return &_slot->vec; }
    duckdb::Vector& Get() const noexcept { return _slot->vec; }

   private:
    void Reset() noexcept {
      if (_slot != nullptr) {
        _slot->in_use = false;
        _slot = nullptr;
      }
    }

    Slot* _slot = nullptr;
  };

  explicit VectorPool(duckdb::Allocator& alloc) noexcept : _alloc{&alloc} {}

  Lease Acquire(const duckdb::LogicalType& type) {
    for (const auto& slot : _slots) {
      if (!slot->in_use && slot->type == type) {
        slot->vec.ResetFromCache(slot->cache);
        slot->in_use = true;
        return Lease{slot.get()};
      }
    }
    auto& slot = _slots.emplace_back(std::make_unique<Slot>(*_alloc, type));
    slot->in_use = true;
    return Lease{slot.get()};
  }

 private:
  duckdb::Allocator* _alloc;
  std::vector<std::unique_ptr<Slot>> _slots;
};

}  // namespace irs
