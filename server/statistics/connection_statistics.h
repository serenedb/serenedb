#pragma once

#include <cstddef>
#include <cstdint>

namespace sdb {

// No-op stub. The ArangoDB connection-statistics machinery is deleted;
// the header survives so `*_comm_task` compiles with default-constructed
// items that ignore all SET_* calls.
class ConnectionStatistics {
 public:
  static void initialize() noexcept {}
  static uint64_t memoryUsage() noexcept { return 0; }

  class Item {
   public:
    constexpr Item() noexcept = default;
    Item(const Item&) = delete;
    Item& operator=(const Item&) = delete;
    Item(Item&&) noexcept = default;
    Item& operator=(Item&&) noexcept = default;
    ~Item() = default;

    void reset() noexcept {}
    void SET_START() noexcept {}
    void SET_END() noexcept {}
    void SET_HTTP() noexcept {}
  };

  static Item acquire() noexcept { return Item{}; }
};

}  // namespace sdb
