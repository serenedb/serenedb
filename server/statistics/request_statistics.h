#pragma once

#include "rest/common_defines.h"

namespace sdb {

// No-op stub. The ArangoDB request-statistics machinery is deleted; this
// header keeps the call-site signature so `*_comm_task` / `RestHandler`
// compile unchanged. Every SET_* / ELAPSED_* / ADD_* call is a no-op.
class RequestStatistics {
 public:
  static void initialize() noexcept {}
  static size_t processAll() noexcept { return 0; }
  static uint64_t memoryUsage() noexcept { return 0; }

  class ItemView {
   public:
    constexpr ItemView() noexcept = default;
    explicit constexpr ItemView(RequestStatistics*) noexcept {}

    constexpr operator bool() const noexcept { return false; }

    void SET_ASYNC() const noexcept {}
    void SET_REQUEST_TYPE(rest::RequestType) const noexcept {}
    void SET_READ_START(double) const noexcept {}
    void SET_READ_END() const noexcept {}
    void SET_WRITE_START() const noexcept {}
    void SET_WRITE_END() const noexcept {}
    void SET_QUEUE_START() const noexcept {}
    void SET_QUEUE_END() const noexcept {}
    void ADD_RECEIVED_BYTES(size_t) const noexcept {}
    void ADD_SENT_BYTES(size_t) const noexcept {}
    void SET_REQUEST_START() const noexcept {}
    void SET_REQUEST_END() const noexcept {}
    void SET_REQUEST_START_END() const noexcept {}
    void SET_SUPERUSER() const noexcept {}

    double ELAPSED_SINCE_READ_START() const noexcept { return 0.0; }
    double ELAPSED_WHILE_QUEUED() const noexcept { return 0.0; }
  };

  class Item : public ItemView {
   public:
    using ItemView::ItemView;

    Item(const Item&) = delete;
    Item& operator=(const Item&) = delete;
    Item(Item&&) noexcept = default;
    Item& operator=(Item&&) noexcept = default;
  };

  static Item acquire() noexcept { return Item{}; }
};

}  // namespace sdb
