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

#include <absl/synchronization/mutex.h>

#include <atomic>
#include <cstdint>
#include <duckdb/common/time_point.hpp>
#include <list>
#include <memory>
#include <random>
#include <span>
#include <typeinfo>
#include <vector>

#include "iresearch/search/filters/filter.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"

namespace irs {

class QueryProfile {
 public:
  struct Prepared {
    std::atomic_uint64_t ticks = 0;
    std::atomic_uint64_t segments = 0;
    std::atomic_uint64_t terms = 0;
    std::atomic_uint64_t postings = 0;
    std::atomic_uint64_t matches = 0;
    std::atomic<QueryKind> kind = QueryKind::Empty;
  };

  struct Executed {
    const std::type_info* type = nullptr;
    std::atomic_uint64_t plans = 0;
    std::atomic_uint64_t plan_ticks = 0;
    std::atomic_uint64_t calls = 0;
    std::atomic_uint64_t docs = 0;
    std::atomic_uint64_t ticks = 0;
    std::atomic_uint64_t timed_calls = 0;
  };

  struct ExecutedNode {
    uint32_t node = 0;
    const Executed* executed = nullptr;
  };

  class Counter {
   public:
    explicit Counter(Executed& slot) : _slot{slot} {}

    Counter(const Counter&) = delete;
    Counter& operator=(const Counter&) = delete;

    ~Counter() {
      _slot.calls.fetch_add(_calls, std::memory_order_relaxed);
      _slot.docs.fetch_add(_docs, std::memory_order_relaxed);
      _slot.ticks.fetch_add(_ticks, std::memory_order_relaxed);
      _slot.timed_calls.fetch_add(_timed, std::memory_order_relaxed);
    }

    template<typename F>
    auto Time(F&& f) {
      ++_calls;
      ++_timed;
      const Stopwatch stopwatch{_ticks};
      return f();
    }

    template<typename F>
    auto Sample(F&& f) {
      if (--_countdown != 0) [[likely]] {
        ++_calls;
        return f();
      }
      _countdown = NextGap();
      return Time(std::forward<F>(f));
    }

    void Docs(uint64_t docs) noexcept { _docs += docs; }

    void Unplan() noexcept {
      _slot.plans.fetch_sub(1, std::memory_order_relaxed);
    }

   private:
    static constexpr uint32_t kSampleEvery = 32;

    struct Stopwatch {
      uint64_t& ticks;
      int64_t start = duckdb::TimePoint::Now();

      ~Stopwatch() {
        ticks += static_cast<uint64_t>(duckdb::TimePoint::Now() - start);
      }
    };

    uint32_t NextGap() {
      return std::uniform_int_distribution<uint32_t>{
        1, 2 * kSampleEvery - 1}(_random);
    }

    Executed& _slot;
    uint64_t _calls = 0;
    uint64_t _docs = 0;
    uint64_t _ticks = 0;
    uint64_t _timed = 0;
    std::minstd_rand _random;
    uint32_t _countdown = NextGap();
  };

  template<typename Base>
  class Profiled : public Base {
   public:
    using Ptr = typename Base::ptr;

    Profiled(Ptr inner, Executed& slot)
      : _inner{std::move(inner)}, _counter{slot} {}

    Ptr Release() {
      _counter.Unplan();
      return std::move(_inner);
    }

   protected:
    Ptr _inner;
    Counter _counter;
  };

  QueryProfile(std::span<const Filter* const> filters, bool nested);

  QueryBuilder::ptr Prepare(const Filter& filter, const SubReader& segment,
                            const PrepareContext& ctx);

  const Prepared& Node(uint32_t node) const noexcept { return _nodes[node]; }

  Executed& Plan(uint32_t node, const std::type_info& type);

  std::vector<ExecutedNode> Slots() const;

  uint64_t TimerTicks() const noexcept { return _timer_ticks; }

  static uint64_t WindowDocs(const uint64_t* mask, doc_id_t min,
                             doc_id_t max) noexcept;

  template<typename Wrapper, typename Ptr>
  Ptr WrapAs(Ptr node, uint32_t id) {
    if (id == 0 || !_nested) {
      return node;
    }
    auto& slot = Plan(id, typeid(*node));
    return memory::make_managed<Wrapper>(std::move(node), slot);
  }

  lead::Node::ptr Wrap(lead::Node::ptr node, uint32_t id);
  probe::Node::ptr Wrap(probe::Node::ptr node, uint32_t id);
  fill::Node::ptr Wrap(fill::Node::ptr node, uint32_t id);

  docs::Root::ptr WrapRoot(docs::Root::ptr root, uint32_t id, uint64_t ticks);
  hits::Root::ptr WrapRoot(hits::Root::ptr root, uint32_t id, uint64_t ticks);
  count::Root::ptr WrapRoot(count::Root::ptr root, uint32_t id, uint64_t ticks);
  top::Root::ptr WrapRoot(top::Root::ptr root, uint32_t id, uint64_t ticks);
  ann::Root::ptr WrapRoot(ann::Root::ptr root, uint32_t id, uint64_t ticks);
  fill::Node::ptr WrapRoot(fill::Node::ptr root, uint32_t id, uint64_t ticks);

 private:
  template<typename Wrapper, typename Ptr>
  Ptr WrapRootAs(Ptr root, uint32_t id, uint64_t ticks);

  containers::FlatHashMap<const Filter*, uint32_t> _ids;
  std::unique_ptr<Prepared[]> _nodes;
  mutable absl::Mutex _mutex;
  std::unique_ptr<std::list<Executed>[]> _executed;
  uint32_t _size = 0;
  uint64_t _timer_ticks = 0;
  bool _nested = false;
};

inline QueryBuilder::ptr PrepareFilter(const Filter& filter,
                                       const SubReader& segment,
                                       const PrepareContext& ctx) {
  if (!ctx.profile) [[likely]] {
    return filter.PrepareSegment(segment, ctx);
  }
  return ctx.profile->Prepare(filter, segment, ctx);
}

template<typename Ptr>
Ptr ProfilePlan(const QueryBuilder& query, Ptr node) {
  if constexpr (requires(QueryProfile& profile, Ptr part) {
                  profile.Wrap(std::move(part), uint32_t{});
                }) {
    if (auto* profile = query.Profile()) [[unlikely]] {
      if (node) {
        return profile->Wrap(std::move(node), query.ProfileNode());
      }
    }
  }
  return node;
}

template<typename Make>
auto ProfileRoot(const QueryBuilder& query, Make&& make) {
  auto* profile = query.Profile();
  if (!profile) [[likely]] {
    return make();
  }
  const auto start = duckdb::TimePoint::Now();
  auto root = make();
  const auto ticks = static_cast<uint64_t>(duckdb::TimePoint::Now() - start);
  if (!root) {
    return root;
  }
  return profile->WrapRoot(std::move(root), query.ProfileNode(), ticks);
}

}  // namespace irs
