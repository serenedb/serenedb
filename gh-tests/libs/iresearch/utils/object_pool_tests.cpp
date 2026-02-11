////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <array>
#include <basics/thread_utils.hpp>
#include <shared_mutex>
#include <thread>

#include "basics/misc.hpp"
#include "basics/object_pool.hpp"
#include "gtest/gtest.h"

using namespace std::chrono_literals;
namespace irs {

// A fixed size pool of objects.
// if the pool is empty then a new object is created via make(...)
// if an object is available in a pool then in is returned but tracked
// by the pool.
// when the object is released it is placed back into the pool
// Object 'ptr' that evaluate to false after make(...) are not considered
// part of the pool.
template<typename T>
class BoundedObjectPool {
 public:
  using element_type = typename T::ptr::element_type;

 private:
  struct Slot : util::Noncopyable {
    BoundedObjectPool* owner;
    typename T::ptr ptr;
    std::atomic<element_type*> value{};
  };

  using stack = ConcurrentStack<Slot>;
  using node_type = typename stack::NodeType;

  // Private because we want to disallow upcasts to std::unique_ptr<...>.
  class Releaser final {
   public:
    explicit Releaser(node_type* slot) noexcept : _slot{slot} {}

    void operator()(element_type*) noexcept {
      SDB_ASSERT(_slot && _slot->value.owner);  // Ensured by emplace(...)
      _slot->value.owner->unlock(*_slot);
    }

   private:
    node_type* _slot;
  };

 public:
  // Represents a control object of unbounded_object_pool
  using ptr = PoolControlPtr<element_type, Releaser>;

  explicit BoundedObjectPool(size_t size) : _pool{std::max(size_t(1), size)} {
    // initialize pool
    for (auto& node : _pool) {
      auto& slot = node.value;
      slot.owner = this;

      _free_list.push(node);
    }
  }

  template<typename... Args>
  ptr emplace(Args&&... args) {
    node_type* head = nullptr;

    while (!(head = _free_list.pop())) {
      wait_for_free_slots();
    }

    auto& slot = head->value;

    auto* p = slot.value.load(std::memory_order_acquire);

    if (!p) {
      auto& value = slot.ptr;

      try {
        value = T::make(std::forward<Args>(args)...);
      } catch (...) {
        _free_list.push(*head);
        _cond.notify_all();
        throw;
      }

      p = value.get();

      if (p) {
        slot.value.store(p, std::memory_order_release);
        return ptr(p, Releaser{head});
      }

      _free_list.push(*head);  // put empty slot back into the free list
      _cond.notify_all();

      return ptr(nullptr, Releaser{nullptr});
    }

    return ptr(p, Releaser{head});
  }

  size_t size() const noexcept { return _pool.size(); }

  template<typename Visitor>
  bool visit(const Visitor& visitor) {
    return const_cast<const BoundedObjectPool&>(*this).visit(visitor);
  }

  template<typename Visitor>
  bool visit(const Visitor& visitor) const {
    stack list;

    Finally release_all = [this, &list]() noexcept {
      while (auto* head = list.pop()) {
        _free_list.push(*head);
      }
    };

    auto size = _pool.size();

    while (size) {
      node_type* head = nullptr;

      while (!(head = _free_list.pop())) {
        wait_for_free_slots();
      }

      list.push(*head);

      auto& obj = head->value.ptr;

      if (obj && !visitor(*obj)) {
        return false;
      }

      --size;
    }

    return true;
  }

 private:
  void wait_for_free_slots() const {
    using namespace std::chrono_literals;

    std::unique_lock lock{_mutex};

    if (_free_list.empty()) {
      _cond.wait_for(lock, 100ms);
    }
  }

  void unlock(node_type& slot) const {
    _free_list.push(slot);
    _cond.notify_all();
  }

  mutable std::condition_variable _cond;
  mutable std::mutex _mutex;
  mutable std::vector<node_type> _pool;
  mutable stack _free_list;
};

// Convenient class storing value and associated read-write lock
template<typename T>
class AsyncValue {
 public:
  using value_type = T;

  template<typename... Args>
  explicit AsyncValue(Args&&... args) : _value{std::forward<Args>(args)...} {}

  const value_type& Value() const noexcept { return _value; }

  value_type& Value() noexcept { return _value; }

  auto LockRead() { return std::shared_lock{_lock}; }

  auto LockWrite() { return std::unique_lock{_lock}; }

 protected:
  value_type _value;
  absl::Mutex _lock;
};

// A fixed size pool of objects.
// if the pool is empty then a new object is created via make(...)
// if an object is available in a pool then in is returned and no
// longer tracked by the pool
// when the object is released it is placed back into the pool if
// space in the pool is available
// pool may be safely destroyed even there are some produced objects
// alive.
// Object 'ptr' that evaluate to false when returnd back into the pool
// will be discarded instead.
template<typename T>
class UnboundedObjectPoolVolatile : public UnboundedObjectPoolBase<T> {
 private:
  struct Generation {
    explicit Generation(UnboundedObjectPoolVolatile* owner) noexcept
      : owner{owner} {}

    // current owner (null == stale generation)
    UnboundedObjectPoolVolatile* owner;
  };

  using BaseT = UnboundedObjectPoolBase<T>;
  using GenerationT = AsyncValue<Generation>;
  using GenerationPtrT = std::shared_ptr<GenerationT>;
  using DeleterType = typename BaseT::deleter_type;

 public:
  using element_type = typename BaseT::element_type;
  using pointer = typename BaseT::pointer;

 private:
  // Private because we want to disallow upcasts to std::unique_ptr<...>.
  class Releaser final {
   public:
    explicit Releaser(GenerationPtrT&& gen) noexcept : _gen{std::move(gen)} {}

    void operator()(pointer p) noexcept {
      SDB_ASSERT(p);     // Ensured by std::unique_ptr<...>
      SDB_ASSERT(_gen);  // Ensured by emplace(...)

      Finally release_gen = [this]() noexcept { _gen = nullptr; };

      // do not remove scope!!!
      // variable 'lock' below must be destroyed before 'gen_'
      {
        auto lock = _gen->LockRead();

        if (auto* owner = _gen->Value().owner; owner) {
          owner->release(p);
          return;
        }
      }

      // clear object oustide the lock if necessary
      DeleterType{}(p);
    }

   private:
    GenerationPtrT _gen;
  };

 public:
  // Represents a control object of unbounded_object_pool
  using ptr = PoolControlPtr<element_type, Releaser>;

  explicit UnboundedObjectPoolVolatile(size_t size = 0)
    : BaseT{size}, _gen{std::make_shared<GenerationT>(this)} {}

  // FIXME check what if
  //
  // unbounded_object_pool_volatile p0, p1;
  // thread0: p0.clear();
  // thread1: unbounded_object_pool_volatile p1(std::move(p0));
  UnboundedObjectPoolVolatile(UnboundedObjectPoolVolatile&& rhs) noexcept
    : BaseT{std::move(rhs)} {
    _gen = std::atomic_load(&rhs._gen);

    auto lock = _gen->LockWrite();
    _gen->Value().owner = this;  // change owner

    this->_free_slots = std::move(rhs._free_slots);
    this->_free_objects = std::move(rhs._free_objects);
  }

  ~UnboundedObjectPoolVolatile() noexcept {
    // prevent existing elements from returning into the pool
    // if pool doesn't own generation anymore
    {
      const auto gen = std::atomic_load(&_gen);
      auto lock = gen->LockWrite();

      auto& value = gen->Value();

      if (value.owner == this) {
        value.owner = nullptr;
      }
    }

    Clear(false);
  }

  // Clears all cached objects and optionally prevents already created
  // objects from returning into the pool.
  // `new_generation` if true, prevents already created objects from
  // returning into the pool, otherwise just clears all cached objects.
  void Clear(bool new_generation = false) {
    // prevent existing elements from returning into the pool
    if (new_generation) {
      {
        auto gen = std::atomic_load(&_gen);
        auto lock = gen->LockWrite();
        gen->Value().owner = nullptr;
      }

      // mark new generation
      std::atomic_store(&_gen, std::make_shared<GenerationT>(this));
    }

    typename BaseT::node* head = nullptr;

    // reset all cached instances
    while ((head = this->_free_objects.pop())) {
      auto p = std::exchange(head->value.value, nullptr);
      SDB_ASSERT(p);
      DeleterType{}(p);
      this->_free_slots.push(*head);
    }
  }

  template<typename... Args>
  ptr Emplace(Args&&... args) {
    // retrieve before seek/instantiate
    auto gen = std::atomic_load(&_gen);
    auto value = this->acquire(std::forward<Args>(args)...);

    if (value) {
      return {value, Releaser{std::move(gen)}};
    }

    return {nullptr, Releaser{nullptr}};
  }

  size_t GenerationSize() const noexcept {
    const auto use_count = std::atomic_load(&_gen).use_count();
    SDB_ASSERT(use_count >= 2);
    return use_count - 2;  // -1 for temporary object, -1 for this->_gen
  }

 private:
  // disallow move assignment
  UnboundedObjectPoolVolatile& operator=(UnboundedObjectPoolVolatile&&) =
    delete;

  GenerationPtrT _gen;  // current generation
};

}  // namespace irs
namespace tests {

template<bool Shared, bool ReturnNull, size_t SleepSec>
struct TestObject {
  using ptr = std::conditional_t<Shared, std::shared_ptr<TestObject>,
                                 std::unique_ptr<TestObject>>;

  static std::atomic<size_t> gTotalCount;  // # number of objects created
  static std::atomic<size_t> gMakeCount;   // # number of objects created

  static ptr make(int i) {
    ++gMakeCount;

    if constexpr (SleepSec > 0) {
      std::this_thread::sleep_for(std::chrono::seconds{SleepSec});
    }

    if constexpr (ReturnNull) {
      return nullptr;
    }

    return ptr{new TestObject{i}};
  }

  static ptr make() { return make(0); }

  explicit TestObject(int i) : id{i} { ++gTotalCount; }

  ~TestObject() { --gTotalCount; }

  int id;
};

template<bool Shared, bool ReturnNull, size_t SleepSec>
std::atomic<size_t> TestObject<Shared, ReturnNull, SleepSec>::gTotalCount{};

template<bool Shared, bool ReturnNull, size_t SleepSec>
std::atomic<size_t> TestObject<Shared, ReturnNull, SleepSec>::gMakeCount{};

using TestSlowSobject = TestObject<true, false, 2>;
using TestSlowUobject = TestObject<false, false, 2>;
using TestSobject = TestObject<true, false, 0>;
using TestUobject = TestObject<false, false, 0>;
using TestSobjectNullptr = TestObject<true, true, 0>;
using TestUobjectNullptr = TestObject<false, true, 0>;

}  // namespace tests

using namespace tests;

TEST(bounded_object_pool_tests, check_total_number_of_instances) {
  const size_t max_count = 2;
  irs::BoundedObjectPool<TestSlowSobject> pool(max_count);

  std::mutex mutex;
  std::condition_variable ready_cv;
  bool ready{false};

  std::atomic<size_t> id{};
  TestSlowSobject::gTotalCount = 0;

  auto job = [&mutex, &ready_cv, &pool, &ready, &id]() {
    // wait for all threads to be ready
    {
      auto lock = std::unique_lock(mutex);

      while (!ready) {
        ready_cv.wait(lock);
      }
    }

    pool.emplace(id++);
  };

  auto job_shared = [&mutex, &ready_cv, &pool, &ready, &id]() {
    // wait for all threads to be ready
    {
      auto lock = std::unique_lock(mutex);

      while (!ready) {
        ready_cv.wait(lock);
      }
    }

    pool.emplace(id++);
  };

  const size_t threads_count = 32;
  std::vector<std::thread> threads;

  for (size_t i = 0; i < threads_count / 2; ++i) {
    threads.emplace_back(job);
    threads.emplace_back(job_shared);
  }

  // ready
  {
    auto lock = std::unique_lock(mutex);
    ready = true;
  }
  ready_cv.notify_all();

  for (auto& thread : threads) {
    thread.join();
  }

  ASSERT_LE(TestSlowSobject::gTotalCount.load(), max_count);
}

TEST(bounded_object_pool_tests, test_sobject_pool) {
  // block on full pool
  {
    std::condition_variable cond;
    std::mutex mutex;
    irs::BoundedObjectPool<TestSobject> pool(1);
    auto obj = pool.emplace(1);

    {
      auto lock = std::unique_lock(mutex);
      std::atomic<bool> emplace(false);
      std::thread thread([&cond, &mutex, &pool, &emplace]() -> void {
        auto obj = pool.emplace(2);
        emplace = true;
        auto lock = std::unique_lock(mutex);
        cond.notify_all();
      });

      auto result =
        cond.wait_for(lock, 1000ms);  // assume thread blocks in 1000ms

      // As declaration for wait_for contains "It may also be unblocked
      // spuriously." for all platforms
      while (!emplace && result == std::cv_status::no_timeout)
        result = cond.wait_for(lock, 1000ms);

      ASSERT_EQ(std::cv_status::timeout, result);
      // ^^^ expecting timeout because pool should block indefinitely
      obj.reset();
      lock.unlock();
      thread.join();
    }
  }

  // null objects not considered part of pool
  {
    std::condition_variable cond;
    std::mutex mutex;
    irs::BoundedObjectPool<TestSobjectNullptr> pool(2);
    TestSobjectNullptr::gMakeCount = 0;
    auto obj = pool.emplace();
    ASSERT_FALSE(obj);
    ASSERT_EQ(1, TestSobjectNullptr::gMakeCount);

    {
      auto lock = std::unique_lock(mutex);
      std::atomic<bool> emplace(false);
      std::thread thread([&cond, &mutex, &pool, &emplace]() -> void {
        auto obj = pool.emplace();
        ASSERT_FALSE(obj);
        ASSERT_EQ(2, TestSobjectNullptr::gMakeCount);
        emplace = true;
        auto lock = std::unique_lock(mutex);
        cond.notify_all();
      });

      auto result =
        cond.wait_for(lock, 1000ms);  // assume thread blocks in 1000ms

      // As declaration for wait_for contains "It may also be unblocked
      // spuriously." for all platforms
      while (!emplace && result == std::cv_status::no_timeout)
        result = cond.wait_for(lock, 1000ms);

      ASSERT_TRUE(emplace);

      obj.reset();
      lock.unlock();
      thread.join();
    }
  }

  // test object reuse
  {
    irs::BoundedObjectPool<TestSobject> pool(1);
    auto obj = pool.emplace(1);
    ASSERT_TRUE(obj);
    auto* obj_ptr = obj.get();

    ASSERT_EQ(1, obj->id);
    obj.reset();
    ASSERT_FALSE(obj);
    auto obj_shared = pool.emplace(2);
    ASSERT_EQ(1, obj_shared->id);
    ASSERT_EQ(obj_ptr, obj_shared.get());
  }

  // test visitation
  {
    irs::BoundedObjectPool<TestSobject> pool(1);
    auto obj = pool.emplace(1);
    ASSERT_TRUE(obj);
    std::condition_variable cond;
    std::mutex mutex;
    auto lock = std::unique_lock(mutex);
    std::atomic<bool> visit(false);
    std::thread thread([&cond, &mutex, &pool, &visit]() -> void {
      auto visitor = [](TestSobject&) -> bool { return true; };
      pool.visit(visitor);
      visit = true;
      auto lock = std::unique_lock(mutex);
      cond.notify_all();
    });
    auto result =
      cond.wait_for(lock, 1000ms);  // assume thread finishes in 1000ms

    // As declaration for wait_for contains "It may also be unblocked
    // spuriously." for all platforms
    while (!visit && result == std::cv_status::no_timeout)
      result = cond.wait_for(lock, 1000ms);

    obj.reset();
    ASSERT_FALSE(obj);

    if (lock) {
      lock.unlock();
    }

    thread.join();
    ASSERT_EQ(
      std::cv_status::timeout,
      result);  // check only after joining with thread to avoid early exit
    // ^^^ expecting timeout because pool should block indefinitely
  }
}

TEST(bounded_object_pool_tests, test_uobject_pool) {
  // block on full pool
  {
    std::condition_variable cond;
    std::mutex mutex;
    irs::BoundedObjectPool<TestUobject> pool(1);
    auto obj = pool.emplace(1);

    {
      auto lock = std::unique_lock(mutex);
      std::atomic<bool> emplace(false);
      std::thread thread([&cond, &mutex, &pool, &emplace]() -> void {
        auto obj = pool.emplace(2);
        emplace = true;
        auto lock = std::unique_lock(mutex);
        cond.notify_all();
      });

      auto result =
        cond.wait_for(lock, 1000ms);  // assume thread blocks in 1000ms

      // As declaration for wait_for contains "It may also be unblocked
      // spuriously." for all platforms
      while (!emplace && result == std::cv_status::no_timeout)
        result = cond.wait_for(lock, 1000ms);

      ASSERT_EQ(std::cv_status::timeout, result);
      // ^^^ expecting timeout because pool should block indefinitely
      obj.reset();
      obj.reset();
      lock.unlock();
      thread.join();
    }
  }

  // null objects not considered part of pool
  {
    std::condition_variable cond;
    std::mutex mutex;
    irs::BoundedObjectPool<TestUobjectNullptr> pool(2);
    TestUobjectNullptr::gMakeCount = 0;
    auto obj = pool.emplace();
    ASSERT_FALSE(obj);
    ASSERT_EQ(1, TestUobjectNullptr::gMakeCount);

    {
      auto lock = std::unique_lock(mutex);
      std::thread thread([&cond, &mutex, &pool]() -> void {
        auto lock = std::unique_lock(mutex);
        auto obj = pool.emplace();
        ASSERT_FALSE(obj);
        ASSERT_EQ(2, TestUobjectNullptr::gMakeCount);
        cond.notify_all();
      });

      ASSERT_EQ(std::cv_status::no_timeout, cond.wait_for(lock, 1000ms));
      obj.reset();
      lock.unlock();
      thread.join();
    }
  }

  // test object reuse
  {
    irs::BoundedObjectPool<TestUobject> pool(1);
    auto obj = pool.emplace(1);
    ASSERT_TRUE(obj);
    auto* obj_ptr = obj.get();

    ASSERT_EQ(1, obj->id);
    obj.reset();
    ASSERT_FALSE(obj);
    obj = pool.emplace(2);
    ASSERT_TRUE(obj);
    ASSERT_EQ(1, obj->id);
    ASSERT_EQ(obj_ptr, obj.get());
  }

  // test visitation
  {
    irs::BoundedObjectPool<TestUobject> pool(1);
    auto obj = pool.emplace(1);
    std::condition_variable cond;
    std::mutex mutex;
    auto lock = std::unique_lock(mutex);
    std::atomic<bool> visit(false);
    std::thread thread([&cond, &mutex, &pool, &visit]() -> void {
      auto visitor = [](TestUobject&) -> bool { return true; };
      pool.visit(visitor);
      visit = true;
      auto lock = std::unique_lock(mutex);
      cond.notify_all();
    });
    auto result =
      cond.wait_for(lock, 1000ms);  // assume thread finishes in 1000ms

    // As declaration for wait_for contains "It may also be unblocked
    // spuriously." for all platforms
    while (!visit && result == std::cv_status::no_timeout)
      result = cond.wait_for(lock, 1000ms);

    obj.reset();

    if (lock) {
      lock.unlock();
    }

    thread.join();
    ASSERT_EQ(std::cv_status::timeout, result);
    // ^^^ expecting timeout because pool should block indefinitely
  }
}

TEST(unbounded_object_pool_tests, construct) {
  irs::UnboundedObjectPool<TestUobject> pool(42);
  ASSERT_EQ(42, pool.size());
}

TEST(unbounded_object_pool_tests, check_total_number_of_cached_instances) {
  constexpr size_t kMaxCount = 8;
  irs::UnboundedObjectPool<TestUobject> pool(kMaxCount);

  std::mutex mutex;
  std::condition_variable ready_cv;
  bool ready{false};

  std::atomic<size_t> id{};
  TestUobject::gTotalCount = 0;

  auto job = [&mutex, &ready_cv, &pool, &ready, &id]() {
    // wait for all threads to be ready
    {
      auto lock = std::unique_lock(mutex);

      while (!ready) {
        ready_cv.wait(lock);
      }
    }

    for (size_t i = 0; i < 100000; ++i) {
      auto p = pool.emplace(id++);
      ASSERT_TRUE(p->id >= 0);
    }
  };

  auto job_shared = [&mutex, &ready_cv, &pool, &ready, &id]() {
    // wait for all threads to be ready
    {
      auto lock = std::unique_lock(mutex);

      while (!ready) {
        ready_cv.wait(lock);
      }
    }

    for (size_t i = 0; i < 100000; ++i) {
      auto p = pool.emplace(id++);
      ASSERT_TRUE(p->id >= 0);
    }
  };

  constexpr size_t kThreadsCount = 32;
  std::vector<std::thread> threads;

  for (size_t i = 0; i < kThreadsCount / 2; ++i) {
    threads.emplace_back(job);
    threads.emplace_back(job_shared);
  }

  // ready
  {
    auto lock = std::unique_lock(mutex);
    ready = true;
  }
  ready_cv.notify_all();

  for (auto& thread : threads) {
    thread.join();
  }

  ASSERT_LE(TestSlowUobject::gTotalCount.load(), kMaxCount);
}

TEST(unbounded_object_pool_tests, test_uobject_pool_1) {
  // create new untracked object on full pool
  {
    std::condition_variable cond;
    std::mutex mutex;
    irs::UnboundedObjectPool<TestUobject> pool(1);
    std::shared_ptr<TestUobject> obj = pool.emplace(1);

    {
      auto lock = std::unique_lock(mutex);
      std::thread thread([&cond, &mutex, &pool]() -> void {
        auto obj = pool.emplace(2);
        auto lock = std::unique_lock(mutex);
        cond.notify_all();
      });
      ASSERT_EQ(
        std::cv_status::no_timeout,
        cond.wait_for(lock, 1000ms));  // assume threads start within 1000msec
      lock.unlock();
      thread.join();
    }
  }

  // test empty pool
  {
    irs::UnboundedObjectPool<TestUobject> pool;
    ASSERT_EQ(0, pool.size());
    auto obj = pool.emplace(1);
    ASSERT_TRUE(obj);

    ASSERT_EQ(1, obj->id);
    obj.reset();
    ASSERT_FALSE(obj);
    ASSERT_EQ(nullptr, obj.get());
    auto obj_shared = pool.emplace(2);
    ASSERT_TRUE(bool(obj_shared));
    ASSERT_EQ(2, obj_shared->id);
  }

  // null objects not considered part of pool
  {
    irs::UnboundedObjectPool<TestUobjectNullptr> pool(2);
    TestUobjectNullptr::gMakeCount = 0;
    auto obj = pool.emplace();
    ASSERT_FALSE(obj);
    ASSERT_EQ(1, TestUobjectNullptr::gMakeCount);
    obj.reset();
    std::shared_ptr<TestUobjectNullptr> obj_shared = pool.emplace();
    ASSERT_FALSE(obj);
    ASSERT_EQ(2, TestUobjectNullptr::gMakeCount);
    obj.reset();
  }

  // test object reuse
  {
    irs::UnboundedObjectPool<TestUobject> pool(1);
    auto obj = pool.emplace(1);
    ASSERT_TRUE(obj);
    auto* obj_ptr = obj.get();

    ASSERT_EQ(1, obj->id);
    obj.reset();
    ASSERT_FALSE(obj);
    ASSERT_EQ(nullptr, obj.get());
    std::shared_ptr<TestUobject> obj_shared{pool.emplace(2)};
    ASSERT_TRUE(bool(obj_shared));
    ASSERT_EQ(1, obj_shared->id);
    ASSERT_EQ(obj_ptr, obj_shared.get());
  }

  // ensure untracked object is not placed back in the pool
  {
    irs::UnboundedObjectPool<TestUobject> pool(1);
    auto obj0 = pool.emplace(1);
    ASSERT_TRUE(obj0);
    std::shared_ptr<TestUobject> obj1{pool.emplace(2)};
    ASSERT_TRUE(bool(obj1));
    auto* obj0_ptr = obj0.get();

    ASSERT_EQ(1, obj0->id);
    ASSERT_EQ(2, obj1->id);
    ASSERT_NE(obj0_ptr, obj1.get());
    obj0.reset();  // will be placed back in pool first
    ASSERT_FALSE(obj0);
    ASSERT_EQ(nullptr, obj0.get());
    obj1.reset();  // will push obj1 out of the pool
    ASSERT_FALSE(obj1);
    ASSERT_EQ(nullptr, obj1.get());

    std::shared_ptr<TestUobject> obj2{pool.emplace(3)};
    ASSERT_TRUE(bool(obj2));
    auto obj3 = pool.emplace(4);
    ASSERT_TRUE(obj3);
    ASSERT_EQ(1, obj2->id);
    ASSERT_EQ(4, obj3->id);
    ASSERT_EQ(obj0_ptr, obj2.get());
    ASSERT_NE(obj0_ptr, obj3.get());
    // obj3 may have been allocated in the same addr as obj1, so can't safely
    // validate
  }

  // test pool clear
  {
    irs::UnboundedObjectPool<TestUobject> pool(1);
    auto obj = pool.emplace(1);
    ASSERT_TRUE(obj);
    auto* obj_ptr = obj.get();

    ASSERT_EQ(1, obj->id);
    obj.reset();
    ASSERT_FALSE(obj);
    ASSERT_EQ(nullptr, obj.get());
    obj = pool.emplace(2);
    ASSERT_TRUE(obj);
    ASSERT_EQ(1, obj->id);
    ASSERT_EQ(obj_ptr, obj.get());

    pool.clear();  // will clear objects inside the pool only
    obj.reset();
    ASSERT_FALSE(obj);
    ASSERT_EQ(nullptr, obj.get());
    obj = pool.emplace(2);
    ASSERT_TRUE(obj);
    ASSERT_EQ(1, obj->id);
    ASSERT_EQ(obj_ptr, obj.get());  // same object

    obj.reset();
    ASSERT_FALSE(obj);
    ASSERT_EQ(nullptr, obj.get());
    pool.clear();           // will clear objects inside the pool only
    obj = pool.emplace(3);  // 'obj' should not be reused
    ASSERT_TRUE(obj);
    ASSERT_EQ(3, obj->id);
  }
}

TEST(unbounded_object_pool_tests, test_uobject_pool_2) {
  // create new untracked object on full pool
  {
    std::condition_variable cond;
    std::mutex mutex;
    irs::UnboundedObjectPool<TestUobject> pool(1);
    std::shared_ptr<TestUobject> obj = pool.emplace(1);

    {
      auto lock = std::unique_lock(mutex);
      std::thread thread([&cond, &mutex, &pool]() -> void {
        auto obj = pool.emplace(2);
        auto lock = std::unique_lock(mutex);
        cond.notify_all();
      });
      ASSERT_EQ(
        std::cv_status::no_timeout,
        cond.wait_for(lock, 1000ms));  // assume threads start within 1000msec
      lock.unlock();
      thread.join();
    }
  }

  // null objects not considered part of pool
  {
    irs::UnboundedObjectPool<TestUobjectNullptr> pool(2);
    TestUobjectNullptr::gMakeCount = 0;
    auto obj = pool.emplace();
    ASSERT_FALSE(obj);
    ASSERT_EQ(1, TestUobjectNullptr::gMakeCount);
    obj.reset();
    std::shared_ptr<TestUobjectNullptr> obj_shared = pool.emplace();
    ASSERT_FALSE(obj);
    ASSERT_EQ(2, TestUobjectNullptr::gMakeCount);
    obj.reset();
  }

  // test object reuse
  {
    irs::UnboundedObjectPool<TestUobject> pool(1);
    auto obj = pool.emplace(1);
    ASSERT_TRUE(obj);
    auto* obj_ptr = obj.get();

    ASSERT_EQ(1, obj->id);
    obj.reset();
    ASSERT_FALSE(obj);
    ASSERT_EQ(nullptr, obj.get());
    std::shared_ptr<TestUobject> obj_shared{pool.emplace(2)};
    ASSERT_TRUE(bool(obj_shared));
    ASSERT_EQ(1, obj_shared->id);
    ASSERT_EQ(obj_ptr, obj_shared.get());
  }

  // ensure untracked object is not placed back in the pool
  {
    irs::UnboundedObjectPool<TestUobject> pool(1);
    auto obj0 = pool.emplace(1);
    ASSERT_TRUE(obj0);
    std::shared_ptr<TestUobject> obj1{pool.emplace(2)};
    ASSERT_TRUE(bool(obj1));
    auto* obj0_ptr = obj0.get();

    ASSERT_EQ(1, obj0->id);
    ASSERT_EQ(2, obj1->id);
    ASSERT_NE(obj0_ptr, obj1.get());
    obj0.reset();  // will be placed back in pool first
    ASSERT_FALSE(obj0);
    ASSERT_EQ(nullptr, obj0.get());
    obj1.reset();  // will push obj1 out of the pool
    ASSERT_FALSE(obj1);
    ASSERT_EQ(nullptr, obj1.get());

    std::shared_ptr<TestUobject> obj2{pool.emplace(3)};
    ASSERT_TRUE(bool(obj2));
    auto obj3 = pool.emplace(4);
    ASSERT_TRUE(obj3);
    ASSERT_EQ(1, obj2->id);
    ASSERT_EQ(4, obj3->id);
    ASSERT_EQ(obj0_ptr, obj2.get());
    ASSERT_NE(obj0_ptr, obj3.get());
    // obj3 may have been allocated in the same addr as obj1, so can't safely
    // validate
  }

  // test pool clear
  {
    irs::UnboundedObjectPool<TestUobject> pool(1);
    auto obj = pool.emplace(1);
    ASSERT_TRUE(obj);
    auto* obj_ptr = obj.get();

    ASSERT_EQ(1, obj->id);
    obj.reset();
    ASSERT_FALSE(obj);
    ASSERT_EQ(nullptr, obj.get());
    obj = pool.emplace(2);
    ASSERT_TRUE(obj);
    ASSERT_EQ(1, obj->id);
    ASSERT_EQ(obj_ptr, obj.get());

    pool.clear();  // will clear objects inside the pool only
    obj.reset();
    ASSERT_FALSE(obj);
    ASSERT_EQ(nullptr, obj.get());
    obj = pool.emplace(2);
    ASSERT_TRUE(obj);
    ASSERT_EQ(1, obj->id);
    ASSERT_EQ(obj_ptr, obj.get());  // same object

    obj.reset();
    ASSERT_FALSE(obj);
    ASSERT_EQ(nullptr, obj.get());
    pool.clear();           // will clear objects inside the pool only
    obj = pool.emplace(3);  // 'obj' should not be reused
    ASSERT_TRUE(obj);
    ASSERT_EQ(3, obj->id);
  }
}

TEST(unbounded_object_pool_tests, control_objectb_move) {
  irs::UnboundedObjectPool<TestUobject> pool(2);
  ASSERT_EQ(2, pool.size());

  // move constructor
  {
    auto moved = pool.emplace(1);
    ASSERT_TRUE(moved);
    ASSERT_NE(nullptr, moved.get());
    ASSERT_EQ(1, moved->id);

    auto obj = std::move(moved);
    ASSERT_FALSE(moved);
    ASSERT_EQ(nullptr, moved);
    ASSERT_EQ(moved, nullptr);
    ASSERT_TRUE(obj);
    ASSERT_EQ(1, obj->id);
  }

  // move assignment
  {
    auto moved = pool.emplace(1);
    ASSERT_TRUE(moved);
    ASSERT_NE(nullptr, moved);
    ASSERT_NE(moved, nullptr);
    ASSERT_EQ(1, moved->id);
    auto* moved_ptr = moved.get();

    auto obj = pool.emplace(2);
    ASSERT_TRUE(obj);
    ASSERT_EQ(2, obj->id);

    obj = std::move(moved);
    ASSERT_TRUE(obj);
    ASSERT_FALSE(moved);
    ASSERT_EQ(nullptr, moved.get());
    ASSERT_EQ(obj.get(), moved_ptr);
    ASSERT_EQ(1, obj->id);
  }
}

TEST(unbounded_object_pool_tests, control_object_move_pools) {
  irs::UnboundedObjectPool<TestUobject> pool(2);
  TestUobject::gMakeCount = 0;
  {
    irs::UnboundedObjectPool<TestUobject> pool_other(2);
    auto from_other = pool_other.emplace(1);
    ASSERT_EQ(1, TestUobject::gMakeCount);
    {
      auto from_this = pool.emplace(1);
      ASSERT_EQ(2, TestUobject::gMakeCount);
      from_other = std::move(from_this);
    }
    // from_other should be returned to pool_other now
    // and no new make should be called
    auto from_other2 = pool_other.emplace(1);
    ASSERT_EQ(2, TestUobject::gMakeCount);
  }
}

TEST(unbounded_object_pool_volatile_tests, construct) {
  irs::UnboundedObjectPoolVolatile<TestUobject> pool(42);
  ASSERT_EQ(42, pool.size());
  ASSERT_EQ(0, pool.GenerationSize());
}

TEST(unbounded_object_pool_volatile_tests, move) {
  irs::UnboundedObjectPoolVolatile<TestUobject> moved(2);
  ASSERT_EQ(2, moved.size());
  ASSERT_EQ(0, moved.GenerationSize());

  auto obj0 = moved.Emplace(1);
  ASSERT_EQ(1, moved.GenerationSize());
  ASSERT_TRUE(obj0);
  ASSERT_NE(nullptr, obj0.get());
  ASSERT_EQ(1, obj0->id);

  irs::UnboundedObjectPoolVolatile<TestUobject> pool(std::move(moved));
  ASSERT_EQ(2, moved.GenerationSize());
  ASSERT_EQ(2, pool.GenerationSize());

  auto obj1 = pool.Emplace(2);
  ASSERT_EQ(3, pool.GenerationSize());  // +1 for moved
  ASSERT_EQ(3, moved.GenerationSize());
  ASSERT_TRUE(obj1);
  ASSERT_NE(nullptr, obj1.get());
  ASSERT_EQ(2, obj1->id);

  // insert via moved pool
  auto obj2 = moved.Emplace(3);
  ASSERT_EQ(4, pool.GenerationSize());
  ASSERT_EQ(4, moved.GenerationSize());
  ASSERT_TRUE(obj2);
  ASSERT_NE(nullptr, obj2.get());
  ASSERT_EQ(3, obj2->id);
}

TEST(unbounded_object_pool_volatile_tests, control_object_move) {
  irs::UnboundedObjectPoolVolatile<TestUobject> pool(2);
  ASSERT_EQ(2, pool.size());
  ASSERT_EQ(0, pool.GenerationSize());

  // move constructor
  {
    auto moved = pool.Emplace(1);
    ASSERT_EQ(1, pool.GenerationSize());
    ASSERT_TRUE(moved);
    ASSERT_NE(nullptr, moved);
    ASSERT_EQ(1, moved->id);

    auto obj = std::move(moved);
    ASSERT_FALSE(moved);
    ASSERT_EQ(nullptr, moved);
    ASSERT_EQ(1, pool.GenerationSize());
    ASSERT_TRUE(obj);
    ASSERT_EQ(1, obj->id);
  }

  // move assignment
  {
    auto moved = pool.Emplace(1);
    ASSERT_EQ(1, pool.GenerationSize());
    ASSERT_TRUE(moved);
    ASSERT_NE(nullptr, moved);
    ASSERT_NE(moved, nullptr);
    ASSERT_EQ(1, moved->id);
    auto* moved_ptr = moved.get();

    auto obj = pool.Emplace(2);
    ASSERT_EQ(2, pool.GenerationSize());
    ASSERT_TRUE(obj);
    ASSERT_EQ(2, obj->id);

    obj = std::move(moved);
    ASSERT_EQ(1, pool.GenerationSize());
    ASSERT_TRUE(obj);
    ASSERT_FALSE(moved);
    ASSERT_EQ(nullptr, moved);
    ASSERT_EQ(moved, nullptr);
    ASSERT_EQ(obj.get(), moved_ptr);
    ASSERT_EQ(1, obj->id);
  }

  // move between two identical pools
  {
    irs::UnboundedObjectPoolVolatile<TestUobject> pool_other(2);
    auto from_other = pool_other.Emplace(1);
    ASSERT_EQ(1, pool_other.GenerationSize());
    {
      auto from_this = pool.Emplace(3);
      ASSERT_EQ(1, pool.GenerationSize());
      from_other = std::move(from_this);
    }
    // from_other should be returned to pool_other now
    ASSERT_EQ(0, pool_other.GenerationSize());
  }

  ASSERT_EQ(0, pool.GenerationSize());
}

TEST(unbounded_object_pool_volatile_tests, test_uobject_pool_1) {
  // create new untracked object on full pool
  {
    std::condition_variable cond;
    std::mutex mutex;
    irs::UnboundedObjectPoolVolatile<TestUobject> pool(1);
    ASSERT_EQ(0, pool.GenerationSize());
    std::shared_ptr<TestUobject> obj = pool.Emplace(1);
    ASSERT_EQ(1, pool.GenerationSize());

    {
      auto lock = std::unique_lock(mutex);
      std::thread thread([&cond, &mutex, &pool]() -> void {
        auto obj = pool.Emplace(2);
        auto lock = std::unique_lock(mutex);
        cond.notify_all();
      });
      // assume threads start within 1000msec
      ASSERT_EQ(std::cv_status::no_timeout, cond.wait_for(lock, 1000ms));
      lock.unlock();
      thread.join();
    }

    ASSERT_EQ(1, pool.GenerationSize());
  }

  // test empty pool
  {
    irs::UnboundedObjectPoolVolatile<TestUobject> pool;
    ASSERT_EQ(0, pool.size());
    auto obj = pool.Emplace(1);
    ASSERT_TRUE(obj);

    ASSERT_EQ(1, obj->id);
    obj.reset();
    ASSERT_FALSE(obj);
    ASSERT_EQ(nullptr, obj.get());
    auto obj_shared = pool.Emplace(2);
    ASSERT_TRUE(bool(obj_shared));
    ASSERT_EQ(2, obj_shared->id);
  }

  // null objects not considered part of pool
  {
    irs::UnboundedObjectPoolVolatile<TestUobjectNullptr> pool(2);
    TestUobjectNullptr::gMakeCount = 0;
    auto obj = pool.Emplace();
    ASSERT_FALSE(obj);
    ASSERT_EQ(1, TestUobjectNullptr::gMakeCount);
    obj.reset();
    std::shared_ptr<TestUobjectNullptr> obj_shared = pool.Emplace();
    ASSERT_FALSE(obj);
    ASSERT_EQ(2, TestUobjectNullptr::gMakeCount);
    obj.reset();
  }

  // test object reuse
  {
    irs::UnboundedObjectPoolVolatile<TestUobject> pool(1);
    ASSERT_EQ(0, pool.GenerationSize());
    auto obj = pool.Emplace(1);
    ASSERT_EQ(1, pool.GenerationSize());
    auto* obj_ptr = obj.get();

    ASSERT_EQ(1, obj->id);
    obj.reset();
    ASSERT_FALSE(obj);
    ASSERT_EQ(nullptr, obj.get());
    ASSERT_EQ(0, pool.GenerationSize());
    std::shared_ptr<TestUobject> obj_shared{pool.Emplace(2)};
    ASSERT_EQ(1, pool.GenerationSize());
    ASSERT_EQ(1, obj_shared->id);
    ASSERT_EQ(obj_ptr, obj_shared.get());
  }

  // ensure untracked object is not placed back in the pool
  {
    irs::UnboundedObjectPoolVolatile<TestUobject> pool(1);
    ASSERT_EQ(0, pool.GenerationSize());
    auto obj0 = pool.Emplace(1);
    ASSERT_EQ(1, pool.GenerationSize());
    ASSERT_TRUE(obj0);
    std::shared_ptr<TestUobject> obj1 = pool.Emplace(2);
    ASSERT_EQ(2, pool.GenerationSize());
    ASSERT_TRUE(bool(obj1));
    auto* obj0_ptr = obj0.get();

    ASSERT_EQ(1, obj0->id);
    ASSERT_EQ(2, obj1->id);
    ASSERT_NE(obj0_ptr, obj1.get());
    obj0.reset();  // will be placed back in pool first
    ASSERT_EQ(1, pool.GenerationSize());
    ASSERT_FALSE(obj0);
    ASSERT_EQ(nullptr, obj0.get());
    obj1.reset();  // will push obj1 out of the pool
    ASSERT_EQ(0, pool.GenerationSize());
    ASSERT_FALSE(obj1);
    ASSERT_EQ(nullptr, obj1.get());

    std::shared_ptr<TestUobject> obj2 = pool.Emplace(3);
    ASSERT_EQ(1, pool.GenerationSize());
    ASSERT_TRUE(bool(obj2));
    auto obj3 = pool.Emplace(4);
    ASSERT_EQ(2, pool.GenerationSize());
    ASSERT_TRUE(obj3);
    ASSERT_EQ(1, obj2->id);
    ASSERT_EQ(4, obj3->id);
    ASSERT_EQ(obj0_ptr, obj2.get());
    ASSERT_NE(obj0_ptr, obj3.get());
    // obj3 may have been allocated in the same addr as obj1, so can't safely
    // validate
  }

  // test pool clear
  {
    irs::UnboundedObjectPoolVolatile<TestUobject> pool(1);
    ASSERT_EQ(0, pool.GenerationSize());
    auto obj_noreuse = pool.Emplace(-1);
    ASSERT_EQ(1, pool.GenerationSize());
    ASSERT_TRUE(obj_noreuse);
    auto obj = pool.Emplace(1);
    ASSERT_EQ(2, pool.GenerationSize());
    ASSERT_TRUE(obj);
    auto* obj_ptr = obj.get();

    ASSERT_EQ(1, obj->id);
    obj.reset();
    ASSERT_EQ(1, pool.GenerationSize());
    ASSERT_FALSE(obj);
    ASSERT_EQ(nullptr, obj.get());
    obj = pool.Emplace(2);
    ASSERT_EQ(2, pool.GenerationSize());
    ASSERT_TRUE(obj);
    ASSERT_EQ(1, obj->id);
    ASSERT_EQ(obj_ptr, obj.get());

    pool.Clear();  // clear existing in a pool
    ASSERT_EQ(2, pool.GenerationSize());
    obj.reset();
    ASSERT_EQ(1, pool.GenerationSize());
    ASSERT_FALSE(obj);
    ASSERT_EQ(nullptr, obj.get());
    obj = pool.Emplace(2);  // may return same memory address as obj_ptr, but
                            // constructor would have been called
    ASSERT_EQ(2, pool.GenerationSize());
    ASSERT_TRUE(obj);
    ASSERT_EQ(1, obj->id);
    ASSERT_EQ(obj_ptr, obj.get());

    pool.Clear(true);  // clear existing in a pool and prevent external object
                       // from returning back
    ASSERT_EQ(0, pool.GenerationSize());
    obj.reset();
    ASSERT_EQ(0, pool.GenerationSize());
    ASSERT_FALSE(obj);
    ASSERT_EQ(nullptr, obj.get());
    obj = pool.Emplace(2);  // may return same memory address as obj_ptr, but
                            // constructor would have been called
    ASSERT_EQ(1, pool.GenerationSize());
    ASSERT_TRUE(obj);
    ASSERT_EQ(2, obj->id);

    obj_noreuse.reset();  // reset value from previuos generation
    ASSERT_EQ(1, pool.GenerationSize());
    ASSERT_FALSE(obj_noreuse);
    ASSERT_EQ(nullptr, obj_noreuse.get());
    obj = pool.Emplace(3);  // 'obj_noreuse' should not be reused
    ASSERT_EQ(1, pool.GenerationSize());
    ASSERT_EQ(3, obj->id);
  }
}

TEST(unbounded_object_pool_volatile_tests, return_object_after_pool_destroyed) {
  auto pool =
    std::make_unique<irs::UnboundedObjectPoolVolatile<TestUobject>>(1);
  ASSERT_EQ(0, pool->GenerationSize());
  ASSERT_NE(nullptr, pool);

  auto obj = pool->Emplace(42);
  ASSERT_EQ(1, pool->GenerationSize());
  ASSERT_TRUE(obj);
  ASSERT_EQ(42, obj->id);
  std::shared_ptr<TestUobject> obj_shared = pool->Emplace(442);
  ASSERT_EQ(2, pool->GenerationSize());
  ASSERT_NE(nullptr, obj_shared);
  ASSERT_EQ(442, obj_shared->id);

  // destroy pool
  pool.reset();
  ASSERT_EQ(nullptr, pool);

  // ensure objects are still there
  ASSERT_EQ(42, obj->id);
  ASSERT_EQ(442, obj_shared->id);
}

TEST(unbounded_object_pool_volatile_tests, test_uobject_pool_2) {
  // create new untracked object on full pool
  {
    std::condition_variable cond;
    std::mutex mutex;
    irs::UnboundedObjectPoolVolatile<TestUobject> pool(1);
    auto obj = pool.Emplace(1);

    {
      auto lock = std::unique_lock(mutex);
      std::thread thread([&cond, &mutex, &pool]() -> void {
        auto obj = pool.Emplace(2);
        auto lock = std::unique_lock(mutex);
        cond.notify_all();
      });
      // assume threads start within 1000msec
      ASSERT_EQ(std::cv_status::no_timeout, cond.wait_for(lock, 1000ms));
      lock.unlock();
      thread.join();
    }
  }

  // null objects not considered part of pool
  {
    irs::UnboundedObjectPoolVolatile<TestUobjectNullptr> pool(2);
    TestUobjectNullptr::gMakeCount = 0;
    auto obj = pool.Emplace();
    ASSERT_FALSE(obj);
    ASSERT_EQ(1, TestUobjectNullptr::gMakeCount);
    obj.reset();
    std::shared_ptr<TestUobjectNullptr> obj_shared = pool.Emplace();
    ASSERT_FALSE(obj);
    ASSERT_EQ(2, TestUobjectNullptr::gMakeCount);
    obj.reset();
  }

  // test object reuse
  {
    irs::UnboundedObjectPoolVolatile<TestUobject> pool(1);
    auto obj = pool.Emplace(1);
    auto* obj_ptr = obj.get();

    ASSERT_EQ(1, obj->id);
    obj.reset();
    ASSERT_FALSE(obj);
    ASSERT_EQ(nullptr, obj.get());
    obj = pool.Emplace(2);
    ASSERT_EQ(1, obj->id);
    ASSERT_EQ(obj_ptr, obj.get());
  }

  // ensure untracked object is not placed back in the pool
  {
    irs::UnboundedObjectPoolVolatile<TestUobject> pool(1);
    auto obj0 = pool.Emplace(1);
    auto obj1 = pool.Emplace(2);
    auto* obj0_ptr = obj0.get();
    auto* obj1_ptr = obj1.get();

    ASSERT_EQ(1, obj0->id);
    ASSERT_EQ(2, obj1->id);
    ASSERT_NE(obj0_ptr, obj1.get());
    obj1.reset();  // will be placed back in pool first
    ASSERT_FALSE(obj1);
    ASSERT_EQ(nullptr, obj1.get());
    obj0.reset();  // will push obj1 out of the pool
    ASSERT_FALSE(obj0);
    ASSERT_EQ(nullptr, obj0.get());

    auto obj2 = pool.Emplace(3);
    auto obj3 = pool.Emplace(4);
    ASSERT_EQ(2, obj2->id);
    ASSERT_EQ(4, obj3->id);
    ASSERT_EQ(obj1_ptr, obj2.get());
    ASSERT_NE(obj1_ptr, obj3.get());
    // obj3 may have been allocated in the same addr as obj1, so can't safely
    // validate
  }

  // test pool clear
  {
    irs::UnboundedObjectPoolVolatile<TestUobject> pool(1);
    ASSERT_EQ(0, pool.GenerationSize());
    auto obj_noreuse = pool.Emplace(-1);
    ASSERT_EQ(1, pool.GenerationSize());
    ASSERT_TRUE(obj_noreuse);
    auto obj = pool.Emplace(1);
    ASSERT_EQ(2, pool.GenerationSize());
    ASSERT_TRUE(obj);
    auto* obj_ptr = obj.get();

    ASSERT_EQ(1, obj->id);
    obj.reset();
    ASSERT_EQ(1, pool.GenerationSize());
    ASSERT_FALSE(obj);
    ASSERT_EQ(nullptr, obj.get());
    obj = pool.Emplace(2);
    ASSERT_EQ(2, pool.GenerationSize());
    ASSERT_TRUE(obj);
    ASSERT_EQ(1, obj->id);
    ASSERT_EQ(obj_ptr, obj.get());

    pool.Clear();  // clear existing in a pool
    ASSERT_EQ(2, pool.GenerationSize());
    obj.reset();
    ASSERT_EQ(1, pool.GenerationSize());
    ASSERT_FALSE(obj);
    ASSERT_EQ(nullptr, obj.get());
    obj = pool.Emplace(2);  // may return same memory address as obj_ptr, but
                            // constructor would have been called
    ASSERT_EQ(2, pool.GenerationSize());
    ASSERT_TRUE(obj);
    ASSERT_EQ(1, obj->id);
    ASSERT_EQ(obj_ptr, obj.get());

    pool.Clear(true);  // clear existing in a pool and prevent external object
                       // from returning back
    ASSERT_EQ(0, pool.GenerationSize());
    obj.reset();
    ASSERT_EQ(0, pool.GenerationSize());
    ASSERT_FALSE(obj);
    ASSERT_EQ(nullptr, obj.get());
    obj = pool.Emplace(2);  // may return same memory address as obj_ptr, but
                            // constructor would have been called
    ASSERT_EQ(1, pool.GenerationSize());
    ASSERT_TRUE(obj);
    ASSERT_EQ(2, obj->id);

    obj_noreuse.reset();  // reset value from previuos generation
    ASSERT_EQ(1, pool.GenerationSize());
    ASSERT_FALSE(obj_noreuse);
    ASSERT_EQ(nullptr, obj_noreuse.get());
    obj = pool.Emplace(3);  // 'obj_noreuse' should not be reused
    ASSERT_EQ(1, pool.GenerationSize());
    ASSERT_EQ(3, obj->id);
  }
}

TEST(unbounded_object_pool_volatile_tests,
     check_total_number_of_cached_instances) {
  constexpr size_t kMaxCount = 2;
  irs::UnboundedObjectPoolVolatile<TestUobject> pool(kMaxCount);

  std::mutex mutex;
  std::condition_variable ready_cv;
  bool ready{false};

  std::atomic<size_t> id{};
  TestUobject::gTotalCount = 0;

  auto job = [&mutex, &ready_cv, &pool, &ready, &id]() {
    // wait for all threads to be ready
    {
      auto lock = std::unique_lock(mutex);

      while (!ready) {
        ready_cv.wait(lock);
      }
    }

    for (size_t i = 0; i < 100000; ++i) {
      auto p = pool.Emplace(id++);
      ASSERT_TRUE(p->id >= 0);
    }
  };

  auto job_shared = [&mutex, &ready_cv, &pool, &ready, &id]() {
    // wait for all threads to be ready
    {
      auto lock = std::unique_lock(mutex);

      while (!ready) {
        ready_cv.wait(lock);
      }
    }

    for (size_t i = 0; i < 100000; ++i) {
      auto p = pool.Emplace(id++);
      ASSERT_TRUE(p->id >= 0);
    }
  };

  constexpr size_t kThreadsCount = 32;
  std::vector<std::thread> threads;

  for (size_t i = 0; i < kThreadsCount / 2; ++i) {
    threads.emplace_back(job);
    threads.emplace_back(job_shared);
  }

  // ready
  {
    auto lock = std::unique_lock(mutex);
    ready = true;
  }
  ready_cv.notify_all();

  for (auto& thread : threads) {
    thread.join();
  }

  ASSERT_LE(TestSlowUobject::gTotalCount.load(), kMaxCount);
}

// create irs::concurrent_stack on heap, otherwise ASAN crashes here
TEST(concurrent_linked_list_test, push_pop) {
  typedef irs::ConcurrentStack<size_t> Stack;
  typedef Stack::NodeType NodeType;

  std::vector<NodeType> nodes(10);

  size_t v = 0;
  for (auto& node : nodes) {
    node.value = v++;
  }

  auto list_ptr = std::make_unique<Stack>();
  Stack& list = *list_ptr;
  ASSERT_TRUE(list.empty());
  ASSERT_EQ(nullptr, list.pop());

  for (auto& node : nodes) {
    list.push(node);
  }
  ASSERT_FALSE(list.empty());

  NodeType* node = 0;
  auto rbegin = nodes.rbegin();
  while ((node = list.pop())) {
    ASSERT_EQ(&*rbegin, node);
    list.push(*node);
    node = list.pop();
    ASSERT_EQ(&*rbegin, node);
    ++rbegin;
  }
  ASSERT_EQ(nodes.rend(), rbegin);
}

TEST(concurrent_linked_list_test, push) {
  typedef irs::ConcurrentStack<size_t> Stack;
  typedef Stack::NodeType NodeType;

  std::vector<NodeType> nodes(10);
  auto list_ptr = std::make_unique<Stack>();
  Stack& list = *list_ptr;
  ASSERT_TRUE(list.empty());
  ASSERT_EQ(nullptr, list.pop());

  for (auto& node : nodes) {
    list.push(node);
  }
  ASSERT_FALSE(list.empty());

  size_t count = 0;
  while ([[maybe_unused]] auto* head = list.pop()) {
    ++count;
  }
  ASSERT_TRUE(list.empty());

  ASSERT_EQ(nodes.size(), count);
}

TEST(concurrent_linked_list_test, move) {
  typedef irs::ConcurrentStack<size_t> Stack;

  std::array<Stack::NodeType, 10> nodes;
  auto moved_ptr = std::make_unique<Stack>();
  Stack& moved = *moved_ptr;
  ASSERT_TRUE(moved.empty());

  size_t i = 0;
  for (auto& node : nodes) {
    node.value = i;
    moved.push(node);
  }
  ASSERT_FALSE(moved.empty());

  auto list_ptr = std::make_unique<Stack>(std::move(moved));
  Stack& list = *list_ptr;
  ASSERT_TRUE(moved.empty());
  ASSERT_FALSE(list.empty());

  auto rbegin = nodes.rbegin();
  while (auto* node = list.pop()) {
    ASSERT_EQ(rbegin->value, node->value);
    ++rbegin;
  }

  ASSERT_EQ(nodes.rend(), rbegin);
  ASSERT_TRUE(list.empty());
}

TEST(concurrent_linked_list_test, move_assignment) {
  typedef irs::ConcurrentStack<size_t> Stack;

  std::array<Stack::NodeType, 10> nodes;

  auto list0_ptr = std::make_unique<Stack>();
  Stack& list0 = *list0_ptr;
  ASSERT_TRUE(list0.empty());

  auto list1_ptr = std::make_unique<Stack>();
  Stack& list1 = *list0_ptr;
  ASSERT_TRUE(list1.empty());

  size_t i = 0;

  for (; i < nodes.size() / 2; ++i) {
    auto& node = nodes[i];
    node.value = i;
    list0.push(node);
  }
  ASSERT_FALSE(list0.empty());

  for (; i < nodes.size(); ++i) {
    auto& node = nodes[i];
    node.value = i;
    list1.push(node);
  }
  ASSERT_FALSE(list1.empty());

  list0 = std::move(list1);

  auto rbegin = nodes.rbegin();
  while (auto* node = list0.pop()) {
    ASSERT_EQ(rbegin->value, node->value);
    ++rbegin;
  }
  ASSERT_TRUE(list0.empty());
  ASSERT_TRUE(list1.empty());
}

TEST(concurrent_linked_list_test, concurrent_pop) {
  static constexpr size_t kNodes = 10000;
  static constexpr size_t kThreads = 16;

  typedef irs::ConcurrentStack<size_t> Stack;
  std::vector<Stack::NodeType> nodes(kNodes);

  // build-up a list
  auto list_ptr = std::make_unique<Stack>();
  Stack& list = *list_ptr;
  ASSERT_TRUE(list.empty());
  size_t size = 0;
  for (auto& node : nodes) {
    node.value = size++;
    list.push(node);
  }
  ASSERT_FALSE(list.empty());

  std::vector<std::vector<size_t>> threads_data(kThreads);
  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  std::mutex mutex;
  std::condition_variable ready_cv;
  bool ready = false;

  auto wait_for_all = [&mutex, &ready, &ready_cv]() {
    // wait for all threads to be registered
    std::unique_lock<std::remove_reference<decltype(mutex)>::type> lock(mutex);
    while (!ready) {
      ready_cv.wait(lock);
    }
  };

  // start threads
  {
    auto lock = std::unique_lock(mutex);
    for (size_t i = 0; i < threads_data.size(); ++i) {
      auto& thread_data = threads_data[i];
      threads.emplace_back([&list, &wait_for_all, &thread_data]() {
        wait_for_all();

        while (auto* head = list.pop()) {
          thread_data.push_back(head->value);
        }
      });
    }
  }

  // all threads are registered... go, go, go...
  {
    std::lock_guard<decltype(mutex)> lock(mutex);
    ready = true;
    ready_cv.notify_all();
  }

  for (auto& thread : threads) {
    thread.join();
  }

  ASSERT_TRUE(list.empty());

  std::set<size_t> results;
  for (auto& thread_data : threads_data) {
    for (auto value : thread_data) {
      ASSERT_TRUE(results.insert(value).second);
    }
  }
  ASSERT_EQ(kNodes, results.size());
}

TEST(concurrent_linked_list_test, concurrent_push) {
  static constexpr size_t kNodes = 10000;
  static constexpr size_t kThreads = 16;

  typedef irs::ConcurrentStack<size_t> Stack;

  // build-up a list
  auto list_ptr = std::make_unique<Stack>();
  Stack& list = *list_ptr;
  ASSERT_TRUE(list.empty());

  std::vector<std::vector<Stack::NodeType>> threads_data;
  threads_data.reserve(kThreads);
  for (size_t i = 0; i < kThreads; ++i) {
    threads_data.emplace_back(kNodes);
  }

  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  std::mutex mutex;
  std::condition_variable ready_cv;
  bool ready = false;

  auto wait_for_all = [&mutex, &ready, &ready_cv]() {
    // wait for all threads to be registered
    auto lock = std::unique_lock(mutex);
    while (!ready) {
      ready_cv.wait(lock);
    }
  };

  // start threads
  {
    auto lock = std::unique_lock(mutex);
    for (size_t i = 0; i < threads_data.size(); ++i) {
      auto& thread_data = threads_data[i];
      threads.emplace_back([&list, &wait_for_all, &thread_data]() {
        wait_for_all();

        size_t idx = 0;
        for (auto& node : thread_data) {
          node.value = idx++;
          list.push(node);
        }
      });
    }
  }

  // all threads are registered... go, go, go...
  {
    std::lock_guard<decltype(mutex)> lock(mutex);
    ready = true;
    ready_cv.notify_all();
  }

  for (auto& thread : threads) {
    thread.join();
  }

  ASSERT_FALSE(list.empty());

  std::vector<size_t> results(kNodes);

  while (auto* node = list.pop()) {
    ASSERT_TRUE(node->value < results.size());
    ++results[node->value];
  }

  ASSERT_TRUE(results.front() == kThreads &&
              irs::irstd::AllEqual(results.begin(), results.end()));
}

TEST(concurrent_linked_list_test, concurrent_pop_push) {
  static constexpr size_t kNodes = 10000;
  static constexpr size_t kThreads = 16;

  struct Data {
    std::atomic_flag visited = ATOMIC_FLAG_INIT;
    std::atomic<size_t> num_owners{};
    size_t value{};
  };

  typedef irs::ConcurrentStack<Data> Stack;
  std::vector<Stack::NodeType> nodes(kNodes);

  // build-up a list
  auto list_ptr = std::make_unique<Stack>();
  Stack& list = *list_ptr;
  ASSERT_TRUE(list.empty());
  for (auto& node : nodes) {
    list.push(node);
  }
  ASSERT_FALSE(list.empty());

  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  std::mutex mutex;
  std::condition_variable ready_cv;
  bool ready = false;

  auto wait_for_all = [&mutex, &ready, &ready_cv]() {
    // wait for all threads to be registered
    auto lock = std::unique_lock(mutex);
    while (!ready) {
      ready_cv.wait(lock);
    }
  };

  // start threads
  {
    auto lock = std::unique_lock(mutex);
    for (size_t i = 0; i < kThreads; ++i) {
      threads.emplace_back([&list, &wait_for_all]() {
        wait_for_all();

        // no more than NODES
        size_t processed = 0;

        while (auto* head = list.pop()) {
          ++processed;
          EXPECT_LE(processed, 2 * kNodes);

          auto& value = head->value;

          if (!value.visited.test_and_set()) {
            ++value.num_owners;
            ++value.value;
            list.push(*head);
          } else {
            --value.num_owners;
            ASSERT_EQ(0, value.num_owners);
          }
        }
      });
    }
  }

  // all threads are registered... go, go, go...
  {
    auto lock = std::unique_lock(mutex);
    ready = true;
    ready_cv.notify_all();
  }

  for (auto& thread : threads) {
    thread.join();
  }

  ASSERT_TRUE(list.empty());

  for (auto& node : nodes) {
    ASSERT_EQ(1, node.value.value);
    ASSERT_EQ(true, node.value.visited.test_and_set());
    ASSERT_EQ(0, node.value.num_owners);
  }
}
