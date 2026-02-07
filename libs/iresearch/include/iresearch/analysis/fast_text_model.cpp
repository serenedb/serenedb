#include "fast_text_model.hpp"

#include <absl/synchronization/mutex.h>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"

namespace sdb::fast_text {
namespace {

constinit absl::Mutex gMutex{absl::kConstInit};
containers::FlatHashMap<std::string_view, std::weak_ptr<Model>> gCache;

ModelPtr FindModel(std::string_view key) noexcept {
  auto it = gCache.find(key);
  return it == gCache.end() ? nullptr : it->second.lock();
}

void DropModel(std::string_view key) noexcept {
  absl::WriterMutexLock lock{&gMutex};
  auto it = gCache.find(key);
  if (it != gCache.end() && it->first.data() == key.data()) {
    SDB_ASSERT(!it->second.lock());
    gCache.erase(it);
  }
}

}  // namespace

ModelPtr CreateModel(std::string_view key) {
  {
    absl::ReaderMutexLock lock{&gMutex};
    if (ModelPtr model = FindModel(key)) {
      return model;
    }
  }
  absl::WriterMutexLock lock{&gMutex};
  ModelPtr model = {new Model{key}, [](Model* p) noexcept {
                      DropModel(p->key());
                      delete p;
                    }};
  gCache.insert_or_assign(model->key(), model);
  return model;
}

}  // namespace sdb::fast_text
