#include "fast_text_model.h"

#include <absl/synchronization/mutex.h>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"

namespace sdb::fast_text {
namespace {

constinit absl::Mutex gMutex{absl::kConstInit};
containers::FlatHashMap<std::string_view, std::weak_ptr<Model>> gCache;

ModelPtr FindModel(std::string_view key) noexcept {
  auto it = gCache.find(key);
  if (it == gCache.end()) {
    return nullptr;
  }
  return it->second.lock();
}

void DropModel(std::string_view key) noexcept {
  absl::WriterMutexLock lock{&gMutex};
  auto it = gCache.find(key);
  // We cannot rely on weak_ptr::lock value here,
  // because it's possible other model already dead
  if (it != gCache.end() && it->first.data() == key.data()) {
    SDB_ASSERT(!it->second.lock());
    gCache.erase(it);
  }
}

}  // namespace

ModelPtr CreateModel(std::string_view key) {
  // We need to destroy model after lock, because otherwise deadlock possible
  ModelPtr model;
  if (absl::ReaderMutexLock lock{&gMutex}; (model = FindModel(key))) {
    return model;
  }
  absl::WriterMutexLock lock{&gMutex};
  model = {new Model{key}, [](Model* p) noexcept {
             DropModel(p->key());
             delete p;
           }};
  // We need to assign because it's possible that outdated model still here
  gCache.insert_or_assign(model->key(), model);
  return model;
}

}  // namespace sdb::fast_text
