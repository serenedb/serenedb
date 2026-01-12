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

#include "timer_utils.hpp"

#include <absl/container/node_hash_map.h>

#include <map>
#include <mutex>

#include "basics/singleton.hpp"

namespace irs::timer_utils {
namespace {

class TimerStates : public Singleton<TimerStates> {
 public:
  TimerStatT& Find(const std::string& key) {
    if (_track_all_keys.load(std::memory_order_relaxed)) {
      std::lock_guard lock{_mutex};
      return _state_map[key];
    }
    auto it = _state_map.find(key);
    if (it != _state_map.end()) {
      return it->second;
    }
    static TimerStatT gUnused;
    return gUnused;
  }

  void Init(bool track_all_keys = false,
            const absl::flat_hash_set<std::string>& tracked_keys = {}) {
    std::lock_guard lock(_mutex);
    for (auto& entry : _state_map) {
      entry.second.count = 0;
      entry.second.time = 0;
    }
    for (const auto& key : tracked_keys) {
      _state_map.try_emplace(key);
    }
    _track_all_keys.store(track_all_keys, std::memory_order_relaxed);
  }

  bool Visit(const std::function<bool(const std::string& key, size_t count,
                                      size_t time_us)>& visitor) {
    static const auto kUs =
      (1'000'000.0 * std::chrono::system_clock::period::num) /
      std::chrono::system_clock::period::den;

    std::lock_guard lock(_mutex);
    for (const auto& [key, entry] : _state_map) {
      if (!visitor(
            key, entry.count,
            static_cast<size_t>(static_cast<double>(entry.time) * kUs))) {
        return false;
      }
    }

    return true;
  }

 private:
  absl::Mutex _mutex{absl::kConstInit};
  absl::node_hash_map<std::string, TimerStatT> _state_map;
  std::atomic_bool _track_all_keys = false;
};

}  // namespace

ScopedTimer::ScopedTimer(TimerStatT& stat)
  : _start(std::chrono::system_clock::now().time_since_epoch().count()),
    _stat(stat) {
  ++(_stat.count);
}

ScopedTimer::~ScopedTimer() {
  _stat.time +=
    std::chrono::system_clock::now().time_since_epoch().count() - _start;
}

TimerStatT& GetStat(const std::string& key) {
  return TimerStates::instance().Find(key);
}

void InitStats(bool track_all_keys /*= false*/,
               const absl::flat_hash_set<std::string>& tracked_keys /*= {} */) {
  TimerStates::instance().Init(track_all_keys, tracked_keys);
}

bool Visit(const std::function<bool(const std::string& key, size_t count,
                                    size_t time_us)>& visitor) {
  return TimerStates::instance().Visit(visitor);
}

void FlushStats(std::ostream& out) {
  std::map<std::string, std::pair<size_t, size_t>> ordered_stats;

  Visit([&ordered_stats](const std::string& key, size_t count,
                         size_t time) -> bool {
    std::string key_str = key;

#if defined(__GNUC__)
    if (key_str.compare(0, strlen("virtual "), "virtual ") == 0) {
      key_str = key_str.substr(strlen("virtual "));
    }

    size_t i = 0;

    if (std::string::npos != (i = key_str.find(' ')) && key_str.find('(') > i) {
      key_str = key_str.substr(i + 1);
    }
#elif defined(_WIN32)
    size_t i;

    if (std::string::npos != (i = key_str.find("__cdecl "))) {
      key_str = key_str.substr(i + strlen("__cdecl "));
    }
#endif

    ordered_stats.emplace(key_str, std::make_pair(count, time));
    return true;
  });

  for (auto& entry : ordered_stats) {
    const auto& key = entry.first;
    auto& count = entry.second.first;
    auto time = static_cast<double>(entry.second.second);
    out << key << "\tcalls:" << count << ",\ttime: " << time / 1000.0
        << " us,\tavg call: " << time / 1000.0 / static_cast<double>(count)
        << " us" << std::endl;
  }
}

}  // namespace irs::timer_utils
