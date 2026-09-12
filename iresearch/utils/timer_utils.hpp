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

#pragma once

#include <absl/container/flat_hash_set.h>

#include <atomic>
#include <functional>

#include "iresearch/utils/noncopyable.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/string.hpp"

#if defined(SDB_DEV)
#include <absl/strings/str_cat.h>
#endif

namespace irs::timer_utils {

struct TimerStatT {
  std::atomic_size_t count{};
  std::atomic_size_t time{};
};

class ScopedTimer : util::Noncopyable {
 public:
  explicit ScopedTimer(TimerStatT& stat);
  ScopedTimer(ScopedTimer&&) = default;
  ~ScopedTimer();

 private:
  ScopedTimer& operator=(ScopedTimer&&) = delete;

  size_t _start;
  TimerStatT& _stat;
};

static_assert(std::is_nothrow_move_constructible_v<ScopedTimer>);

TimerStatT& GetStat(const std::string& key);

// Note: MSVC sometimes initializes the static variable and sometimes leaves it
// as *(nullptr)
//       therefore for MSVC before use, check if the static variable has been
//       initialized
#define REGISTER_TIMER_IMPL(timer_name, line)                                  \
  static auto& timer_state##_##line = ::irs::timer_utils::GetStat(timer_name); \
  ::irs::timer_utils::ScopedTimer timer_stat##_##line(MSVC_ONLY(               \
    &timer_state##_##line == nullptr ? ::irs::timer_utils::GetStat(timer_name) \
                                     :) timer_state##_##line)
#define REGISTER_TIMER_EXPANDER(timer_name, line) \
  REGISTER_TIMER_IMPL(timer_name, line)
#define SCOPED_TIMER(timer_name) REGISTER_TIMER_EXPANDER(timer_name, __LINE__)

#if defined(SDB_DEV)
#define REGISTER_TIMER(timer_name) REGISTER_TIMER_EXPANDER(timer_name, __LINE__)
#define REGISTER_TIMER_DETAILED() \
  REGISTER_TIMER(absl::StrCat(IRS_FUNC_NAME, ":" IRS_TO_STRING(__LINE__)))
#define REGISTER_TIMER_DETAILED_VERBOSE() \
  REGISTER_TIMER(                         \
    absl::StrCat(__FILE__ ":" IRS_TO_STRING(__LINE__) " -> ", IRS_FUNC_NAME))
#define REGISTER_TIMER_NAMED_DETAILED(timer_name) \
  REGISTER_TIMER(absl::StrCat(IRS_FUNC_NAME, " \"", timer_name, "\""))
#define REGISTER_TIMER_NAMED_DETAILED_VERBOSE(timer_name)                  \
  REGISTER_TIMER(absl::StrCat(__FILE__ ":" IRS_TO_STRING(__LINE__) " -> ", \
                              IRS_FUNC_NAME, " \"", timer_name, "\""))
#else
#define REGISTER_TIMER(timer_name)
#define REGISTER_TIMER_DETAILED()
#define REGISTER_TIMER_DETAILED_VERBOSE()
#define REGISTER_TIMER_NAMED_DETAILED(timer_name)
#define REGISTER_TIMER_NAMED_DETAILED_VERBOSE(timer_name)
#endif

/// @brief initialize stat tracking of specific keys
///        all previous timer stats are invalid after this call
///        NOTE: this method call must be externally synchronized with
///              GetStat(...) and Visit(...), i.e. do not call both
///              concurrently
void InitStats(bool track_all_keys = false,
               const absl::flat_hash_set<std::string>& tracked_keys = {});

/// @brief visit all tracked keys
bool Visit(const std::function<bool(const std::string& key, size_t count,
                                    size_t time_us)>& visitor);

/// @brief flush formatted timer stats to a specified stream
void FlushStats(std::ostream& out);

}  // namespace irs::timer_utils
