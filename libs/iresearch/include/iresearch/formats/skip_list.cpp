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
////////////////////////////////////////////////////////////////////////////////

#include "skip_list.hpp"

#include "basics/math_utils.hpp"
#include "basics/shared.hpp"
#include "basics/std.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

// returns maximum number of skip levels needed to store specified
// count of objects for skip list with
// step skip_0 for 0 level, step skip_n for other levels
constexpr size_t CountMaxLevels(size_t skip_0, size_t skip_n,
                                size_t count) noexcept {
  return skip_0 < count ? 1 + math::Log(count / skip_0, skip_n) : 0;
}

static_assert(CountMaxLevels(128, 8, doc_limits::eof()) == 9);

}  // namespace

void SkipWriter::Prepare(size_t max_levels, size_t count) {
  _max_levels = std::min(max_levels, CountMaxLevels(_skip_0, _skip_n, count));
  _levels.reserve(_max_levels);

  // reset existing skip levels
  for (auto& level : _levels) {
    level.Reset();
  }

  // add new skip levels if needed
  for (auto size = std::size(_levels); size < _max_levels; ++size) {
    _levels.emplace_back(_levels.get_allocator().Manager());
  }
}

uint32_t SkipWriter::CountLevels() const {
  auto level = std::make_reverse_iterator(std::begin(_levels) + _max_levels);
  const auto rend = std::rend(_levels);

  // find first filled level
  level = std::find_if(level, rend, [](const MemoryOutput& level) {
    return level.stream.Position();
  });

  // count number of levels
  const auto num_levels = static_cast<uint32_t>(std::distance(level, rend));
  return num_levels;
}

void SkipWriter::FlushLevels(uint32_t num_levels, IndexOutput& out) {
  // write number of levels
  out.WriteV32(num_levels);

  // write levels from n downto 0
  auto level = std::make_reverse_iterator(std::begin(_levels) + num_levels);
  const auto rend = std::rend(_levels);
  for (; level != rend; ++level) {
    auto& stream = level->stream;
    stream.Flush();  // update length of each buffer

    const uint64_t length = stream.Position();
    SDB_ASSERT(length);
    out.WriteV64(length);
    level->file >> out;
  }
}

}  // namespace irs
