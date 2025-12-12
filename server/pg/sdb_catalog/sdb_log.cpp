////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "pg/sdb_catalog/sdb_log.h"

#include <bit>

#include "app/app_server.h"
#include "app/logger_feature.h"
#include "basics/logger/logger.h"
#include "rest_server/log_buffer_feature.h"

namespace sdb::pg {
namespace {
constexpr uint64_t kNullMask = MaskFromNonNulls({
  GetIndex(&SdbLog::id),
  GetIndex(&SdbLog::timestamp),
  GetIndex(&SdbLog::topic),
  GetIndex(&SdbLog::level),
  GetIndex(&SdbLog::message),
});
}

template<>
std::vector<velox::VectorPtr> SystemTableSnapshot<SdbLog>::GetTableData(
  velox::memory::MemoryPool& pool) {
  if (!SerenedServer::Instance().getFeature<LoggerFeature>().isAPIEnabled()) {
    return std::vector<velox::VectorPtr>(boost::pfr::tuple_size_v<SdbLog>);
  }

  std::vector<velox::VectorPtr> result;
  result.reserve(boost::pfr::tuple_size_v<SdbLog>);

  auto entries =
    SerenedServer::Instance().getFeature<LogBufferFeature>().entries(
      LogLevel::TRACE, 0, true, {});
  std::vector<SdbLog> values;
  values.reserve(entries.size());

  for (auto& entry : entries) {
    SdbLog row{
      .id = entry.id,
      .timestamp = std::bit_cast<uint64_t>(entry.timestamp),
      .topic = entry.topic ? entry.topic->GetName() : "",
      .level = magic_enum::enum_name(entry.level),
      .message = entry.message,
    };
    values.push_back(std::move(row));
  }

  boost::pfr::for_each_field(SdbLog{}, [&]<typename Field>(const Field& field) {
    auto column = CreateColumn<Field>(values.size(), &pool);
    result.push_back(std::move(column));
  });

  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, &pool);
  }

  return result;
}

}  // namespace sdb::pg
