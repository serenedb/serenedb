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

#include <absl/base/internal/endian.h>
#include <gtest/gtest.h>

#include <duckdb/common/types.hpp>
#include <string>
#include <vector>

#include "basics/message_buffer.h"
#include "network/pg/wire_frames.h"
#include "pg/errcodes.h"
#include "pg/protocol.h"
#include "pg/sql_exception_macro.h"

using namespace sdb;
using namespace sdb::network::pg;

namespace {

std::string Flatten(message::SequenceView view) {
  std::string out;
  for (const auto buffer : view) {
    out.append(static_cast<const char*>(buffer.data()), buffer.size());
  }
  return out;
}

}  // namespace

TEST(NetworkPgFrames, ParameterStatus) {
  message::Buffer buf{256, 4096};
  WriteParameterStatus(buf, "server_version", "16.0");
  const std::string bytes = Flatten(buf.Written());
  ASSERT_GE(bytes.size(), 5u);
  EXPECT_EQ(bytes[0], PQ_MSG_PARAMETER_STATUS);
  EXPECT_EQ(absl::big_endian::Load32(bytes.data() + 1), bytes.size() - 1);
  EXPECT_NE(bytes.find("server_version"), std::string::npos);
  EXPECT_NE(bytes.find("16.0"), std::string::npos);
}

TEST(NetworkPgFrames, ReadyForQuery) {
  message::Buffer buf{256, 4096};
  WriteReadyForQuery(buf, 'I');
  const std::string bytes = Flatten(buf.Written());
  ASSERT_EQ(bytes.size(), 6u);
  EXPECT_EQ(bytes[0], PQ_MSG_READY_FOR_QUERY);
  EXPECT_EQ(absl::big_endian::Load32(bytes.data() + 1), 5u);
  EXPECT_EQ(bytes[5], 'I');
}

TEST(NetworkPgFrames, RowDescriptionSingleInt) {
  message::Buffer buf{256, 4096};
  const std::vector<duckdb::LogicalType> types{duckdb::LogicalType::INTEGER};
  const std::vector<std::string> names{"answer"};
  const std::vector<sdb::pg::VarFormat> formats{};
  WriteRowDescription(buf, types, names, formats);
  const std::string bytes = Flatten(buf.Written());
  ASSERT_GE(bytes.size(), 7u);
  EXPECT_EQ(bytes[0], PQ_MSG_ROW_DESCRIPTION);
  EXPECT_EQ(absl::big_endian::Load16(bytes.data() + 5), 1);
  EXPECT_NE(bytes.find("answer"), std::string::npos);
  // type oid follows: header(7) + "answer\0"(7) + table_oid(4) + attr(2) = 20
  EXPECT_EQ(absl::big_endian::Load32(bytes.data() + 20), 23u);  // int4
}

TEST(NetworkPgFrames, CommandCompleteAndError) {
  message::Buffer buf{256, 4096};
  WriteCommandComplete(
    buf,
    sdb::pg::CommandTag{"SELECT", duckdb::StatementType::SELECT_STATEMENT,
                        true},
    1);
  const std::string complete = Flatten(buf.Written());
  EXPECT_EQ(complete[0], PQ_MSG_COMMAND_COMPLETE);
  EXPECT_NE(complete.find("SELECT 1"), std::string::npos);

  message::Buffer ebuf{256, 4096};
  WriteErrorResponse(
    ebuf, SQL_ERROR_DATA(ERR_CODE(ERRCODE_SYNTAX_ERROR), ERR_MSG("boom")));
  const std::string error = Flatten(ebuf.Written());
  EXPECT_EQ(error[0], PQ_MSG_ERROR_RESPONSE);
  EXPECT_NE(error.find("42601"), std::string::npos);
  EXPECT_NE(error.find("boom"), std::string::npos);
}
