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

#include <gtest/gtest.h>

#include "network/http/header.h"

using namespace sdb::network;

TEST(NetworkHeader, InternIsCaseInsensitive) {
  EXPECT_EQ(InternHeader("content-type"), HttpHeader::ContentType);
  EXPECT_EQ(InternHeader("Content-Type"), HttpHeader::ContentType);
  EXPECT_EQ(InternHeader("CONTENT-TYPE"), HttpHeader::ContentType);
  EXPECT_EQ(InternHeader("Host"), HttpHeader::Host);
  EXPECT_EQ(InternHeader("Transfer-Encoding"), HttpHeader::TransferEncoding);
  EXPECT_EQ(InternHeader("x-not-a-known-header"), HttpHeader::Unknown);
  EXPECT_EQ(InternHeader(""), HttpHeader::Unknown);
}

TEST(NetworkHeader, NameRoundTrip) {
  EXPECT_EQ(HeaderName(HttpHeader::ContentLength), "content-length");
  EXPECT_EQ(HeaderName(HttpHeader::Connection), "connection");
  EXPECT_EQ(InternHeader(HeaderName(HttpHeader::ETag)), HttpHeader::ETag);
}
