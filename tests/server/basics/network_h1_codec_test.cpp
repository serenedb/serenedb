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

#include <algorithm>
#include <cstddef>
#include <string>
#include <string_view>

#include "basics/message_buffer.h"
#include "network/http/h1_codec.h"

using namespace sdb::network;

namespace {

std::string Flatten(sdb::message::SequenceView view) {
  std::string out;
  for (const auto buffer : view) {
    out.append(static_cast<const char*>(buffer.data()), buffer.size());
  }
  return out;
}

struct HeadResult {
  bool ok = false;
  bool cont = false;
  int error = 0;
  uint64_t content_length = 0;
  bool chunked = false;
  bool keep_alive = false;
  HttpRequest request;
};

HeadResult DriveHead(H1Codec& codec, std::string_view input,
                     size_t chunk_size) {
  std::string buffer;
  size_t offset = 0;
  HeadResult result;
  for (;;) {
    if (buffer.empty()) {
      if (offset >= input.size()) {
        break;
      }
      const size_t take = std::min(chunk_size, input.size() - offset);
      buffer.append(input.data() + offset, take);
      offset += take;
    }
    const auto fed = codec.ParseHead(buffer);
    buffer.erase(0, fed.consumed);
    if (fed.event == H1Event::Head || fed.event == H1Event::Continue) {
      result.ok = true;
      result.cont = fed.event == H1Event::Continue;
      result.content_length = codec.ContentLength();
      result.chunked = codec.IsChunked();
      result.keep_alive = codec.KeepAlive();
      result.request = codec.TakeHead();
      return result;
    }
    if (fed.event == H1Event::Error) {
      result.error = codec.ErrorStatus();
      return result;
    }
  }
  return result;
}

}  // namespace

TEST(NetworkH1Codec, HeadFragmentationInvariant) {
  const std::string_view request =
    "GET /path?q=1 HTTP/1.1\r\n"
    "Host: example\r\n"
    "X-Trace: abc\r\n"
    "\r\n";
  for (const size_t chunk : {size_t{1}, size_t{3}, size_t{7}, size_t{4096}}) {
    H1Codec codec;
    const HeadResult result = DriveHead(codec, request, chunk);
    ASSERT_TRUE(result.ok) << "chunk=" << chunk;
    EXPECT_EQ(result.request.method, HttpMethod::Get);
    EXPECT_EQ(result.request.target, "/path?q=1");
    EXPECT_EQ(result.request.Header(HttpHeader::Host), "example");
    EXPECT_EQ(result.request.Header("x-trace"), "abc");
    EXPECT_EQ(result.content_length, 0u);
    EXPECT_FALSE(result.chunked);
    EXPECT_TRUE(result.keep_alive);
  }
}

TEST(NetworkH1Codec, ContentLengthFraming) {
  H1Codec codec;
  const HeadResult result = DriveHead(
    codec, "POST /ingest HTTP/1.1\r\nHost: h\r\nContent-Length: 11\r\n\r\n",
    4096);
  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.request.method, HttpMethod::Post);
  EXPECT_EQ(result.content_length, 11u);
  EXPECT_FALSE(result.chunked);
}

TEST(NetworkH1Codec, DecodeChunkedBody) {
  H1Codec codec;
  const auto head = codec.ParseHead(
    "POST /x HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n");
  ASSERT_EQ(head.event, H1Event::Head);
  EXPECT_TRUE(codec.IsChunked());
  const HttpRequest request = codec.TakeHead();
  EXPECT_EQ(request.method, HttpMethod::Post);

  sdb::message::Buffer out{256, 4096};
  sdb::message::Writer writer{out};
  std::string_view body = "5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
  bool done = false;
  while (!done) {
    const auto step = codec.DecodeBody(body, writer);
    body.remove_prefix(step.consumed);
    ASSERT_FALSE(step.error);
    if (step.done) {
      done = true;
    } else {
      ASSERT_NE(step.consumed, 0u);
    }
  }
  writer.Commit(false);
  EXPECT_EQ(Flatten(out.Written()), "hello world");
}

TEST(NetworkH1Codec, RejectsTransferEncodingWithContentLength) {
  H1Codec codec;
  const HeadResult result =
    DriveHead(codec,
              "POST /x HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n"
              "Content-Length: 5\r\n\r\n",
              4096);
  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.error, 400);
}

TEST(NetworkH1Codec, RejectsConflictingContentLength) {
  H1Codec codec;
  const HeadResult result =
    DriveHead(codec,
              "POST /x HTTP/1.1\r\nHost: h\r\nContent-Length: "
              "5\r\nContent-Length: 6\r\n\r\n",
              4096);
  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.error, 400);
}

TEST(NetworkH1Codec, RejectsOversizedHead) {
  std::string request = "GET / HTTP/1.1\r\nHost: example\r\nBig: ";
  request.append(2048, 'a');
  request.append("\r\n\r\n");
  H1Limits limits;
  limits.max_head_bytes = 256;
  H1Codec codec{limits};
  const HeadResult result = DriveHead(codec, request, 4096);
  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.error, 431);
}

TEST(NetworkH1Codec, ExpectContinueWithBody) {
  H1Codec codec;
  const HeadResult result =
    DriveHead(codec,
              "POST /x HTTP/1.1\r\nHost: h\r\nContent-Length: 4\r\n"
              "Expect: 100-continue\r\n\r\n",
              4096);
  ASSERT_TRUE(result.ok);
  EXPECT_TRUE(result.cont);
  EXPECT_EQ(result.content_length, 4u);
}

TEST(NetworkH1Codec, ExpectContinueWithoutBodyIsHead) {
  H1Codec codec;
  const HeadResult result = DriveHead(
    codec, "GET / HTTP/1.1\r\nHost: h\r\nExpect: 100-continue\r\n\r\n", 4096);
  ASSERT_TRUE(result.ok);
  EXPECT_FALSE(result.cont);
}
