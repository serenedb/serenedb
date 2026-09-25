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

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>
#include <lz4frame.h>
#include <zlib.h>
#include <zstd.h>
#include <zxc.h>

#include <array>
#include <cstdint>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>
#include <yaclib/algo/wait_group.hpp>
#include <yaclib/async/make.hpp>
#include <yaclib/coro/task.hpp>
#include <yaclib/lazy/make.hpp>

#include "network/acceptor.h"
#include "network/cancel_registry.h"
#include "network/http/compression.h"
#include "network/http/handler.h"
#include "network/http/router.h"
#include "network/http/session.h"
#include "network/io_context.h"
#include "server/utils/asio_ns.h"
#include "server/utils/message_buffer.h"

using namespace sdb;
using network::http::Acceptance;
using network::http::DecodeContent;
using network::http::FindContentCoding;
using network::http::HttpStatus;
using network::http::kMinCompressBytes;
using network::http::NegotiateContentCoding;
using network::http::ParseContentEncoding;

namespace {

using HttpAcceptor =
  network::Acceptor<network::HttpSession<network::SocketKind::Tcp>>;

std::string Payload(size_t size) {
  std::string body;
  body.reserve(size);
  while (body.size() < size) {
    body.append(R"({"k":"the quick brown fox jumps over the lazy dog"},)");
  }
  body.resize(size);
  return body;
}

// Incompressible: every coding's output is LARGER than its input, so a
// re-compression loop can never shrink back under the size threshold.
std::string Incompressible(size_t size) {
  std::string body(size, '\0');
  uint64_t state = 0x9e3779b97f4a7c15ULL;
  for (auto& byte : body) {
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    byte = static_cast<char>(state & 0xFF);
  }
  return body;
}

const std::string kLarge = Payload(64 * 1024);
const std::string kNoise = Incompressible(4 * 1024);
const std::string kSmall = Payload(kMinCompressBytes - 1);

class FixedHandler final : public network::HttpHandler {
 public:
  explicit FixedHandler(std::string_view body) : _body{body} {}

  yaclib::Task<> Handle(network::RequestContext&, const network::HttpRequest&,
                        network::http::HttpResponseWriter& writer) override {
    writer.Json(HttpStatus::Ok, _body);
    return yaclib::MakeTask();
  }

 private:
  std::string_view _body;
};

class ChunkedHandler final : public network::HttpHandler {
 public:
  yaclib::Task<> Handle(network::RequestContext&, const network::HttpRequest&,
                        network::http::HttpResponseWriter& writer) override {
    writer.WriteHeadChunked(HttpStatus::Ok, "application/json");
    for (size_t offset = 0; offset < kLarge.size(); offset += 7000) {
      writer.Write(std::string_view{kLarge}.substr(offset, 7000));
    }
    writer.Finish();
    return yaclib::MakeTask();
  }
};

class KnownLengthHandler final : public network::HttpHandler {
 public:
  yaclib::Task<> Handle(network::RequestContext&, const network::HttpRequest&,
                        network::http::HttpResponseWriter& writer) override {
    writer.WriteHead(HttpStatus::Ok, "application/json", kLarge.size());
    writer.Write(std::string_view{kLarge}.substr(0, 1000));
    writer.Write(std::string_view{kLarge}.substr(1000));
    writer.Finish();
    return yaclib::MakeTask();
  }
};

class EchoHandler final : public network::HttpHandler {
 public:
  yaclib::Task<> Handle(network::RequestContext&,
                        const network::HttpRequest& request,
                        network::http::HttpResponseWriter& writer) override {
    std::string body;
    for (const auto chunk : request.body) {
      body.append(static_cast<const char*>(chunk.data()), chunk.size());
    }
    writer.Fixed(HttpStatus::Ok, "application/octet-stream", body);
    return yaclib::MakeTask();
  }
};

class Harness {
 public:
  Harness() : _context{_router} {
    _router.Add(network::HttpMethod::Get, "/large",
                std::make_unique<FixedHandler>(kLarge));
    _router.Add(network::HttpMethod::Head, "/large",
                std::make_unique<FixedHandler>(kLarge));
    _router.Add(network::HttpMethod::Get, "/small",
                std::make_unique<FixedHandler>(kSmall));
    _router.Add(network::HttpMethod::Get, "/noise",
                std::make_unique<FixedHandler>(kNoise));
    _router.Add(network::HttpMethod::Get, "/chunked",
                std::make_unique<ChunkedHandler>());
    _router.Add(network::HttpMethod::Get, "/known",
                std::make_unique<KnownLengthHandler>());
    _router.Add(network::HttpMethod::Post, "/echo",
                std::make_unique<EchoHandler>());
    _context.cancel = &_cancel;
    _context.sessions = &_sessions;
    _pool.Start();
    const asio_ns::ip::tcp::endpoint any{asio_ns::ip::make_address("127.0.0.1"),
                                         0};
    _acceptor = std::make_shared<HttpAcceptor>(_pool, any, _context);
    server = {asio_ns::ip::make_address("127.0.0.1"),
              _acceptor->LocalEndpoint().port()};
    _acceptor->Start();
  }

  ~Harness() {
    _acceptor->Stop();
    _cancel.TerminateAll();
    _sessions.Done();
    _sessions.Wait();
    _pool.Stop();
  }

  std::string Get(std::string_view path, std::string_view accept_encoding,
                  std::string_view method = "GET") {
    std::string request = absl::StrCat(method, " ", path,
                                       " HTTP/1.1\r\nHost: t\r\nConnection: "
                                       "close\r\n");
    if (!accept_encoding.empty()) {
      absl::StrAppend(&request, "Accept-Encoding: ", accept_encoding, "\r\n");
    }
    absl::StrAppend(&request, "\r\n");
    return Send(request);
  }

  std::string Post(std::string_view body, std::string_view content_encoding,
                   std::string_view accept_encoding = {}) {
    return Send(PostRequest(body, content_encoding, accept_encoding,
                            /*keep_alive=*/false));
  }

  static std::string PostRequest(std::string_view body,
                                 std::string_view content_encoding,
                                 std::string_view accept_encoding,
                                 bool keep_alive) {
    std::string request =
      absl::StrCat("POST /echo HTTP/1.1\r\nHost: t\r\nConnection: ",
                   keep_alive ? "keep-alive" : "close",
                   "\r\nContent-Length: ", body.size(), "\r\n");
    if (!content_encoding.empty()) {
      absl::StrAppend(&request, "Content-Encoding: ", content_encoding, "\r\n");
    }
    if (!accept_encoding.empty()) {
      absl::StrAppend(&request, "Accept-Encoding: ", accept_encoding, "\r\n");
    }
    absl::StrAppend(&request, "\r\n", body);
    return request;
  }

  std::string Send(std::string_view request) {
    asio_ns::io_context io;
    asio_ns::ip::tcp::socket socket{io};
    socket.connect(server);
    asio_ns::write(socket, asio_ns::buffer(request.data(), request.size()));
    std::string response;
    std::array<char, 4096> chunk;
    asio_ns::error_code ec;
    while (!ec) {
      const size_t n = socket.read_some(asio_ns::buffer(chunk), ec);
      response.append(chunk.data(), n);
    }
    return response;
  }

  asio_ns::ip::tcp::endpoint server;

 private:
  network::HttpRouter _router;
  network::IoThreadPool _pool{1};
  network::CancelRegistry _cancel;
  yaclib::WaitGroup<> _sessions{1};
  network::HttpServerContext _context;
  std::shared_ptr<HttpAcceptor> _acceptor;
};

struct Response {
  std::string head;
  std::string body;

  bool Has(std::string_view header) const {
    return head.find(header) != std::string::npos;
  }
};

std::string Dechunk(std::string_view wire) {
  std::string out;
  while (!wire.empty()) {
    const size_t eol = wire.find("\r\n");
    const size_t size =
      std::stoul(std::string{wire.substr(0, eol)}, nullptr, 16);
    wire.remove_prefix(eol + 2);
    if (size == 0) {
      break;
    }
    out.append(wire.substr(0, size));
    wire.remove_prefix(size + 2);
  }
  return out;
}

Response Split(const std::string& raw) {
  const size_t end = raw.find("\r\n\r\n");
  Response response{.head = raw.substr(0, end), .body = raw.substr(end + 4)};
  if (response.Has("Transfer-Encoding: chunked")) {
    response.body = Dechunk(response.body);
  }
  return response;
}

std::string Gunzip(std::string_view in) {
  z_stream stream{};
  EXPECT_EQ(inflateInit2(&stream, 15 + 16), Z_OK);
  stream.next_in =
    const_cast<Bytef*>(reinterpret_cast<const Bytef*>(in.data()));
  stream.avail_in = static_cast<uInt>(in.size());
  std::string out;
  std::array<char, 8192> block;
  int rc = Z_OK;
  while (rc != Z_STREAM_END) {
    stream.next_out = reinterpret_cast<Bytef*>(block.data());
    stream.avail_out = static_cast<uInt>(block.size());
    rc = inflate(&stream, Z_NO_FLUSH);
    if (rc != Z_OK && rc != Z_STREAM_END) {
      ADD_FAILURE() << "inflate: " << rc;
      break;
    }
    out.append(block.data(), block.size() - stream.avail_out);
  }
  inflateEnd(&stream);
  return out;
}

std::string Unzstd(std::string_view in) {
  auto* dctx = ZSTD_createDCtx();
  ZSTD_inBuffer input{in.data(), in.size(), 0};
  std::string out;
  std::array<char, 8192> block;
  while (input.pos < input.size) {
    ZSTD_outBuffer output{block.data(), block.size(), 0};
    const size_t rc = ZSTD_decompressStream(dctx, &output, &input);
    if (ZSTD_isError(rc)) {
      ADD_FAILURE() << ZSTD_getErrorName(rc);
      break;
    }
    out.append(block.data(), output.pos);
  }
  ZSTD_freeDCtx(dctx);
  return out;
}

std::string Unlz4(std::string_view in) {
  LZ4F_dctx* dctx = nullptr;
  EXPECT_FALSE(
    LZ4F_isError(LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION)));
  std::string out;
  std::array<char, 8192> block;
  size_t consumed_total = 0;
  while (consumed_total < in.size()) {
    size_t dst = block.size();
    size_t src = in.size() - consumed_total;
    const size_t rc = LZ4F_decompress(
      dctx, block.data(), &dst, in.data() + consumed_total, &src, nullptr);
    if (LZ4F_isError(rc)) {
      ADD_FAILURE() << LZ4F_getErrorName(rc);
      break;
    }
    out.append(block.data(), dst);
    consumed_total += src;
  }
  LZ4F_freeDecompressionContext(dctx);
  return out;
}

std::string Unzxc(std::string_view in) {
  std::string out(zxc_get_decompressed_size(in.data(), in.size()), '\0');
  const int64_t rc =
    zxc_decompress(in.data(), in.size(), out.data(), out.size(), nullptr);
  EXPECT_GE(rc, 0) << zxc_error_name(static_cast<int>(rc));
  out.resize(rc > 0 ? static_cast<size_t>(rc) : 0);
  return out;
}

std::string Gzip(std::string_view in) {
  z_stream stream{};
  EXPECT_EQ(deflateInit2(&stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 15 + 16, 8,
                         Z_DEFAULT_STRATEGY),
            Z_OK);
  std::string out(deflateBound(&stream, in.size()), '\0');
  stream.next_in =
    const_cast<Bytef*>(reinterpret_cast<const Bytef*>(in.data()));
  stream.avail_in = static_cast<uInt>(in.size());
  stream.next_out = reinterpret_cast<Bytef*>(out.data());
  stream.avail_out = static_cast<uInt>(out.size());
  EXPECT_EQ(deflate(&stream, Z_FINISH), Z_STREAM_END);
  out.resize(stream.total_out);
  deflateEnd(&stream);
  return out;
}

std::string Zstd(std::string_view in) {
  std::string out(ZSTD_compressBound(in.size()), '\0');
  const size_t size =
    ZSTD_compress(out.data(), out.size(), in.data(), in.size(), 3);
  EXPECT_FALSE(ZSTD_isError(size));
  out.resize(size);
  return out;
}

std::string Lz4(std::string_view in) {
  std::string out(LZ4F_compressFrameBound(in.size(), nullptr), '\0');
  const size_t size =
    LZ4F_compressFrame(out.data(), out.size(), in.data(), in.size(), nullptr);
  EXPECT_FALSE(LZ4F_isError(size));
  out.resize(size);
  return out;
}

std::string Zxc(std::string_view in) {
  std::string out(zxc_compress_bound(in.size()), '\0');
  const int64_t size =
    zxc_compress(in.data(), in.size(), out.data(), out.size(), nullptr);
  EXPECT_GT(size, 0);
  out.resize(size > 0 ? static_cast<size_t>(size) : 0);
  return out;
}

std::string Encode(std::string_view coding, std::string_view in) {
  if (coding == "gzip") {
    return Gzip(in);
  }
  if (coding == "zstd") {
    return Zstd(in);
  }
  if (coding == "lz4") {
    return Lz4(in);
  }
  return Zxc(in);
}

std::string Decode(std::string_view coding, std::string_view in) {
  if (coding == "gzip") {
    return Gunzip(in);
  }
  if (coding == "zstd") {
    return Unzstd(in);
  }
  if (coding == "lz4") {
    return Unlz4(in);
  }
  return Unzxc(in);
}

constexpr std::array<std::string_view, 4> kCodings{"gzip", "zstd", "lz4",
                                                   "zxc"};

std::string DecodeAll(std::string_view body, std::string_view field,
                      size_t max_bytes = size_t{64} << 20) {
  message::Buffer buffer{1024, 1 << 20};
  message::Writer writer{buffer};
  writer.Write(body);
  writer.Commit(false);
  const auto codings = ParseContentEncoding(field);
  EXPECT_TRUE(codings.has_value()) << field;
  std::string out;
  DecodeContent(
    buffer.Written(), *codings,
    [&](std::string_view part) { out.append(part); }, max_bytes);
  return out;
}

int DecodeErrcode(std::string_view body, std::string_view field,
                  size_t max_bytes = size_t{64} << 20) {
  try {
    DecodeAll(body, field, max_bytes);
  } catch (const irs::SqlException& error) {
    return error.error().errcode;
  }
  return 0;
}

}  // namespace

TEST(NetworkHttpCompression, NegotiatePrefersServerOrder) {
  const auto token = [](std::string_view header) -> std::string_view {
    const auto negotiated = NegotiateContentCoding(header);
    EXPECT_EQ(negotiated.acceptance, Acceptance::Ok) << header;
    return negotiated.coding == nullptr ? "identity" : negotiated.coding->token;
  };
  EXPECT_EQ(token("gzip"), "gzip");
  EXPECT_EQ(token("GZIP"), "gzip");
  EXPECT_EQ(token("lz4"), "lz4");
  EXPECT_EQ(token("zxc"), "zxc");
  // Server preference decides when the client accepts several.
  EXPECT_EQ(token("gzip, zstd"), "zstd");
  EXPECT_EQ(token("gzip, zxc, lz4"), "gzip");
  EXPECT_EQ(token("zxc, lz4"), "zxc");
  EXPECT_EQ(token("*"), "zstd");
  EXPECT_EQ(token("*;q=0, gzip"), "gzip");
  // A coding the client did not list is not acceptable on its own.
  EXPECT_EQ(token("zstd;q=0"), "identity");
  EXPECT_EQ(token("zstd;q=0, gzip"), "gzip");
  // The client's own weights outrank the server order.
  EXPECT_EQ(token("gzip;q=1.0, zstd;q=0.1"), "gzip");
  EXPECT_EQ(token("lz4;q=0.9, zstd;q=0.2, gzip;q=0.1"), "lz4");
  // The weight must be found whatever its position in the parameter list.
  EXPECT_EQ(token("gzip;q=0.5;x=y, zstd;q=0"), "gzip");
  EXPECT_EQ(token("zstd;x=1;q=0, gzip;x=1"), "gzip");
  // Nothing we encode: the body still goes out, uncompressed.
  EXPECT_EQ(token(""), "identity");
  EXPECT_EQ(token("identity"), "identity");
  EXPECT_EQ(token("br"), "identity");
  EXPECT_EQ(token("gzip;q=0, zstd;q=0, lz4;q=0, zxc;q=0"), "identity");
}

// The client ruled out every coding we have AND the uncompressed form, so
// there is no representation left to send.
TEST(NetworkHttpCompression, NegotiateNotAcceptable) {
  for (const auto* header :
       {"identity;q=0", "*;q=0", "br, identity;q=0, *;q=0"}) {
    EXPECT_EQ(NegotiateContentCoding(header).acceptance,
              Acceptance::NotAcceptable)
      << header;
  }
}

TEST(NetworkHttpCompression, NegotiateMalformed) {
  for (const auto* header :
       {"gzip;q=abc", "gzip;q=", "gzip;q=2", "*;q=-1", "gzip;q=nan",
        "gzip;q=inf", "gzip;q=1e0", "gzip;q=0.5000", "gzip;q=1.001",
        "gzip;q=.5", "gzip;q=+0.5"}) {
    EXPECT_EQ(NegotiateContentCoding(header).acceptance, Acceptance::Malformed)
      << header;
  }
}

TEST(NetworkHttpCompression, UnacceptableAndMalformedGetStatusCodes) {
  Harness harness;
  const auto unacceptable = Split(harness.Get("/large", "identity;q=0"));
  EXPECT_TRUE(unacceptable.Has("HTTP/1.1 406")) << unacceptable.head;
  EXPECT_FALSE(unacceptable.Has("Content-Encoding"));

  const auto malformed = Split(harness.Get("/large", "gzip;q=huh"));
  EXPECT_TRUE(malformed.Has("HTTP/1.1 400")) << malformed.head;

  // A coding we do not have is ordinary negotiation, not an error.
  const auto unknown = Split(harness.Get("/large", "br"));
  EXPECT_TRUE(unknown.Has("HTTP/1.1 200"));
  EXPECT_FALSE(unknown.Has("Content-Encoding"));
  EXPECT_EQ(unknown.body, kLarge);
}

TEST(NetworkHttpCompression, FixedBodyGzip) {
  Harness harness;
  const auto response = Split(harness.Get("/large", "gzip"));
  EXPECT_TRUE(response.Has("HTTP/1.1 200"));
  EXPECT_TRUE(response.Has("Content-Encoding: gzip"));
  EXPECT_TRUE(response.Has("Vary: Accept-Encoding"));
  EXPECT_TRUE(response.Has("Content-Length: "));
  EXPECT_LT(response.body.size(), kLarge.size() / 4);
  EXPECT_EQ(Gunzip(response.body), kLarge);
}

TEST(NetworkHttpCompression, FixedBodyZstdAndLz4) {
  Harness harness;
  const auto zstd = Split(harness.Get("/large", "zstd"));
  EXPECT_TRUE(zstd.Has("Content-Encoding: zstd"));
  EXPECT_EQ(Unzstd(zstd.body), kLarge);

  const auto lz4 = Split(harness.Get("/large", "lz4"));
  EXPECT_TRUE(lz4.Has("Content-Encoding: lz4"));
  EXPECT_EQ(Unlz4(lz4.body), kLarge);

  const auto zxc = Split(harness.Get("/large", "zxc"));
  EXPECT_TRUE(zxc.Has("Content-Encoding: zxc"));
  EXPECT_EQ(Unzxc(zxc.body), kLarge);
}

TEST(NetworkHttpCompression, IdentityWhenNotAcceptedOrSmall) {
  Harness harness;
  const auto plain = Split(harness.Get("/large", ""));
  EXPECT_FALSE(plain.Has("Content-Encoding"));
  EXPECT_EQ(plain.body, kLarge);

  const auto unsupported = Split(harness.Get("/large", "br"));
  EXPECT_FALSE(unsupported.Has("Content-Encoding"));
  EXPECT_EQ(unsupported.body, kLarge);

  const auto small = Split(harness.Get("/small", "gzip"));
  EXPECT_FALSE(small.Has("Content-Encoding"));
  EXPECT_EQ(small.body, kSmall);
}

// An encoding that does not shrink the body is dropped -- and, crucially, the
// encoded bytes are never fed back through the encoder.
TEST(NetworkHttpCompression, IncompressibleBodyStaysIdentity) {
  Harness harness;
  for (const auto* coding : {"gzip", "zstd", "lz4", "zxc"}) {
    const auto response = Split(harness.Get("/noise", coding));
    EXPECT_TRUE(response.Has("HTTP/1.1 200")) << coding;
    EXPECT_FALSE(response.Has("Content-Encoding")) << coding;
    EXPECT_EQ(response.body, kNoise) << coding;
  }
}

TEST(NetworkHttpCompression, HeadStaysIdentity) {
  Harness harness;
  const auto response = Split(harness.Get("/large", "gzip", "HEAD"));
  EXPECT_TRUE(response.Has("HTTP/1.1 200"));
  EXPECT_FALSE(response.Has("Content-Encoding"));
  EXPECT_TRUE(response.body.empty());
}

TEST(NetworkHttpCompression, ChunkedStream) {
  Harness harness;
  const auto response = Split(harness.Get("/chunked", "zstd"));
  EXPECT_TRUE(response.Has("Transfer-Encoding: chunked"));
  EXPECT_TRUE(response.Has("Content-Encoding: zstd"));
  EXPECT_EQ(Unzstd(response.body), kLarge);

  const auto zxc = Split(harness.Get("/chunked", "zxc"));
  EXPECT_TRUE(zxc.Has("Content-Encoding: zxc"));
  EXPECT_EQ(Unzxc(zxc.body), kLarge);
}

TEST(NetworkHttpCompression, KnownLengthStreamBecomesChunked) {
  Harness harness;
  const auto response = Split(harness.Get("/known", "gzip"));
  EXPECT_TRUE(response.Has("Transfer-Encoding: chunked"));
  EXPECT_FALSE(response.Has("Content-Length"));
  EXPECT_TRUE(response.Has("Content-Encoding: gzip"));
  EXPECT_EQ(Gunzip(response.body), kLarge);

  const auto plain = Split(harness.Get("/known", ""));
  EXPECT_TRUE(plain.Has("Content-Length: "));
  EXPECT_EQ(plain.body, kLarge);
}

TEST(NetworkHttpCompression, ParseContentEncoding) {
  const auto tokens = [](std::string_view field) {
    std::vector<std::string_view> out;
    for (const auto* coding : *ParseContentEncoding(field)) {
      out.push_back(coding->token);
    }
    return out;
  };
  EXPECT_TRUE(tokens("").empty());
  EXPECT_TRUE(tokens("identity").empty());
  EXPECT_EQ(tokens("GZIP"), std::vector<std::string_view>{"gzip"});
  EXPECT_EQ(tokens("gzip, identity, zstd"),
            (std::vector<std::string_view>{"gzip", "zstd"}));
  EXPECT_FALSE(ParseContentEncoding("br").has_value());
  EXPECT_FALSE(ParseContentEncoding("gzip, br").has_value());
}

TEST(NetworkHttpCompression, DecodeContentEveryCoding) {
  for (const auto coding : kCodings) {
    EXPECT_EQ(DecodeAll(Encode(coding, kLarge), coding), kLarge) << coding;
  }
}

TEST(NetworkHttpCompression, DecodeContentStackedCodings) {
  const auto body = Zstd(Gzip(kLarge));
  EXPECT_EQ(DecodeAll(body, "gzip, zstd"), kLarge);
  EXPECT_EQ(DecodeErrcode(body, "zstd, gzip"), ERRCODE_DATA_EXCEPTION);
}

TEST(NetworkHttpCompression, DecodeContentCorruptOrTruncated) {
  for (const auto coding : kCodings) {
    const auto encoded = Encode(coding, kLarge);
    EXPECT_EQ(DecodeErrcode(encoded.substr(0, encoded.size() / 2), coding),
              ERRCODE_DATA_EXCEPTION)
      << coding;
    EXPECT_EQ(DecodeErrcode(kNoise, coding), ERRCODE_DATA_EXCEPTION) << coding;
  }
}

TEST(NetworkHttpCompression, DecodeContentSizeLimit) {
  for (const auto coding : kCodings) {
    const auto encoded = Encode(coding, kLarge);
    EXPECT_EQ(DecodeErrcode(encoded, coding, kLarge.size() - 1),
              ERRCODE_PROGRAM_LIMIT_EXCEEDED)
      << coding;
    EXPECT_EQ(DecodeAll(encoded, coding, kLarge.size()), kLarge) << coding;
  }
}

TEST(NetworkHttpCompression, RequestBodyEveryCoding) {
  Harness harness;
  for (const auto coding : kCodings) {
    const auto response = Split(harness.Post(Encode(coding, kLarge), coding));
    EXPECT_TRUE(response.Has("HTTP/1.1 200")) << coding << response.head;
    EXPECT_EQ(response.body, kLarge) << coding;
  }
  const auto identity = Split(harness.Post(kLarge, "identity"));
  EXPECT_EQ(identity.body, kLarge);
  const auto stacked = Split(harness.Post(Zstd(Gzip(kLarge)), "gzip, zstd"));
  EXPECT_EQ(stacked.body, kLarge);
}

TEST(NetworkHttpCompression, RequestAndResponseCodings) {
  Harness harness;
  for (const auto request_coding : kCodings) {
    for (const auto response_coding : kCodings) {
      const auto response = Split(harness.Post(
        Encode(request_coding, kLarge), request_coding, response_coding));
      EXPECT_TRUE(response.Has("HTTP/1.1 200"))
        << request_coding << " -> " << response_coding;
      EXPECT_TRUE(
        response.Has(absl::StrCat("Content-Encoding: ", response_coding)))
        << request_coding << " -> " << response_coding;
      EXPECT_EQ(Decode(response_coding, response.body), kLarge)
        << request_coding << " -> " << response_coding;
    }
  }
}

TEST(NetworkHttpCompression, RequestBodyErrors) {
  Harness harness;
  const auto unknown = Split(harness.Post(kLarge, "br"));
  EXPECT_TRUE(unknown.Has("HTTP/1.1 415")) << unknown.head;
  for (const auto coding : kCodings) {
    const auto corrupt = Split(harness.Post(kNoise, coding));
    EXPECT_TRUE(corrupt.Has("HTTP/1.1 400")) << coding << corrupt.head;
  }
}

TEST(NetworkHttpCompression, EarlyErrorKeepsConnectionUsable) {
  Harness harness;
  const auto second =
    absl::StrCat("GET /small HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
  for (const auto& [content_encoding, accept_encoding, status] : std::array<
         std::tuple<std::string_view, std::string_view, std::string_view>, 3>{
         {{"", "gzip;q=huh", "400"},
          {"", "identity;q=0", "406"},
          {"br", "", "415"}}}) {
    const auto raw = harness.Send(absl::StrCat(
      Harness::PostRequest(kLarge, content_encoding, accept_encoding,
                           /*keep_alive=*/true),
      second));
    EXPECT_TRUE(raw.starts_with(absl::StrCat("HTTP/1.1 ", status)))
      << status << raw.substr(0, 80);
    const size_t next = raw.find("HTTP/1.1 200");
    ASSERT_NE(next, std::string::npos) << status;
    EXPECT_TRUE(raw.ends_with(kSmall)) << status;
  }
}

TEST(NetworkHttpCompression, NegotiateQValueGrammar) {
  const auto token = [](std::string_view header) -> std::string_view {
    const auto negotiated = NegotiateContentCoding(header);
    EXPECT_EQ(negotiated.acceptance, Acceptance::Ok) << header;
    return negotiated.coding == nullptr ? "identity" : negotiated.coding->token;
  };
  EXPECT_EQ(token("gzip;q=0.123, zstd;q=0.12"), "gzip");
  EXPECT_EQ(token("gzip;q=1.000"), "gzip");
  EXPECT_EQ(token("gzip;q=1."), "gzip");
  EXPECT_EQ(token("gzip;q=0."), "identity");
  EXPECT_EQ(token("gzip;q=0.001"), "gzip");
}

TEST(NetworkHttpCompression, TooManyStackedCodings) {
  EXPECT_FALSE(ParseContentEncoding("gzip, zstd, lz4").has_value());
  EXPECT_FALSE(ParseContentEncoding("gzip, gzip, gzip").has_value());
  EXPECT_TRUE(ParseContentEncoding("gzip, identity, zstd").has_value());
  Harness harness;
  const auto response =
    Split(harness.Post(Lz4(Zstd(Gzip(kLarge))), "gzip, zstd, lz4"));
  EXPECT_TRUE(response.Has("HTTP/1.1 415")) << response.head;
}

TEST(NetworkHttpCompression, DecodedBodyOverBodyLimit) {
  const std::string zeros((size_t{64} << 20) + 1, '\0');
  Harness harness;
  const auto response = Split(harness.Post(Zstd(zeros), "zstd"));
  EXPECT_TRUE(response.Has("HTTP/1.1 413")) << response.head;
}
