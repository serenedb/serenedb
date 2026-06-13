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

#include <array>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <yaclib/async/future.hpp>
#include <yaclib/async/make.hpp>

#include "basics/asio_ns.h"
#include "network/acceptor.h"
#include "network/http/handler.h"
#include "network/http/router.h"
#include "network/http/session.h"
#include "network/io_context.h"

using namespace sdb;

namespace {

using HttpAcceptor =
  network::Acceptor<network::HttpSession<network::SocketKind::Tcp>>;

// Fixed-response handler: writes a constant JSON body, no DB. Keeps these
// transport tests self-contained -- the real root/health/ping handlers (and
// their content) are covered by the ES driver suite; here we only exercise the
// HTTP wire (status line, keep-alive, pipelining, HEAD, chunked bodies).
class FixedHandler final : public network::HttpHandler {
 public:
  explicit FixedHandler(std::string body) : _body{std::move(body)} {}

  yaclib::Future<> Handle(network::RequestContext&, const network::HttpRequest&,
                          network::http::HttpResponseWriter& writer) override {
    writer.Json(200, _body);
    return yaclib::MakeFuture();
  }

 private:
  std::string _body;
};

class EchoHandler final : public network::HttpHandler {
 public:
  yaclib::Future<> Handle(network::RequestContext&,
                          const network::HttpRequest& request,
                          network::http::HttpResponseWriter& writer) override {
    std::string body;
    for (const auto buffer : request.body) {
      body.append(static_cast<const char*>(buffer.data()), buffer.size());
    }
    writer.Json(200, body);
    return yaclib::MakeFuture();
  }
};

void RegisterFixtures(network::HttpRouter& router) {
  static constexpr std::string_view kRoot =
    R"({"name":"serenedb","tagline":"You Know, for Search"})";
  router.Add(network::HttpMethod::Get, "/",
             std::make_unique<FixedHandler>(std::string{kRoot}));
  router.Add(network::HttpMethod::Head, "/",
             std::make_unique<FixedHandler>(std::string{kRoot}));
  router.Add(network::HttpMethod::Get, "/_cluster/health",
             std::make_unique<FixedHandler>(R"({"status":"green"})"));
  router.Add(network::HttpMethod::Get, "/ping",
             std::make_unique<FixedHandler>(R"({"status":"ok"})"));
}

std::string Roundtrip(const asio_ns::ip::tcp::endpoint& server,
                      std::string_view request) {
  asio_ns::io_context io;
  asio_ns::ip::tcp::socket socket{io};
  socket.connect(server);
  asio_ns::write(socket, asio_ns::buffer(request.data(), request.size()));

  std::string response;
  std::array<char, 4096> chunk;
  asio_ns::error_code ec;
  for (;;) {
    const size_t n = socket.read_some(asio_ns::buffer(chunk), ec);
    response.append(chunk.data(), n);
    if (ec) {
      break;
    }
  }
  return response;
}

asio_ns::ip::tcp::endpoint Loopback(std::uint16_t port) {
  return {asio_ns::ip::make_address("127.0.0.1"), port};
}

}  // namespace

TEST(NetworkHttp, RootHealthAndPing) {
  network::IoThreadPool pool{1};
  pool.Start();
  network::HttpRouter router;
  RegisterFixtures(router);
  network::HttpServerContext context{router};
  auto acceptor = std::make_shared<HttpAcceptor>(pool, Loopback(0), context);
  const auto server = Loopback(acceptor->LocalEndpoint().port());
  acceptor->Start();

  const std::string root =
    Roundtrip(server, "GET / HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
  EXPECT_NE(root.find("HTTP/1.1 200"), std::string::npos);
  EXPECT_NE(root.find("serenedb"), std::string::npos);
  EXPECT_NE(root.find("You Know, for Search"), std::string::npos);

  const std::string health = Roundtrip(
    server,
    "GET /_cluster/health HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
  EXPECT_NE(health.find("HTTP/1.1 200"), std::string::npos);
  EXPECT_NE(health.find(R"("status":"green")"), std::string::npos);

  const std::string ping = Roundtrip(
    server, "GET /ping HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
  EXPECT_NE(ping.find("HTTP/1.1 200"), std::string::npos);

  const std::string missing = Roundtrip(
    server, "GET /nope HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
  EXPECT_NE(missing.find("HTTP/1.1 404"), std::string::npos);

  acceptor->Stop();
  pool.Stop();
}

TEST(NetworkHttp, KeepAlivePipelining) {
  network::IoThreadPool pool{1};
  pool.Start();
  network::HttpRouter router;
  RegisterFixtures(router);
  network::HttpServerContext context{router};
  auto acceptor = std::make_shared<HttpAcceptor>(pool, Loopback(0), context);
  const auto server = Loopback(acceptor->LocalEndpoint().port());
  acceptor->Start();

  asio_ns::io_context io;
  asio_ns::ip::tcp::socket socket{io};
  socket.connect(server);

  size_t responses = 0;
  std::string buffer;
  std::array<char, 4096> chunk;
  asio_ns::error_code ec;
  const std::string_view pipelined =
    "GET /ping HTTP/1.1\r\nHost: t\r\n\r\n"
    "GET / HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n";
  asio_ns::write(socket, asio_ns::buffer(pipelined.data(), pipelined.size()));
  for (;;) {
    const size_t n = socket.read_some(asio_ns::buffer(chunk), ec);
    buffer.append(chunk.data(), n);
    if (ec) {
      break;
    }
  }
  size_t pos = 0;
  while ((pos = buffer.find("HTTP/1.1 200", pos)) != std::string::npos) {
    ++responses;
    pos += 1;
  }
  EXPECT_EQ(responses, 2u);

  acceptor->Stop();
  pool.Stop();
}

TEST(NetworkHttp, HeadHasNoBody) {
  network::IoThreadPool pool{1};
  pool.Start();
  network::HttpRouter router;
  RegisterFixtures(router);
  network::HttpServerContext context{router};
  auto acceptor = std::make_shared<HttpAcceptor>(pool, Loopback(0), context);
  const auto server = Loopback(acceptor->LocalEndpoint().port());
  acceptor->Start();

  const std::string head = Roundtrip(
    server, "HEAD / HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
  const auto separator = head.find("\r\n\r\n");
  ASSERT_NE(separator, std::string::npos);
  EXPECT_EQ(head.substr(separator + 4), "");

  acceptor->Stop();
  pool.Stop();
}

TEST(NetworkHttp, BodyEcho) {
  network::IoThreadPool pool{1};
  pool.Start();
  network::HttpRouter router;
  router.Add(network::HttpMethod::Post, "/echo",
             std::make_unique<EchoHandler>());
  network::HttpServerContext context{router};
  auto acceptor = std::make_shared<HttpAcceptor>(pool, Loopback(0), context);
  const auto server = Loopback(acceptor->LocalEndpoint().port());
  acceptor->Start();

  const std::string content_length =
    Roundtrip(server,
              "POST /echo HTTP/1.1\r\nHost: t\r\nContent-Length: 11\r\n"
              "Connection: close\r\n\r\nhello world");
  EXPECT_NE(content_length.find("\r\n\r\nhello world"), std::string::npos);

  const std::string chunked = Roundtrip(
    server,
    "POST /echo HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\n"
    "Connection: close\r\n\r\n5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n");
  EXPECT_NE(chunked.find("\r\n\r\nhello world"), std::string::npos);

  acceptor->Stop();
  pool.Stop();
}

TEST(NetworkHttp, ShutdownWithOpenKeepAliveConnection) {
  network::IoThreadPool pool{1};
  pool.Start();
  network::HttpRouter router;
  RegisterFixtures(router);
  network::HttpServerContext context{router};
  auto acceptor = std::make_shared<HttpAcceptor>(pool, Loopback(0), context);
  const auto server = Loopback(acceptor->LocalEndpoint().port());
  acceptor->Start();

  asio_ns::io_context io;
  asio_ns::ip::tcp::socket socket{io};
  socket.connect(server);
  const std::string_view request = "GET /ping HTTP/1.1\r\nHost: t\r\n\r\n";
  asio_ns::write(socket, asio_ns::buffer(request.data(), request.size()));
  std::array<char, 256> chunk;
  asio_ns::error_code ec;
  socket.read_some(asio_ns::buffer(chunk), ec);

  acceptor->Stop();
  pool.Stop();
  SUCCEED();
}
