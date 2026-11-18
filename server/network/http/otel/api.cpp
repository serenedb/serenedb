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

#include "network/http/api.h"

#include <array>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <memory>
#include <string_view>

#include "network/http/common.h"
#include "network/http/listen.h"
#include "network/http/otel/handlers.h"
#include "network/http/otel/schema.h"

namespace sdb::otel {
namespace {

template<Endpoint E>
std::unique_ptr<network::HttpHandler> Handler() {
  return Make(E);
}

constexpr auto kRoutes = std::to_array<network::http::Route>({
  {network::HttpMethod::Post, "/v1/logs", Handler<Endpoint::Logs>},
  {network::HttpMethod::Post, "/v1/traces", Handler<Endpoint::Traces>},
  {network::HttpMethod::Post, "/v1/metrics", Handler<Endpoint::Metrics>},
});

void Start(const network::ListenSpec& spec) {
  const auto& options = network::HttpOptions(spec);
  const std::string_view database = options.database.empty()
                                      ? irs::StaticStrings::kDefaultDatabase
                                      : std::string_view{options.database};
  const std::string_view schema = options.schema.empty()
                                    ? std::string_view{"public"}
                                    : std::string_view{options.schema};
  if (options.create_schema) {
    if (const auto status = EnsureSchema(database, schema); !status.ok()) {
      SDB_FATAL(GENERAL, "endpoint '", spec.url,
                "': OpenTelemetry schema: ", status.message());
    }
  } else if (const auto status = CheckSchema(database, schema); !status.ok()) {
    SDB_WARN(GENERAL, "endpoint '", spec.url,
             "': OpenTelemetry schema: ", status.message(),
             ". Exports answer 503 until it matches.");
  }
}

SDB_HTTP_API kOtel{"otel", kRoutes, network::http::kOtelMaxBodyBytes, &Start};

}  // namespace
}  // namespace sdb::otel
