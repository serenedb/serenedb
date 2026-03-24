////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <vpack/builder.h>

#include <string>

#include "basics/common.h"
#include "rest_server/serened.h"

namespace sdb {
namespace app {

class AppServer;
}
namespace stats {

enum RequestStatisticsSource { kUser, kSuperuser, kAll };

enum class GroupType { System, Client, ClientUser, Http, Server };

std::string FromGroupType(stats::GroupType);

struct Group {
  stats::GroupType type;
  std::string name;
  std::string description;

 public:
  void toVPack(vpack::Builder&) const;
};

enum class FigureType : char { Current, Accumulated, Distribution };

std::string FromFigureType(stats::FigureType);

enum class Unit : char { Seconds, Bytes, Percent, Number };

std::string FromUnit(stats::Unit);

struct Figure {
  stats::GroupType group_type;
  std::string identifier;
  std::string name;
  std::string description;
  stats::FigureType type;
  stats::Unit units;

  std::vector<double> cuts;

 public:
  void toVPack(vpack::Builder&) const;
};

class Descriptions final {
 public:
  explicit Descriptions(SerenedServer&);

  const std::vector<stats::Group>& groups() const { return _groups; }

  const std::vector<stats::Figure>& figures() const { return _figures; }

  void serverStatistics(vpack::Builder&) const;
  void clientStatistics(vpack::Builder&, RequestStatisticsSource source) const;
  void httpStatistics(vpack::Builder&) const;
  void processStatistics(vpack::Builder&) const;

 private:
  SerenedServer& _server;

  std::vector<double> _request_time_cuts;
  std::vector<double> _connection_time_cuts;
  std::vector<double> _bytes_send_cuts;
  std::vector<double> _bytes_received_cuts;

  std::vector<stats::Group> _groups;
  std::vector<stats::Figure> _figures;
};

}  // namespace stats
}  // namespace sdb
