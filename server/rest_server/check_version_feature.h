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

#include <span>

#include "app/app_feature.h"
#include "rest_server/serened.h"

namespace sdb {

class CheckVersionFeature final : public SerenedFeature {
 public:
  static constexpr std::string_view name() noexcept { return "CheckVersion"; }

  explicit CheckVersionFeature(Server& server, int* result,
                               std::span<const size_t> non_server_features);

  void collectOptions(std::shared_ptr<options::ProgramOptions>) final;
  void validateOptions(std::shared_ptr<options::ProgramOptions>) final;
  void start() final;
  bool GetCheckVersion() const { return _check_version; }

 private:
  void checkVersion();

  int* _result;
  bool _check_version = false;
  std::span<const size_t> _non_server_features;
};

}  // namespace sdb
