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

#include <iosfwd>
#include <string>
#include <string_view>

#include "rest_server/serened.h"

namespace vpack {

class Builder;

}  // namespace vpack
namespace sdb::metrics {

class Metric {
 public:
  static void addInfo(std::string& result, std::string_view name,
                      std::string_view help, std::string_view type);
  static void addMark(std::string& result, std::string_view name,
                      std::string_view globals, std::string_view labels);

  Metric(std::string_view name, std::string_view help, std::string_view labels,
         bool dynamic = false);

  virtual ~Metric();

  [[nodiscard]] std::string_view help() const noexcept;
  [[nodiscard]] std::string_view name() const noexcept;
  [[nodiscard]] std::string_view labels() const noexcept;

  [[nodiscard]] virtual std::string_view type() const noexcept = 0;

  //////////////////////////////////////////////////////////////////////////////
  /// \param result toPrometheus handler response
  /// \param globals labels that all metrics have
  //////////////////////////////////////////////////////////////////////////////
  virtual void toPrometheus(std::string& result, std::string_view globals,
                            bool ensure_whitespace) const = 0;
  virtual void toVPack(vpack::Builder& builder, SerenedServer& server) const;

  void setDynamic() noexcept { _dynamic = true; }

  bool isDynamic() const noexcept { return _dynamic; }

 private:
  std::string_view _name;
  std::string _help;
  std::string _labels;
  bool _dynamic;
};

}  // namespace sdb::metrics
