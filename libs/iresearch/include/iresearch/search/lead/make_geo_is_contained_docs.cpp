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

#include "iresearch/search/geo_instantiate.hpp"
#include "iresearch/search/lead/make_geo_impl.hpp"

namespace irs::lead {

#define IRS_GEO_CASE(Parser, ...) \
  template Node::ptr Make(const GeoQuery<Parser, __VA_ARGS__>&);
IRS_GEO_PARSERS(IRS_GEO_CASE, GeoIsContainedAcceptor)
#undef IRS_GEO_CASE

}  // namespace irs::lead
