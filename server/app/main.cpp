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

#include <absl/base/config.h>
#include <pthread.h>

#include <cstddef>
#include <iresearch/search/filters/filter_optimizer.hpp>
#include <iresearch/utils/cpu_features.hpp>

#include "app/command.h"
#include "server/utils/asio_ns.h"
#include "server/utils/init.h"

namespace {

const boost::asio::ssl::detail::openssl_init<true> kSslInit{};

void SetLargeThreadStackSize() {
#if defined(__GLIBC__) &&                  \
  (defined(ABSL_HAVE_ADDRESS_SANITIZER) || \
   defined(ABSL_HAVE_THREAD_SANITIZER) ||  \
   defined(ABSL_HAVE_MEMORY_SANITIZER) || !defined(__OPTIMIZE__))
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setstacksize(&attr, size_t{64} << 20);
  pthread_setattr_default_np(&attr);
  pthread_attr_destroy(&attr);
#endif
}

}  // namespace

int main(int argc, char* argv[]) {
  irs::CheckCpuFeatures(argv[0]);
  sdb::app::ReserveFileDescriptors();
  SetLargeThreadStackSize();
  irs::InitOptimizeRules();
  return sdb::app::RunCommand(argc, argv);
}
