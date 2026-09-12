////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <absl/random/random.h>

namespace sdb::random {
namespace {

class URBG {
 public:
  virtual ~URBG() = default;

  virtual int16_t Interval(int16_t l, int16_t r) = 0;
  virtual int32_t Interval(int32_t l, int32_t r) = 0;
  virtual int64_t Interval(int64_t l, int64_t r) = 0;

  virtual uint16_t Interval(uint16_t r) = 0;
  virtual uint32_t Interval(uint32_t r) = 0;
  virtual uint64_t Interval(uint64_t r) = 0;

  virtual uint64_t RandU64() = 0;
};

template<typename Self>
class URBGImpl : public URBG {
  auto& GetSelf() { return static_cast<Self&>(*this); }

  auto& GetImpl() { return GetSelf().GetImpl(); }

 public:
  int16_t Interval(int16_t l, int16_t r) final {
    return absl::Uniform(absl::IntervalClosed, GetImpl(), l, r);
  }
  int32_t Interval(int32_t l, int32_t r) final {
    return absl::Uniform(absl::IntervalClosed, GetImpl(), l, r);
  }
  int64_t Interval(int64_t l, int64_t r) final {
    return absl::Uniform(absl::IntervalClosed, GetImpl(), l, r);
  }

  uint16_t Interval(uint16_t r) final {
    return absl::Uniform(absl::IntervalClosed, GetImpl(), decltype(r){0}, r);
  }
  uint32_t Interval(uint32_t r) final {
    return absl::Uniform(absl::IntervalClosed, GetImpl(), decltype(r){0}, r);
  }
  uint64_t Interval(uint64_t r) final {
    return absl::Uniform(absl::IntervalClosed, GetImpl(), decltype(r){0}, r);
  }

  uint64_t RandU64() final {
    using R = std::decay_t<decltype(GetImpl())>::result_type;
    if constexpr (std::is_same_v<R, uint64_t>) {
      return GetImpl()();
    } else {
      static_assert(false);
    }
  }
};

class Randen final : public URBGImpl<Randen> {
 public:
  auto& GetImpl() { return _impl; }

 private:
  absl::BitGen _impl;
};

thread_local std::unique_ptr<Randen> gDevice;

}  // namespace

void Reset() { gDevice.reset(); }

void Ensure() {
  if (!gDevice) [[unlikely]] {
    gDevice = std::make_unique<Randen>();
  }
}

int16_t Interval(int16_t l, int16_t r) {
  Ensure();
  return gDevice->Interval(l, r);
}

int32_t Interval(int32_t l, int32_t r) {
  Ensure();
  return gDevice->Interval(l, r);
}

int64_t Interval(int64_t l, int64_t r) {
  Ensure();
  return gDevice->Interval(l, r);
}

uint16_t Interval(uint16_t r) {
  Ensure();
  return gDevice->Interval(r);
}

uint32_t Interval(uint32_t r) {
  Ensure();
  return gDevice->Interval(r);
}

uint64_t Interval(uint64_t r) {
  Ensure();
  return gDevice->Interval(r);
}

uint64_t RandU64() {
  Ensure();
  return gDevice->RandU64();
}

}  // namespace sdb::random
