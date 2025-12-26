#pragma once

#include "basics/down_cast.h"
#include "rest_server/serened.h"

namespace sdb {

#ifdef SDB_CLUSTER
class RocksDBEngine;
#else
class RocksDBEngineCatalog;
#endif

class EngineFeature final : public SerenedFeature {
 public:
  static constexpr std::string_view name() noexcept { return "Engine"; }

#ifdef SDB_CLUSTER
  using EngineType = RocksDBEngine;
#else
  using EngineType = RocksDBEngineCatalog;
#endif

  explicit EngineFeature(Server& server);

  void start() final;
  void stop() final;
  void prepare() final;
  void unprepare() final;
  void beginShutdown() final;

  auto& engine() { return *_engine; }
  bool started() const { return _started.load(std::memory_order_relaxed); }

 protected:
  std::shared_ptr<EngineType> _engine;
  std::atomic_bool _started = false;
};


EngineFeature::EngineType& GetServerEngine();

}  // namespace sdb
