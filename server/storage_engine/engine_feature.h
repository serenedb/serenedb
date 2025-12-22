#pragma once

#include "basics/down_cast.h"
#include "rest_server/serened.h"

namespace sdb {

class RocksDBEngineCatalog;

class EngineFeature final : public SerenedFeature {
 public:
  static constexpr std::string_view name() noexcept { return "Engine"; }

  explicit EngineFeature(Server& server);

  void start() final;
  void stop() final;
  void prepare() final;
  void unprepare() final;
  void beginShutdown() final;

  RocksDBEngineCatalog& engine() { return *_engine; }
  bool started() const { return _started.load(std::memory_order_relaxed); }

 protected:
  std::shared_ptr<RocksDBEngineCatalog> _engine;
  std::atomic_bool _started = false;
};

RocksDBEngineCatalog& GetServerEngine();

}  // namespace sdb
