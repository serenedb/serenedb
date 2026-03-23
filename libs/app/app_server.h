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

#include <absl/synchronization/mutex.h>

#include <magic_enum/magic_enum.hpp>

#include "app/app_feature.h"
#include "app/logger_feature.h"
#include "basics/down_cast.h"
#include "basics/type_traits.h"

namespace sdb {
namespace options {

class ProgramOptions;
}
namespace app {

// the following phases exist:
//
// `collectOptions`
//
// Creates the prgramm options for a feature. Features are not
// allowed to open files or sockets, create threads or allocate
// other resources. This method will be called regardless of whether
// to feature is enabled or disabled. There is no defined order in
// which the features are traversed.
//
// `loadOptions`
//
// Allows a feature to load more options from somewhere. This method
// will only be called for enabled features. There is no defined
// order in which the features are traversed.
//
// `validateOptions`
//
// Validates the feature's options. This method will only be called for enabled
// features. Help is handled before any `validateOptions` of a feature is
// called. The `validateOptions` methods are called in a order that obeys the
// `startsAfter `conditions.
//
// `daemonize`
//
// In this phase process control (like putting the process into the background
// will be handled). This method will only be called for enabled features.
// The `daemonize` methods are called in a order that obeys the `startsAfter`
// conditions.
//
// `prepare`
//
// Now the features will actually do some preparation work
// in the preparation phase, the features must not start any threads
// furthermore, they must not write any files under elevated privileges
// if they want other features to access them, or if they want to access
// these files with dropped privileges. The `prepare` methods are called in a
// order that obeys the `startsAfter` conditions.
//
// `start`
//
// Start the features. Features are now allowed to created threads.
//
// The `start` methods are called in a order that obeys the `startsAfter`
// conditions.
//
// `stop`
//
// Stops the features. The `stop` methods are called in reversed `start` order.
// This must stop all threads, but not destroy the features.
//
// `unprepare`
//
// This destroys the features.

class AppServer {
 public:
  enum class State : int {
    Uninitialized = 0,
    InCollectOptions,
    InValidateOptions,
    InPrepare,
    InStart,
    InWait,
    InShutdown,
    InStop,
    InUnprepare,
    Stopped,
    Aborted
  };

  class ProgressHandler {
   public:
    std::function<void(State)> state;
    std::function<void(State, std::string_view feature_name)> feature;
  };

  inline static std::atomic_bool gCtrlC = false;

 public:
  AppServer(std::shared_ptr<options::ProgramOptions>, const char* binary_path,
            std::span<std::unique_ptr<AppFeature>> features);

  virtual ~AppServer() = default;

  std::string helpSection() const { return _help_section; }
  bool helpShown() const { return !_help_section.empty(); }

  // whether or not the server has made it as least as far as the
  // IN_START state
  bool isPrepared() const noexcept {
    auto tmp = state();
    return tmp == State::InStart || tmp == State::InWait ||
           tmp == State::InShutdown || tmp == State::InStop;
  }

  // whether or not the server has made it as least as far as the
  // IN_SHUTDOWN state
  bool isStopping() const;

  /// whether or not state is the shutting down state or further (i.e.
  /// stopped, aborted etc.)
  bool isStoppingState(State state) const;

  // this method will initialize and validate options
  // of all feature, start them and wait for a shutdown
  // signal. after that, it will shutdown all features
  void run(int argc, char* argv[]);

  // signal a soft shutdown (only used for coordinators so far)
  void initiateSoftShutdown();

  // signal the server to shut down
  void beginShutdown();

  // report that we are going down by fatal error
  void shutdownFatalError();

  auto options() const { return _options; }

  State state() const { return _state.load(std::memory_order_acquire); }

  void addReporter(ProgressHandler reporter) {
    _progress_reports.emplace_back(std::move(reporter));
  }

  const char* getBinaryPath() const { return _binary_path; }

  void registerStartupCallback(const std::function<void()>& callback) {
    _startup_callbacks.emplace_back(callback);
  }

  void SetupFeatures();

  void disableFeatures(std::span<const size_t> types) {
    disableFeatures(types, false);
  }
  void forceDisableFeatures(std::span<const size_t> types) {
    disableFeatures(types, true);
  }

#ifdef SDB_GTEST
  auto GetFeatures() noexcept { return _all_features; }
  void setBinaryPath(const char* path) { _binary_path = path; }
  void setStateUnsafe(State ss) { _state = ss; }
#endif

 private:
  friend AppFeature;

  // checks for the existence of a feature by type. will not throw when used
  // for a non-existing feature
  bool hasFeature(size_t type) const noexcept {
    return type < _all_features.size() && nullptr != _all_features[type];
  }

  auto& getFeature(size_t type) const {
    if (hasFeature(type)) [[likely]] {
      return *_all_features[type];
    }
    SDB_THROW(ERROR_INTERNAL, "unknown feature: ", type);
  }

  void disableFeatures(std::span<const size_t> types, bool force);

  void collectOptions();
  void parseOptions(int argc, char* argv[]);
  void validateOptions();
  void daemonize();
  void prepare();
  void start();
  void stop();
  void unprepare();

  // after start, the server will wait in this method until
  // beginShutdown is called
  void wait();

  void reportServerProgress(State);
  void reportFeatureProgress(State, std::string_view);

 private:
  static auto EnabledFeaturesImpl(auto&& features) noexcept {
    return features | std::views::filter([](const auto& p) {
             return p && p->isEnabled();
           }) |
           std::views::transform([](auto& p) -> AppFeature& { return *p; });
  }

  auto EnabledFeatures() noexcept { return EnabledFeaturesImpl(_all_features); }

  auto EnabledFeaturesReverse() noexcept {
    return EnabledFeaturesImpl(std::views::reverse(_all_features));
  }

  std::atomic<State> _state = State::Uninitialized;

  std::shared_ptr<options::ProgramOptions> _options;

  std::span<std::unique_ptr<AppFeature>> _all_features;

  // will be signaled when the application server is asked to shut down
  struct {
    absl::CondVar cv;
    absl::Mutex mutex;
  } _shutdown_condition;

  // reporter for progress
  std::vector<ProgressHandler> _progress_reports;

  // callbacks that are called after start
  std::vector<std::function<void()>> _startup_callbacks;

  // help section displayed
  std::string _help_section;

  // the install directory of this program:
  const char* _binary_path;

  // the condition variable protects access to this flag
  // the flag is set to true when beginShutdown finishes
  bool _abort_waiting = false;

  // whether or not to dump configuration options
  bool _dump_options = false;
};

template<typename Features>
class AppServerImpl : public AppServer {
  inline static AppServer* gInstance = nullptr;

 public:
  static AppServerImpl& Instance() noexcept {
    SDB_ASSERT(gInstance);
    return basics::downCast<AppServerImpl>(*gInstance);
  }

#ifdef SDB_GTEST
  static void Instance(AppServerImpl* instance) noexcept {
    gInstance = instance;
  }
#endif

  // Returns feature identifier.
  template<typename T>
  static constexpr size_t id() noexcept {
    return Features::template id<T>();
  }

  // Returns true if a feature denoted by `T` is registered with the server.
  template<typename T>
  static constexpr bool contains() noexcept {
    return Features::template contains<T>();
  }

  // Returns true if a feature denoted by `Feature` is created before other
  // feautures denoted by `OtherFeatures`.
  template<typename Feature, typename... OtherFeatures>
  static constexpr bool isCreatedAfter() noexcept {
    return (std::greater{}(id<Feature>(), id<OtherFeatures>()) && ...);
  }

  AppServerImpl(std::shared_ptr<sdb::options::ProgramOptions> opts,
                const char* binary_path)
    : AppServer{opts, binary_path, _features} {
    gInstance = this;
  }

  // Adds all registered features to the application server.
  template<typename Initializer>
  void addFeatures(Initializer&& initializer) {
    Features::visit([&]<typename T>(type::Tag<T>) {
      static_assert(std::is_base_of_v<AppFeature, T>);
      constexpr auto kFeatureId = Features::template id<T>();

      SDB_ASSERT(!hasFeature<T>());
      _features[kFeatureId] =
        std::forward<Initializer>(initializer)(*this, type::Tag<T>{});
      SDB_ASSERT(hasFeature<T>());
    });
  }

#ifdef SDB_GTEST
  // Adds a feature to the application server. the application server
  // will take ownership of the feature object and destroy it in its
  // destructor.
  template<typename Type, typename Impl = Type, typename... Args>
  Impl& addFeature(Args&&... args) {
    static_assert(std::is_base_of_v<AppFeature, Type>);
    static_assert(std::is_base_of_v<AppFeature, Impl>);
    static_assert(std::is_base_of_v<Type, Impl>);
    constexpr auto kFeatureId = Features::template id<Type>();

    SDB_ASSERT(!hasFeature<Type>());
    auto& slot = _features[kFeatureId];
    slot = std::make_unique<Impl>(*this, std::forward<Args>(args)...);

    return static_cast<Impl&>(*slot);
  }
#endif

  // Return whether or not a feature is enabled
  // will throw when called for a non-existing feature.
  template<typename T>
  bool isEnabled() const {
    return getFeature<T>().isEnabled();
  }

  // Return whether or not a feature is optional
  // will throw when called for a non-existing feature.
  template<typename T>
  bool isOptional() const {
    return getFeature<T>().isOptional();
  }

  // Checks for the existence of a feature. will not throw when used for
  // a non-existing feature.
  template<typename Type>
  bool hasFeature() const noexcept {
    static_assert(std::is_base_of_v<AppFeature, Type>);

    constexpr auto kFeatureId = Features::template id<Type>();
    return nullptr != _features[kFeatureId];
  }

  template<typename Type, typename Impl = Type>
  Impl* TryGetFeature() const {
    return hasFeature<Type>() ? &getFeature<Type, Impl>() : nullptr;
  }

  // Returns a const reference to a feature. will throw when used for
  // a non-existing feature.
  template<typename Type, typename Impl = Type>
  Impl& getFeature() const {
    static_assert(std::is_base_of_v<AppFeature, Type>);
    static_assert(std::is_base_of_v<Type, Impl> ||
                  std::is_base_of_v<Impl, Type>);
    constexpr auto kFeatureId = Features::template id<Type>();
    SDB_ASSERT(hasFeature<Type>());
    return basics::downCast<Impl>(*_features[kFeatureId]);
  }

 private:
  std::array<std::unique_ptr<AppFeature>, Features::size()> _features;
};

}  // namespace app
}  // namespace sdb
namespace magic_enum {

template<>
[[maybe_unused]] constexpr customize::customize_t
customize::enum_name<sdb::app::AppServer::State>(
  sdb::app::AppServer::State state) noexcept {
  using State = sdb::app::AppServer::State;

  switch (state) {
    case State::Uninitialized:
      return "uninitialized";
    case State::InCollectOptions:
      return "in collect options";
    case State::InValidateOptions:
      return "in validate options";
    case State::InPrepare:
      return "in prepare";
    case State::InStart:
      return "in start";
    case State::InWait:
      return "in wait";
    case State::InShutdown:
      return "in beginShutdown";
    case State::InStop:
      return "in stop";
    case State::InUnprepare:
      return "in unprepare";
    case State::Stopped:
      return "in stopped";
    case State::Aborted:
      return "in aborted";
  }
  return invalid_tag;
}

}  // namespace magic_enum
