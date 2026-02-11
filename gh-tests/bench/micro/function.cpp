// Copyright 2022 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "boost/function.hpp"

#include <functional>
#include <memory>
#include <print>
#include <string>

#include "absl/base/attributes.h"
#include "absl/functional/any_invocable.h"
#include "absl/functional/function_ref.h"
#include "benchmark/benchmark.h"
#include "function2.hpp"

// #define HAS_FOLLY
#ifdef HAS_FOLLY
#include "folly/Function.h"
#endif

namespace {

int gDummy = 0;

void FreeFunction() {
  benchmark::DoNotOptimize(gDummy);  //
}

struct TrivialFunctor {
  void operator()() const {
    benchmark::DoNotOptimize(gDummy);  //
  }
};

struct LargeFunctor {
  void operator()() const {
    benchmark::DoNotOptimize(this);  //
  }
  std::string a, b, c;
};

struct FunctorWithTrivialArgs {
  void operator()(int a, int b, int c) const {
    benchmark::DoNotOptimize(a);
    benchmark::DoNotOptimize(b);
    benchmark::DoNotOptimize(c);
  }
};

struct FunctorWithNonTrivialArgs {
  void operator()(std::string a, std::string b, std::string c) const {
    benchmark::DoNotOptimize(&a);
    benchmark::DoNotOptimize(&b);
    benchmark::DoNotOptimize(&c);
  }
};

template<typename Function, typename... Args>
void ABSL_ATTRIBUTE_NOINLINE CallFunction(Function f, Args&&... args) {
  f(std::forward<Args>(args)...);
}

template<typename Function, typename Callable, typename... Args>
void ConstructAndCallFunctionBenchmark(benchmark::State& state,
                                       const Callable& c, Args&&... args) {
  for (auto _ : state) {
    CallFunction<Function>(c, std::forward<Args>(args)...);
  }
}

void BmTrivialStdFunction(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<std::function<void()>>(state,
                                                           TrivialFunctor{});
}
BENCHMARK(BmTrivialStdFunction);

void BmTrivialAbslFunctionRef(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<absl::FunctionRef<void()>>(
    state, TrivialFunctor{});
}
BENCHMARK(BmTrivialAbslFunctionRef);

void BmTrivialAbslFunctionValue(benchmark::State& state) {
  for (auto _ : state) {
    CallFunction<absl::FunctionRef<void()>>(
      {absl::FunctionValue{}, TrivialFunctor{}});
  }
}
BENCHMARK(BmTrivialAbslFunctionValue);

void BmTrivialAbslAnyInvocable(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<absl::AnyInvocable<void()>>(
    state, TrivialFunctor{});
}
BENCHMARK(BmTrivialAbslAnyInvocable);

#ifdef HAS_FOLLY

void BM_TrivialFollyFunction(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<folly::Function<void()>>(state,
                                                             TrivialFunctor{});
}
BENCHMARK(BM_TrivialFollyFunction);

void BM_TrivialFollyFunctionRef(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<folly::FunctionRef<void()>>(
    state, TrivialFunctor{});
}
BENCHMARK(BM_TrivialFollyFunctionRef);

#endif

void BmTrivialBoostFunction(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<boost::function<void()>>(state,
                                                             TrivialFunctor{});
}
BENCHMARK(BmTrivialBoostFunction);

void BmTrivialFu2Function(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<fu2::function<void()>>(state,
                                                           TrivialFunctor{});
}
BENCHMARK(BmTrivialFu2Function);

// void BM_TrivialFu2FunctionView(benchmark::State& state) {
//   ConstructAndCallFunctionBenchmark<fu2::function_view<void()>>(
//     state, TrivialFunctor{});
// }
// BENCHMARK(BM_TrivialFu2FunctionView);

void BmTrivialFu2UniqueFunction(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<fu2::unique_function<void()>>(
    state, TrivialFunctor{});
}
BENCHMARK(BmTrivialFu2UniqueFunction);

//////////////////////////////////////////////////////////////////////////////

void BmLargeStdFunction(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<std::function<void()>>(state,
                                                           LargeFunctor{});
}
BENCHMARK(BmLargeStdFunction);

void BmLargeAbslFunctionRef(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<absl::FunctionRef<void()>>(state,
                                                               LargeFunctor{});
}
BENCHMARK(BmLargeAbslFunctionRef);

void BmLargeAbslAnyInvocable(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<absl::AnyInvocable<void()>>(state,
                                                                LargeFunctor{});
}
BENCHMARK(BmLargeAbslAnyInvocable);

#ifdef HAS_FOLLY

void BM_LargeFollyFunction(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<folly::Function<void()>>(state,
                                                             LargeFunctor{});
}
BENCHMARK(BM_LargeFollyFunction);

void BM_LargeFollyFunctionRef(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<folly::FunctionRef<void()>>(state,
                                                                LargeFunctor{});
}
BENCHMARK(BM_LargeFollyFunctionRef);

#endif

void BmLargeBoostFunction(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<boost::function<void()>>(state,
                                                             LargeFunctor{});
}
BENCHMARK(BmLargeBoostFunction);

void BmLargeFu2Function(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<fu2::function<void()>>(state,
                                                           LargeFunctor{});
}
BENCHMARK(BmLargeFu2Function);

// void BM_LargeFu2FunctionView(benchmark::State& state) {
//   ConstructAndCallFunctionBenchmark<fu2::function_view<void()>>(state,
//                                                                 LargeFunctor{});
// }
// BENCHMARK(BM_LargeFu2FunctionView);

void BmLargeFu2UniqueFunction(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<fu2::unique_function<void()>>(
    state, LargeFunctor{});
}
BENCHMARK(BmLargeFu2UniqueFunction);

//////////////////////////////////////////////////////////////////////////////

void BmFunPtrStdFunction(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<std::function<void()>>(state, FreeFunction);
}
BENCHMARK(BmFunPtrStdFunction);

void BmFunPtrAbslFunctionRef(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<absl::FunctionRef<void()>>(state,
                                                               FreeFunction);
}
BENCHMARK(BmFunPtrAbslFunctionRef);

void BmFunPtrAbslFunctionValue(benchmark::State& state) {
  for (auto _ : state) {
    CallFunction<absl::FunctionRef<void()>>(
      {absl::FunctionValue{}, [] { FreeFunction(); }});
  }
}
BENCHMARK(BmFunPtrAbslFunctionValue);

void BmFunPtrAbslAnyInvocable(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<absl::AnyInvocable<void()>>(state,
                                                                FreeFunction);
}
BENCHMARK(BmFunPtrAbslAnyInvocable);

#ifdef HAS_FOLLY

void BM_FunPtrFollyFunction(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<folly::Function<void()>>(state,
                                                             FreeFunction);
}
BENCHMARK(BM_FunPtrFollyFunction);

void BM_FunPtrFollyFunctionRef(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<folly::FunctionRef<void()>>(state,
                                                                FreeFunction);
}
BENCHMARK(BM_FunPtrFollyFunctionRef);

#endif

void BmFunPtrBoostFunction(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<boost::function<void()>>(state,
                                                             FreeFunction);
}
BENCHMARK(BmFunPtrBoostFunction);

void BmFunPtrFu2Function(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<fu2::function<void()>>(state, FreeFunction);
}
BENCHMARK(BmFunPtrFu2Function);

void BmFunPtrFu2FunctionView(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<fu2::function_view<void()>>(state,
                                                                FreeFunction);
}
BENCHMARK(BmFunPtrFu2FunctionView);

void BmFunPtrFu2UniqueFunction(benchmark::State& state) {
  ConstructAndCallFunctionBenchmark<fu2::unique_function<void()>>(state,
                                                                  FreeFunction);
}
BENCHMARK(BmFunPtrFu2UniqueFunction);

//////////////////////////////////////////////////////////////////////////////

// Doesn't include construction or copy overhead in the loop.
template<typename Function, typename Callable, typename... Args>
void CallFunctionBenchmark(benchmark::State& state, const Callable& c,
                           Args... args) {
  Function f = c;
  for (auto _ : state) {
    benchmark::DoNotOptimize(&f);
    f(args...);
  }
}

void BmTrivialArgsStdFunction(benchmark::State& state) {
  CallFunctionBenchmark<std::function<void(int, int, int)>>(
    state, FunctorWithTrivialArgs{}, 1, 2, 3);
}
BENCHMARK(BmTrivialArgsStdFunction);

void BmTrivialArgsAbslFunctionRef(benchmark::State& state) {
  CallFunctionBenchmark<absl::FunctionRef<void(int, int, int)>>(
    state, FunctorWithTrivialArgs{}, 1, 2, 3);
}
BENCHMARK(BmTrivialArgsAbslFunctionRef);

void BmTrivialArgsAbslFunctionValue(benchmark::State& state) {
  CallFunctionBenchmark<absl::FunctionRef<void(int, int, int)>>(
    state,
    absl::FunctionRef<void(int, int, int)>{absl::FunctionValue{},
                                           FunctorWithTrivialArgs{}},
    1, 2, 3);
}
BENCHMARK(BmTrivialArgsAbslFunctionValue);

void BmTrivialArgsAbslAnyInvocable(benchmark::State& state) {
  CallFunctionBenchmark<absl::AnyInvocable<void(int, int, int)>>(
    state, FunctorWithTrivialArgs{}, 1, 2, 3);
}
BENCHMARK(BmTrivialArgsAbslAnyInvocable);

#ifdef HAS_FOLLY

void BM_TrivialArgsFollyFunction(benchmark::State& state) {
  CallFunctionBenchmark<folly::Function<void(int, int, int)>>(
    state, FunctorWithTrivialArgs{}, 1, 2, 3);
}
BENCHMARK(BM_TrivialArgsFollyFunction);

void BM_TrivialArgsFollyFunctionRef(benchmark::State& state) {
  CallFunctionBenchmark<folly::FunctionRef<void(int, int, int)>>(
    state, FunctorWithTrivialArgs{}, 1, 2, 3);
}
BENCHMARK(BM_TrivialArgsFollyFunctionRef);

#endif

void BmTrivialArgsBoostFunction(benchmark::State& state) {
  CallFunctionBenchmark<boost::function<void(int, int, int)>>(
    state, FunctorWithTrivialArgs{}, 1, 2, 3);
}
BENCHMARK(BmTrivialArgsBoostFunction);

void BmTrivialArgsFu2Function(benchmark::State& state) {
  CallFunctionBenchmark<fu2::function<void(int, int, int)>>(
    state, FunctorWithTrivialArgs{}, 1, 2, 3);
}
BENCHMARK(BmTrivialArgsFu2Function);

// void BM_TrivialArgsFu2FunctionView(benchmark::State& state) {
//   CallFunctionBenchmark<fu2::function_view<void(int, int, int)>>(
//     state, FunctorWithTrivialArgs{}, 1, 2, 3);
// }
// BENCHMARK(BM_TrivialArgsFu2FunctionView);

void BmTrivialArgsFu2UniqueFunction(benchmark::State& state) {
  CallFunctionBenchmark<fu2::unique_function<void(int, int, int)>>(
    state, FunctorWithTrivialArgs{}, 1, 2, 3);
}
BENCHMARK(BmTrivialArgsFu2UniqueFunction);

//////////////////////////////////////////////////////////////////////////////

void BmNonTrivialArgsStdFunction(benchmark::State& state) {
  std::string a, b, c;
  CallFunctionBenchmark<
    std::function<void(std::string, std::string, std::string)>>(
    state, FunctorWithNonTrivialArgs{}, a, b, c);
}
BENCHMARK(BmNonTrivialArgsStdFunction);

void BmNonTrivialArgsAbslFunctionRef(benchmark::State& state) {
  std::string a, b, c;
  CallFunctionBenchmark<
    absl::FunctionRef<void(std::string, std::string, std::string)>>(
    state, FunctorWithNonTrivialArgs{}, a, b, c);
}
BENCHMARK(BmNonTrivialArgsAbslFunctionRef);

void BmNonTrivialArgsAbslFunctionValue(benchmark::State& state) {
  std::string a, b, c;
  CallFunctionBenchmark<
    absl::FunctionRef<void(std::string, std::string, std::string)>>(
    state,
    absl::FunctionRef<void(std::string, std::string, std::string)>{
      absl::FunctionValue{}, FunctorWithNonTrivialArgs{}},
    a, b, c);
}
BENCHMARK(BmNonTrivialArgsAbslFunctionValue);

void BmNonTrivialArgsAbslAnyInvocable(benchmark::State& state) {
  std::string a, b, c;
  CallFunctionBenchmark<
    absl::AnyInvocable<void(std::string, std::string, std::string)>>(
    state, FunctorWithNonTrivialArgs{}, a, b, c);
}
BENCHMARK(BmNonTrivialArgsAbslAnyInvocable);

#ifdef HAS_FOLLY

void BM_NonTrivialArgsFollyFunction(benchmark::State& state) {
  std::string a, b, c;
  CallFunctionBenchmark<
    folly::Function<void(std::string, std::string, std::string)>>(
    state, FunctorWithNonTrivialArgs{}, a, b, c);
}
BENCHMARK(BM_NonTrivialArgsFollyFunction);

void BM_NonTrivialArgsFollyFunctionRef(benchmark::State& state) {
  std::string a, b, c;
  CallFunctionBenchmark<
    folly::FunctionRef<void(std::string, std::string, std::string)>>(
    state, FunctorWithNonTrivialArgs{}, a, b, c);
}
BENCHMARK(BM_NonTrivialArgsFollyFunctionRef);

#endif

void BmNonTrivialArgsBoostFunction(benchmark::State& state) {
  std::string a, b, c;
  CallFunctionBenchmark<
    boost::function<void(std::string, std::string, std::string)>>(
    state, FunctorWithNonTrivialArgs{}, a, b, c);
}
BENCHMARK(BmNonTrivialArgsBoostFunction);

void BmNonTrivialArgsFu2Function(benchmark::State& state) {
  std::string a, b, c;
  CallFunctionBenchmark<
    fu2::function<void(std::string, std::string, std::string)>>(
    state, FunctorWithNonTrivialArgs{}, a, b, c);
}
BENCHMARK(BmNonTrivialArgsFu2Function);

// void BM_NonTrivialArgsFu2FunctionView(benchmark::State& state) {
//   std::string a, b, c;
//   CallFunctionBenchmark<
//       fu2::function_view<void(std::string, std::string, std::string)>>(
//       state, FunctorWithNonTrivialArgs{}, a, b, c);
// }
// BENCHMARK(BM_NonTrivialArgsFu2FunctionView);

void BmNonTrivialArgsFu2UniqueFunction(benchmark::State& state) {
  std::string a, b, c;
  CallFunctionBenchmark<
    fu2::unique_function<void(std::string, std::string, std::string)>>(
    state, FunctorWithNonTrivialArgs{}, a, b, c);
}
BENCHMARK(BmNonTrivialArgsFu2UniqueFunction);

}  // namespace

BENCHMARK_MAIN();
