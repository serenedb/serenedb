#pragma once

#include <absl/flags/flag.h>
#include <absl/flags/reflection.h>

#include "gflags_declare.h"

namespace gflags {

using absl::FlagSaver;

template<typename T>
struct Flag : absl::Flag<T> {
  using Base = absl::Flag<T>;

  using Base::Base;

  /*implicit*/ operator T() const { return absl::GetFlag(*this); }
};

}  // namespace gflags

#define GFLAGS_FLAG_IMPL(Type, name, default_value, help)           \
  extern ::gflags::Flag<Type> FLAGS_##name;                         \
  ABSL_FLAG_IMPL_DECLARE_DEF_VAL_WRAPPER(name, Type, default_value) \
  ABSL_FLAG_IMPL_DECLARE_HELP_WRAPPER(name, help)                   \
  ABSL_CONST_INIT ::gflags::Flag<Type> FLAGS_##name{                \
    ABSL_FLAG_IMPL_FLAGNAME(#name), ABSL_FLAG_IMPL_TYPENAME(#Type), \
    ABSL_FLAG_IMPL_FILENAME(), ABSL_FLAG_IMPL_HELP_ARG(name),       \
    ABSL_FLAG_IMPL_DEFAULT_ARG(Type, name)};                        \
  extern ::absl::flags_internal::FlagRegistrarEmpty FLAGS_no##name; \
  ::absl::flags_internal::FlagRegistrarEmpty FLAGS_no##name =       \
    ABSL_FLAG_IMPL_REGISTRAR(Type, FLAGS_##name)

#define DEFINE_bool(name, default_value, help) \
  GFLAGS_FLAG_IMPL(bool, name, default_value, help)
#define DEFINE_int32(name, default_value, help) \
  GFLAGS_FLAG_IMPL(int32_t, name, default_value, help)
#define DEFINE_int64(name, default_value, help) \
  GFLAGS_FLAG_IMPL(int64_t, name, default_value, help)
#define DEFINE_uint64(name, default_value, help) \
  GFLAGS_FLAG_IMPL(uint64_t, name, default_value, help)
#define DEFINE_double(name, default_value, help) \
  GFLAGS_FLAG_IMPL(double, name, default_value, help)
#define DEFINE_string(name, default_value, help) \
  GFLAGS_FLAG_IMPL(std::string, name, default_value, help)

namespace std {

template<>
struct formatter<gflags::Flag<std::string>> : formatter<std::string> {
  auto format(const gflags::Flag<std::string>& flag,
              format_context& ctx) const {
    return formatter<std::string>::format(flag, ctx);
  }
};

}  // namespace std
