#pragma once

#define FAKE_FMT 1

#include <atomic>
#include <format>
#include <filesystem>

template <>
struct std::formatter<std::filesystem::path> : formatter<std::string> {
  template<typename FormatContext>
  auto format(const std::filesystem::path& v, FormatContext& ctx) const {
    return formatter<std::string>::format(v.string(), ctx);
  }
};

template<typename T>
struct std::formatter<std::atomic<T>> : formatter<T> {
  template<typename FormatContext>
  auto format(const std::atomic<T>& v, FormatContext& ctx) const {
    return formatter<T>::format(v.load(), ctx);
  }
};

#define fmt std
#define FMT_VERSION 100100
#define FMT_COMPILE(...) __VA_ARGS__
