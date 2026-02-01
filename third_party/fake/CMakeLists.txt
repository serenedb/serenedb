if (SDB_FAKE_FMT)
  add_library(fake_fmt INTERFACE)
  target_include_directories(fake_fmt INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/fmt)
  add_library(fmt::fmt ALIAS fake_fmt)
endif()

add_library(fake_gflags INTERFACE)
target_include_directories(fake_gflags INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/gflags)
target_link_libraries(fake_gflags
  INTERFACE absl::flags_parse
  INTERFACE absl::flags_reflection
  INTERFACE absl::flags_usage
)
add_library(gflags::gflags ALIAS fake_gflags)

add_library(fake_glog INTERFACE)
target_include_directories(fake_glog INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/glog)
add_library(glog::glog ALIAS fake_glog)
