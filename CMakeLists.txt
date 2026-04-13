cmake_minimum_required(VERSION 3.22.1)
project(MiniRedis)

set(CMAKE_CXX_STANDARD 23)

set(SPDLOG_BUILD_TESTS OFF)
set(SPDLOG_BUILD_EXAMPLE OFF)
set(SPDLOG_BUILD_BENCH OFF)

add_subdirectory(third_party/spdlog)

add_subdirectory(server)
add_subdirectory(client)