add_executable(miniredis-server
        src/server.cpp
)

target_include_directories(miniredis-server
        PRIVATE ${PROJECT_SOURCE_DIR}/include
        PRIVATE ${PROJECT_SOURCE_DIR}/server/include
)

target_link_libraries(
        miniredis-server PRIVATE spdlog::spdlog
)