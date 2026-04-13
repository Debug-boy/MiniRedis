add_executable(miniredis-cli
        src/client.cpp
)

target_include_directories(miniredis-cli
        PRIVATE ${PROJECT_SOURCE_DIR}/include
)