#ifndef MINIREDIS_CONFIGURE_HPP
#define MINIREDIS_CONFIGURE_HPP

#include <chrono>
#include <array>
#include <cstdint>

#include "Policy.hpp"
#include "common/ConfigureParse.hpp"
#include "spdlog/spdlog.h"

namespace MiniRedis {

    namespace ServerConfigure {

        constexpr std::string_view VERSION = "1.1";
        constexpr std::string_view DEFAULT_CONFIG_FILE_PATH = "./miniredis.conf";
        constexpr std::string_view DEFAULT_DUMP_BINARY_FILE_PATH = "./miniredis.dump";
        static std::string TCP_LISTEN_ADDRESS = "0.0.0.0";
        static uint16_t TCP_LISTEN_PORT = 9736;
        static std::string UNIX_SOCKET_FILE_PATH = "/tmp/miniredis.sock";
        static size_t WORKER_NET_IO_THREADS = 2;
        static std::string DUMP_SAVED_BINARY_FILE_PATH = "./miniredis.dump";
        static size_t MAX_PENDING_WRITE_BYTES = 4 * 1024 * 1024;
        static std::chrono::seconds CLIENT_IDLE_TIMEOUT{9999};
        static std::chrono::seconds IDLE_CHECK_INTERVAL{999};

        static std::array<DumpTriggerPolicy, 3> DUMP_TRIGGER_POLICIES = {{
            {std::chrono::milliseconds(9*1000), 1},
            {std::chrono::milliseconds(300*1000), 10},
            {std::chrono::milliseconds(60*1000), 10000}
        }};

        static void loader(std::optional<std::string_view> configureFilePath) {

            if (!configureFilePath.has_value()) {
                return;
            }

            spdlog::info("starting to load configuration file {}.", configureFilePath.value());

            ConfigureParser::ConfigureMap configureMap;
            std::string loadingErrorMessage;
            if (!ConfigureParser::parse(configureFilePath.value().data(), configureMap,loadingErrorMessage)) {
                spdlog::error("loading configuration file error: {}", loadingErrorMessage);
                return;
            }

            if (configureMap.contains("TCP_LISTEN_ADDRESS")) {
                TCP_LISTEN_ADDRESS = configureMap["TCP_LISTEN_ADDRESS"][0][0];
            }
            if (configureMap.contains("TCP_LISTEN_PORT")) {
                TCP_LISTEN_PORT = std::strtoll(configureMap["TCP_LISTEN_PORT"][0][0].c_str(), nullptr, 10);
            }
            if (configureMap.contains("WORKER_NET_IO_THREADS")) {
                WORKER_NET_IO_THREADS = std::strtoll(configureMap["WORKER_NET_IO_THREADS"][0][0].c_str(), nullptr, 10);
            }
            if (configureMap.contains("UNIX_SOCKET_FILE_PATH")) {
                UNIX_SOCKET_FILE_PATH = configureMap["UNIX_SOCKET_FILE_PATH"][0][0];
            }
            if (configureMap.contains("MAX_PENDING_WRITE_BYTES")) {
                CLIENT_IDLE_TIMEOUT = std::chrono::seconds(std::strtoll(configureMap["MAX_PENDING_WRITE_BYTES"][0][0].c_str(), nullptr, 10));
            }
            if (configureMap.contains("DUMP_SAVED_BINARY_FILE_PATH")) {
                DUMP_SAVED_BINARY_FILE_PATH = configureMap["DUMP_SAVED_BINARY_FILE_PATH"][0][0];
            }
            if (configureMap.contains("CLIENT_IDLE_TIMEOUT")) {
                CLIENT_IDLE_TIMEOUT = std::chrono::seconds(std::strtoll(configureMap["CLIENT_IDLE_TIMEOUT"][0][0].c_str(), nullptr, 10));
            }
            if (configureMap.contains("IDLE_CHECK_INTERVAL")) {
                IDLE_CHECK_INTERVAL = std::chrono::seconds(std::strtoll(configureMap["IDLE_CHECK_INTERVAL"][0][0].c_str(), nullptr, 10));
            }
            if (configureMap.contains("DUMP_TRIGGER_POLICIES")) {
                for (int i = 0; i < 3; i++) {
                    const std::chrono::seconds s(std::strtoll(configureMap["DUMP_TRIGGER_POLICIES"][i][0].c_str(), nullptr, 10));
                    const std::uint64_t d = std::strtoll(configureMap["DUMP_TRIGGER_POLICIES"][i][1].c_str(), nullptr, 10);
                    DUMP_TRIGGER_POLICIES[i] = DumpTriggerPolicy(s,d);
                }
            }

            spdlog::info("successfully loaded the configuration file.");
        }

    }

}

#endif //MINIREDIS_CONFIGURE_HPP