//
// Created by Administrator on 2026/4/12.
//

#ifndef MINIREDIS_CONNECTION_HPP
#define MINIREDIS_CONNECTION_HPP

#include <chrono>
#include <vector>
#include <deque>

#include "Channel.hpp"

namespace MiniRedis {

    struct Connection {
        int socketFd = -1;
        int isUnixSocket = 0;
        uint32_t registerEvents{};
        std::string queryBuffer;
        std::deque<std::vector<std::string>> argvPipeline;
        std::deque<std::string> replyQueue;
        size_t replyBytes = 0;
        size_t sendOffset = 0;
        std::chrono::steady_clock::time_point lastActive = std::chrono::steady_clock::now();
        Channel fdContext{Channel::Type::Client, -1};

        [[nodiscard]] bool isExceedActiveDuration(const std::chrono::duration<uint64_t> duration) const {
            return (std::chrono::steady_clock::now() - this->lastActive) > duration;
        }

        [[nodiscard]] bool hasRegisterReadEvent() const {
            return registerEvents & EPOLLIN;
        }

        [[nodiscard]] bool hasRegisterWriteEvent() const {
            return registerEvents & EPOLLOUT;
        }

        void closeSocketOnly() {
            if (socketFd != -1) {
                ::close(socketFd);
                socketFd = -1;
                fdContext.fd = -1;
            }
        }
    };


}

#endif //MINIREDIS_CONNECTION_HPP