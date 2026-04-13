#ifndef MINIREDIS_CONFIGURE_HPP
#define MINIREDIS_CONFIGURE_HPP

#include <chrono>
#include <cstdint>

namespace MiniRedis {

    namespace Configure {

        namespace ServerRuntime {

            constexpr size_t MaxPendingWriteBytes = 4 * 1024 * 1024;
            constexpr std::chrono::seconds ClientIdleTimeout{9999};
            constexpr std::chrono::seconds IdleCheckInterval{999};
            constexpr std::string TcpListenAddress = "0.0.0.0";
            constexpr uint16_t TcpListenPort = 9736;
            constexpr size_t WorkerNetIoThreads = 2;
            std::string UnixSocketPath = "/tmp/miniredis.sock";

        }

    }

}

#endif //MINIREDIS_CONFIGURE_HPP