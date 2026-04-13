
#ifndef MINIREDIS_SYSERROR_HPP
#define MINIREDIS_SYSERROR_HPP

#include <cstring>
#include <string>

namespace MiniRedis {
    static std::string getSysLastError(const std::string &op) {
        return op + ": " + std::string(std::strerror(errno));
    }
}

#endif //MINIREDIS_SYSERROR_HPP